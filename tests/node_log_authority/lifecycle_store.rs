//! Hold real provider requests; never construct lease or ownership records.
use futures_util::{StreamExt, stream::BoxStream};
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub struct Gate {
    pub entered: CancellationToken,
    pub release: CancellationToken,
    pub applied: AtomicUsize,
    pub conflicts: AtomicUsize,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}
impl Gate {
    pub async fn finish(&self) {
        self.release.cancel();
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        for task in tasks {
            tokio::time::timeout(std::time::Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap();
        }
    }
}
impl Drop for Gate {
    fn drop(&mut self) {
        self.release.cancel();
        for task in self.tasks.get_mut().unwrap().drain(..) {
            task.abort();
        }
    }
}

pub struct LifecycleStore {
    inner: Arc<dyn ObjectStore>,
    lease: Path,
    reads: Mutex<Option<Arc<Gate>>>,
    writes: Mutex<Option<Arc<Gate>>>,
    provider: tokio::runtime::Handle,
    domain: Arc<cellar::sdk::ExecutionDomain>,
}
impl LifecycleStore {
    pub fn new(inner: Arc<dyn ObjectStore>, node: &str, provider: tokio::runtime::Handle) -> Self {
        Self {
            inner,
            lease: Path::from(format!("nodes/{node}.json")),
            reads: Default::default(),
            writes: Default::default(),
            domain: Arc::new(cellar::sdk::ExecutionDomain::new(provider.clone())),
            provider,
        }
    }
    pub fn hold_reads(&self) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        assert!(self.reads.lock().unwrap().replace(gate.clone()).is_none());
        gate
    }
    pub fn hold_lease_writes(&self) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        assert!(self.writes.lock().unwrap().replace(gate.clone()).is_none());
        gate
    }
    pub async fn close(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(5), self.domain.shutdown())
            .await
            .unwrap();
    }
    async fn read_barrier(&self) {
        let gate = self.reads.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.entered.cancel();
            gate.release.cancelled().await;
        }
    }
}
impl Drop for LifecycleStore {
    fn drop(&mut self) {
        self.domain.close();
    }
}
impl fmt::Display for LifecycleStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NativeLifecycleStore")
    }
}
impl fmt::Debug for LifecycleStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
#[async_trait::async_trait]
impl ObjectStore for LifecycleStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let gate = (path == &self.lease)
            .then(|| self.writes.lock().unwrap().clone())
            .flatten();
        let Some(gate) = gate else {
            return self.inner.put_opts(path, payload, options).await;
        };
        // An accepted provider request can finish after its caller is cancelled.
        // Run on the fixture's caller-owned executor and join it explicitly.
        // Preserve the SDK's exact payload, conditional options and ETag.
        let inner = self.inner.clone();
        let path = path.clone();
        let request = gate.clone();
        let (sent, received) = tokio::sync::oneshot::channel();
        let domain = self.domain.clone();
        let task = self.provider.spawn(async move {
            request.entered.cancel();
            request.release.cancelled().await;
            let result = domain
                .run(async move { inner.put_opts(&path, payload, options).await })
                .await
                .unwrap();
            match &result {
                Ok(_) => {
                    request.applied.fetch_add(1, Ordering::SeqCst);
                }
                Err(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. },
                ) => {
                    request.conflicts.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            }
            let _ = sent.send(result);
        });
        gate.tasks.lock().unwrap().push(task);
        received
            .await
            .map_err(|error| object_store::Error::Generic {
                store: "NativeLifecycleStore",
                source: error.into(),
            })?
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.read_barrier().await;
        self.inner.get_opts(path, options).await
    }
    async fn delete(&self, path: &Path) -> object_store::Result<()> {
        self.inner.delete(path).await
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let gate = self.reads.lock().unwrap().clone();
        let Some(gate) = gate else {
            return self.inner.list(prefix);
        };
        let prefix = prefix.cloned();
        let inner = self.inner.clone();
        futures_util::stream::once(async move {
            gate.entered.cancel();
            gate.release.cancelled().await;
            inner.list(prefix.as_ref())
        })
        .flatten()
        .boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.read_barrier().await;
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
