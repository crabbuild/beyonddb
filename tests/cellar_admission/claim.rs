//! Delay one SDK absent-owner CAS; forward its original payload and condition.
use futures_util::stream::BoxStream;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use std::{
    fmt,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Default, Debug)]
pub struct Control(Mutex<Option<Weak<Barrier>>>);
#[derive(Debug)]
struct Barrier {
    prefix: String,
    armed: AtomicBool,
    reads: AtomicUsize,
    read_scopes: Mutex<std::collections::BTreeSet<String>>,
    creates: AtomicUsize,
    changed: tokio::sync::Notify,
    applied: AtomicBool,
    scope: Mutex<Option<String>>,
    entered: Semaphore,
    release: CancellationToken,
    completed: CancellationToken,
    rejected: AtomicBool,
}
pub struct Held(Arc<Barrier>);
impl Held {
    pub async fn entered(&self) {
        self.0.entered.acquire().await.unwrap().forget();
    }
    pub fn creates(&self) -> usize {
        self.0.creates.load(Ordering::SeqCst)
    }
    pub async fn creates_at_least(&self, count: usize) {
        loop {
            let changed = self.0.changed.notified();
            if self.creates() >= count {
                break;
            }
            changed.await;
        }
    }
    pub async fn applied(&self) {
        self.0.completed.cancelled().await;
        assert!(
            self.0.applied.load(Ordering::SeqCst),
            "accepted SDK claim did not complete at the provider"
        );
    }
    pub async fn read_scopes_at_least(&self, count: usize) {
        loop {
            let changed = self.0.changed.notified();
            if self.0.read_scopes.lock().unwrap().len() >= count {
                break;
            }
            changed.await;
        }
    }
    pub fn reads(&self) -> usize {
        self.0.reads.load(Ordering::SeqCst)
    }
    pub fn scope(&self) -> String {
        self.0.scope.lock().unwrap().clone().unwrap()
    }
    pub fn release(&self) {
        self.0.release.cancel();
    }
    pub async fn rejected(&self) {
        self.0.completed.cancelled().await;
        assert!(
            self.0.rejected.load(Ordering::SeqCst),
            "SDK absent-owner CAS did not receive a clean conditional rejection"
        );
    }
}
impl Drop for Held {
    fn drop(&mut self) {
        self.release();
    }
}
impl Control {
    pub fn hold(&self, module: &str) -> Held {
        let mut current = self.0.lock().unwrap();
        assert!(current.as_ref().and_then(Weak::upgrade).is_none());
        let barrier = Arc::new(Barrier {
            prefix: format!("cells/{module}:"),
            armed: AtomicBool::new(true),
            reads: AtomicUsize::new(0),
            read_scopes: Mutex::new(std::collections::BTreeSet::new()),
            creates: AtomicUsize::new(0),
            changed: tokio::sync::Notify::new(),
            applied: AtomicBool::new(false),
            scope: Mutex::new(None),
            entered: Semaphore::new(0),
            release: CancellationToken::new(),
            completed: CancellationToken::new(),
            rejected: AtomicBool::new(false),
        });
        *current = Some(Arc::downgrade(&barrier));
        Held(barrier)
    }
}
#[derive(Debug)]
pub struct Store {
    pub inner: Arc<dyn ObjectStore>,
    pub control: Arc<Control>,
}
impl fmt::Display for Store {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("SdkAbsentOwnerClaimGate")
    }
}
#[async_trait::async_trait]
impl ObjectStore for Store {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let barrier = self
            .control
            .0
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)
            .filter(|barrier| {
                if matches!(options.mode, PutMode::Create)
                    && path.as_ref().starts_with(&barrier.prefix)
                    && path.as_ref().ends_with("/own.json")
                {
                    barrier.creates.fetch_add(1, Ordering::SeqCst);
                    barrier.changed.notify_one();
                    barrier.armed.swap(false, Ordering::SeqCst)
                } else {
                    false
                }
            });
        if let Some(barrier) = &barrier {
            *barrier.scope.lock().unwrap() = Some(
                path.as_ref()
                    .strip_prefix("cells/")
                    .unwrap()
                    .strip_suffix("/own.json")
                    .unwrap()
                    .into(),
            );
            barrier.entered.add_permits(1);
            barrier.release.cancelled().await;
        }
        let result = self.inner.put_opts(path, payload, options).await;
        if let Some(barrier) = barrier {
            barrier.applied.store(result.is_ok(), Ordering::SeqCst);
            barrier.rejected.store(
                matches!(
                    &result,
                    Err(object_store::Error::AlreadyExists { .. }
                        | object_store::Error::Precondition { .. })
                ),
                Ordering::SeqCst,
            );
            barrier.completed.cancel();
        }
        result
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let barrier = self
            .control
            .0
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade);
        if let Some(barrier) = barrier
            && path.as_ref().starts_with(&barrier.prefix)
            && path.as_ref().ends_with("/own.json")
        {
            barrier.reads.fetch_add(1, Ordering::SeqCst);
            barrier.read_scopes.lock().unwrap().insert(
                path.as_ref()
                    .strip_prefix("cells/")
                    .unwrap()
                    .strip_suffix("/own.json")
                    .unwrap()
                    .into(),
            );
            barrier.changed.notify_one();
        }
        self.inner.get_opts(path, options).await
    }
    async fn delete(&self, path: &Path) -> object_store::Result<()> {
        self.inner.delete(path).await
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
