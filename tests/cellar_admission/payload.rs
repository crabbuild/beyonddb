//! Pause actual coordinator LTX publication; ownership CAS remains SDK-owned.
use futures_util::stream::BoxStream;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use std::{
    collections::BTreeSet,
    fmt,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Default, Debug)]
pub struct Control(Mutex<Option<Weak<Barrier>>>);
#[derive(Debug)]
struct Barrier {
    prefix: String,
    scopes: Mutex<BTreeSet<String>>,
    calls: AtomicUsize,
    changed: Notify,
    release: CancellationToken,
}
pub struct Held(Arc<Barrier>);
impl Held {
    pub async fn scopes(&self, count: usize) {
        loop {
            let changed = self.0.changed.notified();
            if self.0.scopes.lock().unwrap().len() >= count {
                break;
            }
            changed.await;
        }
    }
    pub fn observed(&self) -> BTreeSet<String> {
        self.0.scopes.lock().unwrap().clone()
    }
    pub fn calls(&self) -> usize {
        self.0.calls.load(Ordering::SeqCst)
    }
    pub fn release(&self) {
        self.0.release.cancel();
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
            scopes: Mutex::new(BTreeSet::new()),
            calls: AtomicUsize::new(0),
            changed: Notify::new(),
            release: CancellationToken::new(),
        });
        *current = Some(Arc::downgrade(&barrier));
        Held(barrier)
    }
    async fn before(&self, path: &Path) {
        let barrier = self.0.lock().unwrap().as_ref().and_then(Weak::upgrade);
        if let Some(barrier) = barrier
            && path.as_ref().starts_with(&barrier.prefix)
            && let Some((scope, _)) = path
                .as_ref()
                .strip_prefix("cells/")
                .unwrap()
                .split_once("/ltx/")
        {
            barrier.scopes.lock().unwrap().insert(scope.into());
            barrier.calls.fetch_add(1, Ordering::SeqCst);
            barrier.changed.notify_one();
            barrier.release.cancelled().await;
        }
    }
}
#[derive(Debug)]
pub struct Store {
    pub inner: Arc<dyn ObjectStore>,
    pub control: Arc<Control>,
}
impl fmt::Display for Store {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("ColdCoordinatorPayloadGate")
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
        self.control.before(path).await;
        self.inner.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.control.before(path).await;
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
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
