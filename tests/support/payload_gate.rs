//! Hold real provider payload PUTs; SDK-owned authority and leases stay intact.
use futures_util::stream::BoxStream;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use std::{
    fmt,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
pub struct Control {
    held: Mutex<Option<(String, Weak<Barrier>)>>,
}
#[derive(Debug)]
struct Barrier {
    entered: Semaphore,
    release: CancellationToken,
}
pub struct Held(Arc<Barrier>);
impl Held {
    pub async fn entered(&self) {
        self.0.entered.acquire().await.unwrap().forget();
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
    pub fn hold(&self, prefix: String) -> Held {
        let barrier = Arc::new(Barrier {
            entered: Semaphore::new(0),
            release: CancellationToken::new(),
        });
        let mut held = self.held.lock().unwrap();
        assert!(held.as_ref().and_then(|(_, old)| old.upgrade()).is_none());
        *held = Some((prefix, Arc::downgrade(&barrier)));
        Held(barrier)
    }
    async fn before(&self, path: &Path) {
        let barrier = self
            .held
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|(prefix, barrier)| {
                path.as_ref()
                    .starts_with(prefix)
                    .then(|| barrier.upgrade())
                    .flatten()
            });
        if let Some(barrier) = barrier {
            barrier.entered.add_permits(1);
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
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NativePayloadGate")
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
