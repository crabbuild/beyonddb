//! Hold a real owner result or conditional PUT, preserving its body and ETag.
use futures_util::stream::BoxStream;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use std::{
    fmt,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Boundary {
    Lookup,
    Claim,
}
#[derive(Debug, Default)]
pub struct Control(Mutex<Option<Weak<Barrier>>>);
#[derive(Debug)]
struct Barrier {
    path: String,
    boundary: Boundary,
    armed: AtomicBool,
    etag: Mutex<Option<String>>,
    entered: CancellationToken,
    release: CancellationToken,
    completed: CancellationToken,
    rejected: AtomicBool,
}
pub struct Held(Arc<Barrier>);
impl Control {
    pub fn hold(&self, scope: &str, boundary: Boundary) -> Held {
        let mut current = self.0.lock().unwrap();
        assert!(current.as_ref().and_then(Weak::upgrade).is_none());
        let barrier = Arc::new(Barrier {
            path: format!("cells/{scope}/own.json"),
            boundary,
            armed: AtomicBool::new(true),
            etag: Mutex::new(None),
            entered: CancellationToken::new(),
            release: CancellationToken::new(),
            completed: CancellationToken::new(),
            rejected: AtomicBool::new(false),
        });
        *current = Some(Arc::downgrade(&barrier));
        Held(barrier)
    }
    fn take(&self, path: &Path, boundary: Boundary) -> Option<Arc<Barrier>> {
        self.0
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)
            .filter(|held| {
                held.path == path.as_ref()
                    && held.boundary == boundary
                    && held.armed.swap(false, Ordering::SeqCst)
            })
    }
}
impl Held {
    pub async fn entered(&self) {
        self.0.entered.cancelled().await;
    }
    pub fn etag(&self) -> String {
        self.0.etag.lock().unwrap().clone().unwrap()
    }
    pub fn release(&self) {
        self.0.release.cancel();
    }
    pub async fn rejected(&self) {
        self.0.completed.cancelled().await;
        assert!(
            self.0.rejected.load(Ordering::SeqCst),
            "delayed SDK CAS did not receive a conditional rejection"
        );
    }
}
impl Drop for Held {
    fn drop(&mut self) {
        self.release();
    }
}
#[derive(Debug)]
pub struct Store {
    pub inner: Arc<dyn ObjectStore>,
    pub control: Arc<Control>,
}
impl fmt::Display for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SdkOwnerBoundary")
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
        let held = if let PutMode::Update(version) = &options.mode {
            self.control.take(path, Boundary::Claim).inspect(|held| {
                *held.etag.lock().unwrap() = version.e_tag.clone();
            })
        } else {
            None
        };
        if let Some(held) = &held {
            held.entered.cancel();
            held.release.cancelled().await;
        }
        let result = self.inner.put_opts(path, payload, options).await;
        if let Some(held) = held {
            held.rejected.store(
                matches!(
                    &result,
                    Err(object_store::Error::Precondition { .. }
                        | object_store::Error::AlreadyExists { .. })
                ),
                Ordering::SeqCst,
            );
            held.completed.cancel();
        }
        result
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        // Obtain the actual snapshot first. The held caller later receives this
        // same provider result even when another SDK changes the owner record.
        let result = self.inner.get_opts(path, options).await?;
        if let Some(held) = self.control.take(path, Boundary::Lookup) {
            *held.etag.lock().unwrap() = result.meta.e_tag.clone();
            held.entered.cancel();
            held.release.cancelled().await;
        }
        Ok(result)
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
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
