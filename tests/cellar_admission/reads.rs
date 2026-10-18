//! Count managed-cell provider GETs separately from SDK fleet membership I/O.
use futures_util::stream::BoxStream;
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
#[derive(Default, Debug)]
pub struct Reads {
    pub cells: AtomicUsize,
    pub coordinator: AtomicUsize,
    scope: Mutex<String>,
}
impl Reads {
    pub fn watch(&self, scope: &str) {
        *self.scope.lock().unwrap() = format!("cells/{scope}/");
    }
    pub fn reset(&self) {
        self.cells.store(0, Ordering::SeqCst);
        self.coordinator.store(0, Ordering::SeqCst);
    }
}
#[derive(Debug)]
pub struct Store {
    pub inner: Arc<dyn ObjectStore>,
    pub reads: Arc<Reads>,
}
impl fmt::Display for Store {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("CoordinatorAdmissionReads")
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
        self.inner.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if path.as_ref().starts_with("cells/") {
            self.reads.cells.fetch_add(1, Ordering::SeqCst);
        }
        let selected = {
            let scope = self.reads.scope.lock().unwrap();
            !scope.is_empty() && path.as_ref().starts_with(scope.as_str())
        };
        if selected {
            self.reads.coordinator.fetch_add(1, Ordering::SeqCst);
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
