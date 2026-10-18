//! Inject transport faults without constructing or modifying authority records.
use futures_util::stream::BoxStream;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub struct HeldRead {
    pub entered: CancellationToken,
    pub release: CancellationToken,
}
impl Drop for HeldRead {
    fn drop(&mut self) {
        self.release.cancel();
    }
}

pub struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    path: Path,
    held: Mutex<Option<Arc<HeldRead>>>,
    pub delay_ms: AtomicU64,
    pub lose_reply: AtomicBool,
    pub refuse_writes: AtomicBool,
    pub gets: AtomicUsize,
    pub applied: AtomicUsize,
    pub conflicts: AtomicUsize,
    pub lost_replies: AtomicUsize,
    pub refused: AtomicUsize,
    pub withhold_payloads: AtomicBool,
    pub blocked_payloads: AtomicUsize,
}
impl FaultStore {
    pub fn new(inner: Arc<dyn ObjectStore>, node: &str) -> Self {
        Self {
            inner,
            path: Path::from(format!("nodes/{node}.json")),
            held: Mutex::new(None),
            delay_ms: AtomicU64::new(0),
            lose_reply: AtomicBool::new(false),
            refuse_writes: AtomicBool::new(false),
            gets: AtomicUsize::new(0),
            applied: AtomicUsize::new(0),
            conflicts: AtomicUsize::new(0),
            lost_replies: AtomicUsize::new(0),
            refused: AtomicUsize::new(0),
            withhold_payloads: AtomicBool::new(false),
            blocked_payloads: AtomicUsize::new(0),
        }
    }
    async fn payload_barrier(&self, path: &Path) {
        let key = path.as_ref();
        if (key.contains("/ltx/") || key.contains("/bundle/"))
            && self.withhold_payloads.load(Ordering::SeqCst)
        {
            self.blocked_payloads.fetch_add(1, Ordering::SeqCst);
            // Withhold admission, including multipart creation. Never forward
            // the payload while the test's publication barrier remains closed.
            while self.withhold_payloads.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    pub fn hold_next_read(&self) -> Arc<HeldRead> {
        let hold = Arc::new(HeldRead::default());
        assert!(self.held.lock().unwrap().replace(hold.clone()).is_none());
        hold
    }
}
impl fmt::Display for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NativeLogFaultStore")
    }
}
impl fmt::Debug for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
fn unavailable(message: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "NativeLogFaultStore",
        source: std::io::Error::other(message).into(),
    }
}
#[async_trait::async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.payload_barrier(path).await;
        if path != &self.path {
            return self.inner.put_opts(path, payload, options).await;
        }
        if self.refuse_writes.load(Ordering::SeqCst) {
            self.refused.fetch_add(1, Ordering::SeqCst);
            return Err(unavailable("lease write transport unavailable"));
        }
        let delay = self.delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        let result = self.inner.put_opts(path, payload, options).await;
        match &result {
            Ok(_) => {
                self.applied.fetch_add(1, Ordering::SeqCst);
            }
            Err(
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. },
            ) => {
                self.conflicts.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
        if result.is_ok() && self.lose_reply.swap(false, Ordering::SeqCst) {
            self.lost_replies.fetch_add(1, Ordering::SeqCst);
            return Err(unavailable("lease write applied; response lost"));
        }
        result
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.payload_barrier(path).await;
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let hold = if path == &self.path {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.held.lock().unwrap().take()
        } else {
            None
        };
        // Capture the actual old body and ETag before delaying delivery. The
        // underlying store remains available to the SDK's lease safety lane.
        let result = self.inner.get_opts(path, options).await?;
        if let Some(hold) = hold {
            hold.entered.cancel();
            hold.release.cancelled().await;
        }
        Ok(result)
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
