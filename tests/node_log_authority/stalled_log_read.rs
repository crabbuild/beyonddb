use super::*;
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::{fmt, sync::Mutex};

#[derive(Debug, Default)]
pub(super) struct HeldReadStore {
    inner: InMemory,
    pub(super) held_path: Mutex<Option<Path>>,
    pub(super) entered: CancellationToken,
    pub(super) release: CancellationToken,
}

impl fmt::Display for HeldReadStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HeldReadStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for HeldReadStore {
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
        let hold = {
            let mut held = self.held_path.lock().unwrap();
            if held.as_ref() == Some(path) {
                held.take();
                true
            } else {
                false
            }
        };
        // Capture this exact record and ETag before holding its response. A
        // heartbeat can then publish a newer version while the log caller waits.
        let result = self.inner.get_opts(path, options).await?;
        if hold {
            self.entered.cancel();
            self.release.cancelled().await;
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_log_read_does_not_block_heartbeat_and_rebases_coverage() {
    let store = Arc::new(HeldReadStore::default());
    let layout = CellStorageLayout::new(
        Store::new(store.clone()),
        Path::from("heartbeat-during-log-read"),
        [49; 16],
    );
    let directory = NodeDirectory::new(layout.clone(), FLEET, IMAGE, RELEASE);
    let leader_node = NodeId::from_bytes([110; 16]);
    let leader_session = SessionId::from_bytes([111; 16]);
    let follower_node = NodeId::from_bytes([112; 16]);
    let now = now_ms();
    directory
        .create(
            advertisement(
                follower_node,
                SessionId::from_bytes([113; 16]),
                114,
                NodeCapacity {
                    free_memory_bytes: 16 << 20,
                    free_disk_bytes: 1 << 30,
                    follower_free_bytes: 1 << 30,
                    job_credits: 8,
                    log_protocol: NODE_LOG_PROTOCOL_VERSION,
                    ..NodeCapacity::default()
                },
                now,
                now + 15_000,
            )
            .unwrap(),
            now,
        )
        .await
        .unwrap();
    let published = NodeLeasePublisher::new(directory.clone(), move |now, expires| {
        advertisement(
            leader_node,
            leader_session,
            115,
            NodeCapacity::default(),
            now,
            expires,
        )
    })
    .publish()
    .await
    .unwrap();
    let guard = published.guard();
    let authority = published.log_authority();
    authority.recruit(1, 4096, 16).await.unwrap();
    authority.activate(1).await.unwrap();
    let before = directory
        .load(leader_session, now_ms())
        .await
        .unwrap()
        .unwrap();
    *store.held_path.lock().unwrap() = Some(layout.node_path(leader_session.as_bytes()));
    let log_authority = authority.clone();
    let coverage = tokio::spawn(async move { log_authority.advance_coverage(1, 1).await });
    tokio::time::timeout(Duration::from_secs(2), store.entered.cancelled())
        .await
        .unwrap();
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let heartbeat = tokio::spawn(async move { published.run(&run_cancellation).await });
    let renewed = tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            let current = directory
                .load(leader_session, now_ms())
                .await
                .unwrap()
                .unwrap();
            if current.advertisement().expires_at_ms() > before.advertisement().expires_at_ms() {
                return current;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    // Always release and join both tasks before asserting, including on the
    // old implementation where the held log read prevents renewal entirely.
    store.release.cancel();
    let covered = tokio::time::timeout(Duration::from_secs(3), coverage).await;
    cancellation.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(2), heartbeat).await;
    let renewed = renewed.expect("log storage I/O blocked the serving heartbeat");
    covered.unwrap().unwrap().unwrap();
    stopped.unwrap().unwrap().unwrap();
    let current = directory
        .load(leader_session, now_ms())
        .await
        .unwrap()
        .unwrap();
    assert!(current.advertisement().expires_at_ms() >= renewed.advertisement().expires_at_ms());
    assert!(current.advertisement().generation() > renewed.advertisement().generation());
    let log = current.advertisement().log().unwrap();
    assert_eq!(log.epoch(), 1);
    assert_eq!(log.members(), &[follower_node]);
    assert!(log.active());
    assert_eq!(log.tiered_through(), 1);
    guard.check().unwrap();
    guard.fence();
    assert!(matches!(
        authority.advance_coverage(1, 2).await,
        Err(cellule_runtime::Error::Fenced)
    ));
}
