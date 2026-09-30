use super::*;

use std::{fmt, sync::Mutex, time::Duration};

use async_trait::async_trait;
use beyonddb::{PeerNodeDurabilityProvider, PeerNodeLogTransport};
use cellule_host::{FOLLOWER_STORE_COMPONENT, NodeDurabilitySupervisorConfig};
use cellule_runtime::node::{NODE_LOG_PROTOCOL_VERSION, NodeCapacity};
use cellule_runtime::{NodeLeaseGuard, follower::FollowerStore, ltx::Limits};
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};

// Only immutable objects for the selected Cell are withheld. Catalogs, leases,
// log activation, and other Cells remain available throughout the experiment.
#[derive(Debug)]
struct WithheldPublication {
    inner: InMemory,
    cell: Mutex<Option<String>>,
    blocked: CancellationToken,
    release: CancellationToken,
}

impl WithheldPublication {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            cell: Mutex::new(None),
            blocked: CancellationToken::new(),
            release: CancellationToken::new(),
        }
    }

    fn withhold(&self, cell: &[u8; 32]) {
        let hex = cell
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        *self.cell.lock().unwrap() = Some(format!("/cells/{hex}/inc/"));
    }

    async fn wait(&self, path: &Path) {
        let blocked = self.cell.lock().unwrap().as_ref().is_some_and(|cell| {
            path.as_ref().contains(cell) && path.as_ref().contains("/objects/")
        });
        if blocked {
            self.blocked.cancel();
            self.release.cancelled().await;
        }
    }
}

impl fmt::Display for WithheldPublication {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WithheldPublication")
    }
}

#[async_trait]
impl ObjectStore for WithheldPublication {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.wait(path).await;
        self.inner.put_opts(path, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.wait(path).await;
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, opts).await
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
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.wait(to).await;
        self.inner.copy_opts(from, to, opts).await
    }
}

struct LogNode {
    node: CellNode,
    _tasks: Arc<CellNodeTaskGroup>,
    provisioner: Arc<CellInitialPartitionProvisioner>,
    guard: NodeLeaseGuard,
    session: SessionId,
    id: NodeId,
    crash: CancellationToken,
    server: tokio::task::JoinHandle<()>,
}

impl LogNode {
    async fn new(fixture: &Fixture, byte: u8, follower: bool, durability: bool) -> Self {
        let root = fixture._files.path();
        let (cert, key) = peer_tls_files(
            root,
            &format!("log-{byte}"),
            &root.join("ca.crt"),
            &root.join("ca.key"),
        );
        let tls = LoadedPeerTls::load(&cert, &key, &root.join("ca.crt"), "localhost").unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("https://{}", listener.local_addr().unwrap());
        let session = SessionId::from_bytes([byte; 16]);
        let id = NodeId::from_bytes([byte; 16]);
        let mut builder = CellNodeBuilder::new(fixture.application.clone())
            .with_runtime(SqlWorkerPool::new(2, 16).unwrap(), 64 << 20)
            .with_replica_host(Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)))
            .with_session(session);
        if follower {
            builder = builder.with_follower_store(
                root.join(format!("follower-{byte}")),
                Limits::default(),
                DiskBudget::new(1 << 30),
            );
        }
        let node = builder.build().unwrap();
        let crash = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let tasks = node
            .install_task_group(crash.child_token(), shutdown.clone())
            .unwrap();
        let signing = tls.signing_key().clone();
        let certificate = tls.certificate();
        let fleet = fixture.directory.fleet();
        let release = fixture.application.registry().release_digest();
        let modules = fixture.application.registry().module_digests();
        let published = NodeLeasePublisher::new(fixture.directory.clone(), {
            let endpoint = endpoint.clone();
            move |now, expires| {
                NodeAdvertisement::sign(
                    id,
                    session,
                    endpoint.clone(),
                    fleet,
                    certificate,
                    Digest::from_bytes([90; 32]),
                    release,
                    &signing,
                    1,
                    now,
                    expires,
                    modules.clone(),
                    vec![1],
                    NodeFailureDomain::default(),
                    NodeCapacity {
                        log_protocol: NODE_LOG_PROTOCOL_VERSION,
                        follower_free_bytes: if follower { 1 << 30 } else { 0 },
                        free_memory_bytes: 64 << 20,
                        free_disk_bytes: 1 << 30,
                        job_credits: 16,
                        ..NodeCapacity::default()
                    },
                )
            }
        })
        .publish()
        .await
        .unwrap();
        let guard = published.guard();
        let authority = published.log_authority();
        node.install_node_lease_for_startup(guard.clone()).unwrap();
        tasks
            .spawn_lease_maintenance({
                let crash = crash.clone();
                async move {
                    tokio::select! {
                        () = crash.cancelled() => Ok(()),
                        result = published.run(&shutdown) => result,
                    }
                }
            })
            .unwrap();
        let transport = Arc::new(PeerNodeLogTransport::new(
            fixture.directory.clone(),
            tls.client_identity(),
            session,
            id,
            guard.clone(),
        ));
        let mut peers = BeyonddbPeers::new(
            &node,
            fixture.layout.clone(),
            fixture.directory.clone(),
            session,
            &tls,
        )
        .unwrap();
        if let Some(store) = node.owned_component::<FollowerStore>(FOLLOWER_STORE_COMPONENT) {
            peers = peers.with_follower_store(id, store, guard.clone());
        }
        let peers = Arc::new(peers);
        let provisioner = Arc::new(
            CellInitialPartitionProvisioner::new(
                node.runtime(),
                fixture.application.clone(),
                fixture.layout.clone(),
                session,
                endpoint,
                root.join(format!("log-data-{byte}")),
            )
            .unwrap()
            .with_peers(peers.clone())
            .with_node_log_recovery(transport.clone())
            .unwrap(),
        );
        let router = peers.router(provisioner.clone());
        let server = tokio::spawn(async move {
            axum::serve(
                tls.listener(listener),
                router.into_make_service_with_connect_info::<PeerTlsIdentity>(),
            )
            .await
            .unwrap();
        });
        if durability {
            let provider = PeerNodeDurabilityProvider::new(
                authority,
                transport.as_ref().clone(),
                session,
                id,
                guard.clone(),
                node.runtime().telemetry_handle(),
            )
            .unwrap();
            node.install_node_durability_provider(
                Arc::new(provider),
                NodeDurabilitySupervisorConfig::new(
                    beyonddb::APPLICATION_ID,
                    Limits::default(),
                    64 << 20,
                    1024,
                    Duration::from_millis(100),
                    Duration::from_secs(300),
                    100_000,
                )
                .unwrap(),
            )
            .unwrap();
        }
        node.start().unwrap();
        Self {
            node,
            _tasks: tasks,
            provisioner,
            guard,
            session,
            id,
            crash,
            server,
        }
    }

    async fn shutdown(self) {
        self.node.shutdown().await.unwrap();
        self.server.abort();
        let _ = self.server.await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_fsync_acknowledges_signed_write_without_object_publication() {
    assert_follower_durability(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_only_signed_write_waits_for_object_publication() {
    assert_follower_durability(false).await;
}

async fn assert_follower_durability(durability: bool) {
    let withheld = Arc::new(WithheldPublication::new());
    let fixture = Fixture::with_store(1, withheld.clone()).await;
    let second_follower = LogNode::new(&fixture, 200, true, false).await;
    let follower = LogNode::new(&fixture, 201, true, false).await;
    let leader = LogNode::new(&fixture, 202, false, durability).await;
    if durability {
        tokio::time::timeout(Duration::from_secs(5), async {
            while leader.node.runtime().node_durability().is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    let table = super::provisioning::table_id(&fixture, "Residency").await;
    let target = beyonddb::data_target("123456789012", &table, &[0; 16]).unwrap();
    fixture.data[0].0.drain().await.unwrap();
    leader
        .provisioner
        .admit_existing_partition("123456789012", &table, &[0; 16])
        .await
        .unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let before = authority.load(target.cell_id()).await.unwrap().unwrap();
    let predecessor = before.value().root.as_ref().unwrap().commit_sequence;
    withheld.withhold(target.cell_id().as_bytes());
    let sdk = super::provisioning::sdk_without_retries(&fixture);
    let item = HashMap::from([
        ("id".into(), AwsAttributeValue::S("follower-only".into())),
        ("value".into(), AwsAttributeValue::S("acknowledged".into())),
    ]);
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        sdk.put_item()
            .table_name("Residency")
            .set_item(Some(item.clone()))
            .send(),
    )
    .await;
    if !durability {
        assert!(
            result.is_err(),
            "object-only write acknowledged before publication"
        );
        assert!(withheld.blocked.is_cancelled());
        let current = authority.load(target.cell_id()).await.unwrap().unwrap();
        assert_eq!(
            current.value().root.as_ref().unwrap().commit_sequence,
            predecessor
        );
        withheld.release.cancel();
        leader.shutdown().await;
        follower.shutdown().await;
        second_follower.shutdown().await;
        fixture.shutdown().await;
        return;
    }
    if result.is_err() {
        withheld.release.cancel();
    }
    assert!(
        result.is_ok(),
        "signed write still waits for object publication despite an eligible follower"
    );
    result.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(5), withheld.blocked.cancelled())
        .await
        .unwrap();
    let after = authority.load(target.cell_id()).await.unwrap().unwrap();
    assert_eq!(
        after.value().root.as_ref().unwrap().commit_sequence,
        predecessor,
        "the acknowledged item must still be untiered"
    );
    let enrolled = fixture
        .directory
        .load(leader.session, now_ms())
        .await
        .unwrap()
        .unwrap();
    assert!(enrolled.advertisement().log().unwrap().active());
    assert_eq!(
        enrolled.advertisement().log().unwrap().members(),
        &[second_follower.id, follower.id]
    );

    // Fence the lost owner while its immutable publication is withheld. This is
    // a component loss test; a separate process-kill SDK test is still required.
    leader.guard.fence();
    leader.crash.cancel();
    leader.server.abort();
    tokio::time::timeout(Duration::from_secs(20), async {
        while fixture
            .directory
            .is_live(leader.session, now_ms())
            .await
            .unwrap()
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let lost = authority.load(target.cell_id()).await.unwrap().unwrap();
    assert_eq!(
        lost.value().root.as_ref().unwrap().commit_sequence,
        predecessor,
        "owner loss must precede object-root publication"
    );
    withheld.release.cancel();
    let successor = LogNode::new(&fixture, 203, false, false).await;
    successor
        .provisioner
        .takeover_expired_partition("123456789012", &table, &[0; 16], &fixture.directory)
        .await
        .unwrap();
    let recovered = sdk
        .get_item()
        .table_name("Residency")
        .key("id", item["id"].clone())
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(recovered.item(), Some(&item));
    assert!(
        fixture
            .directory
            .takeover_proof(leader.session, successor.session, now_ms())
            .await
            .unwrap()
            .is_some()
    );
    successor.shutdown().await;
    follower.shutdown().await;
    second_follower.shutdown().await;
    fixture.shutdown().await;
}
