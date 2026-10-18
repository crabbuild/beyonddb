//! Real native peer sockets with an opaque, controllable network cut.
use bdb_cellar_runtime::{
    app::{CellApplication, CompiledApplication},
    application::cell_name,
    client::CellClient,
    descriptors::BuildDescriptor,
    identity::{CellTarget, Digest},
};
use beyonddb::*;
use cellar::{
    bucket::Bucket,
    ownership_store::BucketOwnership,
    sdk::{
        ActorIdentity, ActorOptions, ActorPolicy, Application, Cell, CellIdentity, CellLimits,
        CompiledApplications, Context, ExecutionDomain, ExternalNodeServices, HandlerFuture,
        Invocation, LogOptions, Node, NodeConfig, NodeFleet, NodeOutcome, Output, OwnershipOnEvict,
        PeerAuth, PeerProbeSigner, ReplicaStorage, ReplicaStorageOptions, ReplicationOptions,
        ShutdownMode, SignedPeerTransport,
    },
};
use extenddb_auth::StoredCredential;
use extenddb_core::{
    expression::ExpressionMaps,
    types::{AttributeValue, Item},
};
use extenddb_storage::{DataEngine, TableEngine};
use std::{
    collections::{BTreeSet, HashMap},
    net::SocketAddr,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

pub const SOURCE: &str = "peer-source";
pub const FRONT: &str = "peer-front";
pub const SECRET: &str = "native-peer-test-secret";
pub const ENCRYPTION: [u8; 32] = [38; 32];

pub type Observations = Arc<Mutex<HashMap<(String, String), CellIdentity>>>;
struct ObservedApplication {
    inner: Arc<dyn Application>,
    node: String,
    observed: Observations,
}
impl Application for ObservedApplication {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        self.inner.clone().invoke(context, invocation)
    }
    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> anyhow::Result<()> {
        self.inner.validate_configuration(options)
    }
    fn supports_cell(&self, class: &str) -> bool {
        self.inner.supports_cell(class)
    }
    fn create_cell(
        &self,
        context: Context,
        identity: &CellIdentity,
    ) -> anyhow::Result<Arc<dyn Cell>> {
        let cell = self.inner.create_cell(context, identity)?;
        self.observed.lock().unwrap().insert(
            (self.node.clone(), identity.name.clone().unwrap()),
            identity.clone(),
        );
        Ok(cell)
    }
}

pub struct Network {
    changes: mpsc::Sender<(bool, oneshot::Sender<()>)>,
    stop: CancellationToken,
    task: JoinHandle<()>,
    pub accepted: Arc<AtomicUsize>,
}
impl Network {
    async fn start(upstream: SocketAddr) -> (Self, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (changes, mut changed) = mpsc::channel::<(bool, oneshot::Sender<()>)>(4);
        let stop = CancellationToken::new();
        let cancelled = stop.clone();
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = accepted.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            let mut blocked = false;
            loop {
                tokio::select! {
                    () = cancelled.cancelled() => break,
                    result = changed.recv() => {
                        let Some((value, acknowledged)) = result else { break; };
                        blocked = value;
                        // Cut established HTTP/WebSocket streams as well as
                        // new connections. Bytes and signed payloads are opaque.
                        connections.abort_all();
                        while connections.join_next().await.is_some() {}
                        let _ = acknowledged.send(());
                    }
                    Some(_) = connections.join_next(), if !connections.is_empty() => {}
                    accepted = listener.accept() => {
                        let (mut inbound, _) = accepted.unwrap();
                        if blocked { continue; }
                        count.fetch_add(1, Ordering::SeqCst);
                        connections.spawn(async move {
                            if let Ok(mut outbound) = TcpStream::connect(upstream).await {
                                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                            }
                        });
                    }
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        (
            Self {
                changes,
                stop,
                task,
                accepted,
            },
            address,
        )
    }
    pub async fn cut(&self, blocked: bool) {
        let (acknowledged, done) = oneshot::channel();
        self.changes.send((blocked, acknowledged)).await.unwrap();
        done.await.unwrap();
    }
}
impl Drop for Network {
    fn drop(&mut self) {
        self.stop.cancel();
        self.task.abort();
    }
}

pub struct Peer {
    pub node: Option<Node>,
    pub client: CellClient,
    pub network: Network,
}
impl Peer {
    pub async fn start(
        app: Arc<CompiledApplication>,
        path: &Path,
        bucket: Bucket,
        observed: Observations,
        route_cache: bool,
        host: &tokio::runtime::Handle,
        core: &tokio::runtime::Handle,
    ) -> Self {
        let public = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (network, address) = Network::start(listener.local_addr().unwrap()).await;
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let signer = Arc::new(PeerProbeSigner::generate());
        let auth = Arc::new(PeerAuth::new([7; 32], &name).unwrap());
        let http = reqwest::Client::new();
        let mut config = NodeConfig::new(
            ActorIdentity {
                node: name.clone(),
                advertise: address.to_string(),
                region: "local".into(),
            },
            ActorOptions {
                core: ActorPolicy {
                    max_resident: 32,
                    max_cached_routes: if route_cache { 128 } else { 0 },
                    max_activations: 8,
                    max_evictions: 4,
                    max_releases: 8,
                    max_outbound_websockets: 16,
                    ownership_on_evict: OwnershipOnEvict::Sticky,
                    require_node_lease: true,
                    peer_protocol: cellar::peer_auth::PROTOCOL_VERSION,
                    operation_deadline_ms: Some(10_000),
                    owner_log_recovery_backoff_ms: 1_000,
                    owner_log_recovery_attempts: 10,
                    alarm_resident_ms: 1_000,
                    idle_evict_ms: None,
                    pressure: Default::default(),
                },
                lease_ttl_ms: 10_000,
                placement_weight: 1,
                generation: signer.public_key_hex(),
                resume_generation: None,
            },
            path.into(),
        );
        config.maintenance.rebalance = false;
        // Match the executable so these cases exercise its real backpressure.
        config.runtime.max_stateless_requests = Some(128);
        config.runtime.max_cell_requests = 16;
        config.runtime.handler_budget = Duration::from_secs(30);
        config.cell_limits = CellLimits::unrestricted();
        config.cell_limits.sql = bdb_cellar_runtime::sql::host_limits();
        let services = ExternalNodeServices {
            wall_clock: None,
            http_client: None,
            host_executor: host.clone(),
            core_executor: core.clone(),
            public_address: public.local_addr().unwrap(),
            ingress_capacity: 128,
            internal_listener: listener,
            peer_http: http.clone(),
            peer_auth: auth.clone(),
            probe_signer: signer,
            fleet: Some(NodeFleet {
                lease_bucket: bucket.clone(),
                replica_storage: ReplicaStorage::new(
                    bucket.store.clone(),
                    ReplicaStorageOptions {
                        prefix: bucket.prefix.clone(),
                        ..Default::default()
                    },
                )
                .unwrap(),
                bucket,
                replication: ReplicationOptions {
                    flush_interval: Duration::ZERO,
                    compaction: None,
                    ..Default::default()
                },
                log_transport: Arc::new(SignedPeerTransport::from_clients(
                    http.clone(),
                    http,
                    auth,
                )),
                log: LogOptions::default(),
                fleet_durability: false,
            }),
            containers: None,
            metrics: None,
        };
        let registry = app.registry();
        let factory = registry.clone();
        let applications = CompiledApplications::default()
            .register("beyonddb", "v1", move || {
                Ok(Arc::new(ObservedApplication {
                    inner: factory.application(Arc::new(now_ms))?,
                    node: name.clone(),
                    observed: observed.clone(),
                }) as Arc<dyn Application>)
            })
            .unwrap();
        let (node, ingress) = tokio::time::timeout(
            Duration::from_secs(30),
            Node::start_external(config, services, registry.deployment(), applications),
        )
        .await
        .unwrap()
        .unwrap();
        tokio::time::timeout(Duration::from_secs(30), async {
            while !node.is_ready().await.unwrap() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();
        Self {
            node: Some(node),
            client: CellClient::new(ingress, registry),
            network,
        }
    }
    pub async fn stop(&mut self) {
        let result = tokio::time::timeout(
            Duration::from_secs(45),
            self.node.take().unwrap().shutdown(ShutdownMode::Preserve),
        )
        .await
        .unwrap()
        .unwrap();
        let NodeOutcome::Stopped(report) = result else {
            panic!("unexpected stop: {result:?}")
        };
        assert!(
            report.ingress_drained
                && report.application_work_drained
                && report.cells_drained
                && report.pending_dispatches == 0
                && report.pending_application_events == 0
                && report.log_quiesced
                && report.local_durability_closed
                && report.clean_reload_prepared
                && !report.deadline_elapsed
                && !report.drain_timed_out,
            "incomplete drain: {report:?}"
        );
    }
}

pub fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}
pub fn application() -> Arc<CompiledApplication> {
    Arc::new(
        Beyonddb::compile(BuildDescriptor {
            source_revision: "native-peer-test".into(),
            cargo_lock_digest: Digest::from_bytes([1; 32]),
        })
        .unwrap(),
    )
}
pub fn identity(observed: &Observations, node: &str, target: &CellTarget) -> Option<CellIdentity> {
    observed
        .lock()
        .unwrap()
        .get(&(node.into(), cell_name(target)))
        .cloned()
}
pub fn provisioner(client: &CellClient) -> Arc<CellInitialPartitionProvisioner> {
    Arc::new(
        CellInitialPartitionProvisioner::new(client.clone(), application())
            .with_initial_partition_count(2)
            .unwrap(),
    )
}
pub async fn put_credential(client: &CellClient, key: &str, account: &str) {
    CellCredentialStore::new(client.clone(), ENCRYPTION)
        .put_credential(
            key,
            StoredCredential {
                secret_key: SECRET.into(),
                account_id: account.into(),
                principal_name: "network-user".into(),
                session_name: None,
                is_session: false,
                session_token: None,
                is_active: true,
                expires_at: None,
            },
        )
        .await
        .unwrap();
}
pub async fn policy(client: &CellClient, account: &str) {
    CellAuthorizationStore::new(client.clone()).put_user_policy(account, "network-user", "tables", &serde_json::json!({
        "Version":"2012-10-17", "Statement":[{"Effect":"Allow", "Action":"dynamodb:*", "Resource":"*"}]
    }).to_string()).await.unwrap();
}
pub async fn table(
    client: &CellClient,
    account: &str,
    name: &str,
    sort: bool,
) -> (TableRecord, Vec<(CellTarget, Item)>) {
    let storage =
        CellStorage::new(client.clone(), "us-east-1").with_initial_partitions(provisioner(client));
    let mut input = serde_json::json!({
        "TableName":name,"BillingMode":"PAY_PER_REQUEST", "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
        "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]
    });
    if sort {
        input["KeySchema"] = serde_json::json!([{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}]);
        input["AttributeDefinitions"] = serde_json::json!([{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"N"}]);
    }
    storage
        .create_table(account, serde_json::from_value(input).unwrap())
        .await
        .unwrap();
    let info = storage.table_key_info(account, name).await.unwrap();
    let record = client
        .query::<DescribeTable>(&account_target(account).unwrap(), None, Json(name.into()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    let mut data = Vec::new();
    if !sort {
        for id in ["first", "Ω-other"] {
            let item = Item::from([
                ("id".into(), AttributeValue::S(id.into())),
                ("value".into(), AttributeValue::N("1".into())),
            ]);
            storage
                .put_item(
                    &info,
                    item.clone(),
                    false,
                    None,
                    &ExpressionMaps::default(),
                    None,
                )
                .await
                .unwrap();
            let key = ordered_data_key(&record.id, &item, &record.key_schema).unwrap();
            let RoutePageOutcome::Page { partitions, .. } = read_route_page(
                client,
                &account_target(account).unwrap(),
                RoutePageInput {
                    table_id: record.id.clone(),
                    start_key: Some(key.clone()),
                    after_lower: None,
                    expected_epoch: None,
                },
            )
            .await
            .unwrap() else {
                panic!("missing route")
            };
            let range = partitions
                .iter()
                .find(|range| {
                    range.lower <= key && range.upper.as_ref().is_none_or(|upper| &key < upper)
                })
                .unwrap();
            data.push((
                data_target(account, &record.id, &range.partition_id).unwrap(),
                item,
            ));
        }
        assert_ne!(data[0].0, data[1].0);
    }
    (record, data)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Owner {
    pub node: Option<String>,
    pub lineage: Option<String>,
    pub epoch: u64,
    pub etag: String,
}
pub struct Inspection {
    pub domain: ExecutionDomain,
    observer: BucketOwnership,
    bucket: Bucket,
}
impl Inspection {
    pub fn new(bucket: Bucket, host: &tokio::runtime::Handle) -> Self {
        Self {
            domain: ExecutionDomain::new(host.clone()),
            observer: BucketOwnership::new(
                bucket.clone(),
                bucket.clone(),
                "peer-inspection".into(),
                String::new(),
            ),
            bucket,
        }
    }
    pub async fn owner(&self, scope: &str) -> Owner {
        self.domain
            .run(async {
                let owner = self.observer.read_owner(scope).await.unwrap().unwrap();
                Owner {
                    node: owner.node,
                    lineage: owner.lineage,
                    epoch: owner.epoch,
                    etag: owner.etag,
                }
            })
            .await
            .unwrap()
    }
    pub async fn live(&self, node: &str) -> bool {
        self.domain
            .run(async {
                self.observer
                    .read_node_lease(node)
                    .await
                    .unwrap()
                    .is_some_and(|lease| lease.expires_ms > cellar::ownership_store::now_ms())
            })
            .await
            .unwrap()
    }
    pub async fn ready_peers(&self) {
        self.domain
            .run(async {
                tokio::time::timeout(Duration::from_secs(20), async {
                    loop {
                        if let Ok((_, peers)) = self.observer.read_shared_capacity_view().await
                            && [SOURCE, FRONT].iter().all(|name| {
                                peers.iter().any(|peer| {
                                    peer.node == *name
                                        && peer.expires_ms > cellar::ownership_store::now_ms()
                                })
                            })
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                })
                .await
                .unwrap();
            })
            .await
            .unwrap();
    }
    pub async fn departed(&self) {
        self.domain
            .run(async {
                tokio::time::timeout(Duration::from_secs(25), async {
                    let expired = loop {
                        let lease = self.observer.read_node_lease(SOURCE).await.unwrap();
                        let now = cellar::ownership_store::now_ms();
                        if lease.as_ref().is_none_or(|lease| lease.expires_ms <= now) {
                            break lease.map_or(now, |lease| lease.expires_ms);
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    };
                    loop {
                        if let Ok((at, peers)) = self.observer.read_shared_capacity_view().await
                            && at >= expired
                            && peers
                                .iter()
                                .all(|peer| peer.node != SOURCE || peer.expires_ms <= at)
                            && peers
                                .iter()
                                .any(|peer| peer.node == FRONT && peer.expires_ms > at)
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                })
                .await
                .unwrap();
            })
            .await
            .unwrap();
    }
    pub async fn credential_scopes(&self, class: &str) -> BTreeSet<String> {
        self.domain
            .run(async {
                let page = self
                    .bucket
                    .common_prefixes_page(&format!("cells/{class}:"), None, None, 64)
                    .await
                    .unwrap();
                assert!(
                    page.page_token.is_none(),
                    "private fixture credential inventory exceeded bound"
                );
                page.prefixes.into_iter().collect()
            })
            .await
            .unwrap()
    }
    pub async fn close(self) {
        tokio::time::timeout(Duration::from_secs(10), self.domain.shutdown())
            .await
            .unwrap();
    }
}

pub struct Http {
    pub sdk: aws_sdk_dynamodb::Client,
    task: JoinHandle<anyhow::Result<()>>,
}
impl Http {
    pub async fn start(client: &CellClient, access: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let state = build_http_state(
            client.clone(),
            provisioner(client),
            ENCRYPTION,
            "us-east-1",
            endpoint.clone(),
        )
        .unwrap();
        let task = tokio::spawn(extenddb_server::start_server(listener, state, None, None));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert!(!task.is_finished());
                if reqwest::get(format!("{endpoint}/health"))
                    .await
                    .is_ok_and(|response| response.status().is_success())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-1"))
            .credentials_provider(aws_credential_types::Credentials::new(
                access,
                SECRET,
                None,
                None,
                "native-peer",
            ))
            .endpoint_url(endpoint)
            .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled())
            .load()
            .await;
        Self {
            sdk: aws_sdk_dynamodb::Client::new(&config),
            task,
        }
    }
    pub async fn stop(self) {
        drop(self.sdk);
        self.task.abort();
        assert!(self.task.await.unwrap_err().is_cancelled());
    }
}
