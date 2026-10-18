//! Observe actual SDK application activations without retaining their handles.
use crate::native_peer;
use cellar::{bucket::Bucket, sdk::*};
use cellar_runtime::{
    app::CompiledApplication, application::cell_name, client::CellClient, identity::CellTarget,
};
use std::{
    collections::{BTreeSet, HashMap},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone)]
pub struct Life {
    pub identity: CellIdentity,
    pub created: usize,
    pub destroyed: usize,
    pub active: usize,
    pub invoked: usize,
}
#[derive(Default)]
pub struct Tracker(Mutex<HashMap<String, Life>>);
impl Tracker {
    pub fn maybe(&self, target: &CellTarget) -> Option<Life> {
        self.0.lock().unwrap().get(&cell_name(target)).cloned()
    }
    pub fn active(&self) -> usize {
        self.0
            .lock()
            .unwrap()
            .values()
            .map(|life| life.active)
            .sum()
    }
    pub fn read(&self, target: &CellTarget) -> Life {
        self.maybe(target).unwrap()
    }
}

/// Retain actual SDK callbacks while keeping the invocation and storage context alive.
pub struct Pins {
    names: BTreeSet<String>,
    entered: Mutex<BTreeSet<String>>,
    changed: tokio::sync::Notify,
    release: tokio_util::sync::CancellationToken,
}
impl Pins {
    pub fn new(targets: &[CellTarget]) -> Self {
        Self {
            names: targets.iter().map(cell_name).collect(),
            entered: Mutex::new(BTreeSet::new()),
            changed: tokio::sync::Notify::new(),
            release: tokio_util::sync::CancellationToken::new(),
        }
    }
    pub fn guard(&self) -> tokio_util::sync::DropGuard {
        self.release.clone().drop_guard()
    }
    pub fn release(&self) {
        self.release.cancel();
    }
    pub async fn entered(&self) {
        loop {
            let changed = self.changed.notified();
            if *self.entered.lock().unwrap() == self.names {
                break;
            }
            changed.await;
        }
    }
    async fn before(&self, name: &str) {
        if self.names.contains(name) {
            self.entered.lock().unwrap().insert(name.into());
            self.changed.notify_one();
            self.release.cancelled().await;
        }
    }
}

struct TrackedApplication {
    inner: Arc<dyn Application>,
    track: Arc<Tracker>,
    pins: Option<Arc<Pins>>,
}
impl Application for TrackedApplication {
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
        let name = identity.name.clone().unwrap();
        let mut track = self.track.0.lock().unwrap();
        let life = track.entry(name.clone()).or_insert_with(|| Life {
            identity: identity.clone(),
            created: 0,
            destroyed: 0,
            active: 0,
            invoked: 0,
        });
        life.identity = identity.clone();
        life.created += 1;
        life.active += 1;
        Ok(Arc::new(TrackedCell {
            inner: cell,
            name,
            track: self.track.clone(),
            pins: self.pins.clone(),
        }))
    }
}
struct TrackedCell {
    inner: Arc<dyn Cell>,
    name: String,
    track: Arc<Tracker>,
    pins: Option<Arc<Pins>>,
}
impl Cell for TrackedCell {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        // Retain the wrapper for the complete inner callback, so Drop is not
        // confused with a detached handler still using the underlying cell.
        Box::pin(async move {
            if let Some(pins) = &self.pins {
                pins.before(&self.name).await;
            }
            self.track
                .0
                .lock()
                .unwrap()
                .get_mut(&self.name)
                .unwrap()
                .invoked += 1;
            let result = self.inner.clone().invoke(context, invocation).await;
            drop(self);
            result
        })
    }
}
impl Drop for TrackedCell {
    fn drop(&mut self) {
        let mut track = self.track.0.lock().unwrap();
        let life = track.get_mut(&self.name).unwrap();
        life.destroyed += 1;
        life.active -= 1;
    }
}
pub async fn start(
    app: Arc<CompiledApplication>,
    path: &Path,
    bucket: Bucket,
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
) -> (Node, CellClient, Arc<Tracker>) {
    start_with_pins(app, path, bucket, host, core, None).await
}
pub async fn start_with_pins(
    app: Arc<CompiledApplication>,
    path: &Path,
    bucket: Bucket,
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
    pins: Option<Arc<Pins>>,
) -> (Node, CellClient, Arc<Tracker>) {
    let public = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let internal = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let name = path.file_name().unwrap().to_str().unwrap().to_owned();
    let signer = Arc::new(PeerProbeSigner::generate());
    let auth = Arc::new(PeerAuth::new([7; 32], &name).unwrap());
    let mut config = NodeConfig::new(
        ActorIdentity {
            node: name,
            advertise: internal.local_addr().unwrap().to_string(),
            region: "local".into(),
        },
        ActorOptions {
            core: ActorPolicy {
                max_resident: 5,
                max_cached_routes: 128,
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
    config.runtime.max_stateless_requests = Some(128);
    config.runtime.max_cell_requests = 16;
    config.runtime.handler_budget = Duration::from_secs(30);
    config.cell_limits = CellLimits::unrestricted();
    config.cell_limits.sql = cellar_runtime::sql::host_limits();
    let http = reqwest::Client::new();
    let services = ExternalNodeServices {
        wall_clock: None,
        http_client: None,
        host_executor: host.clone(),
        core_executor: core.clone(),
        public_address: public.local_addr().unwrap(),
        ingress_capacity: 128,
        internal_listener: internal,
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
            log_transport: Arc::new(SignedPeerTransport::from_clients(http.clone(), http, auth)),
            log: LogOptions::default(),
            fleet_durability: false,
        }),
        containers: None,
        metrics: None,
    };
    let tracker = Arc::new(Tracker::default());
    let track = tracker.clone();
    let registry = app.registry();
    let factory = registry.clone();
    let applications = CompiledApplications::default()
        .register("beyonddb", "v1", move || {
            Ok(Arc::new(TrackedApplication {
                inner: factory.application(Arc::new(native_peer::now_ms))?,
                track: track.clone(),
                pins: pins.clone(),
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
    (node, CellClient::new(ingress, registry), tracker)
}
pub async fn stop(node: Node) {
    let NodeOutcome::Stopped(report) = tokio::time::timeout(
        Duration::from_secs(45),
        node.shutdown(ShutdownMode::Preserve),
    )
    .await
    .unwrap()
    .unwrap() else {
        panic!("SDK did not stop");
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
        "incomplete SDK shutdown: {report:?}"
    );
}
