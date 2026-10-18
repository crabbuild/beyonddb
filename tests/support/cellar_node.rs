//! Bucket-backed embedded Cellar host shared by integration fixtures.
use bdb_cellar_runtime::{app::CompiledApplication, client::CellClient};
use cellar::sdk::{
    ActorIdentity, ActorOptions, ActorPolicy, CellLimits, ExternalNodeServices, LogOptions, Node,
    NodeConfig, NodeFleet, NodeOutcome, OwnershipOnEvict, PeerAuth, PeerProbeSigner,
    ReplicaStorage, ReplicaStorageOptions, ReplicationOptions, ShutdownMode, SignedPeerTransport,
};
use std::{path::Path, sync::Arc, time::Duration};

pub async fn start(
    application: Arc<CompiledApplication>,
    path: &Path,
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
    bucket: cellar::bucket::Bucket,
) -> anyhow::Result<(Node, CellClient)> {
    start_limited(application, path, host, core, bucket, 64).await
}

pub async fn start_limited(
    application: Arc<CompiledApplication>,
    path: &Path,
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
    bucket: cellar::bucket::Bucket,
    max_resident: usize,
) -> anyhow::Result<(Node, CellClient)> {
    let public = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let peer = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let signer = Arc::new(PeerProbeSigner::generate());
    let name = path.file_name().unwrap().to_string_lossy().into_owned();
    let auth = Arc::new(PeerAuth::new([7; 32], &name)?);
    let http = reqwest::Client::new();
    let actor = ActorOptions {
        core: ActorPolicy {
            max_resident,
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
    };
    let mut config = NodeConfig::new(
        ActorIdentity {
            node: name,
            advertise: peer.local_addr()?.to_string(),
            region: "local".into(),
        },
        actor,
        path.into(),
    );
    config.maintenance.rebalance = false;
    config.runtime.handler_budget = Duration::from_secs(30);
    config.cell_limits = CellLimits::unrestricted();
    config.cell_limits.sql = bdb_cellar_runtime::sql::host_limits();
    let services = ExternalNodeServices {
        wall_clock: None,
        http_client: None,
        host_executor: host.clone(),
        core_executor: core.clone(),
        public_address: public.local_addr()?,
        ingress_capacity: 32,
        internal_listener: peer,
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
            )?,
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
    let registry = application.registry();
    let (mut node, ingress) = tokio::time::timeout(
        Duration::from_secs(30),
        Node::start_external(
            config,
            services,
            registry.deployment(),
            registry.compiled(Arc::new(|| {
                i64::try_from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis(),
                )
                .unwrap()
            }))?,
        ),
    )
    .await??;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !node.is_ready().await? {
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "fixture readiness deadline"
        );
        tokio::select! {
            outcome = node.finished() => anyhow::bail!("fixture stopped before readiness: {outcome:?}"),
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }
    Ok((node, CellClient::new(ingress, registry)))
}
pub async fn stop(node: Node, mode: ShutdownMode) -> anyhow::Result<()> {
    let outcome = tokio::time::timeout(Duration::from_secs(45), node.shutdown(mode)).await??;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected stop: {outcome:?}");
    };
    anyhow::ensure!(
        report.ingress_drained
            && report.application_work_drained
            && report.cells_drained
            && report.pending_dispatches == 0
            && report.pending_application_events == 0
            && report.log_quiesced
            && report.local_durability_closed
            && (mode != ShutdownMode::Preserve || report.clean_reload_prepared)
            && !report.drain_timed_out
            && !report.deadline_elapsed,
        "incomplete stop: {report:?}"
    );
    Ok(())
}
