//! Real embedded SDK fixture for domain unit tests.
use cellar::sdk::{
    ActorIdentity, ActorOptions, ActorPolicy, CellLimits, ExternalNodeServices, Node, NodeConfig,
    NodeOutcome, OwnershipOnEvict, PeerAuth, PeerProbeSigner, ShutdownMode,
};
use cellar_runtime::{
    app::CellApplication,
    client::{CellClient, MutationIdentity},
    descriptors::BuildDescriptor,
    identity::{Digest, RequestId},
};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

pub(crate) fn mutation(clock: &AtomicI64) -> MutationIdentity {
    let now = clock.load(Ordering::SeqCst);
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    }
}
pub(crate) async fn start(
    path: &Path,
    clock: Arc<AtomicI64>,
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
) -> anyhow::Result<(Node, CellClient)> {
    let public = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let peer = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let signer = Arc::new(PeerProbeSigner::generate());
    let actor = ActorOptions {
        core: ActorPolicy {
            max_resident: 32,
            max_cached_routes: 32,
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
            node: "bdb-domain-test".into(),
            advertise: peer.local_addr()?.to_string(),
            region: "local".into(),
        },
        actor,
        path.into(),
    );
    config.maintenance.rebalance = false;
    config.runtime.handler_budget = Duration::from_secs(30);
    config.cell_limits = CellLimits::unrestricted();
    let services = ExternalNodeServices {
        wall_clock: None,
        http_client: None,
        host_executor: host.clone(),
        core_executor: core.clone(),
        public_address: public.local_addr()?,
        ingress_capacity: 16,
        internal_listener: peer,
        peer_http: reqwest::Client::new(),
        peer_auth: Arc::new(PeerAuth::new([7; 32], "bdb-domain-test")?),
        probe_signer: signer,
        fleet: None,
        containers: None,
        metrics: None,
    };
    let application = crate::Beyonddb::compile(BuildDescriptor {
        source_revision: "domain-sdk-test".into(),
        cargo_lock_digest: Digest::from_bytes([1; 32]),
    })?;
    let registry = application.registry();
    let (node, ingress) = tokio::time::timeout(
        Duration::from_secs(30),
        Node::start_external(
            config,
            services,
            registry.deployment(),
            registry.compiled(Arc::new(move || clock.load(Ordering::SeqCst)))?,
        ),
    )
    .await??;
    Ok((node, CellClient::new(ingress, registry)))
}
pub(crate) async fn stop(node: Node) -> anyhow::Result<()> {
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        node.shutdown(ShutdownMode::Preserve),
    )
    .await??;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected node stop: {outcome:?}");
    };
    anyhow::ensure!(
        report.ingress_drained
            && report.application_work_drained
            && report.cells_drained
            && report.pending_dispatches == 0
            && !report.drain_timed_out,
        "incomplete drain: {report:?}"
    );
    Ok(())
}
