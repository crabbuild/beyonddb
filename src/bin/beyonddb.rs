//! Process-owned ExtendDB protocol frontend with the embedded Cellar engine.
#![recursion_limit = "512"]

use anyhow::{Context, Result, ensure};
use beyonddb::{CellAuthorizationStore, CellCredentialStore, CellarServer, CellarServerConfig};
use cellar::{
    bucket::{Bucket, StorageBackend},
    sdk::{
        ActorIdentity, ActorOptions, ActorPolicy, ExternalNodeServices, LogOptions, NodeConfig,
        NodeFleet, NodeOutcome, OwnershipOnEvict, PeerAuth, PeerProbeSigner, ReplicaStorage,
        ReplicaStorageOptions, ReplicationOptions, ShutdownMode, SignedPeerTransport,
    },
};
use extenddb_auth::StoredCredential;
use object_store::{
    ClientOptions, RetryConfig,
    aws::{AmazonS3Builder, S3ConditionalPut},
};
use serde::Deserialize;
use std::{
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use zeroize::Zeroizing;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    node_name: String,
    data_dir: PathBuf,
    region: String,
    public_bind: SocketAddr,
    public_endpoint: String,
    peer_bind: SocketAddr,
    /// Cellar's authenticated peer address, HOST:PORT.
    peer_advertise: String,
    peer_key_file: PathBuf,
    encryption_key_file: PathBuf,
    #[serde(default)]
    public_certificate: Option<PathBuf>,
    #[serde(default)]
    public_private_key: Option<PathBuf>,
    #[serde(default)]
    accounts: Vec<String>,
    #[serde(default = "initial_partitions")]
    initial_partitions: u16,
    #[serde(default = "max_cells")]
    max_active_cells: usize,
    #[serde(default = "split_bytes")]
    split_threshold_bytes: u64,
    #[serde(default)]
    auth_cache_enabled: bool,
    #[serde(default)]
    fleet: Option<Fleet>,
    #[serde(default)]
    shutdown_mode: StopMode,
    #[serde(default)]
    bootstrap: Option<Bootstrap>,
}
#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StopMode {
    #[default]
    Preserve,
    Handoff,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fleet {
    bucket: String,
    endpoint: Option<String>,
    /// Dedicated Cellar storage prefix. Never point at Cellule object roots.
    prefix: String,
    /// Permit native follower fsyncs to race ordinary bucket publication.
    #[serde(default)]
    follower_durability: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Bootstrap {
    account_id: String,
    access_key_id: String,
    principal_name: String,
    policy_name: String,
    policy_file: PathBuf,
}
fn initial_partitions() -> u16 {
    4
}
fn max_cells() -> usize {
    128
}
fn split_bytes() -> u64 {
    256 * 1024 * 1024
}

fn key_file(path: &Path) -> Result<[u8; 32]> {
    let bytes = Zeroizing::new(std::fs::read(path).context("could not read key file")?);
    bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("key file must contain exactly 32 raw bytes"))
}

fn bucket(builder: &AmazonS3Builder, name: &str, prefix: &str) -> Result<Bucket> {
    let ordinary = Arc::new(
        builder
            .clone()
            .with_retry(RetryConfig {
                max_retries: 2,
                retry_timeout: Duration::from_secs(30),
                ..Default::default()
            })
            .build()?,
    );
    // An ambiguous CAS is ownership evidence, so the transport must not retry it.
    let conditional = Arc::new(
        builder
            .clone()
            .with_retry(RetryConfig {
                max_retries: 0,
                retry_timeout: Duration::from_secs(30),
                ..Default::default()
            })
            .build()?,
    );
    Bucket::from_clients(
        ordinary.clone(),
        conditional,
        ordinary,
        StorageBackend::S3,
        name.into(),
        prefix.into(),
    )
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init()
        .map_err(|error| anyhow::anyhow!("logger initialization failed: {error}"))?;
    let mut arguments = std::env::args_os().skip(1);
    let config_path = arguments
        .next()
        .context("usage: beyonddb <config.json> [--bootstrap]")?;
    let bootstrap = match (arguments.next(), arguments.next()) {
        (None, None) => false,
        (Some(value), None) if value == "--bootstrap" => true,
        _ => anyhow::bail!("usage: beyonddb <config.json> [--bootstrap]"),
    };
    let config: Config = serde_json::from_slice(&std::fs::read(config_path)?)?;
    ensure!(
        config.max_active_cells > 0 && config.split_threshold_bytes > 0,
        "cell and split budgets must be positive"
    );
    ensure!(
        (2..=256).contains(&config.initial_partitions),
        "initial_partitions must be between 2 and 256"
    );
    let secret = if bootstrap {
        let settings = config
            .bootstrap
            .as_ref()
            .context("bootstrap settings are missing")?;
        ensure!(
            config.accounts.contains(&settings.account_id),
            "bootstrap account must be in accounts"
        );
        let mut value = Zeroizing::new(String::new());
        std::io::stdin().read_to_string(&mut value)?;
        let trimmed = value.trim_end_matches(['\r', '\n']);
        ensure!(!trimmed.is_empty(), "bootstrap secret on stdin is empty");
        Some(Zeroizing::new(trimmed.to_owned()))
    } else {
        None
    };
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("bdb-host")
        .enable_all()
        .build()?;
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("bdb-cellar")
        .enable_all()
        .build()?;
    host.block_on(serve(config, secret, host.handle(), core.handle()))
}

async fn serve(
    config: Config,
    secret: Option<Zeroizing<String>>,
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
) -> Result<()> {
    let tls = match (&config.public_certificate, &config.public_private_key) {
        (Some(cert), Some(key)) => Some(extenddb_server::ServerTlsConfig {
            cert_path: cert.clone(),
            key_path: key.clone(),
        }),
        (None, None) if config.public_bind.ip().is_loopback() => None,
        (None, None) => anyhow::bail!(
            "public HTTP requires a loopback bind; configure TLS for external serving"
        ),
        _ => anyhow::bail!("public TLS requires both certificate and private key"),
    };
    ensure!(
        config
            .public_endpoint
            .starts_with(if tls.is_some() { "https://" } else { "http://" }),
        "public endpoint scheme does not match listener TLS"
    );
    let encryption_key = key_file(&config.encryption_key_file)?;
    let peer_key = key_file(&config.peer_key_file)?;
    let public = tokio::net::TcpListener::bind(config.public_bind).await?;
    let peer = tokio::net::TcpListener::bind(config.peer_bind).await?;
    let signer = Arc::new(PeerProbeSigner::generate());
    let auth = Arc::new(PeerAuth::new(peer_key, &config.node_name)?);
    let peer_http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(30))
        .tcp_nodelay(true)
        .build()?;
    let stream_http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .tcp_nodelay(true)
        .build()?;
    let fleet = if let Some(fleet) = &config.fleet {
        ensure!(
            !fleet.prefix.is_empty(),
            "fleet prefix must be dedicated to this Cellar deployment"
        );
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(&fleet.bucket)
            .with_region(&config.region)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_client_options(
                ClientOptions::new()
                    .with_timeout(Duration::from_secs(15))
                    .with_connect_timeout(Duration::from_secs(3)),
            );
        if let Some(endpoint) = &fleet.endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_allow_http(endpoint.starts_with("http://"));
        }
        let data = bucket(&builder, &fleet.bucket, &fleet.prefix)?;
        let lease = bucket(&builder, &fleet.bucket, &fleet.prefix)?;
        let storage = ReplicaStorage::new(
            data.store.clone(),
            ReplicaStorageOptions {
                prefix: data.prefix.clone(),
                ..Default::default()
            },
        )?;
        Some(NodeFleet {
            bucket: data,
            lease_bucket: lease,
            replica_storage: storage,
            replication: ReplicationOptions::default(),
            log_transport: Arc::new(SignedPeerTransport::from_clients(
                peer_http.clone(),
                stream_http,
                auth.clone(),
            )),
            log: LogOptions::default(),
            fleet_durability: fleet.follower_durability,
        })
    } else {
        None
    };
    let mode = match config.shutdown_mode {
        StopMode::Preserve => ShutdownMode::Preserve,
        StopMode::Handoff => {
            ensure!(
                fleet.is_some(),
                "handoff shutdown requires a fleet and an eligible peer"
            );
            ShutdownMode::Handoff
        }
    };
    // The SDK's clean-reload marker is a replication optimization. Local-only
    // nodes close durability normally but cannot prepare that fleet marker.
    let require_clean_reload = fleet.is_some() && mode == ShutdownMode::Preserve;
    let actor = ActorOptions {
        core: ActorPolicy {
            max_resident: config.max_active_cells,
            max_cached_routes: 1024,
            max_activations: 32,
            max_evictions: 4,
            max_releases: 16,
            max_outbound_websockets: 32,
            ownership_on_evict: OwnershipOnEvict::Sticky,
            require_node_lease: true,
            peer_protocol: cellar::peer_auth::PROTOCOL_VERSION,
            operation_deadline_ms: Some(30_000),
            owner_log_recovery_backoff_ms: 1_000,
            owner_log_recovery_attempts: 60,
            alarm_resident_ms: 60_000,
            idle_evict_ms: None,
            pressure: Default::default(),
        },
        lease_ttl_ms: 10_000,
        placement_weight: 1,
        generation: signer.public_key_hex(),
        resume_generation: None,
    };
    let mut node = NodeConfig::new(
        ActorIdentity {
            node: config.node_name,
            advertise: config.peer_advertise,
            region: config.region.clone(),
        },
        actor,
        config.data_dir.join("cellar-ordered-v1"),
    );
    node.maintenance.rebalance = fleet.is_some();
    node.runtime.max_stateless_requests = Some(128);
    node.runtime.max_cell_requests = 16;
    node.runtime.handler_budget = Duration::from_secs(30);
    let services = ExternalNodeServices {
        wall_clock: None,
        http_client: None,
        host_executor: host.clone(),
        core_executor: core.clone(),
        public_address: public.local_addr()?,
        ingress_capacity: 128,
        internal_listener: peer,
        peer_http,
        peer_auth: auth,
        probe_signer: signer,
        fleet,
        containers: None,
        metrics: None,
    };
    let server = CellarServer::start(
        CellarServerConfig {
            node,
            region: config.region,
            encryption_key,
            initial_partitions: config.initial_partitions,
            cache_enabled: config.auth_cache_enabled,
        },
        services,
    )
    .await?;
    if let (Some(settings), Some(secret)) = (&config.bootstrap, secret) {
        server
            .provisioner()
            .admit_account(&settings.account_id)
            .await?;
        CellCredentialStore::new(server.client(), encryption_key)
            .put_credential(
                &settings.access_key_id,
                StoredCredential {
                    secret_key: secret.to_string(),
                    account_id: settings.account_id.clone(),
                    principal_name: settings.principal_name.clone(),
                    session_name: None,
                    is_session: false,
                    session_token: None,
                    is_active: true,
                    expires_at: None,
                },
            )
            .await?;
        CellAuthorizationStore::new(server.client())
            .put_user_policy(
                &settings.account_id,
                &settings.principal_name,
                &settings.policy_name,
                &std::fs::read_to_string(&settings.policy_file)?,
            )
            .await?;
    }
    server
        .start_account_maintenance(config.accounts, config.split_threshold_bytes)
        .await?;
    let outcome = server
        .serve(public, config.public_endpoint, tls, mode)
        .await?;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("Cellar node stopped abnormally: {outcome:?}");
    };
    ensure!(
        report.ingress_drained
            && report.application_work_drained
            && report.pending_application_events == 0
            && report.cells_drained
            && report.pending_dispatches == 0
            && report.log_quiesced
            && report.local_durability_closed
            && (!require_clean_reload || report.clean_reload_prepared)
            && !report.drain_timed_out
            && !report.deadline_elapsed,
        "Cellar shutdown did not finish its drain: {report:?}"
    );
    Ok(())
}
