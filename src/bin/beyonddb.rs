//! Run ExtendDB's signed DynamoDB endpoint over a leased BeyondDB Cell node.

use std::{error::Error, io, io::Read, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use beyonddb::{
    APPLICATION_ID, Beyonddb, BeyonddbPeers, CellAuthorizationStore, CellCredentialStore,
    CellInitialPartitionProvisioner, CellStorage, NodeLeasePublisher, PeerNodeLogTransport,
    build_http_state_with_cache, measured_node_capacity, shutdown_serving_node,
};
use cellule_app::CellApplication;
use cellule_host::{CellNode, CellNodeBuilder, CellNodeTaskGroup, FOLLOWER_STORE_COMPONENT};
use cellule_peer_http::{LoadedPeerTls, PeerTlsIdentity};
use cellule_runtime::{
    NodeLeaseGuard, SqlWorkerPool,
    follower::FollowerStore,
    identity::{Digest, NodeId, SessionId},
    ltx::{CellStorageLayout, DiskBudget, Host, Limits},
    node::{
        NODE_LOG_PROTOCOL_VERSION, NodeAdvertisement, NodeCapacity, NodeDirectory,
        NodeFailureDomain,
    },
    registry::BuildDescriptor,
};
use cellule_store::{Store, provider_store::build_url_object_store};
use extenddb_auth::StoredCredential;
use extenddb_server::ServerTlsConfig;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zeroize::Zeroizing;

type ServerResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    storage_url: String,
    node_id: Uuid,
    data_dir: PathBuf,
    disk_budget_bytes: u64,
    /// Optional persistent follower-lane budget. Does not enable fleet proofs.
    #[serde(default)]
    follower_store_bytes: Option<u64>,
    #[serde(default = "default_node_retained_bytes")]
    node_retained_bytes: usize,
    #[serde(default = "default_max_active_cells")]
    max_active_cells: usize,
    /// Optional SQL worker override. The runtime caps this at sixteen workers.
    #[serde(default)]
    sql_workers: Option<usize>,
    encryption_key_file: PathBuf,
    region: String,
    peer_bind: SocketAddr,
    peer_endpoint: String,
    peer_certificate: PathBuf,
    peer_private_key: PathBuf,
    peer_ca: PathBuf,
    peer_server_name: String,
    public_bind: SocketAddr,
    public_endpoint: String,
    public_certificate: Option<PathBuf>,
    public_private_key: Option<PathBuf>,
    #[serde(default)]
    owned_accounts: Vec<String>,
    #[serde(default)]
    owned_access_keys: Vec<String>,
    #[serde(default = "default_initial_partitions")]
    initial_partitions: u16,
    #[serde(default = "default_split_threshold")]
    split_threshold_bytes: u64,
    /// Enable stale-while-revalidate auth and table metadata caches plus the
    /// complete-page routed table cache. Changes made on another node become
    /// visible after the cache TTL or a stale-route rejection.
    #[serde(default)]
    auth_cache_enabled: bool,
    bootstrap: Option<BootstrapConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapConfig {
    account_id: String,
    access_key_id: String,
    principal_name: String,
    policy_name: String,
    policy_file: PathBuf,
}

const fn default_initial_partitions() -> u16 {
    1
}

const fn default_split_threshold() -> u64 {
    256 * 1024 * 1024
}

const fn default_node_retained_bytes() -> usize {
    1024 * 1024 * 1024
}

const fn default_max_active_cells() -> usize {
    128
}

const MAX_SQL_WORKERS: usize = 16;

#[tokio::main]
async fn main() -> ServerResult<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(io::stderr)
        .with_ansi(false)
        .try_init()?;
    let mut args = std::env::args_os().skip(1);
    let path = args
        .next()
        .ok_or_else(|| invalid("usage: beyonddb <config.json>"))?;
    let bootstrap = match (args.next(), args.next()) {
        (None, None) => false,
        (Some(mode), None) if mode == "--bootstrap" => true,
        _ => return Err(invalid("usage: beyonddb <config.json> [--bootstrap]").into()),
    };
    let config: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    let secret = if bootstrap {
        if config.bootstrap.is_none() {
            return Err(invalid("bootstrap settings are missing").into());
        }
        let mut secret = Zeroizing::new(String::new());
        io::stdin().read_to_string(&mut secret)?;
        let trimmed = secret.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            return Err(invalid("bootstrap secret on stdin is empty").into());
        }
        Some(Zeroizing::new(trimmed.to_owned()))
    } else {
        None
    };
    serve(config, secret).await
}

async fn serve(config: Config, bootstrap_secret: Option<Zeroizing<String>>) -> ServerResult<()> {
    if config.disk_budget_bytes == 0
        || config.node_retained_bytes == 0
        || config.split_threshold_bytes == 0
        || config.max_active_cells == 0
        || config.follower_store_bytes == Some(0)
        || config
            .sql_workers
            .is_some_and(|workers| !(1..=MAX_SQL_WORKERS).contains(&workers))
    {
        return Err(invalid(
            "disk, optional follower-store, retained-byte, split, and active-cell budgets must be positive; sql_workers must be between 1 and 16",
        )
        .into());
    }
    if !["s3://", "gs://", "az://"]
        .iter()
        .any(|scheme| config.storage_url.starts_with(scheme))
    {
        return Err(invalid("Cell storage must use S3, GCS, or Azure").into());
    }
    if !config.peer_endpoint.starts_with("https://") {
        return Err(invalid("peer endpoint must use HTTPS").into());
    }
    if let Some(bootstrap) = config.bootstrap.as_ref()
        && bootstrap_secret.is_some()
        && (!config.owned_accounts.contains(&bootstrap.account_id)
            || !config.owned_access_keys.contains(&bootstrap.access_key_id))
    {
        return Err(invalid("bootstrap account and access key must be owned by this node").into());
    }
    let public_tls = match (&config.public_certificate, &config.public_private_key) {
        (Some(cert_path), Some(key_path)) => Some(ServerTlsConfig {
            cert_path: cert_path.clone(),
            key_path: key_path.clone(),
        }),
        (None, None) if config.public_bind.ip().is_loopback() => None,
        (None, None) => return Err(invalid("public HTTP requires a loopback bind").into()),
        _ => return Err(invalid("public TLS requires both certificate and private key").into()),
    };
    let public_scheme = if public_tls.is_some() {
        "https://"
    } else {
        "http://"
    };
    if !config.public_endpoint.starts_with(public_scheme) {
        return Err(invalid("public endpoint scheme does not match TLS configuration").into());
    }
    let encryption_key = read_encryption_key(&config.encryption_key_file)?;
    let tls = LoadedPeerTls::load(
        &config.peer_certificate,
        &config.peer_private_key,
        &config.peer_ca,
        &config.peer_server_name,
    )?;
    let source_revision = option_env!("BEYONDDB_SOURCE_REVISION")
        .map(str::to_owned)
        .unwrap_or_else(|| {
            blake3::hash(include_bytes!("beyonddb.rs"))
                .to_hex()
                .to_string()
        });
    let application = Arc::new(Beyonddb::compile(BuildDescriptor {
        source_revision,
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../Cargo.lock")).as_bytes(),
        ),
    })?);
    let release = application.registry().release_digest();
    let image = application.descriptor_digest();
    let url_store = build_url_object_store(&config.storage_url)?;
    let layout = CellStorageLayout::new(
        Store::new(url_store.store_arc()),
        url_store.prefix().clone(),
        *APPLICATION_ID.as_bytes(),
    );
    let directory = NodeDirectory::new(layout.clone(), tls.fleet(), image, release);
    let peer_listener = TcpListener::bind(config.peer_bind).await?;
    let public_listener = TcpListener::bind(config.public_bind).await?;
    let session_uuid = Uuid::now_v7();
    let session = SessionId::from_bytes(*session_uuid.as_bytes());
    let session_dir = config.data_dir.join(session_uuid.to_string());
    tokio::fs::create_dir_all(&config.data_dir).await?;
    let sql_workers = match config.sql_workers {
        Some(workers) => SqlWorkerPool::new(workers, config.max_active_cells)?,
        None => SqlWorkerPool::for_system(config.max_active_cells)?,
    };
    let mut builder = CellNodeBuilder::new(Arc::clone(&application))
        .with_runtime(sql_workers, config.node_retained_bytes)
        .with_replica_host(
            Host::default().with_local_disk_budget(DiskBudget::new(config.disk_budget_bytes)),
        )
        .with_session(session);
    if let Some(bytes) = config.follower_store_bytes {
        builder = builder.with_follower_store(
            config.data_dir.join("follower-store"),
            Limits::default(),
            DiskBudget::new(bytes),
        );
    }
    let node = builder.build()?;
    let node_shutdown = CancellationToken::new();
    let tasks = node.install_task_group(CancellationToken::new(), node_shutdown.clone())?;
    let node_id = NodeId::from_bytes(*config.node_id.as_bytes());
    let endpoint = config.peer_endpoint.clone();
    let signer = tls.signing_key().clone();
    let certificate = tls.certificate();
    let fleet = tls.fleet();
    let capacity_runtime = node.runtime();
    let capacity_dir = config.data_dir.clone();
    let recovery_capable = config.follower_store_bytes.is_some();
    let modules = application.registry().module_digests();
    let published = NodeLeasePublisher::new(directory.clone(), move |now, expires| {
        let (capacity, placement) = if capacity_runtime.is_shutting_down() {
            (NodeCapacity::default(), None)
        } else {
            match measured_node_capacity(&capacity_dir, capacity_runtime.stats()) {
                Ok((mut capacity, placement)) => {
                    // A persistent follower store permits fenced recovery claims.
                    // Zero advertised follower bytes still prevents recruitment
                    // until follower-backed serving has a tested recovery path.
                    if recovery_capable {
                        capacity.log_protocol = NODE_LOG_PROTOCOL_VERSION;
                    }
                    (capacity, Some(placement))
                }
                Err(error) => {
                    // Observation failure disables placement, not the serving lease.
                    // Never renew a stale sample with the next advertisement's time.
                    tracing::warn!(error = ?error, "node placement measurement unavailable");
                    (NodeCapacity::default(), None)
                }
            }
        };
        let advertisement = NodeAdvertisement::sign(
            node_id,
            session,
            endpoint.clone(),
            fleet,
            certificate,
            image,
            release,
            &signer,
            1,
            now,
            expires,
            modules.clone(),
            vec![1],
            NodeFailureDomain::default(),
            capacity,
        )?;
        match placement {
            Some(placement) => advertisement.with_placement_capacity(placement, &signer),
            None => Ok(advertisement),
        }
    })
    .publish()
    .await?;
    let follower_guard = published.guard();
    node.install_node_lease_for_startup(follower_guard.clone())?;
    // Publication during drain still needs the node lease. The host cancels
    // lease maintenance only after the runtime and its durable log close.
    tasks.spawn_lease_maintenance(async move { published.run(&node_shutdown).await })?;
    node.start()?;

    let serving = serve_ready(
        &node,
        tasks,
        &config,
        session_dir.clone(),
        layout,
        directory.clone(),
        application,
        session,
        follower_guard,
        tls,
        peer_listener,
        public_listener,
        public_tls,
        encryption_key,
        bootstrap_secret,
    )
    .await;
    let shutdown = shutdown_serving_node(&node, &directory, session).await;
    let cleanup = if shutdown.is_ok() {
        std::fs::remove_dir_all(session_dir)
    } else {
        Ok(())
    };
    // Joining the supervised tasks exposes the cause of lost readiness. Preserve
    // that source instead of returning only the serving loop's generic symptom.
    shutdown?;
    serving?;
    cleanup?;
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "the bound listeners and node identity are one serving lifecycle"
)]
async fn serve_ready(
    node: &CellNode,
    tasks: Arc<CellNodeTaskGroup>,
    config: &Config,
    session_dir: PathBuf,
    layout: CellStorageLayout,
    directory: NodeDirectory,
    application: Arc<cellule_app::CompiledApplication>,
    session: SessionId,
    follower_guard: NodeLeaseGuard,
    tls: LoadedPeerTls,
    peer_listener: TcpListener,
    public_listener: TcpListener,
    public_tls: Option<ServerTlsConfig>,
    encryption_key: [u8; 32],
    bootstrap_secret: Option<Zeroizing<String>>,
) -> ServerResult<()> {
    let mut peers = BeyonddbPeers::new(node, layout.clone(), directory.clone(), session, &tls)?;
    let mut recovery_transport = None;
    if let Some(store) = node.owned_component::<FollowerStore>(FOLLOWER_STORE_COMPONENT) {
        let node_id = NodeId::from_bytes(*config.node_id.as_bytes());
        recovery_transport = Some(Arc::new(
            PeerNodeLogTransport::new(
                directory.clone(),
                tls.client_identity(),
                session,
                node_id,
                follower_guard.clone(),
            )
            .with_local_follower_store(store.clone()),
        ));
        peers = peers.with_follower_store(node_id, store, follower_guard);
    }
    let peers = Arc::new(peers);
    let mut provisioner = CellInitialPartitionProvisioner::new(
        node.runtime(),
        application,
        layout.clone(),
        session,
        config.peer_endpoint.clone(),
        session_dir,
    )?
    .with_initial_partition_count(config.initial_partitions)?
    .with_peers(peers.clone());
    if let Some(transport) = recovery_transport {
        provisioner = provisioner.with_node_log_recovery(transport)?;
    }
    let provisioner = Arc::new(provisioner);
    for account_id in &config.owned_accounts {
        provisioner
            .recover_configured_account(account_id, &directory)
            .await?;
    }
    for key_id in &config.owned_access_keys {
        provisioner
            .recover_owned_credential(key_id, &directory)
            .await?;
    }
    let client = peers.client_with_cache(provisioner.clone(), config.auth_cache_enabled);
    if let (Some(bootstrap), Some(secret)) = (config.bootstrap.as_ref(), bootstrap_secret) {
        let policy = std::fs::read_to_string(&bootstrap.policy_file)?;
        CellCredentialStore::new(client.clone(), layout.clone(), encryption_key)
            .put_credential(
                &bootstrap.access_key_id,
                StoredCredential {
                    secret_key: secret.to_string(),
                    account_id: bootstrap.account_id.clone(),
                    principal_name: bootstrap.principal_name.clone(),
                    session_name: None,
                    is_session: false,
                    session_token: None,
                    is_active: true,
                    expires_at: None,
                },
            )
            .await?;
        CellAuthorizationStore::new(client.clone())
            .put_user_policy(
                &bootstrap.account_id,
                &bootstrap.principal_name,
                &bootstrap.policy_name,
                &policy,
            )
            .await?;
    }
    let mut state = build_http_state_with_cache(
        node,
        client.clone(),
        layout.clone(),
        provisioner.clone(),
        encryption_key,
        &config.region,
        config.public_endpoint.clone(),
        config.auth_cache_enabled,
    )?;
    state.tls_enabled = public_tls.is_some();
    let peer_cancel = CancellationToken::new();
    let peer_shutdown = peer_cancel.clone();
    let peer_router = peers.router(provisioner.clone());
    // Other recovering nodes may need these local participants. Start private
    // routing before resolving decisions, while public requests remain gated.
    let mut peer_server = tokio::spawn(async move {
        axum::serve(
            tls.listener(peer_listener),
            peer_router.into_make_service_with_connect_info::<PeerTlsIdentity>(),
        )
        .with_graceful_shutdown(async move { peer_shutdown.cancelled().await })
        .await
    });
    let recovery: ServerResult<()> = async {
        let storage = CellStorage::new(client.clone(), config.region.clone());
        for account_id in &config.owned_accounts {
            provisioner
                .recover_registered_account(account_id, &client, &storage, &directory)
                .await?;
            provisioner.install_account_capacity_loop(
                &tasks,
                account_id.clone(),
                client.clone(),
                config.split_threshold_bytes,
                Duration::from_secs(3),
            )?;
        }
        CellStorage::new(client.clone(), config.region.clone()).install_global_index_loop(
            &tasks,
            config.owned_accounts.clone(),
            provisioner.clone(),
            directory.clone(),
        )?;
        CellStorage::new(client.clone(), config.region.clone())
            .install_statistics_loop(&tasks, config.owned_accounts.clone())?;
        provisioner.install_transaction_recovery_loop(
            &tasks,
            storage,
            directory.clone(),
            config.owned_accounts.clone(),
        )?;
        provisioner.install_range_rebalance_loop(&tasks)?;
        let storage = CellStorage::new(client.clone(), config.region.clone());
        let runtime = node.runtime();
        let cancellation = tasks.cancellation_token();
        tasks.spawn(async move {
            let mut ticks = tokio::time::interval(Duration::from_secs(30));
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = cancellation.cancelled() => return Ok::<(), std::io::Error>(()),
                    _ = ticks.tick() => {}
                }
                if let Err(error) = storage.sweep_active_partition_stream_records(&runtime).await {
                    tracing::warn!(%error, "routed stream retention sweep failed");
                }
            }
        })?;
        if !config.owned_accounts.is_empty() {
            let storage = CellStorage::new(client.clone(), config.region.clone());
            let accounts = config.owned_accounts.clone();
            let stream_layout = layout.clone();
            let cancellation = tasks.cancellation_token();
            tasks.spawn(async move {
                let mut ticks = tokio::time::interval(Duration::from_secs(30));
                ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        () = cancellation.cancelled() => return Ok::<(), std::io::Error>(()),
                        _ = ticks.tick() => {}
                    }
                    for account_id in &accounts {
                        if cancellation.is_cancelled() {
                            return Ok(());
                        }
                        if let Err(error) = storage.sweep_account_ttl(account_id).await {
                            tracing::warn!(account_id, %error, "TTL sweep failed");
                        }
                        if let Err(error) = storage.sweep_account_stream_records(account_id).await {
                            tracing::warn!(account_id, %error, "account stream retention sweep failed");
                        }
                        // Cancellation leaves the durable shard cursor in place, so
                        // the next owner can repeat an interrupted catalog scan.
                        let result = tokio::select! {
                            () = cancellation.cancelled() => return Ok(()),
                            result = storage.sweep_account_catalog_stream_records(account_id, &stream_layout) => result,
                        };
                        if let Err(error) = result {
                            tracing::warn!(account_id, %error, "catalog stream retention sweep failed");
                        }
                    }
                }
            })?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = recovery {
        peer_cancel.cancel();
        peer_server.await??;
        return Err(error);
    }
    let mut public_server = tokio::spawn(extenddb_server::start_server(
        public_listener,
        state,
        None,
        public_tls,
    ));
    enum Exit {
        Public(ServerResult<()>),
        Peer(ServerResult<()>),
        Unready(ServerResult<()>),
    }
    let exit = tokio::select! {
        result = &mut public_server => Exit::Public(match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(error.into_boxed_dyn_error()),
            Err(error) => Err(Box::new(error)),
        }),
        result = &mut peer_server => Exit::Peer(join(result)),
        result = wait_until_unready(node) => Exit::Unready(result),
    };
    peer_cancel.cancel();
    match exit {
        Exit::Public(result) => {
            peer_server.await??;
            result
        }
        Exit::Peer(result) => {
            public_server.abort();
            let _ = public_server.await;
            result
        }
        Exit::Unready(result) => {
            public_server.abort();
            let _ = public_server.await;
            peer_server.await??;
            result
        }
    }
}

async fn wait_until_unready(node: &CellNode) -> ServerResult<()> {
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if !node.is_ready() {
            return Err(invalid("Cell node lost its serving lease or task health").into());
        }
    }
}

fn join<T, E>(result: Result<Result<T, E>, tokio::task::JoinError>) -> ServerResult<()>
where
    E: Error + Send + Sync + 'static,
{
    result??;
    Ok(())
}

fn read_encryption_key(path: &PathBuf) -> ServerResult<[u8; 32]> {
    std::fs::read(path)?
        .try_into()
        .map_err(|_| invalid("encryption key file must contain exactly 32 bytes").into())
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
