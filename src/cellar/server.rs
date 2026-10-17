//! Embedded Cellar hosting and ExtendDB HTTP composition.
#[path = "transactions.rs"]
mod transactions;
use crate::{
    Beyonddb, CellCatalogStore, CellCredentialStore, CellInitialPartitionProvisioner, CellStorage,
};
use bdb_cellar_runtime::{
    app::CellApplication, client::CellClient, descriptors::BuildDescriptor, identity::Digest,
};
use cellar::sdk::{ExternalNodeServices, Node, NodeConfig, NodeOutcome, ShutdownMode};
use extenddb_auth::{AuthCacheRegistry, BuiltinAuthProvider};
use extenddb_core::limits::LimitsConfig;
use extenddb_server::{AppState, AuthzCacheConfig, CachedAuthzStore, CachedTableKeyInfoStore};
use extenddb_storage::{StorageEngine, error::StorageError};
use futures_util::FutureExt;
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;
pub use transactions::TransactionRecovery;

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;

#[derive(Default)]
pub struct TaskGroup {
    cancellation: CancellationToken,
    tasks: Mutex<Vec<tokio::task::JoinHandle<Result<(), String>>>>,
}
impl TaskGroup {
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
    pub fn spawn<F, E>(&self, future: F) -> std::io::Result<()>
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        let cancellation = self.cancellation.clone();
        let handle = tokio::spawn(async move {
            let result = std::panic::AssertUnwindSafe(future)
                .catch_unwind()
                .await
                .map_err(|_| "maintenance task panicked".to_owned())
                .and_then(|result| result.map_err(|error| error.to_string()));
            if result.is_err() {
                cancellation.cancel();
            }
            result
        });
        self.tasks
            .lock()
            .map_err(|_| std::io::Error::other("maintenance task lock poisoned"))?
            .push(handle);
        Ok(())
    }
    async fn shutdown(&self) -> anyhow::Result<()> {
        self.cancellation.cancel();
        let handles = std::mem::take(
            &mut *self
                .tasks
                .lock()
                .map_err(|_| anyhow::anyhow!("maintenance task lock poisoned"))?,
        );
        let mut first = None;
        for mut handle in handles {
            let outcome = match tokio::time::timeout(Duration::from_secs(30), &mut handle).await {
                Ok(outcome) => outcome
                    .map_err(|error| error.to_string())
                    .and_then(|result| result),
                Err(_) => {
                    handle.abort();
                    let _ = handle.await;
                    Err("maintenance shutdown deadline".into())
                }
            };
            if let Err(error) = outcome {
                first.get_or_insert(error);
            }
        }
        if let Some(error) = first {
            anyhow::bail!(error);
        }
        Ok(())
    }
}

pub struct CellarServerConfig {
    pub node: NodeConfig,
    pub region: String,
    pub encryption_key: [u8; 32],
    pub initial_partitions: u16,
    pub cache_enabled: bool,
}

pub struct CellarServer {
    node: Node,
    client: CellClient,
    provisioner: Arc<CellInitialPartitionProvisioner>,
    tasks: TaskGroup,
    region: String,
    encryption_key: [u8; 32],
    cache_enabled: bool,
}
impl CellarServer {
    pub async fn start_account_maintenance(
        &self,
        accounts: Vec<String>,
        split_bytes: u64,
    ) -> anyhow::Result<()> {
        let storage = Arc::new(CellStorage::new(self.client.clone(), self.region.clone()));
        let mut transactions: Vec<_> = accounts
            .iter()
            .map(|account| TransactionRecovery::new(storage.clone(), account.clone()))
            .collect::<Result<_, _>>()?;
        let mut failure = None;
        for recovery in &mut transactions {
            if let Err(error) = recovery.initial_pass().await {
                failure.get_or_insert(error);
            }
        }
        if let Some(error) = failure {
            return Err(error.into());
        }
        for account in &accounts {
            self.provisioner.admit_account(account).await?;
            self.provisioner.install_account_capacity_loop(
                &self.tasks,
                account.clone(),
                self.client.clone(),
                split_bytes,
                Duration::from_secs(3),
            )?;
        }
        CellStorage::new(self.client.clone(), self.region.clone())
            .install_global_index_loop(&self.tasks, accounts.clone())?;
        CellStorage::new(self.client.clone(), self.region.clone())
            .install_statistics_loop(&self.tasks, accounts.clone())?;
        let client = self.client.clone();
        let cancellation = self.tasks.cancellation_token();
        self.tasks.spawn(async move {
            // One inventory page per account per tick, including cells whose
            // tables were deleted or whose split source is no longer routed.
            let mut cursors = vec![(0_u32, None::<[u8; 32]>); accounts.len()];
            let mut ticks = tokio::time::interval(Duration::from_secs(1));
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { () = cancellation.cancelled() => return Ok::<(), StorageError>(()), _ = ticks.tick() => {} }
                for (index, account) in accounts.iter().enumerate() {
                    let result = tokio::select! {
                        () = cancellation.cancelled() => return Ok(()),
                        result = async {
                            transactions[index].step().await?;
                            storage.sweep_account_ttl(account).await?;
                            storage.sweep_account_stream_records(account).await?;
                            let tenant = crate::account_target(account).map_err(|error| StorageError::Internal(error.to_string()))?.tenant();
                            let (shard, after) = &mut cursors[index];
                            let inventory = crate::inventory::target(tenant, *shard).map_err(|error| StorageError::Internal(error.to_string()))?;
                            let entries = client.query::<crate::inventory::List>(&inventory, None, crate::Json(*after)).await.map_err(crate::backend::cell_error)?.output.0;
                            for entry in &entries {
                                if entry.namespace == *crate::DATA_NAMESPACE.as_bytes() {
                                    let target = entry.target(tenant).map_err(|error| StorageError::Internal(error.to_string()))?;
                                    storage.sweep_partition_stream_records(&target).await?;
                                }
                            }
                            if entries.len() == 32 { *after = entries.last().map(|entry| entry.cell); }
                            else { *after = None; *shard = (*shard + 1) % crate::inventory::SHARDS; }
                            Ok::<(), StorageError>(())
                        } => result,
                    };
                    if let Err(error) = result { tracing::warn!(account, %error, "account maintenance deferred"); }
                }
            }
        })?;
        Ok(())
    }

    /// ExtendDB owns the public protocol listener; Cellar owns its private peer
    /// listener. A node fence withdraws public serving immediately.
    pub async fn serve(
        self,
        listener: tokio::net::TcpListener,
        endpoint: String,
        tls: Option<extenddb_server::ServerTlsConfig>,
        mode: ShutdownMode,
    ) -> anyhow::Result<NodeOutcome> {
        let mut state = self.http_state(endpoint)?;
        state.tls_enabled = tls.is_some();
        let Self {
            mut node, tasks, ..
        } = self;
        let mut public = tokio::spawn(extenddb_server::start_server(listener, state, None, tls));
        let cancellation = tasks.cancellation_token();
        enum Exit {
            Public(anyhow::Result<()>),
            Node(anyhow::Result<NodeOutcome>),
            Maintenance,
            Unready,
        }
        let exit = loop {
            let event = tokio::select! {
                result = &mut public => Some(Exit::Public(result.map_err(anyhow::Error::new).and_then(|result| result))),
                result = node.finished() => Some(Exit::Node(result)),
                () = cancellation.cancelled() => Some(Exit::Maintenance),
                _ = tokio::time::sleep(Duration::from_secs(1)) => None,
            };
            if let Some(event) = event {
                break event;
            }
            if !matches!(
                tokio::time::timeout(Duration::from_secs(3), node.is_ready()).await,
                Ok(Ok(true))
            ) {
                break Exit::Unready;
            }
        };
        match exit {
            Exit::Public(public_result) => {
                let maintenance = tasks.shutdown().await;
                let outcome = node.shutdown(mode).await;
                public_result?;
                maintenance?;
                outcome
            }
            Exit::Node(outcome) => {
                public.abort();
                let _ = public.await;
                tasks.shutdown().await?;
                outcome
            }
            Exit::Maintenance => {
                public.abort();
                let _ = public.await;
                let maintenance = tasks.shutdown().await;
                let outcome = node.shutdown(mode).await;
                maintenance?;
                outcome
            }
            Exit::Unready => {
                public.abort();
                let _ = public.await;
                let maintenance = tasks.shutdown().await;
                let outcome = node.shutdown(mode).await;
                maintenance?;
                outcome?;
                anyhow::bail!("Cellar readiness was lost; public serving withdrawn")
            }
        }
    }
    pub async fn start(
        mut config: CellarServerConfig,
        services: ExternalNodeServices,
    ) -> anyhow::Result<Self> {
        config.node.cell_limits.sql = bdb_cellar_runtime::sql::host_limits();
        let application = Arc::new(Beyonddb::compile(BuildDescriptor {
            source_revision: env!("CARGO_PKG_VERSION").into(),
            cargo_lock_digest: Digest::from_bytes(
                *blake3::hash(include_bytes!("../../Cargo.lock")).as_bytes(),
            ),
        })?);
        let registry = application.registry();
        let clock = Arc::new(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .and_then(|time| i64::try_from(time.as_millis()).ok())
                .unwrap_or(0)
        });
        let readiness_timeout = config
            .node
            .maintenance
            .readiness_observation_deadline
            .unwrap_or(config.node.startup.bootstrap_timeout);
        let (mut node, ingress) = Node::start_external(
            config.node,
            services,
            registry.deployment(),
            registry.compiled(clock)?,
        )
        .await?;
        // Native startup establishes authority before the fleet observation
        // gate necessarily opens. Do not publish an optimistic HTTP /health
        // or treat that initial gate as a terminal readiness loss.
        let deadline = tokio::time::Instant::now()
            .checked_add(readiness_timeout)
            .ok_or_else(|| anyhow::anyhow!("readiness timeout exceeds the host clock"))?;
        loop {
            if matches!(
                tokio::time::timeout(Duration::from_secs(3), node.is_ready()).await,
                Ok(Ok(true))
            ) {
                break;
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "Cellar startup readiness deadline elapsed"
            );
            tokio::select! {
                outcome = node.finished() => anyhow::bail!("Cellar stopped before readiness: {outcome:?}"),
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
        let client = CellClient::new(ingress, registry);
        let provisioner = Arc::new(
            CellInitialPartitionProvisioner::new(client.clone(), application)
                .with_initial_partition_count(config.initial_partitions)?,
        );
        Ok(Self {
            node,
            client,
            provisioner,
            tasks: TaskGroup::default(),
            region: config.region,
            encryption_key: config.encryption_key,
            cache_enabled: config.cache_enabled,
        })
    }
    pub fn client(&self) -> CellClient {
        self.client.clone()
    }
    pub fn provisioner(&self) -> Arc<CellInitialPartitionProvisioner> {
        self.provisioner.clone()
    }
    pub fn http_state(&self, endpoint: String) -> Result<AppState, StorageError> {
        build_http_state_with_cache(
            self.client.clone(),
            self.provisioner.clone(),
            self.encryption_key,
            &self.region,
            endpoint,
            self.cache_enabled,
        )
    }
    pub async fn shutdown(self, mode: ShutdownMode) -> anyhow::Result<NodeOutcome> {
        let tasks = self.tasks.shutdown().await;
        let outcome = self.node.shutdown(mode).await;
        tasks?;
        outcome
    }
}

pub fn build_http_state(
    client: CellClient,
    provisioner: Arc<CellInitialPartitionProvisioner>,
    encryption_key: [u8; 32],
    region: &str,
    server_addr: String,
) -> Result<AppState, StorageError> {
    build_http_state_with_cache(
        client,
        provisioner,
        encryption_key,
        region,
        server_addr,
        false,
    )
}
pub fn build_http_state_with_cache(
    client: CellClient,
    provisioner: Arc<CellInitialPartitionProvisioner>,
    encryption_key: [u8; 32],
    region: &str,
    server_addr: String,
    cache_enabled: bool,
) -> Result<AppState, StorageError> {
    let storage: Arc<dyn StorageEngine> = Arc::new(
        CellStorage::new(client.clone(), region)
            .with_transaction_coordinators(provisioner.clone())
            .with_initial_partitions(provisioner)
            .with_route_cache(cache_enabled),
    );
    let credentials: Arc<dyn extenddb_auth::CredentialStore> =
        Arc::new(CellCredentialStore::new(client.clone(), encryption_key));
    let cached_credentials = Arc::new(if cache_enabled {
        extenddb_auth::CachedCredentialStore::with_arc(credentials.clone(), Default::default())
    } else {
        extenddb_auth::CachedCredentialStore::pass_through_arc(credentials, Default::default())
    });
    let catalog = Arc::new(CellCatalogStore::new(client));
    let authz_cache = Arc::new(if cache_enabled {
        CachedAuthzStore::new(catalog.clone(), AuthzCacheConfig::default())
    } else {
        CachedAuthzStore::pass_through(catalog.clone(), AuthzCacheConfig::default())
    });
    let table_key_info_cache = Arc::new(if cache_enabled {
        CachedTableKeyInfoStore::new(storage.clone(), Default::default())
    } else {
        CachedTableKeyInfoStore::pass_through(storage.clone(), Default::default())
    });
    let auth_cache = AuthCacheRegistry::empty()
        .with_credential(cached_credentials.clone())
        .with_authz_invalidator(authz_cache.clone())
        .with_table_key_info_invalidator(table_key_info_cache.clone());
    Ok(AppState {
        storage: Arc::clone(&storage),
        auth: Arc::new(BuiltinAuthProvider::new((*cached_credentials).clone())),
        limits: Arc::new(LimitsConfig::default()),
        region: Arc::from(region),
        server_addr,
        catalog_store: Some(catalog),
        version_info: Arc::from(env!("CARGO_PKG_VERSION")),
        metrics: Arc::default(),
        tls_enabled: false,
        dev_mode: false,
        import_paths: Arc::from([]),
        export_paths: Arc::from([]),
        throttle: Arc::default(),
        auth_cache,
        authz_cache,
        table_key_info_cache,
        config_entries: Vec::new(),
        docs_store: None,
    })
}
