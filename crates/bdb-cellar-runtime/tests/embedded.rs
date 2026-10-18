//! Real Cellar invocation tests; no alternate SQLite connection or fake Context.

#[path = "embedded/range_cells.rs"]
mod range_cells;

use std::{
    path::Path,
    sync::{Arc, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context as _;
use bdb_cellar_runtime::{
    Result,
    client::{CellClient, InvocationError, MutationIdentity},
    descriptors::*,
    identity::*,
    registry::{
        CellModule, Command, CommandContext, CommandResult, Query, QueryContext, Registry,
        RegistryBuilder,
    },
    sql::{SqlBatch, SqlStatement},
};
use cellar::sdk::{
    ActorIdentity, ActorOptions, ActorPolicy, CellLimits, ExternalNodeServices, Node, NodeConfig,
    NodeOutcome, OwnershipOnEvict, PeerAuth, PeerProbeSigner, ShutdownMode,
    primitives::sql::Value as SqlValue,
};

const MODULE: &str = "BdbTest";
const NAMESPACE: NamespaceId = NamespaceId::from_bytes([1; 16]);
const SCHEMA: &str = "
CREATE TABLE item (id INTEGER PRIMARY KEY CHECK(id = 1), value BLOB NOT NULL, exact INTEGER NOT NULL);
CREATE TABLE audit (writes INTEGER NOT NULL);
INSERT INTO audit VALUES(0);
CREATE TRIGGER item_insert AFTER INSERT ON item BEGIN UPDATE audit SET writes=writes+1; END;
CREATE TRIGGER item_update AFTER UPDATE ON item BEGIN UPDATE audit SET writes=writes+1; END;
";
const fn operation(id: u32) -> OperationDescriptor {
    OperationDescriptor {
        id,
        codec_version: 1,
        schema_min: 1,
        schema_max: 1,
        input_limit: 1024,
        output_limit: 1024,
    }
}
static COMMANDS: [OperationDescriptor; 3] = [operation(1), operation(2), operation(3)];
static QUERIES: [OperationDescriptor; 2] = [operation(1), operation(2)];
static NAMESPACES: [NamespaceDescriptor; 1] = [NamespaceDescriptor {
    id: NAMESPACE,
    name: MODULE,
    role: CatalogRole::Sql,
    shards: 1,
    effect_targets: &[],
    dead_letter: None,
}];

struct TestModule;
impl CellModule for TestModule {
    const NAME: &'static str = MODULE;
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: MODULE,
            source_digest: Digest::from_bytes([2; 32]),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: Box::leak(Box::new([MigrationDescriptor {
                version: 1,
                sql: SCHEMA,
                digest: Digest::from_bytes(*blake3::hash(SCHEMA.as_bytes()).as_bytes()),
            }])),
            commands: &COMMANDS,
            queries: &QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &NAMESPACES,
        })
    }
    fn register(self, builder: &mut RegistryBuilder) -> Result<()> {
        builder.bind_command::<Write>()?;
        builder.bind_command::<Reject>()?;
        builder.bind_command::<Fail>()?;
        builder.bind_query::<Read>()?;
        builder.bind_query::<IllegalWrite>()
    }
}

fn statement(sql: &str, parameters: Vec<SqlValue>) -> SqlBatch {
    SqlBatch {
        statements: vec![SqlStatement {
            sql: sql.into(),
            parameters,
        }],
    }
}

fn put(context: &CommandContext<'_, '_>, bytes: &[u8]) -> Result<()> {
    let result = context.sql(&statement("INSERT INTO item (id, value, exact) VALUES (1, ?1, ?2) ON CONFLICT(id) DO UPDATE SET value=excluded.value, exact=excluded.exact", vec![SqlValue::Blob(bytes.to_vec()), SqlValue::Integer(i64::MAX)]))?;
    assert_eq!(result[0].rows_affected, 1);
    Ok(())
}

struct Write;
impl Command for Write {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Vec<u8>,
    ) -> Result<CommandResult<Vec<u8>>> {
        put(context, &input)?;
        Ok(CommandResult::Success(input))
    }
}

struct Reject;
impl Command for Reject {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Vec<u8>,
    ) -> Result<CommandResult<Vec<u8>>> {
        put(context, &input)?;
        Ok(CommandResult::Rejected(input))
    }
}

struct Fail;
impl Command for Fail {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 3;
    const CODEC_VERSION: u32 = 1;
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Vec<u8>,
    ) -> Result<CommandResult<Vec<u8>>> {
        put(context, &input)?;
        Err(bdb_cellar_runtime::Error::Command(
            "deliberate command failure",
        ))
    }
}

struct Read;
impl Query for Read {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = ();
    type Output = Option<Vec<u8>>;
    fn execute(context: &mut QueryContext<'_>, (): ()) -> Result<Self::Output> {
        let rows = context.sql(&statement(
            "SELECT value, exact FROM item WHERE id=1",
            vec![],
        ))?;
        let Some(row) = rows[0].rows.first() else {
            return Ok(None);
        };
        match row.as_slice() {
            [SqlValue::Blob(value), SqlValue::Integer(i64::MAX)] => Ok(Some(value.clone())),
            _ => Err(bdb_cellar_runtime::Error::Command(
                "SQL value lost type or precision",
            )),
        }
    }
}

struct IllegalWrite;
impl Query for IllegalWrite {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = ();
    type Output = ();
    fn execute(context: &mut QueryContext<'_>, (): ()) -> Result<()> {
        context.sql(&statement("DELETE FROM item", vec![]))?;
        Ok(())
    }
}

fn registry() -> Result<Arc<Registry>> {
    let mut builder = RegistryBuilder::new();
    builder.register(TestModule)?;
    builder.register(range_cells::RangeModule)?;
    Ok(Arc::new(builder.finish()?))
}

fn target(tenant: u8) -> Result<CellTarget> {
    CellTarget::new(
        TenantId::from_bytes([tenant; 16]),
        ApplicationId::from_bytes([2; 16]),
        NAMESPACE,
        b"same-partition",
    )
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
fn mutation(id: u8) -> MutationIdentity {
    let issued = now();
    MutationIdentity {
        request_id: RequestId::from_bytes([id; 16]),
        issued_at_ms: issued,
        expires_at_ms: issued + 60_000,
    }
}

async fn start(
    path: &Path,
    registry: Arc<Registry>,
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
) -> anyhow::Result<(Node, CellClient)> {
    let public = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let peer = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let signer = Arc::new(PeerProbeSigner::generate());
    let mut config = NodeConfig::new(
        ActorIdentity {
            node: "bdb-cellar-test".into(),
            advertise: peer.local_addr()?.to_string(),
            region: "local".into(),
        },
        ActorOptions {
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
        },
        path.to_path_buf(),
    );
    config.maintenance.rebalance = false;
    config.runtime.max_cell_requests = 8;
    config.runtime.handler_budget = Duration::from_secs(10);
    config.cell_limits = CellLimits::unrestricted();
    let services = ExternalNodeServices {
        wall_clock: None,
        http_client: None,
        host_executor: host.clone(),
        core_executor: core.clone(),
        public_address: public.local_addr()?,
        ingress_capacity: 16,
        internal_listener: peer,
        peer_http: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .build()?,
        peer_auth: Arc::new(PeerAuth::new([7; 32], "bdb-cellar-test")?),
        probe_signer: signer,
        fleet: None,
        containers: None,
        metrics: None,
    };
    let (node, ingress) = tokio::time::timeout(
        Duration::from_secs(30),
        Node::start_external(
            config,
            services,
            registry.deployment(),
            registry.compiled(Arc::new(now))?,
        ),
    )
    .await
    .context("embedded SDK node startup deadline")??;
    Ok((node, CellClient::new(ingress, registry)))
}

async fn stop(node: Node) -> anyhow::Result<()> {
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        node.shutdown(ShutdownMode::Preserve),
    )
    .await
    .context("embedded SDK node shutdown deadline")??;
    let NodeOutcome::Stopped(report) = result else {
        anyhow::bail!("unexpected stop: {result:?}");
    };
    anyhow::ensure!(
        report.ingress_drained
            && report.application_work_drained
            && report.cells_drained
            && report.pending_dispatches == 0
            && !report.drain_timed_out,
        "incomplete stop: {report:?}"
    );
    Ok(())
}

#[test]
fn sdk_commands_preserve_types_replay_rejections_and_restart() -> anyhow::Result<()> {
    let data = tempfile::tempdir()?;
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    host.block_on(async {
        let registry = registry()?;
        let (node, client) =
            start(data.path(), registry.clone(), host.handle(), core.handle()).await?;
        let account = target(3)?;
        let other = target(4)?;
        let original = mutation(1);
        let value = vec![0, 255, 0, 128, b'x'];
        let committed = client
            .command::<Write>(&account, original, value.clone())
            .await?;
        assert_eq!(committed.receipt.commit_sequence, 1);
        let (first, second) = tokio::join!(
            client.command::<Write>(&account, original, value.clone()),
            client.command::<Write>(&account, original, value.clone())
        );
        assert_eq!(first?.receipt, committed.receipt);
        assert_eq!(second?.receipt, committed.receipt);
        assert!(
            client
                .command::<Write>(&account, original, vec![1])
                .await
                .is_err()
        );
        let rejected_id = mutation(2);
        assert!(matches!(
            client
                .command::<Reject>(&account, rejected_id, vec![2])
                .await,
            Err(InvocationError::Rejected(_))
        ));
        assert_eq!(
            client.query::<Read>(&account, None, ()).await?.output,
            Some(value.clone())
        );
        assert!(
            client
                .command::<Fail>(&account, mutation(3), vec![3])
                .await
                .is_err()
        );
        assert!(
            client
                .query::<IllegalWrite>(&account, None, ())
                .await
                .is_err()
        );
        assert_eq!(
            client.query::<Read>(&account, None, ()).await?.output,
            Some(value.clone())
        );
        assert_eq!(client.query::<Read>(&other, None, ()).await?.output, None);
        assert!(matches!(
            client
                .query::<Read>(&other, Some(committed.receipt), ())
                .await,
            Err(InvocationError::NotStarted(_))
        ));
        stop(node).await?;
        drop(client);
        let (node, client) = start(data.path(), registry, host.handle(), core.handle()).await?;
        assert_eq!(
            client
                .query::<Read>(&account, Some(committed.receipt), ())
                .await?
                .output,
            Some(value.clone())
        );
        assert_eq!(
            client
                .command::<Write>(&account, original, value)
                .await?
                .receipt,
            committed.receipt
        );
        assert!(matches!(
            client
                .command::<Reject>(&account, rejected_id, vec![2])
                .await,
            Err(InvocationError::Rejected(_))
        ));
        stop(node).await
    })
}
