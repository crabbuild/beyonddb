//! Durable, sharded discovery of application cells, independent of residency.
//! Entries precede range installation so interrupted creates remain discoverable.

use crate::backend::{cell_error, mutation_identity};
use crate::{Error, Json, Result, SqlValue, table::statement};
use cellar_runtime::{
    app::CellType,
    client::CellClient,
    descriptors::CatalogRole,
    identity::{CellTarget, Digest, NamespaceId, TenantId, partition_for_shard, shard_for_scope},
    registry::{
        CellModule, Command, CommandContext, CommandResult, MigrationDescriptor, ModuleDescriptor,
        NamespaceDescriptor, Query, QueryContext, RegistryBuilder,
    },
};
use extenddb_storage::error::StorageError;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

const MODULE: &str = "beyonddb-inventory";
const NS: NamespaceId = NamespaceId::from_bytes([0x48; 16]);
pub(crate) const SHARDS: u32 = 256;
const SCHEMA: &str =
    "CREATE TABLE bdb_managed_cells (cell BLOB PRIMARY KEY, record BLOB NOT NULL) WITHOUT ROWID;";
static COMMANDS: [cellar_runtime::registry::OperationDescriptor; 1] = [crate::operation(1)];
static QUERIES: [cellar_runtime::registry::OperationDescriptor; 2] =
    [crate::operation(1), crate::operation(2)];
static NAMESPACES: [NamespaceDescriptor; 1] = [NamespaceDescriptor {
    id: NS,
    name: MODULE,
    role: CatalogRole::Sql,
    shards: SHARDS,
    effect_targets: &[],
    dead_letter: None,
}];

pub(crate) struct InventoryModule;
impl CellModule for InventoryModule {
    const NAME: &'static str = MODULE;
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: MODULE,
            source_digest: Digest::from_bytes(
                *blake3::hash(include_bytes!("inventory.rs")).as_bytes(),
            ),
            schema_min: 1,
            schema_max: 1,
            retained_codes: &[],
            migrations: Box::leak(Box::new([MigrationDescriptor {
                version: 1,
                sql: SCHEMA,
                digest: Digest::from_bytes(*blake3::hash(SCHEMA.as_bytes()).as_bytes()),
            }])),
            commands: &COMMANDS,
            queries: &QUERIES,
            namespaces: &NAMESPACES,
            workflow_definitions: &[],
            activity_types: &[],
        })
    }
    fn register(self, builder: &mut RegistryBuilder) -> Result<()> {
        builder.bind_command::<Register>()?;
        builder.bind_query::<Contains>()?;
        builder.bind_query::<List>()
    }
}
pub(crate) fn cell_type() -> Result<CellType> {
    CellType::new(MODULE, MODULE, NS, CatalogRole::Sql, SHARDS)?
        .with_limits(64 * 1024 * 1024, 1 << 20)
}
pub(crate) fn target(tenant: TenantId, shard: u32) -> Result<CellTarget> {
    if shard >= SHARDS {
        return Err(Error::Identity("invalid inventory shard"));
    }
    CellTarget::new(tenant, crate::APPLICATION, NS, &partition_for_shard(shard))
}
fn for_cell(target: &CellTarget) -> Result<CellTarget> {
    self::target(
        target.tenant(),
        shard_for_scope(NS, target.cell_id().as_bytes(), SHARDS)?,
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Entry {
    pub namespace: [u8; 16],
    pub partition: Vec<u8>,
    pub cell: [u8; 32],
}
impl Entry {
    pub fn target(&self, tenant: TenantId) -> Result<CellTarget> {
        let ns = NamespaceId::from_bytes(self.namespace);
        if ![
            crate::DATA_NAMESPACE,
            crate::global_index::NAMESPACE,
            crate::directory::NAMESPACE,
            crate::credentials::NAMESPACE,
            crate::transaction_coordinator::NAMESPACE,
        ]
        .contains(&ns)
        {
            return Err(Error::Identity("unmanaged inventory namespace"));
        }
        let target = CellTarget::new(tenant, crate::APPLICATION, ns, &self.partition)?;
        if target.cell_id().as_bytes() != &self.cell {
            return Err(Error::Identity("inventory identity mismatch"));
        }
        Ok(target)
    }
}
struct Register;
impl Command for Register {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<Entry>;
    type Output = Json<bool>;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(entry): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        let target = entry.target(context.target().tenant())?;
        if for_cell(&target)? != *context.target() {
            return Err(Error::Identity("wrong inventory shard"));
        }
        let record = serde_json::to_vec(&entry)?;
        let rows = context.sql(&statement(
            "SELECT record FROM bdb_managed_cells WHERE cell=?1",
            vec![SqlValue::Blob(entry.cell.to_vec())],
        ))?;
        if let Some(row) = rows[0].rows.first() {
            if row.as_slice() != [SqlValue::Blob(record)] {
                return Err(Error::Identity("inventory collision"));
            }
        } else {
            context.sql(&statement(
                "INSERT INTO bdb_managed_cells VALUES(?1, ?2)",
                vec![SqlValue::Blob(entry.cell.to_vec()), SqlValue::Blob(record)],
            ))?;
        }
        Ok(CommandResult::Success(Json(true)))
    }
}
struct Contains;
impl Query for Contains {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<[u8; 32]>;
    type Output = Json<bool>;
    fn execute(context: &mut QueryContext<'_>, Json(cell): Self::Input) -> Result<Self::Output> {
        Ok(Json(
            !context.sql(&statement(
                "SELECT 1 FROM bdb_managed_cells WHERE cell=?1",
                vec![SqlValue::Blob(cell.to_vec())],
            ))?[0]
                .rows
                .is_empty(),
        ))
    }
}
pub(crate) struct List;
impl Query for List {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<Option<[u8; 32]>>;
    type Output = Json<Vec<Entry>>;
    fn execute(context: &mut QueryContext<'_>, Json(after): Self::Input) -> Result<Self::Output> {
        let rows = context.sql(&statement("SELECT record FROM bdb_managed_cells WHERE (?1 IS NULL OR cell>?1) ORDER BY cell LIMIT 32", vec![after.map_or(SqlValue::Null, |cell| SqlValue::Blob(cell.to_vec()))]))?;
        let mut entries = Vec::new();
        for row in &rows[0].rows {
            let [SqlValue::Blob(record)] = row.as_slice() else {
                return Err(Error::Command("invalid inventory row"));
            };
            entries.push(serde_json::from_slice(record)?);
        }
        Ok(Json(entries))
    }
}
pub(crate) async fn register(
    client: &CellClient,
    target: &CellTarget,
) -> std::result::Result<(), StorageError> {
    let catalog = for_cell(target).map_err(|error| StorageError::Internal(error.to_string()))?;
    client
        .command::<Register>(
            &catalog,
            mutation_identity()?,
            Json(Entry {
                namespace: *target.namespace().as_bytes(),
                partition: target.partition().to_vec(),
                cell: *target.cell_id().as_bytes(),
            }),
        )
        .await
        .map_err(cell_error)?;
    Ok(())
}
pub(crate) async fn contains(
    client: &CellClient,
    target: &CellTarget,
) -> std::result::Result<bool, StorageError> {
    let catalog = for_cell(target).map_err(|error| StorageError::Internal(error.to_string()))?;
    Ok(client
        .query::<Contains>(&catalog, None, Json(*target.cell_id().as_bytes()))
        .await
        .map_err(cell_error)?
        .output
        .0)
}
