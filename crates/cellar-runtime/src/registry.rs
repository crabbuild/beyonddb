//! Compiled BeyondDB commands; all storage access retains the Cellar invocation.

use std::{collections::BTreeMap, marker::PhantomData};

use cellar::sdk::Storage;

use crate::{
    Error, Result,
    codec::{BoundedDecoder, BoundedEncoder, WireValue},
    identity::{CellId, CellTarget, NamespaceId},
    sql::{self, AccessMode, SqlBatch, SqlResultSet},
};

pub use crate::descriptors::*;

pub struct CommandContext<'borrow, 'invocation> {
    storage: &'borrow Storage,
    target: &'borrow CellTarget,
    sequence: u64,
    now_ms: i64,
    maximum_database_bytes: u64,
    _invocation: PhantomData<&'invocation ()>,
}

impl<'a> CommandContext<'a, '_> {
    pub(crate) fn new(
        storage: &'a Storage,
        target: &'a CellTarget,
        sequence: u64,
        now_ms: i64,
        maximum_database_bytes: u64,
    ) -> Self {
        Self {
            storage,
            target,
            sequence,
            now_ms,
            maximum_database_bytes,
            _invocation: PhantomData,
        }
    }

    pub fn sql(&self, batch: &SqlBatch) -> Result<Vec<SqlResultSet>> {
        sql::execute(self.storage, batch, AccessMode::ReadWrite)
    }

    pub fn target(&self) -> &CellTarget {
        self.target
    }
    pub fn cell_id(&self) -> CellId {
        self.target.cell_id()
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn now_ms(&self) -> i64 {
        self.now_ms
    }

    /// Fresh Cellar SQLite databases at the pinned runtime use 4096-byte pages.
    pub fn database_page_size(&self) -> Result<u32> {
        Ok(crate::capacity::PAGE_BYTES as u32)
    }

    pub fn reserve_database_capacity(&self, key: &[u8], bytes: u64) -> Result<()> {
        crate::capacity::reserve(self.storage, key, bytes, self.maximum_database_bytes)
    }

    pub fn release_database_capacity(&self, key: &[u8]) -> Result<bool> {
        crate::capacity::release(self.storage, key)
    }

    /// Bounded writes to the known unindexed item-image BLOB columns. SQL uses
    /// bound binary values and checks the existing allocation before replacing
    /// a slice; a failure propagates to the enclosing command savepoint.
    pub fn write_sql_blob(
        &self,
        table: &str,
        column: &str,
        row_id: i64,
        offset: usize,
        bytes: &[u8],
    ) -> Result<()> {
        use cellar::sdk::primitives::sql::Value as V;
        if column != "item"
            || !matches!(
                table,
                "ddb_items"
                    | "ddb_partition_items"
                    | "ddb_global_index_items"
                    | "ddb_index_changes"
                    | "ddb_transaction_reads"
            )
            || bytes.len() > (1 << 20)
        {
            return Err(Error::Command("unsupported item BLOB write"));
        }
        let rows = self
            .storage
            .sql_cursor(
                &format!("SELECT length(item) FROM {table} WHERE rowid = ?1"),
                &[V::Integer(row_id)],
            )?
            .to_raw_array()?;
        let Some([V::Integer(length)]) = rows.first().map(Vec::as_slice) else {
            return Err(Error::Command("missing item BLOB row"));
        };
        let length =
            usize::try_from(*length).map_err(|_| Error::Command("invalid item BLOB length"))?;
        if offset
            .checked_add(bytes.len())
            .is_none_or(|end| end > length)
        {
            return Err(Error::Command("item BLOB write exceeds allocation"));
        }
        let start = i64::try_from(offset).map_err(|_| Error::Command("invalid BLOB offset"))?;
        let end = i64::try_from(offset + bytes.len())
            .map_err(|_| Error::Command("invalid BLOB offset"))?;
        let mut cursor = self.storage.sql_cursor(&format!("UPDATE {table} SET item = CAST(substr(item, 1, ?1) || ?2 || substr(item, ?3) AS BLOB) WHERE rowid = ?4 RETURNING rowid"), &[V::Integer(start), V::Blob(bytes.to_vec()), V::Integer(end.checked_add(1).ok_or(Error::Command("BLOB offset overflow"))?), V::Integer(row_id)])?;
        // Cellar's billing counter includes trigger writes. Verify the actual
        // target row through RETURNING rather than requiring that counter = 1.
        if !matches!(cursor.next_raw()?.as_deref(), Some([V::Integer(written)]) if *written == row_id)
            || cursor.next_raw()?.is_some()
        {
            return Err(Error::Command("item BLOB update failed"));
        }
        cursor.close()?;
        Ok(())
    }
}

pub struct QueryContext<'borrow> {
    storage: &'borrow Storage,
    cell: CellId,
    sequence: u64,
    now_ms: i64,
}

impl<'a> QueryContext<'a> {
    pub(crate) fn new(
        storage: &'a Storage,
        target: &CellTarget,
        sequence: u64,
        now_ms: i64,
    ) -> Self {
        Self {
            storage,
            cell: target.cell_id(),
            sequence,
            now_ms,
        }
    }

    pub fn sql(&self, batch: &SqlBatch) -> Result<Vec<SqlResultSet>> {
        sql::execute(self.storage, batch, AccessMode::ReadOnly)
    }

    pub fn cell_id(&self) -> CellId {
        self.cell
    }
    pub fn commit_sequence(&self) -> u64 {
        self.sequence
    }
    pub fn now_ms(&self) -> i64 {
        self.now_ms
    }
    pub fn database_used_bytes(&self) -> Result<u64> {
        Ok(self.storage.sql_database_size()?)
    }
}

pub enum CommandResult<T> {
    Success(T),
    Rejected(T),
}

pub trait Command: Send + Sync + 'static {
    const MODULE: &'static str;
    const ID: u32;
    const CODEC_VERSION: u32;
    type Input: WireValue;
    type Output: WireValue;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>>;
}

pub trait Query: Send + Sync + 'static {
    const MODULE: &'static str;
    const ID: u32;
    const CODEC_VERSION: u32;
    type Input: WireValue;
    type Output: WireValue;
    fn execute(context: &mut QueryContext<'_>, input: Self::Input) -> Result<Self::Output>;
}

pub trait CellModule: Send + Sync + 'static {
    const NAME: &'static str;
    fn descriptor(&self) -> &'static ModuleDescriptor;
    fn register(self, registry: &mut RegistryBuilder) -> Result<()>;
}

pub(crate) struct EncodedResult {
    pub output: Vec<u8>,
    pub rejected: bool,
}

type CommandHandler = for<'a, 'b> fn(
    &mut CommandContext<'a, 'b>,
    &[u8],
    OperationDescriptor,
) -> Result<EncodedResult>;
type QueryHandler =
    for<'a> fn(&mut QueryContext<'a>, &[u8], OperationDescriptor) -> Result<Vec<u8>>;
type Key = (&'static str, u32, u32);

#[derive(Default)]
pub struct RegistryBuilder {
    modules: BTreeMap<&'static str, &'static ModuleDescriptor>,
    commands: BTreeMap<Key, CommandHandler>,
    queries: BTreeMap<Key, QueryHandler>,
    database_limits: BTreeMap<&'static str, u64>,
}

impl RegistryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<M: CellModule>(&mut self, module: M) -> Result<()> {
        let descriptor = module.descriptor();
        if descriptor.name != M::NAME || self.modules.contains_key(M::NAME) {
            return Err(Error::Registry("duplicate or mismatched module"));
        }
        if descriptor.name.is_empty()
            || descriptor.name.len() > 128
            || descriptor.namespaces.is_empty()
        {
            return Err(Error::Registry("invalid module identity"));
        }
        // The new deployment starts at schema 1. Do not silently pretend that
        // Cellule persisted state or retained executable code can be adopted.
        if descriptor.schema_min != 1
            || descriptor.schema_max != 1
            || descriptor.migrations.len() != 1
            || descriptor.migrations[0].version != 1
            || !descriptor.retained_codes.is_empty()
        {
            return Err(Error::Registry("unsupported schema migration contract"));
        }
        let migration = descriptor.migrations[0];
        if blake3::hash(migration.sql.as_bytes()).as_bytes() != migration.digest.as_bytes() {
            return Err(Error::Registry("migration SQL digest mismatch"));
        }
        self.modules.insert(M::NAME, descriptor);
        module.register(self)
    }

    pub fn bind_command<C: Command>(&mut self) -> Result<()> {
        let key = (C::MODULE, C::ID, C::CODEC_VERSION);
        if self.commands.contains_key(&key) {
            return Err(Error::Registry("duplicate command"));
        }
        self.operation(key, true)?;
        self.commands.insert(key, command::<C>);
        Ok(())
    }

    pub fn bind_query<Q: Query>(&mut self) -> Result<()> {
        let key = (Q::MODULE, Q::ID, Q::CODEC_VERSION);
        if self.queries.contains_key(&key) {
            return Err(Error::Registry("duplicate query"));
        }
        self.operation(key, false)?;
        self.queries.insert(key, query::<Q>);
        Ok(())
    }

    fn operation(&self, key: Key, command: bool) -> Result<OperationDescriptor> {
        let module = self
            .modules
            .get(key.0)
            .ok_or(Error::Registry("unregistered module"))?;
        find_operation(module, key.1, key.2, command)
    }

    pub fn finish(self) -> Result<Registry> {
        let mut namespaces = BTreeMap::new();
        for module in self.modules.values() {
            for namespace in module.namespaces {
                if namespace.name != module.name
                    || namespaces
                        .insert(*namespace.id.as_bytes(), *module)
                        .is_some()
                {
                    return Err(Error::Registry("duplicate or mismatched namespace"));
                }
            }
            for (operations, command) in [(module.commands, true), (module.queries, false)] {
                let mut seen = std::collections::BTreeSet::new();
                for operation in operations {
                    if !seen.insert((operation.id, operation.codec_version)) {
                        return Err(Error::Registry("duplicate declared operation"));
                    }
                    validate_operation(operation)?;
                    let key = (module.name, operation.id, operation.codec_version);
                    let bound = if command {
                        self.commands.contains_key(&key)
                    } else {
                        self.queries.contains_key(&key)
                    };
                    if !bound {
                        return Err(Error::Registry("operation has no compiled handler"));
                    }
                }
            }
        }
        Ok(Registry {
            modules: self.modules,
            namespaces,
            commands: self.commands,
            queries: self.queries,
            database_limits: self.database_limits,
        })
    }
}

pub struct Registry {
    pub(crate) modules: BTreeMap<&'static str, &'static ModuleDescriptor>,
    namespaces: BTreeMap<[u8; 16], &'static ModuleDescriptor>,
    commands: BTreeMap<Key, CommandHandler>,
    queries: BTreeMap<Key, QueryHandler>,
    database_limits: BTreeMap<&'static str, u64>,
}

impl Registry {
    pub fn set_database_limit(&mut self, module: &'static str, bytes: u64) -> Result<()> {
        if !self.modules.contains_key(module) || bytes < 16 * 1024 || bytes > i64::MAX as u64 {
            return Err(Error::Registry("invalid cell database limit"));
        }
        self.database_limits.insert(module, bytes);
        Ok(())
    }

    pub(crate) fn database_limit(&self, module: &'static str) -> u64 {
        self.database_limits
            .get(module)
            .copied()
            .unwrap_or(crate::capacity::DEFAULT_DATABASE_BYTES)
    }
    pub fn module_for(&self, namespace: NamespaceId) -> Result<&'static ModuleDescriptor> {
        self.namespaces
            .get(namespace.as_bytes())
            .copied()
            .ok_or(Error::Registry("unknown namespace"))
    }

    pub fn command_contract<C: Command>(
        &self,
        namespace: NamespaceId,
    ) -> Result<OperationDescriptor> {
        let module = self.module_for(namespace)?;
        if module.name != C::MODULE {
            return Err(Error::Registry("command targets another module"));
        }
        find_operation(module, C::ID, C::CODEC_VERSION, true)
    }

    pub fn query_contract<Q: Query>(&self, namespace: NamespaceId) -> Result<OperationDescriptor> {
        let module = self.module_for(namespace)?;
        if module.name != Q::MODULE {
            return Err(Error::Registry("query targets another module"));
        }
        find_operation(module, Q::ID, Q::CODEC_VERSION, false)
    }

    pub(crate) fn execute_command(
        &self,
        context: &mut CommandContext<'_, '_>,
        id: u32,
        version: u32,
        input: &[u8],
    ) -> Result<EncodedResult> {
        let module = self.module_for(context.target().namespace())?;
        let operation = find_operation(module, id, version, true)?;
        let handler = self
            .commands
            .get(&(module.name, id, version))
            .ok_or(Error::Registry("missing command handler"))?;
        handler(context, input, operation)
    }

    pub(crate) fn execute_query(
        &self,
        context: &mut QueryContext<'_>,
        namespace: NamespaceId,
        id: u32,
        version: u32,
        input: &[u8],
    ) -> Result<Vec<u8>> {
        let module = self.module_for(namespace)?;
        let operation = find_operation(module, id, version, false)?;
        let handler = self
            .queries
            .get(&(module.name, id, version))
            .ok_or(Error::Registry("missing query handler"))?;
        handler(context, input, operation)
    }
}

fn validate_operation(operation: &OperationDescriptor) -> Result<()> {
    if operation.id == 0
        || operation.codec_version == 0
        || operation.schema_min != 1
        || operation.schema_max != 1
    {
        return Err(Error::Registry("invalid operation contract"));
    }
    BoundedEncoder::new(operation.input_limit)?;
    BoundedEncoder::new(operation.output_limit)?;
    Ok(())
}

fn find_operation(
    module: &ModuleDescriptor,
    id: u32,
    version: u32,
    command: bool,
) -> Result<OperationDescriptor> {
    let operations = if command {
        module.commands
    } else {
        module.queries
    };
    let operation = operations
        .iter()
        .find(|op| op.id == id && op.codec_version == version)
        .ok_or(Error::Registry("undeclared operation or codec"))?;
    validate_operation(operation)?;
    Ok(*operation)
}

fn decode<T: WireValue>(bytes: &[u8], limit: u32) -> Result<T> {
    let mut decoder = BoundedDecoder::new(bytes, limit)?;
    let value = T::decode(&mut decoder)?;
    decoder.finish()?;
    Ok(value)
}

fn encode<T: WireValue>(value: T, limit: u32) -> Result<Vec<u8>> {
    let mut encoder = BoundedEncoder::new(limit)?;
    value.encode(&mut encoder)?;
    Ok(encoder.finish())
}

fn command<C: Command>(
    context: &mut CommandContext<'_, '_>,
    bytes: &[u8],
    descriptor: OperationDescriptor,
) -> Result<EncodedResult> {
    let (value, rejected) = match C::execute(context, decode(bytes, descriptor.input_limit)?)? {
        CommandResult::Success(value) => (value, false),
        CommandResult::Rejected(value) => (value, true),
    };
    Ok(EncodedResult {
        output: encode(value, descriptor.output_limit)?,
        rejected,
    })
}

fn query<Q: Query>(
    context: &mut QueryContext<'_>,
    bytes: &[u8],
    descriptor: OperationDescriptor,
) -> Result<Vec<u8>> {
    encode(
        Q::execute(context, decode(bytes, descriptor.input_limit)?)?,
        descriptor.output_limit,
    )
}
