// Cell topology declarations adapted from crabbuild/cellule at
// 1d0648b3b5ae0cca040c614b505b4d72bb769ab1. SPDX-License-Identifier: Apache-2.0
//! BeyondDB's compiled application inventory over the Cellar adapter.
use crate::{
    Error, Result,
    descriptors::{BuildDescriptor, CatalogRole},
    identity::{NamespaceId, partition_for_shard},
    registry::{CellModule, Registry, RegistryBuilder},
};
use std::sync::Arc;
const MAX_PARTITION_VERSION: u32 = 3;
const ENTITY_PARTITION_VERSION: u32 = 2;
const UUID_PARTITION_VERSION: u32 = 3;
const ENTITY_PARTITION_PREFIX: u8 = 1;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellType {
    module: &'static str,
    name: &'static str,
    namespace: NamespaceId,
    role: CatalogRole,
    shards: u32,
    partition_version: u32,
    schema_min: u32,
    schema_max: u32,
    database_limit_bytes: u64,
    capture_limit_bytes: u64,
}

impl CellType {
    /// Creates a bounded topology declaration with conservative version one defaults.
    pub fn new(
        module: &'static str,
        name: &'static str,
        namespace: NamespaceId,
        role: CatalogRole,
        shards: u32,
    ) -> Result<Self> {
        let cell_type = Self {
            module,
            name,
            namespace,
            role,
            shards,
            partition_version: 1,
            schema_min: 1,
            schema_max: 1,
            database_limit_bytes: 64 * 1024 * 1024,
            capture_limit_bytes: 16 * 1024 * 1024,
        };
        cell_type.validate()?;
        Ok(cell_type)
    }

    /// Replaces the declared application limits without changing stable identity.
    ///
    /// Database limits must be at least 16 KiB and capture limits at least 128 bytes.
    pub fn with_limits(
        mut self,
        database_limit_bytes: u64,
        capture_limit_bytes: u64,
    ) -> Result<Self> {
        self.database_limit_bytes = database_limit_bytes;
        self.capture_limit_bytes = capture_limit_bytes;
        self.validate()?;
        Ok(self)
    }

    /// Selects stable entity partitions instead of fixed shards.
    ///
    /// The namespace descriptor must declare one shard. The application
    /// derives one target per entity with [`Self::entity_partition`].
    pub fn with_entity_partitions(mut self) -> Result<Self> {
        self.partition_version = ENTITY_PARTITION_VERSION;
        self.validate()?;
        Ok(self)
    }

    /// Declares one SQL Cell per canonical 16-byte UUID partition.
    ///
    /// UUID partitions use a distinct descriptor version from the 33-byte
    /// entity digest mode and retain the UUID bytes as the Cell partition.
    pub fn entity_uuid(
        module: &'static str,
        name: &'static str,
        namespace: NamespaceId,
    ) -> Result<Self> {
        let mut cell_type = Self::new(module, name, namespace, CatalogRole::Sql, 1)?;
        cell_type.partition_version = UUID_PARTITION_VERSION;
        cell_type.validate()?;
        Ok(cell_type)
    }

    /// Replaces the schema range while retaining the stable Cell identity.
    pub fn with_schema_range(mut self, schema_min: u32, schema_max: u32) -> Result<Self> {
        self.schema_min = schema_min;
        self.schema_max = schema_max;
        self.validate()?;
        Ok(self)
    }

    /// Returns the stable module owner.
    #[must_use]
    pub const fn module(&self) -> &'static str {
        self.module
    }

    /// Returns the diagnostic Cell type name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the stable namespace identity.
    #[must_use]
    pub const fn namespace(&self) -> NamespaceId {
        self.namespace
    }

    /// Returns the catalog role pinned for this type.
    #[must_use]
    pub const fn role(&self) -> CatalogRole {
        self.role
    }

    /// Returns the fixed shard count, or one for an entity Cell type.
    #[must_use]
    pub const fn shards(&self) -> u32 {
        self.shards
    }

    /// Returns the database ceiling declared for each Cell of this type.
    #[must_use]
    pub const fn database_limit_bytes(&self) -> u64 {
        self.database_limit_bytes
    }

    /// Returns the capture ceiling declared for each Cell of this type.
    #[must_use]
    pub const fn capture_limit_bytes(&self) -> u64 {
        self.capture_limit_bytes
    }

    /// Maps one bounded application scope to its stable fixed shard number.
    pub fn shard_for_scope(&self, scope: &[u8]) -> Result<u32> {
        if self.partition_version != 1 {
            return Err(Error::Identity("entity Cell type has no fixed shard"));
        }
        crate::identity::shard_for_scope(self.namespace, scope, self.shards)
    }

    /// Returns the canonical fixed-shard partition for one application scope.
    pub fn partition_for_scope(&self, scope: &[u8]) -> Result<[u8; 4]> {
        Ok(crate::identity::partition_for_shard(
            self.shard_for_scope(scope)?,
        ))
    }

    /// Derives the canonical partition bytes for one entity identity.
    ///
    /// The scope must be nonempty and at most 1,024 bytes. Its digest, not the
    /// raw identity, enters the catalog partition key.
    pub fn entity_partition(&self, scope: &[u8]) -> Result<[u8; 33]> {
        if self.partition_version != ENTITY_PARTITION_VERSION {
            return Err(Error::Identity(
                "Cell type does not use hashed entity partitions",
            ));
        }
        if scope.is_empty() || scope.len() > 1_024 {
            return Err(Error::Identity("invalid entity scope length"));
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"crab.entity.partition.v1\0");
        hasher.update(self.namespace.as_bytes());
        hasher.update(&(scope.len() as u32).to_be_bytes());
        hasher.update(scope);
        let mut partition = [0_u8; 33];
        partition[0] = ENTITY_PARTITION_PREFIX;
        partition[1..].copy_from_slice(hasher.finalize().as_bytes());
        Ok(partition)
    }

    pub fn valid_partition(&self, partition: &[u8]) -> bool {
        if self.partition_version == ENTITY_PARTITION_VERSION {
            return partition.len() == 33 && partition[0] == ENTITY_PARTITION_PREFIX;
        }
        if self.partition_version == UUID_PARTITION_VERSION {
            return canonical_uuid_partition(partition);
        }
        let Ok(shard) = <[u8; 4]>::try_from(partition) else {
            return false;
        };
        let shard = u32::from_be_bytes(shard);
        shard < self.shards && partition_for_shard(shard).as_slice() == partition
    }

    fn validate(&self) -> Result<()> {
        if !valid_name(self.module)
            || !valid_name(self.name)
            || self.namespace.as_bytes().iter().all(|byte| *byte == 0)
            || !(1..=4096).contains(&self.shards)
            || !self.shards.is_power_of_two()
            || self.partition_version == 0
            || self.partition_version > MAX_PARTITION_VERSION
            || (self.partition_version == ENTITY_PARTITION_VERSION && self.shards != 1)
            || (self.partition_version == UUID_PARTITION_VERSION
                && (self.role != CatalogRole::Sql || self.shards != 1))
            || self.schema_min == 0
            || self.schema_min > self.schema_max
            || self.database_limit_bytes < 16 * 1024
            || self.capture_limit_bytes < 128
        {
            return Err(Error::Registry("invalid Cell type declaration"));
        }
        Ok(())
    }
}

fn canonical_uuid_partition(partition: &[u8]) -> bool {
    partition.len() == 16 && (1..=8).contains(&(partition[6] >> 4)) && partition[8] >> 6 == 2
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

pub struct ApplicationBuilder {
    name: &'static str,
    build: BuildDescriptor,
    registry: RegistryBuilder,
    cells: Vec<CellType>,
}
impl ApplicationBuilder {
    pub fn new(name: &'static str, build: BuildDescriptor) -> Result<Self> {
        if !valid_name(name) || build.source_revision.is_empty() {
            return Err(Error::Registry("invalid application build identity"));
        }
        Ok(Self {
            name,
            build,
            registry: RegistryBuilder::new(),
            cells: Vec::new(),
        })
    }
    pub fn register<M: CellModule>(&mut self, module: M) -> Result<()> {
        self.registry.register(module)
    }
    pub fn cell_type(&mut self, cell: CellType) -> Result<()> {
        cell.validate()?;
        if self
            .cells
            .iter()
            .any(|old| old.namespace == cell.namespace || old.name == cell.name)
        {
            return Err(Error::Registry("duplicate cell topology declaration"));
        }
        self.cells.push(cell);
        Ok(())
    }
    pub fn finish(self) -> Result<CompiledApplication> {
        let mut registry = self.registry.finish()?;
        let expected: usize = registry.modules.values().map(|m| m.namespaces.len()).sum();
        if expected != self.cells.len() {
            return Err(Error::Registry(
                "cell inventory does not cover registered namespaces",
            ));
        }
        for cell in &self.cells {
            let module = registry.module_for(cell.namespace)?;
            let descriptor = module
                .namespaces
                .iter()
                .find(|n| n.id == cell.namespace)
                .ok_or(Error::Registry("undeclared cell namespace"))?;
            if module.name != cell.module
                || descriptor.role != cell.role
                || descriptor.shards != cell.shards
                || cell.schema_min != module.schema_min
                || cell.schema_max != module.schema_max
            {
                return Err(Error::Registry(
                    "cell topology differs from module declaration",
                ));
            }
            registry.set_database_limit(cell.module, cell.database_limit_bytes)?;
        }
        Ok(CompiledApplication {
            name: self.name,
            build: self.build,
            registry: Arc::new(registry),
            cells: self.cells,
        })
    }
}

pub struct CompiledApplication {
    name: &'static str,
    build: BuildDescriptor,
    registry: Arc<Registry>,
    cells: Vec<CellType>,
}
impl CompiledApplication {
    pub fn name(&self) -> &'static str {
        self.name
    }
    pub fn build(&self) -> &BuildDescriptor {
        &self.build
    }
    pub fn registry(&self) -> Arc<Registry> {
        self.registry.clone()
    }
    pub fn cell_types(&self) -> &[CellType] {
        &self.cells
    }
}

pub trait CellApplication: Send + Sync + 'static {
    const NAME: &'static str;
    fn register(builder: &mut ApplicationBuilder) -> Result<()>;
    fn compile(build: BuildDescriptor) -> Result<CompiledApplication> {
        let mut builder = ApplicationBuilder::new(Self::NAME, build)?;
        Self::register(&mut builder)?;
        builder.finish()
    }
}
