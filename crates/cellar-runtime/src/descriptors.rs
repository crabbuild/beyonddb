// Adapted from crabbuild/cellule at 1d0648b3b5ae0cca040c614b505b4d72bb769ab1.
// SPDX-License-Identifier: Apache-2.0
//! Descriptors for BeyondDB's compiled domain commands.
use crate::identity::{Digest, NamespaceId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogRole {
    Sql,
}

pub type RegistryError = crate::Error;

/// Build evidence included in the canonical release descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildDescriptor {
    /// Source revision the image was built from.
    pub source_revision: String,
    /// Digest of the Cargo lockfile the image was built with.
    pub cargo_lock_digest: Digest,
}

/// One exact application migration and its independently verified digest.
#[derive(Clone, Copy, Debug)]
pub struct MigrationDescriptor {
    /// Schema version this migration installs.
    pub version: u32,
    /// Migration SQL, digest-bound to `digest`.
    pub sql: &'static str,
    /// BLAKE3 digest of `sql`.
    pub digest: Digest,
}

/// One predecessor module code intentionally retained by the current binary.
#[derive(Clone, Copy, Debug)]
pub struct RetainedCodeDescriptor {
    /// Code digest the module retains.
    pub code: Digest,
    /// Oldest schema version the retained code serves.
    pub schema_min: u32,
    /// Newest schema version the retained code serves.
    pub schema_max: u32,
}

/// One registered command or query codec and its schema compatibility range.
#[derive(Clone, Copy, Debug)]
pub struct OperationDescriptor {
    /// Command or query id within its module.
    pub id: u32,
    /// Input codec version the operation accepts.
    pub codec_version: u32,
    /// Oldest schema version the operation serves.
    pub schema_min: u32,
    /// Newest schema version the operation serves.
    pub schema_max: u32,
    /// Largest input the operation accepts, in bytes.
    pub input_limit: u32,
    /// Largest output the operation returns, in bytes.
    pub output_limit: u32,
}

/// One stable Cell namespace compiled into a module.
#[derive(Clone, Copy, Debug)]
pub struct NamespaceDescriptor {
    /// Namespace identity.
    pub id: NamespaceId,
    /// Stable namespace name.
    pub name: &'static str,
    /// Catalog role the namespace serves.
    pub role: CatalogRole,
    /// Power-of-two shard count.
    pub shards: u32,
    /// Namespaces this one may emit effects to.
    pub effect_targets: &'static [NamespaceId],
    /// Queue namespace that receives this namespace's dead letters.
    pub dead_letter: Option<NamespaceId>,
}

/// Static module inventory that must match its compiled function bindings.
#[derive(Clone, Copy, Debug)]
pub struct ModuleDescriptor {
    /// Module name, matched against the compiled function bindings.
    pub name: &'static str,
    /// Digest of the module's source.
    pub source_digest: Digest,
    /// Code digests the module retains for older schemas.
    pub retained_codes: &'static [RetainedCodeDescriptor],
    /// Oldest schema version the module serves.
    pub schema_min: u32,
    /// Newest schema version the module serves.
    pub schema_max: u32,
    /// Contiguous migrations covering `schema_min` through `schema_max`.
    pub migrations: &'static [MigrationDescriptor],
    /// Commands the module declares.
    pub commands: &'static [OperationDescriptor],
    /// Queries the module declares.
    pub queries: &'static [OperationDescriptor],
    /// Workflow definition digests the module serves.
    pub workflow_definitions: &'static [Digest],
    /// Activity types the module registers.
    pub activity_types: &'static [&'static str],
    /// Namespaces the module owns.
    pub namespaces: &'static [NamespaceDescriptor],
}
