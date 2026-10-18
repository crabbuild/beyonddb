//! Remaining legacy physical SQLite FULL contract. Migrated behavior executes
//! through the focused native Cellar targets, including cellar_serving.
include!("support/transaction_transport.rs");
use beyonddb::*;
use cellule_app::CellApplication;
use cellule_ltx::{CellReplica, rusqlite};
use cellule_runtime::cell::actor::{CellHandle, CellRuntime};
use cellule_runtime::cell::catalog::{CatalogEntry, CatalogRole, CellCatalog};
use cellule_runtime::client::{CellClient, InvocationError};
use cellule_runtime::control::{Owner, authority::CellAuthority};
use cellule_runtime::identity::{CellTarget, Digest, IncarnationId, RequestId, SessionId};
use cellule_runtime::ltx::{CellStorageLayout, DiskBudget, Host, Limits};
use cellule_runtime::registry::{BuildDescriptor, Registry};
use cellule_runtime::{MutationIdentity, SqlWorkerPool};
use cellule_store::Store;
use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, Item, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use extenddb_storage::{DataEngine, TableEngine, error::StorageError};
use object_store::memory::InMemory;
use std::{sync::Arc, time::UNIX_EPOCH};

fn identity(byte: u8) -> MutationIdentity {
    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    MutationIdentity {
        request_id: RequestId::from_bytes([byte; 16]),
        issued_at_ms: now_ms,
        expires_at_ms: now_ms + 60_000,
    }
}

struct Bootstrap<'a> {
    runtime: CellRuntime,
    registry: &'a Arc<Registry>,
    layout: &'a CellStorageLayout,
    session: SessionId,
}

impl Bootstrap<'_> {
    async fn cell(
        &self,
        target: &CellTarget,
        module: &'static str,
        byte: u8,
        file: &std::path::Path,
        initialize: for<'a> fn(&rusqlite::Transaction<'a>) -> cellule_runtime::Result<()>,
    ) -> CellHandle {
        self.cell_with_storage(target, module, byte, (file, Limits::default()), initialize)
            .await
    }

    async fn directory(
        &self,
        route: TableRoute,
        byte: u8,
        file: &std::path::Path,
    ) -> (CellHandle, beyonddb::TableRoutePublication) {
        let spec = beyonddb::DirectorySpec::root(route.table_id.clone());
        let target = beyonddb::directory_target("123456789012", &spec).unwrap();
        let handle = self
            .cell_with_storage(
                &target,
                "beyonddb-directory",
                byte,
                (
                    file,
                    Limits {
                        max_database_bytes: 16 * 1024 * 1024,
                        max_capture_bytes: 4 * 1024 * 1024,
                        ..Limits::default()
                    },
                ),
                beyonddb::initialize_directory,
            )
            .await;
        let client = CellClient::local(self.registry.clone(), handle.clone());
        let installed = client
            .command::<beyonddb::InstallDirectory>(
                &target,
                identity(byte),
                Json(beyonddb::DirectoryInstall {
                    spec,
                    source: None,
                    ranges: route
                        .partitions
                        .iter()
                        .map(|part| beyonddb::RoutePagePartition {
                            partition_id: part.partition_id,
                            lower: part.lower.unwrap_or([0; 16]),
                            upper: part.upper,
                            epoch: part.epoch,
                        })
                        .collect(),
                }),
            )
            .await
            .unwrap();
        let state = client
            .query::<beyonddb::ReadDirectory>(&target, Some(installed.receipt), Json(()))
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        let receipt = beyonddb::DirectoryCopyReceipt {
            cell_id: *target.cell_id().as_bytes(),
            sequence: installed.receipt.commit_sequence,
            fingerprint: state.initial_fingerprint,
        };
        (handle, beyonddb::TableRoutePublication { route, receipt })
    }

    async fn cell_with_storage(
        &self,
        target: &CellTarget,
        module: &'static str,
        byte: u8,
        (file, limits): (&std::path::Path, Limits),
        initialize: for<'a> fn(&rusqlite::Transaction<'a>) -> cellule_runtime::Result<()>,
    ) -> CellHandle {
        let proof = CellCatalog::new(self.layout.clone(), target.tenant())
            .provision(
                CatalogEntry::new(
                    target,
                    CatalogRole::Sql,
                    self.registry.module_code(module).unwrap(),
                    1,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let authority = CellAuthority::new(self.layout.clone());
        let incarnation = IncarnationId::from_bytes([byte; 16]);
        let control = authority
            .create_initial(
                &proof,
                incarnation,
                Owner {
                    session: self.session,
                    endpoint: "https://beyonddb-partition.internal:8081".into(),
                },
            )
            .await
            .unwrap();
        self.runtime
            .bootstrap(
                proof,
                CellReplica::new(
                    self.layout.clone(),
                    *target.cell_id().as_bytes(),
                    *incarnation.as_bytes(),
                    limits,
                )
                .unwrap(),
                authority,
                control,
                file.to_path_buf(),
                initialize,
            )
            .await
            .unwrap()
    }
}

// Keep the distinct physical SQLite FULL case until it has a native equivalent.
#[path = "elastic_cells/transaction_capacity.rs"]
mod transaction_capacity;
