//! Stream retention orchestration over local owners and tenant catalogs.

use cellule_runtime::CellRuntime;
use cellule_runtime::cell::catalog::{CatalogRole, CellCatalog};
use cellule_runtime::client::InvocationError;
use cellule_runtime::identity::CellTarget;
use cellule_runtime::ltx::CellStorageLayout;
use extenddb_storage::error::StorageError;
use futures_util::{StreamExt, stream};

use super::{CellStorage, cell_error, mutation_identity, target};
use crate::{
    APPLICATION, AdvanceStreamGcShard, DATA_NAMESPACE, HasExpiredAccountStreamRecords,
    HasExpiredPartitionStreamRecords, Json, PruneAccountStreamRecords, PrunePartitionStreamRecords,
    ReadStreamGcShard,
};

impl CellStorage {
    /// Sweep one durable catalog shard, including idle and deleted table data Cells.
    pub async fn sweep_account_catalog_stream_records(
        &self,
        account_id: &str,
        layout: &CellStorageLayout,
    ) -> Result<u64, StorageError> {
        let account = target(account_id)?;
        let shard = self
            .client
            .query::<ReadStreamGcShard>(&account, None, Json(()))
            .await
            .map_err(cell_error)?
            .output
            .0;
        let catalog = CellCatalog::new(layout.clone(), account.tenant());
        let mut scan = catalog
            .scan_shard(shard)
            .await
            .map_err(|error| StorageError::Transient(error.to_string()))?;
        let mut total = 0_u64;
        let mut first_error = None;
        while let Some(page) = scan
            .next_page()
            .await
            .map_err(|error| StorageError::Transient(error.to_string()))?
        {
            let targets = page
                .entries()
                .iter()
                .filter(|proof| proof.entry().namespace() == DATA_NAMESPACE)
                .map(|proof| {
                    let entry = proof.entry();
                    if entry.role() != CatalogRole::Sql {
                        return Err(StorageError::Internal(
                            "data Cell catalog role differs".into(),
                        ));
                    }
                    let target = CellTarget::new(
                        account.tenant(),
                        APPLICATION,
                        DATA_NAMESPACE,
                        entry.partition(),
                    )
                    .map_err(|error| StorageError::Internal(error.to_string()))?;
                    if target.cell_id() != entry.cell() {
                        return Err(StorageError::Internal(
                            "data Cell catalog identity differs".into(),
                        ));
                    }
                    Ok(target)
                })
                .collect::<Result<Vec<_>, StorageError>>()?;
            let mut sweeps = stream::iter(targets)
                .map(|owner| async move {
                    let cell = owner.cell_id();
                    (cell, self.sweep_partition_stream_records(&owner).await)
                })
                .buffer_unordered(8);
            while let Some((cell, result)) = sweeps.next().await {
                match result {
                    Ok(deleted) => total = total.saturating_add(deleted),
                    Err(error) => {
                        tracing::warn!(cell = ?cell, %error, "stream catalog Cell sweep failed");
                        first_error.get_or_insert(error);
                    }
                }
            }
        }
        // A failed Cell is retried on the next catalog cycle. Advancing after
        // a complete scan prevents one unavailable owner from starving others.
        match self
            .client
            .command::<AdvanceStreamGcShard>(&account, mutation_identity()?, Json(shard))
            .await
        {
            Ok(result) if result.output.0 => {}
            Ok(_) | Err(InvocationError::Rejected(_)) => {
                return Err(StorageError::Transient("stream GC cursor changed".into()));
            }
            Err(error) => return Err(cell_error(error)),
        }
        first_error.map_or(Ok(total), Err)
    }

    /// Sweep active routed owner Cells on this node with bounded concurrency.
    pub async fn sweep_active_partition_stream_records(
        &self,
        runtime: &CellRuntime,
    ) -> Result<u64, StorageError> {
        let targets = runtime
            .active_cell_targets()
            .await
            .map_err(|error| StorageError::Transient(error.to_string()))?;
        let mut sweeps = stream::iter(
            targets
                .into_iter()
                .filter(|target| target.namespace() == DATA_NAMESPACE),
        )
        .map(|target| async move { self.sweep_partition_stream_records(&target).await })
        .buffer_unordered(8);
        let mut total = 0_u64;
        let mut first_error = None;
        while let Some(result) = sweeps.next().await {
            match result {
                Ok(deleted) => total = total.saturating_add(deleted),
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        first_error.map_or(Ok(total), Err)
    }

    /// Reclaim expired history in one routed data Cell.
    pub async fn sweep_partition_stream_records(
        &self,
        owner: &CellTarget,
    ) -> Result<u64, StorageError> {
        let expired = self
            .client
            .query::<HasExpiredPartitionStreamRecords>(owner, None, Json(()))
            .await
            .map_err(cell_error)?
            .output
            .0;
        if !expired {
            return Ok(0);
        }
        let mut total = 0;
        for _ in 0..16 {
            let deleted = self
                .client
                .command::<PrunePartitionStreamRecords>(owner, mutation_identity()?, Json(()))
                .await
                .map_err(cell_error)?
                .output
                .0;
            total += deleted;
            if deleted < crate::stream_retention::PRUNE_BATCH as u64 {
                break;
            }
        }
        Ok(total)
    }

    /// Reclaim bounded expired account-local stream history, including deleted generations.
    pub async fn sweep_account_stream_records(
        &self,
        account_id: &str,
    ) -> Result<u64, StorageError> {
        let account = target(account_id)?;
        let mut total = 0;
        let expired = self
            .client
            .query::<HasExpiredAccountStreamRecords>(&account, None, Json(()))
            .await
            .map_err(cell_error)?
            .output
            .0;
        if !expired {
            return Ok(0);
        }
        for _ in 0..16 {
            let deleted = self
                .client
                .command::<PruneAccountStreamRecords>(&account, mutation_identity()?, Json(()))
                .await
                .map_err(cell_error)?
                .output
                .0;
            total += deleted;
            if deleted < crate::stream_retention::PRUNE_BATCH as u64 {
                break;
            }
        }
        Ok(total)
    }
}
