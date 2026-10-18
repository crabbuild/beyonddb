//! Stream retention orchestration over local owners and tenant catalogs.

use bdb_cellar_runtime::identity::CellTarget;
use extenddb_storage::error::StorageError;

use super::{CellStorage, cell_error, mutation_identity, target};
use crate::{
    HasExpiredAccountStreamRecords, HasExpiredPartitionStreamRecords, Json,
    PruneAccountStreamRecords, PrunePartitionStreamRecords,
};

impl CellStorage {
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
