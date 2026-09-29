//! Serializable transactional reads from immutable participant snapshots.

use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use futures_util::{StreamExt, stream};

use super::{CellStorage, cell_error, mutation_identity};
use crate::{
    BeginReadResultRelease, CoordinatorDecision, CoordinatorParticipantTarget, GetItemInput, Json,
    ReadAccountTransactionResult, ReadCoordinatorParticipantInput, ReadCrossCellTransaction,
    ReadPartitionTransactionResult, ReadTransactionInput, ReadTransactionResultInput,
    TransactionFailure, TransactionOperation, TransactionReadResult, account_target,
    coordinator_target, data_target,
};

#[derive(Clone)]
enum ReadTarget {
    Account(cellule_runtime::identity::CellTarget),
    Data(cellule_runtime::identity::CellTarget),
}

impl CellStorage {
    pub(super) async fn transaction_read(
        &self,
        account_id: &str,
        inputs: Vec<GetItemInput>,
        routing: Vec<(TableKeyInfo, Item)>,
    ) -> Result<Vec<Option<Item>>, StorageError> {
        let count = inputs.len();
        let operations = inputs.into_iter().map(TransactionOperation::Read).collect();
        let admitted = self
            .admit_transaction(account_id, None, operations, routing)
            .await?;
        match admitted.decision {
            CoordinatorDecision::Commit => {}
            CoordinatorDecision::Abort { index, reason } => {
                return Err(super::data::transaction_canceled(
                    usize::from(index.unwrap_or(0)),
                    reason.unwrap_or(TransactionFailure::Conflict),
                    count,
                    &[],
                ));
            }
            CoordinatorDecision::Begin => {
                return Err(StorageError::Transient(
                    "transactional read is undecided".into(),
                ));
            }
        }
        let identity = admitted.identity;
        let coordinator = coordinator_target(account_id, &identity.routing_key)
            .map_err(|error| StorageError::Internal(error.to_string()))?;
        let status = self
            .client
            .query::<ReadCrossCellTransaction>(&coordinator, None, Json(identity.clone()))
            .await
            .map_err(cell_error)?
            .output
            .0
            .ok_or_else(|| StorageError::Internal("read transaction disappeared".into()))?;
        let participant_inputs =
            (0..status.participant_count).map(|position| ReadCoordinatorParticipantInput {
                account_id: account_id.into(),
                transaction_id: identity.transaction_id,
                routing_key: identity.routing_key.clone(),
                position,
                chunk: 0,
            });
        let participant_results = stream::iter(
            participant_inputs
                .map(|input| async { self.coordinator_participant(&coordinator, input).await }),
        )
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await;
        let mut image_reads = Vec::new();
        for result in participant_results {
            let participant = result?.ok_or_else(|| {
                StorageError::Internal("committed read operations are missing".into())
            })?;
            let target = match &participant.target {
                CoordinatorParticipantTarget::Account => {
                    account_target(account_id).map(ReadTarget::Account)
                }
                CoordinatorParticipantTarget::Data {
                    table_id,
                    partition_id,
                    ..
                } => data_target(account_id, table_id, partition_id).map(ReadTarget::Data),
            }
            .map_err(|error| StorageError::Internal(error.to_string()))?;
            for (position, operation) in participant.operations.into_iter().enumerate() {
                if !matches!(operation.operation, TransactionOperation::Read(_)) {
                    return Err(StorageError::Internal(
                        "read transaction contains a write".into(),
                    ));
                }
                image_reads.push((
                    operation.index,
                    target.clone(),
                    Json(ReadTransactionResultInput {
                        transaction: ReadTransactionInput {
                            transaction_id: identity.transaction_id,
                            coordinator_cell: *coordinator.cell_id().as_bytes(),
                        },
                        position: u8::try_from(position)
                            .map_err(|_| StorageError::Internal("invalid read position".into()))?,
                    }),
                ));
            }
        }
        let images = stream::iter(image_reads.into_iter().map(
            |(index, target, input)| async move {
                let image = match target {
                    ReadTarget::Account(target) => {
                        self.client
                            .query::<ReadAccountTransactionResult>(&target, None, input)
                            .await
                    }
                    ReadTarget::Data(target) => {
                        self.client
                            .query::<ReadPartitionTransactionResult>(&target, None, input)
                            .await
                    }
                }
                .map_err(cell_error)?
                .output
                .0;
                let TransactionReadResult::Item(image) = image else {
                    return Err(StorageError::Internal(
                        "committed read image is missing".into(),
                    ));
                };
                Ok((index, image))
            },
        ))
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await;
        let mut items = vec![None; count];
        for result in images {
            let (index, image) = result?;
            let slot = items
                .get_mut(usize::from(index))
                .ok_or_else(|| StorageError::Internal("invalid transaction read index".into()))?;
            if slot.replace(image).is_some() {
                return Err(StorageError::Internal(
                    "duplicate transaction read index".into(),
                ));
            }
        }
        for item in &items {
            if item.is_none() {
                return Err(StorageError::Internal(
                    "transaction read result is incomplete".into(),
                ));
            }
        }
        let items = items
            .into_iter()
            .map(|item| {
                item.ok_or_else(|| {
                    StorageError::Internal("transaction read result is incomplete".into())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Only the initiating reader can acknowledge fully assembled images.
        // Persist cleanup intent before deleting anything so cancellation or an
        // unavailable participant leaves discoverable work for recovery.
        let acknowledged = self
            .client
            .command::<BeginReadResultRelease>(
                &coordinator,
                mutation_identity()?,
                Json(identity.clone()),
            )
            .await
            .map_err(cell_error)?;
        if !acknowledged.output.0 {
            return Err(StorageError::Internal(
                "coordinator rejected read result acknowledgement".into(),
            ));
        }
        if let Err(error) = self
            .finish_decided_cross_cell_transaction(
                account_id,
                &identity.routing_key,
                identity.transaction_id,
            )
            .await
        {
            tracing::warn!(%error, "transaction read result cleanup remains pending");
        }
        validate_read_size(items)
    }
}

pub(super) fn validate_read_size(
    items: Vec<Option<Item>>,
) -> Result<Vec<Option<Item>>, StorageError> {
    let size: usize = items
        .iter()
        .flatten()
        .map(extenddb_core::types::item_size_bytes)
        .sum();
    if size > 4 * 1024 * 1024 {
        return Err(StorageError::Validation(
            "transaction read exceeds 4 MiB".into(),
        ));
    }
    Ok(items)
}
