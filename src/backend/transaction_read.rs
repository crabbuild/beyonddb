//! Serializable transactional reads from immutable participant snapshots.

use cellule_runtime::client::InvocationError;
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use futures_util::{StreamExt, stream};
use std::collections::HashMap;
use std::time::Duration;

use super::{CellStorage, cell_error, mutation_identity};
use crate::{
    BeginReadResultRelease, CoordinatorDecision, CoordinatorParticipantTarget,
    CoordinatorPhaseOutcome, GetItemInput, Json, ReadAccountTransactionResult,
    ReadCoordinatorParticipantInput, ReadPartitionTransactionResult, ReadResultRelease,
    ReadTransactionInput, ReadTransactionResultInput, RecordReadResultReleases,
    RecordReadResultReleasesInput, ReleaseAccountTransactionReads,
    ReleasePartitionTransactionReads, TransactionFailure, TransactionOperation,
    TransactionReadResult, account_target, coordinator_target, data_target,
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
        let participant_results = if let Some(participants) =
            admitted.acknowledged_read_participants
        {
            if participants.len() != usize::from(admitted.participant_count) {
                return Err(StorageError::Internal(
                    "acknowledged read participant count differs".into(),
                ));
            }
            // The acknowledged fresh BEGIN fixes this exact ordering. Reuse
            // only routing/operation metadata; images still come from durable
            // participant snapshots, never from live items or current routes.
            participants
                .into_iter()
                .enumerate()
                .map(|(position, participant)| {
                    u8::try_from(position)
                        .map(|position| (position, Ok(Some(participant))))
                        .map_err(|_| {
                            StorageError::Internal("invalid read participant position".into())
                        })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            let participant_inputs =
                (0..admitted.participant_count).map(|position| ReadCoordinatorParticipantInput {
                    account_id: account_id.into(),
                    transaction_id: identity.transaction_id,
                    routing_key: identity.routing_key.clone(),
                    position,
                    chunk: 0,
                });
            stream::iter(participant_inputs.map(|input| async {
                let position = input.position;
                (
                    position,
                    self.coordinator_participant(&coordinator, input).await,
                )
            }))
            .buffer_unordered(8)
            .collect::<Vec<_>>()
            .await
        };
        let mut image_reads: HashMap<[u8; 32], Vec<_>> = HashMap::new();
        let mut participant_targets = Vec::with_capacity(participant_results.len());
        for (participant_position, result) in participant_results {
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
            participant_targets.push((participant_position, target.clone()));
            for (position, operation) in participant.operations.into_iter().enumerate() {
                if !matches!(operation.operation, TransactionOperation::Read(_)) {
                    return Err(StorageError::Internal(
                        "read transaction contains a write".into(),
                    ));
                }
                let cell = match &target {
                    ReadTarget::Account(target) | ReadTarget::Data(target) => {
                        *target.cell_id().as_bytes()
                    }
                };
                image_reads.entry(cell).or_default().push((
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
        // One image query reserves up to the Cell wire result ceiling. Fetch
        // each participant's images serially to stay inside its 16 MiB mailbox,
        // while independent participant Cells can still make progress together.
        let images = stream::iter(image_reads.into_values().map(|reads| async move {
            let mut group = Vec::with_capacity(reads.len());
            for (index, target, input) in reads {
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
                group.push((index, image));
            }
            Ok::<_, StorageError>(group)
        }))
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await;
        let mut items = vec![None; count];
        for group in images {
            for (index, image) in group? {
                let slot = items.get_mut(usize::from(index)).ok_or_else(|| {
                    StorageError::Internal("invalid transaction read index".into())
                })?;
                if slot.replace(image).is_some() {
                    return Err(StorageError::Internal(
                        "duplicate transaction read index".into(),
                    ));
                }
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
        // The coordinator acknowledgement is durable. Release each participant
        // directly from the targets already read above, avoiding a second
        // coordinator status/participant-discovery round trip while preserving
        // the same receipt ordering and restart recovery contract.
        let cleanup = stream::iter(participant_targets.into_iter().map(|(position, target)| {
            let coordinator = coordinator.clone();
            let identity = identity.clone();
            async move {
                let participant_cell = match &target {
                    ReadTarget::Account(target) | ReadTarget::Data(target) => {
                        *target.cell_id().as_bytes()
                    }
                };
                let coordinator_cell = *coordinator.cell_id().as_bytes();
                let mutation = mutation_identity()?;
                let mut admission_retries = 0;
                let released = loop {
                    let input = Json(ReadTransactionInput {
                        transaction_id: identity.transaction_id,
                        coordinator_cell,
                    });
                    let result = match &target {
                        ReadTarget::Account(target) => {
                            self.client
                                .command::<ReleaseAccountTransactionReads>(target, mutation, input)
                                .await
                        }
                        ReadTarget::Data(target) => {
                            self.client
                                .command::<ReleasePartitionTransactionReads>(
                                    target, mutation, input,
                                )
                                .await
                        }
                    };
                    match result {
                        Err(InvocationError::NotStarted(cellule_runtime::Error::Capacity(_)))
                            if admission_retries < 4 =>
                        {
                            admission_retries += 1;
                            tokio::time::sleep(Duration::from_millis(2 << admission_retries)).await;
                        }
                        other => break other.map_err(cell_error)?,
                    }
                };
                if !released.output.0 {
                    return Err(StorageError::Internal(
                        "participant rejected read result release".into(),
                    ));
                }
                Ok::<_, StorageError>(ReadResultRelease {
                    position,
                    participant_cell,
                    sequence: released.receipt.commit_sequence,
                })
            }
        }))
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await;
        let releases = cleanup.into_iter().collect::<Result<Vec<_>, _>>()?;
        let recorded = self
            .client
            .command::<RecordReadResultReleases>(
                &coordinator,
                mutation_identity()?,
                Json(RecordReadResultReleasesInput {
                    account_id: account_id.to_owned(),
                    transaction_id: identity.transaction_id,
                    routing_key: identity.routing_key,
                    releases,
                }),
            )
            .await
            .map_err(cell_error)?;
        if recorded.output.0.iter().any(|outcome| {
            !matches!(
                outcome,
                CoordinatorPhaseOutcome::Recorded | CoordinatorPhaseOutcome::Replay
            )
        }) {
            return Err(StorageError::Internal(
                "coordinator rejected read result release".into(),
            ));
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
