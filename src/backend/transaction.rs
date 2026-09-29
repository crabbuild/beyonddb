//! Resume read and write transactions from immutable coordinator records.

use cellule_runtime::client::{InvocationError, Receipt};
use cellule_runtime::identity::CellTarget;
use extenddb_storage::error::StorageError;
use futures_util::{StreamExt, stream};

use super::transaction_transport::PhaseError;
use super::{CellStorage, cell_error, mutation_identity};
use crate::{
    CoordinatorDecision, CoordinatorParticipantTarget, CoordinatorPhaseInput,
    CoordinatorPhaseOutcome, CrossCellTransactionStatus, DecideCrossCellTransaction,
    DecideCrossCellTransactionInput, DecideCrossCellTransactionOutcome, Json,
    ParticipantTransactionState, PrepareAccountTransaction, PrepareAccountTransactionInput,
    PreparePartitionTransaction, PreparePartitionTransactionInput, PrepareTransactionOutcome,
    ReadCoordinatorParticipantInput, ReadCrossCellTransaction, ReadCrossCellTransactionInput,
    ReadTransactionInput, ReadUnresolvedCoordinatorParticipants, RecordParticipantPrepare,
    TransactionCommandInput, TransactionFailure, account_target, coordinator_target, data_target,
};

impl CellStorage {
    /// Drive an admitted transaction using its durable participant payloads.
    ///
    /// The coordinator must already contain a published BEGIN. Returns its
    /// terminal decision only after every participant resolution is published.
    /// Unresolved transport ambiguity leaves work pending and returns a retryable error.
    pub async fn resume_cross_cell_transaction(
        &self,
        account_id: &str,
        routing_key: &[u8],
        transaction_id: [u8; 16],
    ) -> Result<CoordinatorDecision, StorageError> {
        let coordinator = coordinator_target(account_id, routing_key)
            .map_err(|error| StorageError::Internal(error.to_string()))?;
        let read = ReadCrossCellTransactionInput {
            account_id: account_id.to_owned(),
            transaction_id,
            routing_key: routing_key.to_vec(),
        };
        if self.transaction_status(&coordinator, &read).await?.decision
            != CoordinatorDecision::Begin
        {
            return self.finish_transaction(&coordinator, &read).await;
        }
        let participants = self
            .client
            .query::<ReadUnresolvedCoordinatorParticipants>(&coordinator, None, Json(read.clone()))
            .await
            .map_err(cell_error)?
            .output
            .0;
        // Fetch immutable participant payloads concurrently. Each participant
        // has its own durable cell, so these reads do not need to be serialized.
        let payloads = stream::iter(
            participants
                .into_iter()
                .filter(|participant| {
                    // Durable prepare evidence survives driver and owner replacement.
                    // Keep these participants in recovery's list until resolution, but
                    // do not re-upload their payloads or publish another prepare receipt.
                    !participant.prepared
                })
                .map(|participant| {
                    let participant_coordinator = coordinator.clone();
                    let input = ReadCoordinatorParticipantInput {
                        account_id: read.account_id.clone(),
                        transaction_id,
                        routing_key: read.routing_key.clone(),
                        position: participant.position,
                        chunk: 0,
                    };
                    async move {
                        let payload = self
                            .coordinator_participant(&participant_coordinator, input)
                            .await?;
                        Ok::<_, StorageError>((participant.position, payload))
                    }
                }),
        )
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await;
        let mut attempts = Vec::new();
        for payload in payloads {
            let (position, Some(payload)) = payload? else {
                return self.finish_transaction(&coordinator, &read).await;
            };
            let operations = payload
                .operations
                .iter()
                .map(|operation| operation.operation.clone())
                .collect();
            let coordinator_cell = *coordinator.cell_id().as_bytes();
            let (target, input) = match &payload.target {
                CoordinatorParticipantTarget::Account => (
                    account_target(account_id),
                    ParticipantPrepare::Account(PrepareAccountTransactionInput {
                        transaction_id,
                        coordinator_cell,
                        coordinator_key: read.routing_key.clone(),
                        operations,
                    }),
                ),
                CoordinatorParticipantTarget::Data {
                    table_id,
                    partition_id,
                    epoch,
                } => (
                    data_target(account_id, table_id, partition_id),
                    ParticipantPrepare::Data(PreparePartitionTransactionInput {
                        table_id: table_id.clone(),
                        epoch: *epoch,
                        transaction_id,
                        coordinator_cell,
                        coordinator_key: read.routing_key.clone(),
                        operations,
                    }),
                ),
            };
            let target = target.map_err(|error| StorageError::Internal(error.to_string()))?;
            attempts.push((position, payload, target, input));
        }

        // Prepare independent participant cells concurrently. Results are
        // sorted before coordinator evidence is recorded so rejection and
        // capacity outcomes retain the prior participant order.
        let mut prepared = stream::iter(attempts.into_iter().map(
            |(position, payload, target, input)| async move {
                let result = self.prepare_transaction_participant(&target, input).await;
                (position, payload, target, result)
            },
        ))
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await;
        prepared.sort_unstable_by_key(|(position, ..)| *position);
        let mut evidence = Vec::with_capacity(prepared.len());
        for (position, payload, target, result) in prepared {
            let (outcome, receipt) = match result {
                Ok(prepared) => prepared,
                Err(PhaseError::Capacity(error)) => {
                    tracing::warn!(%error, "transaction participant capacity refused");
                    let operation = payload.operations.first().ok_or_else(|| {
                        StorageError::Internal("participant has no operations".into())
                    })?;
                    // The refusal proves no mutation from this attempt committed.
                    // Competing drivers may still win COMMIT; the coordinator CAS
                    // decides, and all resolutions finish before cancellation returns.
                    return self
                        .decide_transaction(
                            &coordinator,
                            &read,
                            CoordinatorDecision::Abort {
                                index: Some(operation.index),
                                reason: Some(TransactionFailure::Throttled),
                            },
                        )
                        .await;
                }
                Err(error) => return Err(error.into()),
            };
            let rejection = match outcome {
                PrepareTransactionOutcome::Prepared | PrepareTransactionOutcome::Replay => None,
                PrepareTransactionOutcome::Rejected { index, reason } => Some((index, reason)),
                PrepareTransactionOutcome::NotInstalled
                | PrepareTransactionOutcome::StaleRoute
                | PrepareTransactionOutcome::Sealed
                | PrepareTransactionOutcome::NotReady
                | PrepareTransactionOutcome::WrongPartition => {
                    Some((0, TransactionFailure::Conflict))
                }
                PrepareTransactionOutcome::Committed | PrepareTransactionOutcome::Aborted => {
                    // A concurrent driver/recovery owner may have finished. Only
                    // the coordinator can decide which terminal outcome to return.
                    return self.finish_transaction(&coordinator, &read).await;
                }
                PrepareTransactionOutcome::Mismatch => {
                    return Err(StorageError::Internal(
                        "participant transaction identity mismatch".into(),
                    ));
                }
            };
            if let Some((index, reason)) = rejection {
                let operation = payload.operations.get(index).ok_or_else(|| {
                    StorageError::Internal("participant returned invalid operation index".into())
                })?;
                let decision = CoordinatorDecision::Abort {
                    index: Some(operation.index),
                    reason: Some(reason),
                };
                return self.decide_transaction(&coordinator, &read, decision).await;
            }
            evidence.push((position, target, receipt));
        }

        // Evidence records are independent CAS updates on the coordinator.
        // Publish them concurrently after all participant prepares succeed;
        // durable ordering is carried by each participant position.
        let recorded = stream::iter(evidence.into_iter().map(|(position, target, receipt)| {
            let coordinator = coordinator.clone();
            let read = read.clone();
            async move {
                let result = self
                    .client
                    .command::<RecordParticipantPrepare>(
                        &coordinator,
                        mutation_identity()?,
                        Json(CoordinatorPhaseInput {
                            account_id: read.account_id,
                            transaction_id,
                            routing_key: read.routing_key,
                            position,
                            participant_cell: *target.cell_id().as_bytes(),
                            sequence: receipt.commit_sequence,
                        }),
                    )
                    .await;
                Ok::<_, StorageError>(result)
            }
        }))
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await;
        for result in recorded {
            let recorded = result?;
            match recorded {
                Ok(result)
                    if matches!(
                        result.output.0,
                        CoordinatorPhaseOutcome::Recorded | CoordinatorPhaseOutcome::Replay
                    ) => {}
                Err(InvocationError::Rejected(result))
                    if result.output.0 == CoordinatorPhaseOutcome::WrongDecision =>
                {
                    return self.finish_transaction(&coordinator, &read).await;
                }
                Ok(_) | Err(InvocationError::Rejected(_)) => {
                    return Err(StorageError::Internal(
                        "coordinator refused prepare evidence".into(),
                    ));
                }
                Err(error) => return Err(cell_error(error)),
            }
        }
        self.decide_transaction(&coordinator, &read, CoordinatorDecision::Commit)
            .await
    }

    async fn transaction_status(
        &self,
        coordinator: &CellTarget,
        read: &ReadCrossCellTransactionInput,
    ) -> Result<CrossCellTransactionStatus, StorageError> {
        self.client
            .query::<ReadCrossCellTransaction>(coordinator, None, Json(read.clone()))
            .await
            .map_err(cell_error)?
            .output
            .0
            .ok_or_else(|| StorageError::Internal("coordinator transaction is missing".into()))
    }

    async fn finish_transaction(
        &self,
        coordinator: &CellTarget,
        read: &ReadCrossCellTransactionInput,
    ) -> Result<CoordinatorDecision, StorageError> {
        let status = self.transaction_status(coordinator, read).await?;
        if status.decision == CoordinatorDecision::Begin {
            return Err(StorageError::Transient(
                "transaction decision remains pending".into(),
            ));
        }
        self.finish_decided_cross_cell_transaction(
            &read.account_id,
            &read.routing_key,
            read.transaction_id,
        )
        .await?;
        Ok(status.decision)
    }

    async fn decide_transaction(
        &self,
        coordinator: &CellTarget,
        read: &ReadCrossCellTransactionInput,
        decision: CoordinatorDecision,
    ) -> Result<CoordinatorDecision, StorageError> {
        let result = self
            .client
            .command::<DecideCrossCellTransaction>(
                coordinator,
                mutation_identity()?,
                Json(DecideCrossCellTransactionInput {
                    account_id: read.account_id.clone(),
                    transaction_id: read.transaction_id,
                    routing_key: read.routing_key.clone(),
                    decision,
                }),
            )
            .await;
        match result {
            Ok(result)
                if matches!(
                    result.output.0,
                    DecideCrossCellTransactionOutcome::Decided(_)
                ) => {}
            Err(InvocationError::Rejected(result))
                if result.output.0 == DecideCrossCellTransactionOutcome::DecisionConflict => {}
            // A lost reply may conceal a commit; read the authoritative state.
            Err(InvocationError::Pending(_)) => {}
            Ok(_) | Err(InvocationError::Rejected(_)) => {
                return Err(StorageError::Internal(
                    "coordinator refused transaction decision".into(),
                ));
            }
            Err(error) => return Err(cell_error(error)),
        }
        self.finish_transaction(coordinator, read).await
    }

    async fn prepare_transaction_participant(
        &self,
        target: &CellTarget,
        input: ParticipantPrepare,
    ) -> Result<(PrepareTransactionOutcome, Receipt), PhaseError> {
        let read = match &input {
            ParticipantPrepare::Account(input) => ReadTransactionInput {
                transaction_id: input.transaction_id,
                coordinator_cell: input.coordinator_cell,
            },
            ParticipantPrepare::Data(input) => ReadTransactionInput {
                transaction_id: input.transaction_id,
                coordinator_cell: input.coordinator_cell,
            },
        };
        // The prepare command performs the durable idempotency lookup inside
        // the participant Cell. Avoid a separate state query on the normal
        // first-attempt path; only an ambiguous reply needs a follow-up read.
        let identity = mutation_identity()?;
        let inline = match &input {
            ParticipantPrepare::Account(input) => serde_json::to_vec(input),
            ParticipantPrepare::Data(input) => serde_json::to_vec(input),
        }
        .map_err(|error| StorageError::Internal(error.to_string()))?
        .len()
            <= crate::transaction_transport::INLINE_BYTES;
        let result = match input {
            ParticipantPrepare::Account(input) => {
                if inline {
                    self.client
                        .command::<PrepareAccountTransaction>(
                            target,
                            identity,
                            Json(TransactionCommandInput::Inline(input)),
                        )
                        .await
                } else {
                    let reference = self
                        .upload_transaction::<PrepareAccountTransaction>(target, identity, &input)
                        .await?;
                    self.client
                        .command::<PrepareAccountTransaction>(
                            target,
                            identity,
                            Json(TransactionCommandInput::Reference(reference)),
                        )
                        .await
                }
            }
            ParticipantPrepare::Data(input) => {
                if inline {
                    self.client
                        .command::<PreparePartitionTransaction>(
                            target,
                            identity,
                            Json(TransactionCommandInput::Inline(input)),
                        )
                        .await
                } else {
                    let reference = self
                        .upload_transaction::<PreparePartitionTransaction>(target, identity, &input)
                        .await?;
                    self.client
                        .command::<PreparePartitionTransaction>(
                            target,
                            identity,
                            Json(TransactionCommandInput::Reference(reference)),
                        )
                        .await
                }
            }
        };
        match result {
            Ok(result) => Ok((result.output.0, result.receipt)),
            Err(InvocationError::Rejected(result)) => Ok((result.output.0, result.receipt)),
            Err(InvocationError::Pending(_)) => {
                let observed = self.participant_state(target, read).await?;
                let outcome = match observed.output.0 {
                    ParticipantTransactionState::Prepared => PrepareTransactionOutcome::Replay,
                    ParticipantTransactionState::Committed => PrepareTransactionOutcome::Committed,
                    ParticipantTransactionState::Aborted => PrepareTransactionOutcome::Aborted,
                    ParticipantTransactionState::CoordinatorMismatch => {
                        PrepareTransactionOutcome::Mismatch
                    }
                    ParticipantTransactionState::Missing => {
                        return Err(StorageError::Transient(
                            "participant prepare outcome remains pending".into(),
                        )
                        .into());
                    }
                };
                Ok((outcome, observed.receipt))
            }
            Err(error) => Err(error.into()),
        }
    }
}

enum ParticipantPrepare {
    Account(PrepareAccountTransactionInput),
    Data(PreparePartitionTransactionInput),
}
