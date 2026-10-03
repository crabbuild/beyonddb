//! Bounded phase inputs and immutable coordinator payload retrieval.

use cellule_runtime::{
    MutationIdentity,
    client::{Committed, InvocationError},
    identity::{CellTarget, RequestId},
    registry::Command,
};
use extenddb_storage::error::StorageError;

use super::{CellStorage, cell_error, mutation_identity};
use crate::{
    CoordinatorParticipant, Json, MultipartTransactionCommand, PrepareTransactionOutcome,
    ReadCoordinatorParticipant, ReadCoordinatorParticipantInput, TransactionCommandInput,
    TransactionPayloadRef, UploadTransactionPayload,
};

// Preserve proven capacity refusal until the transaction driver can decide
// ABORT. Pending outcomes stay retryable; they are never classified by source
// text or by an error nested inside an unknown mutation result.
pub(super) enum PhaseError {
    Capacity(cellule_runtime::Error),
    Other(StorageError),
}

impl<T> From<InvocationError<T>> for PhaseError {
    fn from(error: InvocationError<T>) -> Self {
        match error {
            InvocationError::NotStarted(error @ cellule_runtime::Error::Capacity(_)) => {
                Self::Capacity(error)
            }
            InvocationError::NotStarted(cellule_runtime::Error::Sqlite(error))
                if error.sqlite_error_code()
                    == Some(cellule_ltx::rusqlite::ErrorCode::DiskFull) =>
            {
                Self::Capacity(cellule_runtime::Error::Sqlite(error))
            }
            error => Self::Other(cell_error(error)),
        }
    }
}

impl From<StorageError> for PhaseError {
    fn from(error: StorageError) -> Self {
        Self::Other(error)
    }
}

impl From<PhaseError> for StorageError {
    fn from(error: PhaseError) -> Self {
        match error {
            PhaseError::Capacity(error) => cell_error::<()>(InvocationError::NotStarted(error)),
            PhaseError::Other(error) => error,
        }
    }
}

impl CellStorage {
    pub(super) async fn prepare_inline<C, B>(
        &self,
        target: &CellTarget,
        identity: MutationIdentity,
        input: C::Payload,
        bounded: bool,
    ) -> Result<
        Committed<Json<PrepareTransactionOutcome>>,
        InvocationError<Json<PrepareTransactionOutcome>>,
    >
    where
        C: MultipartTransactionCommand<Output = Json<PrepareTransactionOutcome>>,
        C::Payload: Clone + Send,
        B: Command<Input = Json<C::Payload>, Output = Json<PrepareTransactionOutcome>>,
    {
        let identity = if bounded {
            let result = self
                .client
                .command::<B>(target, identity, Json(input.clone()))
                .await;
            if !matches!(
                &result,
                Err(InvocationError::Rejected(committed))
                    if committed.output.0 == PrepareTransactionOutcome::WideRequired
            ) {
                // Pending, invalid results and capacity errors retain their
                // original meaning. Only a durable rejected receipt permits
                // fallback; never infer rollback from a missing participant.
                return result;
            }
            MutationIdentity {
                // A different opcode has a different mutation digest. Retain
                // the phase deadline but never reuse its request identity.
                request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
                ..identity
            }
        } else {
            identity
        };
        self.client
            .command::<C>(
                target,
                identity,
                Json(TransactionCommandInput::Inline(input)),
            )
            .await
    }

    pub(super) async fn upload_transaction<C: MultipartTransactionCommand>(
        &self,
        target: &CellTarget,
        identity: MutationIdentity,
        input: &C::Payload,
    ) -> Result<TransactionPayloadRef, PhaseError> {
        let bytes =
            serde_json::to_vec(input).map_err(|error| StorageError::Internal(error.to_string()))?;
        let reference = TransactionPayloadRef::new(&bytes, identity.expires_at_ms)
            .map_err(|error| StorageError::Validation(error.to_string()))?;
        for chunk in reference.chunks(&bytes) {
            // A lost chunk reply is safe to repeat: the receiver compares bytes.
            // Never interpret an upload result as the phase's transaction outcome.
            let result = self
                .client
                .command::<UploadTransactionPayload<C>>(target, mutation_identity()?, chunk.clone())
                .await;
            if matches!(result, Err(InvocationError::Pending(_))) {
                self.client
                    .command::<UploadTransactionPayload<C>>(target, mutation_identity()?, chunk)
                    .await?;
            } else {
                result?;
            }
        }
        Ok(reference)
    }

    pub(super) async fn coordinator_participant(
        &self,
        target: &CellTarget,
        mut input: ReadCoordinatorParticipantInput,
    ) -> Result<Option<CoordinatorParticipant>, StorageError> {
        let mut bytes = Vec::new();
        let mut manifest = None;
        loop {
            let Some(result) = self
                .client
                .query::<ReadCoordinatorParticipant>(target, None, Json(input.clone()))
                .await
                .map_err(cell_error)?
                .output
            else {
                // Resolution can compact between chunks. Discard the partial
                // image so a driver can finish from the authoritative decision.
                return Ok(None);
            };
            let description = (result.target.clone(), result.chunks);
            if result.chunks == 0
                || result.chunks as usize
                    > crate::transaction_transport::MAX_BYTES
                        .div_ceil(crate::transaction_transport::CHUNK_BYTES)
                || manifest.as_ref().is_some_and(|old| old != &description)
                || result.payload.is_empty()
                || result.payload.len() > crate::transaction_transport::CHUNK_BYTES
            {
                return Err(StorageError::Internal(
                    "invalid participant payload manifest".into(),
                ));
            }
            manifest = Some(description);
            bytes.extend_from_slice(&result.payload);
            input.chunk += 1;
            if input.chunk == result.chunks {
                return Ok(Some(CoordinatorParticipant {
                    target: result.target,
                    operations: serde_json::from_slice(&bytes)
                        .map_err(|error| StorageError::Internal(error.to_string()))?,
                }));
            }
        }
    }
}
