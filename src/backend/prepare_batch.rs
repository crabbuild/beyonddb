//! Bounded publication sharing for independent routed transaction prepares.

use std::collections::{HashMap, VecDeque};
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use cellule_runtime::client::{CellClient, Committed, InvocationError, Receipt};
use cellule_runtime::identity::{CellTarget, RequestId};
use extenddb_storage::error::StorageError;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, oneshot};

use super::transaction_transport::PhaseError;
use super::{cell_error, mutation_identity};
use crate::partition::{MAX_PREPARES, PREPARE_BATCH_BYTES};
use crate::{
    Json, ParticipantTransactionState, PreparePartitionBatchOutcome, PreparePartitionTransaction,
    PreparePartitionTransactionBatch, PreparePartitionTransactionBounded,
    PreparePartitionTransactionInput, PrepareTransactionOutcome, ReadPartitionTransaction,
    ReadTransactionInput, TransactionCommandInput,
};

type Prepared = Result<(PrepareTransactionOutcome, Receipt), PhaseError>;
const WINDOW: Duration = Duration::from_millis(2);

struct Pending {
    input: PreparePartitionTransactionInput,
    bytes: usize,
    reply: oneshot::Sender<Prepared>,
    _permit: OwnedSemaphorePermit,
}

struct Slot {
    target: CellTarget,
    queued: Mutex<VecDeque<Pending>>,
    permits: Arc<Semaphore>,
    scheduled: AtomicBool,
}

pub(super) struct PrepareBatcher {
    client: CellClient,
    slots: Mutex<HashMap<[u8; 32], Weak<Slot>>>,
}

impl PrepareBatcher {
    pub(super) fn new(client: CellClient) -> Self {
        Self {
            client,
            slots: Mutex::new(HashMap::new()),
        }
    }

    pub(super) async fn submit(
        self: &Arc<Self>,
        target: CellTarget,
        input: PreparePartitionTransactionInput,
    ) -> Prepared {
        let bytes = serde_json::to_vec(&input)
            .map_err(|error| StorageError::Internal(error.to_string()))?
            .len()
            + 1;
        if bytes + 2 > PREPARE_BATCH_BYTES {
            return self.individual(&target, input).await;
        }
        let slot = {
            let mut slots = self.slots.lock().await;
            let id = *target.cell_id().as_bytes();
            if let Some(slot) = slots.get(&id).and_then(Weak::upgrade) {
                slot
            } else {
                slots.retain(|_, slot| slot.strong_count() > 0);
                let slot = Arc::new(Slot {
                    target,
                    queued: Mutex::new(VecDeque::new()),
                    permits: Arc::new(Semaphore::new(64)),
                    scheduled: AtomicBool::new(false),
                });
                slots.insert(id, Arc::downgrade(&slot));
                slot
            }
        };
        let permit = slot
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StorageError::Transient("prepare queue closed".into()))?;
        let (reply, result) = oneshot::channel();
        let schedule = {
            let mut queued = slot.queued.lock().await;
            queued.push_back(Pending {
                input,
                bytes,
                reply,
                _permit: permit,
            });
            !slot.scheduled.swap(true, Ordering::AcqRel)
        };
        if schedule {
            let batcher = self.clone();
            tokio::spawn(async move {
                batcher.flush(slot).await;
            });
        }
        result
            .await
            .map_err(|_| StorageError::Transient("prepare worker stopped".into()))?
    }

    async fn flush(self: Arc<Self>, slot: Arc<Slot>) {
        loop {
            if slot.queued.lock().await.len() < MAX_PREPARES {
                tokio::time::sleep(WINDOW).await;
            }
            let pending = {
                let mut queue = slot.queued.lock().await;
                queue.retain(|pending| !pending.reply.is_closed());
                if queue.is_empty() {
                    slot.scheduled.store(false, Ordering::Release);
                    return;
                }
                let mut selected = Vec::new();
                let mut bytes = 2;
                while selected.len() < MAX_PREPARES {
                    let Some(candidate) = queue.front() else {
                        break;
                    };
                    if bytes + candidate.bytes > PREPARE_BATCH_BYTES {
                        break;
                    }
                    let Some(candidate) = queue.pop_front() else {
                        break;
                    };
                    bytes += candidate.bytes;
                    selected.push(candidate);
                }
                selected
            };
            if pending.len() == 1 {
                for pending in pending {
                    let result = self.individual(&slot.target, pending.input).await;
                    let _ = pending.reply.send(result);
                }
                continue;
            }
            let identity = match mutation_identity() {
                Ok(identity) => identity,
                Err(error) => {
                    for pending in pending {
                        let _ = pending.reply.send(Err(error.clone().into()));
                    }
                    continue;
                }
            };
            let inputs = pending
                .iter()
                .map(|pending| pending.input.clone())
                .collect();
            let result = self
                .client
                .command::<PreparePartitionTransactionBatch>(&slot.target, identity, Json(inputs))
                .await;
            match result {
                Ok(committed) if committed.output.0 == PreparePartitionBatchOutcome::Prepared => {
                    for pending in pending {
                        let _ = pending
                            .reply
                            .send(Ok((PrepareTransactionOutcome::Prepared, committed.receipt)));
                    }
                }
                Err(InvocationError::Rejected(committed))
                    if committed.output.0 == PreparePartitionBatchOutcome::IndividualRequired =>
                {
                    // This receipt proves rollback of every application effect.
                    // Never use this fallback for an uncertain batch outcome.
                    for pending in pending {
                        if !pending.reply.is_closed() {
                            let result = self.individual(&slot.target, pending.input).await;
                            let _ = pending.reply.send(result);
                        }
                    }
                }
                Err(InvocationError::Pending(_)) => {
                    for pending in pending {
                        let result = self.observe(&slot.target, &pending.input).await;
                        let _ = pending.reply.send(result);
                    }
                }
                Err(InvocationError::NotStarted(cellule_runtime::Error::Capacity(reason))) => {
                    // The complete batch is proven not to have started. Its
                    // combined capture/resolution reservation may exceed the
                    // budget even when each participant fits independently.
                    tracing::debug!(
                        reason,
                        "prepare batch capacity requires individual commands"
                    );
                    for pending in pending {
                        if !pending.reply.is_closed() {
                            let result = self.individual(&slot.target, pending.input).await;
                            let _ = pending.reply.send(result);
                        }
                    }
                }
                Err(InvocationError::NotStarted(cellule_runtime::Error::Sqlite(error)))
                    if error.sqlite_error_code()
                        == Some(cellule_ltx::rusqlite::ErrorCode::DiskFull) =>
                {
                    // Preserve individual FULL admission classification so
                    // the driver can durably decide ABORT when needed.
                    for pending in pending {
                        if !pending.reply.is_closed() {
                            let result = self.individual(&slot.target, pending.input).await;
                            let _ = pending.reply.send(result);
                        }
                    }
                }
                Err(error) => {
                    let error = cell_error(error);
                    for pending in pending {
                        let _ = pending.reply.send(Err(error.clone().into()));
                    }
                }
                Ok(_) => {
                    for pending in pending {
                        let _ = pending.reply.send(Err(StorageError::Internal(
                            "invalid prepare batch reply".into(),
                        )
                        .into()));
                    }
                }
            }
        }
    }

    async fn individual(
        &self,
        target: &CellTarget,
        input: PreparePartitionTransactionInput,
    ) -> Prepared {
        let identity = mutation_identity()?;
        let result = self
            .client
            .command::<PreparePartitionTransactionBounded>(target, identity, Json(input.clone()))
            .await;
        let result = if matches!(&result, Err(InvocationError::Rejected(committed))
            if committed.output.0 == PrepareTransactionOutcome::WideRequired)
        {
            // Different command digests require different mutation identities.
            let identity = cellule_runtime::MutationIdentity {
                request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
                ..identity
            };
            self.client
                .command::<PreparePartitionTransaction>(
                    target,
                    identity,
                    Json(TransactionCommandInput::Inline(input.clone())),
                )
                .await
        } else {
            result
        };
        self.finish_individual(target, &input, result).await
    }

    async fn finish_individual(
        &self,
        target: &CellTarget,
        input: &PreparePartitionTransactionInput,
        result: Result<
            Committed<Json<PrepareTransactionOutcome>>,
            InvocationError<Json<PrepareTransactionOutcome>>,
        >,
    ) -> Prepared {
        match result {
            Ok(committed) => Ok((committed.output.0, committed.receipt)),
            Err(InvocationError::Rejected(committed)) => {
                Ok((committed.output.0, committed.receipt))
            }
            Err(InvocationError::Pending(_)) => self.observe(target, input).await,
            Err(error) => Err(error.into()),
        }
    }

    async fn observe(
        &self,
        target: &CellTarget,
        input: &PreparePartitionTransactionInput,
    ) -> Prepared {
        let observed = self
            .client
            .query::<ReadPartitionTransaction>(
                target,
                None,
                Json(ReadTransactionInput {
                    transaction_id: input.transaction_id,
                    coordinator_cell: input.coordinator_cell,
                }),
            )
            .await
            .map_err(cell_error)?;
        let outcome = match observed.output.0 {
            ParticipantTransactionState::Prepared => PrepareTransactionOutcome::Replay,
            ParticipantTransactionState::Committed => PrepareTransactionOutcome::Committed,
            ParticipantTransactionState::Aborted => PrepareTransactionOutcome::Aborted,
            ParticipantTransactionState::CoordinatorMismatch => PrepareTransactionOutcome::Mismatch,
            ParticipantTransactionState::Missing => {
                return Err(StorageError::Transient(
                    "participant prepare outcome remains pending".into(),
                )
                .into());
            }
        };
        Ok((outcome, observed.receipt))
    }
}
