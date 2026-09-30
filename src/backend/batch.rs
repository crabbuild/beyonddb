//! Bounded coalescing for idempotent routed mutations.

use std::collections::{HashMap, VecDeque};
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use cellule_runtime::client::InvocationError;
use cellule_runtime::identity::CellTarget;
use extenddb_storage::error::StorageError;
use tokio::sync::{Mutex, oneshot};

use super::{cell_error, mutation_identity};
use crate::{
    Json, PartitionTransactWriteInput, PartitionTransactWriteNoReturn,
    PartitionTransactWriteOutcome, TransactionFailure, TransactionOperation,
};

const BATCH_WINDOW: Duration = Duration::from_millis(2);
const MAX_BATCH_OPERATIONS: usize = 16;

/// An unconditional mutation that can be replayed safely as part of a batch.
#[derive(Clone)]
pub(crate) struct NoReturnMutation {
    /// Table generation checked by the partition command.
    pub table_id: String,
    /// Directory epoch checked by the partition command.
    pub epoch: u64,
    /// Canonical key bytes used to avoid duplicate operations in one batch.
    pub key: Vec<u8>,
    /// The already validated operation to stage in the partition transaction.
    pub operation: TransactionOperation,
}

struct PendingMutation {
    mutation: NoReturnMutation,
    reply: oneshot::Sender<Result<(), StorageError>>,
}

struct Slot {
    target: CellTarget,
    queued: Mutex<VecDeque<PendingMutation>>,
    scheduled: AtomicBool,
}

/// Coalesces a small number of idempotent writes before one durable command.
pub(crate) struct NoReturnBatcher {
    client: cellule_runtime::CellClient,
    slots: Mutex<HashMap<[u8; 32], Weak<Slot>>>,
}

impl super::CellStorage {
    pub(crate) async fn submit_no_return(
        &self,
        target: CellTarget,
        mutation: NoReturnMutation,
    ) -> Result<(), StorageError> {
        self.no_return_batcher.submit(target, mutation).await
    }
}

impl NoReturnBatcher {
    pub(crate) fn new(client: cellule_runtime::CellClient) -> Self {
        Self {
            client,
            slots: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn submit(
        self: &Arc<Self>,
        target: CellTarget,
        mutation: NoReturnMutation,
    ) -> Result<(), StorageError> {
        let (reply, result) = oneshot::channel();
        let key = *target.cell_id().as_bytes();
        let slot = {
            let mut slots = self.slots.lock().await;
            if let Some(slot) = slots.get(&key).and_then(Weak::upgrade) {
                slot
            } else {
                let slot = Arc::new(Slot {
                    target: target.clone(),
                    queued: Mutex::new(VecDeque::new()),
                    scheduled: AtomicBool::new(false),
                });
                slots.insert(key, Arc::downgrade(&slot));
                slot
            }
        };
        let should_schedule = {
            let mut queued = slot.queued.lock().await;
            queued.push_back(PendingMutation { mutation, reply });
            !slot.scheduled.swap(true, Ordering::AcqRel)
        };
        if should_schedule {
            let batcher = Arc::clone(self);
            tokio::spawn(async move {
                batcher.flush_slot(slot).await;
            });
        }
        result
            .await
            .map_err(|_| StorageError::Transient("batched mutation worker stopped".into()))?
    }

    async fn flush_slot(self: Arc<Self>, slot: Arc<Slot>) {
        loop {
            // A full queue has already had its chance to coalesce. In
            // particular, avoid adding a fresh window after the preceding
            // durable command completed while more writers were waiting.
            if slot.queued.lock().await.len() < MAX_BATCH_OPERATIONS {
                tokio::time::sleep(BATCH_WINDOW).await;
            }
            let batch = {
                let mut queued = slot.queued.lock().await;
                let Some(first) = queued.front() else {
                    slot.scheduled.store(false, Ordering::Release);
                    return;
                };
                let table_id = first.mutation.table_id.clone();
                let epoch = first.mutation.epoch;
                let mut keys = Vec::new();
                let mut selected = Vec::new();
                let mut deferred = VecDeque::new();
                let queued_len = queued.len();
                while selected.len() < MAX_BATCH_OPERATIONS && !queued.is_empty() {
                    let Some(candidate) = queued.front() else {
                        break;
                    };
                    if candidate.mutation.table_id != table_id || candidate.mutation.epoch != epoch
                    {
                        break;
                    }
                    let Some(candidate) = queued.pop_front() else {
                        break;
                    };
                    if keys
                        .iter()
                        .any(|key: &Vec<u8>| key == &candidate.mutation.key)
                    {
                        // Keep repeated keys for a later command, but continue
                        // collecting independent keys behind them. Reordering
                        // concurrent requests for different items is already
                        // allowed; requests for one item stay FIFO.
                        deferred.push_back(candidate);
                    } else {
                        keys.push(candidate.mutation.key.clone());
                        selected.push(candidate);
                    }
                    if selected.len() + deferred.len() >= queued_len {
                        break;
                    }
                }
                while let Some(candidate) = deferred.pop_back() {
                    queued.push_front(candidate);
                }
                (table_id, epoch, selected)
            };
            let (table_id, epoch, pending) = batch;
            if pending.is_empty() {
                continue;
            }
            let (operations, replies): (Vec<_>, Vec<_>) = pending
                .into_iter()
                .map(|pending| (pending.mutation.operation, pending.reply))
                .unzip();
            let result = self
                .client
                .command::<PartitionTransactWriteNoReturn>(
                    &slot.target,
                    match mutation_identity() {
                        Ok(identity) => identity,
                        Err(error) => {
                            for reply in replies {
                                let _ = reply.send(Err(error.clone()));
                            }
                            continue;
                        }
                    },
                    Json(PartitionTransactWriteInput {
                        table_id,
                        epoch,
                        operations,
                    }),
                )
                .await;
            let outcome = match result {
                Ok(committed) => partition_outcome(committed.output.0),
                Err(InvocationError::Rejected(committed)) => partition_outcome(committed.output.0),
                Err(error) => Err(cell_error(error)),
            };
            for reply in replies {
                let _ = reply.send(outcome.clone());
            }
        }
    }
}

fn partition_outcome(outcome: PartitionTransactWriteOutcome) -> Result<(), StorageError> {
    match outcome {
        PartitionTransactWriteOutcome::Applied => Ok(()),
        PartitionTransactWriteOutcome::NotInstalled
        | PartitionTransactWriteOutcome::StaleRoute
        | PartitionTransactWriteOutcome::Sealed
        | PartitionTransactWriteOutcome::NotReady
        | PartitionTransactWriteOutcome::WrongPartition => Err(StorageError::Transient(
            "table partition route changed; retry the request".into(),
        )),
        PartitionTransactWriteOutcome::Rejected { reason, .. } => match reason {
            TransactionFailure::Throttled => Err(StorageError::LimitExceeded(
                "partition capacity is exhausted".into(),
            )),
            TransactionFailure::Conflict => Err(StorageError::TransactionConflict(
                "item is locked by a transaction".into(),
            )),
            TransactionFailure::Validation(message) => Err(StorageError::Validation(message)),
            TransactionFailure::ConditionFailed(_) => Err(StorageError::Internal(
                "unconditional partition mutation reported a condition failure".into(),
            )),
        },
    }
}
