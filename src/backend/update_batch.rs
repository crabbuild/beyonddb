//! Bounded coalescing for independent updates with returned item images.

use std::collections::{HashMap, VecDeque};
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use cellule_runtime::client::InvocationError;
use cellule_runtime::identity::CellTarget;
use extenddb_storage::error::StorageError;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, oneshot};

use super::{cell_error, mutation_identity};
use crate::partition::{BATCH_INPUT_BYTES, MAX_UPDATES};
use crate::{
    BatchedPartitionUpdate, Json, PartitionUpdateBatch, PartitionUpdateBatchOutcome,
    PartitionUpdateIndividual, PartitionUpdateOutcome,
};

const WINDOW: Duration = Duration::from_millis(2);
const MAX_PENDING: usize = 64;

struct Pending {
    update: BatchedPartitionUpdate,
    key: Vec<u8>,
    bytes: usize,
    reply: oneshot::Sender<Result<PartitionUpdateOutcome, StorageError>>,
    _permit: OwnedSemaphorePermit,
}

struct Slot {
    target: CellTarget,
    queued: Mutex<VecDeque<Pending>>,
    permits: Arc<Semaphore>,
    scheduled: AtomicBool,
}

pub(crate) struct UpdateBatcher {
    client: cellule_runtime::CellClient,
    slots: Mutex<HashMap<[u8; 32], Weak<Slot>>>,
}

impl super::CellStorage {
    pub(crate) async fn submit_update(
        &self,
        target: CellTarget,
        key: Vec<u8>,
        update: BatchedPartitionUpdate,
    ) -> Result<PartitionUpdateOutcome, StorageError> {
        self.update_batcher.submit(target, key, update).await
    }
}

impl UpdateBatcher {
    pub(crate) fn new(client: cellule_runtime::CellClient) -> Self {
        Self {
            client,
            slots: Mutex::new(HashMap::new()),
        }
    }

    async fn submit(
        self: &Arc<Self>,
        target: CellTarget,
        key: Vec<u8>,
        update: BatchedPartitionUpdate,
    ) -> Result<PartitionUpdateOutcome, StorageError> {
        let bytes = serde_json::to_vec(&update)
            .map_err(|error| StorageError::Internal(error.to_string()))?
            .len()
            + 1;
        if bytes + 6 > BATCH_INPUT_BYTES as usize {
            return self.individual(&target, update).await;
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
                    permits: Arc::new(Semaphore::new(MAX_PENDING)),
                    scheduled: AtomicBool::new(false),
                });
                slots.insert(id, Arc::downgrade(&slot));
                slot
            }
        };
        // Backpressure precedes enqueueing. Permits cover the selected batch
        // until its durable result is delivered as well as queued operations.
        let permit = slot
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StorageError::Transient("update batch queue closed".into()))?;
        let (reply, result) = oneshot::channel();
        let schedule = {
            let mut queued = slot.queued.lock().await;
            queued.push_back(Pending {
                update,
                key,
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
            .map_err(|_| StorageError::Transient("update batch worker stopped".into()))?
    }

    async fn flush(self: Arc<Self>, slot: Arc<Slot>) {
        loop {
            if slot.queued.lock().await.len() < MAX_UPDATES {
                tokio::time::sleep(WINDOW).await;
            }
            let pending = {
                let mut queue = slot.queued.lock().await;
                let Some(first) = queue.front() else {
                    slot.scheduled.store(false, Ordering::Release);
                    return;
                };
                let table_id = first.update.input.table_id.clone();
                let epoch = first.update.input.epoch;
                let mut selected: Vec<Pending> = Vec::new();
                let mut deferred = VecDeque::new();
                let mut bytes = 6;
                let count = queue.len();
                while selected.len() < MAX_UPDATES && selected.len() + deferred.len() < count {
                    let Some(candidate) = queue.front() else {
                        break;
                    };
                    if candidate.update.input.table_id != table_id
                        || candidate.update.input.epoch != epoch
                        || bytes + candidate.bytes > BATCH_INPUT_BYTES as usize
                    {
                        break;
                    }
                    let Some(candidate) = queue.pop_front() else {
                        break;
                    };
                    if selected.iter().any(|prior| prior.key == candidate.key) {
                        deferred.push_back(candidate);
                    } else {
                        bytes += candidate.bytes;
                        selected.push(candidate);
                    }
                }
                while let Some(candidate) = deferred.pop_back() {
                    queue.push_front(candidate);
                }
                selected
            };
            let identity = match mutation_identity() {
                Ok(identity) => identity,
                Err(error) => {
                    for pending in pending {
                        let _ = pending.reply.send(Err(error.clone()));
                    }
                    continue;
                }
            };
            let updates = pending
                .iter()
                .map(|pending| pending.update.clone())
                .collect();
            let result = self
                .client
                .command::<PartitionUpdateBatch>(&slot.target, identity, Json(updates))
                .await;
            match result {
                Ok(committed) => match committed.output {
                    PartitionUpdateBatchOutcome::Results(results)
                        if results.len() == pending.len() =>
                    {
                        for (pending, result) in pending.into_iter().zip(results) {
                            let _ = pending.reply.send(Ok(result));
                        }
                    }
                    _ => {
                        for pending in pending {
                            let _ = pending.reply.send(Err(StorageError::Internal(
                                "invalid committed update batch reply".into(),
                            )));
                        }
                    }
                },
                Err(InvocationError::Rejected(committed))
                    if committed.output == PartitionUpdateBatchOutcome::IndividualRequired =>
                {
                    // A confirmed rejected receipt proves the complete batch
                    // rolled back. Never retry after an ambiguous invocation.
                    for pending in pending {
                        let result = self.individual(&slot.target, pending.update).await;
                        let _ = pending.reply.send(result);
                    }
                }
                Err(error) => {
                    let error = cell_error(error);
                    for pending in pending {
                        let _ = pending.reply.send(Err(error.clone()));
                    }
                }
            }
        }
    }

    async fn individual(
        &self,
        target: &CellTarget,
        update: BatchedPartitionUpdate,
    ) -> Result<PartitionUpdateOutcome, StorageError> {
        let output = self
            .client
            .command::<PartitionUpdateIndividual>(target, mutation_identity()?, Json(vec![update]))
            .await
            .map_err(cell_error)?
            .output;
        match output {
            PartitionUpdateBatchOutcome::Results(mut results) if results.len() == 1 => results
                .pop()
                .ok_or_else(|| StorageError::Internal("missing individual update reply".into())),
            _ => Err(StorageError::Internal(
                "invalid individual update reply".into(),
            )),
        }
    }
}
