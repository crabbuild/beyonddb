//! Bounded account registration publication through native SDK ingress.
//! BEGIN waits for its own durable batch receipt; different dispatch contexts
//! never share an invocation or bypass caller admission.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bdb_cellar_runtime::client::CellClient;
use bdb_cellar_runtime::identity::{CellId, CellTarget};
use extenddb_storage::error::StorageError;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, oneshot};

use crate::backend::{cell_error, mutation_identity};
use crate::transaction_coordinator::MAX_REGISTRATION_SHARDS;
use crate::{Json, RegisterCoordinatorShards, RegisterCoordinatorShardsInput};

const WINDOW: Duration = Duration::from_millis(2);

#[derive(Default)]
pub(super) struct CoordinatorRegistrations {
    slots: Mutex<HashMap<CellId, Vec<Weak<Slot>>>>,
}

struct Slot {
    account: CellTarget,
    account_id: String,
    client: CellClient,
    capacity: Arc<Semaphore>,
    queue: Mutex<Queue>,
}

#[derive(Default)]
struct Queue {
    pending: VecDeque<Pending>,
    running: bool,
}

struct Pending {
    shard: u32,
    reply: oneshot::Sender<Result<(), StorageError>>,
    _permit: OwnedSemaphorePermit,
}

impl CoordinatorRegistrations {
    pub(super) async fn register(
        &self,
        client: &CellClient,
        account: &CellTarget,
        account_id: &str,
        shard: u32,
    ) -> Result<(), StorageError> {
        let slot = {
            let mut slots = self.slots.lock().await;
            slots.retain(|_, entries| {
                entries.retain(|slot| slot.strong_count() > 0);
                !entries.is_empty()
            });
            let existing = slots.get(&account.cell_id()).and_then(|entries| {
                entries.iter().filter_map(Weak::upgrade).find(|slot| {
                    slot.account_id == account_id && slot.client.shares_dispatch_context(client)
                })
            });
            if let Some(slot) = existing {
                slot
            } else {
                let slot = Arc::new(Slot {
                    account: account.clone(),
                    account_id: account_id.into(),
                    client: client.clone(),
                    capacity: Arc::new(Semaphore::new(MAX_REGISTRATION_SHARDS * 4)),
                    queue: Mutex::new(Queue::default()),
                });
                slots
                    .entry(account.cell_id())
                    .or_default()
                    .push(Arc::downgrade(&slot));
                slot
            }
        };
        let permit = slot.capacity.clone().acquire_owned().await.map_err(|_| {
            StorageError::Transient("coordinator registration queue stopped".into())
        })?;
        let (reply, result) = oneshot::channel();
        let start = {
            let mut queue = slot.queue.lock().await;
            queue.pending.push_back(Pending {
                shard,
                reply,
                _permit: permit,
            });
            let start = !queue.running;
            queue.running = true;
            start
        };
        if start {
            tokio::spawn(flush(slot));
        }
        // Cancellation after enqueue does not discard a discovery update.
        // It still cannot permit BEGIN: the caller needs this durable receipt.
        result.await.map_err(|_| {
            StorageError::Transient("coordinator registration worker stopped".into())
        })?
    }
}

async fn flush(slot: Arc<Slot>) {
    loop {
        if slot.queue.lock().await.pending.len() < MAX_REGISTRATION_SHARDS {
            tokio::time::sleep(WINDOW).await;
        }
        let batch: Vec<_> = {
            let mut queue = slot.queue.lock().await;
            if queue.pending.is_empty() {
                queue.running = false;
                return;
            }
            let count = queue.pending.len().min(MAX_REGISTRATION_SHARDS);
            queue.pending.drain(..count).collect()
        };
        let mut shards: Vec<_> = batch.iter().map(|pending| pending.shard).collect();
        shards.sort_unstable();
        shards.dedup();
        let outcome = match mutation_identity() {
            Ok(identity) => slot
                .client
                .command::<RegisterCoordinatorShards>(
                    &slot.account,
                    identity,
                    Json(RegisterCoordinatorShardsInput {
                        account_id: slot.account_id.clone(),
                        shards,
                    }),
                )
                .await
                .map(|_| ())
                .map_err(cell_error),
            Err(error) => Err(error),
        };
        for pending in batch {
            let _ = pending.reply.send(outcome.clone());
        }
    }
}
