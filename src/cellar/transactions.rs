//! Fair, bounded transaction discovery independent of physical cell owners.
use crate::backend::cell_error;
use crate::{
    CellStorage, Json, ListCoordinatorShards, ListCoordinatorShardsInput, PendingTransactionCursor,
    account_target,
};
use bdb_cellar_runtime::identity::{CellTarget, partition_for_shard};
use extenddb_storage::error::StorageError;
use std::sync::Arc;

/// Fair, bounded coordinator discovery for one configured account. The caller
/// owns scheduling and cancellation; Cellar still owns physical cell authority.
pub struct TransactionRecovery {
    storage: Arc<CellStorage>,
    account_id: String,
    cursor: Cursor,
}
impl TransactionRecovery {
    pub fn new(storage: Arc<CellStorage>, account_id: String) -> Result<Self, StorageError> {
        account_target(&account_id).map_err(|error| StorageError::Validation(error.to_string()))?;
        Ok(Self {
            storage,
            account_id,
            cursor: Cursor::default(),
        })
    }
    /// Inspect at most one pending record, retaining progress through failures.
    pub async fn step(&mut self) -> Result<(), StorageError> {
        step(&self.storage, &self.account_id, &mut self.cursor).await
    }
}

#[derive(Default)]
pub(super) struct Cursor {
    after_shard: Option<u32>,
    active: Option<Active>,
}
struct Active {
    shard: u32,
    after: Option<PendingTransactionCursor>,
    through: Option<PendingTransactionCursor>,
}
pub(super) async fn step(
    storage: &CellStorage,
    account_id: &str,
    cursor: &mut Cursor,
) -> Result<(), StorageError> {
    let account =
        account_target(account_id).map_err(|error| StorageError::Internal(error.to_string()))?;
    if cursor.active.is_none() {
        let page = storage
            .client()
            .query::<ListCoordinatorShards>(
                &account,
                None,
                Json(ListCoordinatorShardsInput {
                    account_id: account_id.into(),
                    after: cursor.after_shard,
                    limit: 1,
                }),
            )
            .await
            .map_err(cell_error)?
            .output
            .0;
        let Some(shard) = page.first() else {
            cursor.after_shard = None;
            return Ok(());
        };
        cursor.after_shard = Some(shard.shard);
        cursor.active = Some(Active {
            shard: shard.shard,
            after: None,
            through: None,
        });
    }
    let Some(mut active) = cursor.active.take() else {
        return Ok(());
    };
    let coordinator = CellTarget::new(
        account.tenant(),
        crate::APPLICATION,
        crate::transaction_coordinator::NAMESPACE,
        &partition_for_shard(active.shard),
    )
    .map_err(|error| StorageError::Internal(error.to_string()))?;
    let Some((entry, through)) = storage
        .pending_coordinator_transaction(&coordinator, active.after, active.through)
        .await?
    else {
        return Ok(());
    };
    active.after = Some(entry.cursor.clone());
    active.through = Some(through);
    // Advance even when a participant is unavailable. Its durable record will
    // be revisited next pass; one owner must not starve unrelated decisions.
    cursor.active = Some(active);
    storage
        .recover_live_coordinator_entry(&coordinator, entry)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{mutation, start, stop};
    use crate::{
        AbortExpiredTransaction, BeginCrossCellTransaction, BeginCrossCellTransactionInput,
        CoordinatorDecision, CoordinatorParticipant, CoordinatorParticipantTarget, GetItemInput,
        IndexedTransactionOperation, ReadCrossCellTransaction, ReadCrossCellTransactionInput,
        TransactionCommandInput, TransactionOperation, coordinator_target,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    };

    #[test]
    fn begin_expiry_is_atomic_and_its_decision_survives_restart() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let host = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let core = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let clock = Arc::new(AtomicI64::new(1_000));
        host.block_on(async {
            let (node, client) = start(
                directory.path(),
                clock.clone(),
                host.handle(),
                core.handle(),
            )
            .await?;
            let account_id = "123456789012";
            let transaction_id = [42; 16];
            let coordinator = coordinator_target(account_id, &transaction_id)?;
            // Deliberately stop before PREPARE: this fixture exercises abandoned
            // coordinator state, not data validation or transaction commit proof.
            client
                .command::<BeginCrossCellTransaction>(
                    &coordinator,
                    mutation(&clock),
                    Json(TransactionCommandInput::Inline(
                        BeginCrossCellTransactionInput {
                            account_id: account_id.into(),
                            transaction_id,
                            token: None,
                            participants: vec![CoordinatorParticipant {
                                target: CoordinatorParticipantTarget::Account,
                                operations: vec![IndexedTransactionOperation {
                                    index: 0,
                                    operation: TransactionOperation::Read(GetItemInput {
                                        table_name: "Unprepared".into(),
                                        table_id: "not-prepared".into(),
                                        key: Default::default(),
                                    }),
                                }],
                            }],
                        },
                    )),
                )
                .await?;
            let read = ReadCrossCellTransactionInput {
                account_id: account_id.into(),
                transaction_id,
                routing_key: transaction_id.to_vec(),
            };
            clock.store(600_999, Ordering::SeqCst);
            assert!(
                !client
                    .command::<AbortExpiredTransaction>(
                        &coordinator,
                        mutation(&clock),
                        Json(read.clone())
                    )
                    .await?
                    .output
                    .0
            );
            assert_eq!(
                client
                    .query::<ReadCrossCellTransaction>(&coordinator, None, Json(read.clone()))
                    .await?
                    .output
                    .0
                    .unwrap()
                    .decision,
                CoordinatorDecision::Begin
            );
            clock.store(601_000, Ordering::SeqCst);
            assert!(
                client
                    .command::<AbortExpiredTransaction>(
                        &coordinator,
                        mutation(&clock),
                        Json(read.clone())
                    )
                    .await?
                    .output
                    .0
            );
            let abort = CoordinatorDecision::Abort {
                index: None,
                reason: None,
            };
            assert_eq!(
                client
                    .query::<ReadCrossCellTransaction>(&coordinator, None, Json(read.clone()))
                    .await?
                    .output
                    .0
                    .unwrap()
                    .decision,
                abort
            );
            assert!(
                !client
                    .command::<AbortExpiredTransaction>(
                        &coordinator,
                        mutation(&clock),
                        Json(read.clone())
                    )
                    .await?
                    .output
                    .0
            );
            stop(node).await?;
            drop(client);
            let (node, client) = start(
                directory.path(),
                clock.clone(),
                host.handle(),
                core.handle(),
            )
            .await?;
            assert_eq!(
                client
                    .query::<ReadCrossCellTransaction>(&coordinator, None, Json(read))
                    .await?
                    .output
                    .0
                    .unwrap()
                    .decision,
                abort
            );
            stop(node).await
        })
    }
}
