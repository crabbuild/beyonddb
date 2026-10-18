//! Acknowledged transactional read cleanup across fresh embedded SDK owners.
use crate::{cellar_node, local_node, offline_sqlite, replica_snapshot, transaction_fixture};
use beyonddb::*;
use cellar_runtime::{
    MutationIdentity,
    client::{CellClient, InvocationError},
    identity::CellTarget,
};
use extenddb_core::{
    expression::ExpressionMaps,
    types::{AttributeValue, Item},
};
use extenddb_storage::{DataEngine, TransactGetOp};
use std::sync::Arc;

const ACCOUNT: &str = "123456789012";
fn mutation() -> MutationIdentity {
    local_node::wall_mutation()
}

#[test]
fn acknowledged_read_cleanup_survives_restart_before_and_after_participant_delete() {
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    host.block_on(async {
        for delete_first in [false, true] {
            cleanup_restart(delete_first, host.handle(), core.handle()).await;
        }
    });
}

async fn cleanup_restart(
    delete_first: bool,
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
) {
    let application = local_node::application().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let first_dir = directory.path().join("read-release-a");
    let bucket =
        cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite")).unwrap();
    let (node, client) =
        cellar_node::start(application.clone(), &first_dir, host, core, bucket.clone())
            .await
            .unwrap();
    let account = account_target(ACCOUNT).unwrap();
    let (provisioner, infos) = transaction_fixture::mixed(&client).await;
    let storage =
        CellStorage::new(client.clone(), "local").with_transaction_coordinators(provisioner);
    let key = Item::from([("id".into(), AttributeValue::S("saved".into()))]);
    let mut participants = Vec::new();
    for (index, info) in infos.iter().enumerate() {
        storage
            .put_item(
                info,
                key.clone(),
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .unwrap();
        let (target, participant) =
            transaction_fixture::participant_for_key(&client, info, &key).await;
        participants.push((
            target,
            CoordinatorParticipant {
                target: participant,
                operations: vec![IndexedTransactionOperation {
                    index: index as u8,
                    operation: TransactionOperation::Read(GetItemInput {
                        table_name: info.table_name.clone(),
                        table_id: info.table_id.clone(),
                        key: key.clone(),
                    }),
                }],
            },
        ));
    }
    participants.sort_by_key(|(target, _)| *target.cell_id().as_bytes());
    let reads = infos
        .iter()
        .map(|info| TransactGetOp {
            key_info: info,
            key: &key,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        storage.transact_get_items(&reads).await.unwrap(),
        vec![Some(key.clone()), Some(key.clone())]
    );
    let second_dir = directory.path().join("read-release-b");
    let (successor, next) =
        cellar_node::start(application.clone(), &second_dir, host, core, bucket.clone())
            .await
            .unwrap();
    cellar_node::stop(node, cellar::sdk::ShutdownMode::Handoff)
        .await
        .unwrap();
    // Handoff removes local replica files. Restore inspection copies through
    // the SDK instead of opening an owner's live sparse cache.
    let inspection = replica_snapshot::restore(&bucket, host).await;
    offline_sqlite::assert_read_images_collected(inspection.path());
    std::fs::remove_dir_all(first_dir).unwrap();
    let node = successor;
    let client = next;
    let provisioner = Arc::new(
        CellInitialPartitionProvisioner::new(client.clone(), application.clone())
            .with_initial_partition_count(2)
            .unwrap(),
    );
    let storage = CellStorage::new(client.clone(), "local")
        .with_transaction_coordinators(provisioner.clone());
    let id = [251; 16];
    provisioner.ensure(&client, ACCOUNT, &id).await.unwrap();
    let coordinator = coordinator_target(ACCOUNT, &id).unwrap();
    let read = ReadCrossCellTransactionInput {
        account_id: ACCOUNT.into(),
        transaction_id: id,
        routing_key: id.to_vec(),
    };
    transaction_command!(
        client,
        BeginCrossCellTransaction,
        &coordinator,
        mutation(),
        Json(BeginCrossCellTransactionInput {
            account_id: ACCOUNT.into(),
            transaction_id: id,
            token: None,
            participants: participants.iter().map(|(_, p)| p.clone()).collect(),
        })
    )
    .await
    .unwrap();
    assert!(matches!(
        client
            .command::<BeginReadResultRelease>(&coordinator, mutation(), Json(read.clone()))
            .await,
        Err(InvocationError::Rejected(_))
    ));
    storage
        .resume_cross_cell_transaction(ACCOUNT, &id, id)
        .await
        .unwrap();
    for (target, _) in &participants {
        assert_eq!(
            saved(&client, target, &coordinator, id).await,
            TransactionReadResult::Item(Some(key.clone()))
        );
    }
    // A mismatched coordinator cannot discard another reader's saved image.
    assert!(matches!(
        client
            .command::<ReleaseAccountTransactionReads>(
                &account,
                mutation(),
                Json(ReadTransactionInput {
                    transaction_id: id,
                    coordinator_cell: [0; 32],
                })
            )
            .await,
        Err(InvocationError::Rejected(_))
    ));
    // Visit every registered shard while the committed read has no cleanup
    // acknowledgement. A later acknowledgement must become discoverable even
    // after recovery has already observed this coordinator as settled.
    let later_key = (0_u32..100_000)
        .map(u32::to_be_bytes)
        .find(|key| coordinator_target(ACCOUNT, key).unwrap().partition() > coordinator.partition())
        .unwrap();
    provisioner
        .ensure(&client, ACCOUNT, &later_key)
        .await
        .unwrap();
    let mut recovery = TransactionRecovery::new(Arc::new(storage), ACCOUNT.into()).unwrap();
    let shards = client
        .query::<ListCoordinatorShards>(
            &account,
            None,
            Json(ListCoordinatorShardsInput {
                account_id: ACCOUNT.into(),
                after: None,
                limit: 100,
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    for _ in 0..=shards.len() {
        recovery.step().await.unwrap();
    }
    assert!(
        client
            .query::<ReadPendingTransactionBoundary>(&coordinator, None, Json(()))
            .await
            .unwrap()
            .output
            .0
            .is_none()
    );
    for (target, _) in &participants {
        assert_eq!(
            saved(&client, target, &coordinator, id).await,
            TransactionReadResult::Item(Some(key.clone()))
        );
    }
    client
        .command::<BeginReadResultRelease>(&coordinator, mutation(), Json(read.clone()))
        .await
        .unwrap();
    let pending = client
        .query::<ReadPendingCrossCellTransactions>(
            &coordinator,
            None,
            Json(ReadPendingCrossCellTransactionsInput {
                after: None,
                limit: 100,
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    assert_eq!(
        pending.iter().map(|p| p.transaction_id).collect::<Vec<_>>(),
        vec![id]
    );
    if delete_first {
        client
            .command::<ReleaseAccountTransactionReads>(
                &account,
                mutation(),
                Json(ReadTransactionInput {
                    transaction_id: id,
                    coordinator_cell: *coordinator.cell_id().as_bytes(),
                }),
            )
            .await
            .unwrap();
        assert_eq!(
            saved(&client, &account, &coordinator, id).await,
            TransactionReadResult::Unavailable
        );
        // Deliberately omit the coordinator receipt, as if the response was lost.
    }
    let (restored, restored_client) = cellar_node::start(
        application.clone(),
        &directory.path().join("read-release-c"),
        host,
        core,
        bucket.clone(),
    )
    .await
    .unwrap();
    cellar_node::stop(node, cellar::sdk::ShutdownMode::Handoff)
        .await
        .unwrap();
    std::fs::remove_dir_all(second_dir).unwrap();
    let client = restored_client;
    // Inspect the recovered record before a worker or public read can help it.
    let recovered = client
        .query::<ReadCrossCellTransaction>(&coordinator, None, Json(read.clone()))
        .await
        .unwrap();
    assert_eq!(recovered.output.0.unwrap().unreleased_read_results, 2);
    for (target, _) in &participants {
        assert_eq!(
            saved(&client, target, &coordinator, id).await,
            if delete_first && *target == account {
                TransactionReadResult::Unavailable
            } else {
                TransactionReadResult::Item(Some(key.clone()))
            }
        );
    }
    let mut recovery = TransactionRecovery::new(
        Arc::new(CellStorage::new(client.clone(), "local")),
        ACCOUNT.into(),
    )
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            recovery.step().await.unwrap();
            let status = client
                .query::<ReadCrossCellTransaction>(&coordinator, None, Json(read.clone()))
                .await
                .unwrap()
                .output
                .0
                .unwrap();
            if status.unreleased_read_results == 0 {
                break;
            }
        }
    })
    .await
    .unwrap();
    let status = client
        .query::<ReadCrossCellTransaction>(&coordinator, None, Json(read.clone()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert_eq!(status.unreleased_read_results, 0);
    assert_eq!(status.decision, CoordinatorDecision::Commit);
    for (position, (target, participant)) in participants.iter().enumerate() {
        assert_eq!(
            saved(&client, target, &coordinator, id).await,
            TransactionReadResult::Unavailable
        );
        assert!(
            client
                .query::<ReadCoordinatorParticipant>(
                    &coordinator,
                    None,
                    Json(ReadCoordinatorParticipantInput {
                        account_id: ACCOUNT.into(),
                        transaction_id: id,
                        routing_key: id.to_vec(),
                        position: position as u8,
                        chunk: 0,
                    })
                )
                .await
                .unwrap()
                .output
                .is_none()
        );
        let operations = participant
            .operations
            .iter()
            .map(|op| op.operation.clone())
            .collect();
        let replay = match &participant.target {
            CoordinatorParticipantTarget::Account => {
                transaction_command!(
                    client,
                    beyonddb::PrepareAccountTransaction,
                    target,
                    mutation(),
                    Json(beyonddb::PrepareAccountTransactionInput {
                        transaction_id: id,
                        coordinator_cell: *coordinator.cell_id().as_bytes(),
                        coordinator_key: id.to_vec(),
                        operations,
                    })
                )
                .await
            }
            CoordinatorParticipantTarget::Data {
                table_id, epoch, ..
            } => {
                transaction_command!(
                    client,
                    PreparePartitionTransaction,
                    target,
                    mutation(),
                    Json(PreparePartitionTransactionInput {
                        table_id: table_id.clone(),
                        epoch: *epoch,
                        transaction_id: id,
                        coordinator_cell: *coordinator.cell_id().as_bytes(),
                        coordinator_key: id.to_vec(),
                        operations,
                    })
                )
                .await
            }
        };
        assert!(
            matches!(replay, Err(InvocationError::Rejected(result)) if result.output.0 == PrepareTransactionOutcome::Committed)
        );
        assert_eq!(
            saved(&client, target, &coordinator, id).await,
            TransactionReadResult::Unavailable
        );
    }
    client
        .command::<BeginReadResultRelease>(&coordinator, mutation(), Json(read))
        .await
        .unwrap();
    assert!(
        client
            .query::<beyonddb::ReadPendingTransactionBoundary>(&coordinator, None, Json(()))
            .await
            .unwrap()
            .output
            .0
            .is_none()
    );
    cellar_node::stop(restored, cellar::sdk::ShutdownMode::Preserve)
        .await
        .unwrap();
    let inspection = replica_snapshot::restore(&bucket, host).await;
    offline_sqlite::assert_read_images_collected(inspection.path());
    offline_sqlite::assert_coordinator_write_payloads_collected(inspection.path(), &[id]);
}

async fn saved(
    client: &CellClient,
    target: &CellTarget,
    coordinator: &CellTarget,
    id: [u8; 16],
) -> TransactionReadResult {
    let input = Json(ReadTransactionResultInput {
        transaction: ReadTransactionInput {
            transaction_id: id,
            coordinator_cell: *coordinator.cell_id().as_bytes(),
        },
        position: 0,
    });
    if *target == account_target(ACCOUNT).unwrap() {
        client
            .query::<ReadAccountTransactionResult>(target, None, input)
            .await
            .unwrap()
            .output
            .0
    } else {
        client
            .query::<ReadPartitionTransactionResult>(target, None, input)
            .await
            .unwrap()
            .output
            .0
    }
}
