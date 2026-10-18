use crate::{local_node, transaction_fixture};
use bdb_cellar_runtime::{MutationIdentity, client::CellClient, identity::CellTarget};
use beyonddb::*;
use extenddb_core::{
    expression::{Expr, ExpressionMaps, KeyCondition, PathElement},
    types::{AttributeValue, Item, TableKeyInfo},
};
use extenddb_storage::{DataEngine, TableEngine, error::StorageError};
use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicI64},
};
fn identity(_: u8) -> MutationIdentity {
    local_node::wall_mutation()
}
fn mutation() -> MutationIdentity {
    local_node::wall_mutation()
}
pub(super) async fn assert_reads_finish_terminal_decisions(
    client: &CellClient,
    storage: &CellStorage,
    provisioner: &CellInitialPartitionProvisioner,
    infos: &[TableKeyInfo; 2],
) {
    let account_id = &infos[0].account_id;
    for (index, (read_side, scan, commit)) in [
        (0, false, true),
        (1, false, true),
        (0, true, true),
        (1, true, true),
        (0, false, false),
        (1, false, false),
        (0, true, false),
        (1, true, false),
    ]
    .into_iter()
    .enumerate()
    {
        // A token differs from the transaction ID, so participant recovery must
        // retain the routing key rather than guess its coordinator from the ID.
        let token = format!("read-help-{index}");
        let id = *uuid::Uuid::now_v7().as_bytes();
        let key = Item::from([("id".into(), AttributeValue::S(token.clone()))]);
        provisioner
            .ensure(client, account_id, token.as_bytes())
            .await
            .unwrap();
        let coordinator = coordinator_target(account_id, token.as_bytes()).unwrap();
        let mut participants = Vec::new();
        for (position, info) in infos.iter().enumerate() {
            let (target, participant) =
                transaction_fixture::participant_for_key(client, info, &key).await;
            participants.push((
                target,
                CoordinatorParticipant {
                    target: participant,
                    operations: vec![IndexedTransactionOperation {
                        index: position as u8,
                        operation: TransactionOperation::Put(PutItemInput {
                            table_name: info.table_name.clone(),
                            table_id: info.table_id.clone(),
                            item: key.clone(),
                            condition: None,
                        }),
                    }],
                },
            ));
        }
        participants.sort_by_key(|(target, _)| *target.cell_id().as_bytes());
        transaction_command!(
            client,
            BeginCrossCellTransaction,
            &coordinator,
            mutation(),
            Json(BeginCrossCellTransactionInput {
                account_id: account_id.clone(),
                transaction_id: id,
                token: Some(TransactionToken {
                    account_id: account_id.clone(),
                    token: token.clone(),
                    fingerprint: token.clone(),
                }),
                participants: participants
                    .iter()
                    .map(|(_, participant)| participant.clone())
                    .collect(),
            }),
        )
        .await
        .unwrap();
        for (position, (target, participant)) in participants.iter().enumerate() {
            let operations = participant
                .operations
                .iter()
                .map(|op| op.operation.clone())
                .collect();
            let prepared = match &participant.target {
                CoordinatorParticipantTarget::Account => transaction_command!(
                    client,
                    PrepareAccountTransaction,
                    target,
                    mutation(),
                    Json(PrepareAccountTransactionInput {
                        transaction_id: id,
                        coordinator_cell: *coordinator.cell_id().as_bytes(),
                        coordinator_key: token.as_bytes().to_vec(),
                        operations,
                    }),
                )
                .await
                .unwrap(),
                CoordinatorParticipantTarget::Data {
                    table_id, epoch, ..
                } => transaction_command!(
                    client,
                    PreparePartitionTransaction,
                    target,
                    mutation(),
                    Json(PreparePartitionTransactionInput {
                        table_id: table_id.clone(),
                        epoch: *epoch,
                        transaction_id: id,
                        coordinator_cell: *coordinator.cell_id().as_bytes(),
                        coordinator_key: token.as_bytes().to_vec(),
                        operations,
                    }),
                )
                .await
                .unwrap(),
            };
            client
                .command::<RecordParticipantPrepare>(
                    &coordinator,
                    mutation(),
                    Json(CoordinatorPhaseInput {
                        account_id: account_id.clone(),
                        transaction_id: id,
                        routing_key: token.as_bytes().to_vec(),
                        position: position as u8,
                        participant_cell: *target.cell_id().as_bytes(),
                        sequence: prepared.receipt.commit_sequence,
                    }),
                )
                .await
                .unwrap();
        }
        let read = || async {
            if scan {
                storage
                    .scan(&infos[read_side], None, None, None, None, None)
                    .await
                    .map(|(items, _)| items.contains(&key))
            } else {
                storage
                    .get_item(&infos[read_side], &key)
                    .await
                    .map(|item| item == Some(key.clone()))
            }
        };
        assert!(
            matches!(read().await, Err(StorageError::Transient(_))),
            "BEGIN must not be guessed or completed by a read"
        );
        let decision = if commit {
            CoordinatorDecision::Commit
        } else {
            CoordinatorDecision::Abort {
                index: None,
                reason: None,
            }
        };
        client
            .command::<DecideCrossCellTransaction>(
                &coordinator,
                mutation(),
                Json(DecideCrossCellTransactionInput {
                    account_id: account_id.clone(),
                    transaction_id: id,
                    routing_key: token.as_bytes().to_vec(),
                    decision: decision.clone(),
                }),
            )
            .await
            .unwrap();
        // No worker or explicit resolver runs: this read must finish both owners.
        assert_eq!(read().await.unwrap(), commit);
        let status = client
            .query::<ReadCrossCellTransaction>(
                &coordinator,
                None,
                Json(ReadCrossCellTransactionInput {
                    account_id: account_id.clone(),
                    transaction_id: id,
                    routing_key: token.as_bytes().to_vec(),
                }),
            )
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        assert_eq!(status.decision, decision);
        assert_eq!(status.resolved_count, 2);
        for info in infos {
            assert_eq!(
                storage.get_item(info, &key).await.unwrap(),
                commit.then(|| key.clone())
            );
        }
    }
}

#[test]
fn query_resolves_committed_create_in_intent_range() {
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
    let account_id = "123456789012";
    let directory = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(mutation().issued_at_ms));
    let (node, client) = local_node::start(directory.path(), clock, host.handle(), core.handle()).await.unwrap();
    let provisioner = Arc::new(CellInitialPartitionProvisioner::new(client.clone(), local_node::application().unwrap()).with_initial_partition_count(2).unwrap());
    let storage = CellStorage::new(client.clone(), "local").with_initial_partitions(provisioner.clone()).with_transaction_coordinators(provisioner.clone());
    let table = storage.create_table(account_id, serde_json::from_value(serde_json::json!({
        "TableName": "ReadResolution", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "N"}], "BillingMode": "PAY_PER_REQUEST"
    })).unwrap()).await.unwrap();
    let info = storage
        .table_key_info(account_id, &table.table_name)
        .await
        .unwrap();
    let created = Item::from([("pk".into(), AttributeValue::S("same".into())), ("sk".into(), AttributeValue::N("9".into()))]);
    let (target, participant) = transaction_fixture::participant_for_key(&client, &info, &created).await;
    let CoordinatorParticipantTarget::Data {epoch, ..} = participant else {panic!("expected routed table")};
    assert_query_finishes_commit(
        &storage,
        &client,
        &target,
        epoch,
        &info,
        &provisioner,
    )
    .await;
    local_node::stop(node).await.unwrap();
    });
}

async fn assert_query_finishes_commit(
    storage: &CellStorage,
    client: &CellClient,
    target: &CellTarget,
    epoch: u64,
    key_info: &TableKeyInfo,
    provisioner: &CellInitialPartitionProvisioner,
) {
    let created = Item::from([
        ("pk".into(), AttributeValue::S("same".into())),
        ("sk".into(), AttributeValue::N("9".into())),
    ]);
    let condition = KeyCondition {
        pk_path: vec![PathElement::Attribute("pk".into())],
        pk_value: Expr::Placeholder("p".into()),
        extra_pk_conditions: vec![],
        sk_condition: None,
        extra_sk_conditions: vec![],
    };
    let maps = ExpressionMaps::new(
        HashMap::new(),
        HashMap::from([("p".into(), AttributeValue::S("same".into()))]),
    );
    let id = [249; 16];
    provisioner
        .ensure(client, &key_info.account_id, &id)
        .await
        .unwrap();
    let coordinator = coordinator_target(&key_info.account_id, &id).unwrap();
    let (owner, target_participant) =
        transaction_fixture::participant_for_key(client, key_info, &created).await;
    assert_eq!(&owner, target);
    let participant = CoordinatorParticipant {
        target: target_participant,
        operations: vec![IndexedTransactionOperation {
            index: 0,
            operation: TransactionOperation::Put(PutItemInput {
                table_name: key_info.table_name.clone(),
                table_id: key_info.table_id.clone(),
                item: created.clone(),
                condition: None,
            }),
        }],
    };
    transaction_command!(
        client,
        BeginCrossCellTransaction,
        &coordinator,
        identity(249),
        Json(BeginCrossCellTransactionInput {
            account_id: key_info.account_id.clone(),
            transaction_id: id,
            token: None,
            participants: vec![participant.clone()],
        }),
    )
    .await
    .unwrap();
    let prepared = transaction_command!(
        client,
        PreparePartitionTransaction,
        target,
        identity(249),
        Json(PreparePartitionTransactionInput {
            table_id: key_info.table_id.clone(),
            epoch,
            transaction_id: id,
            coordinator_cell: *coordinator.cell_id().as_bytes(),
            coordinator_key: id.to_vec(),
            operations: participant
                .operations
                .into_iter()
                .map(|op| op.operation)
                .collect(),
        }),
    )
    .await
    .unwrap();
    client
        .command::<RecordParticipantPrepare>(
            &coordinator,
            identity(250),
            Json(CoordinatorPhaseInput {
                account_id: key_info.account_id.clone(),
                transaction_id: id,
                routing_key: id.to_vec(),
                position: 0,
                participant_cell: *target.cell_id().as_bytes(),
                sequence: prepared.receipt.commit_sequence,
            }),
        )
        .await
        .unwrap();
    client
        .command::<DecideCrossCellTransaction>(
            &coordinator,
            identity(251),
            Json(DecideCrossCellTransactionInput {
                account_id: key_info.account_id.clone(),
                transaction_id: id,
                routing_key: id.to_vec(),
                decision: CoordinatorDecision::Commit,
            }),
        )
        .await
        .unwrap();
    let resolved = storage
        .query(key_info, &condition, &maps, true, None, None, None)
        .await
        .unwrap()
        .0;
    assert!(
        resolved.contains(&created),
        "Query must resolve the committed create absent from live rows"
    );
    let status = client
        .query::<ReadCrossCellTransaction>(
            &coordinator,
            None,
            Json(ReadCrossCellTransactionInput {
                account_id: key_info.account_id.clone(),
                transaction_id: id,
                routing_key: id.to_vec(),
            }),
        )
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert_eq!(status.resolved_count, 1);
}
