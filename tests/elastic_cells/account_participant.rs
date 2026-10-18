use crate::{cellar_node, local_node, transaction_fixture};
use beyonddb::*;
use cellar_runtime::{MutationIdentity, client::InvocationError};
use extenddb_core::{
    expression::{Expr, ExpressionMaps, KeyCondition, PathElement, UpdateAction},
    types::{AttributeValue, Item},
};
use extenddb_storage::{
    DataEngine, TableEngine, TransactGetOp, TransactWriteOp, error::StorageError,
};
use std::{collections::HashMap, sync::Arc};
fn mutation() -> MutationIdentity {
    local_node::wall_mutation()
}

fn key(value: &str) -> Item {
    Item::from([("id".into(), AttributeValue::S(value.into()))])
}

fn put(table: &TableRecord, value: &str) -> TransactionOperation {
    TransactionOperation::Put(PutItemInput {
        table_name: table.table_name.clone(),
        table_id: table.id.clone(),
        item: key(value),
        condition: None,
    })
}

#[test]
fn mixed_participants_preserve_locks_and_finish_after_owner_restart() {
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
    let application = local_node::application().unwrap();
    let account_id = "123456789012";
    let account = account_target(account_id).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite")).unwrap();
    let source_dir = directory.path().join("mixed-owner-a");
    let (node, client) = cellar_node::start(application.clone(), &source_dir, host.handle(), core.handle(), bucket.clone()).await.unwrap();
    let provisioner = Arc::new(CellInitialPartitionProvisioner::new(client.clone(), application.clone()).with_initial_partition_count(2).unwrap());
    let storage = CellStorage::new(client.clone(), "local").with_initial_partitions(provisioner.clone()).with_transaction_coordinators(provisioner);
    let mut tables = Vec::new();
    for name in ["AccountItems", "DataItems", "OtherItems", "EmptyItems"] {
        if name == "DataItems" {
            storage.create_table(account_id, serde_json::from_value(serde_json::json!({
                "TableName": name, "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"
            })).unwrap()).await.unwrap();
        } else {
            let result = client.command::<CreateTable>(&account, mutation(), Json(transaction_fixture::account_spec(name))).await.unwrap();
            assert!(matches!(result.output.0, CreateTableOutcome::Created(_)));
        }
        tables.push(client.query::<DescribeTable>(&account, None, Json(name.into())).await.unwrap().output.0.unwrap());
    }
    let table = &tables[0];
    let data_table = &tables[1];
    let empty_table = &tables[3];
    let data_info = storage.table_key_info(account_id, &data_table.table_name).await.unwrap();
    let (data, participant) = transaction_fixture::participant_for_key(&client, &data_info, &key("created")).await;
    let CoordinatorParticipantTarget::Data {partition_id, epoch, ..} = participant else {unreachable!()};
    assert_eq!(epoch, 1);
    let info = storage
        .table_key_info(account_id, &table.table_name)
        .await
        .unwrap();
    let maps = ExpressionMaps::default();
    for value in ["deleted", "checked", "updated"] {
        storage
            .put_item(&info, key(value), false, None, &maps, None)
            .await
            .unwrap();
    }
    let operations = vec![
        put(table, "created"),
        TransactionOperation::Delete(DeleteItemInput { return_old: false, table_name: table.table_name.clone(), table_id: table.id.clone(), key: key("deleted"), condition: None }),
        serde_json::from_value(serde_json::json!({"ConditionCheck": {
            "table_name": table.table_name, "table_id": table.id, "key": key("checked"),
            "condition": {"expression": {"Function": {"name": "attribute_exists", "args": [{"Path": [{"Attribute": "id"}]}]}}, "maps": {"names": {}, "values": {}}}
        }})).unwrap(),
        serde_json::from_value(serde_json::json!({"Update": {
            "table_name": table.table_name, "table_id": table.id, "key": key("updated"), "condition": null,
            "update": {"actions": [{"Set": {"path": [{"Attribute": "value"}], "value": {"Placeholder": "v"}}}], "maps": {"names": {}, "values": {"v": {"S": "new"}}}}
        }})).unwrap(),
        put(empty_table, "created"),
    ];
    let transaction_id = [213; 16];
    let coordinator = coordinator_target(account_id, &transaction_id).unwrap();
    let account_participant = CoordinatorParticipant {
        target: CoordinatorParticipantTarget::Account,
        operations: operations
            .iter()
            .enumerate()
            .map(|(index, operation)| IndexedTransactionOperation {
                index: u8::try_from(index).unwrap(),
                operation: operation.clone(),
            })
            .collect(),
    };
    let data_participant = CoordinatorParticipant {
        target: CoordinatorParticipantTarget::Data {
            table_id: data_table.id.clone(),
            partition_id,
            epoch: 1,
        },
        operations: vec![IndexedTransactionOperation {
            index: 5,
            operation: put(data_table, "created"),
        }],
    };
    let mut participants = vec![
        (account.cell_id(), account_participant),
        (data.cell_id(), data_participant),
    ];
    participants.sort_by_key(|(cell, _)| *cell.as_bytes());
    transaction_command!(
        client,
        BeginCrossCellTransaction,
        &coordinator,
        mutation(),
        Json(BeginCrossCellTransactionInput {
            account_id: account_id.into(),
            transaction_id,
            token: None,
            participants: participants
                .into_iter()
                .map(|(_, participant)| participant)
                .collect(),
        }),
    )
    .await
    .unwrap();
    let prepare = PrepareAccountTransactionInput {
        transaction_id,
        coordinator_cell: *coordinator.cell_id().as_bytes(),
        coordinator_key: transaction_id.to_vec(),
        operations,
    };
    assert_eq!(
        transaction_command!(
            client,
            PrepareAccountTransaction,
            &account,
            mutation(),
            Json(prepare.clone())
        )
        .await
        .unwrap()
        .output
        .0,
        PrepareTransactionOutcome::Prepared
    );
    assert!(
        matches!(transaction_command!(client, PrepareAccountTransaction,&account, mutation(), Json(prepare.clone())).await,
        Err(InvocationError::Rejected(result)) if result.output.0 == PrepareTransactionOutcome::Replay)
    );
    let mut mismatch = prepare.clone();
    mismatch.operations[0] = put(table, "different");
    assert!(
        matches!(transaction_command!(client, PrepareAccountTransaction,&account, mutation(), Json(mismatch)).await,
        Err(InvocationError::Rejected(result)) if result.output.0 == PrepareTransactionOutcome::Mismatch)
    );
    for value in ["created", "deleted", "checked", "updated"] {
        assert!(matches!(
            storage.get_item(&info, &key(value)).await,
            Err(StorageError::Transient(_))
        ));
        assert!(matches!(
            storage
                .put_item(&info, key(value), false, None, &maps, None)
                .await,
            Err(StorageError::TransactionConflict(_))
        ));
        assert!(matches!(
            storage
                .delete_item(&info, &key(value), false, None, &maps, None)
                .await,
            Err(StorageError::TransactionConflict(_))
        ));
    }
    let update = [UpdateAction::Set {
        path: vec![PathElement::Attribute("other".into())],
        value: Expr::Placeholder("v".into()),
    }];
    let update_maps = ExpressionMaps::new(
        HashMap::new(),
        HashMap::from([("v".into(), AttributeValue::S("overwrite".into()))]),
    );
    assert!(matches!(
        storage
            .update_item(
                &info,
                &key("updated"),
                &update,
                false,
                false,
                None,
                &update_maps,
                None
            )
            .await,
        Err(StorageError::TransactionConflict(_))
    ));
    assert!(matches!(storage.transact_get_items(&[
        TransactGetOp { key_info: &info, key: &key("absent") },
        TransactGetOp { key_info: &info, key: &key("created") },
    ]).await, Err(StorageError::TransactionCanceled(reasons)) if reasons[0].code == "None" && reasons[1].code == "TransactionConflict"));
    assert!(matches!(storage.transact_write_items(&[
        TransactWriteOp::Put { key_info: &info, item: &key("rollback"), condition: None, maps: &maps, return_values_on_ccf: Default::default(), stream: None },
        TransactWriteOp::Put { key_info: &info, item: &key("created"), condition: None, maps: &maps, return_values_on_ccf: Default::default(), stream: None },
    ], None).await, Err(StorageError::TransactionCanceled(reasons)) if reasons[1].code == "TransactionConflict"));
    assert_eq!(
        storage.get_item(&info, &key("rollback")).await.unwrap(),
        None
    );
    assert!(matches!(
        storage.scan(&info, None, None, None, None, None).await,
        Err(StorageError::Transient(_))
    ));
    assert_eq!(
        storage
            .scan(&info, None, Some(&key("zz")), None, None, None)
            .await
            .unwrap(),
        (vec![], None)
    );
    let query = KeyCondition {
        pk_path: vec![PathElement::Attribute("id".into())],
        pk_value: Expr::Placeholder("id".into()),
        extra_pk_conditions: vec![],
        sk_condition: None,
        extra_sk_conditions: vec![],
    };
    let query_maps = ExpressionMaps::new(
        HashMap::new(),
        HashMap::from([("id".into(), AttributeValue::S("created".into()))]),
    );
    assert!(matches!(
        storage
            .query(&info, &query, &query_maps, true, None, None, None)
            .await,
        Err(StorageError::Transient(_))
    ));
    let other = storage
        .table_key_info(account_id, "OtherItems")
        .await
        .unwrap();
    storage
        .put_item(&other, key("created"), false, None, &maps, None)
        .await
        .unwrap();
    assert_eq!(
        storage.get_item(&other, &key("created")).await.unwrap(),
        Some(key("created"))
    );
    for name in ["AccountItems", "EmptyItems"] {
        let record = client
            .query::<beyonddb::DescribeTable>(&account, None, Json(name.into()))
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        assert!(
            matches!(client.command::<DeleteTable>(&account, mutation(), Json(beyonddb::TableGeneration { table_name: name.into(), table_id: record.id })).await,
            Err(InvocationError::Rejected(result)) if result.output.0 == DeleteTableOutcome::TransactionConflict)
        );
    }
    // Transfer every prepared cell and coordinator to a fresh SDK owner.
    let (restored, client) = cellar_node::start(application.clone(), &directory.path().join("mixed-owner-b"), host.handle(), core.handle(), bucket).await.unwrap();
    cellar_node::stop(node, cellar::sdk::ShutdownMode::Handoff).await.unwrap();
    std::fs::remove_dir_all(source_dir).unwrap();
    let storage = CellStorage::new(client.clone(), "us-east-1");
    assert!(matches!(
        storage.get_item(&info, &key("created")).await,
        Err(StorageError::Transient(_))
    ));
    assert_eq!(
        client
            .query::<ReadAccountTransaction>(
                &account,
                None,
                Json(ReadTransactionInput {
                    transaction_id,
                    coordinator_cell: *coordinator.cell_id().as_bytes(),
                })
            )
            .await
            .unwrap()
            .output
            .0,
        ParticipantTransactionState::Prepared
    );
    let (first, second) = tokio::join!(
        storage.resume_cross_cell_transaction(account_id, &transaction_id, transaction_id),
        storage.resume_cross_cell_transaction(account_id, &transaction_id, transaction_id),
    );
    assert_eq!(first.unwrap(), CoordinatorDecision::Commit);
    assert_eq!(second.unwrap(), CoordinatorDecision::Commit);
    assert_eq!(
        storage.get_item(&info, &key("created")).await.unwrap(),
        Some(key("created"))
    );
    assert_eq!(
        storage.get_item(&info, &key("deleted")).await.unwrap(),
        None
    );
    let mut updated = key("updated");
    updated.insert("value".into(), AttributeValue::S("new".into()));
    assert_eq!(
        storage.get_item(&info, &key("updated")).await.unwrap(),
        Some(updated)
    );
    assert_eq!(
        client
            .query::<PartitionGet>(
                &data,
                None,
                Json(PartitionGetInput {
                    table_id: data_table.id.clone(),
                    epoch: 1,
                    key: key("created"),
                })
            )
            .await
            .unwrap()
            .output
            .0,
        PartitionGetOutcome::Found(Some(key("created")))
    );
    let resolve = ResolveTransactionInput {
        transaction_id,
        coordinator_cell: *coordinator.cell_id().as_bytes(),
        commit: false,
    };
    assert!(
        matches!(client.command::<ResolveAccountTransaction>(&account, mutation(), Json(resolve.clone())).await,
        Err(InvocationError::Rejected(result)) if result.output.0 == ResolveTransactionOutcome::DecisionConflict)
    );
    let abort = ResolveTransactionInput {
        transaction_id: [215; 16],
        ..resolve
    };
    client
        .command::<ResolveAccountTransaction>(&account, mutation(), Json(abort.clone()))
        .await
        .unwrap();
    let delayed = PrepareAccountTransactionInput {
        transaction_id: abort.transaction_id,
        ..prepare
    };
    assert!(
        matches!(transaction_command!(client, PrepareAccountTransaction,&account, mutation(), Json(delayed)).await,
        Err(InvocationError::Rejected(result)) if result.output.0 == PrepareTransactionOutcome::Aborted)
    );

    // A mixed request whose account condition fails must leave its data write absent.
    let failed_id = [216; 16];
    let failed_coordinator = coordinator_target(account_id, &failed_id).unwrap();
    let mut failing = put(table, "created");
    let TransactionOperation::Put(input) = &mut failing else {
        unreachable!()
    };
    input.condition = Some(serde_json::from_value(serde_json::json!({
        "expression": {"Function": {"name": "attribute_not_exists", "args": [{"Path": [{"Attribute": "id"}]}]}}, "maps": {"names": {}, "values": {}}
    })).unwrap());
    let mut participants = vec![
        (
            account.cell_id(),
            CoordinatorParticipant {
                target: CoordinatorParticipantTarget::Account,
                operations: vec![IndexedTransactionOperation {
                    index: 1,
                    operation: failing,
                }],
            },
        ),
        (
            data.cell_id(),
            CoordinatorParticipant {
                target: CoordinatorParticipantTarget::Data {
                    table_id: data_table.id.clone(),
                    partition_id,
                    epoch: 1,
                },
                operations: vec![IndexedTransactionOperation {
                    index: 0,
                    operation: put(data_table, "rollback"),
                }],
            },
        ),
    ];
    participants.sort_by_key(|(cell, _)| *cell.as_bytes());
    transaction_command!(
        client,
        BeginCrossCellTransaction,
        &failed_coordinator,
        mutation(),
        Json(BeginCrossCellTransactionInput {
            account_id: account_id.into(),
            transaction_id: failed_id,
            token: None,
            participants: participants
                .into_iter()
                .map(|(_, participant)| participant)
                .collect(),
        }),
    )
    .await
    .unwrap();
    assert!(matches!(
        storage
            .resume_cross_cell_transaction(account_id, &failed_id, failed_id)
            .await
            .unwrap(),
        CoordinatorDecision::Abort {
            index: Some(1),
            reason: Some(TransactionFailure::ConditionFailed(Some(_)))
        }
    ));
    assert_eq!(
        client
            .query::<PartitionGet>(
                &data,
                None,
                Json(PartitionGetInput {
                    table_id: data_table.id.clone(),
                    epoch: 1,
                    key: key("rollback")
                })
            )
            .await
            .unwrap()
            .output
            .0,
        PartitionGetOutcome::Found(None)
    );
    // Aborting a prepared account participant preserves old images and releases
    // its item locks, including locks on missing rows.
    let prepared_abort = PrepareAccountTransactionInput {
        transaction_id: [217; 16],
        coordinator_cell: *coordinator.cell_id().as_bytes(),
        coordinator_key: vec![213; 16],
        operations: vec![
            put(table, "aborted-create"),
            TransactionOperation::Delete(DeleteItemInput {
                return_old: false,
                table_name: table.table_name.clone(),
                table_id: table.id.clone(),
                key: key("checked"),
                condition: None,
            }),
        ],
    };
    transaction_command!(
        client,
        PrepareAccountTransaction,
        &account,
        mutation(),
        Json(prepared_abort.clone())
    )
    .await
    .unwrap();
    let abort = ResolveTransactionInput {
        transaction_id: prepared_abort.transaction_id,
        coordinator_cell: prepared_abort.coordinator_cell,
        commit: false,
    };
    for _ in 0..2 {
        assert_eq!(
            client
                .command::<ResolveAccountTransaction>(&account, mutation(), Json(abort.clone()))
                .await
                .unwrap()
                .output
                .0,
            ResolveTransactionOutcome::Aborted
        );
    }
    assert_eq!(
        storage
            .get_item(&info, &key("aborted-create"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        storage.get_item(&info, &key("checked")).await.unwrap(),
        Some(key("checked"))
    );
    storage
        .put_item(&info, key("aborted-create"), false, None, &maps, None)
        .await
        .unwrap();
    let data_info = storage
        .table_key_info(account_id, &data_table.table_name)
        .await
        .unwrap();
    let admission = Arc::new(CellInitialPartitionProvisioner::new(client.clone(), application));
    super::public_transactions::assert_lost_replies_and_canceled_token_reuse(&client, admission, [info, data_info]).await;
    cellar_node::stop(restored, cellar::sdk::ShutdownMode::Preserve).await.unwrap();
    });
}
