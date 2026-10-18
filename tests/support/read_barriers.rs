//! Shared ordered-read and prepared-intent contracts over native SDK cells.
use crate::local_node;
use aws_sdk_dynamodb::types::AttributeValue as AwsAttributeValue;
use bdb_cellar_runtime::{
    MutationIdentity,
    client::{CellClient, InvocationError},
    identity::CellTarget,
};
use beyonddb::*;
use extenddb_core::{
    expression::{Expr, ExpressionMaps, KeyCondition, PathElement},
    types::{AttributeValue, Item, TableKeyInfo},
};
use extenddb_storage::{DataEngine, TransactGetOp, error::StorageError};
use std::collections::HashMap;
fn identity(_: u8) -> MutationIdentity {
    local_node::wall_mutation()
}

pub(crate) async fn assert_range_read_barriers(
    storage: &CellStorage,
    client: &CellClient,
    target: &CellTarget,
    epoch: u64,
    key_info: &TableKeyInfo,
) {
    let key = |pk: &str, sk: &str| {
        // Typed cell envelopes require canonical AttributeValues. Use the
        // same numeric normalization as the DynamoDB frontend deserializer.
        serde_json::from_value::<Item>(serde_json::json!({"pk":{"S":pk},"sk":{"N":sk}})).unwrap()
    };
    let created = key("same", "9");
    let deleted = key("same", "2");
    let untouched = key("same", "1.5");
    let transaction_id = [201; 16];
    let coordinator = prepare_blocker(
        client,
        target,
        key_info,
        epoch,
        transaction_id,
        vec![
            TransactionOperation::Put(PutItemInput {
                table_name: key_info.table_name.clone(),
                table_id: key_info.table_id.clone(),
                item: created.clone(),
                condition: None,
            }),
            TransactionOperation::Delete(DeleteItemInput {
                return_old: false,
                table_name: key_info.table_name.clone(),
                table_id: key_info.table_id.clone(),
                key: deleted.clone(),
                condition: None,
            }),
        ],
    )
    .await;
    let coordinator_cell = *coordinator.cell_id().as_bytes();
    let source = client
        .query::<ReadPartitionState>(target, None, Json(()))
        .await
        .unwrap()
        .output
        .0
        .unwrap()
        .spec;
    let seal = PartitionSeal {
        table_id: key_info.table_id.clone(),
        source_partition_id: source.partition_id,
        epoch,
        next_epoch: epoch.checked_add(1).unwrap(),
        source_lower: source.lower,
        source_upper: source.upper,
        boundary: ordered_data_key(&key_info.table_id, &created, &key_info.base_key_schema)
            .unwrap(),
        left_partition_id: *uuid::Uuid::now_v7().as_bytes(),
        right_partition_id: *uuid::Uuid::now_v7().as_bytes(),
    };
    assert!(
        matches!(client.command::<SealPartition>(target, identity(205), Json(seal)).await,
        Err(InvocationError::Rejected(result)) if result.output.0 == SealPartitionOutcome::InFlightTransaction)
    );

    // Neither an absent create nor an existing delete may escape the barrier.
    for locked in [&created, &deleted] {
        assert!(matches!(
            storage.get_item(key_info, locked).await,
            Err(StorageError::Transient(_))
        ));
        let error = storage
            .transact_get_items(&[
                TransactGetOp {
                    key_info,
                    key: &untouched,
                },
                TransactGetOp {
                    key_info,
                    key: locked,
                },
            ])
            .await
            .unwrap_err();
        let StorageError::TransactionCanceled(reasons) = error else {
            panic!("unexpected read failure: {error:?}");
        };
        assert_eq!(
            reasons
                .iter()
                .map(|reason| reason.code.as_str())
                .collect::<Vec<_>>(),
            vec!["None", "TransactionConflict"]
        );
        assert!(reasons.iter().all(|reason| reason.item.is_none()));
    }
    assert_eq!(
        storage.get_item(key_info, &untouched).await.unwrap(),
        Some(untouched.clone())
    );
    assert!(matches!(
        storage
            .scan(key_info, Some(1), None, None, None, None)
            .await,
        Err(StorageError::Transient(_))
    ));
    // The scan continuation excludes already-visited locks, including itself.
    let last_locked = &created;
    assert!(
        storage
            .scan(key_info, None, Some(last_locked), None, None, None)
            .await
            .is_ok()
    );

    // Scan's cursor compares complete ordered components, including absent
    // creates and numerically equivalent sort-key spellings.
    for (cursor, conflicts) in [
        (key("same", "1.5"), true),
        (key("same", "2.0"), true),
        (key("same", "8"), true),
        (key("same", "9"), false),
        (key("same", "9.0"), false),
        (key("same", "10"), false),
        (key("zz", "0"), false),
        (key("aa", "0"), true),
    ] {
        let result = client
            .query::<PartitionScan>(
                target,
                None,
                Json(PartitionScanInput {
                    table_id: key_info.table_id.clone(),
                    index_name: None,
                    epoch,
                    limit: Some(1),
                    exclusive_start_key: Some(cursor.clone()),
                }),
            )
            .await
            .unwrap_or_else(|error| panic!("scan cursor {cursor:?}: {error:?}"))
            .output
            .0;
        if conflicts {
            assert!(
                matches!(result, PartitionScanOutcome::Conflict(conflict) if conflict.transaction.transaction_id == transaction_id),
                "cursor {cursor:?}"
            );
        } else {
            assert!(
                matches!(result, PartitionScanOutcome::Page { .. }),
                "cursor {cursor:?}: {result:?}"
            );
        }
    }

    let base = PartitionQueryInput {
        index_name: None,
        table_id: key_info.table_id.clone(),
        epoch,
        partition_key: Item::from([("pk".into(), AttributeValue::S("same".into()))]),
        sort: None,
        extra_range_equals: vec![],
        forward: true,
        limit: 1,
        exclusive_start_key: None,
    };
    let compare = |op, n: &str| {
        Some(SortPredicate::Compare {
            attribute: "sk".into(),
            op,
            value: serde_json::from_value(serde_json::json!({"N": n})).unwrap(),
        })
    };
    let cases = [
        ("whole HASH", base.clone(), true),
        (
            "absent create",
            PartitionQueryInput {
                index_name: None,
                sort: compare(SortComparison::Eq, "9"),
                ..base.clone()
            },
            true,
        ),
        (
            "delete with equivalent numeric key",
            PartitionQueryInput {
                index_name: None,
                sort: compare(SortComparison::Eq, "2.0"),
                ..base.clone()
            },
            true,
        ),
        (
            "disjoint sort range",
            PartitionQueryInput {
                index_name: None,
                sort: compare(SortComparison::Lt, "2"),
                ..base.clone()
            },
            false,
        ),
        (
            "different HASH",
            PartitionQueryInput {
                index_name: None,
                partition_key: Item::from([("pk".into(), AttributeValue::S("other".into()))]),
                ..base.clone()
            },
            false,
        ),
        (
            "forward before intent",
            PartitionQueryInput {
                index_name: None,
                exclusive_start_key: Some(key("same", "8")),
                ..base.clone()
            },
            true,
        ),
        (
            "forward after intent",
            PartitionQueryInput {
                index_name: None,
                exclusive_start_key: Some(created.clone()),
                ..base.clone()
            },
            false,
        ),
        (
            "reverse before intent",
            PartitionQueryInput {
                index_name: None,
                forward: false,
                exclusive_start_key: Some(key("same", "10")),
                ..base.clone()
            },
            true,
        ),
        (
            "reverse after intent",
            PartitionQueryInput {
                index_name: None,
                forward: false,
                exclusive_start_key: Some(deleted.clone()),
                ..base.clone()
            },
            false,
        ),
    ];
    for (name, input, conflicts) in cases {
        let result = client
            .query::<PartitionQuery>(target, None, Json(input))
            .await
            .unwrap()
            .output
            .0;
        if conflicts {
            assert!(
                matches!(result, PartitionQueryOutcome::Conflict(conflict) if conflict.transaction.transaction_id == transaction_id && conflict.transaction.coordinator_cell == coordinator_cell && conflict.coordinator_key == transaction_id),
                "{name}"
            );
        } else {
            assert!(
                matches!(result, PartitionQueryOutcome::Page { .. }),
                "{name}: {result:?}"
            );
        }
    }
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
    assert!(matches!(
        storage
            .query(key_info, &condition, &maps, true, Some(1), None, None)
            .await,
        Err(StorageError::Transient(_))
    ));
    abort_blocker(client, &key_info.account_id, transaction_id).await;
    assert_eq!(
        storage
            .transact_get_items(&[
                TransactGetOp {
                    key_info,
                    key: &created
                },
                TransactGetOp {
                    key_info,
                    key: &deleted
                },
            ])
            .await
            .unwrap(),
        vec![None, Some(deleted)]
    );
    assert!(
        storage
            .query(key_info, &condition, &maps, true, Some(1), None, None)
            .await
            .is_ok()
    );
    assert!(
        storage
            .scan(key_info, None, None, None, None, None)
            .await
            .is_ok()
    );
}

#[allow(dead_code)] // Convenience wrapper for fixtures containing one PK only.
pub(crate) async fn assert_sdk_read_barrier(
    sdk: &aws_sdk_dynamodb::Client,
    client: &CellClient,
    partition: &PartitionSpec,
    account_id: &str,
) {
    assert_sdk_read_barrier_with_scan(sdk, client, partition, account_id, &[("same", "10")]).await;
}

pub(crate) async fn assert_sdk_read_barrier_with_scan(
    sdk: &aws_sdk_dynamodb::Client,
    client: &CellClient,
    partition: &PartitionSpec,
    account_id: &str,
    expected_scan: &[(&str, &str)],
) {
    let target = data_target(account_id, &partition.table.id, &partition.partition_id).unwrap();
    let transaction_id = [203; 16];
    let key = Item::from([
        ("pk".into(), AttributeValue::S("same".into())),
        ("sk".into(), AttributeValue::N("8".into())),
    ]);
    let info = TableKeyInfo {
        account_id: account_id.into(),
        table_name: partition.table.table_name.clone(),
        table_id: partition.table.id.clone(),
        base_key_schema: partition.table.key_schema.clone(),
        key_schema: partition.table.key_schema.clone(),
        attribute_definitions: partition.table.attribute_definitions.clone(),
        ..Default::default()
    };
    prepare_blocker(
        client,
        &target,
        &info,
        partition.epoch,
        transaction_id,
        vec![TransactionOperation::Delete(DeleteItemInput {
            return_old: false,
            table_name: partition.table.table_name.clone(),
            table_id: partition.table.id.clone(),
            key,
            condition: None,
        })],
    )
    .await;
    // Exercise unnormalized numeric spellings at the public protocol boundary.
    // Both operations exclude the locked key itself and must reach SK 10.
    let scan = sdk
        .scan()
        .table_name(&partition.table.table_name)
        .set_exclusive_start_key(Some(HashMap::from([
            ("pk".into(), AwsAttributeValue::S("same".into())),
            ("sk".into(), AwsAttributeValue::N("8.0".into())),
        ])))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(
        scan.items()
            .iter()
            .map(|item| (
                item["pk"].as_s().unwrap().as_str(),
                item["sk"].as_n().unwrap().as_str()
            ))
            .collect::<Vec<_>>(),
        expected_scan
    );
    let query = sdk
        .query()
        .table_name(&partition.table.table_name)
        .key_condition_expression("pk = :p AND sk > :s")
        .expression_attribute_values(":p", AwsAttributeValue::S("same".into()))
        .expression_attribute_values(":s", AwsAttributeValue::N("8.0".into()))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(
        query
            .items()
            .iter()
            .map(|item| item["sk"].as_n().unwrap().as_str())
            .collect::<Vec<_>>(),
        ["10"]
    );
    let request = || {
        sdk.transact_get_items().set_transact_items(Some(
            ["1.5", "8"]
                .into_iter()
                .map(|sk| {
                    aws_sdk_dynamodb::types::TransactGetItem::builder()
                        .get(
                            aws_sdk_dynamodb::types::Get::builder()
                                .table_name(&partition.table.table_name)
                                .key("pk", AwsAttributeValue::S("same".into()))
                                .key("sk", AwsAttributeValue::N(sk.into()))
                                .build()
                                .unwrap(),
                        )
                        .build()
                })
                .collect(),
        ))
    };
    let error = request().send().await.unwrap_err().into_service_error();
    let aws_sdk_dynamodb::operation::transact_get_items::TransactGetItemsError::TransactionCanceledException(error) = error else {
        panic!("unexpected SDK error: {error:?}");
    };
    assert_eq!(
        error
            .cancellation_reasons()
            .iter()
            .map(|reason| reason.code())
            .collect::<Vec<_>>(),
        vec![Some("None"), Some("TransactionConflict")]
    );
    let write_error = sdk
        .transact_write_items()
        .set_transact_items(Some(
            ["1.5", "8"]
                .into_iter()
                .map(|sk| {
                    aws_sdk_dynamodb::types::TransactWriteItem::builder()
                        .put(
                            aws_sdk_dynamodb::types::Put::builder()
                                .table_name(&partition.table.table_name)
                                .item("pk", AwsAttributeValue::S("same".into()))
                                .item("sk", AwsAttributeValue::N(sk.into()))
                                .item("uncommitted", AwsAttributeValue::Bool(true))
                                .build()
                                .unwrap(),
                        )
                        .build()
                })
                .collect(),
        ))
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    let aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError::TransactionCanceledException(write_error) = write_error else {
        panic!("unexpected SDK write error: {write_error:?}");
    };
    assert_eq!(
        write_error
            .cancellation_reasons()
            .iter()
            .map(|reason| reason.code())
            .collect::<Vec<_>>(),
        vec![Some("None"), Some("TransactionConflict")]
    );
    abort_blocker(client, account_id, transaction_id).await;
    let response = request().send().await.unwrap();
    assert!(
        response
            .responses()
            .iter()
            .all(|item| !item.item().unwrap().contains_key("uncommitted"))
    );
    assert_eq!(
        response
            .responses()
            .iter()
            .map(|item| item.item().unwrap()["sk"].clone())
            .collect::<Vec<_>>(),
        vec![
            AwsAttributeValue::N("1.5".into()),
            AwsAttributeValue::N("8".into())
        ]
    );
}

async fn prepare_blocker(
    client: &CellClient,
    target: &CellTarget,
    info: &TableKeyInfo,
    epoch: u64,
    id: [u8; 16],
    operations: Vec<TransactionOperation>,
) -> CellTarget {
    let provisioner =
        CellInitialPartitionProvisioner::new(client.clone(), local_node::application().unwrap());
    provisioner
        .ensure(client, &info.account_id, &id)
        .await
        .unwrap();
    let coordinator = coordinator_target(&info.account_id, &id).unwrap();
    let state = client
        .query::<ReadPartitionState>(target, None, Json(()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert_eq!(state.spec.epoch, epoch);
    assert_eq!(state.spec.table.id, info.table_id);
    transaction_command!(
        client,
        BeginCrossCellTransaction,
        &coordinator,
        identity(201),
        Json(BeginCrossCellTransactionInput {
            account_id: info.account_id.clone(),
            transaction_id: id,
            token: None,
            participants: vec![CoordinatorParticipant {
                target: CoordinatorParticipantTarget::Data {
                    table_id: info.table_id.clone(),
                    partition_id: state.spec.partition_id,
                    epoch
                },
                operations: operations
                    .iter()
                    .enumerate()
                    .map(|(index, operation)| IndexedTransactionOperation {
                        index: u8::try_from(index).unwrap(),
                        operation: operation.clone()
                    })
                    .collect(),
            }],
        })
    )
    .await
    .unwrap();
    let prepared = transaction_command!(
        client,
        PreparePartitionTransaction,
        target,
        identity(202),
        Json(PreparePartitionTransactionInput {
            table_id: info.table_id.clone(),
            epoch,
            transaction_id: id,
            coordinator_cell: *coordinator.cell_id().as_bytes(),
            coordinator_key: id.to_vec(),
            operations,
        })
    )
    .await
    .unwrap();
    assert_eq!(prepared.output.0, PrepareTransactionOutcome::Prepared);
    coordinator
}
async fn abort_blocker(client: &CellClient, account: &str, id: [u8; 16]) {
    let coordinator = coordinator_target(account, &id).unwrap();
    client
        .command::<DecideCrossCellTransaction>(
            &coordinator,
            identity(204),
            Json(DecideCrossCellTransactionInput {
                account_id: account.into(),
                transaction_id: id,
                routing_key: id.to_vec(),
                decision: CoordinatorDecision::Abort {
                    index: None,
                    reason: None,
                },
            }),
        )
        .await
        .unwrap();
    CellStorage::new(client.clone(), "local")
        .finish_decided_cross_cell_transaction(account, &id, id)
        .await
        .unwrap();
    let status = client
        .query::<ReadCrossCellTransaction>(
            &coordinator,
            None,
            Json(ReadCrossCellTransactionInput {
                account_id: account.into(),
                transaction_id: id,
                routing_key: id.to_vec(),
            }),
        )
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert_eq!(
        status.decision,
        CoordinatorDecision::Abort {
            index: None,
            reason: None
        }
    );
    assert_eq!(status.resolved_count, 1);
}
