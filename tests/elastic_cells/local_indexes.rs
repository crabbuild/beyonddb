use crate::{
    native_peer::{self, FRONT, Inspection, Peer, SOURCE},
    signed_http::SignedHttp,
};
use bdb_cellar_runtime::{
    MutationIdentity,
    client::CellClient,
    identity::{CellTarget, RequestId},
};
use beyonddb::*;
use extenddb_core::{
    expression::{
        CompareOp, Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition, UpdateAction,
    },
    types::{AttributeValue, CreateTableInput, Item, TableKeyInfo},
};
use extenddb_storage::{
    DataEngine, IdempotencyKey, TableEngine, TransactWriteOp, error::StorageError,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

fn item(sk: &str, score: Option<&str>) -> Item {
    let mut item = Item::from([
        ("pk".into(), AttributeValue::S("same".into())),
        ("sk".into(), AttributeValue::N(sk.into())),
    ]);
    if let Some(score) = score {
        item.insert("score".into(), AttributeValue::N(score.into()));
    }
    item
}

async fn indexed(storage: &CellStorage, base: &TableKeyInfo) -> TableKeyInfo {
    let index = storage
        .index_info(&base.account_id, &base.table_name, "Score0")
        .await
        .unwrap();
    TableKeyInfo {
        key_schema: index.key_schema,
        ..base.clone()
    }
}

async fn query(
    storage: &CellStorage,
    info: &TableKeyInfo,
    forward: bool,
    start: Option<&Item>,
    limit: Option<i64>,
) -> Result<(Vec<Item>, Option<Item>), StorageError> {
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
    storage
        .query(
            info,
            &condition,
            &maps,
            forward,
            limit,
            start,
            Some("Score0"),
        )
        .await
}

#[test]
fn local_indexes_follow_mutations_transactions_and_splits() {
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
    let application = native_peer::application();
    let account_id = "123456789012";
    let account = account_target(account_id).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite")).unwrap();
    let observed = Arc::new(Mutex::new(Default::default()));
    let source_path = directory.path().join(SOURCE);
    let mut source = Peer::start(application.clone(), &source_path, bucket.clone(), observed.clone(), true, host.handle(), core.handle()).await;
    let client = source.client.clone();
    let provisioner = Arc::new(CellInitialPartitionProvisioner::new(client.clone(), application.clone()).with_initial_partition_count(2).unwrap());
    provisioner.admit_account(account_id).await.unwrap();
    let storage = CellStorage::new(client.clone(), "us-east-1")
        .with_initial_partitions(provisioner.clone())
        .with_transaction_coordinators(provisioner.clone());
    let mut infos = Vec::new();
    let maps = ExpressionMaps::default();
    for name in ["LocalAccount", "LocalRouted"] {
        let indexes: Vec<_> = (0..5).map(|i| serde_json::json!({
            "IndexName": format!("Score{i}"), "KeySchema": [{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"score","KeyType":"RANGE"}],
            "Projection": {"ProjectionType":"ALL"}
        })).collect();
        let create: CreateTableInput = serde_json::from_value(serde_json::json!({
            "TableName": name, "BillingMode": "PAY_PER_REQUEST",
            "KeySchema": [{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
            "AttributeDefinitions": [{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"N"},{"AttributeName":"score","AttributeType":"N"}],
            "LocalSecondaryIndexes": indexes
        })).unwrap();
        if name == "LocalAccount" {
            // Preserve the private account-generation domain path explicitly.
            // Public CreateTable below always installs multiple ordered cells.
            let created = client.command::<CreateTable>(&account, mutation(), Json(TableSpec {
                table_class: Default::default(), placement: TablePlacement::Account,
                table_name: create.table_name, key_schema: create.key_schema,
                attribute_definitions: create.attribute_definitions,
                local_secondary_indexes: create.local_secondary_indexes.unwrap(),
                global_secondary_indexes: vec![], billing_mode: create.billing_mode.unwrap(),
                provisioned_throughput: None, deletion_protection_enabled: false,
                initial_tags: vec![], resource_arn: None, stream: None,
            })).await.unwrap();
            assert!(matches!(created.output.0, CreateTableOutcome::Created(_)));
        } else {
            let description = storage.create_table(account_id, create).await.unwrap();
            assert_eq!(description.local_secondary_indexes.unwrap().len(), 5);
            assert_eq!(routes(&client, &account, &storage.table_key_info(account_id, name).await.unwrap().table_id).await.len(), 2);
        }
        let base = storage.table_key_info(account_id, name).await.unwrap();
        assert!(base.has_lsi);
        assert_eq!(base.local_secondary_indexes.len(), 5);
        for value in [
            item("-2", Some("10")),
            item("2", Some("2")),
            item("10", Some("2")),
            item("20", None),
        ] {
            storage
                .put_item(&base, value, false, None, &maps, None)
                .await
                .unwrap();
        }
        let info = indexed(&storage, &base).await;
        for (forward, expected) in [(true, ["2", "10", "-2"]), (false, ["-2", "10", "2"])] {
            let mut cursor = None;
            let mut found = Vec::new();
            loop {
                let (page, next) = query(&storage, &info, forward, cursor.as_ref(), Some(1))
                    .await
                    .unwrap();
                found.extend(page.iter().map(|item| item["sk"].clone()));
                if let Some(key) = &next {
                    assert_eq!(key.len(), 3);
                }
                cursor = next;
                if cursor.is_none() {
                    break;
                }
            }
            assert_eq!(found, expected.map(|v| AttributeValue::N(v.into())));
        }
        let mut oversized = item("2", Some("2"));
        oversized.insert("payload".into(), AttributeValue::S("x".repeat(70 * 1024)));
        assert!(matches!(
            storage
                .put_item(&base, oversized, false, None, &maps, None)
                .await,
            Err(StorageError::Validation(_))
        ));
        let mut invalid = item("2", None);
        invalid.insert("score".into(), AttributeValue::S(String::new()));
        assert!(matches!(
            storage
                .put_item(&base, invalid, false, None, &maps, None)
                .await,
            Err(StorageError::Validation(_))
        ));
        let update = [UpdateAction::Remove {
            path: vec![PathElement::Attribute("score".into())],
        }];
        storage
            .update_item(
                &base,
                &item("10", None),
                &update,
                false,
                false,
                None,
                &maps,
                None,
            )
            .await
            .unwrap();
        storage
            .delete_item(&base, &item("-2", None), false, None, &maps, None)
            .await
            .unwrap();
        assert_eq!(
            query(&storage, &info, true, None, None).await.unwrap().0,
            vec![item("2", Some("2"))]
        );
        infos.push(base);
    }
    let images = [item("2", Some("-3")), item("20", Some("4"))];
    let operations: Vec<_> = infos
        .iter()
        .flat_map(|info| {
            images.iter().map(|item| TransactWriteOp::Put {
                key_info: info,
                item,
                condition: None,
                maps: &maps,
                return_values_on_ccf: Default::default(),
                stream: None,
            })
        })
        .collect();
    let token = || IdempotencyKey {
        account_id,
        token: "local-index-mixed",
        fingerprint: "two-tables-two-items",
    };
    storage
        .transact_write_items(&operations, Some(token()))
        .await
        .unwrap();
    assert!(matches!(
        storage
            .transact_write_items(&operations, Some(token()))
            .await,
        Err(StorageError::IdempotentReplay)
    ));
    for info in &infos {
        let indexed = indexed(&storage, info).await;
        assert_eq!(
            query(&storage, &indexed, true, None, None).await.unwrap().0,
            images
        );
        let mut cursor = None;
        let mut scanned = Vec::new();
        loop {
            let (page, next) = storage
                .scan(
                    &indexed,
                    Some(1),
                    cursor.as_ref(),
                    None,
                    None,
                    Some("Score0"),
                )
                .await
                .unwrap();
            scanned.extend(page);
            cursor = next;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(scanned.len(), 2);
        for item in &images {
            assert!(scanned.contains(item));
        }
    }
    for commit in [false, true] {
        assert_prepared_index_barriers(&client, &storage, &provisioner, &infos, commit).await;
    }
    let routed = &infos[1];
    let (original, participant) = participant_for_key(&client, routed, &item("2", None)).await;
    let CoordinatorParticipantTarget::Data { partition_id, epoch, .. } = participant else { unreachable!() };
    let source_spec = client.query::<ReadPartitionState>(&original, None, Json(())).await.unwrap().output.0.unwrap().spec;
    assert_eq!(client.query::<PartitionSplitPoint>(&original, None, Json(())).await.unwrap().output.0, None);
    assert!(matches!(provisioner.split_partition(account_id, client.clone(), &routed.table_id, partition_id, source_spec.lower.clone().unwrap_or_else(bdb_keyspace::RangeKey::minimum)).await, Err(StorageError::LimitExceeded(_))), "one LSI collection must remain together");
    assert_eq!(routes(&client, &account, &routed.table_id).await.len(), 2);
    // A second PK in the same ordered source provides a real collection fence.
    let mut neighbor = item("50", None);
    neighbor.insert("pk".into(), AttributeValue::S("zzz".into()));
    assert_eq!(participant_for_key(&client, routed, &neighbor).await.0, original);
    storage.put_item(routed, neighbor.clone(), false, None, &maps, None).await.unwrap();
    let indexed = indexed(&storage, routed).await;
    let (first, continuation) = query(&storage, &indexed, true, None, Some(1)).await.unwrap();
    assert_eq!(first, vec![images[0].clone()]);
    let continuation = continuation.unwrap();
    let plan = provisioner.split_partition(account_id, client.clone(), &routed.table_id, partition_id, source_spec.lower.clone().unwrap_or_else(bdb_keyspace::RangeKey::minimum)).await.unwrap();
    assert_eq!(plan.source.epoch, epoch);
    assert_eq!(routes(&client, &account, &routed.table_id).await.len(), 3);
    let (left, _) = participant_for_key(&client, routed, &item("2", None)).await;
    let (right, _) = participant_for_key(&client, routed, &neighbor).await;
    assert_ne!(left, right);
    assert_eq!(participant_for_key(&client, routed, &item("20", None)).await.0, left);
    assert_eq!(query(&storage, &indexed, true, Some(&continuation), Some(1)).await.unwrap().0, vec![images[1].clone()]);
    assert_eq!(query(&storage, &indexed, true, None, None).await.unwrap().0, images);
    let http = SignedHttp::start(&client, provisioner.clone(), true).await;
    assert_sdk_indexes(&http.sdk).await;
    http.stop().await;
    let targets = [account.clone(), left, right];
    let mut owners = Vec::new();
    let inspection = Inspection::new(bucket.clone(), host.handle());
    for target in &targets {
        let actual = native_peer::identity(&observed, SOURCE, target).unwrap();
        owners.push((actual.clone(), inspection.owner(&actual.scope).await));
    }
    source.stop().await;
    std::fs::remove_dir_all(&source_path).unwrap();
    let mut replacement = Peer::start(application.clone(), &directory.path().join(FRONT), bucket, observed.clone(), true, host.handle(), core.handle()).await;
    inspection.departed().await;
    let restored_provisioner = Arc::new(CellInitialPartitionProvisioner::new(replacement.client.clone(), application).with_initial_partition_count(2).unwrap());
    let http = SignedHttp::start(&replacement.client, restored_provisioner, false).await;
    assert_sdk_indexes(&http.sdk).await;
    let restored = http.sdk.get_item().table_name("LocalRouted").key("pk", aws_sdk_dynamodb::types::AttributeValue::S("zzz".into())).key("sk", aws_sdk_dynamodb::types::AttributeValue::N("50".into())).consistent_read(true).send().await.unwrap();
    assert_eq!(restored.item().unwrap().len(), 2);
    for (target, (previous, old_owner)) in targets.iter().zip(owners) {
        let current = native_peer::identity(&observed, FRONT, target).unwrap();
        assert_eq!(current.scope, previous.scope);
        let owner = inspection.owner(&current.scope).await;
        assert_eq!(owner.node.as_deref(), Some(FRONT));
        assert!(owner.epoch > old_owner.epoch);
        assert_eq!(owner.epoch, current.epoch);
    }
    http.stop().await;
    replacement.stop().await;
    inspection.close().await;
    });
}

async fn assert_prepared_index_barriers(
    client: &CellClient,
    storage: &CellStorage,
    provisioner: &CellInitialPartitionProvisioner,
    infos: &[TableKeyInfo],
    commit: bool,
) {
    use beyonddb::{PrepareAccountTransaction, PrepareAccountTransactionInput};
    let account_id = &infos[0].account_id;
    let account = account_target(account_id).unwrap();
    let phase = if commit { 95 } else { 93 };
    let id = [phase; 16];
    provisioner.ensure(client, account_id, &id).await.unwrap();
    let coordinator = coordinator_target(account_id, &id).unwrap();
    let mut participants = Vec::new();
    for (i, info) in infos.iter().enumerate() {
        let operations = [
            TransactionOperation::Put(PutItemInput {
                table_name: info.table_name.clone(),
                table_id: info.table_id.clone(),
                item: item("2", Some("12")),
                condition: None,
            }),
            TransactionOperation::Put(PutItemInput {
                table_name: info.table_name.clone(),
                table_id: info.table_id.clone(),
                item: item("30", Some("12")),
                condition: None,
            }),
            TransactionOperation::Delete(DeleteItemInput {
                table_name: info.table_name.clone(),
                table_id: info.table_id.clone(),
                key: item("20", None),
                condition: None,
                return_old: false,
            }),
        ];
        let (_, target) = participant_for_key(client, info, &item("2", None)).await;
        participants.push(CoordinatorParticipant {
            target,
            operations: operations
                .into_iter()
                .enumerate()
                .map(|(position, operation)| IndexedTransactionOperation {
                    index: (i * 3 + position) as u8,
                    operation,
                })
                .collect(),
        });
    }
    participants.sort_by_key(|participant| {
        let target = match &participant.target {
            CoordinatorParticipantTarget::Account => account.clone(),
            CoordinatorParticipantTarget::Data {
                table_id,
                partition_id,
                ..
            } => data_target(account_id, table_id, partition_id).unwrap(),
        };
        *target.cell_id().as_bytes()
    });
    transaction_command!(
        client,
        BeginCrossCellTransaction,
        &coordinator,
        mutation(),
        Json(BeginCrossCellTransactionInput {
            account_id: account_id.clone(),
            transaction_id: id,
            token: None,
            participants: participants.clone(),
        })
    )
    .await
    .unwrap();
    for (position, participant) in participants.into_iter().enumerate() {
        let target = match &participant.target {
            CoordinatorParticipantTarget::Account => account.clone(),
            CoordinatorParticipantTarget::Data {
                table_id,
                partition_id,
                ..
            } => data_target(account_id, table_id, partition_id).unwrap(),
        };
        let operations = participant
            .operations
            .into_iter()
            .map(|operation| operation.operation)
            .collect();
        let prepared = match participant.target {
            CoordinatorParticipantTarget::Account => transaction_command!(
                client,
                PrepareAccountTransaction,
                &account,
                mutation(),
                Json(PrepareAccountTransactionInput {
                    transaction_id: id,
                    coordinator_cell: *coordinator.cell_id().as_bytes(),
                    coordinator_key: id.to_vec(),
                    operations,
                })
            )
            .await
            .unwrap(),
            CoordinatorParticipantTarget::Data {
                table_id,
                partition_id,
                epoch,
            } => {
                let target = data_target(account_id, &table_id, &partition_id).unwrap();
                transaction_command!(
                    client,
                    PreparePartitionTransaction,
                    &target,
                    mutation(),
                    Json(PreparePartitionTransactionInput {
                        table_id,
                        epoch,
                        transaction_id: id,
                        coordinator_cell: *coordinator.cell_id().as_bytes(),
                        coordinator_key: id.to_vec(),
                        operations,
                    })
                )
                .await
                .unwrap()
            }
        };
        assert_eq!(prepared.output.0, PrepareTransactionOutcome::Prepared);
        client
            .command::<RecordParticipantPrepare>(
                &coordinator,
                mutation(),
                Json(CoordinatorPhaseInput {
                    account_id: account_id.clone(),
                    transaction_id: id,
                    routing_key: id.to_vec(),
                    position: position as u8,
                    participant_cell: *target.cell_id().as_bytes(),
                    sequence: prepared.receipt.commit_sequence,
                }),
            )
            .await
            .unwrap();
    }
    for info in infos {
        let indexed = indexed(storage, info).await;
        let condition = KeyCondition {
            pk_path: vec![PathElement::Attribute("pk".into())],
            pk_value: Expr::Placeholder("p".into()),
            extra_pk_conditions: vec![],
            sk_condition: Some(SortKeyCondition::Compare {
                path: vec![PathElement::Attribute("score".into())],
                op: CompareOp::Gt,
                value: Expr::Placeholder("s".into()),
            }),
            extra_sk_conditions: vec![],
        };
        let maps = ExpressionMaps::new(
            HashMap::new(),
            HashMap::from([
                ("p".into(), AttributeValue::S("same".into())),
                ("s".into(), AttributeValue::N("9".into())),
            ]),
        );
        // No live row matches this index range. The prepared create and sort-key
        // move must still fence it, including a cursor beyond the old index rows.
        assert!(matches!(
            storage
                .query(
                    &indexed,
                    &condition,
                    &maps,
                    true,
                    Some(1),
                    Some(&item("20", Some("10"))),
                    Some("Score0")
                )
                .await,
            Err(StorageError::Transient(_))
        ));
        assert!(matches!(
            storage
                .scan(
                    &indexed,
                    Some(1),
                    Some(&item("2", Some("-3"))),
                    None,
                    None,
                    Some("Score0")
                )
                .await,
            Err(StorageError::Transient(_))
        ));
    }
    client
        .command::<DecideCrossCellTransaction>(
            &coordinator,
            mutation(),
            Json(DecideCrossCellTransactionInput {
                account_id: account_id.clone(),
                transaction_id: id,
                routing_key: id.to_vec(),
                decision: if commit {
                    CoordinatorDecision::Commit
                } else {
                    CoordinatorDecision::Abort {
                        index: None,
                        reason: None,
                    }
                },
            }),
        )
        .await
        .unwrap();
    // The first indexed read must help a published decision and repeat its read.
    for info in infos {
        let indexed = indexed(storage, info).await;
        assert_eq!(
            query(storage, &indexed, true, None, None).await.unwrap().0,
            if commit {
                [item("2", Some("12")), item("30", Some("12"))]
            } else {
                [item("2", Some("-3")), item("20", Some("4"))]
            }
        );
        if commit {
            let maps = ExpressionMaps::default();
            storage
                .delete_item(info, &item("30", None), false, None, &maps, None)
                .await
                .unwrap();
            for image in [item("2", Some("-3")), item("20", Some("4"))] {
                storage
                    .put_item(info, image, false, None, &maps, None)
                    .await
                    .unwrap();
            }
        }
    }
}

fn mutation() -> MutationIdentity {
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms: native_peer::now_ms(),
        expires_at_ms: native_peer::now_ms() + 60_000,
    }
}

async fn routes(client: &CellClient, account: &CellTarget, table: &str) -> Vec<RoutePagePartition> {
    let mut result = Vec::new();
    let mut input = RoutePageInput {
        table_id: table.into(),
        start_key: None,
        after_lower: None,
        expected_epoch: None,
    };
    loop {
        match read_route_page(client, account, input.clone())
            .await
            .unwrap()
        {
            RoutePageOutcome::Unrouted => {
                assert!(result.is_empty());
                return result;
            }
            RoutePageOutcome::Page {
                epoch,
                partitions,
                has_more,
            } => {
                input.expected_epoch = Some(epoch);
                input.after_lower = partitions.last().map(|part| part.lower.clone());
                result.extend(partitions);
                assert!(
                    result.len() <= 16,
                    "private index fixture exceeded route bound"
                );
                if !has_more {
                    return result;
                }
            }
            RoutePageOutcome::Changed => panic!("index fixture route changed during inspection"),
        }
    }
}

async fn participant_for_key(
    client: &CellClient,
    info: &TableKeyInfo,
    key: &Item,
) -> (CellTarget, CoordinatorParticipantTarget) {
    let account = account_target(&info.account_id).unwrap();
    let ranges = routes(client, &account, &info.table_id).await;
    if ranges.is_empty() {
        let record = client
            .query::<DescribeTableById>(&account, None, Json(info.table_id.clone()))
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        assert_eq!(record.placement, TablePlacement::Account);
        return (account, CoordinatorParticipantTarget::Account);
    }
    let encoded = ordered_data_key(&info.table_id, key, &info.base_key_schema).unwrap();
    let range = ranges
        .iter()
        .find(|range| {
            range.lower <= encoded && range.upper.as_ref().is_none_or(|upper| &encoded < upper)
        })
        .unwrap();
    (
        data_target(&info.account_id, &info.table_id, &range.partition_id).unwrap(),
        CoordinatorParticipantTarget::Data {
            table_id: info.table_id.clone(),
            partition_id: range.partition_id,
            epoch: range.epoch,
        },
    )
}

async fn assert_sdk_indexes(sdk: &aws_sdk_dynamodb::Client) {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    // Keep AWS retries off so a backend read/owner failure cannot disappear in
    // client retry behavior. The frontend uses its normal pure-read pacing.
    let sdk = aws_sdk_dynamodb::Client::from_conf(
        sdk.config()
            .to_builder()
            .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled())
            .build(),
    );
    for name in ["LocalAccount", "LocalRouted"] {
        for (forward, expected) in [(true, ["2", "20"]), (false, ["20", "2"])] {
            let mut cursor = None;
            let mut found = Vec::new();
            for page in 0..8 {
                let result = sdk
                    .query()
                    .table_name(name)
                    .index_name("Score0")
                    .key_condition_expression("pk = :p")
                    .expression_attribute_values(":p", A::S("same".into()))
                    .scan_index_forward(forward)
                    .consistent_read(true)
                    .limit(1)
                    .set_exclusive_start_key(cursor)
                    .send()
                    .await
                    .unwrap();
                found.extend(
                    result
                        .items()
                        .iter()
                        .map(|item| item["sk"].as_n().unwrap().clone()),
                );
                cursor = result.last_evaluated_key;
                if let Some(key) = &cursor {
                    assert_eq!(key.len(), 3);
                }
                if cursor.is_none() {
                    break;
                }
                assert!(page < 7, "SDK LSI query did not finish");
            }
            assert_eq!(found, expected);
        }
        let mut cursor = None;
        let mut found = Vec::new();
        for page in 0..16 {
            let result = sdk
                .scan()
                .table_name(name)
                .index_name("Score0")
                .consistent_read(true)
                .limit(1)
                .set_exclusive_start_key(cursor)
                .send()
                .await
                .unwrap();
            found.extend(result.items().iter().cloned());
            cursor = result.last_evaluated_key;
            if cursor.is_none() {
                break;
            }
            assert!(page < 15, "SDK LSI scan did not finish");
        }
        assert_eq!(found.len(), 2);
        for (sk, score) in [("2", "-3"), ("20", "4")] {
            assert!(found.contains(&HashMap::from([
                ("pk".into(), A::S("same".into())),
                ("sk".into(), A::N(sk.into())),
                ("score".into(), A::N(score.into())),
            ])));
        }
    }
}
