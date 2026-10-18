use std::{collections::HashMap, sync::Arc, time::UNIX_EPOCH};

use beyonddb::{
    AdvanceTtlSchedule, AdvanceTtlScheduleInput, AdvanceTtlSweep, AdvanceTtlSweepInput, Beyonddb,
    CellInitialPartitionProvisioner, CellStorage, CreateTable, CreateTableOutcome, DeleteItem,
    DeleteItemInput, DescribeTable, GetItem, GetItemInput, GetItemOutcome, ItemMutationOutcome,
    Json, ListTables, ListTablesInput, ListTablesOutcome, PartitionQueryInput,
    PartitionQueryOutcome, PutItem, PutItemInput, QueryAccountItems, ReadAccountStreamJournal,
    ReadStreamGcShard, ReadTtlSchedule, ReadTtlSweep, StreamConfig, StreamJournalInput,
    StreamJournalOutcome, TableSpec, TransactWrite, TransactWriteInput, TransactionOperation,
    TransactionOutcome, UpdateTtl, UpdateTtlInput, account_target,
};
use cellar_runtime::{
    MutationIdentity,
    app::CellApplication,
    client::{CellClient, InvocationError},
    descriptors::BuildDescriptor,
    identity::{CellTarget, Digest, RequestId},
};
use keyspace::RangeKey;
#[path = "support/cellar_node.rs"]
mod cellar_node;
use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, PathElement, UpdateAction};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, DeleteTableInput,
    DescribeStreamInput, Item, KeySchemaElement, KeyType, ReturnValuesOnConditionCheckFailure,
    ScalarAttributeType, StreamEventName, StreamRecord, StreamSpecification, StreamViewType,
    TableStatus, UpdateTableInput,
};
use extenddb_storage::{
    DataEngine, IdempotencyKey, MetadataEngine, StreamCapture, StreamContinuation, StreamEngine,
    TableEngine, TransactGetOp, TransactWriteOp, error::StorageError,
};

fn identity(byte: u8) -> MutationIdentity {
    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    MutationIdentity {
        request_id: RequestId::from_bytes([byte; 16]),
        issued_at_ms: now_ms,
        expires_at_ms: now_ms + 60_000,
    }
}

#[test]
fn account_items_replay_rollback_and_restore_on_new_host() {
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
    let application = Arc::new(
        Beyonddb::compile(BuildDescriptor {
            source_revision: "account-cell-test".into(),
            cargo_lock_digest: Digest::from_bytes([1; 32]),
        })
        .unwrap(),
    );
    let target = account_target("123456789012").unwrap();
    let directory = tempfile::TempDir::new().unwrap();
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite")).unwrap();
    let first_owner = directory.path().join("owner-a");
    let (node, client) = cellar_node::start(application.clone(), &first_owner, host.handle(), core.handle(), bucket.clone()).await.unwrap();
    let provisioner = Arc::new(CellInitialPartitionProvisioner::new(client.clone(), application.clone()));
    let storage = CellStorage::new(client.clone(), "us-east-1")
        .with_transaction_coordinators(provisioner.clone());
    let schema = TableSpec {
        table_class: Default::default(),
        placement: beyonddb::TablePlacement::Account,
        local_secondary_indexes: Vec::new(),
        global_secondary_indexes: Vec::new(),
        table_name: "Books".into(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "id".into(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "id".into(),
            attribute_type: ScalarAttributeType::S,
        }],
        billing_mode: BillingMode::PayPerRequest,
        provisioned_throughput: None,
        deletion_protection_enabled: false,
        initial_tags: Vec::new(),
        resource_arn: None,
        stream: Some(StreamConfig {
            view_type: StreamViewType::NewAndOldImages,
            region: "us-east-1".into(),
            label: "2026-09-27T00:00:00.000".into(),
        }),
    };
    let created = client
        .command::<CreateTable>(&target, identity(4), Json(schema.clone()))
        .await
        .unwrap();
    let book_table_id = match created.output.0 {
        CreateTableOutcome::Created(record) => record.id,
        _ => panic!("expected table creation"),
    };
    client
        .command::<UpdateTtl>(
            &target,
            identity(20),
            Json(UpdateTtlInput {
                table_name: "Books".into(),
                attribute_name: Some("expires".into()),
            }),
        )
        .await
        .unwrap();
    let cursor = AdvanceTtlSweepInput {
        table_name: "Books".into(),
        table_id: book_table_id.clone(),
        attribute_name: "expires".into(),
        expected_after: None,
        next_after: Some(RangeKey::new(vec![7; 16]).unwrap()),
    };
    assert!(
        client
            .command::<AdvanceTtlSweep>(&target, identity(21), Json(cursor.clone()))
            .await
            .unwrap()
            .output
            .0
    );
    assert!(matches!(
        client
            .command::<AdvanceTtlSweep>(&target, identity(22), Json(cursor))
            .await,
        Err(InvocationError::Rejected(_))
    ));
    let schedule = AdvanceTtlScheduleInput {
        expected_last: None,
        next_last: Some("Books".into()),
    };
    client
        .command::<AdvanceTtlSchedule>(&target, identity(23), Json(schedule.clone()))
        .await
        .unwrap();
    assert!(matches!(
        client
            .command::<AdvanceTtlSchedule>(&target, identity(24), Json(schedule))
            .await,
        Err(InvocationError::Rejected(_))
    ));
    let described = storage
        .describe_table(
            "123456789012",
            extenddb_core::types::DescribeTableInput {
                table_name: "Books".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(described.table_status, TableStatus::Active);
    assert_eq!(
        described.billing_mode_summary.unwrap().billing_mode,
        BillingMode::PayPerRequest
    );

    let key = Item::from([("id".into(), AttributeValue::S("book-1".into()))]);
    let mut item = key.clone();
    item.insert("title".into(), AttributeValue::S("Cell Systems".into()));
    let put = PutItemInput {
        table_name: "Books".into(),
        table_id: book_table_id.clone(),
        item: item.clone(),
        condition: None,
    };
    let mutation = identity(5);
    let written = client
        .command::<PutItem>(&target, mutation, Json(put.clone()))
        .await
        .unwrap();
    assert_eq!(written.output.0, ItemMutationOutcome::Applied(None));
    let replay = client
        .command::<PutItem>(&target, mutation, Json(put))
        .await
        .unwrap();
    assert_eq!(replay.receipt, written.receipt);

    let read = client
        .query::<GetItem>(
            &target,
            Some(written.receipt),
            Json(GetItemInput {
                table_name: "Books".into(),
                table_id: book_table_id.clone(),
                key: key.clone(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(read.output.0, GetItemOutcome::Found(Some(item.clone())));

    client
        .command::<PutItem>(
            &target,
            identity(7),
            Json(PutItemInput {
                table_name: "Books".into(),
                table_id: book_table_id.clone(),
                item: item.clone(),
                condition: None,
            }),
        )
        .await
        .unwrap();

    let deleted = client
        .command::<DeleteItem>(
            &target,
            identity(6),
            Json(DeleteItemInput {
                return_old: true,
                table_name: "Books".into(),
                table_id: book_table_id.clone(),
                key,
                condition: None,
            }),
        )
        .await
        .unwrap();
    assert_eq!(deleted.output.0, ItemMutationOutcome::Applied(Some(item)));
    let records = account_records(&client, &target, &book_table_id).await;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].event_name, StreamEventName::Insert);
    assert_eq!(records[1].event_name, StreamEventName::Remove);
    assert_eq!(records[0].dynamodb.new_image, records[1].dynamodb.old_image);

    assert!(matches!(
        storage.create_table("123456789012", CreateTableInput {
            table_name: "MustBeRouted".into(),
            key_schema: schema.key_schema.clone(),
            attribute_definitions: schema.attribute_definitions.clone(),
            billing_mode: Some(BillingMode::PayPerRequest),
            ..CreateTableInput::default()
        }).await,
        Err(StorageError::Connection(_))
    ));
    let author_table = create_account_table(&storage, &client,
            CreateTableInput {
                table_name: "Authors".into(),
                key_schema: schema.key_schema.clone(),
                attribute_definitions: schema.attribute_definitions.clone(),
                billing_mode: Some(BillingMode::PayPerRequest),
                deletion_protection_enabled: Some(true),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    assert!(author_table.deletion_protection_enabled);
    let author_table_id = author_table.table_id.clone();
    assert!(matches!(
        storage
            .index_info_by_table_id(&author_table.table_id, "missing")
            .await,
        Err(StorageError::IndexNotFound(_))
    ));
    let books = storage
        .table_key_info("123456789012", "Books")
        .await
        .unwrap();
    let authors = storage
        .table_key_info("123456789012", "Authors")
        .await
        .unwrap();
    let exists_condition = Expr::Function {
        name: "attribute_not_exists".into(),
        args: vec![Expr::Path(vec![PathElement::Attribute("id".into())])],
    };
    let conditional_key = Item::from([("id".into(), AttributeValue::S("conditioned".into()))]);
    let original = storage
        .put_item(
            &books,
            conditional_key.clone(),
            true,
            Some(&exists_condition),
            &ExpressionMaps::default(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(original, None);
    let rejected = storage
        .put_item(
            &books,
            conditional_key.clone(),
            true,
            Some(&exists_condition),
            &ExpressionMaps::default(),
            None,
        )
        .await;
    assert!(
        matches!(rejected, Err(StorageError::ConditionFailed(Some(old))) if old == conditional_key)
    );
    let update_maps = ExpressionMaps::new(
        HashMap::new(),
        HashMap::from([("title".into(), AttributeValue::S("Updated".into()))]),
    );
    let actions = [UpdateAction::Set {
        path: vec![PathElement::Attribute("title".into())],
        value: Expr::Placeholder("title".into()),
    }];
    let (old, updated) = storage
        .update_item(
            &books,
            &conditional_key,
            &actions,
            true,
            true,
            None,
            &update_maps,
            None,
        )
        .await
        .unwrap();
    let mut expected_updated = conditional_key.clone();
    expected_updated.insert("title".into(), AttributeValue::S("Updated".into()));
    assert_eq!(
        (old, updated),
        (
            Some(conditional_key.clone()),
            Some(expected_updated.clone())
        )
    );
    assert_eq!(
        storage.get_item(&books, &conditional_key).await.unwrap(),
        Some(expected_updated.clone())
    );
    let rejected_key_update = [UpdateAction::Set {
        path: vec![PathElement::Attribute("id".into())],
        value: Expr::Placeholder("title".into()),
    }];
    assert!(matches!(
        storage
            .update_item(
                &books,
                &conditional_key,
                &rejected_key_update,
                false,
                false,
                None,
                &update_maps,
                None
            )
            .await,
        Err(StorageError::Validation(_))
    ));
    let temporary = Item::from([("id".into(), AttributeValue::S("temporary".into()))]);
    storage
        .put_item(
            &books,
            temporary.clone(),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        storage.get_item(&books, &temporary).await.unwrap(),
        Some(temporary.clone())
    );
    assert!(matches!(
        storage
            .delete_item(
                &books,
                &temporary,
                true,
                Some(&exists_condition),
                &ExpressionMaps::default(),
                None,
            )
            .await,
        Err(StorageError::ConditionFailed(Some(old))) if old == temporary
    ));
    assert_eq!(
        storage
            .delete_item(
                &books,
                &temporary,
                true,
                None,
                &ExpressionMaps::default(),
                None
            )
            .await
            .unwrap(),
        Some(temporary)
    );
    let tx_book = Item::from([("id".into(), AttributeValue::S("book-tx".into()))]);
    let tx_author = Item::from([("id".into(), AttributeValue::S("author-tx".into()))]);
    let maps = ExpressionMaps::default();
    storage
        .transact_write_items(
            &[
                TransactWriteOp::Put {
                    key_info: &books,
                    item: &tx_book,
                    condition: None,
                    maps: &maps,
                    return_values_on_ccf: Default::default(),
                    stream: None,
                },
                TransactWriteOp::Put {
                    key_info: &authors,
                    item: &tx_author,
                    condition: None,
                    maps: &maps,
                    return_values_on_ccf: Default::default(),
                    stream: None,
                },
            ],
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        storage
            .transact_get_items(&[
                TransactGetOp {
                    key_info: &books,
                    key: &tx_book,
                },
                TransactGetOp {
                    key_info: &authors,
                    key: &tx_author,
                },
            ])
            .await
            .unwrap(),
        vec![Some(tx_book.clone()), Some(tx_author)]
    );

    let book_key = Item::from([("id".into(), AttributeValue::S("book-2".into()))]);
    let author_key = Item::from([("id".into(), AttributeValue::S("author-1".into()))]);
    let invalid_key = Item::from([("wrong".into(), AttributeValue::S("author-1".into()))]);
    let stream_count: i64 = i64::try_from(account_records(&client, &target, &book_table_id).await.len()).unwrap();
    let attempted = client
        .command::<TransactWrite>(
            &target,
            identity(8),
            Json(TransactWriteInput {
                operations: vec![
                    TransactionOperation::Put(PutItemInput {
                        table_name: "Books".into(),
                        table_id: book_table_id.clone(),
                        item: book_key.clone(),
                        condition: None,
                    }),
                    TransactionOperation::Put(PutItemInput {
                        table_name: "Authors".into(),
                        table_id: author_table_id.clone(),
                        item: invalid_key,
                        condition: None,
                    }),
                ],
            }),
        )
        .await;
    assert!(matches!(
        attempted,
        Err(InvocationError::Rejected(committed))
            if matches!(committed.output.0, TransactionOutcome::Rejected { index: 1, .. })
    ));
    assert_eq!(
        i64::try_from(account_records(&client, &target, &book_table_id).await.len()).unwrap(),
        stream_count
    );

    let absent = client
        .query::<GetItem>(
            &target,
            None,
            Json(GetItemInput {
                table_name: "Books".into(),
                table_id: book_table_id.clone(),
                key: book_key.clone(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(absent.output.0, GetItemOutcome::Found(None));

    let committed = client
        .command::<TransactWrite>(
            &target,
            identity(9),
            Json(TransactWriteInput {
                operations: vec![
                    TransactionOperation::Put(PutItemInput {
                        table_name: "Books".into(),
                        table_id: book_table_id.clone(),
                        item: book_key.clone(),
                        condition: None,
                    }),
                    TransactionOperation::Put(PutItemInput {
                        table_name: "Authors".into(),
                        table_id: author_table_id.clone(),
                        item: author_key.clone(),
                        condition: None,
                    }),
                ],
            }),
        )
        .await
        .unwrap();
    assert_eq!(committed.output.0, TransactionOutcome::Applied);
    assert_eq!(
        i64::try_from(account_records(&client, &target, &book_table_id).await.len()).unwrap(),
        stream_count + 1
    );
    assert_eq!(
        storage
            .transact_get_items(&[
                TransactGetOp {
                    key_info: &books,
                    key: &book_key
                },
                TransactGetOp {
                    key_info: &authors,
                    key: &author_key
                },
            ])
            .await
            .unwrap(),
        vec![Some(book_key.clone()), Some(author_key)]
    );
    let rolled_back = Item::from([("id".into(), AttributeValue::S("rolled-back".into()))]);
    let conditional_tx = storage
        .transact_write_items(
            &[
                TransactWriteOp::Put {
                    key_info: &books,
                    item: &rolled_back,
                    condition: None,
                    maps: &maps,
                    return_values_on_ccf: Default::default(),
                    stream: None,
                },
                TransactWriteOp::Put {
                    key_info: &books,
                    item: &book_key,
                    condition: Some(&exists_condition),
                    maps: &maps,
                    return_values_on_ccf: ReturnValuesOnConditionCheckFailure::AllOld,
                    stream: None,
                },
            ],
            None,
        )
        .await;
    assert!(matches!(
        conditional_tx,
        Err(StorageError::TransactionCanceled(reasons))
            if reasons[1].code == "ConditionalCheckFailed" && reasons[1].item == Some(book_key.clone())
    ));
    assert_eq!(storage.get_item(&books, &rolled_back).await.unwrap(), None);
    let (first_items, continuation) = storage
        .scan(&books, Some(2), None, None, None, None)
        .await
        .unwrap();
    assert_eq!(first_items.len(), 2);
    let continuation = continuation.expect("scan should have another page");
    let (second_items, end) = storage
        .scan(&books, None, Some(&continuation), None, None, None)
        .await
        .unwrap();
    assert_eq!(second_items.len(), 1);
    assert_eq!(end, None);
    storage
        .refresh_table_size("123456789012", "Books")
        .await
        .unwrap();
    let described = storage
        .describe_table(
            "123456789012",
            extenddb_core::types::DescribeTableInput {
                table_name: "Books".into(),
            },
        )
        .await
        .unwrap();
    let bytes: usize = first_items
        .iter()
        .chain(&second_items)
        .map(extenddb_core::types::item_size_bytes)
        .sum();
    assert_eq!(
        (described.item_count, described.table_size_bytes),
        (3, bytes as i64)
    );
    let scanned_ids: Vec<_> = first_items
        .into_iter()
        .chain(second_items)
        .map(|item| item["id"].clone())
        .collect();
    assert_eq!(
        scanned_ids,
        vec![
            AttributeValue::S("book-2".into()),
            AttributeValue::S("book-tx".into()),
            AttributeValue::S("conditioned".into()),
        ]
    );
    let query = KeyCondition {
        pk_path: vec![PathElement::Attribute("id".into())],
        pk_value: Expr::Placeholder("id".into()),
        extra_pk_conditions: Vec::new(),
        sk_condition: None,
        extra_sk_conditions: Vec::new(),
    };
    let query_maps = ExpressionMaps::new(
        HashMap::new(),
        HashMap::from([("id".into(), AttributeValue::S("book-tx".into()))]),
    );
    assert_eq!(
        storage
            .query(&books, &query, &query_maps, true, Some(1), None, None)
            .await
            .unwrap(),
        (vec![tx_book.clone()], None)
    );
    assert_eq!(
        storage
            .query(
                &books,
                &query,
                &query_maps,
                true,
                Some(1),
                Some(&tx_book),
                None,
            )
            .await
            .unwrap(),
        (Vec::new(), None)
    );
    let bulk_writes = (0..70)
        .map(|index| {
            TransactionOperation::Put(PutItemInput {
                table_name: "Books".into(),
                table_id: book_table_id.clone(),
                item: Item::from([("id".into(), AttributeValue::S(format!("bulk-{index:03}")))]),
                condition: None,
            })
        })
        .collect();
    client
        .command::<TransactWrite>(
            &target,
            identity(11),
            Json(TransactWriteInput {
                operations: bulk_writes,
            }),
        )
        .await
        .unwrap();
    let (bulk_first, bulk_cursor) = storage
        .scan(&books, Some(70), None, None, None, None)
        .await
        .unwrap();
    assert_eq!(bulk_first.len(), 70);
    let (bulk_second, bulk_end) = storage
        .scan(&books, None, bulk_cursor.as_ref(), None, None, None)
        .await
        .unwrap();
    assert_eq!((bulk_second.len(), bulk_end), (3, None));
    let first_page = client
        .query::<ListTables>(
            &target,
            Some(committed.receipt),
            Json(ListTablesInput {
                limit: 1,
                exclusive_start: None,
                live_only: false,
            }),
        )
        .await
        .unwrap();
    let ListTablesOutcome::Page(first_page) = first_page.output.0 else {
        panic!("expected first table page");
    };
    assert_eq!(first_page.names, vec!["Authors"]);
    assert_eq!(first_page.last_evaluated.as_deref(), Some("Authors"));
    let second_page = client
        .query::<ListTables>(
            &target,
            Some(committed.receipt),
            Json(ListTablesInput {
                limit: 1,
                exclusive_start: first_page.last_evaluated,
                live_only: false,
            }),
        )
        .await
        .unwrap();
    let ListTablesOutcome::Page(second_page) = second_page.output.0 else {
        panic!("expected second table page");
    };
    assert_eq!(second_page.names, vec!["Books"]);
    assert_eq!(second_page.last_evaluated, None);
    let protected = storage
        .delete_table(
            "123456789012",
            DeleteTableInput {
                table_name: "Authors".into(),
            },
        )
        .await;
    assert!(matches!(
        protected,
        Err(StorageError::DeletionProtected(name)) if name == "Authors"
    ));
    let update: UpdateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": "Authors",
        "DeletionProtectionEnabled": false
    }))
    .unwrap();
    let updated = storage.update_table("123456789012", update).await.unwrap();
    assert!(!updated.deletion_protection_enabled);
    let removed = storage
        .delete_table(
            "123456789012",
            DeleteTableInput {
                table_name: "Authors".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(removed.table_status, TableStatus::Deleting);
    let live_page = client
        .query::<ListTables>(
            &target,
            None,
            Json(ListTablesInput {
                limit: 1,
                exclusive_start: None,
                live_only: true,
            }),
        )
        .await
        .unwrap();
    assert!(matches!(
        live_page.output.0,
        ListTablesOutcome::Page(page) if page.names == ["Books"] && page.last_evaluated.is_none()
    ));
    let present_condition = Expr::Function {
        name: "attribute_exists".into(),
        args: vec![Expr::Path(vec![PathElement::Attribute("id".into())])],
    };
    storage
        .transact_write_items(
            &[
                TransactWriteOp::Update {
                    key_info: &books,
                    key: &book_key,
                    actions: &actions,
                    condition: None,
                    maps: &update_maps,
                    return_values_on_ccf: Default::default(),
                    stream: None,
                },
                TransactWriteOp::ConditionCheck {
                    key_info: &books,
                    key: &conditional_key,
                    condition: &present_condition,
                    maps: &maps,
                    return_values_on_ccf: Default::default(),
                },
            ],
            None,
        )
        .await
        .unwrap();
    let mut updated_book = book_key.clone();
    updated_book.insert("title".into(), AttributeValue::S("Updated".into()));
    let rollback_maps = ExpressionMaps::new(
        HashMap::new(),
        HashMap::from([("title".into(), AttributeValue::S("Rolled back".into()))]),
    );
    let failed_update = storage
        .transact_write_items(
            &[
                TransactWriteOp::Update {
                    key_info: &books,
                    key: &book_key,
                    actions: &actions,
                    condition: None,
                    maps: &rollback_maps,
                    return_values_on_ccf: Default::default(),
                    stream: None,
                },
                TransactWriteOp::ConditionCheck {
                    key_info: &books,
                    key: &rolled_back,
                    condition: &present_condition,
                    maps: &maps,
                    return_values_on_ccf: Default::default(),
                },
            ],
            None,
        )
        .await;
    assert!(matches!(
        failed_update,
        Err(StorageError::TransactionCanceled(_))
    ));
    let token_item = Item::from([("id".into(), AttributeValue::S("token-item".into()))]);
    let token_write = [TransactWriteOp::Put {
        key_info: &books,
        item: &token_item,
        condition: None,
        maps: &maps,
        return_values_on_ccf: Default::default(),
        stream: None,
    }];
    let token_key = |fingerprint| IdempotencyKey {
        account_id: "123456789012",
        token: "account-token",
        fingerprint,
    };
    storage
        .transact_write_items(&token_write, Some(token_key("original")))
        .await
        .unwrap();
    assert!(matches!(
        storage
            .transact_write_items(&token_write, Some(token_key("original")))
            .await,
        Err(StorageError::IdempotentReplay)
    ));
    assert!(matches!(
        storage
            .transact_write_items(&token_write, Some(token_key("different")))
            .await,
        Err(StorageError::IdempotentMismatch)
    ));
    // Ten legal images fit DynamoDB's 4 MiB aggregate but their base64
    // representation exceeds one Cell response. All keys share the account Cell.
    let large_keys = (0..10)
        .map(|i| Item::from([("id".into(), AttributeValue::S(format!("large-read-{i}")))]))
        .collect::<Vec<_>>();
    let mut large_items = Vec::new();
    for key in &large_keys {
        let mut item = key.clone();
        item.insert("payload".into(), AttributeValue::B(vec![0xa5; 380 * 1024]));
        storage
            .put_item(&books, item.clone(), false, None, &maps, None)
            .await
            .unwrap();
        large_items.push(Some(item));
    }
    let reads = large_keys
        .iter()
        .map(|key| TransactGetOp {
            key_info: &books,
            key,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        storage.transact_get_items(&reads).await.unwrap(),
        large_items
    );
    let ordered_table = create_account_table(&storage, &client,
            CreateTableInput {
                table_name: "Ordered".into(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "pk".into(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".into(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sk".into(),
                        attribute_type: ScalarAttributeType::N,
                    },
                ],
                billing_mode: Some(BillingMode::PayPerRequest),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    let ordered = storage
        .table_key_info("123456789012", "Ordered")
        .await
        .unwrap();
    for value in ["10", "-2", "2"] {
        storage
            .put_item(
                &ordered,
                Item::from([
                    ("pk".into(), AttributeValue::S("same".into())),
                    ("sk".into(), AttributeValue::N(value.into())),
                ]),
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .unwrap();
    }
    let ordered_condition = KeyCondition {
        pk_path: vec![PathElement::Attribute("pk".into())],
        pk_value: Expr::Placeholder("pk".into()),
        extra_pk_conditions: Vec::new(),
        sk_condition: None,
        extra_sk_conditions: Vec::new(),
    };
    let ordered_maps = ExpressionMaps::new(
        HashMap::new(),
        HashMap::from([("pk".into(), AttributeValue::S("same".into()))]),
    );
    let (first, cursor) = storage
        .query(
            &ordered,
            &ordered_condition,
            &ordered_maps,
            true,
            Some(2),
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        first.iter().map(|item| &item["sk"]).collect::<Vec<_>>(),
        vec![
            &AttributeValue::N("-2".into()),
            &AttributeValue::N("2".into())
        ]
    );
    let (last, end) = storage
        .query(
            &ordered,
            &ordered_condition,
            &ordered_maps,
            true,
            Some(2),
            cursor.as_ref(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        (last[0]["sk"].clone(), end),
        (AttributeValue::N("10".into()), None)
    );
    let (reverse, _) = storage
        .query(
            &ordered,
            &ordered_condition,
            &ordered_maps,
            false,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        reverse.iter().map(|item| &item["sk"]).collect::<Vec<_>>(),
        vec![
            &AttributeValue::N("10".into()),
            &AttributeValue::N("2".into()),
            &AttributeValue::N("-2".into())
        ]
    );
    let committed_stream_count: i64 = i64::try_from(account_records(&client, &target, &book_table_id).await.len()).unwrap();
    let tail: Option<String> = account_records(&client, &target, &book_table_id).await.last().map(|record| record.dynamodb.sequence_number.clone());
    assert_eq!(
        storage
            .sweep_account_stream_records("123456789012")
            .await
            .unwrap(),
        0
    );
    let retained_stream_count: i64 = i64::try_from(account_records(&client, &target, &book_table_id).await.len()).unwrap();
    assert_eq!(retained_stream_count, committed_stream_count);
    let shard_id = format!("shardId-{book_table_id}-account-2026-09-27T00:00:00.000");
    storage
        .validate_shard(
            "123456789012",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Books/stream/2026-09-27T00:00:00.000",
            &shard_id,
        )
        .await
        .unwrap();
    assert_eq!(
        storage.latest_sequence_number(&shard_id).await.unwrap(),
        tail
    );
    let streamed = create_account_table(&storage, &client,
            CreateTableInput {
                table_name: "Streamed".into(),
                key_schema: schema.key_schema.clone(),
                attribute_definitions: schema.attribute_definitions.clone(),
                billing_mode: Some(BillingMode::PayPerRequest),
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::NewImage),
                }),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    let stream_label = streamed.latest_stream_label.as_deref().unwrap();
    assert!(streamed.latest_stream_arn.is_some());
    let stream_key = storage
        .table_key_info("123456789012", "Streamed")
        .await
        .unwrap();
    assert_eq!(
        stream_key.stream_specification,
        streamed.stream_specification
    );
    let streamed_item = Item::from([("id".into(), AttributeValue::S("captured".into()))]);
    storage
        .put_item(
            &stream_key,
            streamed_item.clone(),
            false,
            None,
            &ExpressionMaps::default(),
            Some(&StreamCapture {
                view_type: StreamViewType::NewImage,
                user_identity: None,
                region: "us-east-1".into(),
            }),
        )
        .await
        .unwrap();
    let streamed_shard = format!("shardId-{}-account-{stream_label}", streamed.table_id);
    let (listed, _) = storage
        .list_streams("123456789012", Some("Streamed"), 100, None)
        .await
        .unwrap();
    assert_eq!(
        listed[0].stream_arn,
        streamed.latest_stream_arn.clone().unwrap()
    );
    let described_stream = storage
        .describe_stream(
            "123456789012",
            &DescribeStreamInput {
                stream_arn: streamed.latest_stream_arn.clone().unwrap(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(described_stream.shards[0].shard_id, streamed_shard);
    storage
        .validate_shard(
            "123456789012",
            streamed.latest_stream_arn.as_deref().unwrap(),
            &streamed_shard,
        )
        .await
        .unwrap();
    let (streamed_records, continuation) = storage
        .get_stream_records("123456789012", &streamed_shard, None, 100)
        .await
        .unwrap();
    assert_eq!(
        continuation,
        StreamContinuation::More(Some(streamed_records[0].dynamodb.sequence_number.clone()))
    );
    assert_eq!(streamed_records[0].dynamodb.new_image, Some(streamed_item));
    let routed_storage = CellStorage::new(client.clone(), "us-east-1")
        .with_initial_partitions(provisioner.clone());
    let routed_table = routed_storage
        .create_table(
            "123456789012",
            CreateTableInput {
                table_name: "RoutedStream".into(),
                key_schema: schema.key_schema.clone(),
                attribute_definitions: schema.attribute_definitions.clone(),
                billing_mode: Some(BillingMode::PayPerRequest),
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::KeysOnly),
                }),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    let routed_stream = routed_storage
        .describe_stream(
            "123456789012",
            &DescribeStreamInput {
                stream_arn: routed_table.latest_stream_arn.clone().unwrap(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(routed_stream.shards.len(), 4);
    let routed_key = routed_storage
        .table_key_info("123456789012", "RoutedStream")
        .await
        .unwrap();
    routed_storage
        .put_item(
            &routed_key,
            Item::from([("id".into(), AttributeValue::S("routed".into()))]),
            false,
            None,
            &ExpressionMaps::default(),
            Some(&StreamCapture {
                view_type: StreamViewType::KeysOnly,
                user_identity: None,
                region: "us-east-1".into(),
            }),
        )
        .await
        .unwrap();
    let mut routed_records = Vec::new();
    for shard in &routed_stream.shards {
        let (records, _) = routed_storage.get_stream_records("123456789012", &shard.shard_id, None, 100).await.unwrap();
        routed_records.extend(records);
    }
    assert_eq!(routed_records.len(), 1);
    let before = client.query::<ReadStreamGcShard>(&target, None, Json(())).await.unwrap().output.0;
    let beyonddb::RoutePageOutcome::Page { partitions, has_more: false, .. } = beyonddb::read_route_page(&client, &target, beyonddb::RoutePageInput { table_id: routed_table.table_id.clone(), start_key: None, after_lower: None, expected_epoch: None }).await.unwrap() else { panic!("missing routed stream ranges"); };
    for range in partitions {
        let owner = beyonddb::data_target("123456789012", &routed_table.table_id, &range.partition_id).unwrap();
        let installed = client.query::<beyonddb::ReadPartitionState>(&owner, None, Json(())).await.unwrap().output.0.unwrap();
        assert_eq!(installed.state, beyonddb::PartitionState::Serving);
        assert_eq!(routed_storage.sweep_partition_stream_records(&owner).await.unwrap(), 0);
    }
    client.command::<beyonddb::AdvanceStreamGcShard>(&target, identity(90), Json(before)).await.unwrap();
    let after = client.query::<ReadStreamGcShard>(&target, None, Json(())).await.unwrap().output.0;
    assert_eq!(after, before.wrapping_add(1));
    let (first_streams, cursor) = storage
        .list_streams("123456789012", None, 1, None)
        .await
        .unwrap();
    let (second_streams, _) = storage
        .list_streams("123456789012", None, 1, cursor.as_deref())
        .await
        .unwrap();
    assert_ne!(first_streams[0].stream_arn, second_streams[0].stream_arn);
    storage
        .delete_table(
            "123456789012",
            DeleteTableInput {
                table_name: "Streamed".into(),
            },
        )
        .await
        .unwrap();
    let (retained, _) = storage
        .list_streams("123456789012", Some("Streamed"), 100, None)
        .await
        .unwrap();
    assert_eq!(retained.len(), 1);
    let retained_description = storage
        .describe_stream(
            "123456789012",
            &DescribeStreamInput {
                stream_arn: streamed.latest_stream_arn.clone().unwrap(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        retained_description.stream_status,
        extenddb_core::types::StreamStatus::Disabled
    );
    let (retained_records, continuation) = storage
        .get_stream_records("123456789012", &streamed_shard, None, 100)
        .await
        .unwrap();
    assert_eq!(retained_records.len(), 1);
    assert_eq!(continuation, StreamContinuation::End);
    create_account_table(&storage, &client,
            CreateTableInput {
                table_name: "Streamed".into(),
                key_schema: schema.key_schema.clone(),
                attribute_definitions: schema.attribute_definitions.clone(),
                billing_mode: Some(BillingMode::PayPerRequest),
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::KeysOnly),
                }),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    let (generations, _) = storage
        .list_streams("123456789012", Some("Streamed"), 100, None)
        .await
        .unwrap();
    assert_eq!(generations.len(), 2);
    let (first_generation, cursor) = storage
        .list_streams("123456789012", Some("Streamed"), 1, None)
        .await
        .unwrap();
    let (next_generation, _) = storage
        .list_streams("123456789012", Some("Streamed"), 1, cursor.as_deref())
        .await
        .unwrap();
    assert_ne!(
        first_generation[0].stream_arn,
        next_generation[0].stream_arn
    );
    let (old_records, continuation) = storage
        .get_stream_records("123456789012", &streamed_shard, None, 100)
        .await
        .unwrap();
    assert_eq!(old_records.len(), 1);
    assert_eq!(continuation, StreamContinuation::End);
    // Handoff needs an eligible successor. Start an independent SDK owner with
    // an empty local directory before stopping the original owner.
    let (restored_node, restored_client) = cellar_node::start(application, &directory.path().join("owner-b"), host.handle(), core.handle(), bucket).await.unwrap();
    cellar_node::stop(node, cellar::sdk::ShutdownMode::Handoff).await.unwrap();
    std::fs::remove_dir_all(first_owner).unwrap();
    let mut after_sequence = None;
    let mut restored_stream_count = 0;
    loop {
        let restored_streams = restored_client
            .query::<ReadAccountStreamJournal>(
                &target,
                None,
                Json(StreamJournalInput {
                    table_id: book_table_id.clone(),
                    label: "2026-09-27T00:00:00.000".into(),
                    after_sequence,
                    limit: 1_000,
                }),
            )
            .await
            .unwrap();
        let StreamJournalOutcome::Page {
            records,
            last_sequence,
            closed,
            ..
        } = restored_streams.output.0
        else {
            panic!("restored stream generation is missing");
        };
        assert!(!closed);
        restored_stream_count += i64::try_from(records.len()).unwrap();
        let Some(next) = last_sequence else {
            break;
        };
        after_sequence = Some(next);
    }
    assert_eq!(restored_stream_count, committed_stream_count);
    let restored_sweep = restored_client
        .query::<ReadTtlSweep>(&target, None, Json("Books".into()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert_eq!(restored_sweep.after_lower, Some(RangeKey::new(vec![7; 16]).unwrap()));
    let restored_schedule = restored_client
        .query::<ReadTtlSchedule>(&target, None, Json(()))
        .await
        .unwrap();
    assert_eq!(restored_schedule.output.0.as_deref(), Some("Books"));
    let persisted = restored_client
        .query::<GetItem>(
            &target,
            Some(committed.receipt),
            Json(GetItemInput {
                table_name: "Books".into(),
                table_id: book_table_id.clone(),
                key: book_key.clone(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        persisted.output.0,
        GetItemOutcome::Found(Some(updated_book))
    );
    let persisted_token = restored_client
        .query::<GetItem>(
            &target,
            None,
            Json(GetItemInput {
                table_name: "Books".into(),
                table_id: book_table_id.clone(),
                key: token_item.clone(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        persisted_token.output.0,
        GetItemOutcome::Found(Some(token_item))
    );
    let persisted_update = restored_client
        .query::<GetItem>(
            &target,
            None,
            Json(GetItemInput {
                table_name: "Books".into(),
                table_id: book_table_id,
                key: conditional_key,
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        persisted_update.output.0,
        GetItemOutcome::Found(Some(expected_updated))
    );
    let absent_table = restored_client
        .query::<DescribeTable>(&target, None, Json("Authors".into()))
        .await
        .unwrap();
    assert_eq!(absent_table.output.0, None);
    let restored_order = restored_client
        .query::<QueryAccountItems>(
            &target,
            None,
            Json(PartitionQueryInput {
                table_id: ordered_table.table_id,
                epoch: 0,
                index_name: None,
                partition_key: Item::from([("pk".into(), AttributeValue::S("same".into()))]),
                sort: None,
                extra_range_equals: Vec::new(),
                forward: true,
                limit: 10,
                exclusive_start_key: None,
            }),
        )
        .await
        .unwrap();
    let PartitionQueryOutcome::Page {
        items,
        last_evaluated_key: None,
    } = restored_order.output.0
    else {
        panic!("restored account query must return a complete page");
    };
    assert_eq!(
        items.iter().map(|item| &item["sk"]).collect::<Vec<_>>(),
        vec![
            &AttributeValue::N("-2".into()),
            &AttributeValue::N("2".into()),
            &AttributeValue::N("10".into())
        ]
    );
    cellar_node::stop(restored_node, cellar::sdk::ShutdownMode::Preserve).await.unwrap();
    });
}

// Explicit private account-table commands retain coverage of the account module.
// Public CreateTable always provisions multiple data cells in the serving fixture.
async fn create_account_table(
    storage: &CellStorage,
    client: &CellClient,
    input: CreateTableInput,
) -> Result<extenddb_core::types::TableDescription, StorageError> {
    let table_name = input.table_name.clone();
    let stream = input
        .stream_specification
        .filter(|stream| stream.stream_enabled)
        .map(|stream| StreamConfig {
            region: "us-east-1".into(),
            label: uuid::Uuid::now_v7().to_string(),
            view_type: stream.stream_view_type.unwrap(),
        });
    client
        .command::<CreateTable>(
            &account_target("123456789012").unwrap(),
            MutationIdentity {
                request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
                ..identity(99)
            },
            Json(TableSpec {
                table_class: Default::default(),
                placement: beyonddb::TablePlacement::Account,
                table_name,
                key_schema: input.key_schema,
                attribute_definitions: input.attribute_definitions,
                local_secondary_indexes: input.local_secondary_indexes.unwrap_or_default(),
                global_secondary_indexes: input.global_secondary_indexes.unwrap_or_default(),
                billing_mode: input.billing_mode.unwrap_or(BillingMode::PayPerRequest),
                provisioned_throughput: input.provisioned_throughput,
                deletion_protection_enabled: input.deletion_protection_enabled.unwrap_or(false),
                initial_tags: Vec::new(),
                resource_arn: None,
                stream,
            }),
        )
        .await
        .unwrap();
    storage
        .describe_table(
            "123456789012",
            extenddb_core::types::DescribeTableInput {
                table_name: input.table_name,
            },
        )
        .await
}
async fn account_records(
    client: &CellClient,
    target: &CellTarget,
    table_id: &str,
) -> Vec<StreamRecord> {
    let mut records = Vec::new();
    let mut after = None;
    loop {
        let page = client
            .query::<ReadAccountStreamJournal>(
                target,
                None,
                Json(StreamJournalInput {
                    table_id: table_id.into(),
                    label: "2026-09-27T00:00:00.000".into(),
                    after_sequence: after,
                    limit: 1_000,
                }),
            )
            .await
            .unwrap()
            .output
            .0;
        let StreamJournalOutcome::Page {
            records: page,
            last_sequence,
            ..
        } = page
        else {
            panic!("account stream generation missing");
        };
        records.extend(page);
        let Some(next) = last_sequence else {
            break;
        };
        after = Some(next);
    }
    records
}
