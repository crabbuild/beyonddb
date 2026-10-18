//! Bounded serving pages over real data cells and a grown metadata-only tree.
use crate::{
    cellar_node,
    directory_fixture::{ACCOUNT, application, mutation, ranges},
};
use beyonddb::*;
use cellar_runtime::{
    client::{CellClient, InvocationError},
    identity::CellTarget,
};
use extenddb_core::{
    expression::ExpressionMaps,
    types::{AttributeValue, Item, StreamEventName, TableStatus},
};
use extenddb_storage::{DataEngine, MetadataEngine, TableEngine, error::StorageError};
use keyspace::{Component, KeyKind, KeySchema, RangeKey};
use std::sync::Arc;

fn input(name: &str) -> extenddb_core::types::CreateTableInput {
    serde_json::from_value(serde_json::json!({
        "TableName": name, "BillingMode":"PAY_PER_REQUEST",
        "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
        "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
        "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"KEYS_ONLY"}
    }))
    .unwrap()
}
fn page(table: &str) -> RoutePageInput {
    RoutePageInput {
        table_id: table.into(),
        start_key: None,
        after_lower: None,
        expected_epoch: None,
    }
}
async fn record(client: &CellClient, name: &str) -> TableRecord {
    client
        .query::<DescribeTable>(&account_target(ACCOUNT).unwrap(), None, Json(name.into()))
        .await
        .unwrap()
        .output
        .0
        .unwrap()
}

#[test]
fn route_pages_cover_many_ranges_without_full_route_result() {
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
        let app = application();
        let files = tempfile::tempdir().unwrap();
        let bucket = cellar::bucket::Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let (node, client) = cellar_node::start_limited(app.clone(), &files.path().join("route-pages-owner"), host.handle(), core.handle(), bucket, 256).await.unwrap();
        let provisioner = Arc::new(CellInitialPartitionProvisioner::new(client.clone(), app.clone()).with_initial_partition_count(65).unwrap());
        let storage = CellStorage::new(client.clone(), "us-east-1").with_initial_partitions(provisioner);
        storage.create_table(ACCOUNT, input("ManyRanges")).await.unwrap();
        let table = record(&client, "ManyRanges").await;
        assert_eq!(table.placement.initial_partitions(), Some(65));
        let account = account_target(ACCOUNT).unwrap();
        let RoutePageOutcome::Page {epoch: 1, partitions: first, has_more: true} = read_route_page(&client, &account, page(&table.id)).await.unwrap() else {panic!("missing first route page")};
        assert_eq!(first.len(), 64);
        let RoutePageOutcome::Page {epoch: 1, partitions: second, has_more: false} = read_route_page(&client, &account, RoutePageInput {
            after_lower: Some(first[63].lower.clone()), expected_epoch: Some(1), ..page(&table.id)
        }).await.unwrap() else {panic!("missing final route page")};
        assert_eq!(second.len(), 1);
        assert_eq!(first[63].upper.as_ref(), Some(&second[0].lower));
        assert_eq!(read_route_page(&client, &account, RoutePageInput {after_lower: Some(second[0].lower.clone()), ..page(&table.id)}).await.unwrap(),
            RoutePageOutcome::Page {epoch: 1, partitions: vec![], has_more: false});
        assert_eq!(read_route_page(&client, &account, RoutePageInput {after_lower: Some(first[63].lower.clone()), expected_epoch: Some(2), ..page(&table.id)}).await.unwrap(), RoutePageOutcome::Changed);
        let all: Vec<_> = first.iter().chain(&second).cloned().collect();
        assert_eq!(all.len(), 65);
        let mut identities = std::collections::HashSet::new();
        for range in &all {
            let target = data_target(ACCOUNT, &table.id, &range.partition_id).unwrap();
            assert!(identities.insert(target.cell_id()));
            let state = client.query::<ReadPartitionState>(&target, None, Json(())).await.unwrap().output.0.unwrap();
            assert_eq!(state.spec.partition_id, range.partition_id);
            assert_eq!(state.spec.lower.unwrap_or_else(RangeKey::minimum), range.lower);
            assert_eq!(state.spec.upper, range.upper);
            assert_eq!(state.spec.table, table);
        }
        eprintln!("65 independently installed ordered ranges, two directory pages");
        let first_item = Item::from([("id".into(), AttributeValue::S("\u{1}first".into()))]);
        let last_item = Item::from([("id".into(), AttributeValue::S("\u{100}last".into()))]);
        for (item, range) in [(&first_item, &all[0]), (&last_item, &all[64])] {
            let key = ordered_data_key(&table.id, item, &table.key_schema).unwrap();
            assert!(key >= range.lower && range.upper.as_ref().is_none_or(|upper| &key < upper));
        }
        assert_eq!(read_route_page(&client, &account, RoutePageInput {
            start_key: Some(ordered_data_key(&table.id, &last_item, &table.key_schema).unwrap()), ..page(&table.id)
        }).await.unwrap(), RoutePageOutcome::Page {epoch: 1, partitions: second.clone(), has_more: false});
        let info = storage.table_key_info(ACCOUNT, "ManyRanges").await.unwrap();
        for item in [&first_item, &last_item] {
            storage.put_item(&info, item.clone(), false, None, &ExpressionMaps::default(), None).await.unwrap();
        }
        let (items, after) = storage.scan(&info, Some(1), None, None, None, None).await.unwrap();
        assert_eq!(items, std::slice::from_ref(&first_item));
        assert!(after.is_some());
        let (items, end) = storage.scan(&info, Some(1), after.as_ref(), None, None, None).await.unwrap();
        assert_eq!(items, std::slice::from_ref(&last_item));
        assert!(end.is_none());
        for key in [&first_item, &last_item] {
            let mut item = key.clone(); item.insert("expires".into(), AttributeValue::N("1".into()));
            storage.put_item(&info, item, false, None, &ExpressionMaps::default(), None).await.unwrap();
        }
        let first_owner = data_target(ACCOUNT, &table.id, &all[0].partition_id).unwrap();
        let early = client.command::<PartitionDelete>(&first_owner, mutation(), Json(PartitionDeleteInput {
            table_id: table.id.clone(), epoch: all[0].epoch, key: first_item.clone(), condition: None, return_old: false, ttl: true,
        })).await;
        assert!(matches!(early, Err(InvocationError::Rejected(result)) if matches!(result.output.0, PartitionDeleteOutcome::ConditionFailed(_))));
        storage.update_ttl(ACCOUNT, "ManyRanges", "expires", true).await.unwrap();
        assert!(matches!(storage.create_ttl_index(ACCOUNT, "ManyRanges", "expires").await, Err(StorageError::Transient(_))));
        assert_eq!(storage.sweep_account_ttl(ACCOUNT).await.unwrap(), 1);
        assert_eq!(storage.get_item(&info, &first_item).await.unwrap(), None);
        let journal = client.query::<ReadPartitionStreamJournal>(&first_owner, None, Json(StreamJournalInput {
            table_id: table.id.clone(), label: table.stream.as_ref().unwrap().label.clone(), after_sequence: None, limit: 100,
        })).await.unwrap().output.0;
        let StreamJournalOutcome::Page {records, ..} = journal else {panic!("missing TTL stream")};
        let removal = records.iter().find(|record| record.event_name == StreamEventName::Remove).unwrap();
        let service = removal.user_identity.as_ref().unwrap();
        assert_eq!((service.identity_type.as_str(), service.principal_id.as_str()), ("Service", "dynamodb.amazonaws.com"));
        assert!(storage.get_item(&info, &last_item).await.unwrap().is_some());
        let sweep = client.query::<ReadTtlSweep>(&account, None, Json("ManyRanges".into())).await.unwrap().output.0.unwrap();
        assert_eq!(sweep.after_lower, Some(first[63].lower.clone()));
        assert_eq!(storage.sweep_account_ttl(ACCOUNT).await.unwrap(), 1);
        assert_eq!(storage.get_item(&info, &last_item).await.unwrap(), None);
        assert!(client.query::<ReadTtlSweep>(&account, None, Json("ManyRanges".into())).await.unwrap().output.0.unwrap().after_lower.is_none());
        storage.update_ttl(ACCOUNT, "ManyRanges", "expires", false).await.unwrap();
        storage.drop_ttl_index(ACCOUNT, "ManyRanges", "expires").await.unwrap();
        let mut retained = first_item.clone(); retained.insert("expires".into(), AttributeValue::N("1".into()));
        storage.put_item(&info, retained.clone(), false, None, &ExpressionMaps::default(), None).await.unwrap();
        assert_eq!(storage.sweep_account_ttl(ACCOUNT).await.unwrap(), 0);
        assert_eq!(storage.get_item(&info, &first_item).await.unwrap(), Some(retained));
        // The table scheduler still stops after sixteen tables. Every auxiliary
        // table uses public two-cell creation rather than an account fallback.
        let provisioner = Arc::new(CellInitialPartitionProvisioner::new(client.clone(), app).with_initial_partition_count(2).unwrap());
        let creator = CellStorage::new(client.clone(), "us-east-1").with_initial_partitions(provisioner);
        for index in 0..17 {
            let name = format!("A{index:02}");
            creator.create_table(ACCOUNT, input(&name)).await.unwrap();
            storage.update_ttl(ACCOUNT, &name, "expires", true).await.unwrap();
        }
        assert_eq!(storage.sweep_account_ttl(ACCOUNT).await.unwrap(), 0);
        assert_eq!(client.query::<ReadTtlSchedule>(&account, None, Json(())).await.unwrap().output.0.as_deref(), Some("A15"));
        assert_eq!(storage.sweep_account_ttl(ACCOUNT).await.unwrap(), 0);
        assert_eq!(client.query::<ReadTtlSchedule>(&account, None, Json(())).await.unwrap().output.0, None);
        cellar_node::stop(node, cellar::sdk::ShutdownMode::Preserve).await.unwrap();
    });
}

fn spec(table: &TableRecord, range: &RoutePagePartition) -> PartitionSpec {
    PartitionSpec {
        table: table.clone(),
        partition_id: range.partition_id,
        lower: (range.lower != RangeKey::minimum()).then(|| range.lower.clone()),
        upper: range.upper.clone(),
        epoch: range.epoch,
    }
}
async fn pages(client: &CellClient, account: &CellTarget, table: &str) -> Vec<RoutePagePartition> {
    let mut input = page(table);
    let mut actual = Vec::new();
    loop {
        let RoutePageOutcome::Page {
            epoch,
            partitions,
            has_more,
        } = read_route_page(client, account, input.clone())
            .await
            .unwrap()
        else {
            panic!("stable tree must remain pageable")
        };
        assert!(!partitions.is_empty() && partitions.len() <= 64);
        input.after_lower = partitions.last().map(|range| range.lower.clone());
        input.expected_epoch = Some(epoch);
        actual.extend(partitions);
        if !has_more {
            return actual;
        }
    }
}
#[test]
fn route_pages_cross_a_grown_directory_tree_and_recover_leaf_transfer() {
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
        let app = application();
        let files = tempfile::tempdir().unwrap();
        let bucket =
            cellar::bucket::Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let original = files.path().join("large-pages-owner-a");
        let (node, client) = cellar_node::start(
            app.clone(),
            &original,
            host.handle(),
            core.handle(),
            bucket.clone(),
        )
        .await
        .unwrap();
        let account = account_target(ACCOUNT).unwrap();
        // This is a metadata fixture: 256 valid initial entries grow to 1,024
        // through committed replacements. It does not install 1,024 data cells.
        let CreateTableOutcome::Created(table) = client
            .command::<CreateTable>(
                &account,
                mutation(),
                Json(TableSpec {
                    table_class: Default::default(),
                    placement: TablePlacement::Routed {
                        initial_partitions: 256,
                    },
                    table_name: "MoreRanges".into(),
                    key_schema: serde_json::from_value(
                        serde_json::json!([{"AttributeName":"id","KeyType":"HASH"}]),
                    )
                    .unwrap(),
                    attribute_definitions: serde_json::from_value(
                        serde_json::json!([{"AttributeName":"id","AttributeType":"S"}]),
                    )
                    .unwrap(),
                    billing_mode: extenddb_core::types::BillingMode::PayPerRequest,
                    provisioned_throughput: None,
                    local_secondary_indexes: vec![],
                    global_secondary_indexes: vec![],
                    deletion_protection_enabled: false,
                    initial_tags: vec![],
                    resource_arn: None,
                    stream: None,
                }),
            )
            .await
            .unwrap()
            .output
            .0
        else {
            panic!("unexpected catalog creation")
        };
        let provisioner = Arc::new(CellInitialPartitionProvisioner::new(
            client.clone(),
            app.clone(),
        ));
        let initial = ranges(256);
        let receipt = provisioner
            .provision_route_directory(&client, ACCOUNT, &table.id, initial.clone())
            .await
            .unwrap();
        client
            .command::<ActivateTableRoute>(
                &account,
                mutation(),
                Json(TableRoutePublication {
                    receipt,
                    route: TableRoute {
                        table_id: table.id.clone(),
                        epoch: 1,
                        partitions: initial.iter().map(|range| spec(&table, range)).collect(),
                    },
                }),
            )
            .await
            .unwrap();
        let root = DirectorySpec::root(table.id.clone());
        let directory = directory_target(ACCOUNT, &root).unwrap();
        let schema = KeySchema {
            partition: KeyKind::String,
            sort: None,
        };
        let mut expected = Vec::new();
        for (ordinal, source) in initial.into_iter().enumerate() {
            let mut source = source;
            for cut in 1..=3 {
                let boundary = schema
                    .encode(
                        Component::String(&format!("range-{ordinal:04}/sub-{cut}")),
                        None,
                    )
                    .unwrap();
                let mut left = source.clone();
                let mut right = source.clone();
                left.partition_id =
                    (0x1_0000 + ordinal as u128 * 8 + cut as u128 * 2).to_be_bytes();
                right.partition_id =
                    (0x1_0001 + ordinal as u128 * 8 + cut as u128 * 2).to_be_bytes();
                left.upper = Some(boundary.clone());
                right.lower = boundary;
                left.epoch += 1;
                right.epoch += 1;
                let change = DirectoryChange {
                    source,
                    children: [left.clone(), right.clone()],
                };
                client
                    .command::<BeginDirectoryChange>(&directory, mutation(), Json(change.clone()))
                    .await
                    .unwrap();
                client
                    .command::<PublishDirectoryChange>(&directory, mutation(), Json(change.clone()))
                    .await
                    .unwrap();
                client
                    .command::<FinishDirectoryChange>(&directory, mutation(), Json(change))
                    .await
                    .unwrap();
                expected.push(left);
                source = right;
            }
            expected.push(source);
        }
        assert_eq!(expected.len(), 1024);
        assert_eq!(pages(&client, &account, &table.id).await, expected);
        eprintln!("1,024 metadata ranges grown from the recorded 256-entry plan");
        provisioner
            .split_directory(&client, ACCOUNT, &root)
            .await
            .unwrap();
        let source = spec(&table, &expected[512]);
        let boundary = schema
            .encode(Component::String("range-0128/sub-0"), None)
            .unwrap();
        assert!(
            source.lower.as_ref().unwrap() < &boundary
                && &boundary < source.upper.as_ref().unwrap()
        );
        let mut left = source.clone();
        let mut right = source.clone();
        left.partition_id = [0xff; 16];
        right.partition_id = [0xfe; 16];
        left.upper = Some(boundary.clone());
        right.lower = Some(boundary);
        left.epoch += 1;
        right.epoch += 1;
        let plan = SplitPlan {
            source,
            children: [left.clone(), right.clone()],
        };
        let leaf = route_directory_target(
            &client,
            &account,
            &table.id,
            plan.source.lower.clone().unwrap(),
        )
        .await
        .unwrap();
        client
            .command::<BeginDirectoryTransfer>(&leaf, mutation(), Json(plan.clone().into()))
            .await
            .unwrap();
        assert_eq!(
            client
                .query::<ReadDirectoryTransfer>(
                    &leaf,
                    None,
                    Json(DirectoryPartitionInput {
                        table_id: table.id.clone(),
                        partition_id: plan.source.partition_id
                    })
                )
                .await
                .unwrap()
                .output
                .0,
            Some(plan.clone().into())
        );
        client
            .command::<PublishDirectoryTransfer>(&leaf, mutation(), Json(plan.clone().into()))
            .await
            .unwrap();
        let range = |part: &PartitionSpec| RoutePagePartition {
            partition_id: part.partition_id,
            lower: part.lower.clone().unwrap_or_else(RangeKey::minimum),
            upper: part.upper.clone(),
            epoch: part.epoch,
        };
        expected.splice(512..=512, [range(&left), range(&right)]);
        assert_eq!(pages(&client, &account, &table.id).await, expected);
        let (restored, next) = cellar_node::start(
            app.clone(),
            &files.path().join("large-pages-owner-b"),
            host.handle(),
            core.handle(),
            bucket,
        )
        .await
        .unwrap();
        cellar_node::stop(node, cellar::sdk::ShutdownMode::Handoff)
            .await
            .unwrap();
        std::fs::remove_dir_all(original).unwrap();
        assert_eq!(pages(&next, &account, &table.id).await, expected);
        let leaf = route_directory_target(&next, &account, &table.id, left.lower.clone().unwrap())
            .await
            .unwrap();
        assert_eq!(
            next.query::<ReadDirectoryTransfer>(
                &leaf,
                None,
                Json(DirectoryPartitionInput {
                    table_id: table.id.clone(),
                    partition_id: left.partition_id
                })
            )
            .await
            .unwrap()
            .output
            .0,
            Some(plan.clone().into())
        );
        next.command::<FinishDirectoryTransfer>(&leaf, mutation(), Json(plan.into()))
            .await
            .unwrap();
        let adapter = CellStorage::new(next.clone(), "us-east-1")
            .with_initial_partitions(Arc::new(CellInitialPartitionProvisioner::new(next, app)));
        assert_eq!(
            adapter
                .table_key_info(ACCOUNT, "MoreRanges")
                .await
                .unwrap()
                .table_id,
            table.id
        );
        assert_eq!(
            adapter
                .describe_table(
                    ACCOUNT,
                    serde_json::from_value(serde_json::json!({"TableName":"MoreRanges"})).unwrap()
                )
                .await
                .unwrap()
                .table_status,
            TableStatus::Active
        );
        cellar_node::stop(restored, cellar::sdk::ShutdownMode::Preserve)
            .await
            .unwrap();
    });
}
