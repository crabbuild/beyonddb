use crate::{
    directory_fault::BlockedDirectory,
    native_peer::{self, FRONT, Inspection, Observations, Peer, SOURCE},
    signed_http::SignedHttp,
};
use bdb_cellar_runtime::{
    MutationIdentity,
    app::CompiledApplication,
    client::{CellClient, InvocationError},
    identity::{CellTarget, RequestId},
};
use beyonddb::*;
use extenddb_core::{
    expression::{Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition},
    types::{AttributeValue, Item, TableKeyInfo},
};
use extenddb_storage::{DataEngine, TableEngine};
use std::{
    collections::HashMap,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
pub(super) const ACCOUNT: &str = "123456789012";
pub(super) fn mutation() -> MutationIdentity {
    let now = native_peer::now_ms();
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    }
}
pub(super) fn runtimes() -> (tokio::runtime::Runtime, tokio::runtime::Runtime) {
    (
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    )
}
pub(super) async fn owner(
    application: Arc<CompiledApplication>,
    bucket: cellar::bucket::Bucket,
    directory: &Path,
    observed: Observations,
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
) -> (
    Peer,
    Arc<CellInitialPartitionProvisioner>,
    CellClient,
    CellStorage,
) {
    let peer = Peer::start(
        application.clone(),
        directory,
        bucket,
        observed,
        true,
        host,
        core,
    )
    .await;
    let client = peer.client.clone();
    let provisioner = Arc::new(
        CellInitialPartitionProvisioner::new(client.clone(), application)
            .with_initial_partition_count(2)
            .unwrap(),
    );
    let storage = CellStorage::new(client.clone(), "us-east-1")
        .with_initial_partitions(provisioner.clone())
        .with_transaction_coordinators(provisioner.clone());
    (peer, provisioner, client, storage)
}

pub(super) async fn ranges(client: &CellClient, table: &str) -> Vec<RoutePagePartition> {
    let RoutePageOutcome::Page {
        partitions,
        has_more: false,
        ..
    } = read_route_page(
        client,
        &account_target(ACCOUNT).unwrap(),
        RoutePageInput {
            table_id: table.into(),
            start_key: None,
            after_lower: None,
            expected_epoch: None,
        },
    )
    .await
    .unwrap()
    else {
        panic!("private GSI fixture must have one bounded page");
    };
    partitions
}
pub(super) fn contains(range: &RoutePagePartition, key: &bdb_keyspace::RangeKey) -> bool {
    &range.lower <= key && range.upper.as_ref().is_none_or(|upper| key < upper)
}

async fn journal(
    client: &CellClient,
    source: &CellTarget,
    table: &str,
) -> Option<serde_json::Value> {
    let header = client
        .query::<ReadPartitionIndexChange>(source, None, Json(table.into()))
        .await
        .unwrap()
        .output
        .0?;
    let mut bytes = Vec::new();
    while bytes.len() < header.bytes as usize {
        let part = client
            .query::<ReadPartitionIndexChangeChunk>(
                source,
                None,
                Json(IndexChangeChunk {
                    id: header.id,
                    offset: bytes.len() as u32,
                }),
            )
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        assert!(!part.is_empty() && part.len() <= 128 * 1024);
        bytes.extend(part);
    }
    assert_eq!(bytes.len(), header.bytes as usize);
    Some(serde_json::from_slice(&bytes).unwrap())
}

async fn indexed_items(storage: &CellStorage, base: &TableKeyInfo, index: &str) -> Vec<Item> {
    let info = TableKeyInfo {
        key_schema: base
            .global_secondary_indexes
            .iter()
            .find(|entry| entry.index_name == index)
            .unwrap()
            .key_schema
            .clone(),
        ..base.clone()
    };
    let condition = KeyCondition {
        pk_path: vec![PathElement::Attribute("group".into())],
        pk_value: Expr::Placeholder("g".into()),
        extra_pk_conditions: vec![],
        sk_condition: None,
        extra_sk_conditions: vec![],
    };
    let maps = ExpressionMaps::new(
        HashMap::new(),
        HashMap::from([("g".into(), AttributeValue::S("same".into()))]),
    );
    storage
        .query(&info, &condition, &maps, true, None, None, Some(index))
        .await
        .unwrap()
        .0
}

#[test]
fn journal_recovers_partial_projection_and_fences_delayed_images() {
    let (host, core) = runtimes();
    host.block_on(async {
    let application = native_peer::application();
    let account = account_target(ACCOUNT).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite")).unwrap();
    let observed = Arc::new(Mutex::new(Default::default()));
    let source_path = directory.path().join(SOURCE);
    let (mut node, provisioner, client, storage) = owner(application.clone(), bucket.clone(), &source_path, observed.clone(), host.handle(), core.handle()).await;
    provisioner.admit_account(ACCOUNT).await.unwrap();
    storage.create_table(ACCOUNT, serde_json::from_value(serde_json::json!({
        "TableName":"JournalItems", "BillingMode":"PAY_PER_REQUEST",
        "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
        "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"},{"AttributeName":"group","AttributeType":"S"},{"AttributeName":"score","AttributeType":"N"},{"AttributeName":"bin","AttributeType":"B"}],
        "GlobalSecondaryIndexes":[{"IndexName":"ByGroup","KeySchema":[{"AttributeName":"group","KeyType":"HASH"},{"AttributeName":"score","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}, {"IndexName":"ByBinary","KeySchema":[{"AttributeName":"group","KeyType":"HASH"},{"AttributeName":"bin","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}]
    })).unwrap()).await.unwrap();
    let base = storage
        .table_key_info(ACCOUNT, "JournalItems")
        .await
        .unwrap();
    let route = ranges(&client, &base.table_id).await;
    assert_eq!(route.len(), 2);
    let table = client.query::<DescribeTableById>(&account, None, Json(base.table_id.clone())).await.unwrap().output.0.unwrap();
    let original = Item::from([
        ("id".into(), AttributeValue::S("a".into())),
        ("group".into(), AttributeValue::S("same".into())),
        ("score".into(), AttributeValue::N("1".into())),
        ("bin".into(), AttributeValue::B(vec![0xff, 0xff, 1])),
        ("payload".into(), AttributeValue::S("x".repeat(350 * 1024))),
    ]);
    let encoded = ordered_data_key(&base.table_id, &original, &base.base_key_schema).unwrap();
    let range = route.iter().find(|range| contains(range, &encoded)).unwrap();
    let source = data_target(ACCOUNT, &base.table_id, &range.partition_id).unwrap();
    storage
        .put_item(
            &base,
            original.clone(),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .unwrap();
    assert!(
        indexed_items(&storage, &base, "ByGroup").await.is_empty(),
        "base commit must enqueue durable asynchronous work"
    );
    let first = journal(&client, &source, &base.table_id).await.unwrap();
    let index = &table.global_secondary_indexes[0];
    let encoded = ordered_index_key(&original, &index.specification.key_schema, &table.key_schema).unwrap();
    let index_ranges = provisioner.provision_global_index(&client, ACCOUNT, &table, index).await.unwrap();
    assert_eq!(index_ranges.len(), 2);
    let index_range = index_ranges.iter().find(|range| range.lower.as_ref().is_none_or(|low| &encoded >= low) && range.upper.as_ref().is_none_or(|high| &encoded < high)).unwrap();
    let destination = global_index_target(ACCOUNT, &index.id, &index_range.partition_id).unwrap();
    let entry_key = |item: &Item| {
        Item::from([
            ("id".into(), item["id"].clone()),
            ("group".into(), item["group"].clone()),
            ("score".into(), item["score"].clone()),
        ])
    };
    let first_mutation = GlobalIndexMutation {
        index_id: index.id.clone(),
        epoch: index_range.epoch,
        key: entry_key(&original),
        version: serde_json::from_value(first["version"].clone()).unwrap(),
        item: Some(original.clone()),
    };
    for expected in [
        GlobalIndexApplyOutcome::Applied,
        GlobalIndexApplyOutcome::Replay,
    ] {
        assert_eq!(
            client
                .command::<ApplyGlobalIndexMutation>(
                    &destination,
                    mutation(),
                    Json(first_mutation.clone())
                )
                .await
                .unwrap()
                .output
                .0,
            expected
        );
    }
    let mut changed = original.clone();
    changed.insert("score".into(), AttributeValue::N("2".into()));
    storage
        .put_item(
            &base,
            changed.clone(),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .unwrap();
    // A lost acknowledgement leaves the first image at the head. Replaying it
    // must advance the journal without replacing the already committed row.
    assert!(
        storage
            .project_index_changes(ACCOUNT, &source, &base.table_id)
            .await
            .unwrap()
    );
    let next = journal(&client, &source, &base.table_id).await.unwrap();
    let next_version: ProjectionVersion = serde_json::from_value(next["version"].clone()).unwrap();
    assert!(next_version > first_mutation.version);
    assert!(serde_json::to_vec(&next).unwrap().len() > 700 * 1024);
    let tombstone = GlobalIndexMutation {
        version: next_version,
        item: None,
        ..first_mutation.clone()
    };
    client
        .command::<ApplyGlobalIndexMutation>(&destination, mutation(), Json(tombstone))
        .await
        .unwrap();
    assert!(indexed_items(&storage, &base, "ByGroup").await.is_empty());
    let seal = PartitionSeal {
        table_id: base.table_id.clone(),
        source_partition_id: range.partition_id,
        epoch: range.epoch,
        next_epoch: 2,
        source_lower: (range.lower != bdb_keyspace::RangeKey::minimum()).then(|| range.lower.clone()),
        source_upper: range.upper.clone(),
        boundary: ordered_data_key(&base.table_id, &Item::from([("id".into(), AttributeValue::S("b".into()))]), &base.base_key_schema).unwrap(),
        left_partition_id: [81; 16],
        right_partition_id: [82; 16],
    };
    let Err(InvocationError::Rejected(refused)) = client
        .command::<SealPartition>(&source, mutation(), Json(seal))
        .await
    else {
        panic!("split ignored pending projection");
    };
    assert_eq!(refused.output.0, SealPartitionOutcome::PendingIndexChanges);
    let inspection = Inspection::new(bucket.clone(), host.handle());
    let old = native_peer::identity(&observed, SOURCE, &destination).unwrap();
    let old_owner = inspection.owner(&old.scope).await;
    node.stop().await;
    std::fs::remove_dir_all(&source_path).unwrap();
    let (mut node, provisioner, client, storage) = owner(application, bucket, &directory.path().join(FRONT), observed.clone(), host.handle(), core.handle()).await;
    inspection.departed().await;
    assert!(
        storage
            .project_index_changes(ACCOUNT, &source, &base.table_id)
            .await
            .unwrap()
    );
    assert!(journal(&client, &source, &base.table_id).await.is_none());
    assert_eq!(
        indexed_items(&storage, &base, "ByGroup").await,
        vec![changed.clone()]
    );
    assert_eq!(
        client
            .command::<ApplyGlobalIndexMutation>(&destination, mutation(), Json(first_mutation))
            .await
            .unwrap()
            .output
            .0,
        GlobalIndexApplyOutcome::Superseded
    );
    assert_eq!(
        indexed_items(&storage, &base, "ByGroup").await,
        vec![changed.clone()]
    );
    let binary = TableKeyInfo {
        key_schema: base.global_secondary_indexes[1].key_schema.clone(),
        ..base.clone()
    };
    for prefix in [vec![], vec![0xff], vec![0xff, 0xff], vec![0xff, 0xff, 1]] {
        let condition = KeyCondition {
            pk_path: vec![PathElement::Attribute("group".into())],
            pk_value: Expr::Placeholder("g".into()),
            extra_pk_conditions: vec![],
            sk_condition: Some(SortKeyCondition::BeginsWith {
                path: vec![PathElement::Attribute("bin".into())],
                prefix: Expr::Placeholder("prefix".into()),
            }),
            extra_sk_conditions: vec![],
        };
        let maps = ExpressionMaps::new(
            HashMap::new(),
            HashMap::from([
                ("g".into(), AttributeValue::S("same".into())),
                ("prefix".into(), AttributeValue::B(prefix)),
            ]),
        );
        let (items, _) = storage
            .query(
                &binary,
                &condition,
                &maps,
                true,
                None,
                None,
                Some("ByBinary"),
            )
            .await
            .unwrap();
        assert_eq!(items, vec![changed.clone()]);
    }
    let actual = native_peer::identity(&observed, FRONT, &destination).unwrap();
    assert_eq!(actual.scope, old.scope);
    let new_owner = inspection.owner(&actual.scope).await;
    assert!(new_owner.epoch > old_owner.epoch);
    assert_eq!(new_owner.node.as_deref(), Some(FRONT));
    let http = SignedHttp::start(&client, provisioner, true).await;
    assert_sdk_gsis(&http.sdk, "JournalItems", &["ByGroup", "ByBinary"], &changed).await;
    http.stop().await;
    node.stop().await;
    inspection.close().await;
    });
}

pub(super) async fn observed_index_recovery(
    inspection: &Inspection,
    observed: &Observations,
    node: &str,
    previous: &[(CellTarget, cellar::sdk::CellIdentity, native_peer::Owner)],
) {
    let mut last_missing = Vec::new();
    let completed = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let mut missing = Vec::new();
            for (target, old, old_owner) in previous {
                if let Some(current) = native_peer::identity(observed, node, target) {
                    assert_eq!(current.scope, old.scope);
                    let owner = inspection.owner(&current.scope).await;
                    assert_eq!(owner.node.as_deref(), Some(node));
                    assert!(owner.epoch > old_owner.epoch);
                    assert_eq!(owner.epoch, current.epoch);
                } else {
                    missing.push(bdb_cellar_runtime::application::cell_name(target));
                }
            }
            if missing.is_empty() {
                return;
            }
            last_missing = missing;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        completed.is_ok(),
        "serving worker did not activate native index ranges: {last_missing:?}"
    );
}

#[test]
fn serving_worker_recovers_expired_ranges_and_keeps_healthy_indexes_progressing() {
    let (host, core) = runtimes();
    host.block_on(async {
        let application = native_peer::application();
        let directory = tempfile::tempdir().unwrap();
        let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite")).unwrap();
        let observed = Arc::new(Mutex::new(Default::default()));
        let source_path = directory.path().join(SOURCE);
        let (mut node, provisioner, client, storage) = owner(application.clone(), bucket.clone(), &source_path, observed.clone(), host.handle(), core.handle()).await;
        provisioner.admit_account(ACCOUNT).await.unwrap();
        storage.create_table(ACCOUNT, serde_json::from_value(serde_json::json!({
            "TableName":"ServingIndexes", "BillingMode":"PAY_PER_REQUEST",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"},{"AttributeName":"group","AttributeType":"S"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"ByGroup","KeySchema":[{"AttributeName":"group","KeyType":"HASH"}],"Projection":{"ProjectionType":"ALL"}},
                {"IndexName":"OtherGroup","KeySchema":[{"AttributeName":"group","KeyType":"HASH"}],"Projection":{"ProjectionType":"ALL"}}
            ]
        })).unwrap()).await.unwrap();
        let base = storage.table_key_info(ACCOUNT, "ServingIndexes").await.unwrap();
        let table = client.query::<DescribeTableById>(&account_target(ACCOUNT).unwrap(), None, Json(base.table_id.clone())).await.unwrap().output.0.unwrap();
        let item = Item::from([("id".into(), AttributeValue::S("a".into())), ("group".into(), AttributeValue::S("same".into()))]);
        let source_ranges = ranges(&client, &base.table_id).await;
        assert_eq!(source_ranges.len(), 2);
        let key = ordered_data_key(&base.table_id, &item, &base.base_key_schema).unwrap();
        let source_range = source_ranges.iter().find(|range| contains(range, &key)).unwrap();
        let source = data_target(ACCOUNT, &base.table_id, &source_range.partition_id).unwrap();
        let mut targets = Vec::new();
        let mut destination = None;
        for (position, index) in table.global_secondary_indexes.iter().enumerate() {
            let key = ordered_index_key(&item, &index.specification.key_schema, &table.key_schema).unwrap();
            let entries = ranges(&client, &index.id).await;
            assert_eq!(entries.len(), 2);
            for range in entries {
                let target = global_index_target(ACCOUNT, &index.id, &range.partition_id).unwrap();
                if position == 0 && contains(&range, &key) { destination = Some(target.clone()); }
                targets.push(target);
            }
        }
        let destination = destination.unwrap();
        storage.put_item(&base, item.clone(), false, None, &ExpressionMaps::default(), None).await.unwrap();
        let blocked = Arc::new(AtomicBool::new(true));
        let attempts = Arc::new(AtomicUsize::new(0));
        let faulty = client.map_transport(|inner| Arc::new(BlockedDirectory {
            inner, target: destination.clone(), blocked: blocked.clone(), attempts: attempts.clone(),
        }));
        // A caller-side refusal of one target is distinct from physical owner
        // loss. Healthy indexes must progress, while the journal remains.
        assert!(CellStorage::new(faulty, "us-east-1").project_index_changes(ACCOUNT, &source, &base.table_id).await.is_err());
        assert!(attempts.load(Ordering::SeqCst) > 0);
        assert_eq!(indexed_items(&storage, &base, "OtherGroup").await, vec![item.clone()]);
        assert!(journal(&client, &source, &base.table_id).await.is_some());
        let inspection = Inspection::new(bucket.clone(), host.handle());
        let mut previous = Vec::new();
        for target in &targets {
            let actual = native_peer::identity(&observed, SOURCE, target).unwrap();
            let owner = inspection.owner(&actual.scope).await;
            assert_eq!(owner.node.as_deref(), Some(SOURCE));
            previous.push((target.clone(), actual, owner));
        }
        node.stop().await;
        std::fs::remove_dir_all(&source_path).unwrap();
        let front_path = directory.path().join(FRONT);
        let (mut node, _, client, storage) = owner(application.clone(), bucket.clone(), &front_path, observed.clone(), host.handle(), core.handle()).await;
        inspection.departed().await;
        let tasks = TaskGroup::default();
        CellStorage::new(client.clone(), "us-east-1").install_global_index_loop(&tasks, vec![ACCOUNT.into()]).unwrap();
        tokio::time::timeout(Duration::from_secs(15), async {
            // The serving worker can retire a journal entry between its
            // header and chunk reads. Poll only the pending header here; the
            // complete-payload helper is for fixtures without a live consumer.
            while client
                .query::<ReadPartitionIndexChange>(&source, None, Json(base.table_id.clone()))
                .await
                .unwrap()
                .output
                .0
                .is_some()
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.expect("serving worker did not finish the retained journal");
        observed_index_recovery(&inspection, &observed, FRONT, &previous).await;
        assert_eq!(indexed_items(&storage, &base, "ByGroup").await, vec![item.clone()]);
        assert_eq!(indexed_items(&storage, &base, "OtherGroup").await, vec![item.clone()]);
        tasks.shutdown().await.unwrap();
        // No new base mutation or outbox entry accompanies the second loss.
        // Observe actual SDK factories before issuing any user index read.
        previous.clear();
        for target in &targets {
            let actual = native_peer::identity(&observed, FRONT, target).unwrap();
            let owner = inspection.owner(&actual.scope).await;
            previous.push((target.clone(), actual, owner));
        }
        node.stop().await;
        std::fs::remove_dir_all(&front_path).unwrap();
        let idle_name = "index-idle-owner";
        let (mut node, provisioner, client, storage) = owner(application, bucket, &directory.path().join(idle_name), observed.clone(), host.handle(), core.handle()).await;
        inspection.departed_node(FRONT, idle_name).await;
        let tasks = TaskGroup::default();
        CellStorage::new(client.clone(), "us-east-1").install_global_index_loop(&tasks, vec![ACCOUNT.into()]).unwrap();
        observed_index_recovery(&inspection, &observed, idle_name, &previous).await;
        assert!(journal(&client, &source, &base.table_id).await.is_none());
        assert_eq!(indexed_items(&storage, &base, "ByGroup").await, vec![item.clone()]);
        let http = SignedHttp::start(&client, provisioner, true).await;
        assert_sdk_gsis(&http.sdk, "ServingIndexes", &["ByGroup", "OtherGroup"], &item).await;
        http.stop().await;
        tasks.shutdown().await.unwrap();
        node.stop().await;
        inspection.close().await;
    });
}

async fn assert_sdk_gsis(
    sdk: &aws_sdk_dynamodb::Client,
    table: &str,
    indexes: &[&str],
    expected: &Item,
) {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    let sdk = aws_sdk_dynamodb::Client::from_conf(
        sdk.config()
            .to_builder()
            .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled())
            .build(),
    );
    let expected: HashMap<_, _> = expected
        .iter()
        .map(|(name, value)| {
            (
                name.clone(),
                match value {
                    AttributeValue::S(value) => A::S(value.clone()),
                    AttributeValue::N(value) => A::N(value.clone()),
                    AttributeValue::B(value) => {
                        A::B(aws_sdk_dynamodb::primitives::Blob::new(value.clone()))
                    }
                    other => panic!("unexpected GSI fixture value: {other:?}"),
                },
            )
        })
        .collect();
    for index in indexes {
        let mut cursor = None;
        let mut found = Vec::new();
        for page in 0..8 {
            let result = sdk
                .query()
                .table_name(table)
                .index_name(*index)
                .key_condition_expression("#group = :g")
                .expression_attribute_names("#group", "group")
                .expression_attribute_values(":g", A::S("same".into()))
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
            assert!(page < 7, "SDK GSI pagination did not finish");
        }
        assert_eq!(found, vec![expected.clone()]);
    }
}
