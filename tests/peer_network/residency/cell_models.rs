use super::provisioning::{create, sdk_without_retries, table_id};
use super::*;
use aws_sdk_dynamodb::types::{Get, Put, Tag, TransactGetItem, TransactWriteItem};
use beyonddb::{RoutePageInput, RoutePageOutcome};
use extenddb_storage::StreamEngine;

const ACCOUNT: &str = "123456789012";

#[derive(Default)]
struct DataCommands(std::sync::atomic::AtomicU64);

impl cellule_runtime::fleet::telemetry::CellTelemetry for DataCommands {
    fn primitive_operation(
        &self,
        module: &'static str,
        kind: cellule_runtime::fleet::telemetry::PrimitiveOperationKind,
        _: cellule_runtime::fleet::telemetry::PrimitiveOperationOutcome,
        _: std::time::Duration,
    ) {
        if module == "beyonddb-data"
            && kind == cellule_runtime::fleet::telemetry::PrimitiveOperationKind::Command
        {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sdk_returned_updates_coalesce_and_survive_owner_restore() {
    let commands = Arc::new(DataCommands::default());
    let fixture = Fixture::with_store_capacity_cache_router_and_telemetry(
        1,
        Arc::new(InMemory::new()),
        16,
        false,
        std::convert::identity,
        Some(commands.clone()),
    )
    .await;
    let remote = super::provisioning::Remote::new(&fixture).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let state = build_http_state(
        &fixture.node,
        remote.client(&fixture),
        fixture.layout.clone(),
        fixture.provisioner.clone(),
        [38; 32],
        "us-east-1",
        endpoint.clone(),
    )
    .unwrap();
    let server = tokio::spawn(async move {
        extenddb_server::start_server(listener, state, None, None)
            .await
            .unwrap();
    });
    let sdk = aws_sdk_dynamodb::Client::from_conf(
        sdk_without_retries(&fixture)
            .config()
            .to_builder()
            .endpoint_url(endpoint)
            .build(),
    );
    // Warm the directory/auth paths before measuring data commands. Every
    // update below uses a distinct key and requests its committed new image.
    sdk.get_item()
        .table_name("Residency")
        .key("id", AwsAttributeValue::S("returned-warmup".into()))
        .send()
        .await
        .unwrap();
    commands.0.store(0, std::sync::atomic::Ordering::Relaxed);
    let mut requests = tokio::task::JoinSet::new();
    let start = Arc::new(tokio::sync::Barrier::new(65));
    for index in 0..64 {
        let sdk = sdk.clone();
        let start = start.clone();
        requests.spawn(async move {
            start.wait().await;
            let item = sdk
                .update_item()
                .table_name("Residency")
                .key("id", AwsAttributeValue::S(format!("returned-{index}")))
                .update_expression("ADD #value :one")
                .expression_attribute_names("#value", "value")
                .expression_attribute_values(":one", AwsAttributeValue::N("1".into()))
                .return_values(aws_sdk_dynamodb::types::ReturnValue::AllNew)
                .send()
                .await
                .unwrap();
            let attributes = item.attributes.unwrap();
            assert_eq!(attributes["value"], AwsAttributeValue::N("1".into()));
            assert_eq!(
                attributes["id"],
                AwsAttributeValue::S(format!("returned-{index}"))
            );
        });
    }
    start.wait().await;
    while let Some(result) = requests.join_next().await {
        result.unwrap();
    }
    let count = commands.0.load(std::sync::atomic::Ordering::Relaxed);
    eprintln!("64 returned SDK updates used {count} durable data commands");
    assert!(count < 64, "returned updates should share durable commands");
    // Concurrent requests for one key must be deferred into separate commands
    // and retain each ADD's own committed result.
    let mut requests = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let sdk = sdk.clone();
        requests.spawn(async move {
            sdk.update_item()
                .table_name("Residency")
                .key("id", AwsAttributeValue::S("returned-repeated".into()))
                .update_expression("ADD #value :one")
                .expression_attribute_names("#value", "value")
                .expression_attribute_values(":one", AwsAttributeValue::N("1".into()))
                .return_values(aws_sdk_dynamodb::types::ReturnValue::AllNew)
                .send()
                .await
                .unwrap()
                .attributes
                .unwrap()["value"]
                .as_n()
                .unwrap()
                .parse::<u64>()
                .unwrap()
        });
    }
    let mut values = std::collections::BTreeSet::new();
    while let Some(result) = requests.join_next().await {
        values.insert(result.unwrap());
    }
    assert_eq!(values, (1..=8).collect());
    let id = table_id(&fixture, "Residency").await;
    let range = ranges(&fixture, "Residency").await.remove(0);
    fixture
        .provisioner
        .admit_existing_partition(ACCOUNT, &id, &range.partition_id)
        .await
        .unwrap()
        .drain()
        .await
        .unwrap();
    fixture
        .provisioner
        .admit_existing_partition(ACCOUNT, &id, &range.partition_id)
        .await
        .unwrap();
    for index in 0..64 {
        let item = sdk
            .get_item()
            .table_name("Residency")
            .key("id", AwsAttributeValue::S(format!("returned-{index}")))
            .send()
            .await
            .unwrap()
            .item
            .unwrap();
        assert_eq!(item["value"], AwsAttributeValue::N("1".into()));
    }
    server.abort();
    let _ = server.await;
    remote.shutdown().await;
    fixture.shutdown().await;
}

#[derive(Default)]
struct CoordinatorQueries(std::sync::atomic::AtomicU64);

impl cellule_runtime::fleet::telemetry::CellTelemetry for CoordinatorQueries {
    fn primitive_operation(
        &self,
        module: &'static str,
        kind: cellule_runtime::fleet::telemetry::PrimitiveOperationKind,
        _: cellule_runtime::fleet::telemetry::PrimitiveOperationOutcome,
        _: std::time::Duration,
    ) {
        if module == "beyonddb-coordinator"
            && kind == cellule_runtime::fleet::telemetry::PrimitiveOperationKind::Query
        {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_fresh_read_reuses_acknowledged_participants() {
    let queries = Arc::new(CoordinatorQueries::default());
    let fixture = Fixture::with_store_capacity_cache_router_and_telemetry(
        2,
        Arc::new(InMemory::new()),
        16,
        false,
        std::convert::identity,
        Some(queries.clone()),
    )
    .await;
    let remote = super::provisioning::Remote::new(&fixture).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let state = build_http_state(
        &fixture.node,
        remote.client(&fixture),
        fixture.layout.clone(),
        fixture.provisioner.clone(),
        [38; 32],
        "us-east-1",
        endpoint.clone(),
    )
    .unwrap();
    let server = tokio::spawn(async move {
        extenddb_server::start_server(listener, state, None, None)
            .await
            .unwrap();
    });
    let sdk = aws_sdk_dynamodb::Client::from_conf(
        sdk_without_retries(&fixture)
            .config()
            .to_builder()
            .endpoint_url(endpoint)
            .build(),
    );
    assert_eq!(fixture.data.len(), 2);
    // Reverse request order relative to the durable participant ordering. The
    // third key is absent: a saved None image must still fill its result slot.
    let mut keys = fixture
        .data
        .iter()
        .rev()
        .map(|(_, item)| item["id"].clone())
        .collect::<Vec<_>>();
    keys.push(AwsAttributeValue::S("fresh-read-missing".into()));
    let read = sdk.transact_get_items().set_transact_items(Some(
        keys.iter()
            .map(|key| {
                TransactGetItem::builder()
                    .get(
                        Get::builder()
                            .table_name("Residency")
                            .key("id", key.clone())
                            .build()
                            .unwrap(),
                    )
                    .build()
            })
            .collect(),
    ));
    queries.0.store(0, std::sync::atomic::Ordering::Relaxed);
    let result = read.clone().send().await.unwrap();
    let query_count = queries.0.load(std::sync::atomic::Ordering::Relaxed);
    eprintln!("fresh signed two-Cell read used {query_count} coordinator queries");
    let images = result.responses.unwrap();
    assert_eq!(images.len(), 3);
    for (image, (_, expected)) in images.iter().zip(fixture.data.iter().rev()) {
        assert_eq!(image.item.as_ref(), Some(expected));
    }
    assert!(images[2].item.is_none());
    // Completed read cleanup must leave no locks after owner restoration.
    for (handle, _) in &fixture.data {
        handle.drain().await.unwrap();
    }
    let id = table_id(&fixture, "Residency").await;
    for range in ranges(&fixture, "Residency").await {
        fixture
            .provisioner
            .admit_existing_partition(ACCOUNT, &id, &range.partition_id)
            .await
            .unwrap();
    }
    for (index, key) in keys.iter().take(2).enumerate() {
        sdk.put_item()
            .table_name("Residency")
            .item("id", key.clone())
            .item("value", AwsAttributeValue::N(index.to_string()))
            .send()
            .await
            .unwrap();
    }
    let restored = read.send().await.unwrap().responses.unwrap();
    for (index, image) in restored.iter().take(2).enumerate() {
        assert_eq!(
            image.item.as_ref().unwrap()["value"],
            AwsAttributeValue::N(index.to_string())
        );
    }
    assert!(restored[2].item.is_none());
    // Large BEGIN payloads retain durable participant discovery. These legal
    // partition keys exceed the shortcut's 32 KiB bound in aggregate.
    queries.0.store(0, std::sync::atomic::Ordering::Relaxed);
    let large = sdk
        .transact_get_items()
        .set_transact_items(Some(
            (0..40)
                .map(|index| {
                    TransactGetItem::builder()
                        .get(
                            Get::builder()
                                .table_name("Residency")
                                .key(
                                    "id",
                                    AwsAttributeValue::S(format!(
                                        "wide-read-{index}-{}",
                                        "x".repeat(1_000)
                                    )),
                                )
                                .build()
                                .unwrap(),
                        )
                        .build()
                })
                .collect(),
        ))
        .send()
        .await
        .unwrap()
        .responses
        .unwrap();
    assert_eq!(large.len(), 40);
    assert!(large.iter().all(|image| image.item.is_none()));
    assert!(
        queries.0.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "oversized BEGIN must retain durable discovery"
    );
    server.abort();
    let _ = server.await;
    remote.shutdown().await;
    fixture.shutdown().await;
    assert_eq!(
        query_count, 3,
        "acknowledged BEGIN removes participant rediscovery; canonical completion retains three status/discovery queries"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_fresh_transaction_reuses_acknowledged_write_completion() {
    let queries = Arc::new(CoordinatorQueries::default());
    let fixture = Fixture::with_store_capacity_cache_router_and_telemetry(
        2,
        Arc::new(InMemory::new()),
        16,
        false,
        std::convert::identity,
        Some(queries.clone()),
    )
    .await;
    let remote = super::provisioning::Remote::new(&fixture).await;
    beyonddb::CoordinatorProvisioner::ensure(
        fixture.provisioner.as_ref(),
        &fixture.client,
        ACCOUNT,
        b"fresh-begin-payload",
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let state = build_http_state(
        &fixture.node,
        remote.client(&fixture),
        fixture.layout.clone(),
        fixture.provisioner.clone(),
        [38; 32],
        "us-east-1",
        endpoint.clone(),
    )
    .unwrap();
    let server = tokio::spawn(async move {
        extenddb_server::start_server(listener, state, None, None)
            .await
            .unwrap();
    });
    let sdk = aws_sdk_dynamodb::Client::from_conf(
        sdk_without_retries(&fixture)
            .config()
            .to_builder()
            .endpoint_url(endpoint)
            .build(),
    );
    queries.0.store(0, std::sync::atomic::Ordering::Relaxed);
    let keys = fixture
        .data
        .iter()
        .map(|(_, item)| item["id"].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        keys.len(),
        2,
        "the write must reach two distinct data Cells"
    );
    let write = sdk
        .transact_write_items()
        .client_request_token("fresh-begin-payload")
        .set_transact_items(Some(
            keys.iter()
                .map(|key| {
                    TransactWriteItem::builder()
                        .put(
                            Put::builder()
                                .table_name("Residency")
                                .item("id", key.clone())
                                .item("value", AwsAttributeValue::N("7".into()))
                                .build()
                                .unwrap(),
                        )
                        .build()
                })
                .collect(),
        ));
    write.clone().send().await.unwrap();
    assert_eq!(
        queries.0.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "fresh write needs token lookup; acknowledged BEGIN, decision and resolution receipts prove completion"
    );
    let coordinator = beyonddb::coordinator_target(ACCOUNT, b"fresh-begin-payload").unwrap();
    let pending = fixture
        .client
        .query::<beyonddb::ReadPendingCrossCellTransactions>(
            &coordinator,
            None,
            beyonddb::Json(beyonddb::ReadPendingCrossCellTransactionsInput {
                after: None,
                limit: 100,
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    assert!(
        pending.is_empty(),
        "successful SDK completion must durably record every resolution"
    );
    for (handle, _) in &fixture.data {
        handle.drain().await.unwrap();
    }
    fixture
        .provisioner
        .admit_coordinator(ACCOUNT, b"fresh-begin-payload")
        .await
        .unwrap()
        .drain()
        .await
        .unwrap();
    fixture
        .provisioner
        .admit_coordinator(ACCOUNT, b"fresh-begin-payload")
        .await
        .unwrap();
    let id = table_id(&fixture, "Residency").await;
    for range in ranges(&fixture, "Residency").await {
        fixture
            .provisioner
            .admit_existing_partition(ACCOUNT, &id, &range.partition_id)
            .await
            .unwrap();
    }
    for key in &keys {
        let item = sdk
            .get_item()
            .table_name("Residency")
            .key("id", key.clone())
            .consistent_read(true)
            .send()
            .await
            .unwrap()
            .item
            .unwrap();
        assert_eq!(item["value"], AwsAttributeValue::N("7".into()));
    }
    sdk.put_item()
        .table_name("Residency")
        .item("id", keys[0].clone())
        .item("value", AwsAttributeValue::N("8".into()))
        .send()
        .await
        .unwrap();
    queries.0.store(0, std::sync::atomic::Ordering::Relaxed);
    write.send().await.unwrap();
    assert_eq!(
        queries.0.load(std::sync::atomic::Ordering::Relaxed),
        3,
        "token replay must read durable coordinator state"
    );
    let item = sdk
        .get_item()
        .table_name("Residency")
        .key("id", keys[0].clone())
        .send()
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(item["value"], AwsAttributeValue::N("8".into()));
    beyonddb::CoordinatorProvisioner::ensure(
        fixture.provisioner.as_ref(),
        &fixture.client,
        ACCOUNT,
        b"large-begin-payload",
    )
    .await
    .unwrap();
    queries.0.store(0, std::sync::atomic::Ordering::Relaxed);
    let padding = "\0".repeat(6_000);
    sdk.transact_write_items()
        .client_request_token("large-begin-payload")
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("Residency")
                        .item("id", AwsAttributeValue::S("large-begin".into()))
                        .item("padding", AwsAttributeValue::S(padding.clone()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        queries.0.load(std::sync::atomic::Ordering::Relaxed),
        7,
        "escaped inputs beyond the reuse bound retain durable chunk discovery"
    );
    let large_item = sdk
        .get_item()
        .table_name("Residency")
        .key("id", AwsAttributeValue::S("large-begin".into()))
        .send()
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(large_item["padding"], AwsAttributeValue::S(padding));
    server.abort();
    let _ = server.await;
    remote.shutdown().await;
    fixture.shutdown().await;
}

fn model_tag(value: &str) -> Tag {
    Tag::builder()
        .key("beyonddb:cell-model")
        .value(value)
        .build()
        .unwrap()
}

pub(super) async fn ranges(fixture: &Fixture, table: &str) -> Vec<beyonddb::RoutePagePartition> {
    let id = table_id(fixture, table).await;
    let page = beyonddb::read_route_page(
        &fixture.client,
        &account_target(ACCOUNT).unwrap(),
        RoutePageInput {
            table_id: id,
            start_hash: None,
            after_lower: None,
            expected_epoch: None,
        },
    )
    .await
    .unwrap();
    let RoutePageOutcome::Page {
        partitions,
        has_more,
        ..
    } = page
    else {
        panic!("table route is missing");
    };
    assert!(!has_more);
    partitions
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_cell_models_select_persisted_table_placement() {
    let fixture = Fixture::with_capacity(4, 32).await;
    let sdk = sdk_without_retries(&fixture);
    for (name, model, expected) in [
        ("ResidencyModelSingle", "single", 1),
        ("ResidencyModelAuto", "auto", 1),
        ("ResidencyModelPartitioned", "partitioned", 4),
    ] {
        create(&sdk, name, false)
            .tags(model_tag(model))
            .send()
            .await
            .unwrap();
        assert_eq!(ranges(&fixture, name).await.len(), expected, "{model}");
    }
    create(&sdk, "ResidencyModelDefault", false)
        .send()
        .await
        .unwrap();
    assert_eq!(ranges(&fixture, "ResidencyModelDefault").await.len(), 4);
    let error = create(&sdk, "ResidencyModelInvalid", false)
        .tags(model_tag("unknown"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
    assert!(
        sdk.describe_table()
            .table_name("ResidencyModelInvalid")
            .send()
            .await
            .is_err()
    );
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_single_cell_resists_splits_and_restores_atomic_reads() {
    let fixture = Fixture::with_capacity(2, 24).await;
    let sdk = sdk_without_retries(&fixture);
    let stream_arn = create(&sdk, "ResidencyModelSingle", false)
        .tags(model_tag("single"))
        .stream_specification(
            aws_sdk_dynamodb::types::StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(aws_sdk_dynamodb::types::StreamViewType::NewAndOldImages)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap()
        .table_description
        .unwrap()
        .latest_stream_arn
        .unwrap();
    let id = table_id(&fixture, "ResidencyModelSingle").await;
    for key in ["first", "second"] {
        sdk.put_item()
            .table_name("ResidencyModelSingle")
            .item("id", AwsAttributeValue::S(key.into()))
            .item("value", AwsAttributeValue::N("7".into()))
            .send()
            .await
            .unwrap();
    }
    let write = sdk
        .transact_write_items()
        .client_request_token("single-cell-restart-token")
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("ResidencyModelSingle")
                        .item("id", AwsAttributeValue::S("first".into()))
                        .item("value", AwsAttributeValue::N("8".into()))
                        .build()
                        .unwrap(),
                )
                .build(),
        );
    write.clone().send().await.unwrap();
    let range = ranges(&fixture, "ResidencyModelSingle").await.remove(0);
    assert!(
        fixture
            .provisioner
            .split_if_over_database_bytes(
                ACCOUNT,
                fixture.client.clone(),
                &id,
                range.partition_id,
                range.lower,
                1,
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .provisioner
            .split_partition(
                ACCOUNT,
                fixture.client.clone(),
                &id,
                range.partition_id,
                range.lower,
            )
            .await
            .is_err()
    );
    let target = beyonddb::data_target(ACCOUNT, &id, &range.partition_id).unwrap();
    fixture
        .provisioner
        .admit_existing_partition(ACCOUNT, &id, &range.partition_id)
        .await
        .unwrap()
        .drain()
        .await
        .unwrap();
    fixture
        .provisioner
        .admit_existing_partition(ACCOUNT, &id, &range.partition_id)
        .await
        .unwrap();
    sdk.put_item()
        .table_name("ResidencyModelSingle")
        .item("id", AwsAttributeValue::S("first".into()))
        .item("value", AwsAttributeValue::N("9".into()))
        .send()
        .await
        .unwrap();
    // The replay must not overwrite a newer item after participant restoration.
    write.send().await.unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let before = authority.load(target.cell_id()).await.unwrap().unwrap();
    let request = sdk.transact_get_items().set_transact_items(Some(
        ["second", "first"]
            .into_iter()
            .map(|key| {
                TransactGetItem::builder()
                    .get(
                        Get::builder()
                            .table_name("ResidencyModelSingle")
                            .key("id", AwsAttributeValue::S(key.into()))
                            .build()
                            .unwrap(),
                    )
                    .build()
            })
            .collect(),
    ));
    let result = request.send().await.unwrap();
    assert_eq!(result.responses().len(), 2);
    assert_eq!(
        result.responses()[0].item().unwrap()["id"],
        AwsAttributeValue::S("second".into())
    );
    assert_eq!(
        result.responses()[1].item().unwrap()["id"],
        AwsAttributeValue::S("first".into())
    );
    assert_eq!(
        result.responses()[1].item().unwrap()["value"],
        AwsAttributeValue::N("9".into())
    );
    let after = authority.load(target.cell_id()).await.unwrap().unwrap();
    assert_eq!(
        before.value().root.as_ref().unwrap().commit_sequence,
        after.value().root.as_ref().unwrap().commit_sequence,
        "a small single-Cell transaction read must not commit participant work"
    );
    assert_eq!(ranges(&fixture, "ResidencyModelSingle").await.len(), 1);
    let streams = beyonddb::CellStorage::new(fixture.client.clone(), "us-east-1");
    let description = streams
        .describe_stream(
            ACCOUNT,
            &extenddb_core::types::DescribeStreamInput {
                stream_arn: stream_arn.clone(),
                limit: Some(100),
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(description.shards.len(), 1);
    let shard = &description.shards[0].shard_id;
    streams
        .validate_shard(ACCOUNT, &stream_arn, shard)
        .await
        .unwrap();
    let (records, _) = streams
        .get_stream_records(ACCOUNT, shard, None, 100)
        .await
        .unwrap();
    assert_eq!(
        records.len(),
        4,
        "token replay must not emit another record"
    );
    // Auto starts at one Cell but remains eligible for the same durable split path.
    create(&sdk, "ResidencyModelAuto", false)
        .tags(model_tag("auto"))
        .send()
        .await
        .unwrap();
    let id = table_id(&fixture, "ResidencyModelAuto").await;
    let range = ranges(&fixture, "ResidencyModelAuto").await.remove(0);
    assert!(
        fixture
            .provisioner
            .split_if_over_database_bytes(
                ACCOUNT,
                fixture.client.clone(),
                &id,
                range.partition_id,
                range.lower,
                1,
            )
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(ranges(&fixture, "ResidencyModelAuto").await.len(), 2);
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_single_cell_keeps_index_growth_and_ignores_later_placement_tags() {
    let fixture = Fixture::with_capacity(4, 32).await;
    let sdk = sdk_without_retries(&fixture);
    const NAME: &str = "ResidencyModelIndex";
    let created = create(&sdk, NAME, true)
        .tags(model_tag("single"))
        .send()
        .await
        .unwrap()
        .table_description
        .unwrap();
    // Pinned ExtendDB authorizes TagResource against table/* rather than the
    // supplied ARN. Keep that existing protocol limitation explicit here.
    CellAuthorizationStore::new(fixture.client.clone())
        .put_user_policy(
            ACCOUNT,
            "network-user",
            "model-tags",
            &serde_json::json!({"Version":"2012-10-17", "Statement":[{
                "Effect":"Allow", "Action":"dynamodb:TagResource", "Resource":"*"
            }]})
            .to_string(),
        )
        .await
        .unwrap();
    sdk.tag_resource()
        .resource_arn(created.table_arn().unwrap())
        .tags(model_tag("partitioned"))
        .send()
        .await
        .unwrap();
    let table = fixture
        .client
        .query::<DescribeTable>(&account_target(ACCOUNT).unwrap(), None, Json(NAME.into()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert_eq!(table.placement, beyonddb::TablePlacement::Single);
    assert_eq!(ranges(&fixture, NAME).await.len(), 1);
    let index = &table.global_secondary_indexes[0];
    let index_ranges = beyonddb::read_route_page(
        &fixture.client,
        &account_target(ACCOUNT).unwrap(),
        RoutePageInput {
            table_id: index.id.clone(),
            start_hash: None,
            after_lower: None,
            expected_epoch: None,
        },
    )
    .await
    .unwrap();
    let RoutePageOutcome::Page {
        partitions,
        has_more,
        ..
    } = index_ranges
    else {
        panic!("index route missing");
    };
    assert!(!has_more);
    assert_eq!(partitions.len(), 1);
    beyonddb::CellStorage::new(fixture.client.clone(), "us-east-1")
        .install_global_index_loop(
            &fixture.tasks,
            vec![ACCOUNT.into()],
            fixture.provisioner.clone(),
            fixture.directory.clone(),
        )
        .unwrap();
    sdk.put_item()
        .table_name(NAME)
        .item("id", AwsAttributeValue::S("indexed".into()))
        .item("bucket", AwsAttributeValue::S("bucket".into()))
        .send()
        .await
        .unwrap();
    let query = sdk
        .query()
        .table_name(NAME)
        .index_name("ByBucket")
        .key_condition_expression("#bucket = :bucket")
        .expression_attribute_names("#bucket", "bucket")
        .expression_attribute_values(":bucket", AwsAttributeValue::S("bucket".into()));
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if query.clone().send().await.unwrap().items().len() == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let range = &partitions[0];
    assert!(
        fixture
            .provisioner
            .split_global_index_if_over_database_bytes(
                ACCOUNT,
                fixture.client.clone(),
                &index.id,
                range.partition_id,
                range.lower,
                1
            )
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        query.send().await.unwrap().items()[0]["id"],
        AwsAttributeValue::S("indexed".into())
    );
    assert_eq!(ranges(&fixture, NAME).await.len(), 1);
    fixture.shutdown().await;
}
