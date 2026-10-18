use crate::fixture::Fixture;
use aws_sdk_dynamodb::types::{
    AttributeValue as AwsAttributeValue, Get, Put, TransactGetItem, TransactWriteItem,
};
use beyonddb::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64},
};
#[path = "transport.rs"]
mod transport;
const ACCOUNT: &str = "123456789012";
fn runtimes() -> (tokio::runtime::Runtime, tokio::runtime::Runtime) {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .with_test_writer()
        .try_init();
    let make = |threads| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()
            .unwrap()
    };
    (make(4), make(1))
}
async fn instrument(
    fixture: &mut Fixture,
    queries: Arc<AtomicU64>,
    drop_batch: bool,
    dropped: Arc<AtomicBool>,
) -> Arc<AtomicU64> {
    let batch_width = Arc::new(AtomicU64::new(0));
    let data_namespace = fixture.data[0].0.namespace();
    let client = fixture.client.map_transport(|inner| {
        Arc::new(transport::Observe {
            inner,
            queries,
            drop_batch,
            dropped,
            data_namespace,
            batch_width: batch_width.clone(),
        })
    });
    fixture.use_client(client).await;
    batch_width
}
async fn sequence(fixture: &Fixture) -> u64 {
    fixture
        .client
        .query::<ReadPartitionState>(&fixture.data[0].0, None, Json(()))
        .await
        .unwrap()
        .receipt
        .commit_sequence
}
#[test]
fn sdk_returned_updates_coalesce_and_survive_owner_restore() {
    let (host, core) = runtimes();
    host.block_on(returned_updates(host.handle(), core.handle()));
}
#[test]
fn sdk_transaction_prepares_coalesce_and_survive_owner_restore() {
    let (host, core) = runtimes();
    host.block_on(transaction_prepares(false, host.handle(), core.handle()));
}
#[test]
fn sdk_transaction_prepares_recover_lost_batch_reply() {
    let (host, core) = runtimes();
    host.block_on(transaction_prepares(true, host.handle(), core.handle()));
}
#[test]
fn sdk_fresh_read_reuses_acknowledged_participants() {
    let (host, core) = runtimes();
    host.block_on(fresh_read(host.handle(), core.handle()));
}
#[test]
fn sdk_fresh_transaction_reuses_acknowledged_write_completion() {
    let (host, core) = runtimes();
    host.block_on(fresh_write(host.handle(), core.handle()));
}
async fn returned_updates(host: &tokio::runtime::Handle, core: &tokio::runtime::Handle) {
    let mut fixture = Fixture::with_limits(host, core, 1, 16, 8).await;
    let mut sdk = fixture.sdk.clone();
    // Warm the directory/auth paths before measuring data commands. Every
    // update below uses a distinct key and requests its committed new image.
    sdk.get_item()
        .table_name("Residency")
        .key("id", AwsAttributeValue::S("returned-warmup".into()))
        .send()
        .await
        .unwrap();
    let before = sequence(&fixture).await;
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
    let count = sequence(&fixture).await - before;
    eprintln!("64 returned SDK updates used {count} durable data commands");
    assert!(count > 0);
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
    fixture.replace().await;
    sdk = fixture.sdk.clone();
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
    fixture.shutdown().await;
}
async fn transaction_prepares(
    drop_reply: bool,
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
) {
    let tokens = (0..32)
        .map(|index| format!("prepare-batch-{index}"))
        .collect::<Vec<_>>();
    let mut fixture = Fixture::with_coordinators(host, core, 1, 128, 8, &tokens).await;
    let queries = Arc::new(AtomicU64::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let batch_width = instrument(&mut fixture, queries, drop_reply, dropped.clone()).await;
    let mut sdk = fixture.sdk.clone();
    sdk.get_item()
        .table_name("Residency")
        .key("id", AwsAttributeValue::S("prepare-warmup".into()))
        .send()
        .await
        .unwrap();
    let before = sequence(&fixture).await;
    let start = Arc::new(tokio::sync::Barrier::new(33));
    let mut requests = tokio::task::JoinSet::new();
    for index in 0..32 {
        let sdk = sdk.clone();
        let start = start.clone();
        requests.spawn(async move {
            let request = sdk
                .transact_write_items()
                .client_request_token(format!("prepare-batch-{index}"))
                .set_transact_items(Some(
                    (0..2)
                        .map(|item| {
                            TransactWriteItem::builder()
                                .put(
                                    Put::builder()
                                        .table_name("Residency")
                                        .item(
                                            "id",
                                            AwsAttributeValue::S(format!("prepare-{index}-{item}")),
                                        )
                                        .item("value", AwsAttributeValue::N(index.to_string()))
                                        .build()
                                        .unwrap(),
                                )
                                .build()
                        })
                        .collect(),
                ));
            start.wait().await;
            request.send().await.unwrap();
        });
    }
    start.wait().await;
    while let Some(result) = requests.join_next().await {
        result.unwrap();
    }
    let count = sequence(&fixture).await - before;
    eprintln!("32 signed transaction writes used {count} durable data commands");
    let width = batch_width.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        width >= 2,
        "the workload must exercise actual prepare batching"
    );
    assert!(
        width <= 2,
        "prepare publication aggregated {width} physical reservations; limit the amplification"
    );
    fixture.replace().await;
    sdk = fixture.sdk.clone();
    for index in 0..32 {
        for item in 0..2 {
            let restored = sdk
                .get_item()
                .table_name("Residency")
                .key(
                    "id",
                    AwsAttributeValue::S(format!("prepare-{index}-{item}")),
                )
                .send()
                .await
                .unwrap()
                .item
                .unwrap();
            assert_eq!(restored["value"], AwsAttributeValue::N(index.to_string()));
        }
    }
    fixture.shutdown().await;
    assert_eq!(
        dropped.load(std::sync::atomic::Ordering::SeqCst),
        drop_reply
    );
    assert!(count > 0);
    assert!(
        count < 64,
        "independent prepares must share durable commits"
    );
}

async fn fresh_read(host: &tokio::runtime::Handle, core: &tokio::runtime::Handle) {
    let mut fixture = Fixture::with_limits(host, core, 2, 16, 8).await;
    let queries = Arc::new(AtomicU64::new(0));
    instrument(
        &mut fixture,
        queries.clone(),
        false,
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    let mut sdk = fixture.sdk.clone();
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
    let read = |sdk: &aws_sdk_dynamodb::Client| {
        sdk.transact_get_items().set_transact_items(Some(
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
        ))
    };
    queries.store(0, std::sync::atomic::Ordering::Relaxed);
    let result = read(&sdk).send().await.unwrap();
    let query_count = queries.load(std::sync::atomic::Ordering::Relaxed);
    eprintln!("fresh signed two-Cell read used {query_count} coordinator queries");
    let images = result.responses.unwrap();
    assert_eq!(images.len(), 3);
    for (image, (_, expected)) in images.iter().zip(fixture.data.iter().rev()) {
        assert_eq!(image.item.as_ref(), Some(expected));
    }
    assert!(images[2].item.is_none());
    // Completed read cleanup must leave no locks after owner restoration.
    fixture.replace().await;
    instrument(
        &mut fixture,
        queries.clone(),
        false,
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    sdk = fixture.sdk.clone();
    for (index, key) in keys.iter().take(2).enumerate() {
        sdk.put_item()
            .table_name("Residency")
            .item("id", key.clone())
            .item("value", AwsAttributeValue::N(index.to_string()))
            .send()
            .await
            .unwrap();
    }
    let restored = read(&sdk).send().await.unwrap().responses.unwrap();
    for (index, image) in restored.iter().take(2).enumerate() {
        assert_eq!(
            image.item.as_ref().unwrap()["value"],
            AwsAttributeValue::N(index.to_string())
        );
    }
    assert!(restored[2].item.is_none());
    // Large BEGIN payloads retain durable participant discovery. These legal
    // partition keys exceed the shortcut's 32 KiB bound in aggregate.
    queries.store(0, std::sync::atomic::Ordering::Relaxed);
    let wide_keys = (0..40)
        .map(|index| {
            format!(
                "{}-{index}-{}",
                if index % 2 == 0 {
                    "wide-read"
                } else {
                    "Ω-wide-read"
                },
                "x".repeat(1_000)
            )
        })
        .collect::<Vec<_>>();
    let route = crate::single_leaf_route(
        &fixture.client,
        &account_target(ACCOUNT).unwrap(),
        &fixture.table.id,
    )
    .await
    .unwrap();
    let mut owners = std::collections::BTreeSet::new();
    for id in &wide_keys {
        let key = ordered_data_key(
            &fixture.table.id,
            &extenddb_core::types::Item::from([(
                "id".into(),
                extenddb_core::types::AttributeValue::S(id.clone()),
            )]),
            &fixture.table.key_schema,
        )
        .unwrap();
        let range = route
            .partitions
            .iter()
            .find(|range| {
                range.lower.as_ref().is_none_or(|lower| lower <= &key)
                    && range.upper.as_ref().is_none_or(|upper| &key < upper)
            })
            .unwrap();
        owners.insert(range.partition_id);
    }
    assert_eq!(
        owners.len(),
        2,
        "oversized read must span both ordered cells"
    );
    let large = sdk
        .transact_get_items()
        .set_transact_items(Some(
            wide_keys
                .iter()
                .map(|id| {
                    TransactGetItem::builder()
                        .get(
                            Get::builder()
                                .table_name("Residency")
                                .key("id", AwsAttributeValue::S(id.clone()))
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
        queries.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "oversized BEGIN must retain durable discovery"
    );
    fixture.shutdown().await;
    assert_eq!(
        query_count, 3,
        "acknowledged BEGIN removes participant rediscovery; canonical completion retains three status/discovery queries"
    );
}

async fn fresh_write(host: &tokio::runtime::Handle, core: &tokio::runtime::Handle) {
    let mut fixture = Fixture::with_limits(host, core, 2, 16, 8).await;
    let queries = Arc::new(AtomicU64::new(0));
    instrument(
        &mut fixture,
        queries.clone(),
        false,
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    let admission = crate::native_peer::provisioner(&fixture.client);
    CoordinatorProvisioner::ensure(
        admission.as_ref(),
        &fixture.client,
        ACCOUNT,
        b"fresh-begin-payload",
    )
    .await
    .unwrap();
    let mut sdk = fixture.sdk.clone();
    queries.store(0, std::sync::atomic::Ordering::Relaxed);
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
    let write = |sdk: &aws_sdk_dynamodb::Client| {
        sdk.transact_write_items()
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
            ))
    };
    write(&sdk).send().await.unwrap();
    assert_eq!(
        queries.load(std::sync::atomic::Ordering::Relaxed),
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
    fixture.replace().await;
    instrument(
        &mut fixture,
        queries.clone(),
        false,
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    sdk = fixture.sdk.clone();
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
    queries.store(0, std::sync::atomic::Ordering::Relaxed);
    write(&sdk).send().await.unwrap();
    assert_eq!(
        queries.load(std::sync::atomic::Ordering::Relaxed),
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
        admission.as_ref(),
        &fixture.client,
        ACCOUNT,
        b"large-begin-payload",
    )
    .await
    .unwrap();
    queries.store(0, std::sync::atomic::Ordering::Relaxed);
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
        queries.load(std::sync::atomic::Ordering::Relaxed),
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
    fixture.shutdown().await;
}
