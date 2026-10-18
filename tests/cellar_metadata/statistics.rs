use crate::residency_fixture::{Fixture, runtimes};
use aws_sdk_dynamodb::types::AttributeValue as AwsAttributeValue;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType,
    LocalSecondaryIndex, Projection, ProjectionType, ScalarAttributeType,
};
use beyonddb::*;
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use cellar_runtime::{
    client::{CellClient, Invocation, InvocationTransport},
    identity::CellTarget,
};
use extenddb_storage::MetadataEngine;
use http_body_util::Full;
use keyspace::RangeKey;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

const ACCOUNT: &str = "123456789012";
const TABLE: &str = "ResidencyStatistics";

fn key(name: &str, kind: KeyType) -> KeySchemaElement {
    KeySchemaElement::builder()
        .attribute_name(name)
        .key_type(kind)
        .build()
        .unwrap()
}

async fn description(sdk: &aws_sdk_dynamodb::Client) -> (i64, i64, i64, i64, i64, i64) {
    let table = sdk
        .describe_table()
        .table_name(TABLE)
        .send()
        .await
        .unwrap()
        .table
        .unwrap();
    let local = &table.local_secondary_indexes()[0];
    let global = &table.global_secondary_indexes()[0];
    (
        table.item_count.unwrap(),
        table.table_size_bytes.unwrap(),
        local.item_count.unwrap(),
        local.index_size_bytes.unwrap(),
        global.item_count.unwrap(),
        global.index_size_bytes.unwrap(),
    )
}

#[test]
fn sdk_statistics_count_sparse_indexes_and_survive_restore_and_splits() {
    let (host, core) = runtimes();
    host.block_on(async {
        // Retain the eleven-cell resident budget while each public base/index
        // directory starts with two independently installed ordered ranges.
        let mut fixture = Fixture::new(host.handle(), core.handle(), 1, 11).await;
        let mut sdk = aws_sdk_dynamodb::Client::from_conf(
            fixture
                .sdk
                .config()
                .to_builder()
                .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled())
                .build(),
        );
        let all = Projection::builder()
            .projection_type(ProjectionType::All)
            .build();
        let mut create = sdk
            .create_table()
            .table_name(TABLE)
            .billing_mode(BillingMode::PayPerRequest)
            .key_schema(key("id", KeyType::Hash))
            .key_schema(key("sort", KeyType::Range))
            .local_secondary_indexes(
                LocalSecondaryIndex::builder()
                    .index_name("ByLocal")
                    .key_schema(key("id", KeyType::Hash))
                    .key_schema(key("local", KeyType::Range))
                    .projection(all)
                    .build()
                    .unwrap(),
            )
            .global_secondary_indexes(
                GlobalSecondaryIndex::builder()
                    .index_name("ByGlobal")
                    .key_schema(key("global", KeyType::Hash))
                    .projection(
                        Projection::builder()
                            .projection_type(ProjectionType::KeysOnly)
                            .build(),
                    )
                    .build()
                    .unwrap(),
            );
        for name in ["id", "sort", "local", "global"] {
            create = create.attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(name)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            );
        }
        create.send().await.unwrap();
        let account = account_target(ACCOUNT).unwrap();
        let table = fixture
            .client
            .query::<DescribeTable>(&account, None, Json(TABLE.into()))
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        let source_range = crate::single_leaf_route(&fixture.client, &account, &table.id)
            .await
            .unwrap()
            .partitions[0]
            .clone();
        let source = beyonddb::data_target(ACCOUNT, &table.id, &source_range.partition_id).unwrap();
        let mut storage = beyonddb::CellStorage::new(fixture.client.clone(), "us-east-1");
        let mut items = Vec::new();
        for n in 0..4 {
            let mut item = HashMap::from([
                ("id".into(), AwsAttributeValue::S(format!("key-{n}"))),
                ("sort".into(), AwsAttributeValue::S("row".into())),
                (
                    "payload".into(),
                    AwsAttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new([0, 1, 2, 255])),
                ),
                (
                    "nested".into(),
                    AwsAttributeValue::L(vec![
                        AwsAttributeValue::Bool(true),
                        AwsAttributeValue::N("12345".into()),
                    ]),
                ),
            ]);
            if n < 3 {
                item.insert("local".into(), AwsAttributeValue::S(format!("local-{n}")));
            }
            if n < 2 {
                item.insert("global".into(), AwsAttributeValue::S(format!("global-{n}")));
            }
            sdk.put_item()
                .table_name(TABLE)
                .set_item(Some(item.clone()))
                .send()
                .await
                .unwrap();
            items.push(item);
        }
        while storage
            .project_index_changes(ACCOUNT, &source, &table.id)
            .await
            .unwrap()
        {}
        // Independent expected byte totals: UTF-8 names/strings, raw binary, and
        // nested-list overhead. GSI KEYS_ONLY contains id, sort and global only.
        let base_bytes = 4 * (2 + 5 + 4 + 3 + 7 + 4 + 6 + 3 + 2 + 5) + 3 * (5 + 7) + 2 * (6 + 8);
        let local_bytes = base_bytes - (2 + 5 + 4 + 3 + 7 + 4 + 6 + 3 + 2 + 5);
        let global_bytes = 2 * (2 + 5 + 4 + 3 + 6 + 8);
        let expected = (4, base_bytes, 3, local_bytes, 2, global_bytes);
        // Exercise the installed background worker, without manually refreshing.
        let workers = TaskGroup::default();
        storage
            .install_statistics_loop(&workers, vec![ACCOUNT.into()])
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if description(&sdk).await == expected {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("statistics did not converge");
        workers.shutdown().await.unwrap();
        // Split after publication, retaining sealed sources. Only serving children
        // contribute to the next sample; source counters remain nonzero.
        let plan = fixture
            .provisioner
            .split_partition(
                ACCOUNT,
                fixture.client.clone(),
                &table.id,
                source_range.partition_id,
                source_range.lower.clone().unwrap_or_else(RangeKey::minimum),
            )
            .await
            .unwrap();
        let index = &table.global_secondary_indexes[0];
        let RoutePageOutcome::Page { partitions, .. } = read_route_page(
            &fixture.client,
            &account,
            RoutePageInput {
                table_id: index.id.clone(),
                start_key: None,
                after_lower: None,
                expected_epoch: None,
            },
        )
        .await
        .unwrap() else {
            panic!("missing global-index route")
        };
        assert_eq!(partitions.len(), 2);
        let index_range = partitions[0].clone();
        let fired = Arc::new(AtomicBool::new(false));
        let interrupted = beyonddb::CellStorage::new(
            fixture.client.map_transport(|inner| {
                Arc::new(SplitDuringSample {
                    inner,
                    client: fixture.client.clone(),
                    provisioner: fixture.provisioner.clone(),
                    table_id: table.id.clone(),
                    index_id: Some(index.id.clone()),
                    source: global_index_target(ACCOUNT, &index.id, &index_range.partition_id)
                        .unwrap(),
                    partition_id: index_range.partition_id,
                    lower: index_range.lower,
                    fired: fired.clone(),
                })
            }),
            "us-east-1",
        );
        assert!(matches!(
            interrupted.refresh_table_size(ACCOUNT, TABLE).await,
            Err(extenddb_storage::error::StorageError::Transient(_))
        ));
        assert!(fired.load(Ordering::SeqCst));
        storage.refresh_table_size(ACCOUNT, TABLE).await.unwrap();
        assert_eq!(description(&sdk).await, expected);
        fixture.replace().await;
        sdk = fixture.sdk.clone();
        storage = beyonddb::CellStorage::new(fixture.client.clone(), "us-east-1");
        assert_eq!(description(&sdk).await, expected);
        sdk.delete_item()
            .table_name(TABLE)
            .key("id", items[0]["id"].clone())
            .key("sort", items[0]["sort"].clone())
            .send()
            .await
            .unwrap();
        sdk.update_item()
            .table_name(TABLE)
            .key("id", items[1]["id"].clone())
            .key("sort", items[1]["sort"].clone())
            .update_expression("REMOVE #l, #g")
            .expression_attribute_names("#l", "local")
            .expression_attribute_names("#g", "global")
            .send()
            .await
            .unwrap();
        for child in &plan.children {
            let target = beyonddb::data_target(ACCOUNT, &table.id, &child.partition_id).unwrap();
            while storage
                .project_index_changes(ACCOUNT, &target, &table.id)
                .await
                .unwrap()
            {}
        }
        storage.refresh_table_size(ACCOUNT, TABLE).await.unwrap();
        let remaining_bytes = base_bytes - 67 - 26;
        assert_eq!(description(&sdk).await, (3, remaining_bytes, 1, 53, 0, 0));
        let deleted = sdk
            .delete_table()
            .table_name(TABLE)
            .send()
            .await
            .unwrap()
            .table_description
            .unwrap();
        assert_eq!(deleted.item_count, Some(3));
        assert_eq!(deleted.table_size_bytes, Some(remaining_bytes));
        complete_deletion(&fixture, &sdk, TABLE).await;
        sdk.create_table()
            .table_name(TABLE)
            .billing_mode(BillingMode::PayPerRequest)
            .key_schema(key("id", KeyType::Hash))
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("id")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
        let recreated = sdk
            .describe_table()
            .table_name(TABLE)
            .send()
            .await
            .unwrap()
            .table
            .unwrap();
        assert_eq!(
            (recreated.item_count, recreated.table_size_bytes),
            (Some(0), Some(0))
        );
        fixture.shutdown().await;
    });
}

struct SplitDuringSample {
    inner: Arc<dyn InvocationTransport>,
    client: CellClient,
    provisioner: Arc<CellInitialPartitionProvisioner>,
    table_id: String,
    index_id: Option<String>,
    source: CellTarget,
    partition_id: [u8; 16],
    lower: RangeKey,
    fired: Arc<AtomicBool>,
}

impl InvocationTransport for SplitDuringSample {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            // Complete a real split immediately before the sampled owner's read
            // reaches ingress. The client still decodes the SDK's actual reply.
            if invocation.target == self.source
                && invocation.mutation.is_none()
                && !self.fired.swap(true, Ordering::SeqCst)
            {
                if let Some(index_id) = &self.index_id {
                    self.provisioner
                        .split_global_index_partition(
                            ACCOUNT,
                            self.client.clone(),
                            index_id,
                            self.partition_id,
                            self.lower.clone(),
                        )
                        .await
                        .unwrap();
                } else {
                    self.provisioner
                        .split_partition(
                            ACCOUNT,
                            self.client.clone(),
                            &self.table_id,
                            self.partition_id,
                            self.lower.clone(),
                        )
                        .await
                        .unwrap();
                }
            }
            self.inner.call(invocation, request).await
        })
    }
}

#[test]
fn statistics_reject_sample_when_split_changes_directory_before_publication() {
    let (host, core) = runtimes();
    host.block_on(async {
        let fixture = Fixture::new(host.handle(), core.handle(), 1, 8).await;
        fixture
            .sdk
            .put_item()
            .table_name("Residency")
            .item("id", AwsAttributeValue::S("first-sibling".into()))
            .item("value", AwsAttributeValue::S("committed".into()))
            .send()
            .await
            .unwrap();
        let account = account_target(ACCOUNT).unwrap();
        let table = fixture
            .client
            .query::<DescribeTable>(&account, None, Json("Residency".into()))
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        let range = crate::single_leaf_route(&fixture.client, &account, &table.id)
            .await
            .unwrap()
            .partitions[0]
            .clone();
        let fired = Arc::new(AtomicBool::new(false));
        let client = fixture.client.map_transport(|inner| {
            Arc::new(SplitDuringSample {
                inner,
                client: fixture.client.clone(),
                provisioner: fixture.provisioner.clone(),
                source: data_target(ACCOUNT, &table.id, &range.partition_id).unwrap(),
                table_id: table.id,
                index_id: None,
                partition_id: range.partition_id,
                lower: range.lower.unwrap_or_else(RangeKey::minimum),
                fired: fired.clone(),
            })
        });
        let storage = beyonddb::CellStorage::new(client, "us-east-1");
        assert!(matches!(
            storage.refresh_table_size(ACCOUNT, "Residency").await,
            Err(extenddb_storage::error::StorageError::Transient(_))
        ));
        assert!(fired.load(std::sync::atomic::Ordering::SeqCst));
        let describe = fixture
            .sdk
            .describe_table()
            .table_name("Residency")
            .send()
            .await
            .unwrap()
            .table
            .unwrap();
        assert_eq!(describe.item_count, Some(0));
        storage
            .refresh_table_size(ACCOUNT, "Residency")
            .await
            .unwrap();
        let describe = fixture
            .sdk
            .describe_table()
            .table_name("Residency")
            .send()
            .await
            .unwrap()
            .table
            .unwrap();
        assert_eq!(describe.item_count, Some(2));
        fixture.shutdown().await;
    });
}

async fn complete_deletion(fixture: &Fixture, sdk: &aws_sdk_dynamodb::Client, name: &str) {
    let tasks = TaskGroup::default();
    fixture
        .provisioner
        .install_account_capacity_loop(
            &tasks,
            ACCOUNT.into(),
            fixture.client.clone(),
            u64::MAX,
            std::time::Duration::from_millis(100),
        )
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            match sdk.describe_table().table_name(name).send().await {
                Ok(result) => assert_eq!(
                    result.table.unwrap().table_status(),
                    Some(&aws_sdk_dynamodb::types::TableStatus::Deleting)
                ),
                Err(error)
                    if error
                        .as_service_error()
                        .is_some_and(|error| error.is_resource_not_found_exception()) =>
                {
                    break;
                }
                Err(error) => panic!("deletion recovery failed: {error}"),
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("directory retirement did not complete");
    tasks.shutdown().await.unwrap();
}
