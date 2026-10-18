use crate::residency_fixture::{Fixture, SdkItem, runtimes};
use aws_sdk_dynamodb::types::AttributeValue as AwsAttributeValue;
use aws_sdk_dynamodb::types::{Put, TransactWriteItem};
use bdb_cellar_runtime::identity::CellTarget;
use beyonddb::*;
use extenddb_storage::MetadataEngine;

const ACCOUNT: &str = "123456789012";

async fn usage(fixture: &Fixture, target: &CellTarget) -> (u64, u64) {
    let usage = fixture
        .client
        .query::<beyonddb::PartitionUsage>(target, None, Json(()))
        .await
        .unwrap()
        .output
        .0;
    (usage.item_count, usage.item_bytes)
}

#[test]
fn sdk_range_usage_survives_mutation_replay_split_and_owner_restore() {
    let (host, core) = runtimes();
    host.block_on(async {
        let mut fixture = Fixture::new(host.handle(), core.handle(), 1, 8).await;
        let original_usage = usage(&fixture, &fixture.data[0].0).await;
        assert_eq!(original_usage.0, 1);
        // Two distinct observed keys permit an ordered split of this source.
        // The other initial public range remains empty.
        let extra = SdkItem::from([
            ("id".into(), AwsAttributeValue::S("first-sibling".into())),
            ("value".into(), AwsAttributeValue::S("seed".into())),
        ]);
        fixture
            .sdk
            .put_item()
            .table_name("Residency")
            .set_item(Some(extra.clone()))
            .send()
            .await
            .unwrap();
        let extra_bytes = (2 + "first-sibling".len() + 5 + "seed".len()) as u64;
        let mut sdk = aws_sdk_dynamodb::Client::from_conf(
            fixture
                .sdk
                .config()
                .to_builder()
                .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled())
                .build(),
        );
        let account = account_target(ACCOUNT).unwrap();
        let table = fixture
            .client
            .query::<DescribeTable>(&account, None, Json("Residency".into()))
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        let source = fixture.data[0].0.clone();
        let source_range = crate::single_leaf_route(&fixture.client, &account, &table.id)
            .await
            .unwrap()
            .partitions
            .into_iter()
            .find(|part| data_target(ACCOUNT, &table.id, &part.partition_id).unwrap() == source)
            .unwrap();
        let before = usage(&fixture, &source).await;
        assert_eq!(before.0, 2);
        // Capacity counts stored JSON plus encoded keys; DynamoDB statistics
        // below independently count logical names and values.
        let extra_stored_bytes = before.1 - original_usage.1;
        assert!(extra_stored_bytes > extra_bytes);
        let mut item = fixture.data[0].1.clone();
        let id = item["id"].clone();
        let original_len = item["value"].as_s().unwrap().len() as u64;
        // This image crosses the bounded SQL BLOB chunk size.
        sdk.update_item()
            .table_name("Residency")
            .key("id", id.clone())
            .update_expression("SET #v = :value")
            .expression_attribute_names("#v", "value")
            .expression_attribute_values(":value", AwsAttributeValue::S("x".repeat(300_000)))
            .send()
            .await
            .unwrap();
        let large = (2, before.1 - original_len + 300_000);
        assert_eq!(usage(&fixture, &source).await, large);
        assert!(
            sdk.delete_item()
                .table_name("Residency")
                .key("id", id.clone())
                .condition_expression("attribute_not_exists(id)")
                .send()
                .await
                .is_err()
        );
        assert_eq!(usage(&fixture, &source).await, large);
        item.insert("value".into(), AwsAttributeValue::S("transaction".into()));
        let write = TransactWriteItem::builder()
            .put(
                Put::builder()
                    .table_name("Residency")
                    .set_item(Some(item.clone()))
                    .build()
                    .unwrap(),
            )
            .build();
        for _ in 0..2 {
            sdk.transact_write_items()
                .client_request_token("usage-replay")
                .transact_items(write.clone())
                .send()
                .await
                .unwrap();
        }
        let committed = (2, before.1 - original_len + "transaction".len() as u64);
        assert_eq!(usage(&fixture, &source).await, committed);
        let plan = fixture
            .provisioner
            .split_partition(
                ACCOUNT,
                fixture.client.clone(),
                &table.id,
                source_range.partition_id,
                source_range
                    .lower
                    .clone()
                    .unwrap_or_else(bdb_keyspace::RangeKey::minimum),
            )
            .await
            .unwrap();
        fixture
            .provisioner
            .resume_split(ACCOUNT, fixture.client.clone(), &plan)
            .await
            .unwrap();
        let mut children = Vec::new();
        let mut after = (0, 0);
        for child in &plan.children {
            let target = beyonddb::data_target(ACCOUNT, &table.id, &child.partition_id).unwrap();
            let value = usage(&fixture, &target).await;
            after.0 += value.0;
            after.1 += value.1;
            children.push(target);
        }
        assert_eq!(after, committed);
        assert_eq!(usage(&fixture, &source).await, committed);
        fixture.replace().await;
        sdk = fixture.sdk.clone();
        // Signed reads restore the serving child; query persisted counters in both.
        assert_eq!(
            sdk.get_item()
                .table_name("Residency")
                .key("id", id.clone())
                .consistent_read(true)
                .send()
                .await
                .unwrap()
                .item,
            Some(item)
        );
        let mut restored = (0, 0);
        for target in &children {
            let value = usage(&fixture, target).await;
            restored.0 += value.0;
            restored.1 += value.1;
        }
        assert_eq!(restored, committed);
        let mut storage = beyonddb::CellStorage::new(fixture.client.clone(), "us-east-1");
        storage
            .refresh_table_size(ACCOUNT, "Residency")
            .await
            .unwrap();
        let described = sdk
            .describe_table()
            .table_name("Residency")
            .send()
            .await
            .unwrap()
            .table
            .unwrap();
        assert_eq!(described.item_count, Some(2));
        let expected =
            (2 + id.as_s().unwrap().len() + 5 + "transaction".len()) as u64 + extra_bytes;
        assert_eq!(described.table_size_bytes, Some(expected as i64));
        fixture.replace().await;
        sdk = fixture.sdk.clone();
        storage = beyonddb::CellStorage::new(fixture.client.clone(), "us-east-1");
        let restored = sdk
            .describe_table()
            .table_name("Residency")
            .send()
            .await
            .unwrap()
            .table
            .unwrap();
        assert_eq!(restored.item_count, described.item_count);
        assert_eq!(restored.table_size_bytes, described.table_size_bytes);
        for _ in 0..2 {
            sdk.delete_item()
                .table_name("Residency")
                .key("id", id.clone())
                .send()
                .await
                .unwrap();
        }
        let mut surviving = (0, 0);
        for target in &children {
            let value = usage(&fixture, target).await;
            surviving.0 += value.0;
            surviving.1 += value.1;
        }
        assert_eq!(surviving, (1, extra_stored_bytes));
        assert_eq!(
            sdk.get_item()
                .table_name("Residency")
                .key("id", extra["id"].clone())
                .consistent_read(true)
                .send()
                .await
                .unwrap()
                .item,
            Some(extra.clone())
        );
        sdk.delete_item()
            .table_name("Residency")
            .key("id", extra["id"].clone())
            .send()
            .await
            .unwrap();
        for target in &children {
            assert_eq!(usage(&fixture, target).await, (0, 0));
        }
        assert!(
            sdk.get_item()
                .table_name("Residency")
                .key("id", id)
                .consistent_read(true)
                .send()
                .await
                .unwrap()
                .item
                .is_none()
        );
        storage
            .refresh_table_size(ACCOUNT, "Residency")
            .await
            .unwrap();
        let empty = sdk
            .describe_table()
            .table_name("Residency")
            .send()
            .await
            .unwrap()
            .table
            .unwrap();
        assert_eq!(
            (empty.item_count, empty.table_size_bytes),
            (Some(0), Some(0))
        );
        fixture.shutdown().await;
    });
}

#[test]
fn sdk_compacted_write_replays_after_owner_restore_without_reapplying_images() {
    let (host, core) = runtimes();
    host.block_on(async {
        use aws_sdk_dynamodb::types::{Get, TransactGetItem};
        let mut fixture = Fixture::new(host.handle(), core.handle(), 2, 8).await;
        let mut sdk = aws_sdk_dynamodb::Client::from_conf(
            fixture
                .sdk
                .config()
                .to_builder()
                .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled())
                .build(),
        );
        let writes = fixture
            .data
            .iter()
            .map(|(_, original)| {
                let mut item = original.clone();
                item.insert("value".into(), AwsAttributeValue::S("transaction".into()));
                TransactWriteItem::builder()
                    .put(
                        Put::builder()
                            .table_name("Residency")
                            .set_item(Some(item))
                            .build()
                            .unwrap(),
                    )
                    .build()
            })
            .collect::<Vec<_>>();
        let request = sdk
            .transact_write_items()
            .client_request_token("compacted-write")
            .set_transact_items(Some(writes.clone()));
        request.clone().send().await.unwrap();
        sdk.update_item()
            .table_name("Residency")
            .key("id", fixture.data[0].1["id"].clone())
            .update_expression("SET #v = :v")
            .expression_attribute_names("#v", "value")
            .expression_attribute_values(":v", AwsAttributeValue::S("later-write".into()))
            .send()
            .await
            .unwrap();
        fixture.replace().await;
        sdk = fixture.sdk.clone();
        // Restored token/decision records must return the original success without
        // overwriting the later mutation, even though operation images are gone.
        sdk.transact_write_items()
            .client_request_token("compacted-write")
            .set_transact_items(Some(writes))
            .send()
            .await
            .unwrap();
        let reads = fixture
            .data
            .iter()
            .map(|(_, item)| {
                TransactGetItem::builder()
                    .get(
                        Get::builder()
                            .table_name("Residency")
                            .key("id", item["id"].clone())
                            .build()
                            .unwrap(),
                    )
                    .build()
            })
            .collect();
        let result = sdk
            .transact_get_items()
            .set_transact_items(Some(reads))
            .send()
            .await
            .unwrap();
        let values = result
            .responses()
            .iter()
            .map(|result| result.item().unwrap()["value"].as_s().unwrap().as_str())
            .collect::<Vec<_>>();
        assert_eq!(values, ["later-write", "transaction"]);
        fixture.shutdown().await;
    });
}
