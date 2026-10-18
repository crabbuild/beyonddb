//! Broad signed SDK behavior through the native Cellar process and restarts.
use crate::*;
use std::time::{SystemTime, UNIX_EPOCH};
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker, aws CLI, and the pinned RustFS GA image"]
async fn bootstrap_sdk_write_survives_unclean_server_restart() {
    let ProcessFixture {
        root: _root,
        _rustfs: rustfs,
        peer,
        public,
        config,
        log,
        s3,
        mut child,
        sdk,
    } = process_fixture(4).await;
    crate::local_indexes::create(&sdk).await;
    crate::global_indexes::create(&sdk).await;
    let created = sdk
        .create_table()
        .table_name("ProcessData")
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .tags(
            aws_sdk_dynamodb::types::Tag::builder()
                .key("created")
                .value("yes")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let arn = created
        .table_description()
        .and_then(|description| description.table_arn())
        .unwrap();
    assert!(created.table_description().unwrap().table_id().is_some());
    let schema = keyspace::KeySchema {
        partition: keyspace::KeyKind::String,
        sort: None,
    };
    let fences: Vec<_> = [64_u8, 128, 192]
        .into_iter()
        .map(|n| {
            schema
                .encode(
                    keyspace::Component::String(&char::from(n).to_string()),
                    None,
                )
                .unwrap()
        })
        .collect();
    let range = |id: &str| {
        let key = schema
            .encode(keyspace::Component::String(id), None)
            .unwrap();
        fences.iter().take_while(|fence| &key >= *fence).count()
    };
    let condition_key = "\u{100}-condition".to_owned();
    let second_write_key = "\u{100}-transaction".to_owned();
    assert_ne!(range(&condition_key), range("process"));
    assert_ne!(range(&second_write_key), range("process"));
    let initial_tags = sdk
        .list_tags_of_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(initial_tags.tags().len(), 1);
    assert_eq!(initial_tags.tags()[0].key(), "created");
    sdk.tag_resource()
        .resource_arn(arn)
        .tags(
            aws_sdk_dynamodb::types::Tag::builder()
                .key("team")
                .value("crab")
                .build()
                .unwrap(),
        )
        .tags(
            aws_sdk_dynamodb::types::Tag::builder()
                .key("env")
                .value("test")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let tagged = sdk
        .list_tags_of_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(tagged.tags().len(), 3);
    sdk.tag_resource()
        .resource_arn(arn)
        .tags(
            aws_sdk_dynamodb::types::Tag::builder()
                .key("team")
                .value("elastic")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    sdk.untag_resource()
        .resource_arn(arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();
    let item = HashMap::from([
        ("id".into(), AttributeValue::S("process".into())),
        ("value".into(), AttributeValue::S("committed".into())),
    ]);
    sdk.put_item()
        .table_name("ProcessData")
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    for (id, return_old, capacity) in [
        ("delete-none", false, false),
        ("delete-old", true, false),
        ("delete-capacity", false, true),
    ] {
        let deleted_item = HashMap::from([
            ("id".into(), AttributeValue::S(id.into())),
            ("payload".into(), AttributeValue::S("x".repeat(8192))),
        ]);
        sdk.put_item()
            .table_name("ProcessData")
            .set_item(Some(deleted_item.clone()))
            .send()
            .await
            .unwrap();
        let deleted = sdk
            .delete_item()
            .table_name("ProcessData")
            .key("id", AttributeValue::S(id.into()))
            .return_values(if return_old {
                aws_sdk_dynamodb::types::ReturnValue::AllOld
            } else {
                aws_sdk_dynamodb::types::ReturnValue::None
            })
            .return_consumed_capacity(if capacity {
                aws_sdk_dynamodb::types::ReturnConsumedCapacity::Total
            } else {
                aws_sdk_dynamodb::types::ReturnConsumedCapacity::None
            })
            .send()
            .await
            .unwrap();
        assert_eq!(deleted.attributes(), return_old.then_some(&deleted_item));
        assert_eq!(deleted.consumed_capacity().is_some(), capacity);
    }
    let mut batch_ids: Vec<String> = Vec::new();
    for index in 0..64 {
        let id = format!(
            "{}batch-{index}",
            ['!', 'b', '\u{80}', '\u{100}'][index % 4]
        );
        if batch_ids
            .iter()
            .any(|existing| range(existing) == range(&id))
        {
            continue;
        }
        batch_ids.push(id);
        if batch_ids.len() == 3 {
            break;
        }
    }
    assert_eq!(batch_ids.len(), 3);
    let batch_items = batch_ids
        .iter()
        .map(|id| {
            HashMap::from([
                ("id".into(), AttributeValue::S(id.clone())),
                ("value".into(), AttributeValue::S("batched".into())),
            ])
        })
        .collect::<Vec<_>>();
    let writes = batch_items
        .iter()
        .map(|batch_item| {
            WriteRequest::builder()
                .put_request(
                    PutRequest::builder()
                        .set_item(Some(batch_item.clone()))
                        .build()
                        .unwrap(),
                )
                .build()
        })
        .collect();
    let written = sdk
        .batch_write_item()
        .request_items("ProcessData", writes)
        .send()
        .await
        .unwrap();
    assert!(written.unprocessed_items().is_none_or(HashMap::is_empty));
    let batch_keys = batch_items
        .iter()
        .map(|item| HashMap::from([("id".into(), item["id"].clone())]))
        .collect::<Vec<_>>();
    let read_batch = || {
        sdk.batch_get_item().request_items(
            "ProcessData",
            KeysAndAttributes::builder()
                .set_keys(Some(batch_keys.clone()))
                .consistent_read(true)
                .build()
                .unwrap(),
        )
    };
    let batch = read_batch().send().await.unwrap();
    let found = &batch.responses().unwrap()["ProcessData"];
    assert!(batch_items.iter().all(|item| found.contains(item)));
    for total_segments in [2, 3] {
        let mut scanned = HashSet::new();
        for segment in 0..total_segments {
            let mut start = None;
            for page_number in 0..20 {
                let page = sdk
                    .scan()
                    .table_name("ProcessData")
                    .segment(segment)
                    .total_segments(total_segments)
                    .limit(2)
                    .set_exclusive_start_key(start)
                    .send()
                    .await
                    .unwrap();
                for item in page.items() {
                    assert!(scanned.insert(item["id"].as_s().unwrap().clone()));
                }
                start = page.last_evaluated_key().cloned();
                if start.is_none() {
                    break;
                }
                assert!(page_number < 19, "parallel Scan did not finish");
            }
        }
        assert_eq!(
            scanned,
            batch_ids
                .iter()
                .cloned()
                .chain(std::iter::once("process".into()))
                .collect()
        );
    }
    // Prove the executable installs its statistics worker without test-side refresh.
    let expected_bytes: usize = batch_items
        .iter()
        .chain(std::iter::once(&item))
        .flat_map(|item| item.iter())
        .map(|(name, value)| name.len() + value.as_s().unwrap().len())
        .sum();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let table = sdk
            .describe_table()
            .table_name("ProcessData")
            .send()
            .await
            .unwrap()
            .table
            .unwrap();
        if table.item_count == Some(batch_items.len() as i64 + 1)
            && table.table_size_bytes == Some(expected_bytes as i64)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "server statistics did not converge: {table:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let expires = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .saturating_sub(1);
    sdk.put_item()
        .table_name("ProcessData")
        .item("id", AttributeValue::S("expired".into()))
        .item("expires", AttributeValue::N(expires.to_string()))
        .send()
        .await
        .unwrap();
    sdk.update_time_to_live()
        .table_name("ProcessData")
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .attribute_name("expires")
                .enabled(true)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let ttl = sdk
        .describe_time_to_live()
        .table_name("ProcessData")
        .send()
        .await
        .unwrap();
    assert_eq!(
        ttl.time_to_live_description()
            .and_then(|description| description.attribute_name()),
        Some("expires")
    );
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let item = sdk
            .get_item()
            .table_name("ProcessData")
            .key("id", AttributeValue::S("expired".into()))
            .send()
            .await
            .unwrap();
        if item.item().is_none() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "TTL worker did not delete expired item"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let updated = HashMap::from([
        ("id".into(), AttributeValue::S("process".into())),
        ("value".into(), AttributeValue::S("updated".into())),
    ]);
    let update = |id: &str, value: &str| {
        TransactWriteItem::builder()
            .update(
                Update::builder()
                    .table_name("ProcessData")
                    .key("id", AttributeValue::S(id.into()))
                    .update_expression("SET #v = :v")
                    .expression_attribute_names("#v", "value")
                    .expression_attribute_values(":v", AttributeValue::S(value.into()))
                    .build()
                    .unwrap(),
            )
            .build()
    };
    let check = |condition: &str| {
        TransactWriteItem::builder()
            .condition_check(
                ConditionCheck::builder()
                    .table_name("ProcessData")
                    .key("id", AttributeValue::S(condition_key.clone()))
                    .condition_expression(condition)
                    .build()
                    .unwrap(),
            )
            .build()
    };
    sdk.transact_write_items()
        .client_request_token("process-update-check")
        .transact_items(update("process", "updated"))
        .transact_items(update(&second_write_key, "updated"))
        .transact_items(check("attribute_not_exists(id)"))
        .send()
        .await
        .unwrap();
    sdk.transact_write_items()
        .client_request_token("process-update-check")
        .transact_items(update("process", "updated"))
        .transact_items(update(&second_write_key, "updated"))
        .transact_items(check("attribute_not_exists(id)"))
        .send()
        .await
        .unwrap();
    assert!(
        sdk.transact_write_items()
            .client_request_token("process-update-check")
            .transact_items(update("process", "different"))
            .transact_items(update(&second_write_key, "different"))
            .transact_items(check("attribute_not_exists(id)"))
            .send()
            .await
            .is_err()
    );
    assert!(
        sdk.transact_write_items()
            .transact_items(update("process", "rolled back"))
            .transact_items(update(&second_write_key, "rolled back"))
            .transact_items(check("attribute_exists(id)"))
            .send()
            .await
            .is_err()
    );
    let transaction_reads = ["process", second_write_key.as_str(), condition_key.as_str()]
        .into_iter()
        .map(|id| {
            aws_sdk_dynamodb::types::TransactGetItem::builder()
                .get(
                    aws_sdk_dynamodb::types::Get::builder()
                        .table_name("ProcessData")
                        .key("id", AttributeValue::S(id.into()))
                        .projection_expression("#v")
                        .expression_attribute_names("#v", "value")
                        .build()
                        .unwrap(),
                )
                .build()
        })
        .collect::<Vec<_>>();
    let snapshot = sdk
        .transact_get_items()
        .set_transact_items(Some(transaction_reads.clone()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        snapshot.responses()[0].item().unwrap().get("value"),
        Some(&AttributeValue::S("updated".into()))
    );
    assert_eq!(
        snapshot.responses()[1].item().unwrap().get("value"),
        Some(&AttributeValue::S("updated".into()))
    );
    assert!(snapshot.responses()[2].item().is_none());
    let mut large_keys = Vec::new();
    for side in [range("process"), range(&second_write_key)] {
        large_keys.extend(
            (0..1_000)
                .map(|i| {
                    format!(
                        "{}large-{i}",
                        if side == range("process") {
                            ""
                        } else {
                            "\u{100}"
                        }
                    )
                })
                .filter(|id| range(id) == side && range(&support::escaped_key(id)) == side)
                .take(5)
                .map(|id| ("ProcessData".into(), id)),
        );
    }
    let large = support::LargeTransaction::write(&sdk, large_keys).await;
    let read_keys = (0..1_000)
        .map(|i| format!("large-read-{i}"))
        .filter(|id| range(id) == range("process"))
        .take(14)
        .collect();
    let large_read = support::LargeTransaction::single_cell(&sdk, "ProcessData", read_keys).await;
    // Historical coordinator count exceeds the binary's 64 active-Cell slots.
    // Every request still follows the signed SDK path and touches two data Cells.
    let mut shards = std::collections::HashSet::new();
    let resident_tokens = (0..10_000)
        .map(|n| format!("resident-{n}"))
        .filter(|token| {
            shards.insert(
                beyonddb::coordinator_target("123456789012", token.as_bytes())
                    .unwrap()
                    .cell_id(),
            )
        })
        .take(70)
        .collect::<Vec<_>>();
    assert_eq!(resident_tokens.len(), 70);
    for token in &resident_tokens {
        sdk.transact_write_items()
            .client_request_token(token)
            .transact_items(update("process", "updated"))
            .transact_items(update(&second_write_key, "updated"))
            .send()
            .await
            .expect(token);
    }
    // The churn can exceed DynamoDB's ten-minute token window. Historical
    // payloads prove durability; this fresh conditional write proves replay.
    let restart_items = [range("process"), range(&second_write_key)].map(|side| {
        let id = (0..1_000)
            .map(|n| {
                format!(
                    "{}restart-{n}",
                    if side == range("process") {
                        ""
                    } else {
                        "\u{100}"
                    }
                )
            })
            .find(|id| range(id) == side)
            .unwrap();
        TransactWriteItem::builder()
            .put(
                aws_sdk_dynamodb::types::Put::builder()
                    .table_name("ProcessData")
                    .item("id", AttributeValue::S(id))
                    .item("value", AttributeValue::S("committed".into()))
                    .condition_expression("attribute_not_exists(id)")
                    .build()
                    .unwrap(),
            )
            .build()
    });
    let restart_write = sdk
        .transact_write_items()
        .client_request_token("fresh-process-restart")
        .set_transact_items(Some(restart_items.to_vec()));
    restart_write.clone().send().await.unwrap();
    let restart_completed = Instant::now();
    child.kill().unwrap();
    child.wait().unwrap();
    // A replacement process can receive a new private address. Durable Cell
    // ownership must follow its fenced session, including historical coordinators.
    let replacement_peer = loop {
        let address = free_addr();
        if address != peer {
            break address;
        }
    };
    let mut replacement_config: serde_json::Value =
        serde_json::from_slice(&fs::read(&config).unwrap()).unwrap();
    replacement_config["peer_bind"] = json!(replacement_peer);
    replacement_config["peer_advertise"] = json!(replacement_peer.to_string());
    fs::write(&config, replacement_config.to_string()).unwrap();
    let mut restarted = start(&config, &log, false, s3);
    wait_healthy(&mut restarted, public, &log);
    assert!(
        restart_completed.elapsed() < Duration::from_secs(600),
        "restart replay must be tested within the client-token validity window"
    );
    restart_write.send().await.unwrap();
    for item in &restart_items {
        let put = item.put().unwrap();
        let recovered = sdk
            .get_item()
            .table_name("ProcessData")
            .key("id", put.item()["id"].clone())
            .consistent_read(true)
            .send()
            .await
            .unwrap();
        assert_eq!(recovered.item(), Some(put.item()));
    }
    for id in ["delete-none", "delete-old", "delete-capacity"] {
        let deleted = sdk
            .get_item()
            .table_name("ProcessData")
            .key("id", AttributeValue::S(id.into()))
            .send()
            .await
            .unwrap();
        assert!(deleted.item().is_none());
    }
    crate::local_indexes::assert_recovered(&sdk).await;
    crate::global_indexes::assert_recovered(&sdk).await;
    large.assert_state(&sdk).await;
    large_read.assert_state(&sdk).await;
    let read = sdk
        .get_item()
        .table_name("ProcessData")
        .key("id", AttributeValue::S("process".into()))
        .send()
        .await
        .unwrap();
    assert_eq!(read.item(), Some(&updated));
    let recovered_snapshot = sdk
        .transact_get_items()
        .set_transact_items(Some(transaction_reads))
        .send()
        .await
        .unwrap();
    assert_eq!(recovered_snapshot.responses(), snapshot.responses());
    let second = sdk
        .get_item()
        .table_name("ProcessData")
        .key("id", AttributeValue::S(second_write_key.clone()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        second.item().unwrap().get("value"),
        Some(&AttributeValue::S("updated".into()))
    );
    let recovered_batch = read_batch().send().await.unwrap();
    let recovered = &recovered_batch.responses().unwrap()["ProcessData"];
    assert!(batch_items.iter().all(|item| recovered.contains(item)));
    let recovered_ttl = sdk
        .describe_time_to_live()
        .table_name("ProcessData")
        .send()
        .await
        .unwrap();
    assert_eq!(
        recovered_ttl
            .time_to_live_description()
            .and_then(|description| description.attribute_name()),
        Some("expires")
    );
    let recovered_tags = sdk
        .list_tags_of_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(recovered_tags.tags().len(), 2);
    assert_eq!(recovered_tags.tags()[0].key(), "created");
    assert_eq!(recovered_tags.tags()[0].value(), "yes");
    assert_eq!(recovered_tags.tags()[1].key(), "team");
    assert_eq!(recovered_tags.tags()[1].value(), "elastic");
    sdk.update_time_to_live()
        .table_name("ProcessData")
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .attribute_name("expires")
                .enabled(false)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    stop(&mut restarted, &log);
    let mut drained = start(&config, &log, false, s3);
    wait_healthy(&mut drained, public, &log);
    let disabled_ttl = sdk
        .describe_time_to_live()
        .table_name("ProcessData")
        .send()
        .await
        .unwrap();
    assert!(
        disabled_ttl
            .time_to_live_description()
            .and_then(|description| description.attribute_name())
            .is_none()
    );
    // Preserve-mode restart recovers the SDK clean-reload proof and published
    // roots as well as taking over expired active owners after a hard restart.
    let idle_read = sdk
        .get_item()
        .table_name("ProcessData")
        .key("id", AttributeValue::S("process".into()))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(idle_read.item(), Some(&updated));
    sdk.delete_table()
        .table_name("ProcessData")
        .send()
        .await
        .unwrap();
    // Name reuse follows durable retirement of every directory, which continues
    // after DeleteTable has returned its DELETING acknowledgement.
    sdk.wait_until_table_not_exists()
        .table_name("ProcessData")
        .wait(Duration::from_secs(60))
        .await
        .unwrap();
    sdk.create_table()
        .table_name("ProcessData")
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap_or_else(|error| {
            panic!(
                "table recreation failed: {error:?}; status {:?}; log {}",
                drained.try_wait(),
                fs::read_to_string(&log).unwrap()
            )
        });
    let recreated_tags = sdk
        .list_tags_of_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert!(recreated_tags.tags().is_empty());
    let recreated_ttl = sdk
        .describe_time_to_live()
        .table_name("ProcessData")
        .send()
        .await
        .unwrap();
    assert!(
        recreated_ttl
            .time_to_live_description()
            .and_then(|description| description.attribute_name())
            .is_none()
    );
    sdk.describe_table()
        .table_name("ProcessData")
        .send()
        .await
        .unwrap();
    stop(&mut drained, &log);
    drop(rustfs);
}
