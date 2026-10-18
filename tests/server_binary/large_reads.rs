use std::collections::HashMap;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, Get, KeySchemaElement, KeyType,
    ScalarAttributeType, TransactGetItem,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker, aws CLI, and the pinned RustFS GA image"]
async fn large_single_cell_transaction_read_survives_owner_restart() {
    let mut fixture = crate::process_fixture_with_cache(2, true).await;
    let table = "ProcessLargeRead";
    fixture
        .sdk
        .create_table()
        .table_name(table)
        .billing_mode(BillingMode::PayPerRequest)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
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
    let mut reads = Vec::new();
    let mut expected = Vec::new();
    for index in 0..10 {
        let key = HashMap::from([("id".into(), AttributeValue::S(format!("key-{index}")))]);
        let mut item = key.clone();
        item.insert(
            "payload".into(),
            AttributeValue::B(vec![0xa5; 380 * 1024].into()),
        );
        fixture
            .sdk
            .put_item()
            .table_name(table)
            .set_item(Some(item.clone()))
            .send()
            .await
            .unwrap();
        reads.push(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(table)
                        .set_key(Some(key))
                        .build()
                        .unwrap(),
                )
                .build(),
        );
        expected.push(item);
    }
    reads.reverse();
    expected.reverse();
    let result = fixture
        .sdk
        .transact_get_items()
        .set_transact_items(Some(reads.clone()))
        .send()
        .await
        .unwrap();
    assert_eq!(result.responses().len(), expected.len());
    for (response, item) in result.responses().iter().zip(&expected) {
        assert_eq!(response.item(), Some(item));
    }
    fixture.child.kill().unwrap();
    fixture.child.wait().unwrap();
    fixture.child = crate::start(&fixture.config, &fixture.log, false, fixture.s3);
    crate::wait_healthy(&mut fixture.child, fixture.public, &fixture.log);
    let restored = fixture
        .sdk
        .transact_get_items()
        .set_transact_items(Some(reads.clone()))
        .send()
        .await
        .unwrap();
    for (response, item) in restored.responses().iter().zip(&expected) {
        assert_eq!(response.item(), Some(item));
    }
    assert_eq!(restored.responses().len(), expected.len());
    // Repeat on a different logical node with no previous local database or
    // log. Only the acknowledged bucket publication can supply these images.
    fixture.child.kill().unwrap();
    fixture.child.wait().unwrap();
    let mut configuration: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&fixture.config).unwrap()).unwrap();
    let old_data = std::path::PathBuf::from(configuration["data_dir"].as_str().unwrap());
    std::fs::remove_dir_all(old_data).unwrap();
    configuration["data_dir"] = serde_json::json!(fixture.root.path().join("replacement"));
    configuration["node_name"] = serde_json::json!("bdb-process-replacement");
    let peer = crate::free_addr();
    configuration["peer_bind"] = serde_json::json!(peer);
    configuration["peer_advertise"] = serde_json::json!(peer.to_string());
    std::fs::write(&fixture.config, configuration.to_string()).unwrap();
    // The configured node lease lasts ten seconds. A distinct node cannot
    // replace the killed owner's authority until that lease has expired.
    tokio::time::sleep(std::time::Duration::from_secs(11)).await;
    fixture.child = crate::start(&fixture.config, &fixture.log, false, fixture.s3);
    crate::wait_healthy(&mut fixture.child, fixture.public, &fixture.log);
    let moved = fixture
        .sdk
        .transact_get_items()
        .set_transact_items(Some(reads))
        .send()
        .await
        .unwrap();
    assert_eq!(moved.responses().len(), expected.len());
    for (response, item) in moved.responses().iter().zip(&expected) {
        assert_eq!(response.item(), Some(item));
    }
    crate::stop(&mut fixture.child, &fixture.log);
}
