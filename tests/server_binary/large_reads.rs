use std::collections::HashMap;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, Get, KeySchemaElement, KeyType,
    ScalarAttributeType, TransactGetItem,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker, aws CLI, and the pinned RustFS GA image"]
async fn oversized_single_cell_transaction_read_uses_saved_images() {
    let mut fixture = crate::process_fixture_with_cache(1, true).await;
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
        .set_transact_items(Some(reads))
        .send()
        .await
        .unwrap();
    assert_eq!(result.responses().len(), expected.len());
    for (response, item) in result.responses().iter().zip(expected) {
        assert_eq!(response.item(), Some(&item));
    }
    crate::stop(&mut fixture.child, &fixture.log);
}
