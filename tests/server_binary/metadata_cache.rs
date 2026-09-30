use crate::*;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker, aws CLI, and the pinned RustFS GA image"]
async fn cached_metadata_observes_local_create_and_update() {
    let mut fixture = process_fixture_with_cache(1, true).await;
    let before = fixture.sdk.list_tables().send().await.unwrap();
    assert!(!before.table_names().contains(&"ProcessData".to_owned()));

    fixture
        .sdk
        .create_table()
        .table_name("ProcessData")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let after = fixture.sdk.list_tables().send().await.unwrap();
    assert!(after.table_names().contains(&"ProcessData".to_owned()));
    let before_update = fixture
        .sdk
        .describe_table()
        .table_name("ProcessData")
        .send()
        .await
        .unwrap();
    assert_eq!(
        before_update
            .table()
            .and_then(|table| table.deletion_protection_enabled()),
        Some(false),
    );

    fixture
        .sdk
        .update_table()
        .table_name("ProcessData")
        .deletion_protection_enabled(true)
        .send()
        .await
        .unwrap();
    let after_update = fixture
        .sdk
        .describe_table()
        .table_name("ProcessData")
        .send()
        .await
        .unwrap();
    assert_eq!(
        after_update
            .table()
            .and_then(|table| table.deletion_protection_enabled()),
        Some(true),
    );
    stop(&mut fixture.child, &fixture.log);
}
