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

    // BatchGetItem reaches the storage table-info lookup directly. Warm its
    // backend cache, then ensure local recreation replaces that generation.
    for id in ["first", "second"] {
        fixture
            .sdk
            .put_item()
            .table_name("ProcessData")
            .item("pk", AttributeValue::S(id.into()))
            .item("value", AttributeValue::S("old".into()))
            .send()
            .await
            .unwrap();
    }
    let batch = || {
        HashMap::from([(
            "ProcessData".into(),
            KeysAndAttributes::builder()
                .keys(HashMap::from([(
                    "pk".into(),
                    AttributeValue::S("first".into()),
                )]))
                .keys(HashMap::from([(
                    "pk".into(),
                    AttributeValue::S("second".into()),
                )]))
                .consistent_read(true)
                .build()
                .unwrap(),
        )])
    };
    let old = fixture
        .sdk
        .batch_get_item()
        .set_request_items(Some(batch()))
        .send()
        .await
        .unwrap();
    assert_eq!(old.responses().unwrap()["ProcessData"].len(), 2);
    assert!(
        old.responses().unwrap()["ProcessData"]
            .iter()
            .all(|item| item.get("value") == Some(&AttributeValue::S("old".into())))
    );
    fixture
        .sdk
        .update_table()
        .table_name("ProcessData")
        .deletion_protection_enabled(false)
        .send()
        .await
        .unwrap();
    fixture
        .sdk
        .delete_table()
        .table_name("ProcessData")
        .send()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            match fixture
                .sdk
                .describe_table()
                .table_name("ProcessData")
                .send()
                .await
            {
                Err(error)
                    if error
                        .as_service_error()
                        .is_some_and(|error| error.is_resource_not_found_exception()) =>
                {
                    break;
                }
                Ok(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                Err(error) => panic!("delete did not settle: {error:?}"),
            }
        }
    })
    .await
    .unwrap();
    let recreated = fixture
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
    assert_ne!(
        recreated.table_description().unwrap().table_id(),
        before_update.table().unwrap().table_id()
    );
    for id in ["first", "second"] {
        fixture
            .sdk
            .put_item()
            .table_name("ProcessData")
            .item("pk", AttributeValue::S(id.into()))
            .item("value", AttributeValue::S("new".into()))
            .send()
            .await
            .unwrap();
    }
    // The published metadata and item values must survive a real owner restart.
    stop(&mut fixture.child, &fixture.log);
    fixture.child = start(&fixture.config, &fixture.log, false, fixture.s3);
    wait_healthy(&mut fixture.child, fixture.public, &fixture.log);
    let new = fixture
        .sdk
        .batch_get_item()
        .set_request_items(Some(batch()))
        .send()
        .await
        .unwrap();
    assert_eq!(new.responses().unwrap()["ProcessData"].len(), 2);
    assert!(
        new.responses().unwrap()["ProcessData"]
            .iter()
            .all(|item| item.get("value") == Some(&AttributeValue::S("new".into())))
    );
    stop(&mut fixture.child, &fixture.log);
}
