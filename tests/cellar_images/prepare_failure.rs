use crate::fixture::{Fixture, SdkItem, runtimes};
use aws_sdk_dynamodb::types::AttributeValue as AwsAttributeValue;
#[test]
fn sdk_bounded_prepare_preserves_large_failure_images_and_token_recovery() {
    let (host, core) = runtimes();
    host.block_on(async {
    use aws_sdk_dynamodb::types::{Put, ReturnValuesOnConditionCheckFailure, TransactWriteItem};

    let mut fixture=Fixture::new(host.handle(),core.handle(),1).await;
    let mut sdk=fixture.sdk.clone();
    let old = SdkItem::from([
        ("id".into(), AwsAttributeValue::S("wide-condition".into())),
        (
            "payload".into(),
            AwsAttributeValue::S("\0".repeat(384 * 1024)),
        ),
    ]);
    sdk.put_item()
        .table_name("Residency")
        .set_item(Some(old.clone()))
        .send()
        .await
        .unwrap();
    let write = |sdk:&aws_sdk_dynamodb::Client| sdk
        .transact_write_items()
        .client_request_token("bounded-wide-condition")
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("Residency")
                        .item("id", AwsAttributeValue::S("wide-staged".into()))
                        .item("value", AwsAttributeValue::S("committed".into()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("Residency")
                        .item("id", old["id"].clone())
                        .condition_expression("attribute_not_exists(id)")
                        .return_values_on_condition_check_failure(
                            ReturnValuesOnConditionCheckFailure::AllOld,
                        )
                        .build()
                        .unwrap(),
                )
                .build(),
        );
    let error = write(&sdk).send().await.unwrap_err();
    let aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError::TransactionCanceledException(error) = error.as_service_error().unwrap() else {
        panic!("expected condition cancellation");
    };
    assert_eq!(
        error.cancellation_reasons()[1].code(),
        Some("ConditionalCheckFailed")
    );
    assert_eq!(error.cancellation_reasons()[1].item(), Some(&old));
    assert!(
        sdk.get_item()
            .table_name("Residency")
            .key("id", AwsAttributeValue::S("wide-staged".into()))
            .consistent_read(true)
            .send()
            .await
            .unwrap()
            .item
            .is_none()
    );
    // Release and restore the data owner after the failed transaction. The old
    // image remains intact and cancellation leaves no blocking write locks.
    fixture.replace().await;
    sdk=fixture.sdk.clone();
    assert_eq!(
        sdk.get_item()
            .table_name("Residency")
            .key("id", old["id"].clone())
            .consistent_read(true)
            .send()
            .await
            .unwrap()
            .item,
        Some(old.clone())
    );
    sdk.delete_item()
        .table_name("Residency")
        .key("id", old["id"].clone())
        .send()
        .await
        .unwrap();
    // Canceled token reuse keeps the identical request fingerprint.
    write(&sdk).send().await.unwrap();
    fixture.replace().await;
    sdk=fixture.sdk.clone();
    let item = sdk
        .get_item()
        .table_name("Residency")
        .key("id", AwsAttributeValue::S("wide-staged".into()))
        .consistent_read(true)
        .send()
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(item["value"], AwsAttributeValue::S("committed".into()));
    sdk.put_item()
        .table_name("Residency")
        .item("id", AwsAttributeValue::S("wide-staged".into()))
        .item("value", AwsAttributeValue::S("newer".into()))
        .send()
        .await
        .unwrap();
    write(&sdk).send().await.unwrap();
    assert_eq!(
        sdk.get_item()
            .table_name("Residency")
            .key("id", AwsAttributeValue::S("wide-staged".into()))
            .consistent_read(true)
            .send()
            .await
            .unwrap()
            .item
            .unwrap()["value"],
        AwsAttributeValue::S("newer".into())
    );
    fixture.shutdown().await;
    });
}
