use crate::residency_fixture::{Fixture, runtimes};
use aws_sdk_dynamodb::types::AttributeValue as AwsAttributeValue;
use bdb_cellar_runtime::{MutationIdentity, client::InvocationError, identity::RequestId};
use beyonddb::*;
use extenddb_core::expression::{CompareOp, Expr, ExpressionMaps, PathElement, UpdateAction};
use extenddb_core::types::{AttributeValue, Item};
use extenddb_storage::StreamEngine;
use std::collections::HashMap;
const ACCOUNT: &str = "123456789012";
fn identity() -> MutationIdentity {
    let now = crate::native_peer::now_ms();
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    }
}
async fn table_id(fixture: &Fixture, name: &str) -> String {
    fixture
        .client
        .query::<DescribeTable>(&account_target(ACCOUNT).unwrap(), None, Json(name.into()))
        .await
        .unwrap()
        .output
        .0
        .unwrap()
        .id
}
fn add(table: &str, epoch: u64, key: &str, condition: bool) -> BatchedPartitionUpdate {
    let path = vec![PathElement::Attribute("value".into())];
    let actions = [UpdateAction::Add {
        path: path.clone(),
        value: Expr::Placeholder("one".into()),
    }];
    let condition = condition.then(|| Expr::Compare {
        left: Box::new(Expr::Path(path)),
        op: CompareOp::Eq,
        right: Box::new(Expr::Placeholder("one".into())),
    });
    let maps = ExpressionMaps::new(
        HashMap::new(),
        HashMap::from([("one".into(), AttributeValue::N("1".into()))]),
    );
    BatchedPartitionUpdate {
        input: PartitionUpdateInput::from_expression(
            table.into(),
            epoch,
            Item::from([("id".into(), AttributeValue::S(key.into()))]),
            &actions,
            condition.as_ref(),
            &maps,
        ),
        return_old: true,
        return_new: true,
    }
}

async fn records(fixture: &Fixture, arn: &str) -> Vec<extenddb_core::types::StreamRecord> {
    let streams = beyonddb::CellStorage::new(fixture.client.clone(), "us-east-1");
    let description = streams
        .describe_stream(
            ACCOUNT,
            &extenddb_core::types::DescribeStreamInput {
                stream_arn: arn.into(),
                limit: Some(100),
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();
    let shard = &description.shards[0].shard_id;
    streams.validate_shard(ACCOUNT, arn, shard).await.unwrap();
    streams
        .get_stream_records(ACCOUNT, shard, None, 100)
        .await
        .unwrap()
        .0
}

#[test]
fn returned_update_batch_isolates_conditions_and_keeps_stream_ordinals() {
    let (host, core) = runtimes();
    host.block_on(async {
    let mut fixture = Fixture::new(host.handle(), core.handle(), 1, 16).await;
    let mut sdk = fixture.sdk.clone();
    let table = "ResidencyReturnedStream";
    let arn = sdk.create_table().table_name(table)
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .key_schema(aws_sdk_dynamodb::types::KeySchemaElement::builder().attribute_name("id")
            .key_type(aws_sdk_dynamodb::types::KeyType::Hash).build().unwrap())
        .attribute_definitions(aws_sdk_dynamodb::types::AttributeDefinition::builder().attribute_name("id")
            .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S).build().unwrap())
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
    let id = table_id(&fixture, table).await;
    let route = crate::single_leaf_route(&fixture.client, &account_target(ACCOUNT).unwrap(), &id)
        .await.unwrap();
    assert_eq!(route.partitions.len(), 2);
    let range = route.partitions[0].clone();
    let target = beyonddb::data_target(ACCOUNT, &id, &range.partition_id).unwrap();
    sdk.put_item()
        .table_name(table)
        .item("id", AwsAttributeValue::S("condition".into()))
        .item("value", AwsAttributeValue::N("0".into()))
        .send()
        .await
        .unwrap();
    let error = sdk
        .update_item()
        .table_name(table)
        .key("id", AwsAttributeValue::S("condition".into()))
        .update_expression("ADD #value :one")
        .condition_expression("#value = :one")
        .expression_attribute_names("#value", "value")
        .expression_attribute_values(":one", AwsAttributeValue::N("1".into()))
        .return_values_on_condition_check_failure(
            aws_sdk_dynamodb::types::ReturnValuesOnConditionCheckFailure::AllOld,
        )
        .send()
        .await
        .unwrap_err();
    let aws_sdk_dynamodb::operation::update_item::UpdateItemError::ConditionalCheckFailedException(
        error,
    ) = error.as_service_error().unwrap()
    else {
        panic!("incorrect SDK condition error");
    };
    assert_eq!(
        error.item().unwrap()["value"],
        AwsAttributeValue::N("0".into())
    );
    let updates = vec![
        add(&id, range.epoch, "first", false),
        add(&id, range.epoch, "condition", true),
        add(&id, range.epoch, "second", false),
    ];
    let request = identity();
    let result = fixture
        .client
        .command::<PartitionUpdateBatch>(&target, request, Json(updates.clone()))
        .await
        .unwrap();
    let PartitionUpdateBatchOutcome::Results(results) = result.output else {
        panic!("unexpected batch fallback");
    };
    assert!(matches!(
        results[0],
        PartitionUpdateOutcome::Applied { old: None, .. }
    ));
    assert!(matches!(
        results[2],
        PartitionUpdateOutcome::Applied { old: None, .. }
    ));
    let PartitionUpdateOutcome::ConditionFailed(Some(old)) = &results[1] else {
        panic!("condition result lost its old image");
    };
    assert_eq!(old["value"], AttributeValue::N("0".into()));
    let before = records(&fixture, &arn).await;
    assert_eq!(
        before.len(),
        3,
        "each successful mutation needs its own stream record"
    );
    assert_ne!(before[1].event_id, before[2].event_id);
    fixture
        .client
        .command::<PartitionUpdateBatch>(&target, request, Json(updates.clone()))
        .await
        .unwrap();
    assert_eq!(
        records(&fixture, &arn).await.len(),
        3,
        "receipt replay must not repeat stream intent"
    );
    fixture.replace().await;
    sdk = fixture.sdk.clone();
    let recovered_replay = fixture.client.command::<PartitionUpdateBatch>(
        &target, request, Json(updates)).await.unwrap();
    assert_eq!(recovered_replay.receipt, result.receipt);
    for (key, value) in [("first", "1"), ("condition", "0"), ("second", "1")] {
        let item = sdk
            .get_item()
            .table_name(table)
            .key("id", AwsAttributeValue::S(key.into()))
            .send()
            .await
            .unwrap()
            .item
            .unwrap();
        assert_eq!(item["value"], AwsAttributeValue::N(value.into()));
    }
    assert_eq!(records(&fixture, &arn).await.len(), 3);
    // A large stored image makes even a tiny ADD exceed the compact batch
    // result bound. Its rejected receipt must roll back both item and stream.
    sdk.put_item()
        .table_name(table)
        .item("id", AwsAttributeValue::S("large".into()))
        .item("value", AwsAttributeValue::N("0".into()))
        .item("padding", AwsAttributeValue::S("x".repeat(140_000)))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
        .send()
        .await
        .unwrap();
    let rejected = fixture
        .client
        .command::<PartitionUpdateBatch>(
            &target,
            identity(),
            Json(vec![add(&id, range.epoch, "large", false)]),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(rejected, InvocationError::Rejected(ref committed) if committed.output == PartitionUpdateBatchOutcome::IndividualRequired)
    );
    let item = sdk
        .get_item()
        .table_name(table)
        .key("id", AwsAttributeValue::S("large".into()))
        .send()
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(item["value"], AwsAttributeValue::N("0".into()));
    assert_eq!(
        records(&fixture, &arn).await.len(),
        4,
        "rolled-back ADD must emit no stream record"
    );
    sdk.update_item()
        .table_name(table)
        .key("id", AwsAttributeValue::S("large".into()))
        .update_expression("ADD #value :one")
        .expression_attribute_names("#value", "value")
        .expression_attribute_values(":one", AwsAttributeValue::N("1".into()))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::AllNew)
        .send()
        .await
        .unwrap();
    let item = sdk
        .get_item()
        .table_name(table)
        .key("id", AwsAttributeValue::S("large".into()))
        .send()
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(
        item["value"],
        AwsAttributeValue::N("1".into()),
        "fallback must apply ADD once"
    );
    assert_eq!(records(&fixture, &arn).await.len(), 5);
    fixture.shutdown().await;
    });
}

#[test]
fn sdk_returned_update_compacts_large_escaped_old_and_new_images() {
    let (host, core) = runtimes();
    host.block_on(async {
        let mut fixture = Fixture::new(host.handle(), core.handle(), 1, 16).await;
        let mut sdk = fixture.sdk.clone();
        let padding = "\0".repeat(390_000);
        sdk.put_item()
            .table_name("Residency")
            .item("id", AwsAttributeValue::S("escaped".into()))
            .item("value", AwsAttributeValue::N("0".into()))
            .item("padding", AwsAttributeValue::S(padding.clone()))
            .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
            .send()
            .await
            .unwrap();
        let result = sdk
            .update_item()
            .table_name("Residency")
            .key("id", AwsAttributeValue::S("escaped".into()))
            .update_expression("ADD #value :one")
            .expression_attribute_names("#value", "value")
            .expression_attribute_values(":one", AwsAttributeValue::N("1".into()))
            .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
            .send()
            .await
            .unwrap();
        let old = result.attributes.unwrap();
        assert_eq!(old["value"], AwsAttributeValue::N("0".into()));
        assert_eq!(old["padding"], AwsAttributeValue::S(padding.clone()));
        let id = table_id(&fixture, "Residency").await;
        let route =
            crate::single_leaf_route(&fixture.client, &account_target(ACCOUNT).unwrap(), &id)
                .await
                .unwrap();
        assert_eq!(route.partitions.len(), 2);
        fixture.replace().await;
        sdk = fixture.sdk.clone();
        let new = sdk
            .get_item()
            .table_name("Residency")
            .key("id", AwsAttributeValue::S("escaped".into()))
            .send()
            .await
            .unwrap()
            .item
            .unwrap();
        assert_eq!(new["value"], AwsAttributeValue::N("1".into()));
        assert_eq!(new["padding"], AwsAttributeValue::S(padding));
        fixture.shutdown().await;
    });
}
