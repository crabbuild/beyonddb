use crate::residency_fixture::{ACCOUNT, Fixture};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType, Tag,
};
use beyonddb::*;

pub(super) fn model_tag(value: &str) -> Tag {
    Tag::builder()
        .key("beyonddb:cell-model")
        .value(value)
        .build()
        .unwrap()
}
pub(super) async fn table(fixture: &Fixture, name: &str) -> TableRecord {
    fixture
        .client
        .query::<DescribeTable>(&account_target(ACCOUNT).unwrap(), None, Json(name.into()))
        .await
        .unwrap()
        .output
        .0
        .unwrap()
}
pub(super) async fn ranges(fixture: &Fixture, id: &str) -> Vec<RoutePagePartition> {
    let RoutePageOutcome::Page {
        partitions,
        has_more: false,
        ..
    } = read_route_page(
        &fixture.client,
        &account_target(ACCOUNT).unwrap(),
        RoutePageInput {
            table_id: id.into(),
            start_key: None,
            after_lower: None,
            expected_epoch: None,
        },
    )
    .await
    .unwrap()
    else {
        panic!("small fixture directory changed or exceeded a page");
    };
    partitions
}
pub(super) async fn target(
    fixture: &Fixture,
    table: &TableRecord,
    id: &str,
) -> cellar_runtime::identity::CellTarget {
    let key = ordered_data_key(
        &table.id,
        &extenddb_core::types::Item::from([(
            "id".into(),
            extenddb_core::types::AttributeValue::S(id.into()),
        )]),
        &table.key_schema,
    )
    .unwrap();
    let range = ranges(fixture, &table.id)
        .await
        .into_iter()
        .find(|range| range.lower <= key && range.upper.as_ref().is_none_or(|upper| &key < upper))
        .unwrap();
    data_target(ACCOUNT, &table.id, &range.partition_id).unwrap()
}
pub(super) fn create(
    sdk: &aws_sdk_dynamodb::Client,
    name: &str,
    index: bool,
) -> aws_sdk_dynamodb::operation::create_table::builders::CreateTableFluentBuilder {
    let mut request = sdk
        .create_table()
        .table_name(name)
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
        );
    if index {
        request = request
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("bucket")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .global_secondary_indexes(
                GlobalSecondaryIndex::builder()
                    .index_name("ByBucket")
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("bucket")
                            .key_type(KeyType::Hash)
                            .build()
                            .unwrap(),
                    )
                    .projection(
                        Projection::builder()
                            .projection_type(ProjectionType::All)
                            .build(),
                    )
                    .build()
                    .unwrap(),
            );
    }
    request
}
