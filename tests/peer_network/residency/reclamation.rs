// All five reclamation contracts run in cellar_reclamation. This AWS builder
// remains for other legacy peer fixtures awaiting their ports.
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType,
};

pub(super) async fn create(sdk: &aws_sdk_dynamodb::Client, name: &str, index: bool) {
    let key = KeySchemaElement::builder()
        .attribute_name("id")
        .key_type(KeyType::Hash)
        .build()
        .unwrap();
    let mut request = sdk
        .create_table()
        .table_name(name)
        .billing_mode(BillingMode::PayPerRequest)
        .key_schema(key.clone())
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        );
    if index {
        request = request.global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("ById")
                .key_schema(key)
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .build()
                .unwrap(),
        );
    }
    request.send().await.unwrap();
}
