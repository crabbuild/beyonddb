use crate::residency_fixture::{ACCOUNT, Fixture};
use aws_sdk_dynamodb::types as sdk;
use beyonddb::*;
use cellar_runtime::{
    MutationIdentity,
    client::{Invocation, InvocationAdmission},
    identity::RequestId,
    registry::Command,
};
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
};

pub const NAME: &str = "ResidencyDelete";
pub fn mutation() -> MutationIdentity {
    let now = crate::native_peer::now_ms();
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    }
}
// Account placement is an internal domain fixture. Public creation below always
// provisions two ordered data cells; no public Single selector is enabled.
pub async fn create_account(fixture: &Fixture) {
    use extenddb_core::types::CreateTableInput;
    let input: CreateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": NAME, "BillingMode":"PAY_PER_REQUEST",
        "KeySchema":[{"AttributeName":"id","KeyType":"HASH"},
                     {"AttributeName":"sort","KeyType":"RANGE"}],
        "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"},
                                {"AttributeName":"sort","AttributeType":"S"},
                                {"AttributeName":"alternate","AttributeType":"S"}],
        "LocalSecondaryIndexes":[{"IndexName":"ByAlternate",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"},
                         {"AttributeName":"alternate","KeyType":"RANGE"}],
            "Projection":{"ProjectionType":"ALL"}}]
    }))
    .unwrap();
    fixture
        .client
        .command::<CreateTable>(
            &account_target(ACCOUNT).unwrap(),
            mutation(),
            Json(TableSpec {
                table_class: Default::default(),
                placement: TablePlacement::Account,
                table_name: input.table_name,
                key_schema: input.key_schema,
                attribute_definitions: input.attribute_definitions,
                local_secondary_indexes: input.local_secondary_indexes.unwrap(),
                global_secondary_indexes: Vec::new(),
                billing_mode: input.billing_mode.unwrap(),
                provisioned_throughput: None,
                deletion_protection_enabled: false,
                initial_tags: Vec::new(),
                resource_arn: Some(format!("arn:aws:dynamodb:us-east-1:{ACCOUNT}:table/{NAME}")),
                stream: None,
            }),
        )
        .await
        .unwrap();
}
pub async fn create_public(fixture: &Fixture) {
    let key = |name: &str, kind| {
        sdk::KeySchemaElement::builder()
            .attribute_name(name)
            .key_type(kind)
            .build()
            .unwrap()
    };
    fixture
        .sdk
        .create_table()
        .table_name(NAME)
        .billing_mode(sdk::BillingMode::PayPerRequest)
        .key_schema(key("id", sdk::KeyType::Hash))
        .key_schema(key("sort", sdk::KeyType::Range))
        .set_attribute_definitions(Some(
            ["id", "sort", "alternate"]
                .into_iter()
                .map(|name| {
                    sdk::AttributeDefinition::builder()
                        .attribute_name(name)
                        .attribute_type(sdk::ScalarAttributeType::S)
                        .build()
                        .unwrap()
                })
                .collect(),
        ))
        .local_secondary_indexes(
            sdk::LocalSecondaryIndex::builder()
                .index_name("ByAlternate")
                .key_schema(key("id", sdk::KeyType::Hash))
                .key_schema(key("alternate", sdk::KeyType::Range))
                .projection(
                    sdk::Projection::builder()
                        .projection_type(sdk::ProjectionType::All)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
}
#[derive(Default)]
pub struct CleanupCalls(pub AtomicUsize);
impl InvocationAdmission for CleanupCalls {
    fn check<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
    ) -> Pin<Box<dyn Future<Output = cellar_runtime::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if invocation.target == account_target(ACCOUNT).unwrap()
                && invocation.mutation.is_some()
                && invocation.operation == ContinueTableDeletion::ID
            {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        })
    }
}
