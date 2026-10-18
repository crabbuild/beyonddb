use super::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType,
};

const ACCOUNT: &str = "123456789012";

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

// Four native eviction/recovery contracts run in cellar_reclamation. The
// remaining creation-time metadata race below still awaits its native port.

struct ReleaseRangesDuringMetadataRead {
    account: cellule_runtime::identity::CellTarget,
    ranges: Vec<CellHandle>,
    fired: Arc<std::sync::atomic::AtomicBool>,
}

impl cellule_runtime::client::LocalCellResolver for ReleaseRangesDuringMetadataRead {
    fn resolve(
        &self,
        target: cellule_runtime::identity::CellTarget,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = cellule_runtime::Result<Option<CellHandle>>>
                + Send
                + 'static,
        >,
    > {
        let selected = target == self.account;
        let ranges = self.ranges.clone();
        let fired = self.fired.clone();
        Box::pin(async move {
            if selected && !fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
                for handle in ranges {
                    handle.drain().await?;
                }
            }
            Ok(None)
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn creation_reclamation_tolerates_owner_release_during_metadata_lookup() {
    let fixture = Fixture::with_capacity(2, 5).await;
    let sdk = super::provisioning::sdk_without_retries(&fixture);
    assert!(
        super::provisioning::create(&sdk, "ResidencyPending", false)
            .send()
            .await
            .is_err()
    );
    let account = account_target(ACCOUNT).unwrap();
    let table = fixture
        .client
        .query::<DescribeTable>(&account, None, Json("ResidencyPending".into()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let client =
        fixture
            .client
            .clone()
            .with_local_resolver(Arc::new(ReleaseRangesDuringMetadataRead {
                account,
                ranges: fixture
                    .data
                    .iter()
                    .map(|(handle, _)| handle.clone())
                    .collect(),
                fired: fired.clone(),
            }));
    // A concurrent release, or metadata restoration itself, can close ranges
    // after discovery. Reclamation must not read them from a stale local list.
    let partitions = fixture
        .provisioner
        .provision(&client, ACCOUNT, &table)
        .await
        .unwrap();
    assert_eq!(partitions.len(), 2);
    assert!(fired.load(std::sync::atomic::Ordering::SeqCst));
    for (_, item) in &fixture.data {
        let observed = sdk
            .get_item()
            .table_name("Residency")
            .key("id", item["id"].clone())
            .consistent_read(true)
            .send()
            .await
            .unwrap();
        assert_eq!(observed.item.as_ref(), Some(item));
    }
    fixture.shutdown().await;
}
