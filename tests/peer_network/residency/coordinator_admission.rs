// Signed cold replay/publication and SDK absence/claim races run in
// cellar_admission. Remaining admission contracts below await native ports.
use super::forward_cache::CountedAuthority;
use super::*;
use beyonddb::{ReadCoordinatorRegistration, RegisterCoordinatorShardInput};

const ACCOUNT: &str = "123456789012";

// Native reservation covers last-slot admission through the SDK owner CAS.
// An accepted SDK claim may finish after caller cancellation, with no handler invocation.

// Native durable_registration warms undiscoverable SDK coordinators, requires
// their account publication, and verifies every row after fresh-owner restore.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_sdk_coordinator_overlaps_catalog_publication_with_authority_lookup() {
    let store = Arc::new(CountedAuthority::default());
    let fixture = Fixture::with_store_capacity_and_peer_cache(1, store.clone(), 16, true).await;
    let token = "cold-catalog-overlap";
    let account = account_target(ACCOUNT).unwrap();
    let target = beyonddb::coordinator_target(ACCOUNT, token.as_bytes()).unwrap();
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let catalog = super::forward_cache::CreationGate {
        paths: [fixture
            .layout
            .catalog_head_path(account.tenant().as_bytes(), target.cell_id().as_bytes()[0])]
        .into_iter()
        .collect(),
        entered: entered.clone(),
        release: release.clone(),
    };
    *store.creation_gate.lock().unwrap() = Some(catalog.clone());
    *store.publication_gate.lock().unwrap() = Some(catalog);
    *store.read_gate.lock().unwrap() = Some(super::forward_cache::CreationGate {
        paths: [fixture.layout.control_path(target.cell_id().as_bytes())]
            .into_iter()
            .collect(),
        entered: entered.clone(),
        release: release.clone(),
    });
    let request = fixture
        .sdk
        .transact_write_items()
        .client_request_token(token)
        .transact_items(
            aws_sdk_dynamodb::types::TransactWriteItem::builder()
                .put(
                    aws_sdk_dynamodb::types::Put::builder()
                        .table_name("Residency")
                        .item("id", AwsAttributeValue::S("cold-catalog-overlap".into()))
                        .item("value", AwsAttributeValue::N("7".into()))
                        .build()
                        .unwrap(),
                )
                .build(),
        );
    let job = tokio::spawn(request.clone().send());
    let overlap = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        entered.acquire_many(2).await.unwrap().forget();
    })
    .await
    .is_ok();
    // Release every gate before collecting the SDK result, including on the
    // serial baseline. The assertion checks dependencies, not request timeout.
    *store.creation_gate.lock().unwrap() = None;
    *store.publication_gate.lock().unwrap() = None;
    *store.read_gate.lock().unwrap() = None;
    release.add_permits(2);
    tokio::time::timeout(std::time::Duration::from_secs(10), job)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let registered = fixture
        .client
        .query::<ReadCoordinatorRegistration>(
            &account,
            None,
            Json(RegisterCoordinatorShardInput {
                account_id: ACCOUNT.into(),
                shard: u32::from_be_bytes(target.partition().try_into().unwrap()),
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    assert!(registered, "SDK success requires durable account discovery");
    let control = CellAuthority::new(fixture.layout.clone())
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert!(control.value().root.is_some());
    let incarnation = control.value().incarnation;
    fixture
        .provisioner
        .admit_coordinator(ACCOUNT, token.as_bytes())
        .await
        .unwrap()
        .drain()
        .await
        .unwrap();
    fixture
        .provisioner
        .admit_coordinator(ACCOUNT, token.as_bytes())
        .await
        .unwrap();
    request.send().await.unwrap();
    let restored = CellAuthority::new(fixture.layout.clone())
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.value().incarnation, incarnation);
    let item = fixture
        .sdk
        .get_item()
        .table_name("Residency")
        .key("id", AwsAttributeValue::S("cold-catalog-overlap".into()))
        .consistent_read(true)
        .send()
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(item["value"], AwsAttributeValue::N("7".into()));
    fixture.shutdown().await;
    assert!(
        overlap,
        "cold coordinator catalog publication waited for the independent authority lookup"
    );
}
