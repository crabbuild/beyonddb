// Signed cold replay/publication and SDK absence/claim races run in
// cellar_admission. Remaining admission contracts below await native ports.
use super::forward_cache::CountedAuthority;
use super::*;
use beyonddb::{
    CoordinatorProvisioner, ReadCoordinatorRegistration, RegisterCoordinatorShardInput,
};

const ACCOUNT: &str = "123456789012";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_cold_admission_reserves_the_last_slot_and_cancellation_releases_it() {
    let store = Arc::new(CountedAuthority::default());
    let fixture = Fixture::with_store_capacity_and_peer_cache(1, store.clone(), 5, true).await;
    assert_eq!(fixture.node.runtime().stats().active_cells(), 4);
    let first = b"slot-first".to_vec();
    let first_target = beyonddb::coordinator_target(ACCOUNT, &first).unwrap();
    let second = (0..100)
        .map(|index| format!("slot-second-{index}").into_bytes())
        .find(|key| {
            beyonddb::coordinator_target(ACCOUNT, key)
                .unwrap()
                .cell_id()
                .as_bytes()[0]
                % 64
                != first_target.cell_id().as_bytes()[0] % 64
        })
        .unwrap();
    let second_target = beyonddb::coordinator_target(ACCOUNT, &second).unwrap();
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    *store.creation_gate.lock().unwrap() = Some(super::forward_cache::CreationGate {
        paths: [&first_target, &second_target]
            .into_iter()
            .map(|target| fixture.layout.control_path(target.cell_id().as_bytes()))
            .collect(),
        entered: entered.clone(),
        release: release.clone(),
    });
    let spawn = |key: Vec<u8>| {
        let provisioner = fixture.provisioner.clone();
        let client = fixture.client.clone();
        tokio::spawn(async move { provisioner.ensure(&client, ACCOUNT, &key).await })
    };
    let first_job = spawn(first);
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let second_job = spawn(second);
    let blocked = tokio::time::timeout(std::time::Duration::from_millis(150), async {
        entered.acquire().await.unwrap().forget();
    })
    .await
    .is_err();
    first_job.abort();
    assert!(first_job.await.unwrap_err().is_cancelled());
    if blocked {
        tokio::time::timeout(std::time::Duration::from_secs(2), entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
    }
    release.add_permits(2);
    tokio::time::timeout(std::time::Duration::from_secs(10), second_job)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        blocked,
        "a second cold claim bypassed the pending last-slot reservation"
    );
    let authority = CellAuthority::new(fixture.layout.clone());
    assert!(
        authority
            .load(first_target.cell_id())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        authority
            .load(second_target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .root
            .is_some()
    );
    assert!(fixture.node.runtime().stats().active_cells() <= 5);
    fixture.shutdown().await;
}

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
