use super::forward_cache::CountedAuthority;
use super::*;
use beyonddb::{
    CoordinatorProvisioner, ReadCoordinatorRegistration, RegisterCoordinatorShardInput,
};
use cellule_runtime::cell::catalog::CellCatalog;
use std::sync::atomic::Ordering;

const ACCOUNT: &str = "123456789012";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_resident_coordinator_admission_skips_provider_io_but_restores_after_drain() {
    let store = Arc::new(CountedAuthority::default());
    let fixture = Fixture::with_store_capacity_and_peer_cache(1, store.clone(), 8, true).await;
    let key = b"resident-admission";
    let coordinator = beyonddb::coordinator_target(ACCOUNT, key).unwrap();
    let handle = fixture
        .provisioner
        .admit_coordinator(ACCOUNT, key)
        .await
        .unwrap();
    *store.path.lock().unwrap() = Some(
        fixture
            .layout
            .control_path(coordinator.cell_id().as_bytes()),
    );
    store.reads.store(0, Ordering::SeqCst);
    store.all_reads.store(0, Ordering::SeqCst);
    let start = std::time::Instant::now();
    for _ in 0..4 {
        fixture
            .provisioner
            .ensure(&fixture.client, ACCOUNT, key)
            .await
            .unwrap();
    }
    let reads = store.reads.load(Ordering::SeqCst);
    println!(
        "four registered resident admissions: {:?}; coordinator authority reads={reads}",
        start.elapsed()
    );
    assert_eq!(
        reads, 0,
        "a registered resident shard should not need provider reads on every admission"
    );
    assert_eq!(
        store.all_reads.load(Ordering::SeqCst),
        0,
        "resident admission must not read any provider object"
    );
    // A new provisioner has no registration receipt, even on the same runtime.
    let fresh = CellInitialPartitionProvisioner::new(
        fixture.node.runtime(),
        fixture.application.clone(),
        fixture.layout.clone(),
        fixture.session,
        fixture.endpoint.clone(),
        fixture._files.path().join("fresh-admission"),
    )
    .unwrap();
    store.reads.store(0, Ordering::SeqCst);
    fresh.ensure(&fixture.client, ACCOUNT, key).await.unwrap();
    assert!(
        store.reads.load(Ordering::SeqCst) > 0,
        "a new cache must establish registration from durable state"
    );
    // Losing account residency invalidates the original admission shortcut.
    fixture
        .provisioner
        .admit_account(ACCOUNT)
        .await
        .unwrap()
        .drain()
        .await
        .unwrap();
    store.reads.store(0, Ordering::SeqCst);
    fixture
        .provisioner
        .ensure(&fixture.client, ACCOUNT, key)
        .await
        .unwrap();
    assert!(
        store.reads.load(Ordering::SeqCst) > 0,
        "a drained account must re-enter canonical admission"
    );
    handle.drain().await.unwrap();
    store.reads.store(0, Ordering::SeqCst);
    fixture
        .provisioner
        .ensure(&fixture.client, ACCOUNT, key)
        .await
        .unwrap();
    assert!(
        store.reads.load(Ordering::SeqCst) > 0,
        "a drained coordinator must re-enter canonical admission"
    );
    let proof = CellCatalog::new(fixture.layout.clone(), coordinator.tenant())
        .lookup(coordinator.cell_id())
        .await
        .unwrap()
        .unwrap();
    let control = CellAuthority::new(fixture.layout.clone())
        .load(coordinator.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert!(
        fixture
            .node
            .runtime()
            .local_handle(proof, &control)
            .await
            .unwrap()
            .is_some()
    );
    *store.path.lock().unwrap() = None;
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resident_coordinator_without_acknowledged_registration_is_not_cached() {
    let fixture = Fixture::with_capacity(1, 8).await;
    let key = b"unregistered-resident";
    let coordinator = beyonddb::coordinator_target(ACCOUNT, key).unwrap();
    fixture
        .provisioner
        .recover_owned_coordinator(ACCOUNT, key, &fixture.directory)
        .await
        .unwrap();
    let input = RegisterCoordinatorShardInput {
        account_id: ACCOUNT.into(),
        shard: u32::from_be_bytes(coordinator.partition().try_into().unwrap()),
    };
    let account = account_target(ACCOUNT).unwrap();
    assert!(
        !fixture
            .client
            .query::<ReadCoordinatorRegistration>(&account, None, Json(input.clone()))
            .await
            .unwrap()
            .output
            .0
    );
    fixture
        .provisioner
        .ensure(&fixture.client, ACCOUNT, key)
        .await
        .unwrap();
    assert!(
        fixture
            .client
            .query::<ReadCoordinatorRegistration>(&account, None, Json(input))
            .await
            .unwrap()
            .output
            .0
    );
    fixture.shutdown().await;
}
