use super::forward_cache::CountedAuthority;
use super::*;
use beyonddb::{
    CoordinatorProvisioner, ReadCoordinatorRegistration, RegisterCoordinatorShardInput,
};
use cellule_runtime::cell::catalog::CellCatalog;
use std::sync::atomic::Ordering;

const ACCOUNT: &str = "123456789012";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sdk_token_replay_serializes_one_cold_coordinator_and_survives_restore() {
    let store = Arc::new(CountedAuthority::default());
    let fixture = Fixture::with_store_capacity_and_peer_cache(1, store.clone(), 8, true).await;
    let token = "same-cold-coordinator";
    let target = beyonddb::coordinator_target(ACCOUNT, token.as_bytes()).unwrap();
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    *store.creation_gate.lock().unwrap() = Some(super::forward_cache::CreationGate {
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
                        .item("id", AwsAttributeValue::S("same-cold".into()))
                        .item("value", AwsAttributeValue::N("7".into()))
                        .build()
                        .unwrap(),
                )
                .build(),
        );
    let first = tokio::spawn(request.clone().send());
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let second = tokio::spawn(request.clone().send());
    let serialized = tokio::time::timeout(std::time::Duration::from_millis(150), async {
        entered.acquire().await.unwrap().forget();
    })
    .await
    .is_err();
    release.add_permits(2);
    for job in [first, second] {
        tokio::time::timeout(std::time::Duration::from_secs(10), job)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    assert!(
        serialized,
        "the same coordinator was initialized concurrently"
    );
    let before = CellAuthority::new(fixture.layout.clone())
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .incarnation;
    fixture
        .provisioner
        .admit_coordinator(ACCOUNT, token.as_bytes())
        .await
        .unwrap()
        .drain()
        .await
        .unwrap();
    request.send().await.unwrap();
    let after = CellAuthority::new(fixture.layout.clone())
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .incarnation;
    assert_eq!(
        before, after,
        "restoration must retain the original coordinator generation"
    );
    let item = fixture
        .sdk
        .get_item()
        .table_name("Residency")
        .key("id", AwsAttributeValue::S("same-cold".into()))
        .consistent_read(true)
        .send()
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(item["value"], AwsAttributeValue::N("7".into()));
    fixture.shutdown().await;
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_cold_sdk_coordinators_overlap_without_losing_durable_registration() {
    let store = Arc::new(CountedAuthority::default());
    let fixture = Fixture::with_store_capacity_and_peer_cache(1, store.clone(), 16, true).await;
    let first = b"cold-parallel-0".to_vec();
    let first_target = beyonddb::coordinator_target(ACCOUNT, &first).unwrap();
    let (second, second_target) = (1..100)
        .map(|index| {
            let key = format!("cold-parallel-{index}").into_bytes();
            let target = beyonddb::coordinator_target(ACCOUNT, &key).unwrap();
            (key, target)
        })
        .find(|(_, target)| {
            target.cell_id().as_bytes()[0] % 64 != first_target.cell_id().as_bytes()[0] % 64
        })
        .unwrap();
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
    let mut jobs = Vec::new();
    for (index, token) in [first.clone(), second.clone()].into_iter().enumerate() {
        let sdk = fixture.sdk.clone();
        jobs.push(tokio::spawn(async move {
            sdk.transact_write_items()
                .client_request_token(String::from_utf8(token).unwrap())
                .transact_items(
                    aws_sdk_dynamodb::types::TransactWriteItem::builder()
                        .put(
                            aws_sdk_dynamodb::types::Put::builder()
                                .table_name("Residency")
                                .item("id", AwsAttributeValue::S(format!("cold-{index}")))
                                .item("value", AwsAttributeValue::N(index.to_string()))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .send()
                .await
        }));
    }
    let overlap = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        entered.acquire_many(2).await.unwrap().forget();
    })
    .await;
    release.add_permits(2);
    for job in jobs {
        tokio::time::timeout(std::time::Duration::from_secs(10), job)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    assert!(
        overlap.is_ok(),
        "independent cold SDK coordinator creates were serialized"
    );
    for (key, target) in [(&first, first_target), (&second, second_target)] {
        let registered = fixture
            .client
            .query::<ReadCoordinatorRegistration>(
                &account_target(ACCOUNT).unwrap(),
                None,
                Json(RegisterCoordinatorShardInput {
                    account_id: ACCOUNT.into(),
                    shard: u32::from_be_bytes(target.partition().try_into().unwrap()),
                }),
            )
            .await
            .unwrap();
        assert!(registered.output.0);
        let control = CellAuthority::new(fixture.layout.clone())
            .load(target.cell_id())
            .await
            .unwrap()
            .unwrap();
        assert!(control.value().root.is_some());
        fixture
            .provisioner
            .ensure(&fixture.client, ACCOUNT, key)
            .await
            .unwrap();
    }
    fixture.data[0].0.drain().await.unwrap();
    for index in 0..2 {
        let item = fixture
            .sdk
            .get_item()
            .table_name("Residency")
            .key("id", AwsAttributeValue::S(format!("cold-{index}")))
            .consistent_read(true)
            .send()
            .await
            .unwrap();
        assert_eq!(
            item.item.unwrap()["value"],
            AwsAttributeValue::N(index.to_string())
        );
    }
    fixture.shutdown().await;
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_coordinator_authority_is_not_read_twice_before_creation_cas() {
    let store = Arc::new(CountedAuthority::default());
    let fixture = Fixture::with_store_capacity_and_peer_cache(1, store.clone(), 8, true).await;
    let key = b"fresh-admission-absence";
    let coordinator = beyonddb::coordinator_target(ACCOUNT, key).unwrap();
    *store.path.lock().unwrap() = Some(
        fixture
            .layout
            .control_path(coordinator.cell_id().as_bytes()),
    );
    store.reads.store(0, Ordering::SeqCst);
    fixture
        .provisioner
        .ensure(&fixture.client, ACCOUNT, key)
        .await
        .unwrap();
    let reads = store.reads.load(Ordering::SeqCst);
    println!("first coordinator admission authority reads={reads}");
    assert_eq!(
        reads, 1,
        "the conditional creation must revalidate observed absence without another GET"
    );
    let account = account_target(ACCOUNT).unwrap();
    let input = RegisterCoordinatorShardInput {
        account_id: ACCOUNT.into(),
        shard: u32::from_be_bytes(coordinator.partition().try_into().unwrap()),
    };
    assert!(
        fixture
            .client
            .query::<ReadCoordinatorRegistration>(&account, None, Json(input))
            .await
            .unwrap()
            .output
            .0
    );
    *store.path.lock().unwrap() = None;
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_coordinator_observation_never_overwrites_a_competing_owner() {
    let store = Arc::new(CountedAuthority::default());
    let fixture = Fixture::with_store_capacity_and_peer_cache(1, store.clone(), 8, true).await;
    let remote = super::provisioning::Remote::new(&fixture).await;
    let key = b"fresh-admission-race";
    let coordinator = beyonddb::coordinator_target(ACCOUNT, key).unwrap();
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    *store.creation_gate.lock().unwrap() = Some(super::forward_cache::CreationGate {
        paths: [fixture
            .layout
            .control_path(coordinator.cell_id().as_bytes())]
        .into_iter()
        .collect(),
        entered: entered.clone(),
        release: release.clone(),
    });
    let local = {
        let provisioner = fixture.provisioner.clone();
        let client = fixture.client.clone();
        tokio::spawn(async move { provisioner.ensure(&client, ACCOUNT, key).await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    // The local request has observed absence and is waiting at creation CAS.
    // Let a real peer create and publish the same Cell before releasing it.
    *store.creation_gate.lock().unwrap() = None;
    let foreign = remote
        .provisioner
        .recover_owned_coordinator(ACCOUNT, key, &fixture.directory)
        .await
        .unwrap();
    release.add_permits(1);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(10), local)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    let assert_foreign_owner = || async {
        let current = CellAuthority::new(fixture.layout.clone())
            .load(coordinator.cell_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.value().incarnation, foreign.incarnation());
        assert_eq!(
            current.value().owner.as_ref().unwrap().session,
            remote.session
        );
        assert!(current.value().root.is_some());
    };
    assert_foreign_owner().await;
    // A retry discovers the published peer root and can register it locally.
    fixture
        .provisioner
        .ensure(&fixture.client, ACCOUNT, key)
        .await
        .unwrap();
    assert_foreign_owner().await;
    let account = account_target(ACCOUNT).unwrap();
    assert!(
        fixture
            .client
            .query::<ReadCoordinatorRegistration>(
                &account,
                None,
                Json(RegisterCoordinatorShardInput {
                    account_id: ACCOUNT.into(),
                    shard: u32::from_be_bytes(coordinator.partition().try_into().unwrap()),
                }),
            )
            .await
            .unwrap()
            .output
            .0
    );
    remote.shutdown().await;
    fixture.shutdown().await;
}

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
