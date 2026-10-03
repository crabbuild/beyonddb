use super::*;
use beyonddb::{
    CoordinatorProvisioner, ReadCoordinatorRegistration, RegisterCoordinatorShardInput,
    RegisterCoordinatorShards, RegisterCoordinatorShardsInput,
};
use cellule_runtime::codec::{BoundedEncoder, WireValue};

const ACCOUNT: &str = "123456789012";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_during_registration_publication_keeps_discovery_and_waits_for_ack() {
    let store = Arc::new(super::forward_cache::CountedAuthority::default());
    // Use canonical routing for the post-drain queries. A direct cached Cell
    // client can return CellDraining until its short-lived handle expires.
    let fixture = Fixture::with_store_capacity_and_peer_cache(1, store.clone(), 12, false).await;
    let account = account_target(ACCOUNT).unwrap();
    let first_key = b"canceled-registration";
    let first_target = beyonddb::coordinator_target(ACCOUNT, first_key).unwrap();
    let second_key = (0..100)
        .map(|index| format!("healthy-registration-{index}").into_bytes())
        .find(|key| beyonddb::coordinator_target(ACCOUNT, key).unwrap() != first_target)
        .unwrap();
    for key in [first_key.as_slice(), second_key.as_slice()] {
        fixture
            .provisioner
            .recover_owned_coordinator(ACCOUNT, key, &fixture.directory)
            .await
            .unwrap();
    }
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    *store.publication_gate.lock().unwrap() = Some(super::forward_cache::CreationGate {
        paths: [fixture.layout.control_path(account.cell_id().as_bytes())]
            .into_iter()
            .collect(),
        entered: entered.clone(),
        release: release.clone(),
    });
    let spawn = |key: Vec<u8>| {
        let provisioner = fixture.provisioner.clone();
        let client = fixture.client.clone();
        tokio::spawn(async move { provisioner.ensure(&client, ACCOUNT, &key).await })
    };
    let first = spawn(first_key.to_vec());
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(
        !first.is_finished(),
        "admission cannot acknowledge an unpublished registration"
    );
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let second = spawn(second_key.clone());
    *store.publication_gate.lock().unwrap() = None;
    release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(10), second)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    fixture
        .provisioner
        .admit_account(ACCOUNT)
        .await
        .unwrap()
        .drain()
        .await
        .unwrap();
    fixture.provisioner.admit_account(ACCOUNT).await.unwrap();
    for key in [first_key.as_slice(), second_key.as_slice()] {
        let target = beyonddb::coordinator_target(ACCOUNT, key).unwrap();
        assert!(
            fixture
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
                .0
        );
    }
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_coordinator_admissions_share_durable_account_registration() {
    let fixture = Fixture::with_capacity(1, 16).await;
    let account = account_target(ACCOUNT).unwrap();
    let mut cells = std::collections::HashSet::new();
    let keys: Vec<_> = (0..100)
        .map(|index| format!("registration-batch-{index}").into_bytes())
        .filter(|key| {
            cells.insert(
                beyonddb::coordinator_target(ACCOUNT, key)
                    .unwrap()
                    .cell_id(),
            )
        })
        .take(4)
        .collect();
    // Give admission published but undiscoverable shards. This isolates the
    // account registration cost from independent coordinator bootstrap I/O.
    for key in &keys {
        fixture
            .provisioner
            .recover_owned_coordinator(ACCOUNT, key, &fixture.directory)
            .await
            .unwrap();
    }
    let sequence = || async {
        CellAuthority::new(fixture.layout.clone())
            .load(account.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .root
            .as_ref()
            .unwrap()
            .commit_sequence
    };
    let before = sequence().await;
    let barrier = Arc::new(tokio::sync::Barrier::new(keys.len()));
    let mut jobs = Vec::new();
    for key in keys.clone() {
        let provisioner = fixture.provisioner.clone();
        let client = fixture.client.clone();
        let barrier = barrier.clone();
        jobs.push(tokio::spawn(async move {
            barrier.wait().await;
            provisioner.ensure(&client, ACCOUNT, &key).await
        }));
    }
    for job in jobs {
        tokio::time::timeout(std::time::Duration::from_secs(10), job)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    let commits = sequence().await - before;
    println!("four concurrent coordinator registrations account commits={commits}");
    assert!(
        commits > 0 && commits < keys.len() as u64,
        "concurrent admissions should publish fewer registration commands than requests"
    );
    fixture
        .provisioner
        .admit_account(ACCOUNT)
        .await
        .unwrap()
        .drain()
        .await
        .unwrap();
    fixture.provisioner.admit_account(ACCOUNT).await.unwrap();
    for key in &keys {
        let target = beyonddb::coordinator_target(ACCOUNT, key).unwrap();
        assert!(
            fixture
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
                .0,
            "acknowledged registration must survive owner restoration"
        );
    }
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinator_registration_batch_enforces_scope_and_limits_without_partial_rows() {
    let fixture = Fixture::with_capacity(1, 12).await;
    let account = account_target(ACCOUNT).unwrap();
    for (account_id, shards) in [
        (ACCOUNT, vec![]),
        (ACCOUNT, (128..193).collect()),
        (ACCOUNT, vec![1234, 4096]),
        ("other-account", vec![1234]),
    ] {
        assert!(
            fixture
                .client
                .command::<RegisterCoordinatorShards>(
                    &account,
                    mutation(),
                    Json(RegisterCoordinatorShardsInput {
                        account_id: account_id.into(),
                        shards,
                    }),
                )
                .await
                .is_err()
        );
    }
    assert!(
        !fixture
            .client
            .query::<ReadCoordinatorRegistration>(
                &account,
                None,
                Json(RegisterCoordinatorShardInput {
                    account_id: ACCOUNT.into(),
                    shard: 1234,
                }),
            )
            .await
            .unwrap()
            .output
            .0
    );

    // Exercise the largest supported account encoding and batch on real Cells.
    let maximal_account = "\0".repeat(128);
    let input = RegisterCoordinatorShardsInput {
        account_id: maximal_account.clone(),
        shards: vec![4095; 64],
    };
    let mut encoder = BoundedEncoder::new(4096).unwrap();
    Json(input.clone()).encode(&mut encoder).unwrap();
    fixture
        .provisioner
        .admit_account(&maximal_account)
        .await
        .unwrap();
    let target = account_target(&maximal_account).unwrap();
    fixture
        .client
        .command::<RegisterCoordinatorShards>(&target, mutation(), Json(input))
        .await
        .unwrap();
    assert!(
        fixture
            .client
            .query::<ReadCoordinatorRegistration>(
                &target,
                None,
                Json(RegisterCoordinatorShardInput {
                    account_id: maximal_account,
                    shard: 4095,
                }),
            )
            .await
            .unwrap()
            .output
            .0
    );
    fixture.shutdown().await;
}

fn mutation() -> cellule_runtime::MutationIdentity {
    let now = now_ms();
    cellule_runtime::MutationIdentity {
        request_id: cellule_runtime::identity::RequestId::from_bytes(
            *uuid::Uuid::now_v7().as_bytes(),
        ),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    }
}
