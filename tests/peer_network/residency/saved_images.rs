//! Admission and immutable semantics of saved transaction images.

use super::*;
use beyonddb::{
    GetItemInput, Json, PreparePartitionTransactionBounded, PreparePartitionTransactionInput,
    PutItemInput, ReadTransactionInput, ResolvePartitionTransaction, ResolveTransactionInput,
    TransactionOperation,
};
use cellule_runtime::{MutationIdentity, identity::RequestId};

const ACCOUNT: &str = "123456789012";

fn identity() -> MutationIdentity {
    let issued_at_ms = now_ms();
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms,
        expires_at_ms: issued_at_ms + 60_000,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eight_saved_image_reads_fit_the_participant_mailbox() {
    saved_image_mailbox_pressure(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wide_saved_image_replies_reproduce_mailbox_pressure() {
    saved_image_mailbox_pressure(false).await;
}

async fn saved_image_mailbox_pressure(bounded: bool) {
    use super::forward_cache::{CountedAuthority, CreationGate};
    use beyonddb::{
        BoundedTransactionReadResult, GetItemInput, ReadPartitionTransactionResult,
        ReadPartitionTransactionResultBounded, ReadTransactionResultInput, TransactionReadResult,
    };
    let store = Arc::new(CountedAuthority::default());
    let fixture = Fixture::with_store_capacity_and_peer_cache(1, store.clone(), 8, true).await;
    let remote = super::provisioning::Remote::new(&fixture).await;
    let client = remote.client_with_cache(&fixture, true);
    let handle = &fixture.data[0].0;
    let table_id = super::provisioning::table_id(&fixture, "Residency").await;
    let range = super::cell_models::ranges(&fixture, "Residency")
        .await
        .remove(0);
    let target = beyonddb::data_target(ACCOUNT, &table_id, &range.partition_id).unwrap();
    let coordinator_cell = *account_target(ACCOUNT).unwrap().cell_id().as_bytes();
    let AwsAttributeValue::S(key) = &fixture.data[0].1["id"] else {
        panic!("string key");
    };
    let expected = Item::from([
        ("id".into(), AttributeValue::S(key.clone())),
        ("value".into(), AttributeValue::S("committed".into())),
    ]);
    client
        .command::<PreparePartitionTransactionBounded>(
            &target,
            identity(),
            Json(PreparePartitionTransactionInput {
                table_id: table_id.clone(),
                epoch: range.epoch,
                transaction_id: [220; 16],
                coordinator_cell,
                coordinator_key: vec![220; 16],
                operations: vec![TransactionOperation::Read(GetItemInput {
                    table_name: "Residency".into(),
                    table_id: table_id.clone(),
                    key: Item::from([("id".into(), AttributeValue::S(key.clone()))]),
                })],
            }),
        )
        .await
        .unwrap();
    client
        .command::<ResolvePartitionTransaction>(
            &target,
            identity(),
            Json(ResolveTransactionInput {
                transaction_id: [220; 16],
                coordinator_cell,
                commit: true,
            }),
        )
        .await
        .unwrap();
    let input = Json(ReadTransactionResultInput {
        transaction: ReadTransactionInput {
            transaction_id: [220; 16],
            coordinator_cell,
        },
        position: 0,
    });
    // Hold a preceding real publication: FIFO snapshot queries must retain
    // their reply reservations until it completes, just as under slow storage.
    let gate = CreationGate {
        paths: std::collections::HashSet::from([fixture
            .layout
            .control_path(handle.cell_id().as_bytes())]),
        entered: Arc::new(tokio::sync::Semaphore::new(0)),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    *store.publication_gate.lock().unwrap() = Some(gate.clone());
    let blocking_client = client.clone();
    let blocking_target = target.clone();
    let blocked = tokio::spawn(async move {
        blocking_client
            .command::<PreparePartitionTransactionBounded>(
                &blocking_target,
                identity(),
                Json(PreparePartitionTransactionInput {
                    table_id: table_id.clone(),
                    epoch: range.epoch,
                    transaction_id: [221; 16],
                    coordinator_cell,
                    coordinator_key: vec![221; 16],
                    operations: vec![TransactionOperation::Put(PutItemInput {
                        table_name: "Residency".into(),
                        table_id,
                        item: Item::from([(
                            "id".into(),
                            AttributeValue::S("held-image-publication".into()),
                        )]),
                        condition: None,
                    })],
                }),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let before = fixture.node.runtime().stats().retained_bytes();
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let client = client.clone();
        let target = target.clone();
        let input = input.clone();
        tasks.push(tokio::spawn(async move {
            if bounded {
                client
                    .query::<ReadPartitionTransactionResultBounded>(&target, None, input)
                    .await
                    .map(|result| result.output)
                    .map_err(|error| format!("{error:?}"))
            } else {
                client
                    .query::<ReadPartitionTransactionResult>(&target, None, input)
                    .await
                    .map(|result| match result.output.0 {
                        TransactionReadResult::Unavailable => {
                            BoundedTransactionReadResult::Unavailable
                        }
                        TransactionReadResult::Item(item) => {
                            BoundedTransactionReadResult::Item(item)
                        }
                    })
                    .map_err(|error| format!("{error:?}"))
            }
        }));
    }
    let observed = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let admitted = if bounded {
                // Each held request reserves a 64 KiB reply and a 64 KiB
                // peer buffer. With these tiny inputs, eight peer buffers and
                // only seven reply reservations stay below this threshold.
                fixture.node.runtime().stats().retained_bytes() >= before + 8 * 128 * 1024
                    && tasks.iter().all(|task| !task.is_finished())
            } else {
                tasks.iter().filter(|task| task.is_finished()).count() >= 5
            };
            if admitted {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    // Wait for refusals to become visible after the first wide reservation.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let retained = fixture.node.runtime().stats().retained_bytes();
    *store.publication_gate.lock().unwrap() = None;
    gate.release.add_permits(64);
    blocked.await.unwrap().unwrap();
    let mut accepted = 0;
    let mut refused = Vec::new();
    for task in tasks {
        match tokio::time::timeout(std::time::Duration::from_secs(30), task)
            .await
            .unwrap()
            .unwrap()
        {
            Ok(result) => {
                assert_eq!(
                    result,
                    BoundedTransactionReadResult::Item(Some(expected.clone()))
                );
                accepted += 1;
            }
            Err(error) => refused.push(error),
        }
    }
    client
        .command::<ResolvePartitionTransaction>(
            &target,
            identity(),
            Json(ResolveTransactionInput {
                transaction_id: [221; 16],
                coordinator_cell,
                commit: false,
            }),
        )
        .await
        .unwrap();
    remote.shutdown().await;
    fixture.shutdown().await;
    println!(
        "held saved-image queries: bounded={bounded}, admitted={accepted}, observed={observed}, retained={before}->{retained}, refused={refused:?}"
    );
    assert!(observed);
    assert_eq!(
        accepted,
        if bounded { 8 } else { 3 },
        "snapshot reply reservations refused: {refused:?}"
    );
    if bounded {
        assert!(retained - before < 8 * 140 * 1024);
        assert!(refused.is_empty());
    } else {
        assert!(retained - before >= 12 * 1024 * 1024);
        assert_eq!(refused.len(), 5);
    }
}

// Immutable-image and signed wide-fallback contracts run in cellar_images.
