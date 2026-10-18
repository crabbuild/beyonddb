use super::*;
use beyonddb::{
    ParticipantTransactionState, PreparePartitionTransaction, PreparePartitionTransactionBounded,
    PreparePartitionTransactionInput, PrepareTransactionOutcome, PutItemInput, ReadPartitionState,
    ReadPartitionTransaction, ReadTransactionInput, ResolvePartitionTransaction,
    ResolveTransactionInput, TransactionCommandInput, TransactionOperation,
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

// Wide failure-image/token recovery runs in cellar_images.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eight_in_flight_prepares_fit_the_participant_mailbox() {
    // Preserve the old wide path as a control: its 4 MiB reply envelope admits
    // only three held requests. The bounded path must admit all eight.
    for bounded in [false, true] {
        held_prepares(bounded).await;
    }
}

async fn held_prepares(bounded: bool) {
    use super::forward_cache::{CountedAuthority, CreationGate};

    let store = Arc::new(CountedAuthority::default());
    let fixture = Fixture::with_store_capacity_and_peer_cache(1, store.clone(), 8, true).await;
    let remote = super::provisioning::Remote::new(&fixture).await;
    let client = remote.client_with_cache(&fixture, true);
    let handle = &fixture.data[0].0;
    let entry = handle.catalog().entry();
    let account = account_target(ACCOUNT).unwrap();
    let target = cellule_runtime::identity::CellTarget::new(
        account.tenant(),
        beyonddb::APPLICATION_ID,
        entry.namespace(),
        entry.partition(),
    )
    .unwrap();
    let spec = client
        .query::<ReadPartitionState>(&target, None, Json(()))
        .await
        .unwrap()
        .output
        .0
        .unwrap()
        .spec;
    let coordinator_cell = *account.cell_id().as_bytes();
    let gate = CreationGate {
        paths: std::collections::HashSet::from([fixture
            .layout
            .control_path(handle.cell_id().as_bytes())]),
        entered: Arc::new(tokio::sync::Semaphore::new(0)),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    *store.publication_gate.lock().unwrap() = Some(gate.clone());
    let before_retained = fixture.node.runtime().stats().retained_bytes();
    let mut tasks = Vec::new();
    for index in 0..8_u8 {
        let client = client.clone();
        let target = target.clone();
        let transaction_id = [170 + index; 16];
        let input = PreparePartitionTransactionInput {
            table_id: spec.table.id.clone(),
            epoch: spec.epoch,
            transaction_id,
            coordinator_cell,
            coordinator_key: transaction_id.to_vec(),
            operations: vec![TransactionOperation::Put(PutItemInput {
                table_name: "Residency".into(),
                table_id: spec.table.id.clone(),
                item: Item::from([
                    (
                        "id".into(),
                        AttributeValue::S(format!("prepare-pressure-{index}")),
                    ),
                    ("value".into(), AttributeValue::S("staged".into())),
                ]),
                condition: None,
            })],
        };
        tasks.push(tokio::spawn(async move {
            (
                transaction_id,
                if bounded {
                    client
                        .command::<PreparePartitionTransactionBounded>(
                            &target,
                            identity(),
                            Json(input),
                        )
                        .await
                } else {
                    client
                        .command::<PreparePartitionTransaction>(
                            &target,
                            identity(),
                            Json(TransactionCommandInput::Inline(input)),
                        )
                        .await
                },
            )
        }));
        if index == 0 {
            // Hold the first real command's publication, and thus its mailbox
            // reservation, while the remaining requests reach the owner.
            tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
        }
    }
    // Prove simultaneous admission before releasing publication. The node
    // ledger includes each actual request's reserved reply bytes; a small
    // bounded prepare batch cannot reach this threshold with fewer than eight.
    let all_held = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let ready = if bounded {
                fixture.node.runtime().stats().retained_bytes() >= before_retained + 8 * 64 * 1024
            } else {
                tasks.iter().filter(|task| task.is_finished()).count() >= 5
            };
            if ready {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let held_retained = fixture.node.runtime().stats().retained_bytes();
    *store.publication_gate.lock().unwrap() = None;
    gate.release.add_permits(64);
    let mut accepted = Vec::new();
    let mut refused = Vec::new();
    for task in tasks {
        let (transaction_id, result) =
            tokio::time::timeout(std::time::Duration::from_secs(30), task)
                .await
                .unwrap()
                .unwrap();
        match result {
            Ok(result) => {
                assert_eq!(result.output.0, PrepareTransactionOutcome::Prepared);
                accepted.push(transaction_id);
            }
            Err(error) => {
                assert!(
                    matches!(
                        error,
                        cellule_runtime::client::InvocationError::NotStarted(
                            cellule_runtime::Error::Capacity(_)
                        )
                    ),
                    "unexpected prepare failure: {error:?}"
                );
                refused.push(format!("{error:?}"));
            }
        }
    }
    // Check durable prepares after owner transfer, then abort every accepted
    // intent before asserting the concurrency result or shutting down.
    handle.drain().await.unwrap();
    remote
        .provisioner
        .admit_existing_partition(ACCOUNT, &spec.table.id, &spec.partition_id)
        .await
        .unwrap();
    for transaction_id in &accepted {
        let observed = client
            .query::<ReadPartitionTransaction>(
                &target,
                None,
                Json(ReadTransactionInput {
                    transaction_id: *transaction_id,
                    coordinator_cell,
                }),
            )
            .await
            .unwrap();
        assert_eq!(observed.output.0, ParticipantTransactionState::Prepared);
        client
            .command::<ResolvePartitionTransaction>(
                &target,
                identity(),
                Json(ResolveTransactionInput {
                    transaction_id: *transaction_id,
                    coordinator_cell,
                    commit: false,
                }),
            )
            .await
            .unwrap();
    }
    remote.shutdown().await;
    fixture.shutdown().await;
    println!(
        "eight held prepares bounded={bounded}: accepted={}, held={all_held}, retained={before_retained}->{held_retained}, refused={refused:?}",
        accepted.len()
    );
    assert!(
        all_held,
        "requests did not reach admission while publication was held"
    );
    assert_eq!(
        accepted.len(),
        if bounded { 8 } else { 3 },
        "prepare reply reservations refused: {refused:?}"
    );
}
