//! Native recovery refuses startup while healthy terminal work progresses.
use crate::{local_node, transaction_fixture};
use bdb_cellar_runtime::{
    MutationIdentity,
    client::{CellClient, Invocation, InvocationAdmission, InvocationTransport},
    identity::CellTarget,
    registry::Command,
};
use beyonddb::*;
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use extenddb_core::types::AttributeValue;
use extenddb_storage::error::StorageError;
use http_body_util::Full;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
const ACCOUNT: &str = "123456789012";
struct ParticipantAdmission {
    target: CellTarget,
    refuse: Arc<AtomicBool>,
    attempts: Arc<AtomicUsize>,
}
impl InvocationAdmission for ParticipantAdmission {
    fn check<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
    ) -> Pin<Box<dyn Future<Output = bdb_cellar_runtime::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if invocation.target == self.target && self.refuse.load(Ordering::SeqCst) {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                return Err(bdb_cellar_runtime::Error::Capacity(
                    "participant admission withheld",
                ));
            }
            Ok(())
        })
    }
}
struct ParticipantTransport {
    inner: Arc<dyn InvocationTransport>,
    target: CellTarget,
    calls: Arc<AtomicUsize>,
}
impl InvocationTransport for ParticipantTransport {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            if invocation.target == self.target {
                self.calls.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.call(invocation, request).await
        })
    }
}
#[test]
fn startup_resolves_healthy_participants_but_keeps_readiness_failed() {
    run(true);
}
#[test]
fn serving_resolves_decisions_but_does_not_prepare_past_failed_admission() {
    run(false);
}
fn run(startup: bool) {
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    host.block_on(async {
        for decision in [
            CoordinatorDecision::Begin,
            CoordinatorDecision::Commit,
            CoordinatorDecision::Abort {
                index: None,
                reason: None,
            },
        ] {
            recovery_case(startup, decision, host.handle(), core.handle()).await;
        }
    });
}
async fn recovery_case(
    startup: bool,
    decision: CoordinatorDecision,
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
) {
    let files = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(mutation().issued_at_ms));
    let (node, client) = local_node::start(files.path(), clock.clone(), host, core)
        .await
        .unwrap();
    let fixture = transaction_fixture::table(&client, "Admission").await;
    let mut participants = fixture.participants;
    for (index, (_, participant)) in participants.iter_mut().enumerate() {
        participant.operations[0].index = index as u8;
        let TransactionOperation::Put(input) = &mut participant.operations[0].operation else {
            unreachable!()
        };
        input
            .item
            .insert("value".into(), AttributeValue::N("2".into()));
    }
    let provisioner =
        CellInitialPartitionProvisioner::new(client.clone(), local_node::application().unwrap());
    let id = [249; 16];
    let coordinator = coordinator_target(ACCOUNT, &id).unwrap();
    provisioner.ensure(&client, ACCOUNT, &id).await.unwrap();
    prepare(
        &client,
        &coordinator,
        id,
        &participants,
        &decision,
        if decision == CoordinatorDecision::Begin {
            1
        } else {
            2
        },
    )
    .await;
    // Later records in the same shard and in a later shard must progress. The
    // extra keys preserve the healthy participant's original ordered interval.
    let later_id = (1_u128..100_000)
        .map(u128::to_be_bytes)
        .find(|id| coordinator_target(ACCOUNT, id).unwrap() == coordinator)
        .unwrap();
    let other_id = (100_000_u128..200_000)
        .map(u128::to_be_bytes)
        .find(|id| coordinator_target(ACCOUNT, id).unwrap().partition() > coordinator.partition())
        .unwrap();
    let other = coordinator_target(ACCOUNT, &other_id).unwrap();
    provisioner
        .ensure(&client, ACCOUNT, &other_id)
        .await
        .unwrap();
    for (id, target, suffix) in [
        (later_id, &coordinator, "-later"),
        (other_id, &other, "-other"),
    ] {
        let mut later = participants[1].clone();
        later.1.operations[0].index = 0;
        let TransactionOperation::Put(input) = &mut later.1.operations[0].operation else {
            unreachable!()
        };
        let AttributeValue::S(key) = input.item.get_mut("id").unwrap() else {
            unreachable!()
        };
        key.push_str(suffix);
        prepare(&client, target, id, &[later], &decision, 1).await;
    }
    let (node, client) = if startup {
        local_node::stop(node).await.unwrap();
        local_node::start(files.path(), clock, host, core)
            .await
            .unwrap()
    } else {
        (node, client)
    };
    let refuse = Arc::new(AtomicBool::new(true));
    let attempts = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let controlled = client
        .map_transport(|inner| {
            Arc::new(ParticipantTransport {
                inner,
                target: participants[0].0.clone(),
                calls: calls.clone(),
            })
        })
        .with_admission(Arc::new(ParticipantAdmission {
            target: participants[0].0.clone(),
            refuse: refuse.clone(),
            attempts: attempts.clone(),
        }));
    let mut recovery = TransactionRecovery::new(
        Arc::new(CellStorage::new(controlled, "local")),
        ACCOUNT.into(),
    )
    .unwrap();
    let cancellation = CancellationToken::new();
    let worker = if startup {
        assert!(
            matches!(
                recovery.initial_pass().await,
                Err(StorageError::LimitExceeded(_))
            ),
            "startup must report known participant admission failure"
        );
        None
    } else {
        let stop = cancellation.clone();
        Some(tokio::spawn(async move {
            let mut ticks = tokio::time::interval(Duration::from_millis(10));
            loop {
                tokio::select! {
                    () = stop.cancelled() => return,
                    _ = ticks.tick() => {
                        tokio::select! { () = stop.cancelled() => return, _ = recovery.step() => {} }
                    }
                }
            }
        }))
    };
    wait_resolved(&client, later_id, 1).await;
    wait_resolved(&client, other_id, 1).await;
    assert!(attempts.load(Ordering::SeqCst) > 0);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "refusal precedes SDK ingress"
    );
    assert!(
        node.is_ready().await.unwrap(),
        "SDK authority is still ready"
    );
    let terminal = decision != CoordinatorDecision::Begin;
    let pending = status(&client, id).await;
    assert_eq!(
        (pending.decision, pending.resolved_count),
        (decision.clone(), u8::from(terminal)),
        "startup={startup}, decision={decision:?}"
    );
    for (position, (target, _)) in participants.iter().enumerate() {
        let state = client
            .query::<ReadPartitionTransaction>(
                target,
                None,
                Json(ReadTransactionInput {
                    transaction_id: id,
                    coordinator_cell: *coordinator.cell_id().as_bytes(),
                }),
            )
            .await
            .unwrap()
            .output
            .0;
        let expected = if position == 0 {
            ParticipantTransactionState::Prepared
        } else if !terminal {
            ParticipantTransactionState::Missing
        } else if decision == CoordinatorDecision::Commit {
            ParticipantTransactionState::Committed
        } else {
            ParticipantTransactionState::Aborted
        };
        assert_eq!(
            state, expected,
            "healthy terminal work must progress independently"
        );
    }
    let (healthy_target, healthy) = &participants[1];
    let TransactionOperation::Put(input) = &healthy.operations[0].operation else {
        unreachable!()
    };
    let CoordinatorParticipantTarget::Data { epoch, .. } = healthy.target else {
        unreachable!()
    };
    let mut newer = input.item.clone();
    newer.insert("value".into(), AttributeValue::N("3".into()));
    if terminal {
        let mut expected = input.item.clone();
        if decision != CoordinatorDecision::Commit {
            expected.insert("value".into(), AttributeValue::N("1".into()));
        }
        assert_eq!(
            client
                .query::<PartitionGet>(
                    healthy_target,
                    None,
                    Json(PartitionGetInput {
                        table_id: input.table_id.clone(),
                        epoch,
                        key: extenddb_core::types::extract_key(
                            &input.item,
                            &fixture.table.key_schema,
                        ),
                    }),
                )
                .await
                .unwrap()
                .output
                .0,
            PartitionGetOutcome::Found(Some(expected)),
            "healthy resolution must apply only the published decision"
        );
        client
            .command::<PartitionPut>(
                healthy_target,
                mutation(),
                Json(PartitionPutInput {
                    table_id: input.table_id.clone(),
                    epoch,
                    item: newer.clone(),
                    condition: None,
                }),
            )
            .await
            .unwrap();
    }
    refuse.store(false, Ordering::SeqCst);
    if startup {
        TransactionRecovery::new(
            Arc::new(CellStorage::new(client.clone(), "local")),
            ACCOUNT.into(),
        )
        .unwrap()
        .initial_pass()
        .await
        .unwrap();
    }
    wait_resolved(&client, id, 2).await;
    let completed = status(&client, id).await;
    let expected = if terminal {
        decision
    } else {
        CoordinatorDecision::Commit
    };
    assert_eq!(
        (completed.decision, completed.resolved_count),
        (expected, 2)
    );
    if terminal {
        assert_eq!(
            client
                .query::<PartitionGet>(
                    healthy_target,
                    None,
                    Json(PartitionGetInput {
                        table_id: input.table_id.clone(),
                        epoch,
                        key: extenddb_core::types::extract_key(
                            &input.item,
                            &fixture.table.key_schema
                        )
                    })
                )
                .await
                .unwrap()
                .output
                .0,
            PartitionGetOutcome::Found(Some(newer)),
            "recovery must preserve later healthy writes"
        );
    }
    cancellation.cancel();
    if let Some(worker) = worker {
        worker.await.unwrap();
    }
    local_node::stop(node).await.unwrap();
}
fn mutation() -> MutationIdentity {
    local_node::wall_mutation()
}

struct AdmitLater {
    inner: Arc<dyn InvocationTransport>,
    client: CellClient,
    coordinator: CellTarget,
    id: [u8; 16],
    participants: Vec<(CellTarget, CoordinatorParticipant)>,
    clock: Arc<AtomicI64>,
    admitted: Arc<AtomicBool>,
}
impl InvocationTransport for AdmitLater {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            if invocation.target == self.coordinator
                && invocation.operation == AbortExpiredTransaction::ID
                && !self.admitted.swap(true, Ordering::SeqCst)
            {
                // The pass has already captured this shard's pending boundary.
                self.clock.fetch_add(1, Ordering::SeqCst);
                prepare(
                    &self.client,
                    &self.coordinator,
                    self.id,
                    &self.participants,
                    &CoordinatorDecision::Begin,
                    0,
                )
                .await;
            }
            self.inner.call(invocation, request).await
        })
    }
}

#[test]
fn initial_pass_stops_at_observed_boundary_and_later_work_remains_discoverable() {
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    host.block_on(async {
        let files = tempfile::tempdir().unwrap();
        let clock = Arc::new(AtomicI64::new(mutation().issued_at_ms));
        let (node, client) =
            local_node::start(files.path(), clock.clone(), host.handle(), core.handle())
                .await
                .unwrap();
        let fixture = transaction_fixture::table(&client, "Boundary").await;
        let id = [249; 16];
        let coordinator = coordinator_target(ACCOUNT, &id).unwrap();
        CellInitialPartitionProvisioner::new(client.clone(), local_node::application().unwrap())
            .ensure(&client, ACCOUNT, &id)
            .await
            .unwrap();
        prepare(
            &client,
            &coordinator,
            id,
            &fixture.participants,
            &CoordinatorDecision::Begin,
            0,
        )
        .await;
        let later_id = (1_u128..100_000)
            .map(u128::to_be_bytes)
            .find(|id| coordinator_target(ACCOUNT, id).unwrap() == coordinator)
            .unwrap();
        let admitted = Arc::new(AtomicBool::new(false));
        let observed = client.map_transport(|inner| {
            Arc::new(AdmitLater {
                inner,
                client: client.clone(),
                coordinator: coordinator.clone(),
                id: later_id,
                participants: fixture.participants.clone(),
                clock,
                admitted: admitted.clone(),
            })
        });
        let mut recovery = TransactionRecovery::new(
            Arc::new(CellStorage::new(observed, "local")),
            ACCOUNT.into(),
        )
        .unwrap();
        recovery.initial_pass().await.unwrap();
        assert!(admitted.load(Ordering::SeqCst));
        assert_eq!(status(&client, id).await.resolved_count, 2);
        assert_eq!(
            status(&client, later_id).await.decision,
            CoordinatorDecision::Begin
        );
        for (target, _) in &fixture.participants {
            assert_eq!(
                client
                    .query::<ReadPartitionTransaction>(
                        target,
                        None,
                        Json(ReadTransactionInput {
                            transaction_id: later_id,
                            coordinator_cell: *coordinator.cell_id().as_bytes(),
                        })
                    )
                    .await
                    .unwrap()
                    .output
                    .0,
                ParticipantTransactionState::Missing
            );
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while status(&client, later_id).await.resolved_count != 2 {
                recovery.step().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(
            status(&client, later_id).await.decision,
            CoordinatorDecision::Commit
        );
        local_node::stop(node).await.unwrap();
    });
}

async fn wait_resolved(client: &CellClient, id: [u8; 16], count: u8) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if status(client, id).await.resolved_count == count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
}

async fn prepare(
    client: &CellClient,
    coordinator: &CellTarget,
    id: [u8; 16],
    participants: &[(CellTarget, CoordinatorParticipant)],
    decision: &CoordinatorDecision,
    prepare_count: usize,
) {
    transaction_command!(
        client,
        BeginCrossCellTransaction,
        coordinator,
        mutation(),
        Json(BeginCrossCellTransactionInput {
            account_id: ACCOUNT.into(),
            transaction_id: id,
            token: None,
            participants: participants
                .iter()
                .map(|(_, participant)| participant.clone())
                .collect(),
        }),
    )
    .await
    .unwrap();
    for (position, (target, participant)) in participants.iter().take(prepare_count).enumerate() {
        let CoordinatorParticipantTarget::Data {
            table_id, epoch, ..
        } = &participant.target
        else {
            unreachable!()
        };
        let result = transaction_command!(
            client,
            PreparePartitionTransaction,
            target,
            mutation(),
            Json(PreparePartitionTransactionInput {
                table_id: table_id.clone(),
                epoch: *epoch,
                transaction_id: id,
                coordinator_cell: *coordinator.cell_id().as_bytes(),
                coordinator_key: id.to_vec(),
                operations: participant
                    .operations
                    .iter()
                    .map(|op| op.operation.clone())
                    .collect(),
            }),
        )
        .await
        .unwrap();
        client
            .command::<RecordParticipantPrepare>(
                coordinator,
                mutation(),
                Json(CoordinatorPhaseInput {
                    account_id: ACCOUNT.into(),
                    transaction_id: id,
                    routing_key: id.to_vec(),
                    position: position as u8,
                    participant_cell: *target.cell_id().as_bytes(),
                    sequence: result.receipt.commit_sequence,
                }),
            )
            .await
            .unwrap();
    }
    if *decision != CoordinatorDecision::Begin {
        client
            .command::<DecideCrossCellTransaction>(
                coordinator,
                mutation(),
                Json(DecideCrossCellTransactionInput {
                    account_id: ACCOUNT.into(),
                    transaction_id: id,
                    routing_key: id.to_vec(),
                    decision: decision.clone(),
                }),
            )
            .await
            .unwrap();
    }
}

async fn status(client: &CellClient, id: [u8; 16]) -> beyonddb::CrossCellTransactionStatus {
    client
        .query::<ReadCrossCellTransaction>(
            &coordinator_target(ACCOUNT, &id).unwrap(),
            None,
            Json(ReadCrossCellTransactionInput {
                account_id: ACCOUNT.into(),
                transaction_id: id,
                routing_key: id.to_vec(),
            }),
        )
        .await
        .unwrap()
        .output
        .0
        .unwrap()
}
