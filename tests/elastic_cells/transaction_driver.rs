use crate::{local_node, transaction_fixture};
use bdb_cellar_runtime::{
    Error, MutationIdentity,
    client::{CellClient, Invocation, InvocationAdmission, InvocationError, InvocationTransport},
    identity::RequestId,
    registry::Query,
};
use beyonddb::*;
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use extenddb_core::types::AttributeValue;
use http_body_util::Full;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU8, Ordering},
    },
};
fn identity(_: u8) -> MutationIdentity {
    local_node::wall_mutation()
}
#[test]
fn driver_resumes_prepares_and_resolves_commit_condition_and_lock_failures() {
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
    let directory = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(identity(1).issued_at_ms));
    let (node, client) = local_node::start(directory.path(), clock, host.handle(), core.handle()).await.unwrap();
    let fixture = transaction_fixture::table(&client, "Driver").await;
    let table = fixture.table;
    let storage = fixture.storage;
    let mut participants = fixture.participants;
    let account_id = "123456789012";
    let mut completed = Vec::new();
    // Operation indexes deliberately oppose participant order.
    for (position, (_, participant)) in participants.iter_mut().enumerate() {
        participant.operations[0].index = u8::try_from(1 - position).unwrap();
    }
    for scenario in [180_u8, 181, 182, 183, 184, 185, 186, 187, 188] {
        let transaction_id = [scenario; 16];
        let coordinator = coordinator_target(account_id, &transaction_id).unwrap();
        let mut request: Vec<_> = participants
            .iter()
            .map(|(_, participant)| participant.clone())
            .collect();
        if !matches!(scenario, 180 | 181 | 185 | 186 | 188) {
            for participant in &mut request {
                let TransactionOperation::Put(input) = &mut participant.operations[0].operation
                else {
                    unreachable!()
                };
                input
                    .item
                    .insert("value".into(), AttributeValue::N("2".into()));
            }
        }
        if scenario == 188 {
            for participant in &mut request {
                let TransactionOperation::Put(input) = &mut participant.operations[0].operation
                else {
                    unreachable!()
                };
                // JSON escaping makes one valid item span coordinator chunks.
                input
                    .item
                    .insert("padding".into(), AttributeValue::S("\0".repeat(140_000)));
            }
        }
        if scenario == 182 {
            let TransactionOperation::Put(input) = &mut request[1].operations[0].operation else {
                unreachable!()
            };
            input.condition = Some(serde_json::from_value(serde_json::json!({
                "expression": {"Function": {"name": "attribute_not_exists", "args": [{"Path": [{"Attribute": "id"}]}]}},
                "maps": {"names": {}, "values": {}}
            })).unwrap());
        }
        if scenario == 184 {
            let CoordinatorParticipantTarget::Data { epoch, .. } = &mut request[1].target else {
                unreachable!()
            };
            *epoch = 2;
        }
        transaction_command!(
            client,
            BeginCrossCellTransaction,
            &coordinator,
            identity(scenario),
            Json(BeginCrossCellTransactionInput {
                account_id: account_id.into(),
                transaction_id,
                token: None,
                participants: request.clone(),
            }),
        )
        .await
        .unwrap();
        let snapshot = client
            .query::<ReadCoordinatorResume>(
                &coordinator,
                None,
                Json(ReadCrossCellTransactionInput {
                    account_id: account_id.into(),
                    transaction_id,
                    routing_key: transaction_id.to_vec(),
                }),
            )
            .await
            .unwrap()
            .output
            .0;
        assert_eq!(
            snapshot.status.unwrap().decision,
            CoordinatorDecision::Begin
        );
        if scenario == 188 {
            assert!(
                snapshot.participants.is_none(),
                "large escaped payloads must retain chunked retrieval"
            );
        } else {
            assert_eq!(
                snapshot
                    .participants
                    .unwrap()
                    .into_iter()
                    .map(|p| p.participant)
                    .collect::<Vec<_>>(),
                request
            );
        }
        if matches!(scenario, 180 | 181 | 183) {
            // Cover recorded and lost prepare receipts, plus a conflicting
            // transaction on the second participant.
            let position = usize::from(scenario == 183);
            let held_id = if scenario == 183 {
                [190; 16]
            } else {
                transaction_id
            };
            let prepared = transaction_command!(
                client,
                PreparePartitionTransaction,
                &participants[position].0,
                identity(if scenario == 180 { 177 } else { scenario }),
                Json(PreparePartitionTransactionInput {
                    table_id: table.id.clone(),
                    epoch: 1,
                    transaction_id: held_id,
                    coordinator_cell: *coordinator.cell_id().as_bytes(),
                    coordinator_key: transaction_id.to_vec(),
                    operations: request[position]
                        .operations
                        .iter()
                        .map(|operation| operation.operation.clone())
                        .collect(),
                }),
            )
            .await
            .unwrap();
            if scenario == 180 {
                client
                    .command::<RecordParticipantPrepare>(
                        &coordinator,
                        identity(179),
                        Json(CoordinatorPhaseInput {
                            account_id: account_id.into(),
                            transaction_id,
                            routing_key: transaction_id.to_vec(),
                            position: 0,
                            participant_cell: *participants[0].0.cell_id().as_bytes(),
                            sequence: prepared.receipt.commit_sequence,
                        }),
                    )
                    .await
                    .unwrap();
                let partial = client
                    .query::<ReadCoordinatorResume>(
                        &coordinator,
                        None,
                        Json(ReadCrossCellTransactionInput {
                            account_id: account_id.into(),
                            transaction_id,
                            routing_key: transaction_id.to_vec(),
                        }),
                    )
                    .await
                    .unwrap()
                    .output
                    .0;
                assert_eq!(partial.status.unwrap().prepared_count, 1);
                let remaining = partial.participants.unwrap();
                assert_eq!(remaining.len(), 1);
                assert_eq!(remaining[0].position, 1);
                assert_eq!(remaining[0].participant, request[1]);
                super::transaction_commit::assert_atomic_prepared_commit(&client, &participants[1], transaction_id).await;
            }
        }
        let decision = if scenario == 181 {
            let (first, second) = tokio::join!(
                storage.resume_cross_cell_transaction(account_id, &transaction_id, transaction_id),
                storage.resume_cross_cell_transaction(account_id, &transaction_id, transaction_id),
            );
            assert_eq!(first.as_ref().unwrap(), &CoordinatorDecision::Commit);
            assert_eq!(second.as_ref().unwrap(), &CoordinatorDecision::Commit);
            first.unwrap()
        } else if scenario >= 185 {
            let lost = Arc::new(AtomicU8::new(0));
            let remote_client = if scenario == 185 {
                client.map_transport(|inner| Arc::new(super::public_transactions::DropPhaseReplies {inner, lost: lost.clone(), enabled: 3}))
            } else if scenario == 188 {
                client.map_transport(|inner| Arc::new(FinishDuringPayload {inner, client: client.clone(), transaction_id, seen: lost.clone()}))
            } else {
                client.with_admission(Arc::new(RefusePhase {command: 28, persistent: scenario == 187,
                    winner: (scenario == 186).then(|| (client.clone(), transaction_id)), refused: lost.clone()}))
            };
            let remote = CellStorage::new(remote_client, "local");
            let decision = remote
                .resume_cross_cell_transaction(account_id, &transaction_id, transaction_id)
                .await
                .unwrap();
            if scenario == 188 {
                assert!(lost.load(Ordering::SeqCst) >= 2);
            } else {
                assert_eq!(
                    lost.load(Ordering::SeqCst),
                    if scenario == 185 { 3 } else { 1 },
                    "scenario={scenario}"
                );
            }
            decision
        } else {
            storage
                .resume_cross_cell_transaction(account_id, &transaction_id, transaction_id)
                .await
                .unwrap()
        };
        match scenario {
            180 | 181 | 185 | 186 | 188 => assert_eq!(decision, CoordinatorDecision::Commit),
            187 => assert_eq!(
                decision,
                CoordinatorDecision::Abort {
                    index: Some(1),
                    reason: Some(TransactionFailure::Throttled)
                }
            ),
            182 => assert!(
                matches!(&decision, CoordinatorDecision::Abort { index: Some(0), reason: Some(TransactionFailure::ConditionFailed(Some(item))) } if item["value"] == AttributeValue::N("1".into()))
            ),
            _ => assert_eq!(
                decision,
                CoordinatorDecision::Abort {
                    index: Some(0),
                    reason: Some(TransactionFailure::Conflict)
                }
            ),
        }
        assert_eq!(
            storage
                .resume_cross_cell_transaction(account_id, &transaction_id, transaction_id)
                .await
                .unwrap(),
            decision
        );
        let status = client
            .query::<ReadCrossCellTransaction>(
                &coordinator,
                None,
                Json(ReadCrossCellTransactionInput {
                    account_id: account_id.into(),
                    transaction_id,
                    routing_key: transaction_id.to_vec(),
                }),
            )
            .await
            .unwrap();
        let status = status.output.0.unwrap();
        assert_eq!(status.resolved_count, 2);
        completed.push(transaction_id);
        for position in 0..2 {
            assert!(client.query::<ReadCoordinatorParticipant>(&coordinator, None, Json(ReadCoordinatorParticipantInput {
                account_id: account_id.into(), transaction_id, routing_key: transaction_id.to_vec(), position, chunk: 0,
            })).await.unwrap().output.is_none());
        }
        for (position, (target, _)) in participants.iter().enumerate() {
            let outcome = transaction_command!(
                client,
                PreparePartitionTransaction,
                target,
                MutationIdentity {
                    request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
                    ..identity(188)
                },
                Json(PreparePartitionTransactionInput {
                    table_id: table.id.clone(),
                    epoch: 1,
                    transaction_id,
                    coordinator_cell: *coordinator.cell_id().as_bytes(),
                    coordinator_key: transaction_id.to_vec(),
                    operations: request[position]
                        .operations
                        .iter()
                        .map(|op| op.operation.clone())
                        .collect(),
                }),
            )
            .await;
            let result = match outcome {
                Ok(value) => value.output.0,
                Err(InvocationError::Rejected(value)) => value.output.0,
                Err(error) => panic!("{error:?}"),
            };
            assert_eq!(
                result,
                if decision == CoordinatorDecision::Commit {
                    PrepareTransactionOutcome::Committed
                } else {
                    PrepareTransactionOutcome::Aborted
                }
            );
        }
        if scenario == 183 {
            client
                .command::<ResolvePartitionTransaction>(
                    &participants[1].0,
                    identity(190),
                    Json(ResolveTransactionInput {
                        transaction_id: [190; 16],
                        coordinator_cell: *coordinator.cell_id().as_bytes(),
                        commit: false,
                    }),
                )
                .await
                .unwrap();
        }
        for (position, (target, participant)) in participants.iter().enumerate() {
            let expected = if scenario == 188 {
                &request[position]
            } else {
                participant
            };
            let TransactionOperation::Put(input) = &expected.operations[0].operation else {
                unreachable!()
            };
            let key = extenddb_core::types::extract_key(&input.item, &table.key_schema);
            assert_eq!(
                client
                    .query::<PartitionGet>(
                        target,
                        None,
                        Json(PartitionGetInput {
                            table_id: table.id.clone(),
                            epoch: 1,
                            key,
                        })
                    )
                    .await
                    .unwrap()
                    .output
                    .0,
                PartitionGetOutcome::Found(Some(input.item.clone()))
            );
            if scenario == 188 {
                let TransactionOperation::Put(original) = &participant.operations[0].operation
                else {
                    unreachable!()
                };
                client
                    .command::<PartitionPut>(
                        target,
                        MutationIdentity {
                            request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
                            ..identity(188)
                        },
                        Json(PartitionPutInput {
                            table_id: table.id.clone(),
                            epoch: 1,
                            item: original.item.clone(),
                            condition: None,
                        }),
                    )
                    .await
                    .unwrap();
            }
        }

    }
    super::transaction_write_skew::assert_condition_checks_prevent_write_skew(
        &client,
        &storage,
        &table,
        &participants,
    )
    .await;
    super::transaction_recovery::assert_background_recovery(&client, &table, &participants).await;
    tokio::time::timeout(std::time::Duration::from_secs(5), local_node::stop(node)).await.unwrap().unwrap();
    crate::offline_sqlite::assert_coordinator_write_payloads_collected(directory.path(), &completed);
    });
}

struct RefusePhase {
    command: u32,
    persistent: bool,
    winner: Option<(CellClient, [u8; 16])>,
    refused: Arc<AtomicU8>,
}
impl InvocationAdmission for RefusePhase {
    fn check<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
    ) -> Pin<Box<dyn Future<Output = bdb_cellar_runtime::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if invocation.mutation.is_some()
                && invocation.operation == self.command
                && (self.persistent || self.refused.swap(1, Ordering::SeqCst) == 0)
            {
                self.refused.store(1, Ordering::SeqCst);
                if let Some((client, transaction_id)) = &self.winner {
                    assert_eq!(
                        CellStorage::new(client.clone(), "local")
                            .resume_cross_cell_transaction(
                                "123456789012",
                                transaction_id,
                                *transaction_id
                            )
                            .await
                            .unwrap(),
                        CoordinatorDecision::Commit
                    );
                }
                // This attempt has not entered ingress. The other driver may
                // already have decided; that decision still takes precedence.
                return Err(Error::Capacity("injected before SDK submission"));
            }
            Ok(())
        })
    }
}
struct FinishDuringPayload {
    inner: Arc<dyn InvocationTransport>,
    client: CellClient,
    transaction_id: [u8; 16],
    seen: Arc<AtomicU8>,
}
impl InvocationTransport for FinishDuringPayload {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            let coordinator = coordinator_target("123456789012", &self.transaction_id).unwrap();
            let chunk = invocation.target == coordinator
                && invocation.mutation.is_none()
                && invocation.operation == ReadCoordinatorParticipant::ID;
            if chunk && self.seen.fetch_add(1, Ordering::SeqCst) == 1 {
                assert_eq!(
                    CellStorage::new(self.client.clone(), "local")
                        .resume_cross_cell_transaction(
                            "123456789012",
                            &self.transaction_id,
                            self.transaction_id
                        )
                        .await
                        .unwrap(),
                    CoordinatorDecision::Commit
                );
            }
            self.inner.call(invocation, request).await
        })
    }
}
