use crate::local_node;
use bdb_cellar_runtime::{MutationIdentity, client::InvocationError, identity::RequestId};
use beyonddb::*;
use extenddb_core::types::{AttributeValue, Item};
use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};
fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

const ACCOUNT: &str = "123456789012";
const TOKEN: &str = "retained-token";

fn request() -> BeginCrossCellTransactionInput {
    let account = account_target(ACCOUNT).unwrap();
    let mut table_bytes = [9; 32];
    table_bytes[..16].copy_from_slice(account.tenant().as_bytes());
    let table_id = blake3::Hash::from_bytes(table_bytes).to_hex().to_string();
    let mut participants: Vec<_> = [1_u8, 2]
        .into_iter()
        .map(|id| CoordinatorParticipant {
            target: CoordinatorParticipantTarget::Data {
                table_id: table_id.clone(),
                partition_id: [id; 16],
                epoch: 1,
            },
            operations: vec![IndexedTransactionOperation {
                index: id - 1,
                operation: TransactionOperation::Put(PutItemInput {
                    table_name: "Tokens".into(),
                    table_id: table_id.clone(),
                    item: Item::from([("id".into(), AttributeValue::S(id.to_string()))]),
                    condition: None,
                }),
            }],
        })
        .collect();
    participants.sort_by_key(|participant| {
        let CoordinatorParticipantTarget::Data { partition_id, .. } = &participant.target else {
            unreachable!()
        };
        *data_target(ACCOUNT, &table_id, partition_id)
            .unwrap()
            .cell_id()
            .as_bytes()
    });
    BeginCrossCellTransactionInput {
        account_id: ACCOUNT.into(),
        transaction_id: [10; 16],
        token: Some(TransactionToken {
            account_id: ACCOUNT.into(),
            token: TOKEN.into(),
            fingerprint: "original".into(),
        }),
        participants,
    }
}

async fn assert_payload(
    client: &bdb_cellar_runtime::client::CellClient,
    coordinator: &bdb_cellar_runtime::identity::CellTarget,
    original: &BeginCrossCellTransactionInput,
    position: usize,
) {
    let payload = client
        .query::<ReadCoordinatorParticipant>(
            coordinator,
            None,
            Json(ReadCoordinatorParticipantInput {
                chunk: 0,
                account_id: ACCOUNT.into(),
                transaction_id: original.transaction_id,
                routing_key: TOKEN.as_bytes().to_vec(),
                position: position as u8,
            }),
        )
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(payload.target, original.participants[position].target);
    assert_eq!(
        serde_json::from_slice::<Vec<IndexedTransactionOperation>>(&payload.payload).unwrap(),
        original.participants[position].operations
    );
}

#[test]
fn tokens_pin_unfinished_work_and_replay_original_routes_until_completion_window_expires() {
    let host_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let core_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    host_runtime.block_on(async {
    // All states are produced by the actual coordinator commands. Only the
    // application clock advances; no live SQLite files are edited or seeded.
    let cases = [
        (0, CoordinatorDecision::Begin, false),
        (1, CoordinatorDecision::Commit, false),
        (2, CoordinatorDecision::Commit, true),
    ];
    for (state, original_decision, expired) in cases {
        let coordinator = coordinator_target(ACCOUNT, TOKEN.as_bytes()).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let clock = Arc::new(AtomicI64::new(now_ms()));
        let identity = |byte| MutationIdentity {
            request_id: RequestId::from_bytes([byte; 16]),
            ..local_node::mutation(&clock)
        };
        let (node, client) = local_node::start(
            directory.path(), clock.clone(), host_runtime.handle(), core_runtime.handle(),
        ).await.unwrap();
        let original = request();
        assert_eq!(transaction_command!(client, BeginCrossCellTransaction, &coordinator,
            identity(200), Json(original.clone())).await.unwrap().output.0,
            BeginCrossCellTransactionOutcome::Begun);
        for position in 0..2 {
            assert_payload(&client, &coordinator, &original, position).await;
        }
        if state != 0 {
            for (position, participant) in original.participants.iter().enumerate() {
                let CoordinatorParticipantTarget::Data { table_id, partition_id, .. } = &participant.target else { unreachable!() };
                client.command::<RecordParticipantPrepare>(&coordinator, identity(201 + position as u8), Json(CoordinatorPhaseInput {
                    account_id: ACCOUNT.into(), transaction_id: original.transaction_id,
                    routing_key: TOKEN.as_bytes().to_vec(), position: position as u8,
                    participant_cell: *data_target(ACCOUNT, table_id, partition_id).unwrap().cell_id().as_bytes(), sequence: 1,
                })).await.unwrap();
            }
            client.command::<DecideCrossCellTransaction>(&coordinator, identity(203), Json(DecideCrossCellTransactionInput {
                account_id: ACCOUNT.into(), transaction_id: original.transaction_id,
                routing_key: TOKEN.as_bytes().to_vec(), decision: CoordinatorDecision::Commit,
            })).await.unwrap();
            for (position, participant) in original.participants.iter().take(state).enumerate() {
                let CoordinatorParticipantTarget::Data { table_id, partition_id, .. } = &participant.target else { unreachable!() };
                client.command::<RecordParticipantResolution>(&coordinator, identity(204 + position as u8), Json(CoordinatorPhaseInput {
                    account_id: ACCOUNT.into(), transaction_id: original.transaction_id,
                    routing_key: TOKEN.as_bytes().to_vec(), position: position as u8,
                    participant_cell: *data_target(ACCOUNT, table_id, partition_id).unwrap().cell_id().as_bytes(), sequence: 2,
                })).await.unwrap();
            }
        }
        clock.fetch_add(10 * 60 * 1_000 + 1, Ordering::SeqCst);
        let token = original.token.clone().unwrap();
        let read = |token| client.query::<ReadCoordinatorToken>(&coordinator, None, Json(token));
        let expected = if expired {
            ReadCoordinatorTokenOutcome::Missing
        } else {
            ReadCoordinatorTokenOutcome::Found {
                transaction_id: original.transaction_id,
                decision: original_decision.clone(),
            }
        };
        assert_eq!(read(token.clone()).await.unwrap().output.0, expected);
        let changed_token = TransactionToken {
            fingerprint: "changed".into(),
            ..token.clone()
        };
        assert_eq!(
            read(changed_token.clone()).await.unwrap().output.0,
            if expired {
                ReadCoordinatorTokenOutcome::Missing
            } else {
                ReadCoordinatorTokenOutcome::Mismatch
            }
        );
        let mut retry = original.clone();
        retry.transaction_id = [20; 16];
        for participant in &mut retry.participants {
            let CoordinatorParticipantTarget::Data { epoch, .. } = &mut participant.target else {
                unreachable!()
            };
            *epoch = 2;
        }
        if expired {
            retry.token.as_mut().unwrap().fingerprint = "replacement".into();
            let TransactionOperation::Put(input) =
                &mut retry.participants[0].operations[0].operation
            else {
                unreachable!()
            };
            input
                .item
                .insert("replacement".into(), AttributeValue::Bool(true));
        }
        let active_token = retry.token.clone().unwrap();
        let begun = transaction_command!(
            client,
            BeginCrossCellTransaction,
            &coordinator,
            identity(211),
            Json(retry.clone())
        )
        .await
        .unwrap()
        .output
        .0;
        assert_eq!(
            begun,
            if expired {
                BeginCrossCellTransactionOutcome::Begun
            } else {
                BeginCrossCellTransactionOutcome::Existing {
                    transaction_id: original.transaction_id,
                    decision: original_decision.clone(),
                }
            }
        );
        for position in state..2 {
            assert_payload(&client, &coordinator, &original, position).await;
        }
        // Resolution deliberately collects write payloads. The old SQL seed
        // bypassed that command; verify its retained decision/identity contract
        // through delayed phase replies instead of requiring unreachable rows.
        for position in 0..state {
            assert!(client.query::<ReadCoordinatorParticipant>(&coordinator, None, Json(ReadCoordinatorParticipantInput {
                chunk: 0, account_id: ACCOUNT.into(), transaction_id: original.transaction_id,
                routing_key: TOKEN.as_bytes().to_vec(), position: position as u8,
            })).await.unwrap().output.is_none());
            let mut phase = CoordinatorPhaseInput {
                account_id: ACCOUNT.into(), transaction_id: original.transaction_id,
                routing_key: TOKEN.as_bytes().to_vec(), position: position as u8,
                participant_cell: [99; 32], sequence: 2,
            };
            assert!(matches!(client.command::<RecordParticipantResolution>(&coordinator, identity(206 + position as u8), Json(phase.clone())).await,
                Err(InvocationError::Rejected(result)) if result.output.0 == CoordinatorPhaseOutcome::WrongParticipant));
            let CoordinatorParticipantTarget::Data {table_id, partition_id, ..} = &original.participants[position].target else { unreachable!() };
            phase.participant_cell = *data_target(ACCOUNT, table_id, partition_id).unwrap().cell_id().as_bytes();
            assert_eq!(client.command::<RecordParticipantResolution>(&coordinator, identity(208 + position as u8), Json(phase)).await.unwrap().output.0, CoordinatorPhaseOutcome::Replay);
        }
        let status = client.query::<ReadCrossCellTransaction>(&coordinator, None, Json(ReadCrossCellTransactionInput {
            account_id: ACCOUNT.into(), transaction_id: original.transaction_id, routing_key: TOKEN.as_bytes().to_vec(),
        })).await.unwrap().output.0.unwrap();
        assert_eq!(status.decision, original_decision);
        assert_eq!(usize::from(status.resolved_count), state);
        assert_eq!(status.participant_count, 2);
        retry.token = Some(changed_token);
        assert!(
            matches!(transaction_command!(client, BeginCrossCellTransaction,&coordinator, identity(212), Json(retry)).await, Err(InvocationError::Rejected(result)) if result.output.0 == BeginCrossCellTransactionOutcome::Mismatch)
        );
        if !expired {
            if original_decision == CoordinatorDecision::Begin {
                client
                    .command::<DecideCrossCellTransaction>(
                        &coordinator,
                        identity(213),
                        Json(DecideCrossCellTransactionInput {
                            account_id: ACCOUNT.into(),
                            transaction_id: original.transaction_id,
                            routing_key: TOKEN.as_bytes().to_vec(),
                            decision: CoordinatorDecision::Abort {
                                index: None,
                                reason: None,
                            },
                        }),
                    )
                    .await
                    .unwrap();
            }
            for (position, participant) in original.participants.iter().enumerate() {
                let CoordinatorParticipantTarget::Data {
                    table_id,
                    partition_id,
                    ..
                } = &participant.target
                else {
                    unreachable!()
                };
                let phase = CoordinatorPhaseInput {
                    account_id: ACCOUNT.into(),
                    transaction_id: original.transaction_id,
                    routing_key: TOKEN.as_bytes().to_vec(),
                    position: position as u8,
                    participant_cell: *data_target(ACCOUNT, table_id, partition_id)
                        .unwrap()
                        .cell_id()
                        .as_bytes(),
                    sequence: 3,
                };
                client
                    .command::<RecordParticipantResolution>(
                        &coordinator,
                        identity(214 + position as u8),
                        Json(phase.clone()),
                    )
                    .await
                    .unwrap();
                client
                    .command::<RecordParticipantResolution>(
                        &coordinator,
                        identity(216 + position as u8),
                        Json(phase),
                    )
                    .await
                    .unwrap();
            }
            // Successful completion starts replay; fully resolved aborts release
            // the token slot like ExtendDB's rolled-back SQLite transaction.
            let outcome = read(token).await.unwrap().output.0;
            if original_decision == CoordinatorDecision::Begin {
                assert_eq!(outcome, ReadCoordinatorTokenOutcome::Missing);
            } else {
                assert!(matches!(
                    outcome,
                    ReadCoordinatorTokenOutcome::Found {
                        transaction_id: [10, ..],
                        ..
                    }
                ));
            }
        } else {
            assert_eq!(
                read(active_token).await.unwrap().output.0,
                ReadCoordinatorTokenOutcome::Found {
                    transaction_id: [20; 16],
                    decision: CoordinatorDecision::Begin
                }
            );
        }
        local_node::stop(node).await.unwrap();
    }
    });
}
