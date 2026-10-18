use crate::{local_node, transaction_fixture};
use beyonddb::*;
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use cellar_runtime::{
    MutationIdentity,
    client::{Invocation, InvocationTransport},
    codec::{BoundedDecoder, WireValue},
    identity::{CellTarget, RequestId},
    registry::Command,
};
use extenddb_core::{
    expression::ExpressionMaps,
    types::{AttributeValue, Item},
};
use extenddb_storage::{DataEngine, TableEngine, error::StorageError};
use http_body_util::{BodyExt, Full};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU8, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Notify;
fn mutation() -> MutationIdentity {
    let now = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    }
}
#[test]
fn terminal_resolution_progresses_past_unavailable_participants_and_receipts() {
    run(2, false);
}
#[test]
fn terminal_resolution_releases_healthy_cells_while_first_rpc_is_pending() {
    run(6, true);
}
fn run(participant_count: usize, hold_first: bool) {
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
    host.block_on(resolution_progress(
        host.handle(),
        core.handle(),
        participant_count,
        hold_first,
    ));
}
async fn resolution_progress(
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
    participant_count: usize,
    hold_first: bool,
) {
    let account_id = "123456789012";
    let account = account_target(account_id).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicI64::new(mutation().issued_at_ms));
    let (node, client) = local_node::start(directory.path(), clock, host, core)
        .await
        .unwrap();
    let provisioner = Arc::new(
        CellInitialPartitionProvisioner::new(client.clone(), local_node::application().unwrap())
            .with_initial_partition_count(2)
            .unwrap(),
    );
    let local = CellStorage::new(client.clone(), "local");
    let routed =
        CellStorage::new(client.clone(), "local").with_initial_partitions(provisioner.clone());
    let mut participants = Vec::new();
    for index in 0..participant_count {
        let name = format!("Items{index}");
        if index == 0 {
            client
                .command::<CreateTable>(
                    &account,
                    mutation(),
                    Json(transaction_fixture::account_spec(&name)),
                )
                .await
                .unwrap();
        } else {
            routed.create_table(account_id, serde_json::from_value(serde_json::json!({
                "TableName": name, "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}], "BillingMode": "PAY_PER_REQUEST"
            })).unwrap()).await.unwrap();
        }
        let info = local.table_key_info(account_id, &name).await.unwrap();
        let (target, participant) = if index == 0 {
            (account.clone(), CoordinatorParticipantTarget::Account)
        } else {
            let RoutePageOutcome::Page { partitions, .. } = read_route_page(
                &client,
                &account,
                RoutePageInput {
                    table_id: info.table_id.clone(),
                    start_key: None,
                    after_lower: None,
                    expected_epoch: None,
                },
            )
            .await
            .unwrap() else {
                panic!("missing table routes");
            };
            assert_eq!(partitions.len(), 2);
            // Every case key has the ASCII resolution- prefix, in this interval.
            let key = Item::from([("id".into(), AttributeValue::S("resolution-0".into()))]);
            let encoded = ordered_data_key(&info.table_id, &key, &info.key_schema).unwrap();
            let spec = partitions
                .iter()
                .find(|spec| {
                    spec.lower <= encoded
                        && spec.upper.as_ref().is_none_or(|upper| &encoded < upper)
                })
                .unwrap();
            (
                data_target(account_id, &info.table_id, &spec.partition_id).unwrap(),
                CoordinatorParticipantTarget::Data {
                    table_id: info.table_id.clone(),
                    partition_id: spec.partition_id,
                    epoch: spec.epoch,
                },
            )
        };
        participants.push((target, participant, info));
    }
    participants.sort_by_key(|(target, _, _)| *target.cell_id().as_bytes());
    let fault = Arc::new(AtomicU8::new(0));
    let pause = hold_first.then(|| Arc::new(ResolutionPause::default()));
    let fault_client = client.map_transport(|inner| {
        Arc::new(ResolutionFault {
            inner,
            first: participants[0].0.clone(),
            fault: fault.clone(),
            pause: pause.clone(),
        })
    });
    let remote = Arc::new(CellStorage::new(fault_client, "local"));
    // Each cut follows a durable decision. Refusing the first state read or
    // apply must not strand the second owner; a lost first receipt must not
    // stop later applies either. Run the same cuts for COMMIT and ABORT.
    for (case, (commit, cut)) in [true, false]
        .into_iter()
        .flat_map(|commit| (1..=3).map(move |cut| (commit, cut)))
        .enumerate()
    {
        let token = format!("resolution-{case}");
        let id = *uuid::Uuid::now_v7().as_bytes();
        let key = Item::from([("id".into(), AttributeValue::S(token.clone()))]);
        let mut proposed = key.clone();
        proposed.insert("value".into(), AttributeValue::S("prepared".into()));
        provisioner
            .ensure(&client, account_id, token.as_bytes())
            .await
            .unwrap();
        let coordinator = coordinator_target(account_id, token.as_bytes()).unwrap();
        let request: Vec<_> = participants
            .iter()
            .enumerate()
            .map(|(index, (_, target, info))| CoordinatorParticipant {
                target: target.clone(),
                operations: vec![IndexedTransactionOperation {
                    index: index as u8,
                    operation: TransactionOperation::Put(PutItemInput {
                        table_name: info.table_name.clone(),
                        table_id: info.table_id.clone(),
                        item: proposed.clone(),
                        condition: None,
                    }),
                }],
            })
            .collect();
        transaction_command!(
            client,
            BeginCrossCellTransaction,
            &coordinator,
            mutation(),
            Json(BeginCrossCellTransactionInput {
                account_id: account_id.into(),
                transaction_id: id,
                token: Some(TransactionToken {
                    account_id: account_id.into(),
                    token: token.clone(),
                    fingerprint: token.clone()
                }),
                participants: request.clone(),
            }),
        )
        .await
        .unwrap();
        for (position, ((target, participant, _), request)) in
            participants.iter().zip(&request).enumerate()
        {
            let operations = request
                .operations
                .iter()
                .map(|operation| operation.operation.clone())
                .collect();
            let prepared = match participant {
                CoordinatorParticipantTarget::Account => transaction_command!(
                    client,
                    PrepareAccountTransaction,
                    target,
                    mutation(),
                    Json(PrepareAccountTransactionInput {
                        transaction_id: id,
                        coordinator_cell: *coordinator.cell_id().as_bytes(),
                        coordinator_key: token.as_bytes().to_vec(),
                        operations,
                    }),
                )
                .await
                .unwrap(),
                CoordinatorParticipantTarget::Data {
                    table_id, epoch, ..
                } => transaction_command!(
                    client,
                    PreparePartitionTransaction,
                    target,
                    mutation(),
                    Json(PreparePartitionTransactionInput {
                        table_id: table_id.clone(),
                        epoch: *epoch,
                        transaction_id: id,
                        coordinator_cell: *coordinator.cell_id().as_bytes(),
                        coordinator_key: token.as_bytes().to_vec(),
                        operations,
                    }),
                )
                .await
                .unwrap(),
            };
            client
                .command::<RecordParticipantPrepare>(
                    &coordinator,
                    mutation(),
                    Json(CoordinatorPhaseInput {
                        account_id: account_id.into(),
                        transaction_id: id,
                        routing_key: token.as_bytes().to_vec(),
                        position: position as u8,
                        participant_cell: *target.cell_id().as_bytes(),
                        sequence: prepared.receipt.commit_sequence,
                    }),
                )
                .await
                .unwrap();
        }
        let decision = if commit {
            CoordinatorDecision::Commit
        } else {
            CoordinatorDecision::Abort {
                index: None,
                reason: None,
            }
        };
        client
            .command::<DecideCrossCellTransaction>(
                &coordinator,
                mutation(),
                Json(DecideCrossCellTransactionInput {
                    account_id: account_id.into(),
                    transaction_id: id,
                    routing_key: token.as_bytes().to_vec(),
                    decision: decision.clone(),
                }),
            )
            .await
            .unwrap();
        fault.store(cut, Ordering::SeqCst);
        let resolver = remote.clone();
        let resolver_token = token.clone();
        let resolving = tokio::spawn(async move {
            resolver
                .resume_cross_cell_transaction(account_id, resolver_token.as_bytes(), id)
                .await
        });
        let resolving = if let Some(pause) = &pause {
            tokio::time::timeout(Duration::from_secs(10), pause.entered.notified())
                .await
                .expect("selected participant RPC must start");
            // Query raw coordinator progress: a public item read would help
            // recovery and conceal a resolver that still waits sequentially.
            if cut != 3 {
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        let status = client
                            .query::<ReadCrossCellTransaction>(
                                &coordinator,
                                None,
                                Json(ReadCrossCellTransactionInput {
                                    account_id: account_id.into(),
                                    transaction_id: id,
                                    routing_key: token.as_bytes().to_vec(),
                                }),
                            )
                            .await
                            .unwrap()
                            .output
                            .0
                            .unwrap();
                        if usize::from(status.resolved_count) == participant_count - 1 {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .expect("healthy participants must resolve before the stalled RPC returns");
            }
            assert!(
                !resolving.is_finished(),
                "incomplete resolution cannot succeed"
            );
            Some(resolving)
        } else {
            assert!(
                matches!(resolving.await.unwrap(), Err(StorageError::Transient(_))),
                "incomplete resolution must remain retryable"
            );
            None
        };
        assert_eq!(fault.load(Ordering::SeqCst), 0, "fault must be exercised");
        let terminal = if commit {
            ParticipantTransactionState::Committed
        } else {
            ParticipantTransactionState::Aborted
        };
        let mut completed_positions = vec![false; participant_count];
        for (position, (target, participant, _)) in participants.iter().enumerate() {
            let read_state = || async {
                let input = Json(ReadTransactionInput {
                    transaction_id: id,
                    coordinator_cell: *coordinator.cell_id().as_bytes(),
                });
                match participant {
                    CoordinatorParticipantTarget::Account => {
                        client
                            .query::<ReadAccountTransaction>(target, None, input)
                            .await
                            .unwrap()
                            .output
                            .0
                    }
                    CoordinatorParticipantTarget::Data { .. } => {
                        client
                            .query::<ReadPartitionTransaction>(target, None, input)
                            .await
                            .unwrap()
                            .output
                            .0
                    }
                }
            };
            let state = read_state().await;
            completed_positions[position] = state == terminal;
            if cut == 3 {
                assert!(
                    state == terminal || state == ParticipantTransactionState::Prepared,
                    "participant has an unexpected state: commit={commit}, position={position}"
                );
            } else {
                assert_eq!(
                    state,
                    if position == 0 {
                        ParticipantTransactionState::Prepared
                    } else {
                        terminal.clone()
                    },
                    "healthy later participant must finish: commit={commit}, cut={cut}, position={position}"
                );
            }
        }
        let healthy_position = if cut == 3 { 0 } else { 1 };
        assert!(completed_positions[healthy_position]);
        let status = client
            .query::<ReadCrossCellTransaction>(
                &coordinator,
                None,
                Json(ReadCrossCellTransactionInput {
                    account_id: account_id.into(),
                    transaction_id: id,
                    routing_key: token.as_bytes().to_vec(),
                }),
            )
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        assert_eq!(status.decision, decision);
        if cut == 3 {
            assert!(usize::from(status.resolved_count) < participant_count);
        } else {
            assert_eq!(usize::from(status.resolved_count), participant_count - 1);
        }
        let healthy_info = &participants[healthy_position].2;
        assert_eq!(
            local.get_item(healthy_info, &key).await.unwrap(),
            commit.then(|| proposed.clone())
        );
        // A newer write proves the healthy lock was released. Retrying the old
        // transaction must not apply its staged image over that newer value.
        let mut newer = key.clone();
        newer.insert("value".into(), AttributeValue::S("newer".into()));
        for (position, (_, _, info)) in participants.iter().enumerate() {
            if !completed_positions[position] {
                continue;
            }
            // Include the first apply whose coordinator receipt was lost.
            // Its terminal participant record must prevent double apply too.
            local
                .put_item(
                    info,
                    newer.clone(),
                    false,
                    None,
                    &ExpressionMaps::default(),
                    None,
                )
                .await
                .unwrap();
        }
        if let Some(resolving) = resolving {
            if cut == 3 {
                // Losing the caller after apply but before its receipt must
                // leave the decision recoverable without overwriting newer data.
                resolving.abort();
                assert!(resolving.await.unwrap_err().is_cancelled());
            } else {
                pause.as_ref().unwrap().released.notify_one();
                assert!(matches!(
                    resolving.await.unwrap(),
                    Err(StorageError::Transient(_))
                ));
            }
        }
        assert_eq!(
            remote
                .resume_cross_cell_transaction(account_id, token.as_bytes(), id)
                .await
                .unwrap(),
            decision
        );
        assert_eq!(
            local.get_item(healthy_info, &key).await.unwrap(),
            Some(newer.clone())
        );
        let status = client
            .query::<ReadCrossCellTransaction>(
                &coordinator,
                None,
                Json(ReadCrossCellTransactionInput {
                    account_id: account_id.into(),
                    transaction_id: id,
                    routing_key: token.as_bytes().to_vec(),
                }),
            )
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        assert_eq!(usize::from(status.resolved_count), participant_count);
        for (position, (_, _, info)) in participants.iter().enumerate() {
            assert_eq!(
                local.get_item(info, &key).await.unwrap(),
                if completed_positions[position] {
                    Some(newer.clone())
                } else {
                    commit.then(|| proposed.clone())
                }
            );
        }
    }
    local_node::stop(node).await.unwrap();
}
struct ResolutionFault {
    inner: Arc<dyn InvocationTransport>,
    first: CellTarget,
    fault: Arc<AtomicU8>,
    pause: Option<Arc<ResolutionPause>>,
}
#[derive(Default)]
struct ResolutionPause {
    entered: Notify,
    released: Notify,
}
impl InvocationTransport for ResolutionFault {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            let cut = self.fault.load(Ordering::SeqCst);
            let command = invocation.mutation.is_some();
            let selected = match cut {
                1 => invocation.target == self.first,
                2 => {
                    invocation.target == self.first
                        && command
                        && matches!(
                            invocation.operation,
                            <ResolvePartitionTransaction as Command>::ID
                                | <ResolveAccountTransaction as Command>::ID
                        )
                }
                3 => {
                    if !command
                        || invocation.operation != RecordParticipantResolutions::ID
                        || invocation.target.namespace()
                            != coordinator_target("123456789012", &[0; 16])
                                .unwrap()
                                .namespace()
                    {
                        false
                    } else {
                        let mut decoder = BoundedDecoder::new(invocation.input, 4096).unwrap();
                        let phases = Json::<Vec<CoordinatorPhaseInput>>::decode(&mut decoder)
                            .unwrap()
                            .0;
                        decoder.finish().unwrap();
                        phases
                            .iter()
                            .any(|phase| phase.participant_cell == *self.first.cell_id().as_bytes())
                    }
                }
                _ => false,
            };
            if selected
                && self
                    .fault
                    .compare_exchange(cut, 0, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                if let Some(pause) = &self.pause {
                    pause.entered.notify_one();
                    pause.released.notified().await;
                }
                let body = Full::new(Bytes::new())
                    .map_err(|never| -> std::io::Error { match never {} })
                    .boxed_unsync();
                let mut response = http::Response::new(body);
                *response.status_mut() = http::StatusCode::SERVICE_UNAVAILABLE;
                response
            } else {
                self.inner.call(invocation, request).await
            }
        })
    }
}
