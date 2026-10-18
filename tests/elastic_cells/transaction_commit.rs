use crate::{cellar_node, local_node, transaction_fixture};
use beyonddb::*;
use cellar_runtime::{
    MutationIdentity,
    client::{CellClient, InvocationError, Receipt},
    identity::CellTarget,
};
fn mutation() -> MutationIdentity {
    local_node::wall_mutation()
}
/// The caller has published the first participant's real prepare receipt.
pub(super) async fn assert_atomic_prepared_commit(
    client: &CellClient,
    second: &(CellTarget, CoordinatorParticipant),
    transaction_id: [u8; 16],
) -> (ReadCrossCellTransactionInput, Receipt) {
    let account_id = "123456789012";
    let coordinator = coordinator_target(account_id, &transaction_id).unwrap();
    let transaction = ReadCrossCellTransactionInput {
        account_id: account_id.into(),
        transaction_id,
        routing_key: transaction_id.to_vec(),
    };
    let input = |prepares| {
        Json(CommitPreparedTransactionInput {
            transaction: transaction.clone(),
            prepares,
        })
    };
    let partial = client
        .command::<CommitPreparedTransaction>(&coordinator, mutation(), input(vec![]))
        .await;
    assert!(matches!(partial, Err(InvocationError::Rejected(result))
        if result.output.0 == CommitPreparedTransactionOutcome::Decision(
            DecideCrossCellTransactionOutcome::NotPrepared)));
    for (position, participant_cell, expected) in [
        (1, [99; 32], CoordinatorPhaseOutcome::WrongParticipant),
        (
            2,
            *second.0.cell_id().as_bytes(),
            CoordinatorPhaseOutcome::Missing,
        ),
    ] {
        let wrong = client
            .command::<CommitPreparedTransaction>(
                &coordinator,
                mutation(),
                input(vec![CoordinatorPrepareReceipt {
                    position,
                    participant_cell,
                    sequence: 1,
                }]),
            )
            .await;
        assert!(matches!(wrong, Err(InvocationError::Rejected(result))
            if result.output.0 == CommitPreparedTransactionOutcome::EvidenceRejected(vec![expected])));
    }
    let invalid_sequence = client
        .command::<CommitPreparedTransaction>(
            &coordinator,
            mutation(),
            input(vec![CoordinatorPrepareReceipt {
                position: 1,
                participant_cell: *second.0.cell_id().as_bytes(),
                sequence: 0,
            }]),
        )
        .await;
    assert!(invalid_sequence.is_err());
    let before = client
        .query::<ReadCrossCellTransaction>(&coordinator, None, Json(transaction.clone()))
        .await
        .unwrap();
    let status = before.output.0.unwrap();
    assert_eq!(status.decision, CoordinatorDecision::Begin);
    assert_eq!(status.prepared_count, 1);
    assert_eq!(status.resolved_count, 0);

    let CoordinatorParticipantTarget::Data {
        table_id, epoch, ..
    } = &second.1.target
    else {
        panic!("fixture requires a data participant");
    };
    let prepared = transaction_command!(
        client,
        PreparePartitionTransaction,
        &second.0,
        mutation(),
        Json(PreparePartitionTransactionInput {
            table_id: table_id.clone(),
            epoch: *epoch,
            transaction_id,
            coordinator_cell: *coordinator.cell_id().as_bytes(),
            coordinator_key: transaction_id.to_vec(),
            operations: second
                .1
                .operations
                .iter()
                .map(|op| op.operation.clone())
                .collect(),
        }),
    )
    .await
    .unwrap();
    let commit_input = input(vec![CoordinatorPrepareReceipt {
        position: 1,
        participant_cell: *second.0.cell_id().as_bytes(),
        sequence: prepared.receipt.commit_sequence,
    }]);
    let commit_identity = mutation();
    let committed = client
        .command::<CommitPreparedTransaction>(&coordinator, commit_identity, commit_input.clone())
        .await
        .unwrap();
    assert_eq!(
        committed.output.0,
        CommitPreparedTransactionOutcome::Decision(DecideCrossCellTransactionOutcome::Decided(
            CoordinatorDecision::Commit
        ))
    );
    assert_eq!(
        committed.receipt.commit_sequence - before.receipt.commit_sequence,
        1,
        "prepare evidence and terminal decision require one durable coordinator commit"
    );
    let replay = client
        .command::<CommitPreparedTransaction>(&coordinator, commit_identity, commit_input.clone())
        .await
        .unwrap();
    assert_eq!(
        replay.receipt.commit_sequence,
        committed.receipt.commit_sequence
    );
    assert_eq!(replay.output.0, committed.output.0);

    (transaction, committed.receipt)
}
#[test]
fn prepared_evidence_and_decision_survive_fresh_owner_before_resolution() {
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
        let bucket =
            cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite")).unwrap();
        let application = local_node::application().unwrap();
        let original_dir = directory.path().join("commit-owner-a");
        let (node, client) = cellar_node::start(
            application.clone(),
            &original_dir,
            host.handle(),
            core.handle(),
            bucket.clone(),
        )
        .await
        .unwrap();
        let fixture = transaction_fixture::table(&client, "PreparedCommit").await;
        let account_id = "123456789012";
        let id = [209; 16];
        let coordinator = coordinator_target(account_id, &id).unwrap();
        transaction_command!(
            client,
            BeginCrossCellTransaction,
            &coordinator,
            mutation(),
            Json(BeginCrossCellTransactionInput {
                account_id: account_id.into(),
                transaction_id: id,
                token: None,
                participants: fixture
                    .participants
                    .iter()
                    .map(|(_, p)| p.clone())
                    .collect(),
            })
        )
        .await
        .unwrap();
        let (first, participant) = &fixture.participants[0];
        let CoordinatorParticipantTarget::Data {
            table_id, epoch, ..
        } = &participant.target
        else {
            unreachable!()
        };
        let prepared = transaction_command!(
            client,
            PreparePartitionTransaction,
            first,
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
            })
        )
        .await
        .unwrap();
        assert_eq!(prepared.output.0, PrepareTransactionOutcome::Prepared);
        client
            .command::<RecordParticipantPrepare>(
                &coordinator,
                mutation(),
                Json(CoordinatorPhaseInput {
                    account_id: account_id.into(),
                    transaction_id: id,
                    routing_key: id.to_vec(),
                    position: 0,
                    participant_cell: *first.cell_id().as_bytes(),
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
                    transaction_id: id,
                    routing_key: id.to_vec(),
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
        assert_eq!(remaining[0].participant, fixture.participants[1].1);
        let (read, receipt) =
            assert_atomic_prepared_commit(&client, &fixture.participants[1], id).await;
        let (successor, restored) = cellar_node::start(
            application,
            &directory.path().join("commit-owner-b"),
            host.handle(),
            core.handle(),
            bucket,
        )
        .await
        .unwrap();
        cellar_node::stop(node, cellar::sdk::ShutdownMode::Handoff)
            .await
            .unwrap();
        std::fs::remove_dir_all(original_dir).unwrap();
        let snapshot = restored
            .query::<ReadCrossCellTransaction>(&coordinator, Some(receipt), Json(read.clone()))
            .await
            .unwrap();
        assert_eq!(snapshot.receipt, receipt);
        let status = snapshot.output.0.unwrap();
        assert_eq!(status.decision, CoordinatorDecision::Commit);
        assert_eq!(status.prepared_count, 2);
        assert_eq!(status.resolved_count, 0);
        for (target, _) in &fixture.participants {
            assert_eq!(
                restored
                    .query::<ReadPartitionTransaction>(
                        target,
                        None,
                        Json(ReadTransactionInput {
                            transaction_id: id,
                            coordinator_cell: *coordinator.cell_id().as_bytes(),
                        })
                    )
                    .await
                    .unwrap()
                    .output
                    .0,
                ParticipantTransactionState::Prepared
            );
        }
        let storage = CellStorage::new(restored.clone(), "local");
        assert_eq!(
            storage
                .resume_cross_cell_transaction(account_id, &id, id)
                .await
                .unwrap(),
            CoordinatorDecision::Commit
        );
        let status = restored
            .query::<ReadCrossCellTransaction>(&coordinator, None, Json(read))
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        assert_eq!(status.resolved_count, 2);
        for (position, (target, participant)) in fixture.participants.iter().enumerate() {
            let TransactionOperation::Put(original) = &participant.operations[0].operation else {
                unreachable!()
            };
            assert_eq!(
                restored
                    .query::<PartitionGet>(
                        target,
                        None,
                        Json(PartitionGetInput {
                            table_id: fixture.table.id.clone(),
                            epoch: 1,
                            key: extenddb_core::types::extract_key(
                                &original.item,
                                &fixture.table.key_schema
                            ),
                        })
                    )
                    .await
                    .unwrap()
                    .output
                    .0,
                PartitionGetOutcome::Found(Some(original.item.clone()))
            );
            assert_eq!(
                restored
                    .query::<ReadPartitionTransaction>(
                        target,
                        None,
                        Json(ReadTransactionInput {
                            transaction_id: id,
                            coordinator_cell: *coordinator.cell_id().as_bytes(),
                        })
                    )
                    .await
                    .unwrap()
                    .output
                    .0,
                ParticipantTransactionState::Committed
            );
            // Completed write images are collected; the immutable decision and
            // phase receipts remain available to delayed participant replies.
            assert!(
                restored
                    .query::<ReadCoordinatorParticipant>(
                        &coordinator,
                        None,
                        Json(ReadCoordinatorParticipantInput {
                            chunk: 0,
                            account_id: account_id.into(),
                            transaction_id: id,
                            routing_key: id.to_vec(),
                            position: position as u8,
                        })
                    )
                    .await
                    .unwrap()
                    .output
                    .is_none()
            );
        }
        cellar_node::stop(successor, cellar::sdk::ShutdownMode::Preserve)
            .await
            .unwrap();
    });
}
