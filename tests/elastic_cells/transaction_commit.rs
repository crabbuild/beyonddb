use crate::*;
use beyonddb::{
    CommitPreparedTransaction, CommitPreparedTransactionInput, CommitPreparedTransactionOutcome,
    CoordinatorPrepareReceipt,
};

fn mutation() -> MutationIdentity {
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        ..identity(209)
    }
}

/// The surrounding driver fixture has recorded one of two actual prepares.
pub(super) async fn assert_atomic_prepared_commit(
    client: &CellClient,
    bootstrap: &Bootstrap<'_>,
    coordinator_handle: &CellHandle,
    application: Arc<cellule_app::CompiledApplication>,
    directory: &std::path::Path,
    second: &(CellTarget, CoordinatorParticipant),
    transaction_id: [u8; 16],
) -> std::path::PathBuf {
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
        .command::<CommitPreparedTransaction>(&coordinator, commit_identity, commit_input)
        .await
        .unwrap();
    assert_eq!(
        replay.receipt.commit_sequence,
        committed.receipt.commit_sequence
    );
    assert_eq!(replay.output.0, committed.output.0);

    // Restore the coordinator from its durable root before the driver resolves
    // participants. The decision and both prepare receipts must survive together.
    coordinator_handle.drain().await.unwrap();
    let provisioner = CellInitialPartitionProvisioner::new(
        bootstrap.runtime.clone(),
        application,
        bootstrap.layout.clone(),
        bootstrap.session,
        "https://commit-restart.internal:8081".into(),
        directory.join("commit-restored"),
    )
    .unwrap();
    provisioner
        .admit_coordinator(account_id, &transaction_id)
        .await
        .unwrap();
    let restored = client
        .query::<ReadCrossCellTransaction>(&coordinator, None, Json(transaction))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert_eq!(restored.decision, CoordinatorDecision::Commit);
    assert_eq!(restored.prepared_count, 2);
    assert_eq!(restored.resolved_count, 0);
    let restored_directory = directory.join("commit-restored").join(
        blake3::Hash::from_bytes(*coordinator.cell_id().as_bytes())
            .to_hex()
            .as_str(),
    );
    let files = std::fs::read_dir(restored_directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "sqlite")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        files.len(),
        1,
        "one restored coordinator activation is expected"
    );
    files[0].clone()
}
