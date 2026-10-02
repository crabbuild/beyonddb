use super::*;
use beyonddb::{
    ParticipantTransactionState, PreparePartitionBatchOutcome, PreparePartitionTransactionBatch,
    PreparePartitionTransactionInput, PutItemInput, ReadPartitionTransaction, ReadTransactionInput,
    ResolvePartitionTransaction, ResolveTransactionInput, ResolveTransactionOutcome,
    TransactionOperation,
};
use cellule_runtime::{MutationIdentity, client::InvocationError, identity::RequestId};

fn identity() -> MutationIdentity {
    let issued_at_ms = now_ms();
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms,
        expires_at_ms: issued_at_ms + 60_000,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_batch_rolls_back_locks_and_restores_independent_intents() {
    let fixture = Fixture::with_capacity(1, 16).await;
    let remote = super::provisioning::Remote::new(&fixture).await;
    let client = remote.client(&fixture);
    let account = account_target("123456789012").unwrap();
    let table_id = super::provisioning::table_id(&fixture, "Residency").await;
    let range = super::cell_models::ranges(&fixture, "Residency")
        .await
        .remove(0);
    let target = beyonddb::data_target("123456789012", &table_id, &range.partition_id).unwrap();
    let coordinator_cell = *account.cell_id().as_bytes();
    let input = |index: u8, key: &str| PreparePartitionTransactionInput {
        table_id: table_id.clone(),
        epoch: range.epoch,
        transaction_id: [index; 16],
        coordinator_cell,
        coordinator_key: vec![index; 16],
        operations: vec![TransactionOperation::Put(PutItemInput {
            table_name: "Residency".into(),
            table_id: table_id.clone(),
            item: Item::from([("id".into(), AttributeValue::S(key.into()))]),
            condition: None,
        })],
    };
    let first = input(180, "batched-first");
    let second = input(181, "batched-second");
    let read = |input: &PreparePartitionTransactionInput| {
        Json(ReadTransactionInput {
            transaction_id: input.transaction_id,
            coordinator_cell,
        })
    };
    // The second prepare conflicts with the first staged lock. The rejection
    // must roll back the first intent and lock, not just the conflicting child.
    let mut conflicting = second.clone();
    conflicting.operations = first.operations.clone();
    let rejected = client
        .command::<PreparePartitionTransactionBatch>(
            &target,
            identity(),
            Json(vec![first.clone(), conflicting]),
        )
        .await;
    assert!(matches!(rejected, Err(InvocationError::Rejected(result))
        if result.output.0 == PreparePartitionBatchOutcome::IndividualRequired));
    for input in [&first, &second] {
        assert_eq!(
            client
                .query::<ReadPartitionTransaction>(&target, None, read(input))
                .await
                .unwrap()
                .output
                .0,
            ParticipantTransactionState::Missing
        );
    }
    // Identical transaction identities within a batch are rejected before any
    // prepare; they must not accidentally share one coordinator receipt.
    let duplicate = client
        .command::<PreparePartitionTransactionBatch>(
            &target,
            identity(),
            Json(vec![first.clone(), first.clone()]),
        )
        .await;
    assert!(matches!(duplicate, Err(InvocationError::Rejected(result))
        if result.output.0 == PreparePartitionBatchOutcome::IndividualRequired));
    let mutation = identity();
    let inputs = Json(vec![first.clone(), second.clone()]);
    let prepared = client
        .command::<PreparePartitionTransactionBatch>(&target, mutation, inputs.clone())
        .await
        .unwrap();
    assert_eq!(prepared.output.0, PreparePartitionBatchOutcome::Prepared);
    let replay = client
        .command::<PreparePartitionTransactionBatch>(&target, mutation, inputs.clone())
        .await
        .unwrap();
    assert_eq!(replay.receipt, prepared.receipt);
    // A *new* command seeing existing intents requests individual handling;
    // the original durable intents survive that rejected application savepoint.
    assert!(
        matches!(client.command::<PreparePartitionTransactionBatch>(&target, identity(), inputs).await,
        Err(InvocationError::Rejected(result)) if result.output.0 == PreparePartitionBatchOutcome::IndividualRequired)
    );
    fixture.data[0].0.drain().await.unwrap();
    remote
        .provisioner
        .admit_existing_partition("123456789012", &table_id, &range.partition_id)
        .await
        .unwrap();
    for input in [&first, &second] {
        assert_eq!(
            client
                .query::<ReadPartitionTransaction>(&target, None, read(input))
                .await
                .unwrap()
                .output
                .0,
            ParticipantTransactionState::Prepared
        );
    }
    // Shared publication never merges terminal decisions. Commit one intent
    // and abort the other, then restore again and verify both durable states.
    for (input, commit) in [(&first, true), (&second, false)] {
        let result = client
            .command::<ResolvePartitionTransaction>(
                &target,
                identity(),
                Json(ResolveTransactionInput {
                    transaction_id: input.transaction_id,
                    coordinator_cell,
                    commit,
                }),
            )
            .await
            .unwrap();
        assert_eq!(
            result.output.0,
            if commit {
                ResolveTransactionOutcome::Committed
            } else {
                ResolveTransactionOutcome::Aborted
            }
        );
    }
    remote
        .provisioner
        .admit_existing_partition("123456789012", &table_id, &range.partition_id)
        .await
        .unwrap()
        .drain()
        .await
        .unwrap();
    fixture
        .provisioner
        .admit_existing_partition("123456789012", &table_id, &range.partition_id)
        .await
        .unwrap();
    for (input, expected) in [
        (&first, ParticipantTransactionState::Committed),
        (&second, ParticipantTransactionState::Aborted),
    ] {
        assert_eq!(
            fixture
                .client
                .query::<ReadPartitionTransaction>(&target, None, read(input))
                .await
                .unwrap()
                .output
                .0,
            expected
        );
    }
    for (key, exists) in [("batched-first", true), ("batched-second", false)] {
        assert_eq!(
            fixture
                .sdk
                .get_item()
                .table_name("Residency")
                .key("id", AwsAttributeValue::S(key.into()))
                .send()
                .await
                .unwrap()
                .item
                .is_some(),
            exists
        );
        fixture
            .sdk
            .put_item()
            .table_name("Residency")
            .item("id", AwsAttributeValue::S(key.into()))
            .send()
            .await
            .unwrap();
    }
    remote.shutdown().await;
    fixture.shutdown().await;
}
