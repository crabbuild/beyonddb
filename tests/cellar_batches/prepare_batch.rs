use crate::native_peer::{self, FRONT, Http, Inspection, Peer, PeerPolicy, SOURCE};
use crate::residency_fixture::runtimes;
use aws_sdk_dynamodb::types::AttributeValue as AwsAttributeValue;
use beyonddb::*;
use cellar_runtime::{MutationIdentity, client::InvocationError, identity::RequestId};
use extenddb_core::types::{AttributeValue, Item};
use std::sync::{Arc, Mutex, atomic::Ordering};
fn identity() -> MutationIdentity {
    let now = native_peer::now_ms();
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    }
}
#[test]
fn prepare_batch_rolls_back_locks_and_restores_independent_intents() {
    let (host, core) = runtimes();
    host.block_on(async {
    let files = tempfile::tempdir().unwrap();
    let application = native_peer::application();
    let bucket = cellar::bucket::Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
    let observed = Arc::new(Mutex::new(Default::default()));
    let profile = || PeerPolicy { max_resident: 16, ..true.into() };
    let mut source = Peer::start(application.clone(), &files.path().join(SOURCE), bucket.clone(),
        observed.clone(), profile(), host.handle(), core.handle()).await;
    let account_id = "123456789012";
    let account = account_target(account_id).unwrap();
    let access = "AKIAIOSFODNN7EXAMPLE";
    native_peer::put_credential(&source.client, access, account_id).await;
    native_peer::policy(&source.client, account_id).await;
    let (table, _) = native_peer::table(&source.client, account_id, "Residency", false).await;
    let table_id = table.id;
    let route = crate::single_leaf_route(&source.client, &account, &table_id).await.unwrap();
    assert_eq!(route.partitions.len(), 2);
    let range = route.partitions[0].clone();
    let target = data_target(account_id, &table_id, &range.partition_id).unwrap();
    let old = native_peer::identity(&observed, SOURCE, &target).unwrap();
    let mut remote = Peer::start(application.clone(), &files.path().join(FRONT), bucket.clone(),
        observed.clone(), PeerPolicy { max_resident: 8, ..true.into() }, host.handle(), core.handle()).await;
    let inspection = Inspection::new(bucket.clone(), host.handle());
    inspection.ready_peers().await;
    let sockets = source.network.accepted.load(Ordering::SeqCst);
    let mut client = remote.client.clone();
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
        matches!(client.command::<PreparePartitionTransactionBatch>(&target, identity(), inputs.clone()).await,
        Err(InvocationError::Rejected(result)) if result.output.0 == PreparePartitionBatchOutcome::IndividualRequired)
    );
    assert!(source.network.accepted.load(Ordering::SeqCst) > sockets);
    assert!(native_peer::identity(&observed, FRONT, &target).is_none());
    let owner = inspection.owner(&old.scope).await;
    assert_eq!(owner.node.as_deref(), Some(SOURCE));
    source.stop().await;
    std::fs::remove_dir_all(files.path().join(SOURCE)).unwrap();
    inspection.departed().await;
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
    let middle = native_peer::identity(&observed, FRONT, &target).unwrap();
    assert_eq!(middle.scope, old.scope);
    assert!(middle.epoch > owner.epoch);
    assert_eq!(inspection.owner(&middle.scope).await.node.as_deref(), Some(FRONT));
    let restored_replay = client.command::<PreparePartitionTransactionBatch>(
        &target, mutation, inputs).await.unwrap();
    assert_eq!(restored_replay.receipt, prepared.receipt);
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
    remote.stop().await;
    std::fs::remove_dir_all(files.path().join(FRONT)).unwrap();
    let replacement = "batch-replacement";
    let mut restored = Peer::start(application, &files.path().join(replacement), bucket,
        observed.clone(), profile(), host.handle(), core.handle()).await;
    inspection.departed_node(FRONT, replacement).await;
    client = restored.client.clone();
    for (input, expected) in [
        (&first, ParticipantTransactionState::Committed),
        (&second, ParticipantTransactionState::Aborted),
    ] {
        assert_eq!(
            client.query::<ReadPartitionTransaction>(&target, None, read(input))
                .await
                .unwrap()
                .output
                .0,
            expected
        );
    }
    let actual = native_peer::identity(&observed, replacement, &target).unwrap();
    assert_eq!(actual.scope, middle.scope);
    assert!(actual.epoch > middle.epoch);
    let owner = inspection.owner(&actual.scope).await;
    assert_eq!(owner.node.as_deref(), Some(replacement));
    assert_eq!(owner.epoch, actual.epoch);
    let http = Http::start(&client, access).await;
    for (key, exists) in [("batched-first", true), ("batched-second", false)] {
        assert_eq!(
            http.sdk
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
        http.sdk
            .put_item()
            .table_name("Residency")
            .item("id", AwsAttributeValue::S(key.into()))
            .send()
            .await
            .unwrap();
    }
    http.stop().await;
    restored.stop().await;
    inspection.close().await;
    });
}
