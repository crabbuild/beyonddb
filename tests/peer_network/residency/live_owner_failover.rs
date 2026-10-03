use super::*;
use beyonddb::{
    BeginCrossCellTransaction, BeginCrossCellTransactionInput, CellStorage, CoordinatorDecision,
    CoordinatorParticipant, CoordinatorParticipantTarget, CoordinatorPhaseInput,
    CoordinatorProvisioner, DecideCrossCellTransaction, DecideCrossCellTransactionInput,
    IndexedTransactionOperation, PreparePartitionTransaction, PreparePartitionTransactionInput,
    PutItemInput, ReadCrossCellTransaction, ReadCrossCellTransactionInput,
    RecordParticipantPrepare, ResolvePartitionTransaction, ResolveTransactionInput,
    TransactionOperation,
};
use cellule_runtime::identity::RequestId;
use extenddb_core::types::{AttributeValue, Item};

fn identity() -> cellule_runtime::MutationIdentity {
    let now = now_ms();
    cellule_runtime::MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serving_recovery_preserves_live_coordinator_then_finishes_after_owner_expiry() {
    let fixture = Fixture::new().await;
    let remote = provisioning::Remote::new(&fixture).await;
    let account = account_target("123456789012").unwrap();
    let table = fixture
        .client
        .query::<DescribeTable>(&account, None, Json("Residency".into()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    let route = crate::single_leaf_route(&fixture.client, &account, &table.id)
        .await
        .unwrap();
    let mut participants = Vec::new();
    for (partition, (original, item)) in route.partitions.iter().zip(&fixture.data) {
        let target =
            beyonddb::data_target("123456789012", &table.id, &partition.partition_id).unwrap();
        assert_eq!(target.cell_id(), original.cell_id());
        original.drain().await.unwrap();
        remote
            .provisioner
            .admit_existing_partition("123456789012", &table.id, &partition.partition_id)
            .await
            .unwrap();
        let id = item["id"].as_s().unwrap().clone();
        participants.push((
            target,
            CoordinatorParticipant {
                target: CoordinatorParticipantTarget::Data {
                    table_id: table.id.clone(),
                    partition_id: partition.partition_id,
                    epoch: partition.epoch,
                },
                operations: vec![IndexedTransactionOperation {
                    index: u8::try_from(participants.len()).unwrap(),
                    operation: TransactionOperation::Put(PutItemInput {
                        table_name: "Residency".into(),
                        table_id: table.id.clone(),
                        item: Item::from([
                            ("id".into(), AttributeValue::S(id)),
                            (
                                "value".into(),
                                AttributeValue::S("owner-expiry-recovered".into()),
                            ),
                        ]),
                        condition: None,
                    }),
                }],
            },
        ));
    }
    assert_eq!(participants.len(), 2);
    participants.sort_by_key(|(target, _)| *target.cell_id().as_bytes());
    fixture
        .provisioner
        .install_transaction_recovery_loop(
            &fixture.tasks,
            CellStorage::new(fixture.client.clone(), "us-east-1"),
            fixture.directory.clone(),
            vec!["123456789012".into()],
        )
        .unwrap();
    let transaction_id = *uuid::Uuid::now_v7().as_bytes();
    let coordinator = beyonddb::coordinator_target("123456789012", &transaction_id).unwrap();
    let client = remote.client(&fixture);
    remote
        .provisioner
        .ensure(&client, "123456789012", &transaction_id)
        .await
        .unwrap();
    client
        .command::<BeginCrossCellTransaction>(
            &coordinator,
            identity(),
            Json(beyonddb::TransactionCommandInput::Inline(
                BeginCrossCellTransactionInput {
                    account_id: "123456789012".into(),
                    transaction_id,
                    token: None,
                    participants: participants
                        .iter()
                        .map(|(_, participant)| participant.clone())
                        .collect(),
                },
            )),
        )
        .await
        .unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let before = authority
        .load(coordinator.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .clone();
    assert_eq!(before.owner.as_ref().unwrap().session, remote.session);
    for (position, (target, participant)) in participants.iter().enumerate() {
        let CoordinatorParticipantTarget::Data {
            table_id, epoch, ..
        } = &participant.target
        else {
            unreachable!()
        };
        let prepared = client
            .command::<PreparePartitionTransaction>(
                target,
                identity(),
                Json(beyonddb::TransactionCommandInput::Inline(
                    PreparePartitionTransactionInput {
                        table_id: table_id.clone(),
                        epoch: *epoch,
                        transaction_id,
                        coordinator_cell: *coordinator.cell_id().as_bytes(),
                        coordinator_key: transaction_id.to_vec(),
                        operations: participant
                            .operations
                            .iter()
                            .map(|operation| operation.operation.clone())
                            .collect(),
                    },
                )),
            )
            .await
            .unwrap();
        client
            .command::<RecordParticipantPrepare>(
                &coordinator,
                identity(),
                Json(CoordinatorPhaseInput {
                    account_id: "123456789012".into(),
                    transaction_id,
                    routing_key: transaction_id.to_vec(),
                    position: u8::try_from(position).unwrap(),
                    participant_cell: *target.cell_id().as_bytes(),
                    sequence: prepared.receipt.commit_sequence,
                }),
            )
            .await
            .unwrap();
    }
    client
        .command::<DecideCrossCellTransaction>(
            &coordinator,
            identity(),
            Json(DecideCrossCellTransactionInput {
                account_id: "123456789012".into(),
                transaction_id,
                routing_key: transaction_id.to_vec(),
                decision: CoordinatorDecision::Commit,
            }),
        )
        .await
        .unwrap();
    client
        .command::<ResolvePartitionTransaction>(
            &participants[0].0,
            identity(),
            Json(ResolveTransactionInput {
                transaction_id,
                coordinator_cell: *coordinator.cell_id().as_bytes(),
                commit: true,
            }),
        )
        .await
        .unwrap();
    // No participant completion receipt is recorded. Recovery was installed
    // before discovery registration, but must leave this live owner alone.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let live = authority
        .load(coordinator.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .clone();
    assert!(
        fixture
            .directory
            .is_live(remote.session, now_ms())
            .await
            .unwrap()
    );
    assert_eq!(live.incarnation, before.incarnation);
    assert_eq!(live.epoch, before.epoch);
    assert_eq!(live.owner.as_ref().unwrap().session, remote.session);
    let input = ReadCrossCellTransactionInput {
        account_id: "123456789012".into(),
        transaction_id,
        routing_key: transaction_id.to_vec(),
    };
    let pending = client
        .query::<ReadCrossCellTransaction>(&coordinator, None, Json(input.clone()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert_eq!(pending.decision, CoordinatorDecision::Commit);
    assert_eq!(pending.resolved_count, 0);
    remote.stop_listener();
    remote.lease.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while fixture
            .directory
            .is_live(remote.session, now_ms())
            .await
            .unwrap()
        {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    // Private status reads cannot resolve participants. Both completion receipts
    // must be recorded by the already-serving recovery worker before SDK reads.
    tokio::time::timeout(std::time::Duration::from_secs(45), async {
        loop {
            if let Ok(status) = fixture
                .client
                .query::<ReadCrossCellTransaction>(&coordinator, None, Json(input.clone()))
                .await
            {
                let status = status.output.0.unwrap();
                assert_eq!(status.decision, CoordinatorDecision::Commit);
                if status.resolved_count == 2 {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("serving recovery must discover the expired owner and record both resolutions");
    for (_, item) in &fixture.data {
        let result = provisioning::sdk_without_retries(&fixture)
            .get_item()
            .table_name("Residency")
            .key("id", item["id"].clone())
            .consistent_read(true)
            .send()
            .await
            .unwrap();
        assert_eq!(
            result.item.unwrap()["value"],
            AwsAttributeValue::S("owner-expiry-recovered".into())
        );
    }
    let recovered = authority
        .load(coordinator.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .clone();
    assert_eq!(recovered.incarnation, before.incarnation);
    assert!(recovered.epoch > before.epoch);
    assert_eq!(recovered.owner.as_ref().unwrap().session, fixture.session);
    assert!(matches!(
        remote.node.shutdown().await,
        Ok(()) | Err(cellule_runtime::Error::Fenced)
    ));
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn published_idle_range_can_move_while_its_former_node_stays_live() {
    let fixture = Fixture::with_partition_count(1).await;
    let remote = provisioning::Remote::new(&fixture).await;
    let account = account_target("123456789012").unwrap();
    let table = fixture
        .client
        .query::<DescribeTable>(&account, None, Json("Residency".into()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    let route = crate::single_leaf_route(&fixture.client, &account, &table.id)
        .await
        .unwrap();
    let partition = &route.partitions[0];
    let target = beyonddb::data_target("123456789012", &table.id, &partition.partition_id).unwrap();
    fixture.data[0].0.drain().await.unwrap();
    let handle = remote
        .provisioner
        .admit_existing_partition("123456789012", &table.id, &partition.partition_id)
        .await
        .unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let before = authority
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .clone();
    fixture
        .provisioner
        .recover_registered_partitions("123456789012", &fixture.client, &fixture.directory)
        .await
        .unwrap();
    let live = authority
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .clone();
    assert_eq!(live.owner.as_ref().unwrap().session, remote.session);
    assert_eq!(live.epoch, before.epoch);
    handle.drain().await.unwrap();
    let idle = authority
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .clone();
    assert!(
        fixture
            .directory
            .is_live(remote.session, now_ms())
            .await
            .unwrap()
    );
    assert_eq!(idle.state, cellule_runtime::control::ControlState::Idle);
    assert!(idle.owner.is_none());
    fixture
        .provisioner
        .recover_registered_partitions("123456789012", &fixture.client, &fixture.directory)
        .await
        .unwrap();
    let restored = authority
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .clone();
    let expected = fixture.data[0].1.clone();
    let read = provisioning::sdk_without_retries(&fixture)
        .get_item()
        .table_name("Residency")
        .key("id", expected["id"].clone())
        .consistent_read(true)
        .send()
        .await;
    let source_still_live = fixture
        .directory
        .is_live(remote.session, now_ms())
        .await
        .unwrap();
    let destination = fixture.session;
    remote.shutdown().await;
    fixture.shutdown().await;
    assert!(source_still_live);
    crate::peer_network::recovery::assert_retained_range(
        &before,
        &restored,
        destination,
        live.owner.as_ref().unwrap().session,
    );
    assert_eq!(restored.owner.as_ref().unwrap().session, destination);
    assert!(restored.epoch > before.epoch);
    assert_eq!(read.unwrap().item, Some(expected));
}
