//! Discovery must recover the account index before abandoned coordinator shards.

use super::provisioning::{Remote, table_id, wait_for_expiry};
use super::*;
use beyonddb::{
    BeginCrossCellTransaction, BeginCrossCellTransactionInput, CoordinatorDecision,
    CoordinatorParticipant, CoordinatorParticipantTarget, GetItemInput,
    IndexedTransactionOperation, ReadCrossCellTransaction, ReadCrossCellTransactionInput,
    TransactionOperation,
};
use cellule_runtime::cell::catalog::CellCatalog;
use std::time::Duration;

const ACCOUNT: &str = "123456789012";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_discovers_new_coordinator_after_its_account_owner_expires() {
    let fixture = Fixture::with_capacity(2, 16).await;
    let remote = Remote::new(&fixture).await;
    let account = account_target(ACCOUNT).unwrap();
    let table = table_id(&fixture, "Residency").await;
    let route = crate::single_leaf_route(&fixture.client, &account, &table)
        .await
        .unwrap();
    fixture
        .provisioner
        .admit_account(ACCOUNT)
        .await
        .unwrap()
        .drain()
        .await
        .unwrap();
    remote.provisioner.admit_account(ACCOUNT).await.unwrap();
    // Install before registration. This worker has never admitted the new shard,
    // so its only durable discovery path is the account's coordinator index.
    fixture
        .provisioner
        .install_transaction_recovery_loop(
            &fixture.tasks,
            beyonddb::CellStorage::new(fixture.client.clone(), "us-east-1"),
            fixture.directory.clone(),
            vec![ACCOUNT.into()],
        )
        .unwrap();
    let transaction_id = [174; 16];
    let coordinator = beyonddb::coordinator_target(ACCOUNT, &transaction_id).unwrap();
    remote
        .provisioner
        .admit_coordinator(ACCOUNT, &transaction_id)
        .await
        .unwrap();
    let mut participants = route
        .partitions
        .iter()
        .enumerate()
        .map(|(index, partition)| {
            let target = beyonddb::data_target(ACCOUNT, &table, &partition.partition_id).unwrap();
            let (_, item) = fixture
                .data
                .iter()
                .find(|(handle, _)| handle.cell_id() == target.cell_id())
                .unwrap();
            CoordinatorParticipant {
                target: CoordinatorParticipantTarget::Data {
                    table_id: table.clone(),
                    partition_id: partition.partition_id,
                    epoch: partition.epoch,
                },
                operations: vec![IndexedTransactionOperation {
                    index: u8::try_from(index).unwrap(),
                    operation: TransactionOperation::Read(GetItemInput {
                        table_name: "Residency".into(),
                        table_id: table.clone(),
                        key: Item::from([(
                            "id".into(),
                            AttributeValue::S(item["id"].as_s().unwrap().clone()),
                        )]),
                    }),
                }],
            }
        })
        .collect::<Vec<_>>();
    participants.sort_by_key(|participant| match &participant.target {
        CoordinatorParticipantTarget::Data {
            table_id,
            partition_id,
            ..
        } => *beyonddb::data_target(ACCOUNT, table_id, partition_id)
            .unwrap()
            .cell_id()
            .as_bytes(),
        _ => unreachable!(),
    });
    let now = now_ms();
    let identity = cellule_runtime::MutationIdentity {
        request_id: cellule_runtime::identity::RequestId::from_bytes(
            *uuid::Uuid::now_v7().as_bytes(),
        ),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    };
    transaction_command!(
        remote.client(&fixture),
        BeginCrossCellTransaction,
        &coordinator,
        identity,
        Json(BeginCrossCellTransactionInput {
            account_id: ACCOUNT.into(),
            transaction_id,
            token: None,
            participants,
        }),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(750)).await;
    let authority = CellAuthority::new(fixture.layout.clone());
    for target in [&account, &coordinator] {
        let current = authority.load(target.cell_id()).await.unwrap().unwrap();
        assert_eq!(
            current.value().owner.as_ref().unwrap().session,
            remote.session,
            "discovery must preserve a live remote owner"
        );
    }
    remote.stop_listener();
    remote.lease.cancel();
    wait_for_expiry(&fixture, remote.session).await;
    let proof = CellCatalog::new(fixture.layout.clone(), coordinator.tenant())
        .lookup(coordinator.cell_id())
        .await
        .unwrap()
        .unwrap();
    let recovered = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let current = authority
                .load(coordinator.cell_id())
                .await
                .unwrap()
                .unwrap();
            // Observe locally: a public/routed query could restore the shard and
            // hide a worker discovery failure. The observer never admits work.
            if let Some(handle) = fixture
                .node
                .runtime()
                .local_handle(proof.clone(), &current)
                .await
                .unwrap()
            {
                let status = CellClient::local(fixture.application.registry(), handle)
                    .query::<ReadCrossCellTransaction>(
                        &coordinator,
                        None,
                        Json(ReadCrossCellTransactionInput {
                            account_id: ACCOUNT.into(),
                            transaction_id,
                            routing_key: transaction_id.to_vec(),
                        }),
                    )
                    .await
                    .unwrap()
                    .output
                    .0
                    .unwrap();
                if status.resolved_count == 2 {
                    assert_eq!(status.decision, CoordinatorDecision::Commit);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    // This expired owner still has an abandoned BEGIN. Its cleanup may report
    // the exact fence that prevents a stale writer from publishing on shutdown.
    let stopped = remote.node.shutdown().await;
    assert!(
        matches!(stopped, Ok(()) | Err(cellule_runtime::Error::Fenced)),
        "unexpected lost-owner shutdown error: {stopped:?}"
    );
    fixture.shutdown().await;
    recovered
        .expect("worker must recover account discovery and resolve the new abandoned coordinator");
}
