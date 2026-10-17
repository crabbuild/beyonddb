//! Settled discovery must not hide a later admitted transaction after handoff.
use super::coordinator_residency::{
    ACCOUNT, RESIDENT, assert_read_results, keys, pending_read, recover, runtimes, storage, tokens,
    write,
};
use crate::{cellar_node, local_node, transaction_fixture};
use beyonddb::*;
use extenddb_core::types::AttributeValue;
use extenddb_storage::{DataEngine, TableEngine};
use std::sync::Arc;

#[test]
fn settled_discovery_survives_restart_but_later_begin_is_not_skipped() {
    let (host, core) = runtimes();
    host.block_on(async {
        let files = tempfile::tempdir().unwrap();
        let first = files.path().join("settled-discovery-a");
        let bucket =
            cellar::bucket::Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let (node, client) = cellar_node::start_limited(
            local_node::application().unwrap(),
            &first,
            host.handle(),
            core.handle(),
            bucket.clone(),
            RESIDENT,
        )
        .await
        .unwrap();
        let fixture = transaction_fixture::table(&client, "Residency").await;
        let info = fixture
            .storage
            .table_key_info(ACCOUNT, "Residency")
            .await
            .unwrap();
        let tokens = tokens("checkpoint", 3);
        for (version, token) in tokens.iter().enumerate() {
            assert!(!write(&fixture.storage, &info, token, version).await);
        }
        let account = account_target(ACCOUNT).unwrap();
        let input = Json(ListCoordinatorShardsInput {
            account_id: ACCOUNT.into(),
            after: None,
            limit: 100,
        });
        let before = client
            .query::<ListCoordinatorShards>(&account, None, input.clone())
            .await
            .unwrap();
        assert_eq!(before.output.0.len(), 3);
        let mut recovery =
            TransactionRecovery::new(Arc::new(storage(&client, 2)), ACCOUNT.into()).unwrap();
        for _ in 0..16 {
            recovery.step().await.unwrap();
        }
        assert_eq!(
            client
                .query::<ListCoordinatorShards>(&account, None, input)
                .await
                .unwrap()
                .receipt,
            before.receipt
        );
        let oldest = coordinator_target(ACCOUNT, tokens[0].as_bytes()).unwrap();
        let id = pending_read(&client, &fixture, &oldest).await;
        let (successor, next) = cellar_node::start_limited(
            local_node::application().unwrap(),
            &files.path().join("settled-discovery-b"),
            host.handle(),
            core.handle(),
            bucket.clone(),
            RESIDENT,
        )
        .await
        .unwrap();
        cellar_node::stop(node, cellar::sdk::ShutdownMode::Handoff)
            .await
            .unwrap();
        std::fs::remove_dir_all(first).unwrap();
        // Inspect raw phase state before any public read could help recovery.
        let read = Json(ReadTransactionInput {
            transaction_id: id,
            coordinator_cell: *oldest.cell_id().as_bytes(),
        });
        assert_eq!(
            next.query::<ReadPartitionTransaction>(&fixture.participants[0].0, None, read.clone())
                .await
                .unwrap()
                .output
                .0,
            ParticipantTransactionState::Prepared
        );
        assert_eq!(
            next.query::<ReadPartitionTransaction>(&fixture.participants[1].0, None, read)
                .await
                .unwrap()
                .output
                .0,
            ParticipantTransactionState::Missing
        );
        // Native live recovery resumes fresh BEGINs from their retained payload.
        // Startup is not authority to abort a transaction in another live cell.
        recover(&next, id).await;
        assert_read_results(&next, &fixture, id, 2).await;
        super::coordinator_residency::departure(
            &bucket,
            host.handle(),
            "settled-discovery-a",
            "settled-discovery-b",
        )
        .await;
        let storage = storage(&next, 2);
        assert!(!write(&storage, &info, "after-checkpoint-recovery", 3).await);
        assert!(write(&storage, &info, &tokens[1], 1).await);
        for key in keys() {
            assert_eq!(
                storage.get_item(&info, &key).await.unwrap().unwrap()["version"],
                AttributeValue::N("3".into())
            );
        }
        cellar_node::stop(successor, cellar::sdk::ShutdownMode::Preserve)
            .await
            .unwrap();
    });
}
