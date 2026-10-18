//! Cellar owns eviction; logical retirement fences must survive its resident cap.
use crate::{
    cellar_node,
    directory_fixture::{ACCOUNT, application, mutation, ranges},
};
use beyonddb::*;
use cellar_runtime::client::InvocationError;
use extenddb_core::{
    expression::ExpressionMaps,
    types::{AttributeValue, Item},
};
use extenddb_storage::{DataEngine, TableEngine};
use std::sync::{Arc, atomic::Ordering};

#[test]
fn retired_directories_make_room_for_new_data_without_losing_fences() {
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
        let application = application();
        let account = account_target(ACCOUNT).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let first = directory.path().join("residency-owner-a");
        let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite")).unwrap();
        let (node, client) = cellar_node::start_limited(application.clone(), &first, host.handle(), core.handle(), bucket.clone(), 16).await.unwrap();
        let load = node.services().node_load().unwrap();
        let ranges = ranges(2);
        let mut directories = Vec::new();
        for ordinal in 0..48_u8 {
            let mut id = [ordinal; 32];
            id[..16].copy_from_slice(account.tenant().as_bytes());
            let spec = DirectorySpec::root(blake3::Hash::from_bytes(id).to_hex().to_string());
            let target = directory_target(ACCOUNT, &spec).unwrap();
            client.command::<InstallDirectory>(&target, mutation(), Json(DirectoryInstall {
                spec: spec.clone(), ranges: ranges.clone(), source: None,
            })).await.unwrap();
            if ordinal != 0 {
                client.command::<RetireDirectory>(&target, mutation(), Json(spec.clone())).await.unwrap();
            }
            directories.push((spec, target));
            assert!(load.resident_cells.load(Ordering::SeqCst) <= 16);
            if ordinal == 15 {
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    while load.resident_cells.load(Ordering::SeqCst) != 16 {tokio::task::yield_now().await;}
                }).await.unwrap();
            }
        }
        // SDK load counters are published by its sampling/lease tick.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while load.owned_cells.load(Ordering::SeqCst) < directories.len() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }).await.unwrap_or_else(|_| panic!("owned={}, resident={}", load.owned_cells.load(Ordering::SeqCst), load.resident_cells.load(Ordering::SeqCst)));
        let provisioner = Arc::new(CellInitialPartitionProvisioner::new(client.clone(), application.clone()).with_initial_partition_count(2).unwrap());
        let storage = CellStorage::new(client.clone(), "local").with_initial_partitions(provisioner);
        storage.create_table(ACCOUNT, serde_json::from_value(serde_json::json!({
            "TableName":"ResidentData", "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}], "BillingMode":"PAY_PER_REQUEST"
        })).unwrap()).await.unwrap();
        let info = storage.table_key_info(ACCOUNT, "ResidentData").await.unwrap();
        let item = Item::from([("id".into(), AttributeValue::S("kept".into()))]);
        storage.put_item(&info, item.clone(), false, None, &ExpressionMaps::default(), None).await.unwrap();
        assert_eq!(storage.get_item(&info, &item).await.unwrap(), Some(item.clone()));
        for (index, (spec, target)) in directories.iter().enumerate() {
            let state = client.query::<ReadDirectory>(target, None, Json(())).await.unwrap().output.0.unwrap();
            assert_eq!(state.spec, *spec);
            assert_eq!(state.mode, if index == 0 {DirectoryMode::Leaf} else {DirectoryMode::Retired});
            let installed = client.command::<InstallDirectory>(target, mutation(), Json(DirectoryInstall {
                spec: spec.clone(), ranges: ranges.clone(), source: None,
            })).await;
            if index == 0 {assert!(installed.unwrap().output.0);}
            else {assert!(matches!(installed, Err(InvocationError::Rejected(result)) if !result.output.0));}
            assert!(load.resident_cells.load(Ordering::SeqCst) <= 16);
        }
        assert_eq!(storage.get_item(&info, &item).await.unwrap(), Some(item.clone()));
        assert!(node.is_ready().await.unwrap());
        let (restored, next) = cellar_node::start_limited(application, &directory.path().join("residency-owner-b"), host.handle(), core.handle(), bucket, 16).await.unwrap();
        cellar_node::stop(node, cellar::sdk::ShutdownMode::Handoff).await.unwrap();
        std::fs::remove_dir_all(first).unwrap();
        let load = restored.services().node_load().unwrap();
        for (index, (spec, target)) in directories.iter().enumerate() {
            let state = next.query::<ReadDirectory>(target, None, Json(())).await.unwrap().output.0.unwrap();
            assert_eq!(state.spec, *spec);
            assert_eq!(state.mode, if index == 0 {DirectoryMode::Leaf} else {DirectoryMode::Retired});
            if index > 0 {
                assert!(matches!(next.command::<InstallDirectory>(target, mutation(), Json(DirectoryInstall {
                    spec: spec.clone(), ranges: ranges.clone(), source: None,
                })).await, Err(InvocationError::Rejected(result)) if !result.output.0));
            }
            assert!(load.resident_cells.load(Ordering::SeqCst) <= 16);
        }
        assert_eq!(CellStorage::new(next, "local").get_item(&info, &item).await.unwrap(), Some(item));
        assert!(restored.is_ready().await.unwrap());
        cellar_node::stop(restored, cellar::sdk::ShutdownMode::Preserve).await.unwrap();
    });
}
