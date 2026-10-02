use crate::*;
use extenddb_core::types::DeleteTableInput;

const ACCOUNT: &str = "123456789012";
// One account and two indexed tables, each with data, index and both directory roots.
const CELL_CAPACITY: usize = 9;

struct ReleaseRetiredDirectory {
    handle: cellule_runtime::cell::actor::CellHandle,
    client: CellClient,
    target: cellule_runtime::identity::CellTarget,
    spec: beyonddb::DirectorySpec,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl cellule_runtime::client::LocalCellResolver for ReleaseRetiredDirectory {
    fn resolve(
        &self,
        _: cellule_runtime::identity::CellTarget,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = cellule_runtime::Result<
                        Option<cellule_runtime::cell::actor::CellHandle>,
                    >,
                > + Send,
        >,
    > {
        let handle = self.handle.clone();
        let client = self.client.clone();
        let target = self.target.clone();
        let spec = self.spec.clone();
        let release = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1;
        Box::pin(async move {
            if release {
                // The first controller call discovers the pending root. Its
                // next call follows the durable retirement receipt: release
                // that exact root before either confirmation or acknowledgement.
                let state = client
                    .query::<beyonddb::ReadDirectory>(&target, None, Json(()))
                    .await
                    .unwrap()
                    .output
                    .0
                    .unwrap();
                assert_eq!(state.spec, spec);
                assert_eq!(state.mode, beyonddb::DirectoryMode::Retired);
                handle.drain().await?;
            }
            Ok(None)
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deletion_acknowledges_retirement_after_directory_owner_release() {
    let application = Arc::new(
        Beyonddb::compile(BuildDescriptor {
            source_revision: "retirement-receipt-gap".into(),
            cargo_lock_digest: Digest::from_bytes([1; 32]),
        })
        .unwrap(),
    );
    let files = tempfile::tempdir().unwrap();
    let account = account_target(ACCOUNT).unwrap();
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        object_store::path::Path::from("retirement-receipt-gap"),
        *account.application().as_bytes(),
    );
    let session = SessionId::from_bytes([199; 16]);
    let host = CellNodeBuilder::new(application.clone())
        .with_runtime(
            SqlWorkerPool::new(1, CELL_CAPACITY).unwrap(),
            16 * 1024 * 1024,
        )
        .with_replica_host(Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)))
        .with_session(session)
        .build_unleased_for_maintenance()
        .unwrap();
    let provisioner = Arc::new(
        CellInitialPartitionProvisioner::new(
            host.runtime(),
            application.clone(),
            layout.clone(),
            session,
            "https://retirement-gap.internal".into(),
            files.path().into(),
        )
        .unwrap(),
    );
    provisioner.admit_account(ACCOUNT).await.unwrap();
    let client = CellClient::local_runtime(application.registry(), host.runtime(), layout.clone());
    let storage =
        CellStorage::new(client.clone(), "us-east-1").with_initial_partitions(provisioner.clone());
    storage
        .create_table(ACCOUNT, table("RetirementGap"))
        .await
        .unwrap();
    let record = client
        .query::<DescribeTable>(&account, None, Json("RetirementGap".into()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    let spec = beyonddb::DirectorySpec::root(record.id.clone());
    let target = beyonddb::directory_target(ACCOUNT, &spec).unwrap();
    let handle = provisioner
        .admit_existing_directory(ACCOUNT, &spec)
        .await
        .unwrap();
    storage
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: "RetirementGap".into(),
            },
        )
        .await
        .unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let controlled = client
        .clone()
        .with_local_resolver(Arc::new(ReleaseRetiredDirectory {
            client: CellClient::local(application.registry(), handle.clone()),
            handle,
            target: target.clone(),
            spec,
            calls: calls.clone(),
        }));
    let result = provisioner
        .continue_table_deletion(&controlled, ACCOUNT, &record.id)
        .await;
    assert!(calls.load(std::sync::atomic::Ordering::SeqCst) >= 2);
    let control = CellAuthority::new(layout.clone())
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert!(control.value().owner.is_none());
    assert!(control.value().root.is_some());
    // Even after release, the account must acknowledge this root and discover
    // the independent index directory for the next bounded controller step.
    let pending = client
        .query::<beyonddb::ReadPendingDirectoryRetirement>(&account, None, Json(record.id.clone()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    host.shutdown().await.unwrap();
    assert!(
        result.is_ok(),
        "retired directory residency blocked acknowledgement: {result:?}"
    );
    assert_eq!(pending.spec.table_id, record.global_secondary_indexes[0].id);
}

fn table(name: &str) -> extenddb_core::types::CreateTableInput {
    serde_json::from_value(serde_json::json!({
        "TableName": name,
        "BillingMode": "PAY_PER_REQUEST",
        "KeySchema": [{"AttributeName":"id", "KeyType":"HASH"}],
        "AttributeDefinitions": [{"AttributeName":"id", "AttributeType":"S"}],
        "GlobalSecondaryIndexes": [{
            "IndexName":"ById",
            "KeySchema":[{"AttributeName":"id", "KeyType":"HASH"}],
            "Projection":{"ProjectionType":"ALL"}
        }]
    }))
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleted_table_ranges_release_residency_without_releasing_live_ranges() {
    let application = Arc::new(
        Beyonddb::compile(BuildDescriptor {
            source_revision: "table-residency".into(),
            cargo_lock_digest: Digest::from_bytes([1; 32]),
        })
        .unwrap(),
    );
    let directory = tempfile::tempdir().unwrap();
    let account = account_target(ACCOUNT).unwrap();
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        object_store::path::Path::from("table-residency"),
        *account.application().as_bytes(),
    );
    let session = SessionId::from_bytes([198; 16]);
    let host = CellNodeBuilder::new(application.clone())
        .with_runtime(
            SqlWorkerPool::new(1, CELL_CAPACITY).unwrap(),
            16 * 1024 * 1024,
        )
        .with_replica_host(Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)))
        .with_session(session)
        .build_unleased_for_maintenance()
        .unwrap();
    let provisioner = Arc::new(
        CellInitialPartitionProvisioner::new(
            host.runtime(),
            application.clone(),
            layout.clone(),
            session,
            "https://table-residency.internal".into(),
            directory.path().into(),
        )
        .unwrap(),
    );
    provisioner.admit_account(ACCOUNT).await.unwrap();
    let client = CellClient::local_runtime(application.registry(), host.runtime(), layout.clone());
    let storage =
        CellStorage::new(client.clone(), "us-east-1").with_initial_partitions(provisioner.clone());
    storage
        .create_table(ACCOUNT, table("KeepAlive"))
        .await
        .unwrap();
    let live = storage.table_key_info(ACCOUNT, "KeepAlive").await.unwrap();
    let item = Item::from([("id".into(), AttributeValue::S("present".into()))]);
    storage
        .put_item(
            &live,
            item.clone(),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .unwrap();
    let authority = CellAuthority::new(layout.clone());
    let mut retired = Vec::new();
    let mut history = None;
    for _ in 0..4 {
        storage
            .create_table(ACCOUNT, table("Recreated"))
            .await
            .unwrap();
        for target in &retired {
            let control = authority.load(*target).await.unwrap().unwrap();
            assert!(control.value().owner.is_none());
            assert!(
                control.value().root.is_some(),
                "reclamation must retain published history"
            );
        }
        assert_eq!(
            storage.get_item(&live, &item).await.unwrap(),
            Some(item.clone())
        );
        let record = client
            .query::<DescribeTable>(&account, None, Json("Recreated".into()))
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        let range = crate::single_leaf_route(&client, &account, &record.id.clone())
            .await
            .unwrap()
            .partitions
            .remove(0);
        let info = storage.table_key_info(ACCOUNT, "Recreated").await.unwrap();
        assert!(storage.get_item(&info, &item).await.unwrap().is_none());
        storage
            .put_item(
                &info,
                item.clone(),
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .unwrap();
        history.get_or_insert((record.clone(), range.clone()));
        retired = vec![
            data_target(ACCOUNT, &record.id, &range.partition_id)
                .unwrap()
                .cell_id(),
            beyonddb::global_index_target(
                ACCOUNT,
                &record.global_secondary_indexes[0].id,
                &range.partition_id,
            )
            .unwrap()
            .cell_id(),
        ];
        assert_eq!(host.runtime().stats().active_cells(), CELL_CAPACITY);
        storage
            .delete_table(
                ACCOUNT,
                DeleteTableInput {
                    table_name: "Recreated".into(),
                },
            )
            .await
            .unwrap();
        // This unleased fixture drives the same bounded controller as serving
        // maintenance. Directory retirement must finish before the name is free.
        let mut cursor = None;
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                let lifecycle = client
                    .query::<beyonddb::ReadTableLifecycle>(&account, None, Json("Recreated".into()))
                    .await
                    .unwrap()
                    .output
                    .0;
                match lifecycle {
                    beyonddb::TableLifecycle::Missing => break,
                    beyonddb::TableLifecycle::Deleting(_) => {}
                    beyonddb::TableLifecycle::Live(_) => panic!("deleted generation is still live"),
                }
                Box::pin(provisioner.reconcile_account_capacity(
                    ACCOUNT,
                    client.clone(),
                    u64::MAX,
                    &mut cursor,
                ))
                .await
                .unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("directory retirement did not complete");
    }
    // Recovery can still restore an original participant after its public
    // table name has been deleted and reused for other generations.
    let (record, range) = history.unwrap();
    provisioner
        .provision(&client, ACCOUNT, &record)
        .await
        .unwrap();
    let target = data_target(ACCOUNT, &record.id, &range.partition_id).unwrap();
    let restored = client
        .query::<PartitionGet>(
            &target,
            None,
            Json(PartitionGetInput {
                table_id: record.id,
                epoch: range.epoch,
                key: item.clone(),
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    assert_eq!(restored, PartitionGetOutcome::Found(Some(item)));
    host.shutdown().await.unwrap();
}
