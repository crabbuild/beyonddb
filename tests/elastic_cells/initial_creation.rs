//! Recover an installed but unpublished two-cell plan through native ownership.
use crate::{
    cellar_node,
    directory_fixture::{ACCOUNT, application},
};
use bdb_cellar_runtime::client::CellClient;
use beyonddb::*;
use extenddb_core::{
    expression::ExpressionMaps,
    types::{AttributeValue, Item, TableStatus},
};
use extenddb_storage::{BoxedFuture, DataEngine, TableEngine, error::StorageError};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

struct FailOnceProvisioner {
    inner: Arc<dyn InitialPartitionProvisioner>,
    failed: AtomicBool,
    installed: Mutex<Vec<PartitionSpec>>,
}
impl InitialPartitionProvisioner for FailOnceProvisioner {
    fn initial_partition_count(&self) -> u16 {
        self.inner.initial_partition_count()
    }
    fn provision_route_directory<'a>(
        &'a self,
        client: &'a CellClient,
        account: &'a str,
        id: &'a str,
        ranges: Vec<RoutePagePartition>,
    ) -> BoxedFuture<'a, Result<DirectoryCopyReceipt, StorageError>> {
        self.inner
            .provision_route_directory(client, account, id, ranges)
    }
    fn provision_global_index<'a>(
        &'a self,
        client: &'a CellClient,
        account: &'a str,
        table: &'a TableRecord,
        index: &'a GlobalIndexRecord,
    ) -> BoxedFuture<'a, Result<Vec<GlobalIndexPartitionSpec>, StorageError>> {
        self.inner
            .provision_global_index(client, account, table, index)
    }
    fn provision<'a>(
        &'a self,
        client: &'a CellClient,
        account: &'a str,
        table: &'a TableRecord,
    ) -> BoxedFuture<'a, Result<Vec<PartitionSpec>, StorageError>> {
        Box::pin(async move {
            let specs = self.inner.provision(client, account, table).await?;
            *self.installed.lock().unwrap() = specs.clone();
            if !self.failed.swap(true, Ordering::SeqCst) {
                return Err(StorageError::Transient(
                    "interrupted after data installation".into(),
                ));
            }
            Ok(specs)
        })
    }
}

#[test]
fn create_table_retries_two_data_cells_before_reporting_active() {
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
        let app = application();
        let files = tempfile::tempdir().unwrap();
        let bucket =
            cellar::bucket::Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let first = files.path().join("create-owner-a");
        let (node, client) = cellar_node::start(
            app.clone(),
            &first,
            host.handle(),
            core.handle(),
            bucket.clone(),
        )
        .await
        .unwrap();
        let interrupted = Arc::new(FailOnceProvisioner {
            inner: Arc::new(
                CellInitialPartitionProvisioner::new(client.clone(), app.clone())
                    .with_initial_partition_count(2)
                    .unwrap(),
            ),
            failed: AtomicBool::new(false),
            installed: Mutex::new(Vec::new()),
        });
        let storage =
            CellStorage::new(client.clone(), "local").with_initial_partitions(interrupted.clone());
        let input = || {
            serde_json::from_value(serde_json::json!({
                "TableName":"Books", "BillingMode":"PAY_PER_REQUEST",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]
            }))
            .unwrap()
        };
        assert!(matches!(
            storage.create_table(ACCOUNT, input()).await,
            Err(StorageError::Transient(_))
        ));
        let installed = interrupted.installed.lock().unwrap().clone();
        assert_eq!(installed.len(), 2);
        assert_eq!(installed[0].upper, installed[1].lower);
        assert!(installed[0].lower.is_none() && installed[1].upper.is_none());
        let account = account_target(ACCOUNT).unwrap();
        let record = client
            .query::<DescribeTable>(&account, None, Json("Books".into()))
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        assert_eq!(
            storage
                .describe_table(
                    ACCOUNT,
                    serde_json::from_value(serde_json::json!({"TableName":"Books"})).unwrap()
                )
                .await
                .unwrap()
                .table_status,
            TableStatus::Creating
        );
        assert!(matches!(
            storage.table_key_info(ACCOUNT, "Books").await,
            Err(StorageError::TableNotActive(_))
        ));
        assert!(matches!(
            read_route_page(
                &client,
                &account,
                RoutePageInput {
                    table_id: record.id.clone(),
                    start_key: None,
                    after_lower: None,
                    expected_epoch: None,
                }
            )
            .await
            .unwrap(),
            RoutePageOutcome::Unrouted
        ));
        let targets: Vec<_> = installed
            .iter()
            .map(|part| data_target(ACCOUNT, &record.id, &part.partition_id).unwrap())
            .collect();
        assert_ne!(targets[0].cell_id(), targets[1].cell_id());
        for (part, target) in installed.iter().zip(&targets) {
            assert_eq!(
                client
                    .query::<ReadPartitionState>(target, None, Json(()))
                    .await
                    .unwrap()
                    .output
                    .0
                    .unwrap()
                    .spec,
                *part
            );
        }
        // Recover before route publication, including the durable CREATING record.
        let second = files.path().join("create-owner-b");
        let (next_node, next) = cellar_node::start(
            app.clone(),
            &second,
            host.handle(),
            core.handle(),
            bucket.clone(),
        )
        .await
        .unwrap();
        cellar_node::stop(node, cellar::sdk::ShutdownMode::Handoff)
            .await
            .unwrap();
        std::fs::remove_dir_all(first).unwrap();
        // Changing the server default cannot change a generation's recorded plan.
        let provisioner = Arc::new(
            CellInitialPartitionProvisioner::new(next.clone(), app.clone())
                .with_initial_partition_count(3)
                .unwrap(),
        );
        let storage =
            CellStorage::new(next.clone(), "local").with_initial_partitions(provisioner.clone());
        let created = storage.create_table(ACCOUNT, input()).await.unwrap();
        assert_eq!(created.table_id, record.id);
        assert_eq!(created.table_status, TableStatus::Active);
        assert_eq!(
            provisioner
                .provision(&next, ACCOUNT, &record)
                .await
                .unwrap(),
            installed
        );
        let RoutePageOutcome::Page {
            partitions,
            has_more: false,
            ..
        } = read_route_page(
            &next,
            &account,
            RoutePageInput {
                table_id: record.id.clone(),
                start_key: None,
                after_lower: None,
                expected_epoch: None,
            },
        )
        .await
        .unwrap()
        else {
            panic!("missing published route")
        };
        assert_eq!(partitions.len(), 2);
        for (route, part) in partitions.iter().zip(&installed) {
            assert_eq!(route.partition_id, part.partition_id);
            assert_eq!(
                route.lower,
                part.lower
                    .clone()
                    .unwrap_or_else(bdb_keyspace::RangeKey::minimum)
            );
            assert_eq!(route.upper, part.upper);
            assert_eq!(route.epoch, part.epoch);
        }
        let info = storage.table_key_info(ACCOUNT, "Books").await.unwrap();
        // Keys deliberately straddle the ordered U+0080 bootstrap fence.
        let key = |half: usize, number: usize| {
            Item::from([(
                "id".into(),
                AttributeValue::S(format!(
                    "{}{number:04}",
                    if half == 0 { "book-" } else { "\u{100}-" }
                )),
            )])
        };
        let mut expected = Vec::new();
        let payload = "x".repeat(300_000);
        for number in 0..120 {
            for (half, part) in installed.iter().enumerate() {
                let mut item = key(half, number);
                let ordered = ordered_data_key(&record.id, &item, &record.key_schema).unwrap();
                assert!(
                    part.lower.as_ref().is_none_or(|lower| &ordered >= lower)
                        && part.upper.as_ref().is_none_or(|upper| &ordered < upper)
                );
                item.insert("payload".into(), AttributeValue::S(payload.clone()));
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
                if number == 0 {
                    expected.push(item);
                }
            }
        }
        let mut usage = Vec::new();
        for target in &targets {
            usage.push(
                next.query::<PartitionUsage>(target, None, Json(()))
                    .await
                    .unwrap()
                    .output
                    .0
                    .item_bytes,
            );
        }
        assert!(usage.iter().sum::<u64>() > 64 * 1024 * 1024);
        let (restored, last) = cellar_node::start(
            app.clone(),
            &files.path().join("create-owner-c"),
            host.handle(),
            core.handle(),
            bucket,
        )
        .await
        .unwrap();
        cellar_node::stop(next_node, cellar::sdk::ShutdownMode::Handoff)
            .await
            .unwrap();
        std::fs::remove_dir_all(second).unwrap();
        let provisioner = CellInitialPartitionProvisioner::new(last.clone(), app)
            .with_initial_partition_count(4)
            .unwrap();
        assert_eq!(
            provisioner
                .provision(&last, ACCOUNT, &record)
                .await
                .unwrap(),
            installed
        );
        let storage = CellStorage::new(last.clone(), "local");
        for (half, target) in targets.iter().enumerate() {
            assert_eq!(
                last.query::<PartitionUsage>(target, None, Json(()))
                    .await
                    .unwrap()
                    .output
                    .0
                    .item_bytes,
                usage[half]
            );
            assert_eq!(
                storage.get_item(&info, &key(half, 0)).await.unwrap(),
                Some(expected[half].clone())
            );
        }
        assert!(restored.is_ready().await.unwrap());
        cellar_node::stop(restored, cellar::sdk::ShutdownMode::Preserve)
            .await
            .unwrap();
    });
}
