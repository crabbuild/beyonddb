//! Table generation retirement remains durable while Cellar bounds residency.
use crate::{
    cellar_node,
    directory_fixture::{ACCOUNT, application},
};
use bdb_cellar_runtime::{
    client::{CellClient, Invocation, InvocationTransport},
    registry::Command,
};
use beyonddb::*;
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use extenddb_core::{
    expression::ExpressionMaps,
    types::{AttributeValue, DeleteTableInput, Item},
};
use extenddb_storage::{DataEngine, TableEngine};
use http_body_util::{BodyExt, Full};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

const CELL_CAPACITY: usize = 16;

fn table(name: &str) -> extenddb_core::types::CreateTableInput {
    serde_json::from_value(serde_json::json!({
        "TableName": name, "BillingMode": "PAY_PER_REQUEST",
        "KeySchema": [{"AttributeName":"id", "KeyType":"HASH"}],
        "AttributeDefinitions": [{"AttributeName":"id", "AttributeType":"S"}],
        "GlobalSecondaryIndexes": [{"IndexName":"ById",
            "KeySchema":[{"AttributeName":"id", "KeyType":"HASH"}],
            "Projection":{"ProjectionType":"ALL"}}]
    }))
    .unwrap()
}
fn provisioner(client: &CellClient) -> Arc<CellInitialPartitionProvisioner> {
    Arc::new(
        CellInitialPartitionProvisioner::new(client.clone(), application())
            .with_initial_partition_count(2)
            .unwrap(),
    )
}
async fn record(client: &CellClient, name: &str) -> TableRecord {
    client
        .query::<DescribeTable>(&account_target(ACCOUNT).unwrap(), None, Json(name.into()))
        .await
        .unwrap()
        .output
        .0
        .unwrap()
}
async fn ranges(client: &CellClient, id: &str) -> Vec<RoutePagePartition> {
    let RoutePageOutcome::Page {
        partitions,
        has_more: false,
        ..
    } = read_route_page(
        client,
        &account_target(ACCOUNT).unwrap(),
        RoutePageInput {
            table_id: id.into(),
            start_key: None,
            after_lower: None,
            expected_epoch: None,
        },
    )
    .await
    .unwrap()
    else {
        panic!("missing two-cell directory")
    };
    assert_eq!(partitions.len(), 2);
    partitions
}
async fn finish_deletion(client: &CellClient, table: &TableRecord) {
    let creator = provisioner(client);
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let lifecycle = client
                .query::<ReadTableLifecycle>(
                    &account_target(ACCOUNT).unwrap(),
                    None,
                    Json(table.table_name.clone()),
                )
                .await
                .unwrap()
                .output
                .0;
            match lifecycle {
                TableLifecycle::Missing => break,
                TableLifecycle::Deleting(current) => assert_eq!(current.id, table.id),
                TableLifecycle::Live(_) => panic!("deleted generation is live"),
            }
            creator
                .continue_table_deletion(client, ACCOUNT, &table.id)
                .await
                .unwrap();
        }
    })
    .await
    .expect("directory retirement did not complete");
}

struct LoseRetirementReply {
    inner: Arc<dyn InvocationTransport>,
    dropped: Arc<AtomicBool>,
}
impl InvocationTransport for LoseRetirementReply {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            let response = self.inner.call(invocation, request).await;
            if invocation.operation == RetireDirectory::ID
                && invocation.mutation.is_some()
                && !self.dropped.swap(true, Ordering::SeqCst)
            {
                assert_eq!(response.status(), http::StatusCode::OK);
                // Observe the SDK's durable body before losing the reply.
                response.into_body().collect().await.unwrap();
                let body = Full::new(Bytes::new())
                    .map_err(|never| -> std::io::Error { match never {} })
                    .boxed_unsync();
                let mut response = http::Response::new(body);
                *response.status_mut() = http::StatusCode::SERVICE_UNAVAILABLE;
                response
            } else {
                response
            }
        })
    }
}

#[test]
fn deletion_recovers_retirement_before_acknowledgement_on_a_fresh_owner() {
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
        let files = tempfile::tempdir().unwrap();
        let first = files.path().join("retirement-owner-a");
        let bucket =
            cellar::bucket::Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let (node, client) = cellar_node::start_limited(
            application(),
            &first,
            host.handle(),
            core.handle(),
            bucket.clone(),
            CELL_CAPACITY,
        )
        .await
        .unwrap();
        let creator = provisioner(&client);
        let storage =
            CellStorage::new(client.clone(), "local").with_initial_partitions(creator.clone());
        storage
            .create_table(ACCOUNT, table("RetirementGap"))
            .await
            .unwrap();
        let table = record(&client, "RetirementGap").await;
        ranges(&client, &table.id).await;
        ranges(&client, &table.global_secondary_indexes[0].id).await;
        storage
            .delete_table(
                ACCOUNT,
                DeleteTableInput {
                    table_name: table.table_name.clone(),
                },
            )
            .await
            .unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let fault = client.map_transport(|inner| {
            Arc::new(LoseRetirementReply {
                inner,
                dropped: dropped.clone(),
            })
        });
        assert!(
            creator
                .continue_table_deletion(&fault, ACCOUNT, &table.id)
                .await
                .is_err()
        );
        assert!(dropped.load(Ordering::SeqCst));
        let spec = DirectorySpec::root(table.id.clone());
        let target = directory_target(ACCOUNT, &spec).unwrap();
        let receipt = client
            .query::<ReadDirectory>(&target, None, Json(()))
            .await
            .unwrap();
        assert_eq!(receipt.output.0.unwrap().mode, DirectoryMode::Retired);
        let pending = client
            .query::<ReadPendingDirectoryRetirement>(
                &account_target(ACCOUNT).unwrap(),
                None,
                Json(table.id.clone()),
            )
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        assert_eq!(pending.spec, spec);
        let (successor, next) = cellar_node::start_limited(
            application(),
            &files.path().join("retirement-owner-b"),
            host.handle(),
            core.handle(),
            bucket.clone(),
            CELL_CAPACITY,
        )
        .await
        .unwrap();
        cellar_node::stop(node, cellar::sdk::ShutdownMode::Handoff)
            .await
            .unwrap();
        std::fs::remove_dir_all(first).unwrap();
        let recovered = next
            .query::<ReadDirectory>(&target, None, Json(()))
            .await
            .unwrap();
        assert_eq!(
            recovered.receipt.commit_sequence,
            receipt.receipt.commit_sequence
        );
        assert_eq!(recovered.output.0.unwrap().mode, DirectoryMode::Retired);
        provisioner(&next)
            .continue_table_deletion(&next, ACCOUNT, &table.id)
            .await
            .unwrap();
        let pending = next
            .query::<ReadPendingDirectoryRetirement>(
                &account_target(ACCOUNT).unwrap(),
                None,
                Json(table.id.clone()),
            )
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        assert_eq!(pending.spec.table_id, table.global_secondary_indexes[0].id);
        finish_deletion(&next, &table).await;
        // Handoff restores existing cells before the departing node's lease
        // necessarily expires. Fresh inventory shards may still place there.
        // Observe the lease fence and its native placement sample before new cells.
        let observer = cellar::ownership_store::BucketOwnership::new(
            bucket.clone(),
            bucket,
            "retirement-observer".into(),
            String::new(),
        );
        let inspection = cellar::sdk::ExecutionDomain::new(host.handle().clone());
        inspection
            .run(async {
                tokio::time::timeout(std::time::Duration::from_secs(20), async {
                    let expired_at = loop {
                        let lease = observer
                            .read_node_lease("retirement-owner-a")
                            .await
                            .unwrap();
                        let now = cellar::ownership_store::now_ms();
                        if lease.as_ref().is_none_or(|record| record.expires_ms <= now) {
                            break lease.map_or(now, |record| record.expires_ms);
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    };
                    // Placement uses the SDK's shared sampled view. Its normal
                    // refresh must also observe expiry; direct lease expiry
                    // alone does not invalidate an earlier placement sample.
                    loop {
                        if let Ok((sampled_at, peers)) = observer.read_shared_capacity_view().await
                            && sampled_at >= expired_at
                            && peers.iter().all(|peer| {
                                peer.node != "retirement-owner-a" || peer.expires_ms <= sampled_at
                            })
                            && peers.iter().any(|peer| {
                                peer.node == "retirement-owner-b" && peer.expires_ms > sampled_at
                            })
                        {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                })
                .await
                .expect("native placement view did not observe departure");
            })
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), inspection.shutdown())
            .await
            .unwrap();
        let storage =
            CellStorage::new(next.clone(), "local").with_initial_partitions(provisioner(&next));
        storage
            .create_table(ACCOUNT, self::table("RetirementGap"))
            .await
            .unwrap();
        assert_ne!(record(&next, "RetirementGap").await.id, table.id);
        assert_eq!(
            next.query::<ReadDirectory>(&target, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap()
                .mode,
            DirectoryMode::Retired
        );
        cellar_node::stop(successor, cellar::sdk::ShutdownMode::Preserve)
            .await
            .unwrap();
    });
}

#[test]
fn deleted_table_generations_preserve_history_under_bounded_sdk_residency() {
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
        let files = tempfile::tempdir().unwrap();
        let first = files.path().join("table-residency-owner-a");
        let bucket =
            cellar::bucket::Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let (node, client) = cellar_node::start_limited(
            application(),
            &first,
            host.handle(),
            core.handle(),
            bucket.clone(),
            CELL_CAPACITY,
        )
        .await
        .unwrap();
        let load = node.services().node_load().unwrap();
        let storage =
            CellStorage::new(client.clone(), "local").with_initial_partitions(provisioner(&client));
        storage
            .create_table(ACCOUNT, table("KeepAlive"))
            .await
            .unwrap();
        let live = storage.table_key_info(ACCOUNT, "KeepAlive").await.unwrap();
        let keys: Vec<_> = ["first", "Ω-last"]
            .into_iter()
            .map(|id| Item::from([("id".into(), AttributeValue::S(id.into()))]))
            .collect();
        for item in &keys {
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
        }
        let mut history = Vec::new();
        for _ in 0..4 {
            storage
                .create_table(ACCOUNT, table("Recreated"))
                .await
                .unwrap();
            let table = record(&client, "Recreated").await;
            assert!(
                !history
                    .iter()
                    .any(|(old, _): &(TableRecord, Vec<RoutePagePartition>)| old.id == table.id)
            );
            let base = ranges(&client, &table.id).await;
            ranges(&client, &table.global_secondary_indexes[0].id).await;
            let info = storage.table_key_info(ACCOUNT, "Recreated").await.unwrap();
            for (item, range) in keys.iter().zip(&base) {
                let key = ordered_data_key(&table.id, item, &table.key_schema).unwrap();
                assert!(
                    key >= range.lower && range.upper.as_ref().is_none_or(|upper| &key < upper)
                );
                assert!(storage.get_item(&info, item).await.unwrap().is_none());
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
                assert_eq!(
                    storage.get_item(&live, item).await.unwrap().as_ref(),
                    Some(item)
                );
            }
            storage
                .delete_table(
                    ACCOUNT,
                    DeleteTableInput {
                        table_name: table.table_name.clone(),
                    },
                )
                .await
                .unwrap();
            finish_deletion(&client, &table).await;
            history.push((table, base));
            assert!(load.resident_cells.load(Ordering::SeqCst) <= CELL_CAPACITY);
        }
        // Wait for the native sampler so the cap check is backed by observed
        // ownership pressure rather than the counters' initial zero values.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while load.owned_cells.load(Ordering::SeqCst) < 30 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("native ownership sampler did not observe five indexed generations");
        assert!(load.resident_cells.load(Ordering::SeqCst) > 0);
        assert!(load.resident_cells.load(Ordering::SeqCst) <= CELL_CAPACITY);
        // Every deleted base/GSI root remains fenced; every original data cell
        // is still recoverable despite more generations than can be resident.
        for (table, base) in &history {
            for id in [&table.id, &table.global_secondary_indexes[0].id] {
                let spec = DirectorySpec::root(id.clone());
                let target = directory_target(ACCOUNT, &spec).unwrap();
                let state = client
                    .query::<ReadDirectory>(&target, None, Json(()))
                    .await
                    .unwrap()
                    .output
                    .0
                    .unwrap();
                assert_eq!(state.spec, spec);
                assert_eq!(state.mode, DirectoryMode::Retired);
            }
            for (item, range) in keys.iter().zip(base) {
                let target = data_target(ACCOUNT, &table.id, &range.partition_id).unwrap();
                assert_eq!(
                    client
                        .query::<PartitionGet>(
                            &target,
                            None,
                            Json(PartitionGetInput {
                                table_id: table.id.clone(),
                                epoch: range.epoch,
                                key: item.clone(),
                            })
                        )
                        .await
                        .unwrap()
                        .output
                        .0,
                    PartitionGetOutcome::Found(Some(item.clone()))
                );
            }
            assert!(load.resident_cells.load(Ordering::SeqCst) <= CELL_CAPACITY);
        }
        let (successor, next) = cellar_node::start_limited(
            application(),
            &files.path().join("table-residency-owner-b"),
            host.handle(),
            core.handle(),
            bucket,
            CELL_CAPACITY,
        )
        .await
        .unwrap();
        cellar_node::stop(node, cellar::sdk::ShutdownMode::Handoff)
            .await
            .unwrap();
        std::fs::remove_dir_all(first).unwrap();
        let restored = CellStorage::new(next.clone(), "local");
        for item in &keys {
            assert_eq!(
                restored.get_item(&live, item).await.unwrap().as_ref(),
                Some(item)
            );
        }
        let load = successor.services().node_load().unwrap();
        for (table, base) in history {
            for id in [&table.id, &table.global_secondary_indexes[0].id] {
                let spec = DirectorySpec::root(id.clone());
                assert_eq!(
                    next.query::<ReadDirectory>(
                        &directory_target(ACCOUNT, &spec).unwrap(),
                        None,
                        Json(())
                    )
                    .await
                    .unwrap()
                    .output
                    .0
                    .unwrap()
                    .mode,
                    DirectoryMode::Retired
                );
            }
            for (item, range) in keys.iter().zip(base) {
                assert_eq!(
                    next.query::<PartitionGet>(
                        &data_target(ACCOUNT, &table.id, &range.partition_id).unwrap(),
                        None,
                        Json(PartitionGetInput {
                            table_id: table.id.clone(),
                            epoch: range.epoch,
                            key: item.clone(),
                        })
                    )
                    .await
                    .unwrap()
                    .output
                    .0,
                    PartitionGetOutcome::Found(Some(item.clone()))
                );
            }
            assert!(load.resident_cells.load(Ordering::SeqCst) <= CELL_CAPACITY);
        }
        assert!(successor.is_ready().await.unwrap());
        cellar_node::stop(successor, cellar::sdk::ShutdownMode::Preserve)
            .await
            .unwrap();
    });
}
