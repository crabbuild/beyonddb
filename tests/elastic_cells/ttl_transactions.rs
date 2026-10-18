use crate::{local_node, transaction_fixture};
use beyonddb::*;
use beyonddb::{ExpiredPartitionInput, ExpiredPartitionOutcome, ReadExpiredPartition};
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use cellar_runtime::{
    MutationIdentity,
    client::{CellClient, Invocation, InvocationTransport},
    identity::CellTarget,
    registry::Query,
};
use extenddb_core::types::TableKeyInfo;
use extenddb_core::{
    expression::ExpressionMaps,
    types::{AttributeValue, Item},
};
use extenddb_storage::{DataEngine, MetadataEngine, TableEngine};
use http_body_util::{BodyExt, Full};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
};

fn mutation() -> MutationIdentity {
    local_node::wall_mutation()
}

fn key(id: &str) -> Item {
    Item::from([("id".into(), AttributeValue::S(id.into()))])
}

async fn seed(storage: &CellStorage, info: &TableKeyInfo, id: &str, expires: u64) {
    let mut item = key(id);
    item.insert("expires".into(), AttributeValue::N(expires.to_string()));
    storage
        .put_item(info, item, false, None, &ExpressionMaps::default(), None)
        .await
        .unwrap();
}

async fn begin(
    client: &CellClient,
    coordinator: &CellTarget,
    info: &TableKeyInfo,
    spec: &PartitionSpec,
    transaction_id: [u8; 16],
    operation: TransactionOperation,
) -> PreparePartitionTransactionInput {
    transaction_command!(
        client,
        BeginCrossCellTransaction,
        coordinator,
        mutation(),
        Json(BeginCrossCellTransactionInput {
            account_id: info.account_id.clone(),
            transaction_id,
            token: None,
            participants: vec![CoordinatorParticipant {
                target: CoordinatorParticipantTarget::Data {
                    table_id: info.table_id.clone(),
                    partition_id: spec.partition_id,
                    epoch: spec.epoch,
                },
                operations: vec![IndexedTransactionOperation {
                    index: 0,
                    operation: operation.clone(),
                }],
            }],
        }),
    )
    .await
    .unwrap();
    PreparePartitionTransactionInput {
        table_id: info.table_id.clone(),
        epoch: spec.epoch,
        transaction_id,
        coordinator_cell: *coordinator.cell_id().as_bytes(),
        coordinator_key: transaction_id.to_vec(),
        operations: vec![operation],
    }
}

#[test]
fn ttl_skips_transaction_locks_and_revisits_after_abort() {
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
        let account_id = "123456789012";
        let directory = tempfile::tempdir().unwrap();
        let clock = Arc::new(AtomicI64::new(mutation().issued_at_ms));
        let (node, client) =
            local_node::start(directory.path(), clock, host.handle(), core.handle())
                .await
                .unwrap();
        let provisioner = Arc::new(
            CellInitialPartitionProvisioner::new(
                client.clone(),
                local_node::application().unwrap(),
            )
            .with_initial_partition_count(2)
            .unwrap(),
        );
        let storage =
            CellStorage::new(client.clone(), "local").with_initial_partitions(provisioner.clone());
        let mut infos = Vec::new();
        for name in ["TtlBlocked", "TtlHealthy"] {
            let input = serde_json::from_value(serde_json::json!({
                "TableName": name,
                "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }))
            .unwrap();
            storage.create_table(account_id, input).await.unwrap();
            infos.push(storage.table_key_info(account_id, name).await.unwrap());
            storage
                .update_ttl(account_id, name, "expires", true)
                .await
                .unwrap();
            storage
                .create_ttl_index(account_id, name, "expires")
                .await
                .unwrap();
        }
        let info = &infos[0];
        let (data, _) =
            transaction_fixture::participant_for_key(&client, info, &key("shared")).await;
        let state = client
            .query::<ReadPartitionState>(&data, None, Json(()))
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        let spec = &state.spec;
        for id in ["exclusive", "healthy", "raced", "race-sibling"] {
            assert_eq!(
                transaction_fixture::participant_for_key(&client, info, &key(id))
                    .await
                    .0,
                data
            );
        }
        for (id, expires) in [("shared", 1), ("exclusive", 2), ("healthy", 3)] {
            seed(&storage, info, id, expires).await;
        }
        seed(&storage, &infos[1], "later-table", 1).await;
        // Use distinct IDs in one shard so unrelated admission pressure cannot
        // mask whether the transaction locks starve TTL candidates.
        let first_id = [239; 16];
        provisioner
            .ensure(&client, account_id, &first_id)
            .await
            .unwrap();
        let coordinator = coordinator_target(account_id, &first_id).unwrap();
        let ids: Vec<_> = (1_u128..100_000)
            .map(u128::to_be_bytes)
            .filter(|id| coordinator_target(account_id, id).unwrap() == coordinator)
            .take(3)
            .collect();
        assert_eq!(ids.len(), 3);
        let mut prepares = Vec::new();
        for (id, name, shared) in [(ids[0], "shared", true), (ids[1], "exclusive", false)] {
            let operation = if shared {
                TransactionOperation::Read(GetItemInput {
                    table_name: info.table_name.clone(),
                    table_id: info.table_id.clone(),
                    key: key(name),
                })
            } else {
                TransactionOperation::Put(PutItemInput {
                    table_name: info.table_name.clone(),
                    table_id: info.table_id.clone(),
                    item: key(name),
                    condition: None,
                })
            };
            let prepare = begin(&client, &coordinator, info, spec, id, operation).await;
            transaction_command!(
                client,
                PreparePartitionTransaction,
                &data,
                mutation(),
                Json(prepare.clone())
            )
            .await
            .unwrap();
            prepares.push(prepare);
        }
        // Both oldest expired rows are locked, in different modes. Selection must
        // still reach the third row, and the sweep must visit the following table.
        let candidates = client
            .query::<ReadExpiredPartition>(
                &data,
                None,
                Json(ExpiredPartitionInput {
                    table_id: info.table_id.clone(),
                    epoch: spec.epoch,
                    attribute_name: "expires".into(),
                    cutoff_epoch: 10,
                }),
            )
            .await
            .unwrap()
            .output
            .0;
        let ExpiredPartitionOutcome::Items(candidates) = candidates else {
            panic!("TTL was not ready")
        };
        assert_eq!(
            candidates
                .iter()
                .map(|item| item["id"].clone())
                .collect::<Vec<_>>(),
            vec![AttributeValue::S("healthy".into())]
        );
        assert_eq!(storage.sweep_account_ttl(account_id).await.unwrap(), 2);

        seed(&storage, info, "raced", 3).await;
        seed(&storage, info, "race-sibling", 4).await;
        seed(&storage, &infos[1], "later-table", 1).await;
        let prepare = begin(
            &client,
            &coordinator,
            info,
            spec,
            ids[2],
            TransactionOperation::Read(GetItemInput {
                table_name: info.table_name.clone(),
                table_id: info.table_id.clone(),
                key: key("raced"),
            }),
        )
        .await;
        prepares.push(prepare.clone());
        let raced = Arc::new(AtomicBool::new(false));
        let routed = client.map_transport(|inner| {
            Arc::new(LockAfterCandidates {
                inner,
                client: client.clone(),
                target: data.clone(),
                prepare,
                raced: raced.clone(),
            })
        });
        assert_eq!(
            CellStorage::new(routed, "us-east-1")
                .sweep_account_ttl(account_id)
                .await
                .unwrap(),
            2
        );
        assert!(raced.load(Ordering::SeqCst));
        assert!(
            storage
                .get_item(info, &key("raced"))
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            storage
                .get_item(info, &key("race-sibling"))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            storage
                .get_item(&infos[1], &key("later-table"))
                .await
                .unwrap()
                .is_none()
        );

        for prepare in prepares {
            client
                .command::<DecideCrossCellTransaction>(
                    &coordinator,
                    mutation(),
                    Json(DecideCrossCellTransactionInput {
                        account_id: account_id.into(),
                        transaction_id: prepare.transaction_id,
                        routing_key: prepare.coordinator_key.clone(),
                        decision: CoordinatorDecision::Abort {
                            index: None,
                            reason: None,
                        },
                    }),
                )
                .await
                .unwrap();
            storage
                .finish_decided_cross_cell_transaction(
                    account_id,
                    &prepare.coordinator_key,
                    prepare.transaction_id,
                )
                .await
                .unwrap();
        }
        // Deferred items remain indexed. Subsequent passes delete them after the
        // authoritative abort releases the locks; none was lost behind a cursor.
        let deleted = storage.sweep_account_ttl(account_id).await.unwrap()
            + storage.sweep_account_ttl(account_id).await.unwrap();
        assert_eq!(deleted, 3);
        for id in ["shared", "exclusive", "raced"] {
            assert!(storage.get_item(info, &key(id)).await.unwrap().is_none());
        }
        local_node::stop(node).await.unwrap();
    });
}

struct LockAfterCandidates {
    inner: Arc<dyn InvocationTransport>,
    client: CellClient,
    target: CellTarget,
    prepare: PreparePartitionTransactionInput,
    raced: Arc<AtomicBool>,
}

impl InvocationTransport for LockAfterCandidates {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            let response = self.inner.call(invocation, request).await;
            if invocation.target == self.target
                && invocation.operation == ReadExpiredPartition::ID
                && !self.raced.swap(true, Ordering::SeqCst)
            {
                assert_eq!(response.status(), http::StatusCode::OK);
                // Finish the actual SDK query before taking the read lock,
                // then return its unchanged candidate page to the TTL worker.
                let (parts, body) = response.into_parts();
                let bytes = body.collect().await.unwrap().to_bytes();
                transaction_command!(
                    self.client,
                    PreparePartitionTransaction,
                    &self.target,
                    mutation(),
                    Json(self.prepare.clone())
                )
                .await
                .unwrap();
                let body = Full::new(bytes)
                    .map_err(|never| -> std::io::Error { match never {} })
                    .boxed_unsync();
                http::Response::from_parts(parts, body)
            } else {
                response
            }
        })
    }
}
