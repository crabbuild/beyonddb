//! Coordinator history and admission pressure through the native Cellar SDK.
use crate::{cellar_node, local_node, transaction_fixture};
use beyonddb::*;
use cellar_runtime::{
    client::{CellClient, Invocation, InvocationAdmission},
    identity::CellTarget,
    registry::Command,
};
use extenddb_core::{
    expression::ExpressionMaps,
    types::{AttributeValue, Item, TableKeyInfo, TableStatus},
};
use extenddb_storage::{
    DataEngine, IdempotencyKey, TableEngine, TransactWriteOp, error::StorageError,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

pub(super) const ACCOUNT: &str = "123456789012";
pub(super) const RESIDENT: usize = 16;
pub(super) fn runtimes() -> (tokio::runtime::Runtime, tokio::runtime::Runtime) {
    (
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    )
}
pub(super) fn storage(client: &CellClient, count: u16) -> CellStorage {
    let provisioner = Arc::new(
        CellInitialPartitionProvisioner::new(client.clone(), local_node::application().unwrap())
            .with_initial_partition_count(count)
            .unwrap(),
    );
    CellStorage::new(client.clone(), "local")
        .with_initial_partitions(provisioner.clone())
        .with_transaction_coordinators(provisioner)
}
pub(super) fn table(name: &str) -> extenddb_core::types::CreateTableInput {
    serde_json::from_value(serde_json::json!({
        "TableName":name,"BillingMode":"PAY_PER_REQUEST",
        "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
        "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]
    }))
    .unwrap()
}
pub(super) fn tokens(prefix: &str, count: usize) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let tokens: Vec<_> = (0..10_000)
        .map(|n| format!("{prefix}-{n}"))
        .filter(|token| {
            seen.insert(
                coordinator_target(ACCOUNT, token.as_bytes())
                    .unwrap()
                    .cell_id(),
            )
        })
        .take(count)
        .collect();
    assert_eq!(tokens.len(), count);
    tokens
}
pub(super) fn keys() -> [Item; 2] {
    ["first", "Ω-other"].map(|id| Item::from([("id".into(), AttributeValue::S(id.into()))]))
}
pub(super) async fn write(
    storage: &CellStorage,
    info: &TableKeyInfo,
    token: &str,
    version: usize,
) -> bool {
    let items = keys().map(|mut item| {
        item.insert("version".into(), AttributeValue::N(version.to_string()));
        item
    });
    let maps = ExpressionMaps::default();
    let operations: Vec<_> = items
        .iter()
        .map(|item| TransactWriteOp::Put {
            key_info: info,
            item,
            condition: None,
            maps: &maps,
            return_values_on_ccf: Default::default(),
            stream: None,
        })
        .collect();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        match storage
            .transact_write_items(
                &operations,
                Some(IdempotencyKey {
                    account_id: ACCOUNT,
                    token,
                    fingerprint: token,
                }),
            )
            .await
        {
            Ok(()) => return false,
            Err(StorageError::IdempotentReplay) => return true,
            Err(StorageError::Transient(_)) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await
            }
            Err(error) => panic!("transaction failed: {error}"),
        }
    }
}
pub(super) async fn routes(client: &CellClient, id: &str) -> Vec<RoutePagePartition> {
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
        panic!("missing bounded table routes")
    };
    partitions
}
pub(super) async fn status(client: &CellClient, id: [u8; 16]) -> CrossCellTransactionStatus {
    client
        .query::<ReadCrossCellTransaction>(
            &coordinator_target(ACCOUNT, &id).unwrap(),
            None,
            Json(ReadCrossCellTransactionInput {
                account_id: ACCOUNT.into(),
                transaction_id: id,
                routing_key: id.to_vec(),
            }),
        )
        .await
        .unwrap()
        .output
        .0
        .unwrap()
}
pub(super) async fn pending_read(
    client: &CellClient,
    fixture: &transaction_fixture::TableFixture,
    coordinator: &CellTarget,
) -> [u8; 16] {
    let id = (1_u128..100_000)
        .map(u128::to_be_bytes)
        .find(|id| coordinator_target(ACCOUNT, id).unwrap() == *coordinator)
        .unwrap();
    let participants: Vec<_> = fixture
        .participants
        .iter()
        .enumerate()
        .map(|(position, (_, part))| {
            let TransactionOperation::Put(put) = &part.operations[0].operation else {
                unreachable!()
            };
            CoordinatorParticipant {
                target: part.target.clone(),
                operations: vec![IndexedTransactionOperation {
                    index: u8::try_from(position).unwrap(),
                    operation: TransactionOperation::Read(GetItemInput {
                        table_name: fixture.table.table_name.clone(),
                        table_id: fixture.table.id.clone(),
                        key: extenddb_core::types::extract_key(
                            &put.item,
                            &fixture.table.key_schema,
                        ),
                    }),
                }],
            }
        })
        .collect();
    transaction_command!(
        client,
        BeginCrossCellTransaction,
        coordinator,
        local_node::wall_mutation(),
        Json(BeginCrossCellTransactionInput {
            account_id: ACCOUNT.into(),
            transaction_id: id,
            token: None,
            participants: participants.clone(),
        })
    )
    .await
    .unwrap();
    // Leave the other ordered range unprepared. Recovery must use the admitted
    // immutable payload, not assume that a shared PK or table is one participant.
    let target = &fixture.participants[0].0;
    let CoordinatorParticipantTarget::Data {
        table_id, epoch, ..
    } = &participants[0].target
    else {
        unreachable!()
    };
    let prepared = transaction_command!(
        client,
        PreparePartitionTransaction,
        target,
        local_node::wall_mutation(),
        Json(PreparePartitionTransactionInput {
            table_id: table_id.clone(),
            epoch: *epoch,
            transaction_id: id,
            coordinator_cell: *coordinator.cell_id().as_bytes(),
            coordinator_key: id.to_vec(),
            operations: participants[0]
                .operations
                .iter()
                .map(|op| op.operation.clone())
                .collect(),
        })
    )
    .await
    .unwrap();
    assert_eq!(prepared.output.0, PrepareTransactionOutcome::Prepared);
    assert_eq!(
        client
            .command::<RecordParticipantPrepare>(
                coordinator,
                local_node::wall_mutation(),
                Json(CoordinatorPhaseInput {
                    account_id: ACCOUNT.into(),
                    transaction_id: id,
                    routing_key: id.to_vec(),
                    position: 0,
                    participant_cell: *target.cell_id().as_bytes(),
                    sequence: prepared.receipt.commit_sequence,
                })
            )
            .await
            .unwrap()
            .output
            .0,
        CoordinatorPhaseOutcome::Recorded
    );
    id
}
pub(super) async fn recover(client: &CellClient, id: [u8; 16]) {
    let mut recovery =
        TransactionRecovery::new(Arc::new(storage(client, 2)), ACCOUNT.into()).unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let current = status(client, id).await;
            if current.resolved_count == 2 {
                assert_eq!(current.decision, CoordinatorDecision::Commit);
                break;
            }
            recovery.step().await.unwrap();
        }
    })
    .await
    .expect("registered partial read was not recovered");
}
pub(super) async fn assert_read_results(
    client: &CellClient,
    fixture: &transaction_fixture::TableFixture,
    id: [u8; 16],
    version: usize,
) {
    let coordinator = coordinator_target(ACCOUNT, &id).unwrap();
    let read = ReadTransactionInput {
        transaction_id: id,
        coordinator_cell: *coordinator.cell_id().as_bytes(),
    };
    for (target, participant) in &fixture.participants {
        assert_eq!(
            client
                .query::<ReadPartitionTransaction>(target, None, Json(read.clone()))
                .await
                .unwrap()
                .output
                .0,
            ParticipantTransactionState::Committed
        );
        let TransactionOperation::Put(put) = &participant.operations[0].operation else {
            unreachable!()
        };
        let mut expected = extenddb_core::types::extract_key(&put.item, &fixture.table.key_schema);
        expected.insert("version".into(), AttributeValue::N(version.to_string()));
        assert_eq!(
            client
                .query::<ReadPartitionTransactionResult>(
                    target,
                    None,
                    Json(ReadTransactionResultInput {
                        transaction: read.clone(),
                        position: 0
                    })
                )
                .await
                .unwrap()
                .output
                .0,
            TransactionReadResult::Item(Some(expected))
        );
    }
}

struct RefuseChildInstall {
    namespace: cellar_runtime::identity::NamespaceId,
    refuse: Arc<AtomicBool>,
    attempts: Arc<AtomicUsize>,
}
impl InvocationAdmission for RefuseChildInstall {
    fn check<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
    ) -> Pin<Box<dyn Future<Output = cellar_runtime::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if invocation.target.namespace() == self.namespace
                && invocation.operation == InstallPartition::ID
                && self.refuse.load(Ordering::SeqCst)
            {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                return Err(cellar_runtime::Error::Capacity(
                    "injected child admission budget",
                ));
            }
            Ok(())
        })
    }
}
#[test]
fn capacity_sweep_survives_admission_backpressure() {
    let (host, core) = runtimes();
    host.block_on(async {
        let files = tempfile::tempdir().unwrap();
        let bucket =
            cellar::bucket::Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let (node, client) = cellar_node::start_limited(
            local_node::application().unwrap(),
            &files.path().join("capacity-owner"),
            host.handle(),
            core.handle(),
            bucket,
            RESIDENT,
        )
        .await
        .unwrap();
        let app = local_node::application().unwrap();
        let provisioner = Arc::new(
            CellInitialPartitionProvisioner::new(client.clone(), app)
                .with_initial_partition_count(2)
                .unwrap(),
        );
        let storage =
            CellStorage::new(client.clone(), "local").with_initial_partitions(provisioner.clone());
        let table = storage
            .create_table(ACCOUNT, table("Pressure"))
            .await
            .unwrap();
        let info = storage.table_key_info(ACCOUNT, "Pressure").await.unwrap();
        for id in ["aaa", "bbb"] {
            storage
                .put_item(
                    &info,
                    Item::from([("id".into(), AttributeValue::S(id.into()))]),
                    false,
                    None,
                    &ExpressionMaps::default(),
                    None,
                )
                .await
                .unwrap();
        }
        let initial = routes(&client, &table.table_id).await;
        assert_eq!(initial.len(), 2);
        let refuse = Arc::new(AtomicBool::new(true));
        let attempts = Arc::new(AtomicUsize::new(0));
        let controlled = client.clone().with_admission(Arc::new(RefuseChildInstall {
            namespace: data_target(ACCOUNT, &table.table_id, &initial[0].partition_id)
                .unwrap()
                .namespace(),
            refuse: refuse.clone(),
            attempts: attempts.clone(),
        }));
        let mut cursor = None;
        let refusal = provisioner
            .reconcile_account_capacity(ACCOUNT, controlled.clone(), 1, &mut cursor)
            .await;
        assert!(
            matches!(refusal, Err(StorageError::LimitExceeded(_))),
            "known pre-submission capacity classification changed: {refusal:?}"
        );
        let directory =
            directory_target(ACCOUNT, &DirectorySpec::root(table.table_id.clone())).unwrap();
        let input = DirectoryPartitionInput {
            table_id: table.table_id.clone(),
            partition_id: initial[0].partition_id,
        };
        let pending = client
            .query::<ReadDirectoryTransfer>(&directory, None, Json(input.clone()))
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        let DirectoryTransfer::Base(plan) = &pending else {
            panic!("unexpected index transfer")
        };
        assert_eq!(plan.source.partition_id, initial[0].partition_id);
        let cancellation = CancellationToken::new();
        let stop = cancellation.clone();
        let running = provisioner.clone();
        let worker = tokio::spawn(async move {
            running
                .run_account_capacity_loop(ACCOUNT, controlled, 1, Duration::from_millis(20), &stop)
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while attempts.load(Ordering::SeqCst) < 3 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!worker.is_finished());
        assert!(node.is_ready().await.unwrap());
        assert_eq!(routes(&client, &table.table_id).await, initial);
        assert_eq!(
            client
                .query::<ReadDirectoryTransfer>(&directory, None, Json(input.clone()))
                .await
                .unwrap()
                .output
                .0
                .as_ref(),
            Some(&pending)
        );
        refuse.store(false, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(10), async {
            while routes(&client, &table.table_id).await.len() != 3 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        cancellation.cancel();
        worker.await.unwrap().unwrap();
        assert!(
            client
                .query::<ReadDirectoryTransfer>(&directory, None, Json(input))
                .await
                .unwrap()
                .output
                .0
                .is_none()
        );
        for id in ["aaa", "bbb"] {
            let key = Item::from([("id".into(), AttributeValue::S(id.into()))]);
            assert_eq!(
                storage.get_item(&info, &key).await.unwrap().as_ref(),
                Some(&key)
            );
        }
        assert!(node.is_ready().await.unwrap());
        cellar_node::stop(node, cellar::sdk::ShutdownMode::Preserve)
            .await
            .unwrap();
    });
}

#[test]
fn coordinator_history_outgrows_residency_and_partial_read_recovers() {
    let (host, core) = runtimes();
    host.block_on(async {
        let files = tempfile::tempdir().unwrap();
        let first = files.path().join("coordinator-history-a");
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
        let load = node.services().node_load().unwrap();
        let fixture = transaction_fixture::table(&client, "Residency").await;
        let info = fixture
            .storage
            .table_key_info(ACCOUNT, "Residency")
            .await
            .unwrap();
        let tokens = tokens("residency", 24);
        for (version, token) in tokens.iter().enumerate() {
            assert!(!write(&fixture.storage, &info, token, version).await);
            assert!(load.resident_cells.load(Ordering::SeqCst) <= RESIDENT);
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while load.owned_cells.load(Ordering::SeqCst) < 24 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(load.resident_cells.load(Ordering::SeqCst) > 0);
        let after_history = fixture
            .storage
            .create_table(ACCOUNT, table("AfterHistory"))
            .await
            .unwrap();
        assert_eq!(after_history.table_status, TableStatus::Active);
        assert_eq!(routes(&client, &after_history.table_id).await.len(), 2);
        let (first_replay, concurrent) = tokio::join!(
            write(&fixture.storage, &info, &tokens[0], 0),
            write(&fixture.storage, &info, &tokens[0], 0)
        );
        assert!(first_replay && concurrent);
        for key in keys() {
            assert_eq!(
                fixture
                    .storage
                    .get_item(&info, &key)
                    .await
                    .unwrap()
                    .unwrap()["version"],
                AttributeValue::N("23".into())
            );
        }
        let oldest = coordinator_target(ACCOUNT, tokens[0].as_bytes()).unwrap();
        let id = pending_read(&client, &fixture, &oldest).await;
        assert_eq!(
            status(&client, id).await.decision,
            CoordinatorDecision::Begin
        );
        // Native residency may evict any idle cell; the application does not
        // release or rewrite SDK owner records to make this fixture pass.
        for token in tokens.iter().skip(1) {
            client
                .query::<ReadPendingTransactionBoundary>(
                    &coordinator_target(ACCOUNT, token.as_bytes()).unwrap(),
                    None,
                    Json(()),
                )
                .await
                .unwrap();
        }
        let (successor, next) = cellar_node::start_limited(
            local_node::application().unwrap(),
            &files.path().join("coordinator-history-b"),
            host.handle(),
            core.handle(),
            bucket,
            RESIDENT,
        )
        .await
        .unwrap();
        cellar_node::stop(node, cellar::sdk::ShutdownMode::Handoff)
            .await
            .unwrap();
        std::fs::remove_dir_all(first).unwrap();
        recover(&next, id).await;
        assert_read_results(&next, &fixture, id, 23).await;
        let storage = storage(&next, 2);
        assert!(write(&storage, &info, &tokens[0], 0).await);
        for key in keys() {
            assert_eq!(
                storage.get_item(&info, &key).await.unwrap().unwrap()["version"],
                AttributeValue::N("23".into())
            );
        }
        // Settled discovery queries must not write a new account checkpoint on
        // every pass. SDK eviction/owner caching remains the SDK's concern.
        let account = account_target(ACCOUNT).unwrap();
        let before = next
            .query::<ListCoordinatorShards>(
                &account,
                None,
                Json(ListCoordinatorShardsInput {
                    account_id: ACCOUNT.into(),
                    after: None,
                    limit: 100,
                }),
            )
            .await
            .unwrap();
        assert_eq!(before.output.0.len(), 24);
        let mut recovery = TransactionRecovery::new(Arc::new(storage), ACCOUNT.into()).unwrap();
        for _ in 0..100 {
            recovery.step().await.unwrap();
        }
        let after = next
            .query::<ListCoordinatorShards>(
                &account,
                None,
                Json(ListCoordinatorShardsInput {
                    account_id: ACCOUNT.into(),
                    after: None,
                    limit: 100,
                }),
            )
            .await
            .unwrap();
        assert_eq!(after.receipt, before.receipt);
        assert!(
            successor
                .services()
                .node_load()
                .unwrap()
                .resident_cells
                .load(Ordering::SeqCst)
                <= RESIDENT
        );
        cellar_node::stop(successor, cellar::sdk::ShutdownMode::Preserve)
            .await
            .unwrap();
    });
}

#[test]
fn table_creation_waits_for_native_residency_capacity() {
    let (host, core) = runtimes();
    host.block_on(async {
        let files = tempfile::tempdir().unwrap();
        let bucket =
            cellar::bucket::Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let (node, client) = cellar_node::start_limited(
            local_node::application().unwrap(),
            &files.path().join("new-table-owner"),
            host.handle(),
            core.handle(),
            bucket,
            RESIDENT,
        )
        .await
        .unwrap();
        let load = node.services().node_load().unwrap();
        let provisioner = CellInitialPartitionProvisioner::new(
            client.clone(),
            local_node::application().unwrap(),
        );
        for token in tokens("empty-coordinator", 32) {
            provisioner
                .ensure(&client, ACCOUNT, token.as_bytes())
                .await
                .unwrap();
            client
                .query::<ReadPendingTransactionBoundary>(
                    &coordinator_target(ACCOUNT, token.as_bytes()).unwrap(),
                    None,
                    Json(()),
                )
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while load.owned_cells.load(Ordering::SeqCst) < 32 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(load.resident_cells.load(Ordering::SeqCst) > 0);
        let storage = storage(&client, 4);
        let table = storage
            .create_table(ACCOUNT, table("AfterHistory"))
            .await
            .unwrap();
        assert_eq!(table.table_status, TableStatus::Active);
        let ranges = routes(&client, &table.table_id).await;
        assert_eq!(ranges.len(), 4);
        let info = storage
            .table_key_info(ACCOUNT, "AfterHistory")
            .await
            .unwrap();
        let mut ids = std::collections::HashSet::new();
        for (id, range) in ["\u{1}first", "a-second", "\u{a0}third", "Ω-fourth"]
            .into_iter()
            .zip(ranges)
        {
            let key = Item::from([("id".into(), AttributeValue::S(id.into()))]);
            let encoded = ordered_data_key(&info.table_id, &key, &info.key_schema).unwrap();
            assert!(
                encoded >= range.lower && range.upper.as_ref().is_none_or(|upper| &encoded < upper)
            );
            let target = data_target(ACCOUNT, &info.table_id, &range.partition_id).unwrap();
            assert!(ids.insert(target.cell_id()));
            let installed = client
                .query::<ReadPartitionState>(&target, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap();
            assert_eq!(installed.spec.partition_id, range.partition_id);
            storage
                .put_item(
                    &info,
                    key.clone(),
                    false,
                    None,
                    &ExpressionMaps::default(),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(
                storage.get_item(&info, &key).await.unwrap().as_ref(),
                Some(&key)
            );
            assert!(load.resident_cells.load(Ordering::SeqCst) <= RESIDENT);
        }
        assert_eq!(ids.len(), 4);
        assert!(node.is_ready().await.unwrap());
        cellar_node::stop(node, cellar::sdk::ShutdownMode::Preserve)
            .await
            .unwrap();
    });
}

/// Observe the native lease and sampled placement view before new-cell demand.
pub(super) async fn departure(
    bucket: &cellar::bucket::Bucket,
    host: &tokio::runtime::Handle,
    departed: &str,
    remaining: &str,
) {
    let observer = cellar::ownership_store::BucketOwnership::new(
        bucket.clone(),
        bucket.clone(),
        "coordinator-observer".into(),
        String::new(),
    );
    let inspection = cellar::sdk::ExecutionDomain::new(host.clone());
    inspection
        .run(async {
            tokio::time::timeout(std::time::Duration::from_secs(20), async {
                let expired_at = loop {
                    let lease = observer.read_node_lease(departed).await.unwrap();
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
                        && peers
                            .iter()
                            .all(|peer| peer.node != departed || peer.expires_ms <= sampled_at)
                        && peers
                            .iter()
                            .any(|peer| peer.node == remaining && peer.expires_ms > sampled_at)
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
}
