//! Reservation contracts on real Cellar owners; physical SQLITE_FULL remains separate.
use crate::{
    capacity_snapshot,
    native_peer::{self, FRONT, Inspection, Peer, SOURCE},
    transaction_fixture,
};
use bdb_cellar_runtime::{
    MutationIdentity,
    client::{CellClient, Committed, InvocationError},
    identity::{CellTarget, RequestId},
};
use beyonddb::*;
use extenddb_core::{
    expression::ExpressionMaps,
    types::{AttributeValue, Item},
};
use extenddb_storage::{DataEngine, TableEngine, error::StorageError};
use std::sync::{Arc, Mutex};

fn mutation() -> MutationIdentity {
    let now = native_peer::now_ms();
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    }
}
fn runtimes() -> (tokio::runtime::Runtime, tokio::runtime::Runtime) {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .with_ansi(false)
        .try_init();
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
#[test]
fn small_item_commit_uses_reserved_capacity_after_other_writers_fill_cell() {
    let (host, core) = runtimes();
    host.block_on(capacity_case(
        &[380 * 1024, 8 * 1024, 0],
        50,
        0,
        144,
        host.handle(),
        core.handle(),
    ));
}
#[test]
fn large_item_commit_uses_reserved_capacity_after_other_writers_fill_cell() {
    let (host, core) = runtimes();
    host.block_on(capacity_case(
        &[380 * 1024, 8 * 1024, 0],
        4,
        380 * 1024,
        32,
        host.handle(),
        core.handle(),
    ));
}
#[test]
fn prepare_refuses_without_resolution_headroom_and_leaves_no_locks() {
    let (host, core) = runtimes();
    host.block_on(capacity_case(&[], 4, 0, 2, host.handle(), core.handle()));
}

type PrepareResult = Result<
    Committed<Json<PrepareTransactionOutcome>>,
    InvocationError<Json<PrepareTransactionOutcome>>,
>;

async fn prepare<C: MultipartTransactionCommand<Output = Json<PrepareTransactionOutcome>>>(
    client: &CellClient,
    target: &CellTarget,
    identity: MutationIdentity,
    input: &C::Payload,
) -> (PrepareResult, TransactionPayloadRef) {
    let bytes = serde_json::to_vec(input).unwrap();
    let reference = TransactionPayloadRef::new(&bytes, identity.expires_at_ms).unwrap();
    for chunk in reference.chunks(&bytes) {
        client
            .command::<UploadTransactionPayload<C>>(target, mutation(), chunk)
            .await
            .unwrap();
    }
    let mut attempts = 0;
    loop {
        let result = client
            .command::<C>(
                target,
                identity,
                Json(TransactionCommandInput::Reference(reference.clone())),
            )
            .await;
        if matches!(result, Err(InvocationError::Ambiguous { .. }))
            && native_peer::now_ms() < identity.expires_at_ms
        {
            attempts += 1;
            eprintln!(
                "resolve ambiguous capacity prepare with original identity/input, attempt {attempts}: {result:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        } else {
            return (result, reference);
        }
    }
}

async fn capacity_case(
    fill_sizes: &[usize],
    per_participant: usize,
    payload_bytes: usize,
    database_mib: u64,
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
) {
    let application = native_peer::application();
    let mut registry = application.registry();
    drop(application);
    // Apply a smaller budget only to these compiled participant modules, before
    // any SDK factory/handle exists. Keep production schemas and type declarations.
    for module in ["beyonddb-account", "beyonddb-data"] {
        Arc::get_mut(&mut registry)
            .unwrap()
            .set_database_limit(module, database_mib * 1024 * 1024)
            .unwrap();
    }
    let account_id = "123456789012";
    let account = account_target(account_id).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let bucket =
        cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite")).unwrap();
    let observed = Arc::new(Mutex::new(Default::default()));
    let source_path = directory.path().join(SOURCE);
    let mut peer = Peer::start_registry(
        registry.clone(),
        &source_path,
        bucket.clone(),
        observed.clone(),
        true,
        host,
        core,
    )
    .await;
    let client = peer.client.clone();
    let provisioner = Arc::new(
        CellInitialPartitionProvisioner::new(client.clone(), native_peer::application())
            .with_initial_partition_count(2)
            .unwrap(),
    );
    provisioner.admit_account(account_id).await.unwrap();
    let storage = CellStorage::new(client.clone(), "us-east-1")
        .with_initial_partitions(provisioner.clone())
        .with_transaction_coordinators(provisioner.clone());
    client
        .command::<CreateTable>(
            &account,
            mutation(),
            Json(transaction_fixture::account_spec("CapacityAccount")),
        )
        .await
        .unwrap();
    storage.create_table(account_id, serde_json::from_value(serde_json::json!({
        "TableName":"CapacityData", "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
        "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}], "BillingMode":"PAY_PER_REQUEST"
    })).unwrap()).await.unwrap();
    let mut tables = Vec::new();
    for name in ["CapacityAccount", "CapacityData"] {
        tables.push(
            client
                .query::<DescribeTable>(&account, None, Json(name.into()))
                .await
                .unwrap()
                .output
                .0
                .unwrap(),
        );
    }
    let info = storage
        .table_key_info(account_id, "CapacityData")
        .await
        .unwrap();
    let (data, _) = transaction_fixture::participant_for_key(
        &client,
        &info,
        &Item::from([("id".into(), AttributeValue::S("committed-0".into()))]),
    )
    .await;
    let spec = client
        .query::<ReadPartitionState>(&data, None, Json(()))
        .await
        .unwrap()
        .output
        .0
        .unwrap()
        .spec;
    let transaction_id = [252; 16];
    provisioner
        .ensure(&client, account_id, &transaction_id)
        .await
        .unwrap();
    let coordinator = coordinator_target(account_id, &transaction_id).unwrap();
    let targets = [&account, &data];
    let operations: Vec<Vec<_>> = tables
        .iter()
        .map(|table| {
            (0..per_participant)
                .map(|index| {
                    TransactionOperation::Put(PutItemInput {
                        table_name: table.table_name.clone(),
                        table_id: table.id.clone(),
                        item: Item::from([
                            ("id".into(), AttributeValue::S(format!("committed-{index}"))),
                            (
                                "payload".into(),
                                AttributeValue::S("x".repeat(payload_bytes)),
                            ),
                        ]),
                        condition: None,
                    })
                })
                .collect()
        })
        .collect();
    let mut participants: Vec<_> = [
        CoordinatorParticipantTarget::Account,
        CoordinatorParticipantTarget::Data {
            table_id: tables[1].id.clone(),
            partition_id: spec.partition_id,
            epoch: spec.epoch,
        },
    ]
    .into_iter()
    .enumerate()
    .map(|(position, target)| CoordinatorParticipant {
        target,
        operations: operations[position]
            .iter()
            .enumerate()
            .map(|(index, operation)| IndexedTransactionOperation {
                index: (position * per_participant + index) as u8,
                operation: operation.clone(),
            })
            .collect(),
    })
    .collect();
    participants.sort_by_key(|participant| {
        *targets[usize::from(participant.operations[0].index) / per_participant]
            .cell_id()
            .as_bytes()
    });
    transaction_command!(
        client,
        BeginCrossCellTransaction,
        &coordinator,
        mutation(),
        Json(BeginCrossCellTransactionInput {
            account_id: account_id.into(),
            transaction_id,
            token: None,
            participants: participants.clone()
        })
    )
    .await
    .unwrap();
    let coordinator_cell = *coordinator.cell_id().as_bytes();
    let mut rejected_receipts = Vec::new();
    for (position, participant) in participants.iter().enumerate() {
        let identity = mutation();
        let index = usize::from(participant.operations[0].index) / per_participant;
        let (prepared, reference) = if index == 0 {
            let input = PrepareAccountTransactionInput {
                transaction_id,
                coordinator_cell,
                coordinator_key: transaction_id.to_vec(),
                operations: operations[0].clone(),
            };
            prepare::<PrepareAccountTransaction>(&client, &account, identity, &input).await
        } else {
            let input = PreparePartitionTransactionInput {
                table_id: tables[1].id.clone(),
                epoch: spec.epoch,
                transaction_id,
                coordinator_cell,
                coordinator_key: transaction_id.to_vec(),
                operations: operations[1].clone(),
            };
            prepare::<PreparePartitionTransaction>(&client, &data, identity, &input).await
        };
        if database_mib == 2 {
            assert!(
                matches!(&prepared, Err(InvocationError::Rejected(result))
                if result.output.0 == PrepareTransactionOutcome::Rejected { index: 0, reason: TransactionFailure::Throttled }),
                "headroom refusal requires a durable rejected receipt: {prepared:?}"
            );
            let Err(InvocationError::Rejected(result)) = prepared else {
                unreachable!()
            };
            rejected_receipts.push((index, identity, reference, result.receipt));
            continue;
        }
        let prepared = prepared.unwrap();
        client
            .command::<RecordParticipantPrepare>(
                &coordinator,
                mutation(),
                Json(CoordinatorPhaseInput {
                    account_id: account_id.into(),
                    transaction_id,
                    routing_key: transaction_id.to_vec(),
                    position: position as u8,
                    participant_cell: *targets[index].cell_id().as_bytes(),
                    sequence: prepared.receipt.commit_sequence,
                }),
            )
            .await
            .unwrap();
    }
    if database_mib <= 2 {
        for (target, locks) in [
            (&account, "ddb_account_transaction_locks"),
            (&data, "ddb_partition_transaction_locks"),
        ] {
            let snapshot = capacity_snapshot::restore(
                &bucket,
                &native_peer::identity(&observed, SOURCE, target)
                    .unwrap()
                    .scope,
                host,
            )
            .await;
            let connection = capacity_snapshot::open(&snapshot);
            for table in [
                locks,
                "ddb_transactions",
                "capacity_reservations",
                "capacity_reservation_chunks",
                "ddb_transaction_payloads",
            ] {
                let rows: i64 = connection
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                assert_eq!(rows, 0, "failed prepare left rows in {table}");
            }
        }
        let decision = storage
            .resume_cross_cell_transaction(account_id, &transaction_id, transaction_id)
            .await;
        assert_eq!(
            decision.unwrap(),
            CoordinatorDecision::Abort {
                index: Some(participants[0].operations[0].index),
                reason: Some(beyonddb::TransactionFailure::Throttled),
            }
        );
        let status = client
            .query::<ReadCrossCellTransaction>(
                &coordinator,
                None,
                Json(ReadCrossCellTransactionInput {
                    account_id: account_id.into(),
                    transaction_id,
                    routing_key: transaction_id.to_vec(),
                }),
            )
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        assert_eq!(status.resolved_count, 2);
        let inspection = Inspection::new(bucket.clone(), host);
        peer.stop().await;
        std::fs::remove_dir_all(&source_path).unwrap();
        let mut peer = Peer::start_registry(
            registry,
            &directory.path().join(FRONT),
            bucket,
            observed,
            true,
            host,
            core,
        )
        .await;
        inspection.departed().await;
        for (index, identity, reference, receipt) in rejected_receipts {
            let replay = if index == 0 {
                peer.client
                    .command::<PrepareAccountTransaction>(
                        &account,
                        identity,
                        Json(TransactionCommandInput::Reference(reference)),
                    )
                    .await
            } else {
                peer.client
                    .command::<PreparePartitionTransaction>(
                        &data,
                        identity,
                        Json(TransactionCommandInput::Reference(reference)),
                    )
                    .await
            };
            let Err(InvocationError::Rejected(replay)) = replay else {
                panic!("lost durable capacity rejection: {replay:?}")
            };
            assert_eq!(replay.receipt, receipt);
            assert_eq!(
                replay.output.0,
                PrepareTransactionOutcome::Rejected {
                    index: 0,
                    reason: TransactionFailure::Throttled
                }
            );
        }
        let recovered = peer
            .client
            .query::<ReadCrossCellTransaction>(
                &coordinator,
                None,
                Json(ReadCrossCellTransactionInput {
                    account_id: account_id.into(),
                    transaction_id,
                    routing_key: transaction_id.to_vec(),
                }),
            )
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        assert!(matches!(
            recovered.decision,
            CoordinatorDecision::Abort {
                reason: Some(TransactionFailure::Throttled),
                ..
            }
        ));
        assert_eq!(recovered.resolved_count, 2);
        let provisioner = Arc::new(
            CellInitialPartitionProvisioner::new(peer.client.clone(), native_peer::application())
                .with_initial_partition_count(2)
                .unwrap(),
        );
        let http = crate::signed_http::SignedHttp::start(&peer.client, provisioner, true).await;
        for table in &tables {
            let item = http
                .sdk
                .get_item()
                .table_name(&table.table_name)
                .key(
                    "id",
                    aws_sdk_dynamodb::types::AttributeValue::S("committed-0".into()),
                )
                .consistent_read(true)
                .send()
                .await
                .unwrap();
            assert!(item.item.is_none());
            http.sdk
                .put_item()
                .table_name(&table.table_name)
                .item(
                    "id",
                    aws_sdk_dynamodb::types::AttributeValue::S("committed-0".into()),
                )
                .send()
                .await
                .unwrap();
        }
        http.stop().await;
        peer.stop().await;
        inspection.close().await;
        return;
    }
    for table in &tables {
        let info = storage
            .table_key_info(account_id, &table.table_name)
            .await
            .unwrap();
        storage
            .put_item(
                &info,
                Item::from([
                    ("id".into(), AttributeValue::S("unrelated".into())),
                    ("payload".into(), AttributeValue::S("y".repeat(100 * 1024))),
                ]),
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .unwrap();
    }
    if !fill_sizes.is_empty() {
        for table in &tables {
            let info = storage
                .table_key_info(account_id, &table.table_name)
                .await
                .unwrap();
            for &fill_bytes in fill_sizes {
                let mut refused = false;
                for i in 0..256 {
                    let result = storage
                        .put_item(
                            &info,
                            Item::from([
                                (
                                    "id".into(),
                                    AttributeValue::S(format!("fill-{fill_bytes}-{i}")),
                                ),
                                ("payload".into(), AttributeValue::S("z".repeat(fill_bytes))),
                            ]),
                            false,
                            None,
                            &ExpressionMaps::default(),
                            None,
                        )
                        .await;
                    if let Err(error) = result {
                        assert!(
                            matches!(error, StorageError::Transient(_)),
                            "capacity refusal must be retryable: {error:?}"
                        );
                        refused = true;
                        break;
                    }
                }
                assert!(refused, "fixture must exhaust the Cell budget");
            }
        }
    }
    for target in [&account, &data] {
        let snapshot = capacity_snapshot::restore(
            &bucket,
            &native_peer::identity(&observed, SOURCE, target)
                .unwrap()
                .scope,
            host,
        )
        .await;
        let connection = capacity_snapshot::open(&snapshot);
        let held: i64 = connection
            .query_row("SELECT pages FROM capacity_total", [], |row| row.get(0))
            .unwrap();
        let allocation: i64 = connection
            .query_row(
                "SELECT COALESCE(SUM(length(allocation)),0) FROM capacity_reservation_chunks",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let used: i64 = connection
            .query_row(
                "SELECT page_count - freelist_count FROM pragma_page_count, pragma_freelist_count",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(held > 0);
        assert_eq!(
            allocation,
            held * 4096,
            "reservation must occupy real BLOB pages"
        );
        // Cellar reservations are physically allocated, so used already includes
        // held pages. Counting them twice would compare unlike storage models.
        assert!(used >= held && used <= (database_mib * 1024 * 1024 / 4096) as i64);
        if fill_sizes.contains(&0) {
            assert!(
                (database_mib * 1024 * 1024 / 4096) as i64 - used < 8,
                "filler must reach the reserved page boundary: used={used}, held={held}"
            );
        }
    }
    client
        .command::<DecideCrossCellTransaction>(
            &coordinator,
            mutation(),
            Json(DecideCrossCellTransactionInput {
                account_id: account_id.into(),
                transaction_id,
                routing_key: transaction_id.to_vec(),
                decision: CoordinatorDecision::Commit,
            }),
        )
        .await
        .unwrap();
    storage
        .finish_decided_cross_cell_transaction(account_id, &transaction_id, transaction_id)
        .await
        .unwrap();
    for (index, table) in tables.iter().enumerate() {
        let info = storage
            .table_key_info(account_id, &table.table_name)
            .await
            .unwrap();
        for operation in &operations[index] {
            let TransactionOperation::Put(input) = operation else {
                unreachable!()
            };
            let key = Item::from([("id".into(), input.item["id"].clone())]);
            assert_eq!(
                storage.get_item(&info, &key).await.unwrap(),
                Some(input.item.clone())
            );
        }
    }
    for (target, locks) in [
        (&account, "ddb_account_transaction_locks"),
        (&data, "ddb_partition_transaction_locks"),
    ] {
        let snapshot = capacity_snapshot::restore(
            &bucket,
            &native_peer::identity(&observed, SOURCE, target)
                .unwrap()
                .scope,
            host,
        )
        .await;
        let connection = capacity_snapshot::open(&snapshot);
        let held: i64 = connection
            .query_row("SELECT pages FROM capacity_total", [], |row| row.get(0))
            .unwrap();
        assert_eq!(held, 0, "resolved participant must release its reservation");
        for table in [
            locks,
            "capacity_reservations",
            "capacity_reservation_chunks",
            "ddb_transaction_payloads",
        ] {
            let rows: i64 = connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(rows, 0, "resolved participant left rows in {table}");
        }
    }
    let inspection = Inspection::new(bucket.clone(), host);
    peer.stop().await;
    std::fs::remove_dir_all(&source_path).unwrap();
    let mut peer = Peer::start_registry(
        registry,
        &directory.path().join(FRONT),
        bucket,
        observed,
        true,
        host,
        core,
    )
    .await;
    inspection.departed().await;
    let provisioner = Arc::new(
        CellInitialPartitionProvisioner::new(peer.client.clone(), native_peer::application())
            .with_initial_partition_count(2)
            .unwrap(),
    );
    let http = crate::signed_http::SignedHttp::start(&peer.client, provisioner, true).await;
    for (index, table) in tables.iter().enumerate() {
        for operation in &operations[index] {
            let TransactionOperation::Put(input) = operation else {
                unreachable!()
            };
            let AttributeValue::S(id) = &input.item["id"] else {
                unreachable!()
            };
            let item = http
                .sdk
                .get_item()
                .table_name(&table.table_name)
                .key("id", aws_sdk_dynamodb::types::AttributeValue::S(id.clone()))
                .consistent_read(true)
                .send()
                .await
                .unwrap();
            assert_eq!(
                item.item().unwrap()["payload"],
                aws_sdk_dynamodb::types::AttributeValue::S("x".repeat(payload_bytes))
            );
        }
    }
    http.stop().await;
    peer.stop().await;
    inspection.close().await;
}
