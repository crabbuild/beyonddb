use crate::*;
use beyonddb::{ReadCoordinatorResume, TableRecord};
use cellule_runtime::fleet::telemetry::{
    CellTelemetry, PrimitiveOperationKind, PrimitiveOperationOutcome,
};

#[derive(Default)]
struct CoordinatorQueries(std::sync::atomic::AtomicU64);
impl CellTelemetry for CoordinatorQueries {
    fn primitive_operation(
        &self,
        module: &'static str,
        kind: PrimitiveOperationKind,
        _: PrimitiveOperationOutcome,
        _: std::time::Duration,
    ) {
        if module == "beyonddb-coordinator" && kind == PrimitiveOperationKind::Query {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_transaction_resume_uses_one_coordinator_payload_snapshot() {
    let application = Arc::new(
        Beyonddb::compile(BuildDescriptor {
            source_revision: "transaction-resume".into(),
            cargo_lock_digest: Digest::from_bytes([1; 32]),
        })
        .unwrap(),
    );
    let account_id = "123456789012";
    let account = account_target(account_id).unwrap();
    let session = SessionId::from_bytes([180; 16]);
    let directory = tempfile::TempDir::new().unwrap();
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        object_store::path::Path::from("transaction-resume"),
        *account.application().as_bytes(),
    );
    let host = CellNodeBuilder::new(application.clone())
        .with_runtime(SqlWorkerPool::new(1, 17).unwrap(), 16 * 1024 * 1024)
        .with_replica_host(Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)))
        .with_session(session)
        .build_unleased_for_maintenance()
        .unwrap();
    let queries = Arc::new(CoordinatorQueries::default());
    host.install_telemetry(queries.clone()).unwrap();
    let registry = application.registry();
    let bootstrap = Bootstrap {
        runtime: host.runtime(),
        registry: &registry,
        layout: &layout,
        session,
    };
    let mut table_bytes = [180; 32];
    table_bytes[..16].copy_from_slice(account.tenant().as_bytes());
    let table = TableRecord {
        table_class: Default::default(),
        table_class_updates_ms: Vec::new(),
        placement: beyonddb::TablePlacement::Routed {
            initial_partitions: 2,
        },
        local_secondary_indexes: Vec::new(),
        global_secondary_indexes: Vec::new(),
        id: blake3::Hash::from_bytes(table_bytes).to_hex().to_string(),
        created_at_ms: 1000,
        table_name: "Driver".into(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "id".into(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "id".into(),
            attribute_type: ScalarAttributeType::S,
        }],
        billing_mode: BillingMode::PayPerRequest,
        provisioned_throughput: None,
        deletion_protection_enabled: false,
        pay_per_request_since_ms: Some(1000),
        stream: None,
    };
    let client = CellClient::local_runtime(registry.clone(), host.runtime(), layout.clone());
    let boundary = [0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    let mut participants = Vec::new();
    let mut handles = Vec::new();
    for (position, left) in [true, false].into_iter().enumerate() {
        let partition_id = [u8::try_from(position + 1).unwrap(); 16];
        let target = data_target(account_id, &table.id, &partition_id).unwrap();
        let handle = bootstrap
            .cell(
                &target,
                "beyonddb-data",
                partition_id[0],
                &directory.path().join(format!("data-{position}.sqlite")),
                initialize_partition,
            )
            .await;
        handles.push(handle);
        client
            .command::<InstallPartition>(
                &target,
                identity(180),
                Json(PartitionInstall::Serving(PartitionSpec {
                    table: table.clone(),
                    partition_id,
                    lower: (!left).then_some(boundary),
                    upper: left.then_some(boundary),
                    epoch: 1,
                })),
            )
            .await
            .unwrap();
        let mut item = key_in_range(&table.id, &table.key_schema, left, 1000);
        item.insert("value".into(), AttributeValue::N("1".into()));
        participants.push((
            target,
            CoordinatorParticipant {
                target: CoordinatorParticipantTarget::Data {
                    table_id: table.id.clone(),
                    partition_id,
                    epoch: 1,
                },
                operations: vec![IndexedTransactionOperation {
                    index: u8::try_from(position).unwrap(),
                    operation: TransactionOperation::Put(PutItemInput {
                        table_name: table.table_name.clone(),
                        table_id: table.id.clone(),
                        item,
                        condition: None,
                    }),
                }],
            },
        ));
    }
    participants.sort_by_key(|(target, _)| *target.cell_id().as_bytes());
    // Operation indexes deliberately oppose participant order.
    for (position, (_, participant)) in participants.iter_mut().enumerate() {
        participant.operations[0].index = u8::try_from(1 - position).unwrap();
    }

    let transaction_id = [199; 16];
    let coordinator = coordinator_target(account_id, &transaction_id).unwrap();
    let coordinator_handle = bootstrap
        .cell(
            &coordinator,
            "beyonddb-coordinator",
            199,
            &directory.path().join("coordinator.sqlite"),
            initialize_coordinator,
        )
        .await;
    handles.push(coordinator_handle);
    let client =
        CellClient::local_many_with_telemetry(registry, handles, host.runtime().telemetry_handle())
            .unwrap();
    let storage = CellStorage::new(client.clone(), "us-east-1");
    transaction_command!(
        client,
        BeginCrossCellTransaction,
        &coordinator,
        identity(199),
        Json(BeginCrossCellTransactionInput {
            account_id: account_id.into(),
            transaction_id,
            token: None,
            participants: participants.iter().map(|(_, p)| p.clone()).collect()
        })
    )
    .await
    .unwrap();
    let read = ReadCrossCellTransactionInput {
        account_id: account_id.into(),
        transaction_id,
        routing_key: transaction_id.to_vec(),
    };
    let initial = client
        .query::<ReadCoordinatorResume>(&coordinator, None, Json(read.clone()))
        .await
        .unwrap()
        .output
        .0;
    assert_eq!(
        initial.status.as_ref().unwrap().decision,
        CoordinatorDecision::Begin
    );
    assert_eq!(
        initial
            .participants
            .as_ref()
            .unwrap()
            .iter()
            .map(|p| p.participant.clone())
            .collect::<Vec<_>>(),
        participants
            .iter()
            .map(|(_, p)| p.clone())
            .collect::<Vec<_>>()
    );
    let missing = client
        .query::<ReadCoordinatorResume>(
            &coordinator,
            None,
            Json(ReadCrossCellTransactionInput {
                transaction_id: [198; 16],
                ..read.clone()
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    assert!(missing.status.is_none() && missing.participants.is_none());
    assert!(
        client
            .query::<ReadCoordinatorResume>(
                &coordinator,
                None,
                Json(ReadCrossCellTransactionInput {
                    account_id: "999999999999".into(),
                    ..read.clone()
                })
            )
            .await
            .is_err()
    );
    let before = queries.0.load(Ordering::Relaxed);
    let decision = storage
        .resume_cross_cell_transaction(account_id, &transaction_id, transaction_id)
        .await
        .unwrap();
    assert_eq!(decision, CoordinatorDecision::Commit);
    let reads = queries.0.load(Ordering::Relaxed) - before;
    println!("coordinator queries for a two-participant resume: {reads}");
    assert_eq!(
        reads, 4,
        "small resume must fetch status and immutable payloads in one query, then retain decision and resolution checks"
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
    let terminal = client
        .query::<ReadCoordinatorResume>(&coordinator, None, Json(read))
        .await
        .unwrap()
        .output
        .0;
    assert_eq!(terminal.status.unwrap(), status);
    assert!(
        terminal.participants.is_none(),
        "compacted or terminal transactions must not reuse payloads"
    );
    host.runtime().shutdown().await.unwrap();
}
