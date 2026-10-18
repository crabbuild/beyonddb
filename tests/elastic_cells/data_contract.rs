//! Ordered data-range cutover, transaction, stream and restoration contracts.
use crate::native_peer::{self, Inspection, Peer, SOURCE};
use beyonddb::*;
use cellar_runtime::{
    MutationIdentity,
    client::{CellClient, InvocationError},
    identity::{CellTarget, RequestId},
};
use extenddb_core::{
    expression::{Expr, ExpressionMaps, PathElement, UpdateAction},
    types::{
        AttributeDefinition, AttributeValue, BillingMode, Item, KeySchemaElement, KeyType,
        ReturnValuesOnConditionCheckFailure, ScalarAttributeType, StreamViewType,
    },
};
use extenddb_storage::{
    DataEngine, IdempotencyKey, StreamContinuation, StreamEngine, TableEngine, TransactGetOp,
    TransactWriteOp, error::StorageError,
};
use keyspace::RangeKey;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

// The old byte labels identify phases, not transport replays. Each invocation
// gets a fresh envelope; explicit transaction/transfer keys test domain replay.
fn identity(_phase: u8) -> MutationIdentity {
    let now = native_peer::now_ms();
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    }
}
fn key_in_range(_table: &str, _schema: &[KeySchemaElement], lower: bool, index: usize) -> Item {
    // The two retained left rows straddle the later f boundary, while all
    // original left/right rows stay on the proper side of the m boundary.
    let prefix = if !lower {
        "z"
    } else if index == 100 {
        "g"
    } else {
        "a"
    };
    Item::from([(
        "id".into(),
        AttributeValue::S(format!("{prefix}-{index:04}")),
    )])
}
fn fence(table: &TableRecord, value: &str) -> RangeKey {
    ordered_data_key(
        &table.id,
        &Item::from([("id".into(), AttributeValue::S(value.into()))]),
        &table.key_schema,
    )
    .unwrap()
}
async fn point_range(
    client: &CellClient,
    account: &CellTarget,
    table: &str,
    key: RangeKey,
) -> Option<([u8; 16], u64)> {
    match read_route_page(
        client,
        account,
        RoutePageInput {
            table_id: table.into(),
            start_key: Some(key),
            after_lower: None,
            expected_epoch: None,
        },
    )
    .await
    .unwrap()
    {
        RoutePageOutcome::Unrouted => None,
        RoutePageOutcome::Page { partitions, .. } => {
            let range = partitions.first().unwrap();
            Some((range.partition_id, range.epoch))
        }
        RoutePageOutcome::Changed => panic!("fixture route changed during inspection"),
    }
}
async fn stream_count(client: &CellClient, target: &CellTarget, table: &str) -> i64 {
    let StreamJournalOutcome::Page { records, .. } = client
        .query::<ReadPartitionStreamJournal>(
            target,
            None,
            Json(StreamJournalInput {
                table_id: table.into(),
                label: "2026-09-27T00:00:00.000".into(),
                after_sequence: None,
                limit: 1_000,
            }),
        )
        .await
        .unwrap()
        .output
        .0
    else {
        panic!("installed range must retain its stream journal")
    };
    assert!(
        records.len() < 1_000,
        "fixture count must include the complete journal"
    );
    i64::try_from(records.len()).unwrap()
}
#[test]
fn data_ranges_use_independent_cells_and_survive_owner_restart() {
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
    let application = native_peer::application();
    let account = account_target("123456789012").unwrap();
    let directory = tempfile::tempdir().unwrap();
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite")).unwrap();
    let observed = Arc::new(Mutex::new(Default::default()));
    let mut source = Peer::start(application.clone(), &directory.path().join(SOURCE),
        bucket.clone(), observed.clone(), true, host.handle(), core.handle()).await;
    let inspection = Inspection::new(bucket.clone(), host.handle());
    let cell_client = source.client.clone();
    let account_client = cell_client.clone();
    let table = match account_client
        .command::<CreateTable>(
            &account,
            identity(13),
            Json(TableSpec {
                table_class: Default::default(),
                placement: beyonddb::TablePlacement::Routed {
                    initial_partitions: 2,
                },
                local_secondary_indexes: Vec::new(),
                global_secondary_indexes: Vec::new(),
                table_name: "Books".into(),
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
                initial_tags: Vec::new(),
                resource_arn: None,
                stream: Some(StreamConfig {
                    view_type: StreamViewType::KeysOnly,
                    region: "us-east-1".into(),
                    label: "2026-09-27T00:00:00.000".into(),
                }),
            }),
        )
        .await
        .unwrap()
        .output
        .0
    {
        CreateTableOutcome::Created(table) => *table,
        other => panic!("unexpected create outcome: {other:?}"),
    };
    let split = fence(&table, "m");
    let left_id = [0; 16];
    let mut right_id = [0; 16];
    right_id[15] = 1;
    let left_target = data_target("123456789012", &table.id, &left_id).unwrap();
    let right_target = data_target("123456789012", &table.id, &right_id).unwrap();
    assert_ne!(left_target.cell_id(), right_target.cell_id());
    let transaction_id = [110; 16];
    let coordinator_target = coordinator_target("123456789012", &transaction_id).unwrap();
    let coordinator_admission = Arc::new(CellInitialPartitionProvisioner::new(
        cell_client.clone(), application.clone(),
    ));
    coordinator_admission.ensure(&cell_client, "123456789012", &transaction_id).await.unwrap();
    let storage = CellStorage::new(cell_client.clone(), "us-east-1")
        .with_transaction_coordinators(coordinator_admission.clone());
    let client = cell_client.clone();
    let shard = u32::from_be_bytes(coordinator_target.partition().try_into().unwrap());
    let registered = client
        .query::<ListCoordinatorShards>(
            &account,
            None,
            Json(ListCoordinatorShardsInput {
                account_id: "123456789012".into(),
                after: None,
                limit: 1,
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    assert_eq!(
        registered
            .iter()
            .map(|entry| entry.shard)
            .collect::<Vec<_>>(),
        vec![shard]
    );
    let partitions = [
        PartitionSpec {
            table: table.clone(),
            partition_id: left_id,
            lower: None,
            upper: Some(split.clone()),
            epoch: 1,
        },
        PartitionSpec {
            table: table.clone(),
            partition_id: right_id,
            lower: Some(split.clone()),
            upper: None,
            epoch: 1,
        },
    ];
    for (target, spec, byte) in [
        (&left_target, &partitions[0], 16),
        (&right_target, &partitions[1], 17),
    ] {
        let installed = client
            .command::<InstallPartition>(
                target,
                identity(byte),
                Json(PartitionInstall::Serving(spec.clone())),
            )
            .await
            .unwrap();
        assert_eq!(installed.output.0, InstallPartitionOutcome::Installed);
    }
    let route = TableRoute {
        table_id: table.id.clone(),
        epoch: 1,
        partitions: partitions.to_vec(),
    };
    let route_target = beyonddb::directory_target(
        "123456789012",
        &beyonddb::DirectorySpec::root(table.id.clone()),
    )
    .unwrap();
    let receipt = coordinator_admission.provision_route_directory(
        &cell_client, "123456789012", &table.id,
        route.partitions.iter().map(|part| RoutePagePartition {
            partition_id: part.partition_id,
            lower: part.lower.clone().unwrap_or_else(RangeKey::minimum),
            upper: part.upper.clone(),
            epoch: part.epoch,
        }).collect(),
    ).await.unwrap();
    let publication = TableRoutePublication { route: route.clone(), receipt };
    assert_eq!(
        point_range(&cell_client, &account, &table.id, RangeKey::minimum()).await,
        None
    );
    let old_account_key = key_in_range(&table.id, &table.key_schema, true, 500);
    client
        .command::<PutItem>(
            &account,
            identity(27),
            Json(PutItemInput {
                table_name: table.table_name.clone(),
                table_id: table.id.clone(),
                item: old_account_key.clone(),
                condition: None,
            }),
        )
        .await
        .unwrap();
    let unsafe_cutover = client
        .command::<ActivateTableRoute>(&account, identity(28), Json(publication.clone()))
        .await;
    assert!(matches!(
        unsafe_cutover,
        Err(InvocationError::Rejected(result))
            if result.output.0 == ActivateTableRouteOutcome::TableNotEmpty
    ));
    client
        .command::<DeleteItem>(
            &account,
            identity(29),
            Json(DeleteItemInput {
                return_old: true,
                table_name: table.table_name.clone(),
                table_id: table.id.clone(),
                key: old_account_key,
                condition: None,
            }),
        )
        .await
        .unwrap();
    let mut gap = publication.clone();
    gap.route.partitions[1].lower = Some(fence(&table, "n"));
    let invalid = client
        .command::<ActivateTableRoute>(&account, identity(22), Json(gap))
        .await;
    assert!(matches!(
        invalid,
        Err(InvocationError::Rejected(result))
            if result.output.0 == ActivateTableRouteOutcome::InvalidRoute
    ));
    let active = client
        .command::<ActivateTableRoute>(&account, identity(23), Json(publication))
        .await
        .unwrap();
    assert_eq!(active.output.0, ActivateTableRouteOutcome::Activated);
    let published = crate::single_leaf_route(&cell_client, &account, &table.id.clone()).await;
    assert_eq!(published, Some(route.clone()));
    for (hash, partition_id, epoch) in [(RangeKey::minimum(), left_id, 1), (split.clone(), right_id, 1)] {
        assert_eq!(
            point_range(&cell_client, &account, &table.id, hash).await,
            Some((partition_id, epoch))
        );
    }
    let cross_left = key_in_range(&table.id, &table.key_schema, true, 600);
    let cross_right = key_in_range(&table.id, &table.key_schema, false, 600);
    let mut sides = [
        (
            left_target.clone(),
            1_u64,
            cross_left.clone(),
            0_u8,
            CoordinatorParticipantTarget::Data {
                table_id: table.id.clone(),
                partition_id: left_id,
                epoch: 1,
            },
        ),
        (
            right_target.clone(),
            1_u64,
            cross_right.clone(),
            1_u8,
            CoordinatorParticipantTarget::Data {
                table_id: table.id.clone(),
                partition_id: right_id,
                epoch: 1,
            },
        ),
    ];
    sides.sort_by_key(|side| *side.0.cell_id().as_bytes());
    let participants: Vec<_> = sides
        .iter()
        .map(|side| CoordinatorParticipant {
            target: side.4.clone(),
            operations: vec![IndexedTransactionOperation {
                index: side.3,
                operation: TransactionOperation::Put(PutItemInput {
                    table_name: table.table_name.clone(),
                    table_id: table.id.clone(),
                    item: side.2.clone(),
                    condition: None,
                }),
            }],
        })
        .collect();
    let mut reversed_participants = participants.clone();
    reversed_participants.reverse();
    let unordered_begin = transaction_command!(
        client,
        BeginCrossCellTransaction,
        &coordinator_target,
        identity(125),
        Json(BeginCrossCellTransactionInput {
            account_id: "123456789012".into(),
            transaction_id,
            token: None,
            participants: reversed_participants,
        }),
    )
    .await;
    assert!(matches!(
        unordered_begin,
        Err(InvocationError::Rejected(result))
            if result.output.0 == BeginCrossCellTransactionOutcome::InvalidParticipants
    ));
    let begun_transaction = transaction_command!(
        client,
        BeginCrossCellTransaction,
        &coordinator_target,
        identity(110),
        Json(BeginCrossCellTransactionInput {
            account_id: "123456789012".into(),
            transaction_id,
            token: None,
            participants: participants.clone(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        begun_transaction.output.0,
        BeginCrossCellTransactionOutcome::Begun
    );
    let pending = client
        .query::<ReadPendingCrossCellTransactions>(
            &coordinator_target,
            None,
            Json(ReadPendingCrossCellTransactionsInput {
                after: None,
                limit: 1,
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].transaction_id, transaction_id);
    assert_eq!(pending[0].state, PendingTransactionState::Begin);
    assert_eq!(pending[0].routing_key, transaction_id);
    assert!(
        client
            .query::<ReadPendingCrossCellTransactions>(
                &coordinator_target,
                None,
                Json(ReadPendingCrossCellTransactionsInput {
                    after: Some(pending[0].cursor.clone()),
                    limit: 1,
                }),
            )
            .await
            .unwrap()
            .output
            .0
            .is_empty()
    );
    let early_decision = client
        .command::<DecideCrossCellTransaction>(
            &coordinator_target,
            identity(111),
            Json(DecideCrossCellTransactionInput {
                account_id: "123456789012".into(),
                transaction_id,
                routing_key: transaction_id.to_vec(),
                decision: CoordinatorDecision::Commit,
            }),
        )
        .await;
    assert!(matches!(
        early_decision,
        Err(InvocationError::Rejected(result))
            if result.output.0 == DecideCrossCellTransactionOutcome::NotPrepared
    ));
    for (position, side) in sides.iter().enumerate() {
        let prepared = transaction_command!(
            client,
            PreparePartitionTransaction,
            &side.0,
            identity(112 + u8::try_from(position).unwrap()),
            Json(PreparePartitionTransactionInput {
                table_id: table.id.clone(),
                epoch: side.1,
                transaction_id,
                coordinator_cell: *coordinator_target.cell_id().as_bytes(),
                coordinator_key: transaction_id.to_vec(),
                operations: vec![participants[position].operations[0].operation.clone()],
            }),
        )
        .await
        .unwrap();
        assert_eq!(prepared.output.0, PrepareTransactionOutcome::Prepared);
        let recorded = client
            .command::<RecordParticipantPrepare>(
                &coordinator_target,
                identity(114 + u8::try_from(position).unwrap()),
                Json(CoordinatorPhaseInput {
                    account_id: "123456789012".into(),
                    transaction_id,
                    routing_key: transaction_id.to_vec(),
                    position: u8::try_from(position).unwrap(),
                    participant_cell: *side.0.cell_id().as_bytes(),
                    sequence: prepared.receipt.commit_sequence,
                }),
            )
            .await
            .unwrap();
        assert_eq!(recorded.output.0, CoordinatorPhaseOutcome::Recorded);
    }
    let decided = client
        .command::<DecideCrossCellTransaction>(
            &coordinator_target,
            identity(116),
            Json(DecideCrossCellTransactionInput {
                account_id: "123456789012".into(),
                transaction_id,
                routing_key: transaction_id.to_vec(),
                decision: CoordinatorDecision::Commit,
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        decided.output.0,
        DecideCrossCellTransactionOutcome::Decided(CoordinatorDecision::Commit)
    );
    let opposing_decision = client
        .command::<DecideCrossCellTransaction>(
            &coordinator_target,
            identity(123),
            Json(DecideCrossCellTransactionInput {
                account_id: "123456789012".into(),
                transaction_id,
                routing_key: transaction_id.to_vec(),
                decision: CoordinatorDecision::Abort {
                    index: None,
                    reason: None,
                },
            }),
        )
        .await;
    assert!(matches!(
        opposing_decision,
        Err(InvocationError::Rejected(result))
            if result.output.0 == DecideCrossCellTransactionOutcome::DecisionConflict
    ));
    let repeated_begin = transaction_command!(
        client,
        BeginCrossCellTransaction,
        &coordinator_target,
        identity(124),
        Json(BeginCrossCellTransactionInput {
            account_id: "123456789012".into(),
            transaction_id,
            token: None,
            participants: participants.clone(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        repeated_begin.output.0,
        BeginCrossCellTransactionOutcome::Existing {
            transaction_id,
            decision: CoordinatorDecision::Commit,
        }
    );
    let pending_commit = client
        .query::<ReadPendingCrossCellTransactions>(
            &coordinator_target,
            None,
            Json(ReadPendingCrossCellTransactionsInput {
                after: None,
                limit: 1,
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    assert_eq!(pending_commit[0].state, PendingTransactionState::Commit);
    for (position, side) in sides.iter().enumerate().take(1) {
        let applied = client
            .command::<ResolvePartitionTransaction>(
                &side.0,
                identity(117 + u8::try_from(position).unwrap()),
                Json(ResolveTransactionInput {
                    transaction_id,
                    coordinator_cell: *coordinator_target.cell_id().as_bytes(),
                    commit: true,
                }),
            )
            .await
            .unwrap();
        assert_eq!(applied.output.0, ResolveTransactionOutcome::Committed);
        let recorded = client
            .command::<RecordParticipantResolution>(
                &coordinator_target,
                identity(119 + u8::try_from(position).unwrap()),
                Json(CoordinatorPhaseInput {
                    account_id: "123456789012".into(),
                    transaction_id,
                    routing_key: transaction_id.to_vec(),
                    position: u8::try_from(position).unwrap(),
                    participant_cell: *side.0.cell_id().as_bytes(),
                    sequence: applied.receipt.commit_sequence,
                }),
            )
            .await
            .unwrap();
        assert_eq!(recorded.output.0, CoordinatorPhaseOutcome::Recorded);
    }
    // A committed transaction has only applied its first participant. The
    // direct query must still fence the second; the public read will resolve COMMIT.
    let visibility_key_info = storage
        .table_key_info("123456789012", "Books")
        .await
        .unwrap();
    assert_eq!(
        storage
            .get_item(&visibility_key_info, &sides[0].2)
            .await
            .unwrap(),
        Some(sides[0].2.clone())
    );
    let blocked = client
        .query::<PartitionGet>(
            &sides[1].0,
            None,
            Json(PartitionGetInput {
                table_id: table.id.clone(),
                epoch: sides[1].1,
                key: sides[1].2.clone(),
            }),
        )
        .await
        .unwrap();
    assert!(matches!(blocked.output.0, PartitionGetOutcome::Conflict(_)));
    let unresolved = client
        .query::<ReadUnresolvedCoordinatorParticipants>(
            &coordinator_target,
            None,
            Json(ReadCrossCellTransactionInput {
                account_id: "123456789012".into(),
                transaction_id,
                routing_key: transaction_id.to_vec(),
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    assert_eq!(unresolved.len(), 1);
    assert_eq!(unresolved[0].position, 1);
    assert!(unresolved[0].prepared);
    assert_eq!(
        storage
            .get_item(&visibility_key_info, &sides[1].2)
            .await
            .unwrap(),
        Some(sides[1].2.clone())
    );
    storage
        .finish_decided_cross_cell_transaction("123456789012", &transaction_id, transaction_id)
        .await
        .unwrap();
    storage
        .finish_decided_cross_cell_transaction("123456789012", &transaction_id, transaction_id)
        .await
        .unwrap();
    let coordinator_state = client
        .query::<ReadCrossCellTransaction>(
            &coordinator_target,
            None,
            Json(ReadCrossCellTransactionInput {
                account_id: "123456789012".into(),
                transaction_id,
                routing_key: transaction_id.to_vec(),
            }),
        )
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert_eq!(coordinator_state.decision, CoordinatorDecision::Commit);
    assert_eq!(coordinator_state.prepared_count, 2);
    assert_eq!(coordinator_state.resolved_count, 2);
    assert!(
        client
            .query::<ReadPendingCrossCellTransactions>(
                &coordinator_target,
                None,
                Json(ReadPendingCrossCellTransactionsInput {
                    after: None,
                    limit: 1,
                }),
            )
            .await
            .unwrap()
            .output
            .0
            .is_empty()
    );
    let cross_key_info = storage
        .table_key_info("123456789012", "Books")
        .await
        .unwrap();
    assert_eq!(
        storage
            .get_item(&cross_key_info, &cross_left)
            .await
            .unwrap(),
        Some(cross_left)
    );
    assert_eq!(
        storage
            .get_item(&cross_key_info, &cross_right)
            .await
            .unwrap(),
        Some(cross_right)
    );
    for (position, side) in sides.into_iter().enumerate() {
        client
            .command::<PartitionDelete>(
                &side.0,
                identity(121 + u8::try_from(position).unwrap()),
                Json(PartitionDeleteInput {
                    return_old: true,
                    table_id: table.id.clone(),
                    epoch: side.1,
                    key: side.2,
                    condition: None,
                    ttl: false,
                }),
            )
            .await
            .unwrap();
    }
    let boundary = fence(&table, "f");
    let children = [
        PartitionSpec {
            table: table.clone(),
            partition_id: [3; 16],
            lower: None,
            upper: Some(boundary.clone()),
            epoch: partitions[0].epoch + 1,
        },
        PartitionSpec {
            table: table.clone(),
            partition_id: [4; 16],
            lower: Some(boundary.clone()),
            upper: Some(split.clone()),
            epoch: partitions[0].epoch + 1,
        },
    ];
    let plan = SplitPlan {
        source: partitions[0].clone(),
        children: children.clone(),
    };
    let next_route = TableRoute {
        table_id: table.id.clone(),
        epoch: 2,
        partitions: vec![
            children[0].clone(),
            children[1].clone(),
            partitions[1].clone(),
        ],
    };
    let mut empty_child = plan.clone();
    empty_child.children[0].upper = Some(RangeKey::minimum());
    empty_child.children[1].lower = Some(RangeKey::minimum());
    let invalid = client
        .command::<beyonddb::BeginDirectoryTransfer>(
            &route_target,
            identity(40),
            Json(empty_child.into()),
        )
        .await;
    assert!(matches!(
        invalid,
        Err(InvocationError::Rejected(result))
            if !result.output.0
    ));
    let begun = client
        .command::<beyonddb::BeginDirectoryTransfer>(
            &route_target,
            identity(41),
            Json(plan.clone().into()),
        )
        .await
        .unwrap();
    assert!(begun.output.0);
    let replayed = client
        .command::<beyonddb::BeginDirectoryTransfer>(
            &route_target,
            identity(42),
            Json(plan.clone().into()),
        )
        .await
        .unwrap();
    assert!(replayed.output.0);
    let mut conflict = plan.clone();
    let different_boundary = fence(&table, "h");
    conflict.children[0].upper = Some(different_boundary.clone());
    conflict.children[1].lower = Some(different_boundary.clone());
    let competing = client
        .command::<beyonddb::BeginDirectoryTransfer>(
            &route_target,
            identity(43),
            Json(conflict.into()),
        )
        .await;
    assert!(matches!(
        competing,
        Err(InvocationError::Rejected(result))
            if !result.output.0
    ));
    let pending = client
        .query::<beyonddb::ReadDirectoryTransfer>(
            &route_target,
            Some(begun.receipt),
            Json(beyonddb::DirectoryPartitionInput {
                table_id: table.id.clone(),
                partition_id: plan.source.partition_id,
            }),
        )
        .await
        .unwrap();
    assert_eq!(pending.output.0, Some(plan.clone().into()));
    let fenced_write = client
        .command::<PutItem>(
            &account,
            identity(30),
            Json(PutItemInput {
                table_name: table.table_name.clone(),
                table_id: table.id.clone(),
                item: key_in_range(&table.id, &table.key_schema, true, 700),
                condition: None,
            }),
        )
        .await;
    assert!(matches!(
        fenced_write,
        Err(InvocationError::Rejected(result))
            if result.output.0 == ItemMutationOutcome::TableNotFound
    ));
    let left_key = key_in_range(&table.id, &table.key_schema, true, 0);
    let right_key = key_in_range(&table.id, &table.key_schema, false, 0);
    let left_streams = stream_count(&client, &left_target, &table.id).await;
    let right_streams = stream_count(&client, &right_target, &table.id).await;
    for (target, key, epoch, byte) in [
        (&left_target, &left_key, partitions[0].epoch, 18),
        (&right_target, &right_key, partitions[1].epoch, 19),
    ] {
        let written = client
            .command::<PartitionPut>(
                target,
                identity(byte),
                Json(PartitionPutInput {
                    table_id: table.id.clone(),
                    epoch,
                    item: key.clone(),
                    condition: None,
                }),
            )
            .await
            .unwrap();
        assert_eq!(written.output.0, PartitionPutOutcome::Applied(None));
        let read = client
            .query::<PartitionGet>(
                target,
                Some(written.receipt),
                Json(PartitionGetInput {
                    table_id: table.id.clone(),
                    epoch,
                    key: key.clone(),
                }),
            )
            .await
            .unwrap();
        assert_eq!(read.output.0, PartitionGetOutcome::Found(Some(key.clone())));
        let page = client
            .query::<PartitionScan>(
                target,
                Some(written.receipt),
                Json(PartitionScanInput {
                    index_name: None,
                    table_id: table.id.clone(),
                    epoch,
                    limit: Some(1),
                    exclusive_start_key: None,
                }),
            )
            .await
            .unwrap();
        assert_eq!(
            page.output.0,
            PartitionScanOutcome::Page {
                items: vec![key.clone()],
                last_evaluated_key: None,
            }
        );
    }
    assert_eq!(
        stream_count(&client, &left_target, &table.id).await,
        left_streams + 1
    );
    assert_eq!(
        stream_count(&client, &right_target, &table.id).await,
        right_streams + 1
    );
    let left_page = client
        .query::<ReadPartitionStreamJournal>(
            &left_target,
            None,
            Json(StreamJournalInput {
                table_id: table.id.clone(),
                label: "2026-09-27T00:00:00.000".into(),
                after_sequence: None,
                limit: 100,
            }),
        )
        .await
        .unwrap();
    assert!(
        matches!(left_page.output.0, StreamJournalOutcome::Page { records, .. } if i64::try_from(records.len()).unwrap() == left_streams + 1)
    );
    let second_left_key = key_in_range(&table.id, &table.key_schema, true, 100);
    client
        .command::<PartitionPut>(
            &left_target,
            identity(26),
            Json(PartitionPutInput {
                table_id: table.id.clone(),
                epoch: 1,
                item: second_left_key.clone(),
                condition: None,
            }),
        )
        .await
        .unwrap();
    let first_page = client
        .query::<PartitionScan>(
            &left_target,
            None,
            Json(PartitionScanInput {
                index_name: None,
                table_id: table.id.clone(),
                epoch: 1,
                limit: Some(1),
                exclusive_start_key: None,
            }),
        )
        .await
        .unwrap();
    let PartitionScanOutcome::Page {
        mut items,
        last_evaluated_key: Some(cursor),
    } = first_page.output.0
    else {
        panic!("expected first partition page and continuation")
    };
    let second_page = client
        .query::<PartitionScan>(
            &left_target,
            None,
            Json(PartitionScanInput {
                index_name: None,
                table_id: table.id.clone(),
                epoch: 1,
                limit: Some(1),
                exclusive_start_key: Some(cursor),
            }),
        )
        .await
        .unwrap();
    let PartitionScanOutcome::Page {
        items: remaining,
        last_evaluated_key: None,
    } = second_page.output.0
    else {
        panic!("expected final partition page")
    };
    items.extend(remaining);
    assert_eq!(items.len(), 2);
    assert!(items.contains(&left_key));
    assert!(items.contains(&second_left_key));
    let wrong = client
        .command::<PartitionPut>(
            &right_target,
            identity(20),
            Json(PartitionPutInput {
                table_id: table.id.clone(),
                epoch: 1,
                item: left_key.clone(),
                condition: None,
            }),
        )
        .await;
    assert!(matches!(
        wrong,
        Err(InvocationError::Rejected(result))
            if result.output.0 == PartitionPutOutcome::WrongPartition
    ));
    let stale = client
        .query::<PartitionGet>(
            &left_target,
            None,
            Json(PartitionGetInput {
                table_id: table.id.clone(),
                epoch: 2,
                key: left_key.clone(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(stale.output.0, PartitionGetOutcome::StaleRoute);
    let maps = ExpressionMaps::new(
        HashMap::new(),
        HashMap::from([("title".into(), AttributeValue::S("Updated".into()))]),
    );
    let actions = [UpdateAction::Set {
        path: vec![PathElement::Attribute("title".into())],
        value: Expr::Placeholder("title".into()),
    }];
    let key_info = storage
        .table_key_info("123456789012", "Books")
        .await
        .unwrap();
    let transaction = storage
        .transact_get_items(&[
            TransactGetOp {
                key_info: &key_info,
                key: &left_key,
            },
            TransactGetOp {
                key_info: &key_info,
                key: &second_left_key,
            },
        ])
        .await
        .unwrap();
    assert_eq!(
        transaction,
        vec![Some(left_key.clone()), Some(second_left_key.clone())]
    );
    let cross_cell = storage
        .transact_get_items(&[
            TransactGetOp {
                key_info: &key_info,
                key: &left_key,
            },
            TransactGetOp {
                key_info: &key_info,
                key: &right_key,
            },
        ])
        .await;
    assert_eq!(
        cross_cell.unwrap(),
        vec![Some(left_key.clone()), Some(right_key.clone())]
    );
    let tx_maps = ExpressionMaps::default();
    let rolled_back = key_in_range(&table.id, &table.key_schema, true, 300);
    let not_exists = Expr::Function {
        name: "attribute_not_exists".into(),
        args: vec![Expr::Path(vec![PathElement::Attribute("id".into())])],
    };
    let failed = storage
        .transact_write_items(
            &[
                TransactWriteOp::Put {
                    key_info: &key_info,
                    item: &rolled_back,
                    condition: None,
                    maps: &tx_maps,
                    return_values_on_ccf: Default::default(),
                    stream: None,
                },
                TransactWriteOp::Put {
                    key_info: &key_info,
                    item: &left_key,
                    condition: Some(&not_exists),
                    maps: &tx_maps,
                    return_values_on_ccf: ReturnValuesOnConditionCheckFailure::AllOld,
                    stream: None,
                },
            ],
            None,
        )
        .await;
    assert!(matches!(
        failed,
        Err(StorageError::TransactionCanceled(reasons))
            if reasons[1].code == "ConditionalCheckFailed"
                && reasons[1].item == Some(left_key.clone())
    ));
    assert_eq!(
        storage.get_item(&key_info, &rolled_back).await.unwrap(),
        None
    );
    storage
        .transact_write_items(
            &[
                TransactWriteOp::Put {
                    key_info: &key_info,
                    item: &left_key,
                    condition: None,
                    maps: &tx_maps,
                    return_values_on_ccf: Default::default(),
                    stream: None,
                },
                TransactWriteOp::Put {
                    key_info: &key_info,
                    item: &second_left_key,
                    condition: None,
                    maps: &tx_maps,
                    return_values_on_ccf: Default::default(),
                    stream: None,
                },
            ],
            None,
        )
        .await
        .unwrap();
    let token = IdempotencyKey {
        account_id: "123456789012",
        token: "same-account-token",
        fingerprint: "left-write",
    };
    let left_write = [TransactWriteOp::Put {
        key_info: &key_info,
        item: &left_key,
        condition: None,
        maps: &tx_maps,
        return_values_on_ccf: Default::default(),
        stream: None,
    }];
    storage
        .transact_write_items(&left_write, Some(token))
        .await
        .unwrap();
    let replay = storage
        .transact_write_items(
            &left_write,
            Some(IdempotencyKey {
                account_id: "123456789012",
                token: "same-account-token",
                fingerprint: "left-write",
            }),
        )
        .await;
    assert!(matches!(replay, Err(StorageError::IdempotentReplay)));
    let right_new = key_in_range(&table.id, &table.key_schema, false, 600);
    let reused = storage
        .transact_write_items(
            &[TransactWriteOp::Put {
                key_info: &key_info,
                item: &right_new,
                condition: None,
                maps: &tx_maps,
                return_values_on_ccf: Default::default(),
                stream: None,
            }],
            Some(IdempotencyKey {
                account_id: "123456789012",
                token: "same-account-token",
                fingerprint: "right-write",
            }),
        )
        .await;
    assert!(matches!(reused, Err(StorageError::IdempotentMismatch)));
    assert_eq!(storage.get_item(&key_info, &right_new).await.unwrap(), None);
    let cross_write = storage
        .transact_write_items(
            &[
                TransactWriteOp::Put {
                    key_info: &key_info,
                    item: &rolled_back,
                    condition: None,
                    maps: &tx_maps,
                    return_values_on_ccf: Default::default(),
                    stream: None,
                },
                TransactWriteOp::Put {
                    key_info: &key_info,
                    item: &right_key,
                    condition: None,
                    maps: &tx_maps,
                    return_values_on_ccf: Default::default(),
                    stream: None,
                },
            ],
            None,
        )
        .await;
    cross_write.unwrap();
    assert_eq!(
        storage.get_item(&key_info, &rolled_back).await.unwrap(),
        Some(rolled_back.clone())
    );
    storage
        .delete_item(&key_info, &rolled_back, false, None, &tx_maps, None)
        .await
        .unwrap();
    let (first_range, continuation) = storage
        .scan(&key_info, Some(2), None, None, None, None)
        .await
        .unwrap();
    assert_eq!(first_range.len(), 2);
    assert!(first_range.contains(&left_key));
    assert!(first_range.contains(&second_left_key));
    let (second_range, end) = storage
        .scan(&key_info, Some(2), continuation.as_ref(), None, None, None)
        .await
        .unwrap();
    assert_eq!(second_range, vec![right_key.clone()]);
    assert_eq!(end, None);
    let routed_key = key_in_range(&table.id, &table.key_schema, false, 200);
    let previous = storage
        .put_item(
            &key_info,
            routed_key.clone(),
            true,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(previous, None);
    let not_exists = Expr::Function {
        name: "attribute_not_exists".into(),
        args: vec![Expr::Path(vec![PathElement::Attribute("id".into())])],
    };
    let conditional = storage
        .put_item(
            &key_info,
            routed_key.clone(),
            true,
            Some(&not_exists),
            &ExpressionMaps::default(),
            None,
        )
        .await;
    assert!(matches!(
        conditional,
        Err(StorageError::ConditionFailed(Some(old))) if old == routed_key
    ));
    let account_item = client
        .query::<GetItem>(
            &account,
            None,
            Json(GetItemInput {
                table_name: "Books".into(),
                table_id: table.id.clone(),
                key: routed_key.clone(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(account_item.output.0, GetItemOutcome::TableNotFound);
    assert_eq!(
        storage.get_item(&key_info, &routed_key).await.unwrap(),
        Some(routed_key.clone())
    );
    let (previous, changed) = storage
        .update_item(
            &key_info,
            &routed_key,
            &actions,
            true,
            true,
            None,
            &maps,
            None,
        )
        .await
        .unwrap();
    let mut changed_item = routed_key.clone();
    changed_item.insert("title".into(), AttributeValue::S("Updated".into()));
    assert_eq!(
        (previous, changed),
        (Some(routed_key.clone()), Some(changed_item.clone()))
    );
    assert_eq!(
        storage
            .delete_item(
                &key_info,
                &routed_key,
                true,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .unwrap(),
        Some(changed_item)
    );
    assert_eq!(
        storage.get_item(&key_info, &routed_key).await.unwrap(),
        None
    );
    let updated = client
        .command::<PartitionUpdate>(
            &right_target,
            identity(24),
            Json(PartitionUpdateInput::from_expression(
                table.id.clone(),
                partitions[1].epoch,
                right_key.clone(),
                &actions,
                None,
                &maps,
            )),
        )
        .await
        .unwrap();
    let mut updated_item = right_key.clone();
    updated_item.insert("title".into(), AttributeValue::S("Updated".into()));
    assert_eq!(
        updated.output.0,
        PartitionUpdateOutcome::Applied {
            old: Some(right_key.clone()),
            new: updated_item.clone(),
        }
    );
    let deleted = client
        .command::<PartitionDelete>(
            &right_target,
            identity(25),
            Json(PartitionDeleteInput {
                return_old: true,
                table_id: table.id.clone(),
                epoch: 1,
                key: right_key.clone(),
                condition: None,
                ttl: false,
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        deleted.output.0,
        PartitionDeleteOutcome::Applied(Some(updated_item))
    );
    let absent = client
        .query::<PartitionGet>(
            &right_target,
            Some(deleted.receipt),
            Json(PartitionGetInput {
                table_id: table.id.clone(),
                epoch: 1,
                key: right_key,
            }),
        )
        .await
        .unwrap();
    assert_eq!(absent.output.0, PartitionGetOutcome::Found(None));
    let export_input = || PartitionScanInput {
        index_name: None,
        table_id: table.id.clone(),
        epoch: 1,
        limit: Some(1),
        exclusive_start_key: None,
    };
    let premature_export = client
        .query::<PartitionExport>(&left_target, None, Json(export_input()))
        .await
        .unwrap();
    assert_eq!(premature_export.output.0, PartitionScanOutcome::NotSealed);
    let child_epoch = next_route.partitions[0].epoch;
    let seal = PartitionSeal {
        table_id: table.id.clone(),
        source_partition_id: left_id,
        epoch: 1,
        next_epoch: child_epoch,
        source_lower: None,
        source_upper: Some(split.clone()),
        boundary: boundary.clone(),
        left_partition_id: next_route.partitions[0].partition_id,
        right_partition_id: next_route.partitions[1].partition_id,
    };
    let intent_item = key_in_range(&table.id, &table.key_schema, true, 800);
    let prepare_input = PreparePartitionTransactionInput {
        table_id: table.id.clone(),
        epoch: 1,
        transaction_id: [90; 16],
        coordinator_cell: *account.cell_id().as_bytes(),
        coordinator_key: [90; 16].to_vec(),
        operations: vec![TransactionOperation::Put(PutItemInput {
            table_name: table.table_name.clone(),
            table_id: table.id.clone(),
            item: intent_item.clone(),
            condition: None,
        })],
    };
    let prepared = transaction_command!(
        client,
        PreparePartitionTransaction,
        &left_target,
        identity(90),
        Json(prepare_input.clone()),
    )
    .await
    .unwrap();
    assert_eq!(prepared.output.0, PrepareTransactionOutcome::Prepared);
    let prepared_state = client
        .query::<ReadPartitionTransaction>(
            &left_target,
            Some(prepared.receipt),
            Json(ReadTransactionInput {
                transaction_id: prepare_input.transaction_id,
                coordinator_cell: prepare_input.coordinator_cell,
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        prepared_state.output.0,
        ParticipantTransactionState::Prepared
    );
    let hidden = client
        .query::<PartitionGet>(
            &left_target,
            Some(prepared.receipt),
            Json(PartitionGetInput {
                table_id: table.id.clone(),
                epoch: 1,
                key: intent_item.clone(),
            }),
        )
        .await
        .unwrap();
    assert!(
        matches!(hidden.output.0, PartitionGetOutcome::Conflict(conflict) if conflict.transaction.transaction_id == prepare_input.transaction_id && conflict.coordinator_key == prepare_input.coordinator_key)
    );
    let conflicting_write = client
        .command::<PartitionPut>(
            &left_target,
            identity(91),
            Json(PartitionPutInput {
                table_id: table.id.clone(),
                epoch: 1,
                item: intent_item.clone(),
                condition: None,
            }),
        )
        .await;
    assert!(matches!(
        conflicting_write,
        Err(InvocationError::Rejected(result))
            if result.output.0 == PartitionPutOutcome::TransactionConflict
    ));
    let conflicting_transaction = client
        .command::<PartitionTransactWrite>(
            &left_target,
            identity(100),
            Json(PartitionTransactWriteInput {
                table_id: table.id.clone(),
                epoch: 1,
                operations: prepare_input.operations.clone(),
            }),
        )
        .await;
    assert!(matches!(
        conflicting_transaction,
        Err(InvocationError::Rejected(result))
            if result.output.0 == (PartitionTransactWriteOutcome::Rejected { index: 0, reason: beyonddb::TransactionFailure::Conflict })
    ));
    let premature_seal = client
        .command::<SealPartition>(&left_target, identity(92), Json(seal.clone()))
        .await;
    assert!(matches!(
        premature_seal,
        Err(InvocationError::Rejected(result))
            if result.output.0 == SealPartitionOutcome::InFlightTransaction
    ));
    let abort = client
        .command::<ResolvePartitionTransaction>(
            &left_target,
            identity(93),
            Json(ResolveTransactionInput {
                transaction_id: prepare_input.transaction_id,
                coordinator_cell: prepare_input.coordinator_cell,
                commit: false,
            }),
        )
        .await
        .unwrap();
    assert_eq!(abort.output.0, ResolveTransactionOutcome::Aborted);
    let retry_after_abort = transaction_command!(
        client,
        PreparePartitionTransaction,
        &left_target,
        identity(94),
        Json(prepare_input)
    )
    .await;
    assert!(matches!(
        retry_after_abort,
        Err(InvocationError::Rejected(result))
            if result.output.0 == PrepareTransactionOutcome::Aborted
    ));
    let late_prepare = PreparePartitionTransactionInput {
        table_id: table.id.clone(),
        epoch: 1,
        transaction_id: [98; 16],
        coordinator_cell: *account.cell_id().as_bytes(),
        coordinator_key: [98; 16].to_vec(),
        operations: vec![TransactionOperation::Put(PutItemInput {
            table_name: table.table_name.clone(),
            table_id: table.id.clone(),
            item: intent_item.clone(),
            condition: None,
        })],
    };
    client
        .command::<ResolvePartitionTransaction>(
            &left_target,
            identity(98),
            Json(ResolveTransactionInput {
                transaction_id: late_prepare.transaction_id,
                coordinator_cell: late_prepare.coordinator_cell,
                commit: false,
            }),
        )
        .await
        .unwrap();
    let fenced_late_prepare = transaction_command!(
        client,
        PreparePartitionTransaction,
        &left_target,
        identity(99),
        Json(late_prepare)
    )
    .await;
    assert!(matches!(
        fenced_late_prepare,
        Err(InvocationError::Rejected(result))
            if result.output.0 == PrepareTransactionOutcome::Aborted
    ));
    let committed_input = PreparePartitionTransactionInput {
        table_id: table.id.clone(),
        epoch: 1,
        transaction_id: [95; 16],
        coordinator_cell: *account.cell_id().as_bytes(),
        coordinator_key: [95; 16].to_vec(),
        operations: vec![TransactionOperation::Put(PutItemInput {
            table_name: table.table_name.clone(),
            table_id: table.id.clone(),
            item: intent_item.clone(),
            condition: None,
        })],
    };
    transaction_command!(
        client,
        PreparePartitionTransaction,
        &left_target,
        identity(95),
        Json(committed_input.clone()),
    )
    .await
    .unwrap();
    let commit = client
        .command::<ResolvePartitionTransaction>(
            &left_target,
            identity(96),
            Json(ResolveTransactionInput {
                transaction_id: committed_input.transaction_id,
                coordinator_cell: committed_input.coordinator_cell,
                commit: true,
            }),
        )
        .await
        .unwrap();
    assert_eq!(commit.output.0, ResolveTransactionOutcome::Committed);
    let visible = client
        .query::<PartitionGet>(
            &left_target,
            Some(commit.receipt),
            Json(PartitionGetInput {
                table_id: table.id.clone(),
                epoch: 1,
                key: intent_item.clone(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        visible.output.0,
        PartitionGetOutcome::Found(Some(intent_item.clone()))
    );
    client
        .command::<PartitionDelete>(
            &left_target,
            identity(97),
            Json(PartitionDeleteInput {
                return_old: true,
                table_id: table.id.clone(),
                epoch: 1,
                key: intent_item,
                condition: None,
                ttl: false,
            }),
        )
        .await
        .unwrap();
    let sealed = client
        .command::<SealPartition>(&left_target, identity(44), Json(seal.clone()))
        .await
        .unwrap();
    assert_eq!(sealed.output.0, SealPartitionOutcome::Sealed);
    let source_status = client
        .query::<ReadPartitionState>(&left_target, Some(sealed.receipt), Json(()))
        .await
        .unwrap();
    assert_eq!(
        source_status.output.0.unwrap().state,
        PartitionState::Sealed(seal.clone())
    );
    let sealed_page = client
        .query::<ReadPartitionStreamJournal>(
            &left_target,
            Some(sealed.receipt),
            Json(StreamJournalInput {
                table_id: table.id.clone(),
                label: "2026-09-27T00:00:00.000".into(),
                after_sequence: None,
                limit: 1_000,
            }),
        )
        .await
        .unwrap();
    let StreamJournalOutcome::Page {
        closed: true,
        last_sequence: Some(last_sequence),
        ..
    } = sealed_page.output.0
    else {
        panic!("sealed source must retain its journal");
    };
    let exhausted = client
        .query::<ReadPartitionStreamJournal>(
            &left_target,
            Some(sealed.receipt),
            Json(StreamJournalInput {
                table_id: table.id.clone(),
                label: "2026-09-27T00:00:00.000".into(),
                after_sequence: Some(last_sequence.clone()),
                limit: 1_000,
            }),
        )
        .await
        .unwrap();
    assert!(
        matches!(exhausted.output.0, StreamJournalOutcome::Page { records, closed: true, .. } if records.is_empty())
    );
    let shard_id = format!(
        "shardId-{}-{:032x}-2026-09-27T00:00:00.000",
        table.id,
        u128::from_be_bytes(left_id)
    );
    let stream_arn =
        "arn:aws:dynamodb:us-east-1:123456789012:table/Books/stream/2026-09-27T00:00:00.000";
    storage
        .validate_shard("123456789012", stream_arn, &shard_id)
        .await
        .unwrap();
    let (final_records, final_continuation) = storage
        .get_stream_records("123456789012", &shard_id, None, 1_000)
        .await
        .unwrap();
    assert!(!final_records.is_empty());
    assert_eq!(final_continuation, StreamContinuation::End);
    assert_eq!(
        storage.latest_sequence_number(&shard_id).await.unwrap(),
        Some(last_sequence.clone())
    );
    assert!(matches!(
        storage
            .validate_shard("999999999999", stream_arn, &shard_id)
            .await,
        Err(StorageError::TableNotFound(_))
    ));
    let (records, continuation) = storage
        .get_stream_records("123456789012", &shard_id, Some(&last_sequence), 100)
        .await
        .unwrap();
    assert!(records.is_empty());
    assert_eq!(continuation, StreamContinuation::End);
    assert!(matches!(
        storage
            .get_stream_records("999999999999", &shard_id, None, 100)
            .await,
        Err(StorageError::TableNotFound(_))
    ));
    let replayed = client
        .command::<SealPartition>(&left_target, identity(45), Json(seal.clone()))
        .await
        .unwrap();
    assert_eq!(replayed.output.0, SealPartitionOutcome::Sealed);
    let mut different_seal = seal.clone();
    different_seal.boundary = fence(&table, "h");
    let conflict = client
        .command::<SealPartition>(&left_target, identity(46), Json(different_seal))
        .await;
    assert!(matches!(
        conflict,
        Err(InvocationError::Rejected(result))
            if result.output.0 == SealPartitionOutcome::Conflict
    ));
    let fenced_write = client
        .command::<PartitionPut>(
            &left_target,
            identity(47),
            Json(PartitionPutInput {
                table_id: table.id.clone(),
                epoch: 1,
                item: left_key.clone(),
                condition: None,
            }),
        )
        .await;
    assert!(matches!(
        fenced_write,
        Err(InvocationError::Rejected(result))
            if result.output.0 == PartitionPutOutcome::Sealed
    ));
    let fenced_delete = client
        .command::<PartitionDelete>(
            &left_target,
            identity(48),
            Json(PartitionDeleteInput {
                return_old: true,
                table_id: table.id.clone(),
                epoch: 1,
                key: left_key.clone(),
                condition: None,
                ttl: false,
            }),
        )
        .await;
    assert!(matches!(
        fenced_delete,
        Err(InvocationError::Rejected(result))
            if result.output.0 == PartitionDeleteOutcome::Sealed
    ));
    let fenced_update = client
        .command::<PartitionUpdate>(
            &left_target,
            identity(49),
            Json(PartitionUpdateInput::from_expression(
                table.id.clone(),
                1,
                left_key.clone(),
                &actions,
                None,
                &maps,
            )),
        )
        .await;
    assert!(matches!(
        fenced_update,
        Err(InvocationError::Rejected(result))
            if result.output.0 == PartitionUpdateOutcome::Sealed
    ));
    let fenced_read = client
        .query::<PartitionGet>(
            &left_target,
            Some(sealed.receipt),
            Json(PartitionGetInput {
                table_id: table.id.clone(),
                epoch: 1,
                key: left_key.clone(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(fenced_read.output.0, PartitionGetOutcome::Sealed);
    let fenced_scan = client
        .query::<PartitionScan>(&left_target, None, Json(export_input()))
        .await
        .unwrap();
    assert_eq!(fenced_scan.output.0, PartitionScanOutcome::Sealed);
    assert!(matches!(
        storage.get_item(&key_info, &left_key).await,
        Err(StorageError::Transient(_))
    ));
    let mut exported = Vec::new();
    let mut cursor = None;
    loop {
        let mut input = export_input();
        input.exclusive_start_key = cursor;
        let response = client
            .query::<PartitionExport>(&left_target, None, Json(input))
            .await
            .unwrap();
        let PartitionScanOutcome::Page {
            items,
            last_evaluated_key,
        } = response.output.0
        else {
            panic!("expected a sealed export page")
        };
        exported.extend(items);
        let Some(next) = last_evaluated_key else {
            break;
        };
        cursor = Some(next);
    }
    assert_eq!(exported.len(), 2);
    assert!(exported.contains(&left_key));
    assert!(exported.contains(&second_left_key));
    let child_targets = [
        data_target("123456789012", &table.id, &[3; 16]).unwrap(),
        data_target("123456789012", &table.id, &[4; 16]).unwrap(),
    ];
    let child_client = cell_client.clone();
    for (index, child) in child_targets.iter().enumerate() {
        let installed = child_client
            .command::<InstallPartition>(
                child,
                identity(52 + u8::try_from(index).unwrap()),
                Json(PartitionInstall::Importing {
                    spec: next_route.partitions[index].clone(),
                    source: seal.clone(),
                }),
            )
            .await
            .unwrap();
        assert_eq!(installed.output.0, InstallPartitionOutcome::Installed);
    }
    let first_item = exported[0].clone();
    let first_hash = ordered_data_key(&table.id, &first_item, &table.key_schema).unwrap();
    let first_child = usize::from(first_hash >= boundary);
    let premature_read = child_client
        .query::<PartitionGet>(
            &child_targets[first_child],
            None,
            Json(PartitionGetInput {
                table_id: table.id.clone(),
                epoch: child_epoch,
                key: first_item.clone(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(premature_read.output.0, PartitionGetOutcome::NotReady);
    let premature_write = child_client
        .command::<PartitionPut>(
            &child_targets[first_child],
            identity(65),
            Json(PartitionPutInput {
                table_id: table.id.clone(),
                epoch: child_epoch,
                item: first_item.clone(),
                condition: None,
            }),
        )
        .await;
    assert!(matches!(
        premature_write,
        Err(InvocationError::Rejected(result))
            if result.output.0 == PartitionPutOutcome::NotReady
    ));
    let premature_scan = child_client
        .query::<PartitionScan>(
            &child_targets[first_child],
            None,
            Json(PartitionScanInput {
                index_name: None,
                table_id: table.id.clone(),
                epoch: child_epoch,
                limit: Some(1),
                exclusive_start_key: None,
            }),
        )
        .await
        .unwrap();
    assert_eq!(premature_scan.output.0, PartitionScanOutcome::NotReady);
    let wrong_child = child_client
        .command::<ImportPartitionItem>(
            &child_targets[1 - first_child],
            identity(54),
            Json(PartitionImportInput {
                table_id: table.id.clone(),
                epoch: child_epoch,
                item: first_item.clone(),
            }),
        )
        .await;
    assert!(matches!(
        wrong_child,
        Err(InvocationError::Rejected(result))
            if result.output.0 == PartitionImportOutcome::WrongPartition
    ));
    let mut expected = [ImportSummary::default(), ImportSummary::default()];
    for item in &exported {
        let hash = ordered_data_key(&table.id, item, &table.key_schema).unwrap();
        let index = usize::from(hash >= boundary);
        expected[index].include(item, &table.key_schema).unwrap();
    }
    let incomplete = child_client
        .command::<ActivateImportedPartition>(
            &child_targets[first_child],
            identity(55),
            Json(ActivateImportedPartitionInput {
                source: seal.clone(),
                expected: expected[first_child].clone(),
            }),
        )
        .await;
    assert!(matches!(
        incomplete,
        Err(InvocationError::Rejected(result))
            if result.output.0 == ActivateImportedPartitionOutcome::Incomplete
    ));
    let copied = child_client
        .command::<ImportPartitionItem>(
            &child_targets[first_child],
            identity(56),
            Json(PartitionImportInput {
                table_id: table.id.clone(),
                epoch: child_epoch,
                item: first_item.clone(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(copied.output.0, PartitionImportOutcome::Imported);
    let duplicate = child_client
        .command::<ImportPartitionItem>(
            &child_targets[first_child],
            identity(58),
            Json(PartitionImportInput {
                table_id: table.id.clone(),
                epoch: child_epoch,
                item: first_item.clone(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(duplicate.output.0, PartitionImportOutcome::Imported);
    let child_shard = format!(
        "shardId-{}-{:032x}-2026-09-27T00:00:00.000",
        table.id,
        u128::from_be_bytes(next_route.partitions[first_child].partition_id)
    );
    assert!(matches!(
        storage
            .get_stream_records("123456789012", &child_shard, None, 100)
            .await,
        Err(StorageError::TableNotFound(_))
    ));
    assert!(matches!(
        storage
            .validate_shard("123456789012", stream_arn, &child_shard)
            .await,
        Err(StorageError::TableNotFound(_))
    ));
    let mut changed_item = first_item.clone();
    changed_item.insert("title".into(), AttributeValue::S("newer".into()));
    let conflict = child_client
        .command::<ImportPartitionItem>(
            &child_targets[first_child],
            identity(59),
            Json(PartitionImportInput {
                table_id: table.id.clone(),
                epoch: child_epoch,
                item: changed_item.clone(),
            }),
        )
        .await;
    assert!(matches!(
        conflict,
        Err(InvocationError::Rejected(result))
            if result.output.0 == PartitionImportOutcome::Conflict
    ));
    let early_write = child_client
        .command::<PartitionPut>(
            &child_targets[first_child],
            identity(67),
            Json(PartitionPutInput {
                table_id: table.id.clone(),
                epoch: child_epoch,
                item: changed_item.clone(),
                condition: None,
            }),
        )
        .await;
    assert!(matches!(
        early_write,
        Err(InvocationError::Rejected(result))
            if result.output.0 == PartitionPutOutcome::NotReady
    ));
    let mut wrong_source = plan.clone();
    wrong_source.source.partition_id = [9; 16];
    let mut wrong_children = plan.clone();
    wrong_children.children[0].partition_id = [9; 16];
    for (request, submitted) in [(64, wrong_source), (138, wrong_children)] {
        let wrong_commit = client
            .command::<beyonddb::PublishDirectoryTransfer>(
                &route_target,
                identity(request),
                Json(submitted.into()),
            )
            .await;
        assert!(matches!(
            wrong_commit,
            Err(InvocationError::Rejected(result)) if !result.output.0
        ));
    }
    let controller_client = cell_client.clone();
    CellSplitController::new(controller_client.clone())
        .resume("123456789012", &plan)
        .await
        .unwrap();
    CellSplitController::new(controller_client.clone())
        .resume("123456789012", &plan)
        .await
        .unwrap();
    // Imported rows produce no stream events in either child. Check after
    // opening, since importing children deliberately hide their journals.
    assert_eq!(
        stream_count(&child_client, &child_targets[0], &table.id).await,
        0
    );
    assert_eq!(
        stream_count(&child_client, &child_targets[1], &table.id).await,
        0
    );
    let (records, continuation) = storage
        .get_stream_records("123456789012", &child_shard, None, 100)
        .await
        .unwrap();
    assert!(records.is_empty());
    assert_eq!(continuation, StreamContinuation::More(None));
    storage
        .validate_shard("123456789012", stream_arn, &child_shard)
        .await
        .unwrap();
    assert_eq!(
        storage.latest_sequence_number(&child_shard).await.unwrap(),
        None
    );
    let described_stream = storage
        .describe_stream(
            "123456789012",
            &extenddb_core::types::DescribeStreamInput {
                stream_arn: stream_arn.into(),
                limit: Some(2),
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(described_stream.shards[0].shard_id, shard_id);
    assert_eq!(
        described_stream.shards[0]
            .sequence_number_range
            .ending_sequence_number,
        Some(last_sequence)
    );
    let next_page = storage
        .describe_stream(
            "123456789012",
            &extenddb_core::types::DescribeStreamInput {
                stream_arn: stream_arn.into(),
                limit: Some(2),
                exclusive_start_shard_id: described_stream.last_evaluated_shard_id.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(next_page.shards.len(), 2);
    assert_eq!(
        next_page.shards[0].parent_shard_id.as_deref(),
        Some(shard_id.as_str())
    );
    assert_eq!(next_page.shards[1].parent_shard_id, None);
    let current_route = crate::single_leaf_route(&cell_client, &account, &table.id.clone()).await;
    assert_eq!(current_route, Some(next_route.clone()));
    for (hash, partition) in [
        (RangeKey::minimum(), &next_route.partitions[0]),
        (boundary.clone(), &next_route.partitions[1]),
        (split.clone(), &next_route.partitions[2]),
    ] {
        assert_eq!(
            point_range(&cell_client, &account, &table.id, hash).await,
            Some((partition.partition_id, partition.epoch))
        );
    }
    let opened = child_client
        .query::<ReadPartitionState>(&child_targets[first_child], None, Json(()))
        .await
        .unwrap();
    assert_eq!(
        opened.output.0.unwrap().state,
        PartitionState::Opened {
            source: seal.clone(),
            summary: expected[first_child].clone(),
        }
    );
    let published_storage = CellStorage::new(cell_client.clone(), "us-east-1");
    assert_eq!(
        published_storage
            .get_item(&key_info, &left_key)
            .await
            .unwrap(),
        Some(left_key.clone())
    );
    assert_eq!(
        published_storage
            .get_item(&key_info, &second_left_key)
            .await
            .unwrap(),
        Some(second_left_key.clone())
    );
    let right_after_split = key_in_range(&table.id, &table.key_schema, false, 300);
    assert_eq!(
        published_storage
            .put_item(
                &key_info,
                right_after_split.clone(),
                true,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        published_storage
            .get_item(&key_info, &right_after_split)
            .await
            .unwrap(),
        Some(right_after_split)
    );
    let newer = child_client
        .command::<PartitionPut>(
            &child_targets[first_child],
            identity(62),
            Json(PartitionPutInput {
                table_id: table.id.clone(),
                epoch: child_epoch,
                item: changed_item.clone(),
                condition: None,
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        newer.output.0,
        PartitionPutOutcome::Applied(Some(first_item.clone()))
    );
    let delayed_copy = child_client
        .command::<ImportPartitionItem>(
            &child_targets[first_child],
            identity(63),
            Json(PartitionImportInput {
                table_id: table.id.clone(),
                epoch: child_epoch,
                item: first_item.clone(),
            }),
        )
        .await;
    assert!(matches!(
        delayed_copy,
        Err(InvocationError::Rejected(result))
            if result.output.0 == PartitionImportOutcome::NotImporting
    ));
    let visible = child_client
        .query::<PartitionGet>(
            &child_targets[first_child],
            None,
            Json(PartitionGetInput {
                table_id: table.id.clone(),
                epoch: child_epoch,
                key: first_item.clone(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        visible.output.0,
        PartitionGetOutcome::Found(Some(changed_item.clone()))
    );
    let recovery_prepare = transaction_command!(
        child_client,
        PreparePartitionTransaction,
        &child_targets[first_child],
        identity(101),
        Json(PreparePartitionTransactionInput {
            table_id: table.id.clone(),
            epoch: child_epoch,
            transaction_id: [101; 16],
            coordinator_cell: *account.cell_id().as_bytes(),
            coordinator_key: [101; 16].to_vec(),
            operations: vec![TransactionOperation::Put(PutItemInput {
                table_name: table.table_name.clone(),
                table_id: table.id.clone(),
                item: changed_item.clone(),
                condition: None,
            })],
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        recovery_prepare.output.0,
        PrepareTransactionOutcome::Prepared
    );
    let mut recovery_participant = participants
        .iter()
        .find(|participant| {
            matches!(
                &participant.target,
                CoordinatorParticipantTarget::Data { partition_id, .. } if *partition_id == left_id
            )
        })
        .unwrap()
        .clone();
    recovery_participant.operations[0].index = 0;
    let pending_id = (0..=u16::MAX)
        .map(|suffix| {
            let mut id = transaction_id;
            id[14..].copy_from_slice(&suffix.to_be_bytes());
            id
        })
        .find(|id| {
            *id != transaction_id
                && beyonddb::coordinator_target("123456789012", id).unwrap() == coordinator_target
        })
        .unwrap();
    transaction_command!(
        client,
        BeginCrossCellTransaction,
        &coordinator_target,
        identity(126),
        Json(BeginCrossCellTransactionInput {
            account_id: "123456789012".into(),
            transaction_id: pending_id,
            token: None,
            participants: vec![recovery_participant.clone()],
        }),
    )
    .await
    .unwrap();
    let token_value = (0..=u16::MAX)
        .map(|suffix| format!("recovery-{suffix}"))
        .find(|token| {
            beyonddb::coordinator_target("123456789012", token.as_bytes()).unwrap()
                == coordinator_target
        })
        .unwrap();
    let token_id = [127; 16];
    transaction_command!(
        client,
        BeginCrossCellTransaction,
        &coordinator_target,
        identity(127),
        Json(BeginCrossCellTransactionInput {
            account_id: "123456789012".into(),
            transaction_id: token_id,
            token: Some(TransactionToken {
                account_id: "123456789012".into(),
                token: token_value.clone(),
                fingerprint: "recovery-request".into(),
            }),
            participants: vec![recovery_participant.clone()],
        }),
    )
    .await
    .unwrap();
    let abort_id = (0..=u16::MAX)
        .map(|suffix| {
            let mut id = transaction_id;
            id[14..].copy_from_slice(&suffix.to_be_bytes());
            id
        })
        .find(|id| {
            *id != transaction_id
                && *id != pending_id
                && beyonddb::coordinator_target("123456789012", id).unwrap() == coordinator_target
        })
        .unwrap();
    transaction_command!(
        client,
        BeginCrossCellTransaction,
        &coordinator_target,
        identity(128),
        Json(BeginCrossCellTransactionInput {
            account_id: "123456789012".into(),
            transaction_id: abort_id,
            token: None,
            participants: vec![recovery_participant],
        }),
    )
    .await
    .unwrap();
    // Preserve the actual SDK scope and owner fence for every state family,
    // including the retired source which is no longer reachable in the route.
    let targets = [
        account.clone(), route_target.clone(), coordinator_target.clone(),
        left_target.clone(), right_target.clone(),
        child_targets[0].clone(), child_targets[1].clone(),
    ];
    let mut before = Vec::new();
    for target in targets {
        let old = native_peer::identity(&observed, SOURCE, &target).unwrap();
        let owner = inspection.owner(&old.scope).await;
        assert_eq!(owner.node.as_deref(), Some(SOURCE));
        before.push((target, old, owner));
    }
    let stream_targets = [&left_target, &right_target, &child_targets[0], &child_targets[1]];
    let mut journals = Vec::new();
    for target in stream_targets {
        journals.push((native_peer::identity(&observed, SOURCE, target).unwrap().scope,
            stream_count(&client, target, &table.id).await));
    }
    source.stop().await;
    // Inspect complete non-owning replicas, never the SDK's live sparse cache.
    for (scope, expected_count) in journals {
        let snapshot = crate::capacity_snapshot::restore(&bucket, &scope, host.handle()).await;
        let count: i64 = crate::capacity_snapshot::open(&snapshot).query_row(
            "SELECT count(*) FROM ddb_stream_records WHERE table_id = ?1",
            [&table.id], |row| row.get(0),
        ).unwrap();
        assert_eq!(count, expected_count);
    }
    std::fs::remove_dir_all(directory.path().join(SOURCE)).unwrap();
    let replacement = "data-range-replacement";
    let mut restored = Peer::start(
        application.clone(), &directory.path().join(replacement), bucket.clone(),
        observed.clone(), true, host.handle(), core.handle(),
    ).await;
    inspection.departed_node(SOURCE, replacement).await;
    let restored_cell_client = restored.client.clone();
    let restored_client = restored_cell_client.clone();
    let restored_provisioner = Arc::new(CellInitialPartitionProvisioner::new(
        restored_cell_client.clone(), application,
    ));
    let restored_storage = CellStorage::new(restored_cell_client.clone(), "us-east-1")
        .with_transaction_coordinators(restored_provisioner.clone());
    let restored_child_target = &child_targets[first_child];
    assert!(next_route.partitions.iter().all(|part| part.partition_id != left_id));
    let registered = restored_client.query::<ListCoordinatorShards>(
        &account, None, Json(ListCoordinatorShardsInput {
            account_id: "123456789012".into(), after: None, limit: 100,
        }),
    ).await.unwrap().output.0;
    assert!(registered.iter().any(|entry| entry.shard == shard));
    let restored_decision = restored_client
        .query::<ReadCrossCellTransaction>(
            &coordinator_target,
            None,
            Json(ReadCrossCellTransactionInput {
                account_id: "123456789012".into(),
                transaction_id,
                routing_key: transaction_id.to_vec(),
            }),
        )
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert_eq!(restored_decision.decision, CoordinatorDecision::Commit);
    assert_eq!(restored_decision.resolved_count, 2);
    let restored_pending = restored_client
        .query::<ReadPendingCrossCellTransactions>(
            &coordinator_target,
            None,
            Json(ReadPendingCrossCellTransactionsInput {
                after: None,
                limit: 1,
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    assert_eq!(restored_pending.len(), 1);
    let next_pending = restored_client
        .query::<ReadPendingCrossCellTransactions>(
            &coordinator_target,
            None,
            Json(ReadPendingCrossCellTransactionsInput {
                after: Some(restored_pending[0].cursor.clone()),
                limit: 1,
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    assert_eq!(next_pending.len(), 1);
    let third_pending = restored_client
        .query::<ReadPendingCrossCellTransactions>(
            &coordinator_target,
            None,
            Json(ReadPendingCrossCellTransactionsInput {
                after: Some(next_pending[0].cursor.clone()),
                limit: 1,
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    assert_eq!(third_pending.len(), 1);
    let mut discovered = [
        restored_pending[0].clone(),
        next_pending[0].clone(),
        third_pending[0].clone(),
    ];
    discovered.sort_by_key(|entry| entry.transaction_id);
    assert_eq!(discovered[0].transaction_id, pending_id);
    assert_eq!(discovered[0].state, PendingTransactionState::Begin);
    assert_eq!(discovered[1].transaction_id, abort_id);
    assert_eq!(discovered[2].transaction_id, token_id);
    assert_eq!(discovered[2].routing_key, token_value.as_bytes());
    let restored_token = TransactionToken {
        account_id: "123456789012".into(),
        token: token_value.clone(),
        fingerprint: "recovery-request".into(),
    };
    assert_eq!(
        restored_client
            .query::<beyonddb::ReadCoordinatorToken>(
                &coordinator_target,
                None,
                Json(restored_token.clone()),
            )
            .await
            .unwrap()
            .output
            .0,
        beyonddb::ReadCoordinatorTokenOutcome::Found {
            transaction_id: token_id,
            decision: CoordinatorDecision::Begin
        }
    );
    assert!(matches!(
        restored_storage
            .finish_decided_cross_cell_transaction("123456789012", &pending_id, pending_id)
            .await,
        Err(StorageError::Transient(_))
    ));
    assert!(
        restored_client
            .query::<ReadPendingCrossCellTransactions>(
                &coordinator_target,
                None,
                Json(ReadPendingCrossCellTransactionsInput {
                    after: Some(third_pending[0].cursor.clone()),
                    limit: 1,
                }),
            )
            .await
            .unwrap()
            .output
            .0
            .is_empty()
    );
    restored_client
        .command::<DecideCrossCellTransaction>(
            &coordinator_target,
            identity(129),
            Json(DecideCrossCellTransactionInput {
                account_id: "123456789012".into(),
                transaction_id: abort_id,
                routing_key: abort_id.to_vec(),
                decision: CoordinatorDecision::Abort {
                    index: None,
                    reason: None,
                },
            }),
        )
        .await
        .unwrap();
    restored_storage
        .finish_decided_cross_cell_transaction("123456789012", &abort_id, abort_id)
        .await
        .unwrap();
    let aborted = restored_client
        .query::<ReadPartitionTransaction>(
            &left_target,
            None,
            Json(ReadTransactionInput {
                transaction_id: abort_id,
                coordinator_cell: *coordinator_target.cell_id().as_bytes(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(aborted.output.0, ParticipantTransactionState::Aborted);
    let abort_status = restored_client
        .query::<ReadCrossCellTransaction>(
            &coordinator_target,
            None,
            Json(ReadCrossCellTransactionInput {
                account_id: "123456789012".into(),
                transaction_id: abort_id,
                routing_key: abort_id.to_vec(),
            }),
        )
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert_eq!(abort_status.resolved_count, 1);
    restored_storage
        .recover_fenced_coordinator(&coordinator_target)
        .await
        .unwrap();
    assert!(
        restored_client
            .query::<ReadPendingCrossCellTransactions>(
                &coordinator_target,
                None,
                Json(ReadPendingCrossCellTransactionsInput {
                    after: None,
                    limit: 1,
                }),
            )
            .await
            .unwrap()
            .output
            .0
            .is_empty()
    );
    assert_eq!(
        restored_client
            .query::<beyonddb::ReadCoordinatorToken>(
                &coordinator_target,
                None,
                Json(restored_token),
            )
            .await
            .unwrap()
            .output
            .0,
        beyonddb::ReadCoordinatorTokenOutcome::Missing
    );
    let restored_participant = restored_client
        .query::<ReadCoordinatorParticipant>(
            &coordinator_target,
            None,
            Json(ReadCoordinatorParticipantInput {
                chunk: 0,
                account_id: "123456789012".into(),
                transaction_id,
                routing_key: transaction_id.to_vec(),
                position: 0,
            }),
        )
        .await
        .unwrap();
    // Terminal write payloads are compacted, while the durable decision and
    // participant tombstones above still fence replay after owner restart.
    assert!(restored_participant.output.is_none());
    let recovered_intent = restored_client
        .query::<ReadPartitionTransaction>(
            restored_child_target,
            None,
            Json(ReadTransactionInput {
                transaction_id: [101; 16],
                coordinator_cell: *account.cell_id().as_bytes(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        recovered_intent.output.0,
        ParticipantTransactionState::Prepared
    );
    let recovered_read = restored_client
        .query::<PartitionGet>(
            restored_child_target,
            None,
            Json(PartitionGetInput {
                table_id: table.id.clone(),
                epoch: child_epoch,
                key: extenddb_core::types::extract_key(&changed_item, &table.key_schema),
            }),
        )
        .await
        .unwrap();
    assert!(
        matches!(recovered_read.output.0, PartitionGetOutcome::Conflict(conflict) if conflict.transaction.transaction_id == [101; 16] && conflict.coordinator_key == vec![101; 16])
    );
    let recovered_scan = restored_client
        .query::<PartitionScan>(
            restored_child_target,
            None,
            Json(PartitionScanInput {
                index_name: None,
                table_id: table.id.clone(),
                epoch: child_epoch,
                limit: None,
                exclusive_start_key: None,
            }),
        )
        .await
        .unwrap();
    assert!(
        matches!(recovered_scan.output.0, PartitionScanOutcome::Conflict(conflict) if conflict.transaction.transaction_id == [101; 16] && conflict.coordinator_key == vec![101; 16])
    );
    let recovered_conflict = restored_client
        .command::<PartitionPut>(
            restored_child_target,
            identity(102),
            Json(PartitionPutInput {
                table_id: table.id.clone(),
                epoch: child_epoch,
                item: changed_item.clone(),
                condition: None,
            }),
        )
        .await;
    assert!(matches!(
        recovered_conflict,
        Err(InvocationError::Rejected(result))
            if result.output.0 == PartitionPutOutcome::TransactionConflict
    ));
    let recovered_abort = restored_client
        .command::<ResolvePartitionTransaction>(
            restored_child_target,
            identity(103),
            Json(ResolveTransactionInput {
                transaction_id: [101; 16],
                coordinator_cell: *account.cell_id().as_bytes(),
                commit: false,
            }),
        )
        .await
        .unwrap();
    assert_eq!(recovered_abort.output.0, ResolveTransactionOutcome::Aborted);
    let restored_transaction = restored_client
        .query::<ReadPartitionTransaction>(
            &left_target,
            None,
            Json(ReadTransactionInput {
                transaction_id: [95; 16],
                coordinator_cell: *account.cell_id().as_bytes(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        restored_transaction.output.0,
        ParticipantTransactionState::Committed
    );
    let persisted = restored_client
        .query::<PartitionGet>(
            &left_target,
            None,
            Json(PartitionGetInput {
                table_id: table.id.clone(),
                epoch: 1,
                key: left_key.clone(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(persisted.output.0, PartitionGetOutcome::Sealed);
    let exported_after_restart = restored_client
        .query::<PartitionExport>(
            &left_target,
            None,
            Json(PartitionScanInput {
                index_name: None,
                table_id: table.id.clone(),
                epoch: 1,
                limit: Some(10),
                exclusive_start_key: None,
            }),
        )
        .await
        .unwrap();
    let PartitionScanOutcome::Page {
        items: recovered_items,
        last_evaluated_key: None,
    } = exported_after_restart.output.0
    else {
        panic!("expected sealed source export after restart")
    };
    assert_eq!(recovered_items.len(), 2);
    assert!(recovered_items.contains(&left_key));
    assert!(recovered_items.contains(&second_left_key));
    let persisted_route =
        crate::single_leaf_route(&restored_cell_client, &account, &table.id.clone()).await;
    assert_eq!(persisted_route, Some(next_route.clone()));
    assert_eq!(
        point_range(&restored_cell_client, &account, &table.id, boundary.clone()).await,
        Some((
            next_route.partitions[1].partition_id,
            next_route.partitions[1].epoch
        ))
    );
    let persisted_plan = restored_client
        .query::<beyonddb::ReadDirectoryTransfer>(
            &route_target,
            None,
            Json(beyonddb::DirectoryPartitionInput {
                table_id: table.id.clone(),
                partition_id: plan.source.partition_id,
            }),
        )
        .await
        .unwrap();
    assert_eq!(persisted_plan.output.0, None);
    let restored_status = restored_client
        .query::<ReadPartitionState>(restored_child_target, None, Json(()))
        .await
        .unwrap();
    assert_eq!(
        restored_status.output.0.unwrap().state,
        PartitionState::Opened {
            source: seal.clone(),
            summary: expected[first_child].clone(),
        }
    );
    let recovered_child = restored_client
        .query::<PartitionGet>(
            restored_child_target,
            None,
            Json(PartitionGetInput {
                table_id: table.id.clone(),
                epoch: child_epoch,
                key: first_item.clone(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        recovered_child.output.0,
        PartitionGetOutcome::Found(Some(changed_item))
    );
    let stale_import = restored_client
        .command::<ImportPartitionItem>(
            restored_child_target,
            identity(64),
            Json(PartitionImportInput {
                table_id: table.id.clone(),
                epoch: child_epoch,
                item: first_item,
            }),
        )
        .await;
    assert!(matches!(
        stale_import,
        Err(InvocationError::Rejected(result))
            if result.output.0 == PartitionImportOutcome::NotImporting
    ));
    assert!(matches!(
        restored_client
            .command::<DeleteTable>(
                &account,
                identity(67),
                Json(beyonddb::TableGeneration {
                    table_name: table.table_name.clone(),
                    table_id: table.id.clone()
                })
            )
            .await
            .unwrap()
            .output
            .0,
        DeleteTableOutcome::Deleted(_)
    ));
    assert_eq!(
        point_range(
            &restored_cell_client,
            &account,
            &next_route.table_id,
            boundary.clone()
        )
        .await,
        None
    );
    let second_key = (0..u16::MAX)
        .map(|suffix| format!("second-shard-{suffix}"))
        .find(|key| {
            beyonddb::coordinator_target("123456789012", key.as_bytes()).unwrap()
                != coordinator_target
        })
        .unwrap();
    let second_target =
        beyonddb::coordinator_target("123456789012", second_key.as_bytes()).unwrap();
    restored_provisioner
        .ensure(&restored_cell_client, "123456789012", second_key.as_bytes())
        .await
        .unwrap();
    let mut actual_shards = Vec::new();
    let mut after = None;
    loop {
        let page = restored_client
            .query::<ListCoordinatorShards>(
                &account,
                None,
                Json(ListCoordinatorShardsInput {
                    account_id: "123456789012".into(),
                    after,
                    limit: 1,
                }),
            )
            .await
            .unwrap()
            .output
            .0;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|entry| entry.shard);
        actual_shards.extend(page.into_iter().map(|entry| entry.shard));
    }
    let mut expected_shards = registered
        .into_iter()
        .map(|entry| entry.shard)
        .collect::<Vec<_>>();
    expected_shards.push(u32::from_be_bytes(
        second_target.partition().try_into().unwrap(),
    ));
    expected_shards.sort_unstable();
    expected_shards.dedup();
    assert_eq!(actual_shards, expected_shards);
    for target in [&left_target, &right_target, &child_targets[0], &child_targets[1]] {
        let state = restored_client.query::<ReadPartitionState>(target, None, Json(()))
            .await.unwrap().output.0.unwrap();
        assert_eq!(state.spec.table.id, table.id);
    }
    for (target, old, old_owner) in before {
        let actual = native_peer::identity(&observed, replacement, &target).unwrap();
        assert_eq!(actual.scope, old.scope);
        assert!(actual.epoch > old_owner.epoch);
        let owner = inspection.owner(&actual.scope).await;
        assert_eq!(owner.node.as_deref(), Some(replacement));
        assert_eq!(owner.epoch, actual.epoch);
    }
    restored.stop().await;
    inspection.close().await;
    });
}
