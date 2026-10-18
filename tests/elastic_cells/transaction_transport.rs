use crate::{
    native_peer::{self, FRONT, Inspection, Peer, PeerPolicy, SOURCE},
    transaction_fixture,
};
use beyonddb::*;
use cellar_runtime::{MutationIdentity, client::InvocationError, identity::RequestId};
use extenddb_core::{
    expression::ExpressionMaps,
    types::{AttributeValue, Item},
};
use extenddb_storage::{DataEngine, TableEngine};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicI64, Ordering},
};

fn runtimes() -> (tokio::runtime::Runtime, tokio::runtime::Runtime) {
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

fn mutation() -> MutationIdentity {
    let now = native_peer::now_ms();
    // A sender thirty seconds ahead is inside the runtime's five-minute
    // tolerance. Its one-minute absolute upload deadline must remain usable.
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms: now + 30_000,
        expires_at_ms: now + 90_000,
    }
}

async fn coordinator_snapshot(
    bucket: &cellar::bucket::Bucket,
    scope: &str,
    host: &tokio::runtime::Handle,
) -> tempfile::TempDir {
    use cellar::sdk::{
        ExecutionDomain, ReplicaStorage, ReplicaStorageOptions, Replication, ReplicationOptions,
    };
    let files = tempfile::tempdir().unwrap();
    let domain = ExecutionDomain::new(host.clone());
    let restored = domain
        .run(async {
            let replication = Replication::start(
                &files.path().join("restore"),
                ReplicaStorage::new(
                    bucket.store.clone(),
                    ReplicaStorageOptions {
                        prefix: bucket.prefix.clone(),
                        ..Default::default()
                    },
                )?,
                ReplicationOptions {
                    flush_interval: std::time::Duration::ZERO,
                    compaction: None,
                    ..Default::default()
                },
            )?;
            let snapshot = replication
                .restore_snapshot(scope)
                .await?
                .ok_or_else(|| anyhow::anyhow!("coordinator snapshot is absent"))?;
            std::fs::copy(snapshot.path(), files.path().join("coordinator.sqlite"))?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    tokio::time::timeout(std::time::Duration::from_secs(10), domain.shutdown())
        .await
        .unwrap();
    restored.unwrap().unwrap();
    files
}
fn upload_count(snapshot: &std::path::Path, upload_id: &[u8; 16]) -> i64 {
    let database = rusqlite::Connection::open_with_flags(
        snapshot.join("coordinator.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    database
        .query_row(
            "SELECT COUNT(*) FROM ddb_transaction_uploads WHERE upload_id=?1",
            [upload_id.as_slice()],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn upload_seals_only_complete_immutable_inputs_after_owner_replacement() {
    let (host, core) = runtimes();
    host.block_on(async {
        let application = native_peer::application();
        let account_id = "123456789012";
        let transaction_id = [246; 16];
        let coordinator = coordinator_target(account_id, &transaction_id).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let bucket =
            cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite")).unwrap();
        let observed = Arc::new(Mutex::new(Default::default()));
        let clock = Arc::new(AtomicI64::new(native_peer::now_ms()));
        let profile = || PeerPolicy {
            application_clock: {
                let clock = clock.clone();
                Arc::new(move || clock.load(Ordering::SeqCst))
            },
            ..true.into()
        };
        let source_path = directory.path().join(SOURCE);
        let mut node = Peer::start(
            application.clone(),
            &source_path,
            bucket.clone(),
            observed.clone(),
            profile(),
            host.handle(),
            core.handle(),
        )
        .await;
        let client = node.client.clone();
        let provisioner = CellInitialPartitionProvisioner::new(client.clone(), application.clone());
        provisioner
            .ensure(&client, account_id, &transaction_id)
            .await
            .unwrap();
        // Stage a real valid upload and observe its complete published snapshot.
        // Advancing only the application clock preserves native lease authority.
        let expired =
            TransactionPayloadRef::new(b"expired", clock.load(Ordering::SeqCst) + 1_000).unwrap();
        client
            .command::<UploadTransactionPayload<BeginCrossCellTransaction>>(
                &coordinator,
                mutation(),
                expired.chunks(b"expired").next().unwrap(),
            )
            .await
            .unwrap();
        let original = native_peer::identity(&observed, SOURCE, &coordinator).unwrap();
        let before = coordinator_snapshot(&bucket, &original.scope, host.handle()).await;
        assert_eq!(upload_count(before.path(), &expired.upload_id), 1);
        clock.store(expired.expires_at_ms + 1, Ordering::SeqCst);
        let input = BeginCrossCellTransactionInput {
            account_id: account_id.into(),
            transaction_id,
            token: None,
            participants: vec![CoordinatorParticipant {
                target: CoordinatorParticipantTarget::Account,
                operations: (0..4)
                    .map(|index| IndexedTransactionOperation {
                        index,
                        operation: TransactionOperation::Put(PutItemInput {
                            table_name: "Uploads".into(),
                            table_id: "unused-at-admission".into(),
                            item: Item::from([
                                ("id".into(), AttributeValue::S(index.to_string())),
                                ("payload".into(), AttributeValue::S("\0".repeat(200 * 1024))),
                            ]),
                            condition: None,
                        }),
                    })
                    .collect(),
            }],
        };
        let bytes = serde_json::to_vec(&input).unwrap();
        assert!(bytes.len() > 4 * 1024 * 1024 + 64 * 1024);
        let reference = TransactionPayloadRef::new(&bytes, mutation().expires_at_ms).unwrap();
        let chunks = reference.chunks(&bytes).collect::<Vec<_>>();
        let read = ReadCrossCellTransactionInput {
            account_id: account_id.into(),
            transaction_id,
            routing_key: transaction_id.to_vec(),
        };
        for _ in 0..2 {
            client
                .command::<UploadTransactionPayload<BeginCrossCellTransaction>>(
                    &coordinator,
                    mutation(),
                    chunks[0].clone(),
                )
                .await
                .unwrap();
        }
        let mut changed = chunks[0].clone();
        changed.payload[0] ^= 1;
        assert!(
            client
                .command::<UploadTransactionPayload<BeginCrossCellTransaction>>(
                    &coordinator,
                    mutation(),
                    changed
                )
                .await
                .is_err()
        );
        assert!(
            client
                .command::<BeginCrossCellTransaction>(
                    &coordinator,
                    mutation(),
                    Json(TransactionCommandInput::Reference(reference.clone())),
                )
                .await
                .is_err()
        );
        assert!(
            client
                .query::<ReadCrossCellTransaction>(&coordinator, None, Json(read.clone()))
                .await
                .unwrap()
                .output
                .0
                .is_none()
        );
        // Neither an invalid deadline nor a complete but forged digest
        // can turn an upload into a durable transaction.
        for deadline in [1, mutation().expires_at_ms + 6 * 60_000] {
            let mut invalid = chunks[0].clone();
            invalid.reference.expires_at_ms = deadline;
            assert!(
                client
                    .command::<UploadTransactionPayload<BeginCrossCellTransaction>>(
                        &coordinator,
                        mutation(),
                        invalid
                    )
                    .await
                    .is_err()
            );
        }
        let mut forged = TransactionPayloadRef::new(b"{}", mutation().expires_at_ms).unwrap();
        forged.digest[0] ^= 1;
        client
            .command::<UploadTransactionPayload<BeginCrossCellTransaction>>(
                &coordinator,
                mutation(),
                forged.chunks(b"{}").next().unwrap(),
            )
            .await
            .unwrap();
        assert!(
            client
                .command::<BeginCrossCellTransaction>(
                    &coordinator,
                    mutation(),
                    Json(TransactionCommandInput::Reference(forged)),
                )
                .await
                .is_err()
        );
        let inspection = Inspection::new(bucket.clone(), host.handle());
        let previous = inspection.owner(&original.scope).await;
        node.stop().await;
        let published = coordinator_snapshot(&bucket, &original.scope, host.handle()).await;
        assert_eq!(
            upload_count(published.path(), &expired.upload_id),
            0,
            "an actual expired staged payload must be collected"
        );
        assert_eq!(
            upload_count(published.path(), &reference.upload_id),
            1,
            "the published first chunk must survive"
        );
        std::fs::remove_dir_all(&source_path).unwrap();
        let mut replacement = Peer::start(
            application,
            &directory.path().join(FRONT),
            bucket.clone(),
            observed.clone(),
            profile(),
            host.handle(),
            core.handle(),
        )
        .await;
        inspection.departed().await;
        let client = replacement.client.clone();
        assert!(
            client
                .query::<ReadCrossCellTransaction>(&coordinator, None, Json(read.clone()))
                .await
                .unwrap()
                .output
                .0
                .is_none()
        );
        // Restore the published first chunk, accept its duplicate, and assemble
        // the remaining chunks out of order on a different owner.
        for chunk in chunks.into_iter().rev() {
            client
                .command::<UploadTransactionPayload<BeginCrossCellTransaction>>(
                    &coordinator,
                    mutation(),
                    chunk,
                )
                .await
                .unwrap();
        }
        // Two drivers can have identical bytes and millisecond deadlines. One
        // driver's seal must not consume the other's independently staged input.
        let competing = TransactionPayloadRef::new(&bytes, reference.expires_at_ms).unwrap();
        for chunk in competing.chunks(&bytes) {
            client
                .command::<UploadTransactionPayload<BeginCrossCellTransaction>>(
                    &coordinator,
                    mutation(),
                    chunk,
                )
                .await
                .unwrap();
        }
        let identity = mutation();
        let first = client
            .command::<BeginCrossCellTransaction>(
                &coordinator,
                identity,
                Json(TransactionCommandInput::Reference(reference.clone())),
            )
            .await
            .unwrap();
        assert_eq!(first.output.0, BeginCrossCellTransactionOutcome::Begun);
        let replay = client
            .command::<BeginCrossCellTransaction>(
                &coordinator,
                identity,
                Json(TransactionCommandInput::Reference(reference.clone())),
            )
            .await
            .unwrap();
        assert_eq!(first.receipt, replay.receipt);
        assert_eq!(
            client
                .command::<BeginCrossCellTransaction>(
                    &coordinator,
                    mutation(),
                    Json(TransactionCommandInput::Reference(competing.clone())),
                )
                .await
                .unwrap()
                .output
                .0,
            BeginCrossCellTransactionOutcome::Existing {
                transaction_id,
                decision: CoordinatorDecision::Begin,
            }
        );
        // Consumption removed the temporary input; the transaction's recovery
        // payload remains authoritative and is independently readable in pieces.
        assert!(
            client
                .command::<BeginCrossCellTransaction>(
                    &coordinator,
                    mutation(),
                    Json(TransactionCommandInput::Reference(reference.clone())),
                )
                .await
                .is_err()
        );
        let mut recovered = Vec::new();
        for chunk in 0.. {
            let result = client
                .query::<ReadCoordinatorParticipant>(
                    &coordinator,
                    None,
                    Json(ReadCoordinatorParticipantInput {
                        account_id: account_id.into(),
                        transaction_id,
                        routing_key: transaction_id.to_vec(),
                        position: 0,
                        chunk,
                    }),
                )
                .await
                .unwrap()
                .output
                .unwrap();
            recovered.extend_from_slice(&result.payload);
            if chunk + 1 == result.chunks {
                break;
            }
        }
        assert_eq!(
            serde_json::from_slice::<Vec<IndexedTransactionOperation>>(&recovered).unwrap(),
            input.participants[0].operations
        );
        assert_eq!(
            client
                .query::<ReadCrossCellTransaction>(&coordinator, None, Json(read))
                .await
                .unwrap()
                .output
                .0
                .unwrap()
                .decision,
            CoordinatorDecision::Begin
        );
        let current = native_peer::identity(&observed, FRONT, &coordinator).unwrap();
        assert_eq!(current.scope, original.scope);
        let owner = inspection.owner(&current.scope).await;
        assert_eq!(owner.node.as_deref(), Some(FRONT));
        assert!(owner.epoch > previous.epoch);
        assert_eq!(owner.epoch, current.epoch);
        replacement.stop().await;
        let published = coordinator_snapshot(&bucket, &current.scope, host.handle()).await;
        assert_eq!(upload_count(published.path(), &reference.upload_id), 0);
        assert_eq!(upload_count(published.path(), &competing.upload_id), 0);
        inspection.close().await;
    });
}

#[test]
fn abort_during_upload_fences_delayed_account_and_data_prepares() {
    use beyonddb::{PrepareAccountTransaction, PrepareAccountTransactionInput};
    let (host, core) = runtimes();
    host.block_on(async {
    let application = native_peer::application();
    let account_id = "123456789012";
    let account = account_target(account_id).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite")).unwrap();
    let observed = Arc::new(Mutex::new(Default::default()));
    let mut node = Peer::start(application.clone(), &directory.path().join(SOURCE), bucket.clone(), observed.clone(), true, host.handle(), core.handle()).await;
    let mut client = node.client.clone();
    let mut provisioner = Arc::new(CellInitialPartitionProvisioner::new(client.clone(), application.clone()).with_initial_partition_count(2).unwrap());
    provisioner.admit_account(account_id).await.unwrap();
    client.command::<CreateTable>(&account, mutation(), Json(transaction_fixture::account_spec("UploadAccount"))).await.unwrap();
    let mut stores = [
        CellStorage::new(client.clone(), "us-east-1").with_transaction_coordinators(provisioner.clone()),
        CellStorage::new(client.clone(), "us-east-1").with_initial_partitions(provisioner.clone()).with_transaction_coordinators(provisioner.clone()),
    ];
    stores[1].create_table(account_id, serde_json::from_value(serde_json::json!({
        "TableName":"UploadData", "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
        "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}], "BillingMode":"PAY_PER_REQUEST"
    })).unwrap()).await.unwrap();
    let infos = [stores[0].table_key_info(account_id, "UploadAccount").await.unwrap(), stores[1].table_key_info(account_id, "UploadData").await.unwrap()];
    let (data, routed) = transaction_fixture::participant_for_key(&client, &infos[1], &Item::from([("id".into(), AttributeValue::S("delayed".into()))])).await;
    let partition = client.query::<ReadPartitionState>(&data, None, Json(())).await.unwrap().output.0.unwrap().spec;
    assert!(matches!(routed, CoordinatorParticipantTarget::Data { .. }));
    let targets = [&account, &data];
    let transaction_id = [249; 16];
    provisioner.ensure(&client, account_id, &transaction_id).await.unwrap();
    let coordinator = coordinator_target(account_id, &transaction_id).unwrap();
    let operations: Vec<_> = infos
        .iter()
        .map(|info| {
            TransactionOperation::Put(PutItemInput {
                table_name: info.table_name.clone(),
                table_id: info.table_id.clone(),
                item: Item::from([
                    ("id".into(), AttributeValue::S("delayed".into())),
                    // Escaped JSON spans upload pieces while the item remains
                    // below the DynamoDB size limit, allowing abort mid-transfer.
                    ("payload".into(), AttributeValue::S("\0".repeat(160 * 1024))),
                ]),
                condition: None,
            })
        })
        .collect();
    let mut participants: Vec<_> = [
        CoordinatorParticipantTarget::Account,
        CoordinatorParticipantTarget::Data {
            table_id: infos[1].table_id.clone(),
            partition_id: partition.partition_id,
            epoch: partition.epoch,
        },
    ]
    .into_iter()
    .enumerate()
    .map(|(index, target)| CoordinatorParticipant {
        target,
        operations: vec![IndexedTransactionOperation {
            index: index as u8,
            operation: operations[index].clone(),
        }],
    })
    .collect();
    participants.sort_by_key(|p| {
        *targets[usize::from(p.operations[0].index)]
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
            participants,
        })
    )
    .await
    .unwrap();
    let coordinator_cell = *coordinator.cell_id().as_bytes();
    let bytes = [
        serde_json::to_vec(&PrepareAccountTransactionInput {
            transaction_id,
            coordinator_cell,
            coordinator_key: transaction_id.to_vec(),
            operations: vec![operations[0].clone()],
        })
        .unwrap(),
        serde_json::to_vec(&PreparePartitionTransactionInput {
            table_id: infos[1].table_id.clone(),
            epoch: partition.epoch,
            transaction_id,
            coordinator_cell,
            coordinator_key: transaction_id.to_vec(),
            operations: vec![operations[1].clone()],
        })
        .unwrap(),
    ];
    let references: Vec<_> = bytes
        .iter()
        .map(|bytes| TransactionPayloadRef::new(bytes, mutation().expires_at_ms).unwrap())
        .collect();
    let chunks: Vec<Vec<_>> = references
        .iter()
        .zip(&bytes)
        .map(|(reference, bytes)| reference.chunks(bytes).collect())
        .collect();
    assert!(chunks.iter().all(|chunks| chunks.len() > 1));
    let decision = CoordinatorDecision::Abort {
        index: None,
        reason: None,
    };
    let inspection = Inspection::new(bucket.clone(), host.handle());
    let mut previous = Vec::new();
    for position in 0..chunks.iter().map(Vec::len).max().unwrap() {
        if position == 1 {
            client
                .command::<DecideCrossCellTransaction>(
                    &coordinator,
                    mutation(),
                    Json(DecideCrossCellTransactionInput {
                        account_id: account_id.into(),
                        transaction_id,
                        routing_key: transaction_id.to_vec(),
                        decision: decision.clone(),
                    }),
                )
                .await
                .unwrap();
            // Resolve the durable coordinator decision while neither participant
            // has enough input to prepare. Later chunks must not recreate locks.
            stores[0]
                .finish_decided_cross_cell_transaction(account_id, &transaction_id, transaction_id)
                .await
                .unwrap();
            for target in [&coordinator, &account, &data] {
                let actual = native_peer::identity(&observed, SOURCE, target).unwrap();
                let owner = inspection.owner(&actual.scope).await;
                assert_eq!(owner.node.as_deref(), Some(SOURCE));
                previous.push((target.clone(), actual, owner));
            }
            node.stop().await;
            std::fs::remove_dir_all(directory.path().join(SOURCE)).unwrap();
            node = Peer::start(application.clone(), &directory.path().join(FRONT), bucket.clone(), observed.clone(), true, host.handle(), core.handle()).await;
            inspection.departed().await;
            client = node.client.clone();
            provisioner = Arc::new(CellInitialPartitionProvisioner::new(client.clone(), application.clone()).with_initial_partition_count(2).unwrap());
            stores = [
                CellStorage::new(client.clone(), "us-east-1").with_transaction_coordinators(provisioner.clone()),
                CellStorage::new(client.clone(), "us-east-1").with_initial_partitions(provisioner.clone()).with_transaction_coordinators(provisioner.clone()),
            ];
        }
        for (index, target) in targets.iter().enumerate() {
            let Some(chunk) = chunks[index].get(position).cloned() else {
                continue;
            };
            if index == 0 {
                client
                    .command::<UploadTransactionPayload<PrepareAccountTransaction>>(
                        target,
                        mutation(),
                        chunk,
                    )
                    .await
                    .unwrap();
            } else {
                client
                    .command::<UploadTransactionPayload<PreparePartitionTransaction>>(
                        target,
                        mutation(),
                        chunk,
                    )
                    .await
                    .unwrap();
            }
        }
    }
    for (index, target) in targets.iter().enumerate() {
        let result = if index == 0 {
            client
                .command::<PrepareAccountTransaction>(
                    target,
                    mutation(),
                    Json(TransactionCommandInput::Reference(
                        references[index].clone(),
                    )),
                )
                .await
        } else {
            client
                .command::<PreparePartitionTransaction>(
                    target,
                    mutation(),
                    Json(TransactionCommandInput::Reference(
                        references[index].clone(),
                    )),
                )
                .await
        };
        assert!(
            matches!(result, Err(InvocationError::Rejected(result)) if result.output.0 == PrepareTransactionOutcome::Aborted)
        );
        let key = Item::from([("id".into(), AttributeValue::S("delayed".into()))]);
        assert_eq!(
            stores[index].get_item(&infos[index], &key).await.unwrap(),
            None
        );
        stores[index]
            .put_item(
                &infos[index],
                key.clone(),
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            stores[index].get_item(&infos[index], &key).await.unwrap(),
            Some(key)
        );
    }
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
    assert_eq!((status.decision, status.resolved_count), (decision, 2));
    assert_eq!(previous.len(), 3);
    for (target, old, old_owner) in previous {
        let actual = native_peer::identity(&observed, FRONT, &target).unwrap();
        assert_eq!(actual.scope, old.scope);
        let owner = inspection.owner(&actual.scope).await;
        assert_eq!(owner.node.as_deref(), Some(FRONT));
        assert!(owner.epoch > old_owner.epoch);
        assert_eq!(owner.epoch, actual.epoch);
    }
    let http = crate::signed_http::SignedHttp::start(&client, provisioner, true).await;
    let sdk = aws_sdk_dynamodb::Client::from_conf(http.sdk.config().to_builder().retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled()).build());
    for info in &infos {
        let result = sdk.get_item().table_name(&info.table_name).key("id", aws_sdk_dynamodb::types::AttributeValue::S("delayed".into())).consistent_read(true).send().await.unwrap();
        assert_eq!(result.item().unwrap().len(), 1, "aborted payload escaped into a later ordinary write");
        assert_eq!(result.item().unwrap()["id"], aws_sdk_dynamodb::types::AttributeValue::S("delayed".into()));
    }
    drop(sdk);
    http.stop().await;
    node.stop().await;
    inspection.close().await;
    });
}
