use super::global_indexes::{ACCOUNT, mutation, observed_index_recovery, owner, runtimes};
use crate::{
    native_peer::{self, Inspection, Observations, Peer, SOURCE},
    signed_http::SignedHttp,
};
use bdb_cellar_runtime::{
    app::CompiledApplication,
    client::{CellClient, InvocationError},
    identity::CellTarget,
};
use beyonddb::*;
use extenddb_core::types::{AttributeValue, Item};
use extenddb_storage::TableEngine;
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

async fn fenced(
    client: &CellClient,
    target: &CellTarget,
    plan: &GlobalIndexSplitPlan,
    epoch: u64,
    key: &Item,
) {
    let scan = client
        .query::<GlobalIndexScan>(
            target,
            None,
            Json(PartitionScanInput {
                table_id: plan.source.index.id.clone(),
                epoch,
                index_name: None,
                limit: None,
                exclusive_start_key: None,
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    assert_eq!(scan, PartitionScanOutcome::StaleRoute);
    let query = client
        .query::<GlobalIndexQuery>(
            target,
            None,
            Json(beyonddb::PartitionQueryInput {
                table_id: plan.source.index.id.clone(),
                epoch,
                index_name: None,
                partition_key: Item::from([("group".into(), key["group"].clone())]),
                sort: None,
                extra_range_equals: vec![],
                forward: true,
                limit: 100,
                exclusive_start_key: None,
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    assert_eq!(query, beyonddb::PartitionQueryOutcome::StaleRoute);
    assert!(
        matches!(client.command::<ApplyGlobalIndexMutation>(target, mutation(), Json(GlobalIndexMutation {
        index_id: plan.source.index.id.clone(), epoch, key: key.clone(),
        version: ProjectionVersion { source_epoch: 99, sequence: 99 }, item: None,
    })).await, Err(InvocationError::Rejected(result)) if result.output.0 == GlobalIndexApplyOutcome::StaleRoute)
    );
}

async fn restored(
    application: Arc<CompiledApplication>,
    bucket: cellar::bucket::Bucket,
    directory: &Path,
    observed: Observations,
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
) -> (Peer, Arc<CellInitialPartitionProvisioner>, CellClient) {
    let (node, provisioner, client, _) =
        owner(application, bucket, directory, observed, host, core).await;
    (node, provisioner, client)
}

async fn snapshot_owners(
    inspection: &Inspection,
    observed: &Observations,
    name: &str,
    targets: &[CellTarget],
) -> Vec<(CellTarget, cellar::sdk::CellIdentity, native_peer::Owner)> {
    let mut result = Vec::new();
    for target in targets {
        let actual = native_peer::identity(observed, name, target).unwrap();
        let owner = inspection.owner(&actual.scope).await;
        assert_eq!(owner.node.as_deref(), Some(name));
        result.push((target.clone(), actual, owner));
    }
    result
}

#[test]
fn index_transfer_retains_versions_tombstones_and_fences_replay_after_restart() {
    let (host, core) = runtimes();
    host.block_on(async {
    let application = native_peer::application();
    let account = account_target(ACCOUNT).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite")).unwrap();
    let observed = Arc::new(Mutex::new(Default::default()));
    let mut owner_dir = directory.path().join(SOURCE);
    let (mut node, provisioner, mut client, storage) = owner(application.clone(), bucket.clone(), &owner_dir, observed.clone(), host.handle(), core.handle()).await;
    let inspection = Inspection::new(bucket.clone(), host.handle());
    provisioner.admit_account(ACCOUNT).await.unwrap();
    storage.create_table(ACCOUNT, serde_json::from_value(serde_json::json!({
        "TableName":"SplitIndexes", "BillingMode":"PAY_PER_REQUEST",
        "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
        "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"},{"AttributeName":"group","AttributeType":"S"},{"AttributeName":"score","AttributeType":"N"}],
        "GlobalSecondaryIndexes":[{"IndexName":"ByGroup","KeySchema":[{"AttributeName":"group","KeyType":"HASH"},{"AttributeName":"score","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}]
    })).unwrap()).await.unwrap();
    let table = client
        .query::<DescribeTable>(&account, None, Json("SplitIndexes".into()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    let index = &table.global_secondary_indexes[0];
    let index_directory =
        beyonddb::directory_target(ACCOUNT, &beyonddb::DirectorySpec::root(index.id.clone()))
            .unwrap();
    let RoutePageOutcome::Page {
        partitions, epoch, ..
    } = beyonddb::read_route_page(
        &client,
        &beyonddb::account_target("123456789012").unwrap(),
        RoutePageInput {
            table_id: index.id.clone(),
            start_key: None,
            after_lower: None,
            expected_epoch: None,
        },
    )
    .await
    .unwrap()
    else {
        panic!("missing index route");
    };
    let source = global_index_target(ACCOUNT, &index.id, &partitions[0].partition_id).unwrap();
    let spec = client
        .query::<ReadGlobalIndexPartition>(&source, None, Json(()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert_eq!(partitions.len(), 2);
    assert_eq!(spec.epoch, epoch);
    let groups = [0, 1].map(|n| AttributeValue::S(format!("{}g{n}", "\u{2}".repeat(1800))));
    // Split inside an equal (GSI PK, SK) set using the full base-key suffix.
    // Both physical children must participate in a query for groups[1].
    let boundary_item = Item::from([
        ("id".into(), AttributeValue::S(format!("{}item-035", "\u{1}".repeat(1800)))),
        ("group".into(), groups[1].clone()), ("score".into(), AttributeValue::N("2".into())),
    ]);
    let boundary = ordered_index_key(&boundary_item, &index.specification.key_schema, &table.key_schema).unwrap();
    assert!(spec.lower.as_ref().is_none_or(|low| &boundary > low));
    assert!(spec.upper.as_ref().is_none_or(|high| &boundary < high));
    let mut children = [spec.clone(), spec.clone()];
    children[0].partition_id = [96; 16];
    children[0].upper = Some(boundary.clone());
    children[0].epoch = epoch + 1;
    children[1].partition_id = [97; 16];
    children[1].lower = Some(boundary.clone());
    children[1].epoch = epoch + 1;
    let plan = GlobalIndexSplitPlan {
        source: spec,
        children,
        expected_epoch: epoch,
    };
    assert!(
        client
            .command::<BeginDirectoryTransfer>(
                &index_directory,
                mutation(),
                Json(plan.clone().into())
            )
            .await
            .unwrap()
            .output
            .0
    );
    let targets = plan
        .children
        .each_ref()
        .map(|child| global_index_target(ACCOUNT, &index.id, &child.partition_id).unwrap());
    // SDK command ingress creates each child's registered schema on demand.
    // Include normalized numeric ties, wide escaped keys, large images and
    // tombstones; 65 complete keys would exceed the old SQL argument budget.
    let mut entries = Vec::new();
    for n in 0..70 {
        let key = Item::from([
            (
                "id".into(),
                AttributeValue::S(format!("{}item-{n:03}", "\u{1}".repeat(1800))),
            ),
            ("group".into(), groups[n % 2].clone()),
            (
                "score".into(),
                serde_json::from_value(serde_json::json!({"N": "2.000"})).unwrap(),
            ),
        ]);
        let mut item = key.clone();
        if n < 4 {
            item.insert(
                "payload".into(),
                AttributeValue::B(vec![n as u8; 350 * 1024]),
            );
        }
        let entry = GlobalIndexEntry {
            key,
            version: ProjectionVersion {
                source_epoch: 1,
                sequence: n as u64 + 1,
            },
            item: (n % 3 != 0).then_some(item),
        };
        assert_eq!(
            client
                .command::<ApplyGlobalIndexMutation>(
                    &source,
                    mutation(),
                    Json(GlobalIndexMutation {
                        index_id: index.id.clone(),
                        epoch,
                        key: entry.key.clone(),
                        version: entry.version,
                        item: entry.item.clone(),
                    })
                )
                .await
                .unwrap()
                .output
                .0,
            GlobalIndexApplyOutcome::Applied
        );
        entries.push(entry);
    }
    assert!(
        client
            .query::<ExportGlobalIndexEntries>(
                &source,
                None,
                Json(GlobalIndexExport {
                    plan: plan.clone(),
                    after: None
                })
            )
            .await
            .unwrap()
            .output
            .0
            .is_none()
    );
    for target in targets.iter().chain([&source]) {
        assert!(
            client
                .command::<PrepareGlobalIndexSplit>(target, mutation(), Json(plan.clone()))
                .await
                .unwrap()
                .output
                .0
        );
    }
    let mut competing = plan.clone();
    competing.children[0].partition_id = [91; 16];
    assert!(matches!(
        client
            .command::<PrepareGlobalIndexSplit>(&source, mutation(), Json(competing))
            .await,
        Err(InvocationError::Rejected(_))
    ));
    fenced(&client, &source, &plan, epoch, &entries[1].key).await;
    for target in &targets {
        fenced(&client, target, &plan, epoch + 1, &entries[1].key).await;
    }
    let mut copied = Vec::new();
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let page = client
            .query::<ExportGlobalIndexEntries>(
                &source,
                None,
                Json(GlobalIndexExport {
                    plan: plan.clone(),
                    after: cursor,
                }),
            )
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        assert!(!page.entries.is_empty() && page.entries.len() <= 64);
        pages += 1;
        copied.extend(page.entries);
        let Some(next) = page.next else {
            break;
        };
        cursor = Some(next);
    }
    assert!(pages >= 2);
    assert_eq!(copied.len(), entries.len());
    assert_eq!(
        copied.iter().filter(|entry| entry.item.is_none()).count(),
        entries.iter().filter(|entry| entry.item.is_none()).count()
    );
    let mut summaries = [
        GlobalIndexFingerprint::default(),
        GlobalIndexFingerprint::default(),
    ];
    for (position, entry) in copied.iter().enumerate() {
        let encoded = ordered_index_key(&entry.key, &index.specification.key_schema, &table.key_schema).unwrap();
        let child = usize::from(encoded >= boundary);
        summaries[child]
            .include(entry, &plan.children[child])
            .unwrap();
        for expected in [
            GlobalIndexApplyOutcome::Applied,
            GlobalIndexApplyOutcome::Replay,
        ] {
            assert_eq!(
                client
                    .command::<ImportGlobalIndexEntry>(
                        &targets[child],
                        mutation(),
                        Json(GlobalIndexImport {
                            plan: plan.clone(),
                            entry: entry.clone()
                        })
                    )
                    .await
                    .unwrap()
                    .output
                    .0,
                expected
            );
        }
        if position == 12 {
            let physical_targets = [source.clone(), targets[0].clone(), targets[1].clone()];
            let previous = snapshot_owners(&inspection, &observed, SOURCE, &physical_targets).await;
            node.stop().await;
            std::fs::remove_dir_all(&owner_dir).unwrap();
            owner_dir = directory.path().join("transfer-partial");
            (node, _, client) = restored(application.clone(), bucket.clone(), &owner_dir, observed.clone(), host.handle(), core.handle()).await;
            inspection.departed_node(SOURCE, "transfer-partial").await;
            for (i, target) in targets.iter().enumerate() {
                assert_eq!(
                    client
                        .query::<ReadGlobalIndexState>(target, None, Json(()))
                        .await
                        .unwrap()
                        .output
                        .0,
                    Some(GlobalIndexState::Importing {
                        plan: plan.clone(),
                        summary: summaries[i].clone()
                    })
                );
                assert!(
                    client
                        .command::<PrepareGlobalIndexSplit>(target, mutation(), Json(plan.clone()))
                        .await
                        .unwrap()
                        .output
                        .0
                );
            }
            fenced(&client, &source, &plan, epoch, &entries[1].key).await;
            observed_index_recovery(&inspection, &observed, "transfer-partial", &previous).await;
        }
    }
    let mut conflict = copied[0].clone();
    conflict.version.sequence += 1000;
    let child = usize::from(
        ordered_index_key(&conflict.key, &index.specification.key_schema, &table.key_schema).unwrap()
            >= boundary,
    );
    assert!(
        matches!(client.command::<ImportGlobalIndexEntry>(&targets[child], mutation(), Json(GlobalIndexImport { plan: plan.clone(), entry: conflict })).await, Err(InvocationError::Rejected(result)) if result.output.0 == GlobalIndexApplyOutcome::VersionConflict)
    );
    for (i, target) in targets.iter().enumerate() {
        let complete = GlobalIndexImportComplete {
            plan: plan.clone(),
            expected: summaries[i].clone(),
        };
        assert!(matches!(
            client
                .command::<OpenGlobalIndexImport>(target, mutation(), Json(complete.clone()))
                .await,
            Err(InvocationError::Rejected(_))
        ));
        let mut wrong = complete.clone();
        wrong.expected.digest[0] ^= 1;
        assert!(matches!(
            client
                .command::<ActivateGlobalIndexImport>(target, mutation(), Json(wrong))
                .await,
            Err(InvocationError::Rejected(_))
        ));
        assert!(
            client
                .command::<ActivateGlobalIndexImport>(target, mutation(), Json(complete))
                .await
                .unwrap()
                .output
                .0
        );
        fenced(&client, target, &plan, epoch + 1, &entries[1].key).await;
    }
    assert!(
        client
            .command::<PublishDirectoryTransfer>(
                &index_directory,
                mutation(),
                Json(plan.clone().into())
            )
            .await
            .unwrap()
            .output
            .0
    );
    for spec in [&plan.source, &plan.children[0], &plan.children[1]] {
        assert_eq!(
            client
                .query::<ReadDirectoryTransfer>(
                    &index_directory,
                    None,
                    Json(DirectoryPartitionInput {
                        table_id: index.id.clone(),
                        partition_id: spec.partition_id
                    })
                )
                .await
                .unwrap()
                .output
                .0,
            Some(plan.clone().into())
        );
    }
    let physical_targets = [source.clone(), targets[0].clone(), targets[1].clone()];
    let previous = snapshot_owners(&inspection, &observed, "transfer-partial", &physical_targets).await;
    node.stop().await;
    std::fs::remove_dir_all(&owner_dir).unwrap();
    owner_dir = directory.path().join("transfer-activated");
    let (mut node, provisioner, client) = restored(application.clone(), bucket, &owner_dir, observed.clone(), host.handle(), core.handle()).await;
    inspection.departed_node("transfer-partial", "transfer-activated").await;
    fenced(&client, &source, &plan, epoch, &entries[1].key).await;
    for (i, target) in targets.iter().enumerate() {
        assert_eq!(
            client
                .query::<ReadGlobalIndexState>(target, None, Json(()))
                .await
                .unwrap()
                .output
                .0,
            Some(GlobalIndexState::Activated {
                plan: plan.clone(),
                summary: summaries[i].clone()
            })
        );
    }
    observed_index_recovery(&inspection, &observed, "transfer-activated", &previous).await;
    // Resume after directory publication but before either child opens. The
    // retained account plan and sealed source drive recovery without old owners.
    provisioner
        .resume_global_index_split(ACCOUNT, client.clone(), &plan)
        .await
        .unwrap();
    for spec in [&plan.source, &plan.children[0], &plan.children[1]] {
        assert!(
            client
                .query::<ReadDirectoryTransfer>(
                    &index_directory,
                    None,
                    Json(DirectoryPartitionInput {
                        table_id: index.id.clone(),
                        partition_id: spec.partition_id
                    })
                )
                .await
                .unwrap()
                .output
                .0
                .is_none()
        );
    }
    for entry in &copied {
        let child = usize::from(
            ordered_index_key(&entry.key, &index.specification.key_schema, &table.key_schema).unwrap()
                >= boundary,
        );
        let old = GlobalIndexMutation {
            index_id: index.id.clone(),
            epoch: epoch + 1,
            key: entry.key.clone(),
            version: ProjectionVersion {
                source_epoch: 0,
                sequence: 1,
            },
            item: Some(entry.key.clone()),
        };
        assert_eq!(
            client
                .command::<ApplyGlobalIndexMutation>(&targets[child], mutation(), Json(old))
                .await
                .unwrap()
                .output
                .0,
            GlobalIndexApplyOutcome::Superseded
        );
        assert!(
            matches!(client.command::<ImportGlobalIndexEntry>(&targets[child], mutation(), Json(GlobalIndexImport { plan: plan.clone(), entry: entry.clone() })).await, Err(InvocationError::Rejected(result)) if result.output.0 == GlobalIndexApplyOutcome::StaleRoute)
        );
    }
    let mut actual = Vec::new();
    let mut ties_per_child = [0_usize; 2];
    for (position, target) in targets.iter().enumerate() {
        let mut after = None;
        loop {
            let PartitionScanOutcome::Page {
                items,
                last_evaluated_key,
            } = client
                .query::<GlobalIndexScan>(
                    target,
                    None,
                    Json(PartitionScanInput {
                        table_id: index.id.clone(),
                        epoch: epoch + 1,
                        index_name: None,
                        limit: Some(10),
                        exclusive_start_key: after,
                    }),
                )
                .await
                .unwrap()
                .output
                .0
            else {
                panic!("child not serving");
            };
            ties_per_child[position] += items.iter().filter(|item| item["group"] == groups[1] && item["score"] == AttributeValue::N("2".into())).count();
            actual.extend(items);
            let Some(next) = last_evaluated_key else {
                break;
            };
            after = Some(next);
        }
    }
    assert!(ties_per_child.iter().all(|count| *count > 0), "equal GSI PK/SK entries must actually span both child cells: {ties_per_child:?}");
    actual.sort_by_key(|item| format!("{:?}", item["id"]));
    let mut expected: Vec<_> = entries.iter().filter_map(|entry| entry.item.clone()).collect();
    expected.sort_by_key(|item| format!("{:?}", item["id"]));
    assert_eq!(
        actual, expected,
        "no tombstones resurrected or duplicate index keys overwritten"
    );
    let http = SignedHttp::start(&client, provisioner, true).await;
    assert_sdk_ties(&http.sdk, &groups, &expected).await;
    http.stop().await;
    node.stop().await;
    inspection.close().await;
    });
}

async fn assert_sdk_ties(
    sdk: &aws_sdk_dynamodb::Client,
    groups: &[AttributeValue; 2],
    expected: &[Item],
) {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    let sdk = aws_sdk_dynamodb::Client::from_conf(
        sdk.config()
            .to_builder()
            .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled())
            .build(),
    );
    for group in groups {
        let AttributeValue::S(group_name) = group else {
            unreachable!()
        };
        let mut group_items: Vec<_> = expected
            .iter()
            .filter(|item| &item["group"] == group)
            .collect();
        group_items.sort_by_key(|item| match &item["id"] {
            AttributeValue::S(id) => id.clone(),
            other => panic!("unexpected base key: {other:?}"),
        });
        let group_items: Vec<std::collections::HashMap<_, _>> = group_items
            .into_iter()
            .map(|item| {
                item.iter()
                    .map(|(name, value)| {
                        (
                            name.clone(),
                            match value {
                                AttributeValue::S(value) => A::S(value.clone()),
                                AttributeValue::N(value) => A::N(value.clone()),
                                AttributeValue::B(value) => {
                                    A::B(aws_sdk_dynamodb::primitives::Blob::new(value.clone()))
                                }
                                other => panic!("unexpected transfer fixture value: {other:?}"),
                            },
                        )
                    })
                    .collect()
            })
            .collect();
        for forward in [true, false] {
            let mut cursor = None;
            let mut found = Vec::new();
            for page in 0..100 {
                let result = sdk
                    .query()
                    .table_name("SplitIndexes")
                    .index_name("ByGroup")
                    .key_condition_expression("#group = :g AND score = :s")
                    .expression_attribute_names("#group", "group")
                    .expression_attribute_values(":g", A::S(group_name.clone()))
                    .expression_attribute_values(":s", A::N("2".into()))
                    .scan_index_forward(forward)
                    .limit(3)
                    .set_exclusive_start_key(cursor)
                    .send()
                    .await
                    .unwrap();
                found.extend(result.items().iter().cloned());
                cursor = result.last_evaluated_key;
                if let Some(key) = &cursor {
                    assert_eq!(
                        key.len(),
                        3,
                        "GSI continuation must retain the full base key"
                    );
                }
                if cursor.is_none() {
                    break;
                }
                assert!(page < 99, "SDK query did not finish across split GSI ties");
            }
            let mut ordered = group_items.clone();
            if !forward {
                ordered.reverse();
            }
            assert_eq!(
                found, ordered,
                "split GSI query lost, reordered or resurrected equal sort keys"
            );
        }
    }
}
