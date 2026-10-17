use crate::{
    cellar_node,
    directory_fault::BlockedDirectory,
    directory_fixture::{ACCOUNT, application, mutation, ranges},
};
use bdb_cellar_runtime::client::InvocationError;
use beyonddb::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[test]
fn directory_tree_fences_copy_cutover_and_stale_leaf_writers() {
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
        for installed_children in 0..=2 {
            interrupted_split(installed_children, host.handle(), core.handle()).await;
        }
    });
}
async fn interrupted_split(
    installed_children: usize,
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
) {
    let application = application();
    let account = account_target(ACCOUNT).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let bucket =
        cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite")).unwrap();
    let first_dir = directory.path().join("tree-owner-a");
    let (node, client) =
        cellar_node::start(application.clone(), &first_dir, host, core, bucket.clone())
            .await
            .unwrap();
    let provisioner = CellInitialPartitionProvisioner::new(client.clone(), application.clone());
    let mut table = [0; 32];
    table[..16].copy_from_slice(account.tenant().as_bytes());
    let root = DirectorySpec::root(blake3::Hash::from_bytes(table).to_hex().to_string());
    let target = directory_target(ACCOUNT, &root).unwrap();
    assert!(
        provisioner
            .split_directory(&client, ACCOUNT, &root)
            .await
            .is_err()
    );
    assert!(
        client
            .query::<ReadDirectory>(&target, None, Json(()))
            .await
            .unwrap()
            .output
            .0
            .is_none()
    );
    let ranges = ranges(1024);
    client
        .command::<InstallDirectory>(
            &target,
            mutation(),
            Json(DirectoryInstall {
                spec: root.clone(),
                ranges: ranges.clone(),
                source: None,
            }),
        )
        .await
        .unwrap();
    for position in [0, 63, 64, 1023] {
        let mut changed = ranges.clone();
        changed[position].partition_id = [0xf0; 16];
        assert!(matches!(
            client
                .command::<InstallDirectory>(
                    &target,
                    mutation(),
                    Json(DirectoryInstall {
                        spec: root.clone(),
                        ranges: changed,
                        source: None,
                    })
                )
                .await,
            Err(InvocationError::Rejected(_))
        ));
    }
    let page = client
        .query::<ReadDirectoryPage>(
            &target,
            None,
            Json(DirectoryPageInput {
                reverse: false,
                hash: bdb_keyspace::RangeKey::minimum(),
                expected_version: Some(1),
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    assert!(matches!(page,DirectoryPage::Leaf {ranges, ..} if ranges.len()==64));
    let plan = change(&ranges[0], 9000);
    assert!(
        matches!(
            client
                .command::<BeginDirectoryChange>(&target, mutation(), Json(plan.clone()))
                .await,
            Err(InvocationError::Rejected(_))
        ),
        "a full leaf must split before accepting another range reservation"
    );
    let split = client
        .command::<FreezeDirectory>(&target, mutation(), Json(1))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert!(matches!(
        client
            .command::<BeginDirectoryChange>(&target, mutation(), Json(plan.clone()))
            .await,
        Err(InvocationError::Rejected(_))
    ));
    let mut copies = Vec::new();
    let mut receipts = Vec::new();
    for (index, spec) in split.children.iter().enumerate() {
        let child = directory_target(ACCOUNT, spec).unwrap();
        let rows = ranges[index * 512..(index + 1) * 512].to_vec();
        copies.push((child.clone(), rows.clone()));
        if index >= installed_children {
            continue;
        }
        let mut corrupt = rows.clone();
        corrupt[4].partition_id = [200; 16];
        assert!(matches!(
            client
                .command::<InstallDirectory>(
                    &child,
                    mutation(),
                    Json(DirectoryInstall {
                        spec: spec.clone(),
                        ranges: corrupt,
                        source: Some(split.clone()),
                    })
                )
                .await,
            Err(InvocationError::Rejected(_))
        ));
        let installed = client
            .command::<InstallDirectory>(
                &child,
                mutation(),
                Json(DirectoryInstall {
                    spec: spec.clone(),
                    ranges: rows.clone(),
                    source: Some(split.clone()),
                }),
            )
            .await
            .unwrap();
        assert_eq!(
            client
                .query::<ReadDirectoryPage>(
                    &child,
                    None,
                    Json(DirectoryPageInput {
                        reverse: false,
                        hash: spec.lower.clone(),
                        expected_version: None
                    })
                )
                .await
                .unwrap()
                .output
                .0,
            DirectoryPage::Unavailable
        );
        receipts.push(DirectoryCopyReceipt {
            cell_id: *child.cell_id().as_bytes(),
            sequence: installed.receipt.commit_sequence,
            fingerprint: split.fingerprints[index],
        });
    }
    if installed_children == 2 {
        client
            .command::<PublishDirectorySplit>(
                &target,
                mutation(),
                Json(DirectorySplitPublication {
                    split: split.clone(),
                    receipts: receipts.try_into().unwrap(),
                }),
            )
            .await
            .unwrap();
    }
    let next_dir = directory.path().join("tree-owner-b");
    let (next_node, next_client) =
        cellar_node::start(application.clone(), &next_dir, host, core, bucket.clone())
            .await
            .unwrap();
    cellar_node::stop(node, cellar::sdk::ShutdownMode::Handoff)
        .await
        .unwrap();
    std::fs::remove_dir_all(first_dir).unwrap();
    let node = next_node;
    let client = next_client;
    let provisioner = CellInitialPartitionProvisioner::new(client.clone(), application.clone());
    let restored = client
        .query::<ReadDirectory>(&target, None, Json(()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert_eq!(
        restored.mode,
        if installed_children == 2 {
            DirectoryMode::Branch(split.clone())
        } else {
            DirectoryMode::Frozen(split.clone())
        }
    );
    assert_eq!(
        provisioner
            .split_directory(&client, ACCOUNT, &root)
            .await
            .unwrap(),
        split
    );
    let state = client
        .query::<ReadDirectory>(&target, None, Json(()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert_eq!(
        client
            .query::<ReadDirectoryPage>(
                &target,
                None,
                Json(DirectoryPageInput {
                    reverse: false,
                    hash: bdb_keyspace::RangeKey::minimum(),
                    expected_version: Some(1)
                })
            )
            .await
            .unwrap()
            .output
            .0,
        DirectoryPage::Redirect(Box::new(split.clone()))
    );
    assert!(matches!(
        client
            .command::<PublishDirectoryChange>(&target, mutation(), Json(plan))
            .await,
        Err(InvocationError::Rejected(_))
    ));
    // Different metadata leaves now accept independent changes; the parent
    // version is unchanged and no account metadata writer participates.
    let plans = [change(&copies[0].1[0], 9100), change(&copies[1].1[0], 9200)];
    let (left, right) = tokio::join!(
        client.command::<BeginDirectoryChange>(&copies[0].0, mutation(), Json(plans[0].clone())),
        client.command::<BeginDirectoryChange>(&copies[1].0, mutation(), Json(plans[1].clone()))
    );
    left.unwrap();
    right.unwrap();
    for (index, plan) in plans.iter().enumerate() {
        let child = &copies[index].0;
        assert!(
            matches!(
                client
                    .command::<FreezeDirectory>(child, mutation(), Json(split.version + 1))
                    .await,
                Err(InvocationError::Rejected(_))
            ),
            "pending data split must keep its recovery owner"
        );
        client
            .command::<PublishDirectoryChange>(child, mutation(), Json(plan.clone()))
            .await
            .unwrap();
        client
            .command::<FinishDirectoryChange>(child, mutation(), Json(plan.clone()))
            .await
            .unwrap();
        assert_eq!(
            client
                .query::<ReadDirectoryPage>(
                    child,
                    None,
                    Json(DirectoryPageInput {
                        reverse: false,
                        hash: plan.source.lower.clone(),
                        expected_version: Some(split.version + 1)
                    })
                )
                .await
                .unwrap()
                .output
                .0,
            DirectoryPage::Changed
        );
        let page = client
            .query::<ReadDirectoryPage>(
                child,
                None,
                Json(DirectoryPageInput {
                    reverse: false,
                    hash: plan.children[1].lower.clone(),
                    expected_version: Some(split.version + 2),
                }),
            )
            .await
            .unwrap()
            .output
            .0;
        assert!(matches!(page,DirectoryPage::Leaf {ranges, ..} if ranges[0]==plan.children[1]));
    }
    // A replayed install after writes must acknowledge the original copy without
    // rolling membership back. Its immutable birth digest authenticates replay.
    client
        .command::<InstallDirectory>(
            &copies[0].0,
            mutation(),
            Json(DirectoryInstall {
                spec: split.children[0].clone(),
                ranges: copies[0].1.clone(),
                source: Some(split.clone()),
            }),
        )
        .await
        .unwrap();
    let replayed = client
        .query::<ReadDirectory>(&copies[0].0, None, Json(()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert_eq!(replayed.version, split.version + 2);
    let grandchildren = provisioner
        .split_directory(&client, ACCOUNT, &split.children[0])
        .await
        .unwrap();
    assert_eq!(grandchildren.children[0].depth, 2);
    assert_eq!(
        provisioner
            .split_directory(&client, ACCOUNT, &split.children[0])
            .await
            .unwrap(),
        grandchildren
    );
    assert_eq!(
        client
            .query::<ReadDirectory>(&target, None, Json(()))
            .await
            .unwrap()
            .output
            .0
            .unwrap(),
        state
    );
    // Retirement can interrupt a pending data split and a frozen metadata copy.
    // Its terminal fence must prevent either stale publisher from proceeding.
    let pending = change(&plans[1].children[0], 9500);
    client
        .command::<BeginDirectoryChange>(&copies[1].0, mutation(), Json(pending.clone()))
        .await
        .unwrap();
    let grandchild = directory_target(ACCOUNT, &grandchildren.children[0]).unwrap();
    let frozen = client
        .command::<FreezeDirectory>(&grandchild, mutation(), Json(grandchildren.version + 1))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    let mut unfinished_copies = Vec::new();
    for spec in &frozen.children {
        let mut rows = Vec::new();
        let mut hash = spec.lower.clone();
        loop {
            let page = client
                .query::<ReadDirectoryPage>(
                    &grandchild,
                    None,
                    Json(DirectoryPageInput {
                        reverse: false,
                        hash,
                        expected_version: Some(frozen.version),
                    }),
                )
                .await
                .unwrap()
                .output
                .0;
            let DirectoryPage::Leaf { ranges, .. } = page else {
                panic!("frozen copy lost its readable source");
            };
            rows.extend(
                ranges.into_iter().take_while(|range| {
                    spec.upper.as_ref().is_none_or(|upper| &range.lower < upper)
                }),
            );
            let next = rows.last().unwrap().upper.clone();
            if next == spec.upper {
                break;
            }
            hash = next.unwrap();
        }
        unfinished_copies.push(DirectoryInstall {
            spec: spec.clone(),
            ranges: rows,
            source: Some(frozen.clone()),
        });
    }
    // Interrupt deletion with zero, one or both unpublished child copies durable.
    for copy in unfinished_copies.iter().take(installed_children) {
        let target = directory_target(ACCOUNT, &copy.spec).unwrap();
        client
            .command::<InstallDirectory>(&target, mutation(), Json(copy.clone()))
            .await
            .unwrap();
    }
    let mut other_generation = root.clone();
    other_generation.table_id.replace_range(63..64, "1");
    assert!(matches!(
        client
            .command::<beyonddb::RetireDirectory>(&target, mutation(), Json(other_generation))
            .await,
        Err(InvocationError::Rejected(_))
    ));
    client
        .command::<beyonddb::RetireDirectory>(&target, mutation(), Json(root.clone()))
        .await
        .unwrap();
    assert_eq!(
        client
            .query::<ReadDirectoryPage>(
                &target,
                None,
                Json(DirectoryPageInput {
                    reverse: false,
                    hash: bdb_keyspace::RangeKey::minimum(),
                    expected_version: None,
                })
            )
            .await
            .unwrap()
            .output
            .0,
        DirectoryPage::Unavailable
    );
    assert!(matches!(
        client
            .command::<beyonddb::RecordDirectoryRetirement>(
                &target,
                mutation(),
                Json(beyonddb::DirectoryRetirementReceipt {
                    parent: root.clone(),
                    child_id: [0xff; 16],
                    sequence: 1,
                })
            )
            .await,
        Err(InvocationError::Rejected(_))
    ));
    client
        .command::<beyonddb::RetireDirectory>(
            &copies[0].0,
            mutation(),
            Json(split.children[0].clone()),
        )
        .await
        .unwrap();
    client
        .command::<beyonddb::RetireDirectory>(
            &grandchild,
            mutation(),
            Json(grandchildren.children[0].clone()),
        )
        .await
        .unwrap();
    // Lose the grandchild's receipt before its parent acknowledges it.
    let remote_dir = directory.path().join("tree-owner-c");
    let (remote, remote_client) =
        cellar_node::start(application.clone(), &remote_dir, host, core, bucket.clone())
            .await
            .unwrap();
    cellar_node::stop(node, cellar::sdk::ShutdownMode::Handoff)
        .await
        .unwrap();
    std::fs::remove_dir_all(next_dir).unwrap();
    let right_target = directory_target(ACCOUNT, &split.children[1]).unwrap();
    remote_client
        .query::<ReadDirectory>(&right_target, None, Json(()))
        .await
        .unwrap();
    let (node, base_client) = cellar_node::start(
        application.clone(),
        &directory.path().join("tree-owner-d"),
        host,
        core,
        bucket,
    )
    .await
    .unwrap();
    let blocked = Arc::new(AtomicBool::new(true));
    let attempts = Arc::new(AtomicUsize::new(0));
    let client = base_client.map_transport(|inner| {
        Arc::new(BlockedDirectory {
            inner,
            target: right_target,
            blocked: blocked.clone(),
            attempts: attempts.clone(),
        })
    });
    let provisioner = CellInitialPartitionProvisioner::new(client.clone(), application);
    // One unavailable published child must keep the root pending while
    // reachable descendants finish and publish their retirement receipts.
    let mut saw_unavailable = false;
    for _ in 0..8 {
        match provisioner
            .retire_directory_step(&client, ACCOUNT, &root)
            .await
        {
            Ok(false) => {}
            Ok(true) => panic!("retirement skipped an unavailable published child"),
            Err(_) => {
                saw_unavailable = true;
                break;
            }
        }
    }
    assert!(saw_unavailable);
    let retiring = client
        .query::<ReadDirectory>(&target, None, Json(()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert!(matches!(
        retiring.mode,
        DirectoryMode::Retiring {
            acknowledged: 1,
            ..
        }
    ));
    assert!(attempts.load(Ordering::SeqCst) > 0);
    assert!(remote.is_ready().await.unwrap());
    blocked.store(false, Ordering::SeqCst);
    assert!(
        provisioner
            .retire_directory_step(&client, ACCOUNT, &root)
            .await
            .unwrap()
    );
    assert!(
        provisioner
            .retire_directory_step(&client, ACCOUNT, &root)
            .await
            .unwrap()
    );
    for spec in std::iter::once(&root)
        .chain(split.children.iter())
        .chain(grandchildren.children.iter())
        .chain(frozen.children.iter())
    {
        let target = directory_target(ACCOUNT, spec).unwrap();
        assert_eq!(
            client
                .query::<ReadDirectory>(&target, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap()
                .mode,
            DirectoryMode::Retired
        );
    }
    assert!(matches!(
        client
            .command::<InstallDirectory>(
                &target,
                mutation(),
                Json(DirectoryInstall {
                    spec: root.clone(),
                    ranges: ranges.clone(),
                    source: None,
                })
            )
            .await,
        Err(InvocationError::Rejected(_))
    ));
    assert!(matches!(
        client
            .command::<beyonddb::OpenDirectory>(&copies[1].0, mutation(), Json(split.clone()))
            .await,
        Err(InvocationError::Rejected(_))
    ));
    assert!(matches!(
        client
            .command::<PublishDirectoryChange>(&copies[1].0, mutation(), Json(pending))
            .await,
        Err(InvocationError::Rejected(_))
    ));
    assert!(
        client
            .query::<beyonddb::ReadDirectoryChanges>(&copies[1].0, None, Json(None))
            .await
            .unwrap()
            .output
            .0
            .is_empty()
    );
    assert!(matches!(
        client
            .command::<FreezeDirectory>(&grandchild, mutation(), Json(frozen.version))
            .await,
        Err(InvocationError::Rejected(_))
    ));
    for copy in unfinished_copies {
        let target = directory_target(ACCOUNT, &copy.spec).unwrap();
        assert!(matches!(
            client
                .command::<InstallDirectory>(&target, mutation(), Json(copy))
                .await,
            Err(InvocationError::Rejected(_))
        ));
        assert!(matches!(
            client
                .command::<beyonddb::OpenDirectory>(&target, mutation(), Json(frozen.clone()))
                .await,
            Err(InvocationError::Rejected(_))
        ));
    }
    assert!(
        provisioner
            .split_directory(&client, ACCOUNT, &root)
            .await
            .is_err()
    );
    cellar_node::stop(node, cellar::sdk::ShutdownMode::Preserve)
        .await
        .unwrap();
    cellar_node::stop(remote, cellar::sdk::ShutdownMode::Preserve)
        .await
        .unwrap();
}

fn change(source: &RoutePagePartition, id: u128) -> DirectoryChange {
    // Metadata bounds admit ordered prefixes. Leave distinct points for the
    // nested changes exercised below; data split planners use observed keys.
    let boundary = [1_u8, 0]
        .into_iter()
        .find_map(|suffix| {
            let mut bytes = source.lower.to_vec();
            bytes.push(suffix);
            let key = bdb_keyspace::RangeKey::new(bytes).unwrap();
            (key > source.lower && source.upper.as_ref().is_none_or(|upper| &key < upper))
                .then_some(key)
        })
        .unwrap();
    DirectoryChange {
        source: source.clone(),
        children: [
            RoutePagePartition {
                partition_id: id.to_be_bytes(),
                lower: source.lower.clone(),
                upper: Some(boundary.clone()),
                epoch: source.epoch + 1,
            },
            RoutePagePartition {
                partition_id: (id + 1).to_be_bytes(),
                lower: boundary,
                upper: source.upper.clone(),
                epoch: source.epoch + 1,
            },
        ],
    }
}
