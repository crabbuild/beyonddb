use super::helpers::*;
use crate::{
    residency_fixture::{ACCOUNT, Fixture, SdkItem, runtimes},
    table_helpers,
};
use aws_sdk_dynamodb::types::{AttributeValue as V, TableStatus};
use beyonddb::*;
use cellar_runtime::client::InvocationError;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

#[test]
fn sdk_large_account_deletion_resumes_after_owner_replacement() {
    let (host, core) = runtimes();
    host.block_on(run(host.handle(), core.handle(), false));
}
#[test]
fn sdk_large_partitioned_deletion_resumes_after_owner_replacement() {
    let (host, core) = runtimes();
    host.block_on(run(host.handle(), core.handle(), true));
}
async fn run(host: &tokio::runtime::Handle, core: &tokio::runtime::Handle, public: bool) {
    let mut fixture = Fixture::new(host, core, 2, 8).await;
    if public {
        create_public(&fixture).await;
    } else {
        create_account(&fixture).await;
    }
    let original = table_helpers::table(&fixture, NAME).await;
    let ranges = if public {
        assert_eq!(
            original.placement,
            TablePlacement::Routed {
                initial_partitions: 2
            }
        );
        let ranges = table_helpers::ranges(&fixture, &original.id).await;
        assert_eq!(ranges.len(), 2);
        ranges
    } else {
        assert_eq!(original.placement, TablePlacement::Account);
        Vec::new()
    };
    let mut expected = Vec::new();
    for ordinal in 0..160 {
        // The account case retains all original keys. The public case spreads
        // the same 160-item/160-LSI workload over both ordered bootstrap ranges.
        let id = if public && ordinal >= 80 {
            format!("Ω-item-{ordinal}")
        } else {
            format!("item-{ordinal}")
        };
        let item = SdkItem::from([
            ("id".into(), V::S(id)),
            ("sort".into(), V::S("row".into())),
            ("alternate".into(), V::S("index-row".into())),
        ]);
        fixture
            .sdk
            .put_item()
            .table_name(NAME)
            .set_item(Some(item.clone()))
            .send()
            .await
            .unwrap();
        expected.push(item);
    }
    let mut indexed = fixture
        .sdk
        .scan()
        .table_name(NAME)
        .index_name("ByAlternate")
        .consistent_read(true)
        .send()
        .await
        .unwrap()
        .items
        .unwrap();
    let sort =
        |items: &mut Vec<SdkItem>| items.sort_by_key(|item| item["id"].as_s().unwrap().clone());
    sort(&mut expected);
    sort(&mut indexed);
    assert_eq!(indexed, expected);
    let mut identities = Vec::new();
    for range in &ranges {
        let target = data_target(ACCOUNT, &original.id, &range.partition_id).unwrap();
        identities.push(fixture.cell_identity(&target).await);
    }
    fixture
        .sdk
        .delete_table()
        .table_name(NAME)
        .send()
        .await
        .unwrap();
    assert_eq!(
        fixture
            .sdk
            .describe_table()
            .table_name(NAME)
            .send()
            .await
            .unwrap()
            .table
            .unwrap()
            .table_status(),
        Some(&TableStatus::Deleting)
    );
    assert!(
        table_helpers::create(&fixture.sdk, NAME, false)
            .send()
            .await
            .unwrap_err()
            .as_service_error()
            .unwrap()
            .is_resource_in_use_exception()
    );
    assert!(
        fixture
            .sdk
            .update_table()
            .table_name(NAME)
            .deletion_protection_enabled(true)
            .send()
            .await
            .unwrap_err()
            .as_service_error()
            .unwrap()
            .is_resource_in_use_exception()
    );
    assert!(
        fixture
            .sdk
            .put_item()
            .table_name(NAME)
            .item("id", V::S("late".into()))
            .send()
            .await
            .unwrap_err()
            .as_service_error()
            .unwrap()
            .is_resource_not_found_exception()
    );

    fixture.replace().await;
    let account = account_target(ACCOUNT).unwrap();
    assert!(
        matches!(fixture.client.query::<ReadTableLifecycle>(&account, None, Json(NAME.into()))
        .await.unwrap().output.0, TableLifecycle::Deleting(table) if table.id == original.id)
    );
    let calls = Arc::new(CleanupCalls::default());
    let tasks = TaskGroup::default();
    fixture
        .provisioner
        .install_account_capacity_loop(
            &tasks,
            ACCOUNT.into(),
            fixture.client.with_admission(calls.clone()),
            u64::MAX,
            Duration::from_millis(100),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match fixture.sdk.describe_table().table_name(NAME).send().await {
                Ok(table) => assert_eq!(
                    table.table.unwrap().table_status(),
                    Some(&TableStatus::Deleting)
                ),
                Err(error)
                    if error
                        .as_service_error()
                        .is_some_and(|error| error.is_resource_not_found_exception()) =>
                {
                    break;
                }
                Err(error) => panic!("deletion recovery failed: {error}"),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    tasks.shutdown().await.unwrap();
    if !public {
        // 160 base rows and 160 LSI rows cannot fit in one 64-row cleanup
        // command. Only the installed capacity worker advances them after restore.
        assert!(calls.0.load(Ordering::SeqCst) >= 4);
    }
    if public {
        let directory =
            directory_target(ACCOUNT, &DirectorySpec::root(original.id.clone())).unwrap();
        assert_eq!(
            fixture
                .client
                .query::<ReadDirectory>(&directory, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap()
                .mode,
            DirectoryMode::Retired
        );
        for index_name in [None, Some("ByAlternate".to_owned())] {
            let mut retained = Vec::new();
            for (range, old) in ranges.iter().zip(&identities) {
                let target = data_target(ACCOUNT, &original.id, &range.partition_id).unwrap();
                let PartitionScanOutcome::Page {
                    items,
                    last_evaluated_key: None,
                } = fixture
                    .client
                    .query::<PartitionScan>(
                        &target,
                        None,
                        Json(PartitionScanInput {
                            table_id: original.id.clone(),
                            index_name: index_name.clone(),
                            epoch: range.epoch,
                            limit: None,
                            exclusive_start_key: None,
                        }),
                    )
                    .await
                    .unwrap()
                    .output
                    .0
                else {
                    panic!("retained base/LSI range missing");
                };
                assert_eq!(items.len(), 80);
                retained.extend(items.into_iter().map(|item| {
                    item.into_iter()
                        .map(|(name, value)| {
                            let extenddb_core::types::AttributeValue::S(value) = value else {
                                panic!("string fixture changed type");
                            };
                            (name, V::S(value))
                        })
                        .collect::<SdkItem>()
                }));
                let actual = fixture.cell_identity(&target).await;
                assert_eq!(actual.scope, old.scope);
                assert!(actual.epoch > old.epoch);
            }
            sort(&mut retained);
            assert_eq!(retained, expected);
        }
    }
    table_helpers::create(&fixture.sdk, NAME, false)
        .send()
        .await
        .unwrap();
    assert_ne!(table_helpers::table(&fixture, NAME).await.id, original.id);
    fixture
        .sdk
        .put_item()
        .table_name(NAME)
        .item("id", V::S("replacement".into()))
        .send()
        .await
        .unwrap();
    let items = fixture
        .sdk
        .scan()
        .table_name(NAME)
        .send()
        .await
        .unwrap()
        .items
        .unwrap();
    assert_eq!(
        items,
        vec![SdkItem::from([("id".into(), V::S("replacement".into()))])]
    );
    assert!(
        fixture
            .client
            .command::<ContinueTableDeletion>(&account, mutation(), Json(original.id.clone()))
            .await
            .unwrap()
            .output
            .0
    );
    assert!(
        matches!(fixture.client.command::<DeleteTable>(&account, mutation(), Json(TableGeneration {
        table_name: NAME.into(), table_id: original.id,
    })).await, Err(InvocationError::Rejected(result)) if result.output.0 == DeleteTableOutcome::TableNotFound)
    );
    assert_eq!(
        fixture
            .sdk
            .scan()
            .table_name(NAME)
            .send()
            .await
            .unwrap()
            .items
            .unwrap(),
        items
    );
    fixture.replace().await;
    assert_eq!(
        fixture
            .sdk
            .scan()
            .table_name(NAME)
            .send()
            .await
            .unwrap()
            .items
            .unwrap(),
        items
    );
    fixture.shutdown().await;
}
