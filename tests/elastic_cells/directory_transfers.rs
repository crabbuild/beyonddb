use crate::{
    cellar_node,
    directory_fixture::{ACCOUNT, application, mutation},
};
use beyonddb::*;
use cellar_runtime::client::InvocationError;
use extenddb_core::types::{AttributeValue, Item};
use extenddb_storage::TableEngine;
use std::sync::Arc;

#[test]
fn base_transfer_retains_exact_contract_through_publication_and_owner_restart() {
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
        let application = application();
        let files = tempfile::tempdir().unwrap();
        let account = account_target(ACCOUNT).unwrap();
        let bucket =
            cellar::bucket::Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let mut owner_dir = files.path().join("transfer-owner-0");
        let (mut node, mut client) = cellar_node::start(
            application.clone(),
            &owner_dir,
            host.handle(),
            core.handle(),
            bucket.clone(),
        )
        .await
        .unwrap();
        let provisioner = Arc::new(
            CellInitialPartitionProvisioner::new(client.clone(), application.clone())
                .with_initial_partition_count(2)
                .unwrap(),
        );
        let storage =
            CellStorage::new(client.clone(), "local").with_initial_partitions(provisioner);
        storage
            .create_table(
                ACCOUNT,
                serde_json::from_value(serde_json::json!({
                    "TableName": "DirectoryTransfers", "BillingMode": "PAY_PER_REQUEST",
                    "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                    "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}]
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        let table = client
            .query::<DescribeTable>(&account, None, Json("DirectoryTransfers".into()))
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        let spec = DirectorySpec::root(table.id.clone());
        let directory = directory_target(ACCOUNT, &spec).unwrap();
        let range = |part: &PartitionSpec| RoutePagePartition {
            partition_id: part.partition_id,
            lower: part
                .lower
                .clone()
                .unwrap_or_else(keyspace::RangeKey::minimum),
            upper: part.upper.clone(),
            epoch: part.epoch,
        };
        let RoutePageOutcome::Page {
            partitions,
            has_more: false,
            ..
        } = read_route_page(
            &client,
            &account,
            RoutePageInput {
                table_id: table.id.clone(),
                start_key: None,
                after_lower: None,
                expected_epoch: None,
            },
        )
        .await
        .unwrap()
        else {
            panic!("initial directory missing")
        };
        assert_eq!(partitions.len(), 2);
        let target = data_target(ACCOUNT, &table.id, &partitions[0].partition_id).unwrap();
        let source = client
            .query::<ReadPartitionState>(&target, None, Json(()))
            .await
            .unwrap()
            .output
            .0
            .unwrap()
            .spec;
        let boundary = ordered_data_key(
            &table.id,
            &Item::from([("id".into(), AttributeValue::S("middle".into()))]),
            &table.key_schema,
        )
        .unwrap();
        assert!(
            source.lower.as_ref().is_none_or(|lower| &boundary > lower)
                && source.upper.as_ref().is_none_or(|upper| &boundary < upper)
        );
        let mut children = [source.clone(), source.clone()];
        children[0].partition_id = [31; 16];
        children[0].upper = Some(boundary.clone());
        children[1].partition_id = [32; 16];
        children[1].lower = Some(boundary);
        for child in &mut children {
            child.epoch = source.epoch + 1;
        }
        let plan = SplitPlan { source, children };
        let transfer = DirectoryTransfer::from(plan.clone());
        client
            .command::<BeginDirectoryTransfer>(&directory, mutation(), Json(transfer.clone()))
            .await
            .unwrap();
        // Identical compact ranges cannot replace the full immutable table contract.
        let mut changed = plan.clone();
        changed.source.table.table_name = "AnotherContract".into();
        for child in &mut changed.children {
            child.table = changed.source.table.clone();
        }
        assert!(matches!(
            client
                .command::<BeginDirectoryTransfer>(&directory, mutation(), Json(changed.into()))
                .await,
            Err(InvocationError::Rejected(_))
        ));
        let mut wrong_generation = plan.clone();
        let mut other_id = *blake3::Hash::from_hex(&table.id).unwrap().as_bytes();
        other_id[31] ^= 1;
        wrong_generation.source.table.id = blake3::Hash::from_bytes(other_id).to_hex().to_string();
        for child in &mut wrong_generation.children {
            child.table = wrong_generation.source.table.clone();
        }
        assert!(matches!(
            client
                .command::<BeginDirectoryTransfer>(
                    &directory,
                    mutation(),
                    Json(wrong_generation.into())
                )
                .await,
            Err(InvocationError::Rejected(_))
        ));
        for published in [false, true] {
            let next_dir = files
                .path()
                .join(format!("transfer-owner-{}", u8::from(published) + 1));
            let (next_node, next_client) = cellar_node::start(
                application.clone(),
                &next_dir,
                host.handle(),
                core.handle(),
                bucket.clone(),
            )
            .await
            .unwrap();
            cellar_node::stop(node, cellar::sdk::ShutdownMode::Handoff)
                .await
                .unwrap();
            std::fs::remove_dir_all(owner_dir).unwrap();
            owner_dir = next_dir;
            node = next_node;
            client = next_client;
            // Recovery must find the original contract through any participant,
            // including after its source has disappeared from published membership.
            for part in [&plan.source, &plan.children[0], &plan.children[1]] {
                let input = DirectoryPartitionInput {
                    table_id: table.id.clone(),
                    partition_id: part.partition_id,
                };
                let recovered = client
                    .query::<ReadDirectoryTransfer>(&directory, None, Json(input.clone()))
                    .await
                    .unwrap()
                    .output
                    .0;
                assert_eq!(recovered, Some(transfer.clone()));
                let member = client
                    .query::<ReadDirectoryRange>(&directory, None, Json(input))
                    .await
                    .unwrap()
                    .output
                    .0;
                let is_child = part.partition_id != plan.source.partition_id;
                assert_eq!(member, (published == is_child).then(|| range(part)));
            }
            let version = client
                .query::<ReadDirectory>(&directory, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap()
                .version;
            assert!(matches!(
                client
                    .command::<FreezeDirectory>(&directory, mutation(), Json(version))
                    .await,
                Err(InvocationError::Rejected(_))
            ));
            if published {
                client
                    .command::<FinishDirectoryTransfer>(
                        &directory,
                        mutation(),
                        Json(transfer.clone()),
                    )
                    .await
                    .unwrap();
            } else {
                client
                    .command::<PublishDirectoryTransfer>(
                        &directory,
                        mutation(),
                        Json(transfer.clone()),
                    )
                    .await
                    .unwrap();
            }
        }
        assert!(
            client
                .query::<ReadDirectoryTransfer>(
                    &directory,
                    None,
                    Json(DirectoryPartitionInput {
                        table_id: table.id,
                        partition_id: plan.children[1].partition_id,
                    })
                )
                .await
                .unwrap()
                .output
                .0
                .is_none()
        );
        assert!(matches!(
            client
                .command::<PublishDirectoryTransfer>(&directory, mutation(), Json(transfer))
                .await,
            Err(InvocationError::Rejected(_))
        ));
        assert!(
            client
                .command::<FreezeDirectory>(&directory, mutation(), Json(2))
                .await
                .unwrap()
                .output
                .0
                .is_some()
        );
        cellar_node::stop(node, cellar::sdk::ShutdownMode::Preserve)
            .await
            .unwrap();
    });
}
