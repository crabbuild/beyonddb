use super::helpers::*;
use crate::{
    residency_fixture::{ACCOUNT, runtimes},
    table_helpers,
};
use aws_sdk_dynamodb::types::AttributeValue as V;
use bdb_cellar_runtime::client::InvocationError;
use beyonddb::*;
use extenddb_core::types::{AttributeValue, DescribeTableInput, Item, TableKeyInfo, TableStatus};
use extenddb_storage::{DataEngine, TableEngine, error::StorageError};

#[test]
fn sdk_update_rejects_incomplete_creation() {
    let (host, core) = runtimes();
    host.block_on(async {
        let (fixture, pending) = partial(host.handle(), core.handle(), true).await;
        let storage = CellStorage::new(fixture.client.clone(), "us-east-1");
        assert_eq!(
            storage
                .describe_table(
                    ACCOUNT,
                    DescribeTableInput {
                        table_name: pending.table_name.clone()
                    }
                )
                .await
                .unwrap()
                .table_status,
            TableStatus::Creating
        );
        let info = TableKeyInfo {
            account_id: ACCOUNT.into(),
            table_name: pending.table_name.clone(),
            table_id: pending.id.clone(),
            base_key_schema: pending.key_schema.clone(),
            key_schema: pending.key_schema.clone(),
            attribute_definitions: pending.attribute_definitions.clone(),
            ..Default::default()
        };
        let key = Item::from([("id".into(), AttributeValue::S("unpublished".into()))]);
        assert!(matches!(
            storage.get_item(&info, &key).await,
            Err(StorageError::TableNotActive(_))
        ));
        assert!(matches!(
            storage.scan(&info, None, None, None, None, None).await,
            Err(StorageError::TableNotActive(_))
        ));
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
        reconcile_three(&fixture, fixture.provisioner.as_ref()).await;
        assert_eq!(
            fixture
                .sdk
                .update_table()
                .table_name(NAME)
                .deletion_protection_enabled(true)
                .send()
                .await
                .unwrap()
                .table_description
                .unwrap()
                .deletion_protection_enabled,
            Some(true)
        );
        fixture.shutdown().await;
    });
}
#[test]
fn sdk_delete_fences_unpublished_directory_installers() {
    let (host, core) = runtimes();
    host.block_on(async {
        for copied in [false, true] {
            let (fixture, table) = partial(host.handle(), core.handle(), false).await;
            let index = &table.global_secondary_indexes[0];
            let ranges = fixture
                .provisioner
                .provision_global_index(&fixture.client, ACCOUNT, &table, index)
                .await
                .unwrap()
                .into_iter()
                .map(|part| RoutePagePartition {
                    partition_id: part.partition_id,
                    lower: part.lower.unwrap_or_else(bdb_keyspace::RangeKey::minimum),
                    upper: part.upper,
                    epoch: part.epoch,
                })
                .collect::<Vec<_>>();
            let spec = DirectorySpec::root(index.id.clone());
            let target = directory_target(ACCOUNT, &spec).unwrap();
            if copied {
                fixture
                    .provisioner
                    .provision_route_directory(&fixture.client, ACCOUNT, &index.id, ranges.clone())
                    .await
                    .unwrap();
            }
            assert!(
                fixture
                    .client
                    .query::<ReadRouteDirectory>(
                        &account_target(ACCOUNT).unwrap(),
                        None,
                        Json(index.id.clone())
                    )
                    .await
                    .unwrap()
                    .output
                    .0
                    .is_none()
            );
            fixture
                .sdk
                .delete_table()
                .table_name(NAME)
                .send()
                .await
                .unwrap();
            complete_deletion(&fixture, NAME).await;
            assert_eq!(
                fixture
                    .client
                    .query::<ReadDirectory>(&target, None, Json(()))
                    .await
                    .unwrap()
                    .output
                    .0
                    .unwrap()
                    .mode,
                DirectoryMode::Retired
            );
            assert!(matches!(
                fixture
                    .client
                    .command::<InstallDirectory>(
                        &target,
                        mutation(),
                        Json(DirectoryInstall {
                            spec,
                            ranges,
                            source: None
                        })
                    )
                    .await,
                Err(InvocationError::Rejected(_))
            ));
            table_helpers::create(&fixture.sdk, NAME, true)
                .send()
                .await
                .unwrap();
            assert_ne!(record(&fixture).await.id, table.id);
            fixture
                .sdk
                .put_item()
                .table_name(NAME)
                .item("id", V::S("new-generation".into()))
                .item("bucket", V::S("new".into()))
                .send()
                .await
                .unwrap();
            assert!(
                fixture
                    .sdk
                    .get_item()
                    .table_name(NAME)
                    .key("id", V::S("new-generation".into()))
                    .consistent_read(true)
                    .send()
                    .await
                    .unwrap()
                    .item
                    .is_some()
            );
            fixture.shutdown().await;
        }
    });
}
