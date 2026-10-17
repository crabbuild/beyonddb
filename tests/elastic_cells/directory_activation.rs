use crate::{
    cellar_node,
    directory_fixture::{ACCOUNT, application, mutation, ranges},
};
use bdb_cellar_runtime::client::{CellClient, InvocationError};
use beyonddb::*;
use extenddb_core::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};
use std::sync::Arc;

#[test]
fn activation_replay_compares_large_base_and_index_directories() {
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
    let account = account_target(ACCOUNT).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite")).unwrap();
    let original = directory.path().join("activation-owner-a");
    let (node, client) = cellar_node::start(application.clone(), &original, host.handle(), core.handle(), bucket.clone()).await.unwrap();
    let provisioner = Arc::new(CellInitialPartitionProvisioner::new(client.clone(), application.clone()));
    // The catalog's immutable initial count is 256. Complete 1,024-range
    // directories must still fail initial publication, even with real receipts.
    let oversized = create(&client, &account, "OversizedDirectory").await;
    let parts = ranges(1024);
    let receipt = provisioner.provision_route_directory(&client, ACCOUNT, &oversized.global_secondary_indexes[0].id, parts.clone()).await.unwrap();
    let rejected = client.command::<ActivateGlobalIndexRoute>(&account, mutation(), Json(GlobalIndexRoute {
        index: oversized.global_secondary_indexes[0].clone(), table: oversized.clone(), partitions: parts.clone(), receipt,
    })).await;
    assert!(matches!(rejected, Err(InvocationError::Rejected(result)) if !result.output.0));
    let receipt = provisioner.provision_route_directory(&client, ACCOUNT, &oversized.id, parts.clone()).await.unwrap();
    let rejected = client.command::<ActivateTableRoute>(&account, mutation(), Json(TableRoutePublication {
        receipt, route: TableRoute {table_id: oversized.id.clone(), epoch: 1, partitions: parts.iter().map(|part| PartitionSpec {
            table: oversized.clone(), partition_id: part.partition_id, lower: (part.lower != bdb_keyspace::RangeKey::minimum()).then(|| part.lower.clone()), upper: part.upper.clone(), epoch: part.epoch,
        }).collect()},
    })).await;
    assert!(matches!(rejected, Err(InvocationError::Rejected(result)) if result.output.0 == ActivateTableRouteOutcome::InvalidRoute));
    assert_eq!(read_route_page(&client, &account, RoutePageInput { table_id: oversized.id, start_key: None, after_lower: None, expected_epoch: None }).await.unwrap(), RoutePageOutcome::Unrouted);
    let table = create(&client, &account, "ReplayDirectory").await;
    let partitions = ranges(256);
    let index = table.global_secondary_indexes[0].clone();
    let receipt = provisioner
        .provision_route_directory(&client, ACCOUNT, &index.id, partitions.clone())
        .await
        .unwrap();
    let route = GlobalIndexRoute {
        table,
        index,
        partitions,
        receipt,
    };
    client
        .command::<ActivateGlobalIndexRoute>(&account, mutation(), Json(route.clone()))
        .await
        .unwrap();
    let base = TableRoute {
        table_id: route.table.id.clone(),
        epoch: 1,
        partitions: route
            .partitions
            .iter()
            .map(|range| PartitionSpec {
                table: route.table.clone(),
                partition_id: range.partition_id,
                lower: (range.lower != bdb_keyspace::RangeKey::minimum()).then(|| range.lower.clone()),
                upper: range.upper.clone(),
                epoch: range.epoch,
            })
            .collect(),
    };
    let receipt = provisioner
        .provision_route_directory(&client, ACCOUNT, &base.table_id, route.partitions.clone())
        .await
        .unwrap();
    let base = beyonddb::TableRoutePublication {
        route: base,
        receipt,
    };
    client
        .command::<ActivateTableRoute>(&account, mutation(), Json(base.clone()))
        .await
        .unwrap();
    let (restored, next) = cellar_node::start(application, &directory.path().join("activation-owner-b"), host.handle(), core.handle(), bucket).await.unwrap();
    cellar_node::stop(node, cellar::sdk::ShutdownMode::Handoff).await.unwrap();
    std::fs::remove_dir_all(original).unwrap();
    let client = next;
    assert_eq!(
        client
            .command::<ActivateTableRoute>(&account, mutation(), Json(base.clone()))
            .await
            .unwrap()
            .output
            .0,
        ActivateTableRouteOutcome::Activated
    );
    assert!(
        client
            .command::<ActivateGlobalIndexRoute>(&account, mutation(), Json(route.clone()))
            .await
            .unwrap()
            .output
            .0
    );
    // The receipt fingerprint covers every range. Changing an identity or
    // boundary anywhere must invalidate publication, including on replay.
    for position in [0, 63, 64, 255] {
        let mut changed = route.clone();
        changed.partitions[position].partition_id = [0xf0; 16];
        let rejected = client
            .command::<ActivateGlobalIndexRoute>(&account, mutation(), Json(changed))
            .await;
        assert!(matches!(rejected, Err(InvocationError::Rejected(result)) if !result.output.0));
        let mut changed = base.clone();
        changed.route.partitions[position].partition_id = [0xf0; 16];
        let rejected = client
            .command::<ActivateTableRoute>(&account, mutation(), Json(changed))
            .await;
        assert!(
            matches!(rejected, Err(InvocationError::Rejected(result)) if result.output.0 == ActivateTableRouteOutcome::InvalidRoute)
        );
    }
    let mut shortened = route.clone();
    shortened.partitions.pop();
    shortened.partitions.last_mut().unwrap().upper = None;
    let rejected = client
        .command::<ActivateGlobalIndexRoute>(&account, mutation(), Json(shortened))
        .await;
    assert!(matches!(rejected, Err(InvocationError::Rejected(result)) if !result.output.0));
    let mut shortened = base.clone();
    shortened.route.partitions.pop();
    shortened.route.partitions.last_mut().unwrap().upper = None;
    let rejected = client
        .command::<ActivateTableRoute>(&account, mutation(), Json(shortened))
        .await;
    assert!(
        matches!(rejected, Err(InvocationError::Rejected(result)) if result.output.0 == ActivateTableRouteOutcome::InvalidRoute)
    );
    cellar_node::stop(restored, cellar::sdk::ShutdownMode::Preserve).await.unwrap();
    });
}

async fn create(
    client: &CellClient,
    account: &bdb_cellar_runtime::identity::CellTarget,
    name: &str,
) -> TableRecord {
    let spec = TableSpec {
        table_class: Default::default(),
        placement: beyonddb::TablePlacement::Routed {
            initial_partitions: 256,
        },
        local_secondary_indexes: Vec::new(),
        global_secondary_indexes: vec![
            serde_json::from_value(serde_json::json!({
                "IndexName": "ByGroup",
                "KeySchema": [{"AttributeName": "group", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }))
            .unwrap(),
        ],
        table_name: name.into(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "id".into(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: ["id", "group"]
            .into_iter()
            .map(|name| AttributeDefinition {
                attribute_name: name.into(),
                attribute_type: ScalarAttributeType::S,
            })
            .collect(),
        billing_mode: BillingMode::PayPerRequest,
        provisioned_throughput: None,
        deletion_protection_enabled: false,
        initial_tags: Vec::new(),
        resource_arn: None,
        stream: None,
    };
    match client
        .command::<CreateTable>(account, mutation(), Json(spec))
        .await
        .unwrap()
        .output
        .0
    {
        CreateTableOutcome::Created(table) => *table,
        other => panic!("unexpected creation: {other:?}"),
    }
}
