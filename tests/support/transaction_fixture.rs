//! Two actual ordered data cells for coordinator/participant integration cases.
use crate::local_node;
use bdb_cellar_runtime::{client::CellClient, identity::CellTarget};
use beyonddb::*;
use extenddb_core::{
    expression::ExpressionMaps,
    types::{AttributeValue, Item},
};
use extenddb_storage::{DataEngine, TableEngine};
use std::sync::Arc;
pub struct TableFixture {
    pub storage: CellStorage,
    pub table: TableRecord,
    pub participants: Vec<(CellTarget, CoordinatorParticipant)>,
}
pub async fn table(client: &CellClient, name: &str) -> TableFixture {
    let provisioner = Arc::new(
        CellInitialPartitionProvisioner::new(client.clone(), local_node::application().unwrap())
            .with_initial_partition_count(2)
            .unwrap(),
    );
    let storage = CellStorage::new(client.clone(), "local")
        .with_initial_partitions(provisioner.clone())
        .with_transaction_coordinators(provisioner);
    let account_id = "123456789012";
    storage.create_table(account_id, serde_json::from_value(serde_json::json!({
            "TableName": name, "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}], "BillingMode": "PAY_PER_REQUEST"
        })).unwrap()).await.unwrap();
    let account = account_target(account_id).unwrap();
    let table = client
        .query::<DescribeTable>(&account, None, Json(name.into()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    let info = storage.table_key_info(account_id, name).await.unwrap();
    let RoutePageOutcome::Page { partitions, .. } = read_route_page(
        client,
        &account,
        RoutePageInput {
            table_id: table.id.clone(),
            start_key: None,
            after_lower: None,
            expected_epoch: None,
        },
    )
    .await
    .unwrap() else {
        panic!("missing routes");
    };
    assert_eq!(partitions.len(), 2);
    let mut participants = Vec::new();
    for (index, key) in ["first", "Ω-other"].into_iter().enumerate() {
        let item = Item::from([
            ("id".into(), AttributeValue::S(key.into())),
            ("value".into(), AttributeValue::N("1".into())),
        ]);
        storage
            .put_item(
                &info,
                item.clone(),
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .unwrap();
        let key = ordered_data_key(&table.id, &item, &table.key_schema).unwrap();
        let range = partitions
            .iter()
            .find(|range| {
                range.lower <= key && range.upper.as_ref().is_none_or(|upper| &key < upper)
            })
            .unwrap();
        let target = data_target(account_id, &table.id, &range.partition_id).unwrap();
        participants.push((
            target,
            CoordinatorParticipant {
                target: CoordinatorParticipantTarget::Data {
                    table_id: table.id.clone(),
                    partition_id: range.partition_id,
                    epoch: range.epoch,
                },
                operations: vec![IndexedTransactionOperation {
                    index: index as u8,
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
    assert_ne!(participants[0].0, participants[1].0);
    TableFixture {
        storage,
        table,
        participants,
    }
}

/// Explicit private account generations remain useful for legacy domain tests;
/// public CreateTable always uses the multi-cell provisioner above.
pub fn account_spec(name: &str) -> TableSpec {
    use extenddb_core::types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
    };
    TableSpec {
        table_class: Default::default(),
        placement: TablePlacement::Account,
        table_name: name.into(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "id".into(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "id".into(),
            attribute_type: ScalarAttributeType::S,
        }],
        local_secondary_indexes: Vec::new(),
        global_secondary_indexes: Vec::new(),
        billing_mode: BillingMode::PayPerRequest,
        provisioned_throughput: None,
        deletion_protection_enabled: false,
        initial_tags: Vec::new(),
        resource_arn: None,
        stream: None,
    }
}

pub async fn mixed(
    client: &CellClient,
) -> (
    Arc<CellInitialPartitionProvisioner>,
    [extenddb_core::types::TableKeyInfo; 2],
) {
    let provisioner = Arc::new(
        CellInitialPartitionProvisioner::new(client.clone(), local_node::application().unwrap())
            .with_initial_partition_count(2)
            .unwrap(),
    );
    let account_id = "123456789012";
    client
        .command::<CreateTable>(
            &account_target(account_id).unwrap(),
            local_node::wall_mutation(),
            Json(account_spec("PrivateImages")),
        )
        .await
        .unwrap();
    let routed =
        CellStorage::new(client.clone(), "local").with_initial_partitions(provisioner.clone());
    routed.create_table(account_id, serde_json::from_value(serde_json::json!({
        "TableName": "RoutedImages", "KeySchema": [{"AttributeName":"id","KeyType":"HASH"}],
        "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}], "BillingMode":"PAY_PER_REQUEST"
    })).unwrap()).await.unwrap();
    let infos = [
        routed
            .table_key_info(account_id, "PrivateImages")
            .await
            .unwrap(),
        routed
            .table_key_info(account_id, "RoutedImages")
            .await
            .unwrap(),
    ];
    (provisioner, infos)
}
pub async fn participant_for_key(
    client: &CellClient,
    info: &extenddb_core::types::TableKeyInfo,
    key: &Item,
) -> (CellTarget, CoordinatorParticipantTarget) {
    let account = account_target(&info.account_id).unwrap();
    let encoded = ordered_data_key(&info.table_id, key, &info.base_key_schema).unwrap();
    match read_route_page(
        client,
        &account,
        RoutePageInput {
            table_id: info.table_id.clone(),
            start_key: Some(encoded.clone()),
            after_lower: None,
            expected_epoch: None,
        },
    )
    .await
    .unwrap()
    {
        RoutePageOutcome::Page { partitions, .. } => {
            let part = partitions
                .iter()
                .find(|part| {
                    part.lower <= encoded
                        && part.upper.as_ref().is_none_or(|upper| &encoded < upper)
                })
                .unwrap();
            (
                data_target(&info.account_id, &info.table_id, &part.partition_id).unwrap(),
                CoordinatorParticipantTarget::Data {
                    table_id: info.table_id.clone(),
                    partition_id: part.partition_id,
                    epoch: part.epoch,
                },
            )
        }
        RoutePageOutcome::Unrouted => {
            let record = client
                .query::<DescribeTableById>(&account, None, Json(info.table_id.clone()))
                .await
                .unwrap()
                .output
                .0
                .unwrap();
            assert_eq!(
                record.placement,
                TablePlacement::Account,
                "only an explicit private account generation can lack routes"
            );
            (account, CoordinatorParticipantTarget::Account)
        }
        RoutePageOutcome::Changed => panic!("fixture directory unexpectedly changed"),
    }
}
