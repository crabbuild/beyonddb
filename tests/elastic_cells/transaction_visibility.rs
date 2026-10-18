use crate::{
    local_node,
    read_barriers::{assert_range_read_barriers, assert_sdk_read_barrier},
    signed_http::SignedHttp,
    transaction_fixture,
};
use aws_sdk_dynamodb::types::AttributeValue as AwsAttributeValue;
use beyonddb::*;
use extenddb_core::{
    expression::ExpressionMaps,
    types::{AttributeValue, Item},
};
use extenddb_storage::{DataEngine, TableEngine};
use std::sync::{Arc, atomic::AtomicI64};
fn key(sk: &str) -> Item {
    Item::from([
        ("pk".into(), AttributeValue::S("same".into())),
        ("sk".into(), AttributeValue::N(sk.into())),
    ])
}
#[test]
fn range_read_barriers_cover_signed_sdk_requests_after_same_pk_split_and_restart() {
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
        let directory = tempfile::tempdir().unwrap();
        let clock = Arc::new(AtomicI64::new(local_node::wall_mutation().issued_at_ms));
        let (node, client) = local_node::start(directory.path(), clock.clone(), host.handle(), core.handle()).await.unwrap();
        let provisioner = Arc::new(CellInitialPartitionProvisioner::new(client.clone(), local_node::application().unwrap()).with_initial_partition_count(2).unwrap());
        let storage = CellStorage::new(client.clone(), "local").with_initial_partitions(provisioner.clone()).with_transaction_coordinators(provisioner.clone());
        let account = "123456789012";
        storage.create_table(account, serde_json::from_value(serde_json::json!({
            "TableName":"Visibility", "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"N"}], "BillingMode":"PAY_PER_REQUEST"
        })).unwrap()).await.unwrap();
        let info = storage.table_key_info(account, "Visibility").await.unwrap();
        for sk in ["1.5", "2", "3", "4", "8", "10"] { storage.put_item(&info, key(sk), false, None, &ExpressionMaps::default(), None).await.unwrap(); }
        let (source, _) = transaction_fixture::participant_for_key(&client, &info, &key("2")).await;
        assert_eq!(transaction_fixture::participant_for_key(&client, &info, &key("8")).await.0, source);
        let spec = client.query::<ReadPartitionState>(&source, None, Json(())).await.unwrap().output.0.unwrap().spec;
        assert_range_read_barriers(&storage, &client, &source, spec.epoch, &info).await;
        let plan = provisioner.split_partition(account, client.clone(), &info.table_id, spec.partition_id, spec.lower.unwrap_or_else(bdb_keyspace::RangeKey::minimum)).await.unwrap();
        assert_eq!(plan.children.len(), 2);
        let (left, _) = transaction_fixture::participant_for_key(&client, &info, &key("1.5")).await;
        let (right, _) = transaction_fixture::participant_for_key(&client, &info, &key("8")).await;
        assert_ne!(left, right, "one PK must now span actual independently owned cells");
        let partition = client.query::<ReadPartitionState>(&right, None, Json(())).await.unwrap().output.0.unwrap().spec;
        let http = SignedHttp::start(&client, provisioner, true).await;
        tokio::time::timeout(std::time::Duration::from_secs(30), assert_sdk_read_barrier(&http.sdk, &client, &partition, account)).await.unwrap();
        http.stop().await;
        local_node::stop(node).await.unwrap();
        let (node, client) = local_node::start(directory.path(), clock, host.handle(), core.handle()).await.unwrap();
        let provisioner = Arc::new(CellInitialPartitionProvisioner::new(client.clone(), local_node::application().unwrap()).with_initial_partition_count(2).unwrap());
        let http = SignedHttp::start(&client, provisioner, false).await;
        let response = http.sdk.query().table_name("Visibility").key_condition_expression("pk = :p").expression_attribute_values(":p", AwsAttributeValue::S("same".into())).consistent_read(true).send().await.unwrap();
        assert_eq!(response.items().iter().map(|item| item["sk"].as_n().unwrap().as_str()).collect::<Vec<_>>(), ["1.5", "2", "3", "4", "8", "10"]);
        assert!(response.last_evaluated_key().is_none());
        assert!(response.items().iter().all(|item| !item.contains_key("uncommitted")));
        http.stop().await;
        local_node::stop(node).await.unwrap();
    });
}
