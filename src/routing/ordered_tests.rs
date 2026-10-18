//! Real serving router: one PK spans cells and a split changes both continuations.
use super::*;
use crate::test_support::{mutation, start, stop};
use crate::{
    Beyonddb, CellInitialPartitionProvisioner, CellStorage, CreateTable, CreateTableOutcome,
    TableClass, TablePlacement, TableSpec,
};
use bdb_cellar_runtime::{app::CellApplication, descriptors::BuildDescriptor, identity::Digest};
use extenddb_core::{
    expression::{ExpressionMaps, parse_key_condition, tokenize},
    types::{
        AttributeDefinition, AttributeValue, BillingMode, Item, KeySchemaElement, KeyType,
        ScalarAttributeType, TableKeyInfo,
    },
};
use extenddb_storage::DataEngine;
use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicI64},
    time::{SystemTime, UNIX_EPOCH},
};

fn item(sk: u32) -> Item {
    Item::from([
        ("pk".into(), AttributeValue::S("hot".into())),
        ("sk".into(), AttributeValue::N(sk.to_string())),
    ])
}
fn numbers(items: &[Item]) -> Vec<u32> {
    items
        .iter()
        .map(|item| match &item["sk"] {
            AttributeValue::N(value) => value.parse().unwrap(),
            _ => panic!("not a number"),
        })
        .collect()
}
async fn query(
    storage: &CellStorage,
    info: &TableKeyInfo,
    forward: bool,
    limit: i64,
    cursor: Option<&Item>,
) -> anyhow::Result<(Vec<Item>, Option<Item>)> {
    let condition = parse_key_condition(&tokenize("pk = :p")?)?;
    let maps = ExpressionMaps::new(
        HashMap::new(),
        HashMap::from([("p".into(), AttributeValue::S("hot".into()))]),
    );
    Ok(storage
        .query(info, &condition, &maps, forward, Some(limit), cursor, None)
        .await?)
}
#[test]
fn one_pk_queries_three_cells_and_resumes_through_an_observed_key_split() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let clock = Arc::new(AtomicI64::new(now));
    host.block_on(async {
        let (node, client) = start(
            directory.path(),
            clock.clone(),
            host.handle(),
            core.handle(),
        )
        .await?;
        let account_id = "123456789012";
        let account = crate::account_target(account_id)?;
        let created = client
            .command::<CreateTable>(
                &account,
                mutation(&clock),
                Json(TableSpec {
                    table_class: TableClass::Standard,
                    placement: TablePlacement::Routed {
                        initial_partitions: 3,
                    },
                    table_name: "OrderedServing".into(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "pk".into(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "sk".into(),
                            key_type: KeyType::Range,
                        },
                    ],
                    attribute_definitions: vec![
                        AttributeDefinition {
                            attribute_name: "pk".into(),
                            attribute_type: ScalarAttributeType::S,
                        },
                        AttributeDefinition {
                            attribute_name: "sk".into(),
                            attribute_type: ScalarAttributeType::N,
                        },
                    ],
                    local_secondary_indexes: Vec::new(),
                    global_secondary_indexes: Vec::new(),
                    billing_mode: BillingMode::PayPerRequest,
                    provisioned_throughput: None,
                    deletion_protection_enabled: false,
                    initial_tags: Vec::new(),
                    resource_arn: None,
                    stream: None,
                }),
            )
            .await?;
        let CreateTableOutcome::Created(table) = created.output.0 else {
            anyhow::bail!("create failed");
        };
        let key = |sk| crate::ordered_data_key(&table.id, &item(sk), &table.key_schema);
        let cuts = [key(10)?, key(20)?];
        let mut parts = Vec::new();
        for index in 0..3 {
            let part = PartitionSpec {
                table: (*table).clone(),
                partition_id: [index as u8 + 1; 16],
                lower: if index == 0 {
                    None
                } else {
                    Some(cuts[index - 1].clone())
                },
                upper: cuts.get(index).cloned(),
                epoch: 1,
            };
            let target = crate::data_target(account_id, &table.id, &part.partition_id)?;
            client
                .command::<crate::InstallPartition>(
                    &target,
                    mutation(&clock),
                    Json(crate::PartitionInstall::Serving(part.clone())),
                )
                .await?;
            parts.push(part);
        }
        let ranges = parts
            .iter()
            .map(|part| RoutePagePartition {
                partition_id: part.partition_id,
                lower: part.lower.clone().unwrap_or_else(RangeKey::minimum),
                upper: part.upper.clone(),
                epoch: part.epoch,
            })
            .collect::<Vec<_>>();
        let spec = crate::DirectorySpec::root(table.id.clone());
        let target = crate::directory_target(account_id, &spec)?;
        let fingerprint = crate::directory::fingerprint(&spec, &ranges)?;
        let installed = client
            .command::<crate::InstallDirectory>(
                &target,
                mutation(&clock),
                Json(crate::DirectoryInstall {
                    spec,
                    ranges,
                    source: None,
                }),
            )
            .await?;
        client
            .command::<ActivateTableRoute>(
                &account,
                mutation(&clock),
                Json(TableRoutePublication {
                    route: TableRoute {
                        table_id: table.id.clone(),
                        epoch: 1,
                        partitions: parts.clone(),
                    },
                    receipt: crate::DirectoryCopyReceipt {
                        cell_id: *target.cell_id().as_bytes(),
                        sequence: installed.receipt.commit_sequence,
                        fingerprint,
                    },
                }),
            )
            .await?;
        let info = TableKeyInfo {
            table_name: table.table_name.clone(),
            table_id: table.id.clone(),
            account_id: account_id.into(),
            key_schema: table.key_schema.clone(),
            base_key_schema: table.key_schema.clone(),
            attribute_definitions: table.attribute_definitions.clone(),
            ..Default::default()
        };
        let storage = CellStorage::new(client.clone(), "local");
        for sk in 0..30 {
            storage
                .put_item(
                    &info,
                    item(sk),
                    false,
                    None,
                    &ExpressionMaps::default(),
                    None,
                )
                .await?;
        }
        for part in &parts {
            let target = crate::data_target(account_id, &table.id, &part.partition_id)?;
            assert_eq!(
                client
                    .query::<crate::PartitionUsage>(&target, None, Json(()))
                    .await?
                    .output
                    .0
                    .item_count,
                10
            );
        }
        let (first, forward_cursor) = query(&storage, &info, true, 12, None).await?;
        assert_eq!(numbers(&first), (0..12).collect::<Vec<_>>());
        let (first, reverse_cursor) = query(&storage, &info, false, 12, None).await?;
        assert_eq!(numbers(&first), (18..30).rev().collect::<Vec<_>>());
        let (scan_first, scan_cursor) = storage
            .scan(&info, Some(12), None, None, None, None)
            .await?;
        assert_eq!(numbers(&scan_first), (0..12).collect::<Vec<_>>());
        let application = Beyonddb::compile(BuildDescriptor {
            source_revision: "domain-sdk-test".into(),
            cargo_lock_digest: Digest::from_bytes([1; 32]),
        })?;
        let provisioner =
            CellInitialPartitionProvisioner::new(client.clone(), Arc::new(application));
        let plan = provisioner
            .split_partition(
                account_id,
                client.clone(),
                &table.id,
                parts[1].partition_id,
                cuts[0].clone(),
            )
            .await?;
        assert_eq!(plan.children[1].lower, Some(key(15)?));
        assert!(
            client
                .query::<crate::ReadDirectoryChanges>(&target, None, Json(Some(cuts[0].clone())))
                .await?
                .output
                .0
                .is_empty()
        );
        provisioner
            .split_directory(
                &client,
                account_id,
                &crate::DirectorySpec::root(table.id.clone()),
            )
            .await?;
        // Numeric SK bounds prune the logical span before directory traversal.
        let cases = [
            ("pk = :p AND sk = :v", vec![("v", "15")], vec![15]),
            ("pk = :p AND sk < :v", vec![("v", "12")], (0..12).collect()),
            (
                "pk = :p AND sk <= :v",
                vec![("v", "12")],
                (0..=12).collect(),
            ),
            ("pk = :p AND sk > :v", vec![("v", "18")], (19..30).collect()),
            (
                "pk = :p AND sk >= :v",
                vec![("v", "18")],
                (18..30).collect(),
            ),
            (
                "pk = :p AND sk BETWEEN :lo AND :hi",
                vec![("lo", "7"), ("hi", "24")],
                (7..=24).collect(),
            ),
        ];
        for (expression, values, expected) in cases {
            let condition = parse_key_condition(&tokenize(expression)?)?;
            let mut parameters = HashMap::from([("p".into(), AttributeValue::S("hot".into()))]);
            parameters.extend(
                values
                    .into_iter()
                    .map(|(name, value)| (name.into(), AttributeValue::N(value.into()))),
            );
            let maps = ExpressionMaps::new(HashMap::new(), parameters);
            for forward in [true, false] {
                let mut cursor = None;
                let mut seen = Vec::new();
                loop {
                    let (page, next) = storage
                        .query(
                            &info,
                            &condition,
                            &maps,
                            forward,
                            Some(4),
                            cursor.as_ref(),
                            None,
                        )
                        .await?;
                    seen.extend(numbers(&page));
                    cursor = next;
                    if cursor.is_none() {
                        break;
                    }
                }
                let mut ordered = expected.clone();
                if !forward {
                    ordered.reverse();
                }
                assert_eq!(seen, ordered, "{expression}, forward={forward}");
            }
        }
        let mut scan_seen = Vec::new();
        let mut cursor = scan_cursor;
        loop {
            let (page, next) = storage
                .scan(&info, Some(4), cursor.as_ref(), None, None, None)
                .await?;
            scan_seen.extend(numbers(&page));
            cursor = next;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(scan_seen, (12..30).collect::<Vec<_>>());

        for (forward, mut cursor, expected) in [
            (true, forward_cursor, (12..30).collect::<Vec<_>>()),
            (false, reverse_cursor, (0..18).rev().collect::<Vec<_>>()),
        ] {
            let mut seen = Vec::new();
            loop {
                let (items, next) = query(&storage, &info, forward, 4, cursor.as_ref()).await?;
                seen.extend(numbers(&items));
                cursor = next;
                if cursor.is_none() {
                    break;
                }
            }
            assert_eq!(seen, expected);
        }
        stop(node).await?;
        let (node, client) = start(directory.path(), clock, host.handle(), core.handle()).await?;
        let storage = Arc::new(CellStorage::new(client, "local"));
        let (all, next) = query(&storage, &info, true, 100, None).await?;
        assert_eq!(numbers(&all), (0..30).collect::<Vec<_>>());
        assert!(next.is_none());
        query_evaluation_budget(&storage, &info).await?;
        stop(node).await
    })
}

/// Exercise the actual frontend accumulator over several ordered cells and
/// several transport chunks. Escaped strings make transport bytes much larger
/// than DynamoDB's evaluated item bytes, so these budgets cannot be conflated.
async fn query_evaluation_budget(
    storage: &Arc<CellStorage>,
    info: &TableKeyInfo,
) -> anyhow::Result<()> {
    for sk in 0..30 {
        let mut value = item(sk);
        value.insert("payload".into(), AttributeValue::S("\0".repeat(70 * 1024)));
        storage
            .put_item(info, value, false, None, &ExpressionMaps::default(), None)
            .await?;
    }
    let context = extenddb_engine::OperationContext {
        storage: storage.clone(),
        limits: Arc::default(),
        region: "local".into(),
        account_id: info.account_id.clone().into(),
        import_paths: Arc::from([]),
        export_paths: Arc::from([]),
        pre_fetched_key_info: Some(info.clone()),
        auth_cache: extenddb_auth::AuthCacheRegistry::empty(),
        table_key_info_lookup: None,
    };
    for forward in [true, false] {
        // A filter that rejects every item still spends the evaluation budget
        // and must advance a logical cursor across the first physical fence.
        let request = serde_json::json!({
            "TableName": info.table_name,
            "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeValues": {":p": {"S": "hot"}},
            "FilterExpression": "attribute_not_exists(payload)",
            "ScanIndexForward": forward,
            "ConsistentRead": true,
        });
        let first = extenddb_engine::handle_query(request.clone(), &context)
            .await?
            .body;
        assert_eq!(first["Count"], 0);
        assert_eq!(first["ScannedCount"], 14);
        assert!(first["Items"].as_array().unwrap().is_empty());
        assert_eq!(
            first["LastEvaluatedKey"]["sk"]["N"],
            if forward { "13" } else { "16" }
        );
        let mut resume = request.clone();
        resume["ExclusiveStartKey"] = first["LastEvaluatedKey"].clone();
        let second = extenddb_engine::handle_query(resume.clone(), &context)
            .await?
            .body;
        assert_eq!(second["Count"], 0);
        assert_eq!(second["ScannedCount"], 14);
        resume["ExclusiveStartKey"] = second["LastEvaluatedKey"].clone();
        let final_page = extenddb_engine::handle_query(resume, &context).await?.body;
        assert_eq!(final_page["ScannedCount"], 2);
        assert!(final_page.get("LastEvaluatedKey").is_none());

        // Projection does not enlarge the evaluation budget. Limit is shared
        // across cells and is counted before filtering, too.
        let mut projected = request.clone();
        projected
            .as_object_mut()
            .unwrap()
            .remove("FilterExpression");
        projected["ProjectionExpression"] = serde_json::json!("pk, sk");
        let page = extenddb_engine::handle_query(projected, &context)
            .await?
            .body;
        assert_eq!(page["Count"], 14);
        assert_eq!(page["ScannedCount"], 14);
        assert!(
            page["Items"]
                .as_array()
                .unwrap()
                .iter()
                .all(|item| item.get("payload").is_none())
        );
        let mut limited = request;
        limited["Limit"] = serde_json::json!(12);
        let page = extenddb_engine::handle_query(limited, &context).await?.body;
        assert_eq!(page["Count"], 0);
        assert_eq!(page["ScannedCount"], 12);
        assert_eq!(
            page["LastEvaluatedKey"]["sk"]["N"],
            if forward { "11" } else { "18" }
        );
    }
    Ok(())
}
