use super::helpers::*;
use crate::residency_fixture::{ACCOUNT, Fixture, runtimes};
use aws_sdk_dynamodb::types::AttributeValue as V;
use beyonddb::*;
use std::time::Duration;

const NAME: &str = "ResidencyModelIndex";
fn query(
    sdk: &aws_sdk_dynamodb::Client,
) -> aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder {
    sdk.query()
        .table_name(NAME)
        .index_name("ByBucket")
        .key_condition_expression("#bucket = :bucket")
        .expression_attribute_names("#bucket", "bucket")
        .expression_attribute_values(":bucket", V::S("bucket".into()))
}
#[test]
fn sdk_index_growth_preserves_multiple_base_cells_and_ignores_later_placement_tags() {
    let (host, core) = runtimes();
    host.block_on(async {
        let mut fixture = Fixture::new(host.handle(), core.handle(), 1, 32).await;
        fixture.set_initial_partition_count(4).await;
        let created = create(&fixture.sdk, NAME, true)
            .tags(model_tag("partitioned"))
            .send()
            .await
            .unwrap()
            .table_description
            .unwrap();
        // Pinned ExtendDB authorizes tag operations against table/*; preserve the
        // original explicit policy rather than broadening all fixture rights.
        CellAuthorizationStore::new(fixture.client.clone())
            .put_user_policy(
                ACCOUNT,
                "network-user",
                "model-tags",
                &serde_json::json!({"Version":"2012-10-17", "Statement":[{
                    "Effect":"Allow", "Action":["dynamodb:TagResource", "dynamodb:ListTagsOfResource"], "Resource":"*"
                }]})
                .to_string(),
            )
            .await
            .unwrap();
        fixture
            .sdk
            .tag_resource()
            .resource_arn(created.table_arn().unwrap())
            .tags(model_tag("single"))
            .send()
            .await
            .unwrap();
        let table = table(&fixture, NAME).await;
        assert_eq!(
            table.placement,
            TablePlacement::Routed {
                initial_partitions: 4
            }
        );
        let base = ranges(&fixture, &table.id).await;
        assert_eq!(base.len(), 4);
        let index = &table.global_secondary_indexes[0];
        let index_plan = ranges(&fixture, &index.id).await;
        assert_eq!(index_plan.len(), 4);
        let tasks = TaskGroup::default();
        CellStorage::new(fixture.client.clone(), "us-east-1")
            .install_global_index_loop(&tasks, vec![ACCOUNT.into()])
            .unwrap();
        // An ordered split needs two observed full keys. Equal GSI partition
        // keys retain distinct base-key suffixes and can still split safely.
        for id in ["indexed", "indexed-sibling"] {
            fixture
                .sdk
                .put_item()
                .table_name(NAME)
                .item("id", V::S(id.into()))
                .item("bucket", V::S("bucket".into()))
                .send()
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while query(&fixture.sdk).send().await.unwrap().items().len() != 2 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        tasks.shutdown().await.unwrap();
        let mut candidates = Vec::new();
        for range in &index_plan {
            let target = global_index_target(ACCOUNT, &index.id, &range.partition_id).unwrap();
            if fixture
                .client
                .query::<GlobalIndexSplitPoint>(&target, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .is_some()
            {
                candidates.push(range);
            }
        }
        assert_eq!(candidates.len(), 1);
        let range = candidates[0];
        assert!(
            fixture
                .provisioner
                .split_global_index_if_over_database_bytes(
                    ACCOUNT,
                    fixture.client.clone(),
                    &index.id,
                    range.partition_id,
                    range.lower.clone(),
                    1
                )
                .await
                .unwrap()
                .is_some()
        );
        let grown = ranges(&fixture, &index.id).await;
        assert_eq!(grown.len(), 5);
        assert_eq!(ranges(&fixture, &table.id).await, base);
        let result = query(&fixture.sdk).send().await.unwrap();
        assert_eq!(
            result
                .items()
                .iter()
                .map(|item| item["id"].clone())
                .collect::<Vec<_>>(),
            vec![V::S("indexed".into()), V::S("indexed-sibling".into())]
        );
        let mut identities = Vec::new();
        for range in &base {
            let target = data_target(ACCOUNT, &table.id, &range.partition_id).unwrap();
            fixture
                .client
                .query::<ReadPartitionState>(&target, None, Json(()))
                .await
                .unwrap();
            let identity = fixture.cell_identity(&target).await;
            identities.push((target, identity, false));
        }
        for range in &grown {
            let target = global_index_target(ACCOUNT, &index.id, &range.partition_id).unwrap();
            assert!(
                fixture
                    .client
                    .query::<ReadGlobalIndexPartition>(&target, None, Json(()))
                    .await
                    .unwrap()
                    .output
                    .0
                    .is_some()
            );
            let identity = fixture.cell_identity(&target).await;
            identities.push((target, identity, true));
        }
        fixture.replace().await;
        let restored = super::helpers::table(&fixture, NAME).await;
        assert_eq!(restored.placement, table.placement);
        assert_eq!(ranges(&fixture, &table.id).await, base);
        assert_eq!(ranges(&fixture, &index.id).await, grown);
        for (target, old, is_index) in identities {
            if is_index {
                assert!(
                    fixture
                        .client
                        .query::<ReadGlobalIndexPartition>(&target, None, Json(()))
                        .await
                        .unwrap()
                        .output
                        .0
                        .is_some()
                );
            } else {
                assert!(
                    fixture
                        .client
                        .query::<ReadPartitionState>(&target, None, Json(()))
                        .await
                        .unwrap()
                        .output
                        .0
                        .is_some()
                );
            }
            let actual = fixture.cell_identity(&target).await;
            assert_eq!(actual.scope, old.scope);
            assert!(actual.epoch > old.epoch);
        }
        let tags = fixture
            .sdk
            .list_tags_of_resource()
            .resource_arn(created.table_arn().unwrap())
            .send()
            .await
            .unwrap();
        assert!(
            tags.tags()
                .iter()
                .any(|tag| tag.key() == "beyonddb:cell-model" && tag.value() == "single")
        );
        let recovered = query(&fixture.sdk).send().await.unwrap();
        assert_eq!(recovered.items(), result.items());
        fixture.shutdown().await;
    });
}
