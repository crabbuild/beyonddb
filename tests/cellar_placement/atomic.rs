use super::helpers::*;
use crate::residency_fixture::{ACCOUNT, Fixture, runtimes};
use aws_sdk_dynamodb::types::{
    AttributeValue as V, Get, Put, StreamSpecification, StreamViewType, TransactGetItem,
    TransactWriteItem,
};
use beyonddb::*;
use extenddb_storage::StreamEngine;

const NAME: &str = "ResidencyModelAtomic";
#[test]
fn sdk_range_local_reads_and_token_replay_survive_owner_replacement() {
    let (host, core) = runtimes();
    host.block_on(async {
        let mut fixture = Fixture::new(host.handle(), core.handle(), 1, 24).await;
        let created = create(&fixture.sdk, NAME, false)
            .tags(model_tag("auto"))
            .stream_specification(
                StreamSpecification::builder()
                    .stream_enabled(true)
                    .stream_view_type(StreamViewType::NewAndOldImages)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap()
            .table_description
            .unwrap();
        let arn = created.latest_stream_arn.unwrap();
        let table = table(&fixture, NAME).await;
        assert_eq!(ranges(&fixture, &table.id).await.len(), 2);
        for id in ["first", "second"] {
            fixture
                .sdk
                .put_item()
                .table_name(NAME)
                .item("id", V::S(id.into()))
                .item("value", V::N("7".into()))
                .send()
                .await
                .unwrap();
        }
        let target = target(&fixture, &table, "first").await;
        assert_eq!(
            target,
            super::helpers::target(&fixture, &table, "second").await
        );
        let write = |sdk: &aws_sdk_dynamodb::Client| {
            sdk.transact_write_items()
                .client_request_token("range-local-restart-token")
                .transact_items(
                    TransactWriteItem::builder()
                        .put(
                            Put::builder()
                                .table_name(NAME)
                                .item("id", V::S("first".into()))
                                .item("value", V::N("8".into()))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
        };
        write(&fixture.sdk).send().await.unwrap();
        let old = fixture.cell_identity(&target).await;
        fixture.replace().await;
        fixture
            .client
            .query::<ReadPartitionState>(&target, None, Json(()))
            .await
            .unwrap();
        let actual = fixture.cell_identity(&target).await;
        assert_eq!(actual.scope, old.scope);
        assert!(actual.epoch > old.epoch);
        fixture
            .sdk
            .put_item()
            .table_name(NAME)
            .item("id", V::S("first".into()))
            .item("value", V::N("9".into()))
            .send()
            .await
            .unwrap();
        write(&fixture.sdk).send().await.unwrap();
        let before = fixture
            .client
            .query::<ReadPartitionState>(&target, None, Json(()))
            .await
            .unwrap()
            .receipt
            .commit_sequence;
        let read = fixture
            .sdk
            .transact_get_items()
            .set_transact_items(Some(
                ["second", "first"]
                    .into_iter()
                    .map(|id| {
                        TransactGetItem::builder()
                            .get(
                                Get::builder()
                                    .table_name(NAME)
                                    .key("id", V::S(id.into()))
                                    .build()
                                    .unwrap(),
                            )
                            .build()
                    })
                    .collect(),
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(read.responses().len(), 2);
        for (image, (id, value)) in read
            .responses()
            .iter()
            .zip([("second", "7"), ("first", "9")])
        {
            let item = image.item().unwrap();
            assert_eq!(item["id"], V::S(id.into()));
            assert_eq!(item["value"], V::N(value.into()));
        }
        let after = fixture
            .client
            .query::<ReadPartitionState>(&target, None, Json(()))
            .await
            .unwrap()
            .receipt
            .commit_sequence;
        assert_eq!(
            before, after,
            "a small range-local read commits no participant work"
        );
        let streams = CellStorage::new(fixture.client.clone(), "us-east-1");
        let description = streams
            .describe_stream(
                ACCOUNT,
                &extenddb_core::types::DescribeStreamInput {
                    stream_arn: arn.clone(),
                    limit: Some(100),
                    exclusive_start_shard_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(description.shards.len(), 2);
        let mut count = 0;
        for shard in description.shards {
            streams
                .validate_shard(ACCOUNT, &arn, &shard.shard_id)
                .await
                .unwrap();
            count += streams
                .get_stream_records(ACCOUNT, &shard.shard_id, None, 100)
                .await
                .unwrap()
                .0
                .len();
        }
        assert_eq!(count, 4, "token replay must not emit another record");
        // Auto uses multiple cells and remains eligible for ordered splitting.
        let range = ranges(&fixture, &table.id).await.remove(0);
        assert!(
            fixture
                .provisioner
                .split_if_over_database_bytes(
                    ACCOUNT,
                    fixture.client.clone(),
                    &table.id,
                    range.partition_id,
                    range.lower,
                    1
                )
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(ranges(&fixture, &table.id).await.len(), 3);
        fixture.replace().await;
        write(&fixture.sdk).send().await.unwrap();
        assert_eq!(
            fixture
                .sdk
                .get_item()
                .table_name(NAME)
                .key("id", V::S("first".into()))
                .send()
                .await
                .unwrap()
                .item
                .unwrap()["value"],
            V::N("9".into())
        );
        assert_eq!(ranges(&fixture, &table.id).await.len(), 3);
        fixture.shutdown().await;
    });
}
