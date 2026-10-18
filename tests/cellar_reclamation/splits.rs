use super::helpers::{ForbidSources, Probe, items};
use crate::{
    residency_fixture::{ACCOUNT, runtimes},
    table_helpers,
};
use aws_sdk_dynamodb::types::AttributeValue as V;
use beyonddb::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[test]
fn sdk_split_sources_retain_seals_and_live_children_beyond_residency() {
    let (host, core) = runtimes();
    host.block_on(async {
        let (mut fixture, probe) = Probe::fixture(host.handle(), core.handle(), 2, 11).await;
        let table = table_helpers::table(&fixture, "Residency").await;
        // Ordered splits require two observed full keys in the selected range.
        // Retain both seeded items and add a lower-range sibling.
        fixture
            .sdk
            .put_item()
            .table_name("Residency")
            .item("id", V::S("second".into()))
            .item("value", V::S("sibling".into()))
            .send()
            .await
            .unwrap();
        let range = table_helpers::ranges(&fixture, &table.id).await.remove(0);
        let base = fixture
            .provisioner
            .split_partition(
                ACCOUNT,
                fixture.client.clone(),
                &table.id,
                range.partition_id,
                range.lower,
            )
            .await
            .unwrap();
        assert_eq!(table_helpers::ranges(&fixture, &table.id).await.len(), 3);
        table_helpers::create(&fixture.sdk, "ResidencyIndexed", true)
            .send()
            .await
            .unwrap();
        let indexed = table_helpers::table(&fixture, "ResidencyIndexed").await;
        let storage = CellStorage::new(fixture.client.clone(), "us-east-1");
        for id in ["indexed", "indexed-sibling"] {
            fixture
                .sdk
                .put_item()
                .table_name("ResidencyIndexed")
                .item("id", V::S(id.into()))
                .item("bucket", V::S("same".into()))
                .send()
                .await
                .unwrap();
        }
        for range in table_helpers::ranges(&fixture, &indexed.id).await {
            while storage
                .project_index_changes(
                    ACCOUNT,
                    &data_target(ACCOUNT, &indexed.id, &range.partition_id).unwrap(),
                    &indexed.id,
                )
                .await
                .unwrap()
            {}
        }
        let index = &indexed.global_secondary_indexes[0];
        let mut selected = None;
        for range in table_helpers::ranges(&fixture, &index.id).await {
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
                selected = Some(range);
            }
        }
        let range = selected.unwrap();
        let split = fixture
            .provisioner
            .split_global_index_partition(
                ACCOUNT,
                fixture.client.clone(),
                &index.id,
                range.partition_id,
                range.lower,
            )
            .await
            .unwrap();
        assert_eq!(table_helpers::ranges(&fixture, &index.id).await.len(), 3);
        probe.at_capacity("residency-owner-0", 11).await;
        let source = data_target(ACCOUNT, &table.id, &base.source.partition_id).unwrap();
        let index_source =
            global_index_target(ACCOUNT, &index.id, &split.source.partition_id).unwrap();
        let forbidden = Arc::new(ForbidSources {
            targets: vec![source.clone(), index_source.clone()],
            refused: AtomicUsize::new(0),
        });
        let replay = fixture.client.with_admission(forbidden.clone());
        fixture
            .provisioner
            .resume_split(ACCOUNT, replay.clone(), &base)
            .await
            .unwrap();
        fixture
            .provisioner
            .resume_global_index_split(ACCOUNT, replay, &split)
            .await
            .unwrap();
        assert_eq!(forbidden.refused.load(Ordering::SeqCst), 0);
        table_helpers::create(&fixture.sdk, "ResidencyAfterSplit", false)
            .send()
            .await
            .unwrap();
        items(&fixture).await;
        let old = fixture.cell_identity(&source).await;
        // Query both historical sources before replacement to observe actual SDK
        // scopes, then recover seals and all serving children from the bucket.
        assert!(matches!(
            fixture
                .client
                .query::<ReadPartitionState>(&source, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap()
                .state,
            PartitionState::Sealed(_)
        ));
        assert_eq!(
            fixture
                .client
                .query::<ReadGlobalIndexState>(&index_source, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap(),
            GlobalIndexState::Sealed(split.clone())
        );
        let index_old = fixture.cell_identity(&index_source).await;
        fixture.replace().await;
        assert!(matches!(
            fixture
                .client
                .query::<ReadPartitionState>(&source, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap()
                .state,
            PartitionState::Sealed(_)
        ));
        assert_eq!(
            fixture
                .client
                .query::<ReadGlobalIndexState>(&index_source, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap(),
            GlobalIndexState::Sealed(split.clone())
        );
        for (target, old) in [(&source, old), (&index_source, index_old)] {
            let actual = fixture.cell_identity(target).await;
            assert_eq!(actual.scope, old.scope);
            assert!(actual.epoch > old.epoch);
        }
        let replay = fixture.client.with_admission(forbidden.clone());
        fixture
            .provisioner
            .resume_split(ACCOUNT, replay.clone(), &base)
            .await
            .unwrap();
        fixture
            .provisioner
            .resume_global_index_split(ACCOUNT, replay, &split)
            .await
            .unwrap();
        assert_eq!(forbidden.refused.load(Ordering::SeqCst), 0);
        items(&fixture).await;
        let mut rows = fixture
            .sdk
            .scan()
            .table_name("ResidencyIndexed")
            .index_name("ByBucket")
            .send()
            .await
            .unwrap()
            .items
            .unwrap();
        rows.sort_by_key(|item| item["id"].as_s().unwrap().clone());
        assert_eq!(
            rows,
            ["indexed", "indexed-sibling"]
                .map(|id| std::collections::HashMap::from([
                    ("id".into(), V::S(id.into())),
                    ("bucket".into(), V::S("same".into()))
                ]))
                .to_vec()
        );
        assert_eq!(
            fixture
                .sdk
                .get_item()
                .table_name("Residency")
                .key("id", V::S("second".into()))
                .consistent_read(true)
                .send()
                .await
                .unwrap()
                .item
                .unwrap()["value"],
            V::S("sibling".into())
        );
        probe.at_capacity("residency-owner-1", 11).await;
        fixture.shutdown().await;
        probe.close().await;
    });
}
