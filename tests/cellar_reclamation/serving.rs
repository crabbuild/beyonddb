use super::helpers::{Probe, items};
use crate::{
    native_peer,
    residency_fixture::{ACCOUNT, runtimes},
    table_helpers,
};
use aws_sdk_dynamodb::types::{AttributeValue as V, TransactWriteItem, Update};
use beyonddb::*;

#[test]
fn sdk_first_cross_cell_transaction_at_capacity_replays_after_owner_replacement() {
    let (host, core) = runtimes();
    host.block_on(async {
        let (mut fixture, probe) = Probe::fixture(host.handle(), core.handle(), 2, 5).await;
        assert_ne!(fixture.data[0].0, fixture.data[1].0);
        probe.at_capacity("residency-owner-0", 5).await;
        let token = "full-residency-first-transaction";
        let target = coordinator_target(ACCOUNT, token.as_bytes()).unwrap();
        let operations: Vec<_> = fixture
            .data
            .iter()
            .map(|(_, item)| {
                TransactWriteItem::builder()
                    .update(
                        Update::builder()
                            .table_name("Residency")
                            .key("id", item["id"].clone())
                            .update_expression("ADD #count :one")
                            .expression_attribute_names("#count", "count")
                            .expression_attribute_values(":one", V::N("1".into()))
                            .build()
                            .unwrap(),
                    )
                    .build()
            })
            .collect();
        let write = |sdk: &aws_sdk_dynamodb::Client| {
            sdk.transact_write_items()
                .client_request_token(token)
                .set_transact_items(Some(operations.clone()))
        };
        write(&fixture.sdk).send().await.unwrap();
        let old = fixture.cell_identity(&target).await;
        fixture.replace().await;
        write(&fixture.sdk).send().await.unwrap();
        let actual = fixture.cell_identity(&target).await;
        assert_eq!(actual.scope, old.scope);
        assert!(actual.epoch > old.epoch);
        for (_, item) in &mut fixture.data {
            item.insert("count".into(), V::N("1".into()));
        }
        items(&fixture).await;
        probe.at_capacity("residency-owner-1", 5).await;
        fixture.shutdown().await;
        probe.close().await;
    });
}
#[test]
fn sdk_reads_restore_data_and_live_directory_under_four_cell_residency() {
    let (host, core) = runtimes();
    host.block_on(async {
        let (mut fixture, probe) = Probe::fixture(host.handle(), core.handle(), 1, 4).await;
        let table = table_helpers::table(&fixture, "Residency").await;
        assert_eq!(table_helpers::ranges(&fixture, &table.id).await.len(), 2);
        let target = directory_target(ACCOUNT, &DirectorySpec::root(table.id)).unwrap();
        let before = fixture
            .client
            .query::<ReadDirectory>(&target, None, Json(()))
            .await
            .unwrap();
        native_peer::put_credential(&fixture.client, "AKIADIRECTORYRESIDENCY", ACCOUNT).await;
        probe.at_capacity("residency-owner-0", 4).await;
        for _ in 0..2 {
            items(&fixture).await;
            let restored = fixture
                .client
                .query::<ReadDirectory>(&target, None, Json(()))
                .await
                .unwrap();
            assert_eq!(restored.output, before.output);
            assert_eq!(
                restored.receipt.commit_sequence,
                before.receipt.commit_sequence
            );
        }
        let old = fixture.cell_identity(&target).await;
        fixture.replace().await;
        items(&fixture).await;
        let restored = fixture
            .client
            .query::<ReadDirectory>(&target, None, Json(()))
            .await
            .unwrap();
        assert_eq!(restored.output, before.output);
        assert_eq!(
            restored.receipt.commit_sequence,
            before.receipt.commit_sequence
        );
        let actual = fixture.cell_identity(&target).await;
        assert_eq!(actual.scope, old.scope);
        assert!(actual.epoch > old.epoch);
        fixture.shutdown().await;
        probe.close().await;
    });
}
