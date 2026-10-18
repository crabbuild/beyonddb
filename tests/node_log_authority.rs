//! Native SDK lease/log integration, replacing BeyondDB's old lease publisher.
use cellar::sdk::{FenceReason, NodeOutcome};
use std::{
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};
#[path = "node_log_authority/fixture.rs"]
mod fixture;
#[path = "node_log_authority/heartbeat_versions.rs"]
mod heartbeat_versions;
#[allow(dead_code)] // Shared peer fixture also exposes table and wire-cut helpers.
#[path = "support/native_peer.rs"]
mod native_peer;
#[path = "node_log_authority/stalled_log_read.rs"]
mod stalled_log_read;
#[path = "node_log_authority/store.rs"]
mod store;
use fixture::*;
use native_peer::{FRONT, SOURCE};

fn runtimes() -> (tokio::runtime::Runtime, tokio::runtime::Runtime) {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .with_ansi(false)
        .try_init();
    (
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    )
}

#[test]
fn folded_log_and_ack_marker_survive_native_heartbeats() {
    let (host, core) = runtimes();
    host.block_on(async {
        let mut fixture = Fixture::start(host.handle(), core.handle()).await;
        fixture.assert_source_owned();
        let before = fixture.lease(SOURCE).await;
        assert_eq!(before.log()["state"], "open");
        assert_eq!(before.log()["may_have_fleet_acks"], true);
        let after = fixture
            .wait_lease(SOURCE, |lease| lease.expires() >= before.expires() + 5_000)
            .await;
        assert_same_session(&before, &after);
        fixture.write("after-heartbeats").await;
        fixture.source.stop().await;
        let sealed = fixture.lease(SOURCE).await;
        assert_eq!(sealed.log()["state"], "sealed");
        assert_eq!(sealed.epoch(), before.epoch());
        assert_eq!(sealed.log()["may_have_fleet_acks"], true);
        assert!(
            fixture
                .source
                .client
                .query::<beyonddb::ListTables>(
                    &beyonddb::account_target(ACCOUNT).unwrap(),
                    None,
                    beyonddb::Json(beyonddb::ListTablesInput {
                        limit: 1,
                        exclusive_start: None,
                        live_only: false
                    }),
                )
                .await
                .is_err()
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(
            fixture.lease(SOURCE).await.etag,
            sealed.etag,
            "shutdown resurrected lease/log work"
        );
        fixture.stop().await;
    });
}

#[test]
fn expired_unreachable_follower_cannot_prove_a_new_write() {
    let (host, core) = runtimes();
    host.block_on(async {
        let mut fixture = Fixture::start(host.handle(), core.handle()).await;
        let before = fixture.lease(SOURCE).await;
        fixture.front.network.cut(true).await;
        let outcome = fixture.front.node.take().unwrap().cancel().await.unwrap();
        assert_eq!(outcome, NodeOutcome::Cancelled);
        fixture
            .wait_lease(FRONT, |lease| {
                lease.expires() <= cellar::ownership_store::now_ms()
            })
            .await;
        // Native Cellar can retain a historical ensemble after expiry. That
        // membership is not a new durability proof: all live acknowledgements
        // are gone, so withholding both payload paths must hold this response.
        fixture
            .ordinary
            .withhold_payloads
            .store(true, Ordering::SeqCst);
        let write = fixture
            .http
            .sdk
            .put_item()
            .table_name("NetworkData")
            .item(
                "id",
                aws_sdk_dynamodb::types::AttributeValue::S("first".into()),
            )
            .item(
                "value",
                aws_sdk_dynamodb::types::AttributeValue::S("after-follower-expiry".into()),
            )
            .send();
        tokio::pin!(write);
        let held = tokio::time::timeout(Duration::from_secs(2), &mut write).await;
        let blocked = fixture.ordinary.blocked_payloads.load(Ordering::SeqCst);
        fixture
            .ordinary
            .withhold_payloads
            .store(false, Ordering::SeqCst);
        assert!(
            held.is_err(),
            "expired follower allowed a response without bucket proof: {held:?}"
        );
        assert!(
            blocked > 0,
            "test did not withhold a native bundle/LTX publication"
        );
        tokio::time::timeout(Duration::from_secs(20), &mut write)
            .await
            .unwrap()
            .unwrap();
        fixture.assert_value("after-follower-expiry").await;
        let after = fixture.lease(SOURCE).await;
        assert_eq!(after.body["generation"], before.body["generation"]);
        assert_eq!(after.body["lineage"], before.body["lineage"]);
        fixture.stop().await;
    });
}

#[test]
fn lease_write_outage_fences_the_native_node_and_closes_ingress() {
    let (host, core) = runtimes();
    host.block_on(async {
        let mut fixture = Fixture::start(host.handle(), core.handle()).await;
        let before = fixture.lease(SOURCE).await;
        fixture.safety.refuse_writes.store(true, Ordering::SeqCst);
        let outcome = tokio::time::timeout(
            Duration::from_secs(20),
            fixture.source.node.as_mut().unwrap().finished(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            matches!(
                outcome,
                NodeOutcome::Fenced {
                    reason: FenceReason::NodeLeaseExpired,
                    ..
                }
            ),
            "{outcome:?}"
        );
        assert!(fixture.safety.refused.load(Ordering::SeqCst) > 0);
        let after = fixture.lease(SOURCE).await;
        assert_eq!(after.etag, before.etag);
        assert_eq!(after.log(), before.log());
        assert!(after.expires() <= cellar::ownership_store::now_ms());
        assert!(
            fixture
                .source
                .client
                .query::<beyonddb::ListTables>(
                    &beyonddb::account_target(ACCOUNT).unwrap(),
                    None,
                    beyonddb::Json(beyonddb::ListTablesInput {
                        limit: 1,
                        exclusive_start: None,
                        live_only: false
                    }),
                )
                .await
                .is_err()
        );
        assert!(fixture.http.sdk.list_tables().send().await.is_err());
        fixture.http.stop().await;
        drop(fixture.source.node.take());
        fixture.front.stop().await;
        fixture.inspection.close().await;
    });
}
