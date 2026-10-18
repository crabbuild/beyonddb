//! A blocked ordinary log-discovery read cannot starve the lease safety lane.
use super::*;

#[test]
fn stalled_log_discovery_read_does_not_block_native_heartbeats() {
    let (host, core) = runtimes();
    host.block_on(async {
        let fixture = Fixture::start(host.handle(), core.handle()).await;
        let hold = fixture.ordinary.hold_next_read();
        tokio::time::timeout(Duration::from_secs(40), hold.entered.cancelled())
            .await
            .unwrap();
        let before = fixture.lease(SOURCE).await;
        // Hold the captured old record past its original expiry. Multiple
        // successful heartbeats must run while this response remains blocked.
        let renewed = fixture
            .wait_lease(SOURCE, |lease| {
                cellar::ownership_store::now_ms() > before.expires()
                    && lease.expires() > cellar::ownership_store::now_ms()
            })
            .await;
        assert_same_session(&before, &renewed);
        assert!(
            fixture
                .source
                .node
                .as_ref()
                .unwrap()
                .is_ready()
                .await
                .unwrap()
        );
        fixture.write("while-log-read-stalled").await;
        let conflicts = fixture.safety.conflicts.load(Ordering::SeqCst);
        hold.release.cancel();
        let after = fixture
            .wait_lease(SOURCE, |lease| lease.expires() > renewed.expires())
            .await;
        assert_same_session(&before, &after);
        assert_eq!(fixture.safety.conflicts.load(Ordering::SeqCst), conflicts);
        fixture.assert_value("while-log-read-stalled").await;
        fixture.stop().await;
    });
}
