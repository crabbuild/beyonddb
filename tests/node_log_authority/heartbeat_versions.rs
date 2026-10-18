//! Observe the SDK's single lease writer; no application-held ETag authority.
use super::*;

#[test]
fn completed_log_publication_needs_no_lease_readback() {
    let (host, core) = runtimes();
    host.block_on(async {
        let fixture = Fixture::start(host.handle(), core.handle()).await;
        let before = fixture.lease(SOURCE).await;
        let reads = fixture.safety.gets.load(Ordering::SeqCst);
        let conflicts = fixture.safety.conflicts.load(Ordering::SeqCst);
        let after = fixture
            .wait_lease(SOURCE, |lease| lease.expires() >= before.expires() + 5_000)
            .await;
        assert_same_session(&before, &after);
        assert_eq!(
            fixture.safety.gets.load(Ordering::SeqCst),
            reads,
            "successful renewals unexpectedly read back authority"
        );
        assert_eq!(
            fixture.safety.conflicts.load(Ordering::SeqCst),
            conflicts,
            "folded log publication left a stale CAS guard"
        );
        fixture.write("after-log-publication").await;
        fixture.stop().await;
    });
}

#[test]
fn lost_applied_cas_reply_reconciles_through_authoritative_readback() {
    let (host, core) = runtimes();
    host.block_on(async {
        let fixture = Fixture::start(host.handle(), core.handle()).await;
        let before = fixture.lease(SOURCE).await;
        let reads = fixture.safety.gets.load(Ordering::SeqCst);
        fixture.safety.lose_reply.store(true, Ordering::SeqCst);
        let after = fixture
            .wait_lease(SOURCE, |lease| {
                fixture.safety.lost_replies.load(Ordering::SeqCst) == 1
                    && fixture.safety.gets.load(Ordering::SeqCst) > reads
                    && lease.expires() >= before.expires() + 5_000
            })
            .await;
        assert_same_session(&before, &after);
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
        assert_eq!(fixture.safety.lost_replies.load(Ordering::SeqCst), 1);
        fixture.write("after-lost-lease-reply").await;
        fixture.stop().await;
    });
}

#[test]
fn slow_successful_cas_keeps_the_log_and_renews_without_conflicts() {
    let (host, core) = runtimes();
    host.block_on(async {
        let fixture = Fixture::start(host.handle(), core.handle()).await;
        let before = fixture.lease(SOURCE).await;
        let writes = fixture.safety.applied.load(Ordering::SeqCst);
        let reads = fixture.safety.gets.load(Ordering::SeqCst);
        let conflicts = fixture.safety.conflicts.load(Ordering::SeqCst);
        // Native renewal attempts have a 2.5-second cap. Keep the injected
        // successful tail below that bound; longer tails require readback.
        fixture.safety.delay_ms.store(1_500, Ordering::SeqCst);
        let after = fixture
            .wait_lease(SOURCE, |lease| {
                fixture.safety.applied.load(Ordering::SeqCst) >= writes + 2
                    && lease.expires() > before.expires()
            })
            .await;
        fixture.safety.delay_ms.store(0, Ordering::SeqCst);
        assert_same_session(&before, &after);
        assert_eq!(fixture.safety.gets.load(Ordering::SeqCst), reads);
        assert_eq!(fixture.safety.conflicts.load(Ordering::SeqCst), conflicts);
        fixture.write("after-slow-renewals").await;
        fixture.stop().await;
    });
}

#[test]
fn delayed_capacity_read_cannot_regress_the_next_lease_guard() {
    let (host, core) = runtimes();
    host.block_on(async {
        let fixture = Fixture::start(host.handle(), core.handle()).await;
        let hold = fixture.ordinary.hold_next_read();
        tokio::time::timeout(Duration::from_secs(40), hold.entered.cancelled())
            .await
            .unwrap();
        let before = fixture.lease(SOURCE).await;
        let renewed = fixture
            .wait_lease(SOURCE, |lease| lease.expires() > before.expires())
            .await;
        let conflicts = fixture.safety.conflicts.load(Ordering::SeqCst);
        hold.release.cancel();
        let after = fixture
            .wait_lease(SOURCE, |lease| lease.expires() > renewed.expires())
            .await;
        assert_same_session(&before, &after);
        assert_eq!(
            fixture.safety.conflicts.load(Ordering::SeqCst),
            conflicts,
            "late advisory response regressed the lease writer guard"
        );
        fixture.write("after-stale-capacity-response").await;
        fixture.stop().await;
    });
}
