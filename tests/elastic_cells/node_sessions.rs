//! Native shutdown proofs replace application-owned session retirement.
use crate::{
    lifecycle_store::LifecycleStore,
    native_peer::{self, FRONT, Http, Inspection, Observations, Peer, PeerPolicy, SOURCE},
    store::FaultStore,
};
use aws_sdk_dynamodb::types::AttributeValue as A;
use cellar::{
    bucket::Bucket,
    sdk::{NodeOutcome, ShutdownMode, ShutdownPolicy, ShutdownReport},
};
use std::{
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};

const ACCOUNT: &str = "123456789012";
const ACCESS: &str = "AKIANATIVESESSIONEXAMPLE";
const TABLE: &str = "SessionItems";

fn bounded_stop() -> ShutdownPolicy {
    ShutdownPolicy {
        total_ms: 3_000,
        token_wait_ms: 1_000,
        no_progress_ms: 1_000,
    }
}

struct Fixture {
    files: tempfile::TempDir,
    bucket: Bucket,
    observed: Observations,
    peer: Peer,
    front: Peer,
    http: Http,
    inspection: Inspection,
    payloads: Arc<FaultStore>,
    ordinary: Arc<LifecycleStore>,
    safety: Arc<LifecycleStore>,
}
impl Fixture {
    async fn start(
        policy: ShutdownPolicy,
        host: &tokio::runtime::Handle,
        core: &tokio::runtime::Handle,
    ) -> Self {
        let files = tempfile::tempdir().unwrap();
        let bucket = Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let ordinary = Arc::new(LifecycleStore::new(
            bucket.store.clone(),
            SOURCE,
            host.clone(),
        ));
        let payloads = Arc::new(FaultStore::new(ordinary.clone(), SOURCE));
        let safety = Arc::new(LifecycleStore::new(
            bucket.cas_store.clone(),
            SOURCE,
            host.clone(),
        ));
        let mut application_bucket = bucket.clone();
        application_bucket.store = payloads.clone();
        application_bucket.cas_store = safety.clone();
        let mut lease_bucket = bucket.clone();
        lease_bucket.store = safety.clone();
        lease_bucket.cas_store = safety.clone();
        let observed = Arc::new(Mutex::new(Default::default()));
        let peer = Peer::start(
            native_peer::application(),
            &files.path().join(SOURCE),
            application_bucket,
            observed.clone(),
            PeerPolicy {
                fleet_durability: true,
                lease_bucket: Some(lease_bucket),
                shutdown: policy,
                ..true.into()
            },
            host,
            core,
        )
        .await;
        native_peer::put_credential(&peer.client, ACCESS, ACCOUNT).await;
        native_peer::policy(&peer.client, ACCOUNT).await;
        let (_, ranges) = native_peer::table(&peer.client, ACCOUNT, TABLE, false).await;
        assert_eq!(ranges.len(), 2);
        let front = Peer::start(
            native_peer::application(),
            &files.path().join(FRONT),
            bucket.clone(),
            observed.clone(),
            PeerPolicy {
                fleet_durability: true,
                ..true.into()
            },
            host,
            core,
        )
        .await;
        let http = Http::start(&peer.client, ACCESS).await;
        let inspection = Inspection::new(bucket.clone(), host);
        inspection.ready_peers().await;
        let fixture = Self {
            files,
            bucket,
            observed,
            peer,
            front,
            http,
            inspection,
            payloads,
            ordinary,
            safety,
        };
        tokio::time::timeout(Duration::from_secs(40), async {
            while fixture.lease().await.0["log"]["state"] != "open" {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            fixture.lease().await.0["log"]["ensemble"],
            serde_json::json!([FRONT])
        );
        fixture.assert_rows().await;
        fixture
    }
    async fn lease(&self) -> (serde_json::Value, String) {
        self.inspection
            .domain
            .run(async {
                let (body, etag) = self
                    .bucket
                    .get(&format!("nodes/{SOURCE}.json"))
                    .await
                    .unwrap()
                    .unwrap();
                (serde_json::from_slice(&body).unwrap(), etag)
            })
            .await
            .unwrap()
    }
    async fn assert_rows(&self) {
        for key in ["first", "Ω-other"] {
            let output = self
                .http
                .sdk
                .get_item()
                .table_name(TABLE)
                .key("id", A::S(key.into()))
                .consistent_read(true)
                .send()
                .await
                .unwrap();
            assert_eq!(output.item().unwrap()["value"], A::N("1".into()));
        }
    }
    async fn shutdown(&mut self) -> ShutdownReport {
        let result = tokio::time::timeout(
            Duration::from_secs(8),
            self.peer
                .node
                .take()
                .unwrap()
                .shutdown(ShutdownMode::Preserve),
        )
        .await
        .unwrap()
        .unwrap();
        let NodeOutcome::Stopped(report) = result else {
            panic!("unexpected native outcome: {result:?}")
        };
        assert_eq!(
            self.files
                .path()
                .join(SOURCE)
                .join(".clean-reload.json")
                .exists(),
            report.clean_reload_prepared,
            "local certificate disagrees with the native drain proof: {report:?}"
        );
        assert!(
            self.peer
                .client
                .query::<beyonddb::ListTables>(
                    &beyonddb::account_target(ACCOUNT).unwrap(),
                    None,
                    beyonddb::Json(beyonddb::ListTablesInput {
                        limit: 1,
                        exclusive_start: None,
                        live_only: false
                    })
                )
                .await
                .is_err(),
            "stopped native ingress accepted new work"
        );
        report
    }
    async fn recover(&mut self, host: &tokio::runtime::Handle, core: &tokio::runtime::Handle) {
        let before = self.lease().await.0;
        // Unclean closure relies on ordinary expiry. Never edit the record or
        // claim a clean local certificate for an incomplete stop.
        tokio::time::timeout(Duration::from_secs(15), async {
            while self.lease().await.0["expires_ms"].as_u64().unwrap()
                > cellar::ownership_store::now_ms()
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        self.peer = Peer::start(
            native_peer::application(),
            &self.files.path().join(SOURCE),
            self.bucket.clone(),
            self.observed.clone(),
            PeerPolicy {
                fleet_durability: true,
                ..true.into()
            },
            host,
            core,
        )
        .await;
        assert_ne!(self.lease().await.0["generation"], before["generation"]);
        let http = Http::start(&self.peer.client, ACCESS).await;
        std::mem::replace(&mut self.http, http).stop().await;
        self.assert_rows().await;
    }
    async fn close(self) {
        let Self {
            files: _files,
            http,
            mut peer,
            mut front,
            inspection,
            ordinary,
            safety,
            ..
        } = self;
        http.stop().await;
        if peer.node.is_some() {
            peer.stop().await;
        }
        if front.node.is_some() {
            front.stop().await;
        }
        ordinary.close().await;
        safety.close().await;
        inspection.close().await;
    }
}

#[test]
fn graceful_shutdown_allows_immediate_same_node_restart() {
    let (host, core) = crate::runtimes();
    host.block_on(async {
        let mut fixture = Fixture::start(Default::default(), host.handle(), core.handle()).await;
        let before = fixture.lease().await.0;
        fixture.peer.stop().await;
        let sealed = fixture.lease().await.0;
        let certificate: serde_json::Value = serde_json::from_slice(
            &std::fs::read(fixture.files.path().join(SOURCE).join(".clean-reload.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(certificate["node"], SOURCE);
        assert_eq!(certificate["generation"], before["generation"]);
        assert_eq!(sealed["log"]["state"], "sealed");
        assert_eq!(sealed["generation"], before["generation"]);
        // Keep the certified directory and restart before the former lease
        // expires. The SDK alone consumes the exact-generation reload proof.
        assert!(sealed["expires_ms"].as_u64().unwrap() > cellar::ownership_store::now_ms());
        fixture.peer = Peer::start(
            native_peer::application(),
            &fixture.files.path().join(SOURCE),
            fixture.bucket.clone(),
            fixture.observed.clone(),
            PeerPolicy {
                fleet_durability: true,
                ..true.into()
            },
            host.handle(),
            core.handle(),
        )
        .await;
        let after = fixture.lease().await.0;
        assert!(
            sealed["expires_ms"].as_u64().unwrap() > cellar::ownership_store::now_ms(),
            "restart waited out the previous lease"
        );
        assert_ne!(after["generation"], before["generation"]);
        fixture.http.stop().await;
        fixture.http = Http::start(&fixture.peer.client, ACCESS).await;
        fixture.assert_rows().await;
        fixture.close().await;
    });
}

#[test]
fn failed_drain_cannot_claim_a_clean_reload() {
    let (host, core) = crate::runtimes();
    host.block_on(async {
        let mut fixture = Fixture::start(bounded_stop(), host.handle(), core.handle()).await;
        fixture
            .payloads
            .withhold_payloads
            .store(true, Ordering::SeqCst);
        fixture.front.network.cut(true).await;
        assert_eq!(
            fixture.front.node.take().unwrap().cancel().await.unwrap(),
            NodeOutcome::Cancelled
        );
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let expired = fixture
                    .inspection
                    .domain
                    .run(async {
                        let (body, _) = fixture
                            .bucket
                            .get(&format!("nodes/{FRONT}.json"))
                            .await
                            .unwrap()
                            .unwrap();
                        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["expires_ms"]
                            .as_u64()
                            .unwrap()
                            <= cellar::ownership_store::now_ms()
                    })
                    .await
                    .unwrap();
                if expired {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        let request = fixture
            .http
            .sdk
            .put_item()
            .table_name(TABLE)
            .item("id", A::S("pending".into()))
            .item("value", A::S("unproven".into()))
            .send();
        tokio::pin!(request);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    result = &mut request => panic!("unpublished write completed: {result:?}"),
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                if fixture.payloads.blocked_payloads.load(Ordering::SeqCst) > 0 {
                    break;
                }
            }
        })
        .await
        .unwrap();
        let report = fixture.shutdown().await;
        fixture
            .payloads
            .withhold_payloads
            .store(false, Ordering::SeqCst);
        assert!(
            report.drain_timed_out || report.deadline_elapsed,
            "{report:?}"
        );
        assert!(
            !report.application_work_drained || !report.cells_drained,
            "{report:?}"
        );
        assert!(!report.clean_reload_prepared, "{report:?}");
        assert!(
            tokio::time::timeout(Duration::from_secs(5), &mut request)
                .await
                .unwrap()
                .is_err()
        );
        fixture.recover(host.handle(), core.handle()).await;
        // The interrupted write is ambiguous. Verify the acknowledged rows and
        // that a later normal mutation can use the same key after recovery.
        fixture
            .http
            .sdk
            .put_item()
            .table_name(TABLE)
            .item("id", A::S("pending".into()))
            .item("value", A::S("recovered".into()))
            .send()
            .await
            .unwrap();
        let item = fixture
            .http
            .sdk
            .get_item()
            .table_name(TABLE)
            .key("id", A::S("pending".into()))
            .consistent_read(true)
            .send()
            .await
            .unwrap();
        assert_eq!(item.item().unwrap()["value"], A::S("recovered".into()));
        fixture.close().await;
    });
}

#[test]
fn late_native_heartbeat_cannot_grant_a_clean_reload() {
    let (host, core) = crate::runtimes();
    host.block_on(async {
        let mut fixture = Fixture::start(bounded_stop(), host.handle(), core.handle()).await;
        let before = fixture.lease().await.0;
        let gate = fixture.safety.hold_lease_writes();
        tokio::time::timeout(Duration::from_secs(8), gate.entered.cancelled())
            .await
            .unwrap();
        let report = fixture.shutdown().await;
        assert!(!report.clean_reload_prepared, "{report:?}");
        assert!(
            !report.log_quiesced || report.deadline_elapsed,
            "{report:?}"
        );
        // The provider, not the stopped SDK domain, now completes the exact
        // captured CAS. This is a real accepted write, not a fabricated lease.
        gate.finish().await;
        assert!(
            gate.applied.load(Ordering::SeqCst) > 0,
            "late native CAS never committed"
        );
        let after = fixture.lease().await;
        assert_eq!(after.0["generation"], before["generation"]);
        assert!(after.0["expires_ms"].as_u64().unwrap() > before["expires_ms"].as_u64().unwrap());
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            fixture.lease().await.1,
            after.1,
            "closed SDK resurrected lease work"
        );
        fixture.recover(host.handle(), core.handle()).await;
        fixture.close().await;
    });
}

#[test]
fn shutdown_storage_read_and_write_stalls_are_bounded() {
    let (host, core) = crate::runtimes();
    host.block_on(async {
        for stall_read in [true, false] {
            let mut fixture = Fixture::start(bounded_stop(), host.handle(), core.handle()).await;
            let gate = if stall_read {
                fixture.ordinary.hold_reads()
            } else {
                fixture.safety.hold_lease_writes()
            };
            let report = fixture.shutdown().await;
            assert!(
                gate.entered.is_cancelled(),
                "shutdown never entered the injected read={stall_read} fault"
            );
            assert!(
                !report.clean_reload_prepared,
                "read={stall_read}: {report:?}"
            );
            assert!(
                report.deadline_elapsed || !report.log_quiesced,
                "read={stall_read}: {report:?}"
            );
            gate.finish().await;
            fixture.recover(host.handle(), core.handle()).await;
            fixture.close().await;
        }
    });
}
