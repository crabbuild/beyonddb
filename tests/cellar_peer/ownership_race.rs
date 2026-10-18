//! Signed reads retain SDK fencing across real ownership changes.
#[path = "ownership_race/authority.rs"]
mod authority;
use crate::{native_peer::*, runtimes};
use authority::{Boundary, Control, Store};
use aws_sdk_dynamodb::types::AttributeValue as V;
use beyonddb::*;
use cellar::{
    bucket::Bucket,
    sdk::{NodeOutcome, ShutdownMode},
};
use cellar_runtime::{app::CompiledApplication, identity::CellTarget};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
const ACCOUNT: &str = "123456789012";
const ACCESS: &str = "AKIAOWNERSHIPRACE";
type Item = HashMap<String, V>;

struct Workload {
    files: tempfile::TempDir,
    bucket: Bucket,
    app: Arc<CompiledApplication>,
    observed: Observations,
    inspection: Inspection,
    data: Vec<(CellTarget, Item)>,
}
impl Workload {
    async fn new(host: &tokio::runtime::Handle, core: &tokio::runtime::Handle) -> (Self, Peer) {
        let files = tempfile::tempdir().unwrap();
        let bucket = Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let app = application();
        let observed = Arc::new(Mutex::new(Default::default()));
        let seed = Peer::start(
            app.clone(),
            &files.path().join("ownership-seed"),
            bucket.clone(),
            observed.clone(),
            PeerPolicy {
                max_resident: 8,
                ..false.into()
            },
            host,
            core,
        )
        .await;
        put_credential(&seed.client, ACCESS, ACCOUNT).await;
        CellAuthorizationStore::new(seed.client.clone()).put_user_policy(ACCOUNT,"network-user","tables", &serde_json::json!({
            "Version":"2012-10-17", "Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"arn:aws:dynamodb:us-east-1:123456789012:table/Residency*"}]
        }).to_string()).await.unwrap();
        let (_, data) = table(&seed.client, ACCOUNT, "Residency", false).await;
        assert_eq!(data.len(), 2);
        assert_ne!(data[0].0, data[1].0);
        let data = data
            .into_iter()
            .map(|(target, item)| {
                (
                    target,
                    item.into_iter()
                        .map(|(key, value)| {
                            (
                                key,
                                match value {
                                    extenddb_core::types::AttributeValue::S(value) => V::S(value),
                                    extenddb_core::types::AttributeValue::N(value) => V::N(value),
                                    _ => panic!("seed attribute changed type"),
                                },
                            )
                        })
                        .collect(),
                )
            })
            .collect();
        let inspection = Inspection::new(bucket.clone(), host);
        (
            Self {
                files,
                bucket,
                app,
                observed,
                inspection,
                data,
            },
            seed,
        )
    }
    async fn peer(
        &self,
        name: &str,
        control: Option<Arc<Control>>,
        host: &tokio::runtime::Handle,
        core: &tokio::runtime::Handle,
    ) -> Peer {
        let mut bucket = self.bucket.clone();
        if let Some(control) = control {
            bucket.store = Arc::new(Store {
                inner: bucket.store.clone(),
                control: control.clone(),
            });
            bucket.cas_store = Arc::new(Store {
                inner: bucket.cas_store.clone(),
                control,
            });
        }
        Peer::start(
            self.app.clone(),
            &self.files.path().join(name),
            bucket,
            self.observed.clone(),
            PeerPolicy {
                max_resident: 8,
                ..false.into()
            },
            host,
            core,
        )
        .await
    }
    async fn exact(&self, sdk: &aws_sdk_dynamodb::Client) {
        for (_, expected) in &self.data {
            assert_eq!(read(sdk, expected).await, *expected);
        }
    }
    async fn restored(
        self,
        mut owners: Vec<(&str, Peer)>,
        old: cellar::sdk::CellIdentity,
        host: &tokio::runtime::Handle,
        core: &tokio::runtime::Handle,
    ) {
        for (name, owner) in &mut owners {
            owner.stop().await;
            std::fs::remove_dir_all(self.files.path().join(*name)).unwrap();
        }
        let next_name = "ownership-next";
        let mut next = self.peer(next_name, None, host, core).await;
        for (name, _) in &owners {
            self.inspection.departed_node(name, next_name).await;
        }
        let http = Http::start(&next.client, ACCESS).await;
        self.exact(&http.sdk).await;
        let actual = identity(&self.observed, next_name, &self.data[0].0).unwrap();
        assert_eq!(actual.scope, old.scope);
        assert!(actual.epoch > old.epoch);
        let owner = self.inspection.owner(&actual.scope).await;
        assert_eq!(owner.node.as_deref(), Some(next_name));
        assert_eq!(owner.epoch, actual.epoch);
        http.stop().await;
        next.stop().await;
        self.inspection.close().await;
    }
}
async fn read(sdk: &aws_sdk_dynamodb::Client, item: &Item) -> Item {
    sdk.get_item()
        .table_name("Residency")
        .key("id", item["id"].clone())
        .consistent_read(true)
        .send()
        .await
        .unwrap()
        .item
        .unwrap()
}
struct Observe {
    inner: Arc<dyn cellar_runtime::client::InvocationTransport>,
}
impl cellar_runtime::client::InvocationTransport for Observe {
    fn call<'a>(
        &'a self,
        invocation: &'a cellar_runtime::client::Invocation<'a>,
        request: http::Request<http_body_util::Full<bytes::Bytes>>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = cellar::sdk::IngressResponse> + Send + 'a>,
    > {
        Box::pin(async move {
            use http_body_util::BodyExt;
            let response = self.inner.call(invocation, request).await;
            if response.status().is_success() {
                return response;
            }
            let (parts, body) = response.into_parts();
            let bytes = body.collect().await.unwrap().to_bytes();
            eprintln!(
                "owner race SDK response {} mutation={:?} operation={} namespace={:?} target={} body={}",
                parts.status,
                invocation.mutation,
                invocation.operation,
                invocation.target.namespace(),
                cellar_runtime::application::cell_name(&invocation.target),
                String::from_utf8_lossy(&bytes)
            );
            http::Response::from_parts(
                parts,
                http_body_util::Full::new(bytes)
                    .map_err(|never| -> std::io::Error { match never {} })
                    .boxed_unsync(),
            )
        })
    }
}
async fn handoff(peer: &mut Peer) {
    // Use the public SDK lifecycle, including its final durability proof and
    // conditional owner release. No fixture writes ownership or catalog state.
    let outcome = tokio::time::timeout(
        Duration::from_secs(45),
        peer.node.take().unwrap().shutdown(ShutdownMode::Handoff),
    )
    .await
    .unwrap()
    .unwrap();
    let NodeOutcome::Stopped(report) = outcome else {
        panic!("handoff failed: {outcome:?}");
    };
    assert_eq!(report.mode, ShutdownMode::Handoff);
    assert!(
        report.ingress_drained
            && report.application_work_drained
            && report.cells_drained
            && report.pending_dispatches == 0
            && report.pending_application_events == 0
            && report.log_quiesced
            && report.local_durability_closed
            && !report.deadline_elapsed
            && !report.drain_timed_out,
        "incomplete handoff: {report:?}"
    );
}

#[test]
fn signed_read_follows_real_sdk_winner_after_stale_lookup_or_conditional_claim() {
    let (host, core) = runtimes();
    host.block_on(async {
        for boundary in [Boundary::Lookup, Boundary::Claim] {
            let (mut fixture, mut seed) = Workload::new(host.handle(), core.handle()).await;
            let target = fixture.data[0].0.clone();
            let original = identity(&fixture.observed, "ownership-seed", &target).unwrap();
            let control = Arc::new(Control::default());
            // A sole donor releases its actual SDK record. Starting both
            // contenders afterwards prevents shutdown adoption from deciding
            // the winner before the provider race is armed.
            handoff(&mut seed).await;
            drop(seed);
            std::fs::remove_dir_all(fixture.files.path().join("ownership-seed")).unwrap();
            let released = fixture.inspection.owner(&original.scope).await;
            assert!(
                released.node.is_none(),
                "donor did not release: {released:?}"
            );
            let source = fixture
                .peer(SOURCE, Some(control.clone()), host.handle(), core.handle())
                .await;
            let front = fixture
                .peer(FRONT, None, host.handle(), core.handle())
                .await;
            fixture.inspection.ready_peers().await;
            let held = control.hold(&original.scope, boundary);
            let http = Http::start(&source.client, ACCESS).await;
            let job = {
                let sdk = http.sdk.clone();
                let item = fixture.data[0].1.clone();
                tokio::spawn(async move { read(&sdk, &item).await })
            };
            tokio::time::timeout(Duration::from_secs(10), held.entered())
                .await
                .unwrap();
            assert_eq!(held.etag(), released.etag);
            assert!(identity(&fixture.observed, SOURCE, &target).is_none());
            let before = front
                .client
                .query::<ReadPartitionState>(&target, None, Json(()))
                .await
                .unwrap();
            let winner = identity(&fixture.observed, FRONT, &target).unwrap();
            assert_eq!(winner.scope, original.scope);
            assert!(winner.epoch > original.epoch);
            let owner = fixture.inspection.owner(&winner.scope).await;
            assert_eq!(owner.node.as_deref(), Some(FRONT));
            assert_eq!(owner.epoch, winner.epoch);
            held.release();
            if boundary == Boundary::Claim {
                tokio::time::timeout(Duration::from_secs(10), held.rejected())
                    .await
                    .unwrap();
            }
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(10), job)
                    .await
                    .unwrap()
                    .unwrap(),
                fixture.data[0].1
            );
            let after = front
                .client
                .query::<ReadPartitionState>(&target, None, Json(()))
                .await
                .unwrap();
            assert_eq!(
                after.receipt.commit_sequence,
                before.receipt.commit_sequence
            );
            assert_eq!(fixture.inspection.owner(&winner.scope).await, owner);
            assert!(identity(&fixture.observed, SOURCE, &target).is_none());
            fixture.exact(&http.sdk).await;
            fixture.data[0].1.insert("value".into(), V::N("2".into()));
            http.sdk
                .put_item()
                .table_name("Residency")
                .set_item(Some(fixture.data[0].1.clone()))
                .send()
                .await
                .unwrap();
            fixture.exact(&http.sdk).await;
            http.stop().await;
            drop(held);
            fixture
                .restored(
                    vec![(SOURCE, source), (FRONT, front)],
                    winner,
                    host.handle(),
                    core.handle(),
                )
                .await;
        }
    });
}
#[test]
fn signed_read_recovers_when_real_sdk_owner_hands_off_after_lookup() {
    let (host, core) = runtimes();
    host.block_on(async {
        let (fixture, mut seed) = Workload::new(host.handle(), core.handle()).await;
        let target = fixture.data[0].0.clone();
        let original = identity(&fixture.observed, "ownership-seed", &target).unwrap();
        let before = fixture.inspection.owner(&original.scope).await;
        let control = Arc::new(Control::default());
        let front = fixture
            .peer(FRONT, Some(control.clone()), host.handle(), core.handle())
            .await;
        let held = control.hold(&original.scope, Boundary::Lookup);
        let observed_client = front
            .client
            .map_transport(|inner| Arc::new(Observe { inner }));
        let http = Http::start(&observed_client, ACCESS).await;
        let job = {
            let sdk = http.sdk.clone();
            let item = fixture.data[0].1.clone();
            tokio::spawn(async move { read(&sdk, &item).await })
        };
        tokio::time::timeout(Duration::from_secs(10), held.entered())
            .await
            .unwrap();
        assert_eq!(held.etag(), before.etag);
        handoff(&mut seed).await;
        // Retire the opaque proxy with its upstream. Leaving it listening after
        // the SDK has stopped fabricates a truncated peer response instead of
        // the real donor's closed endpoint.
        drop(seed);
        std::fs::remove_dir_all(fixture.files.path().join("ownership-seed")).unwrap();
        let handed_off = fixture.inspection.owner(&original.scope).await;
        assert_ne!(
            handed_off.node.as_deref(),
            Some("ownership-seed"),
            "donor retained its authority: {handed_off:?}"
        );
        held.release();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), job)
                .await
                .unwrap()
                .unwrap(),
            fixture.data[0].1
        );
        fixture.exact(&http.sdk).await;
        let actual = identity(&fixture.observed, FRONT, &target).unwrap();
        assert_eq!(actual.scope, original.scope);
        assert!(actual.epoch > original.epoch);
        let owner = fixture.inspection.owner(&actual.scope).await;
        assert_eq!(owner.node.as_deref(), Some(FRONT));
        assert_eq!(owner.epoch, actual.epoch);
        http.stop().await;
        drop(held);
        fixture
            .restored(vec![(FRONT, front)], actual, host.handle(), core.handle())
            .await;
    });
}
