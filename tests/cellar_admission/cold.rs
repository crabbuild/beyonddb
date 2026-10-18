use crate::{
    native_peer::{self, Http},
    payload,
    residency_fixture::{ACCOUNT, Fixture},
};
use aws_sdk_dynamodb::types::{AttributeValue as V, Put, TransactWriteItem};
use bdb_cellar_runtime::{
    client::{Invocation, InvocationTransport},
    identity::CellTarget,
};
use beyonddb::*;
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use http_body_util::Full;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

#[derive(Default)]
struct Calls {
    count: AtomicUsize,
    changed: Notify,
}
impl Calls {
    async fn wait(&self, count: usize) {
        loop {
            let changed = self.changed.notified();
            if self.count.load(Ordering::SeqCst) >= count {
                break;
            }
            changed.await;
        }
    }
}
struct Observe {
    inner: Arc<dyn InvocationTransport>,
    targets: Vec<CellTarget>,
    calls: Arc<Calls>,
}
impl InvocationTransport for Observe {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            if self.targets.contains(&invocation.target) {
                self.calls.count.fetch_add(1, Ordering::SeqCst);
                self.calls.changed.notify_one();
            }
            self.inner.call(invocation, request).await
        })
    }
}
fn runtimes() -> (tokio::runtime::Runtime, tokio::runtime::Runtime) {
    let make = |threads| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()
            .unwrap()
    };
    (make(4), make(1))
}
async fn fixture(
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
    resident: usize,
) -> (Fixture, Arc<payload::Control>, String) {
    let control = Arc::new(payload::Control::default());
    let fixture = Fixture::with_bucket(host, core, 1, resident, |mut bucket| {
        bucket.store = Arc::new(payload::Store {
            inner: bucket.store.clone(),
            control: control.clone(),
        });
        bucket
    })
    .await;
    let target = coordinator_target(ACCOUNT, b"class-discovery-only").unwrap();
    let app = native_peer::application();
    let module = app
        .cell_types()
        .iter()
        .find(|kind| kind.namespace() == target.namespace())
        .unwrap()
        .module()
        .to_owned();
    (fixture, control, module)
}
async fn registered(fixture: &Fixture, target: &CellTarget) -> bool {
    fixture
        .client
        .query::<ReadCoordinatorRegistration>(
            &account_target(ACCOUNT).unwrap(),
            None,
            Json(RegisterCoordinatorShardInput {
                account_id: ACCOUNT.into(),
                shard: u32::from_be_bytes(target.partition().try_into().unwrap()),
            }),
        )
        .await
        .unwrap()
        .output
        .0
}
fn write(
    sdk: &aws_sdk_dynamodb::Client,
    token: &str,
    id: &str,
    value: i32,
) -> aws_sdk_dynamodb::operation::transact_write_items::builders::TransactWriteItemsFluentBuilder {
    sdk.transact_write_items()
        .client_request_token(token)
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("Residency")
                        .item("id", V::S(id.into()))
                        .item("value", V::N(value.to_string()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
}
async fn value(sdk: &aws_sdk_dynamodb::Client, id: &str) -> V {
    sdk.get_item()
        .table_name("Residency")
        .key("id", V::S(id.into()))
        .consistent_read(true)
        .send()
        .await
        .unwrap()
        .item
        .unwrap()
        .remove("value")
        .unwrap()
}
#[test]
fn sdk_concurrent_token_replay_shares_one_cold_publication_and_survives_restore() {
    let (host, core) = runtimes();
    host.block_on(async {
        let (mut fixture, control, module) = fixture(host.handle(), core.handle(), 8).await;
        let token = "same-cold-coordinator";
        let target = coordinator_target(ACCOUNT, token.as_bytes()).unwrap();
        assert!(!registered(&fixture, &target).await);
        let calls = Arc::new(Calls::default());
        let client = fixture.client.map_transport(|inner| {
            Arc::new(Observe {
                inner,
                targets: vec![target.clone()],
                calls: calls.clone(),
            })
        });
        let http = Http::start(&client, "AKIAIOSFODNN7EXAMPLE").await;
        let held = control.hold(&module);
        let first = tokio::spawn(write(&http.sdk, token, "same-cold", 7).send());
        tokio::time::timeout(Duration::from_secs(2), held.scopes(1))
            .await
            .unwrap();
        assert_eq!(calls.count.load(Ordering::SeqCst), 1);
        let second = tokio::spawn(write(&http.sdk, token, "same-cold", 7).send());
        tokio::time::timeout(Duration::from_secs(2), calls.wait(2))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!first.is_finished() && !second.is_finished());
        assert_eq!(
            held.calls(),
            1,
            "duplicate cold initialization reached provider publication"
        );
        let old = fixture.cell_identity(&target).await;
        assert_eq!(held.observed(), [old.scope.clone()].into_iter().collect());
        held.release();
        for job in [first, second] {
            tokio::time::timeout(Duration::from_secs(10), job)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        drop(held);
        assert!(registered(&fixture, &target).await);
        assert_eq!(value(&http.sdk, "same-cold").await, V::N("7".into()));
        http.sdk
            .put_item()
            .table_name("Residency")
            .item("id", V::S("same-cold".into()))
            .item("value", V::N("8".into()))
            .send()
            .await
            .unwrap();
        http.stop().await;
        fixture.replace().await;
        write(&fixture.sdk, token, "same-cold", 7)
            .send()
            .await
            .unwrap();
        assert!(registered(&fixture, &target).await);
        let actual = fixture.cell_identity(&target).await;
        assert_eq!(actual.scope, old.scope);
        assert!(actual.epoch > old.epoch);
        assert_eq!(value(&fixture.sdk, "same-cold").await, V::N("8".into()));
        fixture.shutdown().await;
    });
}
#[test]
fn sdk_distinct_cold_coordinators_overlap_and_retain_durable_registration() {
    let (host, core) = runtimes();
    host.block_on(async {
        let (mut fixture, control, module) = fixture(host.handle(), core.handle(), 16).await;
        let first = "cold-parallel-0".to_owned();
        let first_target = coordinator_target(ACCOUNT, first.as_bytes()).unwrap();
        let second = (1..100)
            .map(|n| format!("cold-parallel-{n}"))
            .find(|token| coordinator_target(ACCOUNT, token.as_bytes()).unwrap() != first_target)
            .unwrap();
        let targets = [
            first_target,
            coordinator_target(ACCOUNT, second.as_bytes()).unwrap(),
        ];
        for target in &targets {
            assert!(!registered(&fixture, target).await);
        }
        let held = control.hold(&module);
        let jobs = [first.as_str(), second.as_str()]
            .into_iter()
            .enumerate()
            .map(|(index, token)| {
                tokio::spawn(
                    write(&fixture.sdk, token, &format!("cold-{index}"), index as i32).send(),
                )
            })
            .collect::<Vec<_>>();
        let overlap = tokio::time::timeout(Duration::from_secs(1), held.scopes(2))
            .await
            .is_ok();
        held.release();
        for job in jobs {
            tokio::time::timeout(Duration::from_secs(10), job)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        assert!(
            overlap,
            "independent cold coordinator publications were serialized"
        );
        let mut previous = Vec::new();
        for target in &targets {
            assert!(registered(&fixture, target).await);
            previous.push(fixture.cell_identity(target).await);
        }
        assert_eq!(
            held.observed(),
            previous
                .iter()
                .map(|identity| identity.scope.clone())
                .collect()
        );
        drop(held);
        fixture.replace().await;
        for (index, (target, old)) in targets.iter().zip(previous).enumerate() {
            assert!(registered(&fixture, target).await);
            fixture
                .client
                .query::<ReadPendingCrossCellTransactions>(
                    target,
                    None,
                    Json(ReadPendingCrossCellTransactionsInput {
                        after: None,
                        limit: 1,
                    }),
                )
                .await
                .unwrap();
            let actual = fixture.cell_identity(target).await;
            assert_eq!(actual.scope, old.scope);
            assert!(actual.epoch > old.epoch);
            assert_eq!(
                value(&fixture.sdk, &format!("cold-{index}")).await,
                V::N(index.to_string())
            );
        }
        fixture.shutdown().await;
    });
}
