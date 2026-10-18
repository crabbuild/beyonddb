use crate::fixture::{ACCOUNT, Fixture, runtimes};
use beyonddb::*;
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use cellar_runtime::{
    client::{CellClient, Invocation, InvocationAdmission, InvocationTransport},
    identity::CellTarget,
};
use http_body_util::{BodyExt, Full};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Policy {
    account: CellTarget,
    refuse: bool,
    entered: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}
impl InvocationAdmission for Policy {
    fn check<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
    ) -> Pin<Box<dyn Future<Output = cellar_runtime::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if invocation.target == self.account && invocation.mutation.is_some() {
                if self.refuse {
                    return Err(cellar_runtime::Error::Capacity(
                        "registration caller refused",
                    ));
                }
                self.entered.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            Ok(())
        })
    }
}
struct Transport {
    inner: Arc<dyn InvocationTransport>,
    calls: Arc<AtomicUsize>,
    lose: Option<Arc<AtomicBool>>,
}
impl InvocationTransport for Transport {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let response = self.inner.call(invocation, request).await;
            if invocation.mutation.is_some()
                && invocation.operation
                    == <RegisterCoordinatorShards as cellar_runtime::registry::Command>::ID
                && response.status().is_success()
                && let Some(lose) = &self.lose
                && !lose.swap(true, Ordering::SeqCst)
            {
                response.into_body().collect().await.unwrap();
                let body = Full::new(Bytes::new())
                    .map_err(|never| -> std::io::Error { match never {} })
                    .boxed_unsync();
                let mut response = http::Response::new(body);
                *response.status_mut() = http::StatusCode::SERVICE_UNAVAILABLE;
                response
            } else {
                response
            }
        })
    }
}
fn shard(key: &[u8]) -> u32 {
    u32::from_be_bytes(
        coordinator_target(ACCOUNT, key)
            .unwrap()
            .partition()
            .try_into()
            .unwrap(),
    )
}
async fn registered(client: &CellClient, key: &[u8]) -> bool {
    client
        .query::<ReadCoordinatorRegistration>(
            &account_target(ACCOUNT).unwrap(),
            None,
            Json(RegisterCoordinatorShardInput {
                account_id: ACCOUNT.into(),
                shard: shard(key),
            }),
        )
        .await
        .unwrap()
        .output
        .0
}
#[test]
fn registration_batches_keep_caller_dispatch_and_admission_isolated() {
    let (host, core) = runtimes();
    host.block_on(async {
        let fixture = Fixture::new(host.handle(), core.handle(), 1).await;
        let raw = fixture.client.clone();
        assert!(raw.shares_dispatch_context(&raw.clone()));
        let calls = Arc::new(AtomicUsize::new(0));
        let client = raw.map_transport(|inner| {
            Arc::new(Transport {
                inner,
                calls: calls.clone(),
                lose: None,
            })
        });
        assert!(!raw.shares_dispatch_context(&client));
        let denied = client.with_admission(Arc::new(Policy {
            account: account_target(ACCOUNT).unwrap(),
            refuse: true,
            entered: Arc::new(tokio::sync::Semaphore::new(0)),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
        }));
        assert!(!client.shares_dispatch_context(&denied));
        assert!(denied.shares_dispatch_context(&denied.clone()));
        let key = b"registration-allowed";
        let other = (0..u16::MAX)
            .map(|n| format!("registration-denied-{n}"))
            .find(|candidate| shard(candidate.as_bytes()) != shard(key))
            .unwrap();
        let admission = crate::native_peer::provisioner(&client);
        let (allowed, refused) = tokio::join!(
            admission.ensure(&client, ACCOUNT, key),
            admission.ensure(&denied, ACCOUNT, other.as_bytes())
        );
        allowed.unwrap();
        assert!(refused.is_err());
        // Two allowed lookups plus one accepted publication reach the shared
        // transport. The denied caller cannot publish through the allowed slot.
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(registered(&raw, key).await);
        assert!(!registered(&raw, other.as_bytes()).await);
        fixture.shutdown().await;
    });
}
#[test]
fn canceled_registration_keeps_discovery_but_grants_no_begin() {
    let (host, core) = runtimes();
    host.block_on(async {
        let fixture = Fixture::new(host.handle(), core.handle(), 1).await;
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let client = fixture.client.with_admission(Arc::new(Policy {
            account: account_target(ACCOUNT).unwrap(),
            refuse: false,
            entered: entered.clone(),
            release: release.clone(),
        }));
        let admission = crate::native_peer::provisioner(&client);
        let task = tokio::spawn(async move {
            admission
                .ensure(&client, ACCOUNT, b"registration-cancel")
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!registered(&fixture.client, b"registration-cancel").await);
        release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(10), async {
            while !registered(&fixture.client, b"registration-cancel").await {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let coordinator = coordinator_target(ACCOUNT, b"registration-cancel").unwrap();
        assert!(
            fixture
                .client
                .query::<ReadPendingCrossCellTransactions>(
                    &coordinator,
                    None,
                    Json(ReadPendingCrossCellTransactionsInput {
                        after: None,
                        limit: 1
                    })
                )
                .await
                .unwrap()
                .output
                .0
                .is_empty()
        );
        fixture.shutdown().await;
    });
}
#[test]
fn lost_registration_reply_requires_fresh_discovery_before_begin() {
    let (host, core) = runtimes();
    host.block_on(async {
        let fixture = Fixture::new(host.handle(), core.handle(), 1).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let lost = Arc::new(AtomicBool::new(false));
        let client = fixture.client.map_transport(|inner| {
            Arc::new(Transport {
                inner,
                calls: calls.clone(),
                lose: Some(lost.clone()),
            })
        });
        let admission = crate::native_peer::provisioner(&client);
        let key = b"registration-lost-reply";
        assert!(admission.ensure(&client, ACCOUNT, key).await.is_err());
        assert!(lost.load(Ordering::SeqCst));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(registered(&fixture.client, key).await);
        // The first attempt had no acknowledged publication and cannot grant
        // BEGIN. A later attempt proves discovery with a new SDK read.
        admission.ensure(&client, ACCOUNT, key).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(
            fixture
                .client
                .query::<ReadPendingCrossCellTransactions>(
                    &coordinator_target(ACCOUNT, key).unwrap(),
                    None,
                    Json(ReadPendingCrossCellTransactionsInput {
                        after: None,
                        limit: 1
                    }),
                )
                .await
                .unwrap()
                .output
                .0
                .is_empty()
        );
        fixture.shutdown().await;
    });
}
#[test]
fn concurrent_cold_registrations_publish_every_shard_before_admission_returns() {
    let (host, core) = runtimes();
    host.block_on(async {
        let fixture = Fixture::new(host.handle(), core.handle(), 1).await;
        // Exercise the production HTTP dispatch policy before native SDK
        // admission, with all 32 cold registrations still submitted together.
        let client = http_dispatch_client(fixture.client.clone());
        let admission = crate::native_peer::provisioner(&client);
        let mut shards = std::collections::BTreeSet::new();
        let keys = (0..u16::MAX)
            .map(|n| format!("registration-cold-{n}"))
            .filter(|key| shards.insert(shard(key.as_bytes())))
            .take(32)
            .collect::<Vec<_>>();
        assert_eq!(keys.len(), 32);
        let start = Arc::new(tokio::sync::Barrier::new(33));
        let mut tasks = tokio::task::JoinSet::new();
        for key in &keys {
            let client = client.clone();
            let admission = admission.clone();
            let start = start.clone();
            let key = key.clone();
            tasks.spawn(async move {
                start.wait().await;
                admission
                    .ensure(&client, ACCOUNT, key.as_bytes())
                    .await
                    .unwrap();
            });
        }
        start.wait().await;
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        for key in keys {
            assert!(registered(&fixture.client, key.as_bytes()).await);
        }
        fixture.shutdown().await;
    });
}
