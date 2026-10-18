//! Durable account discovery before coordinator admission can acknowledge.
use crate::{
    payload_gate,
    residency_fixture::{ACCOUNT, Fixture},
};
use bdb_cellar_runtime::{
    MutationIdentity,
    client::{CellClient, Invocation, InvocationTransport},
    codec::{BoundedEncoder, WireValue},
    identity::{CellTarget, RequestId},
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

fn runtimes(threads: usize) -> (tokio::runtime::Runtime, tokio::runtime::Runtime) {
    let make = |threads| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()
            .unwrap()
    };
    (make(threads), make(1))
}
fn mutation() -> MutationIdentity {
    let now = crate::native_peer::now_ms();
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
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
async fn registration(
    client: &CellClient,
    account: &str,
    shard: u32,
) -> bdb_cellar_runtime::client::Observed<Json<bool>> {
    client
        .query::<ReadCoordinatorRegistration>(
            &account_target(account).unwrap(),
            None,
            Json(RegisterCoordinatorShardInput {
                account_id: account.into(),
                shard,
            }),
        )
        .await
        .unwrap()
}
async fn warm(fixture: &Fixture, key: &[u8]) {
    let target = coordinator_target(ACCOUNT, key).unwrap();
    assert!(
        fixture
            .client
            .query::<ReadPendingCrossCellTransactions>(
                &target,
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
    assert!(
        !registration(&fixture.client, ACCOUNT, shard(key))
            .await
            .output
            .0
    );
}
struct Observe {
    inner: Arc<dyn InvocationTransport>,
    account: CellTarget,
    publications: Arc<AtomicUsize>,
}
impl InvocationTransport for Observe {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            if invocation.target == self.account && invocation.mutation.is_some() {
                assert_eq!(
                    invocation.operation,
                    <RegisterCoordinatorShards as bdb_cellar_runtime::registry::Command>::ID
                );
                self.publications.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.call(invocation, request).await
        })
    }
}

#[test]
fn cancellation_during_registration_publication_keeps_discovery_and_waits_for_ack() {
    let (host, core) = runtimes(4);
    host.block_on(async {
        let control = Arc::new(payload_gate::Control::default());
        let mut fixture =
            Fixture::with_bucket(host.handle(), core.handle(), 1, 12, |mut bucket| {
                bucket.store = Arc::new(payload_gate::Store {
                    inner: bucket.store.clone(),
                    control: control.clone(),
                });
                bucket
            })
            .await;
        let first_key = b"canceled-registration";
        let second_key = (0..100)
            .map(|index| format!("healthy-registration-{index}"))
            .find(|key| shard(key.as_bytes()) != shard(first_key))
            .unwrap();
        // Actual SDK activation creates the coordinator scopes but does not
        // publish account discovery. Both rows must still be absent.
        for key in [first_key.as_slice(), second_key.as_bytes()] {
            warm(&fixture, key).await;
        }
        let account = account_target(ACCOUNT).unwrap();
        let original = fixture.cell_identity(&account).await;
        let held = control.hold(format!("cells/{}/ltx/", original.scope));
        let publications = Arc::new(AtomicUsize::new(0));
        let client = fixture.client.map_transport(|inner| {
            Arc::new(Observe {
                inner,
                account: account.clone(),
                publications: publications.clone(),
            })
        });
        let provisioner = crate::native_peer::provisioner(&client);
        let spawn = |key: Vec<u8>| {
            let client = client.clone();
            let provisioner = provisioner.clone();
            tokio::spawn(async move { provisioner.ensure(&client, ACCOUNT, &key).await })
        };
        let first = spawn(first_key.to_vec());
        tokio::time::timeout(Duration::from_secs(2), held.entered())
            .await
            .unwrap();
        assert_eq!(publications.load(Ordering::SeqCst), 1);
        assert!(
            !first.is_finished(),
            "account LTX publication has no SDK durability acknowledgement yet"
        );
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let second = spawn(second_key.as_bytes().to_vec());
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !second.is_finished(),
            "later admission cannot bypass unpublished account state"
        );
        held.release();
        tokio::time::timeout(Duration::from_secs(10), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        for key in [first_key.as_slice(), second_key.as_bytes()] {
            assert!(
                registration(&fixture.client, ACCOUNT, shard(key))
                    .await
                    .output
                    .0
            );
        }
        fixture.replace().await;
        let actual = fixture.cell_identity(&account).await;
        assert_eq!(actual.scope, original.scope);
        assert!(actual.epoch > original.epoch);
        for key in [first_key.as_slice(), second_key.as_bytes()] {
            assert!(
                registration(&fixture.client, ACCOUNT, shard(key))
                    .await
                    .output
                    .0
            );
            let target = coordinator_target(ACCOUNT, key).unwrap();
            assert!(
                fixture
                    .client
                    .query::<ReadPendingCrossCellTransactions>(
                        &target,
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
        }
        fixture.shutdown().await;
    });
}

#[test]
fn concurrent_coordinator_admissions_share_durable_account_registration() {
    let (host, core) = runtimes(4);
    host.block_on(async {
        let mut fixture = Fixture::new(host.handle(), core.handle(), 1, 16).await;
        let mut unique = std::collections::BTreeSet::new();
        let keys = (0..100)
            .map(|index| format!("registration-batch-{index}"))
            .filter(|key| unique.insert(shard(key.as_bytes())))
            .take(4)
            .collect::<Vec<_>>();
        assert_eq!(keys.len(), 4);
        for key in &keys {
            warm(&fixture, key.as_bytes()).await;
        }
        let before = registration(&fixture.client, ACCOUNT, shard(keys[0].as_bytes()))
            .await
            .receipt
            .commit_sequence;
        let start = Arc::new(tokio::sync::Barrier::new(keys.len()));
        let provisioner = crate::native_peer::provisioner(&fixture.client);
        let mut tasks = Vec::new();
        for key in &keys {
            let client = fixture.client.clone();
            let provisioner = provisioner.clone();
            let start = start.clone();
            let key = key.clone();
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                provisioner.ensure(&client, ACCOUNT, key.as_bytes()).await
            }));
        }
        for task in tasks {
            tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        let commits = registration(&fixture.client, ACCOUNT, shard(keys[0].as_bytes()))
            .await
            .receipt
            .commit_sequence
            - before;
        eprintln!("four native coordinator admissions used {commits} durable account commands");
        assert!(commits > 0 && commits < keys.len() as u64);
        fixture.replace().await;
        for key in keys {
            assert!(
                registration(&fixture.client, ACCOUNT, shard(key.as_bytes()))
                    .await
                    .output
                    .0
            );
        }
        fixture.shutdown().await;
    });
}

#[test]
fn coordinator_registration_batch_enforces_scope_and_limits_without_partial_rows() {
    let (host, core) = runtimes(2);
    host.block_on(async {
        let mut fixture = Fixture::new(host.handle(), core.handle(), 1, 12).await;
        let account = account_target(ACCOUNT).unwrap();
        for (account_id, shards) in [
            (ACCOUNT, vec![]),
            (ACCOUNT, (128..193).collect()),
            (ACCOUNT, vec![1234, 4096]),
            ("other-account", vec![1234]),
        ] {
            assert!(
                fixture
                    .client
                    .command::<RegisterCoordinatorShards>(
                        &account,
                        mutation(),
                        Json(RegisterCoordinatorShardsInput {
                            account_id: account_id.into(),
                            shards
                        })
                    )
                    .await
                    .is_err()
            );
        }
        assert!(!registration(&fixture.client, ACCOUNT, 1234).await.output.0);
        // Retain the largest supported account encoding and all 64 entries.
        let maximal_account = "\0".repeat(128);
        let input = RegisterCoordinatorShardsInput {
            account_id: maximal_account.clone(),
            shards: vec![4095; 64],
        };
        let mut encoder = BoundedEncoder::new(4096).unwrap();
        Json(input.clone()).encode(&mut encoder).unwrap();
        fixture
            .provisioner
            .admit_account(&maximal_account)
            .await
            .unwrap();
        let target = account_target(&maximal_account).unwrap();
        fixture
            .client
            .command::<RegisterCoordinatorShards>(&target, mutation(), Json(input))
            .await
            .unwrap();
        assert!(
            registration(&fixture.client, &maximal_account, 4095)
                .await
                .output
                .0
        );
        let old = fixture.cell_identity(&target).await;
        fixture.replace().await;
        assert!(!registration(&fixture.client, ACCOUNT, 1234).await.output.0);
        assert!(
            registration(&fixture.client, &maximal_account, 4095)
                .await
                .output
                .0
        );
        let actual = fixture.cell_identity(&target).await;
        assert_eq!(actual.scope, old.scope);
        assert!(actual.epoch > old.epoch);
        fixture.shutdown().await;
    });
}
