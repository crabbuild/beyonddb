use crate::{
    native_peer, reads,
    residency_fixture::{ACCOUNT, Fixture, runtimes},
};
use beyonddb::*;
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use cellar_runtime::{
    client::{CellClient, Invocation, InvocationTransport},
    identity::CellTarget,
    registry::Query,
};
use http_body_util::Full;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
#[derive(Default)]
struct Calls {
    reads: AtomicUsize,
    mutations: AtomicUsize,
}
struct Observe {
    inner: Arc<dyn InvocationTransport>,
    account: CellTarget,
    calls: Arc<Calls>,
}
impl InvocationTransport for Observe {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            if invocation.target == self.account {
                if invocation.mutation.is_none() {
                    assert_eq!(invocation.operation, ReadCoordinatorRegistration::ID);
                    self.calls.reads.fetch_add(1, Ordering::SeqCst);
                } else {
                    self.calls.mutations.fetch_add(1, Ordering::SeqCst);
                }
            }
            self.inner.call(invocation, request).await
        })
    }
}
fn observed(client: &CellClient, calls: &Arc<Calls>) -> CellClient {
    client.map_transport(|inner| {
        Arc::new(Observe {
            inner,
            account: account_target(ACCOUNT).unwrap(),
            calls: calls.clone(),
        })
    })
}
fn pending() -> Json<ReadPendingCrossCellTransactionsInput> {
    Json(ReadPendingCrossCellTransactionsInput {
        after: None,
        limit: 1,
    })
}
async fn registration(client: &CellClient, coordinator: &CellTarget) -> u64 {
    let row = client
        .query::<ReadCoordinatorRegistration>(
            &account_target(ACCOUNT).unwrap(),
            None,
            Json(RegisterCoordinatorShardInput {
                account_id: ACCOUNT.into(),
                shard: u32::from_be_bytes(coordinator.partition().try_into().unwrap()),
            }),
        )
        .await
        .unwrap();
    assert!(row.output.0);
    row.receipt.commit_sequence
}
#[test]
fn sdk_registered_admission_reads_canonical_state_without_hot_cell_provider_io() {
    let (host, core) = runtimes();
    host.block_on(async {
        let io = Arc::new(reads::Reads::default());
        let mut fixture = Fixture::with_bucket(host.handle(), core.handle(), 1, 8, |mut bucket| {
            bucket.store = Arc::new(reads::Store {
                inner: bucket.store.clone(),
                reads: io.clone(),
            });
            bucket
        })
        .await;
        let key = b"resident-admission";
        let coordinator = coordinator_target(ACCOUNT, key).unwrap();
        fixture
            .provisioner
            .ensure(&fixture.client, ACCOUNT, key)
            .await
            .unwrap();
        let before = fixture
            .client
            .query::<ReadPendingCrossCellTransactions>(&coordinator, None, pending())
            .await
            .unwrap();
        assert!(before.output.0.is_empty());
        let old = fixture.cell_identity(&coordinator).await;
        let account_old = fixture
            .cell_identity(&account_target(ACCOUNT).unwrap())
            .await;
        io.watch(&old.scope);
        let sequence = registration(&fixture.client, &coordinator).await;
        let calls = Arc::new(Calls::default());
        let client = observed(&fixture.client, &calls);
        io.reset();
        for _ in 0..4 {
            fixture
                .provisioner
                .ensure(&client, ACCOUNT, key)
                .await
                .unwrap();
        }
        assert_eq!(calls.reads.load(Ordering::SeqCst), 4);
        assert_eq!(calls.mutations.load(Ordering::SeqCst), 0);
        assert_eq!(io.coordinator.load(Ordering::SeqCst), 0);
        assert_eq!(
            io.cells.load(Ordering::SeqCst),
            0,
            "hot canonical SDK reads fetched managed-cell provider objects"
        );
        let fresh = CellInitialPartitionProvisioner::new(
            fixture.client.clone(),
            native_peer::application(),
        );
        fresh.ensure(&client, ACCOUNT, key).await.unwrap();
        assert_eq!(
            calls.reads.load(Ordering::SeqCst),
            5,
            "new provisioner bypassed canonical registration read"
        );
        assert_eq!(calls.mutations.load(Ordering::SeqCst), 0);
        assert_eq!(registration(&fixture.client, &coordinator).await, sequence);
        fixture.replace().await;
        let account_actual = fixture
            .cell_identity(&account_target(ACCOUNT).unwrap())
            .await;
        assert_eq!(account_actual.scope, account_old.scope);
        assert!(account_actual.epoch > account_old.epoch);
        let next = observed(&fixture.client, &calls);
        io.reset();
        fixture
            .provisioner
            .ensure(&next, ACCOUNT, key)
            .await
            .unwrap();
        assert_eq!(calls.reads.load(Ordering::SeqCst), 6);
        assert_eq!(calls.mutations.load(Ordering::SeqCst), 0);
        assert_eq!(
            io.coordinator.load(Ordering::SeqCst),
            0,
            "registration admission performed coordinator activation"
        );
        let restored = fixture
            .client
            .query::<ReadPendingCrossCellTransactions>(&coordinator, None, pending())
            .await
            .unwrap();
        assert!(
            io.coordinator.load(Ordering::SeqCst) > 0,
            "cold SDK coordinator restore fetched no provider objects"
        );
        assert_eq!(restored.output, before.output);
        assert_eq!(
            restored.receipt.commit_sequence,
            before.receipt.commit_sequence
        );
        let actual = fixture.cell_identity(&coordinator).await;
        assert_eq!(actual.scope, old.scope);
        assert!(actual.epoch > old.epoch);
        assert_eq!(registration(&fixture.client, &coordinator).await, sequence);
        for (_, expected) in &fixture.data {
            assert_eq!(
                fixture
                    .sdk
                    .get_item()
                    .table_name("Residency")
                    .key("id", expected["id"].clone())
                    .consistent_read(true)
                    .send()
                    .await
                    .unwrap()
                    .item
                    .as_ref(),
                Some(expected)
            );
        }
        fixture.shutdown().await;
    });
}
