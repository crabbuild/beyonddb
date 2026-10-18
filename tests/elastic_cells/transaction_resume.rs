use crate::{local_node, transaction_fixture};
use beyonddb::*;
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use cellar_runtime::{
    client::{Invocation, InvocationTransport},
    identity::NamespaceId,
};
use http_body_util::Full;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
};
struct CoordinatorQueries {
    inner: Arc<dyn InvocationTransport>,
    namespace: NamespaceId,
    count: Arc<AtomicU64>,
}
impl InvocationTransport for CoordinatorQueries {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        if invocation.mutation.is_none() && invocation.target.namespace() == self.namespace {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.call(invocation, request)
    }
}
#[test]
fn small_transaction_resume_uses_one_coordinator_payload_snapshot() {
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    host.block_on(async {
        let account_id = "123456789012";
        let directory = tempfile::tempdir().unwrap();
        let now = i64::try_from(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()).unwrap();
        let clock = Arc::new(AtomicI64::new(now));
        let identity = |_: u8| local_node::mutation(&clock);
        let (node, base_client) = local_node::start(directory.path(), clock.clone(), host.handle(), core.handle()).await.unwrap();
        let queries = Arc::new(AtomicU64::new(0));
        let namespace = coordinator_target(account_id, &[199; 16]).unwrap().namespace();
        let client = base_client.map_transport(|inner| Arc::new(CoordinatorQueries { inner, namespace, count: queries.clone() }));
        let mut fixture = transaction_fixture::table(&client, "Resume").await;
        let storage = fixture.storage;
        let participants = &mut fixture.participants;
        // Operation indexes deliberately oppose participant order.
        for (position, (_, participant)) in participants.iter_mut().enumerate() {
            participant.operations[0].index = u8::try_from(1 - position).unwrap();
        }
        let transaction_id = [199; 16];
        let coordinator = coordinator_target(account_id, &transaction_id).unwrap();
    transaction_command!(
        client,
        BeginCrossCellTransaction,
        &coordinator,
        identity(199),
        Json(BeginCrossCellTransactionInput {
            account_id: account_id.into(),
            transaction_id,
            token: None,
            participants: participants.iter().map(|(_, p)| p.clone()).collect()
        })
    )
    .await
    .unwrap();
    let read = ReadCrossCellTransactionInput {
        account_id: account_id.into(),
        transaction_id,
        routing_key: transaction_id.to_vec(),
    };
    let initial = client
        .query::<ReadCoordinatorResume>(&coordinator, None, Json(read.clone()))
        .await
        .unwrap()
        .output
        .0;
    assert_eq!(
        initial.status.as_ref().unwrap().decision,
        CoordinatorDecision::Begin
    );
    assert_eq!(
        initial
            .participants
            .as_ref()
            .unwrap()
            .iter()
            .map(|p| p.participant.clone())
            .collect::<Vec<_>>(),
        participants
            .iter()
            .map(|(_, p)| p.clone())
            .collect::<Vec<_>>()
    );
    let missing = client
        .query::<ReadCoordinatorResume>(
            &coordinator,
            None,
            Json(ReadCrossCellTransactionInput {
                transaction_id: [198; 16],
                ..read.clone()
            }),
        )
        .await
        .unwrap()
        .output
        .0;
    assert!(missing.status.is_none() && missing.participants.is_none());
    assert!(
        client
            .query::<ReadCoordinatorResume>(
                &coordinator,
                None,
                Json(ReadCrossCellTransactionInput {
                    account_id: "999999999999".into(),
                    ..read.clone()
                })
            )
            .await
            .is_err()
    );
    let before = queries.load(Ordering::Relaxed);
    let decision = storage
        .resume_cross_cell_transaction(account_id, &transaction_id, transaction_id)
        .await
        .unwrap();
    assert_eq!(decision, CoordinatorDecision::Commit);
    let reads = queries.load(Ordering::Relaxed) - before;
    println!("coordinator queries for a two-participant resume: {reads}");
    assert_eq!(
        reads, 4,
        "small resume must fetch status and immutable payloads in one query, then retain decision and resolution checks"
    );
    let status = client
        .query::<ReadCrossCellTransaction>(
            &coordinator,
            None,
            Json(ReadCrossCellTransactionInput {
                account_id: account_id.into(),
                transaction_id,
                routing_key: transaction_id.to_vec(),
            }),
        )
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    assert_eq!(status.resolved_count, 2);
    let terminal = client
        .query::<ReadCoordinatorResume>(&coordinator, None, Json(read))
        .await
        .unwrap()
        .output
        .0;
    assert_eq!(terminal.status.unwrap(), status);
    assert!(
        terminal.participants.is_none(),
        "compacted or terminal transactions must not reuse payloads"
    );
    local_node::stop(node).await.unwrap();
    });
}
