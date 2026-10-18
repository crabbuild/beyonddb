//! Discover an account-index entry after its real SDK owner departs.
use crate::{native_peer::*, runtimes};
use beyonddb::*;
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use cellar_runtime::{
    MutationIdentity,
    client::{Invocation, InvocationAdmission, InvocationTransport},
    identity::{CellTarget, RequestId},
    registry::Query,
};
use http_body_util::{BodyExt, Full};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
const ACCOUNT: &str = "123456789012";
const ACCESS: &str = "AKIADISCOVERYSOURCE";
const ID: [u8; 16] = [174; 16];

struct Admission {
    participants: Vec<CellTarget>,
    refuse: Arc<AtomicBool>,
    attempts: Arc<AtomicUsize>,
}
impl InvocationAdmission for Admission {
    fn check<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
    ) -> Pin<Box<dyn Future<Output = cellar_runtime::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if self.refuse.load(Ordering::SeqCst) && self.participants.contains(&invocation.target)
            {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                return Err(cellar_runtime::Error::Capacity(
                    "participant discovery withheld until owner departure",
                ));
            }
            Ok(())
        })
    }
}
struct Observe {
    inner: Arc<dyn InvocationTransport>,
    account: CellTarget,
    completed: Arc<AtomicUsize>,
}
impl InvocationTransport for Observe {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            let response = self.inner.call(invocation, request).await;
            if invocation.target == self.account
                && invocation.mutation.is_none()
                && invocation.operation == ListCoordinatorShards::ID
                && response.status().is_success()
            {
                let (parts, body) = response.into_parts();
                let bytes = body.collect().await.unwrap().to_bytes();
                self.completed.fetch_add(1, Ordering::SeqCst);
                let body = Full::new(bytes)
                    .map_err(|never| -> std::io::Error { match never {} })
                    .boxed_unsync();
                http::Response::from_parts(parts, body)
            } else {
                response
            }
        })
    }
}
async fn phase(
    bucket: &cellar::bucket::Bucket,
    scope: &str,
    host: &tokio::runtime::Handle,
) -> (i64, i64, i64, i64) {
    let snapshot = crate::capacity_snapshot::restore(bucket, scope, host).await;
    let sql = crate::capacity_snapshot::open(&snapshot);
    sql.query_row("SELECT state, unresolved_count, (SELECT COUNT(*) FROM ddb_coordinator_participants WHERE transaction_id=?1), (SELECT COUNT(resolved_sequence) FROM ddb_coordinator_participants WHERE transaction_id=?1) FROM ddb_coordinator_transactions WHERE transaction_id=?1", [ID.as_slice()], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).unwrap()
}
#[test]
fn recovery_discovers_new_coordinator_after_its_account_owner_expires() {
    let (host, core) = runtimes();
    host.block_on(async {
        let files = tempfile::tempdir().unwrap();
        let bucket = cellar::bucket::Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let observed = Arc::new(Mutex::new(Default::default()));
        let app = application();
        let source_path = files.path().join(SOURCE);
        let mut source = Peer::start(app.clone(), &source_path, bucket.clone(), observed.clone(), PeerPolicy { max_resident: 8, ..false.into() }, host.handle(), core.handle()).await;
        put_credential(&source.client, ACCESS, ACCOUNT).await;
        policy(&source.client, ACCOUNT).await;
        let (table, data) = crate::native_peer::table(&source.client, ACCOUNT, "ResidencyDiscovery", false).await;
        assert_eq!(data.len(), 2); assert_ne!(data[0].0, data[1].0);
        let account = account_target(ACCOUNT).unwrap();
        let coordinator = coordinator_target(ACCOUNT, &ID).unwrap();
        // Establish actual SDK placement while SOURCE is the only node. The
        // account index is still empty, and the worker has not seen this shard.
        source.client.query::<ReadPendingCrossCellTransactions>(&coordinator, None, Json(ReadPendingCrossCellTransactionsInput { after: None, limit: 1 })).await.unwrap();
        let index = Json(ListCoordinatorShardsInput { account_id: ACCOUNT.into(), after: None, limit: 100 });
        assert!(source.client.query::<ListCoordinatorShards>(&account, None, index).await.unwrap().output.0.is_empty());
        let previous = [&account, &coordinator].map(|target| identity(&observed, SOURCE, target).unwrap());
        let mut front = Peer::start(app, &files.path().join(FRONT), bucket.clone(), observed.clone(), PeerPolicy { max_resident: 16, ..false.into() }, host.handle(), core.handle()).await;
        let inspection = Inspection::new(bucket.clone(), host.handle()); inspection.ready_peers().await;
        let completed = Arc::new(AtomicUsize::new(0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let refuse = Arc::new(AtomicBool::new(true));
        let client = front.client.map_transport(|inner| Arc::new(Observe { inner, account: account.clone(), completed: completed.clone() }))
            .with_admission(Arc::new(Admission { participants: data.iter().map(|(target,_)|target.clone()).collect(), refuse: refuse.clone(), attempts: attempts.clone() }));
        let mut recovery = TransactionRecovery::new(Arc::new(CellStorage::new(client, "us-east-1")), ACCOUNT.into()).unwrap();
        let stop = CancellationToken::new();
        let _worker_stop = stop.clone().drop_guard();
        let cancelled = stop.clone();
        let worker = tokio::spawn(async move {
            let mut ticks = tokio::time::interval(Duration::from_millis(250));
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { ()=cancelled.cancelled()=>break, _=ticks.tick()=>{} }
                tokio::select! { ()=cancelled.cancelled()=>break, result=recovery.step()=>{ if let Err(error)=result { tracing::debug!(%error,"discovery deferred"); } } }
            }
        });
        // The worker must complete its own empty-index read before publication.
        tokio::time::timeout(Duration::from_secs(5), async {
            while completed.load(Ordering::SeqCst)==0 { tokio::time::sleep(Duration::from_millis(5)).await; }
        }).await.unwrap();
        let provisioner = crate::native_peer::provisioner(&source.client);
        provisioner.ensure(&source.client, ACCOUNT, &ID).await.unwrap();
        let route = crate::single_leaf_route(&source.client, &account, &table.id).await.unwrap();
        let mut participants = Vec::new();
        for (position, range) in route.partitions.iter().enumerate() {
            let target = data_target(ACCOUNT, &table.id, &range.partition_id).unwrap();
            let (_,item) = data.iter().find(|(candidate,_)| candidate==&target).unwrap();
            participants.push(CoordinatorParticipant {
                target: CoordinatorParticipantTarget::Data { table_id: table.id.clone(), partition_id: range.partition_id, epoch: range.epoch },
                operations: vec![IndexedTransactionOperation { index: position as u8, operation: TransactionOperation::Read(GetItemInput { table_name: table.table_name.clone(), table_id: table.id.clone(), key: extenddb_core::types::extract_key(item,&table.key_schema) }) }],
            });
        }
        participants.sort_by_key(|part| match &part.target { CoordinatorParticipantTarget::Data { table_id, partition_id, .. }=>*data_target(ACCOUNT,table_id,partition_id).unwrap().cell_id().as_bytes(), _=>unreachable!() });
        let now = now_ms();
        transaction_command!(&source.client, BeginCrossCellTransaction, &coordinator,
            MutationIdentity { request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()), issued_at_ms: now, expires_at_ms: now+60_000 },
            Json(BeginCrossCellTransactionInput { account_id: ACCOUNT.into(), transaction_id: ID, token: None, participants })
        ).await.unwrap();
        // Native recovery can safely route live BEGINs. Hold participant
        // preflight admission so a genuine abandoned BEGIN remains at loss.
        tokio::time::sleep(Duration::from_millis(750)).await;
        assert!(attempts.load(Ordering::SeqCst)>0, "worker failed to discover the newly registered shard");
        assert_eq!(phase(&bucket, &previous[1].scope, host.handle()).await, (0,2,2,0));
        for (target, old) in [&account, &coordinator].into_iter().zip(&previous) {
            assert!(identity(&observed, FRONT, target).is_none());
            let owner = inspection.owner(&old.scope).await;
            assert_eq!(owner.node.as_deref(), Some(SOURCE)); assert_eq!(owner.epoch, old.epoch);
        }
        assert!(source.network.accepted.load(Ordering::SeqCst)>0);
        source.network.cut(true).await;
        source.stop().await;
        std::fs::remove_dir_all(&source_path).unwrap();
        inspection.departed().await;
        refuse.store(false, Ordering::SeqCst);
        // No routed query, describe, direct admission or recovery call may help
        // this poll. Only the running worker can activate and settle the shard.
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if [&account,&coordinator].iter().all(|target| identity(&observed, FRONT, target).is_some())
                    && phase(&bucket, &previous[1].scope, host.handle()).await==(1,0,2,2) { break; }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }).await.expect("worker did not recover account discovery and resolve the abandoned coordinator");
        for (target, old) in [&account, &coordinator].into_iter().zip(previous) {
            let current = identity(&observed, FRONT, target).unwrap();
            assert_eq!(current.scope,old.scope); assert!(current.epoch>old.epoch);
            let owner = inspection.owner(&current.scope).await;
            assert_eq!(owner.node.as_deref(),Some(FRONT)); assert_eq!(owner.epoch,current.epoch);
        }
        stop.cancel(); tokio::time::timeout(Duration::from_secs(5),worker).await.unwrap().unwrap();
        let status = front.client.query::<ReadCrossCellTransaction>(&coordinator,None,Json(ReadCrossCellTransactionInput { account_id: ACCOUNT.into(),transaction_id: ID,routing_key:ID.to_vec() })).await.unwrap().output.0.unwrap();
        assert_eq!(status.decision,CoordinatorDecision::Commit); assert_eq!(status.resolved_count,2);
        let http=Http::start(&front.client,ACCESS).await;
        for (_,expected) in data {
            let extenddb_core::types::AttributeValue::S(id)=&expected["id"] else { unreachable!() };
            let item=http.sdk.get_item().table_name("ResidencyDiscovery").key("id",aws_sdk_dynamodb::types::AttributeValue::S(id.clone())).consistent_read(true).send().await.unwrap().item.unwrap();
            let expected = expected.into_iter().map(|(name, value)| {
                let value = match value {
                    extenddb_core::types::AttributeValue::S(value) => aws_sdk_dynamodb::types::AttributeValue::S(value),
                    extenddb_core::types::AttributeValue::N(value) => aws_sdk_dynamodb::types::AttributeValue::N(value),
                    other => panic!("unexpected seeded item attribute: {other:?}"),
                };
                (name, value)
            }).collect::<std::collections::HashMap<_, _>>();
            assert_eq!(item, expected);
        }
        http.stop().await; front.stop().await; inspection.close().await;
    });
}
