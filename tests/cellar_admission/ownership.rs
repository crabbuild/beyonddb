use crate::{
    claim,
    native_peer::{self, FRONT, Http, Inspection, Peer, PeerPolicy, SOURCE},
};
use aws_sdk_dynamodb::types::{AttributeValue as V, Put, TransactWriteItem};
use beyonddb::*;
use cellar::bucket::Bucket;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

const ACCOUNT: &str = "123456789012";
const ACCESS: &str = "AKIAIOSFODNN7EXAMPLE";
const TOKEN: &str = "fresh-admission-race";
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
fn pending() -> Json<ReadPendingCrossCellTransactionsInput> {
    Json(ReadPendingCrossCellTransactionsInput {
        after: None,
        limit: 1,
    })
}
fn write(
    sdk: &aws_sdk_dynamodb::Client,
) -> aws_sdk_dynamodb::operation::transact_write_items::builders::TransactWriteItemsFluentBuilder {
    sdk.transact_write_items()
        .client_request_token(TOKEN)
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("Residency")
                        .item("id", V::S("claimed".into()))
                        .item("value", V::N("9".into()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
}
#[test]
fn sdk_delayed_absent_claim_cannot_overwrite_a_peer_coordinator() {
    let (host, core) = runtimes();
    host.block_on(async {
        let files = tempfile::tempdir().unwrap();
        let bucket = Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let app = native_peer::application();
        let observed = Arc::new(Mutex::new(Default::default()));
        let coordinator = coordinator_target(ACCOUNT, TOKEN.as_bytes()).unwrap();
        let module = app.cell_types().iter().find(|kind| kind.namespace() == coordinator.namespace()).unwrap().module();
        let control = Arc::new(claim::Control::default());
        let mut local_bucket = bucket.clone();
        local_bucket.cas_store = Arc::new(claim::Store { inner: bucket.cas_store.clone(), control: control.clone() });
        let mut source = Peer::start(app.clone(), &files.path().join(SOURCE), local_bucket, observed.clone(),
            PeerPolicy { max_resident: 8, ..true.into() }, host.handle(), core.handle()).await;
        native_peer::put_credential(&source.client, ACCESS, ACCOUNT).await;
        CellAuthorizationStore::new(source.client.clone()).put_user_policy(ACCOUNT, "network-user", "tables", &serde_json::json!({
            "Version":"2012-10-17", "Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"arn:aws:dynamodb:us-east-1:123456789012:table/Residency*"}]
        }).to_string()).await.unwrap();
        let (_, data) = native_peer::table(&source.client, ACCOUNT, "Residency", false).await;
        assert_eq!(data.len(), 2); assert_ne!(data[0].0, data[1].0);
        native_peer::provisioner(&source.client).ensure(&source.client, ACCOUNT, TOKEN.as_bytes()).await.unwrap();
        let held = control.hold(module);
        let local = { let client = source.client.clone(); let target = coordinator.clone(); tokio::spawn(async move { client.query::<ReadPendingCrossCellTransactions>(&target, None, pending()).await }) };
        tokio::time::timeout(Duration::from_secs(2), held.entered()).await.unwrap();
        assert!(native_peer::identity(&observed, SOURCE, &coordinator).is_none());
        // Only now add real capacity: the first SDK CAS selected SOURCE while
        // it was the only node. FRONT competes through ordinary SDK routing.
        let mut front = Peer::start(app.clone(), &files.path().join(FRONT), bucket.clone(), observed.clone(),
            PeerPolicy { max_resident: 8, ..true.into() }, host.handle(), core.handle()).await;
        let inspection = Inspection::new(bucket.clone(), host.handle());
        inspection.ready_peers().await;
        front.client.query::<ReadPendingCrossCellTransactions>(&coordinator, None, pending()).await.unwrap();
        let foreign = native_peer::identity(&observed, FRONT, &coordinator).unwrap();
        assert_eq!(foreign.scope, held.scope());
        let http = Http::start(&front.client, ACCESS).await;
        write(&http.sdk).send().await.unwrap();
        let before = front.client.query::<ReadPendingCrossCellTransactions>(&coordinator, None, pending()).await.unwrap();
        let owner = inspection.owner(&foreign.scope).await;
        assert_eq!(owner.node.as_deref(), Some(FRONT)); assert_eq!(owner.epoch, foreign.epoch);
        held.release();
        tokio::time::timeout(Duration::from_secs(10), held.rejected()).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(10), local).await.unwrap().unwrap();
        eprintln!("delayed SDK claim read result: {}", if result.is_ok() { "forwarded" } else { "requires fresh read" });
        assert_eq!(inspection.owner(&foreign.scope).await, owner);
        let restored = source.client.query::<ReadPendingCrossCellTransactions>(&coordinator, None, pending()).await.unwrap();
        assert_eq!(restored.receipt.commit_sequence, before.receipt.commit_sequence);
        assert_eq!(inspection.owner(&foreign.scope).await, owner);
        assert!(native_peer::identity(&observed, SOURCE, &coordinator).is_none());
        assert!(source.client.query::<ReadCoordinatorRegistration>(&account_target(ACCOUNT).unwrap(), None,
            Json(RegisterCoordinatorShardInput { account_id: ACCOUNT.into(), shard: u32::from_be_bytes(coordinator.partition().try_into().unwrap()) }))
            .await.unwrap().output.0);
        http.sdk.put_item().table_name("Residency").item("id", V::S("claimed".into()))
            .item("value", V::N("10".into())).send().await.unwrap();
        write(&http.sdk).send().await.unwrap();
        assert_eq!(http.sdk.get_item().table_name("Residency").key("id", V::S("claimed".into())).consistent_read(true)
            .send().await.unwrap().item.unwrap()["value"], V::N("10".into()));
        http.stop().await; drop(held);
        source.stop().await; front.stop().await;
        std::fs::remove_dir_all(files.path().join(SOURCE)).unwrap();
        std::fs::remove_dir_all(files.path().join(FRONT)).unwrap();
        let name = "owner-race-next";
        let mut next = Peer::start(app, &files.path().join(name), bucket, observed.clone(), PeerPolicy { max_resident: 8, ..true.into() }, host.handle(), core.handle()).await;
        inspection.departed_node(SOURCE, name).await; inspection.departed_node(FRONT, name).await;
        let http = Http::start(&next.client, ACCESS).await;
        assert!(next.client.query::<ReadCoordinatorRegistration>(&account_target(ACCOUNT).unwrap(), None,
            Json(RegisterCoordinatorShardInput { account_id: ACCOUNT.into(), shard: u32::from_be_bytes(coordinator.partition().try_into().unwrap()) }))
            .await.unwrap().output.0);
        write(&http.sdk).send().await.unwrap();
        assert_eq!(http.sdk.get_item().table_name("Residency").key("id", V::S("claimed".into())).consistent_read(true)
            .send().await.unwrap().item.unwrap()["value"], V::N("10".into()));
        let actual = native_peer::identity(&observed, name, &coordinator).unwrap();
        assert_eq!(actual.scope, foreign.scope); assert!(actual.epoch > foreign.epoch);
        let after = inspection.owner(&actual.scope).await;
        assert_eq!(after.node.as_deref(), Some(name)); assert_eq!(after.epoch, actual.epoch);
        for (_, item) in data {
            let expected = item.into_iter().map(|(name, value)| {
                let value = match value {
                    extenddb_core::types::AttributeValue::S(value) => V::S(value),
                    extenddb_core::types::AttributeValue::N(value) => V::N(value),
                    _ => panic!("fixture attribute changed type"),
                };
                (name, value)
            }).collect::<crate::residency_fixture::SdkItem>();
            assert_eq!(http.sdk.get_item().table_name("Residency").key("id", expected["id"].clone()).consistent_read(true)
                .send().await.unwrap().item, Some(expected));
        }
        http.stop().await; next.stop().await; inspection.close().await;
    });
}
