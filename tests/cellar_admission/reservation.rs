//! Pending ownership occupies the last slot until its accepted SDK CAS settles.
use crate::{
    claim,
    native_peer::{self, Http, Inspection, Peer, PeerPolicy},
    pinned_actor,
};
use beyonddb::*;
use cellar::bucket::Bucket;
use cellar_runtime::identity::CellTarget;
use std::{
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};
const ACCOUNT: &str = "123456789012";
const ACCESS: &str = "AKIAIOSFODNN7EXAMPLE";
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
async fn registered(client: &cellar_runtime::client::CellClient, target: &CellTarget) {
    assert!(
        client
            .query::<ReadCoordinatorRegistration>(
                &account_target(ACCOUNT).unwrap(),
                None,
                Json(RegisterCoordinatorShardInput {
                    account_id: ACCOUNT.into(),
                    shard: u32::from_be_bytes(target.partition().try_into().unwrap())
                })
            )
            .await
            .unwrap()
            .output
            .0
    );
}
#[test]
fn sdk_pending_last_slot_survives_caller_cancellation_and_releases_after_claim_settles() {
    let (host, core) = runtimes();
    host.block_on(async {
        let files = tempfile::tempdir().unwrap();
        let bucket = Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let app = native_peer::application();
        let observed = Arc::new(Mutex::new(Default::default()));
        let seed_name = "reservation-seed";
        let mut seed = Peer::start(app.clone(), &files.path().join(seed_name), bucket.clone(), observed.clone(),
            PeerPolicy { max_resident: 5, ..true.into() }, host.handle(), core.handle()).await;
        native_peer::put_credential(&seed.client, ACCESS, ACCOUNT).await;
        CellAuthorizationStore::new(seed.client.clone()).put_user_policy(ACCOUNT, "network-user", "tables", &serde_json::json!({
            "Version":"2012-10-17", "Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"arn:aws:dynamodb:us-east-1:123456789012:table/Residency*"}]
        }).to_string()).await.unwrap();
        let (table, data) = native_peer::table(&seed.client, ACCOUNT, "Residency", false).await;
        assert_eq!(data.len(), 2); assert_ne!(data[0].0, data[1].0);
        let first_key = b"slot-first";
        let first = coordinator_target(ACCOUNT, first_key).unwrap();
        let second_key = (0..100).map(|n| format!("slot-second-{n}"))
            .find(|key| coordinator_target(ACCOUNT, key.as_bytes()).unwrap() != first).unwrap();
        let second = coordinator_target(ACCOUNT, second_key.as_bytes()).unwrap();
        for key in [first_key.as_slice(), second_key.as_bytes()] { native_peer::provisioner(&seed.client).ensure(&seed.client, ACCOUNT, key).await.unwrap(); }
        let account = account_target(ACCOUNT).unwrap();
        let directory = directory_target(ACCOUNT, &DirectorySpec::root(table.id)).unwrap();
        let targets = vec![account.clone(), directory.clone(), data[0].0.clone(), data[1].0.clone()];
        let pins = Arc::new(pinned_actor::Pins::new(&targets));
        let _release_pins = pins.guard();
        let control = Arc::new(claim::Control::default());
        let mut local_bucket = bucket.clone();
        local_bucket.store = Arc::new(claim::Store { inner: bucket.store.clone(), control: control.clone() });
        local_bucket.cas_store = Arc::new(claim::Store { inner: bucket.cas_store.clone(), control: control.clone() });
        let name = "reservation-owner";
        let path = files.path().join(name);
        let (node, client, tracker) = pinned_actor::start_with_pins(app.clone(), &path, local_bucket, host.handle(), core.handle(), Some(pins.clone())).await;
        seed.stop().await; std::fs::remove_dir_all(files.path().join(seed_name)).unwrap();
        let inspection = Inspection::new(bucket.clone(), host.handle());
        inspection.departed_node(seed_name, name).await;
        let mut jobs = Vec::new();
        { let client = client.clone(); let account = account.clone(); jobs.push(tokio::spawn(async move { client.query::<ListTables>(&account, None,
            Json(ListTablesInput { limit: 1, exclusive_start: None, live_only: false })).await.map(|_| ()).map_err(|error| format!("{error:?}")) })); }
        { let client = client.clone(); jobs.push(tokio::spawn(async move { client.query::<ReadDirectory>(&directory, None, Json(())).await.map(|_| ()).map_err(|error| format!("{error:?}")) })); }
        for (target, _) in &data { let client = client.clone(); let target = target.clone(); jobs.push(tokio::spawn(async move { client.query::<ReadPartitionState>(&target, None, Json(())).await.map(|_| ()).map_err(|error| format!("{error:?}")) })); }
        tokio::time::timeout(Duration::from_secs(2), pins.entered()).await.unwrap();
        assert_eq!(tracker.active(), 4);
        let module = app.cell_types().iter().find(|kind| kind.namespace() == first.namespace()).unwrap().module();
        let held = control.hold(module);
        let first_job = { let client = client.clone(); let target = first.clone(); tokio::spawn(async move { client.query::<ReadPendingCrossCellTransactions>(&target, None, pending()).await }) };
        tokio::time::timeout(Duration::from_secs(2), held.entered()).await.unwrap();
        let second_job = { let client = client.with_read_backpressure(Duration::from_secs(10)); let target = second.clone(); tokio::spawn(async move { client.query::<ReadPendingCrossCellTransactions>(&target, None, pending()).await }) };
        // The second request must reach the SDK's own owner lookup, so CPU or
        // frontend queueing cannot stand in for the pending residency boundary.
        tokio::time::timeout(Duration::from_secs(2), held.read_scopes_at_least(2)).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(150), held.creates_at_least(2)).await.is_err());
        assert!(!second_job.is_finished());
        let retired = node.services().application_work().retired_events;
        first_job.abort(); assert!(first_job.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), async {
            while node.services().application_work().retired_events == retired { tokio::task::yield_now().await; }
        }).await.unwrap();
        assert_eq!(held.creates(), 1);
        assert!(tracker.maybe(&first).is_none());
        held.release();
        tokio::time::timeout(Duration::from_secs(2), held.applied()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), held.creates_at_least(2)).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(10), second_job).await.unwrap().unwrap().unwrap().output.0.is_empty());
        assert!(tracker.maybe(&first).is_none_or(|life| life.invoked == 0), "cancelled caller reached its application handler");
        assert_eq!(tracker.active(), 5);
        let load = node.services().node_load().unwrap();
        assert!(load.resident_cells.load(Ordering::SeqCst) <= 5);
        let first_owner = inspection.owner(&held.scope()).await;
        assert_eq!(first_owner.node.as_deref(), Some(name));
        pins.release();
        for job in jobs { tokio::time::timeout(Duration::from_secs(10), job).await.unwrap().unwrap().unwrap(); }
        drop(held);
        for target in [&first, &second] { registered(&client, target).await; assert!(client.query::<ReadPendingCrossCellTransactions>(target, None, pending()).await.unwrap().output.0.is_empty()); }
        let old = tracker.read(&second).identity;
        pinned_actor::stop(node).await; std::fs::remove_dir_all(path).unwrap();
        let next_name = "reservation-next";
        let mut next = Peer::start(app, &files.path().join(next_name), bucket, observed.clone(), PeerPolicy { max_resident: 5, ..true.into() }, host.handle(), core.handle()).await;
        inspection.departed_node(name, next_name).await;
        for target in [&first, &second] { registered(&next.client, target).await; assert!(next.client.query::<ReadPendingCrossCellTransactions>(target, None, pending()).await.unwrap().output.0.is_empty()); }
        let actual = native_peer::identity(&observed, next_name, &second).unwrap();
        assert_eq!(actual.scope, old.scope); assert!(actual.epoch > old.epoch);
        let http = Http::start(&next.client, ACCESS).await;
        for (_, item) in data { let expected = item.into_iter().map(|(key, value)| { let value = match value {
            extenddb_core::types::AttributeValue::S(value) => aws_sdk_dynamodb::types::AttributeValue::S(value),
            extenddb_core::types::AttributeValue::N(value) => aws_sdk_dynamodb::types::AttributeValue::N(value), _ => panic!("fixture attribute type"),
        }; (key, value) }).collect::<crate::residency_fixture::SdkItem>();
            assert_eq!(http.sdk.get_item().table_name("Residency").key("id", expected["id"].clone()).consistent_read(true).send().await.unwrap().item, Some(expected)); }
        http.stop().await; next.stop().await; inspection.close().await;
    });
}
