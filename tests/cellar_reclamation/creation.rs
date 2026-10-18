use super::creation_actor;
use crate::{
    native_peer::{self, Http, Inspection, Peer, PeerPolicy},
    residency_fixture::{ACCOUNT, SdkItem, runtimes},
    table_helpers,
};
use aws_sdk_dynamodb::types::AttributeValue as V;
use bdb_cellar_runtime::{
    client::{Invocation, InvocationAdmission},
    identity::{CellTarget, NamespaceId},
    registry::{Command, Query},
};
use beyonddb::*;
use cellar::bucket::Bucket;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

const ACCESS: &str = "AKIAIOSFODNN7EXAMPLE";
const NAME: &str = "ResidencyPending";
struct Admission {
    account: CellTarget,
    data: NamespaceId,
    deny_install: AtomicBool,
    refused: AtomicUsize,
    pause_metadata: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
}
impl InvocationAdmission for Admission {
    fn check<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
    ) -> Pin<Box<dyn Future<Output = bdb_cellar_runtime::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if self.deny_install.load(Ordering::SeqCst)
                && invocation.target.namespace() == self.data
                && invocation.mutation.is_some()
                && invocation.operation == InstallPartition::ID
            {
                self.refused.fetch_add(1, Ordering::SeqCst);
                return Err(bdb_cellar_runtime::Error::Capacity(
                    "creation install withheld before SDK submission",
                ));
            }
            if invocation.target == self.account
                && invocation.mutation.is_none()
                && invocation.operation == ReadTableLifecycle::ID
                && self.pause_metadata.swap(false, Ordering::SeqCst)
            {
                self.entered.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            Ok(())
        })
    }
}
async fn record(client: &bdb_cellar_runtime::client::CellClient, name: &str) -> TableRecord {
    client
        .query::<DescribeTable>(&account_target(ACCOUNT).unwrap(), None, Json(name.into()))
        .await
        .unwrap()
        .output
        .0
        .unwrap()
}
#[test]
fn sdk_creation_recovers_when_existing_ranges_evict_during_metadata_lookup() {
    let (host, core) = runtimes();
    host.block_on(async {
        let files = tempfile::tempdir().unwrap();
        let bucket = Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let path = files.path().join("creation-race");
        let app = native_peer::application();
        let (node, client, track) = creation_actor::start(app.clone(), &path, bucket.clone(), host.handle(), core.handle()).await;
        native_peer::put_credential(&client, ACCESS, ACCOUNT).await;
        CellAuthorizationStore::new(client.clone()).put_user_policy(ACCOUNT, "network-user", "tables", &serde_json::json!({
            "Version":"2012-10-17", "Statement":[{"Effect":"Allow", "Action":"dynamodb:*", "Resource":"arn:aws:dynamodb:us-east-1:123456789012:table/Residency*"}]
        }).to_string()).await.unwrap();
        let original_http = Http::start(&client, ACCESS).await;
        table_helpers::create(&original_http.sdk, "Residency", false).send().await.unwrap();
        let table = record(&client, "Residency").await;
        let account = account_target(ACCOUNT).unwrap();
        let route = crate::single_leaf_route(&client, &account, &table.id).await.unwrap();
        assert_eq!(route.partitions.len(), 2);
        let mut expected = Vec::new();
        for id in ["first", "Ω-other"] {
            let item = SdkItem::from([("id".into(), V::S(id.into())), ("value".into(), V::S("committed".into()))]);
            original_http.sdk.put_item().table_name("Residency").set_item(Some(item.clone())).send().await.unwrap();
            expected.push(item);
        }
        let targets: Vec<_> = route.partitions.iter().map(|range| data_target(ACCOUNT, &table.id, &range.partition_id).unwrap()).collect();
        let admission = Arc::new(Admission {
            account: account.clone(), data: targets[0].namespace(), deny_install: AtomicBool::new(true), refused: AtomicUsize::new(0),
            pause_metadata: AtomicBool::new(false), entered: Semaphore::new(0), release: Semaphore::new(0),
        });
        let retry_http = Http::start(&client.with_admission(admission.clone()), ACCESS).await;
        assert!(table_helpers::create(&retry_http.sdk, NAME, false).send().await.is_err());
        assert_eq!(admission.refused.load(Ordering::SeqCst), 1);
        let pending = record(&client, NAME).await;
        assert_eq!(pending.placement, TablePlacement::Routed { initial_partitions: 2 });
        assert!(matches!(read_route_page(&client, &account, RoutePageInput { table_id: pending.id.clone(), start_key: None, after_lower: None, expected_epoch: None }).await.unwrap(), RoutePageOutcome::Unrouted));
        admission.deny_install.store(false, Ordering::SeqCst);
        admission.pause_metadata.store(true, Ordering::SeqCst);
        let sdk = retry_http.sdk.clone();
        let request = tokio::spawn(async move { table_helpers::create(&sdk, NAME, false).send().await });
        tokio::time::timeout(Duration::from_secs(5), admission.entered.acquire()).await.unwrap().unwrap().forget();
        // Warm both ranges while the real retry is paused at its account lookup.
        for target in &targets { client.query::<ReadPartitionState>(target, None, Json(())).await.unwrap(); }
        let before: Vec<_> = targets.iter().map(|target| track.read(target)).collect();
        assert!(before.iter().all(|life| life.active == 1));
        let mut evicted = false;
        for ordinal in 0..8 {
            native_peer::put_credential(&client, &format!("AKIACREATIONPRESSURE{ordinal}"), ACCOUNT).await;
            evicted = targets.iter().zip(&before).all(|(target, old)| {
                let now = track.read(target); now.active == 0 && now.destroyed > old.destroyed
            });
            if evicted { break; }
        }
        assert!(evicted, "both actual SDK activations must close during metadata lookup");
        assert!(!request.is_finished());
        admission.release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(10), request).await.unwrap().unwrap().unwrap();
        assert_eq!(record(&client, NAME).await.id, pending.id);
        assert_eq!(crate::single_leaf_route(&client, &account, &pending.id).await.unwrap().partitions.len(), 2);
        for (target, old) in targets.iter().zip(&before) {
            client.query::<ReadPartitionState>(target, None, Json(())).await.unwrap();
            assert!(track.read(target).created > old.created);
        }
        for item in &expected {
            assert_eq!(original_http.sdk.get_item().table_name("Residency").key("id", item["id"].clone())
                .consistent_read(true).send().await.unwrap().item.as_ref(), Some(item));
        }
        retry_http.sdk.put_item().table_name(NAME).item("id", V::S("created".into())).send().await.unwrap();
        let previous: Vec<_> = targets.iter().map(|target| track.read(target).identity).collect();
        retry_http.stop().await; original_http.stop().await; creation_actor::stop(node).await;
        std::fs::remove_dir_all(path).unwrap();
        let observed = Arc::new(Mutex::new(Default::default()));
        let mut next = Peer::start(app, &files.path().join("creation-race-next"), bucket.clone(), observed.clone(),
            PeerPolicy { max_resident: 5, ..true.into() }, host.handle(), core.handle()).await;
        let inspection = Inspection::new(bucket, host.handle());
        inspection.departed_node("creation-race", "creation-race-next").await;
        let http = Http::start(&next.client, ACCESS).await;
        for (target, old) in targets.iter().zip(previous) {
            next.client.query::<ReadPartitionState>(target, None, Json(())).await.unwrap();
            let actual = native_peer::identity(&observed, "creation-race-next", target).unwrap();
            assert_eq!(actual.scope, old.scope); assert!(actual.epoch > old.epoch);
            let owner = inspection.owner(&actual.scope).await;
            assert_eq!(owner.node.as_deref(), Some("creation-race-next")); assert_eq!(owner.epoch, actual.epoch);
        }
        for item in expected {
            assert_eq!(http.sdk.get_item().table_name("Residency").key("id", item["id"].clone())
                .consistent_read(true).send().await.unwrap().item, Some(item));
        }
        assert_eq!(record(&next.client, NAME).await.id, pending.id);
        assert_eq!(http.sdk.get_item().table_name(NAME).key("id", V::S("created".into()))
            .consistent_read(true).send().await.unwrap().item.unwrap()["id"], V::S("created".into()));
        http.stop().await; next.stop().await; inspection.close().await;
    });
}
