//! Signed requests over actual Cellar peer sockets and fenced owner recovery.
#[path = "peer_network/account_query.rs"]
mod account_query;
#[path = "peer_network/concurrency.rs"]
mod concurrency;
#[path = "peer_network/residency/credential_failover.rs"]
mod credential_failover;
#[path = "support/native_peer.rs"]
mod native_peer;

use bdb_cellar_runtime::application::cell_name;
use beyonddb::{account_target, credential_target};
use native_peer::*;
use std::sync::{Arc, Mutex, atomic::Ordering};

fn runtimes() -> (tokio::runtime::Runtime, tokio::runtime::Runtime) {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .with_ansi(false)
        .try_init();
    (
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    )
}

#[test]
fn signed_sdk_requests_cross_native_owners_and_survive_source_replacement() {
    let (host, core) = runtimes();
    host.block_on(async {
        for cached in [false, true] {
            let files = tempfile::tempdir().unwrap();
            let bucket =
                cellar::bucket::Bucket::open_dev(&files.path().join("bucket.sqlite")).unwrap();
            let observed = Arc::new(Mutex::new(Default::default()));
            let app = application();
            let source_path = files.path().join(SOURCE);
            let mut source = Peer::start(
                app.clone(),
                &source_path,
                bucket.clone(),
                observed.clone(),
                cached,
                host.handle(),
                core.handle(),
            )
            .await;
            const ACCOUNT: &str = "123456789012";
            const ACCESS: &str = "AKIANATIVEPEEREXAMPLE";
            put_credential(&source.client, ACCESS, ACCOUNT).await;
            policy(&source.client, ACCOUNT).await;
            let (_, items) = table(&source.client, ACCOUNT, "NetworkData", false).await;
            table(&source.client, ACCOUNT, "AccountOrdered", true).await;
            let targets = [
                credential_target(ACCESS).unwrap(),
                account_target(ACCOUNT).unwrap(),
                items[0].0.clone(),
                items[1].0.clone(),
            ];
            let inspection = Inspection::new(bucket.clone(), host.handle());
            let mut before = Vec::new();
            for target in &targets {
                let actual = identity(&observed, SOURCE, target).unwrap();
                let owner = inspection.owner(&actual.scope).await;
                assert_eq!(owner.node.as_deref(), Some(SOURCE));
                before.push((actual, owner));
            }
            let mut front = Peer::start(
                app,
                &files.path().join(FRONT),
                bucket,
                observed.clone(),
                cached,
                host.handle(),
                core.handle(),
            )
            .await;
            inspection.ready_peers().await;
            let http = Http::start(&front.client, ACCESS).await;
            assert_items(&http.sdk, &items).await;
            concurrency::increment_without_client_retries(&http.sdk).await;
            account_query::write_and_assert(&http.sdk).await;
            assert!(source.network.accepted.load(Ordering::SeqCst) > 0);
            for target in &targets {
                assert!(
                    identity(&observed, FRONT, target).is_none(),
                    "frontend unexpectedly owned {}",
                    cell_name(target)
                );
            }
            source.network.cut(true).await;
            source.stop().await;
            std::fs::remove_dir_all(&source_path).unwrap();
            inspection.departed().await;
            assert_items(&http.sdk, &items).await;
            concurrency::assert_counter(&http.sdk).await;
            account_query::assert_restored(&http.sdk).await;
            for (target, (original, previous)) in targets.iter().zip(before) {
                let current = identity(&observed, FRONT, target).unwrap();
                assert_eq!(current.scope, original.scope);
                let owner = inspection.owner(&current.scope).await;
                assert_eq!(owner.node.as_deref(), Some(FRONT));
                assert!(
                    owner.epoch > previous.epoch,
                    "owner epoch did not advance: {owner:?}"
                );
                assert_eq!(current.epoch, owner.epoch);
            }
            http.stop().await;
            front.stop().await;
            inspection.close().await;
        }
    });
}

async fn assert_items(
    sdk: &aws_sdk_dynamodb::Client,
    items: &[(
        bdb_cellar_runtime::identity::CellTarget,
        extenddb_core::types::Item,
    )],
) {
    for (_, expected) in items {
        let extenddb_core::types::AttributeValue::S(id) = &expected["id"] else {
            unreachable!()
        };
        let read = sdk
            .get_item()
            .table_name("NetworkData")
            .key("id", aws_sdk_dynamodb::types::AttributeValue::S(id.clone()))
            .consistent_read(true)
            .send()
            .await
            .unwrap();
        assert_eq!(read.item().unwrap()["id"].as_s().unwrap(), id);
        assert_eq!(read.item().unwrap()["value"].as_n().unwrap(), "1");
    }
}
