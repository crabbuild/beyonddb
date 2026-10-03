use super::*;
use cellule_runtime::cell::catalog::CellCatalog;

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_sdk_authentication_recovers_expired_published_credential_owner() {
    let mut recovered = Vec::new();
    for enabled in [false, true] {
        let fixture =
            Fixture::with_store_capacity_and_peer_cache(1, Arc::new(InMemory::new()), 8, enabled)
                .await;
        let remote = provisioning::Remote::new(&fixture).await;
        let target = beyonddb::credential_target(ACCESS_KEY).unwrap();
        let original = fixture
            .provisioner
            .admit_credential(ACCESS_KEY)
            .await
            .unwrap();
        original.drain().await.unwrap();
        let serving = remote
            .provisioner
            .admit_credential(ACCESS_KEY)
            .await
            .unwrap();
        let before = serving.owner_fence();
        let sdk = provisioning::sdk_without_retries(&fixture);
        let expected = fixture.data[0].1.clone();
        let live_read = sdk
            .get_item()
            .table_name("Residency")
            .key("id", expected["id"].clone())
            .consistent_read(true)
            .send()
            .await
            .unwrap();
        assert_eq!(live_read.item, Some(expected.clone()));
        let unknown = (0..256)
            .map(|index| format!("AKIAUNKNOWN{index:08}"))
            .find(|key| beyonddb::credential_target(key).unwrap().cell_id() != target.cell_id())
            .unwrap();
        let unknown_target = beyonddb::credential_target(&unknown).unwrap();
        let unknown_sdk = aws_sdk_dynamodb::Client::from_conf(
            sdk.config()
                .to_builder()
                .credentials_provider(Credentials::new(
                    unknown,
                    "unrecognized-secret",
                    None,
                    None,
                    "unknown-key-test",
                ))
                .build(),
        );
        let unknown_error = unknown_sdk
            .get_item()
            .table_name("Residency")
            .key("id", expected["id"].clone())
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            unknown_error.as_service_error().unwrap().code(),
            Some("UnrecognizedClientException")
        );
        remote.stop_listener();
        assert!(
            sdk.get_item()
                .table_name("Residency")
                .key("id", expected["id"].clone())
                .consistent_read(true)
                .send()
                .await
                .is_err()
        );
        let authority = CellAuthority::new(fixture.layout.clone());
        let live = authority.load(target.cell_id()).await.unwrap().unwrap();
        assert!(
            fixture
                .directory
                .is_live(remote.session, now_ms())
                .await
                .unwrap()
        );
        assert_eq!(live.value().incarnation, before.incarnation);
        assert_eq!(live.value().epoch, before.epoch);
        assert_eq!(live.value().owner.as_ref().unwrap().session, remote.session);
        remote.lease.cancel();
        provisioning::wait_for_expiry(&fixture, remote.session).await;
        let item = sdk
            .get_item()
            .table_name("Residency")
            .key("id", expected["id"].clone())
            .consistent_read(true)
            .send()
            .await;
        let after = authority
            .load(target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .clone();
        let unknown_control = authority.load(unknown_target.cell_id()).await.unwrap();
        let unknown_catalog = CellCatalog::new(fixture.layout.clone(), unknown_target.tenant())
            .lookup(unknown_target.cell_id())
            .await
            .unwrap();
        let session = fixture.session;
        assert!(matches!(
            remote.node.shutdown().await,
            Ok(()) | Err(cellule_runtime::Error::Fenced)
        ));
        fixture.shutdown().await;
        assert!(unknown_control.is_none());
        assert!(unknown_catalog.is_none());
        recovered.push((enabled, item, expected, after, before, session));
    }
    for (enabled, item, expected, after, before, session) in recovered {
        assert_eq!(
            item.unwrap_or_else(|error| panic!("credential recovery cache={enabled}: {error:?}"))
                .item,
            Some(expected)
        );
        assert_eq!(after.incarnation, before.incarnation);
        assert!(after.epoch > before.epoch);
        assert_eq!(after.owner.as_ref().unwrap().session, session);
        assert!(after.root.is_some());
    }
}
