//! Native lease telemetry plus an authenticated, generation-bound peer probe.
use crate::*;
use cellar::{
    bucket::{Bucket, StorageBackend},
    management::{FleetClient, NodeAction, PageRequest},
};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker, aws CLI, and the pinned RustFS GA image"]
async fn process_publishes_measured_capacity_and_proves_current_peer_identity() {
    let mut fixture = process_fixture(3).await;
    fixture.sdk.list_tables().send().await.unwrap();
    let config: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.config).unwrap()).unwrap();
    let store = Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_bucket_name("beyonddb-test")
            .with_region("us-east-1")
            .with_endpoint(format!("http://{}", fixture.s3))
            .with_allow_http(true)
            .with_access_key_id("crab")
            .with_secret_access_key("crab")
            .build()
            .unwrap(),
    );
    let bucket = Bucket::from_clients(
        store.clone(),
        store.clone(),
        store,
        StorageBackend::S3,
        "beyonddb-test".into(),
        config["fleet"]["prefix"].as_str().unwrap().into(),
    )
    .unwrap();
    // The embedding server supplies its key file explicitly. Provision the same
    // test key for Cellar's management client, which loads operator credentials
    // from the bucket. This does not modify any owner or lease record.
    let key = fs::read(fixture.root.path().join("peer.key")).unwrap();
    let secret = |key: &[u8]| {
        serde_json::to_vec(&json!({
            "version": 1, "key": key.iter().map(|byte| format!("{byte:02x}")).collect::<String>()
        }))
        .unwrap()
    };
    bucket
        .put("fleet/peer-auth.json", secret(&key))
        .await
        .unwrap();
    let observer = FleetClient::new(bucket.clone(), false);
    let deadline = Instant::now() + Duration::from_secs(15);
    let observed = loop {
        let page = observer
            .nodes(PageRequest {
                limit: Some(10),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.coverage, "complete");
        assert!(page.next_cursor.is_none() && page.warnings.is_empty());
        assert_eq!(page.items.len(), 1);
        let node = page.items.into_iter().next().unwrap();
        assert_eq!(node.id, config["node_name"].as_str().unwrap());
        assert_eq!(node.address, fixture.peer.to_string());
        assert!(node.lease_current);
        assert_eq!(node.protocol, cellar::peer_auth::PROTOCOL_VERSION);
        if node.load["owned_cells"].as_u64().is_some_and(|n| n >= 2)
            && node.load["rss_bytes"].as_u64().is_some_and(|n| n > 1)
        {
            break node;
        }
        assert!(
            Instant::now() < deadline,
            "no measured native capacity: {}",
            fs::read_to_string(&fixture.log).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let now = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let sampled = observed.load["sampled_ms"].as_u64().unwrap();
    assert!(sampled <= now && now - sampled < 15_000);
    assert!(
        observed.load["resident_cells"].as_u64().unwrap()
            <= config["max_active_cells"].as_u64().unwrap()
    );
    assert!(observed.load["fd_limit"].as_u64().unwrap() > 0);
    assert!(
        observed.load["open_fds"].as_u64().unwrap() <= observed.load["fd_limit"].as_u64().unwrap()
    );
    assert_eq!(observed.load["draining"], false);
    // FleetClient verifies the current lease generation, signed challenge and
    // node identity before sending its authenticated management request.
    let state = observer
        .node_action(&observed.id, &observed.generation, NodeAction::State)
        .await
        .unwrap();
    assert_eq!(state["node"], observed.id);
    assert_eq!(state["generation"], observed.generation);
    assert_eq!(state["authoritative"], true);
    assert_eq!(state["fleet_ready"], true);
    assert!(
        observer
            .node_action(&observed.id, "stale-generation", NodeAction::State)
            .await
            .is_err()
    );
    // A current lease is insufficient when the caller cannot authenticate the
    // probe. Change only this fixture's operator credential, never node state.
    bucket
        .put("fleet/peer-auth.json", secret(&[0xee; 32]))
        .await
        .unwrap();
    let rejected = observer
        .node_action(&observed.id, &observed.generation, NodeAction::State)
        .await
        .unwrap_err();
    assert!(format!("{rejected:#}").contains("rejected the signed probe"));
    bucket
        .put("fleet/peer-auth.json", secret(&key))
        .await
        .unwrap();
    let state = observer
        .node_action(&observed.id, &observed.generation, NodeAction::State)
        .await
        .unwrap();
    assert_eq!(state["authoritative"], true);
    stop(&mut fixture.child, &fixture.log);
}
