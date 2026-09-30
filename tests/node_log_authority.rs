use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use beyonddb::NodeLeasePublisher;
use cellule_runtime::{
    identity::{Digest, NodeId, SessionId},
    ltx::CellStorageLayout,
    node::{
        NODE_LOG_PROTOCOL_VERSION, NodeAdvertisement, NodeCapacity, NodeDirectory,
        NodeFailureDomain, durability::NodeLogAuthority, log::DurabilityGate,
    },
};
use cellule_store::Store;
use ed25519_dalek::SigningKey;
use object_store::{memory::InMemory, path::Path};
use tokio_util::sync::CancellationToken;

const FLEET: Digest = Digest::from_bytes([80; 32]);
const IMAGE: Digest = Digest::from_bytes([81; 32]);
const RELEASE: Digest = Digest::from_bytes([82; 32]);

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn advertisement(
    node: NodeId,
    session: SessionId,
    key: u8,
    capacity: NodeCapacity,
    now: i64,
    expires: i64,
) -> cellule_runtime::Result<NodeAdvertisement> {
    NodeAdvertisement::sign(
        node,
        session,
        format!("https://node-{key}.internal:8081"),
        FLEET,
        Digest::from_bytes([key; 32]),
        IMAGE,
        RELEASE,
        &SigningKey::from_bytes(&[key; 32]),
        1,
        now,
        expires,
        vec![Digest::from_bytes([86; 32])],
        vec![1],
        NodeFailureDomain::default(),
        capacity,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn published_log_authority_reconciles_heartbeat_and_fences_transitions() {
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("beyonddb-node-log-authority"),
        [42; 16],
    );
    let directory = NodeDirectory::new(layout, FLEET, IMAGE, RELEASE);
    let leader_node = NodeId::from_bytes([83; 16]);
    let leader_session = SessionId::from_bytes([90; 16]);
    let follower_node = NodeId::from_bytes([92; 16]);
    let follower_session = SessionId::from_bytes([93; 16]);
    let now = now_ms();
    directory
        .create(
            advertisement(
                follower_node,
                follower_session,
                95,
                NodeCapacity {
                    free_memory_bytes: 16 * 1024 * 1024,
                    free_disk_bytes: 1 << 30,
                    follower_free_bytes: 1 << 30,
                    job_credits: 8,
                    log_protocol: NODE_LOG_PROTOCOL_VERSION,
                    ..NodeCapacity::default()
                },
                now,
                now + 15_000,
            )
            .unwrap(),
            now,
        )
        .await
        .unwrap();
    let published = NodeLeasePublisher::new(directory.clone(), move |now, expires| {
        advertisement(
            leader_node,
            leader_session,
            85,
            NodeCapacity {
                free_memory_bytes: 16 * 1024 * 1024,
                free_disk_bytes: 1 << 30,
                job_credits: 8,
                ..NodeCapacity::default()
            },
            now,
            expires,
        )
    })
    .publish()
    .await
    .unwrap();
    let guard = published.guard();
    let authority = published.log_authority();
    assert_eq!(
        authority.recruit(1, 4096, 16).await.unwrap(),
        Some(vec![follower_node])
    );
    assert_eq!(
        authority.recruit(1, 4096, 16).await.unwrap(),
        Some(vec![follower_node])
    );
    assert!(authority.activate(2).await.is_err());

    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let task = tokio::spawn(async move { published.run(&run_cancellation).await });
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let observed = directory
                .load(leader_session, now_ms())
                .await
                .unwrap()
                .unwrap();
            if observed.advertisement().generation() > 2 {
                assert_eq!(observed.advertisement().log().unwrap().epoch(), 1);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    authority.activate(1).await.unwrap();
    authority.activate(1).await.unwrap();
    assert!(authority.recruit(1, 4096, 16).await.is_err());
    let gate = DurabilityGate::new(leader_session, leader_node, 1, [follower_node]).unwrap();
    let ticket = gate.issue(1).unwrap();
    assert_eq!(gate.prove_object(ticket).unwrap(), 1);
    authority.advance_coverage(1, 1).await.unwrap();
    authority.advance_coverage(1, 1).await.unwrap();
    let observed = directory
        .load(leader_session, now_ms())
        .await
        .unwrap()
        .unwrap();
    let log = observed.advertisement().log().unwrap();
    assert!(log.active());
    assert_eq!(log.tiered_through(), 1);

    let barrier = gate.begin_rotation().unwrap();
    authority.close(&barrier).await.unwrap();
    let observed = directory
        .load(leader_session, now_ms())
        .await
        .unwrap()
        .unwrap();
    assert!(observed.advertisement().log().is_none());
    let closed_generation = observed.advertisement().generation();
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let refreshed = directory
                .load(leader_session, now_ms())
                .await
                .unwrap()
                .unwrap();
            if refreshed.advertisement().generation() > closed_generation {
                assert!(refreshed.advertisement().log().is_none());
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    cancellation.cancel();
    task.await.unwrap().unwrap();
    guard.fence();
    assert!(matches!(
        authority.activate(1).await,
        Err(cellule_runtime::Error::Fenced)
    ));
}
