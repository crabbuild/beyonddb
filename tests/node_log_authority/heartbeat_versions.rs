use super::*;
use cellule_store::{StorageObservation, StorageObserver, StorageOperation, StorageOutcome};
use std::sync::Mutex;

#[derive(Default)]
struct RefreshTrace {
    // Arm only after setup. No SDK or background requests share this store.
    active: Mutex<Option<(CancellationToken, Vec<StorageObservation>)>>,
}

impl RefreshTrace {
    fn arm(&self) -> CancellationToken {
        let done = CancellationToken::new();
        *self.active.lock().unwrap() = Some((done.clone(), Vec::new()));
        done
    }

    fn take(&self) -> Vec<StorageObservation> {
        self.active.lock().unwrap().take().unwrap().1
    }
}

impl StorageObserver for RefreshTrace {
    fn started(&self, _operation: StorageOperation) {}

    fn finished(&self, observation: StorageObservation) {
        if let Some((done, observations)) = self.active.lock().unwrap().as_mut() {
            // Freeze at this publication's successful PUT. A missed interval
            // can start the next heartbeat before the caller cancels the task.
            if done.is_cancelled() {
                return;
            }
            observations.push(observation);
            if observation.operation == StorageOperation::Put
                && observation.outcome == StorageOutcome::Success
            {
                done.cancel();
            }
        }
    }
}

async fn refresh_after_coverage(external_update: bool, slow_refresh: bool) {
    use object_store::throttle::{ThrottleConfig, ThrottledStore};

    let trace = Arc::new(RefreshTrace::default());
    let store = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig::default(),
    ));
    let layout = CellStorageLayout::new(
        Store::new(store.clone()).with_storage_observer(trace.clone()),
        Path::from("heartbeat-after-coverage"),
        [49; 16],
    );
    let directory = NodeDirectory::new(layout, FLEET, IMAGE, RELEASE);
    let leader_node = NodeId::from_bytes([110; 16]);
    let leader_session = SessionId::from_bytes([111; 16]);
    let follower_node = NodeId::from_bytes([112; 16]);
    let now = now_ms();
    directory
        .create(
            advertisement(
                follower_node,
                SessionId::from_bytes([113; 16]),
                114,
                NodeCapacity {
                    free_memory_bytes: 16 << 20,
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
            115,
            NodeCapacity::default(),
            now,
            expires,
        )
    })
    .publish()
    .await
    .unwrap();
    let guard = published.guard();
    let authority = published.log_authority();
    authority.recruit(1, 4096, 16).await.unwrap();
    authority.activate(1).await.unwrap();
    authority.advance_coverage(1, 1).await.unwrap();
    let mut before = directory
        .load(leader_session, now_ms())
        .await
        .unwrap()
        .unwrap();
    if external_update {
        // Bypass the publisher's adapter: its ETag hint must not become authority.
        before = directory
            .advance_log_coverage(&before, 2, now_ms())
            .await
            .unwrap();
    }
    if slow_refresh {
        // One 6.5-second CAS leaves room for repeated renewals. A stale CAS,
        // reload and second CAS exceed the first lease's 12-second remainder.
        store.config_mut(|config| config.wait_put_per_call = Duration::from_millis(6_500));
    }
    let done = trace.arm();
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let heartbeat = tokio::spawn(async move { published.run(&run_cancellation).await });
    let mut completed = tokio::time::timeout(Duration::from_secs(18), async {
        tokio::select! {
            () = done.cancelled() => true,
            () = guard.wait_fenced() => false,
        }
    })
    .await;
    let mut observations = Vec::new();
    if slow_refresh && matches!(completed, Ok(true)) {
        observations.extend(trace.take());
        let second = trace.arm();
        completed = tokio::time::timeout(Duration::from_secs(18), async {
            tokio::select! {
                () = second.cancelled() => true,
                () = guard.wait_fenced() => false,
            }
        })
        .await;
    }
    cancellation.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(2), heartbeat).await;
    observations.extend(trace.take());
    let puts = observations
        .iter()
        .filter(|o| o.operation == StorageOperation::Put)
        .count();
    let gets = observations
        .iter()
        .filter(|o| o.operation == StorageOperation::Get)
        .count();
    let conflicts = observations
        .iter()
        .filter(|o| o.outcome == StorageOutcome::Conflict)
        .count();
    eprintln!(
        "heartbeat refresh after coverage external={external_update}, slow={slow_refresh}: puts={puts}, gets={gets}, conflicts={conflicts}, renewed={completed:?}"
    );
    let stopped = stopped.unwrap().unwrap();
    assert!(
        completed.unwrap(),
        "avoidable stale CAS exhausted the serving lease: {stopped:?}"
    );
    stopped.unwrap();
    let current = directory
        .load(leader_session, now_ms())
        .await
        .unwrap()
        .unwrap();
    assert!(current.advertisement().generation() > before.advertisement().generation());
    assert!(current.advertisement().expires_at_ms() > before.advertisement().expires_at_ms());
    let log = current.advertisement().log().unwrap();
    assert!(log.active());
    assert_eq!(log.members(), &[follower_node]);
    assert_eq!(log.tiered_through(), if external_update { 2 } else { 1 });
    guard.check().unwrap();
    guard.fence();
    if external_update {
        assert_eq!((puts, gets, conflicts), (2, 1, 1));
    } else {
        assert_eq!(
            (puts, gets, conflicts),
            (if slow_refresh { 2 } else { 1 }, 0, 0),
            "known log coverage must not force a stale heartbeat CAS and reload"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_uses_completed_local_log_version_without_redundant_io() {
    refresh_after_coverage(false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_rebases_unseen_log_version_through_authoritative_cas() {
    refresh_after_coverage(true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_survives_slow_cas_after_completed_log_coverage() {
    refresh_after_coverage(false, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_authority_read_cannot_regress_the_next_heartbeat_etag() {
    let trace = Arc::new(RefreshTrace::default());
    let store = Arc::new(super::stalled_log_read::HeldReadStore::default());
    let layout = CellStorageLayout::new(
        Store::new(store.clone()).with_storage_observer(trace.clone()),
        Path::from("heartbeat-late-observation"),
        [49; 16],
    );
    let directory = NodeDirectory::new(layout.clone(), FLEET, IMAGE, RELEASE);
    let leader_node = NodeId::from_bytes([110; 16]);
    let leader_session = SessionId::from_bytes([111; 16]);
    let follower_node = NodeId::from_bytes([112; 16]);
    let now = now_ms();
    directory
        .create(
            advertisement(
                follower_node,
                SessionId::from_bytes([113; 16]),
                114,
                NodeCapacity {
                    free_memory_bytes: 16 << 20,
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
            115,
            NodeCapacity::default(),
            now,
            expires,
        )
    })
    .publish()
    .await
    .unwrap();
    let guard = published.guard();
    let authority = published.log_authority();
    authority.recruit(1, 4096, 16).await.unwrap();
    authority.activate(1).await.unwrap();
    authority.advance_coverage(1, 1).await.unwrap();

    *store.held_path.lock().unwrap() = Some(layout.node_path(leader_session.as_bytes()));
    let check = authority.clone();
    let rotation = tokio::spawn(async move { check.rotation_required(1, 16).await });
    let entered = tokio::time::timeout(Duration::from_secs(2), store.entered.cancelled()).await;
    let first = trace.arm();
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let heartbeat = tokio::spawn(async move { published.run(&run_cancellation).await });
    let refreshed = tokio::time::timeout(Duration::from_secs(6), first.cancelled()).await;
    // The CAS observer fires before the refresh future returns. Give the
    // publisher its next poll before delivering the captured older response.
    tokio::time::sleep(Duration::from_millis(50)).await;
    trace.take();
    store.release.cancel();
    let checked = tokio::time::timeout(Duration::from_secs(2), rotation).await;
    let second = trace.arm();
    let completed = tokio::time::timeout(Duration::from_secs(6), second.cancelled()).await;
    cancellation.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(2), heartbeat).await;
    let observations = trace.take();
    entered.unwrap();
    refreshed.unwrap();
    assert!(!checked.unwrap().unwrap().unwrap());
    completed.unwrap();
    stopped.unwrap().unwrap().unwrap();
    let puts = observations
        .iter()
        .filter(|o| o.operation == StorageOperation::Put)
        .count();
    let gets = observations
        .iter()
        .filter(|o| o.operation == StorageOperation::Get)
        .count();
    let conflicts = observations
        .iter()
        .filter(|o| o.outcome == StorageOutcome::Conflict)
        .count();
    eprintln!(
        "heartbeat after delayed authority read: puts={puts}, gets={gets}, conflicts={conflicts}"
    );
    assert_eq!((puts, gets, conflicts), (1, 0, 0));
    let current = directory
        .load(leader_session, now_ms())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.advertisement().log().unwrap().tiered_through(), 1);
    assert!(current.advertisement().log().unwrap().active());
    guard.check().unwrap();
    guard.fence();
}
