use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
struct RefusalGate {
    armed: AtomicBool,
    refused: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn credential_query_waits_for_peer_memory_and_reads_revocation_after_release() {
    const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
    let gate = Arc::new(RefusalGate::default());
    let intercepted = gate.clone();
    let fixture = Fixture::with_store_capacity_cache_and_router(
        1,
        Arc::new(InMemory::new()),
        8,
        false,
        move |router| {
            router.layer(axum::middleware::from_fn(
                move |request: axum::extract::Request, next: axum::middleware::Next| {
                    let gate = intercepted.clone();
                    async move {
                        let reply = next.run(request).await;
                        if reply.status() == axum::http::StatusCode::SERVICE_UNAVAILABLE
                            && gate.armed.swap(false, Ordering::SeqCst)
                        {
                            // Hold the actual pre-dispatch memory refusal until
                            // authority proves this owner has released the Cell.
                            gate.refused.notify_one();
                            tokio::time::timeout(
                                std::time::Duration::from_secs(10),
                                gate.release.notified(),
                            )
                            .await
                            .unwrap();
                        }
                        reply
                    }
                },
            ))
        },
    )
    .await;
    let remote = provisioning::Remote::new(&fixture).await;
    let credentials =
        CellCredentialStore::new(remote.client(&fixture), fixture.layout.clone(), [38; 32]);
    let credential = fixture
        .provisioner
        .admit_credential(ACCESS_KEY)
        .await
        .unwrap();
    let runtime = fixture.node.runtime();
    let stats = runtime.stats();
    let memory = runtime
        .try_reserve_node_bytes(stats.retained_capacity_bytes() - stats.retained_bytes())
        .unwrap();
    gate.armed.store(true, Ordering::SeqCst);
    let lookup = credentials.lookup_credential(ACCESS_KEY);
    tokio::pin!(lookup);
    tokio::select! {
        result = &mut lookup => panic!("lookup completed before the peer refusal: {}", result.is_ok()),
        () = gate.refused.notified() => {}
    }
    drop(memory);
    credential.drain().await.unwrap();
    let target = beyonddb::credential_target(ACCESS_KEY).unwrap();
    assert!(
        CellAuthority::new(fixture.layout.clone())
            .load(target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .owner
            .is_none()
    );
    gate.release.notify_one();
    let record = tokio::time::timeout(std::time::Duration::from_secs(10), lookup)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(record.account_id, "123456789012");
    assert!(record.is_active);
    assert!(credentials.revoke_credential(ACCESS_KEY).await.unwrap());
    assert!(
        !credentials
            .lookup_credential(ACCESS_KEY)
            .await
            .unwrap()
            .unwrap()
            .is_active
    );
    remote.shutdown().await;
    fixture.shutdown().await;
}
