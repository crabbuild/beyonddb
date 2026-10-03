use super::forward_cache::{CountedAuthority, CreationGate};
use super::*;
use cellule_runtime::{
    cell::catalog::CatalogRole,
    codec::{BoundedEncoder, WireValue},
    identity::{CellTarget, RequestId},
    peer::{PeerOperation, PeerRoundTrip, decode_peer_reply, wire},
};
use futures_util::TryStreamExt;
use object_store::ObjectStore;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_forward_cache_keeps_hydrating_owners_without_authority_reads() {
    let store = Arc::new(CountedAuthority::default());
    let fixture = Fixture::with_store_capacity_and_peer_cache(1, store.clone(), 8, true).await;
    let original = &fixture.data[0].0;
    let target = CellTarget::new(
        account_target("123456789012").unwrap().tenant(),
        beyonddb::APPLICATION_ID,
        original.catalog().entry().namespace(),
        original.catalog().entry().partition(),
    )
    .unwrap();
    // A real published 8 MiB database leaves background hydration to do after
    // cold activation. The SDK item and its normal partition schema remain.
    let now = now_ms();
    original
        .execute(
            cellule_runtime::MutationIdentity {
                request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
                issued_at_ms: now,
                expires_at_ms: now + 60_000,
            },
            Digest::from_bytes([202; 32]),
            now,
            1_024,
            64,
            |transaction| {
                transaction.execute_batch(
                    "CREATE TABLE hydration_payload(value BLOB NOT NULL); \
                     WITH RECURSIVE numbers(value) AS ( \
                       SELECT 1 UNION ALL SELECT value + 1 FROM numbers WHERE value < 512 \
                     ) \
                     INSERT INTO hydration_payload(value) SELECT zeroblob(16384) FROM numbers;",
                )?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    vec![],
                ))
            },
        )
        .await
        .unwrap();
    original.drain().await.unwrap();
    let remote = provisioning::Remote::with_cache(&fixture).await;
    let table = fixture
        .client
        .query::<DescribeTable>(
            &account_target("123456789012").unwrap(),
            None,
            Json("Residency".into()),
        )
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    let route = crate::single_leaf_route(
        &fixture.client,
        &account_target("123456789012").unwrap(),
        &table.id,
    )
    .await
    .unwrap();
    let restored = remote
        .provisioner
        .admit_existing_partition("123456789012", &table.id, &route.partitions[0].partition_id)
        .await
        .unwrap();
    assert_eq!(restored.cell_id(), target.cell_id());
    let body_paths = store
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .filter(|object| {
            object.location.as_ref().ends_with(".ltx")
                || object.location.as_ref().ends_with(".bundle")
        })
        .map(|object| object.location)
        .collect();
    let gate = CreationGate {
        paths: body_paths,
        entered: Arc::new(tokio::sync::Semaphore::new(0)),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    *store.read_gate.lock().unwrap() = Some(gate.clone());
    let reached = tokio::time::timeout(Duration::from_secs(3), gate.entered.acquire()).await;
    let observation = if let Ok(permit) = reached {
        permit.unwrap().forget();
        let runtime = remote.node.runtime();
        let active = runtime
            .active_handle(&target, CatalogRole::Sql)
            .await
            .unwrap();
        let resident = runtime
            .resident_handle(&target, CatalogRole::Sql)
            .await
            .unwrap();
        let jobs = runtime.stats().hydration_jobs();
        // Sign with the ingress identity while targeting the exact known remote
        // node. This isolates receiver authority reads from sender discovery.
        let tls = LoadedPeerTls::load(
            &fixture._files.path().join("owner.crt"),
            &fixture._files.path().join("owner.key"),
            &fixture._files.path().join("ca.crt"),
            "localhost",
        )
        .unwrap();
        let peers = PeerHttpRoundTrip::new(
            Arc::new(BeyonddbPeerScope),
            CellAuthority::new(fixture.layout.clone()),
            fixture.directory.clone(),
            Arc::new(tls.client_identity()),
            fixture.session,
        );
        let signer = PeerSigner::new(
            fixture.session,
            fixture.application.registry().release_digest(),
            tls.signing_key().clone(),
        );
        let principal = PeerPrincipal {
            issuer: format!(
                "beyonddb-peer:{}",
                fixture
                    .directory
                    .fleet()
                    .as_bytes()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            ),
            subject: fixture
                .session
                .as_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect(),
            actions: vec!["beyonddb.cell.invoke".into()],
        };
        let destination = fixture
            .directory
            .load(remote.session, now_ms())
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .clone();
        *store.path.lock().unwrap() =
            Some(fixture.layout.control_path(target.cell_id().as_bytes()));
        store.reads.store(0, Ordering::SeqCst);
        let mut encoder = BoundedEncoder::new(64).unwrap();
        Json(()).encode(&mut encoder).unwrap();
        let input = encoder.finish();
        let mut outcomes = Vec::new();
        for request_index in 0..5 {
            if request_index == 4 {
                tokio::time::sleep(Duration::from_millis(550)).await;
            }
            let now = now_ms();
            let request = signer
                .sign(
                    principal.clone(),
                    now,
                    now + 60_000,
                    30_000,
                    PeerOperation::Read(wire::ReadRequest {
                        target: Some(wire::Target {
                            tenant_id: target.tenant().as_bytes().to_vec(),
                            application_id: target.application().as_bytes().to_vec(),
                            namespace_id: target.namespace().as_bytes().to_vec(),
                            partition: target.partition().to_vec(),
                        }),
                        timeout_ms: 30_000,
                        minimum: None,
                        expected: Some(wire::CellDescription {
                            cell_id: restored.cell_id().as_bytes().to_vec(),
                            incarnation: restored.incarnation().as_bytes().to_vec(),
                            code: restored.code().as_bytes().to_vec(),
                            schema: restored.schema(),
                        }),
                        operation: Some(wire::read_request::Operation::CellQuery(
                            wire::CellQuery {
                                query_id: 4,
                                codec_version: 1,
                                input: input.clone(),
                            },
                        )),
                    }),
                )
                .unwrap();
            outcomes.push(
                tokio::time::timeout(
                    Duration::from_secs(1),
                    peers.send_to_node(target.clone(), destination.clone(), request, 30_000),
                )
                .await,
            );
        }
        Some((
            active.map(|handle| handle.owner_fence()),
            resident.is_some(),
            jobs,
            outcomes,
            store.reads.load(Ordering::SeqCst),
        ))
    } else {
        None
    };
    *store.read_gate.lock().unwrap() = None;
    gate.release.add_permits(1);
    *store.path.lock().unwrap() = None;
    let expected = fixture.data[0].1.clone();
    let item = provisioning::sdk_without_retries(&fixture)
        .get_item()
        .table_name("Residency")
        .key("id", expected["id"].clone())
        .consistent_read(true)
        .send()
        .await;
    let fence = restored.owner_fence();
    remote.shutdown().await;
    fixture.shutdown().await;
    let (active, resident, jobs, outcomes, reads) =
        observation.expect("cold owner did not reach held background hydration");
    assert_eq!(active, Some(fence));
    assert!(!resident);
    assert_eq!(jobs, 1);
    for outcome in outcomes {
        let reply = outcome
            .expect("forwarded read waited for hydration")
            .unwrap();
        let decoded = decode_peer_reply(&reply).unwrap();
        assert!(
            matches!(decoded.outcome, Some(wire::peer_reply::Outcome::Read(_))),
            "forwarded outcome: {decoded:?}"
        );
    }
    assert_eq!(item.unwrap().item, Some(expected));
    println!("five signed hydrating-owner reads including expired cache: authority reads={reads}");
    assert_eq!(reads, 0, "hydrating owner fell back to metadata lookup");
}
