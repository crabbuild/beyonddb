use super::*;
use cellule_runtime::{
    codec::{BoundedEncoder, WireValue},
    identity::CellTarget,
    peer::{PeerOperation, decode_peer_reply, wire},
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Default)]
pub(super) struct CountedAuthority {
    inner: InMemory,
    pub(super) path: std::sync::Mutex<Option<object_store::path::Path>>,
    pub(super) reads: AtomicUsize,
    pub(super) all_reads: AtomicUsize,
    pub(super) creation_gate: std::sync::Mutex<Option<CreationGate>>,
    pub(super) publication_gate: std::sync::Mutex<Option<CreationGate>>,
}

#[derive(Clone, Debug)]
pub(super) struct CreationGate {
    pub(super) paths: std::collections::HashSet<object_store::path::Path>,
    pub(super) entered: Arc<tokio::sync::Semaphore>,
    pub(super) release: Arc<tokio::sync::Semaphore>,
}

impl std::fmt::Display for CountedAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CountedAuthority")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for CountedAuthority {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let gate = self
            .creation_gate
            .lock()
            .unwrap()
            .clone()
            .filter(|gate| {
                matches!(options.mode, object_store::PutMode::Create)
                    && gate.paths.contains(location)
            })
            .or_else(|| {
                self.publication_gate
                    .lock()
                    .unwrap()
                    .clone()
                    .filter(|gate| {
                        matches!(options.mode, object_store::PutMode::Update(_))
                            && gate.paths.contains(location)
                    })
            });
        if let Some(gate) = gate {
            gate.entered.add_permits(1);
            gate.release.acquire().await.unwrap().forget();
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.all_reads.fetch_add(1, Ordering::SeqCst);
        let counted = self.path.lock().unwrap().as_ref() == Some(location);
        if counted {
            self.reads.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_forward_cache_skips_authority_io_and_rejects_drained_owner() {
    for enabled in [false, true] {
        let store = Arc::new(CountedAuthority::default());
        let fixture =
            Fixture::with_store_capacity_and_peer_cache(1, store.clone(), 8, enabled).await;
        let remote = super::provisioning::Remote::new(&fixture).await;
        let handle = &fixture.data[0].0;
        let entry = handle.catalog().entry();
        let target = CellTarget::new(
            account_target("123456789012").unwrap().tenant(),
            beyonddb::APPLICATION_ID,
            entry.namespace(),
            entry.partition(),
        )
        .unwrap();
        let expected = wire::CellDescription {
            cell_id: handle.cell_id().as_bytes().to_vec(),
            incarnation: handle.incarnation().as_bytes().to_vec(),
            code: handle.code().as_bytes().to_vec(),
            schema: handle.schema(),
        };
        let transport = PeerHttpRoundTrip::new(
            Arc::new(BeyonddbPeerScope),
            CellAuthority::new(fixture.layout.clone()),
            fixture.directory.clone(),
            Arc::new(fixture.remote_tls.client_identity()),
            remote.session,
        );
        let signer = PeerSigner::new(
            remote.session,
            fixture.application.registry().release_digest(),
            fixture.remote_tls.signing_key().clone(),
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
            subject: remote
                .session
                .as_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect(),
            actions: vec!["beyonddb.cell.invoke".into()],
        };
        let destination = fixture
            .directory
            .load(fixture.session, now_ms())
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .clone();
        let mut encoder = BoundedEncoder::new(64).unwrap();
        Json(()).encode(&mut encoder).unwrap();
        let input = encoder.finish();
        let send = |authorized: bool| {
            let mut principal = principal.clone();
            if !authorized {
                principal.actions = vec!["beyonddb.cell.invalid".into()];
            }
            let now = now_ms();
            let request = signer
                .sign(
                    principal,
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
                        expected: Some(expected.clone()),
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
            let transport = &transport;
            let target = target.clone();
            let destination = destination.clone();
            async move {
                let reply = transport
                    .send_to_node(target, destination, request, 30_000)
                    .await?;
                Ok::<_, cellule_runtime::Error>(decode_peer_reply(&reply)?.outcome.unwrap())
            }
        };
        *store.path.lock().unwrap() =
            Some(fixture.layout.control_path(handle.cell_id().as_bytes()));
        let warm = send(true).await.unwrap();
        assert!(
            matches!(warm, wire::peer_reply::Outcome::Read(_)),
            "warm read: {warm:?}"
        );
        store.reads.store(0, Ordering::SeqCst);
        let started = std::time::Instant::now();
        for _ in 0..4 {
            assert!(matches!(
                send(true).await.unwrap(),
                wire::peer_reply::Outcome::Read(_)
            ));
        }
        let reads = store.reads.load(Ordering::SeqCst);
        println!(
            "peer cache enabled={enabled}: four signed SQL reads in {:?}, authority reads={reads}",
            started.elapsed()
        );
        assert_eq!(reads, if enabled { 0 } else { 4 });
        assert!(
            matches!(send(false).await.unwrap(), wire::peer_reply::Outcome::Error(error)
            if error.code == wire::error::Code::PermissionDenied as i32)
        );
        if enabled {
            tokio::time::sleep(std::time::Duration::from_millis(550)).await;
            store.reads.store(0, Ordering::SeqCst);
            assert!(matches!(
                send(true).await.unwrap(),
                wire::peer_reply::Outcome::Read(_)
            ));
            assert_eq!(
                store.reads.load(Ordering::SeqCst),
                0,
                "expired cache should resolve the still-resident actor without authority I/O"
            );
        }
        if enabled {
            fixture
                .client
                .query::<beyonddb::ReadPartitionState>(&target, None, Json(()))
                .await
                .unwrap();
        }
        handle.drain().await.unwrap();
        let drained = send(true).await;
        assert!(
            matches!(drained, Err(cellule_runtime::Error::CellNotActive)),
            "drained read: {drained:?}"
        );
        let owner = CellAuthority::new(fixture.layout.clone())
            .load(handle.cell_id())
            .await
            .unwrap()
            .unwrap();
        assert!(
            owner.value().owner.is_none(),
            "forwarded invocation must not acquire a drained Cell"
        );
        if enabled {
            fixture
                .client
                .query::<beyonddb::ReadPartitionState>(&target, None, Json(()))
                .await
                .unwrap();
            let restored = CellAuthority::new(fixture.layout.clone())
                .load(handle.cell_id())
                .await
                .unwrap()
                .unwrap();
            assert!(restored.value().owner.is_some());
            assert!(restored.value().root.is_some());
            assert_eq!(
                restored.value().state,
                cellule_runtime::control::ControlState::Serving
            );
        }
        *store.path.lock().unwrap() = None;
        remote.shutdown().await;
        fixture.shutdown().await;
    }
}
