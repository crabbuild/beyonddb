//! Pinned mTLS transport for Cellule's follower-log operations.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::body::Bytes;
use cellule_peer_http::PeerTlsClient;
use cellule_runtime::{
    Error, NodeLeaseGuard, Result,
    follower::{FollowerReceipt, FollowerStore, FollowerTailPage},
    identity::{Digest, NodeId, SessionId},
    node::{
        NodeAdvertisement, NodeDirectory,
        log_transport::{AppendRequest, NodeLogTransport, RetireRequest, SealRequest, TailRequest},
    },
};
use futures_util::{StreamExt, future::BoxFuture};
use reqwest::{StatusCode, Url, header};

use super::{
    node_lease::unix_time_ms,
    node_log_receiver::{self, Operation, WireRequest},
};

const PEER_CACHE_MS: i64 = 500;
const MAX_CACHED_PEERS: usize = 128;
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(32);
const MAX_TOTAL_TAIL_BYTES: usize = 64 * 1024 * 1024;
const MAX_TAIL_PAGES: usize = 4096;

/// Outbound follower-log transport bound to one live node session.
///
/// The receiver rechecks directory authority for every operation. A short
/// sender cache avoids resolving the same enrolled member for each commit.
#[derive(Clone)]
pub struct PeerNodeLogTransport {
    directory: NodeDirectory,
    tls: PeerTlsClient,
    session: SessionId,
    node: NodeId,
    guard: NodeLeaseGuard,
    local_store: Option<Arc<FollowerStore>>,
    peers: Arc<Mutex<VecDeque<CachedPeer>>>,
}

#[derive(Clone)]
struct CachedPeer {
    member: NodeId,
    session: SessionId,
    certificate: Digest,
    public_key: [u8; 32],
    verified_at_ms: i64,
    expires_at_ms: i64,
    endpoint: Url,
    client: reqwest::Client,
}

impl PeerNodeLogTransport {
    pub(super) const fn session(&self) -> SessionId {
        self.session
    }

    pub(super) const fn node(&self) -> NodeId {
        self.node
    }

    /// Creates a transport that authenticates each follower against the fleet directory.
    #[must_use]
    pub fn new(
        directory: NodeDirectory,
        tls: PeerTlsClient,
        session: SessionId,
        node: NodeId,
        guard: NodeLeaseGuard,
    ) -> Self {
        Self {
            directory,
            tls,
            session,
            node,
            guard,
            local_store: None,
            peers: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Allow a recovery claimant to seal/read its own persistent follower lane.
    ///
    /// Local recovery still requires the directory's fenced-owner claim.
    #[must_use]
    pub fn with_local_follower_store(mut self, store: Arc<FollowerStore>) -> Self {
        self.local_store = Some(store);
        self
    }

    async fn local_recovery_store(
        &self,
        member: NodeId,
        leader: SessionId,
        epoch: u64,
    ) -> Result<&FollowerStore> {
        self.guard.check()?;
        if member != self.node {
            return Err(Error::PeerAuthorization("local follower member differs"));
        }
        let store = self
            .local_store
            .as_deref()
            .ok_or(Error::Peer("local follower store is unavailable"))?;
        self.directory
            .authorize_log_recovery(leader, self.session, member, epoch, unix_time_ms()?)
            .await?;
        self.guard.check()?;
        Ok(store)
    }

    async fn peer(&self, member: NodeId) -> Result<CachedPeer> {
        self.guard.check()?;
        if member == self.node {
            return Err(Error::PeerAuthorization("follower is the local node"));
        }
        let now_ms = unix_time_ms()?;
        if let Some(peer) = self
            .peers
            .lock()
            .map_err(|_| Error::Peer("node-log peer cache is poisoned"))?
            .iter()
            .find(|peer| {
                peer.member == member
                    && now_ms.saturating_sub(peer.verified_at_ms) < PEER_CACHE_MS
                    && now_ms < peer.expires_at_ms
            })
            .cloned()
        {
            return Ok(peer);
        }
        let advertisement = self
            .directory
            .resolve_node(member, now_ms)
            .await?
            .ok_or(Error::Peer("follower has no live advertisement"))?;
        self.guard.check()?;
        self.refresh_peer(member, advertisement, now_ms)
    }

    fn refresh_peer(
        &self,
        member: NodeId,
        advertisement: NodeAdvertisement,
        now_ms: i64,
    ) -> Result<CachedPeer> {
        let certificate = advertisement.certificate();
        let public_key = advertisement.verifying_key()?.to_bytes();
        let endpoint = Url::parse(advertisement.endpoint()).map_err(transport_error)?;
        if endpoint.scheme() != "https" || endpoint.host_str().is_none() {
            return Err(Error::PeerAuthorization("follower endpoint is not HTTPS"));
        }
        let mut peers = self
            .peers
            .lock()
            .map_err(|_| Error::Peer("node-log peer cache is poisoned"))?;
        let previous = peers.iter().find(|peer| {
            peer.member == member
                && peer.session == advertisement.session()
                && peer.certificate == certificate
                && peer.public_key == public_key
                && peer.endpoint == endpoint
        });
        let client = match previous {
            Some(peer) => peer.client.clone(),
            None => self
                .tls
                .client(certificate, public_key)
                .map_err(transport_error)?,
        };
        let peer = CachedPeer {
            member,
            session: advertisement.session(),
            certificate,
            public_key,
            verified_at_ms: now_ms,
            expires_at_ms: advertisement.expires_at_ms(),
            endpoint,
            client,
        };
        peers.retain(|cached| cached.member != member);
        if peers.len() == MAX_CACHED_PEERS {
            peers.pop_front();
        }
        peers.push_back(peer.clone());
        Ok(peer)
    }

    async fn send(&self, member: NodeId, request: WireRequest, tail: bool) -> Result<Bytes> {
        let encoded = request
            .encode()
            .map_err(|()| Error::Peer("invalid node-log request"))?;
        let peer = self.peer(member).await?;
        let url = peer
            .endpoint
            .join("internal/node-log/v1")
            .map_err(transport_error)?;
        self.guard.check()?;
        let response = peer
            .client
            .post(url)
            .header(header::CONTENT_TYPE, node_log_receiver::MEDIA_TYPE)
            .header(header::CACHE_CONTROL, "no-store")
            .header(header::CONTENT_LENGTH, encoded.len())
            .timeout(RESPONSE_TIMEOUT)
            .body(encoded)
            .send()
            .await
            .map_err(|source| {
                if source.is_connect() {
                    transport_error(source)
                } else {
                    unknown_error(source)
                }
            })?;
        self.guard.check()?;
        match response.status() {
            StatusCode::OK => {}
            StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED => {
                return Err(Error::PeerAuthorization(
                    "follower rejected the peer identity",
                ));
            }
            StatusCode::CONFLICT => {
                return Err(Error::Node("follower rejected the log authority"));
            }
            StatusCode::BAD_REQUEST
            | StatusCode::UNSUPPORTED_MEDIA_TYPE
            | StatusCode::LENGTH_REQUIRED => {
                return Err(Error::Peer("follower rejected the node-log request"));
            }
            status if status.is_server_error() => {
                return Err(Error::PeerTransportUnknown {
                    context: "follower may have accepted the node-log operation",
                    source: Box::new(Error::Peer("follower returned a server error")),
                });
            }
            _ => return Err(Error::Peer("follower rejected the node-log request")),
        }
        if response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            != Some(node_log_receiver::MEDIA_TYPE)
        {
            return Err(unknown_error(Error::Peer(
                "follower response type is invalid",
            )));
        }
        let limit = if tail {
            node_log_receiver::MAX_RESPONSE_BYTES
        } else {
            21
        };
        if response
            .content_length()
            .is_some_and(|length| length > limit as u64)
        {
            return Err(unknown_error(Error::Peer(
                "follower response exceeds the byte limit",
            )));
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(unknown_error)?;
            if body.len().saturating_add(chunk.len()) > limit {
                return Err(unknown_error(Error::Peer(
                    "follower response exceeds the byte limit",
                )));
            }
            body.extend_from_slice(&chunk);
        }
        self.guard.check()?;
        Ok(Bytes::from(body))
    }

    async fn receipt(&self, member: NodeId, request: WireRequest) -> Result<FollowerReceipt> {
        let response = self.send(member, request, false).await?;
        node_log_receiver::decode_receipt(response)
            .map_err(|()| unknown_error(Error::Peer("follower receipt is invalid")))
    }
}

impl NodeLogTransport for PeerNodeLogTransport {
    fn append<'a>(
        &'a self,
        member: NodeId,
        request: AppendRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async move {
            if request.leader_session != self.session {
                return Err(Error::PeerAuthorization(
                    "append leader is not the local session",
                ));
            }
            self.receipt(
                member,
                WireRequest {
                    caller: self.session,
                    leader: request.leader_session,
                    epoch: request.log_epoch,
                    argument: request.covered_through,
                    operation: Operation::Append(request.frames),
                },
            )
            .await
        })
    }

    fn seal<'a>(
        &'a self,
        member: NodeId,
        request: SealRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async move {
            if member == self.node {
                let store = self
                    .local_recovery_store(member, request.leader_session, request.log_epoch)
                    .await?;
                let receipt = store
                    .seal(request.leader_session, request.log_epoch)
                    .await?;
                self.guard.check()?;
                return Ok(receipt);
            }
            self.receipt(
                member,
                WireRequest {
                    caller: self.session,
                    leader: request.leader_session,
                    epoch: request.log_epoch,
                    argument: 0,
                    operation: Operation::Seal,
                },
            )
            .await
        })
    }

    fn retire<'a>(
        &'a self,
        member: NodeId,
        request: RetireRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async move {
            if request.leader_session != self.session {
                return Err(Error::PeerAuthorization(
                    "retire leader is not the local session",
                ));
            }
            self.receipt(
                member,
                WireRequest {
                    caller: self.session,
                    leader: request.leader_session,
                    epoch: request.log_epoch,
                    argument: request.covered_through,
                    operation: Operation::Retire,
                },
            )
            .await
        })
    }

    fn tail<'a>(
        &'a self,
        member: NodeId,
        request: TailRequest,
    ) -> BoxFuture<'a, Result<Vec<Bytes>>> {
        Box::pin(async move {
            let mut first_sequence = request.first_sequence;
            let mut frames = Vec::new();
            let mut total = 0_usize;
            for _ in 0..MAX_TAIL_PAGES {
                let page = self
                    .tail_page(
                        member,
                        TailRequest {
                            first_sequence,
                            ..request
                        },
                    )
                    .await?;
                for frame in page.frames {
                    total = total
                        .checked_add(frame.len())
                        .ok_or(Error::Capacity("follower tail exceeds byte limit"))?;
                    if total > MAX_TOTAL_TAIL_BYTES {
                        return Err(Error::Capacity("follower tail exceeds byte limit"));
                    }
                    frames.push(frame);
                }
                match page.next_sequence {
                    Some(next) if next > first_sequence => first_sequence = next,
                    Some(_) => return Err(Error::Peer("follower tail did not advance")),
                    None => return Ok(frames),
                }
            }
            Err(Error::Capacity("follower tail exceeds page limit"))
        })
    }

    fn tail_page<'a>(
        &'a self,
        member: NodeId,
        request: TailRequest,
    ) -> BoxFuture<'a, Result<FollowerTailPage>> {
        Box::pin(async move {
            if member == self.node {
                let store = self
                    .local_recovery_store(member, request.leader_session, request.log_epoch)
                    .await?;
                let page = store
                    .read_tail_page(
                        request.leader_session,
                        request.log_epoch,
                        request.first_sequence,
                    )
                    .await?;
                self.guard.check()?;
                return Ok(page);
            }
            let response = self
                .send(
                    member,
                    WireRequest {
                        caller: self.session,
                        leader: request.leader_session,
                        epoch: request.log_epoch,
                        argument: request.first_sequence,
                        operation: Operation::Tail,
                    },
                    true,
                )
                .await?;
            let page = node_log_receiver::decode_page(response)
                .map_err(|()| unknown_error(Error::Peer("follower tail page is invalid")))?;
            if let Some(next) = page.next_sequence {
                let count = u64::try_from(page.frames.len())
                    .map_err(|_| Error::Peer("follower tail frame count overflow"))?;
                let expected = request
                    .first_sequence
                    .checked_add(count)
                    .ok_or(Error::Peer("follower tail sequence overflow"))?;
                if count == 0 || next != expected {
                    return Err(unknown_error(Error::Peer(
                        "follower tail continuation is invalid",
                    )));
                }
            }
            Ok(page)
        })
    }
}

fn transport_error(source: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::PeerTransport {
        context: "follower HTTP transport failed before acceptance",
        source: Box::new(source),
    }
}

fn unknown_error(source: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::PeerTransportUnknown {
        context: "follower HTTP reply was lost or invalid",
        source: Box::new(source),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{
        path::{Path, PathBuf},
        process::Command,
        sync::Arc,
    };

    use cellule_ltx::{Db, NodeFrameScope, encode_node_frame};
    use cellule_peer_http::{LoadedPeerTls, PeerTlsIdentity};
    use cellule_runtime::{
        SqlWorkerPool,
        cell::actor::CellRuntime,
        follower::FollowerStore,
        ltx::{CellStorageLayout, DiskBudget, Host, Limits},
        node::{
            NODE_LOG_PROTOCOL_VERSION, NodeCapacity, NodeFailureDomain,
            log_recovery::NodeLogRecovery,
        },
    };
    use cellule_store::Store;
    use object_store::{memory::InMemory, path::Path as ObjectPath};

    fn run(command: &mut Command) {
        assert!(command.output().unwrap().status.success());
    }

    fn certificate(root: &Path, name: &str, ca: &Path, ca_key: &Path) -> (PathBuf, PathBuf) {
        let key = root.join(format!("{name}.key"));
        let csr = root.join(format!("{name}.csr"));
        let cert = root.join(format!("{name}.crt"));
        let extension = root.join(format!("{name}.ext"));
        run(Command::new("openssl")
            .args(["genpkey", "-algorithm", "ED25519", "-out"])
            .arg(&key));
        run(Command::new("openssl")
            .args(["req", "-new", "-subj", "/CN=localhost", "-key"])
            .arg(&key)
            .arg("-out")
            .arg(&csr));
        std::fs::write(&extension, "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:localhost\n").unwrap();
        run(Command::new("openssl")
            .args(["x509", "-req", "-days", "1", "-CAcreateserial", "-in"])
            .arg(&csr)
            .arg("-CA")
            .arg(ca)
            .arg("-CAkey")
            .arg(ca_key)
            .arg("-extfile")
            .arg(&extension)
            .arg("-out")
            .arg(&cert));
        (cert, key)
    }

    fn frame(limits: Limits, leader: SessionId) -> Bytes {
        let source = tempfile::TempDir::new().unwrap();
        let mut database = Db::open(&source.path().join("cell.sqlite"), limits).unwrap();
        database.transaction(|transaction| transaction.execute_batch("CREATE TABLE events(id INTEGER PRIMARY KEY, body TEXT NOT NULL); INSERT INTO events(body) VALUES ('one')")).unwrap();
        let capture = database.capture().unwrap();
        let segment = capture.segments.first().unwrap();
        encode_node_frame(
            NodeFrameScope {
                leader_session: *leader.as_bytes(),
                log_epoch: 2,
                node_sequence: 1,
                application: [3; 16],
                cell: [4; 32],
                incarnation: [5; 16],
                cell_epoch: 6,
                commit_sequence: 1,
            },
            segment.info().clone(),
            Bytes::from(std::fs::read(segment.path()).unwrap()),
            limits,
        )
        .unwrap()
        .encoded()
        .clone()
    }

    fn advertisement(
        node: NodeId,
        session: SessionId,
        endpoint: String,
        tls: &LoadedPeerTls,
        follower: bool,
        log_capable: bool,
        now_ms: i64,
        lease_ms: i64,
    ) -> NodeAdvertisement {
        NodeAdvertisement::sign(
            node,
            session,
            endpoint,
            tls.fleet(),
            tls.certificate(),
            Digest::from_bytes([81; 32]),
            Digest::from_bytes([82; 32]),
            tls.signing_key(),
            1,
            now_ms,
            now_ms + lease_ms,
            vec![Digest::from_bytes([86; 32])],
            vec![1],
            NodeFailureDomain::default(),
            NodeCapacity {
                free_memory_bytes: 16 << 20,
                free_disk_bytes: 1 << 30,
                follower_free_bytes: if follower { 1 << 30 } else { 0 },
                job_credits: 8,
                log_protocol: if log_capable {
                    NODE_LOG_PROTOCOL_VERSION
                } else {
                    0
                },
                ..NodeCapacity::default()
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn pinned_mtls_append_survives_follower_store_reopen() {
        let root = tempfile::TempDir::new().unwrap();
        let ca_key = root.path().join("ca.key");
        let ca = root.path().join("ca.crt");
        run(Command::new("openssl")
            .args(["genpkey", "-algorithm", "ED25519", "-out"])
            .arg(&ca_key));
        run(Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-new",
                "-days",
                "1",
                "-subj",
                "/CN=BeyondDB Test CA",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-key",
            ])
            .arg(&ca_key)
            .arg("-out")
            .arg(&ca));
        let (leader_cert, leader_key) = certificate(root.path(), "leader", &ca, &ca_key);
        let (follower_cert, follower_key) = certificate(root.path(), "follower", &ca, &ca_key);
        let leader_tls = LoadedPeerTls::load(&leader_cert, &leader_key, &ca, "localhost").unwrap();
        let follower_tls =
            LoadedPeerTls::load(&follower_cert, &follower_key, &ca, "localhost").unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let follower_endpoint = format!("https://{}", listener.local_addr().unwrap());
        let layout = CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            ObjectPath::from("follower-transport-test"),
            [42; 16],
        );
        let directory = NodeDirectory::new(
            layout,
            leader_tls.fleet(),
            Digest::from_bytes([81; 32]),
            Digest::from_bytes([82; 32]),
        );
        let leader_session = SessionId::from_bytes([1; 16]);
        let follower_session = SessionId::from_bytes([2; 16]);
        let leader_node = NodeId::from_bytes([3; 16]);
        let follower_node = NodeId::from_bytes([4; 16]);
        let now_ms = unix_time_ms().unwrap();
        directory
            .create(
                advertisement(
                    follower_node,
                    follower_session,
                    follower_endpoint,
                    &follower_tls,
                    true,
                    true,
                    now_ms,
                    15_000,
                ),
                now_ms,
            )
            .await
            .unwrap();
        let enrolled = directory
            .create(
                advertisement(
                    leader_node,
                    leader_session,
                    "https://leader.internal:8081".into(),
                    &leader_tls,
                    false,
                    false,
                    now_ms,
                    3_000,
                ),
                now_ms,
            )
            .await
            .unwrap();
        let enrolled = directory
            .try_recruit_log(&enrolled, 2, 4096, 16, now_ms)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            enrolled.advertisement().log().unwrap().members(),
            &[follower_node]
        );

        let limits = Limits::default();
        let store_root = root.path().join("follower-store");
        let store = Arc::new(
            FollowerStore::open(store_root.clone(), limits, DiskBudget::new(1 << 30)).unwrap(),
        );
        let runtime = CellRuntime::new_with_replica_host(
            SqlWorkerPool::new(1, 8).unwrap(),
            64 << 20,
            follower_session,
            Host::default(),
        )
        .unwrap();
        let guard = NodeLeaseGuard::new(now_ms, now_ms + 15_000).unwrap();
        let receiver = node_log_receiver::FollowerEndpoint::new(
            directory.clone(),
            runtime,
            follower_node,
            store.clone(),
            guard.clone(),
        );
        let follower_client = follower_tls.client_identity();
        let server = tokio::spawn(async move {
            axum::serve(
                follower_tls.listener(listener),
                node_log_receiver::router(receiver)
                    .into_make_service_with_connect_info::<PeerTlsIdentity>(),
            )
            .await
        });
        let transport = PeerNodeLogTransport::new(
            directory.clone(),
            leader_tls.client_identity(),
            leader_session,
            leader_node,
            guard.clone(),
        );
        let saved = frame(limits, leader_session);
        for _ in 0..2 {
            let receipt = transport
                .append(
                    follower_node,
                    AppendRequest {
                        leader_session,
                        log_epoch: 2,
                        frames: vec![saved.clone()],
                        covered_through: 0,
                    },
                )
                .await
                .unwrap();
            assert_eq!(receipt.durable_through, 1);
        }
        let local = PeerNodeLogTransport::new(
            directory.clone(),
            follower_client,
            follower_session,
            follower_node,
            guard,
        )
        .with_local_follower_store(store.clone());
        assert!(
            local
                .seal(
                    follower_node,
                    SealRequest {
                        leader_session,
                        log_epoch: 2,
                    },
                )
                .await
                .is_err()
        );
        tokio::time::sleep(Duration::from_millis(3_100)).await;
        let fenced = directory
            .claim_expired_for_recovery(leader_session, follower_session, unix_time_ms().unwrap())
            .await
            .unwrap();
        let recovery_transport: Arc<dyn NodeLogTransport> = Arc::new(local.clone());
        let recovery = NodeLogRecovery::from_fenced(recovery_transport, &fenced, limits)
            .unwrap()
            .with_recovery_scratch(root.path().to_owned());
        let sealed = recovery.ensure_sealed_bounded().await.unwrap();
        assert_eq!(sealed.frame_count(), 1);
        assert_eq!(sealed.scopes(limits).unwrap()[0].cell, [4; 32]);
        assert_eq!(
            local
                .seal(
                    follower_node,
                    SealRequest {
                        leader_session,
                        log_epoch: 2,
                    },
                )
                .await
                .unwrap()
                .durable_through,
            1
        );
        let page = local
            .tail_page(
                follower_node,
                TailRequest {
                    leader_session,
                    log_epoch: 2,
                    first_sequence: 1,
                },
            )
            .await
            .unwrap();
        assert_eq!(page.frames, vec![saved.clone()]);
        assert_eq!(page.next_sequence, None);
        server.abort();
        let _ = server.await;
        drop(transport);
        drop(local);
        drop(store);
        let reopened = FollowerStore::open(store_root, limits, DiskBudget::new(1 << 30)).unwrap();
        assert_eq!(
            reopened
                .seal(leader_session, 2)
                .await
                .unwrap()
                .durable_through,
            1
        );
        assert_eq!(
            reopened.read_tail(leader_session, 2, 1).await.unwrap(),
            vec![saved]
        );
    }

    #[tokio::test]
    async fn remote_claimant_recovers_persisted_follower_tail() {
        let root = tempfile::TempDir::new().unwrap();
        let ca_key = root.path().join("ca.key");
        let ca = root.path().join("ca.crt");
        run(Command::new("openssl")
            .args(["genpkey", "-algorithm", "ED25519", "-out"])
            .arg(&ca_key));
        run(Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-new",
                "-days",
                "1",
                "-subj",
                "/CN=BeyondDB Test CA",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-key",
            ])
            .arg(&ca_key)
            .arg("-out")
            .arg(&ca));
        let (leader_cert, leader_key) = certificate(root.path(), "leader", &ca, &ca_key);
        let (follower_cert, follower_key) = certificate(root.path(), "follower", &ca, &ca_key);
        let (claimant_cert, claimant_key) = certificate(root.path(), "claimant", &ca, &ca_key);
        let leader_tls = LoadedPeerTls::load(&leader_cert, &leader_key, &ca, "localhost").unwrap();
        let follower_tls =
            LoadedPeerTls::load(&follower_cert, &follower_key, &ca, "localhost").unwrap();
        let claimant_tls =
            LoadedPeerTls::load(&claimant_cert, &claimant_key, &ca, "localhost").unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let follower_endpoint = format!("https://{}", listener.local_addr().unwrap());
        let layout = CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            ObjectPath::from("remote-follower-recovery-test"),
            [42; 16],
        );
        let directory = NodeDirectory::new(
            layout,
            leader_tls.fleet(),
            Digest::from_bytes([81; 32]),
            Digest::from_bytes([82; 32]),
        );
        let leader_session = SessionId::from_bytes([1; 16]);
        let follower_session = SessionId::from_bytes([2; 16]);
        let claimant_session = SessionId::from_bytes([3; 16]);
        let leader_node = NodeId::from_bytes([4; 16]);
        let follower_node = NodeId::from_bytes([5; 16]);
        let claimant_node = NodeId::from_bytes([6; 16]);
        let now_ms = unix_time_ms().unwrap();
        directory
            .create(
                advertisement(
                    follower_node,
                    follower_session,
                    follower_endpoint,
                    &follower_tls,
                    true,
                    true,
                    now_ms,
                    15_000,
                ),
                now_ms,
            )
            .await
            .unwrap();
        let leader = directory
            .create(
                advertisement(
                    leader_node,
                    leader_session,
                    "https://leader.internal:8081".into(),
                    &leader_tls,
                    false,
                    false,
                    now_ms,
                    3_000,
                ),
                now_ms,
            )
            .await
            .unwrap();
        let enrolled = directory
            .try_recruit_log(&leader, 2, 4096, 16, now_ms)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            enrolled.advertisement().log().unwrap().members(),
            &[follower_node]
        );
        directory
            .create(
                advertisement(
                    claimant_node,
                    claimant_session,
                    "https://claimant.internal:8081".into(),
                    &claimant_tls,
                    false,
                    true,
                    now_ms,
                    15_000,
                ),
                now_ms,
            )
            .await
            .unwrap();

        let limits = Limits::default();
        let store = Arc::new(
            FollowerStore::open(
                root.path().join("follower-store"),
                limits,
                DiskBudget::new(1 << 30),
            )
            .unwrap(),
        );
        let runtime = CellRuntime::new_with_replica_host(
            SqlWorkerPool::new(1, 8).unwrap(),
            64 << 20,
            follower_session,
            Host::default(),
        )
        .unwrap();
        let follower_guard = NodeLeaseGuard::new(now_ms, now_ms + 15_000).unwrap();
        let receiver = node_log_receiver::FollowerEndpoint::new(
            directory.clone(),
            runtime,
            follower_node,
            store,
            follower_guard,
        );
        let server = tokio::spawn(async move {
            axum::serve(
                follower_tls.listener(listener),
                node_log_receiver::router(receiver)
                    .into_make_service_with_connect_info::<PeerTlsIdentity>(),
            )
            .await
        });
        let leader_transport = PeerNodeLogTransport::new(
            directory.clone(),
            leader_tls.client_identity(),
            leader_session,
            leader_node,
            NodeLeaseGuard::new(now_ms, now_ms + 15_000).unwrap(),
        );
        let saved = frame(limits, leader_session);
        assert_eq!(
            leader_transport
                .append(
                    follower_node,
                    AppendRequest {
                        leader_session,
                        log_epoch: 2,
                        frames: vec![saved.clone()],
                        covered_through: 0,
                    },
                )
                .await
                .unwrap()
                .durable_through,
            1
        );
        let claimant_transport = PeerNodeLogTransport::new(
            directory.clone(),
            claimant_tls.client_identity(),
            claimant_session,
            claimant_node,
            NodeLeaseGuard::new(now_ms, now_ms + 15_000).unwrap(),
        );
        assert!(
            claimant_transport
                .seal(
                    follower_node,
                    SealRequest {
                        leader_session,
                        log_epoch: 2,
                    },
                )
                .await
                .is_err()
        );
        tokio::time::sleep(Duration::from_millis(3_100)).await;
        let fenced = directory
            .claim_expired_for_recovery(leader_session, claimant_session, unix_time_ms().unwrap())
            .await
            .unwrap();
        let recovery_transport: Arc<dyn NodeLogTransport> = Arc::new(claimant_transport.clone());
        let recovery = NodeLogRecovery::from_fenced(recovery_transport, &fenced, limits)
            .unwrap()
            .with_recovery_scratch(root.path().to_owned());
        let sealed = recovery.ensure_sealed_bounded().await.unwrap();
        assert_eq!(sealed.frame_count(), 1);
        assert_eq!(sealed.scopes(limits).unwrap()[0].cell, [4; 32]);
        let page = claimant_transport
            .tail_page(
                follower_node,
                TailRequest {
                    leader_session,
                    log_epoch: 2,
                    first_sequence: 1,
                },
            )
            .await
            .unwrap();
        assert_eq!(page.frames, vec![saved]);
        assert_eq!(page.next_sequence, None);
        server.abort();
        let _ = server.await;
    }
}
