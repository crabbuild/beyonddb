//! Bounded follower-lane requests on BeyondDB's mutual-TLS peer listener.

use std::{sync::Arc, time::Duration};

use axum::{
    Router,
    body::Bytes,
    extract::{ConnectInfo, Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use cellule_peer_http::PeerTlsIdentity;
use cellule_runtime::{
    Error, NodeLeaseGuard, Result,
    cell::actor::CellRuntime,
    follower::{FollowerReceipt, FollowerStore, FollowerTailPage},
    identity::{Digest, NodeId, SessionId},
    node::NodeDirectory,
};
use futures_util::StreamExt;
use tokio::sync::Semaphore;

use super::node_lease::unix_time_ms;

pub(super) const MEDIA_TYPE: &str = "application/vnd.beyonddb.node-log-v1";
const MAGIC: &[u8; 4] = b"BNL1";
pub(super) const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024 + 64 * 1024;
const MAX_APPEND_FRAMES: usize = 64;
const MAX_TAIL_FRAMES: usize = 4096;
const MAX_TAIL_BYTES: usize = 1024 * 1024;
pub(super) const MAX_RESPONSE_BYTES: usize = MAX_TAIL_BYTES + MAX_TAIL_FRAMES * 4 + 17;
const MAX_CONCURRENT_REQUESTS: usize = 8;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// One local follower store and its current node-session fence.
#[derive(Clone)]
pub(super) struct FollowerEndpoint {
    directory: NodeDirectory,
    runtime: CellRuntime,
    member: NodeId,
    store: Arc<FollowerStore>,
    guard: NodeLeaseGuard,
    permits: Arc<Semaphore>,
}

impl FollowerEndpoint {
    pub(super) fn new(
        directory: NodeDirectory,
        runtime: CellRuntime,
        member: NodeId,
        store: Arc<FollowerStore>,
        guard: NodeLeaseGuard,
    ) -> Self {
        Self {
            directory,
            runtime,
            member,
            store,
            guard,
            permits: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
        }
    }

    async fn dispatch(&self, request: WireRequest, identity: PeerTlsIdentity) -> Result<Vec<u8>> {
        self.dispatch_with_identity(request, identity.certificate(), identity.public_key())
            .await
    }

    async fn dispatch_with_identity(
        &self,
        request: WireRequest,
        certificate: Digest,
        public_key: [u8; 32],
    ) -> Result<Vec<u8>> {
        self.guard.check()?;
        let now_ms = unix_time_ms()?;
        let enrollment = self
            .directory
            .peer_verifier(request.caller, certificate, public_key, now_ms)
            .await?;
        self.guard.check()?;
        let now_ms = unix_time_ms()?;
        let response = match request.operation {
            Operation::Append(frames) => {
                if request.caller != request.leader {
                    return Err(Error::PeerAuthorization(
                        "follower append caller is not leader",
                    ));
                }
                // Use this request's fresh mTLS-bound canonical observation;
                // recheck expiry after provider I/O before any durable append.
                enrollment.authorize_log_append(
                    self.member,
                    request.epoch,
                    request.argument,
                    now_ms,
                )?;
                self.guard.check()?;
                encode_receipt(
                    self.store
                        .append(request.leader, request.epoch, frames, request.argument)
                        .await?,
                )
            }
            Operation::Seal => {
                self.directory
                    .authorize_log_recovery(
                        request.leader,
                        request.caller,
                        self.member,
                        request.epoch,
                        now_ms,
                    )
                    .await?;
                self.guard.check()?;
                encode_receipt(self.store.seal(request.leader, request.epoch).await?)
            }
            Operation::Retire => {
                if request.caller != request.leader {
                    return Err(Error::PeerAuthorization(
                        "follower retire caller is not leader",
                    ));
                }
                self.directory
                    .authorize_log_retire(
                        request.leader,
                        self.member,
                        request.epoch,
                        request.argument,
                        now_ms,
                    )
                    .await?;
                self.guard.check()?;
                encode_receipt(
                    self.store
                        .retire(request.leader, request.epoch, request.argument)
                        .await?,
                )
            }
            Operation::Tail => {
                self.directory
                    .authorize_log_recovery(
                        request.leader,
                        request.caller,
                        self.member,
                        request.epoch,
                        now_ms,
                    )
                    .await?;
                self.guard.check()?;
                encode_page(
                    self.store
                        .read_tail_page(request.leader, request.epoch, request.argument)
                        .await?,
                )?
            }
        };
        self.guard.check()?;
        Ok(response)
    }
}

pub(super) fn router(endpoint: FollowerEndpoint) -> Router {
    Router::new()
        .route("/internal/node-log/v1", post(receive))
        .with_state(endpoint)
}

async fn receive(
    State(endpoint): State<FollowerEndpoint>,
    ConnectInfo(identity): ConnectInfo<PeerTlsIdentity>,
    request: Request,
) -> Response {
    match tokio::time::timeout(
        REQUEST_TIMEOUT,
        receive_bounded(endpoint, identity, request),
    )
    .await
    {
        Ok(Ok(encoded)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, MEDIA_TYPE)],
            encoded,
        )
            .into_response(),
        Ok(Err(status)) => status.into_response(),
        Err(_) => StatusCode::GATEWAY_TIMEOUT.into_response(),
    }
}

async fn receive_bounded(
    endpoint: FollowerEndpoint,
    identity: PeerTlsIdentity,
    request: Request,
) -> std::result::Result<Vec<u8>, StatusCode> {
    if request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        != Some(MEDIA_TYPE)
    {
        return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    let wire_bytes = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or(StatusCode::LENGTH_REQUIRED)?;
    if !(1..=MAX_REQUEST_BYTES).contains(&wire_bytes) {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    let Ok(permit) = endpoint.permits.clone().try_acquire_owned() else {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    };
    let admission_bytes = wire_bytes.saturating_mul(2).saturating_add(64 * 1024);
    let Ok(reservation) = endpoint.runtime.try_reserve_node_bytes(admission_bytes) else {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    };
    let mut encoded = Vec::with_capacity(wire_bytes.min(64 * 1024));
    let mut body = request.into_body().into_data_stream();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|_| StatusCode::BAD_REQUEST)?;
        let end = encoded
            .len()
            .checked_add(chunk.len())
            .ok_or(StatusCode::PAYLOAD_TOO_LARGE)?;
        if end > wire_bytes {
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }
        encoded.extend_from_slice(&chunk);
    }
    if encoded.len() != wire_bytes {
        return Err(StatusCode::BAD_REQUEST);
    }
    let request =
        WireRequest::decode(Bytes::from(encoded)).map_err(|()| StatusCode::BAD_REQUEST)?;
    let job = tokio::spawn(async move {
        let _permit = permit;
        let _reservation = reservation;
        endpoint.dispatch(request, identity).await
    });
    job.await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .map_err(|error| match error {
            Error::PeerAuthorization(_) => StatusCode::FORBIDDEN,
            Error::Node(_) | Error::Fenced => StatusCode::CONFLICT,
            _ => StatusCode::SERVICE_UNAVAILABLE,
        })
}

pub(super) struct WireRequest {
    pub(super) caller: SessionId,
    pub(super) leader: SessionId,
    pub(super) epoch: u64,
    pub(super) argument: u64,
    pub(super) operation: Operation,
}

pub(super) enum Operation {
    Append(Vec<Bytes>),
    Seal,
    Retire,
    Tail,
}

impl WireRequest {
    pub(super) fn encode(self) -> std::result::Result<Vec<u8>, ()> {
        if self.epoch == 0 {
            return Err(());
        }
        let mut encoded = Vec::new();
        encoded.extend_from_slice(MAGIC);
        let (tag, count) = match &self.operation {
            Operation::Append(frames) if (1..=MAX_APPEND_FRAMES).contains(&frames.len()) => {
                (1, frames.len())
            }
            Operation::Seal if self.argument == 0 => (2, 0),
            Operation::Retire => (3, 0),
            Operation::Tail if self.argument != 0 => (4, 0),
            _ => return Err(()),
        };
        encoded.push(tag);
        encoded.extend_from_slice(self.caller.as_bytes());
        encoded.extend_from_slice(self.leader.as_bytes());
        encoded.extend_from_slice(&self.epoch.to_be_bytes());
        encoded.extend_from_slice(&self.argument.to_be_bytes());
        encoded.extend_from_slice(&u32::try_from(count).map_err(|_| ())?.to_be_bytes());
        if let Operation::Append(frames) = self.operation {
            for frame in frames {
                if frame.is_empty() {
                    return Err(());
                }
                encoded
                    .extend_from_slice(&u32::try_from(frame.len()).map_err(|_| ())?.to_be_bytes());
                encoded.extend_from_slice(&frame);
                if encoded.len() > MAX_REQUEST_BYTES {
                    return Err(());
                }
            }
        }
        Ok(encoded)
    }

    fn decode(body: Bytes) -> std::result::Result<Self, ()> {
        let wire_bytes = body.len();
        if wire_bytes > MAX_REQUEST_BYTES {
            return Err(());
        }
        let mut reader = Reader::new(body);
        if reader.take(4)?.as_ref() != MAGIC {
            return Err(());
        }
        let tag = reader.u8()?;
        let caller = SessionId::from_bytes(reader.array_16()?);
        let leader = SessionId::from_bytes(reader.array_16()?);
        let epoch = reader.u64()?;
        let argument = reader.u64()?;
        let count = usize::try_from(reader.u32()?).map_err(|_| ())?;
        if epoch == 0 {
            return Err(());
        }
        let operation = match tag {
            1 if (1..=MAX_APPEND_FRAMES).contains(&count) => {
                let mut frames = Vec::with_capacity(count);
                for _ in 0..count {
                    let length = usize::try_from(reader.u32()?).map_err(|_| ())?;
                    if length == 0 {
                        return Err(());
                    }
                    frames.push(reader.take(length)?);
                }
                Operation::Append(frames)
            }
            2 if count == 0 && argument == 0 => Operation::Seal,
            3 if count == 0 => Operation::Retire,
            4 if count == 0 && argument != 0 => Operation::Tail,
            _ => return Err(()),
        };
        reader.finish()?;
        Ok(Self {
            caller,
            leader,
            epoch,
            argument,
            operation,
        })
    }
}

struct Reader {
    bytes: Bytes,
    position: usize,
}

impl Reader {
    const fn new(bytes: Bytes) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> std::result::Result<Bytes, ()> {
        let end = self.position.checked_add(length).ok_or(())?;
        if end > self.bytes.len() {
            return Err(());
        }
        let value = self.bytes.slice(self.position..end);
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> std::result::Result<u8, ()> {
        self.take(1)?.first().copied().ok_or(())
    }

    fn u32(&mut self) -> std::result::Result<u32, ()> {
        let bytes: [u8; 4] = self.take(4)?.as_ref().try_into().map_err(|_| ())?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn u64(&mut self) -> std::result::Result<u64, ()> {
        let bytes: [u8; 8] = self.take(8)?.as_ref().try_into().map_err(|_| ())?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn array_16(&mut self) -> std::result::Result<[u8; 16], ()> {
        self.take(16)?.as_ref().try_into().map_err(|_| ())
    }

    fn finish(self) -> std::result::Result<(), ()> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(())
        }
    }
}

fn encode_receipt(receipt: FollowerReceipt) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(21);
    encoded.extend_from_slice(MAGIC);
    encoded.push(1);
    encoded.extend_from_slice(&receipt.base_sequence.to_be_bytes());
    encoded.extend_from_slice(&receipt.durable_through.to_be_bytes());
    encoded
}

fn encode_page(page: FollowerTailPage) -> Result<Vec<u8>> {
    if page.frames.len() > MAX_TAIL_FRAMES {
        return Err(Error::Peer("follower tail page has too many frames"));
    }
    let bytes = page
        .frames
        .iter()
        .try_fold(0_usize, |total, frame| total.checked_add(frame.len()));
    let Some(bytes) = bytes.filter(|bytes| *bytes <= MAX_TAIL_BYTES) else {
        return Err(Error::Peer("follower tail page exceeds byte limit"));
    };
    let mut encoded = Vec::with_capacity(bytes.saturating_add(17 + page.frames.len() * 4));
    encoded.extend_from_slice(MAGIC);
    encoded.push(2);
    encoded.extend_from_slice(
        &u32::try_from(page.frames.len())
            .map_err(|_| Error::Peer("follower tail frame count overflow"))?
            .to_be_bytes(),
    );
    encoded.extend_from_slice(&page.next_sequence.unwrap_or(0).to_be_bytes());
    for frame in page.frames {
        encoded.extend_from_slice(
            &u32::try_from(frame.len())
                .map_err(|_| Error::Peer("follower tail frame size overflow"))?
                .to_be_bytes(),
        );
        encoded.extend_from_slice(&frame);
    }
    Ok(encoded)
}

pub(super) fn decode_receipt(body: Bytes) -> std::result::Result<FollowerReceipt, ()> {
    let mut reader = Reader::new(body);
    if reader.take(4)?.as_ref() != MAGIC || reader.u8()? != 1 {
        return Err(());
    }
    let base_sequence = reader.u64()?;
    let durable_through = reader.u64()?;
    reader.finish()?;
    if base_sequence == 0 || durable_through < base_sequence.saturating_sub(1) {
        return Err(());
    }
    Ok(FollowerReceipt {
        base_sequence,
        durable_through,
    })
}

pub(super) fn decode_page(body: Bytes) -> std::result::Result<FollowerTailPage, ()> {
    if body.len() > MAX_RESPONSE_BYTES {
        return Err(());
    }
    let mut reader = Reader::new(body);
    if reader.take(4)?.as_ref() != MAGIC || reader.u8()? != 2 {
        return Err(());
    }
    let count = usize::try_from(reader.u32()?).map_err(|_| ())?;
    if count > MAX_TAIL_FRAMES {
        return Err(());
    }
    let next = reader.u64()?;
    let mut bytes = 0_usize;
    let mut frames = Vec::with_capacity(count);
    for _ in 0..count {
        let length = usize::try_from(reader.u32()?).map_err(|_| ())?;
        bytes = bytes.checked_add(length).ok_or(())?;
        if length == 0 || bytes > MAX_TAIL_BYTES {
            return Err(());
        }
        frames.push(reader.take(length)?);
    }
    reader.finish()?;
    Ok(FollowerTailPage {
        frames,
        next_sequence: (next != 0).then_some(next),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellule_ltx::{Db, NodeFrameScope, encode_node_frame};
    use cellule_runtime::{
        SqlWorkerPool,
        ltx::{CellStorageLayout, DiskBudget, Host, Limits},
        node::{NODE_LOG_PROTOCOL_VERSION, NodeAdvertisement, NodeCapacity, NodeFailureDomain},
    };
    use cellule_store::{Store, test_support::CountingObjectStore};
    use ed25519_dalek::SigningKey;
    use object_store::{
        memory::InMemory,
        path::Path,
        throttle::{ThrottleConfig, ThrottledStore},
    };

    fn request(tag: u8, count: u32, frames: &[&[u8]]) -> Bytes {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(MAGIC);
        encoded.push(tag);
        encoded.extend_from_slice(&[1; 16]);
        encoded.extend_from_slice(&[2; 16]);
        encoded.extend_from_slice(&3_u64.to_be_bytes());
        encoded.extend_from_slice(&1_u64.to_be_bytes());
        encoded.extend_from_slice(&count.to_be_bytes());
        for frame in frames {
            encoded.extend_from_slice(&(frame.len() as u32).to_be_bytes());
            encoded.extend_from_slice(frame);
        }
        Bytes::from(encoded)
    }

    #[test]
    fn follower_wire_rejects_malformed_and_noncanonical_requests() {
        let valid = WireRequest::decode(request(1, 1, &[b"frame"])).unwrap();
        assert!(matches!(valid.operation, Operation::Append(_)));
        assert!(WireRequest::decode(request(1, 2, &[b"frame"])).is_err());
        assert!(WireRequest::decode(request(2, 0, &[])).is_err());
        assert!(WireRequest::decode(request(4, 0, &[])).is_ok());
        let mut trailing = request(1, 1, &[b"frame"]).to_vec();
        trailing.push(0);
        assert!(WireRequest::decode(Bytes::from(trailing)).is_err());
    }

    #[test]
    fn follower_wire_round_trips_transport_operations_and_responses() {
        let caller = SessionId::from_bytes([1; 16]);
        let leader = SessionId::from_bytes([2; 16]);
        for operation in [
            Operation::Append(vec![Bytes::from_static(b"frame")]),
            Operation::Seal,
            Operation::Retire,
            Operation::Tail,
        ] {
            let argument = if matches!(operation, Operation::Tail) {
                1
            } else {
                0
            };
            let request = WireRequest {
                caller,
                leader,
                epoch: 3,
                argument,
                operation,
            };
            let decoded = WireRequest::decode(Bytes::from(request.encode().unwrap())).unwrap();
            assert_eq!(decoded.caller, caller);
            assert_eq!(decoded.leader, leader);
            assert_eq!(decoded.epoch, 3);
            assert_eq!(decoded.argument, argument);
        }
        let receipt = FollowerReceipt {
            base_sequence: 2,
            durable_through: 4,
        };
        let decoded = decode_receipt(Bytes::from(encode_receipt(receipt))).unwrap();
        assert_eq!(decoded.base_sequence, 2);
        assert_eq!(decoded.durable_through, 4);
        let page = FollowerTailPage {
            frames: vec![Bytes::from_static(b"frame")],
            next_sequence: Some(5),
        };
        let decoded = decode_page(Bytes::from(encode_page(page).unwrap())).unwrap();
        assert_eq!(decoded.frames, vec![Bytes::from_static(b"frame")]);
        assert_eq!(decoded.next_sequence, Some(5));
        assert!(decode_receipt(Bytes::from_static(b"invalid")).is_err());
        assert!(decode_page(Bytes::from_static(b"invalid")).is_err());
    }

    fn advertisement(
        node: NodeId,
        session: SessionId,
        key: u8,
        follower: bool,
        now_ms: i64,
    ) -> NodeAdvertisement {
        NodeAdvertisement::sign(
            node,
            session,
            format!("https://node-{key}.internal:8081"),
            Digest::from_bytes([80; 32]),
            Digest::from_bytes([key; 32]),
            Digest::from_bytes([81; 32]),
            Digest::from_bytes([82; 32]),
            &SigningKey::from_bytes(&[key; 32]),
            1,
            now_ms,
            now_ms + 15_000,
            vec![Digest::from_bytes([86; 32])],
            vec![1],
            NodeFailureDomain::default(),
            NodeCapacity {
                free_memory_bytes: 16 << 20,
                free_disk_bytes: 1 << 30,
                follower_free_bytes: if follower { 1 << 30 } else { 0 },
                job_credits: 8,
                log_protocol: if follower {
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
    async fn authenticated_append_survives_reopen_and_rejects_wrong_peer() {
        let limits = Limits::default();
        let counted = Arc::new(CountingObjectStore::new(Arc::new(ThrottledStore::new(
            InMemory::new(),
            ThrottleConfig {
                wait_get_per_call: std::time::Duration::from_millis(5),
                ..ThrottleConfig::default()
            },
        ))));
        let layout = CellStorageLayout::new(
            Store::new(counted.clone()),
            Path::from("follower-receiver-test"),
            [42; 16],
        );
        let directory = NodeDirectory::new(
            layout.clone(),
            Digest::from_bytes([80; 32]),
            Digest::from_bytes([81; 32]),
            Digest::from_bytes([82; 32]),
        );
        let leader = SessionId::from_bytes([1; 16]);
        let follower = SessionId::from_bytes([2; 16]);
        let member = NodeId::from_bytes([3; 16]);
        let now_ms = unix_time_ms().unwrap();
        directory
            .create(advertisement(member, follower, 22, true, now_ms), now_ms)
            .await
            .unwrap();
        let enrolled = directory
            .create(
                advertisement(NodeId::from_bytes([4; 16]), leader, 21, false, now_ms),
                now_ms,
            )
            .await
            .unwrap();
        let enrolled = directory
            .try_recruit_log(&enrolled, 2, 4096, 16, now_ms)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(enrolled.advertisement().log().unwrap().members(), &[member]);

        let source = tempfile::TempDir::new().unwrap();
        let mut database = Db::open(&source.path().join("cell.sqlite"), limits).unwrap();
        database
            .transaction(|transaction| {
                transaction.execute_batch(
                    "CREATE TABLE events(id INTEGER PRIMARY KEY, body TEXT NOT NULL);\
                     INSERT INTO events(body) VALUES ('one')",
                )
            })
            .unwrap();
        let capture = database.capture().unwrap();
        let segment = capture.segments.first().unwrap();
        let frame = encode_node_frame(
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
        .clone();

        let root = tempfile::TempDir::new().unwrap();
        let store = Arc::new(
            FollowerStore::open(root.path().to_owned(), limits, DiskBudget::new(1 << 30)).unwrap(),
        );
        let runtime = CellRuntime::new_with_replica_host(
            SqlWorkerPool::new(1, 8).unwrap(),
            64 << 20,
            follower,
            Host::default(),
        )
        .unwrap();
        let guard = NodeLeaseGuard::new(now_ms, now_ms + 15_000).unwrap();
        let endpoint = FollowerEndpoint::new(directory, runtime, member, store.clone(), guard);
        let append = || WireRequest {
            caller: leader,
            leader,
            epoch: 2,
            argument: 0,
            operation: Operation::Append(vec![frame.clone()]),
        };
        let leader_key = SigningKey::from_bytes(&[21; 32]).verifying_key().to_bytes();
        assert!(
            endpoint
                .dispatch_with_identity(append(), Digest::from_bytes([99; 32]), leader_key)
                .await
                .is_err()
        );
        assert!(
            endpoint
                .dispatch_with_identity(append(), Digest::from_bytes([21; 32]), [99; 32])
                .await
                .is_err()
        );
        assert!(
            endpoint
                .dispatch_with_identity(
                    WireRequest {
                        caller: follower,
                        ..append()
                    },
                    Digest::from_bytes([22; 32]),
                    SigningKey::from_bytes(&[22; 32]).verifying_key().to_bytes(),
                )
                .await
                .is_err()
        );
        assert!(
            endpoint
                .dispatch_with_identity(
                    WireRequest {
                        argument: 1,
                        ..append()
                    },
                    Digest::from_bytes([21; 32]),
                    leader_key,
                )
                .await
                .is_err()
        );
        assert_eq!(store.retained_bytes(), 0);
        for _ in 0..2 {
            counted.reset();
            let result = endpoint
                .dispatch_with_identity(append(), Digest::from_bytes([21; 32]), leader_key)
                .await
                .unwrap();
            assert_eq!(&result[result.len() - 8..], &1_u64.to_be_bytes());
            let requests = counted.requests();
            assert_eq!(
                requests.len(),
                1,
                "each append needs one canonical enrollment read"
            );
            assert_eq!(
                requests[0].location,
                layout.node_path(leader.as_bytes()).to_string()
            );
        }
        assert!(
            endpoint
                .dispatch_with_identity(
                    WireRequest {
                        epoch: 3,
                        ..append()
                    },
                    Digest::from_bytes([21; 32]),
                    leader_key,
                )
                .await
                .is_err()
        );
        assert!(
            endpoint
                .dispatch_with_identity(
                    WireRequest {
                        caller: follower,
                        leader,
                        epoch: 2,
                        argument: 0,
                        operation: Operation::Seal,
                    },
                    Digest::from_bytes([22; 32]),
                    SigningKey::from_bytes(&[22; 32]).verifying_key().to_bytes(),
                )
                .await
                .is_err()
        );
        drop(endpoint);
        drop(store);
        let reopened =
            FollowerStore::open(root.path().to_owned(), limits, DiskBudget::new(1 << 30)).unwrap();
        assert_eq!(reopened.seal(leader, 2).await.unwrap().durable_through, 1);
        assert_eq!(reopened.read_tail(leader, 2, 1).await.unwrap(), vec![frame]);
    }
}
