//! Bound this gateway's calls before SDK submission, through response-body EOF.
//! This is local load control; SDK ingress still owns admission and authority.
use bdb_cellar_runtime::{
    client::{Invocation, InvocationTransport},
    identity::CellId,
};
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use http_body_util::{BodyExt, Full, StreamBody};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Weak},
};
use tokio::sync::{Mutex, Semaphore};

pub(super) struct Dispatch {
    inner: Arc<dyn InvocationTransport>,
    capacity: Arc<Semaphore>,
    targets: Mutex<HashMap<CellId, Weak<Semaphore>>>,
}
impl Dispatch {
    pub(super) fn new(inner: Arc<dyn InvocationTransport>) -> Self {
        Self {
            inner,
            capacity: Arc::new(Semaphore::new(128)),
            targets: Mutex::new(HashMap::new()),
        }
    }
}
fn unavailable() -> IngressResponse {
    let body = Full::new(Bytes::from_static(b"gateway dispatch capacity exhausted"))
        .map_err(|never| -> std::io::Error { match never {} })
        .boxed_unsync();
    let mut response = http::Response::new(body);
    *response.status_mut() = http::StatusCode::SERVICE_UNAVAILABLE;
    response
}
impl InvocationTransport for Dispatch {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            let Ok(capacity) = self.capacity.clone().try_acquire_owned() else {
                return unavailable();
            };
            let target = {
                let mut targets = self.targets.lock().await;
                targets.retain(|_, target| target.strong_count() > 0);
                let key = invocation.target.cell_id();
                if let Some(target) = targets.get(&key).and_then(Weak::upgrade) {
                    target
                } else {
                    // Leave headroom below the executable's 16-call SDK limit
                    // for maintenance and invocations from other gateways.
                    let target = Arc::new(Semaphore::new(8));
                    targets.insert(key, Arc::downgrade(&target));
                    target
                }
            };
            let Ok(permit) = target.acquire_owned().await else {
                return unavailable();
            };
            // One transport attempt, including mutations. Never infer execution
            // status or retry a mutation from a native HTTP response code.
            let response = self.inner.call(invocation, request).await;
            let (parts, body) = response.into_parts();
            let stream = futures_util::stream::unfold(
                (body, permit, capacity),
                |(mut body, permit, capacity)| async move {
                    body.frame()
                        .await
                        .map(|frame| (frame, (body, permit, capacity)))
                },
            );
            http::Response::from_parts(parts, StreamBody::new(stream).boxed_unsync())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bdb_cellar_runtime::{
        MutationIdentity,
        identity::{ApplicationId, CellTarget, NamespaceId, RequestId, TenantId},
    };
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    struct Replies {
        calls: AtomicUsize,
        status: http::StatusCode,
    }
    impl InvocationTransport for Replies {
        fn call<'a>(
            &'a self,
            _: &'a Invocation<'a>,
            request: http::Request<Full<Bytes>>,
        ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let mut response = http::Response::new(
                    request
                        .into_body()
                        .map_err(|never| -> std::io::Error { match never {} })
                        .boxed_unsync(),
                );
                *response.status_mut() = self.status;
                response
            })
        }
    }
    fn invocation(partition: u8) -> Invocation<'static> {
        Invocation {
            target: CellTarget::new(
                TenantId::from_bytes([1; 16]),
                ApplicationId::from_bytes([2; 16]),
                NamespaceId::from_bytes([3; 16]),
                &[partition],
            )
            .unwrap(),
            operation: 1,
            mutation: Some(MutationIdentity {
                request_id: RequestId::from_bytes([4; 16]),
                issued_at_ms: 1,
                expires_at_ms: 100,
            }),
            input: b"unchanged",
        }
    }
    fn request() -> http::Request<Full<Bytes>> {
        http::Request::new(Full::new(Bytes::from_static(b"unchanged")))
    }
    #[tokio::test]
    async fn response_bodies_hold_target_slots_and_canceled_waiters_do_not_submit() {
        let inner = Arc::new(Replies {
            calls: AtomicUsize::new(0),
            status: http::StatusCode::OK,
        });
        let dispatch = Arc::new(Dispatch::new(inner.clone()));
        let input = invocation(0);
        let mut held = Vec::new();
        for _ in 0..8 {
            held.push(dispatch.call(&input, request()).await);
        }
        let queued = {
            let dispatch = dispatch.clone();
            tokio::spawn(async move { dispatch.call(&invocation(0), request()).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(inner.calls.load(Ordering::SeqCst), 8);
        assert!(!queued.is_finished());
        // Another target has independent capacity despite this hot target.
        let other = dispatch.call(&invocation(1), request()).await;
        assert_eq!(
            other.into_body().collect().await.unwrap().to_bytes(),
            "unchanged"
        );
        queued.abort();
        assert!(queued.await.unwrap_err().is_cancelled());
        assert_eq!(inner.calls.load(Ordering::SeqCst), 9);
        held.pop();
        let next = tokio::time::timeout(Duration::from_secs(1), dispatch.call(&input, request()))
            .await
            .unwrap();
        assert_eq!(
            next.into_body().collect().await.unwrap().to_bytes(),
            "unchanged"
        );
        assert_eq!(inner.calls.load(Ordering::SeqCst), 10);
        drop(held);
    }
    #[tokio::test]
    async fn global_capacity_bounds_distinct_target_waiters_and_is_reclaimed() {
        let inner = Arc::new(Replies {
            calls: AtomicUsize::new(0),
            status: http::StatusCode::OK,
        });
        let dispatch = Dispatch::new(inner.clone());
        let mut held = Vec::new();
        for partition in 0..128 {
            held.push(dispatch.call(&invocation(partition), request()).await);
        }
        assert_eq!(
            dispatch.call(&invocation(128), request()).await.status(),
            http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(inner.calls.load(Ordering::SeqCst), 128);
        held.pop();
        assert_eq!(
            dispatch.call(&invocation(128), request()).await.status(),
            http::StatusCode::OK
        );
        assert_eq!(inner.calls.load(Ordering::SeqCst), 129);
    }
    #[tokio::test]
    async fn unavailable_mutations_keep_one_transport_attempt() {
        let inner = Arc::new(Replies {
            calls: AtomicUsize::new(0),
            status: http::StatusCode::SERVICE_UNAVAILABLE,
        });
        let dispatch = Dispatch::new(inner.clone());
        let response = dispatch.call(&invocation(0), request()).await;
        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "unchanged"
        );
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    }
}
