//! HTTP status is a read retry hint, never proof that a mutation did not run.
use super::{Invocation, InvocationTransport};
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use http::{Request, StatusCode};
use http_body::Body;
use http_body_util::{BodyExt, Full};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

pub(super) struct ReadBackpressure {
    inner: Arc<dyn InvocationTransport>,
    maximum_wait: Duration,
}
impl ReadBackpressure {
    pub(super) fn new(inner: Arc<dyn InvocationTransport>, maximum_wait: Duration) -> Self {
        Self {
            inner,
            maximum_wait,
        }
    }
}
impl InvocationTransport for ReadBackpressure {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            if invocation.mutation.is_some() || self.maximum_wait.is_zero() {
                return self.inner.call(invocation, request).await;
            }
            let waiting = async {
                let mut backoff = Duration::from_millis(10);
                loop {
                    let mut attempt = Request::new(request.body().clone());
                    *attempt.method_mut() = request.method().clone();
                    *attempt.uri_mut() = request.uri().clone();
                    *attempt.version_mut() = request.version();
                    *attempt.headers_mut() = request.headers().clone();
                    *attempt.extensions_mut() = request.extensions().clone();
                    let response = self.inner.call(invocation, attempt).await;
                    let (response, route_failed) = sdk_read_route_failure(response).await;
                    if !route_failed
                        && !matches!(
                            response.status(),
                            StatusCode::SERVICE_UNAVAILABLE | StatusCode::TOO_MANY_REQUESTS
                        )
                    {
                        return response;
                    }
                    let delay = response
                        .headers()
                        .get(http::header::RETRY_AFTER)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse::<u64>().ok())
                        .map(Duration::from_secs)
                        .unwrap_or(backoff)
                        .max(backoff)
                        .min(self.maximum_wait);
                    // Only readonly calls enter this branch. Discard their
                    // failed bodies before waiting, releasing ingress capacity.
                    drop(response);
                    tokio::time::sleep(delay).await;
                    backoff = (backoff * 2).min(Duration::from_millis(200));
                }
            };
            match tokio::time::timeout(self.maximum_wait, waiting).await {
                Ok(response) => response,
                Err(_) => {
                    // Preserve an unavailable read outcome. This response does
                    // not assert that any particular SDK attempt was unstarted.
                    let body = Full::new(Bytes::from_static(b"read backpressure deadline reached"))
                        .map_err(|never| -> std::io::Error { match never {} })
                        .boxed_unsync();
                    let mut response = http::Response::new(body);
                    *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
                    response
                }
            }
        })
    }
}

/// Cellar 0.1.0 reports failed owner resolution as a small SDK-generated 500.
/// It is a retry hint for a read, never evidence that a mutation was unstarted.
/// Leave every other response (including streamed or unbounded 500s) intact.
async fn sdk_read_route_failure(response: IngressResponse) -> (IngressResponse, bool) {
    const ROUTE_FAILED: &[u8] = b"Worker failed: route failed: ResolveFailed";
    if response.status() != StatusCode::INTERNAL_SERVER_ERROR
        || response.body().size_hint().exact() != Some(ROUTE_FAILED.len() as u64)
    {
        return (response, false);
    }
    let (parts, body) = response.into_parts();
    // Enforce the size even if a provider supplies an inconsistent hint.
    match http_body_util::Limited::new(body, ROUTE_FAILED.len())
        .collect()
        .await
    {
        Ok(collected) => {
            let bytes = collected.to_bytes();
            let route_failed = bytes.as_ref() == ROUTE_FAILED;
            let body = Full::new(bytes)
                .map_err(|never| -> std::io::Error { match never {} })
                .boxed_unsync();
            (http::Response::from_parts(parts, body), route_failed)
        }
        Err(error) => {
            let body = Full::new(Bytes::from(format!(
                "SDK error response could not be read: {error}"
            )))
            .map_err(|never| -> std::io::Error { match never {} })
            .boxed_unsync();
            (http::Response::from_parts(parts, body), false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MutationIdentity,
        identity::{ApplicationId, CellTarget, NamespaceId, RequestId, TenantId},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Replies {
        calls: AtomicUsize,
        refuse: usize,
        status: StatusCode,
        body: &'static [u8],
    }
    impl InvocationTransport for Replies {
        fn call<'a>(
            &'a self,
            _: &'a Invocation<'a>,
            request: Request<Full<Bytes>>,
        ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
            Box::pin(async move {
                assert_eq!(request.method(), "POST");
                assert_eq!(request.uri(), "http://bdb.internal/query");
                assert_eq!(request.headers()["x-test"], "preserved");
                assert_eq!(
                    request.into_body().collect().await.unwrap().to_bytes(),
                    Bytes::from_static(b"unchanged request")
                );
                let call = self.calls.fetch_add(1, Ordering::SeqCst);
                let mut response = http::Response::new(
                    Full::new(Bytes::from_static(self.body))
                        .map_err(|never| -> std::io::Error { match never {} })
                        .boxed_unsync(),
                );
                *response.status_mut() = if call < self.refuse {
                    self.status
                } else {
                    StatusCode::OK
                };
                response
            })
        }
    }
    fn invocation(mutation: bool) -> Invocation<'static> {
        Invocation {
            target: CellTarget::new(
                TenantId::from_bytes([1; 16]),
                ApplicationId::from_bytes([2; 16]),
                NamespaceId::from_bytes([3; 16]),
                &[0; 4],
            )
            .unwrap(),
            operation: 1,
            mutation: mutation.then_some(MutationIdentity {
                request_id: RequestId::from_bytes([4; 16]),
                issued_at_ms: 1,
                expires_at_ms: 100,
            }),
            input: b"unchanged request",
        }
    }
    fn request() -> Request<Full<Bytes>> {
        Request::builder()
            .method("POST")
            .uri("http://bdb.internal/query")
            .header("x-test", "preserved")
            .body(Full::new(Bytes::from_static(b"unchanged request")))
            .unwrap()
    }
    #[tokio::test]
    async fn readonly_calls_pace_only_explicit_backpressure_statuses() {
        for status in [
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
        ] {
            let inner = Arc::new(Replies {
                calls: AtomicUsize::new(0),
                refuse: 2,
                status,
                body: b"",
            });
            let paced = ReadBackpressure::new(inner.clone(), Duration::from_secs(1));
            let response = paced.call(&invocation(false), request()).await;
            let retry = matches!(
                status,
                StatusCode::SERVICE_UNAVAILABLE | StatusCode::TOO_MANY_REQUESTS
            );
            assert_eq!(
                inner.calls.load(Ordering::SeqCst),
                if retry { 3 } else { 1 }
            );
            assert_eq!(
                response.status(),
                if retry { StatusCode::OK } else { status }
            );
        }
    }
    #[tokio::test]
    async fn sdk_owner_resolution_failure_is_retried_only_for_reads() {
        for mutation in [false, true] {
            let inner = Arc::new(Replies {
                calls: AtomicUsize::new(0),
                refuse: 1,
                status: StatusCode::INTERNAL_SERVER_ERROR,
                body: b"Worker failed: route failed: ResolveFailed",
            });
            let paced = ReadBackpressure::new(inner.clone(), Duration::from_secs(1));
            let response = paced.call(&invocation(mutation), request()).await;
            assert_eq!(
                inner.calls.load(Ordering::SeqCst),
                if mutation { 1 } else { 2 }
            );
            assert_eq!(
                response.status(),
                if mutation {
                    StatusCode::INTERNAL_SERVER_ERROR
                } else {
                    StatusCode::OK
                }
            );
        }
        // Equal-size application errors must not become SDK routing hints.
        let body = b"Worker failed: route failed: ApplicationXX";
        assert_eq!(
            body.len(),
            b"Worker failed: route failed: ResolveFailed".len()
        );
        let inner = Arc::new(Replies {
            calls: AtomicUsize::new(0),
            refuse: 1,
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body,
        });
        let paced = ReadBackpressure::new(inner.clone(), Duration::from_secs(1));
        let response = paced.call(&invocation(false), request()).await;
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            body
        );
    }
    #[tokio::test]
    async fn mutations_are_never_retried_by_the_read_policy() {
        for status in [
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            let inner = Arc::new(Replies {
                calls: AtomicUsize::new(0),
                refuse: 2,
                status,
                body: b"",
            });
            let paced = ReadBackpressure::new(inner.clone(), Duration::from_secs(1));
            assert_eq!(
                paced.call(&invocation(true), request()).await.status(),
                status
            );
            assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        }
    }
    #[tokio::test]
    async fn read_deadline_fails_closed_instead_of_inventing_a_result() {
        let inner = Arc::new(Replies {
            calls: AtomicUsize::new(0),
            refuse: usize::MAX,
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: b"",
        });
        let paced = ReadBackpressure::new(inner.clone(), Duration::from_millis(25));
        let response = tokio::time::timeout(
            Duration::from_secs(1),
            paced.call(&invocation(false), request()),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(inner.calls.load(Ordering::SeqCst) > 1);
    }
}
