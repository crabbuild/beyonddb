//! Hold one directory unavailable around the real SDK ingress.
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use cellar_runtime::{
    client::{Invocation, InvocationTransport},
    identity::CellTarget,
};
use http_body_util::{BodyExt, Full};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
pub struct BlockedDirectory {
    pub inner: Arc<dyn InvocationTransport>,
    pub target: CellTarget,
    pub blocked: Arc<AtomicBool>,
    pub attempts: Arc<AtomicUsize>,
}
impl InvocationTransport for BlockedDirectory {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            if invocation.target == self.target && self.blocked.load(Ordering::SeqCst) {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                let body = Full::new(Bytes::new())
                    .map_err(|never| -> std::io::Error { match never {} })
                    .boxed_unsync();
                let mut response = http::Response::new(body);
                *response.status_mut() = http::StatusCode::SERVICE_UNAVAILABLE;
                response
            } else {
                self.inner.call(invocation, request).await
            }
        })
    }
}
