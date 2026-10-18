use bytes::Bytes;
use cellar::sdk::IngressResponse;
use cellar_runtime::client::{Invocation, InvocationTransport};
use http_body_util::{BodyExt, Full};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

pub struct Observe {
    pub inner: Arc<dyn InvocationTransport>,
    pub queries: Arc<AtomicU64>,
    pub drop_batch: bool,
    pub dropped: Arc<AtomicBool>,
    pub data_namespace: cellar_runtime::identity::NamespaceId,
}
impl InvocationTransport for Observe {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            let response = self.inner.call(invocation, request).await;
            let coordinator = beyonddb::coordinator_target("123456789012", b"namespace").unwrap();
            let query = invocation.target.namespace() == coordinator.namespace()
                && invocation.mutation.is_none();
            let batch = invocation.target.namespace() == self.data_namespace
                && invocation.mutation.is_some()
                && invocation.operation
                    == <beyonddb::PreparePartitionTransactionBatch as cellar_runtime::registry::Command>::ID;
            if response.status().is_success() && (query || (self.drop_batch && batch)) {
                let (parts, body) = response.into_parts();
                // Observe or lose the reply only after the real SDK gate proves
                // execution/durability. Returned bytes otherwise stay unchanged.
                let bytes = body.collect().await.unwrap().to_bytes();
                if query {
                    self.queries.fetch_add(1, Ordering::SeqCst);
                }
                let empty = bytes::Bytes::new();
                let lose = self.drop_batch && batch && !self.dropped.swap(true, Ordering::SeqCst);
                let body = Full::new(if lose { empty } else { bytes })
                    .map_err(|never| -> std::io::Error { match never {} })
                    .boxed_unsync();
                let mut response = http::Response::from_parts(parts, body);
                if lose {
                    *response.status_mut() = http::StatusCode::SERVICE_UNAVAILABLE;
                }
                response
            } else {
                response
            }
        })
    }
}
