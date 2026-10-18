//! Typed client over Cellar's bounded, invocation-owned ingress.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

mod read_backpressure;

use bytes::Bytes;
use cellar::sdk::{IngressHandle, IngressResponse};
use http_body_util::{BodyExt, Full};

use crate::{
    Error, Result,
    application::{binding, cell_name},
    codec::{BoundedDecoder, BoundedEncoder, WireValue},
    identity::{CellId, CellTarget},
    registry::{Command, Query, Registry},
    wire::{ENVELOPE_LIMIT, Envelope, Reply},
};

pub use crate::wire::MutationIdentity;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub cell: CellId,
    pub commit_sequence: u64,
}

pub struct Committed<T> {
    pub output: T,
    pub receipt: Receipt,
}

pub type Observed<T> = Committed<T>;

/// Call metadata for trusted in-process transport instrumentation. It carries
/// no execution authority; the compiled gateway still validates the envelope.
pub struct Invocation<'a> {
    pub target: CellTarget,
    pub operation: u32,
    pub mutation: Option<MutationIdentity>,
    /// Bounded operation bytes, borrowed from the validated outbound envelope.
    pub input: &'a [u8],
}

/// Caller-side policy evaluated before entering the SDK or transport. A
/// refusal proves this attempt was not submitted; it says nothing about other
/// attempts with the same mutation identity. HTTP status codes never provide
/// this evidence. Policies must not submit the invocation themselves.
pub trait InvocationAdmission: Send + Sync {
    fn check<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

/// Application-owned admission transport. The default implementation uses the
/// real SDK ingress. Decorators can measure calls or model transport failures;
/// all returned bodies still pass through the client's bounded result decoder.
/// A transport must not retry mutations under a different identity.
pub trait InvocationTransport: Send + Sync {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>>;
}

impl InvocationTransport for IngressHandle {
    fn call<'a>(
        &'a self,
        _invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(IngressHandle::call(self, request))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InvocationError<T> {
    #[error("operation was durably rejected")]
    Rejected(Committed<T>),
    #[error("operation failed before submission: {0}")]
    NotStarted(Error),
    #[error("submitted operation requires resolution: {reason}")]
    Ambiguous {
        identity: Option<MutationIdentity>,
        reason: String,
    },
}

impl<T: std::fmt::Debug> std::fmt::Debug for Committed<T> {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.debug_struct("Committed")
            .field("output", &self.output)
            .field("receipt", &self.receipt)
            .finish()
    }
}

#[derive(Clone)]
pub struct CellClient {
    transport: Arc<dyn InvocationTransport>,
    admission: Option<Arc<dyn InvocationAdmission>>,
    registry: Arc<Registry>,
}

impl CellClient {
    /// Whether calls share the same transport, admission policy and registry.
    /// Application batching may combine calls only within this exact context.
    /// This comparison performs no I/O and grants no execution authority.
    pub fn shares_dispatch_context(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.transport, &other.transport)
            && Arc::ptr_eq(&self.registry, &other.registry)
            && match (&self.admission, &other.admission) {
                (None, None) => true,
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                _ => false,
            }
    }

    pub fn new(ingress: IngressHandle, registry: Arc<Registry>) -> Self {
        Self {
            transport: Arc::new(ingress),
            admission: None,
            registry,
        }
    }

    /// Decorate this client's trusted in-process transport without changing
    /// its registry, codecs, receipt checks or ambiguous-outcome handling.
    pub fn map_transport(
        &self,
        decorate: impl FnOnce(Arc<dyn InvocationTransport>) -> Arc<dyn InvocationTransport>,
    ) -> Self {
        Self {
            transport: decorate(self.transport.clone()),
            admission: self.admission.clone(),
            registry: self.registry.clone(),
        }
    }

    /// Pace read-only invocations through transient SDK admission pressure.
    /// Commands always retain their single-attempt ambiguous-outcome contract.
    /// No credential, policy or item values are cached by this policy.
    /// The caller must provide a Tokio time driver.
    pub fn with_read_backpressure(&self, maximum_wait: Duration) -> Self {
        self.map_transport(|inner| {
            Arc::new(read_backpressure::ReadBackpressure::new(
                inner,
                maximum_wait,
            ))
        })
    }

    /// Apply local admission before submission. SDK ingress and response
    /// failures retain their existing ambiguous-outcome contract.
    pub fn with_admission(&self, admission: Arc<dyn InvocationAdmission>) -> Self {
        Self {
            transport: self.transport.clone(),
            admission: Some(admission),
            registry: self.registry.clone(),
        }
    }

    pub async fn command<C: Command>(
        &self,
        target: &CellTarget,
        identity: MutationIdentity,
        input: C::Input,
    ) -> std::result::Result<Committed<C::Output>, InvocationError<C::Output>> {
        let operation = self
            .registry
            .command_contract::<C>(target.namespace())
            .map_err(InvocationError::NotStarted)?;
        let input = encode(&input, operation.input_limit).map_err(InvocationError::NotStarted)?;
        let envelope = Envelope {
            target: target.clone(),
            id: C::ID,
            version: C::CODEC_VERSION,
            mutation: Some(identity),
            minimum_sequence: 0,
            input,
        };
        let reply = self.call(&envelope).await?;
        let output = decode(&reply.output, operation.output_limit).map_err(|error| {
            InvocationError::Ambiguous {
                identity: Some(identity),
                reason: error.to_string(),
            }
        })?;
        let committed = Committed {
            output,
            receipt: Receipt {
                cell: target.cell_id(),
                commit_sequence: reply.sequence,
            },
        };
        if reply.rejected {
            return Err(InvocationError::Rejected(committed));
        }
        Ok(committed)
    }

    pub async fn query<Q: Query>(
        &self,
        target: &CellTarget,
        minimum: Option<Receipt>,
        input: Q::Input,
    ) -> std::result::Result<Observed<Q::Output>, InvocationError<Q::Output>> {
        let operation = self
            .registry
            .query_contract::<Q>(target.namespace())
            .map_err(InvocationError::NotStarted)?;
        if minimum.is_some_and(|receipt| receipt.cell != target.cell_id()) {
            return Err(InvocationError::NotStarted(Error::Command(
                "read receipt belongs to another cell",
            )));
        }
        let input = encode(&input, operation.input_limit).map_err(InvocationError::NotStarted)?;
        let envelope = Envelope {
            target: target.clone(),
            id: Q::ID,
            version: Q::CODEC_VERSION,
            mutation: None,
            minimum_sequence: minimum.map_or(0, |r| r.commit_sequence),
            input,
        };
        let reply = self.call(&envelope).await?;
        if reply.rejected {
            return Err(InvocationError::Ambiguous {
                identity: None,
                reason: "query returned a mutation rejection".into(),
            });
        }
        let output = decode(&reply.output, operation.output_limit).map_err(|error| {
            InvocationError::Ambiguous {
                identity: None,
                reason: error.to_string(),
            }
        })?;
        Ok(Committed {
            output,
            receipt: Receipt {
                cell: target.cell_id(),
                commit_sequence: reply.sequence,
            },
        })
    }

    async fn call<T>(&self, envelope: &Envelope) -> std::result::Result<Reply, InvocationError<T>> {
        let bytes = envelope.encode().map_err(InvocationError::NotStarted)?;
        let url = format!(
            "http://bdb.internal/_bdb/{}/{}",
            binding(envelope.target.namespace()),
            cell_name(&envelope.target)
        );
        let request = http::Request::builder()
            .method("POST")
            .uri(url)
            .header("content-type", "application/octet-stream")
            .body(Full::new(Bytes::from(bytes)))
            .map_err(|error| InvocationError::NotStarted(Error::Cellar(error.into())))?;
        let invocation = Invocation {
            target: envelope.target.clone(),
            operation: envelope.id,
            mutation: envelope.mutation,
            input: &envelope.input,
        };
        if let Some(admission) = &self.admission {
            admission
                .check(&invocation)
                .await
                .map_err(InvocationError::NotStarted)?;
        }
        let response = self.transport.call(&invocation, request).await;
        let status = response.status();
        let mut body = response.into_body();
        let mut bytes = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|error| InvocationError::Ambiguous {
                identity: envelope.mutation,
                reason: error.to_string(),
            })?;
            if let Some(data) = frame.data_ref() {
                if bytes.len().saturating_add(data.len()) > ENVELOPE_LIMIT {
                    return Err(InvocationError::Ambiguous {
                        identity: envelope.mutation,
                        reason: "result exceeds its frame budget".into(),
                    });
                }
                bytes.extend_from_slice(data);
            }
        }
        if status != http::StatusCode::OK {
            return Err(InvocationError::Ambiguous {
                identity: envelope.mutation,
                reason: format!("Cellar returned HTTP {status}"),
            });
        }
        Reply::decode(&bytes).map_err(|error| InvocationError::Ambiguous {
            identity: envelope.mutation,
            reason: error.to_string(),
        })
    }
}

fn encode<T: WireValue>(value: &T, limit: u32) -> Result<Vec<u8>> {
    let mut out = BoundedEncoder::new(limit)?;
    value.encode(&mut out)?;
    Ok(out.finish())
}

fn decode<T: WireValue>(bytes: &[u8], limit: u32) -> Result<T> {
    let mut input = BoundedDecoder::new(bytes, limit)?;
    let result = T::decode(&mut input)?;
    input.finish()?;
    Ok(result)
}
