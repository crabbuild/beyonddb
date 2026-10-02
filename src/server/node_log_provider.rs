//! Product enrollment adapter for Cellule's host-owned node durability.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use cellule_host::{FacilityResult, NodeDurabilityProvider, NodeDurabilityRotation};
use cellule_runtime::{
    Error, NodeLeaseGuard, Result,
    fleet::telemetry::CellTelemetryHandle,
    identity::{NodeId, SessionId},
    ltx::Limits,
    node::{
        durability::{NodeDurabilityConfig, NodeLogAuthority},
        log_transport::NodeLogTransport,
    },
};

use super::{PeerNodeLogTransport, PublishedNodeLogAuthority};

/// Recruits authoritative follower sets for the host's durability supervisor.
///
/// Constructing this adapter does not install it or enable follower proofs.
/// A serving host must gate recruitment until owner recovery completes.
pub struct PeerNodeDurabilityProvider {
    authority: Arc<PublishedNodeLogAuthority>,
    transport: Arc<PeerNodeLogTransport>,
    session: SessionId,
    node: NodeId,
    guard: NodeLeaseGuard,
    telemetry: CellTelemetryHandle,
    next_epoch: AtomicU64,
    recruitment_ready: Arc<AtomicBool>,
}

impl PeerNodeDurabilityProvider {
    /// Binds recruitment to one published node boot and transport identity.
    pub fn new(
        authority: PublishedNodeLogAuthority,
        transport: PeerNodeLogTransport,
        session: SessionId,
        node: NodeId,
        guard: NodeLeaseGuard,
        telemetry: CellTelemetryHandle,
    ) -> Result<Self> {
        if authority.session() != session
            || transport.session() != session
            || transport.node() != node
        {
            return Err(Error::PeerAuthorization(
                "node-log provider identities differ",
            ));
        }
        guard.check()?;
        Ok(Self {
            authority: Arc::new(authority),
            transport: Arc::new(transport),
            session,
            node,
            guard,
            telemetry,
            next_epoch: AtomicU64::new(1),
            recruitment_ready: Arc::new(AtomicBool::new(true)),
        })
    }

    /// Install during host startup, but defer recruitment until recovery is ready.
    #[must_use]
    pub fn with_recruitment_gate(mut self, ready: Arc<AtomicBool>) -> Self {
        self.recruitment_ready = ready;
        self
    }
}

impl NodeDurabilityProvider for PeerNodeDurabilityProvider {
    fn recruit(
        self: Arc<Self>,
        limits: Limits,
        required_follower_bytes: u64,
        live_node_limit: usize,
    ) -> Pin<Box<dyn Future<Output = FacilityResult<Option<NodeDurabilityConfig>>> + Send>> {
        Box::pin(async move {
            self.guard.check()?;
            if !self.recruitment_ready.load(Ordering::Acquire) {
                return Ok(None);
            }
            let epoch = self.next_epoch.load(Ordering::Acquire);
            let Some(members) = self
                .authority
                .recruit(epoch, required_follower_bytes, live_node_limit)
                .await
                .inspect_err(|error| {
                    tracing::warn!(
                        log_epoch = epoch,
                        error = %error,
                        "node-log follower enrollment failed"
                    );
                })?
            else {
                tracing::debug!(log_epoch = epoch, "node-log follower ensemble unavailable");
                return Ok(None);
            };
            self.guard.check()?;
            tracing::debug!(log_epoch = epoch, ?members, "node-log followers enrolled");
            let transport: Arc<dyn NodeLogTransport> = self.transport.clone();
            let authority: Arc<dyn NodeLogAuthority> = self.authority.clone();
            Ok(Some(NodeDurabilityConfig::new(
                self.session,
                self.node,
                epoch,
                members,
                transport,
                authority,
                self.guard.clone(),
                limits,
                self.telemetry.clone(),
            )?))
        })
    }

    fn rotation_required(
        self: Arc<Self>,
        live_node_limit: usize,
    ) -> Pin<Box<dyn Future<Output = FacilityResult<bool>> + Send>> {
        Box::pin(async move {
            // The host serializes recruitment, installation and rotation.
            // A failed/incomplete epoch must not be reported as healthy by
            // consulting a cached or newly selected candidate member set.
            Ok(self
                .authority
                .rotation_required(self.next_epoch.load(Ordering::Acquire), live_node_limit)
                .await?)
        })
    }

    fn rotation_event(&self, event: NodeDurabilityRotation) {
        tracing::debug!(?event, "node-log durability rotation");
        if event == NodeDurabilityRotation::Failed {
            tracing::warn!("node-log durability supervisor step failed");
        }
        if event == NodeDurabilityRotation::Started {
            self.next_epoch.fetch_add(1, Ordering::AcqRel);
        }
    }
}
