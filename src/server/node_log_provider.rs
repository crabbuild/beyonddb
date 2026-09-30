//! Product enrollment adapter for Cellule's host-owned node durability.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
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
/// The serving binary must complete owner recovery before installing it.
pub struct PeerNodeDurabilityProvider {
    authority: Arc<PublishedNodeLogAuthority>,
    transport: Arc<PeerNodeLogTransport>,
    session: SessionId,
    node: NodeId,
    guard: NodeLeaseGuard,
    telemetry: CellTelemetryHandle,
    next_epoch: AtomicU64,
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
        })
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
            let epoch = self.next_epoch.load(Ordering::Acquire);
            let Some(members) = self
                .authority
                .recruit(epoch, required_follower_bytes, live_node_limit)
                .await?
            else {
                return Ok(None);
            };
            self.guard.check()?;
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

    fn rotation_event(&self, event: NodeDurabilityRotation) {
        if event == NodeDurabilityRotation::Started {
            self.next_epoch.fetch_add(1, Ordering::AcqRel);
        }
    }
}
