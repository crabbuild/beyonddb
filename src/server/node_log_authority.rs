//! Authoritative node-log transitions for a published BeyondDB boot session.

use std::sync::Arc;

use futures_util::future::BoxFuture;
use tokio::sync::Mutex;

use cellule_runtime::{
    Error, NodeLeaseGuard, Result,
    identity::{NodeId, SessionId},
    node::{NodeDirectory, VersionedNodeAdvertisement},
    node::{
        durability::NodeLogAuthority,
        log::NodeLogRotationBarrier,
        log_state::{NodeLogPhase, NodeLogStatus},
    },
};

use super::node_lease::unix_time_ms;

const MAX_CAS_ATTEMPTS: usize = 4;

/// Directory-backed authority for one leased node session.
///
/// Each operation reloads the current version so a concurrent heartbeat does
/// not overwrite or permanently obstruct an enrolled log transition. The
/// directory still validates every transition and compares the exact ETag.
#[derive(Clone)]
pub struct PublishedNodeLogAuthority {
    directory: NodeDirectory,
    session: SessionId,
    guard: NodeLeaseGuard,
    observed: Arc<Mutex<VersionedNodeAdvertisement>>,
}

impl PublishedNodeLogAuthority {
    pub(super) const fn session(&self) -> SessionId {
        self.session
    }

    pub(super) fn new(
        directory: NodeDirectory,
        session: SessionId,
        guard: NodeLeaseGuard,
        observed: Arc<Mutex<VersionedNodeAdvertisement>>,
    ) -> Self {
        Self {
            directory,
            session,
            guard,
            observed,
        }
    }

    async fn load(&self, observed: &mut VersionedNodeAdvertisement) -> Result<i64> {
        self.guard.check()?;
        let now_ms = unix_time_ms()?;
        let current = self
            .directory
            .load(self.session, now_ms)
            .await?
            .ok_or(Error::Fenced)?;
        self.guard.check()?;
        *observed = current;
        Ok(now_ms)
    }

    /// Enroll a complete follower set for the next log epoch, if available.
    ///
    /// A heartbeat CAS race is retried against the latest signed record. A
    /// previously enrolled matching epoch returns its authoritative members.
    pub async fn recruit(
        &self,
        log_epoch: u64,
        required_follower_bytes: u64,
        live_node_limit: usize,
    ) -> Result<Option<Vec<NodeId>>> {
        let mut observed = self.observed.lock().await;
        let mut last_error = None;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let now_ms = self.load(&mut observed).await?;
            if let Some(log) = observed.advertisement().log() {
                if log.epoch() != log_epoch || log.phase() != NodeLogPhase::Open || log.active() {
                    return Err(Error::Node("node session has a different or active log"));
                }
                return Ok(Some(log.members().to_vec()));
            }
            match self
                .directory
                .try_recruit_log(
                    &observed,
                    log_epoch,
                    required_follower_bytes,
                    live_node_limit,
                    now_ms,
                )
                .await
            {
                Ok(Some(enrolled)) => {
                    *observed = enrolled;
                    self.guard.check()?;
                    let log = observed
                        .advertisement()
                        .log()
                        .ok_or(Error::Node("enrolled node log is missing"))?;
                    return Ok(Some(log.members().to_vec()));
                }
                Ok(None) => return Ok(None),
                Err(error) => last_error = Some(error),
            }
        }
        Err(cas_error(
            last_error,
            "node-log enrollment CAS did not complete",
        ))
    }

    /// Checks the exact current epoch's members before host-owned rotation.
    ///
    /// Read the canonical enrollment independently of the heartbeat mutex.
    /// A slow membership scan must not prevent this node from renewing its
    /// own lease. The result requests rotation; it grants no append authority.
    pub async fn rotation_required(&self, log_epoch: u64, live_node_limit: usize) -> Result<bool> {
        self.guard.check()?;
        let current = self
            .directory
            .load(self.session, unix_time_ms()?)
            .await?
            .ok_or(Error::Fenced)?;
        self.guard.check()?;
        let members = exact_open_log(&current, log_epoch)?.members().to_vec();
        let live = self
            .directory
            .live(unix_time_ms()?, live_node_limit)
            .await?;
        self.guard.check()?;
        // Scan I/O can outlive a follower advertisement. Recheck expiry at
        // completion rather than treating its pre-scan timestamp as fresh.
        let now_ms = unix_time_ms()?;
        Ok(!members.iter().all(|member| {
            live.iter().any(|advertisement| {
                advertisement.node() == *member && advertisement.expires_at_ms() > now_ms
            })
        }))
    }

    async fn activate_epoch(&self, log_epoch: u64) -> Result<()> {
        let mut observed = self.observed.lock().await;
        let mut last_error = None;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let now_ms = self.load(&mut observed).await?;
            let log = exact_open_log(&observed, log_epoch)?;
            if log.active() {
                return Ok(());
            }
            match self.directory.activate_log(&observed, now_ms).await {
                Ok(updated) => {
                    *observed = updated;
                    self.guard.check()?;
                    return Ok(());
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(cas_error(
            last_error,
            "node-log activation CAS did not complete",
        ))
    }

    async fn advance_epoch_coverage(&self, log_epoch: u64, tiered_through: u64) -> Result<()> {
        let mut observed = self.observed.lock().await;
        let mut last_error = None;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let now_ms = self.load(&mut observed).await?;
            let log = exact_open_log(&observed, log_epoch)?;
            if log.tiered_through() >= tiered_through {
                return Ok(());
            }
            match self
                .directory
                .advance_log_coverage(&observed, tiered_through, now_ms)
                .await
            {
                Ok(updated) => {
                    *observed = updated;
                    self.guard.check()?;
                    return Ok(());
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(cas_error(
            last_error,
            "node-log coverage CAS did not complete",
        ))
    }

    async fn close_epoch(&self, barrier: &NodeLogRotationBarrier) -> Result<()> {
        if barrier.leader_session() != self.session {
            return Err(Error::Node(
                "node-log close barrier belongs to another session",
            ));
        }
        let mut observed = self.observed.lock().await;
        let mut last_error = None;
        let mut attempted = false;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let now_ms = self.load(&mut observed).await?;
            let Some(log) = observed.advertisement().log() else {
                return if attempted {
                    Ok(())
                } else {
                    Err(Error::Node("node session has no enrolled log"))
                };
            };
            if log.epoch() != barrier.log_epoch() {
                return Err(Error::Node("node-log close barrier epoch differs"));
            }
            attempted = true;
            match self.directory.close_log(&observed, barrier, now_ms).await {
                Ok(updated) => {
                    *observed = updated;
                    self.guard.check()?;
                    return Ok(());
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(cas_error(last_error, "node-log close CAS did not complete"))
    }
}

fn cas_error(last_error: Option<Error>, message: &'static str) -> Error {
    match last_error {
        Some(error) => error,
        None => Error::Node(message),
    }
}

fn exact_open_log(observed: &VersionedNodeAdvertisement, log_epoch: u64) -> Result<&NodeLogStatus> {
    let log = observed
        .advertisement()
        .log()
        .ok_or(Error::Node("node session has no enrolled log"))?;
    if log.epoch() != log_epoch || log.phase() != NodeLogPhase::Open {
        return Err(Error::Node("node-log epoch or phase differs"));
    }
    Ok(log)
}

impl NodeLogAuthority for PublishedNodeLogAuthority {
    fn activate<'a>(&'a self, log_epoch: u64) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.activate_epoch(log_epoch).await })
    }

    fn advance_coverage<'a>(
        &'a self,
        log_epoch: u64,
        tiered_through: u64,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.advance_epoch_coverage(log_epoch, tiered_through).await })
    }

    fn close<'a>(&'a self, barrier: &'a NodeLogRotationBarrier) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.close_epoch(barrier).await })
    }
}
