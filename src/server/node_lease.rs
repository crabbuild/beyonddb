//! Published node-session lease for a BeyondDB serving host.

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use cellule_runtime::node::{NodeAdvertisement, NodeDirectory, VersionedNodeAdvertisement};
use cellule_runtime::{Error, NodeLeaseGuard, Result};
use tokio::sync::{Mutex, watch};
use tokio_util::sync::CancellationToken;

// Use the runtime's maximum advertisement lifetime for storage refresh headroom.
// Takeover still requires expiry, and a refresh cannot revive a fenced guard.
pub(super) const LEASE_MS: i64 = 15_000;
const HEARTBEAT: Duration = Duration::from_secs(3);
const RETRY: Duration = Duration::from_millis(500);
const FENCE_MARGIN: Duration = Duration::from_secs(1);

type SignAdvertisement = dyn Fn(i64, i64) -> Result<NodeAdvertisement> + Send + Sync;

#[derive(Clone, Copy)]
struct RenewalProgress {
    phase: &'static str,
    started: Instant,
}

fn renewal_phase(progress: &watch::Sender<RenewalProgress>, phase: &'static str) {
    progress.send_replace(RenewalProgress {
        phase,
        started: Instant::now(),
    });
}

/// Publishes signed node advertisements into the authoritative object-store directory.
pub struct NodeLeasePublisher {
    directory: NodeDirectory,
    sign: Arc<SignAdvertisement>,
}

impl NodeLeasePublisher {
    /// Bind the directory and the serving node's fixed boot-session signer.
    ///
    /// The signer must preserve its node, session, fleet, image, release, and
    /// key across renewals. The directory rejects a changed boot identity.
    /// It runs on a blocking worker so resource probes cannot block lease timers.
    pub fn new(
        directory: NodeDirectory,
        sign: impl Fn(i64, i64) -> Result<NodeAdvertisement> + Send + Sync + 'static,
    ) -> Self {
        Self {
            directory,
            sign: Arc::new(sign),
        }
    }

    /// Publish the initial lease before installing it in a Cell node.
    pub async fn publish(self) -> Result<PublishedNodeLease> {
        let now_ms = unix_time_ms()?;
        let advertisement = self.advertisement(now_ms, None).await?;
        let observed = self.directory.create(advertisement, now_ms).await?;
        // Object-store publication can take time; lease the remaining
        // authoritative window, not a fresh window after the response.
        let guard = NodeLeaseGuard::new(unix_time_ms()?, observed.advertisement().expires_at_ms())?;
        Ok(PublishedNodeLease {
            publisher: self,
            session: observed.advertisement().session(),
            observed,
            log_transitions: Arc::new(Mutex::new(())),
            guard,
            fence_on_drop: true,
        })
    }

    async fn advertisement(
        &self,
        now_ms: i64,
        progress: Option<&watch::Sender<RenewalProgress>>,
    ) -> Result<NodeAdvertisement> {
        let expires = lease_expiry(now_ms)?;
        let sign = Arc::clone(&self.sign);
        let progress = progress.cloned();
        // Only one sample is in flight per publisher. Its timestamp precedes
        // dispatch, so queue/probe latency cannot extend the signed lease.
        tokio::task::spawn_blocking(move || {
            if let Some(progress) = &progress {
                renewal_phase(progress, "capacity-signing");
            }
            sign(now_ms, expires)
        })
        .await
        .map_err(|source| Error::Facility {
            name: "node-capacity-signing",
            source: Box::new(source),
        })?
    }
}

/// Retained lease task for a serving Cell node.
pub struct PublishedNodeLease {
    publisher: NodeLeasePublisher,
    session: cellule_runtime::identity::SessionId,
    observed: VersionedNodeAdvertisement,
    log_transitions: Arc<Mutex<()>>,
    guard: NodeLeaseGuard,
    fence_on_drop: bool,
}

impl PublishedNodeLease {
    /// Clone the guard to install in the Cell runtime before starting requests.
    #[must_use]
    pub fn guard(&self) -> NodeLeaseGuard {
        self.guard.clone()
    }

    /// Bind node-log CAS operations to this exact published boot session.
    ///
    /// The adapter reloads the authoritative record after heartbeat races;
    /// it cannot publish once this lease guard is fenced.
    #[must_use]
    pub fn log_authority(&self) -> super::PublishedNodeLogAuthority {
        super::PublishedNodeLogAuthority::new(
            self.publisher.directory.clone(),
            self.session,
            self.guard.clone(),
            Arc::clone(&self.log_transitions),
        )
    }

    /// Refresh the authoritative lease until cancellation or terminal failure.
    ///
    /// Serving hosts retain this task in their lease-maintenance phase until
    /// runtime drain finishes. Cancellation leaves the current deadline intact;
    /// serving composition retires the session through `shutdown_serving_node`.
    pub async fn run(mut self, cancellation: &CancellationToken) -> Result<()> {
        let guard = self.guard.clone();
        let (progress, observed_progress) = watch::channel(RenewalProgress {
            phase: "heartbeat-timer",
            started: Instant::now(),
        });
        // A storage request can outlive the lease or shutdown. Dropping its
        // future cannot revoke a remote CAS, but must never renew this guard.
        tokio::select! {
            () = cancellation.cancelled() => {
                self.fence_on_drop = false;
                Ok(())
            },
            () = guard.wait_fenced() => {
                let observed = *observed_progress.borrow();
                tracing::warn!(
                    diagnostic = "node-lease-renewal-phase",
                    session = ?self.session,
                    phase = observed.phase,
                    phase_elapsed_ms = observed.started.elapsed().as_secs_f64() * 1000.0,
                    "serving node lease fenced while waiting for renewal",
                );
                Err(Error::Fenced)
            },
            result = self.renew(&progress) => result,
        }
    }

    async fn renew(&mut self, progress: &watch::Sender<RenewalProgress>) -> Result<()> {
        let mut ticks = tokio::time::interval(HEARTBEAT);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticks.tick().await;
        loop {
            renewal_phase(progress, "heartbeat-timer");
            ticks.tick().await;
            loop {
                match self.refresh(progress).await {
                    Ok(()) => break,
                    Err(error) => {
                        let observed = *progress.borrow();
                        tracing::warn!(
                            diagnostic = "node-lease-renewal-phase",
                            session = ?self.session,
                            phase = observed.phase,
                            phase_elapsed_ms = observed.started.elapsed().as_secs_f64() * 1000.0,
                            lease_remaining_ms = self.guard.remaining().as_secs_f64() * 1000.0,
                            error = %error,
                            "serving node lease refresh failed",
                        );
                        if self.guard.remaining() > FENCE_MARGIN {
                            renewal_phase(progress, "retry-delay");
                            tokio::time::sleep(RETRY).await;
                            self.guard.check()?;
                            if self.guard.remaining() <= FENCE_MARGIN {
                                self.guard.fence();
                                return Err(error);
                            }
                        } else {
                            self.guard.fence();
                            return Err(error);
                        }
                    }
                }
            }
        }
    }

    async fn refresh(&mut self, progress: &watch::Sender<RenewalProgress>) -> Result<()> {
        self.guard.check()?;
        let now_ms = unix_time_ms()?;
        renewal_phase(progress, "capacity-queue");
        let next = self.publisher.advertisement(now_ms, Some(progress)).await?;
        self.guard.check()?;
        renewal_phase(progress, "directory-refresh");
        let renewed = self
            .publisher
            .directory
            .refresh(&self.observed, next, now_ms)
            .await?;
        renewal_phase(progress, "guard-renewal");
        self.guard
            .renew(unix_time_ms()?, renewed.advertisement().expires_at_ms())?;
        self.observed = renewed;
        Ok(())
    }
}

impl Drop for PublishedNodeLease {
    fn drop(&mut self) {
        if self.fence_on_drop {
            self.guard.fence();
        }
    }
}

pub(super) fn unix_time_ms() -> Result<i64> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Node("system clock is before Unix epoch"))?
            .as_millis(),
    )
    .map_err(|_| Error::Node("system clock exceeds node lease range"))
}

fn lease_expiry(now_ms: i64) -> Result<i64> {
    now_ms
        .checked_add(LEASE_MS)
        .ok_or(Error::Node("node lease expiry overflow"))
}
