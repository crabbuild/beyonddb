//! Bounded concurrent initialization of fresh coordinator Cells.

use std::sync::Mutex;

use cellule_runtime::cell::actor::{CellHandle, CellRuntime};
use cellule_runtime::control::{ControlState, Owner, authority::CellAuthority};
use cellule_runtime::identity::{CellTarget, IncarnationId};
use cellule_runtime::{Error, Result};

use super::CellInitialPartitionProvisioner;

const GATES: usize = 64;

pub(super) struct CoordinatorBootstrap {
    gates: [tokio::sync::Mutex<()>; GATES],
    pending: Mutex<usize>,
}

impl Default for CoordinatorBootstrap {
    fn default() -> Self {
        Self {
            gates: std::array::from_fn(|_| tokio::sync::Mutex::new(())),
            pending: Mutex::new(0),
        }
    }
}

impl CoordinatorBootstrap {
    fn reserve(&self, runtime: &CellRuntime) -> Result<Option<PendingBootstrap<'_>>> {
        let mut pending = self.pending.lock().map_err(|_| Error::RuntimeClosed)?;
        let stats = runtime.stats();
        // All exclusive product admission/reclamation waits for our read guards.
        // Cellule's active count includes its in-flight activation reservations.
        // Counting those again while this guard exists is conservative. Reading
        // stats under this mutex prevents a completion from disappearing between
        // the active and pending observations.
        if stats.active_cells().saturating_add(*pending) >= stats.active_cell_capacity() {
            return Ok(None);
        }
        *pending += 1;
        Ok(Some(PendingBootstrap {
            pending: &self.pending,
        }))
    }
}

struct PendingBootstrap<'a> {
    pending: &'a Mutex<usize>,
}

impl Drop for PendingBootstrap<'_> {
    fn drop(&mut self) {
        let mut pending = match self.pending.lock() {
            Ok(pending) => pending,
            Err(poisoned) => poisoned.into_inner(),
        };
        *pending = pending.saturating_sub(1);
    }
}

impl CellInitialPartitionProvisioner {
    pub(super) async fn try_bootstrap_coordinator(
        &self,
        target: &CellTarget,
        proof: cellule_runtime::cell::catalog::CatalogProof,
        initialize: for<'a> fn(&cellule_ltx::rusqlite::Transaction<'a>) -> Result<()>,
    ) -> Result<Option<CellHandle>> {
        if target.namespace() != crate::transaction_coordinator::NAMESPACE {
            return Ok(None);
        }
        // Fixed stripes bound memory and serialize identical Cells. Unrelated
        // stripes can initialize concurrently; collisions only delay admission.
        let stripe = usize::from(target.cell_id().as_bytes()[0]) % GATES;
        let _cell = self.coordinator_bootstrap.gates[stripe].lock().await;
        let _admission = self.admission.read().await;
        let authority = CellAuthority::new(self.layout.clone());
        let observed = authority.load(target.cell_id()).await?;
        if let Some(observed) = &observed {
            if let Some(handle) = self.runtime.local_handle(proof.clone(), observed).await? {
                self.track_coordinator(target)
                    .map_err(super::admission_error)?;
                return Ok(Some(handle));
            }
            if observed.value().root.is_some()
                || observed.value().state != ControlState::Recovering
                || observed.value().owner.as_ref().map(|owner| owner.session) != Some(self.session)
            {
                // Published restore and peer ownership retain exclusive admission.
                return Ok(None);
            }
        }
        let Some(_reservation) = self.coordinator_bootstrap.reserve(&self.runtime)? else {
            // Drop the shared guards before the caller reclaims under exclusivity.
            return Ok(None);
        };
        let observed = match observed {
            Some(observed) => observed,
            None => {
                let incarnation = IncarnationId::from_bytes(*uuid::Uuid::now_v7().as_bytes());
                match authority
                    .create_initial(
                        &proof,
                        incarnation,
                        Owner {
                            session: self.session,
                            endpoint: self.endpoint.clone(),
                        },
                    )
                    .await
                {
                    Ok(observed) => observed,
                    Err(Error::CellAlreadyActive) => return Ok(None),
                    Err(error) => return Err(error),
                }
            }
        };
        // Before runtime admission, cancellation drops the local reservation.
        // After admission, Cellule retains its own counted activation reservation
        // until completion/cleanup, even when this request stops awaiting it.
        self.bootstrap_unpublished(target, proof, observed, initialize)
            .await
            .map(Some)
    }
}
