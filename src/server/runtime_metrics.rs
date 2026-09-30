//! Optional bounded runtime observations for performance diagnosis.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cellule_runtime::fleet::telemetry::{
    ActivationPhase, CatalogReadKind, CellTelemetry, CommandResponseSource,
    DurabilitySubmissionOutcome, PrimitiveOperationKind, PrimitiveOperationOutcome,
    PublicationTiming,
};
use cellule_runtime::identity::CellId;
use serde_json::{Value, json};

#[derive(Default)]
struct Timing {
    count: AtomicU64,
    failed: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
}

impl Timing {
    fn record(&self, elapsed: Duration, succeeded: bool) {
        let micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        self.total_us.fetch_add(micros, Ordering::Relaxed);
        self.max_us.fetch_max(micros, Ordering::Relaxed);
        if !succeeded {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> Value {
        json!({
            "count": self.count.load(Ordering::Relaxed),
            "failed": self.failed.load(Ordering::Relaxed),
            "total_us": self.total_us.load(Ordering::Relaxed),
            "max_us": self.max_us.load(Ordering::Relaxed),
        })
    }
}

/// Fixed counters and timing sums; event callbacks perform no I/O or allocation.
///
/// Snapshots observe concurrent atomics and are approximate, not a transactional
/// ledger. They include background runtime work. No Cell, tenant, or item IDs
/// appear as labels, and command response counts exclude queries and transport.
#[derive(Default)]
pub struct RuntimeMetrics {
    first_snapshot_at_unix_ms: std::sync::OnceLock<Option<u128>>,
    response: [Timing; 3],
    confirmation: Timing,
    queue: Timing,
    worker: Timing,
    primitive: [Timing; 2],
    publication: [Timing; 4],
    submission: [AtomicU64; 4],
    append: [AtomicU64; 2],
    append_bytes: AtomicU64,
    uploaded_objects: AtomicU64,
    uploaded_bytes: AtomicU64,
    catalog: [Timing; 2],
    control: Timing,
    activation: [Timing; 5],
}

impl RuntimeMetrics {
    /// Returns a bounded JSON snapshot with durations in microseconds.
    pub fn snapshot(&self) -> Value {
        let sampled_at_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_millis());
        json!({
            "version": 1,
            "first_snapshot_at_unix_ms": self.first_snapshot_at_unix_ms.get_or_init(|| sampled_at_unix_ms),
            "sampled_at_unix_ms": sampled_at_unix_ms,
            "command_responses": {
                "recorded": self.response[0].snapshot(),
                "fleet": self.response[1].snapshot(),
                "object": self.response[2].snapshot(),
            },
            "confirmation": self.confirmation.snapshot(),
            "command_queue": self.queue.snapshot(),
            "command_worker": self.worker.snapshot(),
            "primitive_execution": {
                "command": self.primitive[0].snapshot(),
                "query": self.primitive[1].snapshot(),
            },
            "publication": {
                "queue": self.publication[0].snapshot(),
                "preparation": self.publication[1].snapshot(),
                "authority": self.publication[2].snapshot(),
                "total": self.publication[3].snapshot(),
                "uploaded_objects": self.uploaded_objects.load(Ordering::Relaxed),
                "uploaded_bytes": self.uploaded_bytes.load(Ordering::Relaxed),
            },
            "durability_submissions": {
                "fleet": self.submission[0].load(Ordering::Relaxed),
                "unsupported": self.submission[1].load(Ordering::Relaxed),
                "unavailable": self.submission[2].load(Ordering::Relaxed),
                "rejected": self.submission[3].load(Ordering::Relaxed),
            },
            "follower_appends": {
                "acknowledged": self.append[0].load(Ordering::Relaxed),
                "failed": self.append[1].load(Ordering::Relaxed),
                "bytes": self.append_bytes.load(Ordering::Relaxed),
            },
            "catalog_reads": {"head": self.catalog[0].snapshot(), "page": self.catalog[1].snapshot()},
            "control_reads": self.control.snapshot(),
            "activation": {
                "ownership": self.activation[0].snapshot(),
                "resume": self.activation[1].snapshot(),
                "root_open": self.activation[2].snapshot(),
                "restore": self.activation[3].snapshot(),
                "activate": self.activation[4].snapshot(),
            },
        })
    }
}

impl CellTelemetry for RuntimeMetrics {
    fn command_response(
        &self,
        source: CommandResponseSource,
        elapsed: Duration,
        confirmation: Duration,
    ) {
        let index = match source {
            CommandResponseSource::Recorded => 0,
            CommandResponseSource::Fleet => 1,
            CommandResponseSource::Object => 2,
        };
        self.response[index].record(elapsed, true);
        self.confirmation.record(confirmation, true);
    }

    fn command_execution(&self, queue: Duration, worker: Duration, succeeded: bool) {
        self.queue.record(queue, succeeded);
        self.worker.record(worker, succeeded);
    }

    fn primitive_operation(
        &self,
        _module: &'static str,
        kind: PrimitiveOperationKind,
        outcome: PrimitiveOperationOutcome,
        elapsed: Duration,
    ) {
        let index = match kind {
            PrimitiveOperationKind::Command => 0,
            PrimitiveOperationKind::Query => 1,
        };
        self.primitive[index].record(elapsed, outcome != PrimitiveOperationOutcome::Failed);
    }

    fn publication_completed(&self, _cell: CellId, timing: PublicationTiming) {
        for (metric, elapsed) in self.publication.iter().zip([
            timing.queue_wait,
            timing.preparation,
            timing.authority,
            timing.total,
        ]) {
            metric.record(elapsed, timing.succeeded);
        }
    }

    fn durability_submission(&self, outcome: DurabilitySubmissionOutcome) {
        let index = match outcome {
            DurabilitySubmissionOutcome::Fleet => 0,
            DurabilitySubmissionOutcome::Unsupported => 1,
            DurabilitySubmissionOutcome::Unavailable => 2,
            DurabilitySubmissionOutcome::Rejected => 3,
        };
        self.submission[index].fetch_add(1, Ordering::Relaxed);
    }

    fn publication_cost(&self, objects: u64, bytes: u64) {
        self.uploaded_objects.fetch_add(objects, Ordering::Relaxed);
        self.uploaded_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn node_log_append(&self, acknowledged: bool, bytes: u64) {
        self.append[usize::from(!acknowledged)].fetch_add(1, Ordering::Relaxed);
        self.append_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn catalog_read(&self, kind: CatalogReadKind, elapsed: Duration, succeeded: bool) {
        let index = match kind {
            CatalogReadKind::Head => 0,
            CatalogReadKind::Page => 1,
        };
        self.catalog[index].record(elapsed, succeeded);
    }

    fn control_read(&self, elapsed: Duration, succeeded: bool) {
        self.control.record(elapsed, succeeded);
    }

    fn activation_phase(&self, phase: ActivationPhase, elapsed: Duration) {
        let index = match phase {
            ActivationPhase::Ownership => 0,
            ActivationPhase::Resume => 1,
            ActivationPhase::RootOpen => 2,
            ActivationPhase::Restore => 3,
            ActivationPhase::Activate => 4,
        };
        self.activation[index].record(elapsed, true);
    }
}
