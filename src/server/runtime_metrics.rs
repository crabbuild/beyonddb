//! Optional bounded runtime observations for performance diagnosis.

use std::{
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use cellule_runtime::fleet::telemetry::{
    ActivationPhase, CatalogReadKind, CellTelemetry, CommandResponseSource,
    DurabilitySubmissionOutcome, PrimitiveOperationKind, PrimitiveOperationOutcome,
    PublicationTiming,
};
use cellule_runtime::identity::CellId;
use cellule_store::{StorageObservation, StorageObserver, StorageOperation, StorageOutcome};
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

#[derive(Default)]
struct StoreOperationMetrics {
    timing: Timing,
    started: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    outcomes: [AtomicU64; StorageOutcome::ALL.len()],
}

#[derive(Clone, Copy)]
pub(super) enum FollowerPhase {
    PeerLookup = 0,
    RoundTrip = 1,
    Enrollment = 2,
    DurableAppend = 3,
}

#[derive(Default)]
struct FollowerPhaseMetrics {
    timing: Timing,
    started: AtomicU64,
    cancelled: AtomicU64,
}

impl FollowerPhaseMetrics {
    fn snapshot(&self) -> Value {
        let mut value = self.timing.snapshot();
        value["started"] = json!(self.started.load(Ordering::Relaxed));
        value["cancelled"] = json!(self.cancelled.load(Ordering::Relaxed));
        value["in_flight"] = json!(
            self.started
                .load(Ordering::Relaxed)
                .saturating_sub(self.timing.count.load(Ordering::Relaxed))
        );
        value
    }
}

struct FollowerObservation<'a> {
    metrics: &'a FollowerPhaseMetrics,
    started: Instant,
    outcome: Option<bool>,
}

impl Drop for FollowerObservation<'_> {
    fn drop(&mut self) {
        if self.outcome.is_none() {
            self.metrics.cancelled.fetch_add(1, Ordering::Relaxed);
        }
        self.metrics
            .timing
            .record(self.started.elapsed(), self.outcome == Some(true));
    }
}

/// Observe only append phases; recovery operations are excluded. Dropped futures
/// count as cancelled failures. With metrics disabled, no clock or atomics run.
pub(super) async fn observe_follower<T>(
    metrics: Option<&RuntimeMetrics>,
    phase: FollowerPhase,
    future: impl Future<Output = cellule_runtime::Result<T>>,
) -> cellule_runtime::Result<T> {
    let Some(metrics) = metrics else {
        return future.await;
    };
    let metrics = &metrics.follower_phases[phase as usize];
    metrics.started.fetch_add(1, Ordering::Relaxed);
    let mut observation = FollowerObservation {
        metrics,
        started: Instant::now(),
        outcome: None,
    };
    let result = future.await;
    observation.outcome = Some(result.is_ok());
    result
}

impl StoreOperationMetrics {
    fn snapshot(&self) -> Value {
        let mut value = self.timing.snapshot();
        let started = self.started.load(Ordering::Relaxed);
        let finished = self.timing.count.load(Ordering::Relaxed);
        value["count"] = json!(finished);
        value["started"] = json!(started);
        value["in_flight"] = json!(started.saturating_sub(finished));
        value["bytes_read"] = json!(self.bytes_read.load(Ordering::Relaxed));
        value["bytes_written"] = json!(self.bytes_written.load(Ordering::Relaxed));
        value["outcomes"] = Value::Object(
            StorageOutcome::ALL
                .into_iter()
                .map(|outcome| {
                    (
                        outcome.label().to_owned(),
                        json!(self.outcomes[outcome.index()].load(Ordering::Relaxed)),
                    )
                })
                .collect(),
        );
        value
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
    store: [StoreOperationMetrics; StorageOperation::ALL.len()],
    follower_phases: [FollowerPhaseMetrics; 4],
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
            "follower_append_phases": {
                "peer_lookup": self.follower_phases[0].snapshot(),
                "round_trip": self.follower_phases[1].snapshot(),
                "enrollment": self.follower_phases[2].snapshot(),
                "durable_append": self.follower_phases[3].snapshot(),
            },
            "catalog_reads": {"head": self.catalog[0].snapshot(), "page": self.catalog[1].snapshot()},
            "control_reads": self.control.snapshot(),
            "object_store": StorageOperation::ALL.into_iter().map(|operation| {
                (operation.label().to_owned(), self.store[operation.index()].snapshot())
            }).collect::<serde_json::Map<_, _>>(),
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

impl StorageObserver for RuntimeMetrics {
    fn started(&self, operation: StorageOperation) {
        self.store[operation.index()]
            .started
            .fetch_add(1, Ordering::Relaxed);
    }

    fn finished(&self, observation: StorageObservation) {
        let metric = &self.store[observation.operation.index()];
        metric
            .bytes_read
            .fetch_add(observation.bytes_read, Ordering::Relaxed);
        metric
            .bytes_written
            .fetch_add(observation.bytes_written, Ordering::Relaxed);
        metric.outcomes[observation.outcome.index()].fetch_add(1, Ordering::Relaxed);
        metric.timing.record(
            observation.duration,
            observation.outcome == StorageOutcome::Success,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellule_store::Store;
    use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
    use std::sync::Arc;
    use tokio_util::bytes::Bytes;

    #[tokio::test]
    async fn follower_metrics_finish_when_an_in_flight_append_is_cancelled() {
        let metrics = RuntimeMetrics::default();
        let observed = observe_follower(
            Some(&metrics),
            FollowerPhase::DurableAppend,
            std::future::pending::<cellule_runtime::Result<()>>(),
        );
        let mut observed = Box::pin(observed);
        assert!(futures_util::poll!(&mut observed).is_pending());
        let snapshot = metrics.snapshot();
        assert_eq!(
            snapshot["follower_append_phases"]["durable_append"]["in_flight"],
            1
        );
        drop(observed);
        let snapshot = metrics.snapshot();
        let phase = &snapshot["follower_append_phases"]["durable_append"];
        assert_eq!(phase["count"], 1);
        assert_eq!(phase["failed"], 1);
        assert_eq!(phase["cancelled"], 1);
        assert_eq!(phase["in_flight"], 0);
    }

    #[tokio::test]
    async fn store_metrics_observe_consumption_cancellation_and_missing_objects_without_labels() {
        let metrics = Arc::new(RuntimeMetrics::default());
        let store = Store::new(Arc::new(InMemory::new())).with_storage_observer(metrics.clone());
        let path = Path::from("private-account/never-a-metric-label");
        store
            .inner()
            .put(&path, Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        let body = store
            .inner()
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(body.as_ref(), b"abc");
        // A returned body is still an active operation until consumed or dropped.
        let unconsumed = store.inner().get(&path).await.unwrap();
        assert_eq!(metrics.snapshot()["object_store"]["get"]["in_flight"], 1);
        drop(unconsumed);
        assert!(
            store
                .inner()
                .head(&Path::from("missing-private-object"))
                .await
                .is_err()
        );
        let snapshot = metrics.snapshot();
        let get = &snapshot["object_store"]["get"];
        assert_eq!(get["count"], 2);
        assert_eq!(get["failed"], 1);
        assert_eq!(get["bytes_read"], 3);
        assert_eq!(get["in_flight"], 0);
        assert_eq!(get["outcomes"]["success"], 1);
        assert_eq!(get["outcomes"]["cancelled"], 1);
        let put = &snapshot["object_store"]["put"];
        assert_eq!(put["count"], 1);
        assert_eq!(put["bytes_written"], 3);
        assert_eq!(snapshot["object_store"]["head"]["outcomes"]["not_found"], 1);
        let serialized = snapshot.to_string();
        assert!(!serialized.contains("private-account"));
        assert!(!serialized.contains("never-a-metric-label"));
        assert!(!serialized.contains("missing-private-object"));
    }
}
