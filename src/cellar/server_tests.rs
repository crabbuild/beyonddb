use super::*;
use bdb_cellar_runtime::{
    client::{Invocation, InvocationAdmission},
    registry::Query,
};
use std::{
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
};

struct AccountAdmission {
    refuse: Arc<AtomicBool>,
    healthy: Arc<AtomicUsize>,
}
impl InvocationAdmission for AccountAdmission {
    fn check<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
    ) -> Pin<Box<dyn Future<Output = bdb_cellar_runtime::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if invocation.operation == crate::ListCoordinatorShards::ID {
                if invocation.target == crate::account_target("123456789012")?
                    && self.refuse.load(Ordering::SeqCst)
                {
                    return Err(bdb_cellar_runtime::Error::Capacity(
                        "startup account admission withheld",
                    ));
                }
                if invocation.target == crate::account_target("210987654321")? {
                    self.healthy.fetch_add(1, Ordering::SeqCst);
                }
            }
            Ok(())
        })
    }
}

#[test]
fn failed_initial_recovery_starts_no_maintenance_and_can_be_retried() {
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    host.block_on(async {
        let files = tempfile::tempdir().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let (node, client) = crate::test_support::start(
            files.path(),
            Arc::new(AtomicI64::new(i64::try_from(now).unwrap())),
            host.handle(),
            core.handle(),
        )
        .await
        .unwrap();
        let refuse = Arc::new(AtomicBool::new(true));
        let healthy = Arc::new(AtomicUsize::new(0));
        let client = client.with_admission(Arc::new(AccountAdmission {
            refuse: refuse.clone(),
            healthy: healthy.clone(),
        }));
        let application = Arc::new(
            Beyonddb::compile(BuildDescriptor {
                source_revision: "domain-sdk-test".into(),
                cargo_lock_digest: Digest::from_bytes([1; 32]),
            })
            .unwrap(),
        );
        let server = CellarServer {
            node,
            provisioner: Arc::new(CellInitialPartitionProvisioner::new(
                client.clone(),
                application,
            )),
            client,
            tasks: TaskGroup::default(),
            region: "local".into(),
            encryption_key: [7; 32],
            cache_enabled: true,
        };
        let accounts = vec!["123456789012".into(), "210987654321".into()];
        let failure = server
            .start_account_maintenance(accounts.clone(), u64::MAX)
            .await
            .unwrap_err();
        assert!(matches!(
            failure.downcast_ref::<StorageError>(),
            Some(StorageError::LimitExceeded(_))
        ));
        assert!(
            healthy.load(Ordering::SeqCst) > 0,
            "later accounts must still be inspected"
        );
        assert!(server.tasks.tasks.lock().unwrap().is_empty());
        assert!(!server.tasks.cancellation_token().is_cancelled());
        assert!(server.node.is_ready().await.unwrap());
        refuse.store(false, Ordering::SeqCst);
        server
            .start_account_maintenance(accounts, u64::MAX)
            .await
            .unwrap();
        assert!(!server.tasks.tasks.lock().unwrap().is_empty());
        let NodeOutcome::Stopped(report) = server.shutdown(ShutdownMode::Preserve).await.unwrap()
        else {
            panic!("unexpected native shutdown")
        };
        assert!(report.ingress_drained && report.application_work_drained && report.cells_drained);
        assert!(!report.deadline_elapsed && !report.drain_timed_out);
    });
}
