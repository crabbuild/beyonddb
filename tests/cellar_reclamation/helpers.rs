use crate::residency_fixture::Fixture;
use cellar::{bucket::Bucket, ownership_store::BucketOwnership, sdk::ExecutionDomain};
use std::time::Duration;

pub struct Probe {
    _files: tempfile::TempDir,
    pub bucket: Bucket,
    domain: ExecutionDomain,
    observer: BucketOwnership,
}
impl Probe {
    pub async fn fixture(
        host: &tokio::runtime::Handle,
        core: &tokio::runtime::Handle,
        rows: usize,
        resident: usize,
    ) -> (Fixture, Self) {
        // Keep provider storage alive independently of either SDK owner's files.
        let files = tempfile::tempdir().unwrap();
        let mut bucket = None;
        let fixture = Fixture::with_bucket(host, core, rows, resident, |_| {
            let value = Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
            bucket = Some(value.clone());
            value
        })
        .await;
        let bucket = bucket.unwrap();
        let probe = Self {
            _files: files,
            domain: ExecutionDomain::new(host.clone()),
            observer: BucketOwnership::new(
                bucket.clone(),
                bucket.clone(),
                "reclamation-inspection".into(),
                String::new(),
            ),
            bucket,
        };
        (fixture, probe)
    }
    // Read real sampled residency from the SDK lease, without activating cells.
    pub async fn at_capacity(&self, node: &str, resident: usize) {
        self.domain
            .run(async {
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        let peers = self.observer.read_capacity_peers().await.unwrap();
                        if let Some(peer) = peers.iter().find(|peer| peer.node == node) {
                            assert!(peer.resident_cells <= resident);
                            if peer.resident_cells == resident
                                && peer.owned_cells.is_some_and(|owned| owned > resident)
                            {
                                println!(
                                    "SDK residency: node={node} owned={:?} resident={}",
                                    peer.owned_cells, peer.resident_cells
                                );
                                break;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .unwrap();
            })
            .await
            .unwrap();
    }
    pub async fn close(self) {
        tokio::time::timeout(Duration::from_secs(10), self.domain.shutdown())
            .await
            .unwrap();
    }
}
pub async fn items(fixture: &Fixture) {
    for (_, expected) in &fixture.data {
        assert_eq!(
            fixture
                .sdk
                .get_item()
                .table_name("Residency")
                .key("id", expected["id"].clone())
                .consistent_read(true)
                .send()
                .await
                .unwrap()
                .item
                .as_ref(),
            Some(expected)
        );
    }
}

// Completed split replay must not issue ingress against historical sources.
pub struct ForbidSources {
    pub targets: Vec<bdb_cellar_runtime::identity::CellTarget>,
    pub refused: std::sync::atomic::AtomicUsize,
}
impl bdb_cellar_runtime::client::InvocationAdmission for ForbidSources {
    fn check<'a>(
        &'a self,
        invocation: &'a bdb_cellar_runtime::client::Invocation<'a>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = bdb_cellar_runtime::Result<()>> + Send + 'a>,
    > {
        Box::pin(async move {
            if self.targets.contains(&invocation.target) {
                self.refused
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Err(bdb_cellar_runtime::Error::Capacity(
                    "historical source ingress forbidden",
                ));
            }
            Ok(())
        })
    }
}
