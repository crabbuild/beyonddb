use crate::{
    native_peer,
    residency_fixture::{ACCOUNT, Fixture},
    table_helpers,
};
use bdb_cellar_runtime::{
    MutationIdentity,
    client::{Invocation, InvocationAdmission},
    identity::{CellTarget, NamespaceId, RequestId},
    registry::Command,
};
use beyonddb::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

pub const NAME: &str = "ResidencyPending";
pub const ACCESS: &str = "AKIAIOSFODNN7EXAMPLE";
pub fn namespace(module: &str) -> NamespaceId {
    native_peer::application()
        .cell_types()
        .iter()
        .find(|kind| kind.name() == module)
        .unwrap()
        .namespace()
}
pub struct RefuseInstall {
    namespace: NamespaceId,
    allowed: usize,
    pub calls: AtomicUsize,
}
impl RefuseInstall {
    pub fn new(base: bool, allowed: usize) -> Self {
        Self {
            namespace: namespace(if base {
                "beyonddb-data"
            } else {
                "beyonddb-global-index"
            }),
            allowed,
            calls: AtomicUsize::new(0),
        }
    }
}
impl InvocationAdmission for RefuseInstall {
    fn check<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
    ) -> Pin<Box<dyn Future<Output = bdb_cellar_runtime::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if invocation.target.namespace() == self.namespace
                && invocation.mutation.is_some()
                && invocation.operation == InstallPartition::ID
                && self.calls.fetch_add(1, Ordering::SeqCst) >= self.allowed
            {
                return Err(bdb_cellar_runtime::Error::Capacity(
                    "creation installation admission withheld",
                ));
            }
            Ok(())
        })
    }
}
pub fn mutation() -> MutationIdentity {
    let now = native_peer::now_ms();
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    }
}
pub async fn http(fixture: &Fixture, admission: Arc<dyn InvocationAdmission>) -> native_peer::Http {
    native_peer::Http::start(&fixture.client.with_admission(admission), ACCESS).await
}
pub async fn record(fixture: &Fixture) -> TableRecord {
    table_helpers::table(fixture, NAME).await
}
pub async fn route(fixture: &Fixture, id: &str) -> RoutePageOutcome {
    read_route_page(
        &fixture.client,
        &account_target(ACCOUNT).unwrap(),
        RoutePageInput {
            table_id: id.into(),
            start_key: None,
            after_lower: None,
            expected_epoch: None,
        },
    )
    .await
    .unwrap()
}
pub async fn first_state(fixture: &Fixture, table: &TableRecord, base: bool) -> CellTarget {
    let target = if base {
        data_target(ACCOUNT, &table.id, &[0; 16])
    } else {
        global_index_target(ACCOUNT, &table.global_secondary_indexes[0].id, &[0; 16])
    }
    .unwrap();
    if base {
        assert_eq!(
            fixture
                .client
                .query::<ReadPartitionState>(&target, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap()
                .spec
                .table
                .id,
            table.id
        );
    } else {
        assert_eq!(
            fixture
                .client
                .query::<ReadGlobalIndexPartition>(&target, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap()
                .table
                .id,
            table.id
        );
    }
    target
}
pub async fn partial(
    host: &tokio::runtime::Handle,
    core: &tokio::runtime::Handle,
    base: bool,
) -> (Fixture, TableRecord) {
    let mut fixture = Fixture::new(host, core, 2, 10).await;
    let refusal = Arc::new(RefuseInstall::new(base, 1));
    let http = http(&fixture, refusal.clone()).await;
    assert!(
        table_helpers::create(&http.sdk, NAME, true)
            .send()
            .await
            .is_err()
    );
    assert_eq!(refusal.calls.load(Ordering::SeqCst), 2);
    let table = record(&fixture).await;
    assert_eq!(
        table.placement,
        TablePlacement::Routed {
            initial_partitions: 2
        }
    );
    assert_eq!(
        fixture
            .sdk
            .describe_table()
            .table_name(NAME)
            .send()
            .await
            .unwrap()
            .table
            .unwrap()
            .table_status(),
        Some(&aws_sdk_dynamodb::types::TableStatus::Creating)
    );
    let target = first_state(&fixture, &table, base).await;
    let old = fixture.cell_identity(&target).await;
    http.stop().await;
    fixture.replace().await;
    first_state(&fixture, &table, base).await;
    let actual = fixture.cell_identity(&target).await;
    assert_eq!(actual.scope, old.scope);
    assert!(actual.epoch > old.epoch);
    assert_eq!(record(&fixture).await.id, table.id);
    (fixture, table)
}
pub async fn complete_deletion(fixture: &Fixture, name: &str) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let lifecycle = fixture
                .client
                .query::<ReadTableLifecycle>(
                    &account_target(ACCOUNT).unwrap(),
                    None,
                    Json(name.into()),
                )
                .await
                .unwrap()
                .output
                .0;
            match lifecycle {
                TableLifecycle::Missing => break,
                TableLifecycle::Deleting(table) => {
                    fixture
                        .provisioner
                        .continue_table_deletion(&fixture.client, ACCOUNT, &table.id)
                        .await
                        .unwrap();
                }
                TableLifecycle::Live(_) => panic!("deleting generation became live"),
            }
        }
    })
    .await
    .unwrap();
}
pub async fn reconcile_three(fixture: &Fixture, provisioner: &CellInitialPartitionProvisioner) {
    let mut cursor = None;
    for _ in 0..3 {
        provisioner
            .reconcile_account_capacity(ACCOUNT, fixture.client.clone(), u64::MAX, &mut cursor)
            .await
            .unwrap();
    }
}
