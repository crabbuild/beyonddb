use super::helpers::*;
use crate::{
    native_peer,
    residency_fixture::{ACCOUNT, Fixture, runtimes},
    table_helpers,
};
use aws_sdk_dynamodb::types::AttributeValue as V;
use bdb_cellar_runtime::{
    client::{Invocation, InvocationAdmission},
    identity::CellTarget,
    registry::Command,
};
use beyonddb::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

struct DeleteDuringInstall {
    sdk: aws_sdk_dynamodb::Client,
    target: CellTarget,
    deleted: Arc<AtomicBool>,
}
impl InvocationAdmission for DeleteDuringInstall {
    fn check<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
    ) -> Pin<Box<dyn Future<Output = bdb_cellar_runtime::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if invocation.target == self.target
                && invocation.mutation.is_some()
                && invocation.operation == InstallPartition::ID
                && !self.deleted.swap(true, Ordering::SeqCst)
            {
                self.sdk
                    .delete_table()
                    .table_name(NAME)
                    .send()
                    .await
                    .unwrap();
            }
            Ok(())
        })
    }
}
#[test]
fn sdk_creation_recovery_tolerates_concurrent_delete() {
    let (host, core) = runtimes();
    host.block_on(async {
        let fixture = Fixture::new(host.handle(), core.handle(), 2, 11).await;
        table_helpers::create(&fixture.sdk, "ResidencyFill", false)
            .send()
            .await
            .unwrap();
        let http = http(&fixture, Arc::new(RefuseInstall::new(true, 0))).await;
        assert!(
            table_helpers::create(&http.sdk, NAME, true)
                .send()
                .await
                .is_err()
        );
        let old = record(&fixture).await;
        http.stop().await;
        fixture
            .sdk
            .delete_table()
            .table_name("ResidencyFill")
            .send()
            .await
            .unwrap();
        let deleted = Arc::new(AtomicBool::new(false));
        let interrupted = fixture.client.with_admission(Arc::new(DeleteDuringInstall {
            sdk: fixture.sdk.clone(),
            target: data_target(ACCOUNT, &old.id, &[0; 16]).unwrap(),
            deleted: deleted.clone(),
        }));
        let mut cursor = None;
        tokio::time::timeout(Duration::from_secs(10), async {
            while !deleted.load(Ordering::SeqCst) {
                fixture
                    .provisioner
                    .reconcile_account_capacity(ACCOUNT, interrupted.clone(), u64::MAX, &mut cursor)
                    .await
                    .unwrap();
            }
        })
        .await
        .unwrap();
        complete_deletion(&fixture, NAME).await;
        table_helpers::create(&fixture.sdk, NAME, true)
            .send()
            .await
            .unwrap();
        assert_ne!(record(&fixture).await.id, old.id);
        fixture
            .sdk
            .put_item()
            .table_name(NAME)
            .item("id", V::S("recreated".into()))
            .send()
            .await
            .unwrap();
        assert_eq!(
            fixture
                .sdk
                .get_item()
                .table_name(NAME)
                .key("id", V::S("recreated".into()))
                .consistent_read(true)
                .send()
                .await
                .unwrap()
                .item
                .unwrap()["id"],
            V::S("recreated".into())
        );
        fixture.shutdown().await;
    });
}
#[test]
fn sdk_worker_finishes_partial_creation_after_account_restore() {
    let (host, core) = runtimes();
    host.block_on(async {
        let mut fixture = Fixture::new(host.handle(), core.handle(), 2, 11).await;
        table_helpers::create(&fixture.sdk, "ResidencyFill", false)
            .send()
            .await
            .unwrap();
        let http = http(&fixture, Arc::new(RefuseInstall::new(true, 0))).await;
        assert!(
            table_helpers::create(&http.sdk, NAME, true)
                .send()
                .await
                .is_err()
        );
        let table = record(&fixture).await;
        let index = table.global_secondary_indexes[0].id.clone();
        let before = route(&fixture, &index).await;
        assert!(matches!(before, RoutePageOutcome::Page { .. }));
        assert!(matches!(
            route(&fixture, &table.id).await,
            RoutePageOutcome::Unrouted
        ));
        fixture
            .sdk
            .delete_table()
            .table_name("ResidencyFill")
            .send()
            .await
            .unwrap();
        http.stop().await;
        fixture.replace().await;
        let tasks = TaskGroup::default();
        fixture
            .provisioner
            .install_account_capacity_loop(
                &tasks,
                ACCOUNT.into(),
                fixture.client.clone(),
                u64::MAX,
                Duration::from_millis(100),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let state = fixture
                    .sdk
                    .describe_table()
                    .table_name(NAME)
                    .send()
                    .await
                    .unwrap()
                    .table
                    .unwrap();
                if state.table_status() == Some(&aws_sdk_dynamodb::types::TableStatus::Active) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();
        tasks.shutdown().await.unwrap();
        assert_eq!(route(&fixture, &index).await, before);
        assert_eq!(record(&fixture).await.id, table.id);
        fixture
            .sdk
            .put_item()
            .table_name(NAME)
            .item("id", V::S("resumed".into()))
            .item("bucket", V::S("ready".into()))
            .send()
            .await
            .unwrap();
        let storage = CellStorage::new(fixture.client.clone(), "us-east-1");
        for range in table_helpers::ranges(&fixture, &table.id).await {
            storage
                .project_index_changes(
                    ACCOUNT,
                    &data_target(ACCOUNT, &table.id, &range.partition_id).unwrap(),
                    &table.id,
                )
                .await
                .unwrap();
        }
        fixture.replace().await;
        let item = fixture
            .sdk
            .get_item()
            .table_name(NAME)
            .key("id", V::S("resumed".into()))
            .consistent_read(true)
            .send()
            .await
            .unwrap()
            .item
            .unwrap();
        assert_eq!(item["bucket"], V::S("ready".into()));
        assert_eq!(
            fixture
                .sdk
                .scan()
                .table_name(NAME)
                .index_name("ByBucket")
                .send()
                .await
                .unwrap()
                .items
                .unwrap(),
            vec![item]
        );
        fixture.shutdown().await;
    });
}
#[test]
fn sdk_creation_resumes_original_partition_count_after_restore() {
    let (host, core) = runtimes();
    host.block_on(async {
        for base in [false, true] {
            let (mut fixture, table) = partial(host.handle(), core.handle(), base).await;
            let recovery = CellInitialPartitionProvisioner::new(
                fixture.client.clone(),
                native_peer::application(),
            )
            .with_initial_partition_count(4)
            .unwrap();
            reconcile_three(&fixture, &recovery).await;
            assert_eq!(table_helpers::ranges(&fixture, &table.id).await.len(), 2);
            assert_eq!(
                table_helpers::ranges(&fixture, &table.global_secondary_indexes[0].id)
                    .await
                    .len(),
                2
            );
            fixture
                .sdk
                .put_item()
                .table_name(NAME)
                .item("id", V::S("original-layout".into()))
                .send()
                .await
                .unwrap();
            fixture.replace().await;
            assert_eq!(
                fixture
                    .sdk
                    .get_item()
                    .table_name(NAME)
                    .key("id", V::S("original-layout".into()))
                    .consistent_read(true)
                    .send()
                    .await
                    .unwrap()
                    .item
                    .unwrap()["id"],
                V::S("original-layout".into())
            );
            assert_eq!(table_helpers::ranges(&fixture, &table.id).await.len(), 2);
            fixture.shutdown().await;
        }
    });
}
