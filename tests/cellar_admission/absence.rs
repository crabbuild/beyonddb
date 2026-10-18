//! SDK absence observation is reused by its original conditional owner claim.
use crate::{
    claim, native_peer,
    residency_fixture::{ACCOUNT, Fixture, runtimes},
};
use beyonddb::*;
use std::{sync::Arc, time::Duration};

#[test]
fn sdk_cold_coordinator_reuses_one_absence_read_before_creation_cas() {
    let (host, core) = runtimes();
    host.block_on(async {
        let control = Arc::new(claim::Control::default());
        let mut fixture = Fixture::with_bucket(host.handle(), core.handle(), 1, 8, |mut bucket| {
            bucket.store = Arc::new(claim::Store {
                inner: bucket.store.clone(),
                control: control.clone(),
            });
            bucket.cas_store = Arc::new(claim::Store {
                inner: bucket.cas_store.clone(),
                control: control.clone(),
            });
            bucket
        })
        .await;
        let key = b"fresh-admission-absence";
        let coordinator = coordinator_target(ACCOUNT, key).unwrap();
        let app = native_peer::application();
        let module = app
            .cell_types()
            .iter()
            .find(|kind| kind.namespace() == coordinator.namespace())
            .unwrap()
            .module();
        let held = control.hold(module);
        // Application admission publishes discovery. Physical activation follows
        // only when a real operation reaches the SDK-owned coordinator.
        fixture
            .provisioner
            .ensure(&fixture.client, ACCOUNT, key)
            .await
            .unwrap();
        assert_eq!(held.reads(), 0);
        let job = {
            let client = fixture.client.clone();
            let target = coordinator.clone();
            tokio::spawn(async move {
                client
                    .query::<ReadPendingCrossCellTransactions>(
                        &target,
                        None,
                        Json(ReadPendingCrossCellTransactionsInput {
                            after: None,
                            limit: 1,
                        }),
                    )
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(2), held.entered())
            .await
            .unwrap();
        assert_eq!(
            held.reads(),
            1,
            "cold SDK claim reread the observed missing owner before CAS"
        );
        held.release();
        assert!(
            tokio::time::timeout(Duration::from_secs(10), job)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .output
                .0
                .is_empty()
        );
        let old = fixture.cell_identity(&coordinator).await;
        assert_eq!(old.scope, held.scope());
        drop(held);
        let registration = || {
            Json(RegisterCoordinatorShardInput {
                account_id: ACCOUNT.into(),
                shard: u32::from_be_bytes(coordinator.partition().try_into().unwrap()),
            })
        };
        assert!(
            fixture
                .client
                .query::<ReadCoordinatorRegistration>(
                    &account_target(ACCOUNT).unwrap(),
                    None,
                    registration()
                )
                .await
                .unwrap()
                .output
                .0
        );
        fixture.replace().await;
        assert!(
            fixture
                .client
                .query::<ReadPendingCrossCellTransactions>(
                    &coordinator,
                    None,
                    Json(ReadPendingCrossCellTransactionsInput {
                        after: None,
                        limit: 1
                    })
                )
                .await
                .unwrap()
                .output
                .0
                .is_empty()
        );
        let actual = fixture.cell_identity(&coordinator).await;
        assert_eq!(actual.scope, old.scope);
        assert!(actual.epoch > old.epoch);
        assert!(
            fixture
                .client
                .query::<ReadCoordinatorRegistration>(
                    &account_target(ACCOUNT).unwrap(),
                    None,
                    registration()
                )
                .await
                .unwrap()
                .output
                .0
        );
        fixture.shutdown().await;
    });
}
