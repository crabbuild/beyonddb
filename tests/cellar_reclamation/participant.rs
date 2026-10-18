use super::helpers::Probe;
use crate::{
    native_peer::{self, Http, Inspection, Peer, PeerPolicy},
    residency_fixture::{ACCOUNT, runtimes},
};
use beyonddb::*;
use std::sync::{Arc, Mutex};

#[test]
fn sdk_participant_restore_at_capacity_does_not_activate_account_metadata() {
    let (host, core) = runtimes();
    host.block_on(async {
        let (fixture, probe) = Probe::fixture(host.handle(), core.handle(), 2, 5).await;
        let expected = fixture.data.clone();
        let old = fixture.cell_identity(&expected[0].0).await;
        let files = tempfile::tempdir().unwrap();
        let observed = Arc::new(Mutex::new(Default::default()));
        let mut next = Peer::start(
            native_peer::application(),
            &files.path().join("reclamation-cold"),
            probe.bucket.clone(),
            observed.clone(),
            PeerPolicy {
                max_resident: 5,
                ..true.into()
            },
            host.handle(),
            core.handle(),
        )
        .await;
        fixture.shutdown().await;
        // The source temp directory has been dropped and cannot supply data.
        let inspection = Inspection::new(probe.bucket.clone(), host.handle());
        inspection
            .departed_node("residency-owner-0", "reclamation-cold")
            .await;
        for key in ["AKIAPARTICIPANTRESTOREONE", "AKIAPARTICIPANTRESTORETWO"] {
            native_peer::put_credential(&next.client, key, ACCOUNT).await;
        }
        for (target, _) in &expected {
            let restored = next
                .client
                .query::<ReadPartitionState>(target, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap();
            assert_eq!(restored.spec.table.table_name, "Residency");
        }
        let actual = native_peer::identity(&observed, "reclamation-cold", &expected[0].0).unwrap();
        assert_eq!(actual.scope, old.scope);
        assert!(actual.epoch > old.epoch);
        let owner = inspection.owner(&actual.scope).await;
        assert_eq!(owner.node.as_deref(), Some("reclamation-cold"));
        assert_eq!(owner.epoch, actual.epoch);
        assert!(
            native_peer::identity(
                &observed,
                "reclamation-cold",
                &account_target(ACCOUNT).unwrap()
            )
            .is_none(),
            "participant restoration activated account metadata"
        );
        probe.at_capacity("reclamation-cold", 5).await;
        let http = Http::start(&next.client, "AKIAIOSFODNN7EXAMPLE").await;
        for (_, item) in expected {
            assert_eq!(
                http.sdk
                    .get_item()
                    .table_name("Residency")
                    .key("id", item["id"].clone())
                    .consistent_read(true)
                    .send()
                    .await
                    .unwrap()
                    .item,
                Some(item)
            );
        }
        http.stop().await;
        next.stop().await;
        inspection.close().await;
        probe.close().await;
    });
}
