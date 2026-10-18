use super::helpers::*;
use crate::residency_fixture::{ACCOUNT, Fixture, runtimes};
use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use beyonddb::*;

#[test]
fn sdk_cell_models_select_and_restore_multiple_ordered_cells() {
    let (host, core) = runtimes();
    host.block_on(async {
        let mut fixture = Fixture::new(host.handle(), core.handle(), 1, 32).await;
        let default = CellInitialPartitionProvisioner::new(
            fixture.client.clone(),
            crate::native_peer::application(),
        );
        assert_eq!(
            InitialPartitionProvisioner::initial_partition_count(&default),
            4
        );
        for count in [0, 1, 257] {
            assert!(
                CellInitialPartitionProvisioner::new(
                    fixture.client.clone(),
                    crate::native_peer::application()
                )
                .with_initial_partition_count(count)
                .is_err()
            );
        }
        for count in [2, 256] {
            assert!(
                CellInitialPartitionProvisioner::new(
                    fixture.client.clone(),
                    crate::native_peer::application()
                )
                .with_initial_partition_count(count)
                .is_ok()
            );
        }
        fixture.set_initial_partition_count(4).await;
        let mut saved = Vec::new();
        for (name, model) in [
            ("ResidencyModelAuto", Some("auto")),
            ("ResidencyModelPartitioned", Some("partitioned")),
            ("ResidencyModelDefault", None),
        ] {
            let mut request = create(&fixture.sdk, name, false);
            if let Some(model) = model {
                request = request.tags(model_tag(model));
            }
            request.send().await.unwrap();
            let table = table(&fixture, name).await;
            assert_eq!(
                table.placement,
                TablePlacement::Routed {
                    initial_partitions: 4
                }
            );
            let plan = ranges(&fixture, &table.id).await;
            assert_eq!(plan.len(), 4);
            assert_eq!(
                plan.iter()
                    .map(|range| range.partition_id)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len(),
                4
            );
            assert_eq!(
                plan.first().unwrap().lower,
                bdb_keyspace::RangeKey::minimum()
            );
            assert!(plan.last().unwrap().upper.is_none());
            for pair in plan.windows(2) {
                assert_eq!(pair[0].upper, Some(pair[1].lower.clone()));
                assert_ne!(pair[0].partition_id, pair[1].partition_id);
            }
            let mut cells = Vec::new();
            for range in &plan {
                let target = data_target(ACCOUNT, &table.id, &range.partition_id).unwrap();
                let state = fixture
                    .client
                    .query::<ReadPartitionState>(&target, None, Json(()))
                    .await
                    .unwrap()
                    .output
                    .0
                    .unwrap();
                assert_eq!(state.spec.partition_id, range.partition_id);
                assert_eq!(
                    state.spec.lower,
                    (range.lower != bdb_keyspace::RangeKey::minimum()).then(|| range.lower.clone())
                );
                assert_eq!(state.spec.upper, range.upper);
                let identity = fixture.cell_identity(&target).await;
                cells.push((target, identity));
            }
            saved.push((name, table, plan, cells));
        }
        for (name, model) in [
            ("ResidencyModelSingle", "single"),
            ("ResidencyModelInvalid", "unknown"),
        ] {
            let error = create(&fixture.sdk, name, false)
                .tags(model_tag(model))
                .send()
                .await
                .unwrap_err();
            assert_eq!(
                error.as_service_error().unwrap().code(),
                Some("ValidationException")
            );
            assert!(
                fixture
                    .sdk
                    .describe_table()
                    .table_name(name)
                    .send()
                    .await
                    .is_err()
            );
            assert!(
                fixture
                    .client
                    .query::<DescribeTable>(
                        &account_target(ACCOUNT).unwrap(),
                        None,
                        Json(name.into())
                    )
                    .await
                    .unwrap()
                    .output
                    .0
                    .is_none()
            );
        }
        // Replacement deletes the former directory and observes native lease/
        // sample departure. Its new creation policy is two; old plans stay four.
        fixture.replace().await;
        assert_eq!(
            InitialPartitionProvisioner::initial_partition_count(fixture.provisioner.as_ref()),
            2
        );
        for (name, original, plan, cells) in saved {
            fixture
                .sdk
                .describe_table()
                .table_name(name)
                .send()
                .await
                .unwrap();
            assert_eq!(table(&fixture, name).await.placement, original.placement);
            assert_eq!(ranges(&fixture, &original.id).await, plan);
            for (target, old) in cells {
                let state = fixture
                    .client
                    .query::<ReadPartitionState>(&target, None, Json(()))
                    .await
                    .unwrap()
                    .output
                    .0
                    .unwrap();
                assert_eq!(state.spec.table.placement, original.placement);
                let actual = fixture.cell_identity(&target).await;
                assert_eq!(actual.scope, old.scope);
                assert!(actual.epoch > old.epoch);
            }
        }
        fixture.shutdown().await;
    });
}
