//! Publication must enforce multi-cell placement independently of the router.
use super::*;
use crate::test_support::{mutation, start, stop};
use cellar_runtime::client::InvocationError;
use extenddb_core::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};
use keyspace::RangeKey;
use std::sync::{Arc, atomic::AtomicI64};

#[test]
fn a_durable_single_range_cannot_publish_a_multi_cell_table() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let clock = Arc::new(AtomicI64::new(1_000));
    host.block_on(async {
        let (node, client) = start(
            directory.path(),
            clock.clone(),
            host.handle(),
            core.handle(),
        )
        .await?;
        let account = crate::account_target("123456789012")?;
        let created = client
            .command::<crate::CreateTable>(
                &account,
                mutation(&clock),
                Json(crate::TableSpec {
                    table_class: crate::TableClass::Standard,
                    placement: crate::TablePlacement::Routed {
                        initial_partitions: 4,
                    },
                    table_name: "MultiCellPublication".into(),
                    key_schema: vec![KeySchemaElement {
                        attribute_name: "pk".into(),
                        key_type: KeyType::Hash,
                    }],
                    attribute_definitions: vec![AttributeDefinition {
                        attribute_name: "pk".into(),
                        attribute_type: ScalarAttributeType::S,
                    }],
                    local_secondary_indexes: Vec::new(),
                    global_secondary_indexes: Vec::new(),
                    billing_mode: BillingMode::PayPerRequest,
                    provisioned_throughput: None,
                    deletion_protection_enabled: false,
                    initial_tags: Vec::new(),
                    resource_arn: None,
                    stream: None,
                }),
            )
            .await?;
        let crate::CreateTableOutcome::Created(table) = created.output.0 else {
            anyhow::bail!("table was not created");
        };
        let partition = PartitionSpec {
            table: (*table).clone(),
            partition_id: [1; 16],
            lower: None,
            upper: None,
            epoch: 1,
        };
        let owner = crate::data_target("123456789012", &table.id, &partition.partition_id)?;
        client
            .command::<crate::InstallPartition>(
                &owner,
                mutation(&clock),
                Json(crate::PartitionInstall::Serving(partition.clone())),
            )
            .await?;
        let spec = crate::DirectorySpec::root(table.id.clone());
        let target = crate::directory_target("123456789012", &spec)?;
        let ranges = vec![RoutePagePartition {
            partition_id: partition.partition_id,
            lower: RangeKey::minimum(),
            upper: None,
            epoch: 1,
        }];
        let fingerprint = crate::directory::fingerprint(&spec, &ranges)?;
        let installed = client
            .command::<crate::InstallDirectory>(
                &target,
                mutation(&clock),
                Json(crate::DirectoryInstall {
                    spec,
                    ranges,
                    source: None,
                }),
            )
            .await?;
        anyhow::ensure!(
            installed.output.0,
            "single metadata leaf should be installable"
        );
        let publication = TableRoutePublication {
            route: TableRoute {
                table_id: table.id.clone(),
                epoch: 1,
                partitions: vec![partition],
            },
            receipt: crate::DirectoryCopyReceipt {
                cell_id: *target.cell_id().as_bytes(),
                sequence: installed.receipt.commit_sequence,
                fingerprint,
            },
        };
        let identity = mutation(&clock);
        let Err(InvocationError::Rejected(rejected)) = client
            .command::<ActivateTableRoute>(&account, identity, Json(publication.clone()))
            .await
        else {
            anyhow::bail!("a one-cell route became active despite the durable four-cell policy");
        };
        assert_eq!(rejected.output.0, ActivateTableRouteOutcome::InvalidRoute);
        let rejected_sequence = rejected.receipt.commit_sequence;
        assert!(
            client
                .query::<ReadRouteDirectory>(&account, None, Json(table.id.clone()))
                .await?
                .output
                .0
                .is_none()
        );
        stop(node).await?;

        let (node, client) = start(directory.path(), clock, host.handle(), core.handle()).await?;
        let Err(InvocationError::Rejected(replayed)) = client
            .command::<ActivateTableRoute>(&account, identity, Json(publication))
            .await
        else {
            anyhow::bail!("publication rejection did not survive restart");
        };
        assert_eq!(replayed.output.0, ActivateTableRouteOutcome::InvalidRoute);
        assert_eq!(replayed.receipt.commit_sequence, rejected_sequence);
        assert!(
            client
                .query::<ReadRouteDirectory>(&account, None, Json(table.id))
                .await?
                .output
                .0
                .is_none()
        );
        stop(node).await
    })
}
