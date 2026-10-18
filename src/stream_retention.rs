//! Bounded stream record retention inside account and data Cells.

use cellar_runtime::registry::{Command, CommandContext, CommandResult, Query, QueryContext};

use crate::table::statement;
use crate::{DATA_MODULE, Error, Json, MODULE, Result, SqlValue};

pub(crate) const RETENTION_MS: i64 = 24 * 60 * 60 * 1_000;
pub(crate) const PRUNE_BATCH: i64 = 1_024;

/// Check whether the account Cell has expired stream records before writing a prune command.
pub struct HasExpiredAccountStreamRecords;

impl Query for HasExpiredAccountStreamRecords {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 48;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<()>;
    type Output = Json<bool>;

    fn execute(context: &mut QueryContext<'_>, Json(()): Self::Input) -> Result<Self::Output> {
        Ok(Json(has_expired(context)?))
    }
}

/// Check the routed owner Cell's expiry index before scheduling a write.
pub struct HasExpiredPartitionStreamRecords;

impl Query for HasExpiredPartitionStreamRecords {
    const MODULE: &'static str = DATA_MODULE;
    const ID: u32 = 18;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<()>;
    type Output = Json<bool>;

    fn execute(context: &mut QueryContext<'_>, Json(()): Self::Input) -> Result<Self::Output> {
        Ok(Json(has_expired(context)?))
    }
}

fn has_expired(context: &mut QueryContext<'_>) -> Result<bool> {
    let rows = context.sql(&statement(
        "SELECT 1 FROM ddb_stream_records WHERE created_at_ms <= ?1 LIMIT 1",
        vec![SqlValue::Integer(
            context.now_ms().saturating_sub(RETENTION_MS),
        )],
    ))?;
    Ok(!rows[0].rows.is_empty())
}

/// Remove one bounded batch of expired records from the account Cell.
pub struct PruneAccountStreamRecords;

impl Command for PruneAccountStreamRecords {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 44;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<()>;
    type Output = Json<u64>;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(()): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        Ok(CommandResult::Success(Json(prune(context)?)))
    }
}

/// Remove one bounded batch of expired records from a routed data Cell.
pub struct PrunePartitionStreamRecords;

impl Command for PrunePartitionStreamRecords {
    const MODULE: &'static str = DATA_MODULE;
    const ID: u32 = 20;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<()>;
    type Output = Json<u64>;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(()): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        Ok(CommandResult::Success(Json(prune(context)?)))
    }
}

fn prune(context: &CommandContext<'_, '_>) -> Result<u64> {
    let cutoff = context.now_ms().saturating_sub(RETENTION_MS);
    // The expiry index selects a fixed-size batch; a Cell command must
    // never delete an unbounded backlog under one actor turn.
    Ok(context.sql(&statement(
        "DELETE FROM ddb_stream_records WHERE rowid IN \
         (SELECT rowid FROM ddb_stream_records WHERE created_at_ms <= ?1 \
          ORDER BY created_at_ms LIMIT ?2)",
        vec![SqlValue::Integer(cutoff), SqlValue::Integer(PRUNE_BATCH)],
    ))?[0]
        .rows_affected)
}

/// Read the next tenant catalog shard to sweep for dormant data Cells.
pub struct ReadStreamGcShard;

impl Query for ReadStreamGcShard {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 49;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<()>;
    type Output = Json<u8>;

    fn execute(context: &mut QueryContext<'_>, Json(()): Self::Input) -> Result<Self::Output> {
        let rows = context.sql(&statement(
            "SELECT next_shard FROM ddb_stream_gc_schedule WHERE singleton = 1",
            vec![],
        ))?;
        let Some([SqlValue::Integer(shard)]) = rows[0].rows.first().map(Vec::as_slice) else {
            return Err(Error::Command("stream GC schedule is missing"));
        };
        Ok(Json(
            u8::try_from(*shard).map_err(|_| Error::Command("invalid stream GC shard"))?,
        ))
    }
}

/// Advance the durable shard cursor only after its catalog scan completes.
pub struct AdvanceStreamGcShard;

impl Command for AdvanceStreamGcShard {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 45;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<u8>;
    type Output = Json<bool>;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(expected): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        let advanced = context.sql(&statement(
            "UPDATE ddb_stream_gc_schedule SET next_shard = ?1 WHERE singleton = 1 AND next_shard = ?2",
            vec![
                SqlValue::Integer(i64::from(expected.wrapping_add(1))),
                SqlValue::Integer(i64::from(expected)),
            ],
        ))?[0]
            .rows_affected
            == 1;
        Ok(if advanced {
            CommandResult::Success(Json(true))
        } else {
            CommandResult::Rejected(Json(false))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{mutation, start, stop};
    use crate::{
        CreateTable, CreateTableOutcome, InstallPartition, PartitionInstall, PartitionPut,
        PartitionPutInput, PartitionSpec, PutItem, PutItemInput, ReadAccountStreamJournal,
        ReadPartitionStreamJournal, StreamConfig, StreamJournalInput, StreamJournalOutcome,
        TableClass, TablePlacement, TableSpec,
    };
    use extenddb_core::types::{
        AttributeDefinition, AttributeValue, BillingMode, Item, KeySchemaElement, KeyType,
        ScalarAttributeType, StreamViewType,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    };

    #[test]
    fn stream_prune_bounds_work_and_preserves_retained_records_in_both_cell_types()
    -> anyhow::Result<()> {
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
            for partitioned in [false, true] {
                let base = if partitioned { 2 * RETENTION_MS } else { 1_000 };
                clock.store(base, Ordering::SeqCst);
                let created = client
                    .command::<CreateTable>(
                        &account,
                        mutation(&clock),
                        Json(TableSpec {
                            table_class: TableClass::Standard,
                            placement: TablePlacement::Routed {
                                initial_partitions: 2,
                            },
                            table_name: format!("Retention{partitioned}"),
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
                            stream: Some(StreamConfig {
                                region: "local".into(),
                                label: "generation".into(),
                                view_type: StreamViewType::NewImage,
                            }),
                        }),
                    )
                    .await?;
                let CreateTableOutcome::Created(table) = created.output.0 else {
                    anyhow::bail!("table not created");
                };
                let target = if partitioned {
                    let partition_id = [3; 16];
                    let target = crate::data_target("123456789012", &table.id, &partition_id)?;
                    client
                        .command::<InstallPartition>(
                            &target,
                            mutation(&clock),
                            Json(PartitionInstall::Serving(PartitionSpec {
                                table: (*table).clone(),
                                partition_id,
                                lower: None,
                                upper: None,
                                epoch: 1,
                            })),
                        )
                        .await?;
                    target
                } else {
                    account.clone()
                };
                for index in 0..PRUNE_BATCH + 2 {
                    if index == PRUNE_BATCH + 1 {
                        clock.store(base + 1, Ordering::SeqCst);
                    }
                    let input = PutItemInput {
                        table_name: table.table_name.clone(),
                        table_id: table.id.clone(),
                        item: Item::from([("pk".into(), AttributeValue::S(index.to_string()))]),
                        condition: None,
                    };
                    if partitioned {
                        client
                            .command::<PartitionPut>(
                                &target,
                                mutation(&clock),
                                Json(PartitionPutInput {
                                    epoch: 1,
                                    table_id: input.table_id,
                                    item: input.item,
                                    condition: input.condition,
                                }),
                            )
                            .await?;
                    } else {
                        client
                            .command::<PutItem>(&target, mutation(&clock), Json(input))
                            .await?;
                    }
                }
                clock.store(base + RETENTION_MS, Ordering::SeqCst);
                if partitioned {
                    assert!(
                        client
                            .query::<HasExpiredPartitionStreamRecords>(&target, None, Json(()))
                            .await?
                            .output
                            .0
                    );
                    assert_eq!(
                        client
                            .command::<PrunePartitionStreamRecords>(
                                &target,
                                mutation(&clock),
                                Json(())
                            )
                            .await?
                            .output
                            .0,
                        PRUNE_BATCH as u64
                    );
                    assert_eq!(
                        client
                            .command::<PrunePartitionStreamRecords>(
                                &target,
                                mutation(&clock),
                                Json(())
                            )
                            .await?
                            .output
                            .0,
                        1
                    );
                    assert!(
                        !client
                            .query::<HasExpiredPartitionStreamRecords>(&target, None, Json(()))
                            .await?
                            .output
                            .0
                    );
                } else {
                    assert!(
                        client
                            .query::<HasExpiredAccountStreamRecords>(&target, None, Json(()))
                            .await?
                            .output
                            .0
                    );
                    assert_eq!(
                        client
                            .command::<PruneAccountStreamRecords>(
                                &target,
                                mutation(&clock),
                                Json(())
                            )
                            .await?
                            .output
                            .0,
                        PRUNE_BATCH as u64
                    );
                    assert_eq!(
                        client
                            .command::<PruneAccountStreamRecords>(
                                &target,
                                mutation(&clock),
                                Json(())
                            )
                            .await?
                            .output
                            .0,
                        1
                    );
                    assert!(
                        !client
                            .query::<HasExpiredAccountStreamRecords>(&target, None, Json(()))
                            .await?
                            .output
                            .0
                    );
                }
                let input = Json(StreamJournalInput {
                    table_id: table.id.clone(),
                    label: "generation".into(),
                    after_sequence: None,
                    limit: 10,
                });
                let journal = if partitioned {
                    client
                        .query::<ReadPartitionStreamJournal>(&target, None, input)
                        .await?
                        .output
                        .0
                } else {
                    client
                        .query::<ReadAccountStreamJournal>(&target, None, input)
                        .await?
                        .output
                        .0
                };
                let StreamJournalOutcome::Page { records, .. } = journal else {
                    anyhow::bail!("missing retained stream");
                };
                assert_eq!(records.len(), 1);
            }
            stop(node).await
        })
    }
}
