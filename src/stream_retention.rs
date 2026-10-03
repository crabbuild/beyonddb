//! Bounded stream record retention inside account and data Cells.

use cellule_runtime::registry::{Command, CommandContext, CommandResult, Query, QueryContext};

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
    use cellule_app::CellApplication;
    use cellule_ltx::rusqlite::{Connection, params};
    use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, WireValue};
    use cellule_runtime::control::OwnerFence;
    use cellule_runtime::identity::{CellTarget, Digest, IncarnationId, TenantId};
    use cellule_runtime::registry::{BuildDescriptor, CommandInvocation, QueryInvocation};

    use super::*;

    #[test]
    fn stream_prune_bounds_work_and_preserves_retained_records_in_both_cell_types() {
        let application = crate::Beyonddb::compile(BuildDescriptor {
            source_revision: "stream-retention-test".into(),
            cargo_lock_digest: Digest::from_bytes([1; 32]),
        })
        .unwrap();
        let registry = application.registry();
        let account = crate::account_target("123456789012").unwrap();
        let partition = CellTarget::new(
            TenantId::from_bytes([1; 16]),
            crate::APPLICATION,
            crate::DATA_NAMESPACE,
            b"stream-retention-test",
        )
        .unwrap();
        for (module, target, query_id, command_id) in [
            (
                MODULE,
                account,
                HasExpiredAccountStreamRecords::ID,
                PruneAccountStreamRecords::ID,
            ),
            (
                DATA_MODULE,
                partition,
                HasExpiredPartitionStreamRecords::ID,
                PrunePartitionStreamRecords::ID,
            ),
        ] {
            let mut connection = Connection::open_in_memory().unwrap();
            connection
                .execute_batch(crate::stream_journal::SCHEMA)
                .unwrap();
            for index in 0..PRUNE_BATCH + 1 {
                connection
                    .execute(
                        "INSERT INTO ddb_stream_records VALUES (?1, ?2, ?3, ?4, ?5)",
                        params!["old", "generation", format!("{index:023}"), 1_000, [1_u8]],
                    )
                    .unwrap();
            }
            connection
                .execute(
                    "INSERT INTO ddb_stream_records VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        "live",
                        "generation",
                        "00000000000000000000001",
                        1_001,
                        [2_u8]
                    ],
                )
                .unwrap();
            let mut encoder = BoundedEncoder::new(crate::OPERATION_BYTES).unwrap();
            Json(()).encode(&mut encoder).unwrap();
            let input = encoder.finish();
            for (sequence, expected_old) in [(1, 1), (2, 0)] {
                let query = registry
                    .execute_query(
                        &connection,
                        QueryInvocation {
                            module,
                            operation_id: query_id,
                            codec_version: 1,
                            schema: 1,
                            cell: target.cell_id(),
                            commit_sequence: sequence - 1,
                            now_ms: RETENTION_MS + 1_000,
                            input: &input,
                        },
                    )
                    .unwrap();
                let mut decoder = BoundedDecoder::new(&query, crate::OPERATION_BYTES).unwrap();
                let has_expired = Json::<bool>::decode(&mut decoder).unwrap().0;
                decoder.finish().unwrap();
                assert!(has_expired);
                let transaction = connection.transaction().unwrap();
                registry
                    .execute_command(
                        &transaction,
                        CommandInvocation {
                            module,
                            operation_id: command_id,
                            codec_version: 1,
                            schema: 1,
                            target: target.clone(),
                            owner_fence: OwnerFence {
                                incarnation: IncarnationId::from_bytes([1; 16]),
                                epoch: 1,
                            },
                            sequence,
                            now_ms: RETENTION_MS + 1_000,
                            input: &input,
                        },
                    )
                    .unwrap();
                transaction.commit().unwrap();
                let old: i64 = connection
                    .query_row(
                        "SELECT count(*) FROM ddb_stream_records WHERE table_id = 'old'",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(old, expected_old);
            }
            let query = registry
                .execute_query(
                    &connection,
                    QueryInvocation {
                        module,
                        operation_id: query_id,
                        codec_version: 1,
                        schema: 1,
                        cell: target.cell_id(),
                        commit_sequence: 2,
                        now_ms: RETENTION_MS + 1_000,
                        input: &input,
                    },
                )
                .unwrap();
            let mut decoder = BoundedDecoder::new(&query, crate::OPERATION_BYTES).unwrap();
            assert!(!Json::<bool>::decode(&mut decoder).unwrap().0);
            decoder.finish().unwrap();
            let retained: i64 = connection
                .query_row(
                    "SELECT count(*) FROM ddb_stream_records WHERE table_id = 'live'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(retained, 1);
        }
    }
}
