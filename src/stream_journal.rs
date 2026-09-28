//! Item changes retained in the Cell that committed them.

use cellule_runtime::registry::{Command, CommandContext, CommandResult, Query, QueryContext};
use extenddb_core::types::{
    Item, KeySchemaElement, StreamEventName, StreamRecord, StreamRecordData, StreamViewType,
    UserIdentity, extract_key, item_size_bytes,
};
use serde::{Deserialize, Serialize};

use crate::table::{TableRecord, statement};
use crate::{DATA_MODULE, Error, Json, MODULE, Result, SqlValue};

pub(crate) const SCHEMA: &str = include_str!("stream_journal.sql");
const RETENTION_MS: i64 = 24 * 60 * 60 * 1_000;
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
        let rows = context.sql(&statement(
            "SELECT 1 FROM ddb_stream_records WHERE created_at_ms <= ?1 LIMIT 1",
            vec![SqlValue::Integer(
                context.now_ms().saturating_sub(RETENTION_MS),
            )],
        ))?;
        Ok(Json(!rows[0].rows.is_empty()))
    }
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
        let cutoff = context.now_ms().saturating_sub(RETENTION_MS);
        // The expiry index selects a fixed-size batch; a Cell command must
        // never delete an unbounded backlog under one actor turn.
        let deleted = context.sql(&statement(
            "DELETE FROM ddb_stream_records WHERE rowid IN \
             (SELECT rowid FROM ddb_stream_records WHERE created_at_ms <= ?1 \
              ORDER BY created_at_ms LIMIT ?2)",
            vec![SqlValue::Integer(cutoff), SqlValue::Integer(PRUNE_BATCH)],
        ))?[0]
            .rows_affected;
        Ok(CommandResult::Success(Json(deleted)))
    }
}

#[cfg(test)]
mod tests {
    use cellule_app::CellApplication;
    use cellule_ltx::rusqlite::{Connection, params};
    use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, WireValue};
    use cellule_runtime::identity::Digest;
    use cellule_runtime::registry::{BuildDescriptor, CommandInvocation, QueryInvocation};

    use super::*;

    #[test]
    fn account_prune_bounds_work_and_preserves_retained_records() {
        let application = crate::Beyonddb::compile(BuildDescriptor {
            source_revision: "stream-retention-test".into(),
            cargo_lock_digest: Digest::from_bytes([1; 32]),
        })
        .unwrap();
        let registry = application.registry();
        let mut connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(SCHEMA).unwrap();
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
        let target = crate::account_target("123456789012").unwrap();
        for (sequence, expected_old) in [(1, 1), (2, 0)] {
            let query = registry
                .execute_query(
                    &connection,
                    QueryInvocation {
                        module: MODULE,
                        operation_id: HasExpiredAccountStreamRecords::ID,
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
                        module: MODULE,
                        operation_id: PruneAccountStreamRecords::ID,
                        codec_version: 1,
                        schema: 1,
                        target: target.clone(),
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
                    module: MODULE,
                    operation_id: HasExpiredAccountStreamRecords::ID,
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

/// Locate a retained stream generation by immutable ID or its public ARN components.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StreamCatalogKey {
    Id(String),
    NameLabel { table_name: String, label: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamCatalogEntry {
    pub record: TableRecord,
    pub disabled_at_ms: Option<i64>,
}

pub struct ReadStreamCatalog;

impl Query for ReadStreamCatalog {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 46;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<StreamCatalogKey>;
    type Output = Json<Option<StreamCatalogEntry>>;

    fn execute(context: &mut QueryContext<'_>, Json(key): Self::Input) -> Result<Self::Output> {
        let (sql, parameters) = match key {
            StreamCatalogKey::Id(id) => (
                "SELECT record, disabled_at_ms FROM ddb_stream_catalog WHERE table_id = ?1",
                vec![SqlValue::Text(id)],
            ),
            StreamCatalogKey::NameLabel { table_name, label } => (
                "SELECT record, disabled_at_ms FROM ddb_stream_catalog WHERE table_name = ?1 AND stream_label = ?2",
                vec![SqlValue::Text(table_name), SqlValue::Text(label)],
            ),
        };
        let rows = context.sql(&statement(sql, parameters))?;
        let entry = match rows[0].rows.first().map(Vec::as_slice) {
            None => None,
            Some([SqlValue::Blob(record), disabled]) => {
                let disabled_at_ms = match disabled {
                    SqlValue::Integer(time) => Some(*time),
                    SqlValue::Null => None,
                    _ => return Err(Error::Command("invalid stream catalog timestamp")),
                };
                if disabled_at_ms
                    .is_some_and(|time| time <= context.now_ms().saturating_sub(RETENTION_MS))
                {
                    None
                } else {
                    Some(StreamCatalogEntry {
                        record: serde_json::from_slice(record)?,
                        disabled_at_ms,
                    })
                }
            }
            _ => return Err(Error::Command("invalid stream catalog row")),
        };
        Ok(Json(entry))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListStreamCatalogInput {
    pub table_name: Option<String>,
    pub after: Option<(String, String)>,
    pub limit: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamCatalogSummary {
    pub table_name: String,
    pub label: String,
    pub region: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListStreamCatalogPage {
    pub streams: Vec<StreamCatalogSummary>,
    pub last_evaluated: Option<(String, String)>,
}

pub struct ListStreamCatalog;

impl Query for ListStreamCatalog {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 47;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<ListStreamCatalogInput>;
    type Output = Json<ListStreamCatalogPage>;

    fn execute(context: &mut QueryContext<'_>, Json(input): Self::Input) -> Result<Self::Output> {
        if input.limit == 0 || input.limit > 100 {
            return Err(Error::Command("stream listing limit is outside 1..=100"));
        }
        let (after_name, after_label) = input.after.unwrap_or_default();
        let rows = context.sql(&statement(
            "SELECT table_name, stream_label, region FROM ddb_stream_catalog WHERE (?1 IS NULL OR table_name = ?1) AND (table_name > ?2 OR (table_name = ?2 AND stream_label > ?3)) AND (disabled_at_ms IS NULL OR disabled_at_ms > ?4) ORDER BY table_name, stream_label LIMIT ?5",
            vec![
                input.table_name.map_or(SqlValue::Null, SqlValue::Text),
                SqlValue::Text(after_name),
                SqlValue::Text(after_label),
                SqlValue::Integer(context.now_ms().saturating_sub(RETENTION_MS)),
                SqlValue::Integer(i64::from(input.limit) + 1),
            ],
        ))?;
        let mut streams = Vec::with_capacity(rows[0].rows.len());
        for row in &rows[0].rows {
            let [
                SqlValue::Text(table_name),
                SqlValue::Text(label),
                SqlValue::Text(region),
            ] = row.as_slice()
            else {
                return Err(Error::Command("invalid stream listing row"));
            };
            streams.push(StreamCatalogSummary {
                table_name: table_name.clone(),
                label: label.clone(),
                region: region.clone(),
            });
        }
        let last_evaluated = if streams.len() > usize::from(input.limit) {
            streams.pop();
            streams
                .last()
                .map(|stream| (stream.table_name.clone(), stream.label.clone()))
        } else {
            None
        };
        Ok(Json(ListStreamCatalogPage {
            streams,
            last_evaluated,
        }))
    }
}

/// The immutable stream generation installed with a table generation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Image policy for this generation.
    pub view_type: StreamViewType,
    /// Region used in emitted records.
    pub region: String,
    /// Generation label used by stream discovery.
    pub label: String,
}

/// A bounded read from one stream generation in its owner Cell.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamJournalInput {
    pub table_id: String,
    pub label: String,
    pub after_sequence: Option<String>,
    pub limit: u16,
}

/// Records available from one Cell, or a stale table/generation identity.
#[derive(Debug, Serialize, Deserialize)]
pub enum StreamJournalOutcome {
    Page {
        records: Vec<StreamRecord>,
        last_sequence: Option<String>,
        /// A sealed source has no future item changes; a caller may stop after its last page.
        closed: bool,
        /// No record follows this page within the installed generation.
        exhausted: bool,
    },
    Missing,
}

/// Identify a stream generation for a constant-time tail lookup.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamTailInput {
    pub table_id: String,
    pub label: String,
}

/// Latest stored sequence, including the empty-shard case.
#[derive(Debug, Serialize, Deserialize)]
pub enum StreamTailOutcome {
    Latest(Option<String>),
    Missing,
}

/// Read an account-local stream generation after checking its table identity.
pub struct ReadAccountStreamJournal;

impl Query for ReadAccountStreamJournal {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 44;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<StreamJournalInput>;
    type Output = Json<StreamJournalOutcome>;

    fn execute(context: &mut QueryContext<'_>, Json(input): Self::Input) -> Result<Self::Output> {
        if !account_generation_exists(context, &input.table_id, &input.label)? {
            return Ok(Json(StreamJournalOutcome::Missing));
        }
        Ok(Json(read(context, input, false)?))
    }
}

/// Read a routed stream generation from the Cell that committed its items.
pub struct ReadPartitionStreamJournal;

impl Query for ReadPartitionStreamJournal {
    const MODULE: &'static str = DATA_MODULE;
    const ID: u32 = 16;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<StreamJournalInput>;
    type Output = Json<StreamJournalOutcome>;

    fn execute(context: &mut QueryContext<'_>, Json(input): Self::Input) -> Result<Self::Output> {
        let rows = context.sql(&statement(
            "SELECT spec FROM ddb_partition WHERE singleton = 1",
            vec![],
        ))?;
        let Some(spec) = crate::partition::decode_spec(&rows[0])? else {
            return Ok(Json(StreamJournalOutcome::Missing));
        };
        if spec.table.id != input.table_id
            || spec
                .table
                .stream
                .as_ref()
                .map(|stream| stream.label.as_str())
                != Some(input.label.as_str())
        {
            return Ok(Json(StreamJournalOutcome::Missing));
        }
        let closed = match crate::partition::query_access(context)? {
            crate::partition::AccessState::Serving => false,
            crate::partition::AccessState::Sealed => true,
            crate::partition::AccessState::Importing => {
                return Ok(Json(StreamJournalOutcome::Missing));
            }
        };
        Ok(Json(read(context, input, closed)?))
    }
}

/// Read the account Cell's latest sequence without paging through records.
pub struct ReadAccountStreamTail;

impl Query for ReadAccountStreamTail {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 45;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<StreamTailInput>;
    type Output = Json<StreamTailOutcome>;

    fn execute(context: &mut QueryContext<'_>, Json(input): Self::Input) -> Result<Self::Output> {
        if !account_generation_exists(context, &input.table_id, &input.label)? {
            return Ok(Json(StreamTailOutcome::Missing));
        }
        Ok(Json(StreamTailOutcome::Latest(tail(context, input)?)))
    }
}

fn account_generation_exists(
    context: &mut QueryContext<'_>,
    table_id: &str,
    label: &str,
) -> Result<bool> {
    let rows = context.sql(&statement(
        "SELECT 1 FROM ddb_stream_catalog WHERE table_id = ?1 AND stream_label = ?2 AND (disabled_at_ms IS NULL OR disabled_at_ms > ?3)",
        vec![
            SqlValue::Text(table_id.into()),
            SqlValue::Text(label.into()),
            SqlValue::Integer(context.now_ms().saturating_sub(RETENTION_MS)),
        ],
    ))?;
    Ok(!rows[0].rows.is_empty())
}

/// Read one data Cell's latest sequence without scanning the journal.
pub struct ReadPartitionStreamTail;

impl Query for ReadPartitionStreamTail {
    const MODULE: &'static str = DATA_MODULE;
    const ID: u32 = 17;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<StreamTailInput>;
    type Output = Json<StreamTailOutcome>;

    fn execute(context: &mut QueryContext<'_>, Json(input): Self::Input) -> Result<Self::Output> {
        let rows = context.sql(&statement(
            "SELECT spec FROM ddb_partition WHERE singleton = 1",
            vec![],
        ))?;
        let Some(spec) = crate::partition::decode_spec(&rows[0])? else {
            return Ok(Json(StreamTailOutcome::Missing));
        };
        if spec.table.id != input.table_id
            || spec
                .table
                .stream
                .as_ref()
                .map(|stream| stream.label.as_str())
                != Some(input.label.as_str())
        {
            return Ok(Json(StreamTailOutcome::Missing));
        }
        if matches!(
            crate::partition::query_access(context)?,
            crate::partition::AccessState::Importing
        ) {
            return Ok(Json(StreamTailOutcome::Missing));
        }
        Ok(Json(StreamTailOutcome::Latest(tail(context, input)?)))
    }
}

fn tail(context: &mut QueryContext<'_>, input: StreamTailInput) -> Result<Option<String>> {
    let rows = context.sql(&statement(
        "SELECT sequence_number FROM ddb_stream_records WHERE table_id = ?1 AND stream_label = ?2 AND created_at_ms > ?3 ORDER BY sequence_number DESC LIMIT 1",
        vec![SqlValue::Text(input.table_id), SqlValue::Text(input.label), SqlValue::Integer(context.now_ms().saturating_sub(RETENTION_MS))],
    ))?;
    match rows[0].rows.first().map(Vec::as_slice) {
        Some([SqlValue::Text(sequence)]) => Ok(Some(sequence.clone())),
        None => Ok(None),
        _ => Err(Error::Command("invalid stream tail row")),
    }
}

fn read(
    context: &mut QueryContext<'_>,
    input: StreamJournalInput,
    closed: bool,
) -> Result<StreamJournalOutcome> {
    if input.limit == 0 || input.limit > 1_000 {
        return Err(Error::Command("stream record limit is outside 1..=1000"));
    }
    let mut cursor = input.after_sequence.unwrap_or_default();
    let mut records = Vec::new();
    let mut bytes = 0;
    let mut exhausted = false;
    while records.len() < usize::from(input.limit) {
        let rows = context.sql(&statement(
            "SELECT sequence_number, record FROM ddb_stream_records WHERE table_id = ?1 AND stream_label = ?2 AND sequence_number > ?3 AND created_at_ms > ?4 ORDER BY sequence_number LIMIT 1",
            vec![
                SqlValue::Text(input.table_id.clone()),
                SqlValue::Text(input.label.clone()),
                SqlValue::Text(cursor.clone()),
                SqlValue::Integer(context.now_ms().saturating_sub(RETENTION_MS)),
            ],
        ))?;
        let Some([SqlValue::Text(sequence), SqlValue::Blob(record)]) =
            rows[0].rows.first().map(Vec::as_slice)
        else {
            exhausted = true;
            break;
        };
        // DynamoDB GetRecords pages stop at 1 MiB; this also stays below the
        // Cell response limit after the surrounding query envelope is encoded.
        if bytes + record.len() > 1024 * 1024 {
            if records.is_empty() {
                return Err(Error::Command("stream record exceeds page budget"));
            }
            break;
        }
        bytes += record.len();
        cursor = sequence.clone();
        records.push(serde_json::from_slice(record)?);
    }
    if !exhausted && records.len() == usize::from(input.limit) {
        let rows = context.sql(&statement(
            "SELECT 1 FROM ddb_stream_records WHERE table_id = ?1 AND stream_label = ?2 AND sequence_number > ?3 AND created_at_ms > ?4 ORDER BY sequence_number LIMIT 1",
            vec![
                SqlValue::Text(input.table_id),
                SqlValue::Text(input.label),
                SqlValue::Text(cursor.clone()),
                SqlValue::Integer(context.now_ms().saturating_sub(RETENTION_MS)),
            ],
        ))?;
        exhausted = rows[0].rows.is_empty();
    }
    let last_sequence = (!records.is_empty()).then_some(cursor);
    Ok(StreamJournalOutcome::Page {
        records,
        last_sequence,
        closed,
        exhausted,
    })
}

pub(crate) fn append(
    context: &CommandContext<'_, '_>,
    table_id: &str,
    key_schema: &[KeySchemaElement],
    stream: Option<&StreamConfig>,
    old: Option<&Item>,
    new: Option<&Item>,
    ordinal: usize,
) -> Result<()> {
    append_record(
        context, table_id, key_schema, stream, old, new, ordinal, None,
    )
}

pub(crate) fn append_ttl_delete(
    context: &CommandContext<'_, '_>,
    table_id: &str,
    key_schema: &[KeySchemaElement],
    stream: Option<&StreamConfig>,
    old: Option<&Item>,
) -> Result<()> {
    append_record(
        context,
        table_id,
        key_schema,
        stream,
        old,
        None,
        0,
        Some(UserIdentity {
            identity_type: "Service".into(),
            principal_id: "dynamodb.amazonaws.com".into(),
        }),
    )
}

fn append_record(
    context: &CommandContext<'_, '_>,
    table_id: &str,
    key_schema: &[KeySchemaElement],
    stream: Option<&StreamConfig>,
    old: Option<&Item>,
    new: Option<&Item>,
    ordinal: usize,
    user_identity: Option<UserIdentity>,
) -> Result<()> {
    let Some(stream) = stream else {
        return Ok(());
    };
    if old == new {
        return Ok(());
    }
    let (event_name, source) = match (old, new) {
        (None, Some(item)) => (StreamEventName::Insert, item),
        (Some(_), Some(item)) => (StreamEventName::Modify, item),
        (Some(item), None) => (StreamEventName::Remove, item),
        (None, None) => return Ok(()),
    };
    let ordinal =
        u16::try_from(ordinal).map_err(|_| crate::Error::Command("stream ordinal overflow"))?;
    let sequence_number = format!("{:020}{:03}", context.sequence(), ordinal);
    let record = StreamRecord {
        event_id: format!(
            "{}-{sequence_number}",
            blake3::Hash::from_bytes(*context.cell_id().as_bytes()).to_hex()
        ),
        event_name,
        event_version: "1.1".into(),
        event_source: "aws:dynamodb".into(),
        aws_region: stream.region.clone(),
        dynamodb: StreamRecordData {
            approximate_creation_date_time: context.now_ms() / 1_000,
            keys: extract_key(source, key_schema),
            new_image: matches!(
                stream.view_type,
                StreamViewType::NewImage | StreamViewType::NewAndOldImages
            )
            .then(|| new.cloned())
            .flatten(),
            old_image: matches!(
                stream.view_type,
                StreamViewType::OldImage | StreamViewType::NewAndOldImages
            )
            .then(|| old.cloned())
            .flatten(),
            sequence_number: sequence_number.clone(),
            size_bytes: i64::try_from(item_size_bytes(source)).unwrap_or(i64::MAX),
            stream_view_type: stream.view_type,
        },
        user_identity,
    };
    // The enclosing Cell command commits the item and this row under one SQL
    // savepoint. A failed append aborts the item mutation as well.
    context.sql(&statement(
        "INSERT INTO ddb_stream_records (table_id, stream_label, sequence_number, created_at_ms, record) VALUES (?1, ?2, ?3, ?4, ?5)",
        vec![
            SqlValue::Text(table_id.into()),
            SqlValue::Text(stream.label.clone()),
            SqlValue::Text(sequence_number),
            SqlValue::Integer(context.now_ms()),
            SqlValue::Blob(serde_json::to_vec(&record)?),
        ],
    ))?;
    Ok(())
}
