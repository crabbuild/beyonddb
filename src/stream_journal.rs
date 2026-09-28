//! Item changes retained in the Cell that committed them.

use cellule_runtime::registry::{CommandContext, Query, QueryContext};
use extenddb_core::types::{
    Item, KeySchemaElement, StreamEventName, StreamRecord, StreamRecordData, StreamViewType,
    extract_key, item_size_bytes,
};
use serde::{Deserialize, Serialize};

use crate::table::statement;
use crate::{DATA_MODULE, Error, Json, MODULE, Result, SqlValue};

pub(crate) const SCHEMA: &str = include_str!("stream_journal.sql");

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
        let rows = context.sql(&statement(
            "SELECT record FROM ddb_tables WHERE table_id = ?1",
            vec![SqlValue::Text(input.table_id.clone())],
        ))?;
        let Some(table) = crate::table::decode_table(&rows[0])? else {
            return Ok(Json(StreamJournalOutcome::Missing));
        };
        if table.stream.as_ref().map(|stream| stream.label.as_str()) != Some(input.label.as_str()) {
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
        let rows = context.sql(&statement(
            "SELECT record FROM ddb_tables WHERE table_id = ?1",
            vec![SqlValue::Text(input.table_id.clone())],
        ))?;
        let Some(table) = crate::table::decode_table(&rows[0])? else {
            return Ok(Json(StreamTailOutcome::Missing));
        };
        if table.stream.as_ref().map(|stream| stream.label.as_str()) != Some(input.label.as_str()) {
            return Ok(Json(StreamTailOutcome::Missing));
        }
        Ok(Json(StreamTailOutcome::Latest(tail(context, input)?)))
    }
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
        "SELECT sequence_number FROM ddb_stream_records WHERE table_id = ?1 AND stream_label = ?2 ORDER BY sequence_number DESC LIMIT 1",
        vec![SqlValue::Text(input.table_id), SqlValue::Text(input.label)],
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
            "SELECT sequence_number, record FROM ddb_stream_records WHERE table_id = ?1 AND stream_label = ?2 AND sequence_number > ?3 ORDER BY sequence_number LIMIT 1",
            vec![
                SqlValue::Text(input.table_id.clone()),
                SqlValue::Text(input.label.clone()),
                SqlValue::Text(cursor.clone()),
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
            "SELECT 1 FROM ddb_stream_records WHERE table_id = ?1 AND stream_label = ?2 AND sequence_number > ?3 ORDER BY sequence_number LIMIT 1",
            vec![
                SqlValue::Text(input.table_id),
                SqlValue::Text(input.label),
                SqlValue::Text(cursor.clone()),
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
        user_identity: None,
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
