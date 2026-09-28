//! Item changes retained in the Cell that committed them.

use cellule_runtime::registry::CommandContext;
use extenddb_core::types::{
    Item, KeySchemaElement, StreamEventName, StreamRecord, StreamRecordData, StreamViewType,
    extract_key, item_size_bytes,
};
use serde::{Deserialize, Serialize};

use crate::table::statement;
use crate::{Result, SqlValue};

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
