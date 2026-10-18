//! Canonical ordered primary keys shared by routing and local BLOB indexes.
use crate::{Error, Result};
use bdb_keyspace::{Component, KeyKind, KeySchema, KeySpan, RangeKey};
use extenddb_core::types::{AttributeValue, Item, KeySchemaElement, KeyType};

pub(crate) fn component(value: &AttributeValue) -> Result<Component<'_>> {
    match value {
        AttributeValue::S(value) => Ok(Component::String(value)),
        AttributeValue::B(value) => Ok(Component::Binary(value)),
        AttributeValue::N(value) => Ok(Component::Number(value)),
        _ => Err(Error::Command("invalid key type")),
    }
}
fn kind(value: &AttributeValue) -> Result<KeyKind> {
    match value {
        AttributeValue::S(_) => Ok(KeyKind::String),
        AttributeValue::B(_) => Ok(KeyKind::Binary),
        AttributeValue::N(_) => Ok(KeyKind::Number),
        _ => Err(Error::Command("invalid key type")),
    }
}
pub(crate) fn sort_component(value: &AttributeValue) -> Result<Vec<u8>> {
    Ok(KeySchema {
        partition: kind(value)?,
        sort: None,
    }
    .encode(component(value)?, None)
    .map_err(|_| Error::Command("invalid ordered key"))?
    .to_vec())
}
pub(crate) fn sort_prefix(value: &AttributeValue) -> Result<Vec<u8>> {
    let (tag, value): (u8, &[u8]) = match value {
        AttributeValue::S(value) => (0x10, value.as_bytes()),
        AttributeValue::B(value) => (0x30, value),
        _ => {
            return Err(Error::Command(
                "begins_with requires a string or binary sort key",
            ));
        }
    };
    let mut bytes = vec![tag];
    for &byte in value {
        bytes.push(byte);
        if byte == 0 {
            bytes.push(0xff);
        }
    }
    Ok(bytes)
}
pub(crate) fn partition_key_bytes(item: &Item, schema: &[KeySchemaElement]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for key in schema.iter().filter(|key| key.key_type == KeyType::Hash) {
        bytes.extend_from_slice(&sort_component(
            item.get(&key.attribute_name)
                .ok_or(Error::Command("missing partition key attribute"))?,
        )?);
    }
    if bytes.is_empty() {
        return Err(Error::Command("table has no partition key"));
    }
    Ok(bytes)
}
pub(crate) fn index_key(item: &Item, schema: &[KeySchemaElement]) -> Result<(Vec<u8>, Vec<u8>)> {
    let partition = partition_key_bytes(item, schema)?;
    let mut sort = Vec::new();
    for element in schema.iter().filter(|key| key.key_type == KeyType::Range) {
        sort.extend_from_slice(&sort_component(
            item.get(&element.attribute_name)
                .ok_or(Error::Command("missing sort key attribute"))?,
        )?);
    }
    Ok((partition, sort))
}
/// Full ordered primary key, scoped by the immutable table generation's directory.
pub fn ordered_data_key(
    _table_id: &str,
    item: &Item,
    schema: &[KeySchemaElement],
) -> Result<RangeKey> {
    let (mut partition, sort) = index_key(item, schema)?;
    partition.extend_from_slice(&sort);
    RangeKey::new(partition).map_err(|_| Error::Command("ordered key exceeds its bound"))
}
/// GSI ties use the complete ordered base key, never JSON item-key bytes.
pub fn ordered_index_key(
    item: &Item,
    index: &[KeySchemaElement],
    base: &[KeySchemaElement],
) -> Result<RangeKey> {
    let mut bytes = ordered_data_key("", item, index)?.to_vec();
    bytes.extend_from_slice(ordered_data_key("", item, base)?.as_bytes());
    RangeKey::new(bytes).map_err(|_| Error::Command("ordered index key exceeds its bound"))
}
/// Every sort-key sibling (and every GSI tie) for an equality partition key.
pub(crate) fn partition_span(item: &Item, schema: &[KeySchemaElement]) -> Result<KeySpan> {
    let lower = partition_key_bytes(item, schema)?;
    let mut upper = lower.clone();
    while let Some(byte) = upper.pop() {
        if byte != u8::MAX {
            upper.push(byte + 1);
            return KeySpan::new(
                Some(RangeKey::new(lower).map_err(|_| Error::Command("invalid partition span"))?),
                Some(RangeKey::new(upper).map_err(|_| Error::Command("invalid partition span"))?),
            )
            .map_err(|_| Error::Command("invalid partition span"));
        }
    }
    Err(Error::Command("partition span has no upper bound"))
}

/// Compile ordinary DynamoDB SK conditions before resolving physical owners.
/// Extended multi-PK schemas retain the conservative complete PK span.
pub(crate) fn query_span(
    item: &Item,
    schema: &[KeySchemaElement],
    predicate: Option<&crate::SortPredicate>,
) -> Result<KeySpan> {
    use crate::{SortComparison as Op, SortPredicate as P};
    use bdb_keyspace::SortCondition as S;
    let Some(predicate) = predicate else {
        return partition_span(item, schema);
    };
    let partitions = schema
        .iter()
        .filter(|key| key.key_type == KeyType::Hash)
        .collect::<Vec<_>>();
    if partitions.len() != 1 {
        return partition_span(item, schema);
    }
    if schema
        .iter()
        .find(|key| key.key_type == KeyType::Range)
        .map(|key| key.attribute_name.as_str())
        != Some(predicate.attribute())
    {
        return Err(Error::Command("invalid Query sort attribute"));
    }
    let partition = item
        .get(&partitions[0].attribute_name)
        .ok_or(Error::Command("missing partition key"))?;
    let (condition, sort_kind) = match predicate {
        P::Compare { op, value, .. } => {
            let value_component = component(value)?;
            let condition = match op {
                Op::Eq => S::Eq(value_component),
                Op::Lt => S::Lt(value_component),
                Op::Le => S::Le(value_component),
                Op::Gt => S::Gt(value_component),
                Op::Ge => S::Ge(value_component),
            };
            (condition, kind(value)?)
        }
        P::Between { low, high, .. } => (S::Between(component(low)?, component(high)?), kind(low)?),
        P::BeginsWith { prefix, .. } => (S::BeginsWith(component(prefix)?), kind(prefix)?),
    };
    KeySchema {
        partition: kind(partition)?,
        sort: Some(sort_kind),
    }
    .query(component(partition)?, Some(condition))
    .map_err(|_| Error::Command("invalid ordered Query span"))
}

/// Stable ParallelScan segmentation only. Never use this hash for ownership.
pub fn data_key_hash(table_id: &str, key: &Item, schema: &[KeySchemaElement]) -> Result<[u8; 16]> {
    let partition = crate::items::item_key(key, schema)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"beyonddb.item.partition.v1\0");
    hasher.update(table_id.as_bytes());
    hasher.update(&partition);
    let mut hash = [0_u8; 16];
    hash.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Ok(hash)
}

/// A complete PK component with no SK suffix; required for initial LSI locality.
pub(crate) fn is_partition_fence(key: &RangeKey, schema: &[KeySchemaElement]) -> bool {
    let bytes = key.as_bytes();
    let mut offset = 0;
    for _ in schema.iter().filter(|key| key.key_type == KeyType::Hash) {
        if !matches!(bytes.get(offset), Some(0x10 | 0x20 | 0x30)) {
            return false;
        }
        offset += 1;
        let mut ended = false;
        while offset + 1 < bytes.len() {
            if bytes[offset] == 0 {
                match bytes[offset + 1] {
                    0 => {
                        offset += 2;
                        ended = true;
                        break;
                    }
                    0xff => {
                        offset += 2;
                        continue;
                    }
                    _ => return false,
                }
            }
            offset += 1;
        }
        if !ended {
            return false;
        }
        if offset == bytes.len() {
            return true;
        }
    }
    false
}
