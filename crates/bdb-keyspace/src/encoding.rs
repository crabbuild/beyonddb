use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{Error, KeySpan, MAX_ENCODED_PRIMARY_KEY_BYTES, RangeKey, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyKind {
    String,
    Number,
    Binary,
}

#[derive(Clone, Copy, Debug)]
pub enum Component<'a> {
    String(&'a str),
    Number(&'a str),
    Binary(&'a [u8]),
}

impl Component<'_> {
    fn kind(self) -> KeyKind {
        match self {
            Self::String(_) => KeyKind::String,
            Self::Number(_) => KeyKind::Number,
            Self::Binary(_) => KeyKind::Binary,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeySchema {
    pub partition: KeyKind,
    pub sort: Option<KeyKind>,
}

#[derive(Clone, Copy, Debug)]
pub enum SortCondition<'a> {
    Eq(Component<'a>),
    Lt(Component<'a>),
    Le(Component<'a>),
    Gt(Component<'a>),
    Ge(Component<'a>),
    Between(Component<'a>, Component<'a>),
    BeginsWith(Component<'a>),
}

impl KeySchema {
    /// A fence before every sort-key sibling for this partition-key value.
    pub fn partition_start(self, partition: Component<'_>) -> Result<RangeKey> {
        RangeKey::new(self.partition_component(partition)?)
    }

    pub fn encode(self, partition: Component<'_>, sort: Option<Component<'_>>) -> Result<RangeKey> {
        let mut encoded = self.partition_component(partition)?;
        match (self.sort, sort) {
            (Some(kind), Some(value)) => component(&mut encoded, kind, value, 1_024, false)?,
            (None, None) => {}
            _ => return Err(Error::SortKey),
        }
        RangeKey::new(encoded)
    }

    /// GSI duplicate keys are distinct ordered entries by complete base key.
    /// The base key must come from the base table's schema, not another index.
    pub fn encode_index(
        self,
        partition: Component<'_>,
        sort: Option<Component<'_>>,
        base_key: &RangeKey,
    ) -> Result<RangeKey> {
        if base_key.as_bytes().len() > MAX_ENCODED_PRIMARY_KEY_BYTES {
            return Err(Error::KeySize);
        }
        let mut key = self.encode(partition, sort)?.as_bytes().to_vec();
        key.extend_from_slice(base_key.as_bytes());
        RangeKey::new(key)
    }

    /// Compile a Query into a half-open physical interval. DynamoDB still
    /// requires equality on the logical partition key.
    pub fn query(
        self,
        partition: Component<'_>,
        sort: Option<SortCondition<'_>>,
    ) -> Result<KeySpan> {
        let partition = self.partition_component(partition)?;
        let partition_end = successor(&partition).ok_or(Error::Range)?;
        let mut lower = partition.clone();
        let mut upper = partition_end;
        if let Some(condition) = sort {
            let kind = self.sort.ok_or(Error::SortKey)?;
            let encode = |value, prefix| {
                let mut key = partition.clone();
                component(&mut key, kind, value, 1_024, prefix)?;
                Ok::<_, Error>(key)
            };
            match condition {
                SortCondition::Eq(value) => {
                    lower = encode(value, false)?;
                    upper = successor(&lower).ok_or(Error::Range)?;
                }
                SortCondition::Lt(value) => upper = encode(value, false)?,
                SortCondition::Le(value) => {
                    upper = successor(&encode(value, false)?).ok_or(Error::Range)?;
                }
                SortCondition::Gt(value) => {
                    lower = successor(&encode(value, false)?).ok_or(Error::Range)?;
                }
                SortCondition::Ge(value) => lower = encode(value, false)?,
                SortCondition::Between(low, high) => {
                    lower = encode(low, false)?;
                    let high = encode(high, false)?;
                    if lower > high {
                        return Err(Error::Range);
                    }
                    upper = successor(&high).ok_or(Error::Range)?;
                }
                SortCondition::BeginsWith(value) => {
                    lower = encode(value, true)?;
                    upper = successor(&lower).ok_or(Error::Range)?;
                }
            }
        }
        KeySpan::new(Some(RangeKey::new(lower)?), Some(RangeKey::new(upper)?))
    }

    fn partition_component(self, value: Component<'_>) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        component(&mut bytes, self.partition, value, 2_048, false)?;
        Ok(bytes)
    }
}

fn component(
    output: &mut Vec<u8>,
    kind: KeyKind,
    value: Component<'_>,
    limit: usize,
    prefix: bool,
) -> Result<()> {
    if kind != value.kind() {
        return Err(Error::KeyType);
    }
    let numeric;
    let (tag, bytes) = match value {
        Component::String(value) => (0x10, value.as_bytes()),
        Component::Number(value) => {
            if prefix {
                return Err(Error::PrefixType);
            }
            numeric = number(value)?;
            (0x20, numeric.as_slice())
        }
        Component::Binary(value) => (0x30, value),
    };
    if bytes.len() > limit || (!prefix && bytes.is_empty()) {
        return Err(Error::KeySize);
    }
    output.push(tag);
    for &byte in bytes {
        output.push(byte);
        if byte == 0 {
            output.push(0xff);
        }
    }
    if !prefix {
        output.extend_from_slice(&[0, 0]);
    }
    Ok(())
}

/// Smallest byte prefix strictly beyond all bytes starting with `prefix`.
pub(crate) fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut next = prefix.to_vec();
    while let Some(byte) = next.pop() {
        if byte != u8::MAX {
            next.push(byte + 1);
            return Some(next);
        }
    }
    None
}

// Fixed precision avoids floating-point conversion and reverses negative
// magnitude ordering correctly, including decimal-prefix cases (-1.01 < -1).
fn number(text: &str) -> Result<Vec<u8>> {
    if text.is_empty() || text.len() > 4_096 {
        return Err(Error::Number);
    }
    if let Some((_, exponent)) = text.split_once(['e', 'E']) {
        let exponent: i32 = exponent.parse().map_err(|_| Error::Number)?;
        if !(-10_000..=10_000).contains(&exponent) {
            return Err(Error::Number);
        }
    }
    let value: BigDecimal = text.parse().map_err(|_| Error::Number)?;
    let zero = BigDecimal::from(0);
    if value == zero {
        return Ok(vec![1]);
    }
    let negative = value < zero;
    let magnitude = if negative { -value } else { value }.normalized();
    let (mantissa, scale) = magnitude.as_bigint_and_exponent();
    let digits = mantissa.to_string();
    let exponent = i64::try_from(digits.len())
        .ok()
        .and_then(|length| length.checked_sub(scale))
        .and_then(|length| length.checked_sub(1))
        .ok_or(Error::Number)?;
    if digits.len() > 38 || !(-130..=125).contains(&exponent) {
        return Err(Error::Number);
    }
    let biased = u8::try_from(exponent + 130).map_err(|_| Error::Number)?;
    let mut encoded = vec![if negative { 0 } else { 2 }];
    encoded.push(if negative { u8::MAX - biased } else { biased });
    for digit in digits.bytes().chain(std::iter::repeat(b'0')).take(38) {
        let digit = digit - b'0';
        encoded.push(if negative { 9 - digit } else { digit });
    }
    Ok(encoded)
}
