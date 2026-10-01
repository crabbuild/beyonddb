//! Compact internal read images. DynamoDB JSON and stored items are unchanged.

use std::collections::{BTreeMap, BTreeSet};

use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError};
use extenddb_core::types::{AttributeValue, Item};

type Result<T> = std::result::Result<T, CodecError>;
const MAX_DEPTH: usize = 32;

fn add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right).ok_or(CodecError::Limit)
}

fn bytes_size(bytes: &[u8]) -> Result<usize> {
    add(4, bytes.len())
}

fn collection_depth(depth: usize) -> Result<usize> {
    if depth >= MAX_DEPTH {
        return Err(CodecError::Invalid("item nesting exceeds 32 levels"));
    }
    Ok(depth + 1)
}

fn map_size(map: &Item, depth: usize) -> Result<usize> {
    map.iter().try_fold(4, |size, (name, value)| {
        add(
            add(size, bytes_size(name.as_bytes())?)?,
            value_size(value, depth)?,
        )
    })
}

fn value_size(value: &AttributeValue, depth: usize) -> Result<usize> {
    let payload = match value {
        AttributeValue::S(value) | AttributeValue::N(value) => bytes_size(value.as_bytes())?,
        AttributeValue::B(value) => bytes_size(value)?,
        AttributeValue::SS(values) | AttributeValue::NS(values) => values
            .iter()
            .try_fold(4, |size, value| add(size, bytes_size(value.as_bytes())?))?,
        AttributeValue::BS(values) => values
            .iter()
            .try_fold(4, |size, value| add(size, bytes_size(value)?))?,
        AttributeValue::Bool(_) => 1,
        AttributeValue::Null => 0,
        AttributeValue::L(values) => {
            let next = collection_depth(depth)?;
            values
                .iter()
                .try_fold(4, |size, value| add(size, value_size(value, next)?))?
        }
        AttributeValue::M(values) => map_size(values, collection_depth(depth)?)?,
    };
    add(1, payload)
}

pub(crate) fn images_size(images: &[Option<Item>]) -> Result<usize> {
    if images.len() > 100 {
        return Err(CodecError::Invalid("transaction read count exceeds 100"));
    }
    images.iter().try_fold(4, |size, item| {
        add(
            add(size, 1)?,
            item.as_ref().map_or(Ok(0), |item| map_size(item, 0))?,
        )
    })
}

pub(crate) fn encode_images(images: &[Option<Item>], encoder: &mut BoundedEncoder) -> Result<()> {
    encoder.write_count(images.len())?;
    for item in images {
        encode_image(item.as_ref(), encoder)?;
    }
    Ok(())
}

pub(crate) fn encode_image(item: Option<&Item>, encoder: &mut BoundedEncoder) -> Result<()> {
    encoder.write_bool(item.is_some())?;
    if let Some(item) = item {
        encode_map(item, encoder, 0)?;
    }
    Ok(())
}

fn encode_map(map: &Item, encoder: &mut BoundedEncoder, depth: usize) -> Result<()> {
    encoder.write_count(map.len())?;
    for (name, value) in map {
        encoder.write_text(name)?;
        encode_value(value, encoder, depth)?;
    }
    Ok(())
}

fn encode_value(value: &AttributeValue, encoder: &mut BoundedEncoder, depth: usize) -> Result<()> {
    let tag = match value {
        AttributeValue::S(_) => 0,
        AttributeValue::N(_) => 1,
        AttributeValue::B(_) => 2,
        AttributeValue::SS(_) => 3,
        AttributeValue::NS(_) => 4,
        AttributeValue::BS(_) => 5,
        AttributeValue::Bool(_) => 6,
        AttributeValue::Null => 7,
        AttributeValue::L(_) => 8,
        AttributeValue::M(_) => 9,
    };
    encoder.write_u8(tag)?;
    match value {
        AttributeValue::S(value) | AttributeValue::N(value) => encoder.write_text(value),
        AttributeValue::B(value) => encoder.write_bytes(value),
        AttributeValue::SS(values) | AttributeValue::NS(values) => {
            encoder.write_count(values.len())?;
            for value in values {
                encoder.write_text(value)?;
            }
            Ok(())
        }
        AttributeValue::BS(values) => {
            encoder.write_count(values.len())?;
            for value in values {
                encoder.write_bytes(value)?;
            }
            Ok(())
        }
        AttributeValue::Bool(value) => encoder.write_bool(*value),
        AttributeValue::Null => Ok(()),
        AttributeValue::L(values) => {
            let next = collection_depth(depth)?;
            encoder.write_count(values.len())?;
            for value in values {
                encode_value(value, encoder, next)?;
            }
            Ok(())
        }
        AttributeValue::M(values) => encode_map(values, encoder, collection_depth(depth)?),
    }
}

pub(crate) fn decode_images(decoder: &mut BoundedDecoder<'_>) -> Result<Vec<Option<Item>>> {
    let count = decoder.read_count()?;
    if count > 100 {
        return Err(CodecError::Invalid("transaction read count exceeds 100"));
    }
    let mut images = Vec::with_capacity(count);
    for _ in 0..count {
        images.push(if decoder.read_bool()? {
            Some(decode_map(decoder, 0)?)
        } else {
            None
        });
    }
    Ok(images)
}

fn decode_map(decoder: &mut BoundedDecoder<'_>, depth: usize) -> Result<Item> {
    let count = decoder.read_count()?;
    let mut map: Item = BTreeMap::new();
    for _ in 0..count {
        let name = decoder.read_text()?;
        if map
            .last_key_value()
            .is_some_and(|(previous, _)| previous.as_str() >= name)
        {
            return Err(CodecError::Invalid("item keys are not strictly ordered"));
        }
        let value = decode_value(decoder, depth)?;
        map.insert(name.to_owned(), value);
    }
    Ok(map)
}

fn decode_set<T: Ord>(
    decoder: &mut BoundedDecoder<'_>,
    read: impl Fn(&mut BoundedDecoder<'_>) -> Result<T>,
) -> Result<BTreeSet<T>> {
    let count = decoder.read_count()?;
    if count == 0 {
        return Err(CodecError::Invalid("empty attribute set"));
    }
    let mut set = BTreeSet::new();
    for _ in 0..count {
        let value = read(decoder)?;
        if set.last().is_some_and(|previous| previous >= &value) {
            return Err(CodecError::Invalid("attribute set is not strictly ordered"));
        }
        set.insert(value);
    }
    Ok(set)
}

fn decode_value(decoder: &mut BoundedDecoder<'_>, depth: usize) -> Result<AttributeValue> {
    Ok(match decoder.read_u8()? {
        0 => AttributeValue::S(decoder.read_text()?.to_owned()),
        1 => AttributeValue::N(decoder.read_text()?.to_owned()),
        2 => AttributeValue::B(decoder.read_bytes()?.to_vec()),
        3 => AttributeValue::SS(decode_set(decoder, |decoder| {
            Ok(decoder.read_text()?.to_owned())
        })?),
        4 => AttributeValue::NS(decode_set(decoder, |decoder| {
            Ok(decoder.read_text()?.to_owned())
        })?),
        5 => AttributeValue::BS(decode_set(decoder, |decoder| {
            Ok(decoder.read_bytes()?.to_vec())
        })?),
        6 => AttributeValue::Bool(decoder.read_bool()?),
        7 => AttributeValue::Null,
        8 => {
            let next = collection_depth(depth)?;
            let count = decoder.read_count()?;
            // Do not preallocate from a peer-supplied count. Truncation is
            // checked as each element is consumed from the bounded decoder.
            let mut values = Vec::new();
            for _ in 0..count {
                values.push(decode_value(decoder, next)?);
            }
            AttributeValue::L(values)
        }
        9 => AttributeValue::M(decode_map(decoder, collection_depth(depth)?)?),
        _ => return Err(CodecError::Invalid("unknown attribute tag")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(bytes: &[u8]) -> Result<Vec<Option<Item>>> {
        let mut decoder = BoundedDecoder::new(bytes, crate::OPERATION_BYTES)?;
        let value = decode_images(&mut decoder)?;
        decoder.finish()?;
        Ok(value)
    }

    #[test]
    fn malformed_images_are_rejected_without_allocating_peer_counts() {
        for bytes in [
            vec![0xff; 4],
            vec![0, 0, 0, 1, 2],
            vec![0, 0, 0, 1, 1, 0xff, 0xff, 0xff, 0xff],
        ] {
            assert!(decode(&bytes).is_err());
        }
        let mut encoded = BoundedEncoder::new(1024).unwrap();
        encode_images(
            &[Some(Item::from([("name".into(), AttributeValue::Null)]))],
            &mut encoded,
        )
        .unwrap();
        let bytes = encoded.finish();
        for length in 0..bytes.len() {
            assert!(decode(&bytes[..length]).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode(&trailing).is_err());
        let mut unknown = bytes;
        *unknown.last_mut().unwrap() = 255;
        assert!(decode(&unknown).is_err());
    }

    #[test]
    fn compact_maps_and_sets_require_canonical_order() {
        for is_set in [false, true] {
            let mut encoder = BoundedEncoder::new(1024).unwrap();
            encoder.write_count(1).unwrap();
            encoder.write_bool(true).unwrap();
            encoder.write_count(if is_set { 1 } else { 2 }).unwrap();
            encoder.write_text("b").unwrap();
            if is_set {
                encoder.write_u8(3).unwrap();
                encoder.write_count(2).unwrap();
                encoder.write_text("b").unwrap();
                encoder.write_text("a").unwrap();
            } else {
                encoder.write_u8(7).unwrap();
                encoder.write_text("a").unwrap();
                encoder.write_u8(7).unwrap();
            }
            assert!(decode(&encoder.finish()).is_err());
        }
    }

    #[test]
    fn compact_values_enforce_the_dynamodb_nesting_limit() {
        let mut value = AttributeValue::Null;
        for _ in 0..32 {
            value = AttributeValue::L(vec![value]);
        }
        let images = [Some(Item::from([("nested".into(), value.clone())]))];
        let size = images_size(&images).unwrap();
        let mut encoder = BoundedEncoder::new(1024).unwrap();
        encode_images(&images, &mut encoder).unwrap();
        let bytes = encoder.finish();
        assert_eq!(size, bytes.len());
        assert!(decode(&bytes).unwrap() == images);
        let invalid = [Some(Item::from([(
            "nested".into(),
            AttributeValue::L(vec![value]),
        )]))];
        assert!(images_size(&invalid).is_err());
        let mut malicious = BoundedEncoder::new(1024).unwrap();
        malicious.write_count(1).unwrap();
        malicious.write_bool(true).unwrap();
        malicious.write_count(1).unwrap();
        malicious.write_text("nested").unwrap();
        for _ in 0..33 {
            malicious.write_u8(8).unwrap();
            malicious.write_count(1).unwrap();
        }
        malicious.write_u8(7).unwrap();
        assert!(decode(&malicious.finish()).is_err());
    }
}
