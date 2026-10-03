//! Independent updates sharing one commit and retaining individual results.

use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};

use super::*;

pub(crate) const MAX_UPDATES: usize = 16;
pub(crate) const BATCH_INPUT_BYTES: u32 = 1024 * 1024;
pub(crate) const BATCH_OUTPUT_BYTES: u32 = 128 * 1024;

pub(crate) const fn batch_operation(id: u32) -> OperationDescriptor {
    OperationDescriptor {
        input_limit: BATCH_INPUT_BYTES,
        output_limit: BATCH_OUTPUT_BYTES,
        ..operation(id)
    }
}

/// A routed update and the successful images required by its caller.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchedPartitionUpdate {
    /// The same validation and expression contract as a standalone update.
    pub input: PartitionUpdateInput,
    /// Keep the previous successful image; condition failures always retain it.
    pub return_old: bool,
    /// Keep the committed new image.
    pub return_new: bool,
}

/// A compact reply, or a confirmed rollback requiring separate commands.
#[derive(Clone, Debug, PartialEq)]
pub enum PartitionUpdateBatchOutcome {
    /// Every entry corresponds to its input, including condition failures.
    Results(Vec<PartitionUpdateOutcome>),
    /// All application writes were rolled back before this rejection committed.
    IndividualRequired,
}

impl WireValue for PartitionUpdateBatchOutcome {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        let Self::Results(results) = self else {
            return encoder.write_u8(0);
        };
        if results.is_empty() || results.len() > MAX_UPDATES {
            return Err(CodecError::Invalid("update batch count is outside 1..=16"));
        }
        encoder.write_u8(1)?;
        encoder.write_count(results.len())?;
        for result in results {
            match result {
                PartitionUpdateOutcome::Applied { old, new } => {
                    encoder.write_u8(0)?;
                    encoder.write_count(2)?;
                    crate::item_wire::encode_image(old.as_ref(), encoder)?;
                    crate::item_wire::encode_image(Some(new), encoder)?;
                }
                PartitionUpdateOutcome::ConditionFailed(old) => {
                    encoder.write_u8(1)?;
                    crate::item_wire::encode_images(std::slice::from_ref(old), encoder)?;
                }
                other => {
                    encoder.write_u8(2)?;
                    Json(other.clone()).encode(encoder)?;
                }
            }
        }
        Ok(())
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        match decoder.read_u8()? {
            0 => return Ok(Self::IndividualRequired),
            1 => {}
            _ => return Err(CodecError::Invalid("unknown update batch envelope")),
        }
        let count = decoder.read_count()?;
        if count == 0 || count > MAX_UPDATES {
            return Err(CodecError::Invalid("update batch count is outside 1..=16"));
        }
        let mut results = Vec::with_capacity(count);
        for _ in 0..count {
            results.push(match decoder.read_u8()? {
                0 => {
                    let mut images = crate::item_wire::decode_images(decoder)?;
                    if images.len() != 2 {
                        return Err(CodecError::Invalid("update result needs two images"));
                    }
                    let Some(Some(new)) = images.pop() else {
                        return Err(CodecError::Invalid("update result lacks new image"));
                    };
                    PartitionUpdateOutcome::Applied {
                        old: images.pop().flatten(),
                        new,
                    }
                }
                1 => {
                    let mut images = crate::item_wire::decode_images(decoder)?;
                    if images.len() != 1 {
                        return Err(CodecError::Invalid("condition result needs one image"));
                    }
                    PartitionUpdateOutcome::ConditionFailed(images.pop().flatten())
                }
                2 => {
                    let Json(result) = Json::<PartitionUpdateOutcome>::decode(decoder)?;
                    if matches!(
                        result,
                        PartitionUpdateOutcome::Applied { .. }
                            | PartitionUpdateOutcome::ConditionFailed(_)
                    ) {
                        return Err(CodecError::Invalid("noncanonical update result envelope"));
                    }
                    result
                }
                _ => return Err(CodecError::Invalid("unknown update result tag")),
            });
        }
        Ok(Self::Results(results))
    }
}

/// Coalesce distinct item updates, isolating ordinary validation rejections.
pub struct PartitionUpdateBatch;

impl Command for PartitionUpdateBatch {
    const MODULE: &'static str = DATA_MODULE;
    const ID: u32 = 25;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<Vec<BatchedPartitionUpdate>>;
    type Output = PartitionUpdateBatchOutcome;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(inputs): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        execute(context, inputs, BATCH_OUTPUT_BYTES)
    }
}

/// One update with a larger compact reply after confirmed batch rollback.
pub struct PartitionUpdateIndividual;

impl Command for PartitionUpdateIndividual {
    const MODULE: &'static str = DATA_MODULE;
    const ID: u32 = 26;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<Vec<BatchedPartitionUpdate>>;
    type Output = PartitionUpdateBatchOutcome;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(inputs): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        if inputs.len() != 1 {
            return Err(Error::Command("individual update requires one operation"));
        }
        execute(context, inputs, OPERATION_BYTES)
    }
}

fn execute(
    context: &mut CommandContext<'_, '_>,
    inputs: Vec<BatchedPartitionUpdate>,
    limit: u32,
) -> Result<CommandResult<PartitionUpdateBatchOutcome>> {
    if inputs.is_empty() || inputs.len() > MAX_UPDATES {
        return Err(Error::Command("update batch count is outside 1..=16"));
    }
    for (index, input) in inputs.iter().enumerate() {
        if inputs[..index].iter().any(|previous| {
            previous.input.table_id == input.input.table_id && previous.input.key == input.input.key
        }) {
            return Err(Error::Command("update batch repeats an item key"));
        }
    }
    let mut results = Vec::with_capacity(inputs.len());
    for (ordinal, update) in inputs.into_iter().enumerate() {
        // Every ordinary rejection in execute_partition_update precedes the
        // first application write. A storage/stream error instead propagates
        // and rolls back the entire command; it is never treated as isolated.
        let result = super::execute_partition_update(context, update.input, true, ordinal)?;
        let Json(mut result) = match result {
            CommandResult::Success(result) | CommandResult::Rejected(result) => result,
        };
        if let PartitionUpdateOutcome::Applied { old, new } = &mut result {
            if !update.return_old {
                *old = None;
            }
            if !update.return_new {
                new.clear();
            }
        }
        results.push(result);
    }
    let output = PartitionUpdateBatchOutcome::Results(results);
    let mut encoder = BoundedEncoder::new(limit)?;
    match output.encode(&mut encoder) {
        Ok(()) => Ok(CommandResult::Success(output)),
        Err(CodecError::Limit) if limit == BATCH_OUTPUT_BYTES => {
            // Cellule's application savepoint rolls back items, indexes,
            // capacity reservations and stream records before recording this
            // rejected receipt. Only that durable rejection permits fallback.
            Ok(CommandResult::Rejected(
                PartitionUpdateBatchOutcome::IndividualRequired,
            ))
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(bytes: &[u8]) -> std::result::Result<PartitionUpdateBatchOutcome, CodecError> {
        let mut decoder = BoundedDecoder::new(bytes, OPERATION_BYTES)?;
        let result = PartitionUpdateBatchOutcome::decode(&mut decoder)?;
        decoder.finish()?;
        Ok(result)
    }

    #[test]
    fn update_batch_codec_rejects_unbounded_counts_truncation_and_trailing_bytes() {
        for bytes in [
            vec![1, 0xff, 0xff, 0xff, 0xff],
            vec![1, 0, 0, 0, 0],
            vec![2],
            vec![0, 0],
        ] {
            assert!(decode(&bytes).is_err());
        }
        let item = Item::from([(
            "padding".into(),
            extenddb_core::types::AttributeValue::S("\0".repeat(390_000)),
        )]);
        let expected =
            PartitionUpdateBatchOutcome::Results(vec![PartitionUpdateOutcome::Applied {
                old: Some(item.clone()),
                new: item,
            }]);
        let mut encoder = BoundedEncoder::new(OPERATION_BYTES).unwrap();
        expected.encode(&mut encoder).unwrap();
        let bytes = encoder.finish();
        assert!(
            bytes.len() < 800_000,
            "escaped images must retain compact size"
        );
        assert_eq!(decode(&bytes).unwrap(), expected);
        for length in [0, 1, 4, 8, bytes.len() - 1] {
            assert!(decode(&bytes[..length]).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(decode(&trailing).is_err());
        let mut small = BoundedEncoder::new(BATCH_OUTPUT_BYTES).unwrap();
        assert!(matches!(
            expected.encode(&mut small),
            Err(CodecError::Limit)
        ));
    }
}
