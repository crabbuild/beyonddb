//! Independent participant prepares sharing a durable publication.

use std::collections::HashSet;

use cellule_runtime::registry::{Command, CommandContext, CommandResult};
use serde::{Deserialize, Serialize};

use super::participant::{PreparePartitionTransactionInput, prepare_partition};
use crate::{Json, PrepareTransactionOutcome, Result};

pub(crate) const MAX_PREPARES: usize = 16;
pub(crate) const PREPARE_BATCH_BYTES: usize = 32 * 1024;

/// Every input was prepared, or the complete application savepoint rolled back.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PreparePartitionBatchOutcome {
    /// All inputs have durable intents and locks at the returned sequence.
    Prepared,
    /// Use individual commands to preserve each rejection or replay outcome.
    IndividualRequired,
}

/// Coalesce bounded prepares without merging transaction identities or decisions.
pub struct PreparePartitionTransactionBatch;

impl Command for PreparePartitionTransactionBatch {
    const MODULE: &'static str = crate::DATA_MODULE;
    const ID: u32 = 29;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<Vec<PreparePartitionTransactionInput>>;
    type Output = Json<PreparePartitionBatchOutcome>;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(inputs): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        let mut identities = HashSet::new();
        if inputs.is_empty()
            || inputs.len() > MAX_PREPARES
            || serde_json::to_vec(&inputs)?.len() > PREPARE_BATCH_BYTES
            || inputs
                .iter()
                .any(|input| !identities.insert(input.transaction_id))
        {
            return Ok(individual_required());
        }
        for input in inputs {
            if !matches!(
                prepare_partition(context, input)?,
                CommandResult::Success(Json(PrepareTransactionOutcome::Prepared))
            ) {
                // Rejection rolls back *all* intents, read images, reservations
                // and locks created by this command. No individual failure
                // image is truncated to fit a shared result envelope.
                return Ok(individual_required());
            }
        }
        Ok(CommandResult::Success(Json(
            PreparePartitionBatchOutcome::Prepared,
        )))
    }
}

fn individual_required() -> CommandResult<Json<PreparePartitionBatchOutcome>> {
    CommandResult::Rejected(Json(PreparePartitionBatchOutcome::IndividualRequired))
}
