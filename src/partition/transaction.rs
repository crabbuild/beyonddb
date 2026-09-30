//! Atomic reads and writes within one routed data Cell.

use crate::participant::StagedEffect;
use std::collections::HashSet;

use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};
use cellule_runtime::registry::{Command, CommandContext, CommandResult, Query, QueryContext};
use extenddb_core::types::Item;
use serde::{Deserialize, Serialize};

use super::{
    AccessState, DATA_MODULE, Json, PartitionSpec, Result, SqlValue, command_access, command_item,
    data_key_hash, item_key, statement, valid_item, valid_key, write_item,
};
use crate::PrepareTransactionOutcome;
use crate::items::{TransactionFailure, TransactionOperation};

/// Ordered writes that must all address the same installed data Cell.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PartitionTransactWriteInput {
    /// Routed table identity.
    pub table_id: String,
    /// Data Cell epoch observed at routing time.
    pub epoch: u64,
    /// Item writes and condition checks in request order.
    pub operations: Vec<TransactionOperation>,
}

/// Result of one partition-local transactional write.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PartitionTransactWriteOutcome {
    /// Every write and its query index entry committed.
    Applied,
    /// No operation committed.
    Rejected {
        /// Position of the failing operation.
        index: usize,
        /// Validation or condition failure.
        reason: TransactionFailure,
    },
    /// The data Cell has no installed partition.
    NotInstalled,
    /// Table identity or routing epoch changed.
    StaleRoute,
    /// The source has been sealed for a split.
    Sealed,
    /// An import-only child is not yet serving.
    NotReady,
    /// A key is outside this partition's range.
    WrongPartition,
}

/// Commit a batch to one data Cell or roll back every staged write.
pub struct PartitionTransactWrite;

impl Command for PartitionTransactWrite {
    const MODULE: &'static str = DATA_MODULE;
    const ID: u32 = 9;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<PartitionTransactWriteInput>;
    type Output = Json<PartitionTransactWriteOutcome>;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(input): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        execute_write(context, input, false)
    }
}

/// Partition-local transactional writes that do not return condition-failure images.
pub struct PartitionTransactWriteNoReturn;

impl Command for PartitionTransactWriteNoReturn {
    const MODULE: &'static str = DATA_MODULE;
    const ID: u32 = 24;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<PartitionTransactWriteInput>;
    type Output = Json<PartitionTransactWriteOutcome>;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(input): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        execute_write(context, input, true)
    }
}

fn execute_write(
    context: &mut CommandContext<'_, '_>,
    input: PartitionTransactWriteInput,
    strip_old_images: bool,
) -> Result<CommandResult<Json<PartitionTransactWriteOutcome>>> {
    let Some(spec) = super::indexes::command_spec(context)? else {
        return Ok(rejected(PartitionTransactWriteOutcome::NotInstalled));
    };
    if spec.table.id != input.table_id || spec.epoch != input.epoch {
        return Ok(rejected(PartitionTransactWriteOutcome::StaleRoute));
    }
    match command_access(context)? {
        AccessState::Serving => {}
        AccessState::Sealed => return Ok(rejected(PartitionTransactWriteOutcome::Sealed)),
        AccessState::Importing => return Ok(rejected(PartitionTransactWriteOutcome::NotReady)),
    }
    if input.operations.is_empty() || input.operations.len() > 100 {
        return Ok(validation(
            0,
            "transaction operation count is outside 1..=100",
        ));
    }

    let staged = match stage_operations(context, &spec, input.operations)? {
        Ok(staged) => staged,
        Err(reason) => return Ok(rejected(reason.single_outcome(strip_old_images))),
    };
    apply_staged(context, &spec.table, spec.epoch, staged)?;
    Ok(CommandResult::Success(Json(
        PartitionTransactWriteOutcome::Applied,
    )))
}

/// Result of an atomic read batch confined to one data Cell.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PartitionTransactReadOutcome {
    /// Every requested image was read from one Cell snapshot.
    Applied(Vec<Option<Item>>),
    /// The encoded aggregate requires reading durable participant images individually.
    SavedImagesRequired,
    /// No image was returned because one operation failed validation or locking.
    Rejected {
        /// Position of the failing read.
        index: usize,
        /// Validation or transaction conflict reason.
        reason: TransactionFailure,
    },
    /// The data Cell has no installed partition.
    NotInstalled,
    /// Table identity or routing epoch changed.
    StaleRoute,
    /// The source has been sealed for a split.
    Sealed,
    /// An import-only child is not yet serving.
    NotReady,
    /// A key is outside this partition's range.
    WrongPartition,
}

/// Read a transaction batch atomically when every item belongs to one data Cell.
pub struct PartitionTransactRead;

impl Command for PartitionTransactRead {
    const MODULE: &'static str = DATA_MODULE;
    const ID: u32 = 23;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<PartitionTransactWriteInput>;
    type Output = Json<PartitionTransactReadOutcome>;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(input): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        let Some(spec) = super::indexes::command_spec(context)? else {
            return Ok(rejected_read(PartitionTransactReadOutcome::NotInstalled));
        };
        if spec.table.id != input.table_id || spec.epoch != input.epoch {
            return Ok(rejected_read(PartitionTransactReadOutcome::StaleRoute));
        }
        match command_access(context)? {
            AccessState::Serving => {}
            AccessState::Sealed => {
                return Ok(rejected_read(PartitionTransactReadOutcome::Sealed));
            }
            AccessState::Importing => {
                return Ok(rejected_read(PartitionTransactReadOutcome::NotReady));
            }
        }
        if input.operations.is_empty() || input.operations.len() > 100 {
            return Ok(rejected_read(PartitionTransactReadOutcome::Rejected {
                index: 0,
                reason: TransactionFailure::Validation(
                    "transaction operation count is outside 1..=100".into(),
                ),
            }));
        }
        let staged = match stage_operations(context, &spec, input.operations)? {
            Ok(staged) => staged,
            Err(reason) => {
                return Ok(rejected_read(match reason {
                    StageError::StaleRoute => PartitionTransactReadOutcome::StaleRoute,
                    StageError::WrongPartition => PartitionTransactReadOutcome::WrongPartition,
                    StageError::Rejected { index, reason } => {
                        PartitionTransactReadOutcome::Rejected { index, reason }
                    }
                }));
            }
        };
        let images = staged.into_iter().map(|image| image.image).collect();
        Ok(CommandResult::Success(Json(
            PartitionTransactReadOutcome::Applied(images),
        )))
    }
}

/// Read a transaction batch through Cellule's read-only query path.
pub struct PartitionTransactReadQuery;

/// Compact read images with an explicit fallback for oversized aggregates.
#[derive(Clone, Debug, PartialEq)]
pub struct PartitionTransactReadQueryOutput(pub PartitionTransactReadOutcome);

impl WireValue for PartitionTransactReadQueryOutput {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        crate::encode_read_query(
            &self.0,
            match &self.0 {
                PartitionTransactReadOutcome::Applied(images) => Some(images),
                _ => None,
            },
            &PartitionTransactReadOutcome::SavedImagesRequired,
            encoder,
        )
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        Ok(Self(
            if let Some(images) = crate::decode_read_query_images(decoder)? {
                PartitionTransactReadOutcome::Applied(images)
            } else {
                let outcome = Json::<PartitionTransactReadOutcome>::decode(decoder)?.0;
                if matches!(outcome, PartitionTransactReadOutcome::Applied(_)) {
                    return Err(CodecError::Invalid("read images require compact envelope"));
                }
                outcome
            },
        ))
    }
}

impl Query for PartitionTransactReadQuery {
    const MODULE: &'static str = DATA_MODULE;
    const ID: u32 = 19;
    const CODEC_VERSION: u32 = 3;
    type Input = Json<PartitionTransactWriteInput>;
    type Output = PartitionTransactReadQueryOutput;

    fn execute(context: &mut QueryContext<'_>, Json(input): Self::Input) -> Result<Self::Output> {
        let rows = context.sql(&statement(
            "SELECT spec FROM ddb_partition WHERE singleton = 1",
            vec![],
        ))?;
        let Some(spec) = super::decode_spec(&rows[0])? else {
            return Ok(PartitionTransactReadQueryOutput(
                PartitionTransactReadOutcome::NotInstalled,
            ));
        };
        if spec.table.id != input.table_id || spec.epoch != input.epoch {
            return Ok(PartitionTransactReadQueryOutput(
                PartitionTransactReadOutcome::StaleRoute,
            ));
        }
        match super::query_access(context)? {
            AccessState::Serving => {}
            AccessState::Sealed => {
                return Ok(PartitionTransactReadQueryOutput(
                    PartitionTransactReadOutcome::Sealed,
                ));
            }
            AccessState::Importing => {
                return Ok(PartitionTransactReadQueryOutput(
                    PartitionTransactReadOutcome::NotReady,
                ));
            }
        }
        if input.operations.is_empty() || input.operations.len() > 100 {
            return Ok(PartitionTransactReadQueryOutput(
                PartitionTransactReadOutcome::Rejected {
                    index: 0,
                    reason: TransactionFailure::Validation(
                        "transaction operation count is outside 1..=100".into(),
                    ),
                },
            ));
        }
        let images = match query_stage_operations(context, &spec, input.operations)? {
            Ok(images) => images,
            Err(error) => {
                return Ok(PartitionTransactReadQueryOutput(match error {
                    StageError::StaleRoute => PartitionTransactReadOutcome::StaleRoute,
                    StageError::WrongPartition => PartitionTransactReadOutcome::WrongPartition,
                    StageError::Rejected { index, reason } => {
                        PartitionTransactReadOutcome::Rejected { index, reason }
                    }
                }));
            }
        };
        Ok(PartitionTransactReadQueryOutput(
            PartitionTransactReadOutcome::Applied(images),
        ))
    }
}

fn query_stage_operations(
    context: &mut QueryContext<'_>,
    spec: &PartitionSpec,
    operations: Vec<TransactionOperation>,
) -> Result<std::result::Result<Vec<Option<Item>>, StageError>> {
    let mut images = Vec::with_capacity(operations.len());
    let mut read_bytes = 0;
    for (index, operation) in operations.into_iter().enumerate() {
        let TransactionOperation::Read(input) = operation else {
            return Ok(Err(stage_validation(
                index,
                "read-only transaction contains a non-read operation",
            )));
        };
        if input.table_id != spec.table.id {
            return Ok(Err(StageError::StaleRoute));
        }
        if !valid_key(&input.key, &spec.table) {
            return Ok(Err(stage_validation(index, "item violates table schema")));
        }
        let key = item_key(&input.key, &spec.table.key_schema)?;
        if !spec.contains(data_key_hash(
            &spec.table.id,
            &input.key,
            &spec.table.key_schema,
        )?) {
            return Ok(Err(StageError::WrongPartition));
        }
        if read_key_conflict(context, &key)?.is_some() {
            return Ok(Err(StageError::Rejected {
                index,
                reason: TransactionFailure::Conflict,
            }));
        }
        let image =
            crate::item_storage::StoredValue::Partition(&key).read(|batch| context.sql(batch))?;
        read_bytes += image
            .as_ref()
            .map_or(0, extenddb_core::types::item_size_bytes);
        if read_bytes > 4 * 1024 * 1024 {
            return Ok(Err(stage_validation(
                index,
                "transaction read exceeds 4 MiB",
            )));
        }
        images.push(image);
    }
    Ok(Ok(images))
}

fn rejected_read(
    outcome: PartitionTransactReadOutcome,
) -> CommandResult<Json<PartitionTransactReadOutcome>> {
    CommandResult::Rejected(Json(outcome))
}

fn rejected(
    outcome: PartitionTransactWriteOutcome,
) -> CommandResult<Json<PartitionTransactWriteOutcome>> {
    CommandResult::Rejected(Json(outcome))
}

fn validation(index: usize, message: &str) -> CommandResult<Json<PartitionTransactWriteOutcome>> {
    rejected(PartitionTransactWriteOutcome::Rejected {
        index,
        reason: TransactionFailure::Validation(message.into()),
    })
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct StagedImage {
    key: Vec<u8>,
    partition_key: Vec<u8>,
    sort_key: Vec<u8>,
    image: Option<Item>,
    effect: StagedEffect,
    index_capacity: crate::secondary_index::Capacity,
}

enum StageError {
    StaleRoute,
    WrongPartition,
    Rejected {
        index: usize,
        reason: TransactionFailure,
    },
}

impl StageError {
    fn single_outcome(self, strip_old_images: bool) -> PartitionTransactWriteOutcome {
        match self {
            Self::StaleRoute => PartitionTransactWriteOutcome::StaleRoute,
            Self::WrongPartition => PartitionTransactWriteOutcome::WrongPartition,
            Self::Rejected { index, reason } => PartitionTransactWriteOutcome::Rejected {
                index,
                reason: if strip_old_images {
                    without_old_image(reason)
                } else {
                    reason
                },
            },
        }
    }

    fn prepare_outcome(self) -> PrepareTransactionOutcome {
        match self {
            Self::StaleRoute => PrepareTransactionOutcome::StaleRoute,
            Self::WrongPartition => PrepareTransactionOutcome::WrongPartition,
            Self::Rejected { index, reason } => {
                PrepareTransactionOutcome::Rejected { index, reason }
            }
        }
    }
}

fn without_old_image(reason: TransactionFailure) -> TransactionFailure {
    match reason {
        TransactionFailure::ConditionFailed(_) => TransactionFailure::ConditionFailed(None),
        reason => reason,
    }
}

fn stage_operations(
    context: &mut CommandContext<'_, '_>,
    spec: &PartitionSpec,
    operations: Vec<TransactionOperation>,
) -> Result<std::result::Result<Vec<StagedImage>, StageError>> {
    let mut touched = HashSet::with_capacity(operations.len());
    let mut staged = Vec::with_capacity(operations.len());
    let mut read_bytes = 0;
    for (index, operation) in operations.into_iter().enumerate() {
        let shared = matches!(operation, TransactionOperation::Read(_));
        let (table_id, item, condition) = match &operation {
            TransactionOperation::Read(input) => (&input.table_id, &input.key, None),
            TransactionOperation::Put(input) => {
                (&input.table_id, &input.item, input.condition.as_ref())
            }
            TransactionOperation::Delete(input) => {
                (&input.table_id, &input.key, input.condition.as_ref())
            }
            TransactionOperation::Update(input) => {
                (&input.table_id, &input.key, input.condition.as_ref())
            }
            TransactionOperation::ConditionCheck(input) => {
                (&input.table_id, &input.key, Some(&input.condition))
            }
        };
        if *table_id != spec.table.id {
            return Ok(Err(StageError::StaleRoute));
        }
        let valid = match operation {
            TransactionOperation::Put(_) => valid_item(item, &spec.table),
            _ => valid_key(item, &spec.table),
        };
        if !valid {
            return Ok(Err(stage_validation(index, "item violates table schema")));
        }
        let key = item_key(item, &spec.table.key_schema)?;
        if !spec.contains(data_key_hash(&spec.table.id, item, &spec.table.key_schema)?) {
            return Ok(Err(StageError::WrongPartition));
        }
        if !touched.insert(key.clone()) && !shared {
            return Ok(Err(stage_validation(
                index,
                "more than one operation addresses the same item",
            )));
        }
        if !context.sql(&lock_query(&key, shared))?[0].rows.is_empty() {
            return Ok(Err(StageError::Rejected {
                index,
                reason: TransactionFailure::Conflict,
            }));
        }
        let old = command_item(context, &key)?;
        if let Some(condition) = condition {
            let empty = Item::new();
            match condition.evaluate(old.as_ref().unwrap_or(&empty)) {
                Ok(true) => {}
                Ok(false) => {
                    return Ok(Err(StageError::Rejected {
                        index,
                        reason: TransactionFailure::ConditionFailed(old),
                    }));
                }
                Err(reason) => return Ok(Err(stage_validation(index, &reason))),
            }
        }
        let (partition_key, sort_key) = super::key::index_key(item, &spec.table.key_schema)?;
        let old_bytes = crate::global_index::outbox::old_bytes(&spec.table, old.as_ref())?;
        let (image, effect) = match operation {
            TransactionOperation::Put(input) => (Some(input.item), StagedEffect::Write),
            TransactionOperation::Delete(_) => (None, StagedEffect::Write),
            TransactionOperation::Update(input) => {
                let mut new = old.unwrap_or(input.key);
                if let Err(reason) = input
                    .update
                    .apply(&mut new, &spec.table.attribute_definitions)
                {
                    return Ok(Err(stage_validation(index, &reason)));
                }
                if !valid_item(&new, &spec.table) || item_key(&new, &spec.table.key_schema)? != key
                {
                    return Ok(Err(stage_validation(index, "item violates table schema")));
                }
                (Some(new), StagedEffect::Write)
            }
            TransactionOperation::ConditionCheck(_) => (None, StagedEffect::Check),
            TransactionOperation::Read(_) => (old, StagedEffect::Read),
        };
        if effect == StagedEffect::Read {
            read_bytes += image
                .as_ref()
                .map_or(0, extenddb_core::types::item_size_bytes);
            if read_bytes > 4 * 1024 * 1024 {
                return Ok(Err(stage_validation(
                    index,
                    "transaction read exceeds 4 MiB",
                )));
            }
        }
        let index_capacity = if effect == StagedEffect::Write {
            let mut capacity = crate::secondary_index::capacity(&spec.table, image.as_ref())?;
            crate::global_index::outbox::reserve(
                &mut capacity,
                &spec.table,
                old_bytes,
                image.as_ref(),
            )?;
            capacity
        } else {
            crate::secondary_index::Capacity::default()
        };
        staged.push(StagedImage {
            index_capacity,
            key,
            partition_key,
            sort_key,
            image,
            effect,
        });
    }
    Ok(Ok(staged))
}

fn stage_validation(index: usize, message: &str) -> StageError {
    StageError::Rejected {
        index,
        reason: TransactionFailure::Validation(message.into()),
    }
}

fn apply_staged(
    context: &mut CommandContext<'_, '_>,
    table: &crate::TableRecord,
    epoch: u64,
    staged: Vec<StagedImage>,
) -> Result<()> {
    for (ordinal, image) in staged.into_iter().enumerate() {
        if image.effect != StagedEffect::Write {
            continue;
        }
        let old = if table.stream.is_some() {
            command_item(context, &image.key)?
        } else {
            None
        };
        if let Some(item) = image.image.as_ref() {
            write_item(context, image.key.clone(), item, table, Some(epoch))?;
        } else {
            super::delete_item(context, table, &image.key, epoch)?;
        }
        crate::stream_journal::append(
            context,
            &table.id,
            &table.key_schema,
            table.stream.as_ref(),
            old.as_ref(),
            image.image.as_ref(),
            ordinal,
        )?;
    }
    Ok(())
}

mod participant;
pub use participant::*;

pub(super) fn key_locked(context: &mut CommandContext<'_, '_>, key: &[u8]) -> Result<bool> {
    Ok(!context.sql(&lock_query(key, false))?[0].rows.is_empty())
}

fn lock_query(key: &[u8], write_only: bool) -> crate::SqlBatch {
    statement(
        if write_only {
            "SELECT transaction_id FROM ddb_partition_transaction_locks WHERE item_key = ?1 AND write_lock = 1 LIMIT 1"
        } else {
            "SELECT 1 FROM ddb_partition_transaction_locks WHERE item_key = ?1"
        },
        vec![SqlValue::Blob(key.to_vec())],
    )
}

pub(super) fn has_transaction_locks(context: &mut CommandContext<'_, '_>) -> Result<bool> {
    let rows = context.sql(&statement(
        "SELECT 1 FROM ddb_partition_transaction_locks LIMIT 1",
        vec![],
    ))?;
    Ok(!rows[0].rows.is_empty())
}

pub(super) fn read_key_conflict(
    context: &QueryContext<'_>,
    key: &[u8],
) -> Result<Option<crate::TransactionReadConflict>> {
    crate::participant::read_conflict(context, &context.sql(&lock_query(key, true))?[0])
}
