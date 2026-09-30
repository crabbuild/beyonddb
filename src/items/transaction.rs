//! Account participant staging and the same-Cell transaction path.

use super::*;
use crate::participant::StagedEffect;
use crate::participant::{
    self, ParticipantTransactionState, PrepareTransactionOutcome, ReadTransactionInput,
    ResolveTransactionInput, ResolveTransactionOutcome,
};

#[derive(Serialize, Deserialize)]
struct StagedImage {
    table_id: String,
    key: Vec<u8>,
    image: Option<Item>,
    effect: StagedEffect,
    index_capacity: crate::secondary_index::Capacity,
}

fn stage(
    context: &mut CommandContext<'_, '_>,
    operations: Vec<TransactionOperation>,
) -> Result<std::result::Result<Vec<StagedImage>, (usize, TransactionFailure)>> {
    if operations.is_empty() || operations.len() > 100 {
        return Ok(Err((
            0,
            TransactionFailure::Validation("transaction operation count is outside 1..=100".into()),
        )));
    }
    let mut touched = HashSet::with_capacity(operations.len());
    let mut staged = Vec::with_capacity(operations.len());
    let mut read_bytes = 0;
    for (index, operation) in operations.into_iter().enumerate() {
        let invalid = |message: &str| (index, TransactionFailure::Validation(message.into()));
        let shared = matches!(operation, TransactionOperation::Read(_));
        let (name, table_id, item, condition) = match &operation {
            TransactionOperation::Read(input) => {
                (&input.table_name, &input.table_id, &input.key, None)
            }
            TransactionOperation::Put(input) => (
                &input.table_name,
                &input.table_id,
                &input.item,
                input.condition.as_ref(),
            ),
            TransactionOperation::Delete(input) => (
                &input.table_name,
                &input.table_id,
                &input.key,
                input.condition.as_ref(),
            ),
            TransactionOperation::Update(input) => (
                &input.table_name,
                &input.table_id,
                &input.key,
                input.condition.as_ref(),
            ),
            TransactionOperation::ConditionCheck(input) => (
                &input.table_name,
                &input.table_id,
                &input.key,
                Some(&input.condition),
            ),
        };
        let Some(table) = command_unrouted_table(context, name)? else {
            return Ok(Err(invalid("table does not exist or has a data route")));
        };
        if table.id != *table_id {
            return Ok(Err(invalid("table identity is stale")));
        }
        let valid = match operation {
            TransactionOperation::Put(_) => valid_item(item, &table),
            _ => valid_key(item, &table),
        };
        if !valid {
            return Ok(Err(invalid("item violates table schema")));
        }
        let key = item_key(item, &table.key_schema)?;
        if !touched.insert((table.id.clone(), key.clone())) && !shared {
            return Ok(Err(invalid(
                "more than one operation addresses the same item",
            )));
        }
        if !context.sql(&lock_query(&table.id, &key, shared))?[0]
            .rows
            .is_empty()
        {
            return Ok(Err((index, TransactionFailure::Conflict)));
        }
        let old = command_item(context, &table.id, &key)?;
        if let Some(condition) = condition {
            let empty = Item::new();
            match condition.evaluate(old.as_ref().unwrap_or(&empty)) {
                Ok(true) => {}
                Ok(false) => return Ok(Err((index, TransactionFailure::ConditionFailed(old)))),
                Err(reason) => return Ok(Err(invalid(&reason))),
            }
        }
        let old_bytes = crate::global_index::outbox::old_bytes(&table, old.as_ref())?;
        let (image, effect) = match operation {
            TransactionOperation::Put(input) => (Some(input.item), StagedEffect::Write),
            TransactionOperation::Delete(_) => (None, StagedEffect::Write),
            TransactionOperation::Update(input) => {
                let mut new = old.unwrap_or(input.key);
                if let Err(reason) = input.update.apply(&mut new, &table.attribute_definitions) {
                    return Ok(Err(invalid(&reason)));
                }
                if !valid_item(&new, &table) || item_key(&new, &table.key_schema)? != key {
                    return Ok(Err(invalid("item violates table schema")));
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
                return Ok(Err(invalid("transaction read exceeds 4 MiB")));
            }
        }
        let index_capacity = if effect == StagedEffect::Write {
            let mut capacity = crate::secondary_index::capacity(&table, image.as_ref())?;
            crate::global_index::outbox::reserve(&mut capacity, &table, old_bytes, image.as_ref())?;
            capacity
        } else {
            crate::secondary_index::Capacity::default()
        };
        staged.push(StagedImage {
            index_capacity,
            table_id: table.id,
            key,
            image,
            effect,
        });
    }
    Ok(Ok(staged))
}

fn apply(context: &mut CommandContext<'_, '_>, staged: Vec<StagedImage>) -> Result<()> {
    for (ordinal, image) in staged.into_iter().enumerate() {
        if image.effect != StagedEffect::Write {
            continue;
        }
        let table = crate::table::decode_table(
            &context.sql(&statement(
                "SELECT record FROM ddb_live_tables WHERE table_id = ?1",
                vec![SqlValue::Text(image.table_id.clone())],
            ))?[0],
        )?
        .ok_or(Error::Command("prepared table is missing"))?;
        let old = if table.stream.is_some() {
            command_item(context, &table.id, &image.key)?
        } else {
            None
        };
        if let Some(item) = image.image.as_ref() {
            write_item(context, &table, &image.key, item)?;
        } else {
            delete_item(context, &table, &image.key)?;
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

pub(super) fn write(
    context: &mut CommandContext<'_, '_>,
    operations: Vec<TransactionOperation>,
) -> Result<TransactionOutcome> {
    let staged = match stage(context, operations)? {
        Ok(staged) => staged,
        Err((index, reason)) => return Ok(TransactionOutcome::Rejected { index, reason }),
    };
    apply(context, staged)?;
    Ok(TransactionOutcome::Applied)
}

pub(super) fn write_without_old_images(
    context: &mut CommandContext<'_, '_>,
    operations: Vec<TransactionOperation>,
) -> Result<TransactionOutcome> {
    Ok(match write(context, operations)? {
        TransactionOutcome::Applied => TransactionOutcome::Applied,
        TransactionOutcome::Rejected { index, reason } => TransactionOutcome::Rejected {
            index,
            reason: without_old_image(reason),
        },
    })
}

pub(super) fn without_old_image(reason: TransactionFailure) -> TransactionFailure {
    match reason {
        TransactionFailure::ConditionFailed(_) => TransactionFailure::ConditionFailed(None),
        reason => reason,
    }
}

/// Result of an atomic read batch confined to one account Cell.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TransactionReadOutcome {
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
}

/// Read a transaction batch atomically when every item belongs to one account Cell.
pub struct TransactRead;

impl Command for TransactRead {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 52;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<TransactWriteInput>;
    type Output = Json<TransactionReadOutcome>;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(input): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        let staged = match stage(context, input.operations)? {
            Ok(staged) => staged,
            Err((index, reason)) => {
                return Ok(CommandResult::Rejected(Json(
                    TransactionReadOutcome::Rejected { index, reason },
                )));
            }
        };
        let images = staged.into_iter().map(|image| image.image).collect();
        Ok(CommandResult::Success(Json(
            TransactionReadOutcome::Applied(images),
        )))
    }
}

/// Read a transaction batch through Cellule's read-only query path.
///
/// The query callback runs on the Cell worker, so all item and lock reads are
/// serialized with commands while avoiding a durable mutation publication.
pub struct TransactReadQuery;

/// Compact read images with an explicit fallback for oversized aggregates.
#[derive(Clone, Debug, PartialEq)]
pub struct TransactionReadQueryOutput(pub TransactionReadOutcome);

impl WireValue for TransactionReadQueryOutput {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        encode_read_query(
            &self.0,
            match &self.0 {
                TransactionReadOutcome::Applied(images) => Some(images),
                _ => None,
            },
            &TransactionReadOutcome::SavedImagesRequired,
            encoder,
        )
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        Ok(Self(
            if let Some(images) = crate::decode_read_query_images(decoder)? {
                TransactionReadOutcome::Applied(images)
            } else {
                let outcome = Json::<TransactionReadOutcome>::decode(decoder)?.0;
                if matches!(outcome, TransactionReadOutcome::Applied(_)) {
                    return Err(CodecError::Invalid("read images require compact envelope"));
                }
                outcome
            },
        ))
    }
}

impl Query for TransactReadQuery {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 54;
    const CODEC_VERSION: u32 = 3;
    type Input = Json<TransactWriteInput>;
    type Output = TransactionReadQueryOutput;

    fn execute(context: &mut QueryContext<'_>, Json(input): Self::Input) -> Result<Self::Output> {
        let images = match query_stage(context, input.operations)? {
            Ok(images) => images,
            Err((index, reason)) => {
                return Ok(TransactionReadQueryOutput(
                    TransactionReadOutcome::Rejected { index, reason },
                ));
            }
        };
        Ok(TransactionReadQueryOutput(TransactionReadOutcome::Applied(
            images,
        )))
    }
}

type ReadStage = std::result::Result<Vec<Option<Item>>, (usize, TransactionFailure)>;

fn query_stage(
    context: &mut QueryContext<'_>,
    operations: Vec<TransactionOperation>,
) -> Result<ReadStage> {
    if operations.is_empty() || operations.len() > 100 {
        return Ok(Err((
            0,
            TransactionFailure::Validation("transaction operation count is outside 1..=100".into()),
        )));
    }
    let mut images = Vec::with_capacity(operations.len());
    let mut read_bytes = 0;
    for (index, operation) in operations.into_iter().enumerate() {
        let TransactionOperation::Read(input) = operation else {
            return Ok(Err((
                index,
                TransactionFailure::Validation(
                    "read-only transaction contains a non-read operation".into(),
                ),
            )));
        };
        let invalid = |message: &str| (index, TransactionFailure::Validation(message.into()));
        let Some(table) = query_unrouted_table(context, &input.table_name)? else {
            return Ok(Err(invalid("table does not exist or has a data route")));
        };
        if table.id != input.table_id {
            return Ok(Err(invalid("table identity is stale")));
        }
        if !valid_key(&input.key, &table) {
            return Ok(Err(invalid("item violates table schema")));
        }
        let key = item_key(&input.key, &table.key_schema)?;
        if read_key_conflict(context, &table.id, &key)?.is_some() {
            return Ok(Err((index, TransactionFailure::Conflict)));
        }
        let image = crate::item_storage::StoredValue::Account {
            table_id: &table.id,
            key: &key,
        }
        .read(|batch| context.sql(batch))?;
        read_bytes += image
            .as_ref()
            .map_or(0, extenddb_core::types::item_size_bytes);
        if read_bytes > 4 * 1024 * 1024 {
            return Ok(Err(invalid("transaction read exceeds 4 MiB")));
        }
        images.push(image);
    }
    Ok(Ok(images))
}

/// Prepare read or write operations on unrouted tables in one account Cell.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PrepareAccountTransactionInput {
    pub transaction_id: [u8; 16],
    pub coordinator_cell: [u8; 32],
    pub coordinator_key: Vec<u8>,
    pub operations: Vec<TransactionOperation>,
}

/// Persist account item images and locks without exposing any writes.
pub struct PrepareAccountTransaction;

impl crate::MultipartTransactionCommand for PrepareAccountTransaction {
    type Payload = PrepareAccountTransactionInput;
    const UPLOAD_COMMAND_ID: u32 = 23;
}

impl Command for PrepareAccountTransaction {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 21;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<crate::TransactionCommandInput<PrepareAccountTransactionInput>>;
    type Output = Json<PrepareTransactionOutcome>;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(input): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        let input = crate::transaction_transport::consume::<Self>(context, input)?;
        let digest = blake3::hash(&serde_json::to_vec(&input)?);
        if let Some(outcome) = participant::prepared(
            context,
            input.transaction_id,
            input.coordinator_cell,
            digest,
        )? {
            return Ok(CommandResult::Rejected(Json(outcome)));
        }
        let staged = match stage(context, input.operations)? {
            Ok(staged) => staged,
            Err((index, reason)) => {
                return Ok(CommandResult::Rejected(Json(
                    PrepareTransactionOutcome::Rejected { index, reason },
                )));
            }
        };
        participant::record_prepare(
            context,
            input.transaction_id,
            input.coordinator_cell,
            digest,
            crate::participant::PreparedPayload {
                bytes: serde_json::to_vec(&staged)?,
                operations: staged.len(),
                index_edits: staged.iter().map(|image| image.index_capacity.edits).sum(),
                index_overflow_bytes: staged
                    .iter()
                    .map(|image| image.index_capacity.overflow_bytes)
                    .sum(),
            },
            &input.coordinator_key,
            staged
                .iter()
                .filter(|image| image.effect == StagedEffect::Read)
                .map(|image| image.image.as_ref()),
        )?;
        for image in staged {
            context.sql(&statement("INSERT OR IGNORE INTO ddb_account_transaction_locks (table_id, item_key, transaction_id, write_lock) VALUES (?1, ?2, ?3, ?4)",
                vec![SqlValue::Text(image.table_id), SqlValue::Blob(image.key), SqlValue::Blob(input.transaction_id.to_vec()), SqlValue::Integer(i64::from(image.effect != StagedEffect::Read))]))?;
        }
        Ok(CommandResult::Success(Json(
            PrepareTransactionOutcome::Prepared,
        )))
    }
}

/// Apply a coordinator decision and release the account participant's locks atomically.
pub struct ResolveAccountTransaction;

impl Command for ResolveAccountTransaction {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 22;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<ResolveTransactionInput>;
    type Output = Json<ResolveTransactionOutcome>;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(input): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        participant::resolve(context, input.clone(), |context, staged| {
            if let Some(bytes) = staged {
                apply(context, serde_json::from_slice(bytes)?)?;
            }
            context.sql(&statement(
                "DELETE FROM ddb_account_transaction_locks WHERE transaction_id = ?1",
                vec![SqlValue::Blob(input.transaction_id.to_vec())],
            ))?;
            Ok(())
        })
    }
}

/// Read the durable account participant state after an uncertain phase reply.
pub struct ReadAccountTransaction;

impl Query for ReadAccountTransaction {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 25;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<ReadTransactionInput>;
    type Output = Json<ParticipantTransactionState>;

    fn execute(context: &mut QueryContext<'_>, Json(input): Self::Input) -> Result<Self::Output> {
        participant::read(context, input)
    }
}

pub(super) fn key_locked(
    context: &CommandContext<'_, '_>,
    table_id: &str,
    key: &[u8],
) -> Result<bool> {
    Ok(!context.sql(&lock_query(table_id, key, false))?[0]
        .rows
        .is_empty())
}

pub(super) fn read_key_conflict(
    context: &QueryContext<'_>,
    table_id: &str,
    key: &[u8],
) -> Result<Option<crate::TransactionReadConflict>> {
    crate::participant::read_conflict(context, &context.sql(&lock_query(table_id, key, true))?[0])
}

fn lock_query(table_id: &str, key: &[u8], write_only: bool) -> SqlBatch {
    statement(
        if write_only {
            "SELECT transaction_id FROM ddb_account_transaction_locks WHERE table_id = ?1 AND item_key = ?2 AND write_lock = 1 LIMIT 1"
        } else {
            "SELECT 1 FROM ddb_account_transaction_locks WHERE table_id = ?1 AND item_key = ?2"
        },
        vec![
            SqlValue::Text(table_id.to_owned()),
            SqlValue::Blob(key.to_vec()),
        ],
    )
}

pub(crate) fn table_locked(context: &CommandContext<'_, '_>, table_id: &str) -> Result<bool> {
    Ok(!context.sql(&statement(
        "SELECT 1 FROM ddb_account_transaction_locks WHERE table_id = ?1 LIMIT 1",
        vec![SqlValue::Text(table_id.to_owned())],
    ))?[0]
        .rows
        .is_empty())
}

/// Read one committed immutable image after a transactional read releases locks.
pub struct ReadAccountTransactionResult;
impl Query for ReadAccountTransactionResult {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 27;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<crate::ReadTransactionResultInput>;
    type Output = Json<crate::TransactionReadResult>;
    fn execute(context: &mut QueryContext<'_>, Json(input): Self::Input) -> Result<Self::Output> {
        crate::participant::read_result(context, input)
    }
}

/// Release assembled read images while retaining the account's terminal decision.
pub struct ReleaseAccountTransactionReads;
impl Command for ReleaseAccountTransactionReads {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 33;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<ReadTransactionInput>;
    type Output = Json<bool>;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(input): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        crate::participant::release_read_result(context, input)
    }
}
