//! Account-scoped DynamoDB state hosted by Cellule.

mod authorization;
mod backend;
mod catalog;
mod credentials;
mod directory;
mod expression_wire;
mod global_index;
mod item_storage;
mod item_wire;
mod items;
mod participant;
mod partition;
mod provision;
mod routing;
mod secondary_index;
mod server;
mod split;
mod statistics;
mod stream_journal;
mod stream_retention;
mod table;
mod tags;
mod transaction_coordinator;
mod transaction_payload;
mod transaction_token;
mod transaction_transport;
mod ttl;
pub use directory::*;

pub use expression_wire::WireCondition;
pub use global_index::*;
pub use items::*;
pub use participant::{
    ParticipantTransactionState, PrepareTransactionOutcome, ReadTransactionInput,
    ReadTransactionResultInput, ResolveTransactionInput, ResolveTransactionOutcome,
    TransactionReadConflict, TransactionReadResult,
};
pub use partition::*;
pub use provision::*;
pub use routing::*;
pub use server::{
    BeyonddbPeerScope, BeyonddbPeers, NodeLeasePublisher, PeerNodeDurabilityProvider,
    PeerNodeLogTransport, PublishedNodeLease, PublishedNodeLogAuthority, build_http_state,
    build_http_state_with_cache, measured_node_capacity, recover_fenced_node_log,
    shutdown_serving_node,
};
pub use split::*;
pub use stream_journal::{
    ListStreamCatalog, ListStreamCatalogInput, ListStreamCatalogPage, ReadAccountStreamJournal,
    ReadAccountStreamTail, ReadPartitionStreamJournal, ReadPartitionStreamTail, ReadStreamCatalog,
    StreamCatalogEntry, StreamCatalogKey, StreamConfig, StreamJournalInput, StreamJournalOutcome,
    StreamTailInput, StreamTailOutcome,
};
pub use stream_retention::{
    AdvanceStreamGcShard, HasExpiredAccountStreamRecords, HasExpiredPartitionStreamRecords,
    PruneAccountStreamRecords, PrunePartitionStreamRecords, ReadStreamGcShard,
};
pub use table::*;
pub use transaction_coordinator::*;
pub use transaction_token::TransactionToken;
pub use transaction_transport::{
    MultipartTransactionCommand, TransactionCommandInput, TransactionPayloadChunk,
    TransactionPayloadRef, UploadTransactionPayload,
};
pub use ttl::*;

pub use authorization::CellAuthorizationStore;
pub use backend::{CellStorage, CoordinatorProvisioner, InitialPartitionProvisioner};
pub use catalog::CellCatalogStore;
pub use credentials::{CellCredentialStore, credential_target, initialize_credentials};

use std::{collections::HashSet, sync::OnceLock};

use cellule_app::{ApplicationBuilder, CellApplication, CellType};
use cellule_runtime::cell::catalog::CatalogRole;
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};
use cellule_runtime::identity::{ApplicationId, CellTarget, Digest, NamespaceId, TenantId};
use cellule_runtime::primitives::sql::{SqlBatch, SqlResultSet, SqlStatement, SqlValue};
use cellule_runtime::registry::{
    Command, CommandContext, CommandResult, MigrationDescriptor, ModuleDescriptor,
    NamespaceDescriptor, OperationDescriptor, Query, QueryContext, RegistryBuilder,
};
use cellule_runtime::{Error, Result, partition_for_shard};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, Item, KeySchemaElement,
    ProvisionedThroughput, extract_key,
};
use extenddb_core::validation;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

const MODULE: &str = "beyonddb-account";
const NAMESPACE: NamespaceId = NamespaceId::from_bytes([0x42; 16]);
const DATA_MODULE: &str = "beyonddb-data";
const DATA_NAMESPACE: NamespaceId = NamespaceId::from_bytes([0x43; 16]);
const APPLICATION: ApplicationId = ApplicationId::from_bytes([0x42; 16]);
/// Stable Cell application identity for BeyondDB storage layouts.
pub const APPLICATION_ID: ApplicationId = APPLICATION;
static SCHEMA: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        cellule_runtime::primitives::capacity::SCHEMA,
        participant::SCHEMA,
        secondary_index::SCHEMA,
        global_index::outbox::SCHEMA,
        stream_journal::SCHEMA,
        include_str!("schema.sql")
    )
});
const OPERATION_BYTES: u32 = 4 * 1024 * 1024 + 64 * 1024;
// Unconditional no-return mutations do not carry item images in their result.
// Keep their mailbox reservation small so concurrent writes are not limited by
// the generic 4 MiB command envelope.
const NO_RETURN_INPUT_BYTES: u32 = 1024 * 1024;
const NO_RETURN_OUTPUT_BYTES: u32 = 64 * 1024;
// A transaction write can return one failed item's old image, but never a
// successful item list. Keep the result envelope below the generic operation
// bound while retaining the full 4 MiB input budget for up to 100 operations.
const TRANSACTION_WRITE_OUTPUT_BYTES: u32 = 1024 * 1024;

static NAMESPACES: [NamespaceDescriptor; 1] = [NamespaceDescriptor {
    id: NAMESPACE,
    name: MODULE,
    role: CatalogRole::Sql,
    shards: 1,
    effect_targets: &[],
    dead_letter: None,
}];

const fn operation(id: u32) -> OperationDescriptor {
    OperationDescriptor {
        id,
        codec_version: 1,
        schema_min: 1,
        schema_max: 1,
        input_limit: OPERATION_BYTES,
        output_limit: OPERATION_BYTES,
    }
}

const fn no_return_operation(id: u32) -> OperationDescriptor {
    OperationDescriptor {
        id,
        codec_version: 1,
        schema_min: 1,
        schema_max: 1,
        input_limit: NO_RETURN_INPUT_BYTES,
        output_limit: NO_RETURN_OUTPUT_BYTES,
    }
}

const fn transaction_write_operation(id: u32) -> OperationDescriptor {
    OperationDescriptor {
        id,
        codec_version: 1,
        schema_min: 1,
        schema_max: 1,
        input_limit: OPERATION_BYTES,
        output_limit: TRANSACTION_WRITE_OUTPUT_BYTES,
    }
}

const fn no_return_transaction_operation(id: u32) -> OperationDescriptor {
    OperationDescriptor {
        id,
        codec_version: 1,
        schema_min: 1,
        schema_max: 1,
        input_limit: OPERATION_BYTES,
        output_limit: NO_RETURN_OUTPUT_BYTES,
    }
}

static COMMANDS: [OperationDescriptor; 33] = [
    operation(1),
    operation(2),
    operation(3),
    transaction_write_operation(5),
    operation(7),
    operation(8),
    operation(9),
    OperationDescriptor {
        codec_version: 2,
        ..operation(10)
    },
    operation(13),
    operation(14),
    operation(15),
    operation(17),
    operation(18),
    operation(19),
    operation(20),
    operation(21),
    participant::phase_operation(22),
    crate::transaction_transport::upload_operation(23),
    OperationDescriptor {
        codec_version: 2,
        input_limit: 64 * 1024,
        ..participant::phase_operation(24)
    },
    OperationDescriptor {
        codec_version: 2,
        ..operation(25)
    },
    OperationDescriptor {
        codec_version: 2,
        ..participant::phase_operation(26)
    },
    OperationDescriptor {
        codec_version: 3,
        ..operation(31)
    },
    operation(32),
    participant::phase_operation(33),
    OperationDescriptor {
        codec_version: 2,
        ..participant::phase_operation(34)
    },
    operation(43),
    operation(44),
    operation(45),
    no_return_operation(50),
    no_return_operation(51),
    operation(52),
    no_return_transaction_operation(53),
    no_return_operation(55),
];
static QUERIES: [OperationDescriptor; 32] = [
    operation(4),
    operation(7),
    OperationDescriptor {
        codec_version: 2,
        // ListTables returns at most 100 names of at most 255 bytes each.
        // The worker queries this account Cell on every projection sweep.
        output_limit: 64 * 1024,
        ..operation(8)
    },
    operation(9),
    operation(10),
    operation(13),
    operation(18),
    operation(20),
    operation(21),
    operation(22),
    operation(23),
    OperationDescriptor {
        // A discovery page contains at most 100 fixed-size shard records.
        // Reserve for the page rather than a full item-operation result.
        output_limit: 128 * 1024,
        ..operation(24)
    },
    participant::phase_operation(25),
    operation(26),
    operation(27),
    operation(28),
    OperationDescriptor {
        codec_version: 3,
        // This query returns only an optional root spec, never item data.
        // Reserving the generic 4 MiB result budget for every route lookup
        // needlessly fills the account Cell mailbox under concurrent reads.
        output_limit: 4 * 1024,
        ..operation(29)
    },
    participant::phase_operation(30),
    global_index::outbox::chunk_operation(31),
    operation(36),
    operation(37),
    operation(38),
    participant::phase_operation(39),
    operation(42),
    operation(43),
    operation(44),
    operation(45),
    operation(46),
    operation(47),
    operation(48),
    operation(49),
    OperationDescriptor {
        codec_version: 3,
        ..operation(54)
    },
];

/// Statically linked account application.
pub struct Beyonddb;

impl CellApplication for Beyonddb {
    const NAME: &'static str = "beyonddb";

    fn register(builder: &mut ApplicationBuilder) -> Result<()> {
        builder.register(AccountModule)?;
        builder.register(directory::DirectoryModule)?;
        builder.register(partition::DataModule)?;
        builder.register(global_index::GlobalIndexModule)?;
        builder.register(transaction_coordinator::CoordinatorModule)?;
        builder.register(credentials::CredentialModule)?;
        builder.cell_type(
            CellType::new(MODULE, MODULE, NAMESPACE, CatalogRole::Sql, 1)?
                .with_limits(512 * 1024 * 1024, 64 * 1024 * 1024)?,
        )?;
        builder.cell_type(data_cell_type()?)?;
        builder.cell_type(directory::cell_type()?)?;
        builder.cell_type(global_index::cell_type()?)?;
        builder.cell_type(transaction_coordinator::cell_type()?)?;
        builder.cell_type(credentials::cell_type()?)
    }
}

fn data_cell_type() -> Result<CellType> {
    CellType::new(
        DATA_MODULE,
        DATA_MODULE,
        DATA_NAMESPACE,
        CatalogRole::Sql,
        1,
    )?
    .with_entity_partitions()?
    .with_limits(512 * 1024 * 1024, 64 * 1024 * 1024)
}

fn data_partition_bytes(table_id: &str, partition_id: &[u8; 16]) -> Result<[u8; 33]> {
    let parsed = blake3::Hash::from_hex(table_id)
        .map_err(|_| Error::Identity("invalid BeyondDB table ID"))?;
    if parsed.to_hex().to_string() != table_id {
        return Err(Error::Identity("noncanonical BeyondDB table ID"));
    }
    let mut scope = Vec::with_capacity(80);
    scope.extend_from_slice(table_id.as_bytes());
    scope.extend_from_slice(partition_id);
    data_cell_type()?.entity_partition(&scope)
}

/// Resolves an account table range to its independently owned data Cell.
pub fn data_target(
    account_id: &str,
    table_id: &str,
    partition_id: &[u8; 16],
) -> Result<CellTarget> {
    let account = account_target(account_id)?;
    let parsed = blake3::Hash::from_hex(table_id)
        .map_err(|_| Error::Identity("invalid BeyondDB table ID"))?;
    if &parsed.as_bytes()[..16] != account.tenant().as_bytes() {
        return Err(Error::Identity("table does not belong to account"));
    }
    CellTarget::new(
        account.tenant(),
        APPLICATION,
        DATA_NAMESPACE,
        &data_partition_bytes(table_id, partition_id)?,
    )
}

/// Resolves an ExtendDB account to its sole account Cell.
pub fn account_target(account_id: &str) -> Result<CellTarget> {
    if account_id.is_empty() || account_id.len() > 128 {
        return Err(Error::Identity("invalid ExtendDB account ID"));
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"beyonddb.account.v1\0");
    hasher.update(account_id.as_bytes());
    let hash = hasher.finalize();
    let mut tenant = [0_u8; 16];
    tenant.copy_from_slice(&hash.as_bytes()[..16]);
    account_for_tenant(TenantId::from_bytes(tenant))
}

fn account_for_tenant(tenant: TenantId) -> Result<CellTarget> {
    CellTarget::new(tenant, APPLICATION, NAMESPACE, &partition_for_shard(0))
}

/// Installs the initial account schema during Cell bootstrap.
pub fn initialize_account(transaction: &cellule_ltx::rusqlite::Transaction<'_>) -> Result<()> {
    transaction.execute_batch(&SCHEMA)?;
    Ok(())
}

/// Installs a data partition's SQL schema during Cell bootstrap.
pub fn initialize_partition(transaction: &cellule_ltx::rusqlite::Transaction<'_>) -> Result<()> {
    transaction.execute_batch(&partition::SCHEMA)?;
    Ok(())
}

struct AccountModule;

impl cellule_runtime::registry::CellModule for AccountModule {
    const NAME: &'static str = MODULE;

    fn descriptor(&self) -> &'static ModuleDescriptor {
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: MODULE,
            source_digest: {
                let mut source = blake3::Hasher::new();
                source.update(include_bytes!("lib.rs"));
                source.update(include_bytes!("statistics.rs"));
                source.update(include_bytes!("stream_journal.rs"));
                source.update(include_bytes!("stream_retention.rs"));
                source.update(include_bytes!("table.rs"));
                source.update(include_bytes!("table/deletion.rs"));
                source.update(include_bytes!("global_index.rs"));
                source.update(include_bytes!("global_index/outbox.rs"));
                source.update(include_bytes!("global_index/routing.rs"));
                source.update(include_bytes!("directory/transfer.rs"));
                source.update(include_bytes!("global_index/transfer.rs"));
                source.update(include_bytes!("items.rs"));
                source.update(include_bytes!("secondary_index.rs"));
                source.update(include_bytes!("secondary_index/read.rs"));
                source.update(include_bytes!("partition/key.rs"));
                source.update(include_bytes!("partition/query.rs"));
                source.update(include_bytes!("item_storage.rs"));
                source.update(include_bytes!("items/transaction.rs"));
                source.update(include_bytes!("participant.rs"));
                source.update(include_bytes!("transaction_payload.rs"));
                source.update(include_bytes!("transaction_transport.rs"));
                source.update(include_bytes!("items/scan.rs"));
                source.update(include_bytes!("expression_wire.rs"));
                source.update(include_bytes!("routing.rs"));
                source.update(include_bytes!("authorization.rs"));
                source.update(include_bytes!("tags.rs"));
                source.update(include_bytes!("ttl.rs"));
                source.update(include_bytes!("transaction_token.rs"));
                source.update(include_bytes!("transaction_coordinator.rs"));
                source.update(include_bytes!("transaction_coordinator/registry.rs"));
                Digest::from_bytes(*source.finalize().as_bytes())
            },
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: Box::leak(Box::new([MigrationDescriptor {
                version: 1,
                sql: &SCHEMA,
                digest: Digest::from_bytes(*blake3::hash(SCHEMA.as_bytes()).as_bytes()),
            }])),
            commands: &COMMANDS,
            queries: &QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &NAMESPACES,
        })
    }

    fn register(self, registry: &mut RegistryBuilder) -> Result<()> {
        registry.bind_command::<PruneAccountStreamRecords>()?;
        registry.bind_command::<AdvanceStreamGcShard>()?;
        registry.bind_command::<statistics::PublishStatistics>()?;
        registry.bind_query::<statistics::ReadAccountStatistics>()?;
        registry.bind_query::<ReadAccountStreamJournal>()?;
        registry.bind_query::<HasExpiredAccountStreamRecords>()?;
        registry.bind_query::<ReadStreamGcShard>()?;
        registry.bind_query::<ReadAccountStreamTail>()?;
        registry.bind_query::<ReadStreamCatalog>()?;
        registry.bind_query::<ListStreamCatalog>()?;
        registry.bind_query::<statistics::ReadTableStatistics>()?;
        registry.bind_command::<CreateTable>()?;
        registry.bind_command::<PutItem>()?;
        registry.bind_command::<PutItemNoReturn>()?;
        registry.bind_command::<DeleteItem>()?;
        registry.bind_command::<DeleteItemNoReturn>()?;
        registry.bind_command::<TransactWrite>()?;
        registry.bind_command::<TransactWriteNoReturn>()?;
        registry.bind_command::<crate::UploadTransactionPayload<PrepareAccountTransaction>>()?;
        registry.bind_command::<PrepareAccountTransaction>()?;
        registry.bind_command::<ResolveAccountTransaction>()?;
        registry.bind_command::<ReleaseAccountTransactionReads>()?;
        registry.bind_command::<DeleteTable>()?;
        registry.bind_command::<ContinueTableDeletion>()?;
        registry.bind_command::<RecordTableDirectoryRetirement>()?;
        registry.bind_query::<ReadPendingDirectoryRetirement>()?;
        registry.bind_query::<ReadTableLifecycle>()?;
        registry.bind_command::<UpdateTable>()?;
        registry.bind_command::<UpdateItem>()?;
        registry.bind_command::<UpdateItemNoReturn>()?;
        registry.bind_command::<TransactRead>()?;
        registry.bind_query::<TransactReadQuery>()?;
        registry.bind_command::<ActivateTableRoute>()?;
        registry.bind_command::<authorization::PutPrincipalPolicy>()?;
        registry.bind_command::<authorization::DeletePrincipalPolicy>()?;
        registry.bind_command::<authorization::SetPrincipalBoundary>()?;
        registry.bind_command::<tags::UpdateTags>()?;
        registry.bind_command::<ttl::UpdateTtl>()?;
        registry.bind_command::<ttl::AdvanceTtlSweep>()?;
        registry.bind_command::<ttl::AdvanceTtlSchedule>()?;
        registry.bind_command::<RegisterCoordinatorShard>()?;
        registry.bind_command::<transaction_coordinator::RecordSettledCoordinators>()?;
        registry.bind_command::<ActivateGlobalIndexRoute>()?;
        registry.bind_command::<RecordAccountIndexDelivery>()?;
        registry.bind_query::<GetItem>()?;
        registry.bind_query::<QueryAccountItems>()?;
        registry.bind_query::<ReadAccountTransaction>()?;
        registry.bind_query::<ReadAccountTransactionResult>()?;
        registry.bind_query::<DescribeTable>()?;
        registry.bind_query::<ListTables>()?;
        registry.bind_query::<DescribeTableById>()?;
        registry.bind_query::<ScanItems>()?;
        registry.bind_query::<secondary_index::QueryAccountIndex>()?;
        registry.bind_query::<authorization::ReadPrincipalPolicies>()?;
        registry.bind_query::<authorization::ReadPrincipalBoundary>()?;
        registry.bind_query::<tags::ReadTags>()?;
        registry.bind_query::<ttl::ReadTtl>()?;
        registry.bind_query::<ttl::ListTtlTables>()?;
        registry.bind_query::<ttl::ReadTtlSweep>()?;
        registry.bind_query::<ttl::ReadTtlSchedule>()?;
        registry.bind_query::<ReadCoordinatorRegistration>()?;
        registry.bind_query::<ReadRouteDirectory>()?;
        registry.bind_query::<ReadAccountIndexChange>()?;
        registry.bind_query::<ReadAccountIndexChangeChunk>()?;
        registry.bind_query::<ListCoordinatorShards>()
    }
}

/// JSON backed value with a canonical Cell wire encoding.
#[derive(Clone, Debug, PartialEq)]
pub struct Json<T>(pub T);

// Compact images avoid JSON binary/escape expansion across peer hops. Keep an
// explicit saved-image fallback when even the compact aggregate exceeds the
// operation envelope; non-item outcomes retain canonical JSON.
fn encode_read_query<T: Serialize>(
    value: &T,
    images: Option<&[Option<Item>]>,
    fallback: &T,
    encoder: &mut BoundedEncoder,
) -> std::result::Result<(), CodecError> {
    let json = if let Some(images) = images {
        match item_wire::images_size(images) {
            Ok(size) if size < OPERATION_BYTES as usize => {
                encoder.write_u8(1)?;
                return item_wire::encode_images(images, encoder);
            }
            Ok(_) | Err(CodecError::Limit) => fallback,
            Err(error) => return Err(error),
        }
    } else {
        value
    };
    let mut bytes = serde_json::to_vec(json)
        .map_err(|_| CodecError::Invalid("DynamoDB value failed to encode"))?;
    // The JSON envelope has a tag and a four-byte length prefix.
    if bytes.len() > OPERATION_BYTES as usize - 5 {
        bytes = serde_json::to_vec(fallback)
            .map_err(|_| CodecError::Invalid("DynamoDB value failed to encode"))?;
    }
    encoder.write_u8(0)?;
    encoder.write_bytes(&bytes)
}

fn decode_read_query_images(
    decoder: &mut BoundedDecoder<'_>,
) -> std::result::Result<Option<Vec<Option<Item>>>, CodecError> {
    match decoder.read_u8()? {
        0 => Ok(None),
        1 => item_wire::decode_images(decoder).map(Some),
        _ => Err(CodecError::Invalid("unknown transaction read envelope")),
    }
}

impl<T> WireValue for Json<T>
where
    T: Serialize + DeserializeOwned + Send + 'static,
{
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        let bytes = serde_json::to_vec(&self.0)
            .map_err(|_| CodecError::Invalid("DynamoDB value failed to encode"))?;
        encoder.write_bytes(&bytes)
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        let bytes = decoder.read_bytes()?;
        let value: T = serde_json::from_slice(bytes)
            .map_err(|_| CodecError::Invalid("invalid DynamoDB JSON value"))?;
        let canonical = serde_json::to_vec(&value)
            .map_err(|_| CodecError::Invalid("DynamoDB value failed to encode"))?;
        if canonical != bytes {
            return Err(CodecError::Invalid("noncanonical DynamoDB JSON value"));
        }
        Ok(Self(value))
    }
}

#[cfg(test)]
mod transaction_read_codec_tests {
    use super::*;
    use extenddb_core::types::AttributeValue;

    fn roundtrip<T: WireValue>(value: T) -> T {
        let mut encoder = BoundedEncoder::new(OPERATION_BYTES).unwrap();
        value.encode(&mut encoder).unwrap();
        let bytes = encoder.finish();
        let mut decoder = BoundedDecoder::new(&bytes, OPERATION_BYTES).unwrap();
        let result = T::decode(&mut decoder).unwrap();
        decoder.finish().unwrap();
        result
    }

    #[test]
    fn compact_read_preserves_nested_attributes_and_all_outcomes() {
        use std::collections::BTreeSet;
        let attributes = Item::from([
            (
                "string".into(),
                AttributeValue::S(['\0', '\n', '\\', '"', '界'].into_iter().collect()),
            ),
            ("number".into(), AttributeValue::N("-123.456".into())),
            ("binary".into(), AttributeValue::B(vec![0, 128, 255])),
            (
                "strings".into(),
                AttributeValue::SS(BTreeSet::from(["a".into(), "界".into()])),
            ),
            (
                "numbers".into(),
                AttributeValue::NS(BTreeSet::from([
                    "-2".into(),
                    "10000000000000000000000000000000000000".into(),
                ])),
            ),
            (
                "binaries".into(),
                AttributeValue::BS(BTreeSet::from([vec![0], vec![255]])),
            ),
            ("boolean".into(), AttributeValue::Bool(true)),
            ("null".into(), AttributeValue::Null),
            (
                "list".into(),
                AttributeValue::L(vec![
                    AttributeValue::Bool(false),
                    AttributeValue::M(Item::from([("nested".into(), AttributeValue::B(vec![]))])),
                ]),
            ),
            (
                "map".into(),
                AttributeValue::M(Item::from([("value".into(), AttributeValue::S("".into()))])),
            ),
        ]);
        let images = vec![Some(attributes), None, Some(Item::new())];
        let account = TransactionReadOutcome::Applied(images.clone());
        let partition = PartitionTransactReadOutcome::Applied(images);
        assert_eq!(
            roundtrip(TransactionReadQueryOutput(account.clone())).0,
            account
        );
        assert_eq!(
            roundtrip(PartitionTransactReadQueryOutput(partition.clone())).0,
            partition
        );
        for outcome in [
            PartitionTransactReadOutcome::NotInstalled,
            PartitionTransactReadOutcome::StaleRoute,
            PartitionTransactReadOutcome::Sealed,
            PartitionTransactReadOutcome::NotReady,
            PartitionTransactReadOutcome::WrongPartition,
            PartitionTransactReadOutcome::SavedImagesRequired,
            PartitionTransactReadOutcome::Rejected {
                index: 3,
                reason: TransactionFailure::Conflict,
            },
        ] {
            assert_eq!(
                roundtrip(PartitionTransactReadQueryOutput(outcome.clone())).0,
                outcome
            );
        }
        let rejected = TransactionReadOutcome::Rejected {
            index: 2,
            reason: TransactionFailure::Validation("invalid read".into()),
        };
        assert_eq!(
            roundtrip(TransactionReadQueryOutput(rejected.clone())).0,
            rejected
        );
    }

    #[test]
    fn read_images_reject_noncanonical_json_envelopes() {
        let mut encoder = BoundedEncoder::new(OPERATION_BYTES).unwrap();
        encoder.write_u8(0).unwrap();
        Json(TransactionReadOutcome::Applied(vec![]))
            .encode(&mut encoder)
            .unwrap();
        let bytes = encoder.finish();
        let mut decoder = BoundedDecoder::new(&bytes, OPERATION_BYTES).unwrap();
        assert!(TransactionReadQueryOutput::decode(&mut decoder).is_err());
        let mut decoder = BoundedDecoder::new(&bytes, OPERATION_BYTES).unwrap();
        assert!(PartitionTransactReadQueryOutput::decode(&mut decoder).is_err());
    }

    #[test]
    fn compact_read_keeps_large_escaped_strings_inside_the_query_envelope() {
        let images = vec![
            Some(Item::from([(
                "payload".into(),
                AttributeValue::S("\0".repeat(380 * 1024))
            )]));
            10
        ];
        let value = TransactionReadOutcome::Applied(images);
        assert!(roundtrip(TransactionReadQueryOutput(value.clone())).0 == value);
    }

    #[test]
    fn transaction_read_codecs_preserve_legal_aggregates_and_bound_output() {
        for (count, size, oversized) in [
            (1, 100, false),
            (10, 380 * 1024, false),
            (12, 380 * 1024, true),
        ] {
            let images = vec![
                Some(Item::from([(
                    "payload".into(),
                    AttributeValue::B(vec![0xa5; size]),
                )]));
                count
            ];
            let account = TransactionReadOutcome::Applied(images.clone());
            let partition = PartitionTransactReadOutcome::Applied(images);
            let account_result = roundtrip(TransactionReadQueryOutput(account.clone())).0;
            let partition_result = roundtrip(PartitionTransactReadQueryOutput(partition.clone())).0;
            if oversized {
                assert_eq!(account_result, TransactionReadOutcome::SavedImagesRequired);
                assert_eq!(
                    partition_result,
                    PartitionTransactReadOutcome::SavedImagesRequired
                );
            } else {
                assert!(
                    account_result == account,
                    "legal account aggregate must retain all items"
                );
                assert!(
                    partition_result == partition,
                    "legal partition aggregate must retain all items"
                );
            }
        }
    }
}
