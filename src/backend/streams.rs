//! DynamoDB Streams storage over account and routed owner Cells.

use cellule_runtime::identity::{CellTarget, TenantId};
use extenddb_core::types::{DescribeStreamInput, StreamDescription, StreamRecord};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BoxedFuture, StreamContinuation, StreamEngine, StreamListResult, StreamRecordsResult,
};

use crate::{
    Json, ReadAccountStreamJournal, ReadAccountStreamTail, ReadPartitionState,
    ReadPartitionStreamJournal, ReadPartitionStreamTail, StreamJournalInput, StreamJournalOutcome,
    StreamTailInput, StreamTailOutcome, data_target,
};

use super::{CellStorage, cell_error, target, unsupported};

enum StreamShard {
    Account {
        table_id: String,
        label: String,
    },
    Partition {
        table_id: String,
        partition_id: [u8; 16],
        label: String,
    },
}

impl StreamShard {
    fn parse(shard_id: &str) -> Option<Self> {
        let mut parts = shard_id.splitn(4, '-');
        if parts.next() != Some("shardId") {
            return None;
        }
        let table_id = parts.next()?.to_owned();
        let hash = blake3::Hash::from_hex(&table_id).ok()?;
        if hash.to_hex().as_str() != table_id {
            return None;
        }
        let partition = parts.next()?;
        let label = parts.next()?.to_owned();
        if label.is_empty() || label.contains('|') {
            return None;
        }
        if partition == "account" {
            return Some(Self::Account { table_id, label });
        }
        if partition.len() != 32 {
            return None;
        }
        let partition_id = u128::from_str_radix(partition, 16).ok()?.to_be_bytes();
        Some(Self::Partition {
            table_id,
            partition_id,
            label,
        })
    }

    fn table_id(&self) -> &str {
        match self {
            Self::Account { table_id, .. } | Self::Partition { table_id, .. } => table_id,
        }
    }

    fn label(&self) -> &str {
        match self {
            Self::Account { label, .. } | Self::Partition { label, .. } => label,
        }
    }

    fn target(&self) -> Result<CellTarget, StorageError> {
        let id = blake3::Hash::from_hex(self.table_id())
            .map_err(|_| StorageError::TableNotFound(self.table_id().into()))?;
        let mut tenant_bytes = [0; 16];
        tenant_bytes.copy_from_slice(&id.as_bytes()[..16]);
        let tenant = TenantId::from_bytes(tenant_bytes);
        match self {
            Self::Account { .. } => crate::account_for_tenant(tenant),
            Self::Partition {
                table_id,
                partition_id,
                ..
            } => {
                let partition = crate::data_partition_bytes(table_id, partition_id)
                    .map_err(|_| StorageError::TableNotFound(self.table_id().into()))?;
                CellTarget::new(
                    tenant,
                    crate::APPLICATION,
                    crate::DATA_NAMESPACE,
                    &partition,
                )
            }
        }
        .map_err(|_| StorageError::TableNotFound(self.table_id().into()))
    }
}

impl StreamEngine for CellStorage {
    fn write_stream_record(
        &self,
        _account_id: &str,
        _record: &StreamRecord,
        _shard_id: &str,
        _table_name: &str,
    ) -> BoxedFuture<'_, Result<(), StorageError>> {
        Box::pin(async { Err(unsupported("DynamoDB Streams")) })
    }

    fn get_stream_records(
        &self,
        account_id: &str,
        shard_id: &str,
        after_sequence: Option<&str>,
        limit: i64,
    ) -> BoxedFuture<'_, StreamRecordsResult> {
        let account_id = account_id.to_owned();
        let shard_id = shard_id.to_owned();
        let after_sequence = after_sequence.map(str::to_owned);
        Box::pin(async move {
            let limit = u16::try_from(limit)
                .ok()
                .filter(|limit| (1..=1_000).contains(limit))
                .ok_or_else(|| {
                    StorageError::Validation("stream record limit is outside 1..=1000".into())
                })?;
            let shard = StreamShard::parse(&shard_id)
                .ok_or_else(|| StorageError::TableNotFound(shard_id.clone()))?;
            let input = StreamJournalInput {
                table_id: shard.table_id().to_owned(),
                label: shard.label().to_owned(),
                after_sequence,
                limit,
            };
            let page = match shard {
                StreamShard::Account { .. } => {
                    let owner = target(&account_id)?;
                    self.client
                        .query::<ReadAccountStreamJournal>(&owner, None, Json(input))
                        .await
                        .map_err(cell_error)?
                        .output
                        .0
                }
                StreamShard::Partition {
                    table_id,
                    partition_id,
                    ..
                } => {
                    let owner = data_target(&account_id, &table_id, &partition_id)
                        .map_err(|_| StorageError::TableNotFound(shard_id.clone()))?;
                    self.client
                        .query::<ReadPartitionStreamJournal>(&owner, None, Json(input))
                        .await
                        .map_err(cell_error)?
                        .output
                        .0
                }
            };
            match page {
                StreamJournalOutcome::Missing => Err(StorageError::TableNotFound(shard_id)),
                StreamJournalOutcome::Page {
                    records,
                    closed: true,
                    ..
                } if records.is_empty() => Ok((records, StreamContinuation::End)),
                StreamJournalOutcome::Page {
                    records,
                    last_sequence,
                    ..
                } => Ok((records, StreamContinuation::More(last_sequence))),
            }
        })
    }

    fn sequence_number_width(&self) -> usize {
        23
    }

    fn describe_stream(
        &self,
        _account_id: &str,
        _input: &DescribeStreamInput,
    ) -> BoxedFuture<'_, Result<StreamDescription, StorageError>> {
        Box::pin(async { Err(unsupported("DynamoDB Streams")) })
    }

    fn list_streams(
        &self,
        _account_id: &str,
        _table_name: Option<&str>,
        _limit: i64,
        _exclusive_start_stream_arn: Option<&str>,
    ) -> BoxedFuture<'_, StreamListResult> {
        Box::pin(async { Err(unsupported("DynamoDB Streams")) })
    }

    fn cleanup_expired_stream_records(
        &self,
        _retention_hours: i64,
    ) -> BoxedFuture<'_, Result<u64, StorageError>> {
        Box::pin(async { Err(unsupported("DynamoDB Streams")) })
    }

    fn assign_shard(
        &self,
        _account_id: &str,
        _table_name: &str,
        _partition_key: &str,
    ) -> BoxedFuture<'_, Result<String, StorageError>> {
        Box::pin(async { Err(unsupported("DynamoDB Streams")) })
    }

    fn next_sequence_number(
        &self,
        _shard_id: &str,
    ) -> BoxedFuture<'_, Result<String, StorageError>> {
        Box::pin(async { Err(unsupported("DynamoDB Streams")) })
    }

    fn validate_shard(
        &self,
        account_id: &str,
        stream_arn: &str,
        shard_id: &str,
    ) -> BoxedFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_owned();
        let stream_arn = stream_arn.to_owned();
        let shard_id = shard_id.to_owned();
        Box::pin(async move {
            let missing = || StorageError::TableNotFound(stream_arn.clone());
            let (table_name, label) =
                extenddb_storage::util::parse_stream_arn(&stream_arn).map_err(|_| missing())?;
            if extenddb_storage::util::stream_arn(&self.region, &account_id, &table_name, &label)
                != stream_arn
            {
                return Err(missing());
            }
            let shard = StreamShard::parse(&shard_id).ok_or_else(missing)?;
            if shard.label() != label {
                return Err(missing());
            }
            let record = match self.lifecycle(&account_id, &table_name).await? {
                crate::TableLifecycle::Live(record) | crate::TableLifecycle::Deleting(record) => {
                    record
                }
                crate::TableLifecycle::Missing => return Err(missing()),
            };
            if record.id != shard.table_id()
                || record.stream.as_ref().map(|stream| stream.label.as_str())
                    != Some(label.as_str())
            {
                return Err(missing());
            }
            match (record.placement, shard) {
                (crate::TablePlacement::Account, StreamShard::Account { .. }) => Ok(()),
                (
                    crate::TablePlacement::Routed { .. },
                    StreamShard::Partition {
                        table_id,
                        partition_id,
                        ..
                    },
                ) => {
                    let owner = data_target(&account_id, &table_id, &partition_id)
                        .map_err(|_| missing())?;
                    let status = self
                        .client
                        .query::<ReadPartitionState>(&owner, None, Json(()))
                        .await
                        .map_err(cell_error)?
                        .output
                        .0;
                    let Some(status) = status else {
                        return Err(missing());
                    };
                    if status.spec.table.id != table_id
                        || status.spec.partition_id != partition_id
                        || status
                            .spec
                            .table
                            .stream
                            .as_ref()
                            .map(|stream| stream.label.as_str())
                            != Some(label.as_str())
                    {
                        return Err(missing());
                    }
                    Ok(())
                }
                _ => Err(missing()),
            }
        })
    }

    fn latest_sequence_number(
        &self,
        shard_id: &str,
    ) -> BoxedFuture<'_, Result<Option<String>, StorageError>> {
        let shard_id = shard_id.to_owned();
        Box::pin(async move {
            // ExtendDB validates account and ARN before this shard-only method.
            let shard = StreamShard::parse(&shard_id)
                .ok_or_else(|| StorageError::TableNotFound(shard_id.clone()))?;
            let owner = shard.target()?;
            let input = StreamTailInput {
                table_id: shard.table_id().to_owned(),
                label: shard.label().to_owned(),
            };
            let outcome = match shard {
                StreamShard::Account { .. } => {
                    self.client
                        .query::<ReadAccountStreamTail>(&owner, None, Json(input))
                        .await
                        .map_err(cell_error)?
                        .output
                        .0
                }
                StreamShard::Partition { .. } => {
                    self.client
                        .query::<ReadPartitionStreamTail>(&owner, None, Json(input))
                        .await
                        .map_err(cell_error)?
                        .output
                        .0
                }
            };
            match outcome {
                StreamTailOutcome::Latest(sequence) => Ok(sequence),
                StreamTailOutcome::Missing => Err(StorageError::TableNotFound(shard_id)),
            }
        })
    }
}
