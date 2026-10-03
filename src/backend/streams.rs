//! DynamoDB Streams storage over account and routed owner Cells.

use crate::{
    Json, ListStreamCatalog, ListStreamCatalogInput, PartitionState, ReadAccountStreamJournal,
    ReadAccountStreamTail, ReadPartitionState, ReadPartitionStreamJournal, ReadPartitionStreamTail,
    ReadStreamCatalog, StreamCatalogKey, StreamJournalInput, StreamJournalOutcome, StreamTailInput,
    StreamTailOutcome, data_target,
};
use cellule_runtime::identity::{CellTarget, TenantId};
use extenddb_core::types::{
    DescribeStreamInput, SequenceNumberRange, Shard, StreamDescription, StreamRecord, StreamStatus,
    StreamSummary,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BoxedFuture, StreamContinuation, StreamEngine, StreamListResult, StreamRecordsResult,
};

use super::{CellStorage, cell_error, target, unsupported};

const FIRST_SEQUENCE: &str = "00000000000000000000001";

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
    fn id(table_id: &str, partition_id: Option<[u8; 16]>, label: &str) -> String {
        match partition_id {
            Some(partition_id) => format!(
                "shardId-{table_id}-{:032x}-{label}",
                u128::from_be_bytes(partition_id)
            ),
            None => format!("shardId-{table_id}-account-{label}"),
        }
    }

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

impl CellStorage {
    async fn stream_record(
        &self,
        account_id: &str,
        arn: &str,
    ) -> Result<(crate::TableRecord, String, StreamStatus), StorageError> {
        let missing = || StorageError::TableNotFound(arn.into());
        let (table_name, label) =
            extenddb_storage::util::parse_stream_arn(arn).map_err(|_| missing())?;
        if extenddb_storage::util::stream_arn(&self.region, account_id, &table_name, &label) != arn
        {
            return Err(missing());
        }
        let owner = target(account_id)?;
        let entry = self
            .client
            .query::<ReadStreamCatalog>(
                &owner,
                None,
                Json(StreamCatalogKey::NameLabel {
                    table_name: table_name.clone(),
                    label: label.clone(),
                }),
            )
            .await
            .map_err(cell_error)?
            .output
            .0
            .ok_or_else(missing)?;
        let record = entry.record;
        let status = if entry.disabled_at_ms.is_some() {
            match self.lifecycle(account_id, &table_name).await? {
                crate::TableLifecycle::Deleting(current) if current.id == record.id => {
                    StreamStatus::Disabling
                }
                _ => StreamStatus::Disabled,
            }
        } else if record.placement.is_routed()
            && !self.route_active_for(account_id, &record.id).await?
        {
            StreamStatus::Enabling
        } else {
            StreamStatus::Enabled
        };
        Ok((record, label, status))
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
            let owner = target(&account_id)?;
            // A table ID embeds its account tenant. Reject a foreign shard
            // before querying an account Cell that may not be provisioned.
            if shard.target()?.tenant() != owner.tenant() {
                return Err(StorageError::TableNotFound(shard_id));
            }
            let entry = self
                .client
                .query::<ReadStreamCatalog>(
                    &owner,
                    None,
                    Json(StreamCatalogKey::Id(shard.table_id().into())),
                )
                .await
                .map_err(cell_error)?
                .output
                .0
                .ok_or_else(|| StorageError::TableNotFound(shard_id.clone()))?;
            if entry
                .record
                .stream
                .as_ref()
                .map(|stream| stream.label.as_str())
                != Some(shard.label())
            {
                return Err(StorageError::TableNotFound(shard_id));
            }
            // Deletion first fences the table, then waits for routed owners to
            // retire. Keep polling possible until the final owner is retired.
            let disabled = if entry.disabled_at_ms.is_some() {
                !matches!(
                    self.lifecycle(&account_id, &entry.record.table_name).await?,
                    crate::TableLifecycle::Deleting(current) if current.id == entry.record.id
                )
            } else {
                false
            };
            let input = StreamJournalInput {
                table_id: shard.table_id().to_owned(),
                label: shard.label().to_owned(),
                after_sequence,
                limit,
            };
            let page = match shard {
                StreamShard::Account { .. } => {
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
                    closed,
                    exhausted: true,
                    ..
                } if closed || disabled => Ok((records, StreamContinuation::End)),
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
        account_id: &str,
        input: &DescribeStreamInput,
    ) -> BoxedFuture<'_, Result<StreamDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let stream_arn = input.stream_arn.clone();
        let limit = input.limit.unwrap_or(100);
        let start = input.exclusive_start_shard_id.clone();
        Box::pin(async move {
            let limit = usize::try_from(limit)
                .ok()
                .filter(|limit| (1..=100).contains(limit))
                .ok_or_else(|| {
                    StorageError::Validation("stream shard limit must be 1..=100".into())
                })?;
            let (record, label, status) = self.stream_record(&account_id, &stream_arn).await?;
            let mut shards = Vec::with_capacity(limit);
            let mut last_evaluated_shard_id = None;
            let mut past_start = start.is_none();
            let mut found_start = past_start;
            let mut pending = if status == StreamStatus::Enabling {
                Vec::new()
            } else {
                match record.placement.initial_partitions() {
                    None => vec![(None, None)],
                    Some(initial_partitions) => (0..initial_partitions)
                        .rev()
                        .map(|index| {
                            let mut partition_id = [0; 16];
                            partition_id[15] = u8::try_from(index).map_err(|_| {
                                StorageError::Internal("invalid initial stream partition".into())
                            })?;
                            Ok((Some(partition_id), None))
                        })
                        .collect::<Result<Vec<_>, StorageError>>()?,
                }
            };
            while let Some((partition_id, parent)) = pending.pop() {
                let shard_id = StreamShard::id(&record.id, partition_id, &label);
                let ending_sequence_number = if let Some(partition_id) = partition_id {
                    let owner = data_target(&account_id, &record.id, &partition_id)
                        .map_err(|error| StorageError::Internal(error.to_string()))?;
                    let state = self
                        .client
                        .query::<ReadPartitionState>(&owner, None, Json(()))
                        .await
                        .map_err(cell_error)?
                        .output
                        .0
                        .ok_or_else(|| {
                            StorageError::Transient("stream partition is not installed".into())
                        })?;
                    if state.spec.table.id != record.id || state.spec.partition_id != partition_id {
                        return Err(StorageError::Internal(
                            "stream partition identity differs".into(),
                        ));
                    }
                    match state.state {
                        PartitionState::Serving | PartitionState::Opened { .. }
                            if status == StreamStatus::Disabled =>
                        {
                            Some(
                                self.latest_sequence_number(&shard_id)
                                    .await?
                                    .unwrap_or_else(|| FIRST_SEQUENCE.into()),
                            )
                        }
                        PartitionState::Serving | PartitionState::Opened { .. } => None,
                        PartitionState::Sealed(seal) => {
                            pending.push((Some(seal.right_partition_id), Some(shard_id.clone())));
                            pending.push((Some(seal.left_partition_id), Some(shard_id.clone())));
                            Some(
                                self.latest_sequence_number(&shard_id)
                                    .await?
                                    .unwrap_or_else(|| FIRST_SEQUENCE.into()),
                            )
                        }
                        PartitionState::Importing { .. } | PartitionState::Activated { .. } => {
                            return Err(StorageError::Transient(
                                "stream split is still publishing".into(),
                            ));
                        }
                    }
                } else if status == StreamStatus::Disabled {
                    Some(
                        self.latest_sequence_number(&shard_id)
                            .await?
                            .unwrap_or_else(|| FIRST_SEQUENCE.into()),
                    )
                } else {
                    None
                };
                if !past_start {
                    if start.as_deref() == Some(&shard_id) {
                        past_start = true;
                        found_start = true;
                    }
                    continue;
                }
                if shards.len() == limit {
                    last_evaluated_shard_id =
                        shards.last().map(|shard: &Shard| shard.shard_id.clone());
                    break;
                }
                shards.push(Shard {
                    shard_id,
                    parent_shard_id: parent,
                    sequence_number_range: SequenceNumberRange {
                        starting_sequence_number: FIRST_SEQUENCE.into(),
                        ending_sequence_number,
                    },
                });
            }
            if !found_start {
                return Err(StorageError::Validation(
                    "ExclusiveStartShardId is not in this stream".into(),
                ));
            }
            Ok(StreamDescription {
                stream_arn,
                stream_label: label,
                stream_status: status,
                stream_view_type: record
                    .stream
                    .as_ref()
                    .map(|stream| stream.view_type)
                    .ok_or_else(|| StorageError::TableNotFound(record.table_name.clone()))?,
                table_name: record.table_name,
                key_schema: record.key_schema,
                shards,
                last_evaluated_shard_id,
            })
        })
    }

    fn list_streams(
        &self,
        account_id: &str,
        table_name: Option<&str>,
        limit: i64,
        exclusive_start_stream_arn: Option<&str>,
    ) -> BoxedFuture<'_, StreamListResult> {
        let account_id = account_id.to_owned();
        let table_name = table_name.map(str::to_owned);
        let start = exclusive_start_stream_arn.map(str::to_owned);
        Box::pin(async move {
            let limit = usize::try_from(limit)
                .ok()
                .filter(|limit| (1..=100).contains(limit))
                .ok_or_else(|| {
                    StorageError::Validation("stream listing limit must be 1..=100".into())
                })?;
            let after = match start {
                Some(arn) => {
                    let (record, label, _) = self.stream_record(&account_id, &arn).await?;
                    if table_name
                        .as_ref()
                        .is_some_and(|name| name != &record.table_name)
                    {
                        return Err(StorageError::Validation(
                            "stream cursor is outside table filter".into(),
                        ));
                    }
                    Some((record.table_name, label))
                }
                None => None,
            };
            let owner = target(&account_id)?;
            let page = self
                .client
                .query::<ListStreamCatalog>(
                    &owner,
                    None,
                    Json(ListStreamCatalogInput {
                        table_name,
                        after,
                        limit: u16::try_from(limit).map_err(|_| {
                            StorageError::Validation("invalid stream listing limit".into())
                        })?,
                    }),
                )
                .await
                .map_err(cell_error)?
                .output
                .0;
            let streams = page
                .streams
                .into_iter()
                .map(|stream| StreamSummary {
                    stream_arn: extenddb_storage::util::stream_arn(
                        &stream.region,
                        &account_id,
                        &stream.table_name,
                        &stream.label,
                    ),
                    stream_label: stream.label,
                    table_name: stream.table_name,
                })
                .collect();
            let last = page.last_evaluated.map(|(name, label)| {
                extenddb_storage::util::stream_arn(&self.region, &account_id, &name, &label)
            });
            Ok((streams, last))
        })
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
            let (record, label, status) = self.stream_record(&account_id, &stream_arn).await?;
            if status == StreamStatus::Enabling {
                return Err(missing());
            }
            let shard = StreamShard::parse(&shard_id).ok_or_else(missing)?;
            if shard.label() != label {
                return Err(missing());
            }
            if record.id != shard.table_id() {
                return Err(missing());
            }
            match (record.placement, shard) {
                (crate::TablePlacement::Account, StreamShard::Account { .. }) => Ok(()),
                (
                    crate::TablePlacement::Routed { .. } | crate::TablePlacement::Single,
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
                        || matches!(
                            status.state,
                            PartitionState::Importing { .. } | PartitionState::Activated { .. }
                        )
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
