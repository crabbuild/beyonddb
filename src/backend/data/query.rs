//! Lazy ordered Query across physical cells with one response budget.
use super::*;
use crate::partition::key::{ordered_data_key, ordered_index_key, partition_span, query_span};
use crate::{RoutePageInput, RoutePageOutcome, data_target};

impl CellStorage {
    pub(super) async fn query_cells(
        &self,
        key_info: &TableKeyInfo,
        mut input: PartitionQueryInput,
    ) -> QueryResult {
        let global = key_info
            .global_secondary_indexes
            .iter()
            .find(|index| Some(index.index_name.as_str()) == input.index_name.as_deref());
        let local = input.index_name.is_some() && global.is_none();
        let route_schema = global.map_or(&key_info.base_key_schema, |index| &index.key_schema);
        let id = global.map_or(&key_info.table_id, |index| &index.index_id);
        let mut cursor_schema = key_info.base_key_schema.clone();
        for key in &key_info.key_schema {
            if !cursor_schema
                .iter()
                .any(|base| base.attribute_name == key.attribute_name)
            {
                cursor_schema.push(key.clone());
            }
        }
        let validation = |error: crate::Error| StorageError::Validation(error.to_string());
        let mut span = if local {
            partition_span(&input.partition_key, route_schema)
        } else {
            query_span(&input.partition_key, route_schema, input.sort.as_ref())
        }
        .map_err(validation)?;
        if let Some(key) = input.exclusive_start_key.as_ref() {
            extenddb_core::validation::validate_key_only(
                key,
                &cursor_schema,
                &key_info.attribute_definitions,
            )
            .map_err(|error| StorageError::Validation(error.to_string()))?;
            if crate::partition::key::partition_key_bytes(key, route_schema).map_err(validation)?
                != crate::partition::key::partition_key_bytes(&input.partition_key, route_schema)
                    .map_err(validation)?
            {
                return Err(StorageError::Validation(
                    "Query continuation has a different partition key".into(),
                ));
            }
            if !local {
                let resume = if global.is_some() {
                    ordered_index_key(key, route_schema, &key_info.base_key_schema)
                } else {
                    ordered_data_key(id, key, route_schema)
                }
                .map_err(validation)?;
                let Some(resumed) = span.resume(&resume, input.forward) else {
                    return Ok((Vec::new(), None));
                };
                span = resumed;
            }
        }
        let account = target(&key_info.account_id)?;
        let mut position = if input.forward {
            span.lower()
        } else {
            span.upper()
        }
        .cloned()
        .ok_or_else(|| StorageError::Internal("Query has no bounded partition span".into()))?;
        input.table_id = id.clone();
        let maximum = input.limit.min(10_000) as usize;
        if maximum == 0 {
            return Err(StorageError::Validation(
                "query limit must be positive".into(),
            ));
        }
        let mut items = Vec::new();
        let mut bytes = 0usize;
        let mut last = None;
        loop {
            let page = crate::read_route_page_direction(
                &self.client,
                &account,
                RoutePageInput {
                    table_id: id.clone(),
                    start_key: Some(position.clone()),
                    after_lower: None,
                    expected_epoch: None,
                },
                input.forward,
            )
            .await?;
            let ranges = match page {
                RoutePageOutcome::Unrouted => {
                    // An absent directory never permits an implicit account fallback.
                    // Only an explicit immutable Account generation can use the
                    // private legacy module; public creation requires Routed.
                    let record = self
                        .client
                        .query::<crate::DescribeTableById>(
                            &account,
                            None,
                            Json(key_info.table_id.clone()),
                        )
                        .await
                        .map_err(cell_error)?
                        .output
                        .0
                        .ok_or_else(|| StorageError::TableNotFound(key_info.table_name.clone()))?;
                    if global.is_some() || record.placement != crate::TablePlacement::Account {
                        return Err(StorageError::TableNotActive(key_info.table_name.clone()));
                    }
                    let output = if local {
                        self.query_resolving::<crate::secondary_index::QueryAccountIndex>(
                            &account,
                            &key_info.account_id,
                            Json(input.clone()),
                        )
                        .await?
                    } else {
                        self.query_resolving::<crate::QueryAccountItems>(
                            &account,
                            &key_info.account_id,
                            Json(input.clone()),
                        )
                        .await?
                    };
                    return match output.output.0 {
                        PartitionQueryOutcome::Page {
                            items,
                            last_evaluated_key,
                        } => Ok((items, last_evaluated_key)),
                        PartitionQueryOutcome::InvalidKey
                        | PartitionQueryOutcome::InvalidCondition => Err(StorageError::Validation(
                            "invalid Query condition or continuation".into(),
                        )),
                        PartitionQueryOutcome::InvalidLimit => Err(StorageError::Validation(
                            "query limit must be positive".into(),
                        )),
                        _ => Err(StorageError::Transient("account Query is not ready".into())),
                    };
                }
                RoutePageOutcome::Changed => return Err(stale_partition()),
                RoutePageOutcome::Page { partitions, .. } => partitions,
            };
            if ranges.is_empty() {
                return Ok((items, None));
            }
            let mut next = None;
            for range in ranges {
                let owned =
                    bdb_keyspace::KeySpan::new(Some(range.lower.clone()), range.upper.clone())
                        .map_err(|_| StorageError::Internal("invalid directory range".into()))?;
                if owned.intersection(&span).is_none() {
                    return Ok((items, None));
                }
                let owner = if global.is_some() {
                    crate::global_index_target(&key_info.account_id, id, &range.partition_id)
                } else {
                    data_target(&key_info.account_id, id, &range.partition_id)
                }
                .map_err(|error| StorageError::Internal(error.to_string()))?;
                loop {
                    input.epoch = range.epoch;
                    input.limit = (maximum - items.len()) as u32;
                    let output = if global.is_some() {
                        self.client
                            .query::<crate::GlobalIndexQuery>(&owner, None, Json(input.clone()))
                            .await
                            .map_err(cell_error)?
                    } else {
                        self.query_resolving::<PartitionQuery>(
                            &owner,
                            &key_info.account_id,
                            Json(input.clone()),
                        )
                        .await?
                    };
                    let (page, continuation) = match output.output.0 {
                        PartitionQueryOutcome::Page {
                            items,
                            last_evaluated_key,
                        } => (items, last_evaluated_key),
                        PartitionQueryOutcome::InvalidKey
                        | PartitionQueryOutcome::InvalidCondition => {
                            return Err(StorageError::Validation(
                                "invalid Query condition or continuation".into(),
                            ));
                        }
                        PartitionQueryOutcome::InvalidLimit => {
                            return Err(StorageError::Validation(
                                "query limit must be positive".into(),
                            ));
                        }
                        PartitionQueryOutcome::Conflict(_) => {
                            return Err(StorageError::Transient(
                                "Query is waiting for transaction resolution".into(),
                            ));
                        }
                        _ => {
                            self.invalidate_route_cache(&key_info.account_id, &key_info.table_id);
                            return Err(stale_partition());
                        }
                    };
                    for item in page {
                        let size = serde_json::to_vec(&item)
                            .map_err(|error| StorageError::Internal(error.to_string()))?
                            .len()
                            .max(extenddb_core::types::item_size_bytes(&item));
                        let next_bytes = bytes.saturating_add(size).saturating_add(128);
                        if !items.is_empty() && next_bytes > 900_000 {
                            return Ok((items, last));
                        }
                        bytes = next_bytes;
                        last = Some(extract_key(&item, &cursor_schema));
                        input.exclusive_start_key = last.clone();
                        items.push(item);
                        if items.len() >= maximum {
                            return Ok((items, last));
                        }
                    }
                    let Some(continuation) = continuation else {
                        break;
                    };
                    input.exclusive_start_key = Some(continuation);
                }
                next = if input.forward {
                    range.upper
                } else {
                    Some(range.lower)
                };
                let Some(bound) = next.as_ref() else {
                    return Ok((items, None));
                };
                if (input.forward && span.upper().is_some_and(|upper| bound >= upper))
                    || (!input.forward && span.lower().is_some_and(|lower| bound <= lower))
                {
                    return Ok((items, None));
                }
            }
            let Some(next) = next else {
                return Ok((items, None));
            };
            if (input.forward && next <= position) || (!input.forward && next >= position) {
                return Err(StorageError::Internal(
                    "ordered Query directory made no progress".into(),
                ));
            }
            position = next;
        }
    }
}
