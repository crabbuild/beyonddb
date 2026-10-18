//! Table installation and domain split orchestration over Cellar SDK routing.
//! Cellar owns all physical activation, placement, owner fences and restoration.

use bdb_keyspace::RangeKey;
#[path = "../provision/capacity.rs"]
mod capacity;
#[path = "provision/coordinator_registration.rs"]
mod coordinator_registration;
#[path = "../provision/directory.rs"]
mod directory;
#[path = "../provision/global_indexes.rs"]
mod global_indexes;

use crate::backend::{
    CoordinatorProvisioner, InitialPartitionProvisioner, cell_error, mutation_identity,
};
use crate::{
    InstallPartition, InstallPartitionOutcome, Json, ListTables, ListTablesInput, PartitionInstall,
    PartitionSpec, SplitPlan, TableRecord, account_target, data_target,
};
use bdb_cellar_runtime::{
    app::CompiledApplication,
    client::{CellClient, InvocationError},
    identity::CellTarget,
};
use extenddb_storage::{BoxedFuture, error::StorageError};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapacityCursor {
    pub table_name: String,
    pub after_lower: Option<RangeKey>,
    pub index: Option<usize>,
}

pub struct CellInitialPartitionProvisioner {
    client: CellClient,
    application: Arc<CompiledApplication>,
    initial_partition_count: u16,
    coordinator_registrations: coordinator_registration::CoordinatorRegistrations,
}
impl CellInitialPartitionProvisioner {
    pub fn new(client: CellClient, application: Arc<CompiledApplication>) -> Self {
        Self {
            client,
            application,
            initial_partition_count: 4,
            coordinator_registrations: Default::default(),
        }
    }
    pub fn with_initial_partition_count(mut self, count: u16) -> Result<Self, StorageError> {
        if !(2..=256).contains(&count) {
            return Err(StorageError::Validation(
                "initial range count must be between 2 and 256".into(),
            ));
        }
        self.initial_partition_count = count;
        Ok(self)
    }
    // Installation commands activate through the SDK. This only checks compiled
    // topology before handing back the ingress client; it claims no owner proof.
    /// Validate the application's logical target before SDK admission.
    /// Ownership and transport authentication remain Cellar's responsibility;
    /// this check performs no I/O and grants no execution authority.
    pub fn validate_target(&self, target: &CellTarget) -> Result<(), StorageError> {
        let kind = self
            .application
            .cell_types()
            .iter()
            .find(|kind| kind.namespace() == target.namespace())
            .ok_or_else(|| StorageError::Validation("namespace is not compiled".into()))?;
        if target.application() != crate::APPLICATION || !kind.valid_partition(target.partition()) {
            return Err(StorageError::Validation(
                "invalid BeyondDB cell target".into(),
            ));
        }
        Ok(())
    }

    async fn provision_range(
        &self,
        target: &CellTarget,
        client: &CellClient,
    ) -> Result<CellClient, StorageError> {
        self.validate_target(target)?;
        crate::inventory::register(client, target).await?;
        Ok(client.clone())
    }
    async fn published_directory(
        &self,
        account_id: &str,
        spec: &crate::DirectorySpec,
    ) -> Result<(CellTarget, ()), StorageError> {
        let target = crate::directory_target(account_id, spec).map_err(provision_error)?;
        let state = self
            .client
            .query::<crate::ReadDirectory>(&target, None, Json(()))
            .await
            .map_err(cell_error)?
            .output
            .0;
        if state.as_ref().is_none_or(|state| state.spec != *spec) {
            return Err(StorageError::Transient(
                "published directory installation is missing or changed".into(),
            ));
        }
        Ok((target, ()))
    }
    pub async fn admit_account(&self, account_id: &str) -> Result<(), StorageError> {
        self.client
            .query::<ListTables>(
                &account_target(account_id).map_err(provision_error)?,
                None,
                Json(ListTablesInput {
                    limit: 1,
                    exclusive_start: None,
                    live_only: false,
                }),
            )
            .await
            .map_err(cell_error)?;
        Ok(())
    }
}
impl CoordinatorProvisioner for CellInitialPartitionProvisioner {
    fn ensure<'a>(
        &'a self,
        client: &'a CellClient,
        account_id: &'a str,
        routing_key: &'a [u8],
    ) -> BoxedFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let account = account_target(account_id).map_err(provision_error)?;
            let coordinator =
                crate::coordinator_target(account_id, routing_key).map_err(provision_error)?;
            let shard: [u8; 4] = coordinator
                .partition()
                .try_into()
                .map_err(|_| StorageError::Internal("invalid coordinator shard".into()))?;
            let shard = u32::from_be_bytes(shard);
            // Registrations are append-only. A current SDK read proves prior
            // durable publication without another mutation on the hot account.
            let registered = client
                .query::<crate::ReadCoordinatorRegistration>(
                    &account,
                    None,
                    Json(crate::RegisterCoordinatorShardInput {
                        account_id: account_id.into(),
                        shard,
                    }),
                )
                .await
                .map_err(cell_error)?
                .output
                .0;
            if registered {
                return Ok(());
            }
            // Persist discoverability before admission writes its BEGIN record.
            self.coordinator_registrations
                .register(client, &account, account_id, shard)
                .await
        })
    }
}

fn split_plan(source: &PartitionSpec, boundary: RangeKey) -> Result<SplitPlan, StorageError> {
    let next_epoch = source
        .epoch
        .checked_add(1)
        .ok_or_else(|| StorageError::LimitExceeded("partition epoch exhausted".into()))?;
    let fresh_id = |other: Option<[u8; 16]>| loop {
        let candidate = *uuid::Uuid::now_v7().as_bytes();
        if other != Some(candidate) && source.partition_id != candidate {
            break candidate;
        }
    };
    let left_id = fresh_id(None);
    let right_id = fresh_id(Some(left_id));
    let mut left = source.clone();
    left.partition_id = left_id;
    left.upper = Some(boundary.clone());
    left.epoch = next_epoch;
    let mut right = source.clone();
    right.partition_id = right_id;
    right.lower = Some(boundary);
    right.epoch = next_epoch;
    Ok(SplitPlan {
        source: source.clone(),
        children: [left, right],
    })
}

impl InitialPartitionProvisioner for CellInitialPartitionProvisioner {
    fn provision_route_directory<'a>(
        &'a self,
        client: &'a CellClient,
        account_id: &'a str,
        index_id: &'a str,
        ranges: Vec<crate::RoutePagePartition>,
    ) -> BoxedFuture<'a, Result<crate::DirectoryCopyReceipt, StorageError>> {
        Box::pin(async move {
            let spec = crate::DirectorySpec::root(index_id.into());
            let target = crate::directory_target(account_id, &spec).map_err(provision_error)?;
            let fingerprint =
                crate::directory::fingerprint(&spec, &ranges).map_err(provision_error)?;
            let client = self.provision_range(&target, client).await?;
            let installed = client
                .command::<crate::InstallDirectory>(
                    &target,
                    mutation_identity()?,
                    Json(crate::DirectoryInstall {
                        spec,
                        ranges,
                        source: None,
                    }),
                )
                .await
                .map_err(cell_error)?;
            if !installed.output.0 {
                return Err(StorageError::Transient(
                    "directory installation was rejected".into(),
                ));
            }
            Ok(crate::DirectoryCopyReceipt {
                cell_id: *target.cell_id().as_bytes(),
                sequence: installed.receipt.commit_sequence,
                fingerprint,
            })
        })
    }

    fn initial_partition_count(&self) -> u16 {
        self.initial_partition_count
    }

    fn provision_global_index<'a>(
        &'a self,
        client: &'a CellClient,
        account_id: &'a str,
        table: &'a TableRecord,
        index: &'a crate::GlobalIndexRecord,
    ) -> BoxedFuture<'a, Result<Vec<crate::GlobalIndexPartitionSpec>, StorageError>> {
        Box::pin(async move {
            let Some(initial_partitions) = table.placement.initial_partitions() else {
                return Err(StorageError::Validation(
                    "account-local table has no initial ranges".into(),
                ));
            };
            let mut partitions = Vec::with_capacity(usize::from(initial_partitions));
            for ordinal in 0..initial_partitions {
                let range = initial_partition(
                    table,
                    &index.specification.key_schema,
                    initial_partitions,
                    ordinal,
                )?;
                let spec = crate::GlobalIndexPartitionSpec {
                    table: table.clone(),
                    index: index.clone(),
                    partition_id: range.partition_id,
                    lower: range.lower,
                    upper: range.upper,
                    epoch: range.epoch,
                };
                let target = crate::global_index_target(account_id, &index.id, &spec.partition_id)
                    .map_err(provision_error)?;
                let client = self.provision_range(&target, client).await?;
                match client
                    .command::<crate::InstallGlobalIndexPartition>(
                        &target,
                        mutation_identity()?,
                        Json(spec.clone()),
                    )
                    .await
                {
                    Ok(result) if result.output.0 => partitions.push(spec),
                    Ok(_) | Err(InvocationError::Rejected(_)) => {
                        return Err(StorageError::Internal(
                            "global-index range installation rejected".into(),
                        ));
                    }
                    Err(error) => return Err(cell_error(error)),
                }
            }
            Ok(partitions)
        })
    }

    fn provision<'a>(
        &'a self,
        client: &'a CellClient,
        account_id: &'a str,
        table: &'a TableRecord,
    ) -> BoxedFuture<'a, Result<Vec<PartitionSpec>, StorageError>> {
        Box::pin(async move {
            let Some(initial_partitions) = table.placement.initial_partitions() else {
                return Err(StorageError::Validation(
                    "account-local table has no initial ranges".into(),
                ));
            };
            let mut partitions = Vec::with_capacity(usize::from(initial_partitions));
            for index in 0..initial_partitions {
                let spec = initial_partition(table, &table.key_schema, initial_partitions, index)?;
                let target = data_target(account_id, &table.id, &spec.partition_id)
                    .map_err(provision_error)?;
                let client = self.provision_range(&target, client).await?;
                match client
                    .command::<InstallPartition>(
                        &target,
                        mutation_identity()?,
                        Json(PartitionInstall::Serving(spec.clone())),
                    )
                    .await
                {
                    Ok(committed) if committed.output.0 == InstallPartitionOutcome::Installed => {
                        partitions.push(spec);
                    }
                    Ok(_) => {
                        return Err(StorageError::Internal(
                            "unexpected successful partition install".into(),
                        ));
                    }
                    Err(InvocationError::Rejected(committed)) => {
                        return Err(StorageError::Internal(format!(
                            "partition install rejected: {:?}",
                            committed.output.0
                        )));
                    }
                    Err(error) => return Err(cell_error(error)),
                }
            }
            Ok(partitions)
        })
    }
}

fn initial_partition(
    table: &TableRecord,
    schema: &[extenddb_core::types::KeySchemaElement],
    count: u16,
    index: u16,
) -> Result<PartitionSpec, StorageError> {
    let mut partition_id = [0_u8; 16];
    partition_id[15] = u8::try_from(index)
        .map_err(|_| StorageError::Internal("invalid initial partition index".into()))?;
    let boundary = |ordinal: u16| -> Result<RangeKey, StorageError> {
        use extenddb_core::types::{AttributeValue, KeyType, ScalarAttributeType};
        let pk = schema
            .iter()
            .find(|key| key.key_type == KeyType::Hash)
            .and_then(|key| {
                table
                    .attribute_definitions
                    .iter()
                    .find(|definition| definition.attribute_name == key.attribute_name)
            })
            .ok_or_else(|| StorageError::Validation("missing partition-key definition".into()))?;
        let prefix = u8::try_from((ordinal * 256) / count)
            .map_err(|_| StorageError::Validation("invalid bootstrap ordinal".into()))?;
        let value = match pk.attribute_type {
            ScalarAttributeType::S => AttributeValue::S(char::from(prefix).to_string()),
            ScalarAttributeType::B => AttributeValue::B(vec![prefix]),
            ScalarAttributeType::N => {
                AttributeValue::N((i32::from(ordinal) - i32::from(count / 2)).to_string())
            }
        };
        RangeKey::new(crate::partition::key::sort_component(&value).map_err(provision_error)?)
            .map_err(|_| StorageError::Validation("invalid bootstrap fence".into()))
    };
    Ok(PartitionSpec {
        table: table.clone(),
        partition_id,
        lower: (index > 0).then(|| boundary(index)).transpose()?,
        upper: (index + 1 < count)
            .then(|| boundary(index + 1))
            .transpose()?,
        epoch: 1,
    })
}

fn provision_error(error: bdb_cellar_runtime::Error) -> StorageError {
    match error {
        bdb_cellar_runtime::Error::Capacity(_) => StorageError::LimitExceeded(error.to_string()),
        _ => StorageError::Internal(error.to_string()),
    }
}
