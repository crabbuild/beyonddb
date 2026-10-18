use super::global_indexes::{ACCOUNT, contains, mutation, owner, ranges, runtimes};
use crate::native_peer::{self, FRONT, Inspection, Observations, Peer, SOURCE};
use beyonddb::*;
use cellar_runtime::{
    app::CompiledApplication,
    client::{CellClient, InvocationError},
    identity::CellTarget,
};
use extenddb_core::{
    expression::ExpressionMaps,
    types::{AttributeValue, Item, ScalarAttributeType},
};
use extenddb_storage::{DataEngine, TableEngine};
use std::sync::{Arc, Mutex};

struct Fixture {
    _files: tempfile::TempDir,
    host: Peer,
    application: Arc<CompiledApplication>,
    bucket: cellar::bucket::Bucket,
    observed: Observations,
    provisioner: Arc<CellInitialPartitionProvisioner>,
    client: CellClient,
    spec: PartitionSpec,
    target: CellTarget,
    policy: PartitionIndexPolicy,
    items: Vec<Item>,
}

impl Fixture {
    async fn new(host_executor: &tokio::runtime::Handle, core: &tokio::runtime::Handle) -> Self {
        let application = native_peer::application();
        let files = tempfile::tempdir().unwrap();
        let bucket =
            cellar::bucket::Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let observed = Arc::new(Mutex::new(Default::default()));
        let (host, provisioner, client, storage) = owner(
            application.clone(),
            bucket.clone(),
            &files.path().join(SOURCE),
            observed.clone(),
            host_executor,
            core,
        )
        .await;
        provisioner.admit_account(ACCOUNT).await.unwrap();
        storage
            .create_table(
                ACCOUNT,
                serde_json::from_value(serde_json::json!({
                    "TableName":"OnlinePolicy", "BillingMode":"PAY_PER_REQUEST",
                    "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                    "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        let info = storage
            .table_key_info(ACCOUNT, "OnlinePolicy")
            .await
            .unwrap();
        let routes = ranges(&client, &info.table_id).await;
        assert_eq!(routes.len(), 2);
        let target = data_target(ACCOUNT, &info.table_id, &routes[0].partition_id).unwrap();
        let spec = client
            .query::<ReadPartitionState>(&target, None, Json(()))
            .await
            .unwrap()
            .output
            .0
            .unwrap()
            .spec;
        let target = data_target(ACCOUNT, &info.table_id, &spec.partition_id).unwrap();
        let items = (0..1000)
            .map(|i| {
                Item::from([
                    ("id".into(), AttributeValue::S(format!("item-{i:04}"))),
                    ("value".into(), AttributeValue::S("before".into())),
                ])
            })
            .filter(|item| {
                let key = ordered_data_key(&spec.table.id, item, &spec.table.key_schema).unwrap();
                contains(&routes[0], &key)
            })
            .take(24)
            .collect::<Vec<_>>();
        assert_eq!(items.len(), 24);
        for item in &items {
            storage
                .put_item(
                    &info,
                    item.clone(),
                    false,
                    None,
                    &ExpressionMaps::default(),
                    None,
                )
                .await
                .unwrap();
        }
        let policy = PartitionIndexPolicy {
            revision: 1,
            attribute_definitions: serde_json::from_value(serde_json::json!([
                {"AttributeName":"id","AttributeType":"S"},
                {"AttributeName":"value","AttributeType":"S"}
            ]))
            .unwrap(),
            indexes: vec![GlobalIndexRecord {
                id: blake3::hash(b"online-policy-index").to_hex().to_string(),
                specification: serde_json::from_value(serde_json::json!({
                    "IndexName":"ByValue", "KeySchema":[{"AttributeName":"value","KeyType":"HASH"}],
                    "Projection":{"ProjectionType":"ALL"}
                }))
                .unwrap(),
            }],
        };
        Self {
            _files: files,
            host,
            application,
            bucket,
            observed,
            provisioner,
            client,
            spec,
            target,
            policy,
            items,
        }
    }

    async fn replace_owner(
        &mut self,
        host_executor: &tokio::runtime::Handle,
        core: &tokio::runtime::Handle,
    ) {
        let inspection = Inspection::new(self.bucket.clone(), host_executor);
        let previous = native_peer::identity(&self.observed, SOURCE, &self.target).unwrap();
        let old_owner = inspection.owner(&previous.scope).await;
        self.host.stop().await;
        std::fs::remove_dir_all(self._files.path().join(SOURCE)).unwrap();
        let (host, provisioner, client, _) = owner(
            self.application.clone(),
            self.bucket.clone(),
            &self._files.path().join(FRONT),
            self.observed.clone(),
            host_executor,
            core,
        )
        .await;
        self.host = host;
        self.provisioner = provisioner;
        self.client = client;
        inspection.departed().await;
        // Activate through the real SDK, then observe its actual identity and
        // immutable installation. No old local file or authority edits exist.
        assert_eq!(
            self.client
                .query::<ReadPartitionState>(&self.target, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap()
                .spec,
            self.spec
        );
        let current = native_peer::identity(&self.observed, FRONT, &self.target).unwrap();
        assert_eq!(current.scope, previous.scope);
        let owner = inspection.owner(&current.scope).await;
        assert_eq!(owner.node.as_deref(), Some(FRONT));
        assert!(owner.epoch > old_owner.epoch);
        assert_eq!(owner.epoch, current.epoch);
        inspection.close().await;
    }

    fn configuration(&self) -> ConfigurePartitionIndexesInput {
        ConfigurePartitionIndexesInput {
            table_id: self.spec.table.id.clone(),
            epoch: self.spec.epoch,
            policy: self.policy.clone(),
        }
    }

    async fn configure(&self) {
        assert_eq!(
            self.client
                .command::<ConfigurePartitionIndexes>(
                    &self.target,
                    mutation(),
                    Json(self.configuration())
                )
                .await
                .unwrap()
                .output
                .0,
            ConfigurePartitionIndexesOutcome::Configured
        );
    }

    async fn backfill(&self, target: &CellTarget, epoch: u64) -> BackfillPartitionIndexOutcome {
        self.client
            .command::<BackfillPartitionIndex>(
                target,
                mutation(),
                Json(BackfillPartitionIndexInput {
                    table_id: self.spec.table.id.clone(),
                    epoch,
                    revision: self.policy.revision,
                    index_id: self.policy.indexes[0].id.clone(),
                }),
            )
            .await
            .unwrap()
            .output
            .0
    }

    async fn put(&self, item: Item) -> Result<(), InvocationError<Json<PartitionPutOutcome>>> {
        self.client
            .command::<PartitionPut>(
                &self.target,
                mutation(),
                Json(PartitionPutInput {
                    table_id: self.spec.table.id.clone(),
                    epoch: self.spec.epoch,
                    item,
                    condition: None,
                }),
            )
            .await
            .map(|_| ())
    }

    async fn next_change(&self, target: &CellTarget) -> Option<([u8; 32], serde_json::Value)> {
        let header = self
            .client
            .query::<ReadPartitionIndexChange>(target, None, Json(self.spec.table.id.clone()))
            .await
            .unwrap()
            .output
            .0?;
        let bytes = self
            .client
            .query::<ReadPartitionIndexChangeChunk>(
                target,
                None,
                Json(IndexChangeChunk {
                    id: header.id,
                    offset: 0,
                }),
            )
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        assert_eq!(bytes.len(), header.bytes as usize);
        Some((header.id, serde_json::from_slice(&bytes).unwrap()))
    }

    async fn ack(&self, target: &CellTarget, delivery: IndexChangeDelivery) {
        self.client
            .command::<RecordPartitionIndexDelivery>(target, mutation(), Json(delivery))
            .await
            .unwrap();
    }
}

#[test]
fn backfill_policy_preserves_prepare_reservations_and_resumes_with_a_delivery_barrier() {
    let (host, core) = runtimes();
    host.block_on(async {
    let mut fixture = Fixture::new(host.handle(), core.handle()).await;
    let transaction_id = [144; 16];
    let coordinator_cell = *account_target(ACCOUNT).unwrap().cell_id().as_bytes();
    transaction_command!(
        fixture.client,
        PreparePartitionTransaction,
        &fixture.target,
        mutation(),
        Json(PreparePartitionTransactionInput {
            table_id: fixture.spec.table.id.clone(),
            epoch: fixture.spec.epoch,
            transaction_id,
            coordinator_cell,
            coordinator_key: transaction_id.to_vec(),
            operations: vec![TransactionOperation::Put(PutItemInput {
                table_name: fixture.spec.table.table_name.clone(),
                table_id: fixture.spec.table.id.clone(),
                item: fixture.items[0].clone(),
                condition: None,
            })],
        })
    )
    .await
    .unwrap();
    assert!(
        matches!(fixture.client.command::<ConfigurePartitionIndexes>(&fixture.target, mutation(), Json(fixture.configuration())).await,
        Err(InvocationError::Rejected(result)) if result.output.0 == ConfigurePartitionIndexesOutcome::InFlightTransaction)
    );
    fixture
        .client
        .command::<ResolvePartitionTransaction>(
            &fixture.target,
            mutation(),
            Json(ResolveTransactionInput {
                transaction_id,
                coordinator_cell,
                commit: true,
            }),
        )
        .await
        .unwrap();
    let mut invalid = fixture.items[23].clone();
    invalid.insert("value".into(), AttributeValue::Bool(true));
    fixture.put(invalid.clone()).await.unwrap();
    fixture.configure().await;
    let mut changed_type = fixture.configuration();
    changed_type.policy.revision += 1;
    changed_type.policy.attribute_definitions[1].attribute_type = ScalarAttributeType::N;
    assert!(
        matches!(fixture.client.command::<ConfigurePartitionIndexes>(
        &fixture.target, mutation(), Json(changed_type),
    ).await, Err(InvocationError::Rejected(result)) if result.output.0 == ConfigurePartitionIndexesOutcome::InvalidPolicy)
    );
    assert!(
        matches!(fixture.put(invalid).await, Err(InvocationError::Rejected(result))
        if result.output.0 == PartitionPutOutcome::InvalidItem)
    );
    assert_eq!(
        fixture.backfill(&fixture.target, fixture.spec.epoch).await,
        BackfillPartitionIndexOutcome::Progress
    );
    let mut changed = fixture.items[0].clone();
    changed.insert("value".into(), AttributeValue::S("after".into()));
    fixture.put(changed).await.unwrap();
    let before = fixture
        .client
        .query::<ReadPartitionIndexes>(&fixture.target, None, Json(()))
        .await
        .unwrap()
        .output
        .0
        .unwrap();
    fixture.replace_owner(host.handle(), core.handle()).await;
    assert_eq!(
        fixture
            .client
            .query::<ReadPartitionIndexes>(&fixture.target, None, Json(()))
            .await
            .unwrap()
            .output
            .0,
        Some(before)
    );
    fixture.configure().await;
    assert_eq!(
        fixture.backfill(&fixture.target, fixture.spec.epoch).await,
        BackfillPartitionIndexOutcome::Scanned
    );
    assert_eq!(
        fixture
            .client
            .query::<ReadPartitionState>(&fixture.target, None, Json(()))
            .await
            .unwrap()
            .output
            .0
            .unwrap()
            .spec,
        fixture.spec
    );
    let mut deferred = None;
    let mut historical = Vec::new();
    while let Some((id, change)) = fixture.next_change(&fixture.target).await {
        if Some(id) == deferred {
            break;
        }
        if change["old"].is_null() {
            historical.push(change["new"]["id"].clone());
            fixture
                .ack(&fixture.target, IndexChangeDelivery::Applied(id))
                .await;
        } else {
            deferred = Some(id);
            fixture
                .ack(&fixture.target, IndexChangeDelivery::Deferred(id))
                .await;
        }
    }
    assert_eq!(
        historical.len(),
        23,
        "invalid historical keys must be skipped"
    );
    assert!(deferred.is_some(), "ordinary write must retain its journal");
    assert!(
        fixture
            .client
            .query::<ReadPartitionIndexes>(&fixture.target, None, Json(()))
            .await
            .unwrap()
            .output
            .0
            .unwrap()
            .pending_backfills
            .is_empty(),
        "ordinary pending work must not extend the historical delivery barrier"
    );
    fixture.host.stop().await;
    });
}

#[test]
fn split_inherits_unfinished_backfill_and_mutable_policy_without_changing_installation() {
    let (host, core) = runtimes();
    host.block_on(async {
        let mut fixture = Fixture::new(host.handle(), core.handle()).await;
        fixture.configure().await;
        // No scan has started. Splitting must preserve the obligation to backfill
        // every child even though the source has no journal entry to drain yet.
        let plan = fixture
            .provisioner
            .split_partition(
                ACCOUNT,
                fixture.client.clone(),
                &fixture.spec.table.id,
                fixture.spec.partition_id,
                fixture
                    .spec
                    .lower
                    .clone()
                    .unwrap_or_else(keyspace::RangeKey::minimum),
            )
            .await
            .unwrap();
        let mut historical = Vec::new();
        for child in &plan.children {
            let target = data_target(ACCOUNT, &child.table.id, &child.partition_id).unwrap();
            let state = fixture
                .client
                .query::<ReadPartitionIndexes>(&target, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap();
            assert_eq!(state.policy, fixture.policy);
            assert_eq!(
                state.pending_backfills,
                vec![fixture.policy.indexes[0].id.clone()]
            );
            loop {
                match fixture.backfill(&target, child.epoch).await {
                    BackfillPartitionIndexOutcome::Progress => {}
                    BackfillPartitionIndexOutcome::Scanned => break,
                    outcome => panic!("unexpected backfill outcome: {outcome:?}"),
                }
            }
            while let Some((id, change)) = fixture.next_change(&target).await {
                historical.push(change["new"]["id"].clone());
                assert_eq!(change["version"]["source_epoch"], child.epoch);
                fixture.ack(&target, IndexChangeDelivery::Applied(id)).await;
            }
            let mut removed = fixture.policy.clone();
            removed.revision += 1;
            removed.indexes.clear();
            removed.attribute_definitions = child.table.attribute_definitions.clone();
            fixture
                .client
                .command::<ConfigurePartitionIndexes>(
                    &target,
                    mutation(),
                    Json(ConfigurePartitionIndexesInput {
                        table_id: child.table.id.clone(),
                        epoch: child.epoch,
                        policy: removed,
                    }),
                )
                .await
                .unwrap();
            let item = fixture
                .items
                .iter()
                .find(|item| {
                    let key =
                        ordered_data_key(&child.table.id, item, &child.table.key_schema).unwrap();
                    child.lower.as_ref().is_none_or(|low| low <= &key)
                        && child.upper.as_ref().is_none_or(|high| &key < high)
                })
                .unwrap();
            let mut item = item.clone();
            item.insert("value".into(), AttributeValue::N("42".into()));
            fixture
                .client
                .command::<PartitionPut>(
                    &target,
                    mutation(),
                    Json(PartitionPutInput {
                        table_id: child.table.id.clone(),
                        epoch: child.epoch,
                        item,
                        condition: None,
                    }),
                )
                .await
                .unwrap();
        }
        historical.sort_by_key(serde_json::Value::to_string);
        let mut expected = fixture
            .items
            .iter()
            .map(|item| serde_json::to_value(&item["id"]).unwrap())
            .collect::<Vec<_>>();
        expected.sort_by_key(serde_json::Value::to_string);
        assert_eq!(historical, expected);
        fixture.host.stop().await;
    });
}
