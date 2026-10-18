use crate::native_peer::{self, FRONT, Http, Inspection, Observations, Peer, PeerPolicy, SOURCE};
use aws_sdk_dynamodb::types::AttributeValue as AwsAttributeValue;
use bdb_cellar_runtime::{app::CompiledApplication, client::CellClient, identity::CellTarget};
use beyonddb::*;
use cellar::bucket::Bucket;
use extenddb_core::types::{AttributeValue, Item};
use extenddb_storage::TableEngine;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, atomic::Ordering},
};

pub const ACCOUNT: &str = "123456789012";
const ACCESS: &str = "AKIAIOSFODNN7EXAMPLE";
pub type SdkItem = HashMap<String, AwsAttributeValue>;

pub struct Fixture {
    files: tempfile::TempDir,
    application: Arc<CompiledApplication>,
    bucket: Bucket,
    observed: Observations,
    source: Option<Peer>,
    front: Peer,
    name: String,
    generation: usize,
    resident: usize,
    http: Option<Http>,
    inspection: Inspection,
    host: tokio::runtime::Handle,
    core: tokio::runtime::Handle,
    pub sdk: aws_sdk_dynamodb::Client,
    pub client: CellClient,
    pub table: TableRecord,
    pub data: Vec<(CellTarget, SdkItem)>,
}
impl Fixture {
    pub async fn new(
        host: &tokio::runtime::Handle,
        core: &tokio::runtime::Handle,
        rows: usize,
    ) -> Self {
        Self::with_limits(host, core, rows, 16, 8).await
    }

    pub async fn with_limits(
        host: &tokio::runtime::Handle,
        core: &tokio::runtime::Handle,
        rows: usize,
        source_resident: usize,
        front_resident: usize,
    ) -> Self {
        Self::with_coordinators(host, core, rows, source_resident, front_resident, &[]).await
    }

    pub async fn with_coordinators(
        host: &tokio::runtime::Handle,
        core: &tokio::runtime::Handle,
        rows: usize,
        source_resident: usize,
        front_resident: usize,
        tokens: &[String],
    ) -> Self {
        assert!((1..=2).contains(&rows));
        let files = tempfile::tempdir().unwrap();
        let application = native_peer::application();
        let bucket = Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let observed = Arc::new(Mutex::new(Default::default()));
        let source = Peer::start(
            application.clone(),
            &files.path().join(SOURCE),
            bucket.clone(),
            observed.clone(),
            PeerPolicy {
                max_resident: source_resident,
                ..true.into()
            },
            host,
            core,
        )
        .await;
        native_peer::put_credential(&source.client, ACCESS, ACCOUNT).await;
        CellAuthorizationStore::new(source.client.clone())
            .put_user_policy(
                ACCOUNT,
                "network-user",
                "tables",
                &serde_json::json!({
                    "Version":"2012-10-17", "Statement":[{"Effect":"Allow","Action":"dynamodb:*",
                        "Resource":"arn:aws:dynamodb:us-east-1:123456789012:table/Residency*"}]
                })
                .to_string(),
            )
            .await
            .unwrap();
        let storage = CellStorage::new(source.client.clone(), "us-east-1")
            .with_initial_partitions(native_peer::provisioner(&source.client));
        storage
            .create_table(
                ACCOUNT,
                serde_json::from_value(serde_json::json!({
                    "TableName":"Residency", "BillingMode":"PAY_PER_REQUEST",
                    "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                    "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        let account = account_target(ACCOUNT).unwrap();
        let table = source
            .client
            .query::<DescribeTable>(&account, None, Json("Residency".into()))
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        let route = crate::single_leaf_route(&source.client, &account, &table.id)
            .await
            .unwrap();
        assert_eq!(route.partitions.len(), 2);
        // Activate warm coordinators through the SDK while this is the only
        // node. The source's resident budget, rather than a client peer's smaller
        // budget, matches the original coalescing workload. No owner is edited.
        let admission = crate::native_peer::provisioner(&source.client);
        for token in tokens {
            admission
                .ensure(&source.client, ACCOUNT, token.as_bytes())
                .await
                .unwrap();
            let coordinator = coordinator_target(ACCOUNT, token.as_bytes()).unwrap();
            source
                .client
                .query::<ReadPendingCrossCellTransactions>(
                    &coordinator,
                    None,
                    Json(ReadPendingCrossCellTransactionsInput {
                        after: None,
                        limit: 1,
                    }),
                )
                .await
                .unwrap();
            assert!(native_peer::identity(&observed, SOURCE, &coordinator).is_some());
        }
        let front = Peer::start(
            application.clone(),
            &files.path().join(FRONT),
            bucket.clone(),
            observed.clone(),
            PeerPolicy {
                max_resident: front_resident,
                ..true.into()
            },
            host,
            core,
        )
        .await;
        let inspection = Inspection::new(bucket.clone(), host);
        inspection.ready_peers().await;
        let sockets = source.network.accepted.load(Ordering::SeqCst);
        let http = Http::start(&front.client, ACCESS).await;
        let sdk = http.sdk.clone();
        let mut data = Vec::new();
        for id in ["first", "Ω-other"].into_iter().take(rows) {
            let item = SdkItem::from([
                ("id".into(), AwsAttributeValue::S(id.into())),
                ("value".into(), AwsAttributeValue::S("committed".into())),
            ]);
            sdk.put_item()
                .table_name("Residency")
                .set_item(Some(item.clone()))
                .send()
                .await
                .unwrap();
            let key = ordered_data_key(
                &table.id,
                &Item::from([("id".into(), AttributeValue::S(id.into()))]),
                &table.key_schema,
            )
            .unwrap();
            let range = route
                .partitions
                .iter()
                .find(|range| {
                    range.lower.as_ref().is_none_or(|lower| lower <= &key)
                        && range.upper.as_ref().is_none_or(|upper| &key < upper)
                })
                .unwrap();
            let target = data_target(ACCOUNT, &table.id, &range.partition_id).unwrap();
            assert!(native_peer::identity(&observed, SOURCE, &target).is_some());
            assert!(native_peer::identity(&observed, FRONT, &target).is_none());
            data.push((target, item));
        }
        assert!(source.network.accepted.load(Ordering::SeqCst) > sockets);
        if rows == 2 {
            assert_ne!(data[0].0, data[1].0);
        }
        let client = front.client.clone();
        Self {
            files,
            application,
            bucket,
            observed,
            source: Some(source),
            front,
            name: FRONT.into(),
            generation: 0,
            resident: front_resident,
            http: Some(http),
            inspection,
            host: host.clone(),
            core: core.clone(),
            sdk,
            client,
            table,
            data,
        }
    }

    pub async fn replace(&mut self) {
        let mut before = Vec::new();
        for (target, _) in &self.data {
            let initial = native_peer::identity(&self.observed, &self.name, target)
                .or_else(|| native_peer::identity(&self.observed, SOURCE, target))
                .unwrap();
            let owner = self.inspection.owner(&initial.scope).await;
            let actual =
                native_peer::identity(&self.observed, owner.node.as_deref().unwrap(), target)
                    .unwrap();
            assert_eq!(actual.epoch, owner.epoch);
            before.push((target.clone(), actual));
        }
        self.http.take().unwrap().stop().await;
        self.front.stop().await;
        let mut departed = vec![self.name.clone()];
        if let Some(mut source) = self.source.take() {
            source.stop().await;
            departed.push(SOURCE.into());
        }
        for name in &departed {
            std::fs::remove_dir_all(self.files.path().join(name)).unwrap();
        }
        self.generation += 1;
        self.name = format!("images-owner-{}", self.generation);
        self.front = Peer::start(
            self.application.clone(),
            &self.files.path().join(&self.name),
            self.bucket.clone(),
            self.observed.clone(),
            PeerPolicy {
                max_resident: self.resident,
                ..true.into()
            },
            &self.host,
            &self.core,
        )
        .await;
        for name in departed {
            self.inspection.departed_node(&name, &self.name).await;
        }
        self.client = self.front.client.clone();
        for (target, old) in before {
            let state = self
                .client
                .query::<ReadPartitionState>(&target, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap();
            assert_eq!(state.spec.table.id, self.table.id);
            let actual = native_peer::identity(&self.observed, &self.name, &target).unwrap();
            assert_eq!(actual.scope, old.scope);
            assert!(actual.epoch > old.epoch);
            let owner = self.inspection.owner(&actual.scope).await;
            assert_eq!(owner.node.as_deref(), Some(self.name.as_str()));
            assert_eq!(owner.epoch, actual.epoch);
        }
        let http = Http::start(&self.client, ACCESS).await;
        self.sdk = http.sdk.clone();
        self.http = Some(http);
    }

    // Gateway instrumentation is used by the coalescing target; images use
    // the undecorated client and the same native host/ownership fixture.
    #[allow(dead_code)]
    pub async fn use_client(&mut self, client: CellClient) {
        self.http.take().unwrap().stop().await;
        self.client = client;
        let http = Http::start(&self.client, ACCESS).await;
        self.sdk = http.sdk.clone();
        self.http = Some(http);
    }

    pub async fn shutdown(mut self) {
        self.http.take().unwrap().stop().await;
        self.front.stop().await;
        if let Some(mut source) = self.source.take() {
            source.stop().await;
        }
        self.inspection.close().await;
    }
}
pub fn runtimes() -> (tokio::runtime::Runtime, tokio::runtime::Runtime) {
    let make = |threads| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()
            .unwrap()
    };
    (make(2), make(1))
}
