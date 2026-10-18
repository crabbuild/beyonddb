//! Domain contracts across native SDK owner replacement.
use crate::native_peer::{self, Http, Inspection, Observations, Peer, PeerPolicy};
use aws_sdk_dynamodb::types::AttributeValue as AwsAttributeValue;
use beyonddb::*;
use cellar::bucket::Bucket;
use cellar_runtime::{app::CompiledApplication, client::CellClient};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub const ACCOUNT: &str = "123456789012";
const ACCESS: &str = "AKIAIOSFODNN7EXAMPLE";
pub type SdkItem = HashMap<String, AwsAttributeValue>;

pub struct Fixture {
    files: tempfile::TempDir,
    application: Arc<CompiledApplication>,
    bucket: Bucket,
    observed: Observations,
    host: tokio::runtime::Handle,
    core: tokio::runtime::Handle,
    generation: usize,
    max_resident: usize,
    node: Peer,
    http: Option<Http>,
    inspection: Inspection,
    pub client: CellClient,
    pub provisioner: Arc<CellInitialPartitionProvisioner>,
    pub sdk: aws_sdk_dynamodb::Client,
    pub data: Vec<(cellar_runtime::identity::CellTarget, SdkItem)>,
}

impl Fixture {
    pub async fn new(
        host: &tokio::runtime::Handle,
        core: &tokio::runtime::Handle,
        seed_rows: usize,
        max_resident: usize,
    ) -> Self {
        Self::with_bucket(host, core, seed_rows, max_resident, std::convert::identity).await
    }

    /// Inject an ordinary provider adapter without editing SDK authority state.
    pub async fn with_bucket(
        host: &tokio::runtime::Handle,
        core: &tokio::runtime::Handle,
        seed_rows: usize,
        max_resident: usize,
        wrap: impl FnOnce(Bucket) -> Bucket,
    ) -> Self {
        assert!((1..=2).contains(&seed_rows));
        let files = tempfile::tempdir().unwrap();
        let application = native_peer::application();
        let bucket = wrap(Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap());
        let observed = Arc::new(Mutex::new(Default::default()));
        let node = Peer::start(
            application.clone(),
            &files.path().join("residency-owner-0"),
            bucket.clone(),
            observed.clone(),
            PeerPolicy {
                max_resident,
                ..true.into()
            },
            host,
            core,
        )
        .await;
        let client = node.client.clone();
        let provisioner = native_peer::provisioner(&client);
        native_peer::put_credential(&client, ACCESS, ACCOUNT).await;
        CellAuthorizationStore::new(client.clone())
            .put_user_policy(
                ACCOUNT,
                "network-user",
                "tables",
                &serde_json::json!({
                    "Version": "2012-10-17", "Statement": [{"Effect":"Allow", "Action":"dynamodb:*",
                        "Resource":"arn:aws:dynamodb:us-east-1:123456789012:table/Residency*"}]
                })
                .to_string(),
            )
            .await
            .unwrap();
        let http = Http::start(&client, ACCESS).await;
        let sdk = http.sdk.clone();
        sdk.create_table()
            .table_name("Residency")
            .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
            .key_schema(
                aws_sdk_dynamodb::types::KeySchemaElement::builder()
                    .attribute_name("id")
                    .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                aws_sdk_dynamodb::types::AttributeDefinition::builder()
                    .attribute_name("id")
                    .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
        let account = account_target(ACCOUNT).unwrap();
        let table = client
            .query::<DescribeTable>(&account, None, Json("Residency".into()))
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        let route = crate::single_leaf_route(&client, &account, &table.id)
            .await
            .unwrap();
        assert_eq!(
            route.partitions.len(),
            2,
            "public tables always span multiple ordered cells"
        );
        let mut data = Vec::new();
        for id in ["first", "Ω-other"].into_iter().take(seed_rows) {
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
                &serde_json::from_value(serde_json::json!({"id":{"S":id}})).unwrap(),
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
            data.push((
                data_target(ACCOUNT, &table.id, &range.partition_id).unwrap(),
                item,
            ));
        }
        if seed_rows == 2 {
            assert_ne!(data[0].0, data[1].0);
        }
        let inspection = Inspection::new(bucket.clone(), host);
        Self {
            files,
            application,
            bucket,
            observed,
            host: host.clone(),
            core: core.clone(),
            generation: 0,
            max_resident,
            node,
            http: Some(http),
            inspection,
            client,
            provisioner,
            sdk,
            data,
        }
    }

    /// Delete the former owner's local files and recover the account from the
    /// bucket only after the SDK observes lease and placement-sample departure.
    pub async fn replace(&mut self) {
        let old_name = format!("residency-owner-{}", self.generation);
        let account = account_target(ACCOUNT).unwrap();
        let old = native_peer::identity(&self.observed, &old_name, &account).unwrap();
        let old_owner = self.inspection.owner(&old.scope).await;
        assert_eq!(old_owner.node.as_deref(), Some(old_name.as_str()));
        self.http.take().unwrap().stop().await;
        self.node.stop().await;
        std::fs::remove_dir_all(self.files.path().join(&old_name)).unwrap();
        self.generation += 1;
        let next = format!("residency-owner-{}", self.generation);
        self.node = Peer::start(
            self.application.clone(),
            &self.files.path().join(&next),
            self.bucket.clone(),
            self.observed.clone(),
            PeerPolicy {
                max_resident: self.max_resident,
                ..true.into()
            },
            &self.host,
            &self.core,
        )
        .await;
        self.inspection.departed_node(&old_name, &next).await;
        self.client = self.node.client.clone();
        self.provisioner = native_peer::provisioner(&self.client);
        self.client
            .query::<ListTables>(
                &account,
                None,
                Json(ListTablesInput {
                    limit: 1,
                    exclusive_start: None,
                    live_only: false,
                }),
            )
            .await
            .unwrap();
        let actual = native_peer::identity(&self.observed, &next, &account).unwrap();
        assert_eq!(actual.scope, old.scope);
        assert!(actual.epoch > old_owner.epoch);
        let owner = self.inspection.owner(&actual.scope).await;
        assert_eq!(owner.node.as_deref(), Some(next.as_str()));
        assert_eq!(owner.epoch, actual.epoch);
        let http = Http::start(&self.client, ACCESS).await;
        self.sdk = http.sdk.clone();
        self.http = Some(http);
    }

    /// Placement tests change creation policy without changing existing plans.
    #[allow(dead_code)] // The metadata target uses the default two-range policy.
    pub async fn set_initial_partition_count(&mut self, count: u16) {
        self.http.take().unwrap().stop().await;
        self.provisioner = Arc::new(
            CellInitialPartitionProvisioner::new(self.client.clone(), self.application.clone())
                .with_initial_partition_count(count)
                .unwrap(),
        );
        let http = Http::with_provisioner(&self.client, ACCESS, self.provisioner.clone()).await;
        self.sdk = http.sdk.clone();
        self.http = Some(http);
    }

    /// Observe the actual SDK factory identity and compare its ownership row.
    #[allow(dead_code)] // Placement audits additional data and index scopes.
    pub async fn cell_identity(
        &self,
        target: &cellar_runtime::identity::CellTarget,
    ) -> cellar::sdk::CellIdentity {
        let name = format!("residency-owner-{}", self.generation);
        let identity = native_peer::identity(&self.observed, &name, target).unwrap();
        let owner = self.inspection.owner(&identity.scope).await;
        assert_eq!(owner.node.as_deref(), Some(name.as_str()));
        assert_eq!(owner.epoch, identity.epoch);
        identity
    }

    pub async fn shutdown(mut self) {
        self.http.take().unwrap().stop().await;
        self.node.stop().await;
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
