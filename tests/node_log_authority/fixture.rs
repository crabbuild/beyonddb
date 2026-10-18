use super::*;
use aws_sdk_dynamodb::types::AttributeValue;
use bdb_cellar_runtime::application::cell_name;
use cellar::bucket::Bucket;
use native_peer::*;
use serde_json::Value;
use store::FaultStore;

pub const ACCESS: &str = "AKIANATIVELOGEXAMPLE";
pub const ACCOUNT: &str = "123456789012";

#[derive(Debug)]
pub struct Lease {
    pub body: Value,
    pub etag: String,
}
impl Lease {
    pub fn expires(&self) -> u64 {
        self.body["expires_ms"].as_u64().unwrap()
    }
    pub fn log(&self) -> &Value {
        &self.body["log"]
    }
    pub fn epoch(&self) -> u64 {
        self.log()["epoch"].as_u64().unwrap()
    }
}
pub struct Fixture {
    _files: tempfile::TempDir,
    pub source: Peer,
    pub front: Peer,
    pub http: Http,
    pub inspection: Inspection,
    pub ordinary: Arc<FaultStore>,
    pub safety: Arc<FaultStore>,
    pub bucket: Bucket,
    pub observed: Observations,
}
impl Fixture {
    pub async fn start(host: &tokio::runtime::Handle, core: &tokio::runtime::Handle) -> Self {
        let files = tempfile::tempdir().unwrap();
        let bucket = Bucket::open_dev(&files.path().join("bucket.sqlite")).unwrap();
        let ordinary = Arc::new(FaultStore::new(bucket.store.clone(), SOURCE));
        let safety = Arc::new(FaultStore::new(bucket.cas_store.clone(), SOURCE));
        let mut application_bucket = bucket.clone();
        application_bucket.store = ordinary.clone();
        application_bucket.cas_store = safety.clone();
        let mut lease_bucket = bucket.clone();
        lease_bucket.store = safety.clone();
        lease_bucket.cas_store = safety.clone();
        let observed = Arc::new(Mutex::new(Default::default()));
        let source = Peer::start(
            application(),
            &files.path().join(SOURCE),
            application_bucket,
            observed.clone(),
            PeerPolicy {
                fleet_durability: true,
                lease_bucket: Some(lease_bucket),
                ..true.into()
            },
            host,
            core,
        )
        .await;
        put_credential(&source.client, ACCESS, ACCOUNT).await;
        policy(&source.client, ACCOUNT).await;
        table(&source.client, ACCOUNT, "NetworkData", false).await;
        let front = Peer::start(
            application(),
            &files.path().join(FRONT),
            bucket.clone(),
            observed.clone(),
            PeerPolicy {
                fleet_durability: true,
                ..true.into()
            },
            host,
            core,
        )
        .await;
        let inspection = Inspection::new(bucket.clone(), host);
        inspection.ready_peers().await;
        let http = Http::start(&source.client, ACCESS).await;
        let fixture = Self {
            _files: files,
            source,
            front,
            http,
            inspection,
            ordinary,
            safety,
            bucket,
            observed,
        };
        fixture
            .wait_lease(SOURCE, |lease| {
                lease.log()["state"] == "open"
                    && lease.log()["ensemble"]
                        .as_array()
                        .is_some_and(|members| members == &[Value::from(FRONT)])
            })
            .await;
        fixture.write("warmup").await;
        fixture
            .wait_lease(SOURCE, |lease| lease.log()["may_have_fleet_acks"] == true)
            .await;
        fixture
    }
    pub async fn lease(&self, node: &str) -> Lease {
        self.inspection
            .domain
            .run(async {
                let (body, etag) = self
                    .bucket
                    .get(&format!("nodes/{node}.json"))
                    .await
                    .unwrap()
                    .unwrap();
                Lease {
                    body: serde_json::from_slice(&body).unwrap(),
                    etag,
                }
            })
            .await
            .unwrap()
    }
    pub async fn wait_lease(&self, node: &str, condition: impl Fn(&Lease) -> bool) -> Lease {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(40);
        loop {
            let lease = self.lease(node).await;
            if condition(&lease) {
                return lease;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "native lease condition timed out for {node}: {lease:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn write(&self, value: &str) {
        self.http
            .sdk
            .put_item()
            .table_name("NetworkData")
            .item("id", AttributeValue::S("first".into()))
            .item("value", AttributeValue::S(value.into()))
            .send()
            .await
            .unwrap();
        self.assert_value(value).await;
    }
    pub async fn assert_value(&self, value: &str) {
        let result = self
            .http
            .sdk
            .get_item()
            .table_name("NetworkData")
            .key("id", AttributeValue::S("first".into()))
            .consistent_read(true)
            .send()
            .await
            .unwrap();
        assert_eq!(
            result.item().unwrap()["value"],
            AttributeValue::S(value.into())
        );
    }
    pub fn assert_source_owned(&self) {
        let target = beyonddb::account_target(ACCOUNT).unwrap();
        assert!(
            identity(&self.observed, SOURCE, &target).is_some(),
            "source did not create {}",
            cell_name(&target)
        );
        assert!(identity(&self.observed, FRONT, &target).is_none());
    }
    pub async fn stop(mut self) {
        self.http.stop().await;
        if self.source.node.is_some() {
            self.source.stop().await;
        }
        if self.front.node.is_some() {
            self.front.stop().await;
        }
        self.inspection.close().await;
    }
}

pub fn assert_same_session(before: &Lease, after: &Lease) {
    assert_eq!(after.body["node"], before.body["node"]);
    assert_eq!(after.body["lineage"], before.body["lineage"]);
    assert_eq!(after.body["generation"], before.body["generation"]);
    assert!(after.expires() > before.expires());
    assert_ne!(after.etag, before.etag);
    assert_eq!(
        after.log(),
        before.log(),
        "renewal regressed folded log state"
    );
}
