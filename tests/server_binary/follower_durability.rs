//! Withhold both native bundle and LTX uploads, then recover follower rows.
use crate::streams::streams_cli;
use crate::*;
use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    response::Response,
};
use cellar::{
    bucket::{Bucket, StorageBackend},
    management::{FleetClient, NodeAction, PageRequest},
    ownership_store::BucketOwnership,
    sdk::{
        ExecutionDomain, ReplicaStorage, ReplicaStorageOptions, Replication, ReplicationOptions,
    },
};
use futures_util::TryStreamExt;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
struct PublicationBarrier {
    upstream: SocketAddr,
    client: reqwest::Client,
    gate: Arc<Mutex<UploadGate>>,
}
#[derive(Default)]
struct UploadGate {
    enabled: bool,
    forwarding: usize,
    rejected: usize,
}
struct Forwarding(Arc<Mutex<UploadGate>>);
impl Drop for Forwarding {
    fn drop(&mut self) {
        self.0.lock().unwrap().forwarding -= 1;
    }
}
async fn proxy(State(barrier): State<PublicationBarrier>, request: Request<Body>) -> Response {
    let (parts, body) = request.into_parts();
    let path = parts.uri.path_and_query().unwrap().as_str();
    let payload = matches!(parts.method.as_str(), "PUT" | "POST")
        && ((path.contains("/cells/") && path.contains("/ltx/"))
            || (path.contains("/log/") && path.contains("/bundle/")));
    let _forwarding = if payload {
        let mut gate = barrier.gate.lock().unwrap();
        if gate.enabled {
            gate.rejected += 1;
            // Never queue a body that could be forwarded after the crash.
            let mut response = Response::new(Body::from("withheld bucket payload"));
            *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
            return response;
        }
        gate.forwarding += 1;
        Some(Forwarding(barrier.gate.clone()))
    } else {
        None
    };
    let body = axum::body::to_bytes(body, 64 << 20).await.unwrap();
    let response = barrier
        .client
        .request(parts.method, format!("http://{}{path}", barrier.upstream))
        .headers(parts.headers)
        .body(body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.bytes().await.unwrap();
    let mut result = Response::new(Body::from(bytes));
    *result.status_mut() = status;
    *result.headers_mut() = headers;
    result
}

fn observation_bucket(s3: SocketAddr, prefix: &str) -> Bucket {
    let store = Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_bucket_name("beyonddb-test")
            .with_region("us-east-1")
            .with_endpoint(format!("http://{s3}"))
            .with_allow_http(true)
            .with_access_key_id("crab")
            .with_secret_access_key("crab")
            .build()
            .unwrap(),
    );
    Bucket::from_clients(
        store.clone(),
        store.clone(),
        store,
        StorageBackend::S3,
        "beyonddb-test".into(),
        prefix.into(),
    )
    .unwrap()
}
async fn lease(bucket: &Bucket, node: &str) -> serde_json::Value {
    // Read-only diagnostic observation. SDK code performs every ownership and
    // folded-log transition; the fixture never writes their records.
    let (bytes, _) = bucket
        .get(&format!("nodes/{node}.json"))
        .await
        .unwrap()
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}
async fn payload_objects(bucket: &Bucket) -> BTreeMap<String, (Option<String>, u64)> {
    let prefix = object_store::path::Path::from(bucket.prefix.clone());
    let mut listed = bucket.store.list(Some(&prefix));
    let mut result = BTreeMap::new();
    while let Some(object) = listed.try_next().await.unwrap() {
        let key = object.location.as_ref();
        if !key.contains("/ltx/") && !key.contains("/bundle/") {
            continue;
        }
        assert!(
            result.len() < 512,
            "private fixture payload inventory exceeded bound"
        );
        result.insert(object.location.to_string(), (object.e_tag, object.size));
    }
    result
}
async fn published_source(bucket: &Bucket) -> Option<String> {
    let files = tempfile::tempdir().unwrap();
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let found = domain
        .run(async {
            let replication = Replication::start(
                &files.path().join("inspection"),
                ReplicaStorage::new(
                    bucket.store.clone(),
                    ReplicaStorageOptions {
                        prefix: bucket.prefix.clone(),
                        ..Default::default()
                    },
                )?,
                ReplicationOptions {
                    compaction: None,
                    ..Default::default()
                },
            )?;
            let prefixes = bucket.common_prefixes("cells/").await?;
            anyhow::ensure!(
                prefixes.len() < 128,
                "private fixture scope inventory exceeded bound"
            );
            let mut found = None;
            let mut data_cells = 0;
            for prefix in prefixes {
                let scope = prefix.strip_prefix("cells/").unwrap().trim_end_matches('/');
                if !scope.starts_with("beyonddb-data:") {
                    continue;
                }
                data_cells += 1;
                if let Some(snapshot) = replication.restore_snapshot(scope).await? {
                    // A non-owning complete SDK snapshot, never the live sparse file.
                    let connection = rusqlite::Connection::open_with_flags(
                        snapshot.path(),
                        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                    )?;
                    let count: u64 = connection.query_row(
                        "SELECT COUNT(*) FROM ddb_partition_items",
                        [],
                        |row| row.get(0),
                    )?;
                    if count > 0 {
                        anyhow::ensure!(
                            count == 1 && found.is_none(),
                            "warmup has unexpected data owners"
                        );
                        found = Some(scope.to_owned());
                    }
                }
            }
            anyhow::ensure!(
                data_cells == 2,
                "public table must have two installed data cells"
            );
            Ok::<_, anyhow::Error>(found)
        })
        .await;
    tokio::time::timeout(Duration::from_secs(10), domain.shutdown())
        .await
        .unwrap();
    found.unwrap().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker, aws CLI, and the pinned RustFS GA image"]
async fn acknowledged_untiered_item_survives_owner_process_kill() {
    let mut fixture = process_fixture_with_cache(2, true).await;
    let mut config: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.config).unwrap()).unwrap();
    let bucket = observation_bucket(fixture.s3, config["fleet"]["prefix"].as_str().unwrap());
    // Establish an exact published warmup while the executable still requires
    // bucket proof. Native bundles are not per-cell restore_snapshot inputs.
    let created = fixture
        .sdk
        .create_table()
        .table_name("FollowerRecovery")
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
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .stream_specification(
            aws_sdk_dynamodb::types::StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(aws_sdk_dynamodb::types::StreamViewType::NewAndOldImages)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let arn = created
        .table_description()
        .unwrap()
        .latest_stream_arn()
        .unwrap();
    fixture
        .sdk
        .put_item()
        .table_name("FollowerRecovery")
        .item("id", AttributeValue::S("warmup".into()))
        .send()
        .await
        .unwrap();
    let source = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(scope) = published_source(&bucket).await {
                break scope;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    stop(&mut fixture.child, &fixture.log);
    let root = fixture.root.path();
    let barrier = PublicationBarrier {
        upstream: fixture.s3,
        client: reqwest::Client::new(),
        gate: Arc::default(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = listener.local_addr().unwrap();
    let router = axum::Router::new()
        .fallback(proxy)
        .with_state(barrier.clone());
    let proxy_server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    config["fleet"]["endpoint"] = json!(format!("http://{proxy_address}"));
    config["fleet"]["follower_durability"] = json!(true);
    fs::write(&fixture.config, config.to_string()).unwrap();
    let key = fs::read(root.join("peer.key")).unwrap();
    bucket
        .put(
            "fleet/peer-auth.json",
            serde_json::to_vec(&json!({"version":1,
        "key":key.iter().map(|byte| format!("{byte:02x}")).collect::<String>()}))
            .unwrap(),
        )
        .await
        .unwrap();
    let fleet = FleetClient::new(bucket.clone(), false);
    let observer = BucketOwnership::new(
        bucket.clone(),
        bucket.clone(),
        "fixture-observer".into(),
        String::new(),
    );
    let mut followers = Vec::new();
    for name in ["follower-a", "follower-b"] {
        let peer = free_addr();
        let public = free_addr();
        let mut next = config.clone();
        next["node_name"] = json!(name);
        next["data_dir"] = json!(root.join(name));
        next["peer_bind"] = json!(peer);
        next["peer_advertise"] = json!(peer.to_string());
        next["public_bind"] = json!(public);
        next["public_endpoint"] = json!(format!("http://{public}"));
        next["accounts"] = json!([]);
        next["bootstrap"] = serde_json::Value::Null;
        let path = root.join(format!("{name}.json"));
        let log = root.join(format!("{name}.log"));
        fs::write(&path, next.to_string()).unwrap();
        let mut child = start(&path, &log, false, proxy_address);
        wait_healthy(&mut child, public, &log);
        followers.push((name.to_owned(), child, path, log));
    }
    fixture.child = start(&fixture.config, &fixture.log, false, proxy_address);
    wait_healthy(&mut fixture.child, fixture.public, &fixture.log);
    // Select a stable crash target through native management, while keeping
    // peer log recruitment and takeover/recovery enabled.
    let nodes = fleet
        .nodes(PageRequest {
            limit: Some(10),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(nodes.items.len(), 3);
    for node in nodes.items {
        fleet
            .node_action(&node.id, &node.generation, NodeAction::PauseBalancing)
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let nodes = fleet
                .nodes(PageRequest {
                    limit: Some(10),
                    ..Default::default()
                })
                .await
                .unwrap();
            if nodes.items.len() == 3
                && nodes
                    .items
                    .iter()
                    .all(|node| node.load["rebalance_paused"] == true)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        fixture
            .sdk
            .get_item()
            .table_name("FollowerRecovery")
            .key("id", AttributeValue::S("warmup".into()))
            .consistent_read(true)
            .send()
            .await
            .unwrap()
            .item()
            .is_some()
    );
    let owner = observer.read_owner(&source).await.unwrap().unwrap();
    let failed = owner.node.clone().unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let observed = lease(&bucket, &failed).await;
            if observed["log"]["ensemble"]
                .as_array()
                .is_some_and(|members| members.len() == 2)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap();
    barrier.gate.lock().unwrap().enabled = true;
    // Close upload admission atomically, then join every payload already
    // admitted by the proxy before recording the bucket cut.
    tokio::time::timeout(Duration::from_secs(10), async {
        while barrier.gate.lock().unwrap().forwarding != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let before = payload_objects(&bucket).await;
    assert!(!before.is_empty());
    let sdk = aws_sdk_dynamodb::Client::from_conf(
        fixture
            .sdk
            .config()
            .to_builder()
            .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled())
            .build(),
    );
    let mut item = HashMap::from([
        ("id".into(), AttributeValue::S("acknowledged".into())),
        (
            "value".into(),
            AttributeValue::S("survived-process-kill".into()),
        ),
    ]);
    tokio::time::timeout(
        Duration::from_secs(5),
        sdk.put_item()
            .table_name("FollowerRecovery")
            .set_item(Some(item.clone()))
            .send(),
    )
    .await
    .expect("follower proof must acknowledge while bundle and LTX publication are withheld")
    .unwrap();
    let log = lease(&bucket, &failed).await;
    assert_eq!(log["log"]["may_have_fleet_acks"], true);
    assert_eq!(log["log"]["state"], "open");
    let updated = sdk
        .update_item()
        .table_name("FollowerRecovery")
        .key("id", item["id"].clone())
        .update_expression("SET #version = :version")
        .expression_attribute_names("#version", "version")
        .expression_attribute_values(":version", AttributeValue::N("1".into()))
        .return_values(aws_sdk_dynamodb::types::ReturnValue::AllNew)
        .send()
        .await
        .unwrap();
    item.insert("version".into(), AttributeValue::N("1".into()));
    assert_eq!(updated.attributes(), Some(&item));
    sdk.delete_item()
        .table_name("FollowerRecovery")
        .key("id", AttributeValue::S("warmup".into()))
        .send()
        .await
        .unwrap();
    sdk.batch_write_item()
        .set_request_items(Some(HashMap::from([(
            "FollowerRecovery".into(),
            ["batch-a", "batch-b"]
                .map(|id| {
                    WriteRequest::builder()
                        .put_request(
                            PutRequest::builder()
                                .item("id", AttributeValue::S(id.into()))
                                .build()
                                .unwrap(),
                        )
                        .build()
                })
                .to_vec(),
        )])))
        .send()
        .await
        .unwrap();
    let transaction: Vec<_> = ["tx-a", "tx-b"]
        .map(|id| {
            TransactWriteItem::builder()
                .put(
                    aws_sdk_dynamodb::types::Put::builder()
                        .table_name("FollowerRecovery")
                        .item("id", AttributeValue::S(id.into()))
                        .condition_expression("attribute_not_exists(id)")
                        .build()
                        .unwrap(),
                )
                .build()
        })
        .into();
    sdk.transact_write_items()
        .client_request_token("follower-process-replay")
        .set_transact_items(Some(transaction.clone()))
        .send()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while barrier.gate.lock().unwrap().rejected == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        payload_objects(&bucket).await,
        before,
        "withheld data must not reach the bucket"
    );
    assert_eq!(observer.read_owner(&source).await.unwrap().unwrap(), owner);
    let failed_dir = if failed == config["node_name"].as_str().unwrap() {
        fixture.child.kill().unwrap();
        fixture.child.wait().unwrap();
        PathBuf::from(config["data_dir"].as_str().unwrap())
    } else {
        let (_, child, path, _) = followers
            .iter_mut()
            .find(|(name, _, _, _)| name == &failed)
            .unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        let settings: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        PathBuf::from(settings["data_dir"].as_str().unwrap())
    };
    std::fs::remove_dir_all(failed_dir).unwrap();
    assert_eq!(
        payload_objects(&bucket).await,
        before,
        "owner kill must not publish lost rows"
    );
    barrier.gate.lock().unwrap().enabled = false;
    // A dead process is still an authoritative owner until its native lease
    // expires. Start the replacement after that fence, as a real takeover must.
    tokio::time::timeout(Duration::from_secs(20), async {
        while observer
            .read_node_lease(&failed)
            .await
            .unwrap()
            .is_some_and(|record| record.expires_ms > cellar::ownership_store::now_ms())
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let public = free_addr();
    let peer = free_addr();
    let mut successor = config.clone();
    successor["node_name"] = json!("follower-successor");
    successor["data_dir"] = json!(root.join("successor-empty"));
    successor["public_bind"] = json!(public);
    successor["public_endpoint"] = json!(format!("http://{public}"));
    successor["peer_bind"] = json!(peer);
    successor["peer_advertise"] = json!(peer.to_string());
    successor["bootstrap"] = serde_json::Value::Null;
    let path = root.join("successor.json");
    let log = root.join("successor.log");
    fs::write(&path, successor.to_string()).unwrap();
    let mut recovered = start(&path, &log, false, proxy_address);
    wait_healthy(&mut recovered, public, &log);
    let sdk = aws_sdk_dynamodb::Client::from_conf(
        sdk.config()
            .to_builder()
            .endpoint_url(format!("http://{public}"))
            .build(),
    );
    let read = sdk
        .get_item()
        .table_name("FollowerRecovery")
        .key("id", item["id"].clone())
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(read.item(), Some(&item));
    let restored = observer.read_owner(&source).await.unwrap().unwrap();
    assert_ne!(restored.node.as_ref(), Some(&failed));
    assert!(restored.epoch > owner.epoch);
    assert_eq!(lease(&bucket, &failed).await["log"]["state"], "sealed");
    sdk.transact_write_items()
        .client_request_token("follower-process-replay")
        .set_transact_items(Some(transaction))
        .send()
        .await
        .unwrap();
    assert!(
        sdk.get_item()
            .table_name("FollowerRecovery")
            .key("id", AttributeValue::S("warmup".into()))
            .consistent_read(true)
            .send()
            .await
            .unwrap()
            .item()
            .is_none()
    );
    for id in ["batch-a", "batch-b", "tx-a", "tx-b"] {
        assert_eq!(
            sdk.get_item()
                .table_name("FollowerRecovery")
                .key("id", AttributeValue::S(id.into()))
                .consistent_read(true)
                .send()
                .await
                .unwrap()
                .item()
                .unwrap()
                .get("id"),
            Some(&AttributeValue::S(id.into()))
        );
    }
    let described = streams_cli(public, &["describe-stream", "--stream-arn", arn]);
    let mut records = Vec::new();
    for shard in described["StreamDescription"]["Shards"].as_array().unwrap() {
        let iterator = streams_cli(
            public,
            &[
                "get-shard-iterator",
                "--stream-arn",
                arn,
                "--shard-id",
                shard["ShardId"].as_str().unwrap(),
                "--shard-iterator-type",
                "TRIM_HORIZON",
            ],
        );
        records.extend(
            streams_cli(
                public,
                &[
                    "get-records",
                    "--shard-iterator",
                    iterator["ShardIterator"].as_str().unwrap(),
                ],
            )["Records"]
                .as_array()
                .unwrap()
                .clone(),
        );
    }
    for (id, event) in [
        ("acknowledged", "INSERT"),
        ("acknowledged", "MODIFY"),
        ("warmup", "REMOVE"),
        ("batch-a", "INSERT"),
        ("batch-b", "INSERT"),
        ("tx-a", "INSERT"),
        ("tx-b", "INSERT"),
    ] {
        assert_eq!(
            records
                .iter()
                .filter(|record| record["dynamodb"]["Keys"]["id"]["S"] == id
                    && record["eventName"] == event)
                .count(),
            1,
            "missing or duplicate {event} for {id}"
        );
    }
    stop(&mut recovered, &log);
    for (name, child, _, log) in &mut followers {
        if name != &failed {
            stop(child, log);
        }
    }
    if failed != config["node_name"].as_str().unwrap() {
        stop(&mut fixture.child, &fixture.log);
    }
    proxy_server.abort();
    let _ = proxy_server.await;
}
