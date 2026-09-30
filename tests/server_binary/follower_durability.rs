use crate::*;

use std::sync::{Arc, Mutex};

use axum::{body::Body, extract::State, http::Request, response::Response};
use beyonddb::{APPLICATION_ID, Beyonddb};
use cellule_app::CellApplication;
use cellule_peer_http::LoadedPeerTls;
use cellule_runtime::{
    Digest, control::authority::CellAuthority, ltx::CellStorageLayout, node::NodeDirectory,
    registry::BuildDescriptor,
};
use cellule_store::Store;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct PublicationBarrier {
    upstream: SocketAddr,
    client: reqwest::Client,
    cell: Arc<Mutex<Option<String>>>,
    blocked: CancellationToken,
    release: CancellationToken,
}

async fn proxy(State(barrier): State<PublicationBarrier>, request: Request<Body>) -> Response {
    let (parts, body) = request.into_parts();
    let path = parts.uri.path_and_query().unwrap().as_str();
    let blocked = matches!(parts.method.as_str(), "PUT" | "POST")
        && barrier
            .cell
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|cell| path.contains(cell) && path.contains("/objects/"));
    let body = axum::body::to_bytes(body, 64 << 20).await.unwrap();
    if blocked {
        barrier.blocked.cancel();
        barrier.release.cancelled().await;
    }
    // Preserve the signed Host, URI, headers, and body while changing only the
    // connection destination. RustFS still validates the original SigV4 request.
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

fn issue_certificate(root: &Path, name: &str) {
    run(Command::new("openssl")
        .args(["genpkey", "-algorithm", "ED25519", "-out"])
        .arg(root.join(format!("{name}.key"))));
    run(Command::new("openssl")
        .args(["req", "-new", "-subj", "/CN=localhost", "-key"])
        .arg(root.join(format!("{name}.key")))
        .arg("-out")
        .arg(root.join(format!("{name}.csr"))));
    run(Command::new("openssl")
        .args(["x509", "-req", "-days", "1", "-CAcreateserial", "-in"])
        .arg(root.join(format!("{name}.csr")))
        .arg("-CA")
        .arg(root.join("ca.crt"))
        .arg("-CAkey")
        .arg(root.join("ca.key"))
        .arg("-extfile")
        .arg(root.join("peer.ext"))
        .arg("-out")
        .arg(root.join(format!("{name}.crt"))));
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker, aws CLI, and the pinned RustFS GA image"]
async fn acknowledged_untiered_item_survives_owner_process_kill() {
    let mut fixture = process_fixture(1).await;
    stop(&mut fixture.child, &fixture.log);
    let root = fixture.root.path();
    let barrier = PublicationBarrier {
        upstream: fixture.s3,
        client: reqwest::Client::new(),
        cell: Arc::new(Mutex::new(None)),
        blocked: CancellationToken::new(),
        release: CancellationToken::new(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = listener.local_addr().unwrap();
    let router = axum::Router::new()
        .fallback(proxy)
        .with_state(barrier.clone());
    let proxy_server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut config: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.config).unwrap()).unwrap();
    config["follower_store_bytes"] = json!(1_u64 << 30);
    config["follower_durability_enabled"] = json!(true);
    config["auth_cache_enabled"] = json!(true);
    fs::write(&fixture.config, config.to_string()).unwrap();
    let mut followers = Vec::new();
    for name in ["follower-a", "follower-b"] {
        issue_certificate(root, name);
        let peer = free_addr();
        let public = free_addr();
        let mut next = config.clone();
        next["node_id"] = json!(uuid::Uuid::now_v7());
        next["data_dir"] = json!(root.join(name));
        next["peer_bind"] = json!(peer);
        next["peer_endpoint"] = json!(format!("https://{peer}"));
        next["public_bind"] = json!(public);
        next["public_endpoint"] = json!(format!("http://{public}"));
        next["peer_certificate"] = json!(root.join(format!("{name}.crt")));
        next["peer_private_key"] = json!(root.join(format!("{name}.key")));
        next["owned_accounts"] = json!([]);
        next["owned_access_keys"] = json!([]);
        next["bootstrap"] = serde_json::Value::Null;
        let path = root.join(format!("{name}.json"));
        let log = root.join(format!("{name}.log"));
        fs::write(&path, next.to_string()).unwrap();
        let child = start(&path, &log, false, proxy_address);
        followers.push((child, path, log, peer, public));
    }
    fixture.child = start(&fixture.config, &fixture.log, false, proxy_address);
    wait_healthy(&mut fixture.child, fixture.public, &fixture.log);
    for (child, _, log, _, public) in &mut followers {
        wait_healthy(child, *public, log);
    }
    let tls = LoadedPeerTls::load(
        &root.join("peer.crt"),
        &root.join("peer.key"),
        &root.join("ca.crt"),
        "localhost",
    )
    .unwrap();
    let application = Beyonddb::compile(BuildDescriptor {
        source_revision: option_env!("BEYONDDB_SOURCE_REVISION")
            .map(str::to_owned)
            .unwrap_or_else(|| {
                blake3::hash(include_bytes!("../../src/bin/beyonddb.rs"))
                    .to_hex()
                    .to_string()
            }),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../Cargo.lock")).as_bytes(),
        ),
    })
    .unwrap();
    let store = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name("beyonddb-test")
        .with_region("us-east-1")
        .with_endpoint(format!("http://{}", fixture.s3))
        .with_allow_http(true)
        .with_access_key_id("crab")
        .with_secret_access_key("crab")
        .build()
        .unwrap();
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(store)),
        object_store::path::Path::from("beyonddb"),
        *APPLICATION_ID.as_bytes(),
    );
    let authority = CellAuthority::new(layout.clone());
    let directory = NodeDirectory::new(
        layout,
        tls.fleet(),
        application.descriptor_digest(),
        application.registry().release_digest(),
    );
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
                .stream_view_type(aws_sdk_dynamodb::types::StreamViewType::KeysOnly)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let table = created.table_description().unwrap().table_id().unwrap();
    let arn = created
        .table_description()
        .unwrap()
        .latest_stream_arn()
        .unwrap();
    let target = beyonddb::data_target("123456789012", table, &[0; 16]).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let owner = loop {
        fixture
            .sdk
            .put_item()
            .table_name("FollowerRecovery")
            .item("id", AttributeValue::S("warmup".into()))
            .send()
            .await
            .unwrap();
        let current = authority.load(target.cell_id()).await.unwrap().unwrap();
        let owner = current.value().owner.as_ref().unwrap().clone();
        let node = directory
            .load(owner.session, now_ms())
            .await
            .unwrap()
            .unwrap();
        if node.advertisement().log().is_some_and(|log| log.active()) {
            break owner;
        }
        assert!(
            Instant::now() < deadline,
            "owner never activated a follower log"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let before = authority.load(target.cell_id()).await.unwrap().unwrap();
    let predecessor = before.value().root.as_ref().unwrap().commit_sequence;
    let cell = target
        .cell_id()
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    *barrier.cell.lock().unwrap() = Some(format!("/cells/{cell}/inc/"));
    let mut item = HashMap::from([
        ("id".into(), AttributeValue::S("acknowledged".into())),
        (
            "value".into(),
            AttributeValue::S("survived-process-kill".into()),
        ),
    ]);
    let sdk = aws_sdk_dynamodb::Client::from_conf(
        fixture
            .sdk
            .config()
            .to_builder()
            .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled())
            .build(),
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        sdk.put_item()
            .table_name("FollowerRecovery")
            .set_item(Some(item.clone()))
            .send(),
    )
    .await
    .expect("follower proof must acknowledge while object publication is withheld")
    .unwrap();
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
    let transaction = ["tx-a", "tx-b"]
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
        .to_vec();
    sdk.transact_write_items()
        .client_request_token("follower-process-replay")
        .set_transact_items(Some(transaction.clone()))
        .send()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), barrier.blocked.cancelled())
        .await
        .unwrap();
    assert_eq!(
        authority
            .load(target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .root
            .as_ref()
            .unwrap()
            .commit_sequence,
        predecessor
    );
    let failed = followers
        .iter()
        .position(|(_, _, _, peer, _)| owner.endpoint == format!("https://{peer}"));
    let (child, path, log, public) = match failed {
        Some(index) => {
            let (child, path, log, _, public) = &mut followers[index];
            (child, path.as_path(), log.as_path(), *public)
        }
        None => {
            assert_eq!(owner.endpoint, format!("https://{}", fixture.peer));
            (
                &mut fixture.child,
                fixture.config.as_path(),
                fixture.log.as_path(),
                fixture.public,
            )
        }
    };
    child.kill().unwrap();
    child.wait().unwrap();
    assert_eq!(
        authority
            .load(target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .root
            .as_ref()
            .unwrap()
            .commit_sequence,
        predecessor
    );
    barrier.release.cancel();
    tokio::time::timeout(Duration::from_secs(20), async {
        while directory.is_live(owner.session, now_ms()).await.unwrap() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    // Recover all Cells acknowledged by the failed boot, even if placement
    // originally chose a follower node as this table's owner.
    let mut successor: serde_json::Value =
        serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    successor["owned_accounts"] = config["owned_accounts"].clone();
    fs::write(path, successor.to_string()).unwrap();
    *child = start(path, log, false, proxy_address);
    wait_healthy(child, public, log);
    let read = sdk
        .get_item()
        .table_name("FollowerRecovery")
        .key("id", item["id"].clone())
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(read.item(), Some(&item));
    let restored = authority.load(target.cell_id()).await.unwrap().unwrap();
    assert_ne!(
        restored.value().owner.as_ref().unwrap().session,
        owner.session
    );
    // Replaying the saved transaction outcome must not re-evaluate its insert
    // conditions or emit another stream record.
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
    let described = streams_cli(fixture.public, &["describe-stream", "--stream-arn", arn]);
    let mut records = Vec::new();
    for shard in described["StreamDescription"]["Shards"].as_array().unwrap() {
        let iterator = streams_cli(
            fixture.public,
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
                fixture.public,
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
    for (child, _, log, _, _) in &mut followers {
        stop(child, log);
    }
    stop(&mut fixture.child, &fixture.log);
    proxy_server.abort();
}
