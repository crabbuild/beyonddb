//! Signed AWS SDK requests against the actual Cellar-backed server process.
#![cfg(unix)]
use anyhow::{Context, Result, ensure};
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::{
    Client,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    },
};
use serde_json::json;
use std::{
    fs,
    io::Write,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct Process {
    child: Child,
    log: PathBuf,
}
impl Drop for Process {
    fn drop(&mut self) {
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
fn address() -> Result<SocketAddr> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?)
}
fn start(config: &Path, log: &Path, bootstrap: bool) -> Result<Process> {
    let output = fs::OpenOptions::new().create(true).append(true).open(log)?;
    let mut command = Command::new(
        config
            .parent()
            .context("configuration has no directory")?
            .join("beyonddb"),
    );
    command
        .arg(config)
        .stdout(output.try_clone()?)
        .stderr(output);
    if bootstrap {
        command.arg("--bootstrap").stdin(Stdio::piped());
    }
    let mut child = command.spawn()?;
    if bootstrap {
        child
            .stdin
            .take()
            .context("missing bootstrap stdin")?
            .write_all(b"bdb-cellar-sdk-secret\n")?;
    }
    Ok(Process {
        child,
        log: log.into(),
    })
}
async fn ready(process: &mut Process, endpoint: &str) -> Result<()> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()?;
    let deadline = Instant::now() + Duration::from_secs(40);
    while Instant::now() < deadline {
        if let Some(status) = process.child.try_wait()? {
            anyhow::bail!(
                "server exited {status}: {}",
                fs::read_to_string(&process.log)?
            );
        }
        if let Ok(response) = http.get(format!("{endpoint}/health")).send().await
            && response.status().is_success()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!(
        "server readiness deadline: {}",
        fs::read_to_string(&process.log)?
    )
}
async fn stop(process: &mut Process) -> Result<()> {
    ensure!(
        Command::new("kill")
            .args(["-TERM", &process.child.id().to_string()])
            .status()?
            .success(),
        "could not signal server"
    );
    let deadline = Instant::now() + Duration::from_secs(40);
    while Instant::now() < deadline {
        if let Some(status) = process.child.try_wait()? {
            ensure!(
                status.success(),
                "server stop failed: {}",
                fs::read_to_string(&process.log)?
            );
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!(
        "server stop deadline: {}",
        fs::read_to_string(&process.log)?
    )
}

async fn check(sdk: &Client) -> Result<()> {
    assert!(
        sdk.get_item()
            .table_name("CellarSdk")
            .key("pk", AttributeValue::S("Deleted".into()))
            .key("sk", AttributeValue::N("1".into()))
            .consistent_read(true)
            .send()
            .await?
            .item
            .is_none()
    );
    let large = sdk
        .get_item()
        .table_name("CellarSdk")
        .key("pk", AttributeValue::S("Large".into()))
        .key("sk", AttributeValue::N("1".into()))
        .consistent_read(true)
        .send()
        .await?
        .item
        .context("missing large item")?;
    assert_eq!(large["payload"].as_s().unwrap(), &"\0".repeat(395 * 1024));
    let item = sdk
        .get_item()
        .table_name("CellarSdk")
        .key("pk", AttributeValue::S("hot".into()))
        .key("sk", AttributeValue::N("10".into()))
        .consistent_read(true)
        .send()
        .await?
        .item
        .context("missing committed item")?;
    assert_eq!(
        item.get("payload"),
        Some(&AttributeValue::B(vec![0, 255, 0, 128].into()))
    );
    assert_eq!(
        item.get("exact"),
        Some(&AttributeValue::N(i64::MAX.to_string()))
    );
    assert_eq!(item.get("marker"), Some(&AttributeValue::S("once".into())));
    for partition in ["A", "Z", "Ω", "😀"] {
        let value = sdk
            .get_item()
            .table_name("CellarSdk")
            .key("pk", AttributeValue::S(partition.into()))
            .key("sk", AttributeValue::N("1".into()))
            .consistent_read(true)
            .send()
            .await?
            .item
            .context("missing transaction item")?;
        assert_eq!(
            value.get("marker"),
            Some(&AttributeValue::S("transaction".into()))
        );
    }
    let mut start = None;
    let mut seen = Vec::new();
    loop {
        let page = sdk
            .query()
            .table_name("CellarSdk")
            .key_condition_expression("pk = :p AND sk BETWEEN :lo AND :hi")
            .expression_attribute_values(":p", AttributeValue::S("hot".into()))
            .expression_attribute_values(":lo", AttributeValue::N("7".into()))
            .expression_attribute_values(":hi", AttributeValue::N("24".into()))
            .limit(4)
            .consistent_read(true)
            .set_exclusive_start_key(start)
            .send()
            .await?;
        for item in page.items() {
            seen.push(
                item.get("sk")
                    .and_then(|value| value.as_n().ok())
                    .context("missing numeric sort key")?
                    .parse::<u8>()?,
            );
        }
        start = page.last_evaluated_key;
        if start.as_ref().is_none_or(|key| key.is_empty()) {
            break;
        }
    }
    assert_eq!(seen, (7..=24).collect::<Vec<_>>());
    let reverse = sdk
        .query()
        .table_name("CellarSdk")
        .key_condition_expression("pk = :p")
        .expression_attribute_values(":p", AttributeValue::S("hot".into()))
        .scan_index_forward(false)
        .limit(3)
        .send()
        .await?;
    assert_eq!(
        reverse
            .items()
            .iter()
            .map(|item| item["sk"].as_n().unwrap().as_str())
            .collect::<Vec<_>>(),
        vec!["30", "29", "28"]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_crud_and_query_survive_cellar_process_restart() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    // Keep the exact built executable on the fixture disk. Cold random reads
    // from a mounted build volume must not consume the server readiness budget.
    fs::copy(env!("CARGO_BIN_EXE_beyonddb"), root.join("beyonddb"))?;
    let public = address()?;
    let peer = address()?;
    let endpoint = format!("http://{public}");
    fs::write(root.join("encryption.key"), [31; 32])?;
    fs::write(root.join("peer.key"), [32; 32])?;
    fs::write(
        root.join("policy.json"),
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}]}"#,
    )?;
    let config = root.join("config.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&json!({
            "node_name": "bdb-sdk", "data_dir": root.join("data"), "region": "us-east-1",
            "public_bind": public, "public_endpoint": endpoint, "peer_bind": peer, "peer_advertise": peer.to_string(),
            "peer_key_file": root.join("peer.key"), "encryption_key_file": root.join("encryption.key"),
            "accounts": ["123456789012"], "initial_partitions": 3,
            "bootstrap": { "account_id": "123456789012", "access_key_id": "AKIACELLARSDKTESTKEY1", "principal_name": "admin", "policy_name": "all", "policy_file": root.join("policy.json") }
        }))?,
    )?;
    let log = root.join("server.log");
    let mut process = start(&config, &log, true)?;
    ready(&mut process, &endpoint).await?;
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region("us-east-1")
        .credentials_provider(Credentials::new(
            "AKIACELLARSDKTESTKEY1",
            "bdb-cellar-sdk-secret",
            None,
            None,
            "test",
        ))
        .endpoint_url(&endpoint)
        .load()
        .await;
    let sdk = Client::new(&sdk_config);
    let result: Result<()> = async {
        sdk.create_table()
            .table_name("CellarSdk")
            .billing_mode(BillingMode::PayPerRequest)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("pk")
                    .attribute_type(ScalarAttributeType::S)
                    .build()?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("sk")
                    .attribute_type(ScalarAttributeType::N)
                    .build()?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("pk")
                    .key_type(KeyType::Hash)
                    .build()?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("sk")
                    .key_type(KeyType::Range)
                    .build()?,
            )
            .send()
            .await?;
        for value in 1..=30 {
            sdk.put_item()
                .table_name("CellarSdk")
                .item("pk", AttributeValue::S("hot".into()))
                .item("sk", AttributeValue::N(value.to_string()))
                .item("exact", AttributeValue::N(i64::MAX.to_string()))
                .item("payload", AttributeValue::B(vec![0, 255, 0, 128].into()))
                .send()
                .await?;
        }
        sdk.put_item()
            .table_name("CellarSdk")
            .item("pk", AttributeValue::S("Large".into()))
            .item("sk", AttributeValue::N("1".into()))
            .item("payload", AttributeValue::S("\0".repeat(395 * 1024)))
            .send()
            .await
            .context("large escaped item")?;
        // Conditional evaluation and transaction coordination stay in the
        // existing BeyondDB domain layer while their commands execute in Cellar.
        sdk.update_item()
            .table_name("CellarSdk")
            .key("pk", AttributeValue::S("hot".into()))
            .key("sk", AttributeValue::N("10".into()))
            .update_expression("SET marker = :v")
            .condition_expression("attribute_not_exists(marker)")
            .expression_attribute_values(":v", AttributeValue::S("once".into()))
            .send()
            .await?;
        assert!(
            sdk.update_item()
                .table_name("CellarSdk")
                .key("pk", AttributeValue::S("hot".into()))
                .key("sk", AttributeValue::N("10".into()))
                .update_expression("SET marker = :v")
                .condition_expression("attribute_not_exists(marker)")
                .expression_attribute_values(":v", AttributeValue::S("twice".into()))
                .send()
                .await
                .is_err()
        );
        let mut transaction = sdk
            .transact_write_items()
            .client_request_token("cellar-sdk-transaction");
        for partition in ["A", "Z", "Ω", "😀"] {
            transaction = transaction.transact_items(
                aws_sdk_dynamodb::types::TransactWriteItem::builder()
                    .put(
                        aws_sdk_dynamodb::types::Put::builder()
                            .table_name("CellarSdk")
                            .item("pk", AttributeValue::S(partition.into()))
                            .item("sk", AttributeValue::N("1".into()))
                            .item("marker", AttributeValue::S("transaction".into()))
                            .build()?,
                    )
                    .build(),
            );
        }
        transaction
            .clone()
            .send()
            .await
            .context("transaction first submission")?;
        transaction
            .send()
            .await
            .context("transaction token replay")?;
        sdk.put_item()
            .table_name("CellarSdk")
            .item("pk", AttributeValue::S("Deleted".into()))
            .item("sk", AttributeValue::N("1".into()))
            .item("marker", AttributeValue::S("delete-me".into()))
            .send()
            .await?;
        let deleted = sdk
            .delete_item()
            .table_name("CellarSdk")
            .key("pk", AttributeValue::S("Deleted".into()))
            .key("sk", AttributeValue::N("1".into()))
            .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
            .send()
            .await?;
        assert_eq!(
            deleted
                .attributes
                .context("missing deleted image")?
                .get("marker"),
            Some(&AttributeValue::S("delete-me".into()))
        );
        check(&sdk).await?;
        stop(&mut process).await?;
        process = start(&config, &log, false)?;
        ready(&mut process, &endpoint).await?;
        check(&sdk).await?;
        stop(&mut process).await
    }
    .await;
    if let Err(error) = result {
        anyhow::bail!(
            "SDK qualification failed: {error:#}\n{}",
            fs::read_to_string(&log)?
        );
    }
    Ok(())
}
