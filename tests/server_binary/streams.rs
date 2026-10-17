//! Signed stream, TTL and retained-generation recovery over the serving process.
use crate::*;
use aws_sdk_dynamodb::{client::Waiters, types::TimeToLiveSpecification};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn streams_cli(public: SocketAddr, args: &[&str]) -> serde_json::Value {
    let output = Command::new("aws")
        .arg("dynamodbstreams")
        .args(args)
        .arg("--endpoint-url")
        .arg(format!("http://{public}"))
        .env("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
        .env(
            "AWS_SECRET_ACCESS_KEY",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        .env("AWS_DEFAULT_REGION", "us-east-1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker, aws CLI, and the pinned RustFS GA image"]
async fn signed_stream_records_survive_hard_server_restart() {
    let mut fixture = process_fixture(2).await;
    let sdk = &fixture.sdk;
    let public = fixture.public;
    let config = &fixture.config;
    let log = &fixture.log;
    let s3 = fixture.s3;
    let table = sdk
        .create_table()
        .table_name("StreamProcess")
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
    let arn = table
        .table_description()
        .and_then(|description| description.latest_stream_arn())
        .unwrap();
    sdk.put_item()
        .table_name("StreamProcess")
        .item("id", AttributeValue::S("streamed".into()))
        .send()
        .await
        .unwrap();
    let listed = streams_cli(public, &["list-streams", "--table-name", "StreamProcess"]);
    assert_eq!(listed["Streams"][0]["StreamArn"], arn);
    let described = streams_cli(public, &["describe-stream", "--stream-arn", arn]);
    let shards = described["StreamDescription"]["Shards"].as_array().unwrap();
    assert_eq!(shards.len(), 2);
    let records = |public| {
        shards
            .iter()
            .flat_map(|shard| {
                let id = shard["ShardId"].as_str().unwrap();
                let iterator = streams_cli(
                    public,
                    &[
                        "get-shard-iterator",
                        "--stream-arn",
                        arn,
                        "--shard-id",
                        id,
                        "--shard-iterator-type",
                        "TRIM_HORIZON",
                    ],
                );
                let token = iterator["ShardIterator"].as_str().unwrap();
                streams_cli(public, &["get-records", "--shard-iterator", token])["Records"]
                    .as_array()
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>()
    };
    let first = records(public);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0]["dynamodb"]["Keys"]["id"]["S"], "streamed");
    let expires = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .saturating_sub(1);
    sdk.put_item()
        .table_name("StreamProcess")
        .item("id", AttributeValue::S("expired".into()))
        .item("expires", AttributeValue::N(expires.to_string()))
        .send()
        .await
        .unwrap();
    sdk.update_time_to_live()
        .table_name("StreamProcess")
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .attribute_name("expires")
                .enabled(true)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(75);
    loop {
        let item = sdk
            .get_item()
            .table_name("StreamProcess")
            .key("id", AttributeValue::S("expired".into()))
            .send()
            .await
            .unwrap();
        if item.item().is_none() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "TTL worker did not delete stream item"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let with_ttl = records(public);
    let removal = with_ttl
        .iter()
        .find(|record| record["eventName"] == "REMOVE")
        .unwrap();
    assert_eq!(removal["userIdentity"]["Type"], "Service");
    assert_eq!(
        removal["userIdentity"]["PrincipalId"],
        "dynamodb.amazonaws.com"
    );
    assert!(first[0].get("userIdentity").is_none());
    fixture.child.kill().unwrap();
    fixture.child.wait().unwrap();
    let mut restarted = start(config, log, false, s3);
    wait_healthy(&mut restarted, public, log);
    assert_eq!(records(public), with_ttl);
    sdk.delete_table()
        .table_name("StreamProcess")
        .send()
        .await
        .unwrap();
    sdk.wait_until_table_not_exists()
        .table_name("StreamProcess")
        .wait(Duration::from_secs(60))
        .await
        .unwrap();
    let retained = streams_cli(public, &["list-streams", "--table-name", "StreamProcess"]);
    assert_eq!(retained["Streams"][0]["StreamArn"], arn);
    let described = streams_cli(public, &["describe-stream", "--stream-arn", arn]);
    assert_eq!(described["StreamDescription"]["StreamStatus"], "DISABLED");
    assert_eq!(records(public), with_ttl);
    sdk.create_table()
        .table_name("StreamProcess")
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
        .send()
        .await
        .unwrap();
    assert_eq!(records(public), with_ttl);
    stop(&mut restarted, log);
}
