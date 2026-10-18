//! Signed SDK and hard-process-loss fixtures over bucket-backed Cellar.
#![cfg(unix)]
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::{
    client::Waiters,
    types::{
        AttributeValue, ConditionCheck, KeysAndAttributes, PutRequest, TimeToLiveSpecification,
        TransactWriteItem, Update, WriteRequest,
    },
};
mod support;
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, OpenOptions},
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
include!("support/process_fixture.rs");
#[path = "server_binary/coordinator_recovery.rs"]
mod coordinator_recovery;
#[path = "server_binary/large_reads.rs"]
mod large_reads;
#[path = "server_binary/local_indexes.rs"]
mod local_indexes;
#[path = "server_binary/metadata_cache.rs"]
mod metadata_cache;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker, aws CLI, and the pinned RustFS GA image"]
async fn segmented_local_index_scan_advances_empty_pages() {
    let mut fixture = process_fixture(4).await;
    local_indexes::create(&fixture.sdk).await;
    stop(&mut fixture.child, &fixture.log);
}

#[path = "server_binary/global_indexes.rs"]
mod global_indexes;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker, aws CLI, and the pinned RustFS GA image"]
async fn global_index_projection_survives_bucket_process_loss() {
    let mut fixture = process_fixture(3).await;
    global_indexes::create(&fixture.sdk).await;
    fixture.child.kill().unwrap();
    fixture.child.wait().unwrap();
    fixture.child = start(&fixture.config, &fixture.log, false, fixture.s3);
    wait_healthy(&mut fixture.child, fixture.public, &fixture.log);
    global_indexes::assert_recovered(&fixture.sdk).await;
    stop(&mut fixture.child, &fixture.log);
}

#[path = "server_binary/streams.rs"]
mod streams;

#[path = "server_binary/capacity.rs"]
mod capacity;

#[path = "server_binary/bootstrap.rs"]
mod bootstrap;

#[path = "server_binary/follower_durability.rs"]
mod follower_durability;
