#![cfg(unix)]
mod support;
mod server_binary {
    mod capacity;
    mod follower_durability;
    pub(super) mod global_indexes;
    pub(super) mod local_indexes;
    pub(super) mod streams;
}
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

use aws_credential_types::Credentials;
use aws_sdk_dynamodb::client::Waiters;
use aws_sdk_dynamodb::types::{
    AttributeValue, ConditionCheck, KeysAndAttributes, PutRequest, TimeToLiveSpecification,
    TransactWriteItem, Update, WriteRequest,
};
use serde_json::json;

include!("support/process_fixture.rs");
#[path = "server_binary/bootstrap.rs"]
mod bootstrap;
use server_binary::{global_indexes, local_indexes, streams};
