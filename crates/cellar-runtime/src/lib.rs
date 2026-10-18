//! BeyondDB domain commands executed through Cellar's public SDK.
//!
//! Cellar remains the authority for cell ownership, transactions and durability.
//! This crate supplies bounded command codecs, stable BeyondDB identities and
//! adapters for the application's existing parameterized SQL operations.

pub mod app;
pub mod application;
pub mod capacity;
pub mod client;
pub mod codec;
pub mod descriptors;
pub mod identity;
pub mod registry;
pub mod sql;
mod wire;

/// Error reported by a BeyondDB operation or its Cellar adapter.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid cell identity: {0}")]
    Identity(&'static str),
    #[error("invalid control value: {0}")]
    Control(&'static str),
    #[error("invalid command: {0}")]
    Command(&'static str),
    #[error("capacity limit: {0}")]
    Capacity(&'static str),
    #[error("invalid registry: {0}")]
    Registry(&'static str),
    #[error("command codec failed: {0}")]
    Codec(#[from] codec::CodecError),
    #[error("JSON encoding failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Cellar operation failed: {0}")]
    Cellar(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub use client::{CellClient, MutationIdentity};
pub use identity::partition_for_shard;
