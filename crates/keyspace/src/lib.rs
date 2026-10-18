//! Versioned, order-preserving keys shared by routing and cell-local storage.
//!
//! A table generation owns one key space. Its keys never include a hash, owner
//! address, or cell identity. Physical boundaries may fall between two sort
//! keys with the same partition key.

mod encoding;
mod ranges;

pub use encoding::{Component, KeyKind, KeySchema, SortCondition};
pub use ranges::{CellRange, Directory, KeySpan, RangeKey, Route, Split};

/// Persist this version with every directory and data-cell installation.
pub const KEY_ENCODING_VERSION: u32 = 1;
/// Worst-case escaped 2 KiB partition key plus 1 KiB sort key, tags and ends.
pub const MAX_ENCODED_PRIMARY_KEY_BYTES: usize = 6_150;
/// Index keys add the complete base key as their duplicate-key tie-breaker.
pub const MAX_ENCODED_KEY_BYTES: usize = 2 * MAX_ENCODED_PRIMARY_KEY_BYTES;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("key type does not match the table schema")]
    KeyType,
    #[error("missing or unexpected sort key")]
    SortKey,
    #[error("key is empty or exceeds its byte limit")]
    KeySize,
    #[error("number is invalid or outside DynamoDB's precision and magnitude bounds")]
    Number,
    #[error("begins_with requires a string or binary sort key")]
    PrefixType,
    #[error("invalid range bounds")]
    Range,
    #[error("directory must contain at least two adjacent ranges with unique cell IDs")]
    Directory,
    #[error("route generation changed")]
    StaleRoute,
    #[error("split cannot divide this range at the proposed key")]
    Split,
}

pub type Result<T> = std::result::Result<T, Error>;
