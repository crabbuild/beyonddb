//! Ordered metadata fixtures over the compiled BeyondDB application.
use bdb_cellar_runtime::{
    MutationIdentity,
    app::{CellApplication, CompiledApplication},
    descriptors::BuildDescriptor,
    identity::{Digest, RequestId},
};
use std::sync::Arc;
pub const ACCOUNT: &str = "123456789012";
pub fn application() -> Arc<CompiledApplication> {
    Arc::new(
        beyonddb::Beyonddb::compile(BuildDescriptor {
            source_revision: "directory-sdk-test".into(),
            cargo_lock_digest: Digest::from_bytes([1; 32]),
        })
        .unwrap(),
    )
}
pub fn mutation() -> MutationIdentity {
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    }
}

/// Complete ordered metadata coverage, with string-key fences rather than hash
/// midpoints. These fixtures qualify directory contracts, not 1,024 data cells.
pub fn ranges(count: u16) -> Vec<beyonddb::RoutePagePartition> {
    use bdb_keyspace::{Component, KeyKind, KeySchema, RangeKey};
    let fence = |position| {
        KeySchema {
            partition: KeyKind::String,
            sort: None,
        }
        .encode(Component::String(&format!("range-{position:04}")), None)
        .unwrap()
    };
    (0..count)
        .map(|position| beyonddb::RoutePagePartition {
            partition_id: u128::from(position).to_be_bytes(),
            lower: if position == 0 {
                RangeKey::minimum()
            } else {
                fence(position)
            },
            upper: (position + 1 < count).then(|| fence(position + 1)),
            epoch: 1,
        })
        .collect()
}
