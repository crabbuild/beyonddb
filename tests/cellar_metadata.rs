//! Signed table metadata and counters through native Cellar owner replacement.
include!("support/ordered_routes.rs");
#[allow(dead_code)] // Shared SDK peer policy exposes independent fault helpers.
#[path = "support/native_peer.rs"]
mod native_peer;
#[path = "support/residency_fixture.rs"]
mod residency_fixture;
#[path = "cellar_metadata/table_class.rs"]
mod table_class;

#[path = "cellar_metadata/usage.rs"]
mod usage;

#[path = "cellar_metadata/statistics.rs"]
mod statistics;
