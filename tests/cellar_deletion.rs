//! Large generation cleanup and public range retirement through native Cellar.
include!("support/ordered_routes.rs");
#[path = "cellar_deletion/helpers.rs"]
mod helpers;
#[allow(dead_code)]
#[path = "support/native_peer.rs"]
mod native_peer;
#[path = "cellar_deletion/recovery.rs"]
mod recovery;
#[allow(dead_code)]
#[path = "support/residency_fixture.rs"]
mod residency_fixture;
#[allow(dead_code)]
#[path = "cellar_placement/helpers.rs"]
mod table_helpers;
