//! Incomplete public creation and generation fencing through native Cellar.
include!("support/ordered_routes.rs");
#[path = "cellar_creation/fences.rs"]
mod fences;
#[path = "cellar_creation/helpers.rs"]
mod helpers;
#[allow(dead_code)]
#[path = "support/native_peer.rs"]
mod native_peer;
#[path = "cellar_creation/recovery.rs"]
mod recovery;
#[allow(dead_code)] // Creation uses named tables rather than seeded images.
#[path = "support/residency_fixture.rs"]
mod residency_fixture;
#[allow(dead_code)] // Shared builders also expose placement-only helpers.
#[path = "cellar_placement/helpers.rs"]
mod table_helpers;
