//! Public multi-cell placement, range-local reads and independent index growth.
include!("support/ordered_routes.rs");
#[path = "cellar_placement/atomic.rs"]
mod atomic;
#[path = "cellar_placement/helpers.rs"]
mod helpers;
#[path = "cellar_placement/index.rs"]
mod index;
#[path = "cellar_placement/models.rs"]
mod models;
#[allow(dead_code)]
#[path = "support/native_peer.rs"]
mod native_peer;
#[allow(dead_code)] // Placement cases create separate tables; metadata reads seeded images.
#[path = "support/residency_fixture.rs"]
mod residency_fixture;
