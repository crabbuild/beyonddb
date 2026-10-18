//! Cold coordinator admission and signed replay on native Cellar owners.
include!("support/ordered_routes.rs");
#[path = "cellar_admission/cold.rs"]
mod cold;
#[allow(dead_code)]
#[path = "support/native_peer.rs"]
mod native_peer;
#[path = "cellar_admission/payload.rs"]
mod payload;
#[allow(dead_code)]
#[path = "support/residency_fixture.rs"]
mod residency_fixture;
