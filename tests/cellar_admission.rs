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

#[path = "cellar_admission/claim.rs"]
mod claim;
#[path = "cellar_admission/ownership.rs"]
mod ownership;

#[path = "cellar_admission/absence.rs"]
mod absence;

#[path = "cellar_admission/hot.rs"]
mod hot;
#[path = "cellar_admission/reads.rs"]
mod reads;
