//! Native SDK eviction preserves application data, split and transaction fences.
include!("support/ordered_routes.rs");
#[path = "cellar_reclamation/helpers.rs"]
mod helpers;
#[allow(dead_code)]
#[path = "support/native_peer.rs"]
mod native_peer;
#[path = "cellar_reclamation/participant.rs"]
mod participant;
#[allow(dead_code)]
#[path = "support/residency_fixture.rs"]
mod residency_fixture;
#[path = "cellar_reclamation/serving.rs"]
mod serving;
#[path = "cellar_reclamation/splits.rs"]
mod splits;
#[allow(dead_code)]
#[path = "cellar_placement/helpers.rs"]
mod table_helpers;

#[path = "cellar_reclamation/creation.rs"]
mod creation;
#[path = "cellar_reclamation/creation_actor.rs"]
mod creation_actor;
