//! Signed request coalescing and acknowledged transaction fast paths.
include!("support/ordered_routes.rs");
#[path = "cellar_coalescing/contracts.rs"]
mod contracts;
#[allow(dead_code)] // This target uses four caller host threads instead.
#[path = "support/forwarded_fixture.rs"]
mod fixture;
#[allow(dead_code)] // Shared peer fixture includes independent fault helpers.
#[path = "support/native_peer.rs"]
mod native_peer;
#[path = "cellar_coalescing/registration.rs"]
mod registration;
