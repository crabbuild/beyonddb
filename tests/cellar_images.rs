//! Immutable transaction images and failure replies on native SDK owners.
include!("support/ordered_routes.rs");
#[path = "cellar_images/fixture.rs"]
mod fixture;
#[allow(dead_code)] // Shared peer fixture also exposes unrelated fault helpers.
#[path = "support/native_peer.rs"]
mod native_peer;
#[path = "cellar_images/prepare_failure.rs"]
mod prepare_failure;
#[path = "cellar_images/saved_images.rs"]
mod saved_images;
