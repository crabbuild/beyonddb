//! Native Cellar batch rollback, receipts, images and stream contracts.
include!("support/ordered_routes.rs");
#[allow(dead_code)] // Shared peer fixture also exposes independent fault helpers.
#[path = "support/native_peer.rs"]
mod native_peer;
#[path = "cellar_batches/prepare_batch.rs"]
mod prepare_batch;
#[allow(dead_code)] // Metadata and batch cases use different fixture fields.
#[path = "support/residency_fixture.rs"]
mod residency_fixture;
#[path = "cellar_batches/update_batch.rs"]
mod update_batch;
