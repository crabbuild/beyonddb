//! Index mutation, transaction, split and recovery contracts on native Cellar.
include!("support/transaction_transport.rs");
#[path = "support/directory_fault.rs"]
mod directory_fault;
#[path = "elastic_cells/global_indexes.rs"]
mod global_indexes;
#[path = "elastic_cells/local_indexes.rs"]
mod local_indexes;
#[allow(dead_code)] // Shared fixture exposes additional peer and auth fault helpers.
#[path = "support/native_peer.rs"]
mod native_peer;
#[path = "support/signed_http.rs"]
mod signed_http;

#[path = "elastic_cells/global_index_splits.rs"]
mod global_index_splits;
#[path = "elastic_cells/index_backfill.rs"]
mod index_backfill;
