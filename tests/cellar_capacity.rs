//! Physical reservation and rollback contracts through the native SDK.
include!("support/transaction_transport.rs");
#[path = "elastic_cells/capacity_reservations.rs"]
mod capacity_reservations;
#[path = "support/capacity_snapshot.rs"]
mod capacity_snapshot;
#[allow(dead_code)] // The shared transaction fixture also exposes local hosts.
#[path = "support/local_node.rs"]
mod local_node;
#[allow(dead_code)] // Shared fixtures expose additional network and table helpers.
#[path = "support/native_peer.rs"]
mod native_peer;
#[path = "support/signed_http.rs"]
mod signed_http;
#[allow(dead_code)] // Only the routing/account helpers are needed here.
#[path = "support/transaction_fixture.rs"]
mod transaction_fixture;
