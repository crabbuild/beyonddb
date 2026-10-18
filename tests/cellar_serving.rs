//! Public serving contracts through ordered cells and native owner replacement.
include!("support/transaction_transport.rs");
include!("support/ordered_routes.rs");
#[path = "support/capacity_snapshot.rs"]
mod capacity_snapshot;
#[allow(dead_code)] // Shared table helpers also expose local host start/stop.
#[path = "support/local_node.rs"]
mod local_node;
#[allow(dead_code)] // Shared peer fixture also exposes unrelated fault helpers.
#[path = "support/native_peer.rs"]
mod native_peer;
#[path = "elastic_cells/numeric_contract.rs"]
mod numeric_contract;
#[path = "support/read_barriers.rs"]
mod read_barriers;
#[allow(dead_code)] // This target uses the shared ordered participant resolver.
#[path = "support/transaction_fixture.rs"]
mod transaction_fixture;

#[path = "elastic_cells/target_admission.rs"]
mod target_admission;

#[path = "elastic_cells/data_contract.rs"]
mod data_contract;
