//! Directory publication, transfer and recovery on the embedded Cellar SDK.
#[path = "support/cellar_node.rs"]
mod cellar_node;
#[path = "support/directory_fixture.rs"]
mod directory_fixture;
#[path = "elastic_cells/directory_transfers.rs"]
mod directory_transfers;

#[path = "elastic_cells/directory_activation.rs"]
mod directory_activation;

#[path = "support/directory_fault.rs"]
mod directory_fault;
#[path = "elastic_cells/directory_tree.rs"]
mod directory_tree;

#[path = "elastic_cells/directory_residency.rs"]
mod directory_residency;

#[path = "elastic_cells/initial_creation.rs"]
mod initial_creation;

#[path = "elastic_cells/route_pages.rs"]
mod route_pages;

#[path = "elastic_cells/table_residency.rs"]
mod table_residency;
