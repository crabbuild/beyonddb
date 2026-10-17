//! Coordinator and participant behavior on the actual embedded Cellar SDK.
include!("support/transaction_transport.rs");
#[path = "elastic_cells/coordinator_tokens.rs"]
mod coordinator_tokens;
#[path = "support/local_node.rs"]
mod local_node;

#[path = "elastic_cells/transaction_write_skew.rs"]
mod transaction_write_skew;

#[path = "support/transaction_fixture.rs"]
mod transaction_fixture;

#[path = "elastic_cells/transaction_resume.rs"]
mod transaction_resume;

#[path = "support/lost_reply.rs"]
mod lost_reply;

#[path = "elastic_cells/transaction_resolution.rs"]
mod transaction_resolution;

#[path = "elastic_cells/read_resolution.rs"]
mod read_resolution;
#[path = "elastic_cells/transaction_reads.rs"]
mod transaction_reads;

#[path = "elastic_cells/public_transactions.rs"]
mod public_transactions;

#[path = "support/cellar_node.rs"]
mod cellar_node;
#[path = "elastic_cells/transaction_commit.rs"]
mod transaction_commit;

#[path = "elastic_cells/account_participant.rs"]
mod account_participant;

#[path = "support/offline_sqlite.rs"]
mod offline_sqlite;
#[path = "elastic_cells/transaction_driver.rs"]
mod transaction_driver;
#[path = "elastic_cells/transaction_recovery.rs"]
mod transaction_recovery;

#[path = "elastic_cells/read_release.rs"]
mod read_release;

#[path = "support/replica_snapshot.rs"]
mod replica_snapshot;

#[path = "elastic_cells/ttl_transactions.rs"]
mod ttl_transactions;

#[path = "support/signed_http.rs"]
mod signed_http;

#[path = "elastic_cells/transaction_visibility.rs"]
mod transaction_visibility;

#[path = "elastic_cells/coordinator_checkpoints.rs"]
mod coordinator_checkpoints;
#[path = "elastic_cells/coordinator_residency.rs"]
mod coordinator_residency;

#[path = "elastic_cells/recovery_admission.rs"]
mod recovery_admission;
