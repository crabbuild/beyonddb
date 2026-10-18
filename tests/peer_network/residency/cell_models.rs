// Signed coalescing and public placement contracts run through the native SDK
// in tests/cellar_coalescing.rs and tests/cellar_placement.rs. Public tables
// reject Single placement; the remaining legacy callers still use this helper.
use super::provisioning::table_id;
use super::*;
use beyonddb::{RoutePageInput, RoutePageOutcome};
const ACCOUNT: &str = "123456789012";

pub(super) async fn ranges(fixture: &Fixture, table: &str) -> Vec<beyonddb::RoutePagePartition> {
    let id = table_id(fixture, table).await;
    let page = beyonddb::read_route_page(
        &fixture.client,
        &account_target(ACCOUNT).unwrap(),
        RoutePageInput {
            table_id: id,
            start_hash: None,
            after_lower: None,
            expected_epoch: None,
        },
    )
    .await
    .unwrap();
    let RoutePageOutcome::Page {
        partitions,
        has_more,
        ..
    } = page
    else {
        panic!("table route is missing");
    };
    assert!(!has_more);
    partitions
}
