# Retired runtime test coverage

Historical record: on 2026-10-18, the unported Cellule-runtime `elastic_cells` and
`peer_network` targets and their exclusive fixture modules were removed.
Their CI invocations were removed too. The general all-target build and strict
Clippy gates remain enabled. Passing native Cellar tests and their shared
helpers remain registered.

This is removal of unqualified coverage, not evidence that these contracts now
pass on Cellar. In particular, the physical SQLite FULL case and the remaining
legacy placement, admission, hydration, rebalance and recovery faults are no
longer release gates. Existing native and process coverage is described in
[the server guide](cellar-server.md#qualification-still-required).

The complete retired source is available at commit
`3b03ebb520cc0e08b6ea48bb6beffd339d973fba`. For example:

```sh
git show 3b03ebb520cc0e08b6ea48bb6beffd339d973fba:tests/peer_network.rs
```

Port individual contracts from that revision when restoring coverage. Historical
design documents may reference the retired paths below; those references describe
the old Cellule fixtures rather than current test targets.

## Removed source and test inventory

| Removed file | Registered tests in that file |
| --- | --- |
| `tests/elastic_cells.rs` | Fixture helpers / retired module declarations |
| `tests/elastic_cells/transaction_capacity.rs` | `sqlite_full_during_upload_aborts_and_resolves_all_participants` |
| `tests/peer_network.rs` | `signed_sdk_request_routes_across_two_owners_and_survives_restart` |
| `tests/support/routes.rs` | Fixture helpers / retired module declarations |
| `tests/peer_network/capacity.rs` | Fixture helpers / retired module declarations |
| `tests/peer_network/global_indexes.rs` | Fixture helpers / retired module declarations |
| `tests/peer_network/recovery.rs` | Fixture helpers / retired module declarations |
| `tests/peer_network/residency/active_owners.rs` | `signed_forward_cache_keeps_hydrating_owners_without_authority_reads` |
| `tests/peer_network/residency/cell_models.rs` | Fixture helpers / retired module declarations |
| `tests/peer_network/residency/codec.rs` | `peer_read_waits_for_codec_capacity_within_its_deadline`, `peer_catalog_io_leaves_cpu_capacity_for_unrelated_work`, `peer_pre_dispatch_waits_obey_signed_request_deadline` |
| `tests/peer_network/residency/coordinator_admission.rs` | `cold_sdk_coordinator_overlaps_catalog_publication_with_authority_lookup` |
| `tests/peer_network/residency/directories.rs` | `directory_split_and_retirement_follow_remote_owners`, `sdk_index_reads_follow_split_metadata_and_generation_retirement`, `unavailable_index_directory_does_not_starve_healthy_index_maintenance`, `unavailable_base_directory_does_not_starve_next_table_capacity` |
| `tests/peer_network/residency/discovery.rs` | `sdk_placement_accepts_heartbeats_renewed_during_discovery` |
| `tests/peer_network/residency/follower_durability.rs` | `follower_fsync_acknowledges_signed_write_without_object_publication`, `object_only_signed_write_waits_for_object_publication` |
| `tests/peer_network/residency/forward_cache.rs` | `signed_forward_cache_skips_authority_io_and_rejects_drained_owner`, `remote_read_overlaps_fresh_authority_and_owner_enrollment` |
| `tests/peer_network/residency/index_splits.rs` | `sdk_capacity_splits_indexes_and_preserves_tombstones_after_owner_restore` |
| `tests/peer_network/residency/live_owner_failover.rs` | `serving_recovery_preserves_live_coordinator_then_finishes_after_owner_expiry`, `published_idle_range_can_move_while_its_former_node_stays_live` |
| `tests/peer_network/residency/placement.rs` | `sdk_cold_placement_activates_remote_and_resumes_its_claim` |
| `tests/peer_network/residency/prepare_capacity.rs` | `eight_in_flight_prepares_fit_the_participant_mailbox` |
| `tests/peer_network/residency/pressure.rs` | `credential_query_waits_for_peer_memory_and_reads_revocation_after_release` |
| `tests/peer_network/residency/provisioning.rs` | `recovery_reads_routes_from_live_remote_account`, `large_transaction_read_from_remote_owner_keeps_read_only_snapshot`, `sdk_read_recovers_expired_directory_and_data_owners`, `sdk_initial_base_and_index_ranges_use_remote_owners`, `sdk_initial_claim_recovery_preserves_unrouted_roots` |
| `tests/peer_network/residency/rebalance.rs` | `sdk_reads_index_moved_automatically_to_added_capacity`, `sdk_restores_released_range_when_rebalance_receiver_is_unreachable`, `sdk_reads_ranges_moved_automatically_to_added_capacity` |
| `tests/peer_network/residency/reclamation.rs` | Fixture helpers / retired module declarations |
| `tests/peer_network/residency/recovery.rs` | `recovered_index_can_release_residency_and_serve_later_sdk_requests`, `configured_recovery_resumes_claimed_roots_before_sdk_requests`, `discovered_recovery_resumes_claimed_ranges_before_sdk_requests`, `metadata_restoration_releases_a_settled_range_without_losing_items`, `abandoned_begin_finishes_with_one_participant_residency_slot` |
| `tests/peer_network/residency/saved_images.rs` | `eight_saved_image_reads_fit_the_participant_mailbox`, `wide_saved_image_replies_reproduce_mailbox_pressure` |
| `tests/peer_network/residency/splits/lineage.rs` | `sdk_split_epochs_follow_source_lineage_across_independent_branches`, `sdk_base_directory_tree_preserves_pages_and_transaction_replay_after_restore` |
| `tests/peer_network/residency/splits.rs` | `sdk_independent_range_splits_preserve_progress_and_replay`, `sdk_capacity_sweep_advances_past_a_transaction_blocked_split`, `sdk_capacity_recovers_publication_before_children_open` |
| `tests/peer_network/residency.rs` | `sdk_read_reacquires_released_cells`, `sdk_read_resumes_interrupted_cell_acquisition`, `sdk_transaction_reacquires_both_released_participants` |
| `tests/peer_network/table_residency.rs` | Fixture helpers / retired module declarations |

## Retained shared modules

- `tests/peer_network/account_query.rs` remains used by `tests/cellar_peer.rs`.
- `tests/peer_network/concurrency.rs` remains used by `tests/cellar_peer.rs`.
- `tests/peer_network/residency/credential_failover.rs` remains used by `tests/cellar_peer.rs`.
