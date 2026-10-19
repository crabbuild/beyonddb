# Metadata ownership and discovery

Metadata is application state executed in Cellar cells. It describes tables and
managed cell targets; it does not replace Cellar's physical ownership authority.
Avoid an unbounded per-table route map or a fleet-wide metadata writer.

## Current topology

| State | Scope | Purpose |
| --- | --- | --- |
| Account catalog | Account | Names, generations, lifecycle anchors, policies and coordinator registrations |
| Credentials | Key-derived shard | Encrypted secrets, status and principal/account binding |
| Range directory | Base table or GSI generation | Bounded ordered routing pages and split publication |
| Managed-cell inventory | Tenant, 256 fixed shards plus discovery root | Find installed, idle, deleted and sealed range targets |
| Transaction coordinators | Account-scoped token sharding, 4096 namespace shards | Immutable requests, decisions and participant progress |
| Data / GSI cells | Range generation | Items, projected rows and range-local durable work |

These are logical shard namespaces, not a requirement that every shard is resident
at all times. Account, credential, inventory, directory and coordinator cells all
consume residency and resources when active. Count them when sizing a node.

## Publication and discovery

A lifecycle operation records intent and discovery metadata before installation.
Matching installations precede visible routes. Bounded inventory/directory pages
let maintenance continue across restarts without depending only on cells currently
resident on a node.

The inventory retains discovery of source cells after splits and table deletion
so pending journal, retention and transaction work remains reachable. Discovery
of a target is not permission to execute it: the native SDK still resolves and
fences its owner.

## Lifecycle and recovery

| Event | Required invariant |
| --- | --- |
| CreateTable retry | Reuse the generation's persisted plan; do not reinterpret new node defaults |
| Split | Publish only matching child installations; retain source work and lineage |
| Delete/recreate same name | New generation cannot inherit old data, tags or continuations |
| Owner loss | Recover under Cellar authority; keep discovery separate from claim proof |
| Metadata outage | Do not fabricate empty state or bypass authorization; preserve retryable/ambiguous outcomes |
| Reclamation | Prove transaction, journal and retained history work no longer needs the target |

One account remains a shared control path. Coordinator sharding does not remove
account registration/admission work. The [local benchmark](local-benchmark.md)
observed account-cell refusals under transaction load; this is a measured
bottleneck, not a reason to bypass its durable registration.

## Source and qualification

| Contract | Entry point |
| --- | --- |
| Managed inventory | [inventory](../src/cellar/inventory.rs) |
| Provisioning and registration | [provisioning](../src/cellar/provision.rs) |
| Catalog and lifecycle | [table](../src/table.rs), [catalog](../src/catalog.rs) |
| Directory protocol | [directory](../src/directory.rs) |
| Coordinator namespace | [coordinator](../src/transaction_coordinator.rs) |
| Current fault targets | [metadata](../tests/cellar_metadata.rs), [directories](../tests/cellar_directories.rs), [creation](../tests/cellar_creation.rs), [reclamation](../tests/cellar_reclamation.rs) |

Fleet-wide scheduling for all dormant targets, storage-history collection and
bounded recovery under sustained overload remain open. Historical hash layouts
and retired fixture counts are not current topology guarantees.
