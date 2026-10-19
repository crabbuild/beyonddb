# DynamoDB API coverage

This page describes the current Cellar-backed server, not a general DynamoDB
compatibility certification. An implemented path can have focused tests while
options, error responses and fleet failures remain unqualified. See
[implementation status](implementation-status.md) for executable evidence.

## Tables, items, and reads

| Operation | Current implementation | Boundary |
| --- | --- | --- |
| Create/Describe/List/DeleteTable | Account metadata and ordered data-cell lifecycle | Creating/deleting work may require background progress |
| UpdateTable | Billing/throughput metadata, table class, deletion protection | Online GSI, Streams changes, vector updates and on-demand ceilings rejected |
| Put/Get/Update/DeleteItem | Routed commands/queries; conditions evaluated at owner | Full expression and error compatibility remains unqualified |
| Query / Scan | Ordered range traversal, pagination and parallel Scan | Cross-cell pages are not one atomic snapshot |
| BatchGet / BatchWrite | Independent item operations | A batch is not an atomic transaction; inspect unprocessed items |
| TransactWrite / TransactGet | Local fast paths and durable coordinator/participant paths | Sustained conflict/capacity and fleet fault matrices remain open |
| Tag/Untag/ListTagsOfResource | Table-generation metadata | Tags changed after creation do not change placement |

Every public table starts with **2–256 ordered ranges**, four by default. The
BeyondDB creation tag `beyonddb:cell-model` accepts `auto` or `partitioned` with the
configured count. `single` is rejected. Omitting the tag uses the configured
count. This is a BeyondDB extension, not an AWS setting.

## Indexes

| Feature | Current contract |
| --- | --- |
| Creation-time LSI, `ALL` projection | Shares base-cell mutations and read context |
| LSI `KEYS_ONLY` / `INCLUDE` | Rejected; shared read-plan/base-fetch contract is incomplete |
| Creation-time GSI | Separate ordered ranges; asynchronous `ALL`, `KEYS_ONLY`, `INCLUDE` projections |
| Strong GSI reads / unprojected base fetch | Unsupported; GSI reads do not fetch missing base attributes |
| Online index changes / vector indexes | Unsupported public paths |

See [LSIs](lsi-contract.md) and [GSIs](global-indexes.md). A hot PK collection with
an LSI must remain local; adding sort-key splits cannot bypass that restriction.

## TTL

`UpdateTimeToLive` and `DescribeTimeToLive` persist policy. Background workers
backfill the expiry index and submit conditional deletes in bounded batches.
Expiry is asynchronous: expired items may remain visible until deletion. Maintain
the account in the server's `accounts` list. See [operations](operations.md).

## Streams

Creation-time Streams capture eligible base changes and TTL removals atomically
with item mutations. Discovery and reads cover retained table generations within
the 24-hour visibility window, including deleted generations.

Streams is **partial**: `UpdateTable` cannot enable/disable it or change view type.
Iterator behavior, split lineage, physical catalog collection and fleet-scale
retention require broader qualification. See [Streams](streams-contract.md).

## Authentication and administration

ExtendDB verifies SigV4 and evaluates stored inline user/role policies and
permission boundaries. Credential secrets are encrypted in Cellar cells.
Bootstrap installs one configured credential and inline user policy. Optional
caching changes cross-node freshness; see [server reference](cellar-server.md#serving-and-caching).

BeyondDB does not issue STS credentials. Public account/user/role/key management,
group/session-policy coverage and administrative surfaces are incomplete.

## Explicitly unsupported or incomplete

| Area | Limit |
| --- | --- |
| Backups, restore and PITR | Backend methods return unsupported; no coordinated table backup cut |
| S3 import/export | The frontend's local-file extensions are disabled by empty allowed paths |
| PartiQL and Global Tables | No supported dispatch/implementation in this server |
| SSE configuration | CreateTable option rejected; credential encryption is not database-wide SSE |
| Capacity/pricing semantics | Table billing/class metadata does not establish AWS charging or throughput guarantees |
| Upgrades and fleet SLAs | In-place format upgrades, bounded recovery and production-scale capacity unqualified |

Source gates: [backend](../src/backend.rs), [remaining operations](../src/backend/remaining.rs),
[frontend wiring](../src/cellar/server.rs). Qualification requires protocol
comparison, signed client behavior, durable results and the relevant restart/fault
checks. See [acceptance proof](implementation-status.md#acceptance-proof-for-a-server-claim).
