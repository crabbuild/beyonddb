# API coverage and compatibility

BeyondDB exposes ExtendDB's DynamoDB JSON endpoint, but an ExtendDB handler
does not by itself make an operation supported by BeyondDB. This page describes
the pinned backend's Cell paths and their qualification. “Verified” means a
signed AWS SDK or CLI request reached a durable Cell commit and the committed
result was checked after owner or process restart. Individual fixtures cover
the cases stated here; they do not prove every DynamoDB option or error shape.

## Tables, items, and reads

| Operation or option | BeyondDB status | Boundary |
| --- | --- | --- |
| `CreateTable`, `DescribeTable`, `ListTables`, `DeleteTable` | Cell-backed; signed SDK restart coverage for creation, metadata, and deletion paths | Routed creation and large deletion can remain in transitional states while workers resume durable progress. |
| `UpdateTable` | Partial | Billing mode, provisioned throughput, table class, and deletion protection use account Cell metadata. GSI create/delete/update, stream specification, vector updates, and on-demand ceilings are rejected. |
| `PutItem`, `GetItem`, `UpdateItem`, `DeleteItem` | Cell-backed; signed SDK and owner-restart coverage | Conditions and updates run in one owning Cell command. Exact expression compatibility still needs the complete protocol suite. |
| `Query`, `Scan` | Cell-backed; signed SDK pagination and restart coverage | Query handles hash and sort keys. Scan uses bounded pages and supports parallel segments. Continuation across a directory change needs broader qualification. |
| `BatchGetItem`, `BatchWriteItem` | Cell-backed through ExtendDB; process smoke and restart coverage | A batch is not a cross-item atomic transaction. Check unprocessed items and retry according to the client contract. |
| `TransactWriteItems`, `TransactGetItems` | Durable coordinator and participant paths; cross-Cell SDK hard-restart coverage | Conflict and capacity behavior at sustained fleet load is unqualified. GSI propagation remains asynchronous after base commit. |
| `TagResource`, `UntagResource`, `ListTagsOfResource` | Cell-backed; signed SDK restart coverage | Table deletion removes tag rows in bounded generation cleanup. |

The [implementation record](implementation-status.md#current-verified-slice)
maps these paths to tests. The full upstream protocol suite remains an
[acceptance gate](implementation-status.md#acceptance-proof-for-a-server-claim).

## Indexes

| Feature | BeyondDB status |
| --- | --- |
| LSI created with a table, `ALL` projection | Implemented with base mutation and index maintenance in one Cell command; account and routed SDK fixtures cover Query/Scan, transactions, and restart. |
| LSI `KEYS_ONLY` or `INCLUDE` | Rejected at CreateTable. ExtendDB needs a shared read-plan and base-fetch capacity contract before BeyondDB can return correct attributes. |
| GSI created with a table | Independent range Cells, asynchronous journal projection, and `ALL`/`KEYS_ONLY`/`INCLUDE` Query and Scan paths. Selected signed SDK and recovery tests pass. |
| Online GSI creation, deletion, or update | Rejected by `UpdateTable`. Index backfill primitives exist, but account lifecycle orchestration and public SDK qualification do not. |
| Vector indexes and `SearchVectors` | ExtendDB has handlers; BeyondDB rejects vector index creation and has no supported Cell vector path. |

See [LSI contract](../LSI_CONTRACT.md) and
[GSI design and limits](../GLOBAL_INDEXES.md). Index support does not imply
fleet-scale index recovery or a guarantee that a hot partition-key group can
grow beyond one Cell.

## TTL

`UpdateTimeToLive` and `DescribeTimeToLive` store settings in the account Cell.
The serving worker configures a fixed expiry index in data Cells, backfills
older items in bounded commands, and conditionally removes expired items.
Settings and sweep progress survive owner restart. Expiry is asynchronous.
The global TTL listing traits remain unsupported; the worker enumerates each
configured account directly. [TTL implementation status](implementation-status.md#running-the-current-server)

## Streams

`CreateTable` can install a stream policy. Item changes and eligible TTL
removals produce records in the same Cell command as the base mutation.
`ListStreams`, `DescribeStream`, `GetShardIterator`, and `GetRecords` can read
current and deleted table generations during the 24-hour retention window.
A signed SDK write and CLI Streams read survived a hard server restart.

This is **partial Streams support**. `UpdateTable` cannot enable, disable, or
change the view type. Bounded retention sweeps cover active routed owners and
catalog-discovered dormant or deleted data Cells. Expired stream catalog rows,
full split-lineage and iterator qualification, and fleet-scale retention work
remain open. See [Streams contract](../STREAMS_CONTRACT.md) and
[implementation status](implementation-status.md#streams-dependency-contract).

## Authentication and administration

The public endpoint verifies SigV4 through ExtendDB. Long-lived credentials,
externally provisioned temporary credentials, and revocation use encrypted
credential Cells. Inline user/role policies and user/role permission boundaries
are Cell-backed; signed tests cover denial and owner restart. BeyondDB does
not issue STS credentials. Group policies, session policies and tags, and the
management APIs for accounts, users, roles, policies, and keys are unfinished.
The server uses a pass-through authorization cache so policy removal takes
effect without a stale cached grant. [Catalog implementation](../src/catalog.rs)

## Explicitly unsupported or incomplete

| Area | Current result |
| --- | --- |
| On-demand backup and restore, continuous backup, PITR | `BackupEngine` methods return `Unsupported`; there is no coordinated table-wide backup cut. |
| DynamoDB S3 import/export | Pinned ExtendDB has local-file, synchronous extensions rather than DynamoDB's S3 workflow. BeyondDB supplies empty allowed path lists, so both handlers reject requests. |
| PartiQL and Global Tables | Pinned ExtendDB does not dispatch these operations. BeyondDB has no Cell implementation or replication protocol for them. |
| SSE specification and on-demand throughput ceilings | Rejected at CreateTable; on-demand ceilings are also rejected at UpdateTable. Table class values are metadata only, without AWS pricing semantics. |
| Account-wide management, metrics, admin login, STS issuance | Cell-backed implementations are absent or explicitly unavailable. |
| Production-scale and upgrade compatibility | 10,000-Cell/multi-TB, sustained load, storage collection, and in-place upgrades of older development roots remain unqualified. |

The source gates are in [table creation/update](../src/backend.rs),
[backup methods](../src/backend/remaining.rs), and
[server wiring](../src/server.rs). For ExtendDB's own standalone feature set,
see its [differences from DynamoDB](https://github.com/ExtendDB/extenddb/blob/main/docs/differences-from-dynamodb.md).

## How to qualify a new API claim

1. Compare the backend result with ExtendDB's SQLite backend and the upstream
   protocol suite, including validation and error responses.
2. Send the operation through ExtendDB's signed public endpoint using an AWS
   SDK or CLI client. Check that its mutation and result reach a durable Cell
   commit.
3. Restart or replace the owner from published object-store state. Replay the
   request where applicable and verify the data, metadata, and error outcome.
4. For cross-Cell work, inject failure before and after the coordinator decision
   and verify idempotent participant resolution.

Until those checks pass, describe a path as implemented or partial rather than
generally compatible. The current [independent qualification runner](implementation-status.md#independent-client-qualification)
supports unchanged ExtendDB Python client tests.
