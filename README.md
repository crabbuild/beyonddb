# BeyondDB

BeyondDB is an **in-progress, self-hosted DynamoDB-compatible service**. It
uses [ExtendDB](https://github.com/ExtendDB/extenddb) for the DynamoDB HTTP
protocol, SigV4, IAM evaluation, validation, and expressions. It uses
[Cellule](https://github.com/crabbuild/cellule) for fenced Cell ownership,
SQLite execution, LTX publication, and object-store durability. The reviewed
dependency revisions are pinned in [Cargo.toml](Cargo.toml).

An AWS SDK can send signed requests to BeyondDB's public endpoint. Core table
and item operations, transactions, TTL, selected secondary indexes, and a
partial Streams path have durable Cell implementations and focused restart
tests. **BeyondDB is not yet a complete or production-qualified DynamoDB
replacement.** See the [API coverage and gaps](docs/api.md) before choosing a
workload.

## Start here

| Need | Read |
| --- | --- |
| Configure and start a node | [Deployment guide](docs/deployment.md) |
| Send AWS CLI requests | [User guide](docs/user-guide.md) |
| Check an operation or limitation | [API coverage](docs/api.md) |
| Understand Cells, routing, and transactions | [Architecture](docs/architecture.md) |
| Follow implementation evidence and open gates | [Implementation status](docs/implementation-status.md) |

## How a request reaches durable storage

```mermaid
flowchart LR
    Client["AWS SDK / CLI"] --> Public["ExtendDB public HTTP endpoint"]
    Public --> Auth["SigV4, IAM, validation, expressions"]
    Auth --> Adapter["BeyondDB StorageEngine adapter"]
    Adapter --> Route["Cell directory and owner routing"]
    Route --> Cell["Owning Cell: one SQLite command"]
    Cell --> LTX["LTX publication"]
    LTX --> Store["Conditional object store"]
    Store --> Reply["Acknowledged response"]
```

The account Cell owns table metadata; separate data Cells own partition-key
ranges. A partition-local mutation, its result, and its stream intent commit
in one Cell command. Cross-Cell transactions use a durable coordinator decision
and idempotent participant resolution. [Architecture](docs/architecture.md)
explains ownership, failure recovery, and index propagation.

## Current capability boundary

| Area | Current state |
| --- | --- |
| Tables and items | Create/describe/list/update/delete tables; keyed CRUD, Query, Scan, batches, conditions, expressions, and pagination have Cell paths. Some `CreateTable` and `UpdateTable` options are rejected. |
| Transactions | `TransactWriteItems` and `TransactGetItems` use durable coordination across Cells, with signed SDK restart tests. |
| Indexes | LSIs support `ALL` projection. GSIs created with a table support `ALL`, `KEYS_ONLY`, and `INCLUDE` through asynchronous projection. Online GSI changes and non-`ALL` LSI projections remain unsupported. |
| TTL and Streams | TTL settings and bounded expiry sweeps are Cell-backed. A stream enabled at table creation can journal writes and expose current and retained generations; stream policy updates and full lifecycle qualification remain open. |
| Operations | Backup, restore, PITR, account/IAM management, and safe data-format upgrades remain unfinished. Fleet-scale throughput and unattended recovery are unqualified. |
| Other DynamoDB features | PartiQL and Global Tables are not dispatched by pinned ExtendDB. Its local-file import/export extensions do not implement DynamoDB's S3 workflow; BeyondDB disables them. |

The table is a guide, not a blanket compatibility claim. The
[API matrix](docs/api.md) distinguishes implemented methods from operations
verified through signed SDK requests and owner restart.

## Minimal client example

After following the [deployment guide](docs/deployment.md), configure an
account's access key and point the AWS CLI at the public listener:

```sh
export AWS_ACCESS_KEY_ID='your-bootstrapped-key-id'
export AWS_SECRET_ACCESS_KEY='your-bootstrapped-secret'
export AWS_DEFAULT_REGION='us-east-1'
export AWS_CA_BUNDLE='/etc/beyonddb/public-ca.crt'
export BEYONDDB_ENDPOINT='https://ddb.example.com:8000'

aws dynamodb list-tables --endpoint-url "$BEYONDDB_ENDPOINT"
```

The CA bundle must trust the public listener certificate. A loopback listener
without TLS uses an `http://` endpoint and does not need `AWS_CA_BUNDLE`.
For runnable table and item commands, see the [user guide](docs/user-guide.md).

## Development and qualification

The ordinary test suite excludes ignored process tests. The latter start a
RustFS fixture, use signed AWS SDK calls, kill the server, and read committed
state after fenced recovery. They require Docker, the AWS CLI, and OpenSSL.
Use a unique target directory under `$HOME/Workspace/crabbuild-target` on
workstations with the mounted Workspace volume.

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/beyonddb-docs-3c23"
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --lib
cargo test --test server_binary -- --ignored --test-threads=1
```

The [independent client qualification instructions](docs/implementation-status.md#independent-client-qualification)
run ExtendDB's Python protocol tests against the BeyondDB binary. Passing
selected tests does not establish full DynamoDB compatibility. For scale
claims, use the [measured completion gates](SCALING.md#completion-proof).

## Design and evidence

- [Cross-Cell transaction protocol](CROSS_CELL_TRANSACTIONS.md)
- [Metadata ownership](METADATA_SHARDING.md)
- [Global secondary indexes](GLOBAL_INDEXES.md)
- [Streams contract](STREAMS_CONTRACT.md)
- [Local secondary index contract](LSI_CONTRACT.md)
- [Scaling and measured limits](SCALING.md)

BeyondDB is licensed under Apache-2.0. Its contributor invariants are in
[AGENTS.md](AGENTS.md).
