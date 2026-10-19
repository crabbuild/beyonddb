# BeyondDB documentation

BeyondDB embeds ExtendDB's DynamoDB frontend and the Cellar runtime in one
executable. Tables use ordered data ranges across multiple cells.

**Release status:** development software with focused SDK and recovery coverage.
Full DynamoDB compatibility, backup/PITR, safe format upgrades, sustained fleet
capacity and unattended recovery are not production-qualified. Use the limits
below to evaluate a deployment; these guides do not establish a production SLA.

## Start here

| Task | Guide |
| --- | --- |
| Run a local node | [Repository quick start](../README.md#quick-start) |
| Deploy, bootstrap and add peers | [Deployment](deployment.md) |
| Configure the current executable | [Server reference](cellar-server.md) |
| Send signed requests | [AWS CLI guide](user-guide.md) |
| Check an operation or option | [API coverage](api.md) |
| Inspect failures and operational limits | [Operations](operations.md) |
| Measure a workload | [Performance](performance.md) · [Local benchmark](local-benchmark.md) |

## Design and evidence

| Topic | Contract |
| --- | --- |
| Runtime ownership | [Architecture](architecture.md) |
| Ordered keys, routing and splits | [Table partitioning](ordered-cell-partitioning.md) |
| Metadata and discovery | [Metadata](metadata-sharding.md) |
| Transaction decisions and recovery | [Cross-cell transactions](cross-cell-transactions.md) |
| Secondary indexes | [GSIs](global-indexes.md) · [LSIs](lsi-contract.md) |
| Streams and retention | [Streams](streams-contract.md) |
| Follower acknowledgement and recovery | [Follower durability](follower-durability.md) |
| Capacity planning | [Scaling](scaling.md) |
| Tests, open gates and historical evidence | [Implementation status](implementation-status.md) |

## Reading conventions

| Label | Meaning |
| --- | --- |
| Implemented | A current code path exists; check its stated limitations |
| Focused coverage | Registered tests exercise specific cases; this is not full compatibility |
| Unsupported | The public path is absent or rejects the option |
| Unqualified | Required compatibility, fault or scale evidence is incomplete |

[Cargo.toml](../Cargo.toml), [the JSON example](../config/cellar.example.json),
[source configuration](../src/bin/beyonddb.rs) and [CI](../.github/workflows)
are the sources of truth for versions, fields and executable checks. Runtime
settings from earlier implementations are not valid configuration examples.

Architecture and flow visuals are self-contained SVGs in `diagram/`. Mermaid
blocks contain sequence diagrams only. Detailed guides retain protocol reasoning, failure analyses and revision-scoped
evidence in place. Current entry points distinguish those records from supported
operational instructions; Git history provides the exact original snapshots.
