# Local secondary indexes

LSIs share their base cell's transaction and read context. BeyondDB currently
accepts **creation-time `ALL` projection only**. `KEYS_ONLY` and `INCLUDE` are
rejected rather than returning an incomplete attribute or capacity contract.

| Projection | CreateTable | Read boundary |
| --- | --- | --- |
| `ALL` | Implemented | Base and local index data share the owning cell |
| `KEYS_ONLY` | Rejected | Nonprojected attributes may require a base fetch |
| `INCLUDE` | Rejected | Same missing read-plan/base-fetch accounting contract |

## Atomicity and locality

Base writes, LSI updates and committed transaction effects execute in one local
command. Index reads use the owning cell's committed context. The PK collection
must remain together when LSIs exist; do not split one hot PK by sort key across
independent writers to bypass that rule.

Logical item/index constraints and physical prepare reservations both matter.
Available cell capacity can reject work before an application's expected
collection size. No cloud-equivalent collection capacity is qualified.

## Why other projections remain gated

The pinned frontend/storage boundary does not convey the complete LSI selection,
base-fetch and capacity plan needed for these cases:

| Request on a partial LSI | Needed result |
| --- | --- |
| Default / `ALL_PROJECTED_ATTRIBUTES` | Projected key/attribute set |
| `ALL_ATTRIBUTES` | Include required base attributes |
| Projection/filter using nonprojected attributes | Fetch in the same read context and account for that work |
| Continuation or `COUNT` | Preserve evaluated-key and capacity semantics after filtering |

A safe implementation needs a reviewed shared ExtendDB contract, backend
adaptation and differential signed-client tests before removing the creation gate.
Do not patch the dependency cache or add a separate BeyondDB HTTP handler.

## Code and qualification

See [table creation](../src/backend.rs),
[partition indexes](../src/partition/indexes.rs),
[local index state](../src/secondary_index.rs),
[native index tests](../tests/cellar_indexes.rs) and
[process assertions](../tests/server_binary/local_indexes.rs).

Current coverage does not establish every Select/filter/capacity combination,
large hot collections or fleet-scale behavior. See [API coverage](api.md#indexes).
