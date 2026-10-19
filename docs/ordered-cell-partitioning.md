# Ordered table partitioning

A table generation owns one ordered key space. Its directory maps adjacent,
nonoverlapping intervals to logical data cells. Cellar places those cells on
nodes independently of the logical range layout.

![One ordered table spans ranges on multiple nodes; moving a cell differs from splitting its range](../diagram/operations/ordered-ranges.svg)

## Ownership and keys

| Concept | Contract |
| --- | --- |
| Point operation | The complete primary key maps to one range owner |
| Query | Visits cells intersecting the requested PK and sort-key bounds |
| Scan | Walks bounded pages; parallel segments use the backend's segmentation contract |
| Cell move | Preserves range/cell identity; changes the physical owner under a native fence |
| Range split | Creates child identities and publishes new directory ranges |
| Route version | Includes table generation, key encoding and application range epoch |

`keyspace` encodes the primary key as ordered components:

```text
partition-key-only table: ordered(PK)
composite-key table:      ordered(PK) || ordered(SK)
```

There is no hash, node address or cell ID before those bytes. String and binary
components preserve byte order. Numbers use canonical exact decimal ordering,
not `f64`. Components use type tags, escaped zeros and terminators. The current
encoding version and maximum sizes are defined in
[keyspace](../crates/keyspace/src/lib.rs); schema mismatch must be rejected.

## Bootstrap and directory

The executable accepts `initial_partitions` from 2 through 256, default four;
counts need not be powers of two. Bootstrap selects schema-aware PK fences.
That template cannot predict an unknown workload's occupancy. Existing table
generations retain their creation plan when node defaults change.

A creation intent and managed-cell inventory precede installation. Each range
must match its table/generation plan before the directory and ACTIVE route are
published. The serving path must not fall back to an account-local item store
when range provisioning is unavailable.

Directory pages are bounded and independently split. Routes carry logical
identities and intervals, not a permanent map of node addresses. Both directory
and data operations validate generation/epoch; stale native owners remain subject
to Cellar's authority fence.

## Query and continuation

A Query's logical key span may intersect multiple cells, including several cells
for one PK. Forward/reverse traversal must retain order and seek from the logical
`LastEvaluatedKey`, not an obsolete physical cell offset. Filtering can yield an
empty result page with a continuation; consumers must follow it.

A page limit is not a whole-table snapshot. Use transactional reads for an atomic
multi-key view. Route changes require current generation/epoch checks; the full
compatibility matrix across concurrent splits and filters remains unqualified.

## Splits and index locality

The split protocol seals the source, transfers bounded state to children and
publishes the replacement route after matching proofs. Replay and recovery must
retain unresolved transactions, index delivery and stream lineage; copied rows
are not new application mutations.

A hot PK can split by SK only when LSI locality allows it. With LSIs, that PK's
item collection and its local indexes must remain together. GSIs use their own
ordered ranges, versions and delivery progress. Do not describe a base split as
an atomic GSI cutover.

## Code and qualification

| Boundary | Source / test |
| --- | --- |
| Ordering and range validation | [keyspace](../crates/keyspace/src/lib.rs) |
| Bootstrap plans | [provisioning](../src/cellar/provision.rs) |
| Point/query/scan routing | [routing](../src/routing.rs), [directory](../src/directory.rs) |
| Split state | [split](../src/split.rs), [partition](../src/partition.rs) |
| Current integration targets | [serving](../tests/cellar_serving.rs), [directories](../tests/cellar_directories.rs) |

Focused serving/split cases are available. Automatic balancing, broad concurrent
split histories, retained-source collection and production-scale fairness require
additional evidence; see [scaling](scaling.md) and [qualification](implementation-status.md).
