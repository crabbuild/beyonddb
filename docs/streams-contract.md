# Streams implementation: closed-shard contract

## At a glance

```text
Item mutation
      |
      v
Item + stream record commit in one Cell command
      |
      v
Open source shard: More(cursor)
      |
      v
Split seals source after its final record
      +--------------------------+
      |                          |
      v                          v
Exhausted source: End      Publish routes and open children
                                 |
                                 v
                           Child shards: new records
```

| Boundary | Current state |
| --- | --- |
| Native journal | Item mutations and stream intents commit together; selected signed restart smoke passed. |
| Closed shard | The storage contract distinguishes an open empty page from an exhausted sealed shard. |
| Open work | Policy transitions, physical collection, and full Streams compatibility qualification remain unfinished. |

See [native journal slice](#native-journal-slice) for implemented behavior,
[concrete proposed change](#concrete-proposed-change) for the shared continuation
contract, and [remaining BeyondDB implementation](#remaining-beyonddb-implementation)
for open work.

## Dependency and verification record

[ExtendDB PR #372](https://github.com/ExtendDB/extenddb/pull/372) and
[ExtendDB fork PR #1](https://github.com/crabbuild/extenddb/pull/1) record the
continuation-contract review. The older
`extenddb-stream-completion.proposed.patch` targeted
`bdb7b3df4ace3b80a6e928f144036d056aec0327`; the PR superseded that patch.
BeyondDB currently pins `7eaa89b437feed0af0f05883d3f1493f86c6fc6d`
in `Cargo.toml`.

At the recorded verification point, focused SQLite and engine tests, all three
backend compile checks, and strict Clippy passed. The signed process smoke
covered SDK CreateTable/PutItem and AWS CLI Streams discovery/read through a
hard restart. Broader Streams qualification remains open.

## Native journal slice

### Write capture

`src/stream_journal.rs` appends a record to the item owner's SQL Cell in the
same command as a direct Put, Update, or Delete. Both account and routed data
Cells install `src/stream_journal.sql`. The table's installed stream policy is
read inside the command, so a stale caller hint cannot suppress capture.
Committed same-Cell and participant transaction writes use the same append
path; rejected and aborted transactions do not apply staged images. Each
command sequence plus operation ordinal gives a stable record position.

Equal before and after images, and deletions of absent items, emit no record.
Split import uses the item write helper without invoking the journal, so an
imported copy does not appear as a new mutation.

### Reading and shard identity

Native account and partition queries read that journal in sequence order with
a 1 MiB record budget and an exclusive sequence cursor. The account test
follows multiple pages after owner restoration; the routed test reads its owner
Cell and confirms imported children have no journal records.

The partition query also reads the Cell's durable split seal. The ExtendDB
storage method maps the final sealed page to `End` and an empty open page to
`More(None)`, with a 23-digit sequence width and account-scoped routing. The
routed test exercises both states and rejects a different account's shard.
Known shard IDs are validated against the canonical stream ARN, account table
generation, and installed data Cell. `LATEST` reads the owner Cell's indexed
journal tail without scanning pages. Native account and routed tests cover
validation and tail lookup. DescribeStream walks installed roots and durable
split seals to expose parent/child lineage with bounded response pages.

### Lifecycle, retention, and TTL

This slice is exercised by the native account Cell test for insert, replay,
equal-image Put, deletion, transaction commit, and rejection. The adapter now
accepts `CreateTable(StreamSpecification)`, returns its stream ARN and view
type in the table description, and passes streamed writes to the Cell command.
ListStreams and DescribeStream use a generation catalog committed with table
creation. DeleteTable marks that generation disabled before removing the table
record.

Catalog reads and record queries apply a 24-hour visibility cutoff; the old
stream remains readable after deletion and table name reuse. The account Cell
test covers this lifecycle. A signed process smoke creates a routed table,
reads its record, restarts the server hard, and reads the same record again.
Supervised account and routed Cell commands now delete expired journal rows in
bounded batches. Active routed owners are swept locally.

A durable per-account cursor scans verified tenant catalog shards to reach
idle and deleted table Cells after owner restart. Expired stream catalog rows
still need collection; policy replacement remains unsupported.

The TTL worker submits a marked delete to the routed owner Cell. That Cell
checks its current TTL policy and the item's expiry before committing the
deletion and stream record together. Its REMOVE record carries the
[DynamoDB TTL service identity](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/time-to-live-ttl-streams.html).
The native range test rejects a TTL-marked delete before TTL is enabled; the
signed SDK/CLI smoke verifies the service identity and hard-restart replay.
The new SQL table changes the unreleased version-1 schema digest; no tagged
BeyondDB release or upgrade contract exists yet.

## Why this dependency change is necessary

BeyondDB's full DynamoDB objective includes Streams and online Cell splits.
A stream shard belongs with the Cell that commits its item mutations. A split
closes the source shard and starts child shards. Consumers need an unambiguous
end to the parent before processing its children; AWS documents shard lineage
and ordering in [DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html).
`GetRecords` must stop returning an iterator when a closed shard is exhausted;
see the [NextShardIterator response contract](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetRecords.html).

The previously pinned ExtendDB contract could not express this:

- `crates/storage/src/lib.rs:159` returns `(records, Option<String>)`. The
  implementation uses that option for the last sequence returned, despite its
  misleading iterator doc comment. An empty open page returns `None`.
- `crates/engine/src/streams.rs:227` calls the backend, preserves the incoming
  sequence when that option is `None`, and always constructs `Some(iterator)`.
- SQLite, PostgreSQL, and MongoDB all implement those last-sequence semantics.
  Their shard metadata already has an ending sequence, but reads do not use it
  to signal completion.
- Upstream main at `998c12b72856dbaba8f3d508c62a4c4302be3229` was
  inspected during the proposal. It still always produced another iterator;
  a pin update to that revision alone would not resolve this boundary.

Returning an empty page, a guessed cursor, or a storage error from BeyondDB
cannot correctly terminate the iterator. A separate BeyondDB Streams HTTP
handler would duplicate ExtendDB's protocol ownership. The backend must be
able to report exhaustion through the shared storage contract.

## Concrete proposed change

The patch covers every producer and consumer found in the pinned source:

| Surface | Proposed change |
| --- | --- |
| Shared storage contract | Add `StreamContinuation::More(Option<String>)` and `End`; keep records in the same result. `More(None)` preserves the incoming position on an empty open shard. |
| Streams handler | Return no next iterator for `End`, while preserving the final page's records. Continue renewing the iterator timestamp for `More`. |
| SQLite | Read the ending sequence in the existing account-ownership join, fetch one lookahead record, and report `End` only when closed and exhausted. |
| PostgreSQL | Read ending sequence with the shard's table ID, retain the catalog ownership check, and use the same lookahead rule. |
| MongoDB | Read ending sequence from the already-fetched shard document, retain account validation, and use the same lookahead rule. |
| Page limits | Validate 1–1000 records before lookahead; fetch at most limit + 1 and return at most limit. |
| BeyondDB | The branch reads Cell-backed pages through the explicit continuation. It reports `End` after an empty sealed page and `More` while the shard remains open. |

The upstream PR tests closed-shard termination, open-page cursor behavior,
SQLite pagination, and account isolation. Focused tests passed against the
fork commit, but PostgreSQL/MongoDB still require their backend integration
gates; compiling their adapters alone does not establish runtime behavior.

**Is this the best fix?** An explicit continuation state removes the ambiguity
at the owning contract. It preserves both valid empty-open polling and final
nonempty pages, without sentinel sequence numbers, error matching, or a second
HTTP implementation. All three sibling backend producers must change with the
engine consumer. The closing writer must publish its final records before the
ending marker and never append afterward; BeyondDB must enforce that ordering
in the source Cell's seal command.

## Upstream integration

The fork PR records the reviewed introduction of this contract. `Cargo.toml`
is the source of truth for BeyondDB's current pin. Moving to an upstream
revision requires reviewing that revision and qualifying the contract again.
PostgreSQL and MongoDB runtime tests and BeyondDB's broader signed SDK matrix
remain open.

## Remaining BeyondDB implementation

This dependency fix is necessary but does not implement Streams by itself.
BeyondDB supports generation creation, discovery, and record reads through a
24-hour visibility window after deletion. Policy transitions, physical
collection, and full public Streams behavior remain unfinished.
The implementation must cover all of these boundaries before support is claimed:

1. Store stream identity, view type, generation, shard lineage, and lifetime
   independently from the current table route. Disabled/deleted tables and
   sealed split sources must retain readable history until retention permits
   collection.
2. Commit records with item changes for Put, Update, Delete, batch writes,
   TTL deletion, and committed transaction resolution. Conditions, ABORT,
   no-op writes, and split import must not emit change records. Retried apply
   must not duplicate them. Include stream images in prepare's capacity budget.
3. Fence stream enable/disable and view-type transitions across all participant
   Cells. A cached table description cannot allow a write to skip the active
   generation. Persist and recover the transition after owner loss.
4. Close the parent with its final records before activating child writers;
   publish retained shard discovery alongside the route switch. Preserve
   per-item ordering through the lineage without imposing one global writer.
5. Scale account-scoped ListStreams and paginated DescribeStream without
   rescanning prior shards or every streamless table. Qualify iterator
   validation, sequence lookups, and GetRecords response byte bounds against
   DynamoDB's limits and the full upstream protocol suite.
6. Add bounded retention and recovery for old stream generations, then qualify
   all view types, no-op/conditional writes, cross-Cell COMMIT and
   ABORT, split lineage, disable/re-enable, delete/recreate, and hard restart
   through signed clients. The current dependency's other Streams gaps, such
   as byte bounds and shard-filter support, also need qualification.

The full API and 10,000-Cell/multi-TB objectives remain open. The current
generation read path is qualified by one signed process smoke, not by the full
Streams compatibility or production-scale suite.
