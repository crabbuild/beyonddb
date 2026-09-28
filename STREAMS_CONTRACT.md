# Streams implementation: closed-shard contract

Status: [ExtendDB PR #371](https://github.com/ExtendDB/extenddb/pull/371) is
open from the reviewed fork commit pinned by this BeyondDB implementation
branch. The older `extenddb-stream-completion.proposed.patch` records the
original proposal against `bdb7b3df4ace3b80a6e928f144036d056aec0327`;
the upstream PR supersedes it. Focused SQLite and engine tests, all three
backend compile checks, and strict Clippy passed on current ExtendDB main.
Signed SDK qualification of BeyondDB Streams is still pending. This branch now
contains a native Cell journal slice, but the public Streams API remains blocked.

## Native journal slice

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

This slice is exercised by the native account Cell test for insert, replay,
equal-image Put, deletion, transaction commit, and rejection. It does not yet
expose `CreateTable(StreamSpecification)`: the adapter still rejects that
request, and the ExtendDB Streams read methods remain unsupported. A native
stream policy can be installed only by the direct Cell command during tests.
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
- Current upstream main `998c12b72856dbaba8f3d508c62a4c4302be3229` was inspected
  too. It still always produces another iterator; a pin update alone does not
  resolve this boundary.

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
| BeyondDB | Its current unsupported `StreamEngine` methods already use the result alias. Implement Cell-backed pages against the new explicit continuation after the dependency is validated. |

Two proposed handler tests cover closed-shard termination and open-page cursor
preservation/advancement with timestamp renewal. Two proposed SQLite tests
cover an empty open/closed shard, a closed shard with multiple pages including
an exactly-full final page, and account isolation. These tests are **not yet
compiled or run**. PostgreSQL/MongoDB require their backend integration gates;
compiling their adapters alone will not establish their runtime behavior.

**Is this the best fix?** An explicit continuation state removes the ambiguity
at the owning contract. It preserves both valid empty-open polling and final
nonempty pages, without sentinel sequence numbers, error matching, or a second
HTTP implementation. All three sibling backend producers must change with the
engine consumer. The closing writer must publish its final records before the
ending marker and never append afterward; BeyondDB must enforce that ordering
in the source Cell's seal command.

## Upstream integration

The fork commit is a temporary dependency while the upstream PR is reviewed.
BeyondDB must pin the merged upstream revision before release. PostgreSQL and
MongoDB runtime tests and BeyondDB's signed SDK and process recovery gates
remain to be run after the Cell-backed implementation exists.

## Remaining BeyondDB implementation

This dependency fix is necessary but does not implement Streams by itself.
BeyondDB still explicitly rejects public streamed writes and Streams API operations.
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
5. Implement account-scoped ListStreams, paginated DescribeStream, iterator
   validation, sequence lookups, and bounded GetRecords. Enforce the response
   byte budget as well as record count; this proposal only changes completion.
6. Add bounded retention and recovery for old stream generations, then qualify
   all view types, no-op/conditional writes, TTL identity, cross-Cell COMMIT and
   ABORT, split lineage, disable/re-enable, delete/recreate, and hard restart
   through signed clients. The current dependency's other Streams gaps, such
   as byte bounds and shard-filter support, also need qualification.

The full API and 10,000-Cell/multi-TB objectives remain open. No Streams support
or production-scale claim follows from this proposal.
