# Embedded Cellar server

Status: implemented with a passing local signed SDK/process-restart fixture on
`beyonddb-cellar`. Root and adapter unit suites also pass. Historical Cellule
results are not Cellar qualification. Ordered routing is implemented with local
serving/split checks. Nine bucket-backed process fixtures pass, including owner
replacement with no previous local state. Legacy fixture migration and the
broader split, transaction and API/index fault matrices remain unfinished.

The `beyonddb` executable owns its configuration, executors, public listener,
TLS and ExtendDB frontend. It starts Cellar with `Node::start_external`, using
only its in-process ingress to execute compiled BeyondDB operations. Cellar
owns the authenticated private listener, physical placement, owner fencing,
replication/logs, recovery and node lifecycle. The internal command gateway is
never exposed on the public listener.

## Configuration

Use [the new configuration example](../config/cellar.example.json). Legacy
Cellule configuration is rejected rather than silently interpreted as Cellar
state. `data_dir/cellar-ordered-v1` is the new local engine directory. Earlier
branch HASH state in `cellar-v1` is not opened. Both key files
contain exactly 32 raw bytes and must be provisioned privately by the operator.
The encryption key must remain stable to read existing credential ciphertext.
Peer authentication keys must match across nodes in the same fleet.

Without `fleet`, the executable uses local Cellar persistence. This mode
qualifies local process restart, not bucket durability or node loss. With
`fleet`, add:

```json
{
  "fleet": {
    "bucket": "my-bdb-bucket",
    "endpoint": "https://s3.example.com",
    "prefix": "beyonddb-cellar/ordered-v1",
    "follower_durability": false
  }
}
```

The application constructs S3 clients from operator-supplied AWS configuration
and passes them into Cellar. The CAS clients disable retries so ownership can
observe ambiguous outcomes. Data and lease lanes use separate clients. Bucket
proof is the default. Set `fleet.follower_durability` to `true` to allow native
follower fsyncs to race bucket publication; bucket fallback remains available.
Cellar owns ensemble policy, proof selection and recovery. The three-process
RustFS fixture verifies acknowledged state on a fresh successor after an owner
kill, deletion of its local directory and withheld bundle/LTX publication. See
[follower durability](follower-durability.md) for its limits. Never reuse Cellule
object roots or another Cellar deployment's prefix. Cellar controls physical
rebalancing in fleet mode.

Public plaintext HTTP is restricted to loopback. External public serving needs
both `public_certificate` and `public_private_key`, and a matching HTTPS
`public_endpoint`. Cellar's private peer transport authenticates its protocol;
configure network isolation and TLS termination according to the deployment's
private transport policy.

The account list drives bounded table, TTL, projection, retention and
coordinator-discovery maintenance. Configure every account assigned to the
serving deployment. A sharded application inventory records range targets
before installation, so idle, deleted and sealed source cells remain
findable independently of node residency. Its fixed 256 shards per tenant
avoid one per-account inventory writer. This inventory is discovery metadata;
Cellar remains the ownership authority.

Every table starts with multiple data cells. `single` placement is rejected;
`auto` and `partitioned` use the configured initial count, from 2 through 256
without a power-of-two restriction. Four is the default. Ownership uses ordered
full-key ranges; the bootstrap template chooses schema-aware PK fences and
later splits use observed keys. An unknown workload may concentrate in one
initial interval. A hot PK can split by SK when the table has no LSIs. See
[ordered cell partitioning](ordered-cell-partitioning.md) for the contracts and
remaining qualification gates.

Cellar publishes measured process load and bounds residency with
`max_active_cells`. The RustFS process fixture verifies current lease telemetry,
the signed peer challenge and rejection of a stale generation through the SDK
management client. This does not qualify Cellule's former disk-budget or
reserved-job admission policy; resource admission beyond native residency is
still migration work.

## Starting and stopping

Build with `cargo build --bin beyonddb`. Run `beyonddb config.json --bootstrap`
once with the new credential secret supplied on stdin; the bootstrap policy
must be an operator-authored IAM policy file. Later starts omit `--bootstrap`.
The bootstrap account must be listed in `accounts`.

The public server starts after Cellar reports ready, bootstrap completes and
an initial transaction recovery pass succeeds for every configured account.
The pass uses each shard's observed pending boundary, continues healthy work
after a failure, and reports any unresolved admission or resolution failure
before maintenance starts. Fresh BEGINs resume normally; startup alone does
not authorize their abort. Initial fleet observation can take longer than
local startup; the listener is not published while that readiness gate is closed.
ExtendDB handles SIGINT/SIGTERM and drains its public listener. The application
then cancels/joins maintenance and requests the Cellar stop protocol. A node
fence or failed maintenance task withdraws public serving. Shutdown succeeds
only with drained ingress, application work and cells, zero pending events and
dispatches, quiesced logs, closed local durability and no drain timeout or elapsed
deadline. Keep both caller-owned executors alive until that outcome is returned.

`shutdown_mode` defaults to `"preserve"`, including in fleet mode. Fleet Preserve
prepares a clean reload of the same logical node using its retained local
directory and requires the SDK's clean-reload proof. Local-only nodes have no
replication marker and reopen their closed local state through normal recovery.
Use `"handoff"` when removing a fleet node
and another eligible node is running to adopt its cells. Handoff cannot complete
on the last node; it is rejected without fleet configuration.

Coordinator discovery pages durable account registrations and retained pending
records. Published decisions resolve their original participants. Fresh BEGINs
resume from the coordinator's immutable payloads, retaining original participant
identities and epochs. An abandoned BEGIN may be atomically changed to ABORT
after ten minutes; the age check and
decision share the coordinator command. This deadline does not authorize
collecting decisions or changing an admitted participant set. Recovery cannot
infer ABORT from an unavailable owner or an uncertain mutation reply.

## Qualification still required

The new `tests/cellar_sdk.rs` uses signed SDK requests against the real process,
including typed/large escaped item values, conditional writes, token replay,
Query pages and restart. The Put/Get/Update/Delete, transaction replay and
query/restart cases pass. It is local qualification only. The separate
`tests/cellar_process.rs` target has passing runs for nine bucket-backed cases:
coordinator history, GSI projection recovery, large transactional reads on a
fresh owner, metadata caching, segmented LSI scans, and signed Streams/TTL recovery with
retained generations after table deletion and recreation, native capacity and
authenticated peer identity, the broad SDK restart scenario and native follower
recovery with bundle/LTX publication withheld. The broad SDK scenario
keeps batch/transaction/index/large-value checks across ordered ranges through
hard and graceful restarts, then deletes and recreates a table. The
account-domain fixture also passes SDK handoff to a second node. Twenty-two cases in
`tests/cellar_transactions.rs` qualify token retention, cross-cell write skew,
bounded resume, delayed resolution, shared read snapshots, lost transaction
phases, caller admission refusal, competing drivers, fair background discovery,
acknowledged read-result cleanup, TTL lock races, ordered range-read barriers and
prepared-commit/mixed-participant handoff through native SDK ingress.
The native coordinator cases also retain history across 24 shards under a
16-cell resident cap, recover a partial read on a fresh owner, discover a later
BEGIN after settled passes, and admit four table ranges after 32 coordinator
shards have exceeded residency. A caller-injected child-install refusal retains
the same pending split until capacity returns. Proven pre-submission Capacity
maps to LimitExceeded; native HTTP failure remains ambiguous. The native recovery
admission cases cover BEGIN/COMMIT/ABORT at startup and while serving, progress
past failed records and shards, retry without overwriting newer values, and
deferred discovery beyond the initial pending boundary. A server regression
checks failure before maintenance starts and successful retry. These use an
explicit caller policy before SDK submission, not physical network faults.
`tests/cellar_peer.rs` adds actual native peer sockets behind an opaque TCP
cut. With SDK route caching both disabled and enabled, signed requests retain
numeric pagination, two-range reads and 50 concurrent increments without AWS
client retries. Removing the old directory before lease-expiry recovery retains
values and advances native owner epochs. Authentication fails closed during a
credential-owner cut while healthy local data remains readable, then recovers
after expiry. Unknown keys create no credential cells; revocation remains fresh.

The public frontend paces pure SDK queries on HTTP 429/503 within a 30-second
backpressure window. This permits bursts through native per-cell admission
without caching credential, authorization or item values. Commands keep their
single-attempt transport contract, including transaction read-image capture;
HTTP status still does not prove a mutation was unstarted. Separate policy
checks cover read retry hints, deadline failure and mutation non-retry.
Eight `tests/node_log_authority.rs` cases exercise the native single lease
writer and folded log through real storage faults. They cover normal and slow
renewals, an applied CAS reply lost before delivery, delayed/stalled ordinary
reads, graceful sealing, natural expiry after sustained renewal failure and
closed ingress after fencing. An expired unavailable follower cannot prove a
new write while both bundle and LTX publication are withheld; that same signed
request completes when publication resumes. The fixtures never edit authority
records or require epoch rotation from TTL expiry alone.
Four additional native shutdown cases verify immediate certified same-node
restart, incomplete drain with unpublished signed work, a real provider CAS
that commits after the node closes, and bounded read/write storage stalls.
A clean report must match the actual local certificate; incomplete stops
cannot grant one and must close ingress. After ordinary expiry, signed reads
retain acknowledged rows in both data ranges. The tests do not retain
Cellule's separate application-owned session retirement API.
Six `tests/cellar_indexes.rs` cases retain local/global index mutation,
transaction, split, replay and restart contracts. LSI splits keep PK collections
together and retain logical query cursors. GSI replay preserves large chunked
images, versions and tombstones. Maintenance visits every published GSI range
through native ingress even when its base outbox is empty, so idle and empty
ranges recover on a new owner. Fixtures observe actual SDK factories and owner
epochs before issuing user index reads, after deleting the former directories.
Backfill cases preserve the historical delivery barrier, prepare reservations
and unfinished scan policy across owner replacement and range splits. The GSI
transfer case retains 70 wide-key records, binary images and tombstones across
partial-import and post-publication replacements. Equal index PK/SK entries
span both children via the complete base-key suffix; signed forward/reverse
queries retain all rows and full continuation keys after recovery.
Embedding callers can own a `TaskGroup` and call its bounded `shutdown` before
stopping the SDK node; the executable follows that same shutdown order.
Two multipart cases preserve escaped inputs larger than one command,
immutable/independent upload identities, exact receipt replay and durable
payload recovery. Complete SDK snapshots verify expired staging cleanup and
consumed-input collection. A fresh owner restores partial chunks after removal
of the former directory. Another replacement occurs after ABORT but before
the remaining account/data pieces arrive: delayed prepares remain rejected,
normal writes succeed and signed reads contain no aborted payload. The
controlled fixture clock governs application deadlines; SDK lease time stays
independent.
Three `tests/cellar_capacity.rs` cases verify physical reservation occupancy,
near-full unrelated writes, durable Throttled prepare rejection and reservation
release on COMMIT/ABORT. The original 144/32/2 MiB fixture budgets and item
sizes are retained. Complete SDK snapshots audit rollback and cleanup; signed
reads and exact rejected receipts survive fresh-owner recovery. Submitted SQL
or transport errors remain ambiguous. A forced physical SQLite FULL upload is
still unqualified on the public SDK and remains a separate migration gate.
The native serving target retains the large ordered numeric/IAM contract:
two initial ranges grow through automatic and explicit splits, one PK spans
actual cells, token replay survives the new routes, and signed calls cross a
separate native peer. Policy boundaries, revocation and encrypted credential
state survive deletion of both owners' directories. Exact-limit Query pages
now retain the owner's exhaustion proof when its range finishes the requested
span, avoiding a redundant terminal page without another cell RPC. Logical
target validation is available before admission; native authority stays in
the SDK. CI executes these contracts alongside the other migrated suites.
The data-range contract also retains activation fencing, durable transaction
visibility, sealed export/import, resumable split publication and child stream
lineage. Ordered fixture rows occupy both children. Complete non-owning SDK
snapshots audit all four data journals after shutdown. Deleting the old local
directory and observing native lease/sample expiry precede another node's
restoration of all seven state scopes at higher ownership epochs, including
the retired source and prepared child intent. Unfinished BEGINs remain pending
until explicit fenced recovery. The physical SQLite FULL upload fixture still
requires its separate SDK equivalent.
Five `tests/cellar_metadata.rs` cases retain table-class change history and
limits, deletion protection, capacity and logical byte counters, compacted
transaction replay and sparse LSI/GSI statistics. They run under 8/11/16-cell
resident budgets with signed SDK retries disabled. Fresh owners recover the
same account scope at higher epochs after local-file deletion and observed
native lease/sample departure. Real base/GSI splits during sampling reject
stale statistics before publication. Public tables retain two initial ordered
ranges; a second observed key is inserted before splitting a one-item source.
Three `tests/cellar_batches.rs` cases preserve per-child returned-update
conditions, stream ordinals, exact receipt replay and the 390,000-NUL escaped
image case. A real SDK peer forwards prepare batches, then recovers prepared
intents and their shared receipt after the original owner loses its lease.
Another owner retains independent COMMIT/ABORT states. Each handoff removes
the former local directory and verifies the same data scope at a higher epoch;
signed reads and ordinary writes prove final visibility and released locks.
Three `tests/cellar_images.rs` cases retain immutable saved images, bounded
wide-read fallback and exact large conditional-failure images. Original
380/384 KiB binary and NUL payloads remain intact. Signed cross-cell reads
preserve response order and absent positions. Canceled-token retry and terminal
replay preserve later writes. Separate SDK peers serve initial requests;
fresh owners recover the same data scopes at higher epochs after deletion of
all former local directories and observed lease/sample departure. Cellule
mailbox measurements remain a separate migration gate.
Two `tests/cellar_domain.rs` cases verify metadata-cache query counts and
generation fencing, and usage counters against the compiled data schema.
Directory fixtures exercise full-contract transfer, immutable initial-count
activation, 1,024-entry metadata copy/cutover and interrupted retirement. A
48-root/16-resident-cell case retains metadata fences and actual two-cell data
through fresh-owner recovery. Those development-bucket cases qualify metadata
behavior; they do not establish fleet capacity or 1,024 active data partitions.
Interrupted creation also recovers its original two-cell plan before ACTIVE,
then retains more than 64 MiB of data through another owner handoff.
Paged serving also runs over 65 distinct installed data cells, checking Scan
and TTL continuation through a 64-entry page boundary and the 16-table TTL
schedule bound. A separate metadata-only fixture grows a valid 256-entry plan
to 1,024 entries, splits its directory, and retains 1,025 pageable entries and
an unfinished leaf transfer on a fresh owner after deleting the old directory.
It does not claim 1,025 installed data cells.
Two table-lifecycle fixtures also exercise repeated deletion/recreation with
independent two-cell base/GSI directories under a 16-cell resident limit, and
recovery of a committed retirement whose reply was lost before acknowledgement.
Retired directory fences and old base items survive a fresh-owner handoff.
Creating new cells after handoff waits for the departed node's observed native
lease to expire and the native placement sample to observe departure; the
fixture does not assert immediate fresh-cell placement.
Signed coalescing qualification also retains 64 returned updates, eight
same-key increments and 32 concurrent two-item transactions, including a lost
prepare-batch reply and fresh-owner recovery. Registration batches preserve
caller transport/admission isolation, durable discovery and cancellation
semantics. One HTTP gateway bounds dispatch to 128 queued/submitted calls and
eight per target through response completion. This is local backpressure;
Cellar still owns cross-gateway admission, execution and durability. Mutations
retain one transport attempt. Development/test builds optimize LTX checksum
and compression dependencies so physical reservation traffic can meet the
same durability deadlines; no reservation or workload bounds were reduced.
Three native public-placement cases also reject Single selection, retain
four-range plans despite later creation-policy and tag changes, preserve atomic
reads within one range of a multi-range table, and split GSI ranges independently
of the base plan. Token replay, exact stream counts and signed index values
survive fresh-owner recovery after local-file deletion. SDK factory identities
and ownership rows prove higher epochs for the restored data/index scopes.
The current fixture suite passes 130 focused native tests and all nine explicitly
executed RustFS process cases, zero failed/ignored. See the current fingerprint
and qualification boundaries in [migration evidence](../CELLAR_MIGRATION.md).
Caller refusal is checked before SDK submission; a native HTTP error remains
ambiguous. These are application transport faults; they do not qualify fleet
fencing or follower-log recovery; the separate process fixture supplies the
specific follower recovery evidence described above. Port the remaining legacy
application fixtures and finish the ordered API/index and fault matrices before
claiming the requested migration complete. Existing CI's all-target checks must
remain enabled; incompatible legacy fixtures are work to port, not tests to
silence.
