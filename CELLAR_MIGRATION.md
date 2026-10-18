# Cellar runtime migration

This worktree migrates BeyondDB from Cellule to the embedded Cellar SDK while
retaining the existing ExtendDB frontend and BeyondDB table algorithms.

The branch starts from BeyondDB `c05cae8` (current `origin/main` when the worktree
was created). Cellar is pinned to the crates.io release
`cellar = { package = "cellar-main", version = "=0.1.0" }`; ExtendDB remains at
its existing reviewed fork revision. Old Cellule storage roots must not be
opened as Cellar state; the initial deployment uses a new storage prefix.

## Registry release

The unified lockfile resolves `cellar-main`, `cellar-logic`, `cellar-native` and
`cellar-ltx` to crates.io `0.1.0` with registry checksums. Metadata inspection
verifies that no Cellar Git dependency or Cellule package remains, and resolving
the release changed only those four dependency packages. The release declares
Rust 1.94.1, within BeyondDB's existing Rust 1.97 requirement. Its package source
records publication commit `f97a8aa4fd7ddc5e81807ddcbe1cc72dcdd9eb19`.

Earlier qualification below used Git revision
`fa62847f496eefe393ce66e4bee6c477b712a832`. Registry-release qualification is
recorded separately; those earlier results remain evidence for that Git build.

### Registry qualification

All 91 focused tests pass on crates.io `0.1.0`: twelve adapter units, two
embedded fixtures, nine keyspace cases, 17 root units, one account fixture,
nine directory cases, two domain cases, six index cases, two native peer
cases, one signed local SDK case, 22 transaction cases and eight native
lease/log cases. The thirteen rebuilt focused test executables were checked
for SHA-256 equality before running from the internal SSD, with unchanged
parallelism, assertions and deadlines. The index target passed all six cases
in 38.94 seconds during this complete rerun.
Strict Clippy passes for workspace libraries/binaries and all selected migrated
integration/process targets. Production build `11432e7`, including the frontend
read policy and idle-index worker fix, passed all nine explicitly executed
RustFS process cases in 464.15 seconds, with zero failures or ignored cases.
The subsequent backfill/transfer port changes only tests and documentation;
its server executable still matches that process build's SHA-256.
They cover the broad signed
SDK/restart scenario, measured capacity and peer identity, 70 coordinator shards
beyond residency, native follower recovery
with bundle and LTX publication withheld, GSI recovery, large transactional
reads, metadata caching, LSI pagination and retained Streams after hard restart.
The process test executable was also copied and checked for SHA-256 equality;
it ran serially with the same flags used by qualification CI. A SHA-256 check
also confirmed the server executable stayed unchanged throughout that run.

### Native peer qualification and read backpressure

`tests/cellar_peer.rs` adds two real-SDK socket cases, each with idle route
caching disabled and enabled. Public signed requests enter a different node
from the observed credential/account/data owner. The fixture forwards opaque
native peer traffic through a TCP proxy; cutting it closes existing streams
and refuses new ones. It observes actual SDK cell identities through a
transparent application factory and reads native owner records; it never
installs or edits ownership/lease records.

One case preserves the existing 50 concurrent AWS SDK increments with client
retries disabled, two independently owned ordered data ranges, and numeric
forward/reverse Query pagination. After SDK Preserve shutdown, deletion of the
old local directory and observed lease/sample expiry, another node recovers
the same scopes at higher physical epochs with all values intact. The
credential case keeps account and selected data reachable on the frontend,
proves that a failed credential-owner connection cannot grant takeover while
its lease is live, then recovers authentication after expiry without the old
directory. Unknown keys create no credential scopes or factories, and durable
revocation remains visible. Frontend value caches stay disabled in both modes.

The port first exposed a fixture-only ingress budget of 32 against 50 callers;
the fixture now matches the executable's 128 ingress/stateless permits and
16 per-cell requests. With ingress corrected, account metadata queries still
reproduced native per-cell HTTP 503 under the burst. The public frontend now
paces only read-only invocations on HTTP 429/503 within a 30-second admission
budget, honoring numeric Retry-After and bounded backoff. It caches no read
values. Commands, including transaction read-image capture, retain one
transport attempt and ambiguous-outcome handling. Unit checks preserve this
boundary, non-backpressure errors and deadline failure. The original native
50-writer scenario and the cleaned two-case suite pass under the actual limits.

Tokio time is now a normal adapter dependency as well as an existing dev
dependency; no package versions or lockfile entries changed. Temporary probes
were removed. The full nine-process run also passed after this frontend policy
change, as recorded in the registry qualification above.

All-target Clippy still exits 101 in two retained legacy roots: 61 errors in
`elastic_cells` and 60 in `peer_network`. The native `node_log_authority` target
now compiles and passes selected strict Clippy. No gates were disabled.

### Native lease and folded-log qualification

`tests/node_log_authority.rs` replaces the old application-owned
`NodeLeasePublisher` fixture with eight integration cases over real Cellar
nodes and signed AWS SDK requests. Ordinary storage and the lease safety lane
share the same backing store through distinct observed clients. Faults delay
real reads/writes, lose an applied CAS reply or withhold payload admission;
they never construct or edit lease, ownership or enrollment records.

| Former publisher concern | Native SDK evidence |
| --- | --- |
| Log transitions and heartbeat reconciliation | The open ensemble and first-ack marker survive successive renewals in the same lineage/generation. A complete Preserve shutdown seals that epoch, closes ingress and starts no later lease work. |
| Expired enrolled follower | A real TCP cut and forced SDK cancellation remove the only follower; its lease expires naturally. With both bundle and LTX payload paths withheld, a new signed write cannot finish. The same request finishes after publication resumes and its value is readable. |
| Completed local log version | Successive successful lease CAS operations need no safety-lane readback and produce no stale-guard conflict. |
| Unseen durable version | Losing the reply after an actual applied lease CAS triggers an authoritative safety-lane readback; later renewals and signed writes retain the folded log. |
| Slow CAS | Two successful 1.5-second CAS tails preserve authority without readback or conflicts. Cellar caps renewal attempts at 2.5 seconds, so the old publisher's 6.5-second successful-tail assumption does not apply. |
| Delayed authority observation | Delivering a captured older ordinary capacity response after a renewal cannot regress the lease writer's CAS guard or folded log. |
| Stalled log discovery | An ordinary record read remains blocked beyond that captured record's expiry while native heartbeats, signed writes and the lease safety lane continue. |
| Sustained renewal failure | The SDK reports `NodeLeaseExpired`, joins its domain, and closes both typed ingress and signed frontend access. |

Cellar retains historical ensemble membership after a follower expires; TTL
alone does not promise immediate epoch rotation. The port therefore checks
durability proof rather than reproducing the removed publisher's explicit
`rotation_required` API. The read-stall fixtures wait for the SDK's real
30-second maintenance cadence before asserting the fault is active. All
per-record state assertions and waits after fault entry remain bounded.
This lease/log fixture port did not change production source. The subsequent
idle-index worker fix and its qualification are described below.

### Native index contracts and idle range recovery

`tests/cellar_indexes.rs` ports six local/global-index behavior cases onto real
SDK ingress and shared bucket storage. The private account
fixture remains an explicit internal generation; the public table starts with
two ordered data cells and never uses the single-cell public creation path.

The LSI case preserves five indexes, numeric sort/tie ordering in both
directions, sparse entries, projected item-size/key validation, mutation
maintenance, mixed account/data transaction replay, prepared Query/Scan
barriers and read-help for both COMMIT and ABORT. A single PK collection has
no legal split; after adding another collection, an actual PK-boundary split
produces three ranges and preserves a previously returned logical cursor.
Signed AWS queries/scans with retries disabled recover after removal of the
old directory and natural SDK lease/sample expiry. Observed native factories
retain scopes at higher physical owner epochs.

The GSI journal case preserves a 350 KiB value, chunked change records larger
than 700 KiB, partial projection, lost acknowledgement replay, superseded
images, tombstones, pending-projection split refusal and binary prefix queries.
A fresh owner restores the same index scope, completes the retained journal
and serves the large numeric/binary index values through signed AWS requests.

The serving-worker case refuses one target before SDK ingress while the
second index progresses; the source journal remains until every destination
is durable. Removing the first owner's directory then exercises native owner
recovery and automatic journal replay. A second directory loss has no new
base mutation or outbox entry. Before any user index read, the fixture
requires actual SDK factories for every published range, including empty
ranges, and checks the unchanged scopes and increased native owner epochs.

The two typed backfill cases retain 24 historical items, configuration refusal
while a prepare owns reservations, immutable key-type validation, invalid
historical-key skipping, deferred ordinary journal work and the historical
delivery barrier. A partially scanned policy/cursor survives a fresh SDK owner
after removal of the old directory. Splitting before scanning copies the
unfinished obligation and mutable policy to both children; delivery covers
every original row, and removing the policy changes future write validation
without changing the immutable installation. These are internal policy/phase
contracts, not qualification of a public online-index update API.

The transfer case retains 70 entries with 1,800-character escaped keys,
normalized numeric ties, 350 KiB binary images and deletion tombstones. It
checks bounded export pages, replay-safe imports, version/fingerprint conflicts,
pre-publication traffic fences and retained directory plans. SDK replacement
after entry 13 preserves both partial fingerprints; another replacement after
publication preserves Activated children before opening. Both replacements
remove the former directories and observe native lease/sample expiry, stable
scopes and higher actual owner epochs. After resumption, stale images remain
superseded and tombstones stay absent. An equal GSI PK/SK set is stored in both
children through its complete ordered base-key suffix, and signed AWS queries
with retries disabled verify forward/reverse pagination and complete logical
continuation keys across that split.

This regression failed in both the combined suite and alone: the worker
listed index ranges but its idle-index branch did no SDK work. Journal replay
activated populated destinations, leaving other ranges undiscovered. Each
bounded index-page job now reads `ReadGlobalIndexState` through native
ingress; missing installation is deferred, and cursor advancement still lets
healthy ranges progress. Cellar continues to own placement and restoration.
`TaskGroup` is exported with its existing bounded shutdown so embedding
callers and fixtures can cancel and join maintenance before stopping the SDK.
The canonical ordered GSI-key helper is public for typed routing consumers.

## Current state

The new `crates/bdb-cellar-runtime` crate implements compiled command/query
registration, canonical logical targets, bounded codecs, typed parameterized
SQL, per-cell command receipts and a Cellar application/gateway. Its client
submits through `Node::start_external`'s `IngressHandle`, consumes the complete
gated response, and preserves ambiguous mutation outcomes rather than replaying
them under new identities. Application rejection rolls back application writes
while committing the rejection receipt.

The root manifest now depends on Cellar and the adapter, and both new crates
are members of the root workspace. The unified lockfile contains no Cellule
packages. Domain command/query/client imports use the adapter. The new embedded
host and provisioning layer compile, as does the replacement server binary.
Signed AWS SDK process/restart qualification now passes for Put/Get/Delete,
conditional Update, large escaped values, Query pagination and transaction-token
replay. Legacy runtime fixtures still need porting; this is not a completed
migration.

Base and GSI publication now enforce the cataloged initial range count. The
real-SDK publication regression verifies that a durably installed full-space
single data range cannot activate a table cataloged for four cells, and that
the rejection and unpublished catalog anchor survive node restart.

Nine adapter unit checks pass for logical identities and SQL access modes.
The embedded-node integration test exercises command replay, rejection/error
rollback, exact BLOB/integer values, tenant isolation and local state reopening.
That embedded test passed, including local node restart and receipt replay.
This local test does not
qualify object-store durability, hard-kill recovery or a fleet.

The serving source now uses ordered `(PK, SK)` ranges and requires multiple data
cells for every table. The executable design and file map are in
[ordered cell partitioning](docs/ordered-cell-partitioning.md).
`crates/bdb-keyspace` implements encoding, query spans, validated multi-cell
directories, split CAS and topology-independent resume. Its nine focused tests
and clippy check pass. The three-cell SDK fixture also passes, including ordered
reads, range/epoch rejection and restart. The root serving-router fixture also
passes: one PK has ten items in each of three cells; forward/reverse Query
crosses those cells, a real controller splits the middle cell at observed SK 15,
saved logical continuations keep their order and all items survive restart.
The fixture also splits the metadata directory, resumes Scan across the new
leaves, and checks numeric equality, comparisons and BETWEEN in both directions
with paginated Query results. The signed process fixture now starts three data
ranges and submits a transaction whose PKs fall in different ordered ranges.

Persisted bounds are bounded variable-length keys. Data and GSI item stores
carry the same ordered key used for ownership; GSI keys append the complete
base-key suffix. Directory traversal supports both directions and limits route
pages by serialized bytes. The configured initial count may be any value from
2 through 256, with four as the default. Bootstrap fences use a deterministic
schema-aware template; unknown workloads may still be skewed. Splits choose an
observed median key, and LSI base splits are restricted to PK collection fences.
Read-load weighting, every Query predicate/filter/byte-limit combination and
GSI/LSI fault qualification remain work to verify.

The real serving-router fixture also exercises ExtendDB's frontend Query
accumulator over thirty heavily escaped 70-KiB items across cells. In both
directions, it verifies the shared 1-MiB evaluation boundary (14/14/2 items),
filtered empty pages with advancing logical continuations, projection before
return but after evaluation accounting, and a shared Limit of twelve. The
900,000-byte internal transport chunk is separate from this frontend budget.

The fresh local engine path is now `data_dir/cellar-ordered-v1`. Tables and
directory installations persist encoding version 1; incompatible stored
contracts fail closed. Earlier branch HASH state in `cellar-v1` is not opened.
Fleet deployments also require a fresh object-store prefix for this layout.

The account-domain integration fixture is now ported to the embedded SDK. Its
replay, rollback, conditions, transactions, large transactional reads, metadata,
TTL and stream-retention assertions pass. It starts a second SDK node against
the same development object/CAS store, hands off the first node, deletes that
node's local directory, and verifies recovered state on the second node. This
is a real SDK handoff check; it does not replace S3/process-loss qualification.
Public CreateTable still requires a multi-cell provisioner; the fixture's
explicit account placement is confined to private domain-command coverage.

The independent large-read, metadata-cache and coordinator-history process
fixtures now run in `tests/cellar_process.rs`, preserving their original SDK
assertions. LSI segmentation, GSI process-loss and signed Streams/TTL cases also
use that target. The Streams fixture retains checks for hard restart, TTL
service identity and old generations after table deletion and recreation.
They require Docker, AWS CLI and the pinned RustFS image and must be explicitly
run with `--ignored`. Their default ignored status is not a passing fault test.
Remaining legacy runtime fixtures have not been removed or disabled.

These six cases passed against pinned RustFS on 2026-10-17. The large-read case
also hard-kills the owner, removes its local directory and recovers ten 380-KiB
transactional read images on a different logical node. The coordinator-history
case commits across 70 coordinator shards, exceeding the fixture's 64 resident
cells, before process replacement and replay. These checks establish the tested
bucket-backed recovery paths, not fleet-scale capacity or arbitrary fault safety.

The native capacity process case also passes against the pinned RustFS image.
It observes current Cellar leases with measured RSS, descriptor limits, sample
age, ownership and bounded residency, then uses `FleetClient` to verify the
current generation's signed challenge and authenticated management state.
A stale generation and a wrong operator key are rejected; restoring the correct
key permits another authenticated observation. The fixture provisions the
configured test key for the SDK's bucket-backed operator client; it does not
mutate ownership records.
Cellule's signed memory/disk/job advertisement is not Cellar's lease format.
This case does not qualify the old disk-budget or reserved-job admission gates;
those require an explicit native resource policy and remain migration work.

The broad signed SDK process scenario now also passes on Cellar (170 seconds).
It preserves CRUD return-value/capacity checks, batches across three ordered
ranges, segmented scans, background statistics, TTL configuration, tags,
conditional writes, transaction cancellation and token replay. Large string,
binary and escaped values retain their transaction/Query/Scan checks. It churns
70 coordinator shards beyond the 64-cell resident limit, hard-kills the process,
replays on a new private address, and checks another Preserve-mode restart plus
table deletion/recreation. Test keys cross typed ordered bootstrap fences;
escaped variants append a suffix so their selected intervals remain unchanged.
Together with native capacity, eight process cases have passing RustFS runs.
The default ignored status still requires explicit execution for qualification.

Native follower recovery is also ported. `fleet.follower_durability` defaults
to false; enabling it installs the SDK's coupled log shipper and bundle sink,
letting follower fsyncs race bucket publication. The three-process fixture
starts from a bucket-published warmup in a public two-cell table, observes the
actual owner and enrolled peers, and pauses balancing through native management.
Its S3 fault rejects both bundle and per-cell LTX uploads. Admission closes
atomically and joins already-forwarded payloads before recording a bucket cut;
rejected bodies cannot publish after the crash. Signed CRUD, batches and
transaction writes acknowledge while payload keys, ETags and sizes stay fixed.
The actual owner is killed and its local directory deleted. After native lease
expiry, a new logical node with an empty directory recovers the items, deletion,
conditional token replay and exactly-once stream events; ownership advances and
the failed log is sealed. The SDK owns the proof policy and recovery, including
native node bundles; a per-cell LTX-only observer is not a complete fleet-log
durability test. See [the current follower guide](docs/follower-durability.md).
The full nine-case RustFS run passes on 2026-10-17 in 344 seconds, with zero
ignored tests, including a two-member enrolled ensemble before the follower
fault. The same build passes all 60 focused tests and selected strict Clippy.

The `server_binary` root now uses the same native process fixture and compiles
under strict Clippy. CI explicitly runs `cellar_process` and the focused
tests; legacy all-target and qualification commands remain enabled.

`tests/cellar_transactions.rs` now runs twenty-two cases over real SDK ingress:

| Ported behavior | Evidence |
| --- | --- |
| Coordinator token retention | BEGIN and unresolved COMMIT pin their tokens beyond ten minutes; changed fingerprints fail; unfinished work keeps its original routes; completed tokens become reusable. A controlled application clock advances time without editing live SQLite state. |
| Condition-check write skew | Two ordered data cells hold independent intents; a conflicting transaction aborts, and a later prepare evaluates the newly committed value even when its BEGIN is older. Both terminal paths release their locks. |
| Small coordinator resume | One status/payload snapshot preserves the original participant targets and operation indexes; the full resume retains the original four-query assertion, and terminal snapshots contain no executable payloads. |
| Terminal resolution faults | Twelve COMMIT/ABORT cut scenarios cover unavailable reads, blocked applies and missing coordinator receipts. Healthy participants finish while the first RPC is held; cancellation and retry preserve later writes and complete the original decision. |
| Lost mutation reply | A real SDK increment commits and passes its durability gate before its reply is discarded. The caller receives the original ambiguous identity; replay before and after node restart returns the same receipt and increments once. |
| Lost transaction phases and cancellation | The storage-contract fixture loses completed prepare, BEGIN, decision, resolution and multipart-upload replies. It preserves token replay/mismatch, large escaped payloads, conditional rollback, cancellation token reuse and response ordering with absent/repeated read keys. |
| Shared snapshots and read help | The lost-phase fixture retains its shared-reader checks for account and routed participants. Point reads and scans finish COMMIT/ABORT decisions using the original coordinator routing key; Query resolves a committed create absent from live rows. |
| Atomic prepared commit | Wrong participant/position/sequence and incomplete prepare evidence fail. The remaining prepare receipt and COMMIT publish in one coordinator sequence. A fresh SDK owner recovers the same receipt, both prepared participants and the decision before any resolution, then completes and collects write payloads. |
| Mixed participant recovery | Private account-table intents and a two-range data table retain their locks across SDK handoff to a fresh owner. Concurrent recovery commits once; delayed prepare/resolve, conditional failure, rollback and large lost-reply checks remain intact. |
| Caller admission refusal | A local policy refuses before SDK ingress: the transport sees zero calls, the item and sequence remain unchanged, and retry with the original identity commits exactly once. Native HTTP failures retain their ambiguous classification. |
| Driver races and background recovery | Nine driver scenarios preserve condition/conflict indexes, partial prepare, competing COMMIT versus capacity refusal, lost replies and multipart payload compaction. The production recovery cursor finishes a healthy BEGIN past an unavailable participant, revisits skipped work, and discovers a later shard's ABORT. A read-only audit after SDK shutdown verifies payload collection for every completed driver transaction. |
| Acknowledged read cleanup | Cleanup intent is discovered after a settled pass and recovered on a fresh SDK owner, both before participant deletion and after deletion without its coordinator acknowledgement. Saved images survive before acknowledgement; cleanup collects images and operation payloads; delayed prepare cannot resurrect them. Previous owners' local directories are removed. SDK non-owning replica snapshots preserve raw SQL cleanup audits despite handoff removing local database files. |
| TTL and transaction locks | Two public multi-cell tables retain TTL progress past shared and exclusive locks. A real candidate-page reply is held while a read lock is acquired; the worker rechecks deletion, reaches healthy siblings and the next table, then revisits all deferred items after ABORT. |
| Range read visibility and signed SDK cancellation | Prepared creates and deletes fence point, Scan, Query and transactional reads; disjoint ranges and exclusive continuations remain readable. The Scan regression exposed and fixed a whole-cell intent probe that ignored the ordered continuation. Source sealing refuses outstanding intents. After ABORT, an actual observed-key split divides one PK collection; signed SDK read/write transactions preserve cancellation-reason ordering across its cells. Scan/Query with an `8.0` boundary exclude the lock at SK 8. Restart retains the original rows, encrypted credential and IAM policy without bootstrap. |

`tests/cellar_domain.rs` ports the backend metadata-cache and usage-counter
invariants. Account query counters verify that a warm key-info cache executes
no query, while each uncached routed lookup reads both catalog and activation
state. Local invalidation and remote recreation preserve generation fencing.
Deletion completes through the real multi-cell retirement path before name
reuse. The usage check loads the compiled data module's exact schema and covers
128 inserts, ordered-index-only changes, incremental BLOB writes, rollback, TTL
metadata and repeated deletes. It reproduced a 28-byte undercount before the
usage trigger was fixed to watch `ordered_key` updates.

`tests/cellar_directories.rs` qualifies directory contracts through native SDK
ingress and fresh-owner recovery after deleting the previous local directory.
Transfer recovery preserves the full immutable table contract before and after
publication. Activation accepts the recorded 256-range base/GSI plan and rejects
a 1,024-range initial publication; the catalog's initial-count limit is not
relaxed. The separate metadata-tree fixture preserves a 1,024-entry fingerprint
through copy, cutover, stale writers and interrupted descendant retirement.
Its unavailable-child injection wraps actual ingress with an ambiguous transport
failure; it is not a physical network-partition or ownership-fencing test.

A residency case creates 48 metadata roots under a 16-cell resident cap, keeps
a live root readable, preserves retired-root installer fences and serves actual
two-cell table data after handoff. Native SDK sampling confirms ownership and
residency separately. Cellar owns eviction policy; the fixture does not recreate
Cellule's terminal-directory-only reclamation API. These development-bucket
checks qualify metadata recovery, not 1,024 data cells or fleet capacity.

The creation case interrupts after installing both initial data cells but before
directory publication. A fresh owner recovers the CREATING generation and its
exact two-cell plan despite a changed server default, then publishes ACTIVE.
It writes more than 64 MiB across both ordered ranges and checks exact usage and
items on another fresh owner after deleting the old local directory.

The paged-routing port creates a public table with a recorded count of 65 and
checks 65 distinct installed data cells through native SDK ingress. Its route
pages contain 64 entries and one final entry; point starts, exclusive cursors
and stale versions retain their checks. Scan resumes across empty cells. TTL
configuration and expiry continue across that page boundary, preserve service
identity in the removal event, and stop after sixteen tables per schedule pass.
The seventeen auxiliary TTL tables use public two-cell creation.

The large-directory half is a separate metadata fixture. It publishes a valid
256-entry initial plan, grows it through 768 recorded range replacements to
1,024 entries, splits the directory, then publishes another leaf transfer.
All 1,025 resulting entries remain pageable on a fresh SDK owner after deletion
of the previous local directory. The full pending transfer is recovered through
its child and completed. This qualifies metadata membership and serving
discovery; it does not represent 1,025 installed data cells or fleet capacity.

Two table-generation cases now use public two-cell base tables and two-cell
GSIs under a native 16-cell resident limit. Four delete/recreate cycles retain
both retired directory fences and the exact base items from every generation;
more than thirty owned cells are observed before checking residency. Live table
items and deleted-generation history survive handoff after deleting the old
local directory. The separate interrupted-retirement case loses a native durable
RetireDirectory reply before catalog acknowledgement, recovers the unchanged
receipt on a fresh owner, advances to the independent GSI root and completes
deletion before reusing the name. This ports application invariants; it does not
recreate Cellule's explicit owner-release policy or qualify GSI projection data.

The recreation case initially reproduced HTTP 500 when a new inventory shard
was placed on the departed node during its still-valid lease. Diagnostic output
identified the stopped peer endpoint. Waiting only for lease expiry passed in
isolation but failed in the combined suite. The fixture now waits for both the
native lease to expire (or disappear) and the SDK shared placement sample to
observe departure through a non-owning execution domain before creating new cells. It does not edit lease state, treat peer refusal as non-execution or
claim immediate new-cell placement after handoff. Temporary tracing was removed.

The coordinator residency/checkpoint fixtures now use native SDK ingress and
public ordered tables. Their new cases cover 24 distinct coordinator shards
under a 16-cell resident limit, concurrent replay of an old token after later
writes, and a partially prepared read across two actual data cells recovered
on a fresh owner after deletion of the old local directory. Another case admits
four independently installed data ranges after 32 coordinator shards have
exceeded residency. SDK load sampling backs the ownership/residency assertions.

Settled discovery does not publish an account checkpoint on each pass. A later
BEGIN in an already visited shard remains discoverable across handoff and resumes
from its original payload. Fresh BEGINs commit through normal recovery; startup
alone is not permission to abort another live coordinator. These checks replace
the old app-driven owner-release and opaque settled-root optimization assertions;
they do not prove avoidance of all SDK activation or bucket reads.

The capacity-loop port injects a caller-side child-install budget refusal before
SDK submission, retains the exact pending split while other sweeps continue,
then admits the same recorded children when the budget returns. Its first run
reproduced a classification regression: `cell_error` mapped a proven
`NotStarted(Capacity)` to Transient. That explicit variant now maps to
LimitExceeded; native HTTP errors remain ambiguous. The original selected run
had five passes and reproduced that one classification failure. After the fix,
all nineteen transaction cases passed together in the rebuilt 68-case focused
runtime run.

Startup now visits registered coordinator shards and their observed pending
boundaries before starting maintenance or public HTTP. It retains a failure
while continuing healthy records, later shards and later configured accounts.
Fresh BEGINs use live recovery; only the atomic expiry command may abort them
because of their age. Before acquiring more locks, recovery observes the original
participant targets through SDK queries. Terminal decisions still resolve
healthy participants independently of a failed peer.

The native admission port covers BEGIN, COMMIT and ABORT both after local SDK
restart and during serving recovery. A caller policy refuses the first
participant before submission; counters prove zero transport calls for that
target. Later records and shards settle, a partial BEGIN gains no extra locks,
and a successful retry preserves newer healthy writes. Another case admits a
record after the observed boundary, verifies that the initial pass leaves it
untouched, then discovers and commits it through the serving cursor. The server
initialization regression verifies that a failed pass starts no maintenance,
still inspects a later account, and can be retried successfully. These four new
checks pass. They exercise caller admission and real SDK state, not a physical
network partition or a native resource-pressure policy.

After this startup change, all 17 root-library tests and all 22 transaction
tests passed with zero ignored cases on the Git dependency. Strict Clippy passed for workspace libraries,
binaries and the transaction target. The rebuilt test executables were copied
to the internal SSD, checked for SHA-256 equality, then run with their normal
parallelism and unchanged assertions/deadlines. The nine RustFS process cases
also passed after the startup change in 483 seconds, including the 70-shard
replacement and native follower fault. Registry-release results are recorded
separately from these Git-build results.

The token fixture now builds its states with actual coordinator commands. Those
commands collect resolved write payloads, which the old raw SQL seed left
behind. The port checks exact payloads before resolution and for every
unfinished participant after a rerouted retry, then checks payload collection
and retained decision/participant identity through delayed phase replies.
The mixed account/data resolution fixture creates private account generations
explicitly; public table creation still requires multiple ordered data cells.

The adapter's `InvocationTransport` defaults to the native SDK `IngressHandle`.
Trusted in-process decorators expose bounded operation metadata for counters
and transport fault fixtures. The client retains its registry, codec limits,
receipt checks and ambiguous-outcome handling. This is not a public command
gateway or a replacement execution runtime. These domain transport tests do not
qualify network partitions, owner fencing or follower-log durability.
The new shared-read helpers select participants by the complete ordered key;
they no longer assume hash buckets or a one-range routed table. The old
Cellule-only restored SQLite path/count inspection is replaced in the atomic
commit case by a fresh SDK node, deletion of the first owner's local directory,
the unchanged coordinator receipt and authoritative participant-state queries.
These two-node checks use the SDK development object/CAS store, not S3.

The driver's known-before-submission capacity refusals use the adapter's
`InvocationAdmission` policy, evaluated before SDK ingress or transport. The
policy proves only that this attempt was not submitted; the coordinator CAS
still decides races with other drivers. A generic native HTTP 503 does not
prove non-execution and must not authorize ABORT or a new mutation identity.
The recovery cursor resumes fresh admitted BEGINs using their durable payloads;
an atomic age check can instead expire them after ten minutes. Cursor progress
is retained through participant errors so unrelated work can complete.
The replacement adapter exposes authoritative SDK ingress and has no legacy
replica-read-policy override.

## Remaining implementation

1. The adapter runs the root domain suite and signed SDK fixture. Reservations
   use bounded physical BLOB chunks; large-image writes verify RETURNING rowid,
   and rows_affected uses SQLite changes() rather than trigger-inclusive billing.
   The host installs bounded SQL limits for legal escaped DynamoDB values.
   Broader capacity/fault qualification remains.
2. Command/client imports and compiled application registration now use the
   adapter. Keep domain identities and key semantics while qualifying behavior.
   Ordered routing/storage bounds have been changed together. Qualify the full
   behavior matrix; do not mix encoding-v1 ranges with old HASH directories.
3. The new host/provisioner use Cellar's SDK node, namespaces, ownership and
   replication. Qualify lifecycle, replace coordinator recovery, and remove
   obsolete source after equivalent coverage is ported. A bounded coordinator
   discovery/expiry worker and stream-retention fixture pass real SDK/restart
   unit checks. A fixed-shard inventory persists cell discovery before
   installation, including retired sources.
4. Done: removed Cellule dependencies from the production manifest, joined the
   adapter/keyspace crates to the workspace and regenerated the root lockfile.
   Obsolete nested lockfiles have been removed. Cellar is pinned once in
   workspace dependencies as the exact crates.io release described above.
5. Port the existing behavior fixtures to the new host. Verify CRUD, queries,
   scans, batches, transactions, indexes, streams and TTL through ExtendDB and
   signed AWS SDK requests. Runtime-specific legacy fixtures require a Cellar
   equivalent rather than a mechanical dependency rename.
6. Run process replacement, node-fencing, split, transaction-decision and index
   recovery cases against bucket-backed Cellar before declaring completion.
7. Update contributor guidance, startup configuration, CI and operational docs
   to describe the actual Cellar server and its qualified capability boundary.

The remaining `elastic_cells` setup still covers SQLite-full and reservation
behavior, multipart upload replacement, native session shutdown, and its large
root fixture. GSI backfill/transfer now run in the selected native index target. Its previously ported modules
also remain included there until that root is migrated. `peer_network` retains
its separate physical and residency fixtures. Passing the selected native
targets does not complete either aggregate suite.

The new executable configuration and lifecycle are described in
[the Cellar server guide](docs/cellar-server.md). Legacy all-target fixtures are
not disabled; their remaining Cellule setup must be replaced with SDK fixtures.

## Checks

Use the checkout-specific target directory on the mounted workspace.
`RUSTC_WRAPPER=` bypasses an inherited compiler-cache configuration without
changing shared settings. The task's optional read cache lives outside the
repository and is not required to build or test.

```sh
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/bdb-cellar-adapter"
export RUSTC_WRAPPER=
export CARGO_BUILD_JOBS=4
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_PROFILE_TEST_DEBUG=0
cargo fmt --manifest-path crates/bdb-keyspace/Cargo.toml -- --check
cargo test --manifest-path crates/bdb-keyspace/Cargo.toml --locked
cargo clippy --manifest-path crates/bdb-keyspace/Cargo.toml --all-targets --locked -- -D warnings
cargo fmt --manifest-path crates/bdb-cellar-runtime/Cargo.toml -- --check
cargo test --manifest-path crates/bdb-cellar-runtime/Cargo.toml --locked
cargo clippy --manifest-path crates/bdb-cellar-runtime/Cargo.toml --all-targets --locked -- -D warnings
```

The preceding complete focused run passed 68 tests: nine adapter units, two embedded
fixtures, nine keyspace cases, sixteen root units, the signed local process
test, the ported account SDK handoff fixture, nineteen transaction/transport
fixtures, two domain schema/cache cases and nine directory/creation/lifecycle cases. The
ordered-router unit now includes the frontend Query budget assertions described
above. Run them with:

```sh
cargo test --workspace --lib --test account_cell --test cellar_sdk --test cellar_transactions --test cellar_domain --test cellar_directories --test cellar_peer --test node_log_authority --test cellar_indexes --test embedded --test ranges --locked
```

That 68-case runtime verification executed all ten built test programs
from the internal SSD, after checking every copy's SHA-256 against its mounted
build artifact. Test parallelism, assertions and deadlines were unchanged; the
result was 68 passed, zero failed and zero ignored; all nine directory cases
passed together. Earlier seven-case directory runs also passed from both
locations, including with output capture enabled.
Earlier combined runs hit embedded or directory startup deadlines, and Cargo
was separately observed waiting in uninterruptible I/O on the mounted build
volume. Their timing cause is not established; these reruns do not claim a
startup fix. The embedded fixture now labels startup/shutdown deadline errors.

The selected strict-Clippy rerun passed after the coordinator fixture ports
and pre-submission capacity-classification fix. The startup recovery change
subsequently passed all 17 root units and 22 transaction fixtures, plus strict
Clippy for workspace libraries/binaries and the transaction target. Those are
the affected targets rerun for this change. Run the selected library, binary and integration checks:

```sh
cargo clippy --workspace --lib --bins --test account_cell --test cellar_sdk --test cellar_transactions --test cellar_domain --test cellar_directories --test cellar_peer --test node_log_authority --test cellar_indexes --test embedded --test ranges --test cellar_process --test server_binary --locked -- -D warnings
```

Run the separate bucket-backed process target explicitly:

```sh
cargo test --test cellar_process --locked -- --ignored --nocapture --test-threads=1
```

The full legacy all-target suite still needs its Cellule host fixtures ported.
These results do not establish fleet capacity or complete DynamoDB compatibility.
The 2026-10-17 `cargo clippy --workspace --all-targets --locked -- -D warnings`
run exits 101 in legacy Cellule fixtures. Unported fixture roots are
`elastic_cells` and `peer_network`;
they still import Cellule host/lease APIs and older object-store interfaces.
The latest registry-release rerun reported 61 errors in `elastic_cells` and
60 in `peer_network`; the ported `node_log_authority` target is clean.
The selected Cellar targets above pass. No CI checks or legacy fixtures have
been disabled to hide this failure.
