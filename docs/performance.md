# Measure BeyondDB performance

BeyondDB does not yet have a qualified production throughput or latency target. Earlier September 2026 single-node samples suggested that warm point reads could exceed the file-backed SQLite fixture, while durable writes and transactions remained slower. The refresh below did not reproduce those high read rates. Do not use these numbers to plan a fleet. BeyondDB's request path and durability contract differ: by default an item write waits for Cellule to publish committed state to object storage. Experimental follower durability can acknowledge a durable follower receipt while object tiering continues.

Benchmark reports, raw logs and fixture snapshots are kept locally and excluded from Git. The summaries below retain measured revisions and limitations; durable behavior is checked by the committed integration tests.

## Active owner lookup during restore

The local handle cache validates the exact owner fence, code and schema against Cellule's active-owner capability. Sparse and hydrating owners can serve foreground work while background hydration continues. An expired cache entry can be rebuilt from that capability without an authority read. A miss follows the normal catalog and fresh-authority path; it does not authorize acquisition. Peer enrollment and request authorization are still checked for every invocation.

```text
Signed peer request
        │
        ▼
Enrollment + authorization
        │
        ▼
Active owner lookup ── found ──► Check fence / code / schema ──► Dispatch
        │                                                        │
      absent                                           Actor admission rechecks
        │                                              lease, drain and ownership
        ▼
Catalog + fresh authority
        │
        ▼
Normal resolution / eligible recovery
```

A controlled restored-owner regression holds background hydration open while five signed foreground reads run, including one after cache expiry. The earlier resolver performs five receiver authority reads; the active-owner path performs zero. A separate owner-reacquisition regression rejects reuse of a cached handle from an earlier epoch of the same incarnation. These checks prove reduced metadata work and correct cache invalidation. They do not establish an end-to-end throughput improvement or resolve the remaining write/transaction durability costs.

## Recovery verification and owner placement

A range can release residency and publish an Idle root while its former node is still alive. Later recovery may acquire that root on another node. Recovery checks therefore compare the retained Cell ID, incarnation, code, schema, owner epoch and published commit sequence, followed by signed SDK item reads. A fixed owner name alone does not establish durable recovery.

A focused transaction regression installs discovery before coordinator registration, leaves one participant completion receipt unrecorded, and verifies that recovery preserves the exact live-owner fence. After the owner's lease expires, the serving worker must record both participant resolutions before signed SDK reads confirm the recovered values.

Authentication also restores a cataloged credential shard after its owner expires. This uses the existing fenced takeover path and requires a published root and fresh lease evidence. The regression covers enabled and disabled peer caches, refusal to replace a live owner, and an unknown access key that creates neither a catalog entry nor authority.

The reviewed Cellule EOF fix keeps observed object streams finished after completion. Both new regressions reproduce the original panic before the fix. With the updated pin, the original follower durability process test passes, including withheld object publication, owner process kill, recovery, and graceful shutdown. This closes that local failure; full fleet recovery and sustained performance still require qualification.

## Why writes and transactions cost more than SQLite

The pinned ExtendDB SQLite backend uses WAL with `synchronous=NORMAL`. SQLite documents that this mode does not synchronize the WAL after every commit; `FULL` adds a synchronization for each commit. BeyondDB waits for object-store publication or a durable follower receipt before acknowledging a mutation. The benchmark therefore compares different durability paths. [SQLite durability reference](https://www.sqlite.org/pragma.html#pragma_synchronous)

```text
ExtendDB SQLite                  BeyondDB follower mode
request                          request
   |                                |
SQLite transaction               owning Cell command
   |                                |
WAL commit                       follower append + fsync
   |                                |
response                         durable receipt -> response
                                    |
                                 async object tiering
```

The latest completed release comparison measured SQL commands at 0.800 ms on average in four-Cell mode and 1.059 ms in single-Cell mode. Command responses with follower durability averaged 101.332 and 85.691 ms respectively. These populations include background work and overlap other scopes; they are evidence of costs around SQL, not an additive explanation of request latency. The host was heavily loaded and using about 36–37 GiB of swap, which limits comparison with earlier samples.

Cross-Cell transactions add a durable coordinator admission, preparation, decision, participant resolution, and cleanup. A fresh coordinator shard can also require authority and catalog I/O. Boto3 automatically supplies an idempotency token for `TransactWriteItems`; BeyondDB preserves account-scoped replay and mismatch checks through the coordinator even when all items occupy one Cell.

[Single-Cell placement](scaling.md#choose-a-tables-cell-model) removes cross-Cell work from eligible transaction reads and reduces the number of write participants. It retains follower durability and tokenized write coordination. Reaching SQLite write latency requires further work on durable I/O, batching, and coordinator admission, plus measurements with declared durability settings. It is not an established property of the new placement option.

### Where optimization can help

| Cost | What can reduce it | What must remain correct |
| --- | --- | --- |
| One durable publication per independent mutation | Coalesce mutations targeting the same Cell. | Individual conditions, results, stream records and durable acknowledgements. |
| Transaction prepare publications | Coalesce independent participant prepares targeting the same Cell. | Separate transaction identities, locks and coordinator decisions; atomic rollback of a rejected batch. |
| Fresh coordinator admission | Reuse admitted owners and coalesce discovery/catalog work. | Account-scoped token replay and mismatch checks, owner fencing and recovery discovery. |
| Several participants for a small table | Start with one data Cell using `single` or `auto`. | Disk and write budgets, index placement and recovery time. |
| Follower/object I/O | Measure and reduce transport, authority lookup and storage latency. | The chosen durability contract and restart survival. |

These changes address different parts of the request. A single data Cell can remove participant fan-out, but tokenized transaction writes still have several serial durability barriers. Lower SQL execution time alone cannot remove those barriers.

### Avoid stale heartbeat writes after local log updates

A completed node-log update changes the node advertisement's ETag. Previously, the heartbeat retained its older version, so its next renewal first attempted a stale conditional write, then read the current record and retried. The publisher and its log adapter now share their newest completed canonical observation. Late replies cannot replace a higher generation.

```text
Local log update -> completed record + ETag -> shared version hint
                                                     |
Heartbeat -------------------------------------------+
                                                     |
                                              conditional write
                                                     |
                              conflict -> canonical read -> retry
```

The hint reduces the measured local-update renewal path from **two PUTs and one GET to one PUT**. It grants no authority: the directory still checks the signed identity, transition and exact ETag. Log operations continue to read canonical state; unseen updates still require a conflict and authoritative rebase. No provider I/O holds the shared observation lock, and the existing 15-second lease and fencing checks remain.

The [heartbeat regression](../tests/node_log_authority/heartbeat_versions.rs) injects 6.5-second conditional-write latency after a completed log coverage update. The old implementation fences before its first renewal; the updated implementation completes two successive renewals and remains live. Delayed read replies, unseen changes and stalled log I/O are also checked. This isolates an avoidable renewal cost; it does not establish that every previous benchmark fence has this cause or prove SQLite throughput parity.

### Share transaction prepare publications

Independent small transactions targeting the same data Cell can share a prepare publication. Each transaction retains its own intent, locks and coordinator decision. The queue is bounded to 64 pending prepares per Cell; each batch contains at most 16 prepares and 32 KiB of input, with a 2 ms collection window. A single selected prepare uses the existing command. Legacy account participants and large payloads retain their existing paths.

```text
Transaction A: durable BEGIN -> prepare A --+
                                           |  one data Cell publication
Transaction B: durable BEGIN -> prepare B --+  stores separate intents and locks
                                           |
                                 durable receipt for each coordinator
                                      /                \
                              A: COMMIT/ABORT      B: COMMIT/ABORT
                                      |                |
                              resolve A            resolve B
```

A rejected prepare rolls back the whole batch. Individual commands follow only a durable rejected receipt or a proven refusal before the batch starts. Uncertain replies trigger durable participant-state reads. Coordinator decisions, participant resolutions and stream records retain their existing semantics.

The signed SDK verification reduces **64 durable data commands to 38–40** for 32 independent two-item transaction writes. It checks complete results after owner restoration and recovery after losing a batch reply following durable publication. **43 distinct tests, formatting and strict Clippy pass.** Batch grouping varies with scheduling. This proves reduced durability work; release throughput and SQLite parity require separate measurement.

### Avoid repeating acknowledged transaction work

For a fresh read transaction involving only routed data Cells whose BEGIN payload fits 32 KiB, an acknowledged BEGIN fixes the participant list and its ordering. BeyondDB retains that list while the existing prepare, COMMIT, and resolution path completes. It then uses the retained list to assemble responses from saved participant images, avoiding one coordinator payload query per participant. Canonical completion status and unresolved-participant checks remain.

```text
Durable BEGIN -> prepare participants -> record receipts + COMMIT
                                               |
                                  resolve participants + record receipts
                                               |
                                  fetch saved participant images
                                               |
                                  durably acknowledge assembled images
                                               |
                                  release images + record cleanup receipts
                                               |
                                           SDK response
```

The signed two-Cell regression measures **five coordinator queries before this change and three afterward**. It checks response order, absent items, owner restoration, and subsequent writes. Oversized BEGIN payloads and transactions involving the legacy account Cell still use durable participant discovery. Token replay, uncertain replies, and recovery retain authoritative coordinator reads. Participant commands, durable decisions, saved-image reads, and cleanup receipts remain required. An earlier broader shortcut failed mixed-participant recovery verification and remains excluded; its investigation is retained. Removing queries is a measured reduction in work; it does not establish an end-to-end throughput gain or SQLite parity.

### Reserve small replies for saved transaction images

Each saved-image query first uses a 64 KiB compact reply envelope. It reads the immutable image captured by the participant, using the original coordinator identity and item position. A missing live item is a valid absent image; an unavailable saved image is an error for a committed response.

```text
committed saved image -> compact query
                             |
                  +----------+-----------+
                  |                      |
              complete item         WideRequired
                  |                      |
                  |               existing wide query
                  |               same saved identity
                  +----------+-----------+
                             |
                      assemble response
                             |
                 durable acknowledgement + cleanup
```

A large image returns an explicit `WideRequired` marker, then uses the existing wide query. Errors do not trigger fallback. Items are never truncated, and the read does not substitute current live data. Wide fallback adds a query round trip. Reads remain serial within each participant so multiple large images do not reserve several wide replies together.

This reduces reserved reply memory for small items. The HTTP peer receiver also retains its own request buffer, so total in-flight memory exceeds the 64 KiB reply alone. End-to-end throughput, large-item latency and SQLite parity require separate release measurements. Existing query opcodes and persisted transaction/image formats remain available; upgrades of roots created by older application releases still need separate qualification.

## Latest release verification

### Coalesced prepares: release qualification fails

The prepare-batching release attempt measures source `2ebb948` with unchanged dependency pins and durability settings. **All 72 unique cases run**, including one declared unchanged single-Cell retry after the original trial fences during item seeding. The failed original trial remains in the report. The retry completes 6,117 requests with zero SDK errors; four Cells complete 170 requests with **77 errors across 17 cases**; SQLite completes 25,340 requests with zero errors. **SQLite parity and runtime qualification remain unmet.**

| API, eight clients | Single req/s | Four Cells req/s | SQLite req/s |
| --- | ---: | ---: | ---: |
| GetItem | 36.70 | 14.77 | 228.90 |
| PutItem | 11.14 | 0.00* | 261.38 |
| UpdateItem | 3.62 | 0.00* | 320.22 |
| TransactGetItems | 30.68 | 0.07* | 648.64 |
| TransactWriteItems | 0.84 | 0.00* | 203.80 |

\* Cases contain errors; zero means no successful sample after owner/authorization failures, not healthy capacity. Single-Cell transaction writes complete eight calls in 9.49 seconds, p95 **8,378.14 ms**, versus SQLite's 1,022 calls and **118.60 ms**. Calls contain two items. Eight completions do not establish a tail distribution. Full tables retain all APIs, errors, counts and elapsed times.

The 43-test verification proves 32 independent signed SDK transaction writes use **38–40 data commands instead of 64**, including recovery from a batch reply lost after publication. Results survive owner restoration. Formatting and strict Clippy pass. This is reduced publication work; the release attempt does not establish a throughput gain.

Single-Cell SQL commands average **1.857 ms**, while follower durability responses average **356.278 ms**, peer lookup **193.687 ms** and enrollment **61.573 ms**. These scopes overlap and cannot be added. The original single trial and four-Cell run fence after directory-refresh waits of about **12.0/12.3 seconds**; the cause inside that phase remains unisolated. Reliable renewal and coordinator admission remain priorities.

On 12 CPUs, SDK-window load is single retry **94.87→34.83**, four Cells **40.72→59.68**, SQLite **57.61→47.50**, with about **39.6–43.5 GiB of swap**. No task-local builds, tests or provider probes overlap measurement. Varying contention and ordering prevent causal attribution. Ten owned processes and three containers stop without forced BeyondDB cleanup; volumes remain retained. Full recovery, larger single-Cell budgets, live conversion and fleet capacity remain unqualified.

### Scoped transaction-read metadata release

The scoped-read release comparison measures committed source `eda759e` with unchanged reviewed dependencies. All **72 cases complete with zero SDK errors**: single Cell 12,116 completions, four Cells 18,859, SQLite 52,086. **SQLite parity remains unmet.** No API exceeds SQLite's eight-client rate in both BeyondDB modes.

| API, eight clients | Single req/s | Four Cells req/s | SQLite req/s |
| --- | ---: | ---: | ---: |
| GetItem | 83.75 | 308.51 | 785.56 |
| PutItem | 45.46 | 54.13 | 506.17 |
| UpdateItem | 62.02 | 41.04 | 495.30 |
| TransactGetItems | 105.41 | 0.98 | 598.33 |
| TransactWriteItems | 3.78 | 4.58 | 369.00 |

Eight-client transaction writes complete 27/28/1,846 requests (single/four/SQLite), with successful p95 **2,338.10/2,472.83/81.39 ms**. Each call contains two items. The four-Cell read case completes only nine transactions in 9.14 seconds, with successful p95 **8,876.13 ms**. Low counts and short targets do not qualify sustained throughput or a tail distribution. Complete tables retain all APIs, errors, completions and elapsed times.

The scoped optimization reduces fresh two-Cell read coordinator queries from five to three and passes 35 focused/library tests, formatting and strict Clippy. An earlier broader shortcut fails mixed account/data recovery and is excluded; its failures and baseline comparison remain in the verification record. This is a reduction in coordinator work, not an isolated end-to-end speedup.

Four-Cell SQL command primitives average 0.800 ms, while follower durability command responses average 101.332 ms and durable follower append 24.537 ms. These scopes overlap, include different populations, and cannot be added. During concurrent transaction reads, peer lookup and enrollment average 87.868 and 49.800 ms. Discovery, durability and batching need further work. Runtime warnings record two short directory-refresh session-change failures; no terminal fencing occurs, and the earlier endpoint loss remains unresolved.

On 12 CPUs, SDK-window host load is single **25.74→31.39**, four Cells **27.65→22.49**, SQLite **22.29→21.43**, with about 36–37 GiB of swap. No task-local build, tests or provider probes overlap measurement. Changing contention prevents causal attribution. All seven owned PIDs and two containers are absent; no BeyondDB process requires forced cleanup and volumes remain retained. The preceding head's [full recovery CI](https://github.com/crabbuild/beyonddb/actions/runs/37050870335) still fails missing GSI ownership and observed-stream EOF shutdown. Zero errors in this fixture do not close full recovery, old-root upgrades, larger single-Cell budgets, live conversion or SQLite parity.

### Previous independent heartbeat release

The independent heartbeat release attempt measures committed source `e1f7664` with unchanged reviewed dependencies. All **72 cases run**. Single-Cell BeyondDB and SQLite complete their 24 cases with zero SDK errors; four-Cell BeyondDB records **72 errors across 16 cases**, loses its endpoint and exits fenced. **SQLite parity and runtime qualification remain unmet.**

| API, eight clients | Single req/s | Four Cells req/s | SQLite req/s |
| --- | ---: | ---: | ---: |
| GetItem | 234.27 | 122.33 | 727.21 |
| PutItem | 31.42 | 0.00* | 751.03 |
| UpdateItem | 81.76 | 0.00* | 350.87 |
| TransactGetItems | 215.46 | 0.00* | 665.99 |
| TransactWriteItems | 4.54 | 0.00* | 278.76 |

\* Cases contain errors after the four-Cell runtime fails; zero is not healthy capacity. Single-Cell transaction writes complete 29 requests with p95 **1,959.57 ms**, versus SQLite's 1,412 requests and **92.63 ms**. Calls contain two items. Full tables and raw results retain counts, errors, tails and actual elapsed time.

The regression proves stalled log I/O held the heartbeat's shared mutex. The fix keeps a separate log-transition mutex and lets heartbeat renewal proceed; signed ETag retries preserve log state. Formatting, strict Clippy and 39 focused/library tests pass. The original fresh transaction diagnostic now completes 32 distinct writes with zero errors and verifies 64 items by strong reads. Resident coordinator waves are not consistently faster, so that diagnostic does not prove a coordinator-reuse speedup.

**The fix does not resolve every fencing failure.** Four-Cell logs retain terminal Fenced errors; the precise remaining renewal phase is not yet isolated. SDK-window host load is single **33.50→28.05**, four Cells **52.76→69.93**, SQLite **69.45→54.44**, on 12 CPUs with about 36 GiB of swap. No local build, test or provider probe overlaps measurement. All ten fixture PIDs and three containers are absent; no forced cleanup is needed. Changing contention prevents attribution of an end-to-end speedup. Full recovery, larger single-Cell budgets, live conversion, sustained capacity and SQLite parity remain unfinished. The preceding source's [full SDK CI](https://github.com/crabbuild/beyonddb/actions/runs/37040618370) fails: native 48/48, peers 74/75 with missing GSI ownership, processes 7/8 with the observed-stream EOF shutdown panic.

The follow-up renewal-phase diagnostic repeats the original four-Cell workload at source `7cba575`, adding operational failure observations with unchanged deadlines and durability. All 24 cases run: 8,502 completions, **six timeouts** (five transaction reads, one transaction write). No renewal warning or terminal Fenced error occurs; the earlier endpoint loss is not reproduced and remains unresolved. Eight-client transaction writes complete eight requests at 0.60 req/s with successful p95 7,613.65 ms. This is a diagnostic, with no fresh SQLite comparison or speedup claim. SDK-window load is 74.40→49.94 on 12 CPUs, with about 37 GiB of swap. Three PIDs and one container are absent without forced cleanup. A retained mailbox refusal and incomplete-resolution warning do not establish the cause of a specific timeout.

### Previous Cellule member-expiry release verification

The Cellule member-expiry release refresh measures committed source `61a74a4`, pinning reviewed Cellule `0f4ca09`. All **72 cases run**. Four-Cell BeyondDB and SQLite complete 24 cases each with zero request errors; single-Cell BeyondDB records **eight timeouts** in its concurrent transaction-write case. **The all-API SQLite performance goal remains unmet.**

| API, eight clients | Single req/s | Four Cells req/s | SQLite req/s |
| --- | ---: | ---: | ---: |
| GetItem | 55.65 | 63.56 | 509.12 |
| PutItem | 25.86 | 25.92 | 456.79 |
| UpdateItem | 30.93 | 41.83 | 400.27 |
| TransactGetItems | 82.34 | 2.83 | 526.00 |
| TransactWriteItems | 0.00* | 2.26 | 273.94 |

\* No successful sample: all eight requests time out. Four-Cell transaction writes complete 17 requests with p95 **3,911.42 ms**, versus SQLite's **100.14 ms**. Calls contain two items. The complete tables retain every API, client count, tail, completion and error; raw JSON retains actual elapsed time.

The upgrade includes private peer TCP_NODELAY, vectored writes, fresh-authority routing improvements, combined compaction/append publication, admitted owner fences and host member-expiry rotation. BeyondDB implements the required rotation callback using fresh enrollment and bounded signed fleet observations on the host's 30-second interval. Verification records passing focused authority, durability, prepare and single-Cell SDK cases, formatting, strict Clippy and 28 library tests. The broad two-owner test fails on a missing GSI owner. Preceding-source SDK CI passes native **48/48**, peers **74/75** and processes **7/8**, retaining coordinator ownership and EOF shutdown failures. No dependency source is patched; full recovery remains unqualified.

**This sample does not isolate an end-to-end speedup or regression.** On 12 logical CPUs, SDK-window load is single **26.78→32.03**, four Cells **35.65→29.46**, SQLite **28.64→30.43**, with about **35–36 GiB of swap** in use. No task-local build, test or provider probe overlaps measurement. One mailbox-capacity warning occurs in single mode; four Cells retain two deferred transaction-resolution warnings. All seven fixture PIDs and both containers are absent without forced BeyondDB cleanup. Zero errors in four-Cell mode does not prove maintenance convergence or sustained capacity. Larger configurable single-Cell budgets, live conversion, old-root upgrades and SQLite parity remain unfinished.

### Previous bounded prepare release verification

The bounded prepare release refresh measures committed source `8b2f92e`. **Performance qualification fails:** the single/four-Cell fixtures lose their serving leases during measurement, record **26/71 request errors**, and complete no transaction writes. SQLite completes its 24 cases with zero errors after its missing release binary is rebuilt from the same pinned source. All **72 unique cases** run across the retained experiment and repaired SQLite invocation. **The all-API SQLite goal remains unmet.**

| API, eight clients | Single req/s | Four Cells req/s | SQLite req/s |
| --- | ---: | ---: | ---: |
| GetItem | 115.37 | 64.67 | 486.48 |
| PutItem | 12.45 | 0.00* | 297.95 |
| UpdateItem | 9.61 | 0.00* | 272.60 |
| TransactGetItems | 13.23 | 0.09* | 391.10 |
| TransactWriteItems | 0.00* | 0.00* | 239.73 |

\* Cases contain errors. Zero means no request completed after endpoint loss, not healthy capacity. Four-Cell transaction reads complete one request and time out eight; its successful latency percentile cannot represent a latency distribution. The full tables retain all APIs, client counts, p95 values, completions and errors; raw JSON retains actual elapsed times.

The held-publication regression proves **eight concurrent small prepares fit instead of three**. New account/data commands reserve 64 KiB replies for inputs up to 32 KiB; the prior roughly 4 MiB reply reservation exhausted the 16 MiB Cell mailbox. Complete failure images use the existing wide command only after a durable rejected receipt proves rollback. Reply loss retains authoritative recovery. Owner restoration, token reuse, mixed participants, capacity refusal, formatting, strict Clippy and 28 library cases pass. This is an admission improvement; the failed release run does not establish a transaction speedup.

**This workstation sample does not isolate a source regression or speedup.** On 12 logical CPUs, SDK-window load rises 20.49→43.70 for single mode and 47.06→71.54 for four Cells; SQLite runs at 39.42→36.69. Swap is about 34.4–35.3 GiB for BeyondDB and 39 GiB for SQLite. No task-local build, test or provider probe overlaps measurement. The SQLite rebuild is separate and its new binary identity is recorded. All seven fixture PIDs and both containers are absent; volumes are retained.

Owned BeyondDB logs end with `Fenced`; the immediate cause of delayed lease renewal is not isolated. Preceding-source full SDK CI also fails: native 48/48, peers 72/73, processes 7/8. Two-owner recovery, observed-stream EOF shutdown, full current-source recovery and old-root upgrades remain unqualified. Cellule stays at `a4500ad` for this measurement. Review of newer `origin/main` routing, TCP_NODELAY, compaction and member-expiry changes is preparatory; that upgrade is not qualified here.

### Previous fresh remote routing release verification

The fresh remote routing release comparison measures committed source `b285dae`. All **72 cases ran**, with one Single transaction-write throttling cancellation, one four-Cell transaction-read timeout and zero SQLite request errors. **The all-API SQLite performance goal remains unmet.** Only ListTables exceeds SQLite's eight-client request rate in both BeyondDB modes.

| API, eight clients | Single req/s | Four Cells req/s | SQLite req/s |
| --- | ---: | ---: | ---: |
| GetItem | 162.86 | 157.81 | 520.78 |
| PutItem | 37.71 | 18.14 | 137.04 |
| UpdateItem | 55.41 | 18.39 | 290.52 |
| TransactGetItems | 118.85 | 0.80 | 299.70 |
| TransactWriteItems | 0.72 | 1.97 | 403.02 |

Transaction-write p95 is 9623.62/5522.40 ms for single/four Cells, versus SQLite's 46.15 ms. Completed requests are 7/16/2026, with one Single cancellation excluded from successful latency percentiles. Calls contain two items. The full report retains all API rates, tails, counts, errors and actual elapsed time.

The gated regression proves fresh authority and owner enrollment reads overlap; neither read is skipped. A bounded session hint selects the probe, but fresh authority still decides ownership and lease expiry is rechecked after I/O. Changed owners get their own fresh enrollment read. The test checks current-owner probe failure, restoration on a third node and a signed strong read with SDK retries disabled. Existing drain/expiry cases, formatting, strict Clippy and all 28 library cases pass.

**The measurement does not isolate an end-to-end speedup or regression.** Host load is single: 24.76→33.82; partitioned: 29.82→26.20; sqlite: 26.67→26.44, on 12 logical CPUs with about 42–43 GiB of swap in use. No task-local build, test or provider probe overlaps measurement. The temporary Python environment was recreated with the same boto3 version; the previous botocore version was not recorded. All seven fixture PIDs and both containers are absent, without forced BeyondDB cleanup.

Single mode records a participant-capacity warning during SDK measurement and two follower-append warnings after it. Four-Cell owned logs are empty. The preceding source's [full SDK CI](https://github.com/crabbuild/beyonddb/actions/runs/36971574005) fails: native 48/48, peers 70/72 and process 7/8. Post-drain token replay, GSI ownership recovery and follower shutdown with the observed-stream EOF panic remain unresolved. Current-source full peer/process CI and old-root upgrades remain unqualified.

Participant capacity evidence identifies the next hypothesis: prepare advertises a roughly 4 MiB maximum reply while the runtime reserves that bound against a 16 MiB per-Cell mailbox. A correctly bounded metadata-only prepare path needs a regression with real replies held in flight; failure images and durable transaction rules must remain. No prepare change is included in this release.

### Previous follower discovery release verification

The follower discovery release comparison measures committed source `d88f580`, with unchanged reviewed Cellule and ExtendDB pins. All **72 cases ran**, but four-Cell TransactGetItems has **nine SDK read timeouts**. Single mode and SQLite have zero request errors. **SQLite parity remains unmet; every eight-client API is below SQLite's request rate in this sample.**

| API, eight clients | Single req/s | Four Cells req/s | SQLite req/s |
| --- | ---: | ---: | ---: |
| GetItem | 91.90 | 17.28 | 565.38 |
| PutItem | 12.03 | 6.81 | 724.96 |
| UpdateItem | 20.22 | 8.44 | 730.31 |
| TransactGetItems | 19.04 | 0.07 | 437.19 |
| TransactWriteItems | 5.40 | 1.16 | 496.07 |

Transaction-write p95 is 1692.89/7442.14 ms for single/four Cells, versus SQLite's 36.88 ms. Completed requests are 32/9/2485; requests contain two items. Four-Cell transaction reads complete zero/one requests at one/eight clients, with one/eight timeouts. Latency percentiles cover successful requests only and exclude failures. The full report retains rates, counts, errors, tails and actual elapsed time for every case.

The transport regression proves two concurrent cold or expired follower lookups perform one signed fleet scan instead of two. Each follower still performs fresh canonical mTLS-bound authorization and fsyncs. The shared discovery survives only its in-flight callers; existing peer cache and hard advertisement expiry bounds remain. Cancellation, duplicate identities, durable recovery and store reopen are checked. Formatting, strict Clippy and 28 library tests pass.

**This sample does not isolate an end-to-end speedup or regression.** The host is heavily contended: SDK-window load is 56.19→34.77 for single mode, 41.61→156.51 for four Cells and 149.14→62.17 for SQLite, on 12 logical CPUs with 34–36 GiB of swap in use. No task-local build, test or provider probe overlaps measurement. The failed experiment is retained; deadlines and retries are unchanged. All seven fixture PIDs and both containers are absent, with no forced BeyondDB cleanup.

Both measurements retain two mailbox-capacity warnings; four-Cell shutdown adds two follower-append warnings outside the SDK window. No background convergence is claimed for timed-out requests. The preceding source's [full SDK CI](https://github.com/crabbuild/beyonddb/actions/runs/36826386923) is now terminal failure: native 48/48, peers 71/72, process 7/8. GSI ownership recovery and graceful shutdown with Cellule's observed-stream EOF panic remain unresolved. Current-source full peer/process CI and old-root upgrades remain unqualified.

Read-locality evidence from the preceding release shows that a single base Cell often executes on a follower. Remote read routing and authorization are the next investigation; the GET counters include background work and do not identify each read's purpose. Single-Cell placement alone does not remove private peer I/O.

### Previous cold catalog release verification

The cold catalog release comparison measures committed source `1e8765c`, with unchanged reviewed Cellule and ExtendDB pins. All **72 cases have zero SDK request errors**. **SQLite write and transaction parity remains unmet.** Single mode exceeds SQLite's request rate only for eight-client ListTables; four Cells exceed it only for single-client DescribeTable.

| API, eight clients | Single req/s | Four Cells req/s | SQLite req/s |
| --- | ---: | ---: | ---: |
| GetItem | 257.50 | 475.01 | 1034.55 |
| PutItem | 49.97 | 42.92 | 731.88 |
| UpdateItem | 58.76 | 78.82 | 769.80 |
| TransactGetItems | 122.41 | 4.76 | 565.82 |
| TransactWriteItems | 6.14 | 3.06 | 515.66 |

Transaction-write p95 is 1772.24/3469.94 ms for single/four Cells, versus SQLite's 36.97 ms. Completed requests are 35/19/2583; four-Cell single-client writes complete only three requests. Calls contain two items. The full report includes every API, client count, percentile and completed-request count.

The signed regression proves cold coordinator catalog publication overlaps registration discovery and authority lookup. Bootstrap still requires the published catalog proof, and BEGIN still requires durable registration. Missing-generation checks, competing-owner fences, token identity and the resident fast path remain intact. Registration, incarnation restoration, token replay and the committed item are verified.

**This sample does not establish an end-to-end speedup.** Transaction rates are below the preceding sample; ordinary writes and reads also vary. Host load is 28.64→30.53 for single mode, 30.45→52.43 for four Cells and 49.19→26.57 for SQLite, on 12 logical CPUs. Start snapshots report 31–33 GiB of swap in use. No task-local build, test or provider probe overlaps measurement. The old-release cold/warm diagnostic also shows substantial latency on warmed shards, but its sequential populations and changing host load do not isolate admission cost.

Formatting, strict Clippy, 28 library tests and nine focused admission/registration cases pass. The native run passes 46/48; both failures pass individually without edits, and **the intermittent ownership/activity failures remain unresolved**. Preceding full SDK CI runs retain token replay, coordinator/GSI owner recovery and graceful shutdown failures, including the observed-stream EOF panic. Full peer/process CI and old-root upgrades remain unqualified. One incomplete-resolution WARN occurs during four-Cell measurement. All seven fixture PIDs and both containers are absent, without forced BeyondDB cleanup. Zero SDK errors does not establish maintenance convergence or sustained capacity.

### Previous acknowledged-completion release verification

The acknowledged-completion release comparison measures committed source `daa73a8`, with unchanged reviewed Cellule and ExtendDB pins. All **72 cases have zero SDK request errors**. **SQLite performance parity remains unmet; every eight-client API is slower than SQLite in this run.**

| API, eight clients | Single req/s | Four Cells req/s | SQLite req/s |
| --- | ---: | ---: | ---: |
| GetItem | 404.23 | 568.23 | 1323.04 |
| PutItem | 116.46 | 66.84 | 1210.74 |
| UpdateItem | 113.94 | 73.94 | 968.88 |
| TransactGetItems | 329.42 | 5.92 | 1177.13 |
| TransactWriteItems | 9.58 | 7.48 | 977.79 |

Transaction-write p95 is 1074.01/1283.80 ms for single/four Cells, versus SQLite's 13.97 ms. Calls contain two items. The full report includes every API, client count, percentile and completed-request count.

The signed regression proves fresh small transaction writes use **one coordinator query instead of four**. After an acknowledged fresh BEGIN and COMMIT, the adapter uses the exact accepted participant set and durably records every resolution receipt before success. Token replay, uncertain decisions, larger inputs and read-image cleanup retain authoritative readback. Lost participant replies require observed committed state; lost coordinator receipts return a transient error until replay confirms completion. Two-Cell values survive owner restoration.

This reduces coordinator work but **does not demonstrate an end-to-end speedup**. The previous sample measured 10.76/10.15 transaction writes/s; this run measures 9.58/7.48. The ordinary read/write paths are unchanged and their rates also vary. Host load is 16.60→17.98 for single mode, 16.69→25.86 for four Cells and 25.63→14.56 for SQLite, on 12 logical CPUs. No task-local build, test or provider probe overlaps measurement. SQL handler means are 0.869/0.532 ms; durable follower append means are 14.657/17.394 ms. These overlapping background-inclusive populations are not additive request timings.

Formatting, strict Clippy, 28 library tests, signed restoration and completion-fault cases pass. The full native run passes 47/48; the remaining new fixture's incorrect GetItem key is corrected and its focused rerun passes. All 48 pass across those two invocations. Full peer/process CI and older-root upgrades remain unqualified. The previous head's [full signed SDK CI](https://github.com/crabbuild/beyonddb/actions/runs/36815844420) subsequently fails: native 48/48, peers 70/71 and process 7/8. GSI owner recovery and killed-owner graceful shutdown fail; the latter again retains Cellule's observed-stream EOF panic. Three background WARN lines occur during measurement: two mailbox-capacity deferrals and one incomplete-resolution deferral. All seven fixture PIDs and both containers are absent, without forced BeyondDB cleanup. Zero SDK errors does not establish maintenance convergence or sustained capacity.

### Previous returned-update release verification

The returned-update release refresh measures committed source `a07acaf`, Cellule `a4500ad` and ExtendDB `7eaa89b`. It completes **72 cases with zero SDK request errors** across fresh single-Cell, four-Cell and SQLite fixtures. **SQLite write and transaction parity remains unmet.**

| API, eight clients | Single req/s | Four Cells req/s | SQLite req/s |
| --- | ---: | ---: | ---: |
| GetItem | 506.27 | 724.07 | 1630.70 |
| PutItem | 103.43 | 111.20 | 1287.67 |
| UpdateItem | 116.05 | 110.98 | 1067.54 |
| TransactGetItems | 412.00 | 7.39 | 1489.57 |
| TransactWriteItems | 10.76 | 10.15 | 868.46 |

UpdateItem p95 is 97.97/112.52 ms for single/four Cells, versus SQLite's 17.56 ms. Transaction-write p95 is 977.59/919.62 ms, versus 18.65 ms. Transactions contain two items per call. The full report retains all APIs, client counts, completed requests and tail latency.

**These are contended workstation samples.** The 12-core host's load falls from 39.26→25.84 during single mode, 25.35→22.68 for four Cells and 21.75→20.65 for SQLite. No task-local build, test or provider probe overlaps measurement. Read and transaction paths are unchanged, yet their rates rise too; the rate changes do not isolate a source speedup. SQL handler means are 0.876/0.609 ms, while follower durable append means are 16.368/20.103 ms. These overlapping populations include background work and must not be added into a request latency.

The returned-update verification proves **64 concurrent signed updates use 24 durable commands instead of 64**. The pinned ExtendDB UpdateItem handler always requests the new image for capacity calculation, including `ReturnValues=NONE`; this previously bypassed the no-return batcher. Routed updates now coalesce distinct keys and return compact individual results. Conditions retain their own failures, successful writes retain individual stream ordinals, and repeated keys are deferred. The per-Cell queue has 64 permits, with batches capped at 16 updates, 1 MiB input and 128 KiB reply. A 2 ms partial-queue window trades a small single-client delay for sharing durable work.

Large replies fall back to separate commands only after a confirmed rejected receipt proves that items, indexes and stream records rolled back. An ambiguous invocation is never retried internally. Signed tests verify same-key ADD results, condition-failure images, stream replay, owner restoration and 390,000-byte escaped item images. The same returned-image cases match the pinned SQLite server. Formatting, strict Clippy, 28 library tests, 48 native cases and three focused update tests pass.

Cellule `a4500ad` contains website/documentation changes with identical Rust code to the previous pin. It does not fix runtime recovery or performance. The preceding source's [full SDK CI](https://github.com/crabbuild/beyonddb/actions/runs/36810977455) fails: native 48/48, peers 67/68, process 7/8. Coordinator ownership during restart and graceful shutdown after an owner kill remain open qualification gaps; the earlier post-drain replay failure does not recur in that run, without a claimed fix. The new source has not completed full peer/process qualification or old-root upgrade checks. Matching compiled peers are required.

All seven fixture PIDs and both containers are absent. Neither BeyondDB fixture requires forced process cleanup; SQLite exits 0. Four-Cell logs retain two deferred transaction-resolution warnings during measurement. Zero SDK errors does not qualify maintenance convergence. A separate Cellule EOF guard proposal is unapplied and awaits the dependency approval required by AGENTS.md.

### Previous acknowledged-BEGIN release comparison

The acknowledged-BEGIN release refresh measures committed source `d46f6f9`, Cellule `e07670e` and ExtendDB `7eaa89b`. It completed **72 cases with zero SDK request errors** across fresh single-Cell, four-Cell and SQLite fixtures. SQLite write and transaction parity remains unmet.

| API, eight clients | Single req/s | Four Cells req/s | SQLite req/s |
| --- | ---: | ---: | ---: |
| GetItem | 125.36 | 324.92 | 1019.35 |
| PutItem | 70.63 | 66.97 | 639.97 |
| TransactGetItems | 85.82 | 5.43 | 913.05 |
| TransactWriteItems | 4.79 | 7.72 | 557.23 |

Transaction-write p95 was 1990.78/1227.48 ms for single/four Cells, versus SQLite's 31.91 ms. Calls contain two items, so transaction item throughput is twice the request rate.

**This is a contended workstation sample.** The 12-core host's load changed from 15.86→34.83 during the single run, 38.78→28.02 for four Cells and 26.97→22.00 for SQLite. No task-local build, test or provider probe overlapped measurement. Four-Cell transaction writes are higher than the preceding sample, while single-Cell writes are lower; this experiment does not isolate a source speedup.

The signed SDK regression verifies fresh transaction coordinator queries fall from five to four. After a confirmed fresh BEGIN, the adapter reuses its exact accepted participants for inputs at most 32 KiB. Existing tokens, ambiguous replies, larger inputs and recovery still read durable coordinator state. All durable prepare, decision, resolution and completion checks remain.

The full 48-case native suite, four signed Cell-model tests, formatting and strict all-target Clippy pass for this source. Its production-equivalent `bbe1803` [full SDK CI](https://github.com/crabbuild/beyonddb/actions/runs/36810977455) subsequently fails: native 48/48, peers 67/68 and process 7/8. Coordinator ownership during restart and graceful shutdown remain open. An earlier run also failed post-drain token replay and GSI ownership; no fix is claimed from their absence in this run. Older-root upgrades remain unqualified. All seven benchmark fixture PIDs and both containers are absent.

### Previous coordinator-admission release comparison

The coordinator-admission release refresh measures committed source `58ba0dd`, Cellule `e07670e` and ExtendDB `7eaa89b`. It completed **72 cases with zero SDK errors** across fresh single-Cell, four-Cell and SQLite fixtures, using signed AWS CLI creation and the unchanged boto3 harness.

| API, eight clients | Single req/s | Four Cells req/s | SQLite req/s |
| --- | ---: | ---: | ---: |
| GetItem | 237.59 | 679.86 | 786.23 |
| PutItem | 130.73 | 49.19 | 796.17 |
| TransactGetItems | 516.32 | 6.12 | 766.76 |
| TransactWriteItems | 9.35 | 5.65 | 539.37 |

Single-Cell transaction reads measured p95 20.54 ms, versus 1816.47 ms with four Cells and 21.73 ms with SQLite. Single-Cell transaction writes measured p95 1126.21 ms, versus SQLite's 37.02 ms. SQLite parity remains unmet.

The admission regression reduces fresh coordinator authority reads from two to one and four concurrent account registrations to one durable command. Metadata envelopes now reserve 4 KiB per input/result. Cancellation cannot acknowledge an unpublished registration, and acknowledged rows survive account owner restoration. Coordinator shard identities and token replay routing remain unchanged.

This sample does **not** establish an end-to-end transaction-write speedup: the preceding record measured 10.39/7.55 requests/s, versus 9.35/5.65 here. Single-mode PutItem increased while four-Cell writes decreased. Fresh owner placement, I/O latency, host load and acknowledgment populations differ between fixtures. The count regression proves reduced admission work; the SDK samples do not isolate its throughput effect.

Host load at SDK start/end was 15.37→13.67 for single, 13.34→14.53 for partitioned, and 14.89→23.91 for SQLite. No task-local build, test or provider probe overlapped measurement. All seven new server processes and both containers are absent. Four-Cell logs retain a deferred capacity sweep during measurement; single-mode follower-advertisement warnings occur during cleanup. Zero SDK errors does not qualify maintenance convergence or graceful shutdown.

Focused checks, signed placement tests, 27 library tests, formatting and strict Clippy passed for this source. Subsequent [full SDK CI for production-equivalent `b9cbf50`](https://github.com/crabbuild/beyonddb/actions/runs/36806586859) failed: native 48/48, peers 65/67 and process 7/8. The latest qualification gaps are described above. Matching compiled peers are required, and older-root upgrades remain unqualified.

### Previous placement release comparison

The single/four-Cell release comparison measures source `8ecd6d5`, Cellule `e07670e`, and ExtendDB `7eaa89b`. Both BeyondDB fixtures use the same binary and four configured initial partitions, with `single` or `partitioned` selected at creation. AWS CLI creates each table; signed boto3 measures the unchanged workload. All 24 cases per fixture completed: **72 cases, zero SDK errors** across two BeyondDB modes and SQLite.

| API, eight clients | Single req/s | Four Cells req/s | SQLite req/s |
| --- | ---: | ---: | ---: |
| GetItem | 345.76 | 525.60 | 946.39 |
| PutItem | 110.31 | 78.80 | 1106.55 |
| TransactGetItems | 336.73 | 6.20 | 724.66 |
| TransactWriteItems | 10.39 | 7.55 | 459.16 |

Single-Cell transaction reads measured p95 33.14 ms, versus 1646.43 ms with four Cells. Single-Cell PutItem p95 was 97.21 ms, and transaction-write p95 was 920.8 ms; SQLite measured 15.27 and 47.44 ms. Tokenized writes still use the coordinator. The all-API SQLite objective remains unmet.

Single mode returned 1,691 eight-client transaction reads, while the four-Cell sample returned 38; transaction writes completed 58/43. These short samples do not establish sustained capacity. One Cell also measured lower point-read throughput than four, so placement has workload tradeoffs. Host load, fresh owner placement, and provider latency differ between fixtures; the ratios do not isolate placement's service speedup.

Host load was 17.93→16.75 for single, 17.46→23.30 for partitioned, and 24.15→26.65 for SQLite. No task-local build/test/provider probe overlapped measurement. Every new fixture PID/container is absent; volumes and scratch data are retained. Partitioned logs retain deferred recovery and mailbox-byte pressure in maintenance workers. Zero SDK errors does not qualify those workers' convergence.

The placement verification passes three signed model tests, five creation/lifecycle tests, two statistics tests, 27 library tests, formatting and strict Clippy. The original record captured Rust CI success and SDK CI pending. Subsequent production-equivalent SDK CI failed as described above; full recovery and older-root upgrades remain unqualified. The new Single variant requires matching compiled peers.

### Previous native-volume release pair

The native-volume release pair
reuses source `7cf8f46` and the exact binary from the preceding host-bind run,
with Cellule `e07670e` and ExtendDB `7eaa89b`. RustFS stores data in a named
volume inside Colima. All 24 cases completed per backend with **zero SDK errors**.

| API, eight clients | BeyondDB requests/s | SQLite requests/s | BeyondDB p95 |
| --- | ---: | ---: | ---: |
| GetItem | 470.78 | 999.19 | 31.08 ms |
| PutItem | 61.89 | 760.40 | 221.77 ms |
| TransactGetItems | 5.22 | 771.38 | 2084.94 ms |
| TransactWriteItems | 8.01 | 544.07 | 1291.82 ms |

Only DescribeTable exceeded SQLite at one/eight clients. Item operations and
transactions remain slower; the all-API performance objective is unmet. These
are five-second samples: eight-client transaction reads/writes completed 31/46
requests, and do not establish a sustained capacity or production target.

The prior host-bind pair measured PutItem at 22.27 requests/s and transaction
writes at 0.61 with five timeouts. This pair changed storage placement while
reusing the binary, but host load and fresh owner placement also changed.
BeyondDB host load was 25.33→27.14; SQLite ran afterward at 27.34→22.96.
No task-local build/test overlapped either measurement. The comparison cannot
isolate a code or storage speedup. All fixture processes/container are absent;
the named volume and server scratch data remain available for inspection.

A direct S3 diagnostic
completed 48 cases without errors, using opposite storage orders. Native-volume
1-KiB conditional replacements reached 226–238 requests/s at eight clients,
versus 71–77 on binds. GET throughput was lower in those native samples while
load varied. Keep provider storage placement explicit in benchmark results.
The [deployment example](deployment.md#local-object-store-fixture) already uses
a named volume.

Runtime means were SQL command 0.542 ms, provider GET/PUT 6.492/27.109 ms,
and publication total 146.625 ms. Across 3,610 samples per follower phase,
lookup averaged 6.140 ms, round trip 30.778 ms, fresh enrollment 12.447 ms,
and durable append 17.620 ms. These scopes overlap and include background work;
they must not be summed into SDK latency or treated as a causal decomposition.
The full record retains every API, counts, phases, source/binary hashes and host
snapshots. Full signed SDK recovery CI failed on this source: native 48/48
passed, peers 57/59 and process 7/8 failed. Full recovery remains unqualified.

### Previous bounded-snapshot host-bind release pair

The bounded coordinator snapshot release pair
measures source `7cf8f46`, Cellule `e07670e`, and ExtendDB `7eaa89b`.
All 24 cases completed per backend. BeyondDB recorded **five SDK timeouts**,
all in eight-client TransactWriteItems; SQLite recorded zero.

| API, eight clients | BeyondDB requests/s | SQLite requests/s | BeyondDB p95 |
| --- | ---: | ---: | ---: |
| GetItem | 323.31 | 745.57 | 62.65 ms |
| PutItem | 22.27 | 723.85 | 711.47 ms |
| TransactGetItems | 0.98 | 830.83 | 8614.57 ms |
| TransactWriteItems | 0.61 | 990.81 | 6829.62 ms* |

\* Nine transaction writes completed; five timeouts are excluded from the
percentile. Only single-client ListTables exceeded SQLite. Every eight-client
case remained slower; the all-API SQLite objective is unmet.

Host load was 37.42→32.62 during BeyondDB, then 32.57→26.22 during SQLite on
12 logical CPUs. No task-local build/test overlapped measurement. Fresh owner
placement and provider costs also vary. This sample does not isolate the query
fusion's throughput effect or establish production/fleet capacity. All fixture
PIDs and the exact RustFS container are absent, with no forced PID cleanup.

The counted driver regression
fails before the change with seven coordinator queries and passes after with
four. One bounded query observes durable status and small immutable payloads;
large inputs retain chunked retrieval. Durable preparation, decision, resolution
and final-status checks remain. The new query changes the compiled coordinator
contract; older-root upgrades remain unqualified.

Local gates pass: 12 transaction cases, restart read cleanup, seven signed
coordinator SDK cases, 27 library tests, formatting and strict all-target Clippy.
The focused count regression overlaps the transaction selection. Source Rust CI
passed; full SDK CI subsequently failed: native 48/48 passed, peers 57/59 and
process 7/8 failed. The additional large remote read root assertion remains
unqualified. Prior-source SDK CI on the
same Cellule pin failed the global-index owner and graceful-stop assertions:
native 47/47 passed, peers 58/59 and process 7/8 failed. Full recovery is open.

SQL command execution averaged 0.384 ms, provider GET/PUT 14.825/261.135 ms and
publication total 1271.151 ms. Follower lookup averaged 82.857 ms across 1,488
samples; HTTP round trip 49.520 ms, enrollment 33.808 ms and durable append
15.005 ms each had 1,486 samples. These overlapping populations include
background work and in-flight operations; counts need not match, and event
times must not be added into SDK latency. The full record retains every API,
sample count, failure, phase metric, source/binary hash and host snapshot.

### Previous Cellule e07670e release pair

The Cellule e07670e release pair
measures source `664e07a`, reviewed upstream Cellule `e07670e`, and ExtendDB
`7eaa89b`. All 24 cases completed per backend. BeyondDB recorded **seven SDK
timeouts** in eight-client TransactGetItems; SQLite recorded zero.

| API, eight clients | BeyondDB requests/s | SQLite requests/s | BeyondDB p95 |
| --- | ---: | ---: | ---: |
| GetItem | 310.54 | 459.75 | 64.82 ms |
| PutItem | 10.09 | 800.97 | 1880.62 ms |
| TransactGetItems | 0.19 | 752.99 | 1429.53 ms* |
| TransactWriteItems | 1.14 | 453.42 | 8179.69 ms |

\* Only two transaction reads completed; seven timeouts are excluded from the
percentile. This p95 does not establish improvement over an error-free run.
Only single-client Query exceeded SQLite. Every eight-client case remains slower;
the all-API SQLite objective is unmet.

The workstation was heavily contended: BeyondDB host load was 69.98→23.59 on
12 logical CPUs; SQLite ran afterward at 22.59→21.72. No local build or test
from this task overlapped measurement. Fresh owner placement and provider costs
also vary. This pair does not isolate the upgrade's performance effect or qualify
production/fleet capacity. All fixture processes and the exact RustFS container
are absent, with no forced PID cleanup. Raw data and hashes are retained.

The new main includes the approved one-read append API unchanged, lease-fenced
resident-route reuse, bounded owner-discovery coalescing, read-replica release
checks and serialized directory-cache index snapshots. The lockfile changes only
seven Cellule Git sources. Upgrade verification
passes 27 library tests, 58 residency/routing tests, formatting and strict Clippy;
seven coordinator cases also pass and overlap residency. Upstream's four CI
workflows and BeyondDB source Rust CI pass. The subsequent full signed recovery CI failed: native 47/47 passed, peers
58/59 and process 7/8 failed. Recovery qualification remains open.

Across 1,572 samples per follower phase, lookup averaged 85.002 ms, HTTP round
trip 45.940 ms, fresh enrollment 29.041 ms and durable append 16.119 ms. SQL
command execution averaged 0.423 ms, provider GET/PUT 15.753/264.510 ms and
publication total 1240.378 ms. These overlapping populations include background
work; do not add them into SDK latency. The complete report includes all APIs,
request failures, case metrics and fixture metadata.

### Previous parallel coordinator release pair

The parallel coordinator release pair
measures source `388ae62`, reviewed Cellule `8ca658b`, and ExtendDB `7eaa89b`.
Both backends completed all 24 cases with **zero SDK errors**.

| API, eight clients | BeyondDB requests/s | SQLite requests/s | BeyondDB p95 |
| --- | ---: | ---: | ---: |
| GetItem | 357.58 | 870.42 | 36.02 ms |
| PutItem | 22.97 | 912.31 | 882.76 ms |
| TransactGetItems | 1.06 | 893.41 | 7560.11 ms |
| TransactWriteItems | 1.25 | 673.41 | 6953.71 ms |

Only single-client DescribeTable and ListTables exceeded SQLite. Every
eight-client case remained slower. Host load was 21.34→18.98 during BeyondDB
and 18.82→19.52 during SQLite. Owner placement and provider latency also vary.
Point reads slowed despite their unchanged path, so this sequential pair does
not isolate the admission change's effect. The all-API SQLite objective remains unmet.

The concurrent signed SDK regression
proves that two independent cold coordinators can enter authority creation
concurrently. It failed before the change and passes afterward, with durable
registration and data-owner restoration checks retained. Bounded pending-slot
accounting protects capacity; cancellation releases its reservation. Published
roots and reclamation continue through exclusive admission.

Seven coordinator cases and five reclamation cases pass with CI fixture
scheduling, as do all 27 library tests, formatting and strict Clippy. A parallel
fixture run failed replay after drain; its cause remains open. Earlier full SDK
CI on `a18652a` passed native 47/47 but failed peers 55/56 and process 7/8.
Full signed recovery qualification remains open.

Across 1,918 samples per phase, peer lookup averaged 32.302 ms, HTTP round trip
36.595 ms, fresh enrollment 21.180 ms, and durable append 14.768 ms. Provider
GET/PUT means were 10.430/221.287 ms and publication total was 1099.160 ms,
compared with SQL command execution of 0.430 ms. These overlapping populations
include background work; they cannot be added into SDK latency. The complete
record retains all cases, sample counts, host metrics, source and binary hashes,
and verified cleanup.

### Previous follower-phase release pair

The follower-phase release pair
measures source `22002dc`, reviewed Cellule `8ca658b`, and ExtendDB `7eaa89b`.
Both backends completed all 24 cases with **zero SDK errors**.

| API, eight clients | BeyondDB requests/s | SQLite requests/s | BeyondDB p95 |
| --- | ---: | ---: | ---: |
| GetItem | 616.61 | 1307.63 | 20.03 ms |
| PutItem | 61.24 | 1565.61 | 280.96 ms |
| TransactGetItems | 2.46 | 1128.43 | 4472.35 ms |
| TransactWriteItems | 1.83 | 1131.71 | 4676.52 ms |

Only single-client ListTables exceeded SQLite. BeyondDB host load fell from
16.94 to 13.58; SQLite ran afterward at
13.58→11.78. Fresh owner placement also differs between fixtures.
This instrumentation revision does not establish a speedup or fleet capacity.

The new append observations recorded 2,948 samples per phase across all nodes:
peer lookup mean 8.463 ms, HTTP round trip 26.766 ms, fresh enrollment
12.071 ms, and durable append 14.247 ms. Receiver phases overlap the round trip;
these event populations include background work and cannot be summed into SDK
latency. SQL command execution mean was 0.386 ms, provider PUT mean 86.542 ms,
and publication total mean 416.128 ms. The evidence supports examining cold
transaction admission and object publication next, while retaining exact fences
and fresh authorization. It does not isolate a network-only duration.

Phase verification
passed all 27 library tests, formatting and strict Clippy, including actual mTLS
append/reopen phase counts and cancellation accounting. Full signed peer/restart
qualification and the all-API SQLite objective remain open.

### Previous resident-admission release pair

The resident-admission release pair
measures source `6e92b5d`, Cellule `8ca658b`, and ExtendDB `7eaa89b`. All 24
cases completed per backend: BeyondDB recorded **four SDK errors** in
eight-client TransactGetItems; SQLite recorded zero.

| API, eight clients | BeyondDB requests/s | SQLite requests/s | BeyondDB p95 |
| --- | ---: | ---: | ---: |
| GetItem | 307.70 | 818.16 | 86.21 ms |
| PutItem | 21.17 | 1102.79 | 877.43 ms |
| TransactGetItems | 0.48 | 759.27 | 7441.80 ms* |
| TransactWriteItems | 1.74 | 783.82 | 5192.66 ms |

\* Successful calls only; four failed requests are excluded. SQLite was faster
in every measured case. Host load during BeyondDB was 41.43→32.36; SQLite ran
sequentially at 32.36→20.40. Fresh owner placement also varies. This busy local
sample cannot isolate the optimization's speedup or establish fleet capacity.

The focused admission regression
proves four warm admissions remove four canonical coordinator reads and perform
zero provider reads. Drain, a new provisioner, and missing registration still
use canonical admission. Two focused regressions, five coordinator lifecycle
checks, formatting, strict Clippy, and source Rust CI pass. Full signed peer/restart
qualification remains open. Provider and follower costs remain the next profiling
focus: SQL command/query means were 0.456/0.115 ms, logical provider PUT mean
181.040 ms, and fleet command-response mean 149.089 ms. These overlapping
populations must not be summed into SDK latency. The all-API SQLite objective
remains unmet.

### Previous provider-observation release pair

The provider-observation release pair
measures source `b2b6350`, Cellule `8ca658b`, and ExtendDB `7eaa89b`. Both
backends completed all 24 cases with **zero SDK errors**.

| API, eight clients | BeyondDB requests/s | SQLite requests/s | BeyondDB p95 |
| --- | ---: | ---: | ---: |
| GetItem | 655.57 | 1305.32 | 18.73 ms |
| PutItem | 57.05 | 1178.69 | 325.14 ms |
| TransactGetItems | 2.19 | 1213.74 | 5557.30 ms |
| TransactWriteItems | 2.13 | 1019.84 | 3922.09 ms |

Only single-client DescribeTable exceeded SQLite. Every eight-client case
remained slower. Host load fell from 16.96 to 13.70 during BeyondDB and from
13.70 to 11.66 during SQLite; fresh owner placement also varies. This sample
does not establish a speedup from the discovery recovery fix or fleet capacity.

The new provider metrics report GET mean 3.972 ms and PUT mean 84.768 ms across
the snapshot interval, while SQL primitive command/query means were
0.358/0.103 ms. These are overlapping events with different counts, including
background work; do not sum them into SDK latency. The full report contains
all 24 cases, raw results, metrics, host metadata, hashes, and verified cleanup.
The focused discovery regression
passes, but the full signed peer/restart scenario still fails. Production
recovery qualification and the all-API SQLite objective remain open.

### Previous one-read follower append release pair

The one-read follower append release pair
measures source `9b62001` and reviewed Cellule `8ca658b`
([dependency PR](https://github.com/crabbuild/cellule/pull/31)). ExtendDB remains
`7eaa89b`. All 24 cases completed per backend; BeyondDB recorded **four SDK
errors** in eight-client TransactGetItems, while SQLite recorded zero.

| API, eight clients | BeyondDB requests/s | SQLite requests/s | BeyondDB p95 |
| --- | ---: | ---: | ---: |
| GetItem | 302.92 | 889.90 | 61.61 ms |
| PutItem | 27.24 | 541.20 | 788.70 ms |
| TransactGetItems | 0.62 | 685.53 | 7,935.19 ms* |
| TransactWriteItems | 1.18 | 691.08 | 7,293.37 ms |

\* Successful calls only; four failed requests are excluded. The report contains
all APIs, both client counts, completed/error counts, runtime metrics, host
snapshots, artifact hashes, and cleanup evidence. Only single-client
DescribeTable and ListTables exceed SQLite in this sample. All eight-client
rates remain below SQLite. The all-API performance objective is unmet.

The approved change removes one consecutive canonical enrollment read per
follower append while retaining identity, scope, lease, log authority, and fsync
checks. Its counted-store and signed SDK process-kill tests pass. Host load was
19.02→19.03 during BeyondDB and 19.03→20.34 during SQLite; fresh owner placement
also varies between fixtures. This local sample does not establish a service
speedup. Cellule's four CI workflows and BeyondDB Rust CI passed. Full SDK CI
failed one long peer recovery test; native SDK and process suites passed. The
recovery follow-up
records the focused fix and the remaining full-scenario failure.

### Previous resident-routing release pair

The resident-routing release pair
measures source `c6fb584`, still pinned to Cellule `70bd25f` and ExtendDB `7eaa89b`.
All 24 cases completed per backend. BeyondDB recorded two SDK errors in the
eight-client TransactGetItems case; SQLite recorded none.

| API, eight clients | BeyondDB requests/s | SQLite requests/s | BeyondDB p95 |
| --- | ---: | ---: | ---: |
| GetItem | 359.56 | 975.69 | 58.69 ms |
| PutItem | 42.26 | 523.24 | 421.49 ms |
| TransactGetItems | 0.78 | 595.03 | 9,243.71 ms* |
| TransactWriteItems | 1.47 | 528.34 | 5,746.33 ms |

\* Successful requests only; two failed requests are excluded. See the report
for all APIs, both client counts, sample sizes, first-error details and raw logs.
DescribeTable and ListTables exceed this SQLite sample at both client counts;
item operations and transactions remain slower. The all-API objective is unmet.

Host load was 17.02→21.29 during BeyondDB and 21.19→18.06 during SQLite, with
roughly 19.6 GiB of allocated swap on the 12-CPU workstation. These sequential
samples do not isolate a code improvement from host conditions. Native SDK and
process CI passed, but one long peer recovery test failed; full qualification
remains open. The approved one-read follower append change is not included in
this measured release.

### Previous Cellule upgrade release pair

The Cellule `70bd25f` release rerun
uses source `37b25ff`, upgraded from Cellule `30671d5` to the latest `origin/main`
checked on September 30. All six direct dependencies and seven lockfile packages
pin the new revision; ExtendDB remains `7eaa89b`. The measured code also adds the
500 ms backend table-key metadata cache for batch APIs, so this is a combined
product/dependency sample.

Both backends completed all 24 cases. BeyondDB recorded **25 SDK errors**;
SQLite recorded zero. Each failing case’s retained first error was a read timeout.

| API, eight clients | BeyondDB requests/s | SQLite requests/s | BeyondDB p95 |
| --- | ---: | ---: | ---: |
| GetItem | 100.58 | 415.51 | 389.14 ms |
| PutItem | 12.28 | 261.16 | 1,844.92 ms |
| TransactGetItems | 0.09 | 297.73 | 651.35 ms* |
| TransactWriteItems | 0.00 | 221.13 | — |

\* The transaction-read percentile is a single successful call, excluding eight
timeouts. All eight transaction writes timed out. Eight batch writes at eight
clients and one transaction write at one client also timed out. The report has
all API rates, sample counts, latencies, raw results, runtime snapshots, and fixture
settings.

Host load rose from 28.35 to 52.46 during BeyondDB, then fell from 52.86 to 38.85
during SQLite, on 12 logical CPUs with about 19.4–19.5 GiB of swap in use. This
checkout had no build or test running during measurement; other work continued.
These sequential runs do not establish a controlled speed improvement, a regression,
or production capacity. The all-API SQLite objective remains unmet.

The locked release build, 25 library tests, backend-cache regression, formatting,
strict Clippy, and Rust CI passed. Full SDK CI on the measured source failed:
native 45/47, peers 51/52, process 8/8. Two native fixtures warmed the new route
cache before blocking control reads; both failures reproduced locally and passed
with their original assertions after giving recovery a fresh client. This test-only
correction happened after measurement. The follow-up CI run passed native 47/47
and process 8/8 but failed two peer cases: directory retirement saw a draining
Cell, and abandoned-coordinator recovery missed its deadline. A focused credential
pressure regression now passes after one bounded retry of a proven not-started
query. The original long test passed that step, then failed coordinator recovery.
The opt-in resident resolver also avoids authority reads after handle-cache expiry
and checks the current actor before reusing a cached handle. Signed owner-expiry
recovery passes with caches off and on. These source changes are now measured in the resident-routing pair above;
see the follow-up verification record.
Two local process-suite attempts failed owner recovery and subsequent fixture
creation; Docker’s filesystem had almost no free inodes. All eight process tests
passed in CI. Full recovery qualification remains open.

### Previous instrumented release pair

The instrumented release rerun
uses source `8e33705` and Cellule `30671d5`, which was current at measurement. Both backends
completed all 24 cases: BeyondDB recorded **38 SDK errors**, SQLite zero. Each
failing case's recorded first error was a read timeout.

| API, eight clients | BeyondDB requests/s | SQLite requests/s | BeyondDB p95 |
| --- | ---: | ---: | ---: |
| GetItem | 48.82 | 896.78 | 555.58 ms |
| PutItem | 0.47 | 104.16 | 6,004.70 ms* |
| TransactGetItems | 0.27 | 401.82 | 8,635.70 ms* |
| TransactWriteItems | 0.10 | 222.09 | 8,877.42 ms* |

\* Percentiles exclude errors. Only seven puts, three transaction reads, and
one transaction write succeeded in these eight-client cases.

The 12-CPU, 32 GiB workstation was heavily contended: one-minute load was
84.28–84.45 during BeyondDB and 75.27–49.92 during SQLite, with about 20–21 GiB
of swap in use. Local builds and tests finished before measurement; other work
continued. These sequential samples do not establish a controlled speed ratio,
a code regression, or production capacity. The all-API SQLite objective remains unmet.

Runtime counters recorded 288 follower-backed command replies and two object-backed
replies. Mean command worker time was 6.34 ms, queue time 188.94 ms, and object
publication 2,790.98 ms. These observations include seeding/background work and
different overlapping events; they cannot be summed into SDK latency. Product
routing, peer metadata reads, and bootstrap provisioning are not covered by
these runtime counters in this fixture. The report retains case-boundary snapshots,
all API rates, sample counts, errors, and host memory observations.

Fresh local checks passed all 46 native integration tests, 25 library tests,
both signed durability controls, actual owner process-kill recovery, formatting,
strict Clippy, and the locked release build. Rust CI passed; full SDK CI on the
measured source failed with native 46/46, peers 51/52, and process tests 8/8.
Abandoned-coordinator recovery after the remote owner stopped renewing missed
its 45-second deadline. Recovery qualification remains open. The follower recovery fixture now waits
for its warmup receipt to publish and requires activation on the first write
whose object publication is blocked.

### Previous follower diagnostic pair

At that measurement, Cellule `origin/main` was `30671d5`, used
by all direct dependencies and lockfile packages. The fresh release rerun
uses measured source `9cba5f1` (production code `d04176c`) and completed all
24 cases per backend. BeyondDB had **three transaction read timeouts**:
one in `TransactGetItems`, two in `TransactWriteItems` at eight clients.
SQLite had zero errors.

| API, eight clients | BeyondDB requests/s | SQLite requests/s | BeyondDB p95 |
| --- | ---: | ---: | ---: |
| GetItem | 186.05 | 449.22 | 103.82 ms |
| PutItem | 11.96 | 415.28 | 2,253.55 ms |
| TransactGetItems | 0.80 | 470.92 | 9,089.13 ms* |
| TransactWriteItems | 0.62 | 321.38 | 9,072.64 ms* |

\* Percentiles exclude timeouts; only nine transaction reads and eight writes
succeeded in those cases. Host load rose from 14.31 to 25.59 during BeyondDB
and from 28.81 to 29.49 during SQLite on 12 logical CPUs. No local build or
test overlapped the measurements. Placement, IAM, and durability contracts
also differ, so this pair does not establish a controlled speed ratio or
production capacity. The all-API SQLite objective remains unmet.

The release build, 25 library tests, 11 transaction tests, formatting, and
strict Clippy passed. Full SDK CI on the production code failed: elastic tests
46/46, peers 51/52, and process tests 7/8. Credential lookup failed after the
explicit memory-exhaustion probe, and the follower test did not activate its
log during unblocked warmup. Recovery qualification remains open. Four background
operations were deferred by Cell mailbox-byte capacity during measurement.
The follower closure was not reproduced; append errors occurred only after
measurement during cleanup. The report retains all API rates, latencies,
errors, fixture metadata, and raw logs.

### Previous combined coordinator release pair

The combined coordinator-commit release pair
uses source `3ccab15`, Cellule `30671d5`, three BeyondDB processes, four initial
partitions, experimental follower durability, and signed boto3. Both backends
completed all 24 cases. BeyondDB recorded **eight timeouts**: five in eight-client
`TransactGetItems`, three in eight-client `TransactWriteItems`. SQLite had zero errors.

| API, eight clients | BeyondDB requests/s | SQLite requests/s | BeyondDB p95 |
| --- | ---: | ---: | ---: |
| GetItem | 234.19 | 778.00 | 66.49 ms |
| PutItem | 23.57 | 477.86 | 645.39 ms |
| TransactGetItems | 0.50 | 525.13 | 7,889.40 ms* |
| TransactWriteItems | 0.56 | 458.64 | 8,429.05 ms* |

\* Transaction percentiles exclude timeouts. Only six reads and eight writes
succeeded in the eight-client cases. One-minute host load changed from
27.75 to 44.72 during BeyondDB and from 45.67 to 51.88 during SQLite, on
12 logical CPUs. These sequential samples do not establish a controlled
speed improvement or production capacity. The all-API SQLite objective remains unmet.

The successful transaction path now records participant prepare receipts and
COMMIT in one coordinator command. Its regression verifies exactly one durable
coordinator commit, idempotent replay, rejected incomplete/wrong receipts, and
restoration from the published root before participant resolution. All 25
library tests, 11 transaction tests, formatting, strict Clippy, the release
build, and Rust CI passed. This proves the command reduction; it does not
prove an end-to-end speedup.

Full SDK CI on `3ccab15` **failed**: elastic Cells 46/46, peers 51/52, process
tests 7/8. A two-owner recovery test failed its index-owner assertion and the
follower durability test never activated a log. Broad SDK owner restart passed
in CI; a local retry committed transactions but missed the replacement server's
45-second health deadline. The measured frontend also logged 12 node-log
submission rejections with `RuntimeClosed` and fallback to object coverage.
The dependency pin and follower durability remain incompletely qualified.
A diagnostic follow-up
adds first-error logging and records two passing local follower process-kill
checks. The CI activation failure remains unresolved; these checks do not
change the benchmark results.

The preceding compact transaction-read pair
recorded six transaction-read timeouts. That response codec keeps legal large
binary and escaped-string reads on the single-Cell query path, avoiding JSON
expansion into durable saved images. Signed remote-owner reads preserved exact
values without mutating the participant root; large binary reads also survived
an actual owner restart. Query codec versions are now 3; mixed-version peer
rollout is unqualified. Its full SDK CI failed with elastic Cells 46/46,
peers 50/52, and process tests 7/8. See both reports for raw measurements,
sample counts, CI links, and local evidence.

The preceding peer owner-cache pair
recorded 19 timeouts under different load. Its signed mTLS regression proves
that the opt-in 500 ms private receiver cache removes repeated resident
handle authority reads, refreshes after expiry, and rejects unauthorized or
drained-owner requests. That focused proof does not establish end-to-end
SQLite parity.

## Earlier object-publication fixture

The release repeat after the peer read fix
uses BeyondDB `832da2a`, Cellule `30671d5`, and pinned ExtendDB SQLite. Both
backends completed all 24 signed API/client cases with **zero foreground
errors**. At eight clients, BeyondDB/SQLite measured 857/778 `GetItem`,
16.9/746 `PutItem`, 1.33/576 `TransactGetItems`, and 1.20/371
`TransactWriteItems` requests/s. BeyondDB logged three deferred background
sweeps from mailbox capacity. RustFS used a fresh bind mount in Colima's
shared home directory after the Docker VM ran out of space; the preceding
named-volume attempt
became fenced and recorded 45 foreground errors. Host load and storage paths
differ between runs, so these rates do not prove a code-driven improvement.

The peer read fix passed a signed remote-owner regression and the large
binary/escaped transaction checks after owner restart. The longer recovery
test subsequently failed an index-owner assertion with `owner=None` after
the signed index query and journal acknowledgement converged. That full SDK CI run did not pass. Zero foreground errors in this benchmark
does not mean recovery qualification or the all-API performance target is met.

The initial signed-API rerun on this pin used Cellule `30671d5` and ExtendDB SQLite
to `7eaa89b`. Both harnesses completed all 24 five-second cases. At eight
clients, BeyondDB/SQLite measured 327/721 `GetItem`, 13/481 `PutItem`, and
2.11/277 `TransactWriteItems` successful requests/s. BeyondDB's eight-client
`TransactGetItems` case also had **seven read timeouts**; the other 23 cases
had zero foreground errors. The server logged five deferred background
operations from Cell mailbox-byte exhaustion. One-minute load on the
12-logical-CPU host changed from 30.2 to 48.1 during BeyondDB and ended at
27.0 after SQLite. See the full table, p95 latencies, fixture, and raw
JSON. These sequential
samples do not establish a controlled speed ratio or production capacity.

The [preceding complete Cellule `9e17746` rerun](../benchmarks/2026-09-29-cellule-main-rerun/README.md)
finished all 24 cases for each backend with zero foreground errors; it is
historical context rather than a matched baseline for the new pin. An
[earlier attempt at that pin](../benchmarks/2026-09-29-cellule-main-attempt/README.md)
stopped after ten BeyondDB cases when eight-client TransactGetItems returned
a throttling cancellation. On the `9e17746` pin, GitHub Actions later passed
all seven server-binary restart tests and 47 of 48 peer-network tests. On the
new `30671d5` pin, the first GitHub qualification run passed all 46 elastic
tests, 46 of 48 peer-network tests, and six of seven server-binary tests.
Concurrent-delete placement, a two-owner large binary transaction read, and
an oversized same-Cell read failed. Later qualification runs are recorded above; the
new pin remains incompletely qualified.

## Refresh of the earlier high-throughput sample

The `1,488.6/1,903.2` GetItem and `81.2/176.2` PutItem requests/s figures previously quoted in PR #14 were reported with commit `880b4aa` from a then-fresh local fixture. We could not locate its raw benchmark JSON. They are historical observations, not verified current-release rates.

On September 29, 2026, two fresh four-partition RustFS fixtures ran the `af8fab7` release binary with `auth_cache_enabled: true`, 64 seeded items carrying 1 KiB payloads, signed boto3, five-second cases, and SDK retries disabled. In the complete run, GetItem measured **164.13/549.53 requests/s** at one/eight clients, PutItem **10.13/31.56**, and TransactWriteItems **0.74/2.10**. Its 24 cases had no SDK request errors. The other run stopped at eight-client TransactGetItems after eight read timeouts. Both servers logged deferred background work from Cell mailbox-byte exhaustion. Other virtual machines and Rust builds drove the 12-logical-CPU host's load average above 30 during the test.

This refresh does not reproduce the earlier rates and is not a clean matched regression test: the code revision and host load differ. The [raw results and fixture details](../benchmarks/2026-09-29-claim-refresh/README.md) provide the full API table, latencies, errors, and conditions. Repeat on an otherwise idle host with retained raw results and background-work checks before using either sample as a performance target.

A later release check with an opt-in persistent follower store also left the
serving path on RustFS publication. It measured GetItem at **272.61/768.80**
requests/s and PutItem at **10.91/45.55** at one/eight clients. All 24 cases
had zero SDK request errors, but the server logged six deferred background
operations and the host was heavily loaded. The [complete API table and raw
results](../benchmarks/2026-09-29-follower-receiver-check/README.md) show that
the historical peaks still are not reproducible on this fixture. Enabling the
receiver alone does not change write durability or imply a throughput gain.

## Contemporaneous comparison with pinned ExtendDB SQLite

A later pair of fresh release fixtures exercised all 12 benchmark APIs with the same signed boto3 workload. The [full comparison and raw results](../benchmarks/2026-09-29-sqlite-comparison/README.md) show BeyondDB near SQLite on warm `GetItem` (583 versus 646 requests/s at one client), but far behind on `PutItem` (9 versus 811) and cross-partition `TransactGetItems` (1.5 versus 572). Eight-client `Scan` and one-client `BatchGetItem` were the only cases in which BeyondDB exceeded SQLite. Both backends had zero SDK request errors; BeyondDB also logged deferred background work.

These sequential cases ran while other virtual machines consumed CPU, and host load changed between fixtures. ExtendDB's SQLite development mode uses open authorization and local-file durability, while BeyondDB verified IAM and waited for RustFS publication. The result identifies the remaining work; it does not establish a controlled throughput ratio or a production capacity target.

A separate [direct RustFS PUT probe](../benchmarks/2026-09-29-rustfs-put-probe/README.md)
measured object-store calls without the DynamoDB or Cell request path. It ran
under even higher host load, so its rates are diagnostic. It reinforces the
need to measure publication I/O and evaluate Cellule's follower durability
path before expecting local SQLite write latency from this RustFS fixture. The
[follower durability design](follower-durability.md) lists the BeyondDB
integration and recovery gates required before that mode can be benchmarked.

## What a request waits for

```text
signed AWS SDK request
    │
    ├─ credential Cell read (fresh for revocation)
    ├─ IAM policy and boundary Cell reads
    ├─ table record Cell read
    ├─ live route anchor + directory leaf reads
    ├─ item route anchor + directory leaf reads
    └─ data Cell read, or one mutation and durable publication
                                              └─ object store
```

With the default `auth_cache_enabled: false`, the server has no process-wide credential or authorization-result cache. It keeps a positive in-memory proof that a credential Cell exists, while it still reads the credential record for every signed request. Table metadata and authorization use pass-through stores so concurrent deletion, recreation, and policy changes are observed. Item routing reads the published directory; writes also wait for durable publication. These choices protect correctness but add work compared with an embedded SQLite test server. The route anchor and leaf can be read concurrently because the anchor remains the authority for whether a route is published.

For a workload that accepts bounded cross-node visibility, set `auth_cache_enabled` to `true` in the node configuration. This enables ExtendDB's 60-second stale-while-revalidate credential, IAM, and table metadata caches and wires local management invalidation through `AuthCacheRegistry`. It also caches immutable catalog proofs and resident local Cell handles for 500 ms. After expiry, the runtime can resolve the resident actor without provider reads. Cached handles are checked against the current resident actor before reuse. Handles still fence drained owners; remote routing, admission and recovery use exact authority checks. Keep the flag disabled when immediate remote credential revocation or table recreation visibility is required.

The same opt-in mode caches positive `DescribeTable`, `ListTables`, and backend table-key metadata for 500 ms, with at most 128 entries of each type per node. Batch APIs call the backend metadata lookup directly, so this cache also removes repeated account-Cell queries on that path. Local table creation, deletion, and update invalidate these entries as soon as the durable command completes. A remote node's table change can remain absent from a cached response until its entry expires. Item reads and writes still reach their owning Cell.

On a fresh release fixture, the metadata cache measured 489/571 `DescribeTable` and 583/786 `ListTables` requests/s at one/eight clients, compared with 285/384 and 215/472 in an earlier BeyondDB fixture. A nearby SQLite fixture reached 835/1,196 and 701/343 respectively; its eight-client listing rate fell under host contention. The [raw metadata sample](../benchmarks/2026-09-29-metadata-cache/README.md) records the conditions. These short runs show an improvement in the cached path, not consistent SQLite parity.

## Release comparison: one local node

Two ten-second repetitions used the same Mac14,13 host (12 logical CPUs, 32 GiB RAM), boto3 client, signed requests, no SDK retries, 64 seeded 1 KiB items, strongly consistent `GetItem`, and unique-key `PutItem`. BeyondDB was built from `71c31f1` with `cargo build --release --locked`, used four initial partitions, and published to a local RustFS container over S3. ExtendDB was built at pinned revision `7eaa89b` with `sqlite,dev-mode` features and used a file-backed SQLite database. Runs were sequential; the SQLite server's development authorization mode differs from BeyondDB's IAM path.

| API, clients | BeyondDB requests/s, two runs | ExtendDB SQLite requests/s, two runs | BeyondDB p95 latency | SQLite p95 latency |
| --- | ---: | ---: | ---: | ---: |
| GetItem, 1 | 26.0 / 24.3 | 1,388.5 / 1,341.0 | 66.7 / 68.3 ms | 1.06 / 1.13 ms |
| GetItem, 8 | 128.9 / 127.8 | 1,611.6 / 1,682.6 | 88.5 / 95.5 ms | 9.35 / 8.91 ms |
| PutItem, 1 | 18.0 / 18.6 | 866.7 / 926.5 | 100.1 / 86.5 ms | 1.71 / 1.52 ms |
| PutItem, 8 | 57.9 / 63.9 | 1,137.6 / 1,062.1 | 269.1 / 220.3 ms | 10.57 / 10.95 ms |

All four workloads had zero request errors in both repetitions. A separate direct-to-RustFS run with a 16 GiB local disk budget yielded 27.1 GetItem/s and 19.6 PutItem/s at one client; increasing that budget alone did not close the gap. These are closed-loop samples on a local fixture, not maximum sustainable throughput, production latency, or a comparison of equal durability contracts.

After overlapping the independent live-route anchor and directory-leaf reads, a fresh five-second smoke sample on the same release path measured 32.8 GetItem/s at one client (p95 38.5 ms) and 160.8 GetItem/s at eight clients (p95 61.2 ms), with zero errors. The sample is shorter than the comparison above and should be repeated for qualification; it records the direction and size of this request-path improvement, not a new capacity target.

An opt-in cache sample (`auth_cache_enabled: true`) on a fresh 16 GiB RustFS fixture measured 112.9 GetItem/s at one client (p95 11.7 ms), 471.3 GetItem/s at eight clients (p95 23.7 ms), 41.3 PutItem/s at one client (p95 41.1 ms), and 103.8 PutItem/s at eight clients (p95 169.8 ms). All requests succeeded. This is a warm-cache result with a 60-second cross-node visibility window; it remains below the file-backed SQLite baseline and must not be read as a fleet capacity claim.

The transaction path overlaps coordinator publication, route discovery, payload reads, participant prepares, and prepare evidence with bounded concurrency. A fresh five-second release sample from the current transaction path completed `TransactGetItems` at 3.04 requests/s with one client (p95 422 ms) and 7.08 requests/s with eight clients (p95 1.41 s). `TransactWriteItems` reached 1.73 requests/s with one client (p95 1.34 s) and 1.51 requests/s with eight clients (p95 6.38 s). The eight-client cases contend on a single account coordinator and use the same closed-loop host, so they show contention behavior rather than a capacity target. All transaction requests succeeded. Transaction throughput remains far below the SQLite baseline and needs coordinator and durable-publication work before it is suitable for large-scale application workloads.

The server now sizes Cellule's SQL worker pool from host parallelism (capped by Cellule at sixteen workers) instead of pinning every node to four workers. On a separate clean RustFS fixture, this configuration measured 117.3 `GetItem` requests/s at one client (p95 10.3 ms) and 559.0 requests/s at eight clients (p95 18.3 ms), 47.6 `UpdateItem` requests/s at one client (p95 35.0 ms) and 121.2 requests/s at eight clients (p95 143.7 ms), and 19.9 `BatchWriteItem` requests/s at one client (p95 102.2 ms) and 59.7 requests/s at eight clients (p95 218.7 ms). `TransactWriteItems` measured 1.56 requests/s at one client and 1.74 requests/s at eight clients; a concurrent eight-client `TransactGetItems` case hit one `ThrottlingError`. These results show better parallel point and batch work on this host, while the transaction bottleneck remains.

When `auth_cache_enabled` is enabled, routed table directory leaf pages are cached by account and table generation. Large directories can retain up to 64 partial pages instead of caching only a complete page. The owning data Cell still checks the cached epoch, and stale or split routes invalidate the entry; this keeps route changes safe while avoiding a directory traversal on steady-state point operations. The local handle cache is 500 ms. An earlier release fixture measured `GetItem` at 1,323.6 requests/s with one client (p95 1.4 ms) and 1,776.6 requests/s with eight clients (p95 7.4 ms), `Query` at 1,293.9/1,722.2 requests/s (p95 1.4/7.6 ms), `PutItem` at 62.6/120.8 requests/s (p95 25.4/155.2 ms), and `UpdateItem` at 54.4/103.6 requests/s (p95 68.0/170.1 ms). All cases had zero errors. In that earlier run, reads exceeded the one-client SQLite samples; durable writes remained slower because each mutation waited for publication.

Increasing the local handle cache from 50 ms to 500 ms reduces repeated authority resolution inside a transaction. On a fresh four-partition fixture, signed `TransactWriteItems` reached 4.46 requests/s at one client (p95 306 ms) and 1.55 requests/s at four clients (p95 2.87 s), with zero errors. An eight-client run reached 2.02 requests/s before one `ServiceUnavailable`; the coordinator and durable participant Cells still contend under concurrency. The longer cache remains safe for owner fencing because each resident `CellHandle` rejects drained ownership. This historical sample predates the resident-actor resolution path.

The same earlier run measured `Scan` at 1,101.1/1,660.0 requests/s, `BatchGetItem` at 857.2/1,498.2 requests/s (1,714.3/2,996.4 items/s), `BatchWriteItem` at 33.8/64.3 requests/s (67.5/128.5 items/s), `DescribeTable` at 919.0/1,474.0 requests/s, and `ListTables` at 1,457.8/1,725.0 requests/s for one/eight clients. Transactions reached 4.17/4.95 `TransactGetItems` requests/s and 1.27/1.53 `TransactWriteItems` requests/s; all cases completed without request errors, but transaction latency and durable-write throughput remain the limiting gap.

The no-return mutation path now avoids fetching and encoding an old image for an unconditional `PutItem` or `DeleteItem`, and avoids cloning and returning the old image when `UpdateItem` does not request return values. Stream-enabled tables and conditional requests still read the previous image when the protocol requires it. The `880b4aa` report described a fresh release fixture with the same 12-logical-CPU host, four initial partitions, local RustFS, 1 KiB items, signed boto3 requests, and zero errors. In that report, `PutItem` measured 81.2/176.2 requests/s at one/eight clients (p95 15.0/102.3 ms), up from 62.6/120.8 in the preceding handle-cache sample. `UpdateItem` measured 65.4/132.4 requests/s (p95 57.5/134.6 ms), up from 54.4/103.6. The broader sample measured `Scan` at 1,159.5/1,806.9 requests/s, `BatchGetItem` at 898.2/1,602.7 requests/s, `BatchWriteItem` at 32.9/72.3 requests/s, and metadata at 1,028.4–1,957.0 requests/s. `TransactGetItems` reached 4.78/10.92 requests/s and `TransactWriteItems` 2.45/1.55 requests/s; all requests succeeded, but transaction latency remained 209–3,652 ms. That report attributed the write gain to the no-return path. The figures are local comparison measurements rather than capacity claims, and the refreshed run above did not reproduce them.

The transaction completion path now reuses the coordinator status it already read instead of issuing a second identical status query before participant resolution. On the same fresh release fixture, `TransactWriteItems` measured 1.19 requests/s with one client (p95 1.28 s) and 1.39 requests/s with eight clients (p95 6.69 s), with zero errors. `TransactGetItems` measured 4.55/4.36 requests/s (p95 276/4,048 ms) for one/eight clients. The write improvement is a reduction in coordination overhead, not a change to the durable two-phase protocol; transactions remain far below the file-backed SQLite baseline.

Unconditional no-return mutations now use a 1 MiB input and 64 KiB result envelope, and successful no-return updates return a completion marker instead of serializing the updated item. Conditional mutations keep the full envelope so a condition failure can still carry the previous image. This reduces the per-request Cell mailbox reservation from roughly 8 MiB to roughly 1 MiB for the common write path. A fresh signed eight-client release smoke run completed 2,679 `PutItem`, 1,949 `UpdateItem`, and 1,044 `BatchWriteItem` requests in 20 seconds with zero request errors; the run recorded 133.64, 97.06, and 51.87 requests/s respectively. These are closed-loop observations on one local fixture, not a production capacity claim.

Small item images up to the 256 KiB storage chunk now use one bounded SQLite blob update instead of allocating a zeroblob, looking up its rowid, and issuing a chunk write. The isolated signed 1 KiB fixture measured `PutItem` at 52.16/112.86 requests/s and `UpdateItem` at 53.66/93.95 requests/s for one/eight clients, with zero errors. This removes local SQL work; the remaining write latency is still dominated by durable Cell publication to the object store.

The write path also stores serialized images up to 256 KiB directly in the primary item row. Larger images keep the chunked storage path. This avoids the follow-up blob update for the common small-item case while preserving the same read and recovery format; it does not change the durability boundary or the performance caveat above.

The high-concurrency fixture also logged deferred background maintenance and transaction-recovery work when the runtime resource ledger filled. That does not invalidate the completed API calls, but it is additional evidence that these numbers are a comparison sample rather than a sustainable capacity target.

Small transaction payloads now travel inline in the durable phase command, avoiding a separate upload command; payloads near the transfer limit still use the multipart path. A clean fixture measured `TransactGetItems` at 3.37 requests/s with one client (p95 362 ms). The corresponding eight-client run encountered service-unavailable errors under contention, and `TransactWriteItems` measured 1.29 requests/s with one client and 1.43 requests/s with eight clients. This removes avoidable round trips but does not yet close the transaction gap; treat the high-concurrency cases as failure evidence, not capacity results.

Same-Cell transactions now use one atomic Cell command after routing. This removes the coordinator and participant prepare/resolve round trips when every operation belongs to one account or one installed data Cell; requests that span Cells, or that carry an idempotency token, keep the durable coordinator protocol. On a fresh one-partition fixture with two 1 KiB items per request, signed boto3, local RustFS, and zero request errors, `TransactGetItems` measured 55.28 requests/s at one client (p95 77.49 ms) and 63.08 requests/s at eight clients (p95 202.39 ms). `TransactWriteItems` measured 6.04 requests/s at one client (p95 214.67 ms) and 1.45 requests/s at eight clients (p95 6.30 s). The eight-client write result is contention on one durable Cell, while the one-client result shows the remaining Cell commit cost; these numbers are a targeted fast-path sample, not a fleet capacity target. A four-partition random-key workload still sends most two-item requests through the coordinator.

Same-Cell `TransactGetItems` now uses a Cellule read-only query instead of a mutation command. The query runs the lock checks and item reads on the Cell worker without writing `sys_requests` or publishing an LTX for a read-only batch; the worker serialization preserves the single-Cell snapshot boundary. On a fresh one-partition release fixture with two 256-byte items per request, signed boto3, local RustFS, and zero errors, this measured 577.49 requests/s at one client (p95 2.86 ms) and 1,134.32 requests/s at eight clients (p95 11.82 ms), or 1,154.97 and 2,268.64 items/s. The preceding same-Cell command path measured 55.28 and 63.08 requests/s, so this removes the read publication cost. The optimization applies only when routing proves one participant; cross-Cell reads still use the durable coordinator protocol, and writes still require durable publication.

Cross-Cell read cleanup now records all participant release receipts in one coordinator command after the participant Cells have durably released their images. On a fresh four-partition fixture, random two-item signed SDK `TransactGetItems` measured 6.09 requests/s at one client (p95 268 ms) and 4.52 requests/s at eight clients (p95 6.46 s), with zero errors. The preceding equivalent run measured 5.58/3.43 requests/s and returned one eight-client `ServiceUnavailable`; this is a targeted stability and round-trip reduction, not evidence of SQLite-parity transaction capacity.

Cross-Cell prepare and terminal-resolution receipts now use bounded coordinator batch commands after participant Cell work completes concurrently. A fresh four-partition fixture measured `TransactGetItems` at 5.71/4.28 requests/s for one/eight clients and `TransactWriteItems` at 1.17/1.26 requests/s, with zero errors. This removes one coordinator command per participant while retaining replay checks; the new fixture does not show a throughput increase, and durable participant publication remains the transaction bottleneck.

The route-cache lock path is now synchronous: the process-local route and account-placement maps use one short `std::sync::RwLock` critical section instead of awaiting Tokio locks for every routed request. A matched five-second comparison used the previous 500 ms-handle-cache release and the current release, fresh four-partition tables, local RustFS, signed boto3 requests, 256-byte items, 64 seeded keys, and one/eight clients. Every case completed without errors. The current release improved eight-client `Scan` from 1,249 to 1,457 requests/s (+16.6%), `Query` from 1,430 to 1,509 (+5.5%), and `BatchGetItem` from 1,195 to 1,244 (+4.1%); one-client `Query` improved 11.1%. `PutItem` improved 12.4–13.4% (51.7→58.2 and 95.3→108.1 requests/s), while `BatchWriteItem` and `TransactWriteItems` changed by less than measurement noise (−2.2% to +3.5% and −5.4% to +2.9%). Durable publication remains the dominant write and transaction cost, so this optimization does not establish SQLite parity or a production capacity target.

The node now exposes `max_active_cells` instead of fixing the runtime admission ceiling at 64. A clean 64-partition table could be provisioned with `max_active_cells: 128`, whereas the previous ceiling left the table in `CREATING` after capacity exhaustion. On the 64-partition fixture, random-key `PutItem` reached 20.4/48.3/58.8 requests/s at 1/8/32 clients and fell to 46.8 at 64 clients; `UpdateItem` reached 14.3/14.1/54.8 requests/s at 1/8/32 clients, and the 64-client run returned five `ServiceUnavailable` errors. More resident Cells remove the admission ceiling but do not remove the 16-worker and durable-publication bottlenecks, so this is a capacity-control improvement rather than evidence of SQLite-parity throughput.

The serving binary now accepts an optional `sql_workers` override. Without it, Cellule derives the worker count from host parallelism and caps it at sixteen; setting it explicitly is useful on a multi-partition node when the host has spare CPU and memory. Workers own their SQLite connections and improve scheduling between independent Cells, but each Cell still executes commands in order and publishes one durable root at a time. This knob therefore addresses partition fan-out and worker undersizing, not the per-Cell object-publication ceiling. Record the chosen value with benchmark results and verify resident memory before increasing it.

A matched exploratory comparison on fresh four-partition RustFS fixtures (256-byte items, signed boto3, five-second cases) did not show a consistent write gain from forcing sixteen workers over the host-derived twelve: `PutItem` was 29.4/79.5 versus 38.2/62.1 requests/s at 1/8 clients, `UpdateItem` was 35.4/41.0 versus 28.8/75.7, and `BatchWriteItem` was 9.9/15.9 versus 13.7/26.0. The fixtures are too short to establish a capacity target; retain the override for measured multi-Cell workloads, not as a general single-Cell write optimization.

A fresh release sample on 2026-09-29 measured the current no-return delete path on a four-partition RustFS fixture with `max_active_cells: 128`, `sql_workers: 12`, `auth_cache_enabled: true`, signed boto3 requests, 256-byte items, and five-second cases. All requests completed without errors. `PutItem` reached 15.0/22.7 requests/s at 1/8 clients (p95 94.9/611.0 ms), `UpdateItem` 15.5/20.6 (p95 106.4/780.3 ms), and unconditional absent-key `DeleteItem` 15.9/21.7 (p95 93.5/776.7 ms). `BatchWriteItem` reached 7.4/10.6 requests/s, or 14.9/21.2 items/s, at 1/8 clients (p95 192.8/1,152.5 ms). The delete case used a unique key that was absent, so it exercises the no-return envelope without an old image. This run confirms zero-error behavior for the release binary and the reduced delete work, while durable publication remains the dominant cost and these write rates remain below the file-backed SQLite baseline.

A 64-partition release fixture after this change reached 11.9/19.6/23.4/21.1 `PutItem` requests/s at 1/8/32/64 clients and 11.4/21.3/19.2/19.2 `UpdateItem` requests/s, all with zero errors. The result did not materially improve on the previous 64-partition run, which confirms that directory traversal is not the dominant write cost at this scale; Cell command publication and its object-store round trips remain the next optimization boundary.

The routed no-return path now coalesces a bounded burst of unconditional `PutItem`, `UpdateItem`, and `DeleteItem` requests for the same partition into one partition-local durable command. The batcher waits up to 2 ms for a partial queue, flushes a full queue immediately, accepts at most 16 distinct keys, and keeps conditional, old-image, account-local, and coordinator transaction requests on their existing paths. On fresh four-partition RustFS fixtures with 256-byte items, signed boto3, `max_active_cells: 128`, `sql_workers: 12`, and zero errors, the short baseline/current comparisons at one/eight clients were: `PutItem` 14.1/22.4 versus 12.9/36.4 requests/s, missing-key `DeleteItem` 13.6/20.3 versus 14.3/32.5, `UpdateItem` 13.7/18.6 versus 14.1/20.3, and two-item `BatchWriteItem` 13.8/19.7 versus 13.0/35.0 items/s. The short single-client samples vary with object-store timing; the larger concurrent gains come from sharing one publication across requests. Update expressions still spend more time in SQLite/index work, so their gain is smaller. This is a local-node optimization, not a fleet capacity claim.

The pinned ExtendDB `BatchWriteItem` handler awaits each item mutation in a request before starting the next. This means a single two-item request still incurs two durable publications when both items are handled individually. The no-return batcher can combine mutations arriving from *different concurrent requests*, but it cannot combine items that ExtendDB submits sequentially within one request. This is a separate bottleneck from the batcher's queue window; improving it requires a batch-aware protocol path that preserves ExtendDB's validation and result semantics.

Temporary stage timings on a separate instrumented fixture put typical credential, IAM, table record, and data Cell reads around 3–4 ms each, while each route traversal took around 6–7 ms. The requests perform several of these operations in sequence. The instrumented write fixture differed materially from the clean fixture, so its write timings are not a publication-cost estimate. A temporary S3 proxy disrupted publication and its counts were discarded.

## Observe runtime costs and durability acknowledgements

For a diagnostic run, add an absolute path to the server's JSON configuration:

```json
{
  "runtime_metrics_file": "/var/lib/beyonddb/runtime-metrics.json"
}
```

The parent directory must exist. Give each process its own file path. The server
refreshes the file once per second through a temporary file and atomic rename.
This option is disabled by default; enabling it adds atomic counter updates and
a periodic snapshot task. A write failure logs a warning and sampling continues.

| Observation | What it measures |
| --- | --- |
| `command_responses` | Runtime command replies using recorded results, follower proof (`fleet`), or object publication (`object`) |
| `command_queue`, `command_worker` | Command admission queue and worker execution time |
| `primitive_execution` | Command and query primitive execution time |
| `publication` | Queue, preparation, authority, total duration, and uploaded objects/bytes |
| `activation` | Ownership, resume, root opening, restore, and activation phases |
| `durability_submissions`, `follower_appends` | Follower submission outcomes and append acknowledgement/failure counts |
| `follower_append_phases` | Sender peer lookup and HTTP round trip; receiver fresh enrollment and durable store append |
| `catalog_reads`, `control_reads` | Reads observed by the runtime telemetry hooks |
| `node_resources` | Sampled active Cells, retained bytes, worker jobs, and unpublished log bytes |
| `object_store` | Logical storage operations across the configured provider, including routing and enrollment I/O |

Timing objects contain cumulative `count`, `failed`, `total_us`, and `max_us`.
Durations use microseconds. Counter snapshots are approximate because work can
continue during sampling. They include background commands; runtime command
reply counts are **not SDK request counts** and exclude queries, transport, and
abandoned replies. Catalog/control counters do not cover all application or
object-store reads. Application routing, peer metadata, and fresh bootstrap
provisioning can bypass these hooks: zero activation/catalog/control counters
do not imply zero work in those phases. These counters cannot provide p95 latency.

`object_store` adds fixed operation and outcome labels, byte totals, `started`,
and `in_flight` counts. It observes the shared provider used by the server, peer
directory, and provisioner. A read finishes when its body is consumed or dropped;
duration includes that lifetime, rather than measuring network time alone.
`failed` includes normal `not_found`, `conflict`, and `cancelled` outcomes, so it
is **not an SDK error count**. No object paths, tenant IDs, or credentials are
recorded. These counters also include startup and background work.

`follower_append_phases` observes Append only. Seal, Tail and Retire are excluded.
The sender's `round_trip` starts after peer lookup and includes the complete
bounded HTTP response body. The receiver's `enrollment` covers the fresh
mTLS-bound canonical read; `durable_append` covers `FollowerStore::append`,
including its durable acknowledgement. Neither receiver phase includes HTTP
body admission or response scheduling. Timings add `started`, `in_flight` and
`cancelled`; dropping an active future records a cancelled failure. They use
fixed labels with no node/session IDs. Sender and receiver observations cover
different, overlapping scopes: do not subtract their aggregate means to claim
network latency or add them into SDK latency.

Save snapshots before and after a run, check that
`first_snapshot_at_unix_ms` is unchanged, and check the freshness of
`sampled_at_unix_ms`. A server restart begins a new counter series. Divide the
change in `total_us` by the change in `count` to estimate a phase's mean duration.
Phases can overlap and cover different events; do not sum them into SDK latency.
Use the signed SDK harness for end-to-end latency and foreground errors.

## Run a repeatable point-operation sample

Build an optimized server with `cargo build --release`, start it using [the deployment guide](deployment.md), and create a dedicated table. The commands below use test credentials already configured for that server.

```sh
export BEYONDDB_ENDPOINT=http://127.0.0.1:18443
export AWS_DEFAULT_REGION=us-east-1
# Export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for your test account.

aws dynamodb create-table \
  --endpoint-url "$BEYONDDB_ENDPOINT" \
  --table-name PerfData \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

python3 -m pip install boto3
python3 scripts/bench.py \
  --endpoint "$BEYONDDB_ENDPOINT" \
  --table PerfData \
  --seconds 30 \
  --clients 1 4 16 64 \
  --output perf-results.json

# Run a broader API sample on a disposable table.
python3 scripts/bench.py \
  --endpoint "$BEYONDDB_ENDPOINT" \
  --table PerfData \
  --seconds 30 \
  --clients 1 4 \
  --operations get query scan batch_get transact_get \
    describe_table list_tables put update delete_missing \
    batch_write transact_write \
  --output perf-api-results.json
```

The script seeds 64 one-KiB items, signs requests through boto3, disables SDK retries, and reports completed requests per second, items per second, p50/p95/p99 latency, and errors for each client count. It stops after an error. `BatchGetItem`, `BatchWriteItem`, and both transaction cases use two items per request; `delete_missing` deletes a unique absent key. This distinction matters when comparing request rate with item rate. The supplied table needs a string `pk` key. For a table with a numeric sort key, also pass `--sort-key-name sk --sort-key-value 1`. Each run changes its data, so use a disposable table and declare whether Streams, indexes, TTL, and background workers are enabled.

## Interpret the result

The current [harness](../scripts/bench.py) sorts **successful** request durations and selects sample index `floor(p × (n - 1))`, then rounds to two decimals. It does not interpolate. With two successes, the reported p95 is the smaller duration; with eight, it is the second largest. Always read percentiles together with completion counts, actual elapsed time and errors. Low-count transaction cases do not establish a production tail-latency target.

Short, closed-loop runs are useful for comparing code changes under the *same* fixture. They are not a peak TPS rating. Report at least the build profile and Git revision; server and client CPU; object-store latency and request counts; table partitions; item size and key distribution; Streams/index settings; client concurrency; errors; p99; and recovery after owner loss. Test hot keys separately from evenly spread keys. Run sustained mixed read/write load, table creation, splits, transactions, and restarts before using a result for capacity planning. A healthy `/health` response alone does not prove that background work is keeping up.

Do not use a debug build or a node reporting mailbox exhaustion to set a capacity target. Fix readiness and background-work errors before comparing throughput.
