# Measure BeyondDB performance

BeyondDB does not yet have a qualified production throughput or latency target. Earlier September 2026 single-node samples suggested that warm point reads could exceed the file-backed SQLite fixture, while durable writes and transactions remained slower. The refresh below did not reproduce those high read rates. Do not use these numbers to plan a fleet. BeyondDB's request path and durability contract differ: by default an item write waits for Cellule to publish committed state to object storage. Experimental follower durability can acknowledge a durable follower receipt while object tiering continues.

## Latest release verification

The [Cellule `70bd25f` release rerun](../benchmarks/2026-09-30-cellule-70bd25f/README.md)
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
correction happened after measurement. The peer credential lookup failure after
the explicit memory-exhaustion probe remains open. Two local process-suite attempts
failed owner recovery and subsequent fixture creation; Docker’s filesystem had
almost no free inodes. All eight process tests passed in CI. These records leave
full recovery qualification open.

### Previous instrumented release pair

The [instrumented release rerun](../benchmarks/2026-09-30-runtime-metrics/README.md)
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
by all direct dependencies and lockfile packages. The [fresh release rerun](../benchmarks/2026-09-30-follower-diagnostics/README.md)
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

The [combined coordinator-commit release pair](../benchmarks/2026-09-30-prepared-commit/README.md)
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
A [diagnostic follow-up](../benchmarks/2026-09-30-prepared-commit/diagnostics/README.md)
adds first-error logging and records two passing local follower process-kill
checks. The CI activation failure remains unresolved; these checks do not
change the benchmark results.

The preceding [compact transaction-read pair](../benchmarks/2026-09-30-compact-transaction-read/README.md)
recorded six transaction-read timeouts. That response codec keeps legal large
binary and escaped-string reads on the single-Cell query path, avoiding JSON
expansion into durable saved images. Signed remote-owner reads preserved exact
values without mutating the participant root; large binary reads also survived
an actual owner restart. Query codec versions are now 3; mixed-version peer
rollout is unqualified. Its full SDK CI failed with elastic Cells 46/46,
peers 50/52, and process tests 7/8. See both reports for raw measurements,
sample counts, CI links, and local evidence.

The preceding [peer owner-cache pair](../benchmarks/2026-09-30-peer-owner-cache/README.md)
recorded 19 timeouts under different load. Its signed mTLS regression proves
that the opt-in 500 ms private receiver cache removes repeated resident
handle authority reads, refreshes after expiry, and rejects unauthorized or
drained-owner requests. That focused proof does not establish end-to-end
SQLite parity.

## Earlier object-publication fixture

The [release repeat after the peer read fix](../benchmarks/2026-09-30-peer-read-fallback/README.md)
uses BeyondDB `832da2a`, Cellule `30671d5`, and pinned ExtendDB SQLite. Both
backends completed all 24 signed API/client cases with **zero foreground
errors**. At eight clients, BeyondDB/SQLite measured 857/778 `GetItem`,
16.9/746 `PutItem`, 1.33/576 `TransactGetItems`, and 1.20/371
`TransactWriteItems` requests/s. BeyondDB logged three deferred background
sweeps from mailbox capacity. RustFS used a fresh bind mount in Colima's
shared home directory after the Docker VM ran out of space; the preceding
[named-volume attempt](../benchmarks/2026-09-30-peer-read-fallback-attempt/README.md)
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
27.0 after SQLite. See the [full table, p95 latencies, fixture, and raw
JSON](../benchmarks/2026-09-30-cellule-main-rerun/README.md). These sequential
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

For a workload that accepts bounded cross-node visibility, set `auth_cache_enabled` to `true` in the node configuration. This enables ExtendDB's 60-second stale-while-revalidate credential, IAM, and table metadata caches and wires local management invalidation through `AuthCacheRegistry`. It also caches immutable catalog proofs and resident local Cell handles for 500 ms, so warm requests avoid repeated catalog and owner-resolution reads. The Cell handle still fences drained owners; authority is refreshed after the cache window. Keep it disabled when immediate remote credential revocation, table recreation visibility, or owner changes are required.

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

Increasing the local handle cache from 50 ms to 500 ms reduces repeated authority resolution inside a transaction. On a fresh four-partition fixture, signed `TransactWriteItems` reached 4.46 requests/s at one client (p95 306 ms) and 1.55 requests/s at four clients (p95 2.87 s), with zero errors. An eight-client run reached 2.02 requests/s before one `ServiceUnavailable`; the coordinator and durable participant Cells still contend under concurrency. The longer cache remains safe for owner fencing because each resident `CellHandle` rejects drained ownership; it bounds fresh authority discovery at 500 ms when the cache is enabled.

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
| `catalog_reads`, `control_reads` | Reads observed by the runtime telemetry hooks |
| `node_resources` | Sampled active Cells, retained bytes, worker jobs, and unpublished log bytes |

Timing objects contain cumulative `count`, `failed`, `total_us`, and `max_us`.
Durations use microseconds. Counter snapshots are approximate because work can
continue during sampling. They include background commands; runtime command
reply counts are **not SDK request counts** and exclude queries, transport, and
abandoned replies. Catalog/control counters do not cover all application or
object-store reads. Application routing, peer metadata, and fresh bootstrap
provisioning can bypass these hooks: zero activation/catalog/control counters
do not imply zero work in those phases. These counters cannot provide p95 latency.

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

Short, closed-loop runs are useful for comparing code changes under the *same* fixture. They are not a peak TPS rating. Report at least the build profile and Git revision; server and client CPU; object-store latency and request counts; table partitions; item size and key distribution; Streams/index settings; client concurrency; errors; p99; and recovery after owner loss. Test hot keys separately from evenly spread keys. Run sustained mixed read/write load, table creation, splits, transactions, and restarts before using a result for capacity planning. A healthy `/health` response alone does not prove that background work is keeping up.

Do not use a debug build or a node reporting mailbox exhaustion to set a capacity target. Fix readiness and background-work errors before comparing throughput.
