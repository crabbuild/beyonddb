# Cellule `9e17746` release qualification attempt

This is a **failed, incomplete performance qualification**, retained to make the
result reproducible. BeyondDB was built with Cellule
`9e17746a633ca1046bd93866866074226091f81a`; ExtendDB SQLite remained at
`7eaa89b437feed0af0f05883d3f1493f86c6fc6d`. The BeyondDB binary SHA-256
and the exact pinned RustFS image are in [meta.json](meta.json). Both backends
used signed boto3 requests, a 1 KiB payload, 5 seconds per case, and one/eight
clients. BeyondDB used four initial partitions, 12 SQL workers, and the auth
cache. SQLite used its local file backend. See the fixture details in
[meta.json](meta.json) and [sqlite-meta.json](sqlite-meta.json).

The host had 12 logical CPUs. Load was **44.5** at pair start, **47.5** after
BeyondDB, and **90.4** after SQLite. Other builds and virtual machines were
active. These numbers cannot establish an intrinsic throughput difference or
the impact of the Cellule update.

| API | BeyondDB 1 / 8 clients (req/s) | SQLite 1 / 8 clients (req/s) |
| --- | ---: | ---: |
| GetItem | 125.85 / 283.41 | 260.56 / 482.32 |
| Query | 153.15 / 228.51 | 224.34 / 538.40 |
| Scan | 107.93 / 86.75 | 215.92 / 475.59 |
| BatchGetItem | 30.78 / 191.86 | 115.40 / 353.96 |
| TransactGetItems | 1.41 / 2.14 | 76.73 / 225.91 |

The eight-client TransactGetItems case completed 21 requests and returned one
`TransactionCanceledException` with a `ThrottlingError` cancellation reason.
The fail-fast harness then stopped, leaving **10 of 24 BeyondDB cases** in
[run.json](run.json). SQLite completed **24 of 24 cases with zero request
errors** in [sqlite-run.json](sqlite-run.json). Full p50/p95/p99 latency,
elapsed time, and error fields are in those raw files. The harness now offers
`--continue-on-error` so the next overloaded attempt can collect all cases and
still exit unsuccessfully when any request fails.

The separate signed release smoke check wrote a 1 KiB item, killed the serving
process, and read the same item after restart. All 17 library tests, formatting,
and strict Clippy passed. The broader ignored SDK restart test failed earlier
in GSI setup with HTTP 503 after Cell SQL deadlines on a host with load near
100. That failure remains unresolved. Neither this attempt nor the smoke check
proves the all-API SQLite performance objective.

Run control, server warnings, and raw case output are in
[pair-status.json](pair-status.json), [server.log](server.log), and
[bench.log](bench.log).
