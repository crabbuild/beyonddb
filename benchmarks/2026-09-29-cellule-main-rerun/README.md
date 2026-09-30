# Cellule `9e17746` release rerun

This is a complete **local diagnostic run**, not a production capacity or
controlled speed-ratio claim. BeyondDB used a release binary built with
Cellule `9e17746a633ca1046bd93866866074226091f81a`; the comparison used
file-backed ExtendDB SQLite at `7eaa89b437feed0af0f05883d3f1493f86c6fc6d`.
The binaries' SHA-256 values are in [BeyondDB metadata](meta.json) and
[SQLite metadata](sqlite-meta.json). BeyondDB published to the pinned RustFS
image recorded in its metadata.

Both fixtures used signed boto3 requests, 64 seeded items with 1 KiB payloads,
five seconds per case, one and eight clients, strong reads where the API
supports them, and SDK retries disabled. BeyondDB used four initial
partitions, 12 SQL workers, and `auth_cache_enabled: true`. The cases ran
sequentially on a 12-logical-CPU workstation. The host's one-minute load was
**47.5** at pair start, **30.3** after BeyondDB, and **37.3** after SQLite;
other builds and virtual machines were active. SQLite's local durability and
development authorization differ from BeyondDB's IAM checks and RustFS
publication. See [run control](pair-status.json) for exact times and load.

## Requests per second

| API | BeyondDB 1 | SQLite 1 | BeyondDB 8 | SQLite 8 |
| --- | ---: | ---: | ---: | ---: |
| GetItem | 61.01 | 279.56 | 235.04 | 507.24 |
| Query | 50.52 | 381.05 | 232.39 | 610.54 |
| Scan | 70.57 | 367.69 | 656.98 | 754.88 |
| BatchGetItem | 236.93 | 187.15 | 618.38 | 578.24 |
| TransactGetItems | 1.28 | 171.78 | 4.18 | 473.26 |
| DescribeTable | 491.35 | 258.86 | 493.66 | 408.94 |
| ListTables | 571.06 | 359.12 | 819.75 | 496.83 |
| PutItem | 22.57 | 92.52 | 77.61 | 54.71 |
| UpdateItem | 18.36 | 57.18 | 34.23 | 47.02 |
| DeleteItem, absent key | 13.04 | 85.32 | 15.93 | 246.95 |
| BatchWriteItem | 2.53 | 31.34 | 8.92 | 32.28 |
| TransactWriteItems | 0.30 | 16.97 | 1.50 | 131.07 |

Batch and transaction cases carry two items per request. For example,
BeyondDB's eight-client BatchGetItem result is **1,236.76 items/s**. The raw
[BeyondDB](run.json) and [SQLite](sqlite-run.json) JSON contain item rates,
completed requests, elapsed times, and all latency percentiles.

## p95 latency in milliseconds

| API | BeyondDB 1 | SQLite 1 | BeyondDB 8 | SQLite 8 |
| --- | ---: | ---: | ---: | ---: |
| GetItem | 68.03 | 9.81 | 109.67 | 30.47 |
| Query | 88.26 | 5.52 | 129.83 | 29.65 |
| Scan | 44.03 | 6.65 | 24.54 | 25.21 |
| BatchGetItem | 7.40 | 12.96 | 24.04 | 28.67 |
| TransactGetItems | 1,148.83 | 18.75 | 2,714.87 | 33.01 |
| DescribeTable | 3.71 | 10.40 | 28.67 | 45.37 |
| ListTables | 3.32 | 5.47 | 20.97 | 35.28 |
| PutItem | 77.21 | 30.00 | 288.42 | 311.05 |
| UpdateItem | 94.44 | 58.88 | 516.49 | 380.32 |
| DeleteItem, absent key | 136.76 | 38.59 | 1,509.38 | 104.49 |
| BatchWriteItem | 722.80 | 78.07 | 1,668.79 | 329.08 |
| TransactWriteItems | 1,812.97 | 126.79 | 5,995.43 | 170.65 |

All **24 BeyondDB** and **24 SQLite** cases completed with **zero foreground
SDK request errors**. BeyondDB's [server log](server.log) nevertheless records
two deferred capacity sweeps from Cell mailbox-byte exhaustion and one
deferred cross-Cell participant resolution. Foreground success does not prove
that background work settled.

BeyondDB exceeded SQLite in BatchGetItem and metadata reads at both client
counts, and in PutItem at eight clients on this run. It remained far behind in
transactions and most writes. The changing external load prevents attributing
any rate difference to the Cellule pin. The all-API SQLite performance
objective is **not met**.
