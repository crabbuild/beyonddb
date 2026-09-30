# Release comparison: BeyondDB and ExtendDB SQLite

On September 29, 2026 (Pacific time), the same Mac14,13 host ran two fresh,
sequential, five-second-per-case fixtures. Both used `scripts/bench.py`, signed
boto3, no SDK retries, 64 seeded items with a 1 KiB payload attribute, and
one/eight closed-loop clients. Batch and transaction operations used two items
per request. All 24 cases on each backend completed with zero SDK request
errors.

- ExtendDB revision `7eaa89b437feed0af0f05883d3f1493f86c6fc6d` ran its
  release `sqlite,dev-mode` binary with a fresh file-backed SQLite database.
  Dev mode verifies SigV4 but uses open authorization.
- BeyondDB revision `af8fab7` ran its release binary with four initial
  partitions, 12 SQL workers, 128 active Cell slots, a 1 GiB retained-byte
  budget, `auth_cache_enabled: true`, and a fresh RustFS container pinned to
  `ghcr.io/rustfs/rustfs:1.0.0-glibc@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`.
  BeyondDB verified IAM as well as SigV4.
- Host load was high and changed between fixtures: the one-minute average was
  14.5 at SQLite startup, 21.8 at SQLite completion, 23.7 at BeyondDB startup,
  and 28.8 at BeyondDB completion on 12 logical CPUs. Other virtual machines
  were active. These are contemporary diagnostics, not a controlled capacity
  or causal comparison.

| API | SQLite requests/s, 1 / 8 clients | BeyondDB requests/s, 1 / 8 clients |
| --- | ---: | ---: |
| GetItem | 645.75 / 1,015.78 | 582.99 / 880.55 |
| Query | 662.96 / 841.20 | 538.11 / 536.53 |
| Scan | 572.81 / 737.67 | 281.28 / 787.87 |
| BatchGetItem | 352.52 / 903.73 | 375.54 / 631.17 |
| TransactGetItems | 572.18 / 937.65 | 1.50 / 4.76 |
| DescribeTable | 984.46 / 1,119.81 | 285.22 / 384.20 |
| ListTables | 1,089.09 / 1,357.85 | 214.85 / 472.34 |
| PutItem | 810.72 / 1,302.96 | 9.09 / 33.24 |
| UpdateItem | 734.55 / 1,272.89 | 10.40 / 28.57 |
| DeleteItem (missing) | 871.35 / 444.58 | 17.92 / 72.85 |
| BatchWriteItem | 75.96 / 166.93 | 9.82 / 35.11 |
| TransactWriteItems | 78.62 / 285.20 | 1.72 / 3.34 |

BeyondDB exceeded SQLite only for eight-client `Scan` and one-client
`BatchGetItem` in this sample. It stayed close on `GetItem`, but durable
mutations and cross-partition transactions remained far slower. Its server
logged eight deferred background operations, including Cell mailbox-byte
exhaustion and incomplete transaction recovery. Zero SDK request errors do
not establish sustainable capacity.

See the raw [SQLite results](extenddb-sqlite.json), [BeyondDB results](beyonddb-rustfs.json),
and [BeyondDB warnings](beyonddb-warnings.txt). Repeat on an otherwise idle
host with equal authorization policy and longer runs before drawing production
sizing conclusions. The two backends do not have equal durability contracts:
BeyondDB awaits published Cell state in RustFS for each durable write, while
ExtendDB SQLite writes to its local database file.
