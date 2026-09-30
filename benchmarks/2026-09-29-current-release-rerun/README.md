# Current release rerun against ExtendDB SQLite

On September 29, 2026 (Pacific time), the BeyondDB release binary from
`7655c608cb7020836459502010aed787fc48bb1c` and the pinned ExtendDB
SQLite release from `7eaa89b437feed0af0f05883d3f1493f86c6fc6d` ran the
same signed `scripts/bench.py` workload, sequentially, on one Mac14,13 host.
The BeyondDB binary was rebuilt with `cargo build --release --locked --bin
beyonddb`; its SHA-256 is
`01c4842a804689f7705e3388f0450d3a3778ab2d259de9977abc2afdead536af`.
The ExtendDB binary reports version `0.1.12`, catalog `0.0.3 (sqlite)`, and
commit `7eaa89b`; its SHA-256 is
`dfc9cd868d2bc06b71fa9fbc460a074dca330cb8ceffaebf62d8d54bdb485e38`.

Both runs used fresh tables, 64 seeded items with 1 KiB payloads, boto3
`1.43.105`, zero SDK retries, one and eight closed-loop clients, and five
seconds per case. Batch and transaction requests carried two items. `GetItem`,
`Query`, `Scan`, and `BatchGetItem` requested consistent reads. `DeleteItem`
targeted a unique absent key. Each backend completed all 24 cases with zero
foreground SDK errors.

| API | BeyondDB requests/s, 1 / 8 clients | SQLite requests/s, 1 / 8 clients | BeyondDB p95 ms, 1 / 8 clients | SQLite p95 ms, 1 / 8 clients |
| --- | ---: | ---: | ---: | ---: |
| GetItem | 346.56 / 723.43 | 545.85 / 898.90 | 7.17 / 23.39 | 3.63 / 17.83 |
| Query | 385.50 / 748.14 | 504.04 / 764.27 | 6.16 / 22.84 | 3.60 / 21.40 |
| Scan | 322.35 / 654.17 | 454.04 / 791.91 | 5.82 / 23.02 | 5.08 / 19.97 |
| BatchGetItem | 149.32 / 390.22 | 432.05 / 784.41 | 17.46 / 46.55 | 4.87 / 19.94 |
| TransactGetItems | 1.34 / 5.62 | 577.47 / 1,108.67 | 1,021.77 / 1,894.27 | 3.24 / 14.07 |
| DescribeTable | 518.99 / 684.89 | 875.52 / 1,126.43 | 3.30 / 28.58 | 2.08 / 14.11 |
| ListTables | 584.98 / 896.89 | 783.11 / 939.65 | 3.29 / 19.73 | 2.59 / 18.17 |
| PutItem | 24.29 / 82.97 | 377.16 / 358.10 | 67.36 / 223.29 | 6.81 / 60.32 |
| UpdateItem | 21.00 / 51.32 | 104.34 / 268.37 | 94.74 / 344.00 | 25.88 / 79.00 |
| DeleteItem, absent key | 15.22 / 46.14 | 150.10 / 527.73 | 125.13 / 359.98 | 19.06 / 35.27 |
| BatchWriteItem | 6.20 / 27.95 | 122.42 / 221.15 | 322.46 / 518.20 | 23.70 / 96.72 |
| TransactWriteItems | 1.24 / 2.94 | 37.11 / 145.89 | 904.19 / 3,480.67 | 72.41 / 101.73 |

`BatchGetItem` completed 298.63/780.44 items/s in BeyondDB and
864.10/1,568.82 items/s in SQLite. `BatchWriteItem` completed 12.39/55.91
items/s in BeyondDB and 244.84/442.30 items/s in SQLite. ExtendDB SQLite
was faster in every API and client-count case in this rerun. The large
transaction gap and durable write gap remain open.

## Fixture and limits

- BeyondDB used four initial partitions, twelve SQL workers, 128 active Cell
  slots, `auth_cache_enabled: true`, a 1 GiB opt-in persistent follower-store
  budget, and a fresh RustFS bucket. RustFS used the pinned image
  `ghcr.io/rustfs/rustfs:1.0.0-glibc@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`.
  The follower receiver did **not** enable follower-backed commit proofs; the
  serving path still awaited object-store publication. BeyondDB verified IAM.
- ExtendDB used its file-backed SQLite `sqlite,dev-mode` release. Dev mode
  verified SigV4 and used open authorization. Its local-file durability and
  authorization contract therefore differed from BeyondDB's.
- The twelve-logical-CPU host was contended. The one-minute load average rose
  from 15.07 to 22.64 during BeyondDB and from 18.75 to 23.40 during SQLite.
  Other virtual machines were active, so the sequential numbers cannot prove
  a stable causal speed ratio or production capacity.
- BeyondDB logged four deferred background operations: three global-index
  projection deferrals and one capacity-sweep deferral, all reporting Cell
  mailbox-byte exhaustion. Zero SDK errors do not establish sustainable
  throughput while this background work falls behind.

See [BeyondDB raw cases](run.json), [SQLite raw cases](sqlite-run.json),
[BeyondDB fixture metadata](meta.json), [SQLite fixture metadata](sqlite-meta.json),
and the [BeyondDB server warnings](server-warnings.txt). The JSON contains
counts, elapsed time, throughput, item rates, and p50/p95/p99 for every case.
