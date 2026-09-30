# Index batching release benchmark rerun

On September 29, 2026 (Pacific time), a release build of BeyondDB at commit `1c38cca32251867236f9c11e09d5ae6fa3526cd4` plus uncommitted recovery wiring and bounded global-index projection ran against pinned ExtendDB SQLite commit `7eaa89b437feed0af0f05883d3f1493f86c6fc6d`. The BeyondDB source diff SHA-256 was `0d0d600c7d90a57485ee24001590b1068042f66fd0ebd96c15f2dd4110cf1244`; the release binary SHA-256 was `1ff8d4525c055bf89b2ffd5e05315c373ca3346b03e41147f48f63b0f522c73f`. Both builds ran sequentially on the same 12-logical-CPU Mac.

The signed boto3 1.43.105 harness used fresh tables, 64 seeded 1 KiB items, five seconds per case, one and eight closed-loop clients, consistent reads, zero SDK retries, and two items per batch or transaction. `DeleteItem` targeted unique absent keys. Both backends completed all 24 cases with zero foreground SDK errors.

| API | BeyondDB req/s (1 / 8 clients) | SQLite req/s (1 / 8 clients) | BeyondDB p95 ms (1 / 8 clients) | SQLite p95 ms (1 / 8 clients) |
| --- | ---: | ---: | ---: | ---: |
| GetItem | 170.02 / 616.17 | 349.73 / 518.20 | 16.66 / 29.26 | 9.66 / 35.57 |
| Query | 255.35 / 477.00 | 211.37 / 620.45 | 11.77 / 40.15 | 14.44 / 28.49 |
| Scan | 215.15 / 582.24 | 204.54 / 612.03 | 11.90 / 26.59 | 12.68 / 28.27 |
| BatchGetItem | 117.17 / 520.94 | 106.92 / 510.79 | 22.57 / 33.78 | 26.44 / 35.03 |
| TransactGetItems | 1.84 / 3.79 | 65.12 / 437.19 | 1,012.49 / 2,999.63 | 59.85 / 43.48 |
| DescribeTable | 540.04 / 754.54 | 68.53 / 400.34 | 3.77 / 23.36 | 34.68 / 44.01 |
| ListTables | 348.04 / 607.27 | 237.51 / 586.67 | 5.87 / 29.59 | 13.39 / 29.68 |
| PutItem | 13.35 / 74.99 | 113.02 / 456.42 | 154.15 / 240.39 | 19.08 / 37.85 |
| UpdateItem | 14.49 / 21.97 | 290.76 / 410.65 | 134.92 / 826.87 | 7.93 / 52.18 |
| DeleteItem, absent key | 14.08 / 71.90 | 322.06 / 781.15 | 119.25 / 266.14 | 7.64 / 21.47 |
| BatchWriteItem | 7.39 / 21.47 | 192.13 / 163.82 | 227.62 / 882.15 | 11.48 / 101.78 |
| TransactWriteItems | 1.58 / 3.22 | 122.88 / 262.70 | 643.43 / 2,843.73 | 25.32 / 86.60 |

BatchGetItem completed 234.34 / 1,041.88 items/s in BeyondDB and 213.84 / 1,021.58 items/s in SQLite. BatchWriteItem completed 14.78 / 42.94 items/s in BeyondDB and 384.26 / 327.64 items/s in SQLite.

## Fixture and limits

- BeyondDB used four initial partitions, twelve SQL workers, 128 active Cell slots, `auth_cache_enabled: true`, a 1 GiB follower-store budget, and a fresh RustFS container at the pinned `ghcr.io/rustfs/rustfs:1.0.0-glibc@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858` image. Follower-backed commit proof remains disabled, so writes await object-store publication. IAM was verified.
- ExtendDB used file-backed SQLite in `sqlite,dev-mode`. SigV4 was verified; authorization was open in dev mode. Its local-file durability and authorization differ from BeyondDB.
- One-minute host load was 27.04 to 21.58 during BeyondDB and 21.91 to 26.99 during SQLite, against 12 logical CPUs. These contended, sequential five-second cases cannot establish a stable speed ratio or fleet-scale capacity. The global-index batching change targets background projection; this workload did not create or query a GSI.
- BeyondDB logged one deferred transaction recovery pass: `cross-Cell participant resolution is incomplete`. Zero foreground SDK errors do not prove all background work has caught up. The transaction and write gaps remain substantial, and this run does not meet the performance objective.
- The source tree was uncommitted when the binary was built. This report describes that exact binary, not a clean release commit.

See [BeyondDB raw cases](run.json), [SQLite raw cases](sqlite-run.json), [BeyondDB metadata](meta.json), [SQLite metadata](sqlite-meta.json), and [BeyondDB warnings](server-warnings.txt).
