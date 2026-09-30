# Recovery wiring release benchmark rerun

On September 29, 2026 (Pacific time), a release build of BeyondDB at commit `1c38cca32251867236f9c11e09d5ae6fa3526cd4` plus the uncommitted recovery wiring (working-tree diff SHA-256 `276cf032a9bf92c3340d0d435ae5e0c91552170163f94affbe4c34b761fab469`) ran the same signed boto3 workload as pinned ExtendDB SQLite commit `7eaa89b437feed0af0f05883d3f1493f86c6fc6d`. The BeyondDB binary SHA-256 was `6210b02f6a0bd3d9c776da5c227a5818a8a55d798c873dc8e6fc777261cf5c89`. Both backends ran sequentially on the same 12-logical-CPU Mac.

The harness used fresh tables, 64 seeded 1 KiB items, five seconds per case, one and eight closed-loop clients, consistent reads, signed boto3 1.43.105 requests with zero retries, and two items per batch or transaction. `DeleteItem` targeted unique absent keys. Both backends completed all 24 cases with zero foreground SDK errors.

| API | BeyondDB req/s (1 / 8 clients) | SQLite req/s (1 / 8 clients) | BeyondDB p95 ms (1 / 8 clients) | SQLite p95 ms (1 / 8 clients) |
| --- | ---: | ---: | ---: | ---: |
| GetItem | 453.35 / 650.82 | 423.14 / 772.65 | 4.41 / 26.01 | 6.20 / 21.55 |
| Query | 225.86 / 522.46 | 348.79 / 615.96 | 11.12 / 33.69 | 6.85 / 26.94 |
| Scan | 211.00 / 673.24 | 386.09 / 649.88 | 12.93 / 24.91 | 6.05 / 25.70 |
| BatchGetItem | 164.66 / 519.08 | 253.78 / 641.22 | 14.31 / 34.41 | 10.14 / 26.85 |
| TransactGetItems | 1.26 / 4.32 | 182.98 / 541.11 | 1,081.00 / 2,627.54 | 14.52 / 32.02 |
| DescribeTable | 541.96 / 681.30 | 381.88 / 557.12 | 3.38 / 25.84 | 6.22 / 28.81 |
| ListTables | 518.50 / 753.05 | 305.57 / 860.62 | 3.80 / 23.04 | 7.48 / 19.48 |
| PutItem | 12.19 / 56.05 | 305.12 / 317.73 | 149.16 / 431.66 | 8.34 / 58.22 |
| UpdateItem | 17.56 / 28.87 | 184.41 / 349.01 | 122.85 / 588.63 | 13.50 / 50.13 |
| DeleteItem, absent key | 14.42 / 66.58 | 209.71 / 344.37 | 101.96 / 301.83 | 11.91 / 55.85 |
| BatchWriteItem | 8.72 / 16.25 | 96.72 / 159.27 | 224.60 / 1,004.40 | 24.59 / 107.01 |
| TransactWriteItems | 0.92 / 2.66 | 67.94 / 215.97 | 1,148.92 / 3,883.23 | 33.46 / 78.71 |

BatchGetItem completed 329.33 / 1,038.17 items/s in BeyondDB and 507.56 / 1,282.44 items/s in SQLite. BatchWriteItem completed 17.44 / 32.50 items/s in BeyondDB and 193.44 / 318.54 items/s in SQLite.

## Fixture and interpretation

- BeyondDB used four initial partitions, twelve SQL workers, 128 active Cell slots, `auth_cache_enabled: true`, a 1 GiB persistent follower-store budget, and fresh RustFS at the pinned `ghcr.io/rustfs/rustfs:1.0.0-glibc@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858` image. Follower-backed commit proof remains disabled; writes await object-store publication. IAM was verified.
- ExtendDB used file-backed SQLite `sqlite,dev-mode`. SigV4 was verified, while authorization was open in dev mode. Local-file durability and authorization therefore differ from BeyondDB.
- Host load averaged 26.03 to 28.55 during BeyondDB and 28.40 to 26.95 during SQLite on 12 logical CPUs. These heavily contended, sequential runs do not establish a stable causal speed ratio or production capacity.
- BeyondDB logged four deferred background operations due to Cell mailbox-byte exhaustion: two global-index projections, one capacity sweep, and one routed stream retention sweep. Zero foreground SDK errors do not prove sustainable throughput while background work falls behind.
- The BeyondDB binary includes uncommitted recovery wiring that does not enable follower-backed commits. This run is a functional and performance check of that exact working tree; it is not a benchmark of a clean release commit.

See [BeyondDB raw cases](run.json), [SQLite raw cases](sqlite-run.json), [BeyondDB metadata](meta.json), [SQLite metadata](sqlite-meta.json), and [BeyondDB warnings](server-warnings.txt).
