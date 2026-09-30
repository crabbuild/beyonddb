# Current release check with an opt-in follower store

This September 29, 2026 (Pacific time) sample checks the release binary built
from PR #14 after adding a persistent follower receiver. It does **not** test
follower-backed commit acknowledgments: the node advertises no follower
capacity, and the serving path still waits for RustFS publication. The binary
SHA-256 was `c75ecbcf8ad2277c5b6ee6356604fb427539edf21d6660ad5c2513a9d16431f3`.

The Mac14,13 host has 12 logical CPUs and was heavily loaded (one-minute load
observed at 22.8–25.8 around this fixture, rising above 30 during the run).
A fresh RustFS container used the pinned
`ghcr.io/rustfs/rustfs:1.0.0-glibc@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`
image. The server used four initial partitions, `auth_cache_enabled: true`,
the default 500 ms routed-page/local-handle caches, and a 1 GiB opt-in
`follower_store_bytes` budget. The benchmark used signed boto3 with zero SDK
retries, 64 seeded 1 KiB items, two items per batch or transaction request,
one/eight closed-loop clients, and five seconds per case. All 24 cases
completed with zero SDK request errors.

| API | Requests/s, 1 / 8 clients | p95 ms, 1 / 8 clients |
| --- | ---: | ---: |
| GetItem | 272.61 / 768.80 | 9.27 / 22.16 |
| Query | 447.11 / 688.26 | 4.85 / 23.72 |
| Scan | 284.74 / 619.05 | 7.60 / 25.33 |
| BatchGetItem | 210.19 / 480.45 | 10.95 / 33.19 |
| TransactGetItems | 1.31 / 3.35 | 1,250.76 / 3,470.13 |
| DescribeTable | 508.32 / 755.48 | 4.15 / 23.07 |
| ListTables | 623.49 / 620.23 | 3.38 / 28.34 |
| PutItem | 10.91 / 45.55 | 174.28 / 462.72 |
| UpdateItem | 18.97 / 31.03 | 98.15 / 558.94 |
| DeleteItem, absent key | 14.36 / 54.32 | 123.78 / 391.83 |
| BatchWriteItem | 6.04 / 21.95 | 265.89 / 687.50 |
| TransactWriteItems | 1.39 / 4.31 | 837.09 / 2,286.77 |

BatchGetItem moved 420.39/960.90 items/s and BatchWriteItem moved
12.07/43.91 items/s at one/eight clients. The server logged six deferred
background operations, including Cell mailbox-byte exhaustion and incomplete
transaction recovery. Zero foreground errors therefore do not establish
sustainable throughput. The host load and code revision differ from the old
1,488.6/1,903.2 GetItem sample, so this is another failed reproduction of
those peaks, not a controlled regression measurement.

The [raw results](run.json) contain every case, elapsed duration, count,
error count, and latency percentile. A signed AWS CLI smoke separately created
a table, wrote and read an item with the follower store configured, then
checked that item after a hard server restart. The follower-lane unit test
covers an authorized append and reopen of the store. Neither test proves that
the serving write path uses follower durability.
