# Opt-in metadata response cache sample

A release binary built from `fef8da8` plus the metadata-cache working-tree
change ran against a fresh four-partition RustFS fixture. It used
`auth_cache_enabled: true`, signed boto3, 64 seeded items with 1 KiB payloads,
no SDK retries, and five seconds per case. The `DescribeTable` and
`ListTables` responses were cached for 500 ms on this node. Local
`CreateTable`, `DeleteTable`, and `UpdateTable` commits invalidate the cache;
a signed SDK integration test checked immediate visibility after create and
update. The four benchmark cases completed with zero SDK request errors and
no server warnings.

A fresh file-backed ExtendDB SQLite `7eaa89b` development-mode fixture ran
the same two operations and client counts. It verified SigV4 but used open
authorization. The earlier BeyondDB column comes from the full
[12-API comparison](../2026-09-29-sqlite-comparison/README.md) before this
cache change; it is a separate fixture, not a controlled A/B run.

| API | BeyondDB before, requests/s 1 / 8 | BeyondDB with cache, 1 / 8 | SQLite fresh, 1 / 8 |
| --- | ---: | ---: | ---: |
| DescribeTable | 285.22 / 384.20 | 488.70 / 571.05 | 835.34 / 1,196.07 |
| ListTables | 214.85 / 472.34 | 582.96 / 785.79 | 700.59 / 342.71 |

The one-minute host load average was 27.8 to 32.8 during the cache fixture
and 29.2 to 28.8 during the SQLite fixture on 12 logical CPUs. Other virtual
machines remained active. The cached metadata cases improved over the earlier
BeyondDB sample, but `DescribeTable` and one-client `ListTables` remained below
SQLite. The eight-client SQLite `ListTables` result fell sharply under host
contention and is not a reliable parity target. This is a short local
diagnostic, not sustainable throughput or a fleet sizing claim.

Raw results: [BeyondDB](beyonddb.json), [ExtendDB SQLite](extenddb-sqlite.json),
and [BeyondDB warnings](beyonddb-warnings.txt) (empty). Both fixtures used
the same `scripts/bench.py` call shape with
`--seconds 5 --clients 1 8 --payload-bytes 1024 --operations describe_table list_tables`.
