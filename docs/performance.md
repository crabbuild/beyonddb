# Measure BeyondDB performance

BeyondDB does not yet have a qualified production throughput or latency target. The September 2026 single-node release comparison below shows a large gap from ExtendDB's file-backed SQLite backend. Do not use these numbers to plan a fleet. BeyondDB's request path and durability contract differ: an item write waits for Cellule to publish the committed Cell state to an object store.

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

For a workload that accepts bounded cross-node visibility, set `auth_cache_enabled` to `true` in the node configuration. This enables ExtendDB's 60-second stale-while-revalidate credential, IAM, and table metadata caches and wires local management invalidation through `AuthCacheRegistry`. It can remove several catalog reads from a warm request, but it does not remove the data Cell lookup or durable write publication. Keep it disabled when immediate remote credential revocation or table recreation visibility is required.

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

Temporary stage timings on a separate instrumented fixture put typical credential, IAM, table record, and data Cell reads around 3–4 ms each, while each route traversal took around 6–7 ms. The requests perform several of these operations in sequence. The instrumented write fixture differed materially from the clean fixture, so its write timings are not a publication-cost estimate. A temporary S3 proxy disrupted publication and its counts were discarded.

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
