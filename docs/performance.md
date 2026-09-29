# Measure BeyondDB performance

BeyondDB does not yet have a qualified production throughput or latency target. The September 2026 single-node samples below show that warm point reads can exceed the file-backed SQLite fixture, while durable writes and transactions remain slower. Do not use these numbers to plan a fleet. BeyondDB's request path and durability contract differ: an item write waits for Cellule to publish the committed Cell state to an object store.

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

When `auth_cache_enabled` is enabled, a complete routed table directory page is also cached by account and table generation. The owning data Cell still checks the cached epoch, and stale or split routes invalidate the entry; this keeps route changes safe while avoiding a directory traversal on steady-state point operations. The local handle cache is 500 ms. A fresh current-release fixture measured `GetItem` at 1,323.6 requests/s with one client (p95 1.4 ms) and 1,776.6 requests/s with eight clients (p95 7.4 ms), `Query` at 1,293.9/1,722.2 requests/s (p95 1.4/7.6 ms), `PutItem` at 62.6/120.8 requests/s (p95 25.4/155.2 ms), and `UpdateItem` at 54.4/103.6 requests/s (p95 68.0/170.1 ms). All cases had zero errors. Reads now exceed the one-client SQLite samples; durable writes remain slower because each mutation waits for publication.

Increasing the local handle cache from 50 ms to 500 ms reduces repeated authority resolution inside a transaction. On a fresh four-partition fixture, signed `TransactWriteItems` reached 4.46 requests/s at one client (p95 306 ms) and 1.55 requests/s at four clients (p95 2.87 s), with zero errors. An eight-client run reached 2.02 requests/s before one `ServiceUnavailable`; the coordinator and durable participant Cells still contend under concurrency. The longer cache remains safe for owner fencing because each resident `CellHandle` rejects drained ownership; it bounds fresh authority discovery at 500 ms when the cache is enabled.

The same current-release run measured `Scan` at 1,101.1/1,660.0 requests/s, `BatchGetItem` at 857.2/1,498.2 requests/s (1,714.3/2,996.4 items/s), `BatchWriteItem` at 33.8/64.3 requests/s (67.5/128.5 items/s), `DescribeTable` at 919.0/1,474.0 requests/s, and `ListTables` at 1,457.8/1,725.0 requests/s for one/eight clients. Transactions reached 4.17/4.95 `TransactGetItems` requests/s and 1.27/1.53 `TransactWriteItems` requests/s; all cases completed without request errors, but transaction latency and durable-write throughput remain the limiting gap.

The no-return mutation path now avoids fetching and encoding an old image for an unconditional `PutItem`, and avoids cloning and returning the old image when `UpdateItem` does not request return values. On a fresh release fixture with the same 12-logical-CPU host, four initial partitions, local RustFS, 1 KiB items, signed boto3 requests, and zero errors, `PutItem` measured 81.2/176.2 requests/s at one/eight clients (p95 15.0/102.3 ms), up from 62.6/120.8 in the preceding handle-cache sample. `UpdateItem` measured 65.4/132.4 requests/s (p95 57.5/134.6 ms), up from 54.4/103.6. The broader sample measured `Scan` at 1,159.5/1,806.9 requests/s, `BatchGetItem` at 898.2/1,602.7 requests/s, `BatchWriteItem` at 32.9/72.3 requests/s, and metadata at 1,028.4–1,957.0 requests/s. `TransactGetItems` reached 4.78/10.92 requests/s and `TransactWriteItems` 2.45/1.55 requests/s; all requests succeeded, but transaction latency remained 209–3,652 ms. This change improves ordinary durable mutations; it does not make BeyondDB faster than SQLite for writes or transactions, and the samples remain local comparison measurements rather than capacity claims.

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
