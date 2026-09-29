# September 29, 2026 performance claim refresh

These are raw results from two independent local RustFS fixtures. They test
whether the earlier rates quoted in PR #14 and `docs/performance.md` reproduce
on the current `af8fab7` release binary. We could not locate raw output from
the earlier `880b4aa` sample, so this is a refresh, not a replay of its
exact host state or code revision.

## Fixture

- Mac14,13: 12 logical CPUs, 32 GiB RAM. Other virtual machines and Rust
  builds were active. The one-minute host load average was 16.8 at the start
  of run 1 and 35.1 at the start of run 2, and rose above 30 during testing.
- A new pinned RustFS container and bucket for each run. Containers were
  stopped after the tests.
- BeyondDB `af8fab7`, `cargo build --locked --release --bin beyonddb`, four
  initial partitions, `auth_cache_enabled: true`, 12 SQL workers, 128 active
  Cell slots, and a 1 GiB node retained-byte budget.
- One PAY_PER_REQUEST table with a string `pk`, no indexes or Streams, 64
  seeded 1 KiB items, signed boto3 requests, and zero SDK retries.
- `scripts/bench.py` used five seconds per API and client count, testing one
  then eight clients. Batch and transaction calls contained two items.

## Outcome

| API | Run 2 requests/s, 1 / 8 clients | Run 2 p95 ms, 1 / 8 clients |
| --- | ---: | ---: |
| GetItem | 164.13 / 549.53 | 15.62 / 35.71 |
| Query | 164.70 / 475.40 | 16.67 / 43.78 |
| Scan | 119.55 / 413.38 | 21.44 / 52.36 |
| BatchGetItem | 85.75 / 323.10 | 40.10 / 54.50 |
| TransactGetItems | 0.48 / 2.32 | 2,294.76 / 4,399.31 |
| DescribeTable | 193.18 / 327.33 | 10.84 / 52.39 |
| ListTables | 202.16 / 624.79 | 7.71 / 26.95 |
| PutItem | 10.13 / 31.56 | 211.23 / 682.53 |
| UpdateItem | 7.51 / 11.95 | 201.03 / 1,518.74 |
| DeleteItem, absent key | 6.68 / 35.03 | 302.84 / 444.75 |
| BatchWriteItem | 4.66 / 20.98 | 364.55 / 858.28 |
| TransactWriteItems | 0.74 / 2.10 | 1,368.98 / 4,174.11 |

Run 2 completed all 24 cases with zero SDK request errors. Run 1 stopped at
eight-client `TransactGetItems` after eight SDK read timeouts; only its first
ten cases were recorded. Both server logs recorded three deferred background
operations caused by Cell mailbox-byte exhaustion. The completed run therefore
does not establish sustainable capacity. The high host load and code changes
since `880b4aa` prevent attribution of the lower rates to one cause.

See [run 1](run-1.json), [run 2](run-2.json),
[fixture metadata](summary.json), and the server warnings from
[run 1](warnings-run-1.txt) and [run 2](warnings-run-2.txt). The benchmark
invocation, after starting a fresh fixture and setting AWS credentials for the
server's bootstrap identity, was:

```sh
python3 scripts/bench.py \
  --endpoint http://127.0.0.1:8000 --table PerfData \
  --seconds 5 --clients 1 8 --payload-bytes 1024 \
  --operations get query scan batch_get transact_get describe_table list_tables \
    put update delete_missing batch_write transact_write \
  --output results.json
```

The `8000` endpoint is illustrative; the recorded runs used ephemeral local
ports. See [deployment configuration](../../docs/deployment.md) for fixture
setup.
