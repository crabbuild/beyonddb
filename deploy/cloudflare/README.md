# Cloudflare benchmark deployment

Run three BeyondDB database Containers, each requesting **1 vCPU, 3072 MiB RAM
and 5000 MB ephemeral disk**. R2 holds Cellar's durable state. This fixture uses
one table with **30 ordered data ranges**, ten on each node. Metadata and
coordinator Cells share each node's resources.

| Path | Implementation |
| --- | --- |
| Public DynamoDB | HTTPS Worker → Container port 8080 → loopback ExtendDB listener |
| Private peers | Loopback TCP forwarders → secret-gated WSS Worker route → native Cellar peer port |
| Durability | Direct R2 S3 API; writes wait for bucket publication; follower durability disabled |
| Node lifecycle | Explicitly named Durable Objects; native Container API, with monitored process lifetime |
| Resource evidence | Cloudflare instance API allocation, guest/process observations and disk usage |
| Load generator | Workstation outside all three database-node budgets; WAN/Worker/bridge costs are included |

The bridge preserves native Cellar bytes and authentication. It does not interpret
or replay mutations. Public requests cannot reach the private gateway without the
separate tunnel secret. Keep the generated credentials, keys and raw reports in an
ignored private run directory. Do not mount R2 as a SQLite filesystem.

## Prepare

Use a reviewed **Linux x86-64 release binary** built with the committed lockfile.
Cloudflare requires `linux/amd64`; the earlier local ARM binary is unsuitable.
Docker must be available to Wrangler. Install client dependencies separately:

```sh
python3 -m venv benchmarks/cloudflare-venv
benchmarks/cloudflare-venv/bin/pip install -r scripts/perf/requirements-cloudflare.txt
```

Load the protected credential file into the shell environment; do not paste it
into commands, logs or Git. Provisioning expects `CF_ACCOUNT_ID`, `CF_API_TOKEN`,
`CF_ACCESS_KEY_ID`, `CF_SECRET_ACCESS_KEY` and `CF_S3_API_ENDPOINT`.
The API token needs permission to deploy Workers/Containers and create R2 buckets;
the S3 credentials must access the new dedicated bucket.

```sh
set -a
source "$HOME/.cf.env"
set +a
export CLOUDFLARE_ACCOUNT_ID="$CF_ACCOUNT_ID"
export CLOUDFLARE_API_TOKEN="$CF_API_TOKEN"

CF_PERF_PY="$PWD/benchmarks/cloudflare-venv/bin/python"
CF_PERF_RUN="$PWD/benchmarks/YYYY-MM-DD-cloudflare-benchmark"
"$CF_PERF_PY" deploy/cloudflare/provision.py \
  --run-dir "$CF_PERF_RUN" --binary /absolute/path/to/linux-amd64/beyonddb
```

Provisioning creates a dedicated R2 bucket and checks conditional create/update,
stale-write rejection and read-back. It saves source/binary/lockfile fingerprints,
private keys, Worker secrets and the generated Wrangler configuration. An existing
run directory is rejected. Preserve it after any failure; do not reinitialize it.

## Deploy and start

```sh
npm exec --yes --package=wrangler@4.151.2 -- wrangler deploy \
  --config "$CF_PERF_RUN/wrangler.json"
npm exec --yes --package=wrangler@4.151.2 -- wrangler secret bulk \
  "$CF_PERF_RUN/worker-secrets.json" --config "$CF_PERF_RUN/wrangler.json"

# Initial provisioning only. Later starts omit --bootstrap.
"$CF_PERF_PY" scripts/perf/cloudflare.py start \
  --run-dir "$CF_PERF_RUN" --node 1 --bootstrap
"$CF_PERF_PY" scripts/perf/cloudflare.py ready --run-dir "$CF_PERF_RUN" --node 1
```

Wait for signed `ListTables` to succeed through node 1 before starting nodes 2
and 3. Start those nodes without `--bootstrap`, then verify signed calls through
each node. A running Container is not proof that its database is ready.

```sh
"$CF_PERF_PY" scripts/perf/cloudflare.py start --run-dir "$CF_PERF_RUN" --node 2
"$CF_PERF_PY" scripts/perf/cloudflare.py start --run-dir "$CF_PERF_RUN" --node 3
"$CF_PERF_PY" scripts/perf/cloudflare.py ready --run-dir "$CF_PERF_RUN"
"$CF_PERF_PY" scripts/perf/cloudflare.py status --run-dir "$CF_PERF_RUN"
```

The Worker exposes only three fixed node names. The `durable_object` scheduling
policy has no `max_instances` configuration; the router never creates arbitrary
instance names. A western North America hint does not guarantee one datacenter;
record every node's actual location. The configured inactivity timeout is one
hour; a stopped node requires an explicit `start` without bootstrap.

## Prepare and measure

Run the local private tunnel forwarders in a separate terminal. They provide the
existing perf control protocol with access to the remote native peer ports:

```sh
"$CF_PERF_PY" deploy/cloudflare/agent.py --tunnels "$CF_PERF_RUN/secrets.json"
```

With all three signed endpoints ready, prepare once and then measure:

```sh
"$CF_PERF_PY" scripts/perf/cloudflare.py prepare --run-dir "$CF_PERF_RUN"
"$CF_PERF_PY" scripts/perf/cloudflare.py measure \
  --run-dir "$CF_PERF_RUN" --label main \
  --operations get query put transact_get transact_write \
  --clients 1 8 30 60 --seconds 60 --warmup 5
```

Preparation pauses native balancing for controlled first placement, installs the
table through normal DynamoDB `CreateTable`, seeds every ordered interval and
verifies authoritative owner inventories. The measurement reuses
[`run_case`](../../scripts/perf/bench.py): signed requests, uniform 1-KiB payloads,
strong point/range reads, two-item cross-node transactions and disabled SDK
retries. It keeps successful samples, error latencies, actual elapsed time,
per-range/ingress coverage, resource observations and acknowledged-write read-backs.

Use a fresh result label for another sweep. Do not rerun `prepare` over an existing
table or creation plan. If `CreateTable` times out, preparation observes the same
generation becoming active and verifies empty ranges before seeding; it never
resubmits creation. `resume-creation` refuses to replay a prior seed submission.
Failed/incomplete windows exit unsuccessfully while retaining their data;
failed mutations may have uncertain outcomes. Cloudflare guest CPU count and
memory totals can include platform overhead; use the instance API to verify the
requested allocation and retain guest observations separately.

Generate a report and optional chart from every retained sweep:

```sh
"$CF_PERF_PY" scripts/perf/cloudflare_report.py \
  --input "$CF_PERF_RUN/results-main.json" --output "$CF_PERF_RUN/report"
# Optional plotting dependency, kept outside the runtime image.
"$CF_PERF_PY" -m pip install -r scripts/perf/requirements-plot.txt
"$CF_PERF_PY" scripts/perf/plot.py \
  --input "$CF_PERF_RUN/report/combined-results.json" --output "$CF_PERF_RUN/report"
```

## Retain diagnostics and stop

```sh
"$CF_PERF_PY" scripts/perf/cloudflare.py logs --run-dir "$CF_PERF_RUN"
"$CF_PERF_PY" scripts/perf/cloudflare.py stop --run-dir "$CF_PERF_RUN"
```

Collect final inventories and logs before stopping: Container disk is ephemeral.
A clean signal drains BeyondDB; startup failures retain diagnostic access for ten
minutes. R2 data, deployment metadata and encryption keys must remain available
for recovery. A restart omits bootstrap and restores state through Cellar.
Never infer durable user-item recovery from a successful credential lookup alone;
read back acknowledged items after the relevant restart.

For a planned all-node cold restart, first retain final logs and inventories,
stop all three and observe them stopped before starting any node. Start without
`--bootstrap`, wait for signed readiness, then verify the original acknowledged
keys and one seed value per range:

```sh
"$CF_PERF_PY" scripts/perf/cloudflare.py start --run-dir "$CF_PERF_RUN" --node 1
"$CF_PERF_PY" scripts/perf/cloudflare.py start --run-dir "$CF_PERF_RUN" --node 2
"$CF_PERF_PY" scripts/perf/cloudflare.py start --run-dir "$CF_PERF_RUN" --node 3
"$CF_PERF_PY" scripts/perf/cloudflare.py ready --run-dir "$CF_PERF_RUN"
"$CF_PERF_PY" scripts/perf/cloudflare.py check-writes --run-dir "$CF_PERF_RUN" \
  --results "$CF_PERF_RUN/results-main.json" --label cold-restart
```

This read-only check retains every result, including missing/different values and
request errors. It samples original successful writes; it does not resolve failed
mutations or qualify every crash/transaction recovery boundary. Cold recovery can
change ownership, so retain a new authoritative inventory rather than assuming
the pre-restart ten-per-node placement. Start every recovery peer before waiting
for signed readiness: restored metadata may need a different owner.

Compare requests/s with p50/p95/p99, successful counts, errors, coverage, resource
use and locations. These closed-loop experiments do not establish an arrival-rate
SLO or peak fleet capacity. See [performance interpretation](../../docs/performance.md#read-the-report)
and the [recorded Cloudflare experiment](../../docs/cloudflare-benchmark.md), whose
cold recovery required operator assistance after the five-minute readiness guard.

References: [Container sizes](https://developers.cloudflare.com/containers/platform/limits/),
[Container API](https://developers.cloudflare.com/containers/api/durable-object-container/),
[R2 conditional operations](https://developers.cloudflare.com/r2/api/s3/api/).
