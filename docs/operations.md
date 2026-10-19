# Operate BeyondDB

Use this runbook with [deployment](deployment.md), [server reference](cellar-server.md)
and the deployment's retained configuration. Production capacity, backup/PITR,
format upgrades and bounded fleet recovery remain unqualified; do not infer those
contracts from a healthy process.

## Health and evidence

| Check | Interpretation |
| --- | --- |
| Public `/health` | Listener readiness after startup gates; not a durable table test |
| Signed ListTables/DescribeTable | Authentication, authorization and metadata path |
| Conditional write + consistent read | Selected data path; use an isolated probe table |
| Supervisor exit status and stderr | Abnormal shutdown, fencing or maintenance failures |
| Node/container/store resource counters | Pressure, throttling, OOM, FD or disk exhaustion |
| Current cell ownership | Actual placement; range count alone is insufficient |

The executable initializes warning-level structured stderr logging. Do not assume
`RUST_LOG` changes that configuration or that a Prometheus exporter is configured;
check the current binary integration. Preserve logs, revision, binary hash,
lockfile, configuration and timestamps for diagnosis. Redact credentials and user
items from shared artifacts.

## Diagnose request failures

| Symptom | First checks | Avoid |
| --- | --- | --- |
| Authentication/authorization failure | Region, endpoint, key state, account, policy ARN and cache mode | Disabling signing or granting blanket access as a diagnostic fix |
| Table remains CREATING/DELETING | Configured account maintenance, admission and storage/peer errors | Deleting lifecycle rows or republishing routes manually |
| High 429/503 or latency | Per-cell/account contention, native residency, object-store waits, client retries | Raising deadlines or concurrency without measuring the bottleneck |
| Write times out | Retain request/token identity; determine replay/transaction outcome | Assuming rollback or retrying under a new identity |
| GSI lags | Pending journal work, index owners, split progress and capacity | Treating a stale index result as proof a base write failed |
| TTL item remains | Numeric expiry attribute, enabled policy and maintenance progress | Assuming expiry immediately hides an item |
| Streams page empty | Shard/iterator position, all shards, open-versus-sealed status and retention | Treating one empty shard as stream exhaustion |
| Node stops or fences | Native lease/authority, store reachability, task failure and shutdown report | Editing ownership records to force serving |

An HTTP response is not the adapter's pre-submission proof. Even a service error
can follow committed work. Use [transaction outcome rules](cross-cell-transactions.md#retry-and-capacity-rules).

## Shutdown and replacement

Send SIGTERM through the supervisor and wait for exit. Preserve mode drains
application/native work and retains same-node reload state. Fleet handoff requires
an eligible live peer. Exit failure or a kill is not a clean-reload proof.

For an unplanned owner loss, keep the bucket prefix, encryption key and surviving
node/follower state. Let Cellar fence the previous generation and recover from its
native records. Do not erase local directories or follower logs while their
acknowledged writes may still need recovery. Test replacement with a new logical
node in an isolated deployment before documenting a recovery objective.

## Backups, retention and upgrades

No supported coordinated table backup, restore or PITR workflow exists. A live
SQLite copy or arbitrary object listing is not a consistent cross-cell backup.
Do not prescribe bucket lifecycle deletion for native logs, sealed sources or
retained generations without a proven collection contract.

In-place storage upgrades are unqualified. Retain the source/binary/configuration
for existing roots; validate compatibility in a disposable clone or explicitly
migrate to a fresh prefix. Reusing an incompatible development root is not an
upgrade plan. See [qualification gates](cellar-server.md#qualification-still-required).
