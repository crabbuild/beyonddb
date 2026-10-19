# BeyondDB server reference

The `beyonddb` executable embeds ExtendDB and Cellar. BeyondDB owns configuration,
public TLS and application maintenance. Cellar owns execution, placement,
ownership, private peer transport and durability. See [deployment](deployment.md)
for setup and [operations](operations.md) for failure handling.

## Configuration

Use [config/cellar.example.json](../config/cellar.example.json). The parser
rejects unknown fields. Current fields are defined in
[src/bin/beyonddb.rs](../src/bin/beyonddb.rs).

| Field | Meaning / default |
| --- | --- |
| `node_name` | Unique running node identity |
| `data_dir` | Persistent local directory; engine state is under `cellar-ordered-v1` |
| `region` | Deployment and request-signing region |
| `public_bind`, `public_endpoint` | Listener address and advertised public URL |
| `public_certificate`, `public_private_key` | Optional PEM pair; required for non-loopback public serving |
| `peer_bind`, `peer_advertise` | Private listener and reachable `HOST:PORT` |
| `peer_key_file` | Exactly 32 raw bytes; matching key across fleet nodes |
| `encryption_key_file` | Exactly 32 raw bytes; preserve to decrypt credential secrets |
| `accounts` | Accounts maintained by this deployment; default empty |
| `initial_partitions` | Initial base/GSI data-range count, 2–256; default 4 |
| `max_active_cells` | Positive native residency limit for all cell types; default 128 |
| `split_threshold_bytes` | Positive threshold supplied to range maintenance; default 268435456 |
| `auth_cache_enabled` | Credential, authorization, table metadata and route caching; default false |
| `shutdown_mode` | `preserve` (default) or fleet-only `handoff` |
| `fleet` | Optional S3-compatible fleet configuration; absent means local persistence |
| `bootstrap` | Optional account/key/principal/policy settings, used with `--bootstrap` |

The optional fleet object is:

```json
{
  "fleet": {
    "bucket": "my-beyonddb-bucket",
    "endpoint": "https://s3.example.com",
    "prefix": "beyonddb/ordered-v1",
    "follower_durability": false
  }
}
```

`endpoint` may be omitted for the provider's standard endpoint. The server builds
S3 clients from its environment. It disables conditional-operation retries to
preserve ambiguous ownership evidence. GCS/Azure URL configuration, peer mTLS
fields, `storage_url`, `sql_workers`, `disk_budget_bytes` and
`node_retained_bytes` are not fields in this executable.

The encryption key protects stored credentials; it is not a claim of encryption
for every application row or storage file. Public TLS and peer authentication
are separate controls. Isolate and encrypt the private network externally.

## Serving and caching

Readiness waits for Cellar startup, optional bootstrap and an initial transaction
recovery pass for each configured account. A node fence or failed supervised
maintenance task withdraws public serving. Use `/health` for listener readiness
and signed requests for application checks.

`accounts` drives table lifecycle, projection, TTL, retention and coordinator
maintenance. Empty or incomplete account lists do not provide maintenance for
omitted accounts. Table data remains in separately routed cells.

Caching is off by default. When enabled, the frontend uses the pinned ExtendDB
cache defaults and BeyondDB's bounded route metadata cache. Cross-node changes
can remain cached until expiry; local invalidation is not a fleet-wide immediate
revocation guarantee. Leave caching disabled when testing fresh authorization
and inspect the pinned implementation before relying on a specific TTL.

The current binary fixes native per-cell concurrency at 16 and the handler budget
at 30 seconds. These are source settings, not JSON knobs. Pure queries use bounded
backpressure; commands retain single-attempt submission semantics. More request
concurrency can worsen latency or cause admission errors.

## Persistence and shutdown

| Mode | Acknowledgement and restart boundary |
| --- | --- |
| Local, no `fleet` | Local Cellar persistence; retain node data; does not cover machine loss |
| Fleet, follower durability false | Bucket-publication proof is the default acknowledgement path |
| Fleet, follower durability true | Native follower fsync proof may race bucket publication; see [follower durability](follower-durability.md) |
| `preserve` shutdown | Drain work and local durability; fleet mode also requires clean-reload proof |
| `handoff` shutdown | Fleet-only transfer to an eligible live peer; cannot complete on the last eligible node |

SIGINT/SIGTERM drains the public server, stops application maintenance and invokes
Cellar shutdown. A successful exit requires drained work/cells, no pending
dispatches, quiesced logs, closed local durability and no shutdown deadline failure.
Keep persistent state after exit; never confuse a process kill with a clean stop.

## Qualification still required

[Current test targets](implementation-status.md#current-verified-slice) cover
selected public operations, native peer/fencing faults, owner restoration and
process loss. They do not establish full DynamoDB compatibility, automatic fleet
recovery within a bound, independent-host follower-loss tolerance, sustained
capacity, backup/PITR or safe upgrades of existing development roots.

Use fresh storage for incompatible formats. Do not open roots from a different
runtime or reuse an older hash-layout root as ordered-layout state. Capture the
binary revision, dependency lockfile and configuration with every qualification.
