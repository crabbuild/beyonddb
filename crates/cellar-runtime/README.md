# BeyondDB Cellar runtime adapter

This crate executes BeyondDB's compiled domain commands inside Cellar SDK
invocations. Cellar supplies the real database transaction, owner routing,
fencing, response gate and node lifecycle. The adapter supplies domain operation
descriptors, bounded wire values, typed SQL and replay receipts.

The executable builds a registry, passes its `deployment()` and `compiled()`
results to `Node::start_external`, then constructs `client::CellClient` from the
returned ingress handle. The internal gateway routes to a namespace-derived cell
name; each cell independently checks the target and registered operation.
Keep this gateway out of the public DynamoDB router.

SQL queries accept only read-only statements. Commands accept parameterized
query/insert/update/delete statements. Transaction control, schema changes,
runtime receipt tables and Cellar-owned tables are unavailable through the
application SQL surface. Bootstrap SQL comes from digest-checked compiled module
migrations. BLOBs and signed 64-bit integers use Cellar's native typed cursor API.

One local transaction commits the application result, sequence and replay
receipt. Rejected commands roll back their application savepoint and retain
their committed rejection. A response is decoded only after Cellar's complete
gated body is consumed; submitted transport or proof failures remain ambiguous.

The public frontend opts into `with_read_backpressure`: pure queries pace
HTTP 429/503 within a caller-supplied budget, honoring numeric Retry-After and
bounded backoff. The policy needs the caller's Tokio time driver and caches no
values. Commands always remain single-attempt, including commands used by
transactional reads. A status code never establishes non-execution of a write.

The crate is a member of the BeyondDB workspace and shares its pinned Cellar
dependency and root lockfile. See the repository's
[server qualification boundaries](../../docs/cellar-server.md#qualification-still-required) for outstanding work and checks.
