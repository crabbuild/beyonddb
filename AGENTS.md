# BeyondDB contributor guide

These instructions apply to the entire repository. Preserve existing contracts
when changing a subsystem; inspect its implementation and tests before editing.

## Project and sources of truth

BeyondDB is a standalone DynamoDB-compatible service that embeds ExtendDB and
Cellar in one executable. It is under development: implemented behavior, tested
behavior and production qualification are different claims.

- **ExtendDB** owns HTTP, SigV4, IAM evaluation, DynamoDB validation and expressions.
- **BeyondDB** owns table metadata, ordered range routing, indexes, transaction
  coordination, TTL and Streams integration.
- **Cellar** owns Cell execution, physical durability, fenced ownership,
  placement, node lifecycle and peer transport.

Read [README.md](README.md) for product scope, [architecture](docs/architecture.md)
for subsystem boundaries, [the server guide](docs/cellar-server.md) for deployment
and qualification, and [API coverage](docs/api.md) before changing public behavior.
The [ordered partitioning contract](docs/ordered-cell-partitioning.md) governs
routing. Detailed design and revision-scoped evidence remain in the original guides;
confirm current behavior against source and registered tests.

[Cargo.toml](Cargo.toml), [Cargo.lock](Cargo.lock) and the
[CI workflows](.github/workflows) define dependency and toolchain versions.
The current baseline is Rust 2024, minimum Rust 1.97, and Cellar's exact crates.io
`cellar-main =0.1.0` release. Keep examples and code compatible with the declared
minimum Rust version; a newer local compiler does not establish that compatibility.

## Repository map

| Location | Responsibility |
| --- | --- |
| `src/bin/beyonddb.rs`, `config/cellar.example.json` | Executable configuration, startup, bootstrap and shutdown |
| `src/cellar/server.rs`, `src/server/` | Embed the frontend and SDK; wire authorization, background tasks and recovery |
| `src/backend.rs`, `src/backend/` | ExtendDB storage adapters, admission, batching and transaction drivers |
| `src/table.rs`, `src/table/`, `src/catalog.rs`, `src/credentials.rs` | Account/catalog state, table lifecycle and credential records |
| `src/partition.rs`, `src/partition/`, `src/items.rs`, `src/items/` | Partition-local commands, item reads/writes and index maintenance |
| `src/routing.rs`, `src/routing/`, `src/directory.rs`, `src/directory/`, `src/split.rs`, `src/cellar/provision.rs` | Ordered routing, directory publication, provisioning and splits |
| `src/transaction_coordinator.rs`, `src/participant.rs`, `src/transaction_transport.rs`, `src/transaction_payload.rs` | Durable transaction decisions, participant resolution and bounded payload transfer |
| `src/global_index*`, `src/secondary_index*`, `src/stream_*`, `src/ttl.rs` | Secondary indexes, journal delivery, Streams retention and TTL |
| `crates/cellar-runtime/` | Typed commands/queries, bounded codecs, SQL adapter, identities, registry and SDK ingress |
| `crates/keyspace/` | Versioned ordered keys, ranges and logical continuations |
| `tests/`, `tests/support/` | Native, signed SDK, ownership, fault and process-recovery fixtures |
| `scripts/perf/`, `docs/local-benchmark.md` | Configurable local performance tooling and interpretation |

Keep dependencies directed through these boundaries. Add domain behavior to its
owning module; introduce shared abstractions only for a demonstrated contract.

## Database invariants

- Keep DynamoDB parsing, SigV4, IAM and HTTP outside Cell handlers. Preserve
  tenant/account scope when building targets, caches, receipts and continuations.
- A partition-local mutation must commit its item changes, replay result and
  associated index/stream intents through one Cell command. Acknowledgement must
  follow the configured Cellar durability gate, including full gated-body decoding.
- Cross-Cell transactions require a durable coordinator decision and idempotent
  participant resolution. Lost replies, duplicate prepares and repeated recovery
  must preserve locks, reservations, outcomes and client-token semantics.
- Distinguish `NotStarted`, durable rejection and ambiguous submission outcomes.
  An HTTP error, timeout or cancelled future after submission does not prove that
  a mutation failed to execute. Do not retry it with a fresh mutation identity.
  Pure-query backpressure may use the existing bounded read policy; a transaction
  read implemented as a command still follows mutation rules.
- A table spans ordered data ranges across Cells; never restore a one-table,
  one-Cell data shortcut. New tables require 2–256 initial ranges, default four.
  The ranges must cover their key space with unique identities and adjacent,
  nonoverlapping intervals. Multiple ranges on one node share its failure domain.
- Preserve `(PK, SK)` ordering, numeric canonicalization, half-open boundaries,
  forward/reverse pagination and logical continuation semantics. Never replace
  ordered routing with hash partitioning. Keep LSI locality and GSI tie-breakers.
- Publish routes only after matching installations are proven. Preserve table
  generation, key encoding version and range epoch checks. An application range
  epoch does not replace Cellar's ownership fence.
- Keep base writes and GSI delivery intents atomic; projection is asynchronous.
  Splits, replay, TTL and journal retention must preserve pending transaction,
  index and stream work until the owning protocol permits reclamation.
- Use Cellar's supported ownership and recovery interfaces. Do not edit lease,
  authority, receipt or runtime SQLite records to make a fixture or operation pass.

## Rust implementation practices

Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/checklist.html),
with the repository-specific rules below.

### Types, ownership and interfaces

- Prefer explicit domain types, existing identity wrappers and enums over
  interchangeable strings, raw integers or positional boolean flags. Validate
  externally constructed values at the boundary; keep invalid states private.
- Borrow when ownership is unnecessary. Use `Arc` for shared ownership, and clone
  payloads only when their lifetime or measured cost justifies it. Prefer concrete
  types until a generic or trait boundary has a clear caller or testing purpose.
- Keep APIs narrow: private by default, `pub(crate)` for internal sharing. Use
  standard conversion traits and fallible `TryFrom` for checked conversions.
- Use checked arithmetic for untrusted sizes, offsets, counts and byte budgets.
  Validate before indexing or allocating; avoid lossy casts and `f64` for DynamoDB
  decimal identity or ordered numeric keys.
- Document public contracts with rustdoc: invariants, errors, ownership, replay,
  durability and cancellation behavior where relevant. Examples should use `?`.
  Comments should explain the invariant or tradeoff behind non-obvious code.

### Errors and safety

- No `unwrap`, `expect` or `panic!` outside tests. Treat malformed input, resource
  exhaustion, missing objects and transport failures as expected error paths.
- Preserve typed errors and causal context in libraries. Add operational context
  at application boundaries; map errors to ExtendDB responses without exposing
  secrets or changing rejection-versus-ambiguity semantics. Do not silently discard
  a `Result`, replace corruption with defaults, or log an error and return success.
- Prefer safe Rust. Isolate unavoidable `unsafe` in a small abstraction, document
  each block's safety justification and every unsafe API's caller obligations,
  and test those invariants. Never use unchecked operations to bypass validation.
  Follow the [Rust Reference](https://doc.rust-lang.org/reference/unsafe-keyword.html).
- Fix warnings rather than disabling them globally. A narrow lint allowance needs
  an explanation. Keep refactoring separate from behavior changes when practical.

### Async execution and resource bounds

- Keep blocking I/O, long CPU loops and synchronous waits off Tokio workers.
  Use existing async interfaces or bounded blocking work with an explicit owner.
  `spawn_blocking` work requires its own completion/cancellation plan.
- Keep lock scope short. Never hold a synchronous mutex/RwLock guard across
  `.await`. Use an async lock only when the protocol requires it; avoid network
  calls under shared locks and document lock order for multi-lock paths. See
  [Tokio shared-state guidance](https://tokio.rs/tokio/tutorial/shared-state).
- Bound queues, task fan-out, batches, scans, reply sizes and concurrent uploads.
  Account for resident state, physical prepare/apply reservations and durability
  work, not just encoded request bytes. Release permits and temporary resources
  on success, rejection, cancellation and shutdown.
- Carry caller deadlines through admission, routing and transport; do not restart
  the budget on each hop. Use bounded backoff only for retry-safe operations.
- Review `select!`, timeouts and dropped futures for cancellation safety. Give
  spawned work a lifecycle owner, observe task failures, and join/drain it during
  shutdown. Cancellation of a driver must not erase a durable transaction decision.
  See [Tokio cancellation safety](https://docs.rs/tokio/1.49.0/tokio/macro.select.html#cancellation-safety).
- Keep command execution replay-safe. Supply external time/randomness through
  the existing command/context contract; do not introduce hidden network calls,
  mutable process globals or detached side effects inside a durable command.

## Persistence, dependencies and security

- Treat SQL schemas, operation IDs, codec versions, key encodings, cell identities
  and serialized enum representations as compatibility boundaries. Never silently
  reuse an ID or reinterpret persisted bytes. Specify upgrade/replay behavior,
  add compatibility fixtures, or explicitly reject incompatible state.
  Safe format upgrades remain unqualified; incompatible roots require an explicit
  migration or a new storage prefix. Never open roots from an incompatible runtime as Cellar state.
- Keep application SQL parameterized and behind the typed adapter. Cellar owns
  transaction control and runtime tables. Do not bypass its SQL allowlist or
  construct SQL by concatenating user input.
- Pin Cellar to an exact reviewed crates.io release and all ExtendDB crates to
  the reviewed Git revision. Review lockfile/source changes before commit. Do not
  patch either dependency without user approval; preserve any existing authorization.
- Add dependencies only for a concrete need. Check MSRV, license, features and
  runtime cost; reuse workspace dependencies. Keep production features minimal.
  Use `--locked` for reproducible checks and do not resolve unrelated upgrades.
- Never commit secrets or log credentials, signing material, encryption keys or
  complete user items. Use redacted structured `tracing` fields, bounded messages
  and scoped request/cell identifiers. Avoid unbounded identifiers as metric labels.
- Preserve authorization and revocation checks, tenant isolation and authenticated
  peer traffic. Validate configuration before serving. Tests use isolated accounts,
  temporary roots and local stores; keep retained state and unrelated services safe.

## Testing and validation

Choose tests from the contract that changed. Keep failures reproducible with
explicit barriers, controlled clocks and fault injection rather than long sleeps.
Use bounded timeouts to expose hangs. Prefer observable outcomes over private
implementation details; reversible documentation edits need link/syntax checks.

| Changed contract | Required evidence |
| --- | --- |
| Keys, ranges or pagination | Boundary and malformed-input cases, numeric equivalence, split/reverse continuation and `keyspace` tests |
| A Cell command, schema or codec | Native command tests, rejection/replay, malformed/oversized payloads and restoration from committed state |
| DynamoDB behavior | Differential results/errors against ExtendDB's SQLite backend and applicable upstream protocol cases, plus signed SDK coverage |
| Transactions or mutation transport | Lost batch/reply, duplicate/token mismatch, capacity, owner loss before/after decision and idempotent resolution |
| Placement, durability or recovery | Native ownership/fencing faults and relevant signed process-restart cases; distinguish local files from bucket/follower recovery |
| Benchmark tooling | `scripts/perf/test_perf.py`, saved topology/coverage and old-result compatibility; label simulated and live evidence separately |

From the repository root, the baseline Rust checks are:

```sh
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo build --locked --bin beyonddb
```

Run the affected target first, then the broader relevant library/integration gates
in [rust.yml](.github/workflows/rust.yml) and
[qualification.yml](.github/workflows/qualification.yml). These workflows are the
source of truth for the complete CI matrix. For example:

```sh
cargo test --locked -p keyspace
cargo test --workspace --lib --locked
cargo test --locked --test cellar_coalescing --test cellar_transactions -- --test-threads=1
```

The large transaction fixtures retain their internal concurrent workload but run
one fixture at a time on constrained hosts. Do not make them pass by deleting
assertions, reducing coverage, removing tests or changing durability deadlines.
Explain infrastructure limitations separately from code failures.

Process qualification requires Docker, OpenSSL and the AWS CLI. Run it when the
changed contract needs process/bucket/restart evidence; the usual suite skips
ignored tests:

```sh
cargo test --locked --test cellar_process -- --ignored --test-threads=1
cargo test --locked --test server_binary -- --include-ignored --test-threads=1
```

For benchmark tooling, use Python 3.12+ and an isolated environment:

```sh
python3 -m venv benchmarks/perf-venv
benchmarks/perf-venv/bin/python -m pip install -r scripts/perf/requirements.txt
benchmarks/perf-venv/bin/python -m unittest discover -s scripts/perf -p 'test_*.py'
```

On workstations with the mounted Workspace volume, set a checkout-specific
`CARGO_TARGET_DIR` under `$HOME/Workspace/crabbuild-target` before compiling.
Reuse the configured target directory when available; avoid starting simultaneous
heavy builds or qualification fixtures against the same host resource budget.

## Benchmarks, documentation and delivery

- Reproduce performance changes with a release binary and an unchanged workload.
  Record revision, lockfile/dependency pins, durability mode, topology, payload,
  client concurrency, warmup, elapsed time, errors and resource limits.
- Evaluate throughput together with latency/sample counts and error rates. Verify
  traffic and ownership across every configured data range; count system and
  coordinator Cells against node resources too. Retain interrupted/failed runs.
  A local shared VM is not fleet capacity or an arrival-rate latency SLO.
- Store local result directories as `benchmarks/YYYY-MM-DD-local-benchmark` and
  follow [the benchmark guide](docs/local-benchmark.md). New raw reports, logs and
  fixture credentials are local/ignored by default; do not force-add them merely
  to support a performance claim. Commit reviewed summaries and diagrams.
- Keep unsupported behavior and qualification boundaries explicit in README and
  API docs. Do not claim support until a signed SDK request reaches a durable
  result and the result survives the relevant owner restart. Historical retired
  tests in [the coverage inventory](docs/retired-tests.md) are not gates.
- Before delivery, inspect the diff for unrelated edits, accidental dependency
  changes, generated artifacts and secrets; run `git diff --check`. Update affected
  config examples, docs and links. A PR should explain the problem, final behavior,
  durability/compatibility implications, validation and any remaining failures.
- Report exactly which checks ran and which could not run. Separate focused local
  results from Linux CI and process qualification; never describe pending checks
  as passed.

Documentation uses SVG for architecture/flow visuals and Mermaid only for
sequence diagrams. Preserve the original substantive coverage: contracts, rationale,
fault cases, measurements and reproduction steps. Tighten wording and navigation
without replacing detailed guides with summaries. Keep operational instructions
current and label older evidence by revision; do not relabel historical runtime
results as Cellar qualification.
Run `python3 scripts/check-docs.py` after documentation edits; it checks local
links/headings, shell and JSON examples, SVG structure and the sequence-only rule.
