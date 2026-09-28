# BeyondDB contributor guide

BeyondDB is a standalone DynamoDB-compatible service. ExtendDB owns the HTTP,
SigV4, IAM, and DynamoDB protocol layers. Cellule owns Cell execution,
durability, authority, hosting, and peer transport.

- Keep DynamoDB request parsing, SigV4, IAM, and HTTP outside Cell handlers.
- Store each partition-local mutation, result, and stream intent through one Cell command. Cross-Cell transactions require a durable coordinator decision and idempotent participant resolution.
- Preserve ExtendDB's item and key semantics. Compare backend results with ExtendDB's SQLite backend and the protocol suite before claiming compatibility.
- Do not report an API as supported until an AWS SDK request reaches a durable Cell commit and the result survives owner restart.
- Pin Cellule and ExtendDB to reviewed Git revisions. Review lockfile changes before commit. Do not patch either dependency without approval.
- Keep unimplemented behavior explicit in the README. Never claim fleet-scale capacity from a local fixture.
- No `unwrap`, `expect`, or `panic!` outside tests. Run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and focused tests for changed behavior.
- On workstations with the mounted Workspace volume, set a unique `CARGO_TARGET_DIR` beneath `$HOME/Workspace/crabbuild-target` for this checkout before compiling.
