# Deploy a BeyondDB node

This guide builds and bootstraps one BeyondDB node, then explains the requirements for adding peers. The serving binary has restart-tested process fixtures, but table backup and point-in-time recovery (PITR), upgrades of older roots, unattended fleet recovery, and sustained load remain unqualified. Preserve the encryption key and object-store root as durable state.

```mermaid
flowchart LR
    Client["AWS CLI or SDK<br/>DynamoDB credentials"] -->|"signed HTTPS or loopback HTTP"| Public["Public listener"]
    Public --> Node["BeyondDB node<br/>ExtendDB + Cellule"]
    Node <-->|"private mTLS"| Peer["Other Cell owners"]
    Node -->|"conditional writes + LTX"| Store["Durable object store"]
    Node --> Scratch["Session scratch directory"]
```

The object store and encryption key must survive a node restart. Scratch storage belongs to one node session. The [architecture guide](architecture.md) explains the authority and recovery steps behind this diagram.

## Requirements

Prepare these resources before starting the server:

- Rust 1.97 or newer to build this checkout; `cargo` and a unique
  `CARGO_TARGET_DIR` if the workstation has the mounted Workspace volume.
- An S3, GCS, or Azure object store with strict create and conditional update
  semantics. Local `file://` storage cannot provide Cell authority. Configure
  that provider's credentials outside `config.json` for the server process.
- OpenSSL (or an equivalent CA workflow) for peer mTLS. Peers require an
  Ed25519 leaf with both client and server authentication, signed by the
  configured CA, with a SAN matching `peer_server_name`.
- A 32-byte binary encryption key file. Losing it makes stored access-key
  secrets unreadable. It is shared by nodes in one fleet.
- A writable scratch directory and enough RAM and disk for the configured
  Cell and capture budgets. `disk_budget_bytes`, `node_retained_bytes`, and
  `split_threshold_bytes` must be positive. `node_retained_bytes` bounds
  in-flight Cell command and publication state; it defaults to 1 GiB so a
  write-heavy node does not hit the runtime mailbox ceiling before durable
  publication catches up. Lower it on a memory-constrained node or raise it
  for a larger workload after measuring memory use.

### Local object-store fixture

Use this fixture only for a local exercise. It starts RustFS and creates the bucket used by the configuration example below.

The ignored server-process test uses the same digest-pinned RustFS image. The credentials below are test-only S3 credentials. Set them in the server's shell; use a separate shell for the bootstrapped DynamoDB key in the [client guide](user-guide.md#connect).

```sh
docker run --detach --name beyonddb-rustfs \
  --publish 127.0.0.1:9000:9000 \
  --env RUSTFS_ACCESS_KEY=crab \
  --env RUSTFS_SECRET_KEY=crab \
  --env RUSTFS_CONSOLE_ENABLE=false \
  --volume beyonddb-rustfs:/data \
  ghcr.io/rustfs/rustfs:1.0.0-glibc@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858

export AWS_ACCESS_KEY_ID=crab
export AWS_SECRET_ACCESS_KEY=crab
export AWS_DEFAULT_REGION=us-east-1
export AWS_ALLOW_HTTP=true
export AWS_ENDPOINT_URL_S3=http://127.0.0.1:9000
export AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
aws --endpoint-url "$AWS_ENDPOINT_URL_S3" s3api create-bucket --bucket my-bucket
```

Wait for RustFS to accept S3 requests before creating the bucket. Keep its
named volume for restart testing; removing it removes this fixture's data.
Use a service with qualified conditional writes for any nonlocal deployment.

## Prepare keys, policy, and configuration

The example uses `/etc/beyonddb` for secrets and `/srv/beyonddb` for scratch files. Run the provisioning commands with an account that can write those paths, and run the server with read access to the key and certificate files. For a local account without that access, change every path in the commands and JSON to directories it owns.

Create the credential encryption key with restricted permissions:

```sh
install -d -m 700 /etc/beyonddb
openssl rand -out /etc/beyonddb/encryption.key 32
chmod 600 /etc/beyonddb/encryption.key
```

For a local single-node exercise, issue an Ed25519 peer certificate for
`localhost`. For a fleet, use a managed private CA and issue a distinct leaf
per node. The CA certificate must be the same trust root on every node.

```sh
openssl genpkey -algorithm ED25519 -out /etc/beyonddb/peer-ca.key
openssl req -new -x509 -key /etc/beyonddb/peer-ca.key \
  -out /etc/beyonddb/peer-ca.crt -days 365 \
  -subj '/CN=BeyondDB Peer CA' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign'
openssl genpkey -algorithm ED25519 -out /etc/beyonddb/peer.key
openssl req -new -key /etc/beyonddb/peer.key \
  -out /etc/beyonddb/peer.csr -subj '/CN=localhost'
cat > /etc/beyonddb/peer.ext <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:localhost
EOF
openssl x509 -req -in /etc/beyonddb/peer.csr \
  -CA /etc/beyonddb/peer-ca.crt -CAkey /etc/beyonddb/peer-ca.key \
  -CAcreateserial -out /etc/beyonddb/peer.crt \
  -days 365 -extfile /etc/beyonddb/peer.ext
chmod 600 /etc/beyonddb/peer-ca.key /etc/beyonddb/peer.key
```

Save the following bootstrap policy as `/etc/beyonddb/operator-policy.json`. It grants table access to the example account; narrow its actions and resources for your workload.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "dynamodb:*",
      "Resource": ["arn:aws:dynamodb:us-east-1:123456789012:table/*"]
    }
  ]
}
```

Save the next JSON block as `config.json` in the repository root. Replace its object-store URL and local paths as needed. The public listener is loopback-only without TLS; the peer listener always uses mTLS.

```json
{
  "storage_url": "s3://my-bucket/beyonddb",
  "node_id": "01994f26-5966-7b20-8b58-2fddf198a321",
  "data_dir": "/srv/beyonddb/scratch",
  "disk_budget_bytes": 107374182400,
  "node_retained_bytes": 1073741824,
  "max_active_cells": 128,
  "sql_workers": 12,
  "encryption_key_file": "/etc/beyonddb/encryption.key",
  "region": "us-east-1",
  "peer_bind": "127.0.0.1:9001",
  "peer_endpoint": "https://localhost:9001",
  "peer_certificate": "/etc/beyonddb/peer.crt",
  "peer_private_key": "/etc/beyonddb/peer.key",
  "peer_ca": "/etc/beyonddb/peer-ca.crt",
  "peer_server_name": "localhost",
  "public_bind": "127.0.0.1:8000",
  "public_endpoint": "http://127.0.0.1:8000",
  "owned_accounts": ["123456789012"],
  "owned_access_keys": ["AKIAIOSFODNN7EXAMPLE"],
  "initial_partitions": 4,
  "split_threshold_bytes": 268435456,
  "auth_cache_enabled": false,
  "bootstrap": {
    "account_id": "123456789012",
    "access_key_id": "AKIAIOSFODNN7EXAMPLE",
    "principal_name": "operator",
    "policy_name": "tables",
    "policy_file": "/etc/beyonddb/operator-policy.json"
  }
}
```

`auth_cache_enabled` is disabled in the example because credentials and table
generations can be changed by another node. Set it to `true` only when the
cache's 60-second cross-node visibility window is acceptable. Local management
mutations invalidate cached credentials, policies, boundaries, and table
metadata immediately; changes made through another node become visible after
the cache TTL. Enabling the flag also caches complete routed directory pages
for point operations. The owning data Cell rejects a stale epoch and the
server drops that route entry, so a split is refreshed on the next request. It
also keeps immutable catalog proofs and resident local Cell handles for 500 ms;
the authority check resumes after that window, and a drained Cell handle still
rejects work immediately. This short owner cache improves warm local latency
while bounding visibility of an ownership change.

The parser rejects unknown fields. `initial_partitions` defaults to one and can provision 1–256 initial data Cells per new table. `max_active_cells` defaults to 128 and reserves the Cell runtime capacity for account, coordinator, management, and data Cells together; size it for the number of simultaneously resident Cells on the node and the available memory. `sql_workers` is optional; when omitted, the runtime derives the worker count from host parallelism, capped at sixteen. Set it explicitly when a node serves many partitions and you have measured enough CPU and memory headroom. Each worker owns its SQLite connections, so increasing the value does not make one hot Cell publish concurrently. The split threshold defaults to 256 MiB of occupied SQLite pages. `node_id` identifies a physical node; each running node needs a distinct ID and scratch path.

`follower_store_bytes` is an optional positive disk budget for persistent
follower lanes under `data_dir/follower-store`. Reserve it in addition to
`disk_budget_bytes`, and retain the directory across process restart. By
itself, it opens the authenticated receiver for recovery and advertises zero
follower capacity; writes still wait for object publication.

Experimental `follower_durability_enabled: true` requires a follower-store
budget of at least 64 MiB. It installs the host provider during startup, gates
recruitment until configured-account recovery completes, and advertises actual
remaining follower bytes only after the private listener starts. Each enrolled
member must fsync before a follower proof; absent an eligible ensemble, the
object-publication path remains available. Use separate node IDs, certificates,
and data directories for every server. The three-process crash test exercises
same-partition item mutations, transaction replay, and stream records;
independent-host failure, cross-partition transaction faults, rotation under
load, and sustained throughput still need qualification. See the
[follower durability guide](follower-durability.md).

The S3 serving path uses Cellule's provider builder, including multipart
conditional copy support needed to pin recovery overlays. The URL-only builder
previously left that operation unconfigured, which the real RustFS process-kill
test exposed. GCS and Azure retain their URL-based configuration paths.

| Credential or file | Used by | Keep across restart? |
| --- | --- | --- |
| Object-store credentials | Server process to read and write Cell roots | Yes, or replace with equivalent authorized credentials |
| Encryption key file | Server process to decrypt stored access secrets | Yes; losing it makes those secrets unreadable |
| Peer CA and leaf certificate | Nodes to authenticate private traffic | Preserve a common trust root; rotate leaves deliberately |
| Bootstrapped access key and secret | AWS CLI or SDK client to sign DynamoDB requests | Preserve until you rotate or revoke the key |

## Build and bootstrap

Build the checked-out revision, enter the bootstrap secret at the hidden prompt, and keep the process running. The first command uses a target directory unique to this checkout on workstations with the mounted Workspace volume.

```bash
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/beyonddb-node-3c23"
cargo build --locked --bin beyonddb
printf 'Bootstrap secret: '
read -rs BOOTSTRAP_SECRET
printf '\n'
printf '%s' "$BOOTSTRAP_SECRET" | \
  "$CARGO_TARGET_DIR/debug/beyonddb" config.json --bootstrap
unset BOOTSTRAP_SECRET
```

Enter the secret at the hidden `read` prompt and press Enter. Bootstrap reads
the secret from stdin, encrypts it in a credential Cell, and commits the inline
policy in the account Cell. The process remains serving after bootstrap; stop
it normally to restart. Subsequent starts omit `--bootstrap`:

```sh
"$CARGO_TARGET_DIR/debug/beyonddb" config.json
```

The server writes warnings and errors to stderr. Use the [AWS CLI connection example](user-guide.md#connect) from a separate shell to test the signed public endpoint. A supervisor should keep the server in the foreground and provide the configured scratch budget.

## Expose the public listener or add nodes

Once the local node works, set the public TLS identity and peer addresses for any non-loopback deployment. Every node must agree on the durable storage and trust configuration.

For a non-loopback `public_bind`, supply both `public_certificate` and
`public_private_key` in `config.json` and use an `https://` public endpoint.
The certificate must match the hostname clients use; distribute its CA bundle
to AWS CLI/SDK clients. The peer endpoint must always use HTTPS with the
configured mTLS identity and an address reachable by other nodes.

All nodes in one fleet must use the same CA, object-store root, compiled
release, and encryption key. Configure each account and access key on the node
that recovers its Cell; a public node without local account or credential
ownership can forward signed requests to the live owner. A replacement must
fence an expired node session before claiming its Cells. Reusing a node ID
after a clean drain is supported; an unclean exit waits for authoritative
lease expiry. See [architecture](architecture.md#failure-recovery-and-current-limits).

Linux automatic placement requires a visible, complete cgroup-v2 memory
hierarchy. Unsupported host measurements advertise no placement capacity,
although configured owned Cells can still serve. Initial data/GSI placement,
request-driven restoration, and settled-range movement exist; a general
distributed recovery scheduler and fleet load qualification do not.
[Measured placement requirements](scaling.md#fleet-controller-boundary)

## Recovery and upgrade limits

Object-store roots are the recovery source. Scratch files are disposable only after a clean drain or a fenced takeover by a new owner.
A crashed session can leave scratch files for operator cleanup. Preserve the
encryption key, peer trust root, object-store data, and the exact compiled
release used to write the roots. Several unreleased schema revisions require
reprovisioning development roots; there is no qualified in-place migration.

BeyondDB has no supported `CreateBackup`, restore, or PITR API. Independent
object-store copies are not a consistent multi-Cell table backup. Do not use
this deployment for data that requires a tested table-wide restore until that
protocol and its recovery drills exist. [Backup boundary](api.md#explicitly-unsupported-or-incomplete)

## Verification

Check formatting and static diagnostics before running the signed process fixture:

```sh
cargo fmt --check
CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/beyonddb-node-3c23" \
  cargo clippy --all-targets -- -D warnings
CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/beyonddb-node-3c23" \
  cargo test --test server_binary -- --ignored --test-threads=1
```

The ignored [`server_binary` process suite](../tests/server_binary.rs) starts RustFS, uses signed SDK requests, kills a server, and checks recovered state. It requires Docker, AWS CLI, and OpenSSL; Colima users can select their daemon with `DOCKER_CONTEXT=colima`. For broader API validation, run unchanged upstream client tests with the [independent qualification runner](implementation-status.md#independent-client-qualification). Passing these fixtures does not establish production-scale capacity.
