# Native Cellar follower durability

The Cellar executable requires bucket publication by default. Opt in to native
follower proofs with this fleet setting:

```json
{
  "fleet": {
    "bucket": "my-bdb-bucket",
    "prefix": "beyonddb-cellar/ordered-v1",
    "follower_durability": true
  }
}
```

All nodes need the same privately provisioned peer authentication key and a
dedicated deployment prefix. Keep each node's data directory persistent. This
is the native Cellar protocol; the old Cellule `follower_store_bytes`,
`follower_durability_enabled` and mTLS configuration fields are rejected.

With the option enabled, the SDK installs its coupled log shipper and bundle
sink. A write can be proven by follower fsyncs or bucket publication, whichever
wins. Node bundles and per-cell LTX files are distinct publication paths.
Without an eligible follower ensemble, writes wait for bucket proof. The
pinned SDK recruits two followers and can serve with one in-sync witness; this
flag does not introduce a configurable quorum or require two receipts for every
write. Provision and qualify the failure policy required by your deployment.

BeyondDB still commits each partition-local mutation, result, index intent and
stream intent atomically in its cell. Cross-cell transactions retain their
durable coordinator decision and idempotent participant resolution. The SDK
owns follower files, ensemble transitions, sequence coverage, authenticated
transport and log recovery. BeyondDB does not implement a second authority or
rewrite SDK ownership records.

An owner process dying does not immediately expire its lease. Takeover waits
for the native authority fence. When a failed generation may have acknowledged
fleet writes, Cellar seals and validates its retained log range before
certifying recovered cells. Keep surviving follower data available until the
SDK has recovered and tiered those writes. A generic transport error remains
an ambiguous result and must retain the original mutation identity.

`preserve` shutdown drains application work and log tasks, closes durability
and prepares the SDK's clean-reload proof. `handoff` requires an eligible live
peer. A kill or canceled shutdown supplies no graceful proof; recovery must
follow the retained native records.

## Verified process boundary

`acknowledged_untiered_item_survives_owner_process_kill` runs three real
executables against pinned RustFS. It creates a public two-cell table and a
published warmup under bucket proof, then enables native follower durability.
The test waits for two enrolled followers and pauses balancing through native
management to select a stable crash target.

An S3 proxy then rejects both node-bundle and per-cell LTX uploads. Admission
closes atomically, and every previously forwarded payload finishes before the
bucket cut is recorded. Rejected request bodies are discarded, so releasing
the fault after a kill cannot publish an old queued body.

Signed SDK Put, Update, Delete, BatchWrite and transaction requests still
succeed. The native lease records possible fleet acknowledgements, while the
bucket payload keys, ETags and sizes remain unchanged. The
test kills the actual cell owner, removes its local directory, checks the same
bucket cut, waits for lease expiry and starts a new logical node with an empty
directory. The new node reads the acknowledged items, retains the deletion,
replays the conditional transaction without reevaluation and exposes exactly
one stream event for each operation. Ownership advances and the failed log is
sealed. No credential or IAM-policy bootstrap runs on the successor.

Run the fixture explicitly:

```sh
cargo test --test cellar_process --locked acknowledged_untiered_item_survives_owner_process_kill -- --ignored --nocapture
```

It requires Docker, AWS CLI and the pinned RustFS image. Ordinary ignored-test
counts do not qualify recovery. This case covers one owner loss with surviving
followers; simultaneous witness loss, network partitions, slow-disk behavior,
resource admission and fleet-scale latency/throughput remain unqualified.
