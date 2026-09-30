# Follower durability for low-latency writes

## Why this work is needed

The default BeyondDB server acknowledges a mutation after its Cell state is
published to the configured object store. On the recent four-partition local
RustFS fixture, this path remained far below file-backed ExtendDB SQLite for
`PutItem`, `UpdateItem`, `BatchWriteItem`, and transactions. A separate
[direct RustFS PUT probe](../benchmarks/2026-09-29-rustfs-put-probe/README.md)
also observed material object-store latency under heavy host contention. Those
short runs do not establish an idle-host throughput limit, but removing small
amounts of item SQL cannot eliminate the object publication round trip.

The pinned Cellule revision already exposes a node-log durability supervisor,
follower stores, and a durability gate. BeyondDB has a lease-bound authority
adapter, an opt-in inbound follower receiver, a pinned mTLS outbound
transport, and a host-provider enrollment adapter. Experimental
`follower_durability_enabled` now installs that provider. Recruitment waits for
startup recovery; available follower capacity is advertised only after the
private receiver starts. A three-process signed SDK crash test passes, while
broader fault and performance qualification remains open.

```text
Default serving path                  Experimental multi-node path

AWS SDK request                       AWS SDK request
       |                                      |
Cell command + local capture          Cell command + local capture
       |                                      |
object-store publication              append to every enrolled follower
       |                                      |
durable object proof                   follower fsync receipts
       |                                      |
SDK success                            authoritative log activation/proof
                                              |
                                          SDK success

                                   Object tiering continues; recovery must
                                   replay any acknowledged untiered frames.
```

Cellule's gate requires every member in the enrolled follower set to fsync a
ticket before a fleet proof. Its first fleet proof also requires an
authoritative activation of that exact log epoch. Object proof remains a
fallback. These are durability conditions, not optional performance hints.

## Product integration boundary

| BeyondDB component | Required behavior |
| --- | --- |
| Follower store | Open `FollowerStore` in each node's durable data directory, reserve disk, and retain lanes across process restart. Advertise follower capacity only after the store and authenticated listener are ready. |
| Peer transport | Implement Cellule's `NodeLogTransport` append, seal, retire, and bounded tail operations over the private mTLS listener. Pin each remote certificate to its live node advertisement. Bound request bytes, time, and concurrent work. |
| Follower authorization | Match the mTLS identity to the advertised session. Use `NodeDirectory::authorize_log_append`, `authorize_log_retire`, and `authorize_log_recovery` before touching a lane. Reject wrong members, epochs, coverage watermarks, and unfenced recovery attempts. |
| Enrollment and authority | Implement `NodeDurabilityProvider` using `NodeDirectory::try_recruit_log`. Implement `NodeLogAuthority` with the directory's activate, coverage, and close CAS operations; reconcile CAS races with lease heartbeats without losing the enrolled log. Supply the exact session, node ID, members, lease guard, transport, and limits to `NodeDurabilityConfig`. |
| Recovery | Before public readiness after an owner loss, seal and fetch the failed owner's authorized follower tail, reconcile object coverage, and restore acknowledged Cell commits. Do not return success for a write whose proof cannot be recovered on a successor. |
| Lifecycle | Rotate and retire only after the recorded coverage barrier. Drain the node, settle publications, and preserve follower files if withdrawal or recovery has not completed. |

The private `BeyonddbPeers` router now has a bounded node-log receiver when
`follower_store_bytes` is configured. It opens Cellule's persistent
`FollowerStore` beneath `data_dir`, requires a live mTLS identity bound to the
advertised session, and checks the directory's append, retirement, or fenced
recovery authority before touching a lane. The outbound
`PeerNodeLogTransport` resolves a live advertised member, pins its certificate
and key, and bounds requests, replies, and tail paging. Tests cover a durable
append and duplicate append over real two-identity mTLS, follower-store
reopen, wrong certificate, wrong epoch, and a seal attempt before owner
fencing. The recovery claimant test also fences the expired leader,
seals its own follower lane under directory authority, and passes that lane
through Cellule's bounded witness reader. A separate three-identity mTLS test
lets another claimant fence the leader, seal a remote follower, and read its
persisted tail through the bounded witness reader; sealing before the claim
is rejected. The test also confirms that a recovery claimant must advertise
Cellule's node-log protocol even when it offers no follower bytes. With an
opt-in persistent store, the serving binary now advertises that protocol but
zero follower bytes unless experimental follower durability is also enabled.
It can therefore claim recovery without being recruited for write
acknowledgments. The process test now exercises successor overlay attachment;
retirement under concurrent load still needs qualification.

The lease-bound `PublishedNodeLogAuthority` adapter can enroll a follower set
and apply the directory's activation, coverage, and close transitions. It
serializes those mutations with heartbeat refreshes and reloads the exact
session after an ambiguous CAS. `PeerNodeDurabilityProvider` gives Cellule's
host supervisor the enrolled members, authority, transport, and lease for
each epoch. The server installs it before `CellNode::start` and uses a startup
gate to keep recruitment disabled until recovery finishes. With the option
enabled, ready receivers advertise their remaining disk budget. Adding a
local in-process follower under a second logical node ID would not provide
an independent failure domain and must not be used as a production durability
shortcut.

`recover_fenced_node_log` now composes Cellule's bounded witness reader,
tenant catalog inventory, overlay manifest pinning, and final directory seal
for a caller that already holds a fenced recovery claim. It resolves every
authenticated Cell scope through a durable tenant catalog and rejects missing,
ambiguous, or over-limit inventory before attaching an overlay. When normal
Cell takeover encounters an active, untiered node log, the opt-in serving path
claims fenced recovery, runs this coordinator, and only then passes its
takeover proof to Cellule. A product test now captures a real account Cell
frame, appends it to a persistent follower lane, and verifies that a
successor restores the untiered commit before takeover. A separate
[server process test](../tests/server_binary/follower_durability.rs) now kills
the owner after SDK success while its data Cell object uploads are withheld.
The replacement recovers PutItem, UpdateItem return values, DeleteItem,
BatchWriteItem, and a same-partition TransactWriteItems outcome. Replaying the
transaction preserves its conditional-insert result, and the AWS Streams CLI
finds exactly one corresponding record for each tested mutation. All nodes use
separate processes, keys, and persistent directories on one workstation.

This test also exposed an S3 configuration gap: recovery's immutable overlay
pinning requires conditional copy support. BeyondDB now uses Cellule's S3
provider builder, which configures multipart conditional copies. No dependency
source was patched.

The signed SDK component test in
[`tests/peer_network/residency/follower_durability.rs`](../tests/peer_network/residency/follower_durability.rs)
now connects the real host provider to two persistent followers over mTLS.
It withholds only the data Cell's immutable object uploads and verifies that
the signed `PutItem` succeeds while the object root remains unchanged. After
fencing the owner and stopping renewal, a successor recovers the item through
the authorized follower tail. A matching object-only control uses the same
fixture and confirms that the write waits for publication.

This test runs the nodes within one process. It establishes the composition
of enrollment, fsync proof, SDK acknowledgement, and successor recovery;
it does not by itself establish process-crash durability, independent failure
domains, transaction or stream recovery, or release throughput. The separate
server test above covers the listed process-crash cases. The provider must be
installed during node startup, before `CellNode::start`. Cellule selects a
complete follower ensemble from the live fleet, so this fixture's additional
frontend means two eligible followers are required for recruitment.

## Verification before comparing throughput

1. Prove append, duplicate append, lost acknowledgement, seal, tail paging,
   retirement, wrong epoch, and forged peer rejection against persisted
   follower lanes.
2. Run at least three BeyondDB nodes with separate data directories and
   authenticated private endpoints. Kill an owner after an acknowledged
   signed SDK write and verify the item, stream intent, and transaction result
   through a replacement owner before reporting that API as supported in
   follower mode.
3. Repeat the full signed boto3 API fixture on a release build with raw JSON,
   errors, p50/p95/p99, host and object-store load, node count, follower proof
   source, and background-work logs. Run sustained mixed traffic and hot-key
   cases as well as short closed-loop samples.
4. Compare every API and client count with a fresh file-backed ExtendDB SQLite
   fixture. Report the differing IAM and durability contracts. A local
   three-process fixture establishes functionality, not fleet-scale capacity
   or independent-host fault tolerance.

The all-API SQLite performance objective remains open. Follower durability is
the principal architectural path to test for durable write latency; its actual
gain must be measured after the complete recovery contract works.
