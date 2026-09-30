# Follower durability for low-latency writes

## Why this work is needed

The current BeyondDB server acknowledges a mutation after its Cell state is
published to the configured object store. On the recent four-partition local
RustFS fixture, this path remained far below file-backed ExtendDB SQLite for
`PutItem`, `UpdateItem`, `BatchWriteItem`, and transactions. A separate
[direct RustFS PUT probe](../benchmarks/2026-09-29-rustfs-put-probe/README.md)
also observed material object-store latency under heavy host contention. Those
short runs do not establish an idle-host throughput limit, but removing small
amounts of item SQL cannot eliminate the object publication round trip.

The pinned Cellule revision already exposes a node-log durability supervisor,
follower stores, and a durability gate. BeyondDB has a lease-bound authority
adapter and an opt-in inbound follower receiver, but it has not completed the
outbound transport, enrollment provider, or owner recovery. No follower mode
should be advertised or enabled until that integration is proven.

```text
Current serving path                  Proposed multi-node path

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
recovery authority before touching a lane. A focused test covers a durable
append, duplicate append, restart of the follower store, wrong certificate,
wrong epoch, and a seal attempt before owner fencing. The outbound transport
and full seal/tail/retirement recovery tests remain open.

The lease-bound `PublishedNodeLogAuthority` adapter can enroll a follower set
and apply the directory's activation, coverage, and close transitions. It
serializes those mutations with heartbeat refreshes and reloads the exact
session after an ambiguous CAS. The serving binary does not yet install a
durability provider or use that adapter, and it advertises no usable follower
capacity. Adding a local in-process follower under a second logical node ID
would not provide an independent failure domain and must not be used as a
production durability shortcut.

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
