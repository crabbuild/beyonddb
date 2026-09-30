# Direct RustFS PUT diagnostic

This probe isolates the object store from BeyondDB's DynamoDB request path.
It sent signed S3 `PutObject` calls for unique 1 KiB keys to a fresh container
pinned to `ghcr.io/rustfs/rustfs:1.0.0-glibc@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`.
Each client used its own boto3 S3 client, disabled SDK retries, and ran a
five-second closed loop. All calls succeeded.

| Clients | Completed PUTs | PUTs/s | p50 | p95 | p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 117 | 23.4 | 31.53 ms | 89.45 ms | 104.34 ms |
| 8 | 719 | 142.8 | 51.56 ms | 109.95 ms | 128.77 ms |
| 32 | 1,579 | 308.9 | 88.59 ms | 190.72 ms | 246.70 ms |

The one-minute load average climbed from 38.7 to 43.3 on the 12-logical-CPU
host while other virtual machines were active. These numbers are **not** an
idle-host RustFS capacity measurement or a bound on BeyondDB throughput.
BeyondDB's Cell publication includes more work than one S3 PUT. The probe
does show that object-store timing is material under the same host conditions
as the recent BeyondDB release tests; tuning only item SQL cannot establish
SQLite write parity on this fixture.

The pinned Cellule revision exposes a node-log durability supervisor and
follower fsync proofs. BeyondDB currently does not enroll followers or provide
the authenticated node-log transport and authority integration required to
use that path. A production comparison needs a multi-node fixture, verified
recovery after owner loss, and sustained mixed workloads before claiming that
follower durability improves the write gap.

See [raw results](results.json). This probe used a fresh bucket, so it does
not include any BeyondDB table or item operations.
