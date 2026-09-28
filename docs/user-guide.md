# Use BeyondDB with the AWS CLI

This guide assumes a node is running with a bootstrapped access key. Follow
the [deployment guide](deployment.md) first. BeyondDB accepts signed DynamoDB
JSON requests at its `public_endpoint`; the AWS CLI's `--endpoint-url` option
selects that endpoint. The examples use a table with a string partition key.

## Connect

```sh
export AWS_ACCESS_KEY_ID='your-bootstrapped-key-id'
export AWS_SECRET_ACCESS_KEY='your-bootstrapped-secret'
export AWS_DEFAULT_REGION='us-east-1'
export AWS_CA_BUNDLE='/etc/beyonddb/public-ca.crt'
export BEYONDDB_ENDPOINT='https://ddb.example.com:8000'

aws dynamodb list-tables --endpoint-url "$BEYONDDB_ENDPOINT"
```

The region must match the configured region and the IAM policy's resource
ARNs. `AWS_CA_BUNDLE` must trust the public listener certificate. Omit that
variable for a loopback-only `http://` listener. Do not use `--no-sign-request`:
BeyondDB authenticates SigV4 credentials through its credential Cell.

## Create a table and wait for it

```sh
aws dynamodb create-table \
  --table-name Notes \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --endpoint-url "$BEYONDDB_ENDPOINT"

aws dynamodb wait table-exists \
  --table-name Notes \
  --endpoint-url "$BEYONDDB_ENDPOINT"

aws dynamodb describe-table \
  --table-name Notes \
  --endpoint-url "$BEYONDDB_ENDPOINT"
```

The serving binary provisions `initial_partitions` data Cells for a new routed
table. Creation can report `CREATING` while range publication finishes. The
configured count affects new table generations only.

## Write and read an item

```sh
aws dynamodb put-item \
  --table-name Notes \
  --item '{"pk":{"S":"note-1"},"body":{"S":"hello"},"version":{"N":"1"}}' \
  --endpoint-url "$BEYONDDB_ENDPOINT"

aws dynamodb get-item \
  --table-name Notes \
  --key '{"pk":{"S":"note-1"}}' \
  --consistent-read \
  --endpoint-url "$BEYONDDB_ENDPOINT"

aws dynamodb update-item \
  --table-name Notes \
  --key '{"pk":{"S":"note-1"}}' \
  --update-expression 'SET #body = :body' \
  --condition-expression '#version = :expected' \
  --expression-attribute-names '{"#body":"body","#version":"version"}' \
  --expression-attribute-values '{":body":{"S":"updated"},":expected":{"N":"1"}}' \
  --return-values ALL_NEW \
  --endpoint-url "$BEYONDDB_ENDPOINT"
```

Conditions and update expressions execute in the owning Cell command against
the item version being changed. A failed condition does not commit an item
mutation. To read several items, use `BatchGetItem`; to write several,
`BatchWriteItem`. For atomic multi-item work, use `TransactWriteItems`.

## Query and scan

```sh
aws dynamodb query \
  --table-name Notes \
  --key-condition-expression 'pk = :pk' \
  --expression-attribute-values '{":pk":{"S":"note-1"}}' \
  --endpoint-url "$BEYONDDB_ENDPOINT"

aws dynamodb scan \
  --table-name Notes \
  --page-size 25 \
  --endpoint-url "$BEYONDDB_ENDPOINT"
```

For a table with a sort key, Query also supports ordered sort-key predicates,
forward and reverse pages, and continuation. The CLI follows pages by default;
`--no-paginate` returns one CLI page when inspecting continuation behavior.
Scan is a table walk and can cost more work than a keyed Query.

## TTL and Streams

To configure TTL on the numeric Unix-seconds attribute `expires_at`:

```sh
aws dynamodb update-time-to-live \
  --table-name Notes \
  --time-to-live-specification Enabled=true,AttributeName=expires_at \
  --endpoint-url "$BEYONDDB_ENDPOINT"

aws dynamodb describe-time-to-live \
  --table-name Notes \
  --endpoint-url "$BEYONDDB_ENDPOINT"
```

The background worker performs bounded sweeps. Expiration is asynchronous;
the TTL time is not a read-time visibility deadline. A stream can be enabled
when creating a table:

```sh
aws dynamodb create-table \
  --table-name StreamNotes \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES \
  --endpoint-url "$BEYONDDB_ENDPOINT"

aws dynamodbstreams list-streams \
  --table-name StreamNotes \
  --endpoint-url "$BEYONDDB_ENDPOINT"
```

Use `aws dynamodbstreams describe-stream`, `get-shard-iterator`, and
`get-records` with the returned stream ARN and shard IDs. The current stream
read path covers table-created policies and retained generations; stream
enable/disable through `UpdateTable` and full lifecycle qualification are
unfinished. See [API coverage](api.md#streams) before using Streams for a
consumer that must never miss an event.

## Errors and recovery

- An authentication or authorization error can mean the access key is unknown,
  inactive, signed for the wrong region, or denied by its inline policy or
  permission boundary. Check the account, table ARN, and bootstrap policy.
- A retryable service error can occur while a Cell owner is unavailable,
  recovering, or at its admission limit. A timed-out write may have committed;
  use conditions or a transaction client token when retry semantics matter.
- A table can remain `CREATING` or `DELETING` while durable provisioning or
  cleanup continues. Inspect `DescribeTable`; do not assume a timed-out
  control-plane request was rolled back.

The [API matrix](api.md) lists explicit unsupported cases. The
[implementation status](implementation-status.md) records focused SDK and
restart evidence, including known load failures.
