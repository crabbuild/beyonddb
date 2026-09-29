# Use BeyondDB with the AWS CLI

Use these commands to connect, create a table, change an item, and inspect time to live (TTL) and Streams behavior. First [deploy a node](deployment.md) and bootstrap an access key. BeyondDB accepts signed DynamoDB JSON requests at `public_endpoint`; pass that address with `--endpoint-url`. The examples use a string partition key named `pk`.

```text
AWS CLI -- SigV4 --> public endpoint -- route --> owning Cell
                                                  |
                                                  +--> durable result
```

Run the commands in one shell so later examples can use `BEYONDDB_ENDPOINT` and the AWS credential variables.

## Connect

Set the bootstrapped DynamoDB credentials in your client shell. These are distinct from any credentials the server uses to access its object store.

```sh
export AWS_ACCESS_KEY_ID='your_bootstrapped_key_id'
export AWS_SECRET_ACCESS_KEY='your_bootstrapped_secret'
export AWS_DEFAULT_REGION='us-east-1'
export AWS_CA_BUNDLE='/etc/beyonddb/public-ca.crt'
export BEYONDDB_ENDPOINT='https://ddb.example.com:8000'

aws dynamodb list-tables --endpoint-url "$BEYONDDB_ENDPOINT"
```

Set the region to the node's configured region and the policy's resource ARNs. `AWS_CA_BUNDLE` must trust the public listener certificate. For a loopback `http://` endpoint, omit that variable and change `BEYONDDB_ENDPOINT`. Keep request signing enabled: BeyondDB looks up SigV4 credentials in a credential Cell.

## Create a table and wait for it

Create `Notes`, then wait for its route to become active before writing. This example uses on-demand billing; it does not imply AWS pricing semantics.

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

The serving binary provisions `initial_partitions` data Cells for a new routed table. `DescribeTable` can report `CREATING` until range publication finishes. Changing `initial_partitions` later affects new table generations only. The node's `max_active_cells` budget must include those data Cells plus account, coordinator, and management Cells; if the budget is too small, provisioning remains pending until capacity is available.

## Write and read an item

Write a DynamoDB attribute-value JSON item, read it consistently, then update it only if its `version` is still `1`.

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

Conditions and update expressions run against the item version being changed inside one Cell command. A failed condition does not commit the mutation. Use `BatchGetItem` or `BatchWriteItem` for independent items; use `TransactWriteItems` when the items must commit atomically.

## Query and scan

Query by partition key when you know it. Scan visits the table in bounded pages and can perform more work.

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

TTL is an asynchronous deletion policy. Streams can record the resulting eligible removals if you enable a stream when creating the table.

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

The background worker performs bounded sweeps. An expired item may still appear in a read before the worker removes it. Enable a stream at table creation:

```sh
aws dynamodb create-table \
  --table-name StreamNotes \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES \
  --endpoint-url "$BEYONDDB_ENDPOINT"

aws dynamodb wait table-exists \
  --table-name StreamNotes \
  --endpoint-url "$BEYONDDB_ENDPOINT"
```

Write an item, then obtain the stream ARN. A routed table can have more than one stream shard, so read every shard returned by `DescribeStream` for this new table:

```sh
aws dynamodb put-item \
  --table-name StreamNotes \
  --item '{"pk":{"S":"stream-note-1"}}' \
  --endpoint-url "$BEYONDDB_ENDPOINT"

STREAM_ARN=$(aws dynamodbstreams list-streams \
  --table-name StreamNotes --endpoint-url "$BEYONDDB_ENDPOINT" \
  --query 'Streams[0].StreamArn' --output text)
for SHARD_ID in $(aws dynamodbstreams describe-stream \
  --stream-arn "$STREAM_ARN" --endpoint-url "$BEYONDDB_ENDPOINT" \
  --query 'StreamDescription.Shards[].ShardId' --output text); do
  ITERATOR=$(aws dynamodbstreams get-shard-iterator \
    --stream-arn "$STREAM_ARN" --shard-id "$SHARD_ID" \
    --shard-iterator-type TRIM_HORIZON \
    --endpoint-url "$BEYONDDB_ENDPOINT" \
    --query 'ShardIterator' --output text)
  aws dynamodbstreams get-records \
    --shard-iterator "$ITERATOR" --endpoint-url "$BEYONDDB_ENDPOINT"
done
```

Check that `STREAM_ARN` and each `ITERATOR` are not `None`. Empty `Records` on one shard does not mean another shard is empty. A long-running consumer must also follow returned `NextShardIterator` values and any `LastEvaluatedShardId` from `DescribeStream`. The current read path covers policies set at table creation and retained generations. Enabling or disabling Streams through `UpdateTable` remains unsupported; full iterator and split-lineage qualification is unfinished. See [Streams API coverage](api.md#streams) before using this path for a consumer that cannot miss an event.

## Errors and recovery

A retry can outlive the request that caused it. Use the response and current table state to distinguish rejected work from an unknown outcome:

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
