use crate::fixture::{ACCOUNT, Fixture, SdkItem, runtimes};
use aws_sdk_dynamodb::types::AttributeValue as AwsAttributeValue;
use beyonddb::*;
use cellar_runtime::{MutationIdentity, identity::RequestId};
use extenddb_core::types::{AttributeValue, Item};
fn identity() -> MutationIdentity {
    let now = crate::native_peer::now_ms();
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    }
}
#[test]
fn saved_images_preserve_identity_wide_items_and_owner_restoration() {
    let (host, core) = runtimes();
    host.block_on(async {
        use beyonddb::{
            BoundedTransactionReadResult, ParticipantTransactionState, ReadPartitionTransaction,
            ReadPartitionTransactionResult, ReadPartitionTransactionResultBounded,
            ReadTransactionResultInput, ReleasePartitionTransactionReads, TransactionReadResult,
        };
        let mut fixture = Fixture::new(host.handle(), core.handle(), 1).await;
        let sdk = fixture.sdk.clone();
        let small = fixture.data[0].1.clone();
        let mut expected = vec![Some(small.clone())];
        for (id, payload) in [
            (
                "saved-binary",
                AwsAttributeValue::B(vec![0xa5; 380 * 1024].into()),
            ),
            (
                "saved-escaped",
                AwsAttributeValue::S("\0".repeat(380 * 1024)),
            ),
        ] {
            let item = SdkItem::from([
                ("id".into(), AwsAttributeValue::S(id.into())),
                ("payload".into(), payload),
            ]);
            sdk.put_item()
                .table_name("Residency")
                .set_item(Some(item.clone()))
                .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
                .send()
                .await
                .unwrap();
            expected.push(Some(item));
        }
        expected.push(None);
        let table_id = fixture.table.id.clone();
        let route = crate::single_leaf_route(
            &fixture.client,
            &account_target(ACCOUNT).unwrap(),
            &table_id,
        )
        .await
        .unwrap();
        assert_eq!(route.partitions.len(), 2);
        let range = route.partitions[0].clone();
        let target = beyonddb::data_target(ACCOUNT, &table_id, &range.partition_id).unwrap();
        let coordinator_cell = *account_target(ACCOUNT).unwrap().cell_id().as_bytes();
        let transaction = ReadTransactionInput {
            transaction_id: [224; 16],
            coordinator_cell,
        };
        let operations = expected
            .iter()
            .map(|item| {
                let id = item
                    .as_ref()
                    .map(|item| item["id"].clone())
                    .unwrap_or_else(|| AwsAttributeValue::S("saved-absent".into()));
                let AwsAttributeValue::S(id) = id else {
                    panic!("string key");
                };
                TransactionOperation::Read(GetItemInput {
                    table_name: "Residency".into(),
                    table_id: table_id.clone(),
                    key: Item::from([("id".into(), AttributeValue::S(id))]),
                })
            })
            .collect();
        fixture
            .client
            .command::<PreparePartitionTransactionBounded>(
                &target,
                identity(),
                Json(PreparePartitionTransactionInput {
                    table_id: table_id.clone(),
                    epoch: range.epoch,
                    transaction_id: transaction.transaction_id,
                    coordinator_cell,
                    coordinator_key: transaction.transaction_id.to_vec(),
                    operations,
                }),
            )
            .await
            .unwrap();
        let input = |position| {
            Json(ReadTransactionResultInput {
                transaction: transaction.clone(),
                position,
            })
        };
        assert_eq!(
            fixture
                .client
                .query::<ReadPartitionTransactionResultBounded>(&target, None, input(0))
                .await
                .unwrap()
                .output,
            BoundedTransactionReadResult::Unavailable
        );
        fixture
            .client
            .command::<ResolvePartitionTransaction>(
                &target,
                identity(),
                Json(ResolveTransactionInput {
                    transaction_id: transaction.transaction_id,
                    coordinator_cell,
                    commit: true,
                }),
            )
            .await
            .unwrap();
        // A saved image is independent of later writes to the live item.
        let mut changed = small;
        changed.insert("value".into(), AwsAttributeValue::S("newer".into()));
        sdk.put_item()
            .table_name("Residency")
            .set_item(Some(changed))
            .send()
            .await
            .unwrap();
        for restored in [false, true] {
            if restored {
                fixture.replace().await;
            }
            let client = fixture.client.clone();
            for (position, item) in expected.iter().enumerate() {
                let request = input(u8::try_from(position).unwrap());
                let bounded = client
                    .query::<ReadPartitionTransactionResultBounded>(&target, None, request.clone())
                    .await
                    .unwrap()
                    .output;
                let wide = client
                    .query::<ReadPartitionTransactionResult>(&target, None, request)
                    .await
                    .unwrap()
                    .output
                    .0;
                let core_item = item.as_ref().map(|item| {
                    item.iter()
                        .map(|(name, value)| {
                            let value = match value {
                                AwsAttributeValue::S(value) => AttributeValue::S(value.clone()),
                                AwsAttributeValue::B(value) => {
                                    AttributeValue::B(value.as_ref().to_vec())
                                }
                                value => panic!("unexpected fixture attribute: {value:?}"),
                            };
                            (name.clone(), value)
                        })
                        .collect::<Item>()
                });
                assert_eq!(wide, TransactionReadResult::Item(core_item.clone()));
                if matches!(position, 1 | 2) {
                    assert_eq!(bounded, BoundedTransactionReadResult::WideRequired);
                } else {
                    assert_eq!(bounded, BoundedTransactionReadResult::Item(core_item));
                }
            }
            for request in [
                input(4),
                Json(ReadTransactionResultInput {
                    transaction: ReadTransactionInput {
                        coordinator_cell: [225; 32],
                        ..transaction.clone()
                    },
                    position: 0,
                }),
            ] {
                assert_eq!(
                    client
                        .query::<ReadPartitionTransactionResultBounded>(&target, None, request)
                        .await
                        .unwrap()
                        .output,
                    BoundedTransactionReadResult::Unavailable
                );
            }
        }
        let client = fixture.client.clone();
        client
            .command::<ReleasePartitionTransactionReads>(
                &target,
                identity(),
                Json(transaction.clone()),
            )
            .await
            .unwrap();
        assert_eq!(
            client
                .query::<ReadPartitionTransactionResultBounded>(&target, None, input(0))
                .await
                .unwrap()
                .output,
            BoundedTransactionReadResult::Unavailable
        );
        assert_eq!(
            client
                .query::<ReadPartitionTransaction>(&target, None, Json(transaction))
                .await
                .unwrap()
                .output
                .0,
            ParticipantTransactionState::Committed
        );
        fixture.shutdown().await;
    });
}

#[test]
fn signed_cross_cell_saved_images_and_wide_fallback_survive_owner_change() {
    let (host, core) = runtimes();
    host.block_on(async {
        use aws_sdk_dynamodb::types::{Get, TransactGetItem};
        let mut fixture = Fixture::new(host.handle(), core.handle(), 2).await;
        let mut sdk = fixture.sdk.clone();
        let mut expected = fixture
            .data
            .iter()
            .map(|(_, item)| Some(item.clone()))
            .collect::<Vec<_>>();
        let mut keys = expected
            .iter()
            .map(|item| item.as_ref().unwrap()["id"].clone())
            .collect::<Vec<_>>();
        keys.push(AwsAttributeValue::S("saved-sdk-absent".into()));
        expected.push(None);
        let reads = keys
            .into_iter()
            .map(|id| {
                TransactGetItem::builder()
                    .get(
                        Get::builder()
                            .table_name("Residency")
                            .key("id", id)
                            .build()
                            .unwrap(),
                    )
                    .build()
            })
            .rev()
            .collect::<Vec<_>>();
        for wide in [false, true] {
            if wide {
                for (position, item) in expected.iter_mut().flatten().enumerate() {
                    item.insert(
                        "payload".into(),
                        if position == 0 {
                            AwsAttributeValue::B(vec![0xa5; 380 * 1024].into())
                        } else {
                            AwsAttributeValue::S("\0".repeat(380 * 1024))
                        },
                    );
                    sdk.put_item()
                        .table_name("Residency")
                        .set_item(Some(item.clone()))
                        .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
                        .send()
                        .await
                        .unwrap();
                }
            }
            let read = sdk
                .transact_get_items()
                .set_transact_items(Some(reads.clone()))
                .send()
                .await
                .unwrap();
            assert_eq!(read.responses().len(), expected.len());
            for (result, item) in read.responses().iter().zip(expected.iter().rev()) {
                assert_eq!(result.item(), item.as_ref());
            }
        }
        fixture.replace().await;
        sdk = fixture.sdk.clone();
        let restored = sdk
            .transact_get_items()
            .set_transact_items(Some(reads))
            .send()
            .await
            .unwrap();
        assert_eq!(restored.responses().len(), expected.len());
        for (result, item) in restored.responses().iter().zip(expected.iter().rev()) {
            assert_eq!(result.item(), item.as_ref());
        }
        fixture.shutdown().await;
    });
}
