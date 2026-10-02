use super::*;
use beyonddb::{
    ParticipantTransactionState, PreparePartitionTransaction, PreparePartitionTransactionBounded,
    PreparePartitionTransactionInput, PrepareTransactionOutcome, PutItemInput, ReadPartitionState,
    ReadPartitionTransaction, ReadTransactionInput, ResolvePartitionTransaction,
    ResolveTransactionInput, TransactionCommandInput, TransactionOperation,
};
use cellule_runtime::{MutationIdentity, identity::RequestId};

const ACCOUNT: &str = "123456789012";

fn identity() -> MutationIdentity {
    let issued_at_ms = now_ms();
    MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms,
        expires_at_ms: issued_at_ms + 60_000,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_bounded_prepare_preserves_large_failure_images_and_token_recovery() {
    use aws_sdk_dynamodb::types::{Put, ReturnValuesOnConditionCheckFailure, TransactWriteItem};

    let fixture = Fixture::with_capacity(1, 16).await;
    let sdk = super::provisioning::sdk_without_retries(&fixture);
    let old = SdkItem::from([
        ("id".into(), AwsAttributeValue::S("wide-condition".into())),
        (
            "payload".into(),
            AwsAttributeValue::S("\0".repeat(384 * 1024)),
        ),
    ]);
    sdk.put_item()
        .table_name("Residency")
        .set_item(Some(old.clone()))
        .send()
        .await
        .unwrap();
    let write = sdk
        .transact_write_items()
        .client_request_token("bounded-wide-condition")
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("Residency")
                        .item("id", AwsAttributeValue::S("wide-staged".into()))
                        .item("value", AwsAttributeValue::S("committed".into()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("Residency")
                        .item("id", old["id"].clone())
                        .condition_expression("attribute_not_exists(id)")
                        .return_values_on_condition_check_failure(
                            ReturnValuesOnConditionCheckFailure::AllOld,
                        )
                        .build()
                        .unwrap(),
                )
                .build(),
        );
    let error = write.clone().send().await.unwrap_err();
    let aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError::TransactionCanceledException(error) = error.as_service_error().unwrap() else {
        panic!("expected condition cancellation");
    };
    assert_eq!(
        error.cancellation_reasons()[1].code(),
        Some("ConditionalCheckFailed")
    );
    assert_eq!(error.cancellation_reasons()[1].item(), Some(&old));
    assert!(
        sdk.get_item()
            .table_name("Residency")
            .key("id", AwsAttributeValue::S("wide-staged".into()))
            .consistent_read(true)
            .send()
            .await
            .unwrap()
            .item
            .is_none()
    );
    // Release and restore the data owner after the failed transaction. The old
    // image remains intact and cancellation leaves no blocking write locks.
    let id = super::provisioning::table_id(&fixture, "Residency").await;
    let range = super::cell_models::ranges(&fixture, "Residency")
        .await
        .remove(0);
    fixture.data[0].0.drain().await.unwrap();
    fixture
        .provisioner
        .admit_existing_partition(ACCOUNT, &id, &range.partition_id)
        .await
        .unwrap();
    assert_eq!(
        sdk.get_item()
            .table_name("Residency")
            .key("id", old["id"].clone())
            .consistent_read(true)
            .send()
            .await
            .unwrap()
            .item,
        Some(old.clone())
    );
    sdk.delete_item()
        .table_name("Residency")
        .key("id", old["id"].clone())
        .send()
        .await
        .unwrap();
    // Canceled token reuse keeps the identical request fingerprint.
    write.clone().send().await.unwrap();
    fixture
        .provisioner
        .admit_existing_partition(ACCOUNT, &id, &range.partition_id)
        .await
        .unwrap()
        .drain()
        .await
        .unwrap();
    fixture
        .provisioner
        .admit_existing_partition(ACCOUNT, &id, &range.partition_id)
        .await
        .unwrap();
    let item = sdk
        .get_item()
        .table_name("Residency")
        .key("id", AwsAttributeValue::S("wide-staged".into()))
        .consistent_read(true)
        .send()
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(item["value"], AwsAttributeValue::S("committed".into()));
    sdk.put_item()
        .table_name("Residency")
        .item("id", AwsAttributeValue::S("wide-staged".into()))
        .item("value", AwsAttributeValue::S("newer".into()))
        .send()
        .await
        .unwrap();
    write.send().await.unwrap();
    assert_eq!(
        sdk.get_item()
            .table_name("Residency")
            .key("id", AwsAttributeValue::S("wide-staged".into()))
            .consistent_read(true)
            .send()
            .await
            .unwrap()
            .item
            .unwrap()["value"],
        AwsAttributeValue::S("newer".into())
    );
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eight_in_flight_prepares_fit_the_participant_mailbox() {
    // Preserve the old wide path as a control: its 4 MiB reply envelope admits
    // only three held requests. The bounded path must admit all eight.
    for bounded in [false, true] {
        held_prepares(bounded).await;
    }
}

async fn held_prepares(bounded: bool) {
    use super::forward_cache::{CountedAuthority, CreationGate};

    let store = Arc::new(CountedAuthority::default());
    let fixture = Fixture::with_store_capacity_and_peer_cache(1, store.clone(), 8, true).await;
    let remote = super::provisioning::Remote::new(&fixture).await;
    let client = remote.client_with_cache(&fixture, true);
    let handle = &fixture.data[0].0;
    let entry = handle.catalog().entry();
    let account = account_target(ACCOUNT).unwrap();
    let target = cellule_runtime::identity::CellTarget::new(
        account.tenant(),
        beyonddb::APPLICATION_ID,
        entry.namespace(),
        entry.partition(),
    )
    .unwrap();
    let spec = client
        .query::<ReadPartitionState>(&target, None, Json(()))
        .await
        .unwrap()
        .output
        .0
        .unwrap()
        .spec;
    let coordinator_cell = *account.cell_id().as_bytes();
    let gate = CreationGate {
        paths: std::collections::HashSet::from([fixture
            .layout
            .control_path(handle.cell_id().as_bytes())]),
        entered: Arc::new(tokio::sync::Semaphore::new(0)),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    *store.publication_gate.lock().unwrap() = Some(gate.clone());
    let before_retained = fixture.node.runtime().stats().retained_bytes();
    let mut tasks = Vec::new();
    for index in 0..8_u8 {
        let client = client.clone();
        let target = target.clone();
        let transaction_id = [170 + index; 16];
        let input = PreparePartitionTransactionInput {
            table_id: spec.table.id.clone(),
            epoch: spec.epoch,
            transaction_id,
            coordinator_cell,
            coordinator_key: transaction_id.to_vec(),
            operations: vec![TransactionOperation::Put(PutItemInput {
                table_name: "Residency".into(),
                table_id: spec.table.id.clone(),
                item: Item::from([
                    (
                        "id".into(),
                        AttributeValue::S(format!("prepare-pressure-{index}")),
                    ),
                    ("value".into(), AttributeValue::S("staged".into())),
                ]),
                condition: None,
            })],
        };
        tasks.push(tokio::spawn(async move {
            (
                transaction_id,
                if bounded {
                    client
                        .command::<PreparePartitionTransactionBounded>(
                            &target,
                            identity(),
                            Json(input),
                        )
                        .await
                } else {
                    client
                        .command::<PreparePartitionTransaction>(
                            &target,
                            identity(),
                            Json(TransactionCommandInput::Inline(input)),
                        )
                        .await
                },
            )
        }));
        if index == 0 {
            // Hold the first real command's publication, and thus its mailbox
            // reservation, while the remaining requests reach the owner.
            tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
        }
    }
    // Prove simultaneous admission before releasing publication. The node
    // ledger includes each actual request's reserved reply bytes; a small
    // bounded prepare batch cannot reach this threshold with fewer than eight.
    let all_held = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let ready = if bounded {
                fixture.node.runtime().stats().retained_bytes() >= before_retained + 8 * 64 * 1024
            } else {
                tasks.iter().filter(|task| task.is_finished()).count() >= 5
            };
            if ready {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let held_retained = fixture.node.runtime().stats().retained_bytes();
    *store.publication_gate.lock().unwrap() = None;
    gate.release.add_permits(64);
    let mut accepted = Vec::new();
    let mut refused = Vec::new();
    for task in tasks {
        let (transaction_id, result) =
            tokio::time::timeout(std::time::Duration::from_secs(30), task)
                .await
                .unwrap()
                .unwrap();
        match result {
            Ok(result) => {
                assert_eq!(result.output.0, PrepareTransactionOutcome::Prepared);
                accepted.push(transaction_id);
            }
            Err(error) => {
                assert!(
                    matches!(
                        error,
                        cellule_runtime::client::InvocationError::NotStarted(
                            cellule_runtime::Error::Capacity(_)
                        )
                    ),
                    "unexpected prepare failure: {error:?}"
                );
                refused.push(format!("{error:?}"));
            }
        }
    }
    // Check durable prepares after owner transfer, then abort every accepted
    // intent before asserting the concurrency result or shutting down.
    handle.drain().await.unwrap();
    remote
        .provisioner
        .admit_existing_partition(ACCOUNT, &spec.table.id, &spec.partition_id)
        .await
        .unwrap();
    for transaction_id in &accepted {
        let observed = client
            .query::<ReadPartitionTransaction>(
                &target,
                None,
                Json(ReadTransactionInput {
                    transaction_id: *transaction_id,
                    coordinator_cell,
                }),
            )
            .await
            .unwrap();
        assert_eq!(observed.output.0, ParticipantTransactionState::Prepared);
        client
            .command::<ResolvePartitionTransaction>(
                &target,
                identity(),
                Json(ResolveTransactionInput {
                    transaction_id: *transaction_id,
                    coordinator_cell,
                    commit: false,
                }),
            )
            .await
            .unwrap();
    }
    remote.shutdown().await;
    fixture.shutdown().await;
    println!(
        "eight held prepares bounded={bounded}: accepted={}, held={all_held}, retained={before_retained}->{held_retained}, refused={refused:?}",
        accepted.len()
    );
    assert!(
        all_held,
        "requests did not reach admission while publication was held"
    );
    assert_eq!(
        accepted.len(),
        if bounded { 8 } else { 3 },
        "prepare reply reservations refused: {refused:?}"
    );
}
