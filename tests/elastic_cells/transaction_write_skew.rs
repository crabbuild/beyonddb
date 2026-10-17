use crate::local_node;
use bdb_cellar_runtime::{
    MutationIdentity,
    client::CellClient,
    identity::{CellTarget, RequestId},
};
use beyonddb::*;
use extenddb_core::types::AttributeValue;
use std::sync::{Arc, atomic::AtomicI64};
fn identity(byte: u8) -> MutationIdentity {
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    MutationIdentity {
        request_id: RequestId::from_bytes([byte; 16]),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    }
}

pub(super) async fn assert_condition_checks_prevent_write_skew(
    client: &CellClient,
    storage: &CellStorage,
    table: &TableRecord,
    participants: &[(CellTarget, CoordinatorParticipant)],
) {
    let account_id = "123456789012";
    let condition = serde_json::json!({
        "expression": {"Compare": {
            "left": {"Path": [{"Attribute": "value"}]},
            "op": "Eq",
            "right": {"Placeholder": "enabled"},
        }},
        "maps": {"names": {}, "values": {"enabled": {"N": "1"}}},
    });
    let requests: Vec<Vec<_>> = (0..2)
        .map(|checked| {
            participants
                .iter()
                .enumerate()
                .map(|(position, (_, participant))| {
                    let mut participant = participant.clone();
                    let op = &mut participant.operations[0];
                    let TransactionOperation::Put(input) = &mut op.operation else {
                        unreachable!()
                    };
                    op.index = u8::try_from(position).unwrap();
                    if position == checked {
                        op.operation = TransactionOperation::ConditionCheck(
                            serde_json::from_value(serde_json::json!({
                                "table_name": table.table_name,
                                "table_id": table.id,
                                "key": extenddb_core::types::extract_key(&input.item, &table.key_schema),
                                "condition": condition,
                            }))
                            .unwrap(),
                        );
                    } else {
                        input.item.insert("value".into(), AttributeValue::N("0".into()));
                    }
                    participant
                })
                .collect()
        })
        .collect();
    // Both keys start enabled. T1 checks A and disables B; T2 checks B and
    // disables A. Pause T1 after its check so T2 encounters that exact intent.
    for (byte, request) in [
        (195, &requests[0]),
        (196, &requests[1]),
        (197, &requests[1]),
    ] {
        let id = [byte; 16];
        let coordinator = coordinator_target(account_id, &id).unwrap();
        transaction_command!(
            client,
            BeginCrossCellTransaction,
            &coordinator,
            identity(byte),
            Json(BeginCrossCellTransactionInput {
                account_id: account_id.into(),
                transaction_id: id,
                token: None,
                participants: request.clone(),
            }),
        )
        .await
        .unwrap();
    }
    let first = [195; 16];
    let coordinator = coordinator_target(account_id, &first).unwrap();
    let prepared = transaction_command!(
        client,
        PreparePartitionTransaction,
        &participants[0].0,
        identity(195),
        Json(PreparePartitionTransactionInput {
            table_id: table.id.clone(),
            epoch: 1,
            transaction_id: first,
            coordinator_cell: *coordinator.cell_id().as_bytes(),
            coordinator_key: first.to_vec(),
            operations: vec![requests[0][0].operations[0].operation.clone()],
        }),
    )
    .await
    .unwrap();
    assert_eq!(prepared.output.0, PrepareTransactionOutcome::Prepared);

    let blocked = [196; 16];
    assert_eq!(
        storage
            .resume_cross_cell_transaction(account_id, &blocked, blocked)
            .await
            .unwrap(),
        CoordinatorDecision::Abort {
            index: Some(0),
            reason: Some(TransactionFailure::Conflict),
        }
    );
    assert_eq!(
        storage
            .resume_cross_cell_transaction(account_id, &first, first)
            .await
            .unwrap(),
        CoordinatorDecision::Commit
    );
    // The later attempt evaluates B at prepare, even though its BEGIN predates
    // T1's COMMIT. Admission must never capture an unprotected condition result.
    let retry = [197; 16];
    assert!(matches!(
        storage.resume_cross_cell_transaction(account_id, &retry, retry).await.unwrap(),
        CoordinatorDecision::Abort {
            index: Some(1),
            reason: Some(TransactionFailure::ConditionFailed(Some(item))),
        } if item["value"] == AttributeValue::N("0".into())
    ));
    for (position, (target, participant)) in participants.iter().enumerate() {
        let TransactionOperation::Put(original) = &participant.operations[0].operation else {
            unreachable!()
        };
        let mut expected = original.item.clone();
        expected.insert(
            "value".into(),
            AttributeValue::N(if position == 0 { "1" } else { "0" }.into()),
        );
        assert_eq!(
            client
                .query::<PartitionGet>(
                    target,
                    None,
                    Json(PartitionGetInput {
                        table_id: table.id.clone(),
                        epoch: 1,
                        key: extenddb_core::types::extract_key(&original.item, &table.key_schema),
                    })
                )
                .await
                .unwrap()
                .output
                .0,
            PartitionGetOutcome::Found(Some(expected))
        );
        // Restore the shared fixture and prove both terminal paths released locks.
        client
            .command::<PartitionPut>(
                target,
                identity(198),
                Json(PartitionPutInput {
                    table_id: table.id.clone(),
                    epoch: 1,
                    item: original.item.clone(),
                    condition: None,
                }),
            )
            .await
            .unwrap();
    }
}

#[test]
fn condition_checks_prevent_cross_cell_write_skew() {
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    host.block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let clock = Arc::new(AtomicI64::new(identity(1).issued_at_ms));
        let (node, client) =
            local_node::start(directory.path(), clock, host.handle(), core.handle())
                .await
                .unwrap();
        let fixture = crate::transaction_fixture::table(&client, "WriteSkew").await;
        assert_condition_checks_prevent_write_skew(
            &client,
            &fixture.storage,
            &fixture.table,
            &fixture.participants,
        )
        .await;
        local_node::stop(node).await.unwrap();
    });
}
