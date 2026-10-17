use crate::{local_node, transaction_fixture};
use bdb_cellar_runtime::client::{CellClient, Invocation, InvocationTransport};
use beyonddb::*;
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use extenddb_core::{
    expression::{Expr, ExpressionMaps, PathElement},
    types::{AttributeValue, Item, ReturnValuesOnConditionCheckFailure, TableKeyInfo},
};
use extenddb_storage::{
    DataEngine, IdempotencyKey, TransactGetOp, TransactWriteOp, error::StorageError,
};
use http_body_util::{BodyExt, Full};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU8, Ordering},
    },
};

pub(super) async fn assert_lost_replies_and_canceled_token_reuse(
    base: &CellClient,
    provisioner: Arc<CellInitialPartitionProvisioner>,
    infos: [TableKeyInfo; 2],
) {
    let phase_client = |enabled, lost| {
        base.map_transport(|inner| {
            Arc::new(DropPhaseReplies {
                inner,
                lost,
                enabled,
            })
        })
    };
    // Drop only completion replies so BEGIN and COMMIT are acknowledged and
    // the fresh-write shortcut is exercised. A missing participant reply can
    // be recovered by observing its durable state; a missing coordinator
    // receipt must leave the first call uncertain until token replay.
    for phase in [8, 128] {
        let dropped = Arc::new(std::sync::atomic::AtomicU8::new(0));
        let fresh = CellStorage::new(phase_client(phase, dropped.clone()), "us-east-1")
            .with_transaction_coordinators(provisioner.clone());
        let item = Item::from([
            (
                "id".into(),
                AttributeValue::S(format!("fresh-completion-{phase}")),
            ),
            ("value".into(), AttributeValue::N("7".into())),
        ]);
        let maps = ExpressionMaps::default();
        let ops = infos
            .iter()
            .map(|info| TransactWriteOp::Put {
                key_info: info,
                item: &item,
                condition: None,
                maps: &maps,
                return_values_on_ccf: Default::default(),
                stream: None,
            })
            .collect::<Vec<_>>();
        let token_text = format!("fresh-completion-{phase}");
        let token = || IdempotencyKey {
            account_id: "123456789012",
            token: &token_text,
            fingerprint: "two-participant-write",
        };
        let result = fresh.transact_write_items(&ops, Some(token())).await;
        if phase == 8 {
            result.unwrap();
        } else {
            assert!(
                matches!(result, Err(StorageError::Transient(_))),
                "lost coordinator receipt cannot prove completion: {result:?}"
            );
        }
        assert_eq!(dropped.load(Ordering::SeqCst), phase);
        assert!(matches!(
            fresh.transact_write_items(&ops, Some(token())).await,
            Err(StorageError::IdempotentReplay)
        ));
        for info in &infos {
            assert_eq!(
                fresh
                    .get_item(info, &Item::from([("id".into(), item["id"].clone())]))
                    .await
                    .unwrap(),
                Some(item.clone())
            );
        }
    }
    let lost = Arc::new(std::sync::atomic::AtomicU8::new(0));
    let client = phase_client(127, lost.clone());
    // Native ingress routes all decision and intent reads to the authority.
    // The replacement adapter exposes no replica-read override.
    let storage = CellStorage::new(client.clone(), "us-east-1")
        .with_transaction_coordinators(provisioner.clone());
    let maps = ExpressionMaps::default();
    let item = Item::from([("id".into(), AttributeValue::S("lost-admission".into()))]);
    let ops = infos
        .iter()
        .map(|info| TransactWriteOp::Put {
            key_info: info,
            item: &item,
            condition: None,
            maps: &maps,
            return_values_on_ccf: Default::default(),
            stream: None,
        })
        .collect::<Vec<_>>();
    let token = |fingerprint| IdempotencyKey {
        account_id: "123456789012",
        token: "lost-public-phases",
        fingerprint,
    };
    storage
        .transact_write_items(&ops, Some(token("both-items")))
        .await
        .unwrap();
    assert_eq!(
        lost.load(Ordering::SeqCst),
        15,
        "inline BEGIN, prepare, decision and resolution replies were dropped after dispatch"
    );
    for info in &infos {
        assert_eq!(
            storage.get_item(info, &item).await.unwrap(),
            Some(item.clone())
        );
    }
    assert!(matches!(
        storage
            .transact_write_items(&ops, Some(token("both-items")))
            .await,
        Err(StorageError::IdempotentReplay)
    ));
    assert!(matches!(
        storage
            .transact_write_items(&ops, Some(token("changed")))
            .await,
        Err(StorageError::IdempotentMismatch)
    ));

    lost.store(0, Ordering::SeqCst);
    assert_large_participant_payloads(&storage, &infos).await;
    assert_eq!(lost.load(Ordering::SeqCst), 127);

    let new_item = Item::from([("id".into(), AttributeValue::S("canceled-retry".into()))]);
    let not_exists = Expr::Function {
        name: "attribute_not_exists".into(),
        args: vec![Expr::Path(vec![PathElement::Attribute("id".into())])],
    };
    let retry_ops = [
        TransactWriteOp::Put {
            key_info: &infos[0],
            item: &new_item,
            condition: None,
            maps: &maps,
            return_values_on_ccf: Default::default(),
            stream: None,
        },
        TransactWriteOp::Put {
            key_info: &infos[1],
            item: &item,
            condition: Some(&not_exists),
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::AllOld,
            stream: None,
        },
    ];
    let retry_token = || IdempotencyKey {
        account_id: "123456789012",
        token: "canceled-public-phases",
        fingerprint: "retry-conditions",
    };
    assert!(
        matches!(storage.transact_write_items(&retry_ops, Some(retry_token())).await, Err(StorageError::TransactionCanceled(reasons)) if reasons[1].code == "ConditionalCheckFailed" && reasons[1].item == Some(item.clone()))
    );
    assert_eq!(storage.get_item(&infos[0], &new_item).await.unwrap(), None);
    storage
        .delete_item(&infos[1], &item, false, None, &maps, None)
        .await
        .unwrap();
    storage
        .transact_write_items(&retry_ops, Some(retry_token()))
        .await
        .unwrap();
    assert_eq!(
        storage.get_item(&infos[0], &new_item).await.unwrap(),
        Some(new_item.clone())
    );
    assert_eq!(
        storage.get_item(&infos[1], &item).await.unwrap(),
        Some(item.clone())
    );
    let absent = Item::from([("id".into(), AttributeValue::S("absent-read".into()))]);
    lost.store(0, Ordering::SeqCst);
    let read = storage
        .transact_get_items(&[
            TransactGetOp {
                key_info: &infos[1],
                key: &item,
            },
            TransactGetOp {
                key_info: &infos[0],
                key: &new_item,
            },
            TransactGetOp {
                key_info: &infos[0],
                key: &absent,
            },
            TransactGetOp {
                key_info: &infos[1],
                key: &item,
            },
        ])
        .await
        .unwrap();
    assert_eq!(
        read,
        vec![Some(item.clone()), Some(new_item), None, Some(item.clone())]
    );
    assert_eq!(lost.load(Ordering::SeqCst), 15);
    // The direct phase-command fixtures below exercise snapshot recovery,
    // not lost replies. Keep the injector from consuming their uploads.
    lost.store(127, Ordering::SeqCst);
    super::transaction_reads::assert_shared_snapshots(
        &client,
        &storage,
        &provisioner,
        &infos,
        &item,
    )
    .await;
    super::read_resolution::assert_reads_finish_terminal_decisions(
        &client,
        &storage,
        &provisioner,
        &infos,
    )
    .await;
    // Exercise bounded prepares for both legacy account and routed data
    // participants. A small conditional request can return a much larger old
    // image. Losing its bounded rejection must stay uncertain, not fall back.
    for (index, info) in infos.iter().enumerate() {
        let old_key = Item::from([(
            "id".into(),
            AttributeValue::S(format!("wide-failure-{index}")),
        )]);
        let mut old = old_key.clone();
        old.insert("payload".into(), AttributeValue::S("\0".repeat(384 * 1024)));
        storage
            // Seed through the wide command. The legacy account no-return
            // PutItem envelope is 1 MiB; JSON escaping expands this valid item.
            .put_item(info, old.clone(), true, None, &maps, None)
            .await
            .unwrap();
        let new = Item::from([(
            "id".into(),
            AttributeValue::S(format!("wide-rollback-{index}")),
        )]);
        let operations = [
            TransactWriteOp::Put {
                key_info: info,
                item: &new,
                condition: None,
                maps: &maps,
                return_values_on_ccf: Default::default(),
                stream: None,
            },
            TransactWriteOp::Put {
                key_info: info,
                item: &old_key,
                condition: Some(&not_exists),
                maps: &maps,
                return_values_on_ccf: ReturnValuesOnConditionCheckFailure::AllOld,
                stream: None,
            },
        ];
        let dropped = Arc::new(std::sync::atomic::AtomicU8::new(0));
        let uncertain = CellStorage::new(phase_client(1, dropped.clone()), "us-east-1")
            .with_transaction_coordinators(provisioner.clone());
        let token_value = format!("wide-condition-{index}");
        let token = || IdempotencyKey {
            account_id: "123456789012",
            token: &token_value,
            fingerprint: "wide-failure-image",
        };
        assert!(matches!(
            uncertain
                .transact_write_items(&operations, Some(token()))
                .await,
            Err(StorageError::Transient(_))
        ));
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(storage.get_item(info, &new).await.unwrap(), None);
        assert_eq!(
            storage.get_item(info, &old_key).await.unwrap(),
            Some(old.clone())
        );
        assert!(matches!(
            uncertain.transact_write_items(&operations, Some(token())).await,
            Err(StorageError::TransactionCanceled(reasons))
                if reasons[1].code == "ConditionalCheckFailed" && reasons[1].item == Some(old.clone())
        ));
        assert_eq!(storage.get_item(info, &new).await.unwrap(), None);
        // No prepared locks remain after the rejected attempt and ABORT.
        storage
            .delete_item(info, &old_key, false, None, &maps, None)
            .await
            .unwrap();
        uncertain
            .transact_write_items(&operations, Some(token()))
            .await
            .unwrap();
        assert_eq!(storage.get_item(info, &new).await.unwrap(), Some(new));
        assert_eq!(
            storage.get_item(info, &old_key).await.unwrap(),
            Some(old_key)
        );
    }
}

async fn assert_large_participant_payloads(storage: &CellStorage, infos: &[TableKeyInfo; 2]) {
    let keys: Vec<_> = (0..10)
        .map(|i| Item::from([("id".into(), AttributeValue::S(format!("large-{i}")))]))
        .collect();
    let items: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| {
            let mut item = key.clone();
            let payload = if i == 0 {
                format!("{}{}", "\0".repeat(160 * 1024), "🙂".repeat(20 * 1024))
            } else {
                "\0".repeat(320 * 1024)
            };
            item.insert("payload".into(), AttributeValue::S(payload));
            item
        })
        .collect();
    let maps = ExpressionMaps::default();
    let ops: Vec<_> = items
        .iter()
        .enumerate()
        .map(|(i, item)| TransactWriteOp::Put {
            key_info: &infos[i % 2],
            item,
            condition: None,
            maps: &maps,
            return_values_on_ccf: Default::default(),
            stream: None,
        })
        .collect();
    // Reuse a resident shard with a different token; payload tests should not
    // depend on movement budget left by the surrounding owner-recovery fixture.
    let existing = coordinator_target(&infos[0].account_id, b"lost-public-phases").unwrap();
    let token = (0..100_000)
        .map(|i| format!("large-payload-{i}"))
        .find(|token| {
            coordinator_target(&infos[0].account_id, token.as_bytes()).unwrap() == existing
        })
        .unwrap();
    let identity = || IdempotencyKey {
        account_id: &infos[0].account_id,
        token: &token,
        fingerprint: "large-put",
    };
    storage
        .transact_write_items(&ops, Some(identity()))
        .await
        .unwrap();
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            storage.get_item(&infos[i % 2], key).await.unwrap(),
            Some(items[i].clone())
        );
    }
    for (i, key) in keys.iter().enumerate() {
        storage
            .delete_item(&infos[i % 2], key, false, None, &maps, None)
            .await
            .unwrap();
    }
    assert!(matches!(
        storage.transact_write_items(&ops, Some(identity())).await,
        Err(StorageError::IdempotentReplay)
    ));
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(storage.get_item(&infos[i % 2], key).await.unwrap(), None);
    }
}

pub(super) struct DropPhaseReplies {
    pub(super) inner: Arc<dyn InvocationTransport>,
    pub(super) lost: Arc<AtomicU8>,
    pub(super) enabled: u8,
}
impl InvocationTransport for DropPhaseReplies {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            let phase = if invocation.mutation.is_some() {
                match invocation.operation {
                    12 | 21 | 28 | 57 => 1,
                    3 | 11 => 2,
                    1 => 4,
                    13 | 22 => 8,
                    5 => 16,
                    10 => 128,
                    14 => 32,
                    23 => 64,
                    _ => 0,
                }
            } else {
                0
            } & self.enabled;
            let response = self.inner.call(invocation, request).await;
            if response.status() == http::StatusCode::OK
                && phase != 0
                && self.lost.fetch_or(phase, Ordering::SeqCst) & phase == 0
            {
                // Complete the SDK body gate before losing the response. A
                // durable rejection is uncertain too until its receipt is read.
                response.into_body().collect().await.unwrap();
                let body = Full::new(Bytes::new())
                    .map_err(|never| -> std::io::Error { match never {} })
                    .boxed_unsync();
                let mut unknown = http::Response::new(body);
                *unknown.status_mut() = http::StatusCode::SERVICE_UNAVAILABLE;
                unknown
            } else {
                response
            }
        })
    }
}
#[test]
fn public_transactions_recover_lost_phases_large_payloads_and_canceled_tokens() {
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
        let clock = Arc::new(AtomicI64::new(local_node::wall_mutation().issued_at_ms));
        let (node, client) =
            local_node::start(directory.path(), clock, host.handle(), core.handle())
                .await
                .unwrap();
        let (provisioner, infos) = transaction_fixture::mixed(&client).await;
        assert_lost_replies_and_canceled_token_reuse(&client, provisioner, infos).await;
        local_node::stop(node).await.unwrap();
    });
}
