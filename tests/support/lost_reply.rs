//! Lost transport replies around real SDK execution and durable command replay.
use crate::{local_node, transaction_fixture};
use beyonddb::*;
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use cellar_runtime::{
    client::{Invocation, InvocationAdmission, InvocationError, InvocationTransport},
    registry::Command,
};
use extenddb_core::{
    expression::{Expr, ExpressionMaps, PathElement, UpdateAction},
    types::AttributeValue,
};
use http_body_util::{BodyExt, Full};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
    },
};

struct LoseUpdateReply {
    inner: Arc<dyn InvocationTransport>,
    dropped: Arc<AtomicBool>,
}

struct RefuseAtCaller;
impl InvocationAdmission for RefuseAtCaller {
    fn check<'a>(
        &'a self,
        _: &'a Invocation<'a>,
    ) -> Pin<Box<dyn Future<Output = cellar_runtime::Result<()>> + Send + 'a>> {
        Box::pin(async { Err(cellar_runtime::Error::Capacity("injected caller budget")) })
    }
}
struct CountIngress {
    inner: Arc<dyn InvocationTransport>,
    calls: Arc<AtomicUsize>,
}
impl InvocationTransport for CountIngress {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.call(invocation, request)
    }
}

#[test]
fn caller_refusal_preserves_identity_without_entering_sdk_ingress() {
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
        let (node, client) = local_node::start(
            directory.path(),
            clock.clone(),
            host.handle(),
            core.handle(),
        )
        .await
        .unwrap();
        let fixture = transaction_fixture::table(&client, "Admission").await;
        let (target, participant) = &fixture.participants[0];
        let TransactionOperation::Put(original) = &participant.operations[0].operation else {
            unreachable!()
        };
        let read = Json(PartitionGetInput {
            table_id: fixture.table.id.clone(),
            epoch: 1,
            key: extenddb_core::types::extract_key(&original.item, &fixture.table.key_schema),
        });
        let before = client
            .query::<PartitionGet>(target, None, read.clone())
            .await
            .unwrap();
        let mut item = original.item.clone();
        item.insert("value".into(), AttributeValue::N("99".into()));
        let input = Json(PartitionPutInput {
            table_id: fixture.table.id.clone(),
            epoch: 1,
            item: item.clone(),
            condition: None,
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let allowed = client.map_transport(|inner| {
            Arc::new(CountIngress {
                inner,
                calls: calls.clone(),
            })
        });
        let refused = allowed.with_admission(Arc::new(RefuseAtCaller));
        let identity = local_node::mutation(&clock);
        assert!(matches!(
            refused
                .command::<PartitionPut>(target, identity, input.clone())
                .await,
            Err(InvocationError::NotStarted(
                cellar_runtime::Error::Capacity("injected caller budget")
            ))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let unchanged = client
            .query::<PartitionGet>(target, None, read.clone())
            .await
            .unwrap();
        assert_eq!(unchanged.receipt, before.receipt);
        assert_eq!(
            unchanged.output.0,
            PartitionGetOutcome::Found(Some(original.item.clone()))
        );
        let committed = allowed
            .command::<PartitionPut>(target, identity, input)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            committed.receipt.commit_sequence,
            before.receipt.commit_sequence + 1
        );
        assert_eq!(
            client
                .query::<PartitionGet>(target, None, read)
                .await
                .unwrap()
                .output
                .0,
            PartitionGetOutcome::Found(Some(item))
        );
        local_node::stop(node).await.unwrap();
    });
}
impl InvocationTransport for LoseUpdateReply {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        Box::pin(async move {
            let response = self.inner.call(invocation, request).await;
            if invocation.mutation.is_some()
                && invocation.operation == PartitionUpdate::ID
                && !self.dropped.swap(true, Ordering::SeqCst)
            {
                assert_eq!(response.status(), http::StatusCode::OK);
                // Consume the SDK durability gate before discarding its reply.
                response.into_body().collect().await.unwrap();
                let body = Full::new(Bytes::new())
                    .map_err(|never| -> std::io::Error { match never {} })
                    .boxed_unsync();
                let mut lost = http::Response::new(body);
                *lost.status_mut() = http::StatusCode::SERVICE_UNAVAILABLE;
                lost
            } else {
                response
            }
        })
    }
}

#[test]
fn lost_update_reply_keeps_identity_and_replays_once_after_restart() {
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
        let now = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        let clock = Arc::new(AtomicI64::new(now));
        let (node, client) = local_node::start(
            directory.path(),
            clock.clone(),
            host.handle(),
            core.handle(),
        )
        .await
        .unwrap();
        let fixture = transaction_fixture::table(&client, "LostReply").await;
        let (target, participant) = &fixture.participants[0];
        let TransactionOperation::Put(original) = &participant.operations[0].operation else {
            unreachable!()
        };
        let key = extenddb_core::types::extract_key(&original.item, &fixture.table.key_schema);
        let read = Json(PartitionGetInput {
            table_id: fixture.table.id.clone(),
            epoch: 1,
            key: key.clone(),
        });
        let before = client
            .query::<PartitionGet>(target, None, read.clone())
            .await
            .unwrap();
        let maps = ExpressionMaps::new(
            Default::default(),
            std::collections::HashMap::from([("one".into(), AttributeValue::N("1".into()))]),
        );
        let update = Json(PartitionUpdateInput::from_expression(
            fixture.table.id.clone(),
            1,
            key,
            &[UpdateAction::Add {
                path: vec![PathElement::Attribute("value".into())],
                value: Expr::Placeholder("one".into()),
            }],
            None,
            &maps,
        ));
        let identity = local_node::mutation(&clock);
        let dropped = Arc::new(AtomicBool::new(false));
        let fault = client.map_transport(|inner| {
            Arc::new(LoseUpdateReply {
                inner,
                dropped: dropped.clone(),
            })
        });
        assert!(
            matches!(fault.command::<PartitionUpdate>(target, identity, update.clone()).await,
            Err(InvocationError::Ambiguous { identity: Some(actual), .. }) if actual == identity)
        );
        assert!(dropped.load(Ordering::SeqCst));
        let mut twice = original.item.clone();
        twice.insert("value".into(), AttributeValue::N("2".into()));
        let after = client
            .query::<PartitionGet>(target, None, read.clone())
            .await
            .unwrap();
        assert_eq!(
            after.output.0,
            PartitionGetOutcome::Found(Some(twice.clone()))
        );
        assert_eq!(
            after.receipt.commit_sequence,
            before.receipt.commit_sequence + 1
        );
        let replay = fault
            .command::<PartitionUpdate>(target, identity, update.clone())
            .await
            .unwrap();
        assert_eq!(replay.receipt, after.receipt);
        let PartitionUpdateOutcome::Applied { old, new } = replay.output.0 else {
            panic!("missing original update result")
        };
        assert_eq!(old, Some(original.item.clone()));
        assert_eq!(new, twice);
        local_node::stop(node).await.unwrap();
        let (node, restored) = local_node::start(
            directory.path(),
            clock.clone(),
            host.handle(),
            core.handle(),
        )
        .await
        .unwrap();
        let replay = restored
            .command::<PartitionUpdate>(target, identity, update.clone())
            .await
            .unwrap();
        assert_eq!(replay.receipt, after.receipt);
        let image = restored
            .query::<PartitionGet>(target, None, read.clone())
            .await
            .unwrap();
        assert_eq!(image.output.0, PartitionGetOutcome::Found(Some(twice)));
        restored
            .command::<PartitionUpdate>(target, local_node::mutation(&clock), update)
            .await
            .unwrap();
        let PartitionGetOutcome::Found(Some(image)) = restored
            .query::<PartitionGet>(target, None, read)
            .await
            .unwrap()
            .output
            .0
        else {
            panic!("missing incremented item")
        };
        assert_eq!(image["value"], AttributeValue::N("3".into()));
        local_node::stop(node).await.unwrap();
    });
}
