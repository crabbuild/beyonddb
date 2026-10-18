//! Backend metadata cache behavior through actual native SDK ingress.
use crate::local_node;
use bdb_cellar_runtime::client::{CellClient, Invocation, InvocationTransport};
use beyonddb::*;
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, Item, KeySchemaElement,
    KeyType, ScalarAttributeType,
};
use extenddb_storage::{DataEngine, TableEngine, error::StorageError};
use http_body_util::Full;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
};

struct AccountQueries {
    inner: Arc<dyn InvocationTransport>,
    queries: Arc<AtomicU64>,
    account: bdb_cellar_runtime::identity::CellTarget,
}
impl InvocationTransport for AccountQueries {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        if invocation.target == self.account && invocation.mutation.is_none() {
            self.queries.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.call(invocation, request)
    }
}

#[test]
fn backend_key_info_cache_reduces_queries_and_fences_recreated_generations() {
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
        let account_id = "123456789012";
        let files = tempfile::tempdir().unwrap();
        let clock = Arc::new(AtomicI64::new(local_node::wall_mutation().issued_at_ms));
        let (node, client) = local_node::start(files.path(), clock, host.handle(), core.handle())
            .await
            .unwrap();
        let queries = Arc::new(AtomicU64::new(0));
        let client = client.map_transport(|inner| {
            Arc::new(AccountQueries {
                inner,
                queries: queries.clone(),
                account: account_target(account_id).unwrap(),
            })
        });
        let provisioner = Arc::new(
            CellInitialPartitionProvisioner::new(
                client.clone(),
                local_node::application().unwrap(),
            )
            .with_initial_partition_count(2)
            .unwrap(),
        );
        let cached = CellStorage::new(client.clone(), "local")
            .with_initial_partitions(provisioner.clone())
            .with_route_cache(true);
        let fresh =
            CellStorage::new(client.clone(), "local").with_initial_partitions(provisioner.clone());
        let create = || CreateTableInput {
            table_name: "KeyInfo".into(),
            billing_mode: Some(BillingMode::PayPerRequest),
            key_schema: vec![KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: KeyType::Hash,
            }],
            attribute_definitions: vec![AttributeDefinition {
                attribute_name: "pk".into(),
                attribute_type: ScalarAttributeType::S,
            }],
            ..Default::default()
        };
        cached.create_table(account_id, create()).await.unwrap();
        let first = cached.table_key_info(account_id, "KeyInfo").await.unwrap();
        let before = queries.load(Ordering::Relaxed);
        assert_eq!(
            cached
                .table_key_info(account_id, "KeyInfo")
                .await
                .unwrap()
                .table_id,
            first.table_id
        );
        assert_eq!(
            queries.load(Ordering::Relaxed),
            before,
            "warm backend metadata lookup must not execute another Cell query"
        );
        fresh.table_key_info(account_id, "KeyInfo").await.unwrap();
        fresh.table_key_info(account_id, "KeyInfo").await.unwrap();
        assert_eq!(
            queries.load(Ordering::Relaxed),
            before + 4,
            "each uncached routed lookup must read the catalog and activation state"
        );

        cached
            .update_table(
                account_id,
                serde_json::from_value(serde_json::json!({
                    "TableName": "KeyInfo", "DeletionProtectionEnabled": false
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        let before = queries.load(Ordering::Relaxed);
        cached.table_key_info(account_id, "KeyInfo").await.unwrap();
        assert_eq!(
            queries.load(Ordering::Relaxed),
            before + 2,
            "a local update invalidates both cached catalog and activation metadata"
        );
        cached
            .delete_table(
                account_id,
                extenddb_core::types::DeleteTableInput {
                    table_name: "KeyInfo".into(),
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            cached.table_key_info(account_id, "KeyInfo").await,
            Err(StorageError::TableNotFound(_))
        ));
        finish_deletion(&provisioner, &client, account_id, &first.table_id).await;
        cached.create_table(account_id, create()).await.unwrap();
        let next = cached.table_key_info(account_id, "KeyInfo").await.unwrap();
        assert_ne!(next.table_id, first.table_id);

        // A different backend represents a remote management writer. Its recreation
        // is observed after expiry; cached metadata cannot alias the new generation.
        fresh
            .delete_table(
                account_id,
                extenddb_core::types::DeleteTableInput {
                    table_name: "KeyInfo".into(),
                },
            )
            .await
            .unwrap();
        finish_deletion(&provisioner, &client, account_id, &next.table_id).await;
        fresh.create_table(account_id, create()).await.unwrap();
        let current = fresh.table_key_info(account_id, "KeyInfo").await.unwrap();
        assert_ne!(current.table_id, next.table_id);
        let key = Item::from([("pk".into(), AttributeValue::S("same".into()))]);
        assert!(
            matches!(
                cached.get_item(&next, &key).await,
                Err(StorageError::TableNotFound(_))
            ),
            "an old key-info generation must not read a recreated table"
        );
        tokio::time::sleep(std::time::Duration::from_millis(550)).await;
        assert_eq!(
            cached
                .table_key_info(account_id, "KeyInfo")
                .await
                .unwrap()
                .table_id,
            current.table_id
        );
        local_node::stop(node).await.unwrap();
    });
}

async fn finish_deletion(
    provisioner: &CellInitialPartitionProvisioner,
    client: &CellClient,
    account: &str,
    table_id: &str,
) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !provisioner
            .continue_table_deletion(client, account, table_id)
            .await
            .unwrap()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
