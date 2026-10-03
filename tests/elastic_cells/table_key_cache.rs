use crate::*;

use cellule_runtime::fleet::telemetry::{
    CellTelemetry, PrimitiveOperationKind, PrimitiveOperationOutcome,
};

#[derive(Default)]
struct AccountQueries(std::sync::atomic::AtomicU64);

impl CellTelemetry for AccountQueries {
    fn primitive_operation(
        &self,
        module: &'static str,
        kind: PrimitiveOperationKind,
        _: PrimitiveOperationOutcome,
        _: std::time::Duration,
    ) {
        if module == "beyonddb-account" && kind == PrimitiveOperationKind::Query {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backend_key_info_cache_reduces_queries_and_fences_recreated_generations() {
    let application = Arc::new(
        Beyonddb::compile(BuildDescriptor {
            source_revision: "backend-key-info-cache".into(),
            cargo_lock_digest: Digest::from_bytes([1; 32]),
        })
        .unwrap(),
    );
    let account_id = "123456789012";
    let account = account_target(account_id).unwrap();
    let session = SessionId::from_bytes([97; 16]);
    let files = tempfile::tempdir().unwrap();
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        object_store::path::Path::from("key-info-cache"),
        *account.application().as_bytes(),
    );
    let host = CellNodeBuilder::new(application.clone())
        .with_runtime(SqlWorkerPool::new(1, 8).unwrap(), 16 << 20)
        .with_replica_host(Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)))
        .with_session(session)
        .build_unleased_for_maintenance()
        .unwrap();
    let queries = Arc::new(AccountQueries::default());
    host.install_telemetry(queries.clone()).unwrap();
    let registry = application.registry();
    let bootstrap = Bootstrap {
        runtime: host.runtime(),
        registry: &registry,
        layout: &layout,
        session,
    };
    let handle = bootstrap
        .cell(
            &account,
            "beyonddb-account",
            98,
            &files.path().join("account.sqlite"),
            initialize_account,
        )
        .await;
    let client =
        CellClient::local_with_telemetry(registry, handle, host.runtime().telemetry_handle());
    let cached = CellStorage::new(client.clone(), "us-east-1").with_route_cache(true);
    let fresh = CellStorage::new(client, "us-east-1");
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
    let before = queries.0.load(Ordering::Relaxed);
    assert_eq!(
        cached
            .table_key_info(account_id, "KeyInfo")
            .await
            .unwrap()
            .table_id,
        first.table_id
    );
    assert_eq!(
        queries.0.load(Ordering::Relaxed),
        before,
        "warm backend metadata lookup must not execute another Cell query"
    );
    fresh.table_key_info(account_id, "KeyInfo").await.unwrap();
    fresh.table_key_info(account_id, "KeyInfo").await.unwrap();
    assert_eq!(
        queries.0.load(Ordering::Relaxed),
        before + 2,
        "the default mode must retain fresh metadata reads"
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
    let before = queries.0.load(Ordering::Relaxed);
    cached.table_key_info(account_id, "KeyInfo").await.unwrap();
    assert_eq!(
        queries.0.load(Ordering::Relaxed),
        before + 1,
        "a local update invalidates backend metadata"
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
    host.shutdown().await.unwrap();
}
