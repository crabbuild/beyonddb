//! Native admission retains application fences; ordered keys permit SK splits.
use crate::native_peer::{self, Peer, SOURCE};
use bdb_cellar_runtime::identity::{ApplicationId, CellTarget, NamespaceId};
use bdb_keyspace::{Component, KeyKind, KeySchema};
use beyonddb::*;
use extenddb_core::types::{AttributeValue, Item, KeySchemaElement, KeyType};
use extenddb_storage::error::StorageError;
use std::sync::{Arc, Mutex};

#[test]
fn logical_scope_validation_accepts_beyonddb_targets_and_rejects_foreign_scopes() {
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
        let application = native_peer::application();
        let files = tempfile::tempdir().unwrap();
        let bucket =
            cellar::bucket::Bucket::open_dev(&files.path().join("objects.sqlite")).unwrap();
        let observed = Arc::new(Mutex::new(Default::default()));
        let mut peer = Peer::start(
            application.clone(),
            &files.path().join(SOURCE),
            bucket,
            observed.clone(),
            true,
            host.handle(),
            core.handle(),
        )
        .await;
        let provisioner = CellInitialPartitionProvisioner::new(peer.client.clone(), application);
        let account = account_target("123456789012").unwrap();
        let mut table_id = [0_u8; 32];
        table_id[..16].copy_from_slice(account.tenant().as_bytes());
        let table_id = blake3::Hash::from_bytes(table_id).to_hex().to_string();
        for target in [
            account.clone(),
            credential_target("AKIAIOSFODNN7EXAMPLE").unwrap(),
            data_target("123456789012", &table_id, &[1; 16]).unwrap(),
            coordinator_target("123456789012", b"token").unwrap(),
        ] {
            provisioner.validate_target(&target).unwrap();
        }
        let before = observed.lock().unwrap().len();
        for target in [
            CellTarget::new(
                account.tenant(),
                ApplicationId::from_bytes([99; 16]),
                account.namespace(),
                account.partition(),
            )
            .unwrap(),
            CellTarget::new(
                account.tenant(),
                account.application(),
                NamespaceId::from_bytes([99; 16]),
                account.partition(),
            )
            .unwrap(),
            CellTarget::new(
                account.tenant(),
                account.application(),
                account.namespace(),
                b"not-a-shard",
            )
            .unwrap(),
        ] {
            assert!(matches!(
                provisioner.validate_target(&target),
                Err(StorageError::Validation(_))
            ));
            assert_eq!(
                observed.lock().unwrap().len(),
                before,
                "invalid admission created an SDK cell"
            );
        }
        peer.stop().await;
    });
}

#[test]
fn sort_key_siblings_share_a_query_span_but_have_distinct_ordered_positions() {
    let schema = vec![
        KeySchemaElement {
            attribute_name: "pk".into(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "sk".into(),
            key_type: KeyType::Range,
        },
    ];
    let item = |sort: &str| {
        Item::from([
            ("pk".into(), AttributeValue::S("books".into())),
            ("sk".into(), AttributeValue::S(sort.into())),
        ])
    };
    let first = ordered_data_key("table", &item("first"), &schema).unwrap();
    let second = ordered_data_key("table", &item("second"), &schema).unwrap();
    assert!(first < second);
    let span = KeySchema {
        partition: KeyKind::String,
        sort: Some(KeyKind::String),
    }
    .query(Component::String("books"), None)
    .unwrap();
    assert!(span.contains(&first) && span.contains(&second));
    let other = ordered_data_key(
        "table",
        &Item::from([
            ("pk".into(), AttributeValue::S("other".into())),
            ("sk".into(), AttributeValue::S("first".into())),
        ]),
        &schema,
    )
    .unwrap();
    assert!(!span.contains(&other));
}
