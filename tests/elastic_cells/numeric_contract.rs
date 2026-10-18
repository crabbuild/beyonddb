//! Ordered Query/Scan, IAM and restoration contracts from the legacy root.
use crate::native_peer::{self, FRONT, Inspection, Peer, SOURCE};
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::types::AttributeValue as AwsAttributeValue;
use beyonddb::*;
use bytes::Bytes;
use cellar::sdk::IngressResponse;
use cellar_runtime::client::{Invocation, InvocationTransport};
use extenddb_auth::{AuthCacheRegistry, CredentialStore, StoredCredential};
use extenddb_core::{
    expression::{CompareOp, Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition},
    limits::LimitsConfig,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, Item, KeySchemaElement,
        KeyType, ScalarAttributeType, TableStatus,
    },
};
use extenddb_engine::OperationContext;
use extenddb_storage::{
    DataEngine, IdempotencyKey, TableEngine, TransactGetOp, TransactWriteOp,
    authorization_store::AuthorizationStore, error::StorageError, management_store::OpError,
};
use http_body_util::Full;
use keyspace::RangeKey;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
const TEST_ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const TEST_SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const TEST_ENCRYPTION_KEY: [u8; 32] = [0x36; 32];
struct CredentialQueries {
    inner: Arc<dyn InvocationTransport>,
    target: cellar_runtime::identity::CellTarget,
    queries: Arc<AtomicUsize>,
}
impl InvocationTransport for CredentialQueries {
    fn call<'a>(
        &'a self,
        invocation: &'a Invocation<'a>,
        request: http::Request<Full<Bytes>>,
    ) -> Pin<Box<dyn Future<Output = IngressResponse> + Send + 'a>> {
        if invocation.target == self.target && invocation.mutation.is_none() {
            self.queries.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.call(invocation, request)
    }
}
#[test]
fn numeric_query_pages_survive_ordered_splits_signed_auth_and_owner_replacement() {
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
        let account = account_target("123456789012").unwrap();
        let directory = tempfile::tempdir().unwrap();
        let bucket =
            cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite")).unwrap();
        let observed = Arc::new(Mutex::new(Default::default()));
        let mut source = Peer::start(
            application.clone(),
            &directory.path().join(SOURCE),
            bucket.clone(),
            observed.clone(),
            true,
            host.handle(),
            core.handle(),
        )
        .await;
        let capacity_client = source.client.clone();
        let provisioner = Arc::new(
            CellInitialPartitionProvisioner::new(capacity_client.clone(), application.clone())
                .with_initial_partition_count(2)
                .unwrap(),
        );
        provisioner.admit_account("123456789012").await.unwrap();
        let tasks = TaskGroup::default();
        let creator = CellStorage::new(capacity_client.clone(), "us-east-1")
            .with_initial_partitions(provisioner.clone())
            .with_transaction_coordinators(provisioner.clone());
        let created = creator
            .create_table(
                "123456789012",
                CreateTableInput {
                    table_name: "Numbers".into(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "pk".into(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "sk".into(),
                            key_type: KeyType::Range,
                        },
                    ],
                    attribute_definitions: vec![
                        AttributeDefinition {
                            attribute_name: "pk".into(),
                            attribute_type: ScalarAttributeType::S,
                        },
                        AttributeDefinition {
                            attribute_name: "sk".into(),
                            attribute_type: ScalarAttributeType::N,
                        },
                    ],
                    billing_mode: Some(BillingMode::PayPerRequest),
                    ..CreateTableInput::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(created.table_status, TableStatus::Active);
        let route = crate::single_leaf_route(&capacity_client, &account, &created.table_id.clone())
            .await
            .unwrap();
        assert_eq!(route.partitions.len(), 2);
        let data = data_target(
            "123456789012",
            &created.table_id,
            &route.partitions[0].partition_id,
        )
        .unwrap();
        let storage = creator;
        let key_info = storage
            .table_key_info("123456789012", "Numbers")
            .await
            .unwrap();
        for number in ["10", "-10", "2", "1.5"] {
            let item = Item::from([
                ("pk".into(), AttributeValue::S("same".into())),
                ("sk".into(), AttributeValue::N(number.into())),
            ]);
            storage
                .put_item(
                    &key_info,
                    item,
                    false,
                    None,
                    &ExpressionMaps::default(),
                    None,
                )
                .await
                .unwrap();
        }
        let usage = capacity_client
            .clone()
            .query::<PartitionUsage>(&data, None, Json(()))
            .await
            .unwrap()
            .output
            .0;
        assert_eq!(usage.item_count, 4);
        assert!(usage.item_bytes > 0);
        assert!(usage.database_bytes > usage.item_bytes);
        let equivalent_numeric_key: Item =
            serde_json::from_str(r#"{"pk":{"S":"same"},"sk":{"N":"2.0"}}"#).unwrap();
        assert_eq!(
            storage
                .get_item(&key_info, &equivalent_numeric_key)
                .await
                .unwrap()
                .unwrap()["sk"],
            AttributeValue::N("2".into())
        );
        let condition = KeyCondition {
            pk_path: vec![PathElement::Attribute("pk".into())],
            pk_value: Expr::Placeholder("p".into()),
            extra_pk_conditions: vec![],
            sk_condition: None,
            extra_sk_conditions: vec![],
        };
        let maps = ExpressionMaps::new(
            HashMap::new(),
            HashMap::from([("p".into(), AttributeValue::S("same".into()))]),
        );
        let (first, cursor) = storage
            .query(&key_info, &condition, &maps, true, Some(2), None, None)
            .await
            .unwrap();
        assert_eq!(
            first
                .iter()
                .map(|item| item["sk"].clone())
                .collect::<Vec<_>>(),
            vec![
                AttributeValue::N("-10".into()),
                AttributeValue::N("1.5".into())
            ]
        );
        let (second, end) = storage
            .query(
                &key_info,
                &condition,
                &maps,
                true,
                Some(2),
                cursor.as_ref(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            second
                .iter()
                .map(|item| item["sk"].clone())
                .collect::<Vec<_>>(),
            vec![
                AttributeValue::N("2".into()),
                AttributeValue::N("10".into())
            ]
        );
        let local = capacity_client
            .query::<PartitionQuery>(
                &data,
                None,
                Json(PartitionQueryInput {
                    table_id: created.table_id.clone(),
                    epoch: route.partitions[0].epoch,
                    index_name: None,
                    partition_key: Item::from([("pk".into(), AttributeValue::S("same".into()))]),
                    sort: None,
                    extra_range_equals: vec![],
                    forward: true,
                    limit: 2,
                    exclusive_start_key: cursor.clone(),
                }),
            )
            .await
            .unwrap()
            .output
            .0;
        assert_eq!(
            local,
            PartitionQueryOutcome::Page {
                items: second.clone(),
                last_evaluated_key: None
            }
        );
        let span = keyspace::KeySchema {
            partition: keyspace::KeyKind::String,
            sort: Some(keyspace::KeyKind::Number),
        }
        .query(keyspace::Component::String("same"), None)
        .unwrap();
        assert!(
            route.partitions[0]
                .upper
                .as_ref()
                .is_none_or(|upper| span.upper().unwrap() <= upper)
        );
        assert_eq!(end, None);
        let range = KeyCondition {
            sk_condition: Some(SortKeyCondition::Compare {
                path: vec![PathElement::Attribute("sk".into())],
                op: CompareOp::Gt,
                value: Expr::Placeholder("min".into()),
            }),
            ..condition
        };
        let range_maps = ExpressionMaps::new(
            HashMap::new(),
            HashMap::from([
                ("p".into(), AttributeValue::S("same".into())),
                ("min".into(), AttributeValue::N("0".into())),
            ]),
        );
        let (reverse, end) = storage
            .query(&key_info, &range, &range_maps, false, Some(10), None, None)
            .await
            .unwrap();
        assert_eq!(
            reverse
                .iter()
                .map(|item| item["sk"].clone())
                .collect::<Vec<_>>(),
            vec![
                AttributeValue::N("10".into()),
                AttributeValue::N("2".into()),
                AttributeValue::N("1.5".into()),
            ]
        );
        assert_eq!(end, None);
        let (exact_reverse, exact_end) = storage
            .query(&key_info, &range, &range_maps, false, Some(3), None, None)
            .await
            .unwrap();
        assert_eq!(exact_reverse, reverse);
        assert_eq!(
            exact_end, None,
            "exact reverse limit must retain the owner's exhaustion proof"
        );
        let new_first = Item::from([
            ("pk".into(), AttributeValue::S("same".into())),
            ("sk".into(), AttributeValue::N("3".into())),
        ]);
        let new_second = Item::from([
            ("pk".into(), AttributeValue::S("same".into())),
            ("sk".into(), AttributeValue::N("4".into())),
        ]);
        let tx_maps = ExpressionMaps::default();
        storage
            .transact_write_items(
                &[
                    TransactWriteOp::Put {
                        key_info: &key_info,
                        item: &new_first,
                        condition: None,
                        maps: &tx_maps,
                        return_values_on_ccf: Default::default(),
                        stream: None,
                    },
                    TransactWriteOp::Put {
                        key_info: &key_info,
                        item: &new_second,
                        condition: None,
                        maps: &tx_maps,
                        return_values_on_ccf: Default::default(),
                        stream: None,
                    },
                ],
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            storage
                .transact_get_items(&[
                    TransactGetOp {
                        key_info: &key_info,
                        key: &new_first,
                    },
                    TransactGetOp {
                        key_info: &key_info,
                        key: &new_second,
                    },
                ])
                .await
                .unwrap(),
            vec![Some(new_first), Some(new_second)]
        );
        let (after_write, _) = storage
            .query(&key_info, &range, &range_maps, true, Some(10), None, None)
            .await
            .unwrap();
        assert_eq!(
            after_write
                .iter()
                .map(|item| item["sk"].clone())
                .collect::<Vec<_>>(),
            vec![
                AttributeValue::N("1.5".into()),
                AttributeValue::N("2".into()),
                AttributeValue::N("3".into()),
                AttributeValue::N("4".into()),
                AttributeValue::N("10".into()),
            ]
        );
        crate::read_barriers::assert_range_read_barriers(
            &storage,
            &capacity_client.clone(),
            &data,
            route.partitions[0].epoch,
            &key_info,
        )
        .await;
        let usage_before_split = capacity_client
            .clone()
            .query::<PartitionUsage>(&data, None, Json(()))
            .await
            .unwrap()
            .output
            .0;
        let split_threshold = usage_before_split.database_bytes - 1;
        assert!(split_threshold > usage_before_split.item_bytes);
        assert!(
            provisioner
                .reconcile_table_capacity(
                    "123456789012",
                    capacity_client.clone(),
                    &created.table_id,
                    usage_before_split.database_bytes,
                )
                .await
                .unwrap()
                .is_none()
        );
        // Both start in the hot initial range, then straddle its ordered split.
        let left_token_item = Item::from([
            ("pk".into(), AttributeValue::S("samd".into())),
            ("sk".into(), AttributeValue::N("0".into())),
        ]);
        let right_token_item = Item::from([
            ("pk".into(), AttributeValue::S("samf".into())),
            ("sk".into(), AttributeValue::N("0".into())),
        ]);
        for item in [&left_token_item, &right_token_item] {
            assert_eq!(
                crate::transaction_fixture::participant_for_key(&capacity_client, &key_info, item)
                    .await
                    .0,
                data
            );
        }
        let source_id = route.partitions[0].partition_id;
        let token_writes = [
            TransactWriteOp::Put {
                key_info: &key_info,
                item: &left_token_item,
                condition: None,
                maps: &tx_maps,
                return_values_on_ccf: Default::default(),
                stream: None,
            },
            TransactWriteOp::Put {
                key_info: &key_info,
                item: &right_token_item,
                condition: None,
                maps: &tx_maps,
                return_values_on_ccf: Default::default(),
                stream: None,
            },
        ];
        storage
            .transact_write_items(
                &token_writes,
                Some(IdempotencyKey {
                    account_id: "123456789012",
                    token: "before-split",
                    fingerprint: "two-ranges",
                }),
            )
            .await
            .unwrap();
        provisioner
            .install_account_capacity_loop(
                &tasks,
                "123456789012".into(),
                capacity_client.clone(),
                split_threshold,
                Duration::from_secs(5),
            )
            .unwrap();
        let grown = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                assert!(
                    source.node.as_ref().unwrap().is_ready().await.unwrap(),
                    "capacity loop stopped serving"
                );
                if let Some(route) =
                    crate::single_leaf_route(&capacity_client, &account, &created.table_id.clone())
                        .await
                    && route.epoch == 2
                {
                    // Publication precedes child opening. The capacity task's
                    // terminal boundary is the durable Finish, not the new epoch.
                    if capacity_client
                        .query::<beyonddb::ReadDirectoryTransfer>(
                            &beyonddb::directory_target(
                                "123456789012",
                                &beyonddb::DirectorySpec::root(created.table_id.clone()),
                            )
                            .unwrap(),
                            None,
                            Json(beyonddb::DirectoryPartitionInput {
                                table_id: created.table_id.clone(),
                                partition_id: source_id,
                            }),
                        )
                        .await
                        .unwrap()
                        .output
                        .0
                        .is_some()
                    {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                    break route;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tasks.shutdown().await.unwrap();
        assert_eq!(grown.partitions.len(), 3);
        assert_eq!(grown.epoch, 2);
        let replay_after_split = storage
            .transact_write_items(
                &token_writes,
                Some(IdempotencyKey {
                    account_id: "123456789012",
                    token: "before-split",
                    fingerprint: "two-ranges",
                }),
            )
            .await;
        assert!(matches!(
            replay_after_split,
            Err(StorageError::IdempotentReplay)
        ));
        let mut capacity_cursor = None;
        for (position, partition) in grown.partitions.iter().enumerate() {
            assert!(
                !provisioner
                    .reconcile_account_capacity(
                        "123456789012",
                        capacity_client.clone(),
                        u64::MAX,
                        &mut capacity_cursor
                    )
                    .await
                    .unwrap()
            );
            let expected = (position + 1 < grown.partitions.len())
                .then(|| partition.lower.clone().unwrap_or_else(RangeKey::minimum));
            assert_eq!(capacity_cursor.as_ref().unwrap().after_lower, expected);
        }
        assert!(
            !provisioner
                .reconcile_account_capacity(
                    "123456789012",
                    capacity_client.clone(),
                    u64::MAX,
                    &mut capacity_cursor
                )
                .await
                .unwrap()
        );
        assert!(capacity_cursor.is_none());
        assert_eq!(
            crate::single_leaf_route(&capacity_client, &account, &created.table_id.clone()).await,
            Some(grown.clone())
        );
        let completed = provisioner
            .split_if_over_database_bytes(
                "123456789012",
                capacity_client.clone(),
                &created.table_id,
                route.partitions[0].partition_id,
                route.partitions[0]
                    .lower
                    .clone()
                    .unwrap_or_else(RangeKey::minimum),
                1,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.source, route.partitions[0]);
        assert_eq!(completed.children.as_slice(), &grown.partitions[..2]);
        let hash = ordered_data_key(
            &created.table_id,
            &after_write[0],
            &key_info.base_key_schema,
        )
        .unwrap();
        let owner = grown
            .partitions
            .iter()
            .find(|partition| {
                partition.lower.as_ref().is_none_or(|lower| &hash >= lower)
                    && partition.upper.as_ref().is_none_or(|upper| &hash < upper)
            })
            .unwrap();
        let split = provisioner
            .split_partition(
                "123456789012",
                capacity_client.clone(),
                &created.table_id,
                owner.partition_id,
                owner.lower.clone().unwrap_or_else(RangeKey::minimum),
            )
            .await
            .unwrap();
        assert_eq!(split.source.partition_id, owner.partition_id);
        assert_eq!(split.children[0].epoch, owner.epoch + 1);
        let grown = crate::single_leaf_route(&capacity_client, &account, &created.table_id.clone())
            .await
            .unwrap();
        assert_eq!(grown.partitions.len(), 4);
        assert_eq!(grown.epoch, 3);
        let mut sibling_owners = Vec::new();
        for sk in ["-10", "8"] {
            let key = serde_json::from_value::<Item>(
                serde_json::json!({"pk":{"S":"same"},"sk":{"N":sk}}),
            )
            .unwrap();
            sibling_owners.push(
                crate::transaction_fixture::participant_for_key(&capacity_client, &key_info, &key)
                    .await
                    .0,
            );
        }
        assert_ne!(
            sibling_owners[0], sibling_owners[1],
            "same-PK sort ranges did not reach different SDK cells"
        );
        for partition in &grown.partitions {
            let target =
                data_target("123456789012", &created.table_id, &partition.partition_id).unwrap();
            let state = capacity_client
                .query::<ReadPartitionState>(&target, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap();
            assert_eq!(state.spec.partition_id, partition.partition_id);
            assert!(matches!(
                state.state,
                PartitionState::Serving | PartitionState::Opened { .. }
            ));
        }
        let routed = Arc::new(
            CellStorage::new(capacity_client.clone(), "us-east-1")
                .with_initial_partitions(provisioner.clone())
                .with_transaction_coordinators(provisioner.clone()),
        );
        assert_eq!(
            routed.get_item(&key_info, &left_token_item).await.unwrap(),
            Some(left_token_item)
        );
        assert_eq!(
            routed.get_item(&key_info, &right_token_item).await.unwrap(),
            Some(right_token_item)
        );
        let (after_split, _) = routed
            .query(&key_info, &range, &range_maps, true, Some(10), None, None)
            .await
            .unwrap();
        assert_eq!(after_split, after_write);
        let credential_target = credential_target(TEST_ACCESS_KEY).unwrap();
        let credential_store =
            CellCredentialStore::new(capacity_client.clone(), TEST_ENCRYPTION_KEY);
        credential_store
            .put_credential(
                TEST_ACCESS_KEY,
                StoredCredential {
                    secret_key: TEST_SECRET_KEY.into(),
                    account_id: "123456789012".into(),
                    principal_name: "test-user".into(),
                    session_name: None,
                    is_session: false,
                    session_token: None,
                    is_active: true,
                    expires_at: None,
                },
            )
            .await
            .unwrap();
        // Every credential value lookup still reaches its native cell. Cellar
        // owns route metadata caching; BeyondDB does not cache plaintext values.
        let value_queries = Arc::new(AtomicUsize::new(0));
        let observed_client = capacity_client.map_transport(|inner| {
            Arc::new(CredentialQueries {
                inner,
                target: credential_target.clone(),
                queries: value_queries.clone(),
            })
        });
        let observed_credentials = CellCredentialStore::new(observed_client, TEST_ENCRYPTION_KEY);
        assert!(
            observed_credentials
                .lookup_credential(TEST_ACCESS_KEY)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(value_queries.load(Ordering::SeqCst), 1);
        assert!(
            observed_credentials
                .lookup_credential(TEST_ACCESS_KEY)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(value_queries.load(Ordering::SeqCst), 2);
        let missing_same_cell = (0..10_000)
            .map(|index| format!("AKIAMISSING{index:05}"))
            .find(|key| {
                beyonddb::credential_target(key).unwrap().cell_id() == credential_target.cell_id()
            })
            .expect("one missing key should share the credential Cell");
        assert!(
            observed_credentials
                .lookup_credential(&missing_same_cell)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(value_queries.load(Ordering::SeqCst), 3);
        let authorization = Arc::new(CellAuthorizationStore::new(capacity_client.clone()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let mut front = Peer::start(
            application.clone(),
            &directory.path().join(FRONT),
            bucket.clone(),
            observed.clone(),
            true,
            host.handle(),
            core.handle(),
        )
        .await;
        let inspection = Inspection::new(bucket.clone(), host.handle());
        inspection.ready_peers().await;
        let state = build_http_state(
            front.client.clone(),
            provisioner.clone(),
            TEST_ENCRYPTION_KEY,
            "us-east-1",
            endpoint.clone(),
        )
        .unwrap();
        assert!(matches!(
            state
                .catalog_store
                .as_ref()
                .unwrap()
                .list_all_accounts()
                .await,
            Err(OpError::Internal(_))
        ));
        let server = tokio::spawn(extenddb_server::start_server(listener, state, None, None));
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                TEST_ACCESS_KEY,
                TEST_SECRET_KEY,
                None,
                None,
                "beyonddb-test",
            ))
            .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled())
            .endpoint_url(endpoint.clone())
            .load()
            .await;
        let sdk = aws_sdk_dynamodb::Client::new(&config);
        let sdk_item = HashMap::from([
            ("pk".to_owned(), AwsAttributeValue::S("same".into())),
            ("sk".to_owned(), AwsAttributeValue::N("8".into())),
        ]);
        let denied_without_policy = sdk
            .put_item()
            .table_name("Numbers")
            .set_item(Some(sdk_item.clone()))
            .send()
            .await
            .unwrap_err();
        assert!(format!("{denied_without_policy:?}").contains("AccessDeniedException"));
        let policy_document = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:*",
                "Resource": [
                    "arn:aws:dynamodb:us-east-1:123456789012:table/Numbers",
                    "arn:aws:dynamodb:us-east-1:123456789012:table/NewTable"
                ]
            }]
        })
        .to_string();
        authorization
            .put_user_policy("123456789012", "test-user", "tables", &policy_document)
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            sdk.put_item()
                .table_name("Numbers")
                .set_item(Some(sdk_item.clone()))
                .send(),
        )
        .await
        .unwrap()
        .unwrap();
        let boundary_document = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:GetItem",
                "Resource": "arn:aws:dynamodb:us-east-1:123456789012:table/Numbers"
            }]
        })
        .to_string();
        authorization
            .set_user_boundary("123456789012", "test-user", Some(&boundary_document))
            .await
            .unwrap();
        let boundary_denied = sdk
            .put_item()
            .table_name("Numbers")
            .set_item(Some(sdk_item.clone()))
            .send()
            .await
            .unwrap_err();
        assert!(format!("{boundary_denied:?}").contains("AccessDeniedException"));
        assert!(
            authorization
                .set_user_boundary("123456789012", "test-user", Some("not JSON"))
                .await
                .is_err()
        );
        assert!(
            authorization
                .set_user_boundary("123456789012", "test-user", None)
                .await
                .unwrap()
        );
        authorization
            .set_role_boundary("123456789012", "test-role", Some(&boundary_document))
            .await
            .unwrap();
        assert_eq!(
            authorization
                .fetch_role_boundary("123456789012", "test-role")
                .await
                .unwrap(),
            Some(boundary_document.clone())
        );
        assert!(
            authorization
                .set_role_boundary("123456789012", "test-role", None)
                .await
                .unwrap()
        );
        let sdk_read = tokio::time::timeout(
            Duration::from_secs(10),
            sdk.get_item()
                .table_name("Numbers")
                .set_key(Some(sdk_item.clone()))
                .send(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(sdk_read.item(), Some(&sdk_item));
        let same_hash = ordered_data_key(
            &key_info.table_id,
            &Item::from([
                ("pk".into(), AttributeValue::S("same".into())),
                ("sk".into(), AttributeValue::N("8".into())),
            ]),
            &key_info.base_key_schema,
        )
        .unwrap();
        let same_partition = grown
            .partitions
            .iter()
            .find(|partition| {
                partition
                    .lower
                    .as_ref()
                    .is_none_or(|lower| &same_hash >= lower)
                    && partition
                        .upper
                        .as_ref()
                        .is_none_or(|upper| &same_hash < upper)
            })
            .unwrap();
        crate::read_barriers::assert_sdk_read_barrier_with_scan(
            &sdk,
            &capacity_client.clone(),
            same_partition,
            &key_info.account_id,
            &[("same", "10"), ("samf", "0")],
        )
        .await;
        assert!(
            source.network.accepted.load(Ordering::SeqCst) > 0,
            "signed calls bypassed native peer sockets"
        );
        let described = sdk
            .describe_table()
            .table_name("Numbers")
            .send()
            .await
            .unwrap();
        assert_eq!(
            described.table().and_then(|table| table.table_name()),
            Some("Numbers")
        );
        let new_table = sdk
            .create_table()
            .table_name("NewTable")
            .key_schema(
                aws_sdk_dynamodb::types::KeySchemaElement::builder()
                    .attribute_name("id")
                    .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                aws_sdk_dynamodb::types::AttributeDefinition::builder()
                    .attribute_name("id")
                    .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
            .send()
            .await
            .unwrap();
        assert_eq!(
            new_table
                .table_description()
                .and_then(|table| table.table_name()),
            Some("NewTable")
        );
        let new_table_id = new_table
            .table_description()
            .and_then(|table| table.table_id())
            .unwrap()
            .to_owned();
        let new_route = crate::single_leaf_route(&capacity_client, &account, &new_table_id.clone())
            .await
            .unwrap();
        let mut capacity_cursor = None;
        provisioner
            .reconcile_account_capacity(
                "123456789012",
                capacity_client.clone(),
                u64::MAX,
                &mut capacity_cursor,
            )
            .await
            .unwrap();
        assert_eq!(capacity_cursor.as_ref().unwrap().table_name, "NewTable");
        provisioner
            .reconcile_account_capacity(
                "123456789012",
                capacity_client.clone(),
                u64::MAX,
                &mut capacity_cursor,
            )
            .await
            .unwrap();
        assert_eq!(capacity_cursor.as_ref().unwrap().table_name, "NewTable");
        assert_eq!(capacity_cursor.as_ref().unwrap().after_lower, None);
        provisioner
            .reconcile_account_capacity(
                "123456789012",
                capacity_client.clone(),
                u64::MAX,
                &mut capacity_cursor,
            )
            .await
            .unwrap();
        assert_eq!(capacity_cursor.as_ref().unwrap().table_name, "Numbers");
        assert_eq!(
            capacity_cursor.as_ref().unwrap().after_lower,
            Some(RangeKey::minimum())
        );
        let new_target = data_target(
            "123456789012",
            &new_table_id,
            &new_route.partitions[0].partition_id,
        )
        .unwrap();
        let new_item = HashMap::from([("id".to_owned(), AwsAttributeValue::S("first".into()))]);
        sdk.put_item()
            .table_name("NewTable")
            .set_item(Some(new_item.clone()))
            .send()
            .await
            .unwrap();
        let new_read = sdk
            .get_item()
            .table_name("NewTable")
            .set_key(Some(new_item.clone()))
            .send()
            .await
            .unwrap();
        assert_eq!(new_read.item(), Some(&new_item));
        let mut queried = Vec::new();
        let mut query_cursor = None;
        for _ in 0..10 {
            let page = sdk
                .query()
                .table_name("Numbers")
                .key_condition_expression("pk = :pk")
                .expression_attribute_values(":pk", AwsAttributeValue::S("same".into()))
                .limit(3)
                .set_exclusive_start_key(query_cursor)
                .send()
                .await
                .unwrap();
            queried.extend_from_slice(page.items());
            query_cursor = page.last_evaluated_key().cloned();
            if query_cursor.is_none() {
                break;
            }
        }
        assert!(query_cursor.is_none());
        assert_eq!(queried.len(), 7);
        assert_eq!(queried[0]["sk"], AwsAttributeValue::N("-10".into()));
        assert_eq!(queried[6]["sk"], AwsAttributeValue::N("10".into()));
        let mut scanned = Vec::new();
        let mut scan_cursor = None;
        for _ in 0..10 {
            let page = sdk
                .scan()
                .table_name("Numbers")
                .limit(3)
                .set_exclusive_start_key(scan_cursor)
                .send()
                .await
                .unwrap();
            scanned.extend_from_slice(page.items());
            scan_cursor = page.last_evaluated_key().cloned();
            if scan_cursor.is_none() {
                break;
            }
        }
        assert!(scan_cursor.is_none());
        assert_eq!(scanned.len(), 9);
        assert!(scanned.contains(&sdk_item));
        let mutation_key = HashMap::from([
            ("pk".to_owned(), AwsAttributeValue::S("same".into())),
            ("sk".to_owned(), AwsAttributeValue::N("9".into())),
        ]);
        sdk.put_item()
            .table_name("Numbers")
            .set_item(Some(mutation_key.clone()))
            .send()
            .await
            .unwrap();
        sdk.update_item()
            .table_name("Numbers")
            .set_key(Some(mutation_key.clone()))
            .update_expression("SET note = :note")
            .expression_attribute_values(":note", AwsAttributeValue::S("updated".into()))
            .condition_expression("attribute_exists(pk)")
            .send()
            .await
            .unwrap();
        let updated = sdk
            .get_item()
            .table_name("Numbers")
            .set_key(Some(mutation_key.clone()))
            .send()
            .await
            .unwrap();
        assert_eq!(
            updated.item().and_then(|item| item.get("note")),
            Some(&AwsAttributeValue::S("updated".into()))
        );
        sdk.delete_item()
            .table_name("Numbers")
            .set_key(Some(mutation_key.clone()))
            .condition_expression("attribute_exists(pk)")
            .send()
            .await
            .unwrap();
        let deleted = sdk
            .get_item()
            .table_name("Numbers")
            .set_key(Some(mutation_key))
            .send()
            .await
            .unwrap();
        assert!(deleted.item().is_none());
        let denied_table = sdk
            .describe_table()
            .table_name("Forbidden")
            .send()
            .await
            .unwrap_err();
        assert!(format!("{denied_table:?}").contains("AccessDeniedException"));
        assert!(
            authorization
                .delete_user_policy("123456789012", "test-user", "tables")
                .await
                .unwrap()
        );
        let denied_after_removal = sdk
            .get_item()
            .table_name("Numbers")
            .set_key(Some(sdk_item.clone()))
            .send()
            .await
            .unwrap_err();
        assert!(format!("{denied_after_removal:?}").contains("AccessDeniedException"));
        authorization
            .put_user_policy("123456789012", "test-user", "tables", &policy_document)
            .await
            .unwrap();
        let wrong_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                TEST_ACCESS_KEY,
                "incorrect-test-secret",
                None,
                None,
                "beyonddb-negative-test",
            ))
            .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled())
            .endpoint_url(endpoint)
            .load()
            .await;
        let wrong_sdk = aws_sdk_dynamodb::Client::new(&wrong_config);
        let rejected = tokio::time::timeout(
            Duration::from_secs(10),
            wrong_sdk
                .get_item()
                .table_name("Numbers")
                .set_key(Some(sdk_item.clone()))
                .send(),
        )
        .await
        .unwrap();
        assert!(rejected.is_err());
        authorization
            .set_user_boundary("123456789012", "test-user", Some(&boundary_document))
            .await
            .unwrap();
        let management_store =
            CellCredentialStore::new(capacity_client.clone(), TEST_ENCRYPTION_KEY);
        assert!(
            management_store
                .revoke_credential(TEST_ACCESS_KEY)
                .await
                .unwrap()
        );
        assert!(
            management_store
                .revoke_credential(TEST_ACCESS_KEY)
                .await
                .unwrap()
        );
        let observed_revoked = observed_credentials
            .lookup_credential(TEST_ACCESS_KEY)
            .await
            .unwrap()
            .unwrap();
        assert!(!observed_revoked.is_active);
        assert_eq!(value_queries.load(Ordering::SeqCst), 4);
        let revoked = tokio::time::timeout(
            Duration::from_secs(10),
            sdk.get_item()
                .table_name("Numbers")
                .set_key(Some(sdk_item.clone()))
                .send(),
        )
        .await
        .unwrap();
        assert!(revoked.is_err());
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        let context = OperationContext {
            storage: routed.clone(),
            limits: Arc::new(LimitsConfig::default()),
            region: Arc::from("us-east-1"),
            account_id: Arc::from("123456789012"),
            import_paths: Arc::from([]),
            export_paths: Arc::from([]),
            pre_fetched_key_info: None,
            auth_cache: AuthCacheRegistry::empty(),
            table_key_info_lookup: None,
        };
        let wire_item = serde_json::json!({"pk": {"S": "same"}, "sk": {"N": "7"}});
        let written = extenddb_engine::dispatch(
            "PutItem",
            serde_json::json!({"TableName": "Numbers", "Item": wire_item}),
            &context,
            "http://localhost",
        )
        .await
        .unwrap();
        assert!(written.body.get("Attributes").is_none());
        let wire_read = extenddb_engine::dispatch(
            "GetItem",
            serde_json::json!({"TableName": "Numbers", "Key": wire_item}),
            &context,
            "http://localhost",
        )
        .await
        .unwrap();
        assert_eq!(wire_read.body["Item"], wire_item);
        let persisted_keys = [
            serde_json::from_value::<Item>(wire_item).unwrap(),
            serde_json::from_value::<Item>(serde_json::json!({"pk":{"S":"same"},"sk":{"N":"8"}}))
                .unwrap(),
        ];
        let mut before = Vec::new();
        for key in &persisted_keys {
            let (target, _) =
                crate::transaction_fixture::participant_for_key(&capacity_client, &key_info, key)
                    .await;
            let old = native_peer::identity(&observed, SOURCE, &target)
                .or_else(|| native_peer::identity(&observed, FRONT, &target))
                .unwrap();
            before.push((target, old.clone(), inspection.owner(&old.scope).await));
        }
        let credential_identity =
            native_peer::identity(&observed, SOURCE, &credential_target).unwrap();
        front.stop().await;
        source.stop().await;
        let snapshot =
            crate::capacity_snapshot::restore(&bucket, &credential_identity.scope, host.handle())
                .await;
        let stored_record: Vec<u8> = crate::capacity_snapshot::open(&snapshot)
            .query_row(
                "SELECT record FROM ddb_credentials WHERE access_key_id = ?1",
                [TEST_ACCESS_KEY],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            !stored_record
                .windows(TEST_SECRET_KEY.len())
                .any(|window| window == TEST_SECRET_KEY.as_bytes())
        );
        std::fs::remove_dir_all(directory.path().join(SOURCE)).unwrap();
        std::fs::remove_dir_all(directory.path().join(FRONT)).unwrap();
        let replacement = "numeric-replacement";
        let mut restored = Peer::start(
            application,
            &directory.path().join(replacement),
            bucket,
            observed.clone(),
            true,
            host.handle(),
            core.handle(),
        )
        .await;
        inspection.departed_node(SOURCE, replacement).await;
        inspection.departed_node(FRONT, replacement).await;
        let new_client = restored.client.clone();
        let restored_authorization = CellAuthorizationStore::new(new_client.clone());
        assert_eq!(
            restored_authorization
                .fetch_user_policies("123456789012", "test-user")
                .await
                .unwrap(),
            vec![policy_document]
        );
        assert_eq!(
            restored_authorization
                .fetch_user_boundary("123456789012", "test-user")
                .await
                .unwrap(),
            Some(boundary_document)
        );
        let new_key = Item::from([("id".into(), AttributeValue::S("first".into()))]);
        assert_eq!(
            new_client
                .query::<PartitionGet>(
                    &new_target,
                    None,
                    Json(PartitionGetInput {
                        table_id: new_table_id,
                        epoch: new_route.partitions[0].epoch,
                        key: new_key.clone()
                    })
                )
                .await
                .unwrap()
                .output
                .0,
            PartitionGetOutcome::Found(Some(new_key))
        );
        let restored_store = CellCredentialStore::new(new_client.clone(), TEST_ENCRYPTION_KEY);
        assert!(
            restored_store
                .lookup_credential(TEST_ACCESS_KEY)
                .await
                .unwrap()
                .is_some_and(
                    |credential| credential.secret_key == TEST_SECRET_KEY && !credential.is_active
                )
        );
        let wrong_key_store = CellCredentialStore::new(new_client.clone(), [0; 32]);
        assert!(
            wrong_key_store
                .lookup_credential(TEST_ACCESS_KEY)
                .await
                .is_err()
        );
        for ((target, old, old_owner), key) in before.into_iter().zip(persisted_keys) {
            let state = new_client
                .query::<ReadPartitionState>(&target, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap();
            assert_eq!(
                new_client
                    .query::<PartitionGet>(
                        &target,
                        None,
                        Json(PartitionGetInput {
                            table_id: created.table_id.clone(),
                            epoch: state.spec.epoch,
                            key: key.clone()
                        })
                    )
                    .await
                    .unwrap()
                    .output
                    .0,
                PartitionGetOutcome::Found(Some(key))
            );
            let actual = native_peer::identity(&observed, replacement, &target).unwrap();
            assert_eq!(actual.scope, old.scope);
            assert!(actual.epoch > old_owner.epoch);
            let owner = inspection.owner(&actual.scope).await;
            assert_eq!(owner.node.as_deref(), Some(replacement));
            assert_eq!(actual.epoch, owner.epoch);
        }
        restored.stop().await;
        inspection.close().await;
    });
}
