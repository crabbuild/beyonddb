//! Authentication must fail closed until an unreachable native owner expires.
use crate::{native_peer::*, runtimes};
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use beyonddb::*;
use std::sync::{Arc, Mutex, atomic::Ordering};

#[test]
fn signed_sdk_authentication_recovers_expired_published_credential_owner() {
    let (host, core) = runtimes();
    host.block_on(async {
        for cached in [false, true] {
            let files = tempfile::tempdir().unwrap();
            let bucket =
                cellar::bucket::Bucket::open_dev(&files.path().join("bucket.sqlite")).unwrap();
            let observed = Arc::new(Mutex::new(Default::default()));
            let app = application();
            let source_path = files.path().join(SOURCE);
            let mut source = Peer::start(
                app.clone(),
                &source_path,
                bucket.clone(),
                observed.clone(),
                cached,
                host.handle(),
                core.handle(),
            )
            .await;
            // Establish the credential shard before the other node exists.
            // Its real owner remains SOURCE when a second key is added below.
            const WARM: &str = "AKIACREDENTIALWARMUP";
            put_credential(&source.client, WARM, "000000000000").await;
            let credential = credential_target(WARM).unwrap();
            let original = identity(&observed, SOURCE, &credential).unwrap();
            let mut front = Peer::start(
                app,
                &files.path().join(FRONT),
                bucket.clone(),
                observed.clone(),
                cached,
                host.handle(),
                core.handle(),
            )
            .await;
            let inspection = Inspection::new(bucket, host.handle());
            inspection.ready_peers().await;
            // Choose from actual SDK placements, never install or rewrite an
            // owner record. Account/IAM must remain reachable across the cut.
            let mut account = None;
            for n in 0..32 {
                let candidate = format!("{n:012}");
                let target = account_target(&candidate).unwrap();
                front
                    .client
                    .query::<ListTables>(
                        &target,
                        None,
                        Json(ListTablesInput {
                            limit: 1,
                            exclusive_start: None,
                            live_only: false,
                        }),
                    )
                    .await
                    .unwrap();
                if identity(&observed, FRONT, &target).is_some() {
                    account = Some(candidate);
                    break;
                }
            }
            let account = account.expect("SDK did not place any candidate account on the frontend");
            policy(&front.client, &account).await;
            let access = (0..100_000)
                .map(|n| format!("AKIANATIVE{n:08}"))
                .find(|key| credential_target(key).unwrap() == credential)
                .unwrap();
            put_credential(&source.client, &access, &account).await;
            assert!(identity(&observed, FRONT, &credential).is_none());
            let mut table_info = None;
            for n in 0..16 {
                let name = format!("CredentialData{n}");
                let (record, data) = table(&front.client, &account, &name, false).await;
                if let Some((target, item)) = data
                    .into_iter()
                    .find(|(target, _)| identity(&observed, FRONT, target).is_some())
                {
                    table_info = Some((record, target, item));
                    break;
                }
            }
            let (record, target, expected) =
                table_info.expect("SDK did not place any candidate data on the frontend");
            let http = Http::start(&front.client, &access).await;
            let extenddb_core::types::AttributeValue::S(id) = &expected["id"] else {
                unreachable!()
            };
            let expected_sdk = std::collections::HashMap::from([
                (
                    "id".into(),
                    aws_sdk_dynamodb::types::AttributeValue::S(id.clone()),
                ),
                (
                    "value".into(),
                    aws_sdk_dynamodb::types::AttributeValue::N("1".into()),
                ),
            ]);
            let read = || {
                http.sdk
                    .get_item()
                    .table_name(&record.table_name)
                    .key("id", aws_sdk_dynamodb::types::AttributeValue::S(id.clone()))
                    .consistent_read(true)
                    .send()
            };
            assert_eq!(read().await.unwrap().item, Some(expected_sdk.clone()));
            let unknown = (0..256)
                .map(|n| format!("AKIAUNKNOWN{n:08}"))
                .find(|key| credential_target(key).unwrap() != credential)
                .unwrap();
            let unknown_target = credential_target(&unknown).unwrap();
            let unknown_sdk = aws_sdk_dynamodb::Client::from_conf(
                http.sdk
                    .config()
                    .to_builder()
                    .credentials_provider(Credentials::new(
                        unknown,
                        "unknown-secret",
                        None,
                        None,
                        "unknown-key-test",
                    ))
                    .build(),
            );
            let scopes_before = inspection.credential_scopes(&original.class).await;
            let unknown_error = unknown_sdk
                .get_item()
                .table_name(&record.table_name)
                .key("id", aws_sdk_dynamodb::types::AttributeValue::S(id.clone()))
                .send()
                .await
                .unwrap_err();
            assert_eq!(
                unknown_error.as_service_error().unwrap().code(),
                Some("UnrecognizedClientException")
            );
            let before = inspection.owner(&original.scope).await;
            assert_eq!(before.node.as_deref(), Some(SOURCE));
            assert!(source.network.accepted.load(Ordering::SeqCst) > 0);
            source.network.cut(true).await;
            assert!(
                read().await.is_err(),
                "credential failure must not bypass authentication"
            );
            // Raw domain access on the frontend still succeeds. The failed
            // signed request is therefore not merely an unavailable data owner.
            assert!(identity(&observed, FRONT, &account_target(&account).unwrap()).is_some());
            let state = front
                .client
                .query::<ReadPartitionState>(&target, None, Json(()))
                .await
                .unwrap()
                .output
                .0
                .unwrap();
            let result = front
                .client
                .query::<PartitionGet>(
                    &target,
                    None,
                    Json(PartitionGetInput {
                        table_id: record.id.clone(),
                        epoch: state.spec.epoch,
                        key: extenddb_core::types::extract_key(&expected, &record.key_schema),
                    }),
                )
                .await
                .unwrap()
                .output
                .0;
            assert_eq!(result, PartitionGetOutcome::Found(Some(expected.clone())));
            assert!(inspection.live(SOURCE).await);
            assert_eq!(
                inspection.owner(&original.scope).await,
                before,
                "a network failure cannot grant takeover authority"
            );
            assert!(source.node.as_ref().unwrap().is_ready().await.unwrap());
            source.network.cut(false).await;
            assert_eq!(read().await.unwrap().item, Some(expected_sdk.clone()));
            source.network.cut(true).await;
            source.stop().await;
            std::fs::remove_dir_all(&source_path).unwrap();
            inspection.departed().await;
            assert_eq!(
                read().await.unwrap().item,
                Some(expected_sdk),
                "credential recovery cache={cached}"
            );
            let after = inspection.owner(&original.scope).await;
            assert_eq!(after.node.as_deref(), Some(FRONT));
            assert!(after.epoch > before.epoch);
            assert_eq!(
                identity(&observed, FRONT, &credential).unwrap().scope,
                original.scope
            );
            assert_eq!(
                inspection.credential_scopes(&original.class).await,
                scopes_before,
                "unknown access keys must not create credential scopes"
            );
            for node in [SOURCE, FRONT] {
                assert!(identity(&observed, node, &unknown_target).is_none());
            }
            assert!(
                CellCredentialStore::new(front.client.clone(), ENCRYPTION)
                    .revoke_credential(&access)
                    .await
                    .unwrap()
            );
            let revoked = read().await.unwrap_err();
            assert_eq!(
                revoked.as_service_error().unwrap().code(),
                Some("UnrecognizedClientException")
            );
            drop(unknown_sdk);
            http.stop().await;
            front.stop().await;
            inspection.close().await;
        }
    });
}
