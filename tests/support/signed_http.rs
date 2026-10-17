//! ExtendDB's real signed HTTP frontend over a supplied native SDK client.
use bdb_cellar_runtime::client::CellClient;
use beyonddb::{
    CellAuthorizationStore, CellCredentialStore, CellInitialPartitionProvisioner, build_http_state,
};
use extenddb_auth::StoredCredential;
use std::{sync::Arc, time::Duration};

pub struct SignedHttp {
    pub sdk: aws_sdk_dynamodb::Client,
    server: tokio::task::JoinHandle<anyhow::Result<()>>,
}
impl SignedHttp {
    pub async fn start(
        client: &CellClient,
        provisioner: Arc<CellInitialPartitionProvisioner>,
        bootstrap: bool,
    ) -> Self {
        let access = "AKIAIOSFODNN7EXAMPLE";
        let secret = "bdb-visibility-sdk-secret";
        let encryption = [19; 32];
        if bootstrap {
            CellCredentialStore::new(client.clone(), encryption)
                .put_credential(
                    access,
                    StoredCredential {
                        secret_key: secret.into(),
                        account_id: "123456789012".into(),
                        principal_name: "visibility-user".into(),
                        session_name: None,
                        is_session: false,
                        session_token: None,
                        is_active: true,
                        expires_at: None,
                    },
                )
                .await
                .unwrap();
            CellAuthorizationStore::new(client.clone()).put_user_policy("123456789012", "visibility-user", "visibility", &serde_json::json!({
            "Version":"2012-10-17", "Statement":[{"Effect":"Allow", "Action":"dynamodb:*", "Resource":"*"}]
        }).to_string()).await.unwrap();
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let state = build_http_state(
            client.clone(),
            provisioner,
            encryption,
            "us-east-1",
            endpoint.clone(),
        )
        .unwrap();
        let server = tokio::spawn(extenddb_server::start_server(listener, state, None, None));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert!(
                    !server.is_finished(),
                    "HTTP frontend stopped before readiness"
                );
                if let Ok(response) = reqwest::get(format!("{endpoint}/health")).await
                    && response.status().is_success()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-1"))
            .credentials_provider(aws_credential_types::Credentials::new(
                access, secret, None, None, "fixture",
            ))
            .endpoint_url(endpoint)
            .load()
            .await;
        Self {
            sdk: aws_sdk_dynamodb::Client::new(&config),
            server,
        }
    }
    pub async fn stop(self) {
        // Every test request is awaited before teardown. Drop its connection
        // pool, join the listener, then let the SDK node prove its full drain.
        drop(self.sdk);
        self.server.abort();
        assert!(self.server.await.unwrap_err().is_cancelled());
    }
}
