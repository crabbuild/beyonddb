//! Non-owning SDK replica inspection; never open a cell's live sparse cache.
use cellar::sdk::{
    ExecutionDomain, ReplicaStorage, ReplicaStorageOptions, Replication, ReplicationOptions,
};
use std::time::Duration;

pub async fn restore(
    bucket: &cellar::bucket::Bucket,
    host: &tokio::runtime::Handle,
) -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    let domain = ExecutionDomain::new(host.clone());
    let result = domain
        .run(async {
            let replication = Replication::start(
                &directory.path().join("staging"),
                ReplicaStorage::new(
                    bucket.store.clone(),
                    ReplicaStorageOptions {
                        prefix: bucket.prefix.clone(),
                        ..Default::default()
                    },
                )?,
                ReplicationOptions {
                    flush_interval: Duration::ZERO,
                    compaction: None,
                    ..Default::default()
                },
            )?;
            // The bucket belongs only to this fixture. Walk its bounded cell
            // inventory so inspection uses actual persisted SDK identities.
            let mut page_token = None;
            let mut copied = 0;
            let mut visited = 0;
            loop {
                let page = bucket
                    .common_prefixes_page("cells/", None, page_token, 64)
                    .await?;
                for prefix in page.prefixes {
                    visited += 1;
                    anyhow::ensure!(
                        visited <= 128,
                        "fixture replica inventory exceeded its bound"
                    );
                    let scope = prefix
                        .strip_prefix("cells/")
                        .ok_or_else(|| anyhow::anyhow!("unexpected cell prefix: {prefix}"))?
                        .trim_end_matches('/');
                    if let Some(snapshot) = replication.restore_snapshot(scope).await? {
                        std::fs::copy(
                            snapshot.path(),
                            directory.path().join(format!("{copied}.sqlite")),
                        )?;
                        copied += 1;
                    }
                }
                page_token = page.page_token;
                if page_token.is_none() {
                    break;
                }
            }
            anyhow::ensure!(
                copied >= 2,
                "fixture has no complete participant replica inventory"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await;
    tokio::time::timeout(Duration::from_secs(10), domain.shutdown())
        .await
        .unwrap();
    result.unwrap().unwrap();
    directory
}
