//! Complete non-owning SDK snapshots for capacity audits.
pub async fn restore(
    bucket: &cellar::bucket::Bucket,
    scope: &str,
    host: &tokio::runtime::Handle,
) -> tempfile::TempDir {
    use cellar::sdk::{
        ExecutionDomain, ReplicaStorage, ReplicaStorageOptions, Replication, ReplicationOptions,
    };
    let files = tempfile::tempdir().unwrap();
    let domain = ExecutionDomain::new(host.clone());
    let restored = domain
        .run(async {
            let replication = Replication::start(
                &files.path().join("restore"),
                ReplicaStorage::new(
                    bucket.store.clone(),
                    ReplicaStorageOptions {
                        prefix: bucket.prefix.clone(),
                        ..Default::default()
                    },
                )?,
                ReplicationOptions {
                    flush_interval: std::time::Duration::ZERO,
                    compaction: None,
                    ..Default::default()
                },
            )?;
            let snapshot = replication
                .restore_snapshot(scope)
                .await?
                .ok_or_else(|| anyhow::anyhow!("capacity snapshot is absent"))?;
            std::fs::copy(snapshot.path(), files.path().join("snapshot.sqlite"))?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    tokio::time::timeout(std::time::Duration::from_secs(10), domain.shutdown())
        .await
        .unwrap();
    restored.unwrap().unwrap();
    files
}

pub fn open(snapshot: &tempfile::TempDir) -> rusqlite::Connection {
    rusqlite::Connection::open_with_flags(
        snapshot.path().join("snapshot.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap()
}
