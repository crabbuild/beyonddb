//! Product-side inventory and overlay attachment for a fenced node log.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

use cellule_ltx::NodeFrameScope;
use cellule_runtime::{
    Error, Result,
    cell::catalog::CellCatalog,
    control::authority::CellAuthority,
    identity::{CellId, TenantId},
    ltx::{CellStorageLayout, Limits},
    node::{
        FencedNodeSession, NodeDirectory,
        log_recovery::{
            CompletedNodeRecovery, NodeLogRecovery, RecoveryCell, RecoveryCoordinator,
            recoverable_cells_from_scopes,
        },
        log_transport::NodeLogTransport,
    },
    recovery::manifest::RecoveryManifestStore,
};
use futures_util::StreamExt;

use super::node_lease::unix_time_ms;

/// Seal a claimed dead owner's log, pin every untiered Cell overlay, then seal
/// the directory record so ordinary Cell takeover can proceed.
///
/// The caller must retain its live node lease and provide a transport bound to
/// the claiming session. A failed inventory or overlay attachment leaves the
/// log unsealed; retry with a fresh fenced claim rather than taking over a Cell.
pub async fn recover_fenced_node_log(
    directory: &NodeDirectory,
    layout: &CellStorageLayout,
    transport: Arc<dyn NodeLogTransport>,
    fenced: FencedNodeSession,
    limits: Limits,
    scratch: PathBuf,
    max_tenants: usize,
    max_cells: usize,
) -> Result<CompletedNodeRecovery> {
    if max_tenants == 0 || max_cells == 0 {
        return Err(Error::Capacity("node-log recovery inventory bound is zero"));
    }
    tokio::fs::create_dir_all(&scratch)
        .await
        .map_err(|source| Error::Facility {
            name: "node-log recovery scratch",
            source: Box::new(source),
        })?;
    let recovery = NodeLogRecovery::from_fenced(transport, &fenced, limits)?
        .with_recovery_scratch(scratch.clone());
    let sealed = recovery.ensure_sealed_bounded().await?;
    let scopes = sealed.scopes(limits)?;
    let cells = recovery_cells(layout, fenced.session(), &scopes, max_tenants, max_cells).await?;
    let manifests =
        RecoveryManifestStore::new(layout.clone(), limits).with_recovery_scratch(scratch);
    let coordinator = RecoveryCoordinator::new(recovery, manifests);
    let controls = coordinator
        .recover_sealed(fenced.clone(), cells, sealed)
        .await?;
    coordinator
        .finish(directory, fenced, controls, unix_time_ms()?)
        .await
}

async fn recovery_cells(
    layout: &CellStorageLayout,
    owner: cellule_runtime::identity::SessionId,
    scopes: &[NodeFrameScope],
    max_tenants: usize,
    max_cells: usize,
) -> Result<Vec<RecoveryCell>> {
    let grouped = locate_scope_tenants(layout, scopes, max_tenants, max_cells).await?;
    let authority = CellAuthority::new(layout.clone());
    let mut cells = Vec::with_capacity(scopes.len());
    for (tenant, tenant_scopes) in grouped {
        let catalog = CellCatalog::new(layout.clone(), TenantId::from_bytes(tenant));
        let mut found =
            recoverable_cells_from_scopes(&catalog, &authority, owner, &tenant_scopes, max_cells)
                .await?;
        cells.append(&mut found);
    }
    if cells.len() != scopes.len() {
        return Err(Error::Catalog(
            "node-log recovery Cell inventory is incomplete",
        ));
    }
    Ok(cells)
}

async fn locate_scope_tenants(
    layout: &CellStorageLayout,
    scopes: &[NodeFrameScope],
    max_tenants: usize,
    max_cells: usize,
) -> Result<BTreeMap<[u8; 16], Vec<NodeFrameScope>>> {
    if max_tenants == 0 || max_cells == 0 || scopes.len() > max_cells {
        return Err(Error::Capacity(
            "node-log recovery inventory exceeds its limit",
        ));
    }
    let mut unique = BTreeSet::new();
    let mut required_shards = BTreeSet::new();
    for scope in scopes {
        if scope.application != *layout.application_id() {
            return Err(Error::Catalog("node-log recovery application differs"));
        }
        if !unique.insert(scope.cell) {
            return Err(Error::Catalog("node-log recovery Cell scope repeats"));
        }
        required_shards.insert(scope.cell[0]);
    }
    if scopes.is_empty() {
        return Ok(BTreeMap::new());
    }

    let prefix = layout.catalog_tenants_prefix();
    let prefix_with_separator = format!("{prefix}/");
    let mut stream = layout.store().list_stream(&prefix);
    let mut tenants = BTreeSet::new();
    let mut heads = BTreeMap::<u8, BTreeSet<[u8; 16]>>::new();
    let max_heads = max_tenants
        .checked_mul(256)
        .ok_or(Error::Capacity("node-log recovery catalog head bound"))?;
    let mut head_count = 0_usize;
    while let Some(meta) = stream.next().await {
        let meta = meta?;
        head_count = head_count
            .checked_add(1)
            .ok_or(Error::Capacity("node-log recovery catalog head count"))?;
        if head_count > max_heads {
            return Err(Error::Capacity("node-log recovery catalog head limit"));
        }
        let (tenant, shard) = parse_catalog_head(&prefix_with_separator, meta.location.as_ref())?;
        tenants.insert(tenant);
        if tenants.len() > max_tenants {
            return Err(Error::Capacity("node-log recovery tenant limit"));
        }
        if required_shards.contains(&shard) {
            heads.entry(shard).or_default().insert(tenant);
        }
    }

    let mut grouped = BTreeMap::<[u8; 16], Vec<NodeFrameScope>>::new();
    for scope in scopes {
        let mut found = None;
        if let Some(candidates) = heads.get(&scope.cell[0]) {
            for tenant in candidates {
                let catalog = CellCatalog::new(layout.clone(), TenantId::from_bytes(*tenant));
                if catalog
                    .lookup(CellId::from_bytes(scope.cell))
                    .await?
                    .is_some()
                    && found.replace(*tenant).is_some()
                {
                    return Err(Error::Catalog(
                        "node-log recovery Cell has multiple tenants",
                    ));
                }
            }
        }
        let tenant = found.ok_or(Error::Catalog(
            "node-log recovery Cell has no tenant catalog entry",
        ))?;
        grouped.entry(tenant).or_default().push(*scope);
    }
    Ok(grouped)
}

fn parse_catalog_head(prefix: &str, path: &str) -> Result<([u8; 16], u8)> {
    let relative = path
        .strip_prefix(prefix)
        .ok_or(Error::Catalog("node-log recovery catalog path differs"))?;
    let mut parts = relative.split('/');
    let tenant = parts.next().ok_or(Error::Catalog(
        "node-log recovery tenant path is incomplete",
    ))?;
    let shard = parts
        .next()
        .ok_or(Error::Catalog("node-log recovery shard path is incomplete"))?;
    if parts.next() != Some("head.json") || parts.next().is_some() {
        return Err(Error::Catalog(
            "node-log recovery catalog head path differs",
        ));
    }
    let tenant = decode_lower_hex::<16>(tenant)?;
    let shard = decode_lower_hex::<1>(shard)?[0];
    Ok((tenant, shard))
}

fn decode_lower_hex<const N: usize>(value: &str) -> Result<[u8; N]> {
    if value.len() != N * 2 {
        return Err(Error::Catalog("node-log recovery catalog hex length"));
    }
    let mut bytes = [0_u8; N];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = nibble(pair[0]).ok_or(Error::Catalog("node-log recovery catalog hex"))?;
        let low = nibble(pair[1]).ok_or(Error::Catalog("node-log recovery catalog hex"))?;
        bytes[index] = (high << 4) | low;
    }
    Ok(bytes)
}

const fn nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Bytes;
    use cellule_app::CellApplication;
    use cellule_host::CellNodeBuilder;
    use cellule_ltx::{CellReplica, encode_node_frame};
    use cellule_runtime::{
        SqlWorkerPool,
        cell::catalog::{CatalogEntry, CatalogRole},
        control::Owner,
        follower::FollowerReceipt,
        identity::{CellTarget, Digest, IncarnationId, NodeId, SessionId, TenantId},
        ltx::{DiskBudget, Host},
        node::{
            NODE_LOG_PROTOCOL_VERSION, NodeAdvertisement, NodeCapacity, NodeFailureDomain,
            log_transport::{
                AppendRequest, LocalFollowerTransport, RetireRequest, SealRequest, TailRequest,
            },
        },
        registry::BuildDescriptor,
    };
    use cellule_store::Store;
    use ed25519_dalek::SigningKey;
    use futures_util::future::BoxFuture;
    use object_store::{memory::InMemory, path::Path as ObjectPath};
    use std::time::Duration;

    use crate::{
        APPLICATION, APPLICATION_ID, Beyonddb, NAMESPACE, account_target, initialize_account,
    };

    fn scope(cell: CellId) -> NodeFrameScope {
        NodeFrameScope {
            leader_session: [1; 16],
            log_epoch: 2,
            node_sequence: 1,
            application: *APPLICATION_ID.as_bytes(),
            cell: *cell.as_bytes(),
            incarnation: [2; 16],
            cell_epoch: 3,
            commit_sequence: 4,
        }
    }

    #[tokio::test]
    async fn scopes_resolve_only_their_durable_tenant_catalogs() {
        let layout = CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            ObjectPath::from("node-log-recovery-inventory-test"),
            *APPLICATION_ID.as_bytes(),
        );
        let one = CellTarget::new(
            TenantId::from_bytes([1; 16]),
            APPLICATION,
            NAMESPACE,
            b"one",
        )
        .unwrap();
        let two = CellTarget::new(
            TenantId::from_bytes([2; 16]),
            APPLICATION,
            NAMESPACE,
            b"two",
        )
        .unwrap();
        for target in [&one, &two] {
            CellCatalog::new(layout.clone(), target.tenant())
                .provision(
                    CatalogEntry::new(target, CatalogRole::Sql, Digest::from_bytes([3; 32]), 1)
                        .unwrap(),
                )
                .await
                .unwrap();
        }
        let scopes = [scope(one.cell_id()), scope(two.cell_id())];
        let grouped = locate_scope_tenants(&layout, &scopes, 2, 2).await.unwrap();
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[one.tenant().as_bytes()][0], scopes[0]);
        assert_eq!(grouped[two.tenant().as_bytes()][0], scopes[1]);
        assert!(locate_scope_tenants(&layout, &scopes, 1, 2).await.is_err());
        let missing = CellTarget::new(
            TenantId::from_bytes([3; 16]),
            APPLICATION,
            NAMESPACE,
            b"missing",
        )
        .unwrap();
        assert!(
            locate_scope_tenants(&layout, &[scope(missing.cell_id())], 2, 1)
                .await
                .is_err()
        );
    }

    #[test]
    fn catalog_head_parser_rejects_noncanonical_paths() {
        let prefix = "root/catalog/tenants/";
        let canonical = format!("{prefix}{}/ab/head.json", "01".repeat(16));
        assert_eq!(
            parse_catalog_head(prefix, &canonical).unwrap(),
            ([1; 16], 0xab)
        );
        assert!(parse_catalog_head(prefix, &canonical.replace("/ab/", "/AB/")).is_err());
        assert!(parse_catalog_head(prefix, &canonical.replace("head.json", "tail.json")).is_err());
    }

    struct EmptySealedFollower;

    impl NodeLogTransport for EmptySealedFollower {
        fn append<'a>(
            &'a self,
            _member: NodeId,
            _request: AppendRequest,
        ) -> BoxFuture<'a, Result<FollowerReceipt>> {
            Box::pin(async { Err(Error::Node("test follower cannot append")) })
        }

        fn seal<'a>(
            &'a self,
            _member: NodeId,
            _request: SealRequest,
        ) -> BoxFuture<'a, Result<FollowerReceipt>> {
            Box::pin(async {
                Ok(FollowerReceipt {
                    base_sequence: 0,
                    durable_through: 0,
                })
            })
        }

        fn retire<'a>(
            &'a self,
            _member: NodeId,
            _request: RetireRequest,
        ) -> BoxFuture<'a, Result<FollowerReceipt>> {
            Box::pin(async { Err(Error::Node("test follower cannot retire")) })
        }

        fn tail<'a>(
            &'a self,
            _member: NodeId,
            _request: TailRequest,
        ) -> BoxFuture<'a, Result<Vec<Bytes>>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    fn node_advertisement(
        node: NodeId,
        session: SessionId,
        key: u8,
        follower_bytes: u64,
        now: i64,
        lease_ms: i64,
    ) -> NodeAdvertisement {
        NodeAdvertisement::sign(
            node,
            session,
            format!("https://node-{key}.internal:8081"),
            Digest::from_bytes([80; 32]),
            Digest::from_bytes([key; 32]),
            Digest::from_bytes([81; 32]),
            Digest::from_bytes([82; 32]),
            &SigningKey::from_bytes(&[key; 32]),
            1,
            now,
            now + lease_ms,
            vec![Digest::from_bytes([86; 32])],
            vec![1],
            NodeFailureDomain::default(),
            NodeCapacity {
                free_memory_bytes: 16 << 20,
                free_disk_bytes: 1 << 30,
                follower_free_bytes: follower_bytes,
                job_credits: 8,
                log_protocol: NODE_LOG_PROTOCOL_VERSION,
                ..NodeCapacity::default()
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn empty_follower_tail_seals_claim_before_takeover() {
        let layout = CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            ObjectPath::from("node-log-recovery-empty-tail"),
            *APPLICATION_ID.as_bytes(),
        );
        let directory = NodeDirectory::new(
            layout.clone(),
            Digest::from_bytes([80; 32]),
            Digest::from_bytes([81; 32]),
            Digest::from_bytes([82; 32]),
        );
        let follower_node = NodeId::from_bytes([1; 16]);
        let follower_session = SessionId::from_bytes([2; 16]);
        let leader_node = NodeId::from_bytes([3; 16]);
        let leader_session = SessionId::from_bytes([4; 16]);
        let claimant_node = NodeId::from_bytes([5; 16]);
        let claimant_session = SessionId::from_bytes([6; 16]);
        let now = unix_time_ms().unwrap();
        directory
            .create(
                node_advertisement(follower_node, follower_session, 1, 1 << 30, now, 15_000),
                now,
            )
            .await
            .unwrap();
        let leader = directory
            .create(
                node_advertisement(leader_node, leader_session, 3, 0, now, 3_000),
                now,
            )
            .await
            .unwrap();
        let enrolled = directory
            .recruit_log(&leader, 1, 4_096, 16, now)
            .await
            .unwrap();
        directory.activate_log(&enrolled, now).await.unwrap();
        directory
            .create(
                node_advertisement(claimant_node, claimant_session, 5, 0, now, 15_000),
                now,
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(3_100)).await;
        let fenced = directory
            .claim_expired_for_recovery(leader_session, claimant_session, unix_time_ms().unwrap())
            .await
            .unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let result = recover_fenced_node_log(
            &directory,
            &layout,
            Arc::new(EmptySealedFollower),
            fenced,
            Limits::default(),
            scratch.path().to_owned(),
            1,
            1,
        )
        .await
        .unwrap();
        assert!(result.controls.is_empty());
        assert_eq!(result.takeover.claimant(), claimant_session);
        assert!(
            directory
                .takeover_proof(leader_session, claimant_session, unix_time_ms().unwrap())
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn untiered_account_frame_is_pinned_and_restored_before_takeover() {
        let application = Arc::new(
            Beyonddb::compile(BuildDescriptor {
                source_revision: "node-log-recovery-test".into(),
                cargo_lock_digest: Digest::from_bytes([7; 32]),
            })
            .unwrap(),
        );
        let target = account_target("123456789012").unwrap();
        let layout = CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            ObjectPath::from("node-log-recovery-account-frame"),
            *APPLICATION_ID.as_bytes(),
        );
        let files = tempfile::tempdir().unwrap();
        let source = CellNodeBuilder::new(Arc::clone(&application))
            .with_runtime(SqlWorkerPool::new(1, 8).unwrap(), 16 << 20)
            .with_replica_host(Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)))
            .with_session(SessionId::from_bytes([4; 16]))
            .build_unleased_for_maintenance()
            .unwrap();
        let catalog = CellCatalog::new(layout.clone(), target.tenant());
        let proof = catalog
            .provision(
                CatalogEntry::new(
                    &target,
                    CatalogRole::Sql,
                    application
                        .registry()
                        .module_code("beyonddb-account")
                        .unwrap(),
                    1,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let authority = CellAuthority::new(layout.clone());
        let leader_session = SessionId::from_bytes([4; 16]);
        let incarnation = IncarnationId::from_bytes([8; 16]);
        let initial = authority
            .create_initial(
                &proof,
                incarnation,
                Owner {
                    session: leader_session,
                    endpoint: "https://leader.internal:8081".into(),
                },
            )
            .await
            .unwrap();
        let replica = CellReplica::new(
            layout.clone(),
            *target.cell_id().as_bytes(),
            *incarnation.as_bytes(),
            Limits::default(),
        )
        .unwrap();
        let handle = source
            .runtime()
            .bootstrap(
                proof.clone(),
                replica.clone(),
                authority.clone(),
                initial,
                files.path().join("leader.sqlite"),
                initialize_account,
            )
            .await
            .unwrap();
        drop(handle);
        let observed = authority.load(target.cell_id()).await.unwrap().unwrap();
        let predecessor = observed.value().ltx_root().unwrap();
        let tail_path = files.path().join("tail.sqlite");
        let writable = replica
            .open_root(&predecessor)
            .await
            .unwrap()
            .paged()
            .prepare_writable(&tail_path)
            .await
            .unwrap();
        let mut writer = writable.open_writable(&tail_path).unwrap();
        writer
            .transaction(|transaction| {
                transaction.execute(
                    "UPDATE sys_meta SET commit_sequence = commit_sequence + 1, logical_time_ms = logical_time_ms + 1 WHERE singleton = 1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let capture = writer.capture().unwrap();
        let frames = capture
            .segments
            .iter()
            .enumerate()
            .map(|(index, segment)| {
                encode_node_frame(
                    NodeFrameScope {
                        leader_session: *leader_session.as_bytes(),
                        log_epoch: 1,
                        node_sequence: u64::try_from(index).unwrap() + 1,
                        application: *APPLICATION_ID.as_bytes(),
                        cell: *target.cell_id().as_bytes(),
                        incarnation: *incarnation.as_bytes(),
                        cell_epoch: observed.value().epoch,
                        commit_sequence: predecessor.commit_sequence + 1,
                    },
                    segment.info().clone(),
                    Bytes::from(std::fs::read(segment.path()).unwrap()),
                    Limits::default(),
                )
                .unwrap()
                .encoded()
                .clone()
            })
            .collect::<Vec<_>>();
        assert!(!frames.is_empty());
        writer.close().unwrap();

        let directory = NodeDirectory::new(
            layout.clone(),
            Digest::from_bytes([80; 32]),
            Digest::from_bytes([81; 32]),
            Digest::from_bytes([82; 32]),
        );
        let follower_node = NodeId::from_bytes([1; 16]);
        let follower_session = SessionId::from_bytes([2; 16]);
        let leader_node = NodeId::from_bytes([3; 16]);
        let claimant_session = SessionId::from_bytes([6; 16]);
        let now = unix_time_ms().unwrap();
        directory
            .create(
                node_advertisement(follower_node, follower_session, 1, 1 << 30, now, 15_000),
                now,
            )
            .await
            .unwrap();
        let leader = directory
            .create(
                node_advertisement(leader_node, leader_session, 3, 0, now, 3_000),
                now,
            )
            .await
            .unwrap();
        let enrolled = directory
            .recruit_log(&leader, 1, 4_096, 16, now)
            .await
            .unwrap();
        directory.activate_log(&enrolled, now).await.unwrap();
        directory
            .create(
                node_advertisement(
                    NodeId::from_bytes([5; 16]),
                    claimant_session,
                    5,
                    0,
                    now,
                    15_000,
                ),
                now,
            )
            .await
            .unwrap();
        let follower = cellule_runtime::FollowerStore::open(
            files.path().join("follower"),
            Limits::default(),
            DiskBudget::new(1 << 30),
        )
        .unwrap();
        let transport: Arc<dyn NodeLogTransport> =
            Arc::new(LocalFollowerTransport::new(follower_node, follower));
        transport
            .append(
                follower_node,
                AppendRequest {
                    leader_session,
                    log_epoch: 1,
                    frames,
                    covered_through: 0,
                },
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(3_100)).await;
        let fenced = directory
            .claim_expired_for_recovery(leader_session, claimant_session, unix_time_ms().unwrap())
            .await
            .unwrap();
        let scratch = files.path().join("recovery");
        let recovered = recover_fenced_node_log(
            &directory,
            &layout,
            transport,
            fenced,
            Limits::default(),
            scratch.clone(),
            1,
            1,
        )
        .await
        .unwrap();
        assert_eq!(recovered.controls.len(), 1);
        assert!(recovered.controls[0].value().recovery.is_some());

        let successor = cellule_runtime::CellRuntime::new(
            SqlWorkerPool::new(1, 8).unwrap(),
            16 << 20,
            claimant_session,
        )
        .unwrap();
        let restored = successor
            .takeover_restored(
                proof,
                replica,
                authority.clone(),
                recovered.controls[0].clone(),
                recovered.takeover,
                RecoveryManifestStore::new(layout.clone(), Limits::default())
                    .with_recovery_scratch(scratch),
                files.path().join("successor.sqlite"),
                Owner {
                    session: claimant_session,
                    endpoint: "https://successor.internal:8081".into(),
                },
            )
            .await
            .unwrap();
        let replayed_sequence = restored
            .query(64, 64, |connection| {
                let sequence = connection.query_row(
                    "SELECT commit_sequence FROM sys_meta WHERE singleton = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )?;
                Ok(sequence.to_be_bytes().to_vec())
            })
            .await
            .unwrap();
        assert_eq!(
            replayed_sequence,
            (predecessor.commit_sequence as i64 + 1).to_be_bytes()
        );
        assert_eq!(
            authority
                .load(target.cell_id())
                .await
                .unwrap()
                .unwrap()
                .value()
                .root
                .as_ref()
                .unwrap()
                .commit_sequence,
            predecessor.commit_sequence + 1
        );
        restored.drain().await.unwrap();
        successor.shutdown().await.unwrap();
    }
}
