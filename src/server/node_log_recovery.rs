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
    use cellule_runtime::{
        cell::catalog::{CatalogEntry, CatalogRole},
        identity::{CellTarget, Digest, TenantId},
    };
    use cellule_store::Store;
    use object_store::{memory::InMemory, path::Path as ObjectPath};

    use crate::{APPLICATION, APPLICATION_ID, NAMESPACE};

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
}
