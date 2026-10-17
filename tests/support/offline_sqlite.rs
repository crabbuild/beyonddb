//! Read-only audits of test-owned state after the SDK has closed durability.
use std::{
    collections::HashMap,
    io::Read,
    path::{Path, PathBuf},
};

pub fn assert_coordinator_write_payloads_collected(root: &Path, ids: &[[u8; 16]]) {
    let mut found = HashMap::<[u8; 16], PathBuf>::new();
    inspect_databases(root, |path, db| {
        let coordinator: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='ddb_coordinator_transactions')", [], |row| row.get(0)).unwrap();
        if !coordinator {
            return;
        }
        for id in ids {
            let exists: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM ddb_coordinator_transactions WHERE transaction_id=?1)", [id.as_slice()], |row| row.get(0)).unwrap();
            if !exists {
                continue;
            }
            assert!(
                found.insert(*id, path.to_path_buf()).is_none(),
                "transaction found in more than one restored database: {id:?}"
            );
            let counts: (u64, u64) = db.query_row(
                    "SELECT (SELECT COUNT(*) FROM ddb_transaction_payloads WHERE transaction_id=?1 AND position>=0), (SELECT COUNT(*) FROM ddb_coordinator_participants WHERE transaction_id=?1 AND operation_chunks IS NULL)",
                    [id.as_slice()], |row| Ok((row.get(0)?, row.get(1)?)),
                ).unwrap();
            assert_eq!(
                counts,
                (0, 2),
                "completed operation images must be released for {id:?} in {path:?}"
            );
        }
    });
    assert_eq!(
        found.len(),
        ids.len(),
        "all coordinator states must be present after SDK shutdown"
    );
}

/// Every admitted participant image must be gone after an acknowledged public
/// read or recovered cleanup, including images not returned by a point query.
pub fn assert_read_images_collected(root: &Path) {
    let mut participants = 0;
    inspect_databases(root, |path, db| {
        let exists: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='ddb_transaction_reads')", [], |row| row.get(0)).unwrap();
        if exists {
            participants += 1;
            let saved: u64 = db
                .query_row("SELECT COUNT(*) FROM ddb_transaction_reads", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(
                saved, 0,
                "returned or acknowledged read leaked saved images in {path:?}"
            );
        }
    });
    assert!(
        participants >= 2,
        "both account and data participant databases must be audited"
    );
}

fn inspect_databases(root: &Path, mut inspect: impl FnMut(&Path, &rusqlite::Connection)) {
    let mut paths = vec![root.to_path_buf()];
    while let Some(path) = paths.pop() {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                paths.push(entry.path());
                continue;
            }
            if !kind.is_file() {
                continue;
            }
            let path = entry.path();
            let mut header = [0; 16];
            if std::fs::File::open(&path)
                .unwrap()
                .read_exact(&mut header)
                .is_err()
                || &header != b"SQLite format 3\0"
            {
                continue;
            }
            let db = rusqlite::Connection::open_with_flags(
                &path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .unwrap();
            inspect(&path, &db);
        }
    }
}
