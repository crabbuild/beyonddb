//! Physical SQLite reservations for prepared application work.
//!
//! Cellar deliberately exposes no raw database pointer or max_page_count PRAGMA.
//! Allocate the reserved bytes as bounded BLOB chunks: another command cannot
//! consume its occupied pages. Resolution releases it in the same transaction
//! that installs the deferred item/index/stream changes.

use cellar::sdk::{Storage, primitives::sql::Value as V};

use crate::{Error, Result};

pub const PAGE_BYTES: u64 = 4096;
pub const DEFAULT_DATABASE_BYTES: u64 = 512 * 1024 * 1024;
const CHUNK_PAGES: u64 = 256;
pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS capacity_reservations (
 reservation_key BLOB PRIMARY KEY,
 pages INTEGER NOT NULL CHECK(pages > 0)
);
CREATE TABLE IF NOT EXISTS capacity_reservation_chunks (
 reservation_key BLOB NOT NULL,
 chunk INTEGER NOT NULL CHECK(chunk >= 0),
 allocation BLOB NOT NULL,
 PRIMARY KEY(reservation_key, chunk)
);
CREATE TABLE IF NOT EXISTS capacity_total (
 singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
 pages INTEGER NOT NULL CHECK(pages >= 0)
);
INSERT OR IGNORE INTO capacity_total(singleton, pages) VALUES (1, 0);
";

pub(crate) fn reserve(storage: &Storage, key: &[u8], bytes: u64, maximum: u64) -> Result<()> {
    if key.is_empty() || key.len() > 128 || bytes == 0 {
        return Err(Error::Command("invalid database capacity reservation"));
    }
    let pages = bytes.div_ceil(PAGE_BYTES);
    let allocation = pages
        .checked_mul(PAGE_BYTES)
        .and_then(|n| i64::try_from(n).ok())
        .ok_or(Error::Capacity("reservation is too large"))?;
    let existing = storage
        .sql_cursor(
            "SELECT pages FROM capacity_reservations WHERE reservation_key = ?1",
            &[V::Blob(key.to_vec())],
        )?
        .to_raw_array()?;
    if let Some(row) = existing.first() {
        return if matches!(row.as_slice(), [V::Integer(old)] if u64::try_from(*old).ok() == Some(pages))
        {
            Ok(())
        } else {
            Err(Error::Command("database reservation identity changed"))
        };
    }
    let used = storage.sql_database_size()?;
    let chunks = pages.div_ceil(CHUNK_PAGES);
    // Include chunk-row and index overhead. The final
    // check below observes SQLite's actual occupancy, including overflow pages.
    if used
        .checked_add(allocation as u64)
        .and_then(|n| n.checked_add((chunks + 1) * 2 * PAGE_BYTES))
        .is_none_or(|n| n > maximum)
    {
        return Err(Error::Capacity(
            "database reservation exceeds the cell limit",
        ));
    }
    mutate(
        storage,
        "INSERT INTO capacity_reservations(reservation_key, pages) VALUES (?1, ?2)",
        &[V::Blob(key.to_vec()), V::Integer(pages as i64)],
    )?;
    let mut remaining = pages;
    for chunk in 0..chunks {
        let count = remaining.min(CHUNK_PAGES);
        mutate(
            storage,
            "INSERT INTO capacity_reservation_chunks(reservation_key, chunk, allocation) VALUES (?1, ?2, zeroblob(?3))",
            &[
                V::Blob(key.to_vec()),
                V::Integer(chunk as i64),
                V::Integer((count * PAGE_BYTES) as i64),
            ],
        )?;
        remaining -= count;
    }
    if mutate(
        storage,
        "UPDATE capacity_total SET pages = pages + ?1 WHERE singleton = 1",
        &[V::Integer(pages as i64)],
    )? != 1
    {
        return Err(Error::Command("database reservation total is missing"));
    }
    validate(storage, maximum)
}

pub(crate) fn release(storage: &Storage, key: &[u8]) -> Result<bool> {
    if key.is_empty() || key.len() > 128 {
        return Err(Error::Command("invalid reservation key"));
    }
    let rows = storage
        .sql_cursor(
            "SELECT pages FROM capacity_reservations WHERE reservation_key = ?1",
            &[V::Blob(key.to_vec())],
        )?
        .to_raw_array()?;
    let Some([V::Integer(pages)]) = rows.first().map(Vec::as_slice) else {
        if rows.is_empty() {
            return Ok(false);
        }
        return Err(Error::Command("invalid database reservation row"));
    };
    let chunks = u64::try_from(*pages)
        .map_err(|_| Error::Command("invalid reservation page count"))?
        .div_ceil(CHUNK_PAGES);
    if mutate(
        storage,
        "DELETE FROM capacity_reservation_chunks WHERE reservation_key = ?1",
        &[V::Blob(key.to_vec())],
    )? != chunks
    {
        return Err(Error::Command("database reservation chunks are incomplete"));
    }
    if mutate(
        storage,
        "DELETE FROM capacity_reservations WHERE reservation_key = ?1",
        &[V::Blob(key.to_vec())],
    )? != 1
        || mutate(
            storage,
            "UPDATE capacity_total SET pages = pages - ?1 WHERE singleton = 1 AND pages >= ?1",
            &[V::Integer(*pages)],
        )? != 1
    {
        return Err(Error::Command("database reservation accounting failed"));
    }
    Ok(true)
}

pub(crate) fn validate(storage: &Storage, maximum: u64) -> Result<()> {
    if storage.sql_database_size()? > maximum {
        return Err(Error::Capacity("cell database exceeds its capacity"));
    }
    Ok(())
}

fn mutate(storage: &Storage, sql: &str, values: &[V]) -> Result<u64> {
    let mut cursor = storage.sql_cursor(sql, values)?;
    if cursor.next_raw()?.is_some() {
        return Err(Error::Command("unexpected reservation result"));
    }
    let changed = cursor.rows_written();
    cursor.close()?;
    Ok(changed)
}
