//! Short-lived metadata responses for the explicitly stale-tolerant serving mode.

use std::{
    collections::HashMap,
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::{Duration, Instant},
};

use extenddb_core::types::{ListTablesOutput, TableDescription, TableKeyInfo};

const TTL: Duration = Duration::from_millis(500);
const MAX_ENTRIES: usize = 128;

#[derive(Default)]
pub(super) struct MetadataCache {
    state: RwLock<State>,
}

#[derive(Default)]
struct State {
    generation: u64,
    descriptions: HashMap<(String, String), Entry<TableDescription>>,
    listings: HashMap<(String, i64, Option<String>), Entry<ListTablesOutput>>,
    key_infos: HashMap<(String, String), Entry<TableKeyInfo>>,
}

struct Entry<T> {
    at: Instant,
    generation: u64,
    value: T,
}

impl MetadataCache {
    fn read(&self) -> RwLockReadGuard<'_, State> {
        match self.state.read() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn write(&self) -> RwLockWriteGuard<'_, State> {
        match self.state.write() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    pub(super) fn generation(&self) -> u64 {
        self.read().generation
    }

    pub(super) fn key_info(&self, account_id: &str, name: &str) -> Option<TableKeyInfo> {
        let state = self.read();
        let entry = state
            .key_infos
            .get(&(account_id.to_owned(), name.to_owned()))?;
        (entry.at.elapsed() < TTL && entry.generation == state.generation)
            .then(|| entry.value.clone())
    }

    pub(super) fn insert_key_info(
        &self,
        account_id: &str,
        name: String,
        generation: u64,
        value: TableKeyInfo,
    ) {
        let mut state = self.write();
        if state.generation != generation {
            return;
        }
        if state.key_infos.len() >= MAX_ENTRIES {
            state.key_infos.clear();
        }
        state.key_infos.insert(
            (account_id.to_owned(), name),
            Entry {
                at: Instant::now(),
                generation,
                value,
            },
        );
    }

    pub(super) fn description(&self, account_id: &str, name: &str) -> Option<TableDescription> {
        let state = self.read();
        let entry = state
            .descriptions
            .get(&(account_id.to_owned(), name.to_owned()))?;
        (entry.at.elapsed() < TTL && entry.generation == state.generation)
            .then(|| entry.value.clone())
    }

    pub(super) fn listing(
        &self,
        account_id: &str,
        limit: i64,
        start: &Option<String>,
    ) -> Option<ListTablesOutput> {
        let state = self.read();
        let entry = state
            .listings
            .get(&(account_id.to_owned(), limit, start.clone()))?;
        (entry.at.elapsed() < TTL && entry.generation == state.generation)
            .then(|| entry.value.clone())
    }

    pub(super) fn insert_description(
        &self,
        account_id: &str,
        name: String,
        generation: u64,
        value: TableDescription,
    ) {
        let mut state = self.write();
        if state.generation != generation {
            return;
        }
        if state.descriptions.len() >= MAX_ENTRIES {
            state.descriptions.clear();
        }
        state.descriptions.insert(
            (account_id.to_owned(), name),
            Entry {
                at: Instant::now(),
                generation,
                value,
            },
        );
    }

    pub(super) fn insert_listing(
        &self,
        account_id: &str,
        limit: i64,
        start: Option<String>,
        generation: u64,
        value: ListTablesOutput,
    ) {
        let mut state = self.write();
        if state.generation != generation {
            return;
        }
        if state.listings.len() >= MAX_ENTRIES {
            state.listings.clear();
        }
        state.listings.insert(
            (account_id.to_owned(), limit, start),
            Entry {
                at: Instant::now(),
                generation,
                value,
            },
        );
    }

    pub(super) fn invalidate(&self) {
        let mut state = self.write();
        state.generation = state.generation.saturating_add(1);
        state.descriptions.clear();
        state.listings.clear();
        state.key_infos.clear();
    }
}
