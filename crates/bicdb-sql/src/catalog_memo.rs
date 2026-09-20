//! Per-thread, generation-validated memos for catalog listings that the write
//! path consults on every statement.
//!
//! Before this module every INSERT/UPDATE/DELETE re-scanned (and re-parsed
//! from JSON) the trigger catalog, the extension event-binding catalog and,
//! for UPDATE, every table schema to find inbound foreign keys — even when all
//! of those are empty. A TPC-C NEWORD issues ~25 write statements, so that was
//! ~75 catalog scans per transaction for information that changes only on DDL.
//!
//! Each memo is keyed by the database instance and validated against the
//! generation counter of the catalog collection it derives from; any write to
//! that collection bumps the generation and invalidates the entry. Entries are
//! `Arc`s so a hit is a refcount bump, not a clone.

use std::cell::RefCell;
use std::sync::Arc;

use bicdb_core::BicDb;
use rustc_hash::FxHashMap;

use crate::extension_catalog::{
    list_event_bindings, EventBindingDefinition, EXTENSION_EVENT_BINDING_COLLECTION,
};
use crate::records::{unique_arbiters_for_table_uncached, UniqueConflictArbiter};
use crate::routines::list_triggers;
use crate::schema_meta::{
    inbound_foreign_key_delete_constraints_uncached,
    inbound_foreign_key_update_constraints_uncached,
};
use crate::TableSchema;
use crate::{
    InboundForeignKeyDelete, InboundForeignKeyUpdate, Result, TriggerSchema, SCHEMA_COLLECTION,
    TRIGGER_COLLECTION,
};

struct Entry<T> {
    generation: u64,
    value: Arc<T>,
}

thread_local! {
    static TRIGGERS: RefCell<FxHashMap<u64, Entry<Vec<TriggerSchema>>>> =
        RefCell::new(FxHashMap::default());
    static EVENT_BINDINGS: RefCell<FxHashMap<u64, Entry<Vec<EventBindingDefinition>>>> =
        RefCell::new(FxHashMap::default());
    static INBOUND_FK_UPDATES: RefCell<FxHashMap<(u64, String), Entry<Vec<InboundForeignKeyUpdate>>>> =
        RefCell::new(FxHashMap::default());
    static INBOUND_FK_DELETES: RefCell<FxHashMap<(u64, String), Entry<Vec<InboundForeignKeyDelete>>>> =
        RefCell::new(FxHashMap::default());
    static UNIQUE_ARBITERS: RefCell<FxHashMap<(u64, String), Entry2<Vec<UniqueConflictArbiter>>>> =
        RefCell::new(FxHashMap::default());
}

struct Entry2<T> {
    generation: u64,
    index_generation: u64,
    value: Arc<T>,
}

const MAX_TABLE_ENTRIES: usize = 512;

/// The instance id, not the address: a fresh database allocated where a
/// dropped one lived starts every generation at 0 and would otherwise inherit
/// the old database's entries.
fn db_key(db: &BicDb) -> u64 {
    db.instance_id()
}

/// All triggers, shared. Validated against the trigger catalog generation.
pub(crate) fn triggers_shared(db: &BicDb) -> Result<Arc<Vec<TriggerSchema>>> {
    let generation = db.collection_generation(TRIGGER_COLLECTION);
    let key = db_key(db);
    let hit = TRIGGERS.with(|memo| {
        memo.borrow()
            .get(&key)
            .filter(|entry| entry.generation == generation)
            .map(|entry| Arc::clone(&entry.value))
    });
    if let Some(hit) = hit {
        return Ok(hit);
    }
    let value = Arc::new(list_triggers(db)?);
    TRIGGERS.with(|memo| {
        memo.borrow_mut().insert(
            key,
            Entry {
                generation,
                value: Arc::clone(&value),
            },
        );
    });
    Ok(value)
}

/// All extension event bindings, shared. Validated against the binding
/// catalog generation.
pub(crate) fn event_bindings_shared(db: &BicDb) -> Result<Arc<Vec<EventBindingDefinition>>> {
    let generation = db.collection_generation(EXTENSION_EVENT_BINDING_COLLECTION);
    let key = db_key(db);
    let hit = EVENT_BINDINGS.with(|memo| {
        memo.borrow()
            .get(&key)
            .filter(|entry| entry.generation == generation)
            .map(|entry| Arc::clone(&entry.value))
    });
    if let Some(hit) = hit {
        return Ok(hit);
    }
    let value = Arc::new(list_event_bindings(db)?);
    EVENT_BINDINGS.with(|memo| {
        memo.borrow_mut().insert(
            key,
            Entry {
                generation,
                value: Arc::clone(&value),
            },
        );
    });
    Ok(value)
}

/// Foreign keys whose parent is `table`, shared. Validated against the schema
/// catalog generation (constraints live inside table schemas).
pub(crate) fn inbound_foreign_key_updates_shared(
    db: &BicDb,
    table: &str,
) -> Result<Arc<Vec<InboundForeignKeyUpdate>>> {
    let generation = db.collection_generation(SCHEMA_COLLECTION);
    let key = (db_key(db), table.to_ascii_lowercase());
    let hit = INBOUND_FK_UPDATES.with(|memo| {
        memo.borrow()
            .get(&key)
            .filter(|entry| entry.generation == generation)
            .map(|entry| Arc::clone(&entry.value))
    });
    if let Some(hit) = hit {
        return Ok(hit);
    }
    let value = Arc::new(inbound_foreign_key_update_constraints_uncached(db, table)?);
    INBOUND_FK_UPDATES.with(|memo| {
        let mut memo = memo.borrow_mut();
        if memo.len() >= MAX_TABLE_ENTRIES {
            memo.clear();
        }
        memo.insert(
            key,
            Entry {
                generation,
                value: Arc::clone(&value),
            },
        );
    });
    Ok(value)
}

/// Foreign keys whose parent is `table`, for DELETE (referential actions),
/// shared and validated like `inbound_foreign_key_updates_shared`. Every
/// DELETE statement used to scan the whole schema catalog to find these.
pub(crate) fn inbound_foreign_key_deletes_shared(
    db: &BicDb,
    table: &str,
) -> Result<Arc<Vec<InboundForeignKeyDelete>>> {
    let generation = db.collection_generation(SCHEMA_COLLECTION);
    let key = (db_key(db), table.to_ascii_lowercase());
    let hit = INBOUND_FK_DELETES.with(|memo| {
        memo.borrow()
            .get(&key)
            .filter(|entry| entry.generation == generation)
            .map(|entry| Arc::clone(&entry.value))
    });
    if let Some(hit) = hit {
        return Ok(hit);
    }
    let value = Arc::new(inbound_foreign_key_delete_constraints_uncached(db, table)?);
    INBOUND_FK_DELETES.with(|memo| {
        let mut memo = memo.borrow_mut();
        if memo.len() >= MAX_TABLE_ENTRIES {
            memo.clear();
        }
        memo.insert(
            key,
            Entry {
                generation,
                value: Arc::clone(&value),
            },
        );
    });
    Ok(value)
}

/// Unique-key arbiters (primary key, UNIQUE constraints, unique indexes) for
/// `table`, shared. Validated against both the schema catalog generation
/// (constraints) and the index generation (which index backs each key).
pub(crate) fn unique_arbiters_shared(
    db: &BicDb,
    table: &str,
    schema: &TableSchema,
) -> Arc<Vec<UniqueConflictArbiter>> {
    let generation = db.collection_generation(SCHEMA_COLLECTION);
    let index_generation = db.index_generation();
    let key = (db_key(db), table.to_ascii_lowercase());
    let hit = UNIQUE_ARBITERS.with(|memo| {
        memo.borrow()
            .get(&key)
            .filter(|entry| {
                entry.generation == generation && entry.index_generation == index_generation
            })
            .map(|entry| Arc::clone(&entry.value))
    });
    if let Some(hit) = hit {
        return hit;
    }
    let value = Arc::new(unique_arbiters_for_table_uncached(db, table, schema));
    UNIQUE_ARBITERS.with(|memo| {
        let mut memo = memo.borrow_mut();
        if memo.len() >= MAX_TABLE_ENTRIES {
            memo.clear();
        }
        memo.insert(
            key,
            Entry2 {
                generation,
                index_generation,
                value: Arc::clone(&value),
            },
        );
    });
    value
}
