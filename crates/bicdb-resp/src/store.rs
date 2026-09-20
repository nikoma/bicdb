//! Key-value cache store layered on the bicdb engine.
//!
//! Each Redis logical database (SELECT 0..15) maps to one bicdb collection
//! (`cache_db0`..`cache_db15`). A key becomes the record id, the value lives in
//! `Record.payload` (binary-safe), and the expiry deadline (epoch millis) rides
//! in `Record.timestamp`. Because entries are ordinary records, the cache
//! inherits WAL durability: keys and their TTLs survive a restart.
//!
//! Expiry is enforced twice: lazily (reads treat a past-deadline record as
//! absent) and by a background sweeper draining a min-heap of deadlines. The
//! heap may hold stale entries for overwritten keys; the sweeper re-checks the
//! live record before deleting, so stale entries are harmless.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD_NO_PAD as BASE64;
use base64::Engine as _;
use bicdb_core::{BicDb, BicDbError, DbConfig, Record};
use parking_lot::{Mutex, RwLock};

use crate::{RespServerError, Result};

/// Identity every HotView statement runs as.
///
/// Deliberately an ordinary role: the cache port authenticates with one
/// shared password (often unset), so its SQL must carry no more authority
/// than an operator explicitly grants to this name.
const HOTVIEW_SQL_ROLE: &str = "bicdb_hotview";

pub const NUM_DATABASES: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// Reject writes of new keys once `max_keys` is reached (Redis default).
    NoEviction,
    /// Evict a pseudo-random key from the written database.
    AllKeysRandom,
    /// Evict the key with the nearest expiry deadline; keys without a TTL are
    /// never evicted.
    VolatileTtl,
}

impl std::str::FromStr for EvictionPolicy {
    type Err = String;

    fn from_str(text: &str) -> std::result::Result<Self, String> {
        match text {
            "noeviction" => Ok(Self::NoEviction),
            "allkeys-random" => Ok(Self::AllKeysRandom),
            "volatile-ttl" => Ok(Self::VolatileTtl),
            other => Err(format!("unknown eviction policy: {other}")),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CacheStoreConfig {
    /// fsync every commit. Off by default: a cache prefers speed, and the WAL
    /// still reaches the OS page cache on every write.
    pub fsync: bool,
    /// Total key budget across all databases; `None` = unbounded.
    pub max_keys: Option<usize>,
    pub eviction: EvictionPolicy,
    /// Keep cache entries in memory only (no engine commit per write). Keys
    /// do not survive a restart — but HotView materializations still do in
    /// spirit: their definitions live in the engine and recompute at startup
    /// from the durable SQL tables, so derived entries come back hot.
    pub ephemeral: bool,
}

impl Default for CacheStoreConfig {
    fn default() -> Self {
        Self {
            fsync: false,
            max_keys: None,
            eviction: EvictionPolicy::NoEviction,
            ephemeral: false,
        }
    }
}

/// An in-memory cache entry (ephemeral mode).
struct MemEntry {
    value: Vec<u8>,
    expires_at_ms: Option<i64>,
}

fn mem_is_live(entry: &MemEntry, now: i64) -> bool {
    !matches!(entry.expires_at_ms, Some(deadline) if deadline <= now)
}

/// A live cache entry as returned to command handlers.
#[derive(Clone, Debug)]
pub struct Entry {
    pub value: Vec<u8>,
    pub expires_at_ms: Option<i64>,
}

/// TTL probe result, mirroring Redis TTL semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TtlState {
    Missing,
    NoExpiry,
    ExpiresInMs(i64),
}

pub struct CacheStore {
    db: RwLock<BicDb>,
    /// Ephemeral mode: one map per logical database, keyed by encoded key
    /// (same encoding as record ids, so the expiry heap is shared). `None`
    /// in durable mode. Compound read-modify-writes hold one map's lock for
    /// their whole span, matching the durable path's write-guard atomicity.
    mem: Option<Vec<Mutex<std::collections::HashMap<String, MemEntry>>>>,
    /// Min-heap of (deadline_ms, db_index, encoded_key) candidates for the sweeper.
    expiry: Mutex<BinaryHeap<Reverse<(i64, u8, String)>>>,
    /// Live keys across all databases (includes expired-but-unswept entries).
    key_count: AtomicUsize,
    /// Seed for the eviction sampler; any drifting counter works.
    evict_seed: AtomicUsize,
    max_keys: Option<usize>,
    eviction: EvictionPolicy,
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

fn collection_name(db_index: u8) -> String {
    format!("cache_db{db_index}")
}

/// Record ids must be lossless for arbitrary key bytes: UTF-8 keys get an `s`
/// prefix verbatim, anything else is base64 under a `b` prefix. The prefixes
/// keep the two spaces disjoint.
fn encode_key(key: &[u8]) -> String {
    match std::str::from_utf8(key) {
        Ok(text) => format!("s{text}"),
        Err(_) => format!("b{}", BASE64.encode(key)),
    }
}

fn decode_key(id: &str) -> Vec<u8> {
    match id.as_bytes().first() {
        Some(b's') => id[1..].as_bytes().to_vec(),
        Some(b'b') => BASE64.decode(&id[1..]).unwrap_or_default(),
        _ => id.as_bytes().to_vec(),
    }
}

fn is_expired(record: &Record, now: i64) -> bool {
    matches!(record.timestamp, Some(deadline) if deadline <= now)
}

/// Map "collection missing" (database never written) to a normal empty result.
fn absent_ok<T: Default>(result: bicdb_core::Result<T>) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(BicDbError::CollectionNotFound(_)) => Ok(T::default()),
        Err(err) => Err(err.into()),
    }
}

impl CacheStore {
    pub fn open(path: impl AsRef<Path>, config: &CacheStoreConfig) -> Result<Self> {
        let db_config = DbConfig::default()
            .with_fsync(config.fsync)
            .with_sync_outbox(false);
        let db = BicDb::open_with_config(path, db_config)?;
        let mem = config.ephemeral.then(|| {
            (0..NUM_DATABASES)
                .map(|_| Mutex::new(std::collections::HashMap::new()))
                .collect()
        });
        let store = Self {
            db: RwLock::new(db),
            mem,
            expiry: Mutex::new(BinaryHeap::new()),
            key_count: AtomicUsize::new(0),
            evict_seed: AtomicUsize::new(0),
            max_keys: config.max_keys,
            eviction: config.eviction,
        };
        // Ephemeral starts cold by definition; durable rebuilds count + TTLs.
        if store.mem.is_none() {
            store.load_existing_state()?;
        }
        Ok(store)
    }

    /// Rebuild the key count and expiry heap from persisted records so TTLs
    /// keep firing after a restart.
    fn load_existing_state(&self) -> Result<()> {
        let db = self.db.read();
        let mut heap = self.expiry.lock();
        let mut total = 0usize;
        for db_index in 0..NUM_DATABASES as u8 {
            let collection = collection_name(db_index);
            let records = absent_ok(db.scan_collection(&collection))?;
            total += records.len();
            for record in records {
                if let Some(deadline) = record.timestamp {
                    heap.push(Reverse((deadline, db_index, record.id)));
                }
            }
        }
        self.key_count.store(total, Ordering::Relaxed);
        Ok(())
    }

    fn track_expiry(&self, deadline: Option<i64>, db_index: u8, id: &str) {
        if let Some(deadline) = deadline {
            self.expiry
                .lock()
                .push(Reverse((deadline, db_index, id.to_string())));
        }
    }

    // ---- reads (shared lock) ----

    pub fn get(&self, db_index: u8, key: &[u8]) -> Result<Option<Entry>> {
        let id = encode_key(key);
        if let Some(maps) = &self.mem {
            let map = maps[db_index as usize].lock();
            let now = now_ms();
            return Ok(map
                .get(&id)
                .filter(|entry| mem_is_live(entry, now))
                .map(|entry| Entry {
                    value: entry.value.clone(),
                    expires_at_ms: entry.expires_at_ms,
                }));
        }
        let db = self.db.read();
        let record = absent_ok(db.get(&collection_name(db_index), &id))?;
        let now = now_ms();
        Ok(record.and_then(|record| {
            if is_expired(&record, now) {
                None
            } else {
                Some(Entry {
                    value: record.payload.clone().unwrap_or_default(),
                    expires_at_ms: record.timestamp,
                })
            }
        }))
    }

    pub fn ttl(&self, db_index: u8, key: &[u8]) -> Result<TtlState> {
        let entry = self.get(db_index, key)?;
        let now = now_ms();
        Ok(match entry {
            None => TtlState::Missing,
            Some(entry) => match entry.expires_at_ms {
                None => TtlState::NoExpiry,
                Some(deadline) => TtlState::ExpiresInMs(deadline - now),
            },
        })
    }

    /// All live (unexpired) keys of one database, decoded, in stable
    /// (encoded-id sorted) order so SCAN cursors behave the same in both modes.
    pub fn keys(&self, db_index: u8) -> Result<Vec<Vec<u8>>> {
        if let Some(maps) = &self.mem {
            let map = maps[db_index as usize].lock();
            let now = now_ms();
            let mut ids: Vec<&String> = map
                .iter()
                .filter(|(_, entry)| mem_is_live(entry, now))
                .map(|(id, _)| id)
                .collect();
            ids.sort();
            return Ok(ids.into_iter().map(|id| decode_key(id)).collect());
        }
        let db = self.db.read();
        let records = absent_ok(db.scan_collection(&collection_name(db_index)))?;
        let now = now_ms();
        Ok(records
            .into_iter()
            .filter(|record| !is_expired(record, now))
            .map(|record| decode_key(&record.id))
            .collect())
    }

    // ---- writes (exclusive lock; compound read-modify-write stays atomic) ----

    /// Insert or overwrite an entry. Returns the previous live entry, if any.
    pub fn set(
        &self,
        db_index: u8,
        key: &[u8],
        value: Vec<u8>,
        expires_at_ms: Option<i64>,
    ) -> Result<Option<Entry>> {
        self.write_entry(db_index, key, value, expires_at_ms, SetCondition::Always)
    }

    /// SET with NX/XX semantics. Returns (written, previous live entry).
    pub fn set_conditional(
        &self,
        db_index: u8,
        key: &[u8],
        value: Vec<u8>,
        expires_at_ms: Option<i64>,
        condition: SetCondition,
    ) -> Result<(bool, Option<Entry>)> {
        let previous = self.write_entry(db_index, key, value, expires_at_ms, condition);
        match previous {
            Ok(previous) => Ok((true, previous)),
            Err(RespServerError::ConditionFailed(previous)) => Ok((false, previous)),
            Err(err) => Err(err),
        }
    }

    fn write_entry(
        &self,
        db_index: u8,
        key: &[u8],
        value: Vec<u8>,
        expires_at_ms: Option<i64>,
        condition: SetCondition,
    ) -> Result<Option<Entry>> {
        let id = encode_key(key);
        if let Some(maps) = &self.mem {
            let mut map = maps[db_index as usize].lock();
            let now = now_ms();
            let live = map.get(&id).filter(|entry| mem_is_live(entry, now));
            let previous = live.map(|entry| Entry {
                value: entry.value.clone(),
                expires_at_ms: entry.expires_at_ms,
            });
            match condition {
                SetCondition::Always => {}
                SetCondition::IfAbsent if previous.is_none() => {}
                SetCondition::IfPresent if previous.is_some() => {}
                _ => return Err(RespServerError::ConditionFailed(previous)),
            }
            let keep_ttl = matches!(expires_at_ms, Some(KEEP_TTL_SENTINEL));
            let deadline = if keep_ttl {
                live.and_then(|entry| entry.expires_at_ms)
            } else {
                expires_at_ms
            };
            let existed = map.contains_key(&id);
            if !existed {
                self.reserve_slot_mem(&mut map)?;
            }
            map.insert(
                id.clone(),
                MemEntry {
                    value,
                    expires_at_ms: deadline,
                },
            );
            if !existed {
                self.key_count.fetch_add(1, Ordering::Relaxed);
            }
            drop(map);
            self.track_expiry(deadline, db_index, &id);
            return Ok(previous);
        }
        let collection = collection_name(db_index);
        let mut db = self.db.write();
        db.create_collection(&collection)?;
        let now = now_ms();
        let existing = db.get(&collection, &id)?;
        let live = existing
            .as_deref()
            .filter(|record| !is_expired(record, now));
        let previous = live.map(|record| Entry {
            value: record.payload.clone().unwrap_or_default(),
            expires_at_ms: record.timestamp,
        });
        match condition {
            SetCondition::Always => {}
            SetCondition::IfAbsent if previous.is_none() => {}
            SetCondition::IfPresent if previous.is_some() => {}
            _ => return Err(RespServerError::ConditionFailed(previous)),
        }
        let keep_ttl = matches!(expires_at_ms, Some(KEEP_TTL_SENTINEL));
        let deadline = if keep_ttl {
            live.and_then(|record| record.timestamp)
        } else {
            expires_at_ms
        };
        if existing.is_none() {
            self.reserve_slot(&mut db, db_index)?;
        }
        let mut record = Record::new(id.clone()).with_payload(value);
        record.timestamp = deadline;
        db.insert(&collection, record)?;
        if existing.is_none() {
            self.key_count.fetch_add(1, Ordering::Relaxed);
        }
        drop(db);
        self.track_expiry(deadline, db_index, &id);
        Ok(previous)
    }

    /// Remove keys; returns how many existed (live) and were removed.
    pub fn delete(&self, db_index: u8, keys: &[&[u8]]) -> Result<usize> {
        if let Some(maps) = &self.mem {
            let mut map = maps[db_index as usize].lock();
            let now = now_ms();
            let mut removed = 0usize;
            for key in keys {
                if let Some(entry) = map.remove(&encode_key(key)) {
                    self.key_count.fetch_sub(1, Ordering::Relaxed);
                    if mem_is_live(&entry, now) {
                        removed += 1;
                    }
                }
            }
            return Ok(removed);
        }
        let collection = collection_name(db_index);
        let mut db = self.db.write();
        let now = now_ms();
        let mut removed = 0usize;
        for key in keys {
            let id = encode_key(key);
            let live = absent_ok(db.get(&collection, &id))?
                .is_some_and(|record| !is_expired(&record, now));
            if absent_ok(db.delete(&collection, &id))? {
                self.key_count.fetch_sub(1, Ordering::Relaxed);
                if live {
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    /// Fetch-and-delete (GETDEL). Returns the previous live entry.
    pub fn get_del(&self, db_index: u8, key: &[u8]) -> Result<Option<Entry>> {
        let id = encode_key(key);
        if let Some(maps) = &self.mem {
            let mut map = maps[db_index as usize].lock();
            let now = now_ms();
            return Ok(map.remove(&id).and_then(|entry| {
                self.key_count.fetch_sub(1, Ordering::Relaxed);
                mem_is_live(&entry, now).then_some(Entry {
                    value: entry.value,
                    expires_at_ms: entry.expires_at_ms,
                })
            }));
        }
        let collection = collection_name(db_index);
        let mut db = self.db.write();
        let now = now_ms();
        let record = absent_ok(db.get(&collection, &id))?;
        let live = record.as_deref().filter(|record| !is_expired(record, now));
        let previous = live.map(|record| Entry {
            value: record.payload.clone().unwrap_or_default(),
            expires_at_ms: record.timestamp,
        });
        if record.is_some() && absent_ok(db.delete(&collection, &id))? {
            self.key_count.fetch_sub(1, Ordering::Relaxed);
        }
        Ok(previous)
    }

    /// Set or clear the expiry of an existing live key. `deadline_ms = None`
    /// persists the key. Returns false if the key is missing.
    pub fn set_expiry(&self, db_index: u8, key: &[u8], deadline_ms: Option<i64>) -> Result<bool> {
        let id = encode_key(key);
        if let Some(maps) = &self.mem {
            let mut map = maps[db_index as usize].lock();
            let now = now_ms();
            let Some(entry) = map.get_mut(&id).filter(|entry| mem_is_live(entry, now)) else {
                return Ok(false);
            };
            entry.expires_at_ms = deadline_ms;
            drop(map);
            self.track_expiry(deadline_ms, db_index, &id);
            return Ok(true);
        }
        let collection = collection_name(db_index);
        let mut db = self.db.write();
        let now = now_ms();
        let record = absent_ok(db.get(&collection, &id))?;
        let Some(record) = record.as_deref().filter(|record| !is_expired(record, now)) else {
            return Ok(false);
        };
        let mut updated = record.clone();
        updated.timestamp = deadline_ms;
        db.insert(&collection, updated)?;
        drop(db);
        self.track_expiry(deadline_ms, db_index, &id);
        Ok(true)
    }

    /// Numeric read-modify-write for INCR/DECR/INCRBY/DECRBY.
    pub fn incr_by(&self, db_index: u8, key: &[u8], delta: i64) -> Result<i64> {
        self.numeric_update(db_index, key, |current| {
            let current: i64 = match current {
                Some(bytes) => parse_int(bytes)?,
                None => 0,
            };
            let next = current.checked_add(delta).ok_or_else(|| {
                RespServerError::Command("increment or decrement would overflow".into())
            })?;
            Ok((next.to_string().into_bytes(), next.to_string()))
        })
    }

    /// INCRBYFLOAT; returns the formatted new value.
    pub fn incr_by_float(&self, db_index: u8, key: &[u8], delta: f64) -> Result<String> {
        let mut formatted_result = String::new();
        self.numeric_update(db_index, key, |current| {
            let current: f64 = match current {
                Some(bytes) => parse_float(bytes)?,
                None => 0.0,
            };
            let next = current + delta;
            if !next.is_finite() {
                return Err(RespServerError::Command(
                    "increment would produce NaN or Infinity".into(),
                ));
            }
            let text = format_float(next);
            formatted_result = text.clone();
            Ok((text.clone().into_bytes(), text))
        })?;
        Ok(formatted_result)
    }

    fn numeric_update(
        &self,
        db_index: u8,
        key: &[u8],
        mut update: impl FnMut(Option<&[u8]>) -> Result<(Vec<u8>, String)>,
    ) -> Result<i64> {
        let id = encode_key(key);
        if let Some(maps) = &self.mem {
            let mut map = maps[db_index as usize].lock();
            let now = now_ms();
            let live = map.get(&id).filter(|entry| mem_is_live(entry, now));
            let (new_bytes, text) = update(live.map(|entry| entry.value.as_slice()))?;
            let deadline = live.and_then(|entry| entry.expires_at_ms);
            let existed = map.contains_key(&id);
            if !existed {
                self.reserve_slot_mem(&mut map)?;
            }
            map.insert(
                id,
                MemEntry {
                    value: new_bytes,
                    expires_at_ms: deadline,
                },
            );
            if !existed {
                self.key_count.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(text.parse::<i64>().unwrap_or(0));
        }
        let collection = collection_name(db_index);
        let mut db = self.db.write();
        db.create_collection(&collection)?;
        let now = now_ms();
        let existing = db.get(&collection, &id)?;
        let live = existing
            .as_deref()
            .filter(|record| !is_expired(record, now));
        let (new_bytes, text) = update(live.and_then(|record| record.payload.as_deref()))?;
        let deadline = live.and_then(|record| record.timestamp);
        if existing.is_none() {
            self.reserve_slot(&mut db, db_index)?;
        }
        let mut record = Record::new(id.clone()).with_payload(new_bytes);
        record.timestamp = deadline;
        db.insert(&collection, record)?;
        if existing.is_none() {
            self.key_count.fetch_add(1, Ordering::Relaxed);
        }
        drop(db);
        self.track_expiry(deadline, db_index, &id);
        Ok(text.parse::<i64>().unwrap_or(0))
    }

    /// MSETNX: write all pairs only if none of the keys exist. Atomic — checks
    /// and writes happen under one exclusive lock. Returns whether it wrote.
    pub fn mset_nx(&self, db_index: u8, pairs: &[(&[u8], Vec<u8>)]) -> Result<bool> {
        if let Some(maps) = &self.mem {
            let mut map = maps[db_index as usize].lock();
            let now = now_ms();
            let any_live = pairs.iter().any(|(key, _)| {
                map.get(&encode_key(key))
                    .is_some_and(|entry| mem_is_live(entry, now))
            });
            if any_live {
                return Ok(false);
            }
            for (key, value) in pairs {
                let id = encode_key(key);
                let existed = map.contains_key(&id);
                if !existed {
                    self.reserve_slot_mem(&mut map)?;
                }
                map.insert(
                    id,
                    MemEntry {
                        value: value.clone(),
                        expires_at_ms: None,
                    },
                );
                if !existed {
                    self.key_count.fetch_add(1, Ordering::Relaxed);
                }
            }
            return Ok(true);
        }
        let collection = collection_name(db_index);
        let mut db = self.db.write();
        db.create_collection(&collection)?;
        let now = now_ms();
        for (key, _) in pairs {
            let live = db
                .get(&collection, &encode_key(key))?
                .is_some_and(|record| !is_expired(&record, now));
            if live {
                return Ok(false);
            }
        }
        for (key, value) in pairs {
            let id = encode_key(key);
            let existed = db.get(&collection, &id)?.is_some();
            if !existed {
                self.reserve_slot(&mut db, db_index)?;
            }
            db.insert(&collection, Record::new(id).with_payload(value.clone()))?;
            if !existed {
                self.key_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(true)
    }

    /// APPEND; returns the new value length.
    pub fn append(&self, db_index: u8, key: &[u8], suffix: &[u8]) -> Result<usize> {
        let id = encode_key(key);
        if let Some(maps) = &self.mem {
            let mut map = maps[db_index as usize].lock();
            let now = now_ms();
            if let Some(entry) = map.get_mut(&id) {
                if !mem_is_live(entry, now) {
                    entry.value.clear();
                    entry.expires_at_ms = None;
                }
                entry.value.extend_from_slice(suffix);
                return Ok(entry.value.len());
            }
            self.reserve_slot_mem(&mut map)?;
            map.insert(
                id,
                MemEntry {
                    value: suffix.to_vec(),
                    expires_at_ms: None,
                },
            );
            self.key_count.fetch_add(1, Ordering::Relaxed);
            return Ok(suffix.len());
        }
        let collection = collection_name(db_index);
        let mut db = self.db.write();
        db.create_collection(&collection)?;
        let now = now_ms();
        let existing = db.get(&collection, &id)?;
        let live = existing
            .as_deref()
            .filter(|record| !is_expired(record, now));
        let mut value = live
            .and_then(|record| record.payload.clone())
            .unwrap_or_default();
        let deadline = live.and_then(|record| record.timestamp);
        value.extend_from_slice(suffix);
        let new_len = value.len();
        if existing.is_none() {
            self.reserve_slot(&mut db, db_index)?;
        }
        let mut record = Record::new(id.clone()).with_payload(value);
        record.timestamp = deadline;
        db.insert(&collection, record)?;
        if existing.is_none() {
            self.key_count.fetch_add(1, Ordering::Relaxed);
        }
        drop(db);
        self.track_expiry(deadline, db_index, &id);
        Ok(new_len)
    }

    /// RENAME src dst. Errors if src is missing; dst is overwritten.
    pub fn rename(&self, db_index: u8, src: &[u8], dst: &[u8]) -> Result<()> {
        let src_id = encode_key(src);
        let dst_id = encode_key(dst);
        if let Some(maps) = &self.mem {
            let mut map = maps[db_index as usize].lock();
            let now = now_ms();
            let live = map
                .get(&src_id)
                .is_some_and(|entry| mem_is_live(entry, now));
            if !live {
                return Err(RespServerError::Command("no such key".into()));
            }
            let entry = map.remove(&src_id).expect("checked above");
            let deadline = entry.expires_at_ms;
            if map.insert(dst_id.clone(), entry).is_none() {
                // src slot freed, dst slot newly occupied: net zero.
            } else {
                self.key_count.fetch_sub(1, Ordering::Relaxed);
            }
            drop(map);
            self.track_expiry(deadline, db_index, &dst_id);
            return Ok(());
        }
        let collection = collection_name(db_index);
        let mut db = self.db.write();
        let now = now_ms();
        let record = absent_ok(db.get(&collection, &src_id))?;
        let Some(record) = record.as_deref().filter(|record| !is_expired(record, now)) else {
            return Err(RespServerError::Command("no such key".into()));
        };
        let deadline = record.timestamp;
        let mut renamed = record.clone();
        renamed.id = dst_id.clone();
        let dst_existed = db.get(&collection, &dst_id)?.is_some();
        db.insert(&collection, renamed)?;
        db.delete(&collection, &src_id)?;
        if dst_existed {
            self.key_count.fetch_sub(1, Ordering::Relaxed);
        }
        drop(db);
        self.track_expiry(deadline, db_index, &dst_id);
        Ok(())
    }

    pub fn flush_db(&self, db_index: u8) -> Result<()> {
        if let Some(maps) = &self.mem {
            let mut map = maps[db_index as usize].lock();
            self.key_count.fetch_sub(map.len(), Ordering::Relaxed);
            map.clear();
            return Ok(());
        }
        let collection = collection_name(db_index);
        let mut db = self.db.write();
        let count = absent_ok(db.collection_record_count(&collection))?;
        absent_ok(db.drop_collection(&collection))?;
        self.key_count.fetch_sub(count, Ordering::Relaxed);
        Ok(())
    }

    pub fn flush_all(&self) -> Result<()> {
        if let Some(maps) = &self.mem {
            for map in maps {
                map.lock().clear();
            }
            self.key_count.store(0, Ordering::Relaxed);
            self.expiry.lock().clear();
            return Ok(());
        }
        let mut db = self.db.write();
        for db_index in 0..NUM_DATABASES as u8 {
            absent_ok(db.drop_collection(&collection_name(db_index)))?;
        }
        self.key_count.store(0, Ordering::Relaxed);
        self.expiry.lock().clear();
        Ok(())
    }

    // ---- SQL execution & hotview metadata (the HotView feature) ----

    /// Run arbitrary SQL under the exclusive engine lock and report which
    /// collections it changed. The changed set comes from diffing per-collection
    /// generation counters around the execution — exact regardless of what the
    /// SQL looked like (triggers, cascades, multi-statement), no parsing trusted.
    pub fn execute_sql(
        &self,
        sql: &str,
    ) -> Result<(bicdb_sql::SqlResult, std::collections::HashSet<String>)> {
        let mut db = self.db.write();
        let before = db.collection_generations();
        let result = {
            // The cache port is a NETWORK surface with a single shared
            // password, so its SQL must not run as the bootstrap identity.
            // A context-less session resolves to that identity, which is
            // hardcoded superuser and owns every table by default — HotView
            // would have bypassed RLS, tenant policy and every GRANT.
            let mut session = bicdb_sql::SqlSession::new_unprivileged(&mut db, HOTVIEW_SQL_ROLE);
            session
                .execute(sql)
                .map_err(|err| RespServerError::Command(format!("SQL error: {err}")))?
        };
        let after = db.collection_generations();
        drop(db);
        let changed = after
            .into_iter()
            .filter(|(name, generation)| before.get(name) != Some(generation))
            .map(|(name, _)| name)
            .collect();
        Ok((result, changed))
    }

    /// Run a read-only query (shared lock) — used to (re)compute hotviews.
    pub fn query_sql(&self, sql: &str) -> Result<bicdb_sql::SqlResult> {
        let db = self.db.read();
        bicdb_sql::SqlSession::new_shared_unprivileged(&db, HOTVIEW_SQL_ROLE)
            .execute(sql)
            .map_err(|err| RespServerError::Command(format!("hotview query failed: {err}")))
    }

    /// Persist a hotview definition (outside the cache key budget).
    pub fn save_meta(&self, collection: &str, id: &str, metadata: serde_json::Value) -> Result<()> {
        let mut db = self.db.write();
        db.create_collection(collection)?;
        db.insert(collection, Record::new(id).with_metadata(metadata))?;
        Ok(())
    }

    pub fn delete_meta(&self, collection: &str, id: &str) -> Result<bool> {
        let mut db = self.db.write();
        absent_ok(db.delete(collection, id))
    }

    pub fn load_meta(&self, collection: &str) -> Result<Vec<serde_json::Value>> {
        let db = self.db.read();
        let records = absent_ok(db.scan_collection(collection))?;
        Ok(records.into_iter().map(|record| record.metadata).collect())
    }

    // ---- expiry sweeping & eviction ----

    /// Drain due deadlines from the heap and delete records that are really
    /// expired (a newer write may have replaced the deadline — re-check).
    pub fn sweep_expired(&self) -> Result<usize> {
        let now = now_ms();
        let mut due: Vec<(u8, String)> = Vec::new();
        {
            let mut heap = self.expiry.lock();
            while let Some(Reverse((deadline, _, _))) = heap.peek() {
                if *deadline > now || due.len() >= 4096 {
                    break;
                }
                let Reverse((_, db_index, id)) = heap.pop().expect("peeked entry");
                due.push((db_index, id));
            }
        }
        if due.is_empty() {
            return Ok(0);
        }
        if let Some(maps) = &self.mem {
            let mut removed = 0usize;
            for (db_index, id) in due {
                let mut map = maps[db_index as usize].lock();
                let expired = map.get(&id).is_some_and(|entry| !mem_is_live(entry, now));
                if expired && map.remove(&id).is_some() {
                    self.key_count.fetch_sub(1, Ordering::Relaxed);
                    removed += 1;
                }
            }
            return Ok(removed);
        }
        let mut db = self.db.write();
        let mut removed = 0usize;
        for (db_index, id) in due {
            let collection = collection_name(db_index);
            let still_expired =
                absent_ok(db.get(&collection, &id))?.is_some_and(|record| is_expired(&record, now));
            if still_expired && absent_ok(db.delete(&collection, &id))? {
                self.key_count.fetch_sub(1, Ordering::Relaxed);
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Ephemeral-mode slot reservation. Eviction is scoped to the database
    /// being written (cross-database eviction would need a second map lock —
    /// a lock-order hazard for no practical gain in a cache).
    fn reserve_slot_mem(
        &self,
        map: &mut std::collections::HashMap<String, MemEntry>,
    ) -> Result<()> {
        let Some(max_keys) = self.max_keys else {
            return Ok(());
        };
        while self.key_count.load(Ordering::Relaxed) >= max_keys {
            let evicted = match self.eviction {
                EvictionPolicy::NoEviction => false,
                EvictionPolicy::VolatileTtl => {
                    let victim = map
                        .iter()
                        .filter_map(|(id, entry)| entry.expires_at_ms.map(|exp| (exp, id)))
                        .min()
                        .map(|(_, id)| id.clone());
                    victim.is_some_and(|id| {
                        map.remove(&id);
                        self.key_count.fetch_sub(1, Ordering::Relaxed);
                        true
                    })
                }
                EvictionPolicy::AllKeysRandom => {
                    if map.is_empty() {
                        false
                    } else {
                        let seed = self
                            .evict_seed
                            .fetch_add(1, Ordering::Relaxed)
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(now_ms() as usize);
                        let victim = map
                            .keys()
                            .nth(seed % map.len())
                            .cloned()
                            .expect("non-empty map");
                        map.remove(&victim);
                        self.key_count.fetch_sub(1, Ordering::Relaxed);
                        true
                    }
                }
            };
            if !evicted {
                return Err(RespServerError::Command(
                    "OOM command not allowed when used memory > 'maxmemory'".into(),
                ));
            }
        }
        Ok(())
    }

    /// Called before inserting a NEW key while holding the write lock: enforce
    /// `max_keys` by evicting per policy or rejecting the write.
    fn reserve_slot(&self, db: &mut BicDb, db_index: u8) -> Result<()> {
        let Some(max_keys) = self.max_keys else {
            return Ok(());
        };
        while self.key_count.load(Ordering::Relaxed) >= max_keys {
            if !self.evict_one(db, db_index)? {
                return Err(RespServerError::Command(
                    "OOM command not allowed when used memory > 'maxmemory'".into(),
                ));
            }
        }
        Ok(())
    }

    fn evict_one(&self, db: &mut BicDb, db_index: u8) -> Result<bool> {
        let now = now_ms();
        match self.eviction {
            EvictionPolicy::NoEviction => Ok(false),
            EvictionPolicy::VolatileTtl => {
                // Nearest-deadline first; the heap already orders candidates.
                loop {
                    let candidate = self.expiry.lock().pop();
                    let Some(Reverse((_, victim_db, id))) = candidate else {
                        return Ok(false);
                    };
                    let collection = collection_name(victim_db);
                    // Only trust heap entries that still describe a TTL'd record.
                    let valid = absent_ok(db.get(&collection, &id))?
                        .is_some_and(|record| record.timestamp.is_some());
                    if valid && absent_ok(db.delete(&collection, &id))? {
                        self.key_count.fetch_sub(1, Ordering::Relaxed);
                        return Ok(true);
                    }
                }
            }
            EvictionPolicy::AllKeysRandom => {
                let collection = collection_name(db_index);
                let ids = absent_ok(db.scan_collection_record_ids_with_prefix(&collection, ""))?;
                if ids.is_empty() {
                    return Ok(false);
                }
                // Cheap LCG over a drifting seed; cache eviction only needs
                // "not always the same key", not real randomness.
                let seed = self
                    .evict_seed
                    .fetch_add(1, Ordering::Relaxed)
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(now as usize);
                let victim = &ids[seed % ids.len()];
                if absent_ok(db.delete(&collection, victim))? {
                    self.key_count.fetch_sub(1, Ordering::Relaxed);
                    return Ok(true);
                }
                Ok(false)
            }
        }
    }
}

/// SET/write preconditions (Redis NX / XX).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetCondition {
    Always,
    IfAbsent,
    IfPresent,
}

/// Sentinel deadline meaning "keep the existing TTL" (SET ... KEEPTTL). Never a
/// real deadline: it is i64::MIN, unreachable as an epoch timestamp.
pub const KEEP_TTL_SENTINEL: i64 = i64::MIN;

fn parse_int(bytes: &[u8]) -> Result<i64> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse::<i64>().ok())
        .ok_or_else(|| RespServerError::Command("value is not an integer or out of range".into()))
}

fn parse_float(bytes: &[u8]) -> Result<f64> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .ok_or_else(|| RespServerError::Command("value is not a valid float".into()))
}

/// Redis prints floats without trailing zeros; Rust's shortest-roundtrip
/// `Display` produces the same human form ("10.6", not "10.59999999999999964").
fn format_float(value: f64) -> String {
    format!("{value}")
}
