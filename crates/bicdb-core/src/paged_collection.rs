//! `Record` storage on the paged engine.
//!
//! The adapter between `bicdb-core`'s record model and `bicdb-page`'s durable
//! key-value store. This is what `storage_mode = server_paged` stores rows in.
//!
//! # Why this layer exists at all
//!
//! `bicdb-page` deliberately knows nothing about records — it stores bytes under
//! keys, with MVCC, crash recovery, and a bounded buffer pool. Everything
//! record-shaped lives here: how a [`Record`] is encoded, how a collection maps
//! to a key space, and how the engine's errors translate into BicDB's.
//!
//! Keeping the split sharp is what makes it possible to test the storage engine
//! against adversarial crash and isolation scenarios without constructing a
//! database, and to change the record encoding without touching the page layer.
//!
//! # Key space
//!
//! Collections share one paged store, separated by a length-prefixed collection
//! name:
//!
//! ```text
//! key = [name_len: u16 BE] [collection name] [record id]
//! ```
//!
//! Length-prefixed rather than delimited, because a record id may contain any
//! byte and a delimiter would let a crafted id read another collection's rows.
//! Big-endian so that bytewise key order groups a collection's rows together and
//! keeps them in record-id order within it — which is what makes a per-collection
//! scan a contiguous range rather than a filter over everything.

use rustc_hash::FxHashMap;
use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bicdb_page::{
    BTreeVerifyCursor, BTreeVerifyLimits, BTreeVerifyStepReport, PagedCheckpointCursor,
    PagedCheckpointLimits, PagedCheckpointStepReport, PagedIntegrityReport, PagedStore,
    PagedStoreOptions, PagedStoreSnapshot, ReadAheadLimits, ReadAheadStepReport,
    ReadAheadSubmitReport, Snapshot, TupleLocator, VacuumCursor, VacuumLimits, VacuumReport,
    VersionChainInspection, VersionChainRepairReport, VersionChainVerifyCursor,
    VersionChainVerifyLimits, VersionChainVerifyStepReport, WritebackCursor, WritebackLimits,
    WritebackStepReport, Xid,
};

use crate::error::{BicDbError, Result};
use crate::record::{Record, StoredRecord};
use crate::CancellationToken;

/// Bounds the memory a single record read may allocate.
const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

/// Durable record storage over the paged engine.
#[derive(Debug)]
pub struct PagedRecords {
    store: Arc<PagedStore>,
    /// Opt-in zstd compression for newly written record values.
    value_compression: std::sync::atomic::AtomicBool,
    index_aliases: parking_lot::RwLock<BTreeMap<String, String>>,
    /// Lazy cache over the durable `[0,0,15]` entry-format records: one
    /// durable read per index per handle lifetime, then a map probe on the
    /// write path. Write-through on [`Self::set_index_entry_format`] — the
    /// only writers (create_index, rekey) commit immediately, so the cache
    /// can never outlive an aborted format flip in practice.
    entry_formats: parking_lot::RwLock<FxHashMap<String, (IndexEntryFormat, Option<String>)>>,
    /// Packed FTS segments (physical-format v2), keyed by physical index
    /// name. `None` caches a clean miss; an open error is never cached, so a
    /// damaged segment fails loudly on every touch rather than once.
    fts_segments:
        parking_lot::RwLock<FxHashMap<String, Option<Arc<crate::fts_segment::SegmentSet>>>>,
    fts_segments_root: PathBuf,
}

/// Options for opening paged record storage.
#[derive(Clone, Debug)]
pub struct PagedRecordsOptions {
    pub page_size: u32,
    pub buffer_pool_bytes: u64,
    pub read_ahead_queue_pages: usize,
    pub fsync: bool,
    pub wal_max_bytes: u64,
    /// Bytes at which the active WAL seals and rotates to an immutable
    /// `store.wal.<seq>` segment. Independent of `wal_max_bytes`.
    pub wal_segment_bytes: u64,
    /// Bytes per page-file extent for newly created stores; `0` keeps the
    /// single-file layout. Existing stores use their recorded layout.
    pub extent_bytes: u64,
    /// Accept and transform a pre-durable-abort-exceptions meta page at open.
    /// See `PagedStoreOptions::accept_legacy_meta` for why this is opt-in.
    pub accept_legacy_meta: bool,
}

impl Default for PagedRecordsOptions {
    fn default() -> Self {
        Self {
            page_size: 8192,
            buffer_pool_bytes: 256 * 1024 * 1024,
            read_ahead_queue_pages: 1_024,
            fsync: true,
            wal_max_bytes: 256 * 1024 * 1024,
            wal_segment_bytes: 64 * 1024 * 1024,
            extent_bytes: 0,
            accept_legacy_meta: false,
        }
    }
}

/// Merges the paged prefix scan with a segment's term range, term-major.
///
/// Both sides yield terms in ascending order. A term present on the paged
/// side was folded there and SUPERSEDES the segment's copy, so the segment's
/// blocks for that term are skipped; every other term interleaves by order.
/// A segment term is expanded into blocks only once it is decided to be the
/// strictly next term, so a superseded term costs no block reads and the
/// output never interleaves two terms.
struct MergedTermBlockScan<'store> {
    paged: Box<dyn Iterator<Item = Result<(Vec<u8>, u64, Vec<u8>)>> + 'store>,
    pending_paged: Option<(Vec<u8>, u64, Vec<u8>)>,
    cursor: crate::fts_segment::SetTermCursor,
    cursor_done: bool,
    segment_pending: Option<(
        Vec<u8>,
        Vec<(
            std::sync::Arc<crate::fts_segment::FtsSegmentReader>,
            std::sync::Arc<crate::fts_segment::SegmentTermMeta>,
        )>,
    )>,
    prefix: Vec<u8>,
    queued: std::collections::VecDeque<(Vec<u8>, u64, Vec<u8>)>,
    failed: bool,
}

impl MergedTermBlockScan<'_> {
    fn step(&mut self) -> Result<Option<(Vec<u8>, u64, Vec<u8>)>> {
        loop {
            if let Some(item) = self.queued.pop_front() {
                return Ok(Some(item));
            }
            if self.pending_paged.is_none() {
                self.pending_paged = match self.paged.next() {
                    Some(entry) => Some(entry?),
                    None => None,
                };
            }
            if self.segment_pending.is_none() && !self.cursor_done {
                match self.cursor.next_term()? {
                    Some((term, metas)) if term.starts_with(&self.prefix) => {
                        self.segment_pending = Some((term, metas));
                    }
                    // Terms are sorted: once one leaves the prefix, all
                    // later ones do too.
                    _ => self.cursor_done = true,
                }
            }
            match (&self.pending_paged, &self.segment_pending) {
                (None, None) => return Ok(None),
                (Some(_), None) => return Ok(self.pending_paged.take()),
                (None, Some(_)) => {
                    let (term, metas) = self.segment_pending.take().expect("checked");
                    for (reader, meta) in &metas {
                        for (last_document_id, bytes) in reader.pk_blocks_all(meta)? {
                            self.queued
                                .push_back((term.clone(), last_document_id, bytes));
                        }
                    }
                }
                (Some((paged_term, _, _)), Some((segment_term, _))) => {
                    use std::cmp::Ordering;
                    match segment_term.cmp(paged_term) {
                        Ordering::Less => {
                            let (term, metas) = self.segment_pending.take().expect("checked");
                            for (reader, meta) in &metas {
                                for (last_document_id, bytes) in reader.pk_blocks_all(meta)? {
                                    self.queued
                                        .push_back((term.clone(), last_document_id, bytes));
                                }
                            }
                        }
                        Ordering::Equal => {
                            // Folded term: the paged copy wins, and the
                            // segment copy is dropped before any block read.
                            self.segment_pending = None;
                            return Ok(self.pending_paged.take());
                        }
                        Ordering::Greater => return Ok(self.pending_paged.take()),
                    }
                }
            }
        }
    }
}

impl Iterator for MergedTermBlockScan<'_> {
    type Item = Result<(Vec<u8>, u64, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.step() {
            Ok(next) => next.map(Ok),
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}

fn page_error_from_message(message: String) -> BicDbError {
    BicDbError::PagedStorage(message)
}

impl PagedRecords {
    /// Open or create paged record storage under `dir`, recovering if needed.
    pub fn open(dir: impl AsRef<Path>, options: PagedRecordsOptions) -> Result<Self> {
        let dir = dir.as_ref();
        let store_options = PagedStoreOptions::default()
            .with_page_size(options.page_size)
            .with_buffer_pool_bytes(options.buffer_pool_bytes)
            .with_read_ahead_queue_pages(options.read_ahead_queue_pages)
            .with_fsync(options.fsync)
            .with_wal_max_bytes(options.wal_max_bytes)
            .with_wal_segment_bytes(options.wal_segment_bytes)
            .with_extent_bytes(options.extent_bytes)
            .with_accept_legacy_meta(options.accept_legacy_meta);
        let (store, _recovery) = PagedStore::open(dir, store_options).map_err(page_error)?;
        let aliases = load_nearby_index_aliases(dir)?;
        Ok(Self {
            value_compression: std::sync::atomic::AtomicBool::new(false),
            store: Arc::new(store),
            index_aliases: parking_lot::RwLock::new(aliases),
            entry_formats: parking_lot::RwLock::new(FxHashMap::default()),
            fts_segments: parking_lot::RwLock::new(FxHashMap::default()),
            fts_segments_root: dir.join("fts-segments"),
        })
    }

    pub fn store(&self) -> &Arc<PagedStore> {
        &self.store
    }

    pub(crate) fn page_size(&self) -> u32 {
        self.store.page_size()
    }

    pub(crate) fn set_index_aliases(&self, aliases: BTreeMap<String, String>) {
        *self.index_aliases.write() = aliases;
    }

    pub(crate) fn set_index_alias(&self, logical: &str, physical: &str) {
        self.index_aliases
            .write()
            .insert(logical.to_string(), physical.to_string());
    }

    pub(crate) fn remove_index_alias(&self, logical: &str) -> Option<String> {
        self.index_aliases.write().remove(logical)
    }

    pub(crate) fn resolve_index_name(&self, index: &str) -> String {
        self.index_aliases
            .read()
            .get(index)
            .cloned()
            .unwrap_or_else(|| index.to_string())
    }

    /// Begin a transaction and its read snapshot.
    pub fn begin(&self) -> (Xid, Snapshot) {
        self.store.begin_transaction()
    }

    pub fn latest_snapshot(&self) -> Snapshot {
        self.store.latest_snapshot()
    }

    /// DEBUG: version chain of a record key.
    pub fn debug_chain(&self, collection: &str, id: &str) -> Vec<(u64, u64)> {
        self.store.debug_chain(&record_key(collection, id))
    }

    pub fn inspect_version_chain(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<VersionChainInspection> {
        self.store
            .inspect_version_chain(&record_key(collection, id))
            .map_err(page_error)
    }

    pub fn verify_integrity(&self, max_fault_samples: usize) -> Result<PagedIntegrityReport> {
        self.store
            .verify_integrity(max_fault_samples)
            .map_err(page_error)
    }

    pub fn verify_btree_step(
        &self,
        cursor: BTreeVerifyCursor,
        limits: BTreeVerifyLimits,
    ) -> Result<BTreeVerifyStepReport> {
        self.store
            .verify_btree_step(cursor, limits)
            .map_err(page_error)
    }

    pub(crate) fn validate_new_btree_verify_limits(
        &self,
        limits: &BTreeVerifyLimits,
    ) -> Result<()> {
        self.store
            .validate_new_btree_verify_limits(limits)
            .map_err(page_error)
    }

    pub fn verify_version_chains_step(
        &self,
        cursor: VersionChainVerifyCursor,
        limits: VersionChainVerifyLimits,
    ) -> Result<VersionChainVerifyStepReport> {
        self.store
            .verify_version_chains_step(cursor, limits)
            .map_err(page_error)
    }

    pub fn replace_faulty_version_chain(
        &self,
        collection: &str,
        id: &str,
        expected_head: TupleLocator,
        replacement: &Record,
    ) -> Result<VersionChainRepairReport> {
        if replacement.id != id {
            return Err(BicDbError::PagedStorage(format!(
                "replacement record id `{}` does not match requested id `{id}`",
                replacement.id
            )));
        }
        let value = self.encode_value(replacement)?;
        self.store
            .replace_faulty_version_chain(&record_key(collection, id), expected_head, &value)
            .map_err(page_error)
    }

    /// Hold the store's writer lock across a whole multi-operation
    /// transaction — see `PagedStore::write_guard` for why interleaving two
    /// mirrored commit batches is corruption rather than mere contention.
    pub fn write_guard(&self) -> impl Drop + '_ {
        self.store.write_guard()
    }

    pub fn commit(&self, transaction: Xid) -> Result<()> {
        self.store.commit(transaction).map_err(page_error)
    }

    pub fn abort(&self, transaction: Xid) -> Result<()> {
        self.store.abort(transaction).map_err(page_error)
    }

    /// Store a record. Returns the heap locator of the version written — the
    /// chain head at write time, which ordered-index entries record as a
    /// TID-style hint so lookups can skip the key B-tree descent.
    /// Enable/disable zstd compression for values written from now on.
    /// Reads always understand both forms.
    pub fn set_value_compression(&self, enabled: bool) {
        self.value_compression
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    fn encode_value(&self, record: &Record) -> Result<Vec<u8>> {
        let raw = encode_record(record)?;
        if !self
            .value_compression
            .load(std::sync::atomic::Ordering::Relaxed)
            || raw.len() < VALUE_COMPRESSION_MIN_BYTES
        {
            return Ok(raw);
        }
        compress_record_value(&raw)
    }

    pub fn put(&self, transaction: Xid, collection: &str, record: &Record) -> Result<TupleLocator> {
        let key = record_key(collection, &record.id);
        let value = self.encode_value(record)?;
        self.store
            .put_returning_locator(transaction, &key, &value)
            .map_err(page_error)
    }

    /// Read a record via a TID-style hint recorded by [`encode_tid_hint`].
    ///
    /// Returns `None` when the hint cannot answer authoritatively — vacuumed
    /// or reused slot (xmin mismatch), superseded version, or a decoded row
    /// whose id disagrees — and the caller must re-read through the key
    /// descent. A `Some` is exactly what `get` would have returned.
    pub fn get_hinted(
        &self,
        snapshot: &Snapshot,
        id: &str,
        hint: TupleLocator,
        hint_xmin: Xid,
    ) -> Result<Option<Record>> {
        match self
            .store
            .get_as_of_hinted(snapshot, hint, hint_xmin)
            .map_err(page_error)?
        {
            bicdb_page::HintedRead::Hit(bytes) => {
                let record = decode_record(&bytes)?;
                // Belt and braces on top of the xmin identity check.
                if record.id == id {
                    Ok(Some(record))
                } else {
                    Ok(None)
                }
            }
            bicdb_page::HintedRead::Fallback => Ok(None),
        }
    }

    /// Read a record as of `snapshot`.
    /// SURGICAL pass-through: see `PagedStore::repair_version_header_stamp`.
    pub fn repair_record_header_stamp(
        &self,
        collection: &str,
        id: &str,
        field: bicdb_page::paged::HeaderStampField,
        expected: u64,
    ) -> Result<bicdb_page::paged::HeaderRepair> {
        let key = record_key(collection, id);
        self.store
            .repair_version_header_stamp(&key, field, expected)
            .map_err(page_error)
    }

    /// Checkpoint pass-through for repair durability.
    pub fn checkpoint_store(&self) -> Result<()> {
        self.store.checkpoint().map_err(page_error)
    }

    /// REPAIR pass-through: see `PagedStore::advance_transaction_floor`.
    /// Returns `(frozen_xid, next_xid)` after the durable checkpoint.
    pub fn advance_transaction_floor(&self, beyond: u64) -> Result<(u64, u64)> {
        self.store
            .advance_transaction_floor(beyond)
            .map_err(page_error)
    }

    pub fn get(&self, snapshot: &Snapshot, collection: &str, id: &str) -> Result<Option<Record>> {
        let key = record_key(collection, id);
        match self.store.get_as_of(snapshot, &key).map_err(page_error)? {
            Some(bytes) => Ok(Some(decode_record(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Batch point fetch with heap-page locality: many primary keys resolve
    /// their chain heads under one structure guard, then the heap reads run
    /// page-ordered across bounded workers. The win over one [`Self::get`]
    /// per key is IO shape — a scattered hydration batch (ranked search
    /// results) becomes clustered reads instead of random descent-plus-walk
    /// per record. Results align with `ids`.
    pub fn get_batch(
        &self,
        snapshot: &Snapshot,
        collection: &str,
        ids: &[&str],
    ) -> Result<Vec<Option<Record>>> {
        let keys = ids
            .iter()
            .map(|id| record_key(collection, id))
            .collect::<Vec<_>>();
        self.store
            .get_locality_batch(snapshot, &keys)
            .map_err(page_error)?
            .into_iter()
            .map(|bytes| bytes.as_deref().map(decode_record).transpose())
            .collect()
    }

    /// Delete a record. Returns whether it was present.
    pub fn delete(&self, transaction: Xid, collection: &str, id: &str) -> Result<bool> {
        let key = record_key(collection, id);
        self.store.delete(transaction, &key).map_err(page_error)
    }

    /// [`Self::scan`], additionally yielding each row's chain-head locator and
    /// that head's xmin — the TID hint an index backfill records.
    pub fn scan_with_heads<'a>(
        &'a self,
        snapshot: &Snapshot,
        collection: &str,
    ) -> Result<impl Iterator<Item = Result<(Record, TupleLocator, Xid)>> + 'a> {
        let prefix = collection_prefix(collection);
        let cursor = self
            .store
            .scan_from_with_heads(snapshot, &prefix)
            .map_err(page_error)?;
        let bound = prefix.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _, _, _)) => key.starts_with(&bound),
                Err(_) => true,
            })
            .map(|entry| {
                let (_, bytes, head, head_xmin) = entry.map_err(page_error)?;
                Ok((decode_record(&bytes)?, head, head_xmin))
            }))
    }

    /// Every record in a collection as of `snapshot`, as a bounded cursor.
    ///
    /// Stops at the collection's key-range boundary rather than filtering the
    /// whole store, so one large collection does not make another's scan slow.
    pub fn scan<'a>(
        &'a self,
        snapshot: &Snapshot,
        collection: &str,
    ) -> Result<impl Iterator<Item = Result<Record>> + 'a> {
        let prefix = collection_prefix(collection);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&prefix),
                Err(_) => true,
            })
            .map(|entry| {
                let (_, value) = entry.map_err(page_error)?;
                decode_record(&value)
            }))
    }

    /// Read at most one explicitly byte-bounded row batch, resumed strictly
    /// after `after_id`. The bound is charged from the encoded row before it
    /// is decoded, so a pathological value cannot silently blow the builder's
    /// memory budget. A single row larger than the budget is rejected.
    pub fn scan_batch_after(
        &self,
        snapshot: &Snapshot,
        collection: &str,
        after_id: Option<&str>,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Vec<Record>> {
        if max_rows == 0 || max_bytes == 0 {
            return Err(BicDbError::PagedStorage(
                "paged row batch limits must be non-zero".to_string(),
            ));
        }
        let prefix = collection_prefix(collection);
        let mut resume = prefix.clone();
        if let Some(after_id) = after_id {
            resume.extend_from_slice(after_id.as_bytes());
            resume.push(0);
        }
        let mut cursor = self
            .store
            .scan_from(snapshot, &resume)
            .map_err(page_error)?;
        let mut rows = Vec::with_capacity(max_rows.min(4096));
        let mut bytes = 0usize;
        while rows.len() < max_rows {
            let Some(entry) = cursor.next() else { break };
            let (key, value) = entry.map_err(page_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            if value.len() > max_bytes && rows.is_empty() {
                return Err(BicDbError::PagedStorage(format!(
                    "record `{}` is {} bytes, exceeding the {} byte batch limit",
                    String::from_utf8_lossy(&key[prefix.len()..]),
                    value.len(),
                    max_bytes
                )));
            }
            if !rows.is_empty() && bytes.saturating_add(value.len()) > max_bytes {
                break;
            }
            bytes = bytes.saturating_add(value.len());
            rows.push(decode_record(&value)?);
        }
        Ok(rows)
    }

    /// Read one bounded primary-key range while resolving its heap records in
    /// physical page order. The returned rows remain in primary-key order and
    /// use the same exclusive resume cursor as [`Self::scan_batch_after`].
    ///
    /// This is intended for large export and projection jobs on cold paged
    /// stores: it turns thousands of scattered synchronous heap reads into one
    /// locality-sorted, parallel batch without changing restart semantics.
    pub fn scan_locality_batch_after(
        &self,
        snapshot: &Snapshot,
        collection: &str,
        after_id: Option<&str>,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Vec<Record>> {
        if max_rows == 0 || max_bytes == 0 {
            return Err(BicDbError::PagedStorage(
                "paged row batch limits must be non-zero".to_string(),
            ));
        }
        let prefix = collection_prefix(collection);
        let resume = match after_id {
            Some(after_id) => {
                let mut key = record_key(collection, after_id);
                key.push(0);
                key
            }
            None => prefix.clone(),
        };
        let batch = self
            .store
            .scan_locality_batch(snapshot, &prefix, &resume, max_rows)
            .map_err(page_error)?;
        let decoded = decode_record_values_parallel(&batch.rows)?;
        let mut rows = Vec::with_capacity(decoded.len());
        let mut bytes = 0usize;
        for record in decoded {
            let encoded_bytes = serde_json::to_vec(&record)?.len();
            if encoded_bytes > max_bytes && rows.is_empty() {
                return Err(BicDbError::PagedStorage(format!(
                    "record `{}` is {} bytes, exceeding the {} byte batch limit",
                    record.id, encoded_bytes, max_bytes
                )));
            }
            if !rows.is_empty() && bytes.saturating_add(encoded_bytes) > max_bytes {
                break;
            }
            bytes = bytes.saturating_add(encoded_bytes);
            rows.push(record);
        }
        Ok(rows)
    }

    /// [`Self::scan_batch_after`], additionally yielding each row's chain-head
    /// locator and that head's xmin — the TID hint a resumable index backfill
    /// records in the entries it writes. Same bounds, same resume contract.
    pub fn scan_batch_after_with_heads(
        &self,
        snapshot: &Snapshot,
        collection: &str,
        after_id: Option<&str>,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Vec<(Record, TupleLocator, Xid)>> {
        if max_rows == 0 || max_bytes == 0 {
            return Err(BicDbError::PagedStorage(
                "paged row batch limits must be non-zero".to_string(),
            ));
        }
        let prefix = collection_prefix(collection);
        let mut resume = prefix.clone();
        if let Some(after_id) = after_id {
            resume.extend_from_slice(after_id.as_bytes());
            resume.push(0);
        }
        let mut cursor = self
            .store
            .scan_from_with_heads(snapshot, &resume)
            .map_err(page_error)?;
        let mut rows = Vec::with_capacity(max_rows.min(4096));
        let mut bytes = 0usize;
        while rows.len() < max_rows {
            let Some(entry) = cursor.next() else { break };
            let (key, value, head, head_xmin) = entry.map_err(page_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            if value.len() > max_bytes && rows.is_empty() {
                return Err(BicDbError::PagedStorage(format!(
                    "record `{}` is {} bytes, exceeding the {} byte batch limit",
                    String::from_utf8_lossy(&key[prefix.len()..]),
                    value.len(),
                    max_bytes
                )));
            }
            if !rows.is_empty() && bytes.saturating_add(value.len()) > max_bytes {
                break;
            }
            bytes = bytes.saturating_add(value.len());
            rows.push((decode_record(&value)?, head, head_xmin));
        }
        Ok(rows)
    }

    /// Every record's *identity* in a collection as of `snapshot`: id, vector,
    /// geometry, and timestamp, with the metadata lexed past rather than
    /// parsed and the payload dropped.
    ///
    /// This exists for open. Building resident stubs needs exactly the
    /// identity fields, and the full decode's cost is dominated by the two
    /// things stubs never keep: copying the metadata JSON and materializing
    /// its `Value` tree. Skipping both is what makes opening an indexed paged
    /// collection proportional to key+identity bytes rather than to corpus
    /// bytes.
    pub fn scan_identities<'a>(
        &'a self,
        snapshot: &Snapshot,
        collection: &str,
    ) -> Result<impl Iterator<Item = Result<Record>> + 'a> {
        let prefix = collection_prefix(collection);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&prefix),
                Err(_) => true,
            })
            .map(|entry| {
                let (_, value) = entry.map_err(page_error)?;
                decode_identity(&value)
            }))
    }

    /// Like [`Self::scan_identities`], but starting strictly AFTER `after`
    /// (exclusive cursor) — what a resumable corpus scan checkpoints on.
    /// Record keys embed the id in byte order, so the cursor is one seek.
    pub fn scan_identities_after<'a>(
        &'a self,
        snapshot: &Snapshot,
        collection: &str,
        after: Option<&str>,
    ) -> Result<impl Iterator<Item = Result<Record>> + 'a> {
        let prefix = collection_prefix(collection);
        let start = match after {
            Some(after) => record_key(collection, after),
            None => prefix.clone(),
        };
        let skip_exact = after.map(|after| record_key(collection, after));
        let cursor = self.store.scan_from(snapshot, &start).map_err(page_error)?;
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&prefix),
                Err(_) => true,
            })
            .filter_map(move |entry| match entry {
                Ok((key, value)) => {
                    if skip_exact.as_deref() == Some(key.as_slice()) {
                        None
                    } else {
                        Some(decode_identity(&value))
                    }
                }
                Err(error) => Some(Err(page_error(error))),
            }))
    }

    /// Every record id in a collection, from the index keys alone — no heap
    /// page is read and no value is decoded.
    ///
    /// This is the cheapest possible corpus walk: record keys embed the id, so
    /// a leaf-order traversal of the collection's key range yields every id at
    /// the cost of the leaf pages only. Ids whose versions are all deleted
    /// still appear (the key outlives its versions until vacuum); callers that
    /// resolve an id and find nothing must treat that as "row gone", exactly
    /// as they do for a GC'd chain.
    pub fn scan_ids<'a>(
        &'a self,
        snapshot: &Snapshot,
        collection: &str,
    ) -> Result<impl Iterator<Item = Result<String>> + 'a> {
        let prefix = collection_prefix(collection);
        let prefix_len = prefix.len();
        let cursor = self
            .store
            .scan_keys_from(snapshot, &prefix)
            .map_err(page_error)?;
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok(key) => key.starts_with(&prefix),
                Err(_) => true,
            })
            .map(move |entry| {
                let key = entry.map_err(page_error)?;
                String::from_utf8(key[prefix_len..].to_vec())
                    .map_err(|_| BicDbError::PagedStorage("record key id is not utf-8".to_string()))
            }))
    }

    /// Visit a collection's rows in bounded batches, as of `snapshot`.
    ///
    /// Each batch is a fresh key-range scan resumed just past the previous
    /// batch's last key, so peak memory is one batch — not the collection. The
    /// callback returns `false` to stop early, which is what lets `LIMIT`
    /// avoid reading rows nobody asked for.
    ///
    /// Batching rather than one long-lived iterator is deliberate: a cursor
    /// borrowing the store cannot be handed across `bicdb-core`'s API without a
    /// self-referential type, and holding one open across arbitrary caller code
    /// would pin buffer-pool pages for as long as the caller took.
    pub fn for_each_batch(
        &self,
        snapshot: &Snapshot,
        collection: &str,
        batch_size: usize,
        visit: impl FnMut(Vec<Record>) -> Result<bool>,
    ) -> Result<()> {
        self.for_each_batch_cancellable(
            snapshot,
            collection,
            batch_size,
            &CancellationToken::uncancelable(),
            visit,
        )
    }

    /// Cancellation-aware bounded batch scan.
    pub fn for_each_batch_cancellable(
        &self,
        snapshot: &Snapshot,
        collection: &str,
        batch_size: usize,
        cancellation: &CancellationToken,
        visit: impl FnMut(Vec<Record>) -> Result<bool>,
    ) -> Result<()> {
        self.for_each_batch_after_cancellable(
            snapshot,
            collection,
            batch_size,
            None,
            cancellation,
            visit,
        )
    }

    /// Bounded batch scan resumed strictly after `after_id`.
    pub fn for_each_batch_after(
        &self,
        snapshot: &Snapshot,
        collection: &str,
        batch_size: usize,
        after_id: Option<&str>,
        visit: impl FnMut(Vec<Record>) -> Result<bool>,
    ) -> Result<()> {
        self.for_each_batch_after_cancellable(
            snapshot,
            collection,
            batch_size,
            after_id,
            &CancellationToken::uncancelable(),
            visit,
        )
    }

    /// Cancellation-aware bounded batch scan resumed strictly after
    /// `after_id`. Cancellation is checked before every page-backed record is
    /// decoded and before caller code receives a batch, so all cursor and page
    /// guards have unwound when the error reaches the caller.
    pub fn for_each_batch_after_cancellable(
        &self,
        snapshot: &Snapshot,
        collection: &str,
        batch_size: usize,
        after_id: Option<&str>,
        cancellation: &CancellationToken,
        mut visit: impl FnMut(Vec<Record>) -> Result<bool>,
    ) -> Result<()> {
        let prefix = collection_prefix(collection);
        let mut resume = prefix.clone();
        if let Some(after_id) = after_id {
            resume.extend_from_slice(after_id.as_bytes());
            resume.push(0);
        }
        loop {
            cancellation.check()?;
            let mut batch = Vec::with_capacity(batch_size.min(4096));
            let mut last_key = None;
            {
                let mut cursor = self
                    .store
                    .scan_from(snapshot, &resume)
                    .map_err(page_error)?;
                loop {
                    cancellation.check()?;
                    let Some(entry) = cursor.next() else {
                        break;
                    };
                    let (key, value) = entry.map_err(page_error)?;
                    if !key.starts_with(&prefix) {
                        last_key = None;
                        break;
                    }
                    batch.push(decode_record(&value)?);
                    last_key = Some(key);
                    if batch.len() >= batch_size {
                        break;
                    }
                }
            }
            let exhausted = batch.len() < batch_size || last_key.is_none();
            cancellation.check()?;
            if !batch.is_empty() && !visit(batch)? {
                return Ok(());
            }
            match last_key {
                // Resume strictly after the last key: appending a zero byte
                // yields the smallest key greater than it, so no row is
                // visited twice and none is skipped.
                Some(key) if !exhausted => {
                    resume = key;
                    resume.push(0);
                }
                _ => return Ok(()),
            }
        }
    }

    /// Bounded resumable scan that resolves each candidate range in heap-page
    /// order, then restores primary-key order before invoking the caller.
    pub fn for_each_locality_batch_after_cancellable(
        &self,
        snapshot: &Snapshot,
        collection: &str,
        batch_size: usize,
        after_id: Option<&str>,
        cancellation: &CancellationToken,
        mut visit: impl FnMut(Vec<Record>) -> Result<bool>,
    ) -> Result<()> {
        let prefix = collection_prefix(collection);
        let mut resume = match after_id {
            Some(after_id) => {
                let mut key = record_key(collection, after_id);
                key.push(0);
                key
            }
            None => prefix.clone(),
        };
        loop {
            cancellation.check()?;
            let batch = self
                .store
                .scan_locality_batch(snapshot, &prefix, &resume, batch_size.max(1))
                .map_err(page_error)?;
            let records = decode_record_values_parallel(&batch.rows)?;
            cancellation.check()?;
            if !records.is_empty() && !visit(records)? {
                return Ok(());
            }
            match batch.last_key {
                Some(mut key) if !batch.exhausted => {
                    key.push(0);
                    resume = key;
                }
                _ => return Ok(()),
            }
        }
    }

    /// Number of rows visible to `snapshot` in a collection.
    ///
    /// Visibility-checked (so tombstoned keys are excluded) but without
    /// decoding any record — the cost is the heap reads the visibility check
    /// needs, not the JSON parse a scan would add.
    pub fn count_live(&self, snapshot: &Snapshot, collection: &str) -> Result<usize> {
        self.count_live_cancellable(snapshot, collection, &CancellationToken::uncancelable())
    }

    /// Visibility-checked count that abandons its page cursor promptly when a
    /// query is canceled or reaches its deadline.
    pub fn count_live_cancellable(
        &self,
        snapshot: &Snapshot,
        collection: &str,
        cancellation: &CancellationToken,
    ) -> Result<usize> {
        let prefix = collection_prefix(collection);
        let mut cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let mut count = 0usize;
        loop {
            cancellation.check()?;
            let Some(entry) = cursor.next() else {
                break;
            };
            let (key, _) = entry.map_err(page_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    /// Flush and bound the log.
    pub fn checkpoint(&self) -> Result<()> {
        self.store.checkpoint().map_err(page_error)
    }

    pub fn writeback_step(
        &self,
        cursor: WritebackCursor,
        limits: WritebackLimits,
    ) -> Result<WritebackStepReport> {
        self.store
            .writeback_step(cursor, limits)
            .map_err(page_error)
    }

    pub fn request_read_ahead<I>(&self, page_ids: I) -> ReadAheadSubmitReport
    where
        I: IntoIterator<Item = u64>,
    {
        self.store.request_read_ahead(page_ids)
    }

    pub fn read_ahead_queue_depth(&self) -> u64 {
        self.store.read_ahead_queue_depth()
    }

    pub fn read_ahead_step(&self, limits: ReadAheadLimits) -> Result<ReadAheadStepReport> {
        self.store.read_ahead_step(limits).map_err(page_error)
    }

    /// See [`bicdb_page::pool::BufferPool::register_read_ahead_driver`].
    pub fn register_read_ahead_driver(&self) {
        self.store.register_read_ahead_driver();
    }

    /// See [`bicdb_page::pool::BufferPool::unregister_read_ahead_driver`].
    pub fn unregister_read_ahead_driver(&self) {
        self.store.unregister_read_ahead_driver();
    }

    pub fn checkpoint_step(
        &self,
        cursor: PagedCheckpointCursor,
        limits: PagedCheckpointLimits,
    ) -> Result<PagedCheckpointStepReport> {
        self.store
            .checkpoint_step(cursor, limits)
            .map_err(page_error)
    }

    pub(crate) fn validate_checkpoint_limits(&self, limits: PagedCheckpointLimits) -> Result<()> {
        self.store
            .validate_checkpoint_limits(limits)
            .map_err(page_error)
    }

    /// Reclaim versions no snapshot can see.
    /// Remove B-tree keys whose whole version chain was vacuumed away.
    pub fn sweep_dead_index_entries(&self, max_keys: usize) -> Result<u64> {
        self.store
            .sweep_dead_index_entries(max_keys)
            .map_err(page_error)
    }

    pub fn vacuum(&self, max_pages: usize) -> Result<u64> {
        Ok(self
            .store
            .vacuum(max_pages)
            .map_err(page_error)?
            .versions_reclaimed)
    }

    /// Reclaim one page/byte/time-bounded slice and return its exact restart
    /// cursor. Callers checkpoint the serializable cursor only after this
    /// method succeeds; repeating a page after a crash is safe.
    pub fn punch_free_pages(&self, max_pages: u64) -> Result<bicdb_page::HolePunchReport> {
        self.store.punch_free_pages(max_pages).map_err(page_error)
    }

    pub fn vacuum_step(&self, cursor: VacuumCursor, limits: VacuumLimits) -> Result<VacuumReport> {
        self.store.vacuum_step(cursor, limits).map_err(page_error)
    }

    /// Validate a maintenance envelope against this store's durable page size
    /// without touching any page or advancing a cursor.
    pub(crate) fn validate_vacuum_limits(&self, limits: &VacuumLimits) -> Result<()> {
        self.store
            .validate_vacuum_limits(limits)
            .map_err(page_error)
    }

    /// Bytes currently held by the buffer pool — the bounded figure.
    pub fn resident_bytes(&self) -> u64 {
        self.store.buffer_pool().snapshot().resident_bytes
    }

    /// Scrape-safe cache, page-file, and WAL telemetry.
    pub fn storage_snapshot(&self) -> Result<PagedStoreSnapshot> {
        self.store.snapshot().map_err(page_error)
    }

    // ------------------------------------------------------------------
    // Secondary-index entries (Phase 4).
    //
    // Entries live in the SAME tree, WAL, and transactions as records, under a
    // reserved prefix no record key can produce (record keys start with a
    // non-zero u16 collection-name length). Writing an entry in the same
    // transaction as its row is what makes the index crash-consistent by
    // construction: recovery keeps or discards row and entry together, so
    // "restart without a corpus-wide rebuild" needs no separate machinery.
    // ------------------------------------------------------------------

    /// Store one index entry: `pk` appears under `encoded_key` in `index`.
    pub fn put_index_entry(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        pk: &str,
        value: &[u8],
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        let key = index_entry_key(&index, encoded_key, pk);
        // B-tree entries pass an empty value (everything a lookup yields is
        // in the key); full-text entries carry the posting payload
        // (positions/weights + document scalars) so ranking can read the
        // index instead of re-parsing the document.
        self.store.put(transaction, &key, value).map_err(page_error)
    }

    /// Remove one index entry. Returns whether it was present.
    pub fn delete_index_entry(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        pk: &str,
    ) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let key = index_entry_key(&index, encoded_key, pk);
        self.store.delete(transaction, &key).map_err(page_error)
    }

    /// Every `(encoded_key, pk)` pair in `index` as of `snapshot`, in
    /// `(encoded_key, pk)` order — the durable content of the index, as a
    /// bounded cursor over the index's own key range.
    pub fn scan_index<'a>(
        &'a self,
        snapshot: &Snapshot,
        index: &str,
    ) -> Result<impl Iterator<Item = Result<(Vec<u8>, String)>> + 'a> {
        let index = self.resolve_index_name(index);
        let prefix = index_prefix(&index);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let resolve_snapshot = snapshot.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&prefix),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, _) = entry.map_err(page_error)?;
                let (encoded, entry_ref) = decode_index_entry_key_any(&index, &key)?;
                let pk = self.resolve_entry_ref(&resolve_snapshot, &index, entry_ref)?;
                Ok((encoded, pk))
            }))
    }

    /// Read one bounded batch from an index namespace. `after` is the last
    /// `(encoded key, primary key)` returned by the previous call.
    pub fn scan_index_batch_after(
        &self,
        snapshot: &Snapshot,
        index: &str,
        after: Option<(&[u8], &str)>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<(Vec<u8>, String)>> {
        if max_entries == 0 || max_bytes == 0 {
            return Err(BicDbError::PagedStorage(
                "paged index batch limits must be non-zero".to_string(),
            ));
        }
        let index = self.resolve_index_name(index);
        let prefix = index_prefix(&index);
        let mut resume = after
            .map(|(key, pk)| {
                let mut key = index_entry_key(&index, key, pk);
                key.push(0);
                key
            })
            .unwrap_or_else(|| prefix.clone());
        if resume < prefix {
            resume = prefix.clone();
        }
        let mut cursor = self
            .store
            .scan_from(snapshot, &resume)
            .map_err(page_error)?;
        let mut entries = Vec::with_capacity(max_entries.min(4096));
        let mut bytes = 0usize;
        while entries.len() < max_entries {
            let Some(entry) = cursor.next() else { break };
            let (key, value) = entry.map_err(page_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let charge = key.len().saturating_add(value.len());
            if charge > max_bytes && entries.is_empty() {
                return Err(BicDbError::PagedStorage(format!(
                    "index entry is {charge} bytes, exceeding the {max_bytes} byte batch limit"
                )));
            }
            if !entries.is_empty() && bytes.saturating_add(charge) > max_bytes {
                break;
            }
            bytes = bytes.saturating_add(charge);
            entries.push(decode_index_entry_key(&index, &key)?);
        }
        Ok(entries)
    }

    /// One bounded batch of raw index entries, resumed strictly after a RAW
    /// store key — the format-agnostic cursor for counting/verification over
    /// indexes that may hold v3 (intern-keyed) entries, where a (key, pk)
    /// resume position cannot address an entry.
    pub fn scan_index_batch_after_raw(
        &self,
        snapshot: &Snapshot,
        index: &str,
        after_raw: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>, IndexEntryRef)>> {
        if max_entries == 0 || max_bytes == 0 {
            return Err(BicDbError::PagedStorage(
                "paged index batch limits must be non-zero".to_string(),
            ));
        }
        let index = self.resolve_index_name(index);
        let prefix = index_prefix(&index);
        let mut resume = match after_raw {
            Some(raw) => {
                let mut key = raw.to_vec();
                key.push(0);
                key
            }
            None => prefix.clone(),
        };
        if resume < prefix {
            resume = prefix.clone();
        }
        let mut cursor = self
            .store
            .scan_from(snapshot, &resume)
            .map_err(page_error)?;
        let mut entries = Vec::with_capacity(max_entries.min(4096));
        let mut bytes = 0usize;
        while entries.len() < max_entries {
            let Some(entry) = cursor.next() else { break };
            let (key, value) = entry.map_err(page_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let charge = key.len().saturating_add(value.len());
            if charge > max_bytes && entries.is_empty() {
                return Err(BicDbError::PagedStorage(format!(
                    "index entry is {charge} bytes, exceeding the {max_bytes} byte batch limit"
                )));
            }
            if !entries.is_empty() && bytes.saturating_add(charge) > max_bytes {
                break;
            }
            bytes = bytes.saturating_add(charge);
            let (encoded, entry_ref) = decode_index_entry_key_any(&index, &key)?;
            entries.push((key, encoded, entry_ref));
        }
        Ok(entries)
    }

    /// Primary keys of every visible entry of `index` whose ENCODED index key
    /// equals `encoded_key` exactly — a bounded range scan: one B-tree descent
    /// plus the matching leaf run, served through the buffer pool. This is
    /// the read-through point lookup for durable inverted indexes: nothing
    /// about the posting list is resident beyond the pages it touches.
    pub fn scan_index_exact<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
    ) -> Result<impl Iterator<Item = Result<(String, Vec<u8>)>> + 'store> {
        // The full entry prefix for ONE key: escaped key then the 0x00 0x00
        // terminator; everything after it in a matching entry is the pk. The
        // value rides along — it is the posting payload for FTS entries.
        let index = self.resolve_index_name(index);
        let mut prefix = index_entry_prefix(&index, encoded_key);
        prefix.extend_from_slice(&[0, 0]);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let filter_prefix = prefix.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&filter_prefix),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let pk = String::from_utf8(key[prefix.len()..].to_vec()).map_err(|error| {
                    BicDbError::PagedStorage(format!("index entry pk not UTF-8: {error}"))
                })?;
                Ok((pk, value))
            }))
    }

    /// `(encoded_key, pk)` for every visible entry of `index` whose ENCODED
    /// index key starts with `encoded_prefix` — the read-through prefix scan
    /// (`term:*`), bounded to the matching key range instead of the whole
    /// index namespace.
    pub fn scan_index_encoded_prefix<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_prefix: &[u8],
    ) -> Result<impl Iterator<Item = Result<(Vec<u8>, String, Vec<u8>)>> + 'store> {
        let index = self.resolve_index_name(index);
        let prefix = index_entry_prefix(&index, encoded_prefix);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&prefix),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let (encoded_key, pk) = decode_index_entry_key(&index, &key)?;
                Ok((encoded_key, pk, value))
            }))
    }

    /// `(encoded_key, pk)` for every visible entry of `index` whose ENCODED
    /// index key is >= `encoded_start`, ascending — the read-through RANGE
    /// scan primitive. One B-tree descent to the start key, then the leaf
    /// run; the caller decides where to stop (the entry escaping is
    /// order-preserving, so entry order equals `(encoded_key, pk)` order).
    /// Bounded to the index namespace, not the whole store.
    pub fn scan_index_from<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_start: &[u8],
    ) -> Result<impl Iterator<Item = Result<(Vec<u8>, String)>> + 'store> {
        let index = self.resolve_index_name(index);
        let namespace = index_prefix(&index);
        let start = index_entry_prefix(&index, encoded_start);
        let cursor = self.store.scan_from(snapshot, &start).map_err(page_error)?;
        let resolve_snapshot = snapshot.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&namespace),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, _) = entry.map_err(page_error)?;
                let (encoded, entry_ref) = decode_index_entry_key_any(&index, &key)?;
                let pk = self.resolve_entry_ref(&resolve_snapshot, &index, entry_ref)?;
                Ok((encoded, pk))
            }))
    }

    /// [`Self::scan_index_from`] for BOTH entry formats, with values:
    /// `(encoded_key, row reference, entry value)` ascending from
    /// `encoded_start`.
    pub fn scan_index_from_refs<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_start: &[u8],
    ) -> Result<impl Iterator<Item = Result<(Vec<u8>, IndexEntryRef, Vec<u8>)>> + 'store> {
        let index = self.resolve_index_name(index);
        let namespace = index_prefix(&index);
        let start = index_entry_prefix(&index, encoded_start);
        let cursor = self.store.scan_from(snapshot, &start).map_err(page_error)?;
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&namespace),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let (encoded, entry_ref) = decode_index_entry_key_any(&index, &key)?;
                Ok((encoded, entry_ref, value))
            }))
    }

    /// `(encoded_key, pk)` for every visible entry of `index` in DESCENDING
    /// `(encoded_key, pk)` order, restricted to encoded keys strictly below
    /// `encoded_bound` (`None` = from the greatest key). One descent to the
    /// bound, then descending leaf runs; a previous-leaf step costs one root
    /// descent. This is what lets `ORDER BY k DESC LIMIT n` and `MAX(k)`
    /// touch n entries instead of forward-collecting the whole range.
    pub fn scan_index_rev_below<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_bound: &'store [u8],
    ) -> Result<impl Iterator<Item = Result<(Vec<u8>, String)>> + 'store> {
        let index = self.resolve_index_name(index);
        let namespace = index_prefix(&index);
        // Entries for keys >= bound all sort at or above `namespace ++
        // escaped(bound)` (the escaping is order-preserving and the [0,0]
        // terminator sorts below every escaped continuation), so that store
        // key is an exact exclusive bound for "encoded key < bound".
        let store_bound = index_entry_prefix(&index, encoded_bound);
        let cursor = self
            .store
            .scan_rev_below(snapshot, Some(&store_bound))
            .map_err(page_error)?;
        let resolve_snapshot = snapshot.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&namespace),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, _) = entry.map_err(page_error)?;
                let (encoded, entry_ref) = decode_index_entry_key_any(&index, &key)?;
                let pk = self.resolve_entry_ref(&resolve_snapshot, &index, entry_ref)?;
                Ok((encoded, pk))
            }))
    }

    /// [`Self::scan_index_rev_below`] for BOTH entry formats, with values.
    pub fn scan_index_rev_below_refs<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_bound: &'store [u8],
    ) -> Result<impl Iterator<Item = Result<(Vec<u8>, IndexEntryRef, Vec<u8>)>> + 'store> {
        let index = self.resolve_index_name(index);
        let namespace = index_prefix(&index);
        let store_bound = index_entry_prefix(&index, encoded_bound);
        let cursor = self
            .store
            .scan_rev_below(snapshot, Some(&store_bound))
            .map_err(page_error)?;
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&namespace),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let (encoded, entry_ref) = decode_index_entry_key_any(&index, &key)?;
                Ok((encoded, entry_ref, value))
            }))
    }

    /// Like [`Self::scan_index_rev_below`] with no key bound: descending over
    /// the whole index namespace, starting from its greatest entry.
    pub fn scan_index_rev<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
    ) -> Result<impl Iterator<Item = Result<(Vec<u8>, String)>> + 'store> {
        let index = self.resolve_index_name(index);
        let namespace = index_prefix(&index);
        // The exclusive upper bound for the whole namespace is its prefix
        // successor; the reserved prefix always has one.
        let mut bound = namespace.clone();
        while bound.last() == Some(&0xFF) {
            bound.pop();
        }
        let cursor = match bound.last_mut() {
            Some(last) => {
                *last += 1;
                self.store
                    .scan_rev_below(snapshot, Some(&bound))
                    .map_err(page_error)?
            }
            None => self
                .store
                .scan_rev_below(snapshot, None)
                .map_err(page_error)?,
        };
        let resolve_snapshot = snapshot.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&namespace),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, _) = entry.map_err(page_error)?;
                let (encoded, entry_ref) = decode_index_entry_key_any(&index, &key)?;
                let pk = self.resolve_entry_ref(&resolve_snapshot, &index, entry_ref)?;
                Ok((encoded, pk))
            }))
    }

    /// One durable index entry by exact (encoded key, pk): a single B-tree
    /// descent through the buffer pool. This is the POINT PROBE that turns an
    /// AND over a rare and a common term from "walk the common term's whole
    /// posting list" into "|rare| probes".
    /// [`Self::scan_index_rev`] for BOTH entry formats, with values.
    pub fn scan_index_rev_refs<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
    ) -> Result<impl Iterator<Item = Result<(Vec<u8>, IndexEntryRef, Vec<u8>)>> + 'store> {
        let index = self.resolve_index_name(index);
        let namespace = index_prefix(&index);
        // The exclusive upper bound for the whole namespace is its prefix
        // successor; the reserved prefix always has one.
        let mut bound = namespace.clone();
        while bound.last() == Some(&0xFF) {
            bound.pop();
        }
        let cursor = match bound.last_mut() {
            Some(last) => {
                *last += 1;
                self.store
                    .scan_rev_below(snapshot, Some(&bound))
                    .map_err(page_error)?
            }
            None => self
                .store
                .scan_rev_below(snapshot, None)
                .map_err(page_error)?,
        };
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&namespace),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let (encoded, entry_ref) = decode_index_entry_key_any(&index, &key)?;
                Ok((encoded, entry_ref, value))
            }))
    }

    /// One durable index entry by exact (encoded key, pk): a single B-tree
    /// descent through the buffer pool. This is the POINT PROBE that turns an
    /// AND over a rare and a common term from "walk the common term's whole
    /// posting list" into "|rare| probes".

    pub fn get_index_entry(
        &self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
        pk: &str,
    ) -> Result<Option<Vec<u8>>> {
        let index = self.resolve_index_name(index);
        // Format-aware point probe: a v3 index's entry for this row lives at
        // (key, intern id), so resolve the pk through the reverse dictionary
        // first. A pk with no intern id has no v3 entry by construction.
        let key = match self.entry_format_full_cached(&index)? {
            (IndexEntryFormat::V2, _) => index_entry_key(&index, encoded_key, pk),
            (IndexEntryFormat::V3, collection) => {
                let collection = collection.ok_or_else(|| {
                    BicDbError::PagedStorage(format!(
                        "index `{index}` has v3 entries but no recorded intern collection"
                    ))
                })?;
                match self.intern_id_for_pk(snapshot, &collection, pk)? {
                    Some(id) => index_entry_key_v3(&index, encoded_key, id),
                    None => return Ok(None),
                }
            }
        };
        self.store.get_as_of(snapshot, &key).map_err(page_error)
    }

    /// Approximate posting count for one exact key: KEY-ONLY leaf walk, no
    /// heap page reads, no visibility resolution — dead versions count too.
    /// Only a heuristic (driver selection); `None` means "more than cap".
    pub fn index_posting_keys_capped(
        &self,
        index: &str,
        encoded_key: &[u8],
        cap: usize,
    ) -> Result<Option<usize>> {
        let index = self.resolve_index_name(index);
        let mut prefix = index_entry_prefix(&index, encoded_key);
        prefix.extend_from_slice(&[0, 0]);
        let snapshot = self.latest_snapshot();
        let cursor = self
            .store
            .scan_keys_from(&snapshot, &prefix)
            .map_err(page_error)?;
        let mut count = 0usize;
        for key in cursor {
            let key = key.map_err(page_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
            if count > cap {
                return Ok(None);
            }
        }
        Ok(Some(count))
    }

    /// v2 (impact-ordered) writes/reads. `bucket` is the UN-inverted impact
    /// bucket (higher = more relevant); inversion happens in the key.
    pub fn put_index_entry_v2(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        bucket: u16,
        pk: &str,
        value: &[u8],
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        let key = index_entry_key_v2(&index, encoded_key, bucket, pk);
        self.store.put(transaction, &key, value).map_err(page_error)
    }

    pub fn delete_index_entry_v2(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        bucket: u16,
        pk: &str,
    ) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let key = index_entry_key_v2(&index, encoded_key, bucket, pk);
        self.store.delete(transaction, &key).map_err(page_error)
    }

    /// Postings of one exact term in DESCENDING impact order:
    /// `(bucket, pk, payload)`.
    pub fn scan_index_exact_v2<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
    ) -> Result<impl Iterator<Item = Result<(u16, String, Vec<u8>)>> + 'store> {
        let index = self.resolve_index_name(index);
        let mut prefix = index_entry_prefix_v2(&index, encoded_key);
        prefix.extend_from_slice(&[0, 0]);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let filter_prefix = prefix.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&filter_prefix),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let tail = &key[prefix.len()..];
                if tail.len() < 2 {
                    return Err(BicDbError::PagedStorage(
                        "v2 index entry missing bucket".to_string(),
                    ));
                }
                let inverted = u16::from_be_bytes([tail[0], tail[1]]);
                let pk = std::str::from_utf8(&tail[2..])
                    .map_err(|error| {
                        BicDbError::PagedStorage(format!("index entry pk not UTF-8: {error}"))
                    })?
                    .to_string();
                Ok((u16::MAX - inverted, pk, value))
            }))
    }

    /// Point probe in the v2 keyspace WITHOUT knowing the bucket: bounded
    /// range scan over the (term, *) space filtered by pk would be O(term);
    /// instead the caller supplies the bucket recomputed from the record's
    /// projection (deterministic), so this stays a single descent.
    pub fn get_index_entry_v2(
        &self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
        bucket: u16,
        pk: &str,
    ) -> Result<Option<Vec<u8>>> {
        let index = self.resolve_index_name(index);
        let key = index_entry_key_v2(&index, encoded_key, bucket, pk);
        self.store.get_as_of(snapshot, &key).map_err(page_error)
    }

    pub fn index_has_entries_v2(&self, snapshot: &Snapshot, index: &str) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let prefix = index_prefix_v2(&index);
        let mut cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        match cursor.next() {
            Some(Ok((key, _))) => Ok(key.starts_with(&prefix)),
            Some(Err(error)) => Err(page_error(error)),
            None => Ok(false),
        }
    }

    /// Key-only capped count in the v2 keyspace (driver-selection heuristic).
    pub fn index_posting_keys_capped_v2(
        &self,
        index: &str,
        encoded_key: &[u8],
        cap: usize,
    ) -> Result<Option<usize>> {
        let index = self.resolve_index_name(index);
        let mut prefix = index_entry_prefix_v2(&index, encoded_key);
        prefix.extend_from_slice(&[0, 0]);
        let snapshot = self.latest_snapshot();
        let cursor = self
            .store
            .scan_keys_from(&snapshot, &prefix)
            .map_err(page_error)?;
        let mut count = 0usize;
        for key in cursor {
            let key = key.map_err(page_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
            if count > cap {
                return Ok(None);
            }
        }
        Ok(Some(count))
    }

    /// Write one posting block for `encoded_key`, keyed by its LAST pk.
    pub fn put_posting_block(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        last_pk: &str,
        block: &[u8],
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        let key = ns_term_key(INDEX_NS_V3, &index, encoded_key, last_pk);
        self.store.put(transaction, &key, block).map_err(page_error)
    }

    pub fn delete_posting_block(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        last_pk: &str,
    ) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let key = ns_term_key(INDEX_NS_V3, &index, encoded_key, last_pk);
        self.store.delete(transaction, &key).map_err(page_error)
    }

    /// All blocks of one term, in pk order: `(last_pk, block_bytes)`.
    pub fn scan_posting_blocks<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
    ) -> Result<impl Iterator<Item = Result<(String, Vec<u8>)>> + 'store> {
        let index = self.resolve_index_name(index);
        let mut prefix = ns_entry_prefix(INDEX_NS_V3, &index, encoded_key);
        prefix.extend_from_slice(&[0, 0]);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let filter_prefix = prefix.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&filter_prefix),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let last_pk = String::from_utf8(key[prefix.len()..].to_vec()).map_err(|error| {
                    BicDbError::PagedStorage(format!("block key pk not UTF-8: {error}"))
                })?;
                Ok((last_pk, value))
            }))
    }

    /// The block that could contain `pk` (first block whose last_pk >= pk):
    /// one descent. `None` when pk sorts after every block.
    pub fn posting_block_for(
        &self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
        pk: &str,
    ) -> Result<Option<Vec<u8>>> {
        let index = self.resolve_index_name(index);
        let mut prefix = ns_entry_prefix(INDEX_NS_V3, &index, encoded_key);
        prefix.extend_from_slice(&[0, 0]);
        let mut start = prefix.clone();
        start.extend_from_slice(pk.as_bytes());
        let mut cursor = self.store.scan_from(snapshot, &start).map_err(page_error)?;
        match cursor.next() {
            Some(Ok((key, value))) if key.starts_with(&prefix) => Ok(Some(value)),
            Some(Err(error)) => Err(page_error(error)),
            _ => Ok(None),
        }
    }

    /// Publish (or replace) a packed spatial index's meta — the atomic
    /// generation swap for the immutable node tree.
    pub(crate) fn put_spatial_meta(
        &self,
        transaction: Xid,
        index: &str,
        meta: &[u8],
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        self.store
            .put(transaction, &spatial_meta_key(&index), meta)
            .map_err(page_error)
    }

    pub(crate) fn spatial_meta(&self, snapshot: &Snapshot, index: &str) -> Result<Option<Vec<u8>>> {
        let index = self.resolve_index_name(index);
        self.store
            .get_as_of(snapshot, &spatial_meta_key(&index))
            .map_err(page_error)
    }

    /// Unpublish a packed generation: with the meta gone, nothing reads the
    /// nodes or delta again (they become sweepable garbage).
    pub(crate) fn delete_spatial_meta(&self, transaction: Xid, index: &str) -> Result<bool> {
        let index = self.resolve_index_name(index);
        self.store
            .delete(transaction, &spatial_meta_key(&index))
            .map_err(page_error)
    }

    /// Write one immutable packed node under `generation`. Unpublished
    /// generations are invisible: nothing reads a node the meta doesn't
    /// reference.
    pub(crate) fn put_spatial_node(
        &self,
        transaction: Xid,
        index: &str,
        generation: u64,
        node: u64,
        bytes: &[u8],
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        self.store
            .put(
                transaction,
                &spatial_node_key(&index, generation, node),
                bytes,
            )
            .map_err(page_error)
    }

    pub(crate) fn spatial_node(
        &self,
        snapshot: &Snapshot,
        index: &str,
        generation: u64,
        node: u64,
    ) -> Result<Option<Vec<u8>>> {
        let index = self.resolve_index_name(index);
        self.store
            .get_as_of(snapshot, &spatial_node_key(&index, generation, node))
            .map_err(page_error)
    }

    /// Upsert one delta row for `pk` (value: spatial delta codec — tombstone
    /// or current entry). Written by the commit's paged apply, so the delta
    /// is exactly as durable and transactional as the row it shadows.
    pub(crate) fn put_spatial_delta(
        &self,
        transaction: Xid,
        index: &str,
        pk: &str,
        payload: &[u8],
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        self.store
            .put(transaction, &spatial_delta_key(&index, pk), payload)
            .map_err(page_error)
    }

    pub(crate) fn delete_spatial_delta(
        &self,
        transaction: Xid,
        index: &str,
        pk: &str,
    ) -> Result<bool> {
        let index = self.resolve_index_name(index);
        self.store
            .delete(transaction, &spatial_delta_key(&index, pk))
            .map_err(page_error)
    }

    /// Distinct packed-node generations present for `index` — the live one
    /// plus any retired or crash-orphaned side-builds awaiting GC.
    pub(crate) fn spatial_node_generations(
        &self,
        snapshot: &Snapshot,
        index: &str,
    ) -> Result<Vec<u64>> {
        let index = self.resolve_index_name(index);
        let mut prefix = ns_prefix(INDEX_NS_V13, &index);
        prefix.push(SPATIAL_SUBSPACE_NODE);
        let cursor = self
            .store
            .scan_keys_from(snapshot, &prefix)
            .map_err(page_error)?;
        let mut generations = Vec::new();
        for key in cursor {
            let key = key.map_err(page_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let Some(bytes) = key.get(prefix.len()..prefix.len() + 8) else {
                continue;
            };
            let generation = u64::from_be_bytes(bytes.try_into().expect("len 8"));
            if generations.last() != Some(&generation) {
                generations.push(generation);
            }
        }
        Ok(generations)
    }

    /// One delta row's current payload, by pk.
    pub(crate) fn spatial_delta_value(
        &self,
        snapshot: &Snapshot,
        index: &str,
        pk: &str,
    ) -> Result<Option<Vec<u8>>> {
        let index = self.resolve_index_name(index);
        self.store
            .get_as_of(snapshot, &spatial_delta_key(&index, pk))
            .map_err(page_error)
    }

    /// Every delta row of a packed spatial index: `(pk, payload)`.
    pub(crate) fn scan_spatial_delta<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
    ) -> Result<impl Iterator<Item = Result<(String, Vec<u8>)>> + 'store> {
        let index = self.resolve_index_name(index);
        let prefix = spatial_delta_prefix(&index);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let filter_prefix = prefix.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&filter_prefix),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let pk = String::from_utf8(key[prefix.len()..].to_vec()).map_err(|error| {
                    BicDbError::PagedStorage(format!("spatial delta key pk not UTF-8: {error}"))
                })?;
                Ok((pk, value))
            }))
    }

    /// Whether ANY block exists for `index` (one descent) — the signal that
    /// per-posting fast paths must defer to the block layer.
    pub fn index_has_posting_blocks(&self, snapshot: &Snapshot, index: &str) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let prefix = ns_prefix(INDEX_NS_V3, &index);
        let mut cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        match cursor.next() {
            Some(Ok((key, _))) => Ok(key.starts_with(&prefix)),
            Some(Err(error)) => Err(page_error(error)),
            None => Ok(false),
        }
    }

    /// Distinct encoded term keys present in the tombstone namespace.
    pub fn scan_index_tombstone_terms<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
    ) -> Result<impl Iterator<Item = Result<Vec<u8>>> + 'store> {
        let index_owned = self.resolve_index_name(index);
        let prefix = ns_prefix(INDEX_NS_V4, &index_owned);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let filter_prefix = prefix.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&filter_prefix),
                Err(_) => true,
            })
            .filter_map(move |entry| match entry {
                Ok((key, _)) => {
                    // Reuse the v1 decoder shape: body = escaped key ++ [0,0] ++ pk.
                    let body = &key[prefix.len()..];
                    let mut encoded = Vec::new();
                    let mut rest = body;
                    loop {
                        match rest {
                            [0, 0, ..] => return Some(Ok(encoded)),
                            [0, 0xFF, tail @ ..] => {
                                encoded.push(0);
                                rest = tail;
                            }
                            [byte, tail @ ..] => {
                                encoded.push(*byte);
                                rest = tail;
                            }
                            [] => {
                                return Some(Err(BicDbError::PagedStorage(format!(
                                    "tombstone key for `{index_owned}` missing terminator"
                                ))));
                            }
                        }
                    }
                }
                Err(error) => Some(Err(page_error(error))),
            }))
    }

    /// Every posting block of `index`: `(encoded_term_key, block_bytes)`.
    pub fn scan_all_posting_blocks<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
    ) -> Result<impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'store> {
        let index_owned = self.resolve_index_name(index);
        let prefix = ns_prefix(INDEX_NS_V3, &index_owned);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let filter_prefix = prefix.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&filter_prefix),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let body = &key[prefix.len()..];
                let mut encoded = Vec::new();
                let mut rest = body;
                loop {
                    match rest {
                        [0, 0, ..] => return Ok((encoded, value)),
                        [0, 0xFF, tail @ ..] => {
                            encoded.push(0);
                            rest = tail;
                        }
                        [byte, tail @ ..] => {
                            encoded.push(*byte);
                            rest = tail;
                        }
                        [] => {
                            return Err(BicDbError::PagedStorage(format!(
                                "block key for `{index_owned}` missing terminator"
                            )));
                        }
                    }
                }
            }))
    }

    /// Every posting block whose TERM's encoded key starts with
    /// `encoded_prefix`, in (term, last_pk) order:
    /// `(encoded term key, last_pk, block_bytes)`.
    ///
    /// The entry escape (0x00 -> 0x00 0xFF) preserves byte-prefix
    /// relationships, so an encoded-key prefix bound maps directly onto a key
    /// range — same property `scan_index_encoded_prefix` relies on. This is
    /// what serves `term:*` once a term's postings live in blocks and its v1
    /// tail is folded away.
    pub fn scan_posting_blocks_encoded_prefix<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_prefix: &[u8],
    ) -> Result<impl Iterator<Item = Result<(Vec<u8>, String, Vec<u8>)>> + 'store> {
        let index_owned = self.resolve_index_name(index);
        let prefix = ns_entry_prefix(INDEX_NS_V3, &index_owned, encoded_prefix);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let filter_prefix = prefix.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&filter_prefix),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let ns = ns_prefix(INDEX_NS_V3, &index_owned);
                let body = &key[ns.len()..];
                let mut encoded = Vec::new();
                let mut rest = body;
                loop {
                    match rest {
                        [0, 0, tail @ ..] => {
                            let last_pk = String::from_utf8(tail.to_vec()).map_err(|_| {
                                BicDbError::PagedStorage(format!(
                                    "block pk for `{index_owned}` is not utf-8"
                                ))
                            })?;
                            return Ok((encoded, last_pk, value));
                        }
                        [0, 0xFF, tail @ ..] => {
                            encoded.push(0);
                            rest = tail;
                        }
                        [byte, tail @ ..] => {
                            encoded.push(*byte);
                            rest = tail;
                        }
                        [] => {
                            return Err(BicDbError::PagedStorage(format!(
                                "block key for `{index_owned}` missing terminator"
                            )));
                        }
                    }
                }
            }))
    }

    /// DOC TERMS blob for `(index, pk)` — see `INDEX_NS_V6`.
    pub fn put_doc_terms(
        &self,
        transaction: Xid,
        index: &str,
        pk: &str,
        blob: &[u8],
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        let mut key = ns_prefix(INDEX_NS_V6, &index);
        key.extend_from_slice(pk.as_bytes());
        self.store.put(transaction, &key, blob).map_err(page_error)
    }

    pub fn get_doc_terms(
        &self,
        snapshot: &Snapshot,
        index: &str,
        pk: &str,
    ) -> Result<Option<Vec<u8>>> {
        let index = self.resolve_index_name(index);
        let mut key = ns_prefix(INDEX_NS_V6, &index);
        key.extend_from_slice(pk.as_bytes());
        self.store.get_as_of(snapshot, &key).map_err(page_error)
    }

    pub fn delete_doc_terms(&self, transaction: Xid, index: &str, pk: &str) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let mut key = ns_prefix(INDEX_NS_V6, &index);
        key.extend_from_slice(pk.as_bytes());
        self.store.delete(transaction, &key).map_err(page_error)
    }

    /// Every doc-terms blob of an index in ASCENDING pk order: `(pk, blob)`.
    /// This is the direct build's fast corpus source — compact blobs instead
    /// of full rows.
    pub fn scan_doc_terms<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
    ) -> Result<impl Iterator<Item = Result<(String, Vec<u8>)>> + 'store> {
        let index = self.resolve_index_name(index);
        let prefix = ns_prefix(INDEX_NS_V6, &index);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let filter_prefix = prefix.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&filter_prefix),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let pk = String::from_utf8(key[prefix.len()..].to_vec()).map_err(|_| {
                    BicDbError::PagedStorage("doc-terms pk is not utf-8".to_string())
                })?;
                Ok((pk, value))
            }))
    }

    /// [`Self::scan_doc_terms`] resumed strictly AFTER `after_pk` (chunked
    /// consumers drop the cursor between chunks so block writes can commit).
    pub fn scan_doc_terms_after<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        after_pk: &str,
    ) -> Result<impl Iterator<Item = Result<(String, Vec<u8>)>> + 'store> {
        let index = self.resolve_index_name(index);
        let prefix = ns_prefix(INDEX_NS_V6, &index);
        let mut start = prefix.clone();
        start.extend_from_slice(after_pk.as_bytes());
        start.push(0);
        let cursor = self.store.scan_from(snapshot, &start).map_err(page_error)?;
        let filter_prefix = prefix.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&filter_prefix),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let pk = String::from_utf8(key[prefix.len()..].to_vec()).map_err(|_| {
                    BicDbError::PagedStorage("doc-terms pk is not utf-8".to_string())
                })?;
                Ok((pk, value))
            }))
    }

    /// Whether an index has ANY doc-terms blob: one descent.
    pub fn index_has_doc_terms(&self, snapshot: &Snapshot, index: &str) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let prefix = ns_prefix(INDEX_NS_V6, &index);
        let mut cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        match cursor.next() {
            Some(Ok((key, _))) => Ok(key.starts_with(&prefix)),
            Some(Err(error)) => Err(page_error(error)),
            None => Ok(false),
        }
    }

    /// Impact-ordered block copy (v5): `seq` ascending = impact descending.
    pub fn put_impact_block(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        seq: u32,
        block: &[u8],
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        let mut key = ns_entry_prefix(INDEX_NS_V5, &index, encoded_key);
        key.extend_from_slice(&[0, 0]);
        key.extend_from_slice(&seq.to_be_bytes());
        self.store.put(transaction, &key, block).map_err(page_error)
    }

    pub fn delete_impact_block(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        seq: u32,
    ) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let mut key = ns_entry_prefix(INDEX_NS_V5, &index, encoded_key);
        key.extend_from_slice(&[0, 0]);
        key.extend_from_slice(&seq.to_be_bytes());
        self.store.delete(transaction, &key).map_err(page_error)
    }

    /// One term's impact blocks in DESCENDING max-impact order:
    /// `(seq, block_bytes)`.
    pub fn scan_impact_blocks<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
    ) -> Result<impl Iterator<Item = Result<(u32, Vec<u8>)>> + 'store> {
        let index = self.resolve_index_name(index);
        let mut prefix = ns_entry_prefix(INDEX_NS_V5, &index, encoded_key);
        prefix.extend_from_slice(&[0, 0]);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let filter_prefix = prefix.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&filter_prefix),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let tail = &key[prefix.len()..];
                if tail.len() != 4 {
                    return Err(BicDbError::PagedStorage(
                        "impact block key has no sequence".to_string(),
                    ));
                }
                Ok((u32::from_be_bytes(tail.try_into().unwrap()), value))
            }))
    }

    /// Numeric document-id map. A primary key is stored once per generation,
    /// never repeated in every term posting.
    pub fn put_full_text_document_id(
        &self,
        transaction: Xid,
        index: &str,
        document_id: u64,
        pk: &str,
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        let by_id = full_text_document_id_key(&index, document_id);
        let by_pk = full_text_document_pk_key(&index, pk);
        self.store
            .put(transaction, &by_id, pk.as_bytes())
            .map_err(page_error)?;
        self.store
            .put(transaction, &by_pk, &document_id.to_be_bytes())
            .map_err(page_error)
    }

    pub fn full_text_pk_for_document_id(
        &self,
        snapshot: &Snapshot,
        index: &str,
        document_id: u64,
    ) -> Result<Option<String>> {
        let index = self.resolve_index_name(index);
        let key = full_text_document_id_key(&index, document_id);
        self.store
            .get_as_of(snapshot, &key)
            .map_err(page_error)?
            .map(|value| {
                String::from_utf8(value).map_err(|error| {
                    BicDbError::PagedStorage(format!(
                        "full-text document id {document_id} has invalid UTF-8 pk: {error}"
                    ))
                })
            })
            .transpose()
    }

    pub fn full_text_document_id_for_pk(
        &self,
        snapshot: &Snapshot,
        index: &str,
        pk: &str,
    ) -> Result<Option<u64>> {
        let index = self.resolve_index_name(index);
        let key = full_text_document_pk_key(&index, pk);
        self.store
            .get_as_of(snapshot, &key)
            .map_err(page_error)?
            .map(|value| {
                value
                    .as_slice()
                    .try_into()
                    .map(u64::from_be_bytes)
                    .map_err(|_| {
                        BicDbError::PagedStorage(
                            "full-text pk map has invalid document id".to_string(),
                        )
                    })
            })
            .transpose()
    }

    pub fn put_full_text_document_statistics(
        &self,
        transaction: Xid,
        index: &str,
        document_id: u64,
        statistics: &crate::fts_format::FullTextDocumentStatistics,
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        let key = full_text_document_statistics_key(&index, document_id);
        let value = crate::fts_format::encode_document_statistics(statistics)?;
        self.store
            .put(transaction, &key, &value)
            .map_err(page_error)
    }

    pub fn full_text_document_statistics(
        &self,
        snapshot: &Snapshot,
        index: &str,
        document_id: u64,
    ) -> Result<Option<crate::fts_format::FullTextDocumentStatistics>> {
        let index = self.resolve_index_name(index);
        let key = full_text_document_statistics_key(&index, document_id);
        self.store
            .get_as_of(snapshot, &key)
            .map_err(page_error)?
            .as_deref()
            .map(crate::fts_format::decode_document_statistics)
            .transpose()
    }

    /// Store retrieval text in the index generation. The caller supplies a
    /// versioned compression envelope so this page layer remains codec-neutral.
    pub fn put_full_text_stored_text(
        &self,
        transaction: Xid,
        index: &str,
        document_id: u64,
        value: &[u8],
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        let key = full_text_stored_text_key(&index, document_id);
        self.store.put(transaction, &key, value).map_err(page_error)
    }

    pub fn full_text_stored_text(
        &self,
        snapshot: &Snapshot,
        index: &str,
        document_id: u64,
    ) -> Result<Option<Vec<u8>>> {
        let index = self.resolve_index_name(index);
        let key = full_text_stored_text_key(&index, document_id);
        self.store.get_as_of(snapshot, &key).map_err(page_error)
    }

    /// Scan one bounded page of decoded retrieval text directly from the
    /// physical generation's stored-text namespace.
    pub(crate) fn full_text_stored_text_page(
        &self,
        snapshot: &Snapshot,
        physical_index: &str,
        after_document_id: Option<u64>,
        limit: usize,
    ) -> Result<(Vec<(u64, Vec<u8>)>, Option<u64>)> {
        if !(1..=4_096).contains(&limit) {
            return Err(BicDbError::Index(format!(
                "full-text stored-text page limit must be between 1 and 4096; got {limit}"
            )));
        }
        let prefix = index_stored_text_prefix(physical_index);
        let mut start = prefix.clone();
        if let Some(document_id) = after_document_id {
            start.extend_from_slice(&document_id.to_be_bytes());
            start.push(0);
        }
        let mut cursor = self.store.scan_from(snapshot, &start).map_err(page_error)?;
        let mut rows = Vec::with_capacity(limit);
        let mut previous_document_id = after_document_id;
        let mut has_more = false;
        while let Some(entry) = cursor.next() {
            let (key, value) = entry.map_err(page_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let document_id = decode_full_text_stored_text_document_id(&prefix, &key)?;
            validate_full_text_stored_text_document_id(previous_document_id, document_id)?;
            previous_document_id = Some(document_id);
            if rows.len() == limit {
                has_more = true;
                break;
            }
            rows.push((document_id, crate::fts_format::decode_stored_text(&value)?));
        }
        let next_after_document_id =
            has_more.then(|| rows.last().expect("a full page has a final row").0);
        Ok((rows, next_after_document_id))
    }

    pub fn put_numeric_posting_block(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        last_document_id: u64,
        block: &[u8],
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        let key = ns_term_u64_key(INDEX_NS_V9, &index, encoded_key, last_document_id);
        self.store.put(transaction, &key, block).map_err(page_error)
    }

    pub fn delete_numeric_posting_block(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        last_document_id: u64,
    ) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let key = ns_term_u64_key(INDEX_NS_V9, &index, encoded_key, last_document_id);
        self.store.delete(transaction, &key).map_err(page_error)
    }

    pub fn scan_numeric_posting_blocks<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Result<(u64, Vec<u8>)>> + 'store>> {
        let index = self.resolve_index_name(index);
        if !self.term_in_paged_v9(snapshot, &index, encoded_key)? {
            if let Some(set) = self.fts_segment_for(&index)? {
                // Sub-segments cover disjoint ascending document ranges:
                // concatenation in sub order IS document order.
                let mut blocks = Vec::new();
                for reader in set.readers() {
                    if let Some(meta) = reader.term_meta(encoded_key)? {
                        blocks.extend(reader.pk_blocks_all(&meta)?);
                    }
                }
                return Ok(Box::new(blocks.into_iter().map(Ok)));
            }
        }
        let mut prefix = ns_entry_prefix(INDEX_NS_V9, &index, encoded_key);
        prefix.extend_from_slice(&[0, 0]);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let filter_prefix = prefix.clone();
        Ok(Box::new(
            cursor
                .take_while(move |entry| match entry {
                    Ok((key, _)) => key.starts_with(&filter_prefix),
                    Err(_) => true,
                })
                .map(move |entry| {
                    let (key, value) = entry.map_err(page_error)?;
                    let tail: [u8; 8] = key[prefix.len()..].try_into().map_err(|_| {
                        BicDbError::PagedStorage(
                            "numeric posting block key has invalid last document id".to_string(),
                        )
                    })?;
                    Ok((u64::from_be_bytes(tail), value))
                }),
        ))
    }

    pub fn scan_all_numeric_posting_blocks<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, u64, Vec<u8>)>> + 'store>> {
        self.scan_numeric_posting_blocks_encoded_prefix(snapshot, index, &[])
    }

    fn scan_all_numeric_posting_blocks_paged<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
    ) -> Result<impl Iterator<Item = Result<(Vec<u8>, u64, Vec<u8>)>> + 'store> {
        let index_owned = self.resolve_index_name(index);
        let prefix = ns_prefix(INDEX_NS_V9, &index_owned);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let filter_prefix = prefix.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&filter_prefix),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let (encoded_key, tail) =
                    decode_namespaced_term_key(&key[prefix.len()..], &index_owned)?;
                let last_document_id: [u8; 8] = tail.try_into().map_err(|_| {
                    BicDbError::PagedStorage(
                        "numeric posting block key has invalid last document id".to_string(),
                    )
                })?;
                Ok((encoded_key, u64::from_be_bytes(last_document_id), value))
            }))
    }

    pub fn scan_numeric_posting_blocks_encoded_prefix<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, u64, Vec<u8>)>> + 'store>> {
        let resolved = self.resolve_index_name(index);
        let paged =
            self.scan_numeric_posting_blocks_encoded_prefix_paged(snapshot, index, encoded_prefix)?;
        let Some(set) = self.fts_segment_for(&resolved)? else {
            return Ok(Box::new(paged));
        };
        let cursor = set.terms_from(encoded_prefix)?;
        Ok(Box::new(MergedTermBlockScan {
            paged: Box::new(paged),
            pending_paged: None,
            cursor,
            cursor_done: false,
            segment_pending: None,
            prefix: encoded_prefix.to_vec(),
            queued: std::collections::VecDeque::new(),
            failed: false,
        }))
    }

    fn scan_numeric_posting_blocks_encoded_prefix_paged<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_prefix: &[u8],
    ) -> Result<impl Iterator<Item = Result<(Vec<u8>, u64, Vec<u8>)>> + 'store> {
        let index_owned = self.resolve_index_name(index);
        let prefix = ns_entry_prefix(INDEX_NS_V9, &index_owned, encoded_prefix);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let filter_prefix = prefix.clone();
        let namespace = ns_prefix(INDEX_NS_V9, &index_owned);
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&filter_prefix),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let (encoded_key, tail) =
                    decode_namespaced_term_key(&key[namespace.len()..], &index_owned)?;
                let last_document_id: [u8; 8] = tail.try_into().map_err(|_| {
                    BicDbError::PagedStorage(
                        "numeric posting block key has invalid last document id".to_string(),
                    )
                })?;
                Ok((encoded_key, u64::from_be_bytes(last_document_id), value))
            }))
    }

    pub fn numeric_posting_block_for(
        &self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
        document_id: u64,
    ) -> Result<Option<Vec<u8>>> {
        Ok(self
            .numeric_posting_block_for_with_key(snapshot, index, encoded_key, document_id)?
            .map(|(_, value)| value))
    }

    pub(crate) fn numeric_posting_block_for_with_key(
        &self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
        document_id: u64,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        Ok(self
            .numeric_posting_blocks_for_with_key(snapshot, index, encoded_key, document_id, 1)?
            .into_iter()
            .next())
    }

    /// Fetch the first numeric posting block whose upper document-id bound
    /// covers `document_id`, plus a bounded number of adjacent blocks. One
    /// range descent serves the whole batch; ranked intersections can consume
    /// sequential driver blocks without re-descending the page-store B-tree.
    pub(crate) fn numeric_posting_blocks_for_with_key(
        &self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
        document_id: u64,
        max_blocks: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        Ok(self
            .numeric_posting_fetch_for_with_key(
                snapshot,
                index,
                encoded_key,
                document_id,
                max_blocks,
            )?
            .into_iter()
            .map(|fetched| fetched.into_v1())
            .collect::<Result<Vec<_>>>()?)
    }

    /// Batch fetch preserving the native representation: paged rows carry v1
    /// block bytes; segment terms carry the stored slim bytes plus a shared
    /// docs table and (for multi-block terms) the v1 header triple from the
    /// term directory. The ranked path consumes this directly — no v1 bytes
    /// are materialized on it. `into_v1` is the compatibility bridge for
    /// everything else, and doubles as the equivalence oracle.
    pub(crate) fn numeric_posting_fetch_for_with_key(
        &self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
        document_id: u64,
        max_blocks: usize,
    ) -> Result<Vec<FetchedPostingBlock>> {
        let index = self.resolve_index_name(index);
        // Overlay: a folded term lives in the paged keyspace and supersedes
        // the immutable segment; an unfolded term is served from the segment.
        if !self.term_in_paged_v9(snapshot, &index, encoded_key)? {
            if let Some(set) = self.fts_segment_for(&index)? {
                let mut fetched: Vec<FetchedPostingBlock> = Vec::new();
                let budget = max_blocks.max(1);
                for reader in set.readers() {
                    if fetched.len() >= budget {
                        break;
                    }
                    let Some(meta) = reader.term_meta(encoded_key)? else {
                        continue;
                    };
                    if meta.last_doc_id < document_id {
                        continue;
                    }
                    let docs = reader.docs_handle();
                    fetched.extend(
                        reader
                            .pk_slim_blocks_from(&meta, document_id, budget - fetched.len())?
                            .into_iter()
                            .map(|(last_document_id, bytes, max_term_frequency, header)| {
                                FetchedPostingBlock {
                                    last_document_id,
                                    bytes,
                                    slim: Some(SlimBlockContext {
                                        docs: Arc::clone(&docs),
                                        max_term_frequency,
                                        header,
                                    }),
                                }
                            }),
                    );
                }
                return Ok(fetched);
            }
        }
        let mut prefix = ns_entry_prefix(INDEX_NS_V9, &index, encoded_key);
        prefix.extend_from_slice(&[0, 0]);
        let mut start = prefix.clone();
        start.extend_from_slice(&document_id.to_be_bytes());
        let mut cursor = self.store.scan_from(snapshot, &start).map_err(page_error)?;
        let mut blocks: Vec<FetchedPostingBlock> = Vec::with_capacity(max_blocks.max(1));
        while blocks.len() < max_blocks.max(1) {
            match cursor.next() {
                Some(Ok((key, value))) if key.starts_with(&prefix) => {
                    let last_document_id = key
                        .get(prefix.len()..)
                        .and_then(|tail| <[u8; 8]>::try_from(tail).ok())
                        .map(u64::from_be_bytes)
                        .ok_or_else(|| {
                            BicDbError::PagedStorage(
                                "numeric posting block key has invalid last document id"
                                    .to_string(),
                            )
                        })?;
                    blocks.push(FetchedPostingBlock {
                        last_document_id,
                        bytes: value,
                        slim: None,
                    });
                }
                Some(Ok(_)) | None => break,
                Some(Err(error)) => return Err(page_error(error)),
            }
        }
        Ok(blocks)
    }

    /// Read only the document-id boundaries of adjacent numeric posting
    /// blocks. The boundary is encoded in the B-tree key and the rank ceiling
    /// fits in a fixed 64-byte value prefix. Ranked retrieval uses this as a
    /// Tantivy-style skip stream and materializes a posting payload only when a
    /// candidate actually reaches that block.
    pub(crate) fn numeric_posting_block_boundaries_for_with_key(
        &self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
        document_id: u64,
        max_blocks: usize,
    ) -> Result<Vec<(u64, u32)>> {
        let index = self.resolve_index_name(index);
        if !self.term_in_paged_v9(snapshot, &index, encoded_key)? {
            if let Some(set) = self.fts_segment_for(&index)? {
                let mut boundaries: Vec<(u64, u32)> = Vec::new();
                let budget = max_blocks.max(1);
                for reader in set.readers() {
                    if boundaries.len() >= budget {
                        break;
                    }
                    let Some(meta) = reader.term_meta(encoded_key)? else {
                        continue;
                    };
                    if meta.last_doc_id < document_id {
                        continue;
                    }
                    boundaries.extend(reader.pk_boundaries_from(
                        &meta,
                        document_id,
                        budget - boundaries.len(),
                    ));
                }
                crate::fts_format::record_posting_block_boundaries(boundaries.len());
                return Ok(boundaries);
            }
        }
        let mut prefix = ns_entry_prefix(INDEX_NS_V9, &index, encoded_key);
        prefix.extend_from_slice(&[0, 0]);
        let mut start = prefix.clone();
        start.extend_from_slice(&document_id.to_be_bytes());
        let mut cursor = self
            .store
            .scan_visible_key_prefixes_from(snapshot, &start, 64)
            .map_err(page_error)?;
        let mut boundaries = Vec::with_capacity(max_blocks.max(1));
        while boundaries.len() < max_blocks.max(1) {
            match cursor.next() {
                Some(Ok((key, value_prefix))) if key.starts_with(&prefix) => {
                    let last_document_id = key
                        .get(prefix.len()..)
                        .and_then(|tail| <[u8; 8]>::try_from(tail).ok())
                        .map(u64::from_be_bytes)
                        .ok_or_else(|| {
                            BicDbError::PagedStorage(
                                "numeric posting block key has invalid last document id"
                                    .to_string(),
                            )
                        })?;
                    let (max_term_frequency, _) =
                        numeric_posting_block_rank_metadata(&value_prefix).ok_or_else(|| {
                            BicDbError::PagedStorage(
                                "corrupt numeric posting block rank header".to_string(),
                            )
                        })?;
                    boundaries.push((last_document_id, max_term_frequency));
                }
                Some(Ok(_)) | None => break,
                Some(Err(error)) => return Err(page_error(error)),
            }
        }
        crate::fts_format::record_posting_block_boundaries(boundaries.len());
        Ok(boundaries)
    }

    pub fn put_numeric_impact_block(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        seq: u32,
        block: &[u8],
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        let mut key = ns_entry_prefix(INDEX_NS_V10, &index, encoded_key);
        key.extend_from_slice(&[0, 0]);
        key.extend_from_slice(&seq.to_be_bytes());
        self.store.put(transaction, &key, block).map_err(page_error)
    }

    pub fn delete_numeric_impact_block(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        seq: u32,
    ) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let mut key = ns_entry_prefix(INDEX_NS_V10, &index, encoded_key);
        key.extend_from_slice(&[0, 0]);
        key.extend_from_slice(&seq.to_be_bytes());
        self.store.delete(transaction, &key).map_err(page_error)
    }

    pub fn scan_numeric_impact_blocks<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Result<(u32, Vec<u8>)>> + 'store>> {
        let index = self.resolve_index_name(index);
        if !self.term_in_paged_v9(snapshot, &index, encoded_key)? {
            if let Some(set) = self.fts_segment_for(&index)? {
                // Each sub's sidecar is already ordered by descending block
                // max impact; a k-way merge on the header bound restores one
                // globally non-increasing stream, which is the ordering the
                // score-first gate depends on. Sequence numbers are
                // reassigned over the merged order.
                let mut streams = Vec::new();
                for reader in set.readers() {
                    if let Some(meta) = reader.term_meta(encoded_key)? {
                        streams.push(reader.impact_blocks(meta)?);
                    }
                }
                let mut heads: Vec<Option<(u16, Vec<u8>)>> = Vec::new();
                for stream in &mut streams {
                    heads.push(match stream.next().transpose()? {
                        Some((_, bytes)) => {
                            let bound = compact_impact_block_header(&bytes)
                                .map(|header| header.max_impact)
                                .unwrap_or(0);
                            Some((bound, bytes))
                        }
                        None => None,
                    });
                }
                let mut sequence = 0u32;
                let mut pending_error = None;
                return Ok(Box::new(std::iter::from_fn(move || {
                    if let Some(error) = pending_error.take() {
                        return Some(Err(error));
                    }
                    let best = (0..heads.len())
                        .filter(|slot| heads[*slot].is_some())
                        .max_by_key(|slot| heads[*slot].as_ref().expect("some").0)?;
                    let (_, bytes) = heads[best].take().expect("some");
                    match streams[best].next() {
                        Some(Ok((_, next_bytes))) => {
                            let bound = compact_impact_block_header(&next_bytes)
                                .map(|header| header.max_impact)
                                .unwrap_or(0);
                            heads[best] = Some((bound, next_bytes));
                        }
                        Some(Err(error)) => pending_error = Some(error),
                        None => {}
                    }
                    let current = sequence;
                    sequence = sequence.saturating_add(1);
                    Some(Ok((current, bytes)))
                })));
            }
        }
        let mut prefix = ns_entry_prefix(INDEX_NS_V10, &index, encoded_key);
        prefix.extend_from_slice(&[0, 0]);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let filter_prefix = prefix.clone();
        Ok(Box::new(
            cursor
                .take_while(move |entry| match entry {
                    Ok((key, _)) => key.starts_with(&filter_prefix),
                    Err(_) => true,
                })
                .map(move |entry| {
                    let (key, value) = entry.map_err(page_error)?;
                    let tail: [u8; 4] = key[prefix.len()..].try_into().map_err(|_| {
                        BicDbError::PagedStorage(
                            "numeric impact block key has invalid sequence".to_string(),
                        )
                    })?;
                    Ok((u32::from_be_bytes(tail), value))
                }),
        ))
    }

    pub fn index_has_numeric_posting_blocks(
        &self,
        snapshot: &Snapshot,
        index: &str,
    ) -> Result<bool> {
        let index = self.resolve_index_name(index);
        if self.fts_segment_for(&index)?.is_some() {
            return Ok(true);
        }
        let prefix = ns_prefix(INDEX_NS_V9, &index);
        let mut cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        match cursor.next() {
            Some(Ok((key, _))) => Ok(key.starts_with(&prefix)),
            Some(Err(error)) => Err(page_error(error)),
            None => Ok(false),
        }
    }

    pub fn index_has_any_posting_blocks(&self, snapshot: &Snapshot, index: &str) -> Result<bool> {
        Ok(self.index_has_posting_blocks(snapshot, index)?
            || self.index_has_numeric_posting_blocks(snapshot, index)?)
    }

    /// Store the compact planner-facing dictionary record for one term.
    pub fn put_full_text_term_statistics(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        statistics: &crate::fts_format::FullTextTermStatistics,
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        let key = full_text_dictionary_term_key(&index, encoded_key);
        let value = crate::fts_format::encode_term_statistics(statistics)?;
        self.store
            .put(transaction, &key, &value)
            .map_err(page_error)
    }

    pub fn delete_full_text_term_statistics(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
    ) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let key = full_text_dictionary_term_key(&index, encoded_key);
        self.store.delete(transaction, &key).map_err(page_error)
    }

    /// One O(log pages) dictionary lookup. No posting key or block is read.
    pub fn full_text_term_statistics(
        &self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
    ) -> Result<Option<crate::fts_format::FullTextTermStatistics>> {
        let index = self.resolve_index_name(index);
        let key = full_text_dictionary_term_key(&index, encoded_key);
        let value = self.store.get_as_of(snapshot, &key).map_err(page_error)?;
        let mut decoded = value
            .as_deref()
            .map(crate::fts_format::decode_term_statistics)
            .transpose()?;
        if decoded.is_none() {
            // A fold writes the term's dictionary row alongside its blocks,
            // so a paged miss means the segment is authoritative for it.
            if let Some(set) = self.fts_segment_for(&index)? {
                decoded = set.term_statistics(encoded_key)?;
            }
        }
        crate::fts_format::record_dictionary_lookup(decoded.is_some());
        Ok(decoded)
    }

    pub fn put_full_text_collection_statistics(
        &self,
        transaction: Xid,
        index: &str,
        statistics: &crate::fts_format::FullTextCollectionStatistics,
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        let key = full_text_dictionary_collection_key(&index);
        let value = crate::fts_format::encode_collection_statistics(statistics)?;
        self.store
            .put(transaction, &key, &value)
            .map_err(page_error)
    }

    pub fn full_text_collection_statistics(
        &self,
        snapshot: &Snapshot,
        index: &str,
    ) -> Result<Option<crate::fts_format::FullTextCollectionStatistics>> {
        let index = self.resolve_index_name(index);
        let key = full_text_dictionary_collection_key(&index);
        self.store
            .get_as_of(snapshot, &key)
            .map_err(page_error)?
            .as_deref()
            .map(crate::fts_format::decode_collection_statistics)
            .transpose()
    }

    pub fn index_has_full_text_dictionary(&self, snapshot: &Snapshot, index: &str) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let prefix = full_text_dictionary_prefix(&index);
        let mut cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        match cursor.next() {
            Some(Ok((key, _))) => Ok(key.starts_with(&prefix)),
            Some(Err(error)) => Err(page_error(error)),
            None => Ok(false),
        }
    }

    /// Tombstones: postings deleted after their term was folded into blocks.
    pub fn put_posting_tombstone(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        pk: &str,
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        let key = ns_term_key(INDEX_NS_V4, &index, encoded_key, pk);
        self.store.put(transaction, &key, &[]).map_err(page_error)
    }

    pub fn delete_posting_tombstone(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        pk: &str,
    ) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let key = ns_term_key(INDEX_NS_V4, &index, encoded_key, pk);
        self.store.delete(transaction, &key).map_err(page_error)
    }

    pub fn posting_tombstone_exists(
        &self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
        pk: &str,
    ) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let key = ns_term_key(INDEX_NS_V4, &index, encoded_key, pk);
        Ok(self
            .store
            .get_as_of(snapshot, &key)
            .map_err(page_error)?
            .is_some())
    }

    /// All tombstoned pks of one term.
    pub fn scan_posting_tombstones<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
    ) -> Result<impl Iterator<Item = Result<String>> + 'store> {
        let index = self.resolve_index_name(index);
        let mut prefix = ns_entry_prefix(INDEX_NS_V4, &index, encoded_key);
        prefix.extend_from_slice(&[0, 0]);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let filter_prefix = prefix.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&filter_prefix),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, _) = entry.map_err(page_error)?;
                String::from_utf8(key[prefix.len()..].to_vec()).map_err(|error| {
                    BicDbError::PagedStorage(format!("tombstone pk not UTF-8: {error}"))
                })
            }))
    }

    /// The v2 COMPLETENESS SENTINEL: written once by a full backfill, its
    /// key sorts before every real v2 entry (real entries start with an
    /// encoded-key tag byte >= 1). Early termination is sound only when the
    /// v2 ordering covers every document; incremental writes on a
    /// pre-sentinel index add v2 twins but must not enable it.
    pub fn put_index_v2_sentinel(&self, transaction: Xid, index: &str) -> Result<()> {
        let index = self.resolve_index_name(index);
        let mut key = index_prefix_v2(&index);
        key.push(0);
        self.store.put(transaction, &key, &[]).map_err(page_error)
    }

    pub fn index_v2_complete(&self, snapshot: &Snapshot, index: &str) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let mut key = index_prefix_v2(&index);
        key.push(0);
        Ok(self
            .store
            .get_as_of(snapshot, &key)
            .map_err(page_error)?
            .is_some())
    }

    /// Every key under a raw prefix, dead versions included — namespace
    /// purges (DROP INDEX, backfill unwind) delete by key and a delete on a
    /// dead key is a no-op.
    pub(crate) fn keys_with_raw_prefix(
        &self,
        snapshot: &Snapshot,
        prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>> {
        let cursor = self
            .store
            .scan_keys_from(snapshot, prefix)
            .map_err(page_error)?;
        let mut keys = Vec::new();
        for key in cursor {
            let key = key.map_err(page_error)?;
            if !key.starts_with(prefix) {
                break;
            }
            keys.push(key);
        }
        Ok(keys)
    }

    /// Return a bounded page of keys under a raw namespace prefix.
    ///
    /// `after` is an exclusive raw-key cursor. It lets namespace cleanup
    /// release each page before opening the delete transaction and still
    /// make progress when the page store exposes dead historical keys.
    pub(crate) fn raw_keys_with_prefix_batch_after(
        &self,
        snapshot: &Snapshot,
        prefix: &[u8],
        after: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<Vec<u8>>> {
        if max_entries == 0 || max_bytes == 0 {
            return Ok(Vec::new());
        }
        let start = after.unwrap_or(prefix);
        let cursor = self
            .store
            .scan_keys_from(snapshot, start)
            .map_err(page_error)?;
        let mut keys = Vec::new();
        let mut bytes = 0usize;
        for key in cursor {
            let key = key.map_err(page_error)?;
            if !key.starts_with(prefix) {
                break;
            }
            if after.is_some_and(|after| key.as_slice() <= after) {
                continue;
            }
            let would_exceed_bytes = bytes.saturating_add(key.len()) > max_bytes;
            if !keys.is_empty() && (keys.len() >= max_entries || would_exceed_bytes) {
                break;
            }
            bytes = bytes.saturating_add(key.len());
            keys.push(key);
        }
        Ok(keys)
    }

    pub(crate) fn delete_raw(&self, transaction: Xid, key: &[u8]) -> Result<bool> {
        self.store.delete(transaction, key).map_err(page_error)
    }

    /// Where packed FTS segments live: `<paged>/fts-segments/<physical>/`.
    pub(crate) fn fts_segments_root(&self) -> &Path {
        &self.fts_segments_root
    }

    /// The open segment SET for a RESOLVED physical index name, opening
    /// lazily. A finished build is a set of one; a progressive build is its
    /// published sub-segments, each covering a disjoint ascending document
    /// range — which is why everything composes by concatenation. A clean
    /// miss is cached; an open failure is returned every time, so a torn
    /// segment is loud rather than silently absent.
    pub(crate) fn fts_segment_for(
        &self,
        physical: &str,
    ) -> Result<Option<Arc<crate::fts_segment::SegmentSet>>> {
        if let Some(slot) = self.fts_segments.read().get(physical) {
            return Ok(slot.clone());
        }
        let opened =
            crate::fts_segment::SegmentSet::open(&self.fts_segments_root, physical)?.map(Arc::new);
        self.fts_segments
            .write()
            .insert(physical.to_string(), opened.clone());
        Ok(opened)
    }

    /// (Re)open a just-published segment (or sub-segment set), replacing any
    /// cached state.
    pub(crate) fn register_fts_segment(&self, physical: &str) -> Result<()> {
        let opened =
            crate::fts_segment::SegmentSet::open(&self.fts_segments_root, physical)?.map(Arc::new);
        if opened.is_none() {
            return Err(page_error_from_message(format!(
                "published segment `{physical}` is not readable"
            )));
        }
        self.fts_segments
            .write()
            .insert(physical.to_string(), opened);
        Ok(())
    }

    /// Forget and delete a physical index's segment. Readers holding the old
    /// `Arc` keep their open file handles across the unlink.
    pub(crate) fn drop_fts_segment(&self, physical: &str) {
        self.fts_segments.write().remove(physical);
        crate::fts_segment::remove_segment(&self.fts_segments_root, physical);
    }

    /// THE OVERLAY RULE, per term: a fold rewrites a term's pk blocks, impact
    /// blocks and dictionary row into the paged keyspace together, and from
    /// then on the paged copy supersedes the immutable segment for that term.
    /// This probe decides which side serves, snapshot-consistently — the
    /// paged check runs under the caller's snapshot, so a query that began
    /// before a fold committed keeps reading the pre-fold state.
    fn term_in_paged_v9(
        &self,
        snapshot: &Snapshot,
        resolved_index: &str,
        encoded_key: &[u8],
    ) -> Result<bool> {
        let mut prefix = ns_entry_prefix(INDEX_NS_V9, resolved_index, encoded_key);
        prefix.extend_from_slice(&[0, 0]);
        let mut cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        match cursor.next() {
            Some(Ok((key, _))) => Ok(key.starts_with(&prefix)),
            Some(Err(error)) => Err(page_error(error)),
            None => Ok(false),
        }
    }

    pub(crate) fn raw_prefix_accounting(
        &self,
        snapshot: &Snapshot,
        prefix: &[u8],
    ) -> Result<crate::fts_format::StorageNamespaceAccounting> {
        let cursor = self.store.scan_from(snapshot, prefix).map_err(page_error)?;
        let mut accounting = crate::fts_format::StorageNamespaceAccounting::default();
        for entry in cursor {
            let (key, value) = entry.map_err(page_error)?;
            if !key.starts_with(prefix) {
                break;
            }
            accounting.add(key.len(), value.len());
        }
        Ok(accounting)
    }

    /// Decompose the KEY bytes of a namespace into their structural parts.
    ///
    /// Every FTS key is laid out as
    /// `[3 ns][2 len][index name][escaped term][0x00 0x00][8 suffix]`, so the
    /// only variable part is the term. Attributing the rest is arithmetic, and
    /// it answers the question that matters: **how much of each key is already
    /// implied by the tree it lives in?**
    pub(crate) fn raw_prefix_key_forensics(
        &self,
        snapshot: &Snapshot,
        prefix: &[u8],
        index_name_len: usize,
        has_term_and_suffix: bool,
    ) -> Result<crate::fts_format::KeyForensics> {
        let cursor = self.store.scan_from(snapshot, prefix).map_err(page_error)?;
        let mut out = crate::fts_format::KeyForensics::default();
        // Fixed cost per key, in the order the encoder writes it.
        let namespace = 3usize;
        let length_prefix = 2usize;
        let framing = if has_term_and_suffix { 2usize } else { 0 };
        let suffix = if has_term_and_suffix { 8usize } else { 0 };
        for entry in cursor {
            let (key, _value) = entry.map_err(page_error)?;
            if !key.starts_with(prefix) {
                break;
            }
            out.entries += 1;
            out.total_bytes += key.len() as u64;
            out.namespace_bytes += namespace as u64;
            out.length_prefix_bytes += length_prefix as u64;
            out.index_name_bytes += index_name_len as u64;
            out.framing_bytes += framing as u64;
            out.suffix_bytes += suffix as u64;
            let fixed = namespace + length_prefix + index_name_len + framing + suffix;
            let variable = key.len().saturating_sub(fixed);
            out.term_bytes += variable as u64;
            // Escapes are the 0x00 -> 0x00 0xFF expansion inside the term.
            let body_start = (namespace + length_prefix + index_name_len).min(key.len());
            let body_end = key.len().saturating_sub(framing + suffix).max(body_start);
            out.escape_bytes += key[body_start..body_end]
                .iter()
                .filter(|byte| **byte == 0xFF)
                .count() as u64;
        }
        Ok(out)
    }

    /// Whether `index` has ANY durable entry: one descent, first entry only.
    /// This is the pre-upgrade probe — a database indexed before durable
    /// entries existed has none, and must rebuild from rows instead of
    /// serving an empty index.
    pub fn index_has_entries(&self, snapshot: &Snapshot, index: &str) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let prefix = index_prefix(&index);
        let mut cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        match cursor.next() {
            Some(Ok((key, _))) => Ok(key.starts_with(&prefix)),
            Some(Err(error)) => Err(page_error(error)),
            None => Ok(false),
        }
    }
}

fn decode_record_values_parallel(rows: &[(Vec<u8>, Vec<u8>)]) -> Result<Vec<Record>> {
    if rows.len() < 256 {
        return rows.iter().map(|(_, value)| decode_record(value)).collect();
    }
    let workers = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(16)
        .min(rows.len());
    let chunk_size = rows.len().div_ceil(workers);
    std::thread::scope(|scope| {
        let handles = rows
            .chunks(chunk_size)
            .map(|chunk| {
                scope.spawn(move || {
                    chunk
                        .iter()
                        .map(|(_, value)| decode_record(value))
                        .collect::<Result<Vec<_>>>()
                })
            })
            .collect::<Vec<_>>();
        let mut records = Vec::with_capacity(rows.len());
        for handle in handles {
            records.extend(handle.join().map_err(|_| {
                BicDbError::PagedStorage("parallel record decoder panicked".to_string())
            })??);
        }
        Ok(records)
    })
}

/// Load the database-level generation catalog when `PagedRecords` is opened
/// directly by integrity/recovery tooling rather than through `BicDb`. This is
/// read-only, bounded, and no-follow; absence is the legacy no-alias format.
fn load_nearby_index_aliases(paged_dir: &Path) -> Result<BTreeMap<String, String>> {
    const MAX_ALIAS_CATALOG_BYTES: u64 = 16 * 1024 * 1024;
    #[derive(serde::Deserialize)]
    struct Catalog {
        #[serde(default)]
        indexes: BTreeMap<String, String>,
    }
    let Some(root) = paged_dir.parent() else {
        return Ok(BTreeMap::new());
    };
    let path = root.join("fts-index-generations.json");
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > MAX_ALIAS_CATALOG_BYTES
    {
        return Err(BicDbError::PagedStorage(
            "index generation catalog is unsafe or outside its bound".to_string(),
        ));
    }
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)?
    };
    #[cfg(not(unix))]
    let file = fs::File::open(&path)?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_ALIAS_CATALOG_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != metadata.len() || bytes.len() as u64 > MAX_ALIAS_CATALOG_BYTES {
        return Err(BicDbError::PagedStorage(
            "index generation catalog changed or grew while reading".to_string(),
        ));
    }
    let catalog: Catalog = serde_json::from_slice(&bytes)?;
    Ok(catalog.indexes)
}

/// Every raw key-range prefix an index owns in the durable keyspace: v1
/// entries, v2 impact twins (the sentinel shares the v2 prefix), v3 posting
/// blocks, v4 tombstones, v5 impact blocks. DROP INDEX must clear all of
/// them — a leaked block namespace poisons a later same-name CREATE, whose
/// reads would merge the dead index's postings.
pub(crate) fn index_purge_prefixes(index: &str) -> Vec<Vec<u8>> {
    vec![
        index_prefix(index),
        index_prefix_v2(index),
        ns_prefix(INDEX_NS_V3, index),
        ns_prefix(INDEX_NS_V4, index),
        ns_prefix(INDEX_NS_V5, index),
        ns_prefix(INDEX_NS_V6, index),
        ns_prefix(INDEX_NS_V7, index),
        ns_prefix(INDEX_NS_V8, index),
        ns_prefix(INDEX_NS_V9, index),
        ns_prefix(INDEX_NS_V10, index),
        ns_prefix(INDEX_NS_V11, index),
        ns_prefix(INDEX_NS_V12, index),
        ns_prefix(INDEX_NS_V13, index),
    ]
}

/// The block namespaces only (v3 + v5) — what a direct backfill owns and what
/// its unwind clears.
pub(crate) fn index_block_prefixes(index: &str) -> Vec<Vec<u8>> {
    vec![ns_prefix(INDEX_NS_V3, index), ns_prefix(INDEX_NS_V5, index)]
}

pub(crate) fn index_posting_block_prefix(index: &str) -> Vec<u8> {
    ns_prefix(INDEX_NS_V3, index)
}

pub(crate) fn index_impact_block_prefix(index: &str) -> Vec<u8> {
    ns_prefix(INDEX_NS_V5, index)
}

pub(crate) fn index_full_text_dictionary_prefix(index: &str) -> Vec<u8> {
    ns_prefix(INDEX_NS_V7, index)
}

pub(crate) fn index_numeric_posting_block_prefix(index: &str) -> Vec<u8> {
    ns_prefix(INDEX_NS_V9, index)
}

pub(crate) fn index_numeric_impact_block_prefix(index: &str) -> Vec<u8> {
    ns_prefix(INDEX_NS_V10, index)
}

pub(crate) fn index_document_terms_prefix(index: &str) -> Vec<u8> {
    ns_prefix(INDEX_NS_V6, index)
}

pub(crate) fn index_document_ids_prefix(index: &str) -> Vec<u8> {
    ns_prefix(INDEX_NS_V8, index)
}

pub(crate) fn index_document_statistics_prefix(index: &str) -> Vec<u8> {
    ns_prefix(INDEX_NS_V11, index)
}

pub(crate) fn index_stored_text_prefix(index: &str) -> Vec<u8> {
    ns_prefix(INDEX_NS_V12, index)
}

pub(crate) fn index_tail_prefix(index: &str) -> Vec<u8> {
    index_prefix(index)
}

/// The whole packed-spatial namespace of one index (meta + every node
/// generation + delta tail) — what a create-time clean slate purges.
pub(crate) fn index_spatial_prefix(index: &str) -> Vec<u8> {
    ns_prefix(INDEX_NS_V13, index)
}

pub(crate) fn index_impact_tail_prefix(index: &str) -> Vec<u8> {
    index_prefix_v2(index)
}

pub(crate) fn index_tombstone_prefix(index: &str) -> Vec<u8> {
    ns_prefix(INDEX_NS_V4, index)
}

pub(crate) fn collection_storage_prefix(collection: &str) -> Vec<u8> {
    collection_prefix(collection)
}

pub(crate) fn collection_intern_prefix(collection: &str) -> Vec<u8> {
    ns_prefix(INTERN_NS, collection)
}

/// Reserved first bytes of every index-entry key. A record key begins with its
/// collection name's length as a non-zero big-endian `u16`, so `[0, 0]` can
/// never collide with one — which is what lets both keyspaces share a tree
/// (and therefore a WAL, a transaction, and a recovery pass) safely.
const INDEX_NS: [u8; 3] = [0, 0, 1];
/// v2 inverted-index namespace: IMPACT-ORDERED full-text entries. Layout:
/// `[prefix2][escaped encoded_key][0x00 0x00][255 - impact_bucket][pk]` —
/// the inverted bucket byte makes an ascending key scan visit postings in
/// DESCENDING impact order, which is what lets a ranked top-k stop early.
/// v1 entries ([0,0,1]) remain readable forever; an index serves v2 when it
/// has any v2 entry (new/reindexed indexes write only v2).
const INDEX_NS_V2: [u8; 3] = [0, 0, 2];
/// v3 namespace: BLOCK postings — the folded, compressed base of a term's
/// posting list. Key: `[prefix3][escaped term key][0x00 0x00][last pk]`;
/// keying each block by its LAST pk makes "which block contains pk" a single
/// `first_at_or_after` descent. Value: [`encode_posting_block`]. The
/// per-posting v1/v2 entries remain the transactional tail; a fold merges
/// tail into blocks atomically per term.
const INDEX_NS_V3: [u8; 3] = [0, 0, 3];
/// v4 namespace: TOMBSTONES for postings deleted after folding (the block
/// still lists them). Key: `[prefix4][escaped term key][0x00 0x00][pk]`,
/// empty value. Consumed (deleted) by the next fold of the term.
const INDEX_NS_V4: [u8; 3] = [0, 0, 4];
/// v5 namespace: the IMPACT-ORDERED copy of a term's folded blocks. Same
/// block encoding as v3, but postings are sorted (impact bucket DESC, pk)
/// before re-blocking and keys carry a big-endian sequence number, so an
/// ascending scan yields blocks in DESCENDING max-impact order — which is
/// what makes block-max termination actually skip (pk-ordered blocks
/// interleave impact classes and every block max ties).
const INDEX_NS_V5: [u8; 3] = [0, 0, 5];
/// DOC TERMS (v6): per-document packed term/position blobs for a full-text
/// index: `[prefix6][index][pk] -> blob`. This is the reverse mapping the
/// UPDATE/DELETE diff needs (doc -> terms), stored ONCE in the index keyspace
/// instead of as a JSON lexeme map inside every row's metadata — which is
/// what let CREATE INDEX stop rewriting (and bloating) every row it indexes.
const INDEX_NS_V6: [u8; 3] = [0, 0, 6];
/// Compact term dictionary and collection statistics. Dictionary keys are one
/// record per term rather than one key per posting, so query planning performs
/// one point lookup regardless of document frequency.
const INDEX_NS_V7: [u8; 3] = [0, 0, 7];
/// Bidirectional dense document-id map. Subspace 0 is doc-id -> pk; subspace
/// 1 is pk -> doc-id.
const INDEX_NS_V8: [u8; 3] = [0, 0, 8];
/// Numeric, document-id-ordered posting blocks.
const INDEX_NS_V9: [u8; 3] = [0, 0, 9];
/// Numeric, impact-ordered posting blocks.
const INDEX_NS_V10: [u8; 3] = [0, 0, 10];
/// Per-document field lengths for BM25/BM25F normalization.
const INDEX_NS_V11: [u8; 3] = [0, 0, 11];
/// Optional compressed retrieval/source text, keyed once per numeric document.
const INDEX_NS_V12: [u8; 3] = [0, 0, 12];
/// Packed spatial index namespace: the bulk-packed immutable R-tree base plus
/// its transactional delta tail (the FTS "folded blocks + tail" model applied
/// to spatial). Subspaces after the index-name prefix:
/// - `[0]` — meta: one [`crate::spatial_packed::PackedSpatialMeta`] value.
///   Writing it is the atomic generation swap; a build writes its nodes under
///   an unpublished generation first, so readers never see a partial tree.
/// - `[1][generation u64 BE][node u64 BE]` — immutable packed nodes. The
///   generation lives in the key so a side-build and the live tree coexist,
///   and one prefix delete garbage-collects a retired generation.
/// - `[2][pk]` — delta entries written by commits after a pack: any delta row
///   masks the packed base for its pk (id-level tombstone) and, when it is an
///   upsert, carries the pk's current MBR/point. Reopen rebuilds the resident
///   delta R-tree from this subspace instead of rescanning the corpus.
const INDEX_NS_V13: [u8; 3] = [0, 0, 13];
const SPATIAL_SUBSPACE_META: u8 = 0;
const SPATIAL_SUBSPACE_NODE: u8 = 1;
const SPATIAL_SUBSPACE_DELTA: u8 = 2;

fn spatial_meta_key(index: &str) -> Vec<u8> {
    let mut key = ns_prefix(INDEX_NS_V13, index);
    key.push(SPATIAL_SUBSPACE_META);
    key
}

/// Prefix owning every node of one packed generation — the unit of GC.
pub(crate) fn spatial_generation_prefix(index: &str, generation: u64) -> Vec<u8> {
    let mut key = ns_prefix(INDEX_NS_V13, index);
    key.push(SPATIAL_SUBSPACE_NODE);
    key.extend_from_slice(&generation.to_be_bytes());
    key
}

fn spatial_node_key(index: &str, generation: u64, node: u64) -> Vec<u8> {
    let mut key = spatial_generation_prefix(index, generation);
    key.extend_from_slice(&node.to_be_bytes());
    key
}

fn spatial_delta_prefix(index: &str) -> Vec<u8> {
    let mut key = ns_prefix(INDEX_NS_V13, index);
    key.push(SPATIAL_SUBSPACE_DELTA);
    key
}

fn spatial_delta_key(index: &str, pk: &str) -> Vec<u8> {
    let mut key = spatial_delta_prefix(index);
    key.extend_from_slice(pk.as_bytes());
    key
}

fn full_text_dictionary_prefix(index: &str) -> Vec<u8> {
    ns_prefix(INDEX_NS_V7, index)
}

fn full_text_dictionary_term_key(index: &str, encoded_key: &[u8]) -> Vec<u8> {
    let mut key = full_text_dictionary_prefix(index);
    key.push(0);
    for &byte in encoded_key {
        key.push(byte);
        if byte == 0 {
            key.push(0xFF);
        }
    }
    key.extend_from_slice(&[0, 0]);
    key
}

fn full_text_dictionary_collection_key(index: &str) -> Vec<u8> {
    let mut key = full_text_dictionary_prefix(index);
    key.push(1);
    key
}

fn full_text_document_id_key(index: &str, document_id: u64) -> Vec<u8> {
    let mut key = ns_prefix(INDEX_NS_V8, index);
    key.push(0);
    key.extend_from_slice(&document_id.to_be_bytes());
    key
}

fn full_text_document_pk_key(index: &str, pk: &str) -> Vec<u8> {
    let mut key = ns_prefix(INDEX_NS_V8, index);
    key.push(1);
    key.extend_from_slice(pk.as_bytes());
    key
}

fn full_text_document_statistics_key(index: &str, document_id: u64) -> Vec<u8> {
    let mut key = ns_prefix(INDEX_NS_V11, index);
    key.extend_from_slice(&document_id.to_be_bytes());
    key
}

fn full_text_stored_text_key(index: &str, document_id: u64) -> Vec<u8> {
    let mut key = ns_prefix(INDEX_NS_V12, index);
    key.extend_from_slice(&document_id.to_be_bytes());
    key
}

fn decode_full_text_stored_text_document_id(prefix: &[u8], key: &[u8]) -> Result<u64> {
    let suffix = key.strip_prefix(prefix).ok_or_else(|| {
        BicDbError::PagedStorage("full-text stored-text key escaped its namespace".to_string())
    })?;
    let bytes: [u8; 8] = suffix.try_into().map_err(|_| {
        BicDbError::PagedStorage(format!(
            "malformed full-text stored-text key: expected an 8-byte document id, got {} bytes",
            suffix.len()
        ))
    })?;
    Ok(u64::from_be_bytes(bytes))
}

fn validate_full_text_stored_text_document_id(previous: Option<u64>, current: u64) -> Result<()> {
    if previous.is_some_and(|previous| current <= previous) {
        return Err(BicDbError::PagedStorage(format!(
            "full-text stored-text document ids are duplicate or out of order: {current} follows {}",
            previous.expect("checked above")
        )));
    }
    Ok(())
}

/// Key prefix identifying one index's entries.
fn index_prefix_v2(index: &str) -> Vec<u8> {
    let mut key = INDEX_NS_V2.to_vec();
    key.extend_from_slice(&durable_name_length(index));
    key.extend_from_slice(index.as_bytes());
    key
}

fn index_entry_prefix_v2(index: &str, encoded_prefix: &[u8]) -> Vec<u8> {
    let mut key = index_prefix_v2(index);
    for &byte in encoded_prefix {
        key.push(byte);
        if byte == 0 {
            key.push(0xFF);
        }
    }
    key
}

fn index_entry_key_v2(index: &str, encoded_key: &[u8], bucket: u16, pk: &str) -> Vec<u8> {
    let mut key = index_entry_prefix_v2(index, encoded_key);
    key.extend_from_slice(&[0, 0]);
    key.extend_from_slice(&(u16::MAX - bucket).to_be_bytes());
    key.extend_from_slice(pk.as_bytes());
    key
}

fn index_prefix(index: &str) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(INDEX_NS.len() + 2 + index.len());
    prefix.extend_from_slice(&INDEX_NS);
    prefix.extend_from_slice(&durable_name_length(index));
    prefix.extend_from_slice(index.as_bytes());
    prefix
}

/// Full key for one index entry.
///
/// Layout: `[prefix][escaped encoded_key][0x00 0x00][pk]`.
///
/// The encoded key is escaped (`0x00` -> `0x00 0xFF`) and terminated with
/// `0x00 0x00` so that (a) the terminator can never appear inside the escaped
/// key, making the split unambiguous, and (b) entries order by encoded key
/// first and pk second even when one key is a byte-prefix of another —
/// appending the pk directly would let `("a", pk="b")` and `("ab", pk="")`
/// collide into the same bytes.
/// The durable-key prefix that all entries of `index` whose ENCODED index key
/// starts with `encoded_prefix` share: `[index prefix][escaped encoded_prefix]`.
/// The entry escape (0x00 -> 0x00 0xFF) preserves byte-prefix relationships,
/// so a range scan from this prefix visits exactly those entries.
/// Durable ordered-entry format of one server_paged B-tree index.
///
/// Persisted as an ENGINE-OWNED record in the durable keyspace (namespace
/// `[0,0,15]`, key = index name), NOT in the JSON catalog — a catalog rewrite
/// cannot change what format an index's entries are in, so the source of
/// truth lives next to the entries themselves. Absent record = v2: every
/// store written before v3 existed reads and writes exactly as it always did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexEntryFormat {
    #[default]
    V2,
    V3,
}

const ENTRY_FORMAT_NS: [u8; 3] = [0, 0, 15];

fn entry_format_key(index: &str) -> Vec<u8> {
    ns_prefix(ENTRY_FORMAT_NS, index)
}

/// The row identity an ordered-index entry key carries after its terminator.
///
/// v2 entries (terminator `0x00 0x00`) embed the full pk string; v3 entries
/// (terminator `0x00 0x01`) carry the row's 8-byte intern id instead (see
/// `docs/durable-rowid-entries.md`). The escape rule (`0x00` in the encoded
/// key is always written `0x00 0xFF`) keeps both terminators unambiguous, and
/// one index only ever contains one format — but decoders accept either so a
/// mixed-version deployment can always READ.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexEntryRef {
    Pk(String),
    Intern(u64),
}

pub(crate) const INDEX_ENTRY_TERM_V2: [u8; 2] = [0, 0];
pub(crate) const INDEX_ENTRY_TERM_V3: [u8; 2] = [0, 1];

fn index_entry_key_v3(index: &str, encoded_key: &[u8], id: u64) -> Vec<u8> {
    let mut key = index_entry_prefix(index, encoded_key);
    key.extend_from_slice(&INDEX_ENTRY_TERM_V3);
    key.extend_from_slice(&id.to_be_bytes());
    key
}

/// Decode either entry format: `(encoded_key, row reference)`.
pub(crate) fn decode_index_entry_key_any(
    index: &str,
    key: &[u8],
) -> Result<(Vec<u8>, IndexEntryRef)> {
    let corrupt = |message: &str| BicDbError::Corruption {
        path: std::path::PathBuf::from("<paged storage>"),
        message: format!("index entry for `{index}`: {message}"),
    };
    let body = key
        .get(index_prefix(index).len()..)
        .ok_or_else(|| corrupt("key shorter than its prefix"))?;
    let mut encoded = Vec::new();
    let mut rest = body;
    loop {
        match rest {
            [0, 0, tail @ ..] => {
                let pk = std::str::from_utf8(tail)
                    .map_err(|_| corrupt("pk is not utf-8"))?
                    .to_string();
                return Ok((encoded, IndexEntryRef::Pk(pk)));
            }
            [0, 1, tail @ ..] => {
                let id: [u8; 8] = tail
                    .try_into()
                    .map_err(|_| corrupt("intern id suffix is not 8 bytes"))?;
                return Ok((encoded, IndexEntryRef::Intern(u64::from_be_bytes(id))));
            }
            [0, 0xFF, tail @ ..] => {
                encoded.push(0);
                rest = tail;
            }
            [byte, tail @ ..] => {
                encoded.push(*byte);
                rest = tail;
            }
            [] => return Err(corrupt("key ends inside its encoded prefix")),
        }
    }
}

/// Per-collection durable intern dictionary (namespace `[0,0,14]`), the
/// identity behind v3 ordered-index entries (see
/// `docs/durable-rowid-entries.md`). Subspaces under
/// `[NS][len BE2][collection]`:
///   `[0][id BE8] -> pk bytes`   (forward: read-path resolution)
///   `[1][pk]     -> id BE8`     (reverse: write-path dedup)
///   `[2]         -> next id BE8` (allocation counter)
/// All three are ordinary MVCC rows: an aborted transaction rolls its
/// allocation (counter bump + both mappings) back atomically.
const INTERN_NS: [u8; 3] = [0, 0, 14];

fn intern_fwd_key(collection: &str, id: u64) -> Vec<u8> {
    let mut key = ns_prefix(INTERN_NS, collection);
    key.push(0);
    key.extend_from_slice(&id.to_be_bytes());
    key
}

fn intern_rev_key(collection: &str, pk: &str) -> Vec<u8> {
    let mut key = ns_prefix(INTERN_NS, collection);
    key.push(1);
    key.extend_from_slice(pk.as_bytes());
    key
}

fn intern_ctr_key(collection: &str) -> Vec<u8> {
    let mut key = ns_prefix(INTERN_NS, collection);
    key.push(2);
    key
}

fn decode_intern_id(value: &[u8], context: &str) -> Result<u64> {
    let bytes: [u8; 8] = value.try_into().map_err(|_| {
        BicDbError::PagedStorage(format!(
            "intern {context} value is {} bytes, expected 8",
            value.len()
        ))
    })?;
    Ok(u64::from_be_bytes(bytes))
}

impl PagedRecords {
    /// The intern id already allocated for `pk`, if any.
    pub fn intern_id_for_pk(
        &self,
        snapshot: &Snapshot,
        collection: &str,
        pk: &str,
    ) -> Result<Option<u64>> {
        self.store
            .get_as_of(snapshot, &intern_rev_key(collection, pk))
            .map_err(page_error)?
            .map(|value| decode_intern_id(&value, "reverse"))
            .transpose()
    }

    /// Resolve an intern id back to its pk.
    pub fn pk_for_intern_id(
        &self,
        snapshot: &Snapshot,
        collection: &str,
        id: u64,
    ) -> Result<Option<String>> {
        self.store
            .get_as_of(snapshot, &intern_fwd_key(collection, id))
            .map_err(page_error)?
            .map(|value| {
                String::from_utf8(value).map_err(|error| {
                    BicDbError::PagedStorage(format!(
                        "intern id {id} resolves to invalid UTF-8: {error}"
                    ))
                })
            })
            .transpose()
    }

    /// Resolve `pk`'s intern id, allocating one inside `transaction` if it has
    /// none. Reads through an own-writes snapshot so a transaction that
    /// touches the same new row twice allocates exactly once, and the counter
    /// bump commits or aborts atomically with the entries that need it.
    pub fn intern_or_alloc(&self, transaction: Xid, collection: &str, pk: &str) -> Result<u64> {
        let own_writes = Snapshot {
            xid: transaction,
            xmax: transaction + 1,
            in_flight: Arc::new(std::collections::BTreeSet::new()),
        };
        if let Some(id) = self.intern_id_for_pk(&own_writes, collection, pk)? {
            return Ok(id);
        }
        let counter_key = intern_ctr_key(collection);
        let next = self
            .store
            .get_as_of(&own_writes, &counter_key)
            .map_err(page_error)?
            .map(|value| decode_intern_id(&value, "counter"))
            .transpose()?
            .unwrap_or(0);
        self.store
            .put(transaction, &counter_key, &(next + 1).to_be_bytes())
            .map_err(page_error)?;
        self.store
            .put(
                transaction,
                &intern_fwd_key(collection, next),
                pk.as_bytes(),
            )
            .map_err(page_error)?;
        self.store
            .put(
                transaction,
                &intern_rev_key(collection, pk),
                &next.to_be_bytes(),
            )
            .map_err(page_error)?;
        Ok(next)
    }

    /// The durable entry format of `index` plus the collection its intern ids
    /// resolve in (uncached read; [`PagedRecords`] callers go through the
    /// cached accessor). Record value: `b"3\x00{collection}"` for v3.
    pub fn index_entry_format_full(
        &self,
        snapshot: &Snapshot,
        index: &str,
    ) -> Result<(IndexEntryFormat, Option<String>)> {
        let index = self.resolve_index_name(index);
        match self
            .store
            .get_as_of(snapshot, &entry_format_key(&index))
            .map_err(page_error)?
        {
            Some(value) if value.first() == Some(&b'3') => {
                let collection = value
                    .get(2..)
                    .filter(|rest| !rest.is_empty())
                    .map(|rest| {
                        String::from_utf8(rest.to_vec()).map_err(|error| {
                            BicDbError::PagedStorage(format!(
                                "entry-format record for `{index}` has invalid collection: {error}"
                            ))
                        })
                    })
                    .transpose()?;
                Ok((IndexEntryFormat::V3, collection))
            }
            Some(_) | None => Ok((IndexEntryFormat::V2, None)),
        }
    }

    /// [`Self::index_entry_format_full`], format only.
    pub fn index_entry_format(&self, snapshot: &Snapshot, index: &str) -> Result<IndexEntryFormat> {
        Ok(self.index_entry_format_full(snapshot, index)?.0)
    }

    /// [`Self::index_entry_format`] through the per-handle cache — the form
    /// every write-path caller uses.
    pub fn entry_format_cached(&self, index: &str) -> Result<IndexEntryFormat> {
        Ok(self.entry_format_full_cached(index)?.0)
    }

    /// Cached `(format, intern collection)` of `index`.
    pub fn entry_format_full_cached(
        &self,
        index: &str,
    ) -> Result<(IndexEntryFormat, Option<String>)> {
        let resolved = self.resolve_index_name(index);
        if let Some(cached) = self.entry_formats.read().get(&resolved) {
            return Ok(cached.clone());
        }
        let full = self.index_entry_format_full(&self.latest_snapshot(), &resolved)?;
        self.entry_formats.write().insert(resolved, full.clone());
        Ok(full)
    }

    /// Resolve a decoded entry reference to its pk using the index's RECORDED
    /// intern collection — what lets every legacy String-yielding scan read a
    /// v3 index without its caller knowing the format exists.
    fn resolve_entry_ref(
        &self,
        snapshot: &Snapshot,
        index: &str,
        entry_ref: IndexEntryRef,
    ) -> Result<String> {
        match entry_ref {
            IndexEntryRef::Pk(pk) => Ok(pk),
            IndexEntryRef::Intern(id) => {
                let (_, collection) = self.entry_format_full_cached(index)?;
                let collection = collection.ok_or_else(|| {
                    BicDbError::PagedStorage(format!(
                        "index `{index}` has v3 entries but no recorded intern collection"
                    ))
                })?;
                self.pk_for_intern_id(snapshot, &collection, id)?
                    .ok_or_else(|| {
                        BicDbError::PagedStorage(format!(
                            "index `{index}` entry references intern id {id} with no mapping"
                        ))
                    })
            }
        }
    }

    /// Publish `index`'s entry format (create_index / rekey only — the format
    /// of a populated index changes exclusively through a completed rekey).
    pub fn set_index_entry_format(
        &self,
        transaction: Xid,
        index: &str,
        collection: &str,
        format: IndexEntryFormat,
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        let value: Vec<u8> = match format {
            IndexEntryFormat::V2 => b"2".to_vec(),
            IndexEntryFormat::V3 => {
                let mut value = b"3\x00".to_vec();
                value.extend_from_slice(collection.as_bytes());
                value
            }
        };
        self.store
            .put(transaction, &entry_format_key(&index), &value)
            .map_err(page_error)?;
        self.entry_formats
            .write()
            .insert(index, (format, Some(collection.to_string())));
        Ok(())
    }

    /// Write one v3 ordered-index entry: intern-id suffix, TID-hint value.
    pub fn put_index_entry_intern(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        id: u64,
        value: &[u8],
    ) -> Result<()> {
        let index = self.resolve_index_name(index);
        let key = index_entry_key_v3(&index, encoded_key, id);
        self.store.put(transaction, &key, value).map_err(page_error)
    }

    /// Remove one v3 ordered-index entry. Returns whether it was present.
    pub fn delete_index_entry_intern(
        &self,
        transaction: Xid,
        index: &str,
        encoded_key: &[u8],
        id: u64,
    ) -> Result<bool> {
        let index = self.resolve_index_name(index);
        let key = index_entry_key_v3(&index, encoded_key, id);
        self.store.delete(transaction, &key).map_err(page_error)
    }

    /// [`Self::scan_index_exact`] for BOTH entry formats: every visible entry
    /// whose encoded key equals `encoded_key`, yielding the row reference and
    /// the entry value (TID hint / posting payload).
    pub fn scan_index_exact_refs<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_key: &[u8],
    ) -> Result<impl Iterator<Item = Result<(IndexEntryRef, Vec<u8>)>> + 'store> {
        let index = self.resolve_index_name(index);
        // Both terminators start with 0x00 — but so does the 0x00 0xFF escape
        // of a LONGER key that continues with a NUL byte. The terminators sort
        // first (0x00 0x00 < 0x00 0x01 < 0x00 0xFF), so scanning from
        // `prefix + 0x00` and stopping at the first byte-after-prefix that is
        // not a terminator tag yields exactly this key's v2-then-v3 entries.
        let mut prefix = index_entry_prefix(&index, encoded_key);
        prefix.push(0);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let filter_prefix = prefix.clone();
        let index_name = index.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => {
                    key.starts_with(&filter_prefix)
                        && matches!(key.get(filter_prefix.len()), Some(0) | Some(1))
                }
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let (_, entry_ref) = decode_index_entry_key_any(&index_name, &key)?;
                Ok((entry_ref, value))
            }))
    }

    /// [`Self::scan_index_encoded_prefix`] for BOTH entry formats.
    pub fn scan_index_encoded_prefix_refs<'store>(
        &'store self,
        snapshot: &Snapshot,
        index: &str,
        encoded_prefix: &[u8],
    ) -> Result<impl Iterator<Item = Result<(Vec<u8>, IndexEntryRef, Vec<u8>)>> + 'store> {
        let index = self.resolve_index_name(index);
        let prefix = index_entry_prefix(&index, encoded_prefix);
        let cursor = self
            .store
            .scan_from(snapshot, &prefix)
            .map_err(page_error)?;
        let filter_prefix = prefix.clone();
        let index_name = index.clone();
        Ok(cursor
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(&filter_prefix),
                Err(_) => true,
            })
            .map(move |entry| {
                let (key, value) = entry.map_err(page_error)?;
                let (encoded, entry_ref) = decode_index_entry_key_any(&index_name, &key)?;
                Ok((encoded, entry_ref, value))
            }))
    }

    /// One bounded batch of a v2→v3 index rekey: rewrite up to `max_entries`
    /// v2 entries as intern-keyed v3 entries (same encoded key, same value)
    /// and delete the originals, in ONE transaction. Returns how many were
    /// rewritten; `0` means the index has no v2 entries left.
    ///
    /// The migration is SELF-CHECKPOINTING: rewritten v2 entries are deleted,
    /// so a crashed or killed run resumes by scanning for the first surviving
    /// v2 entry. A mixed-format index reads correctly throughout (every
    /// decoder accepts both formats); the caller flips the durable format
    /// record only after a batch returns 0.
    pub fn rekey_ordered_index_batch(
        &self,
        index: &str,
        collection: &str,
        max_entries: usize,
    ) -> Result<u64> {
        let resolved = self.resolve_index_name(index);
        let snapshot = self.latest_snapshot();
        let mut batch: Vec<(Vec<u8>, String, Vec<u8>)> = Vec::new();
        {
            let namespace = index_prefix(&resolved);
            let cursor = self
                .store
                .scan_from(&snapshot, &namespace)
                .map_err(page_error)?;
            for entry in cursor {
                let (key, value) = entry.map_err(page_error)?;
                if !key.starts_with(&namespace) {
                    break;
                }
                let (encoded, entry_ref) = decode_index_entry_key_any(&resolved, &key)?;
                if let IndexEntryRef::Pk(pk) = entry_ref {
                    batch.push((encoded, pk, value));
                    if batch.len() >= max_entries {
                        break;
                    }
                }
            }
        }
        if batch.is_empty() {
            return Ok(0);
        }
        let rewritten = batch.len() as u64;
        let (xid, _) = self.begin();
        let result: Result<()> = (|| {
            for (encoded, pk, value) in &batch {
                let id = self.intern_or_alloc(xid, collection, pk)?;
                self.put_index_entry_intern(xid, &resolved, encoded, id, value)?;
                self.delete_index_entry(xid, &resolved, encoded, pk)?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.commit(xid)?;
                Ok(rewritten)
            }
            Err(error) => {
                self.abort(xid)?;
                Err(error)
            }
        }
    }

    /// Retire `pk`'s intern mapping on row delete (MVCC delete: readers on
    /// older snapshots still resolve). A later re-insert of the same pk
    /// allocates a FRESH id — ids never span a delete boundary.
    pub fn retire_intern(&self, transaction: Xid, collection: &str, pk: &str) -> Result<()> {
        let own_writes = Snapshot {
            xid: transaction,
            xmax: transaction + 1,
            in_flight: Arc::new(std::collections::BTreeSet::new()),
        };
        if let Some(id) = self.intern_id_for_pk(&own_writes, collection, pk)? {
            self.store
                .delete(transaction, &intern_fwd_key(collection, id))
                .map_err(page_error)?;
            self.store
                .delete(transaction, &intern_rev_key(collection, pk))
                .map_err(page_error)?;
        }
        Ok(())
    }
}

/// Encoded size of a TID hint riding in an ordered index entry's value:
/// a 14-byte [`TupleLocator`] plus the writing transaction's 8-byte xmin.
pub(crate) const TID_HINT_BYTES: usize = 22;

/// Encode the TID hint an ordered index entry records: the row's chain-head
/// locator at entry-write time plus the writer's xmin (the identity stamp a
/// hinted read validates against slot reuse). An EMPTY entry value means "no
/// hint" — every store written before hints existed reads unchanged.
pub(crate) fn encode_tid_hint(locator: TupleLocator, xmin: Xid) -> [u8; TID_HINT_BYTES] {
    let mut hint = [0u8; TID_HINT_BYTES];
    hint[..14].copy_from_slice(&locator.encode());
    hint[14..].copy_from_slice(&xmin.to_be_bytes());
    hint
}

/// Decode a TID hint from an ordered index entry's value. `None` for the
/// empty (pre-hint) value, a foreign length, or an undecodable locator — all
/// of which simply mean "read through the key descent".
pub(crate) fn decode_tid_hint(value: &[u8]) -> Option<(TupleLocator, Xid)> {
    if value.len() != TID_HINT_BYTES {
        return None;
    }
    let locator = TupleLocator::decode(&value[..14])?;
    let xmin = Xid::from_be_bytes(value[14..].try_into().ok()?);
    Some((locator, xmin))
}

fn index_entry_prefix(index: &str, encoded_prefix: &[u8]) -> Vec<u8> {
    let mut key = index_prefix(index);
    for &byte in encoded_prefix {
        key.push(byte);
        if byte == 0 {
            key.push(0xFF);
        }
    }
    key
}

fn ns_prefix(ns: [u8; 3], index: &str) -> Vec<u8> {
    let mut key = ns.to_vec();
    key.extend_from_slice(&durable_name_length(index));
    key.extend_from_slice(index.as_bytes());
    key
}

fn ns_entry_prefix(ns: [u8; 3], index: &str, encoded_prefix: &[u8]) -> Vec<u8> {
    let mut key = ns_prefix(ns, index);
    for &byte in encoded_prefix {
        key.push(byte);
        if byte == 0 {
            key.push(0xFF);
        }
    }
    key
}

fn ns_term_key(ns: [u8; 3], index: &str, encoded_key: &[u8], suffix: &str) -> Vec<u8> {
    let mut key = ns_entry_prefix(ns, index, encoded_key);
    key.extend_from_slice(&[0, 0]);
    key.extend_from_slice(suffix.as_bytes());
    key
}

fn ns_term_u64_key(ns: [u8; 3], index: &str, encoded_key: &[u8], suffix: u64) -> Vec<u8> {
    let mut key = ns_entry_prefix(ns, index, encoded_key);
    key.extend_from_slice(&[0, 0]);
    key.extend_from_slice(&suffix.to_be_bytes());
    key
}

fn decode_namespaced_term_key<'a>(body: &'a [u8], index: &str) -> Result<(Vec<u8>, &'a [u8])> {
    let mut encoded = Vec::new();
    let mut rest = body;
    loop {
        match rest {
            [0, 0, tail @ ..] => return Ok((encoded, tail)),
            [0, 0xFF, tail @ ..] => {
                encoded.push(0);
                rest = tail;
            }
            [byte, tail @ ..] => {
                encoded.push(*byte);
                rest = tail;
            }
            [] => {
                return Err(BicDbError::PagedStorage(format!(
                    "numeric block key for `{index}` missing terminator"
                )));
            }
        }
    }
}

// ---- posting block codec -------------------------------------------------

/// One posting inside a block, exactly the data a per-posting v1 payload
/// carries plus the pk it was keyed by.
#[derive(Debug)]
pub struct BlockPosting {
    pub pk: String,
    pub doc_length: u32,
    pub doc_distinct: u32,
    pub packed_positions: Vec<u16>,
}

pub(crate) fn push_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

pub(crate) fn read_varint(bytes: &[u8], cursor: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(*cursor)?;
        *cursor += 1;
        value |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
}

pub(crate) const POSTING_BLOCK_VERSION_V1: u8 = 1;
pub(crate) const POSTING_BLOCK_VERSION: u8 = 2;

/// Encode a pk-sorted run of postings:
/// `[version u8][doc_count varint][max_impact varint][max_rank f32-bits
/// varint (v2)]` then per posting `[shared_prefix varint][suffix_len varint]
/// [suffix][doc_len varint][distinct varint][pos_count varint][pos varint]*`.
/// Front-coding the pks plus varints is what removes the per-entry B-tree
/// overhead that made the per-posting layout ~40x tantivy's size.
///
/// `max_rank` is the EXACT best single-term ts_rank in the block (default
/// weights, normalization 0 — the ranked fast path's guard domain). The u16
/// impact bucket alone cannot terminate a tie plateau: every block of a
/// uniform-tf term carries the same bucket, so the bucket bound never
/// proves the kth unbeatable and the scan decodes the whole posting list.
/// The exact maximum can: `kth >= max_rank` skips the block outright (ties
/// lose — scan order within a bucket is pk-ascending, so any remaining
/// equal-rank posting has a larger pk than every equal-rank member of the
/// top-k).
pub(crate) fn encode_posting_block(
    postings: &[BlockPosting],
    max_impact: u16,
    max_rank: f32,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(postings.len() * 16);
    out.push(POSTING_BLOCK_VERSION);
    push_varint(&mut out, postings.len() as u64);
    push_varint(&mut out, u64::from(max_impact));
    push_varint(&mut out, u64::from(max_rank.to_bits()));
    let mut previous_pk = "";
    for posting in postings {
        let shared = previous_pk
            .as_bytes()
            .iter()
            .zip(posting.pk.as_bytes())
            .take_while(|(left, right)| left == right)
            .count();
        push_varint(&mut out, shared as u64);
        let suffix = &posting.pk.as_bytes()[shared..];
        push_varint(&mut out, suffix.len() as u64);
        out.extend_from_slice(suffix);
        push_varint(&mut out, u64::from(posting.doc_length));
        push_varint(&mut out, u64::from(posting.doc_distinct));
        push_varint(&mut out, posting.packed_positions.len() as u64);
        for packed in &posting.packed_positions {
            push_varint(&mut out, u64::from(*packed));
        }
        previous_pk = &posting.pk;
    }
    out
}

/// Decode a block; `(postings, max_impact)`.
/// Header-only max impact of a block.
pub(crate) fn posting_block_max_impact(bytes: &[u8]) -> Result<u16> {
    let corrupt = || BicDbError::PagedStorage("corrupt posting block".to_string());
    posting_block_header(bytes)
        .ok_or_else(corrupt)
        .map(|header| header.max_impact)
}

pub(crate) struct PostingBlockHeader {
    pub doc_count: usize,
    pub max_impact: u16,
    /// Exact best single-term rank in the block; `None` on v1 blocks
    /// (pre-0.9.65 folds) — readers fall back to the bucket bound.
    pub max_rank: Option<f32>,
    /// Offset of the first posting.
    pub body: usize,
}

pub(crate) fn posting_block_header(bytes: &[u8]) -> Option<PostingBlockHeader> {
    let version = *bytes.first()?;
    if version != POSTING_BLOCK_VERSION && version != POSTING_BLOCK_VERSION_V1 {
        return None;
    }
    let mut cursor = 1usize;
    let doc_count = read_varint(bytes, &mut cursor)? as usize;
    if doc_count > bytes.len() {
        return None;
    }
    let max_impact = read_varint(bytes, &mut cursor)? as u16;
    let max_rank = if version == POSTING_BLOCK_VERSION {
        Some(f32::from_bits(read_varint(bytes, &mut cursor)? as u32))
    } else {
        None
    };
    Some(PostingBlockHeader {
        doc_count,
        max_impact,
        max_rank,
        body: cursor,
    })
}

/// Header-only doc count of a block (no posting decode).
pub(crate) fn posting_block_doc_count(bytes: &[u8]) -> Result<usize> {
    let corrupt = || BicDbError::PagedStorage("corrupt posting block".to_string());
    posting_block_header(bytes)
        .ok_or_else(corrupt)
        .map(|header| header.doc_count)
}

/// Walk a block's postings WITHOUT materializing them: the pk and positions
/// are handed to `visit` as borrows of two caller-owned scratch buffers, so a
/// scan allocates nothing per posting (the scratches grow to the largest
/// posting once and are reused). `visit` returning false stops the walk.
/// Returns `(kept_walking, max_impact)`.
pub(crate) fn visit_posting_block(
    bytes: &[u8],
    pk_scratch: &mut Vec<u8>,
    pos_scratch: &mut Vec<u16>,
    visit: impl FnMut(&str, u32, u32, &[u16]) -> Result<bool>,
) -> Result<(bool, u16)> {
    visit_posting_block_filtered(bytes, pk_scratch, pos_scratch, |_| true, visit)
}

/// [`visit_posting_block`] with a pre-decode filter: postings whose pk fails
/// `want` have their positions SKIPPED (cursor walk only, nothing written to
/// the scratch) — what makes a probe walk over a broad term pay for decoded
/// positions only on actual candidates, not on the ~90% of block residents
/// that are not being probed.
pub(crate) fn visit_posting_block_filtered(
    bytes: &[u8],
    pk_scratch: &mut Vec<u8>,
    pos_scratch: &mut Vec<u16>,
    mut want: impl FnMut(&str) -> bool,
    mut visit: impl FnMut(&str, u32, u32, &[u16]) -> Result<bool>,
) -> Result<(bool, u16)> {
    let corrupt = || BicDbError::PagedStorage("corrupt posting block".to_string());
    let header = posting_block_header(bytes).ok_or_else(corrupt)?;
    let count = header.doc_count;
    let max_impact = header.max_impact;
    crate::fts_format::record_posting_block_read(bytes.len(), count);
    let mut cursor = header.body;
    pk_scratch.clear();
    for _ in 0..count {
        let shared = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
        let suffix_len = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
        if shared > pk_scratch.len() {
            return Err(corrupt());
        }
        let suffix = bytes.get(cursor..cursor + suffix_len).ok_or_else(corrupt)?;
        cursor += suffix_len;
        pk_scratch.truncate(shared);
        pk_scratch.extend_from_slice(suffix);
        let doc_length = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u32;
        let doc_distinct = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u32;
        let pos_count = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
        if pos_count > bytes.len().saturating_sub(cursor) {
            return Err(corrupt());
        }
        if !want(std::str::from_utf8(pk_scratch).map_err(|_| corrupt())?) {
            for _ in 0..pos_count {
                read_varint(bytes, &mut cursor).ok_or_else(corrupt)?;
            }
            continue;
        }
        pos_scratch.clear();
        for _ in 0..pos_count {
            pos_scratch.push(read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u16);
        }
        let pk = std::str::from_utf8(pk_scratch).map_err(|_| corrupt())?;
        if !visit(pk, doc_length, doc_distinct, pos_scratch)? {
            return Ok((false, max_impact));
        }
    }
    Ok((true, max_impact))
}

pub(crate) fn decode_posting_block(bytes: &[u8]) -> Result<(Vec<BlockPosting>, u16)> {
    let corrupt = || BicDbError::PagedStorage("corrupt posting block".to_string());
    let header = posting_block_header(bytes).ok_or_else(corrupt)?;
    let count = header.doc_count;
    let max_impact = header.max_impact;
    crate::fts_format::record_posting_block_read(bytes.len(), count);
    let mut cursor = header.body;
    let mut postings = Vec::with_capacity(count);
    let mut previous_pk = String::new();
    for _ in 0..count {
        let shared = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
        let suffix_len = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
        if shared > previous_pk.len() {
            return Err(corrupt());
        }
        let suffix = bytes.get(cursor..cursor + suffix_len).ok_or_else(corrupt)?;
        cursor += suffix_len;
        let mut pk = previous_pk.as_bytes()[..shared].to_vec();
        pk.extend_from_slice(suffix);
        let pk = String::from_utf8(pk).map_err(|_| corrupt())?;
        let doc_length = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u32;
        let doc_distinct = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u32;
        let pos_count = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
        if pos_count > bytes.len().saturating_sub(cursor) {
            return Err(corrupt());
        }
        let mut packed_positions = Vec::with_capacity(pos_count);
        for _ in 0..pos_count {
            packed_positions.push(read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u16);
        }
        previous_pk = pk.clone();
        postings.push(BlockPosting {
            pk,
            doc_length,
            doc_distinct,
            packed_positions,
        });
    }
    Ok((postings, max_impact))
}

// ---- numeric posting block codec ----------------------------------------

const NUMERIC_POSTING_BLOCK_VERSION_V1: u8 = 1;
const NUMERIC_POSTING_BLOCK_VERSION_V2: u8 = 2;
const NUMERIC_POSTING_BLOCK_VERSION_V3: u8 = 3;
pub(crate) const NUMERIC_POSTING_BLOCK_VERSION: u8 = 4;
pub(crate) const COMPACT_IMPACT_BLOCK_VERSION: u8 = 0x81;
const NUMERIC_POSTING_IDS_ASCENDING: u8 = 1;
pub(crate) const BP128_FRAME_VALUES: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NumericBlockPosting {
    pub document_id: u64,
    pub doc_length: u32,
    pub doc_distinct: u32,
    pub packed_positions: Vec<u16>,
}

/// The fields needed to intersect and score a numeric posting without
/// allocating or decoding its position list. Ranked BM25 only expands the
/// full posting for candidates which actually enter the top-k.
/// Context a slim segment block needs to decode away from its reader.
#[derive(Clone)]
pub(crate) struct SlimBlockContext {
    /// Per-document (doc_length, doc_distinct) table, shared from the reader.
    pub docs: Arc<crate::fts_segment::DocsBacking>,
    /// The block's max term frequency, always served from the term
    /// directory — the BM25 leader bound needs nothing else.
    pub max_term_frequency: u32,
    /// (max_rank, weight_mask) from the term directory for multi-block
    /// terms; `None` for single-block terms, whose one-off decode computes
    /// the full header.
    pub header: Option<(f32, u8)>,
}

/// One fetched posting block in its native representation.
pub(crate) struct FetchedPostingBlock {
    pub last_document_id: u64,
    pub bytes: Vec<u8>,
    /// `Some` when `bytes` is a slim segment block; `None` when v1.
    pub slim: Option<SlimBlockContext>,
}

impl FetchedPostingBlock {
    /// The compatibility bridge: regenerate exact v1 bytes. The header is a
    /// deterministic function of the postings, so this is bit-identical to
    /// what the keyed format stored — the equivalence oracle.
    pub(crate) fn into_v1(self) -> Result<(u64, Vec<u8>)> {
        let Some(slim) = &self.slim else {
            return Ok((self.last_document_id, self.bytes));
        };
        let postings = crate::fts_segment::decode_slim_pk_block(&self.bytes, &slim.docs.view())?;
        // The v1 header is a deterministic function of the postings; the
        // run helper recomputes it wholesale. This bridge is correctness
        // scaffolding, not a hot path.
        let encoded = crate::db::encode_numeric_posting_run(&postings).1;
        Ok((self.last_document_id, encoded))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NumericBlockScorePosting {
    pub document_id: u64,
    pub doc_length: u32,
    pub doc_distinct: u32,
    pub term_frequency: u32,
}

pub(crate) struct NumericPostingBlockHeader {
    pub version: u8,
    pub doc_count: usize,
    pub max_impact: u16,
    pub max_rank: f32,
    pub max_term_frequency: u32,
    pub weight_mask: u8,
    pub body: usize,
    pub document_stream_bytes: usize,
    /// v4 only: byte length of the per-posting metadata stream between the
    /// document-id frames and the position stream. Zero for v1-v3 blocks,
    /// whose positions are interleaved with the metadata.
    pub metadata_stream_bytes: usize,
    pub ascending: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct CompactImpactBlockHeader {
    pub doc_count: usize,
    pub max_impact: u16,
    pub max_rank: f32,
    pub body: usize,
    pub document_stream_bytes: usize,
}

pub(crate) fn zigzag_encode(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

pub(crate) fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

fn bit_width(value: u64) -> u8 {
    (u64::BITS - value.leading_zeros()) as u8
}

pub(crate) fn pack_bp128_frame(values: &[u64], output: &mut Vec<u8>) {
    debug_assert!(values.len() <= BP128_FRAME_VALUES);
    push_varint(output, values.len() as u64);
    let width = values.iter().copied().map(bit_width).max().unwrap_or(0);
    output.push(width);
    if width == 0 {
        return;
    }
    let width = usize::from(width);
    output.reserve((values.len() * width).div_ceil(8));
    // Streamed LSB-first: value bit `b` of value `i` lands at packed bit
    // `i * width + b`. `available` never exceeds 7 + 64, so the u128 staging
    // buffer cannot lose bits before the flush loop drains it.
    let mut buffer = 0u128;
    let mut available = 0usize;
    for &value in values {
        buffer |= u128::from(value) << available;
        available += width;
        while available >= 8 {
            output.push(buffer as u8);
            buffer >>= 8;
            available -= 8;
        }
    }
    if available > 0 {
        output.push(buffer as u8);
    }
}

pub(crate) fn unpack_bp128_frame(
    bytes: &[u8],
    cursor: &mut usize,
    output: &mut Vec<u64>,
) -> Option<()> {
    let count = read_varint(bytes, cursor)? as usize;
    if count == 0 || count > BP128_FRAME_VALUES {
        return None;
    }
    let width = *bytes.get(*cursor)?;
    *cursor += 1;
    if width > 64 {
        return None;
    }
    let byte_count = (count * usize::from(width)).div_ceil(8);
    let packed = bytes.get(*cursor..cursor.checked_add(byte_count)?)?;
    *cursor += byte_count;
    output.clear();
    output.reserve(count);
    if width == 0 {
        output.resize(count, 0);
        return Some(());
    }
    let width = usize::from(width);
    let mask = if width == 64 {
        u64::MAX
    } else {
        (1u64 << width) - 1
    };
    // Refill the staging buffer eight bytes at a time. The refill happens
    // only while `available < width <= 64`, so `available + 64 <= 127` and
    // the u128 buffer never overflows.
    let mut buffer = 0u128;
    let mut available = 0usize;
    let mut consumed = 0usize;
    for _ in 0..count {
        if available < width {
            if let Some(window) = packed.get(consumed..consumed + 8) {
                let word = u64::from_le_bytes(window.try_into().expect("eight-byte window"));
                buffer |= u128::from(word) << available;
                consumed += 8;
                available += 64;
            } else {
                while consumed < packed.len() && available < width {
                    buffer |= u128::from(packed[consumed]) << available;
                    consumed += 1;
                    available += 8;
                }
                if available < width {
                    return None;
                }
            }
        }
        output.push((buffer as u64) & mask);
        buffer >>= width;
        available -= width;
    }
    Some(())
}

/// Advance `cursor` past `count` varints without decoding their values. A
/// varint ends at every byte whose continuation bit is clear, so eight input
/// bytes are classified per step with one mask and popcount.
fn skip_varints(bytes: &[u8], cursor: &mut usize, mut remaining: usize) -> Option<()> {
    while remaining > 0 {
        if let Some(window) = bytes.get(*cursor..*cursor + 8) {
            let word = u64::from_le_bytes(window.try_into().expect("eight-byte window"));
            let terminators = !word & 0x8080_8080_8080_8080;
            let count = terminators.count_ones() as usize;
            if count < remaining {
                *cursor += 8;
                remaining -= count;
                continue;
            }
            let mut bits = terminators;
            for _ in 1..remaining {
                bits &= bits - 1;
            }
            *cursor += bits.trailing_zeros() as usize / 8 + 1;
            return Some(());
        }
        let byte = *bytes.get(*cursor)?;
        *cursor += 1;
        if byte & 0x80 == 0 {
            remaining -= 1;
        }
    }
    Some(())
}

/// Numeric block v4. Document-id deltas are bit-packed in BP128-style
/// 128-value frames, followed by the per-posting metadata stream
/// (length/distinct/term-frequency varints) and then the position stream.
/// Keeping positions in their own stream lets ranked BM25 decode scores
/// without touching position bytes at all. Document-order blocks use
/// unsigned deltas; impact-order blocks use ZigZag deltas. The v1-v3
/// decoders remain below for rolling upgrades (v2/v3 interleave positions
/// with the metadata).
pub(crate) fn encode_numeric_posting_block(
    postings: &[NumericBlockPosting],
    max_impact: u16,
    max_rank: f32,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(postings.len() * 10);
    out.push(NUMERIC_POSTING_BLOCK_VERSION);
    let ascending = postings
        .windows(2)
        .all(|pair| pair[0].document_id <= pair[1].document_id);
    out.push(if ascending {
        NUMERIC_POSTING_IDS_ASCENDING
    } else {
        0
    });
    push_varint(&mut out, postings.len() as u64);
    push_varint(&mut out, u64::from(max_impact));
    push_varint(&mut out, u64::from(max_rank.to_bits()));
    let mut max_term_frequency = 1usize;
    let mut weight_mask = 0u8;
    for posting in postings {
        max_term_frequency = max_term_frequency.max(posting.packed_positions.len().max(1));
        if posting.packed_positions.is_empty() {
            weight_mask |= 1;
        } else {
            for packed in &posting.packed_positions {
                weight_mask |= 1 << ((packed >> 14) & 0x3);
            }
        }
    }
    push_varint(&mut out, max_term_frequency as u64);
    out.push(weight_mask);
    let mut document_stream = Vec::with_capacity(postings.len() * 2);
    let mut deltas = Vec::with_capacity(BP128_FRAME_VALUES);
    let mut previous = 0u64;
    for frame in postings.chunks(BP128_FRAME_VALUES) {
        deltas.clear();
        for posting in frame {
            let delta = if ascending {
                posting.document_id - previous
            } else {
                let current = i64::try_from(posting.document_id)
                    .expect("full-text document id exceeds signed delta codec");
                let previous_signed = i64::try_from(previous)
                    .expect("full-text document id exceeds signed delta codec");
                zigzag_encode(current - previous_signed)
            };
            deltas.push(delta);
            previous = posting.document_id;
        }
        pack_bp128_frame(&deltas, &mut document_stream);
    }
    let mut metadata_stream = Vec::with_capacity(postings.len() * 4);
    let mut position_stream = Vec::with_capacity(postings.len() * 2);
    for posting in postings {
        push_varint(&mut metadata_stream, u64::from(posting.doc_length));
        push_varint(&mut metadata_stream, u64::from(posting.doc_distinct));
        push_varint(&mut metadata_stream, posting.packed_positions.len() as u64);
        for packed in &posting.packed_positions {
            push_varint(&mut position_stream, u64::from(*packed));
        }
    }
    push_varint(&mut out, document_stream.len() as u64);
    push_varint(&mut out, metadata_stream.len() as u64);
    out.extend_from_slice(&document_stream);
    out.extend_from_slice(&metadata_stream);
    out.extend_from_slice(&position_stream);
    out
}

/// Encode the impact-order sidecar without copying document lengths,
/// distinct counts or positions. Exact posting data remains solely in the
/// document-order namespace and is probed only for blocks that survive the
/// block-max gate.
pub(crate) fn encode_compact_impact_block(
    postings: &[NumericBlockPosting],
    max_impact: u16,
    max_rank: f32,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + postings.len() * 2);
    out.push(COMPACT_IMPACT_BLOCK_VERSION);
    push_varint(&mut out, postings.len() as u64);
    push_varint(&mut out, u64::from(max_impact));
    push_varint(&mut out, u64::from(max_rank.to_bits()));
    let mut document_stream = Vec::with_capacity(postings.len() * 2);
    let mut deltas = Vec::with_capacity(BP128_FRAME_VALUES);
    let mut previous = 0u64;
    for frame in postings.chunks(BP128_FRAME_VALUES) {
        deltas.clear();
        for posting in frame {
            let current = i64::try_from(posting.document_id)
                .expect("full-text document id exceeds signed delta codec");
            let previous_signed =
                i64::try_from(previous).expect("full-text document id exceeds signed delta codec");
            deltas.push(zigzag_encode(current - previous_signed));
            previous = posting.document_id;
        }
        pack_bp128_frame(&deltas, &mut document_stream);
    }
    push_varint(&mut out, document_stream.len() as u64);
    out.extend_from_slice(&document_stream);
    out
}

/// Byte-identical sibling of [`encode_compact_impact_block`] taking only
/// what the block actually stores: the impact-ordered document ids. The
/// single-pass build accumulates (bucket, rank, id) triples instead of full
/// postings, and this keeps the emitted bytes exactly what the two-pass
/// build wrote.
pub(crate) fn encode_compact_impact_block_from_ids(
    document_ids: &[u64],
    max_impact: u16,
    max_rank: f32,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + document_ids.len() * 2);
    out.push(COMPACT_IMPACT_BLOCK_VERSION);
    push_varint(&mut out, document_ids.len() as u64);
    push_varint(&mut out, u64::from(max_impact));
    push_varint(&mut out, u64::from(max_rank.to_bits()));
    let mut document_stream = Vec::with_capacity(document_ids.len() * 2);
    let mut deltas = Vec::with_capacity(BP128_FRAME_VALUES);
    let mut previous = 0u64;
    for frame in document_ids.chunks(BP128_FRAME_VALUES) {
        deltas.clear();
        for document_id in frame {
            let current = i64::try_from(*document_id)
                .expect("full-text document id exceeds signed delta codec");
            let previous_signed =
                i64::try_from(previous).expect("full-text document id exceeds signed delta codec");
            deltas.push(zigzag_encode(current - previous_signed));
            previous = *document_id;
        }
        pack_bp128_frame(&deltas, &mut document_stream);
    }
    push_varint(&mut out, document_stream.len() as u64);
    out.extend_from_slice(&document_stream);
    out
}

pub(crate) fn compact_impact_block_header(bytes: &[u8]) -> Option<CompactImpactBlockHeader> {
    if bytes.first().copied()? != COMPACT_IMPACT_BLOCK_VERSION {
        return None;
    }
    let mut cursor = 1usize;
    let doc_count = read_varint(bytes, &mut cursor)? as usize;
    if doc_count > bytes.len().saturating_mul(BP128_FRAME_VALUES) {
        return None;
    }
    let max_impact = read_varint(bytes, &mut cursor)? as u16;
    let max_rank = f32::from_bits(read_varint(bytes, &mut cursor)? as u32);
    let document_stream_bytes = read_varint(bytes, &mut cursor)? as usize;
    bytes.get(cursor..cursor.checked_add(document_stream_bytes)?)?;
    Some(CompactImpactBlockHeader {
        doc_count,
        max_impact,
        max_rank,
        body: cursor,
        document_stream_bytes,
    })
}

pub(crate) fn visit_compact_impact_block(
    bytes: &[u8],
    mut visit: impl FnMut(u64) -> Result<bool>,
) -> Result<bool> {
    let corrupt = || BicDbError::PagedStorage("corrupt compact impact block".to_string());
    let header = compact_impact_block_header(bytes).ok_or_else(corrupt)?;
    crate::fts_format::record_posting_block_read(bytes.len(), header.doc_count);
    let end = header
        .body
        .checked_add(header.document_stream_bytes)
        .ok_or_else(corrupt)?;
    let stream = bytes.get(header.body..end).ok_or_else(corrupt)?;
    let mut cursor = 0usize;
    let mut previous = 0u64;
    let mut decoded = 0usize;
    let mut frame = Vec::with_capacity(BP128_FRAME_VALUES);
    while decoded < header.doc_count {
        unpack_bp128_frame(stream, &mut cursor, &mut frame).ok_or_else(corrupt)?;
        if decoded.saturating_add(frame.len()) > header.doc_count {
            return Err(corrupt());
        }
        for delta in &frame {
            let previous_signed = i64::try_from(previous).map_err(|_| corrupt())?;
            let current = previous_signed
                .checked_add(zigzag_decode(*delta))
                .filter(|value| *value >= 0)
                .ok_or_else(corrupt)?;
            previous = current as u64;
            decoded += 1;
            if !visit(previous)? {
                return Ok(false);
            }
        }
    }
    if cursor != stream.len() {
        return Err(corrupt());
    }
    Ok(true)
}

fn numeric_posting_block_header_prefix(bytes: &[u8]) -> Option<NumericPostingBlockHeader> {
    let version = *bytes.first()?;
    let mut cursor = 1usize;
    let (ascending, flags) = if matches!(
        version,
        NUMERIC_POSTING_BLOCK_VERSION
            | NUMERIC_POSTING_BLOCK_VERSION_V3
            | NUMERIC_POSTING_BLOCK_VERSION_V2
    ) {
        let flags = *bytes.get(cursor)?;
        cursor += 1;
        (flags & NUMERIC_POSTING_IDS_ASCENDING != 0, flags)
    } else if version == NUMERIC_POSTING_BLOCK_VERSION_V1 {
        (false, 0)
    } else {
        return None;
    };
    if flags & !NUMERIC_POSTING_IDS_ASCENDING != 0 {
        return None;
    }
    let doc_count = read_varint(bytes, &mut cursor)? as usize;
    if doc_count > bytes.len().saturating_mul(BP128_FRAME_VALUES) {
        return None;
    }
    let max_impact = read_varint(bytes, &mut cursor)? as u16;
    let max_rank = f32::from_bits(read_varint(bytes, &mut cursor)? as u32);
    let (max_term_frequency, weight_mask) = if matches!(
        version,
        NUMERIC_POSTING_BLOCK_VERSION | NUMERIC_POSTING_BLOCK_VERSION_V3
    ) {
        let frequency = read_varint(bytes, &mut cursor)? as u32;
        let mask = *bytes.get(cursor)?;
        cursor += 1;
        (frequency, mask)
    } else {
        (u32::MAX, 0x0F)
    };
    let document_stream_bytes = if matches!(
        version,
        NUMERIC_POSTING_BLOCK_VERSION
            | NUMERIC_POSTING_BLOCK_VERSION_V3
            | NUMERIC_POSTING_BLOCK_VERSION_V2
    ) {
        read_varint(bytes, &mut cursor)? as usize
    } else {
        0
    };
    let metadata_stream_bytes = if version == NUMERIC_POSTING_BLOCK_VERSION {
        read_varint(bytes, &mut cursor)? as usize
    } else {
        0
    };
    Some(NumericPostingBlockHeader {
        version,
        doc_count,
        max_impact,
        max_rank,
        max_term_frequency,
        weight_mask,
        body: cursor,
        document_stream_bytes,
        metadata_stream_bytes,
        ascending,
    })
}

/// Rank metadata is wholly contained in the small prefix before the compressed
/// document stream. Shallow BM25 seeks use this parser on a bounded heap read;
/// full decoding still validates the declared stream length below.
pub(crate) fn numeric_posting_block_rank_metadata(bytes: &[u8]) -> Option<(u32, u8)> {
    let header = numeric_posting_block_header_prefix(bytes)?;
    Some((header.max_term_frequency, header.weight_mask))
}

pub(crate) fn numeric_posting_block_header(bytes: &[u8]) -> Option<NumericPostingBlockHeader> {
    let header = numeric_posting_block_header_prefix(bytes)?;
    let streams = header
        .document_stream_bytes
        .checked_add(header.metadata_stream_bytes)?;
    bytes.get(header.body..header.body.checked_add(streams)?)?;
    Some(header)
}

pub(crate) fn visit_numeric_posting_block(
    bytes: &[u8],
    positions: &mut Vec<u16>,
    mut visit: impl FnMut(u64, u32, u32, &[u16]) -> Result<bool>,
) -> Result<bool> {
    let corrupt = || BicDbError::PagedStorage("corrupt numeric posting block".to_string());
    let header = numeric_posting_block_header(bytes).ok_or_else(corrupt)?;
    crate::fts_format::record_posting_block_read(bytes.len(), header.doc_count);
    if matches!(
        header.version,
        NUMERIC_POSTING_BLOCK_VERSION
            | NUMERIC_POSTING_BLOCK_VERSION_V3
            | NUMERIC_POSTING_BLOCK_VERSION_V2
    ) {
        let document_end = header
            .body
            .checked_add(header.document_stream_bytes)
            .ok_or_else(corrupt)?;
        let document_bytes = bytes.get(header.body..document_end).ok_or_else(corrupt)?;
        let metadata_end = document_end
            .checked_add(header.metadata_stream_bytes)
            .ok_or_else(corrupt)?;
        // v4 keeps positions in their own stream after the metadata; v2/v3
        // interleave them, which the shared metadata cursor expresses as
        // "positions follow their posting's metadata".
        let split = header.version == NUMERIC_POSTING_BLOCK_VERSION;
        let mut document_cursor = 0usize;
        let mut metadata_cursor = document_end;
        let mut position_cursor = metadata_end;
        let mut previous = 0u64;
        let mut decoded = 0usize;
        let mut frame = Vec::with_capacity(BP128_FRAME_VALUES);
        while decoded < header.doc_count {
            unpack_bp128_frame(document_bytes, &mut document_cursor, &mut frame)
                .ok_or_else(corrupt)?;
            if decoded.saturating_add(frame.len()) > header.doc_count {
                return Err(corrupt());
            }
            for delta in &frame {
                let document_id = if header.ascending {
                    previous.checked_add(*delta).ok_or_else(corrupt)?
                } else {
                    let previous_signed = i64::try_from(previous).map_err(|_| corrupt())?;
                    let current = previous_signed
                        .checked_add(zigzag_decode(*delta))
                        .filter(|value| *value >= 0)
                        .ok_or_else(corrupt)?;
                    current as u64
                };
                let doc_length =
                    read_varint(bytes, &mut metadata_cursor).ok_or_else(corrupt)? as u32;
                let doc_distinct =
                    read_varint(bytes, &mut metadata_cursor).ok_or_else(corrupt)? as u32;
                let position_count =
                    read_varint(bytes, &mut metadata_cursor).ok_or_else(corrupt)? as usize;
                let cursor = if split {
                    &mut position_cursor
                } else {
                    &mut metadata_cursor
                };
                positions.clear();
                for _ in 0..position_count {
                    positions.push(read_varint(bytes, cursor).ok_or_else(corrupt)? as u16);
                }
                if !visit(document_id, doc_length, doc_distinct, positions)? {
                    return Ok(false);
                }
                previous = document_id;
                decoded += 1;
            }
        }
        if document_cursor != document_bytes.len() {
            return Err(corrupt());
        }
        if split && metadata_cursor != metadata_end {
            return Err(corrupt());
        }
        return Ok(true);
    }
    let mut cursor = header.body;
    let mut previous = 0i64;
    for _ in 0..header.doc_count {
        let delta = read_varint(bytes, &mut cursor).ok_or_else(corrupt)?;
        let document_id = previous
            .checked_add(zigzag_decode(delta))
            .filter(|value| *value >= 0)
            .ok_or_else(corrupt)?;
        let doc_length = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u32;
        let doc_distinct = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u32;
        let position_count = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
        positions.clear();
        for _ in 0..position_count {
            positions.push(read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u16);
        }
        if !visit(document_id as u64, doc_length, doc_distinct, positions)? {
            return Ok(false);
        }
        previous = document_id;
    }
    Ok(true)
}

pub(crate) fn decode_numeric_posting_block(bytes: &[u8]) -> Result<Vec<NumericBlockPosting>> {
    let header = numeric_posting_block_header(bytes)
        .ok_or_else(|| BicDbError::PagedStorage("corrupt numeric posting block".to_string()))?;
    let mut postings = Vec::with_capacity(header.doc_count);
    let mut positions = Vec::new();
    visit_numeric_posting_block(
        bytes,
        &mut positions,
        |document_id, doc_length, doc_distinct, packed_positions| {
            postings.push(NumericBlockPosting {
                document_id,
                doc_length,
                doc_distinct,
                packed_positions: packed_positions.to_vec(),
            });
            Ok(true)
        },
    )?;
    Ok(postings)
}

pub(crate) fn decode_numeric_posting_block_scores(
    bytes: &[u8],
) -> Result<Vec<NumericBlockScorePosting>> {
    let corrupt = || BicDbError::PagedStorage("corrupt numeric posting block".to_string());
    let header = numeric_posting_block_header(bytes).ok_or_else(corrupt)?;
    crate::fts_format::record_posting_block_read(bytes.len(), header.doc_count);
    let mut postings = Vec::with_capacity(header.doc_count);
    if matches!(
        header.version,
        NUMERIC_POSTING_BLOCK_VERSION
            | NUMERIC_POSTING_BLOCK_VERSION_V3
            | NUMERIC_POSTING_BLOCK_VERSION_V2
    ) {
        let document_end = header
            .body
            .checked_add(header.document_stream_bytes)
            .ok_or_else(corrupt)?;
        let document_bytes = bytes.get(header.body..document_end).ok_or_else(corrupt)?;
        // v4 blocks segregate positions after the metadata stream, so the
        // score decode never reaches a position byte. v2/v3 interleave them
        // and must still skip (without decoding) each position run.
        let split = header.version == NUMERIC_POSTING_BLOCK_VERSION;
        let mut document_cursor = 0usize;
        let mut metadata_cursor = document_end;
        let mut previous = 0u64;
        let mut decoded = 0usize;
        let mut frame = Vec::with_capacity(BP128_FRAME_VALUES);
        while decoded < header.doc_count {
            unpack_bp128_frame(document_bytes, &mut document_cursor, &mut frame)
                .ok_or_else(corrupt)?;
            if decoded.saturating_add(frame.len()) > header.doc_count {
                return Err(corrupt());
            }
            for delta in &frame {
                let document_id = if header.ascending {
                    previous.checked_add(*delta).ok_or_else(corrupt)?
                } else {
                    let previous_signed = i64::try_from(previous).map_err(|_| corrupt())?;
                    previous_signed
                        .checked_add(zigzag_decode(*delta))
                        .filter(|value| *value >= 0)
                        .ok_or_else(corrupt)? as u64
                };
                let doc_length =
                    read_varint(bytes, &mut metadata_cursor).ok_or_else(corrupt)? as u32;
                let doc_distinct =
                    read_varint(bytes, &mut metadata_cursor).ok_or_else(corrupt)? as u32;
                let position_count =
                    read_varint(bytes, &mut metadata_cursor).ok_or_else(corrupt)? as usize;
                if !split {
                    skip_varints(bytes, &mut metadata_cursor, position_count)
                        .ok_or_else(corrupt)?;
                }
                postings.push(NumericBlockScorePosting {
                    document_id,
                    doc_length,
                    doc_distinct,
                    term_frequency: position_count.max(1) as u32,
                });
                previous = document_id;
                decoded += 1;
            }
        }
        if document_cursor != document_bytes.len() {
            return Err(corrupt());
        }
        return Ok(postings);
    }

    let mut cursor = header.body;
    let mut previous = 0i64;
    for _ in 0..header.doc_count {
        let delta = read_varint(bytes, &mut cursor).ok_or_else(corrupt)?;
        let document_id = previous
            .checked_add(zigzag_decode(delta))
            .filter(|value| *value >= 0)
            .ok_or_else(corrupt)?;
        let doc_length = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u32;
        let doc_distinct = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u32;
        let position_count = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
        skip_varints(bytes, &mut cursor, position_count).ok_or_else(corrupt)?;
        postings.push(NumericBlockScorePosting {
            document_id: document_id as u64,
            doc_length,
            doc_distinct,
            term_frequency: position_count.max(1) as u32,
        });
        previous = document_id;
    }
    Ok(postings)
}

/// Packed doc-terms blob. Version 2 adds four native BM25F field lengths:
/// `[2u8][doc_len][distinct][D len][C len][B len][A len]`, followed by the
/// same sorted term/position stream as v1. V1 remains readable.
pub(crate) fn encode_doc_terms(
    doc_length: u32,
    doc_distinct: u32,
    terms: &[(String, Vec<u16>)],
) -> Vec<u8> {
    let mut field_lengths = [0u32; 4];
    for (_, positions) in terms {
        if positions.is_empty() {
            field_lengths[0] = field_lengths[0].saturating_add(1);
        } else {
            for packed in positions {
                let field = ((packed >> 14) & 0x3) as usize;
                field_lengths[field] = field_lengths[field].saturating_add(1);
            }
        }
    }
    encode_doc_terms_with_field_lengths(doc_length, doc_distinct, field_lengths, terms)
}

pub(crate) fn encode_doc_terms_with_field_lengths(
    doc_length: u32,
    doc_distinct: u32,
    field_lengths: [u32; 4],
    terms: &[(String, Vec<u16>)],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + terms.len() * 12);
    out.push(2u8);
    push_varint(&mut out, u64::from(doc_length));
    push_varint(&mut out, u64::from(doc_distinct));
    for length in field_lengths {
        push_varint(&mut out, u64::from(length));
    }
    for (term, positions) in terms {
        push_varint(&mut out, term.len() as u64);
        out.extend_from_slice(term.as_bytes());
        push_varint(&mut out, positions.len() as u64);
        for packed in positions {
            push_varint(&mut out, u64::from(*packed));
        }
    }
    out
}

pub(crate) fn decode_doc_terms(bytes: &[u8]) -> Result<(u32, u32, Vec<(String, Vec<u16>)>)> {
    let corrupt = || BicDbError::PagedStorage("corrupt doc-terms blob".to_string());
    let version = bytes.first().copied().ok_or_else(corrupt)?;
    if !matches!(version, 1 | 2) {
        return Err(corrupt());
    }
    let mut cursor = 1usize;
    let doc_length = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u32;
    let doc_distinct = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u32;
    if version == 2 {
        for _ in 0..4 {
            read_varint(bytes, &mut cursor).ok_or_else(corrupt)?;
        }
    }
    let mut terms = Vec::new();
    while cursor < bytes.len() {
        let term_len = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
        let term = bytes.get(cursor..cursor + term_len).ok_or_else(corrupt)?;
        cursor += term_len;
        let term = String::from_utf8(term.to_vec()).map_err(|_| corrupt())?;
        let pos_count = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
        let mut positions = Vec::with_capacity(pos_count);
        for _ in 0..pos_count {
            positions.push(read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u16);
        }
        terms.push((term, positions));
    }
    Ok((doc_length, doc_distinct, terms))
}

pub(crate) fn decode_doc_terms_header(bytes: &[u8]) -> Result<(u32, u32)> {
    let corrupt = || BicDbError::PagedStorage("corrupt doc-terms blob".to_string());
    if !matches!(bytes.first(), Some(1 | 2)) {
        return Err(corrupt());
    }
    let mut cursor = 1usize;
    let doc_length = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u32;
    let doc_distinct = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u32;
    Ok((doc_length, doc_distinct))
}

pub(crate) fn decode_doc_term_statistics(
    bytes: &[u8],
) -> Result<crate::fts_format::FullTextDocumentStatistics> {
    let corrupt = || BicDbError::PagedStorage("corrupt doc-terms blob".to_string());
    if bytes.first() == Some(&2u8) {
        let mut cursor = 1usize;
        let document_length = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u32;
        let distinct_terms = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u32;
        let mut field_lengths = [0u32; 4];
        for length in &mut field_lengths {
            *length = read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u32;
        }
        return Ok(crate::fts_format::FullTextDocumentStatistics {
            document_length,
            distinct_terms,
            field_lengths,
        });
    }
    let (document_length, distinct_terms, terms) = decode_doc_terms(bytes)?;
    let mut field_lengths = [0u32; 4];
    for (_, positions) in terms {
        for packed in positions {
            let field = ((packed >> 14) & 0x3) as usize;
            field_lengths[field] = field_lengths[field].saturating_add(1);
        }
    }
    Ok(crate::fts_format::FullTextDocumentStatistics {
        document_length,
        distinct_terms,
        field_lengths,
    })
}

fn index_entry_key(index: &str, encoded_key: &[u8], pk: &str) -> Vec<u8> {
    let mut key = index_prefix(index);
    for &byte in encoded_key {
        key.push(byte);
        if byte == 0 {
            key.push(0xFF);
        }
    }
    key.extend_from_slice(&[0, 0]);
    key.extend_from_slice(pk.as_bytes());
    key
}

/// Split a stored index-entry key back into `(encoded_key, pk)`.
fn decode_index_entry_key(index: &str, key: &[u8]) -> Result<(Vec<u8>, String)> {
    // Delegate to the format-aware decoder and REFUSE v3 entries: the legacy
    // v2 walk would silently misparse an intern id (its 8 BE bytes routinely
    // contain the 0x00 0x00 terminator sequence), yielding garbage keys and
    // pks. Callers that can legitimately see v3 entries use the refs scans
    // or the internal resolver.
    match decode_index_entry_key_any(index, key)? {
        (encoded, IndexEntryRef::Pk(pk)) => Ok((encoded, pk)),
        (_, IndexEntryRef::Intern(id)) => Err(BicDbError::Corruption {
            path: std::path::PathBuf::from("<paged storage>"),
            message: format!(
                "index entry for `{index}`: v2 decode reached a v3 entry (intern id {id}); \
                 this code path was not converted for intern-keyed indexes"
            ),
        }),
    }
}

/// Key prefix identifying a collection.
fn collection_prefix(collection: &str) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + collection.len());
    prefix.extend_from_slice(&durable_name_length(collection));
    prefix.extend_from_slice(collection.as_bytes());
    prefix
}

fn durable_name_length(name: &str) -> [u8; 2] {
    u16::try_from(name.len())
        .expect("durable collection and index names are validated before key encoding")
        .to_be_bytes()
}

/// Full key for one record.
fn record_key(collection: &str, id: &str) -> Vec<u8> {
    let mut key = collection_prefix(collection);
    key.extend_from_slice(id.as_bytes());
    key
}

/// Binary row format tag. A legacy JSON row starts with `{` (0x7B), so the
/// tag byte disambiguates the two encodings for free: rows written before
/// this format decode through the JSON path forever, rows written from now
/// on take the binary one. No migration step, no format flag.
const BINARY_RECORD_TAG: u8 = 0x01;
/// A zstd-compressed [`BINARY_RECORD_TAG`] frame:
/// `[0x02][u32 raw_len LE][zstd bytes]`. Read support is unconditional
/// (write-new-read-both, the no-rebuild rule); writing is opt-in via
/// [`PagedRecords::set_value_compression`].
const COMPRESSED_RECORD_TAG: u8 = 0x02;
/// Values below this stay uncompressed: small rows rarely win and always
/// pay decode latency.
const VALUE_COMPRESSION_MIN_BYTES: usize = 1_024;

/// Encode a record for storage: version-tagged binary envelope.
///
/// Field content that is inherently structured (metadata, geometry) stays as
/// embedded JSON bytes — its parse cost is intrinsic to handing the caller a
/// `Value`. What the binary envelope removes is everything AROUND it: the
/// full-document lex to find field boundaries (identity decodes now skip by
/// length prefix instead of lexing megabytes of metadata), per-element JSON
/// float parsing for vectors (raw little-endian f32s), and re-serializing
/// the envelope on every write.
///
/// This intentionally gives up "paged bytes == segment JSON bytes";
/// migration between engines re-encodes through the Record API.
fn encode_record(record: &Record) -> Result<Vec<u8>> {
    let metadata = if record.metadata.is_null() {
        None
    } else {
        Some(serde_json::to_vec(&record.metadata)?)
    };
    let geometry = record
        .geometry
        .as_ref()
        .map(serde_json::to_vec)
        .transpose()?;
    let mut out = Vec::with_capacity(
        16 + record.id.len()
            + record.vector.as_ref().map_or(0, |vector| vector.len() * 4)
            + metadata.as_ref().map_or(0, Vec::len)
            + geometry.as_ref().map_or(0, Vec::len)
            + record.payload.as_ref().map_or(0, Vec::len),
    );
    out.push(BINARY_RECORD_TAG);
    let mut flags = 0u8;
    if record.vector.is_some() {
        flags |= 1;
    }
    if geometry.is_some() {
        flags |= 1 << 1;
    }
    if record.timestamp.is_some() {
        flags |= 1 << 2;
    }
    if record.payload.is_some() {
        flags |= 1 << 3;
    }
    if metadata.is_some() {
        flags |= 1 << 4;
    }
    out.push(flags);
    out.extend_from_slice(&(record.id.len() as u32).to_le_bytes());
    out.extend_from_slice(record.id.as_bytes());
    if let Some(vector) = &record.vector {
        out.extend_from_slice(&(vector.len() as u32).to_le_bytes());
        for component in vector {
            out.extend_from_slice(&component.to_le_bytes());
        }
    }
    if let Some(geometry) = &geometry {
        out.extend_from_slice(&(geometry.len() as u32).to_le_bytes());
        out.extend_from_slice(geometry);
    }
    if let Some(timestamp) = record.timestamp {
        out.extend_from_slice(&timestamp.to_le_bytes());
    }
    if let Some(payload) = &record.payload {
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(payload);
    }
    if let Some(metadata) = &metadata {
        out.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
        out.extend_from_slice(metadata);
    }
    Ok(out)
}

struct BinaryRecordCursor<'bytes> {
    bytes: &'bytes [u8],
    position: usize,
}

impl<'bytes> BinaryRecordCursor<'bytes> {
    fn corrupt() -> BicDbError {
        BicDbError::PagedStorage("truncated binary record".to_string())
    }
    fn take(&mut self, count: usize) -> Result<&'bytes [u8]> {
        let slice = self
            .bytes
            .get(self.position..self.position + count)
            .ok_or_else(Self::corrupt)?;
        self.position += count;
        Ok(slice)
    }
    fn take_u32(&mut self) -> Result<usize> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize)
    }
    fn take_prefixed(&mut self) -> Result<&'bytes [u8]> {
        let len = self.take_u32()?;
        self.take(len)
    }
}

/// Decode a binary row. `identity_only` skips metadata entirely — a length
/// jump, not a parse — which is what makes identity scans proportional to
/// identity bytes.
fn decode_binary_record(bytes: &[u8], identity_only: bool) -> Result<Record> {
    let mut cursor = BinaryRecordCursor { bytes, position: 1 };
    let flags = cursor.take(1)?[0];
    let id = String::from_utf8(cursor.take_prefixed()?.to_vec())
        .map_err(|error| BicDbError::PagedStorage(format!("binary record id: {error}")))?;
    let vector = if flags & 1 != 0 {
        let count = cursor.take_u32()?;
        let raw = cursor.take(count * 4)?;
        Some(
            raw.chunks_exact(4)
                .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
                .collect(),
        )
    } else {
        None
    };
    let geometry = if flags & (1 << 1) != 0 {
        Some(serde_json::from_slice(cursor.take_prefixed()?)?)
    } else {
        None
    };
    let timestamp = if flags & (1 << 2) != 0 {
        Some(i64::from_le_bytes(cursor.take(8)?.try_into().unwrap()))
    } else {
        None
    };
    let payload = if flags & (1 << 3) != 0 {
        Some(cursor.take_prefixed()?.to_vec())
    } else {
        None
    };
    let metadata = if identity_only || flags & (1 << 4) == 0 {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(cursor.take_prefixed()?)?
    };
    Ok(Record {
        id,
        vector,
        metadata,
        geometry,
        timestamp,
        payload: if identity_only { None } else { payload },
    })
}

fn ensure_record_size(bytes: &[u8]) -> Result<()> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(BicDbError::PagedStorage(format!(
            "stored record is {} bytes, above the {MAX_RECORD_BYTES}-byte ceiling",
            bytes.len()
        )));
    }
    Ok(())
}

/// Decode only a record's identity fields. Binary rows jump straight past
/// the metadata by length prefix; legacy JSON rows still lex it to find the
/// field boundaries (unavoidable in JSON), but copy and tree-parse nothing.
fn decode_identity(bytes: &[u8]) -> Result<Record> {
    ensure_record_size(bytes)?;
    if bytes.first() == Some(&BINARY_RECORD_TAG) {
        return decode_binary_record(bytes, true);
    }
    if bytes.first() == Some(&COMPRESSED_RECORD_TAG) {
        return decode_identity(&decompress_record_value(bytes)?);
    }
    #[derive(serde::Deserialize)]
    struct Identity {
        id: String,
        #[serde(default)]
        vector: Option<Vec<f32>>,
        #[serde(default)]
        geometry: Option<crate::geometry::Geometry>,
        #[serde(default)]
        timestamp: Option<i64>,
    }
    let identity: Identity = serde_json::from_slice(bytes)?;
    Ok(Record {
        id: identity.id,
        vector: identity.vector,
        metadata: serde_json::Value::Null,
        geometry: identity.geometry,
        timestamp: identity.timestamp,
        payload: None,
    })
}

fn decode_record(bytes: &[u8]) -> Result<Record> {
    ensure_record_size(bytes)?;
    if bytes.first() == Some(&BINARY_RECORD_TAG) {
        return decode_binary_record(bytes, false);
    }
    if bytes.first() == Some(&COMPRESSED_RECORD_TAG) {
        return decode_record(&decompress_record_value(bytes)?);
    }
    let stored: StoredRecord = serde_json::from_slice(bytes)?;
    stored.to_record()
}

#[cfg(feature = "compression")]
fn compress_record_value(raw: &[u8]) -> Result<Vec<u8>> {
    let compressed = zstd::bulk::compress(raw, 3)
        .map_err(|error| BicDbError::PagedStorage(format!("value compression: {error}")))?;
    // Keep the smaller form: incompressible values stay raw.
    if compressed.len() + 5 >= raw.len() {
        return Ok(raw.to_vec());
    }
    let mut out = Vec::with_capacity(5 + compressed.len());
    out.push(COMPRESSED_RECORD_TAG);
    out.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    out.extend_from_slice(&compressed);
    Ok(out)
}

#[cfg(not(feature = "compression"))]
fn compress_record_value(raw: &[u8]) -> Result<Vec<u8>> {
    Ok(raw.to_vec())
}

fn decompress_record_value(bytes: &[u8]) -> Result<Vec<u8>> {
    let declared = bytes
        .get(1..5)
        .map(|len| u32::from_le_bytes(len.try_into().unwrap()) as usize)
        .ok_or_else(|| {
            BicDbError::PagedStorage("truncated compressed record header".to_string())
        })?;
    if declared > MAX_RECORD_BYTES {
        return Err(BicDbError::PagedStorage(format!(
            "compressed record declares {declared} bytes, above the {MAX_RECORD_BYTES}-byte ceiling"
        )));
    }
    #[cfg(feature = "compression")]
    {
        let raw = zstd::bulk::decompress(&bytes[5..], declared)
            .map_err(|error| BicDbError::PagedStorage(format!("value decompression: {error}")))?;
        if raw.len() != declared {
            return Err(BicDbError::PagedStorage(format!(
                "compressed record decoded to {} bytes, declared {declared}",
                raw.len()
            )));
        }
        Ok(raw)
    }
    #[cfg(not(feature = "compression"))]
    {
        Err(BicDbError::PagedStorage(
            "this build lacks the `compression` feature needed to read compressed records"
                .to_string(),
        ))
    }
}

/// Translate a page-engine error, preserving the distinction between a
/// concurrency conflict (retry) and corruption (do not).
fn page_error(error: bicdb_page::PageError) -> BicDbError {
    // A conflict is not a failure of the storage layer — it is a concurrency
    // outcome the caller should retry. Collapsing it into a generic storage
    // error would make every retryable conflict look like a fault.
    if error.is_write_conflict() {
        return BicDbError::TransactionConflict(error.to_string());
    }
    if error.is_version_chain_fault() {
        return BicDbError::VersionChain(error.to_string());
    }
    if error.is_corruption() {
        return BicDbError::Corruption {
            path: std::path::PathBuf::from("<paged storage>"),
            message: error.to_string(),
        };
    }
    BicDbError::PagedStorage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    fn records(dir: &TempDir) -> PagedRecords {
        PagedRecords::open(
            dir.path(),
            PagedRecordsOptions {
                page_size: 4096,
                buffer_pool_bytes: 4 * 1024 * 1024,
                read_ahead_queue_pages: 1_024,
                fsync: false,
                wal_max_bytes: 8 * 1024 * 1024,
                wal_segment_bytes: 8 * 1024 * 1024,
                extent_bytes: 0,
                accept_legacy_meta: false,
            },
        )
        .unwrap()
    }

    fn record(id: &str, body: &str) -> Record {
        Record::new(id).with_metadata(json!({ "body": body, "n": id.len() }))
    }

    #[test]
    fn stored_text_pages_use_one_snapshot_and_decode_envelopes() {
        let dir = TempDir::new().unwrap();
        let store = records(&dir);
        let first = crate::fts_format::encode_stored_text(b"first").unwrap();
        let second = crate::fts_format::encode_stored_text(b"second").unwrap();

        let (xid, _) = store.begin();
        store
            .put_full_text_stored_text(xid, "physical-v1", 0, &first)
            .unwrap();
        store.commit(xid).unwrap();
        let pinned = store.latest_snapshot();

        let (xid, _) = store.begin();
        store
            .put_full_text_stored_text(xid, "physical-v1", 1, &second)
            .unwrap();
        store.commit(xid).unwrap();

        let (old_rows, old_next) = store
            .full_text_stored_text_page(&pinned, "physical-v1", None, 1)
            .unwrap();
        assert_eq!(old_rows, vec![(0, b"first".to_vec())]);
        assert_eq!(old_next, None);

        let (current_rows, current_next) = store
            .full_text_stored_text_page(&store.latest_snapshot(), "physical-v1", None, 1)
            .unwrap();
        assert_eq!(current_rows, vec![(0, b"first".to_vec())]);
        assert_eq!(current_next, Some(0));
    }

    #[test]
    fn stored_text_pages_reject_malformed_keys_and_values() {
        let value_dir = TempDir::new().unwrap();
        let value_store = records(&value_dir);
        let (xid, _) = value_store.begin();
        value_store
            .put_full_text_stored_text(xid, "physical-v1", 0, b"not-an-envelope")
            .unwrap();
        value_store.commit(xid).unwrap();
        assert!(value_store
            .full_text_stored_text_page(&value_store.latest_snapshot(), "physical-v1", None, 1,)
            .is_err());

        let key_dir = TempDir::new().unwrap();
        let key_store = records(&key_dir);
        let mut malformed_key = index_stored_text_prefix("physical-v1");
        malformed_key.push(0);
        let encoded = crate::fts_format::encode_stored_text(b"preview").unwrap();
        let (xid, _) = key_store.begin();
        key_store
            .store()
            .put(xid, &malformed_key, &encoded)
            .unwrap();
        key_store.commit(xid).unwrap();
        assert!(key_store
            .full_text_stored_text_page(&key_store.latest_snapshot(), "physical-v1", None, 1,)
            .is_err());
    }

    #[test]
    fn stored_text_pages_reject_duplicate_and_out_of_order_document_ids() {
        assert!(validate_full_text_stored_text_document_id(Some(7), 7).is_err());
        assert!(validate_full_text_stored_text_document_id(Some(7), 6).is_err());
        assert!(validate_full_text_stored_text_document_id(Some(7), 8).is_ok());
    }

    #[test]
    fn raw_namespace_cleanup_pages_advance_after_deleted_keys() {
        let dir = TempDir::new().unwrap();
        let store = records(&dir);
        let (xid, _) = store.begin();
        for index in 0..5 {
            store
                .put_index_entry(xid, "shadow", &[1], &format!("row-{index}"), &[])
                .unwrap();
        }
        store.commit(xid).unwrap();

        let prefix = index_purge_prefixes("shadow").remove(0);
        let first = store
            .raw_keys_with_prefix_batch_after(
                &store.latest_snapshot(),
                &prefix,
                None,
                2,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(first.len(), 2);
        let cursor = first.last().cloned().unwrap();

        let (xid, _) = store.begin();
        for key in &first {
            store.delete_raw(xid, key).unwrap();
        }
        store.commit(xid).unwrap();

        let second = store
            .raw_keys_with_prefix_batch_after(
                &store.latest_snapshot(),
                &prefix,
                Some(&cursor),
                2,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(second.len(), 2);
        assert!(second[0] > cursor);
        let third = store
            .raw_keys_with_prefix_batch_after(
                &store.latest_snapshot(),
                &prefix,
                second.last().map(Vec::as_slice),
                2,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(third.len(), 1);
        assert!(store
            .raw_keys_with_prefix_batch_after(
                &store.latest_snapshot(),
                &prefix,
                third.last().map(Vec::as_slice),
                2,
                usize::MAX,
            )
            .unwrap()
            .is_empty());
    }

    fn numeric_posting(document_id: u64) -> NumericBlockPosting {
        NumericBlockPosting {
            document_id,
            doc_length: 200,
            doc_distinct: 30,
            packed_positions: vec![1, 17, 65],
        }
    }

    #[test]
    fn bp128_numeric_blocks_round_trip_both_orders_and_read_v1() {
        let ascending: Vec<_> = (0..300).map(|id| numeric_posting(id * 3)).collect();
        let bytes = encode_numeric_posting_block(&ascending, 65, 1.25);
        let header = numeric_posting_block_header(&bytes).unwrap();
        assert_eq!(header.version, NUMERIC_POSTING_BLOCK_VERSION);
        assert!(header.ascending);
        assert_eq!(header.doc_count, ascending.len());
        assert_eq!(decode_numeric_posting_block(&bytes).unwrap(), ascending);

        let impact = vec![
            numeric_posting(900),
            numeric_posting(2),
            numeric_posting(700),
            numeric_posting(12),
        ];
        let impact_bytes = encode_numeric_posting_block(&impact, 65, 1.25);
        assert!(
            !numeric_posting_block_header(&impact_bytes)
                .unwrap()
                .ascending
        );
        assert_eq!(decode_numeric_posting_block(&impact_bytes).unwrap(), impact);

        // Rolling-upgrade pin: a generation containing the interleaved v1
        // representation remains readable after v2 becomes the writer.
        let mut v1 = vec![NUMERIC_POSTING_BLOCK_VERSION_V1];
        push_varint(&mut v1, 2);
        push_varint(&mut v1, 7);
        push_varint(&mut v1, u64::from(0.5f32.to_bits()));
        let mut previous = 0i64;
        for posting in [numeric_posting(4), numeric_posting(11)] {
            let current = posting.document_id as i64;
            push_varint(&mut v1, zigzag_encode(current - previous));
            push_varint(&mut v1, u64::from(posting.doc_length));
            push_varint(&mut v1, u64::from(posting.doc_distinct));
            push_varint(&mut v1, posting.packed_positions.len() as u64);
            for position in posting.packed_positions {
                push_varint(&mut v1, u64::from(position));
            }
            previous = current;
        }
        assert_eq!(
            decode_numeric_posting_block(&v1)
                .unwrap()
                .iter()
                .map(|posting| posting.document_id)
                .collect::<Vec<_>>(),
            vec![4, 11]
        );
    }

    #[test]
    fn numeric_posting_score_decoder_skips_positions_without_losing_bm25_inputs() {
        let postings = vec![
            NumericBlockPosting {
                document_id: 3,
                doc_length: 240,
                doc_distinct: 91,
                packed_positions: vec![1, 4, 9, 16],
            },
            NumericBlockPosting {
                document_id: 18,
                doc_length: 12,
                doc_distinct: 7,
                packed_positions: Vec::new(),
            },
            NumericBlockPosting {
                document_id: 4_100_000,
                doc_length: 900,
                doc_distinct: 320,
                packed_positions: (0..31).collect(),
            },
        ];
        let bytes = encode_numeric_posting_block(&postings, 0, 0.0);
        let scores = decode_numeric_posting_block_scores(&bytes).unwrap();
        assert_eq!(scores.len(), postings.len());
        for (score, posting) in scores.iter().zip(&postings) {
            assert_eq!(score.document_id, posting.document_id);
            assert_eq!(score.doc_length, posting.doc_length);
            assert_eq!(score.doc_distinct, posting.doc_distinct);
            assert_eq!(
                score.term_frequency,
                posting.packed_positions.len().max(1) as u32
            );
        }
    }

    /// The v3 writer layout: positions interleaved with each posting's
    /// metadata. Generations written before the v4 split must stay readable.
    fn encode_numeric_posting_block_v3(
        postings: &[NumericBlockPosting],
        max_impact: u16,
        max_rank: f32,
    ) -> Vec<u8> {
        let mut out = vec![NUMERIC_POSTING_BLOCK_VERSION_V3];
        let ascending = postings
            .windows(2)
            .all(|pair| pair[0].document_id <= pair[1].document_id);
        out.push(if ascending {
            NUMERIC_POSTING_IDS_ASCENDING
        } else {
            0
        });
        push_varint(&mut out, postings.len() as u64);
        push_varint(&mut out, u64::from(max_impact));
        push_varint(&mut out, u64::from(max_rank.to_bits()));
        let mut max_term_frequency = 1usize;
        let mut weight_mask = 0u8;
        for posting in postings {
            max_term_frequency = max_term_frequency.max(posting.packed_positions.len().max(1));
            if posting.packed_positions.is_empty() {
                weight_mask |= 1;
            } else {
                for packed in &posting.packed_positions {
                    weight_mask |= 1 << ((packed >> 14) & 0x3);
                }
            }
        }
        push_varint(&mut out, max_term_frequency as u64);
        out.push(weight_mask);
        let mut document_stream = Vec::new();
        let mut deltas = Vec::with_capacity(BP128_FRAME_VALUES);
        let mut previous = 0u64;
        for frame in postings.chunks(BP128_FRAME_VALUES) {
            deltas.clear();
            for posting in frame {
                let delta = if ascending {
                    posting.document_id - previous
                } else {
                    zigzag_encode(posting.document_id as i64 - previous as i64)
                };
                deltas.push(delta);
                previous = posting.document_id;
            }
            pack_bp128_frame(&deltas, &mut document_stream);
        }
        push_varint(&mut out, document_stream.len() as u64);
        out.extend_from_slice(&document_stream);
        for posting in postings {
            push_varint(&mut out, u64::from(posting.doc_length));
            push_varint(&mut out, u64::from(posting.doc_distinct));
            push_varint(&mut out, posting.packed_positions.len() as u64);
            for packed in &posting.packed_positions {
                push_varint(&mut out, u64::from(*packed));
            }
        }
        out
    }

    /// The shallow BM25 seek reads only a 64-byte value prefix per block, so
    /// the rank header of every writable version must fit inside it — for a
    /// 1TB corpus this is what lets skip navigation avoid resolving payloads.
    #[test]
    fn rank_metadata_parses_from_a_bounded_value_prefix() {
        let postings: Vec<_> = (0..300)
            .map(|index| NumericBlockPosting {
                document_id: 40 + index * 811,
                doc_length: 5_000,
                doc_distinct: 900,
                packed_positions: (0..37u16).map(|p| p * 5 | (p % 4) << 14).collect(),
            })
            .collect();
        let v4 = encode_numeric_posting_block(&postings, u16::MAX, f32::MAX);
        let v3 = encode_numeric_posting_block_v3(&postings, u16::MAX, f32::MAX);
        for bytes in [v4, v3] {
            let full = numeric_posting_block_header(&bytes).unwrap();
            let prefix = &bytes[..64.min(bytes.len())];
            let (max_term_frequency, weight_mask) =
                numeric_posting_block_rank_metadata(prefix).unwrap();
            assert_eq!(max_term_frequency, full.max_term_frequency);
            assert_eq!(weight_mask, full.weight_mask);
        }
    }

    #[test]
    fn v3_interleaved_blocks_decode_identically_to_v4() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let postings: Vec<_> = (0..300)
            .map(|index| NumericBlockPosting {
                document_id: 5 + index * 97,
                doc_length: (next() % 5_000) as u32,
                doc_distinct: (next() % 900) as u32,
                packed_positions: (0..(next() % 40) as u16)
                    .map(|p| p * 3 | (p % 4) << 14)
                    .collect(),
            })
            .collect();
        let v3 = encode_numeric_posting_block_v3(&postings, 65, 1.25);
        let v4 = encode_numeric_posting_block(&postings, 65, 1.25);
        let v3_header = numeric_posting_block_header(&v3).unwrap();
        let v4_header = numeric_posting_block_header(&v4).unwrap();
        assert_eq!(v3_header.version, NUMERIC_POSTING_BLOCK_VERSION_V3);
        assert_eq!(v4_header.version, NUMERIC_POSTING_BLOCK_VERSION);
        assert_eq!(v3_header.doc_count, v4_header.doc_count);
        assert_eq!(v3_header.max_term_frequency, v4_header.max_term_frequency);
        assert_eq!(v3_header.weight_mask, v4_header.weight_mask);
        assert_eq!(decode_numeric_posting_block(&v3).unwrap(), postings);
        assert_eq!(decode_numeric_posting_block(&v4).unwrap(), postings);
        assert_eq!(
            decode_numeric_posting_block_scores(&v3).unwrap(),
            decode_numeric_posting_block_scores(&v4).unwrap()
        );
    }

    #[test]
    fn bp128_frames_round_trip_every_width() {
        let mut state = 0xD1B5_4A32_D192_ED03u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for width in 0..=64u32 {
            for count in [1usize, 7, 8, 9, 64, 127, 128] {
                let values: Vec<u64> = (0..count)
                    .map(|_| {
                        if width == 0 {
                            0
                        } else if width == 64 {
                            next()
                        } else {
                            next() & ((1u64 << width) - 1)
                        }
                    })
                    .collect();
                let mut packed = Vec::new();
                pack_bp128_frame(&values, &mut packed);
                let mut cursor = 0usize;
                let mut output = Vec::new();
                unpack_bp128_frame(&packed, &mut cursor, &mut output).unwrap();
                assert_eq!(cursor, packed.len(), "width {width} count {count}");
                assert_eq!(output, values, "width {width} count {count}");
            }
        }
    }

    #[test]
    fn skip_varints_lands_exactly_after_the_requested_run() {
        let mut state = 0xA076_1D64_78BD_642Fu64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..500 {
            let count = (next() % 40 + 1) as usize;
            let mut bytes = Vec::new();
            for _ in 0..count {
                push_varint(&mut bytes, next() % 2_000_000);
            }
            let expected = bytes.len();
            bytes.extend_from_slice(&[0xAB; 3]);
            let mut cursor = 0usize;
            skip_varints(&bytes, &mut cursor, count).unwrap();
            assert_eq!(cursor, expected);
        }
        // Exhausting the input mid-run reports failure instead of landing
        // somewhere plausible.
        let mut truncated = Vec::new();
        push_varint(&mut truncated, 300);
        let mut cursor = 0usize;
        assert!(skip_varints(&truncated, &mut cursor, 2).is_none());
    }

    #[test]
    #[ignore = "explicit ranked-search decoder microbenchmark"]
    fn bm25_score_only_decoder_microbenchmark() {
        let postings = (0..128)
            .map(|document_id| NumericBlockPosting {
                document_id,
                doc_length: 400 + document_id as u32,
                doc_distinct: 200,
                packed_positions: (0..48).collect(),
            })
            .collect::<Vec<_>>();
        let bytes = encode_numeric_posting_block(&postings, 0, 0.0);
        const ITERATIONS: usize = 20_000;

        let full_started = std::time::Instant::now();
        for _ in 0..ITERATIONS {
            std::hint::black_box(decode_numeric_posting_block(&bytes).unwrap());
        }
        let full = full_started.elapsed();
        let score_started = std::time::Instant::now();
        for _ in 0..ITERATIONS {
            std::hint::black_box(decode_numeric_posting_block_scores(&bytes).unwrap());
        }
        let score_only = score_started.elapsed();
        eprintln!(
            "BM25 posting decode x{ITERATIONS}: full_positions={full:?} score_only={score_only:?} speedup={:.2}x",
            full.as_secs_f64() / score_only.as_secs_f64()
        );
    }

    #[test]
    fn compact_impact_blocks_store_only_document_ids_and_bounds() {
        let postings: Vec<_> = (0..300)
            .map(|offset| numeric_posting(((offset * 197) % 307) as u64))
            .collect();
        let compact = encode_compact_impact_block(&postings, 61, 0.875);
        let duplicated = encode_numeric_posting_block(&postings, 61, 0.875);
        let header = compact_impact_block_header(&compact).unwrap();
        assert_eq!(header.doc_count, postings.len());
        assert_eq!(header.max_impact, 61);
        assert_eq!(header.max_rank, 0.875);
        let mut ids = Vec::new();
        assert!(visit_compact_impact_block(&compact, |document_id| {
            ids.push(document_id);
            Ok(true)
        })
        .unwrap());
        assert_eq!(
            ids,
            postings
                .iter()
                .map(|posting| posting.document_id)
                .collect::<Vec<_>>()
        );
        assert!(
            compact.len() * 3 < duplicated.len(),
            "compact={} duplicated={}",
            compact.len(),
            duplicated.len()
        );
    }

    #[test]
    fn doc_terms_v2_preserves_native_field_lengths_and_reads_v1() {
        let terms = vec![
            ("body".to_string(), vec![1, 2]),
            ("title".to_string(), vec![(3 << 14) | 1]),
        ];
        let encoded = encode_doc_terms_with_field_lengths(3, 2, [2, 0, 0, 1], &terms);
        assert_eq!(decode_doc_terms(&encoded).unwrap().2, terms);
        assert_eq!(
            decode_doc_term_statistics(&encoded).unwrap().field_lengths,
            [2, 0, 0, 1]
        );

        let mut v1 = vec![1];
        push_varint(&mut v1, 1);
        push_varint(&mut v1, 1);
        push_varint(&mut v1, 4);
        v1.extend_from_slice(b"term");
        push_varint(&mut v1, 1);
        push_varint(&mut v1, 7);
        assert_eq!(decode_doc_terms(&v1).unwrap().0, 1);
        assert_eq!(
            decode_doc_term_statistics(&v1).unwrap().field_lengths,
            [1, 0, 0, 0]
        );
    }

    #[test]
    fn records_round_trip_with_their_metadata() {
        let dir = TempDir::new().unwrap();
        let store = records(&dir);
        let (xid, _) = store.begin();
        store.put(xid, "notes", &record("n1", "hello")).unwrap();
        store.commit(xid).unwrap();

        let snapshot = store.latest_snapshot();
        let found = store.get(&snapshot, "notes", "n1").unwrap().unwrap();
        assert_eq!(found.id, "n1");
        assert_eq!(found.metadata["body"], json!("hello"));
    }

    #[test]
    fn collections_are_isolated_from_each_other() {
        let dir = TempDir::new().unwrap();
        let store = records(&dir);
        let (xid, _) = store.begin();
        store
            .put(xid, "alpha", &record("shared-id", "in-alpha"))
            .unwrap();
        store
            .put(xid, "beta", &record("shared-id", "in-beta"))
            .unwrap();
        store.commit(xid).unwrap();

        let snapshot = store.latest_snapshot();
        assert_eq!(
            store
                .get(&snapshot, "alpha", "shared-id")
                .unwrap()
                .unwrap()
                .metadata["body"],
            json!("in-alpha")
        );
        assert_eq!(
            store
                .get(&snapshot, "beta", "shared-id")
                .unwrap()
                .unwrap()
                .metadata["body"],
            json!("in-beta")
        );
    }

    #[test]
    fn a_crafted_record_id_cannot_reach_another_collection() {
        // Length-prefixing rather than delimiting is what prevents this: with a
        // delimiter, an id containing it would let one collection address
        // another's key space.
        let dir = TempDir::new().unwrap();
        let store = records(&dir);
        let (xid, _) = store.begin();
        store
            .put(xid, "secret", &record("row", "classified"))
            .unwrap();
        store.commit(xid).unwrap();

        let snapshot = store.latest_snapshot();
        // "a" + "secretrow" must not collide with "secret" + "row".
        assert!(store.get(&snapshot, "a", "secretrow").unwrap().is_none());
        assert!(store.get(&snapshot, "sec", "retrow").unwrap().is_none());
        assert!(store.get(&snapshot, "secret", "row").unwrap().is_some());
    }

    #[test]
    fn a_scan_returns_only_its_own_collection() {
        let dir = TempDir::new().unwrap();
        let store = records(&dir);
        let (xid, _) = store.begin();
        for index in 0..40 {
            store
                .put(xid, "wanted", &record(&format!("w{index:03}"), "x"))
                .unwrap();
            store
                .put(xid, "other", &record(&format!("o{index:03}"), "y"))
                .unwrap();
        }
        store.commit(xid).unwrap();

        let snapshot = store.latest_snapshot();
        let found: Vec<Record> = store
            .scan(&snapshot, "wanted")
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(found.len(), 40);
        assert!(found.iter().all(|r| r.id.starts_with('w')));
    }

    #[test]
    fn canceled_batch_scan_releases_every_page_pin() {
        let dir = TempDir::new().unwrap();
        let store = records(&dir);
        let (xid, _) = store.begin();
        for index in 0..40 {
            store
                .put(xid, "notes", &record(&format!("n{index:03}"), "body"))
                .unwrap();
        }
        store.commit(xid).unwrap();

        let cancellation = CancellationToken::uncancelable();
        let mut visited = 0usize;
        let error = store
            .for_each_batch_cancellable(
                &store.latest_snapshot(),
                "notes",
                4,
                &cancellation,
                |batch| {
                    visited += batch.len();
                    cancellation.cancel();
                    Ok(true)
                },
            )
            .unwrap_err();
        assert!(matches!(error, BicDbError::QueryCanceled));
        assert_eq!(visited, 4);
        assert_eq!(
            store.store().buffer_pool().snapshot().pinned_pages,
            0,
            "a canceled cursor retained a buffer-pool pin"
        );
    }

    #[test]
    fn deletes_are_visible_and_reported() {
        let dir = TempDir::new().unwrap();
        let store = records(&dir);
        let (xid, _) = store.begin();
        store.put(xid, "notes", &record("n1", "v")).unwrap();
        store.commit(xid).unwrap();

        let (deleter, _) = store.begin();
        assert!(store.delete(deleter, "notes", "n1").unwrap());
        assert!(!store.delete(deleter, "notes", "missing").unwrap());
        store.commit(deleter).unwrap();

        assert!(store
            .get(&store.latest_snapshot(), "notes", "n1")
            .unwrap()
            .is_none());
    }

    #[test]
    fn a_snapshot_is_unaffected_by_later_commits() {
        let dir = TempDir::new().unwrap();
        let store = records(&dir);
        let (setup, _) = store.begin();
        store
            .put(setup, "notes", &record("n1", "original"))
            .unwrap();
        store.commit(setup).unwrap();

        let (_reader, snapshot) = store.begin();

        let (writer, _) = store.begin();
        store
            .put(writer, "notes", &record("n1", "changed"))
            .unwrap();
        store.commit(writer).unwrap();

        assert_eq!(
            store
                .get(&snapshot, "notes", "n1")
                .unwrap()
                .unwrap()
                .metadata["body"],
            json!("original"),
            "snapshot isolation did not survive the record adapter"
        );
    }

    #[test]
    fn a_write_conflict_surfaces_as_a_retryable_error() {
        let dir = TempDir::new().unwrap();
        let store = records(&dir);
        let (setup, _) = store.begin();
        store.put(setup, "notes", &record("n1", "v")).unwrap();
        store.commit(setup).unwrap();

        let (first, _) = store.begin();
        let (second, _) = store.begin();
        store.put(first, "notes", &record("n1", "first")).unwrap();
        store.commit(first).unwrap();

        let error = store
            .put(second, "notes", &record("n1", "second"))
            .unwrap_err();
        assert!(
            matches!(error, BicDbError::TransactionConflict(_)),
            "a write conflict must not be reported as corruption or IO: {error:?}"
        );
    }

    #[test]
    fn records_survive_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let store = records(&dir);
            let (xid, _) = store.begin();
            for index in 0..200 {
                store
                    .put(xid, "notes", &record(&format!("n{index:04}"), "durable"))
                    .unwrap();
            }
            store.commit(xid).unwrap();
        }

        let store = records(&dir);
        let snapshot = store.latest_snapshot();
        for index in 0..200 {
            assert!(
                store
                    .get(&snapshot, "notes", &format!("n{index:04}"))
                    .unwrap()
                    .is_some(),
                "record {index} lost across reopen"
            );
        }
    }

    #[test]
    fn resident_memory_stays_bounded_across_a_large_collection() {
        let dir = TempDir::new().unwrap();
        let store = PagedRecords::open(
            dir.path(),
            PagedRecordsOptions {
                page_size: 4096,
                buffer_pool_bytes: 2 * 1024 * 1024,
                read_ahead_queue_pages: 1_024,
                fsync: false,
                wal_max_bytes: 8 * 1024 * 1024,
                wal_segment_bytes: 8 * 1024 * 1024,
                extent_bytes: 0,
                accept_legacy_meta: false,
            },
        )
        .unwrap();

        let (xid, _) = store.begin();
        for index in 0..4_000 {
            store
                .put(
                    xid,
                    "big",
                    &record(&format!("r{index:06}"), &"z".repeat(300)),
                )
                .unwrap();
        }
        store.commit(xid).unwrap();

        assert!(
            store.resident_bytes() <= 2 * 1024 * 1024,
            "record storage exceeded its buffer pool budget: {} bytes",
            store.resident_bytes()
        );
        // And the data is all still readable.
        let snapshot = store.latest_snapshot();
        assert!(store.get(&snapshot, "big", "r000000").unwrap().is_some());
        assert!(store.get(&snapshot, "big", "r003999").unwrap().is_some());
    }

    #[test]
    fn binary_codec_round_trips_every_field_combination() {
        let full = Record {
            id: "row-1".to_string(),
            vector: Some(vec![1.5, -2.25, 0.0]),
            metadata: serde_json::json!({"title": "études\u{0}", "year": 2020, "nested": {"a": [1, 2]}}),
            geometry: Some(crate::geometry::Geometry::point(1.0, 2.0).unwrap()),
            timestamp: Some(-42),
            payload: Some(vec![0, 255, 7]),
        };
        let sparse = Record {
            id: String::new(),
            vector: None,
            metadata: serde_json::Value::Null,
            geometry: None,
            timestamp: None,
            payload: None,
        };
        for record in [&full, &sparse] {
            let bytes = encode_record(record).unwrap();
            assert_eq!(bytes[0], BINARY_RECORD_TAG);
            let decoded = decode_record(&bytes).unwrap();
            assert_eq!(&decoded, record);
            let identity = decode_identity(&bytes).unwrap();
            assert_eq!(identity.id, record.id);
            assert_eq!(identity.vector, record.vector);
            assert_eq!(identity.geometry, record.geometry);
            assert_eq!(identity.timestamp, record.timestamp);
            assert!(identity.metadata.is_null());
            assert!(identity.payload.is_none());
        }
    }

    #[test]
    fn legacy_json_rows_still_decode() {
        // A row written by a pre-binary release is plain Record JSON.
        let record = Record {
            id: "legacy".to_string(),
            vector: Some(vec![0.5]),
            metadata: serde_json::json!({"kept": true}),
            geometry: None,
            timestamp: Some(7),
            payload: None,
        };
        let bytes = serde_json::to_vec(&record).unwrap();
        assert_eq!(bytes[0], b'{');
        assert_eq!(decode_record(&bytes).unwrap(), record);
        let identity = decode_identity(&bytes).unwrap();
        assert_eq!(identity.id, "legacy");
        assert_eq!(identity.vector, Some(vec![0.5]));
        assert!(identity.metadata.is_null());
    }

    #[test]
    fn truncated_binary_rows_error_loudly() {
        let record = Record {
            id: "row".to_string(),
            vector: Some(vec![1.0, 2.0]),
            metadata: serde_json::json!({"k": "v"}),
            geometry: None,
            timestamp: Some(1),
            payload: None,
        };
        let bytes = encode_record(&record).unwrap();
        for cut in [1, 2, 5, bytes.len() - 1] {
            assert!(
                decode_record(&bytes[..cut]).is_err(),
                "truncation at {cut} decoded silently"
            );
        }
    }
}

#[cfg(test)]
mod posting_block_tests {
    use super::*;

    /// Pre-0.9.65 stores hold VERSION-1 blocks (no exact max rank). Decode,
    /// header reads, and the zero-alloc visitor must all keep serving them;
    /// the missing bound surfaces as `max_rank: None` (readers fall back to
    /// the bucket bound).
    #[test]
    fn version_one_blocks_still_decode() {
        let postings = vec![
            BlockPosting {
                pk: "a1".to_string(),
                doc_length: 7,
                doc_distinct: 3,
                packed_positions: vec![1, 5],
            },
            BlockPosting {
                pk: "a2".to_string(),
                doc_length: 4,
                doc_distinct: 2,
                packed_positions: vec![2],
            },
        ];
        // Hand-encode the v1 layout: [1][count][max_impact] then postings.
        let mut v1 = vec![POSTING_BLOCK_VERSION_V1];
        push_varint(&mut v1, postings.len() as u64);
        push_varint(&mut v1, 1234u64);
        let mut previous = "";
        for posting in &postings {
            let shared = previous
                .as_bytes()
                .iter()
                .zip(posting.pk.as_bytes())
                .take_while(|(left, right)| left == right)
                .count();
            push_varint(&mut v1, shared as u64);
            let suffix = &posting.pk.as_bytes()[shared..];
            push_varint(&mut v1, suffix.len() as u64);
            v1.extend_from_slice(suffix);
            push_varint(&mut v1, u64::from(posting.doc_length));
            push_varint(&mut v1, u64::from(posting.doc_distinct));
            push_varint(&mut v1, posting.packed_positions.len() as u64);
            for packed in &posting.packed_positions {
                push_varint(&mut v1, u64::from(*packed));
            }
            previous = &posting.pk;
        }

        let header = posting_block_header(&v1).expect("v1 header");
        assert_eq!(header.doc_count, 2);
        assert_eq!(header.max_impact, 1234);
        assert_eq!(header.max_rank, None, "v1 carries no exact bound");
        assert_eq!(posting_block_doc_count(&v1).unwrap(), 2);
        assert_eq!(posting_block_max_impact(&v1).unwrap(), 1234);

        let (decoded, max_impact) = decode_posting_block(&v1).unwrap();
        assert_eq!(max_impact, 1234);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].pk, "a1");
        assert_eq!(decoded[1].pk, "a2");
        assert_eq!(decoded[1].packed_positions, vec![2]);

        let mut pk_scratch = Vec::new();
        let mut pos_scratch = Vec::new();
        let mut seen = Vec::new();
        let (kept, max) = visit_posting_block(
            &v1,
            &mut pk_scratch,
            &mut pos_scratch,
            |pk, len, distinct, packed| {
                seen.push((pk.to_string(), len, distinct, packed.to_vec()));
                Ok(true)
            },
        )
        .unwrap();
        assert!(kept);
        assert_eq!(max, 1234);
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].0, "a1");

        // A v2 round-trip carries the exact bound through.
        let v2 = encode_posting_block(&postings, 1234, 0.125);
        let header = posting_block_header(&v2).expect("v2 header");
        assert_eq!(header.max_rank, Some(0.125));
        let (decoded, _) = decode_posting_block(&v2).unwrap();
        assert_eq!(decoded.len(), 2);
    }
}

#[cfg(test)]
mod single_pass_impact_tests {
    use super::*;

    #[test]
    fn ids_encoder_matches_the_posting_encoder_byte_for_byte() {
        let postings: Vec<NumericBlockPosting> = [3u64, 9, 4, 120, 77]
            .iter()
            .map(|id| NumericBlockPosting {
                document_id: *id,
                doc_length: 10,
                doc_distinct: 5,
                packed_positions: vec![1, 2],
            })
            .collect();
        let ids: Vec<u64> = postings.iter().map(|posting| posting.document_id).collect();
        let full = encode_compact_impact_block(&postings, 991, 0.3125);
        let lean = encode_compact_impact_block_from_ids(&ids, 991, 0.3125);
        assert_eq!(full, lean, "the two sidecar encoders diverged");
    }
}
