//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;

impl BicDb {
    pub fn get(&self, collection: &str, id: &str) -> Result<Option<Arc<Record>>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        self.get_unchecked(collection, id)
    }

    /// Point read with no policy or protection gate; see
    /// [`Transaction::get_for_integrity_check`] for the contract.
    /// Whether reads of `collection` are governed by a collection policy
    /// (protected-field projection / row visibility), i.e. whether a
    /// [`SecurityContext`] read must materialize the `Record` to be filtered.
    pub fn collection_has_policy(&self, collection: &str) -> Result<bool> {
        Ok(self.collection_state(collection)?.meta.policy.is_some())
    }

    /// Latest committed row without parsing its JSON; the non-transactional
    /// twin of [`Transaction::get_stored`]. Same access checks as
    /// [`Self::get`].
    pub fn get_stored(&self, collection: &str, id: &str) -> Result<Option<VisibleRow>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        if let Some(paged) = &self.paged_records {
            let _ = self.collection_state(collection)?;
            let snapshot = paged.latest_snapshot();
            return Ok(paged
                .get(&snapshot, collection, id)?
                .map(|record| VisibleRow::Pending(Arc::new(record))));
        }
        let state = self.collection_state(collection)?;
        let shard = state.shard(id).read();
        Ok(shard
            .get_record(id)
            .map(|entry| VisibleRow::Stored(Arc::clone(&entry.record))))
    }

    pub fn get_unchecked(&self, collection: &str, id: &str) -> Result<Option<Arc<Record>>> {
        if let Some(paged) = &self.paged_records {
            // Existence of the collection is still checked against the catalog,
            // so a read of an unknown collection errors identically in both
            // modes rather than silently returning None. The guard is dropped
            // immediately — only the existence check is wanted.
            let _ = self.collection_state(collection)?;
            let snapshot = paged.latest_snapshot();
            return Ok(paged.get(&snapshot, collection, id)?.map(Arc::new));
        }
        let state = self.collection_state(collection)?;
        let shard = state.shard(id).read();
        match shard.get_record(id) {
            Some(entry) => Ok(Some(Arc::new(entry.record.to_record()?))),
            None => Ok(None),
        }
    }

    /// Resolve a physical [`RowId`] (e.g. an index payload) to its record — O(1) to
    /// the shard via the rowid's encoded shard, then one rowid-keyed map lookup, with
    /// no String hashing or cloning. The PG-TID read-path primitive: index scans
    /// yield `Copy` rowids that route straight here.
    pub(crate) fn get_record_by_rowid(
        &self,
        collection: &str,
        rowid: RowId,
    ) -> Result<Option<Arc<StoredRecord>>> {
        let state = self.collection_state(collection)?;
        let shard = state.shard_by_rowid(rowid).read();
        if let Some(entry) = shard.records.get(&rowid) {
            return Ok(Some(entry.record.clone()));
        }
        if state.paged_lazy {
            let pk = shard.rowid_to_pk.get(&rowid).cloned();
            drop(shard);
            return self.paged_record_for_registry_rowid(collection, pk.as_deref());
        }
        Ok(None)
    }

    /// Resolve a registry rowid's pk to its latest committed record in the
    /// page store, as the compact resident form. Registry mode keeps no
    /// resident rows, so this is how index scans reach row content: rowid ->
    /// pk (reverse registry) -> page store.
    pub(crate) fn paged_record_for_registry_rowid(
        &self,
        collection: &str,
        pk: Option<&str>,
    ) -> Result<Option<Arc<StoredRecord>>> {
        let (Some(pk), Some(paged)) = (pk, &self.paged_records) else {
            return Ok(None);
        };
        let snapshot = paged.latest_snapshot();
        // The index scan that produced this rowid may have stashed the row's
        // TID hint: one validated heap probe instead of the key descent.
        if let Some((locator, xmin)) = stashed_tid_hint(collection, pk) {
            match paged.get_hinted(&snapshot, pk, locator, xmin)? {
                Some(record) => {
                    count_tid_hint(true);
                    return Ok(Some(Arc::new(StoredRecord::from_record(&record)?)));
                }
                None => count_tid_hint(false),
            }
        }
        match paged.get(&snapshot, collection, pk)? {
            Some(record) => Ok(Some(Arc::new(StoredRecord::from_record(&record)?))),
            None => Ok(None),
        }
    }

    /// The latest committed record for a physical [`RowId`] (the non-transactional
    /// read, mirroring [`Self::get`] but addressed by locator). Returns `None` if no
    /// resident record. No String hashing/cloning on the lookup.
    pub fn get_by_rowid(&self, collection: &str, rowid: RowId) -> Result<Option<Arc<Record>>> {
        match self.get_record_by_rowid(collection, rowid)? {
            Some(stored) => Ok(Some(Arc::new(stored.to_record()?))),
            None => Ok(None),
        }
    }

    /// Batch twin of [`Self::get_by_rowid`] that holds the per-collection read lock
    /// once, collects resident `StoredRecord` Arcs under it, and deserializes after
    /// releasing the lock.
    pub fn get_records_by_rowids(
        &self,
        collection: &str,
        rowids: &[RowId],
    ) -> Result<Vec<Option<Arc<Record>>>> {
        let stored = self.get_stored_by_rowids(collection, rowids)?;
        deserialize_stored_batch(stored)
    }

    /// Batch rowid fetch of the RESIDENT compact records (no metadata
    /// deserialization): the typed-row read path builds output cells straight
    /// from [`StoredRecord::typed_row`], so the heavy `Record` form is never
    /// materialized.
    pub fn get_stored_by_rowids(
        &self,
        collection: &str,
        rowids: &[RowId],
    ) -> Result<Vec<Option<Arc<StoredRecord>>>> {
        let state = self.collection_state(collection)?;
        let mut out = Vec::with_capacity(rowids.len());
        for &rowid in rowids {
            let (resident, registry_pk) = {
                let shard = state.shard_by_rowid(rowid).read();
                match shard.records.get(&rowid) {
                    Some(entry) => (Some(entry.record.clone()), None),
                    None if state.paged_lazy => (None, shard.rowid_to_pk.get(&rowid).cloned()),
                    None => (None, None),
                }
            };
            out.push(match resident {
                Some(record) => Some(record),
                None => self.paged_record_for_registry_rowid(collection, registry_pk.as_deref())?,
            });
        }
        Ok(out)
    }

    /// Batch primary-key fetch that fuses pk -> record under one collection read
    /// lock, avoiding the separate pk -> rowid pass for primary-key selections.
    pub fn get_records_by_pks(
        &self,
        collection: &str,
        pks: &[String],
    ) -> Result<Vec<Option<Arc<Record>>>> {
        let (mut stored, lazy_misses): (Vec<Option<Arc<StoredRecord>>>, Vec<usize>) = {
            let state = self.collection_state(collection)?;
            let mut lazy_misses = Vec::new();
            let mut stored = Vec::with_capacity(pks.len());
            for (index, pk) in pks.iter().enumerate() {
                let resident = {
                    let shard = state.shard(pk).read();
                    shard.get_record(pk).map(|entry| entry.record.clone())
                };
                if resident.is_none() && state.paged_lazy {
                    lazy_misses.push(index);
                }
                stored.push(resident);
            }
            (stored, lazy_misses)
        };
        if !lazy_misses.is_empty() {
            if let Some(paged) = self.paged_records.as_ref() {
                let snapshot = paged.latest_snapshot();
                let mut unresolved = Vec::with_capacity(lazy_misses.len());
                // TID hints are statement-thread local. Consume them before
                // dispatching the remaining independent page reads.
                for index in lazy_misses {
                    let pk = &pks[index];
                    let hinted = match stashed_tid_hint(collection, pk) {
                        Some((locator, xmin)) => {
                            let hinted = paged.get_hinted(&snapshot, pk, locator, xmin)?;
                            count_tid_hint(hinted.is_some());
                            hinted
                        }
                        None => None,
                    };
                    match hinted {
                        Some(record) => {
                            stored[index] = Some(Arc::new(StoredRecord::from_record(&record)?));
                        }
                        None => unresolved.push(index),
                    }
                }
                // One locality batch instead of an independent descent and
                // chain walk per key: ranked-search hydration reads scatter
                // across the heap, and page-ordered parallel resolution is
                // what keeps a 100-record fetch from costing 100 random IOs.
                let unresolved_pks: Vec<&str> = unresolved
                    .iter()
                    .map(|index| pks[*index].as_str())
                    .collect();
                let fetched = paged.get_batch(&snapshot, collection, &unresolved_pks)?;
                for (index, record) in unresolved.into_iter().zip(fetched) {
                    stored[index] = record
                        .map(|record| StoredRecord::from_record(&record).map(Arc::new))
                        .transpose()?;
                }
            }
        }
        deserialize_stored_batch(stored)
    }

    /// The physical [`RowId`] for a primary key in `collection`, if a record is
    /// resident — O(1), one shard hash. Lets a caller that already holds a pk (e.g.
    /// a primary-key match) address the rowid read path without re-fetching.
    pub fn rowid_for(&self, collection: &str, pk: &str) -> Result<Option<RowId>> {
        let state = self.collection_state(collection)?;
        let rowid = state.shard(pk).read().rowid_of(pk);
        Ok(rowid)
    }

    pub fn record_system_metadata(
        &self,
        collection: &str,
        pk: &str,
    ) -> Result<Option<RecordSystemMetadata>> {
        self.record_system_metadata_with_presence(collection, pk, false)
    }

    /// Batch variant of [`Self::record_system_metadata_with_presence`] for rows
    /// the caller has already materialized from the current snapshot. Skipping
    /// the page-store presence probe matters: on a lazy paged collection the
    /// probe is a full point lookup per row (B-tree descent plus
    /// transaction-status resolution), which turns row output into the dominant
    /// query cost while stamping metadata whose values never depend on it.
    pub fn record_system_metadata_batch_for_materialized(
        &self,
        collection: &str,
        pks: &[String],
    ) -> Result<Vec<Option<RecordSystemMetadata>>> {
        pks.iter()
            .map(|pk| self.record_system_metadata_with_presence(collection, pk, true))
            .collect()
    }

    pub(crate) fn record_system_metadata_with_presence(
        &self,
        collection: &str,
        pk: &str,
        // The caller holds a record for `pk` materialized from the current
        // snapshot, so existence in the page store is already proven and the
        // per-row `paged.get` probe can be skipped.
        row_materialized: bool,
    ) -> Result<Option<RecordSystemMetadata>> {
        let state = self.collection_state(collection)?;
        let shard = state.shard(pk).read();
        let Some(rowid) = shard.rowid_of(pk) else {
            if state.paged_lazy {
                if let Some(paged) = &self.paged_records {
                    if row_materialized
                        || paged
                            .get(&paged.latest_snapshot(), collection, pk)?
                            .is_some()
                    {
                        let rowid = RowId::provisional(pk);
                        let (tid_block, tid_offset) =
                            postgres_tid_at_epoch(rowid, self.current_commit_seq());
                        return Ok(Some(RecordSystemMetadata {
                            xmin: TransactionId(0),
                            xmax: None,
                            tid_block,
                            tid_offset,
                        }));
                    }
                }
            }
            return Ok(None);
        };
        if state.paged_lazy && !shard.versions.contains_key(&rowid) {
            // Registry rowid without a chain: baseline metadata if the row
            // exists in the page store.
            drop(shard);
            if let Some(paged) = &self.paged_records {
                if row_materialized
                    || paged
                        .get(&paged.latest_snapshot(), collection, pk)?
                        .is_some()
                {
                    let (tid_block, tid_offset) =
                        postgres_tid_at_epoch(rowid, self.current_commit_seq());
                    return Ok(Some(RecordSystemMetadata {
                        xmin: TransactionId(0),
                        xmax: None,
                        tid_block,
                        tid_offset,
                    }));
                }
            }
            return Ok(None);
        }
        let Some(version) = shard.versions.get(&rowid).and_then(|versions| {
            versions
                .iter()
                .filter(|version| version.deleted_tx.is_none())
                .max_by_key(|version| version.created_tx)
        }) else {
            return Ok(None);
        };
        let (tid_block, tid_offset) = postgres_tid_at_epoch(rowid, self.current_commit_seq());
        Ok(Some(RecordSystemMetadata {
            xmin: version.created_tx,
            xmax: version.deleted_tx,
            tid_block,
            tid_offset,
        }))
    }

    /// The collection a B-tree/spatial index belongs to.
    pub(crate) fn index_collection(&self, name: &str) -> Result<String> {
        Ok(self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read()
            .definition
            .collection
            .clone())
    }

    /// Map a batch of rowids to their String primary keys (legacy-API adapter),
    /// dropping any whose record is no longer resident. The collection guard is
    /// resolved ONCE for the whole batch: this runs per index hit on the point
    /// read path (`lookup_index` and friends), and the per-row
    /// `collection_state` hash + shared-RwLock acquire inside the old
    /// `rowid_pk` loop profiled at ~10% of total CPU at VU32.
    pub(crate) fn rowids_to_pks(&self, collection: &str, rowids: &[RowId]) -> Vec<String> {
        let Ok(state) = self.collection_state(collection) else {
            return Vec::new();
        };
        rowids
            .iter()
            .filter_map(|&rowid| {
                let shard = state.shard_by_rowid(rowid).read();
                if let Some(entry) = shard.records.get(&rowid) {
                    return Some(entry.record.id.clone());
                }
                // Registry mode: rows are not resident; the reverse registry
                // is the rowid's identity.
                shard.rowid_to_pk.get(&rowid).map(|pk| pk.to_string())
            })
            .collect()
    }

    /// Like [`Self::rowids_to_pks`] but sorted by primary key. The unordered point
    /// lookups historically returned ids in pk order (a `BTreeSet<String>`
    /// artifact); the rowid-payload store yields rowid order, so the legacy
    /// String-returning point APIs re-sort to preserve that deterministic order.
    pub(crate) fn rowids_to_pks_sorted(&self, collection: &str, rowids: &[RowId]) -> Vec<String> {
        let mut pks = self.rowids_to_pks(collection, rowids);
        pks.sort();
        pks
    }

    pub fn scan_collection(&self, collection: &str) -> Result<Vec<Record>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        self.scan_collection_unchecked(collection)
    }

    /// Read a deterministic, explicitly bounded collection batch in primary-key order.
    ///
    /// `after_id` is exclusive. Reusing the final record id as the next call's cursor
    /// makes large scans restartable without materializing the collection. In paged
    /// mode the row and encoded-byte limits are applied before decoding each batch.
    pub fn scan_collection_batch_after(
        &self,
        collection: &str,
        after_id: Option<&str>,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Vec<Record>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        if max_rows == 0 || max_bytes == 0 {
            return Err(BicDbError::PagedStorage(
                "collection batch limits must be non-zero".to_string(),
            ));
        }
        if let Some(paged) = &self.paged_records {
            let _ = self.collection_state(collection)?;
            let snapshot = paged.latest_snapshot();
            return paged.scan_batch_after(&snapshot, collection, after_id, max_rows, max_bytes);
        }

        let mut rows = Vec::with_capacity(max_rows.min(4096));
        let mut bytes = 0usize;
        for record in self.scan_collection_unchecked(collection)? {
            if after_id.is_some_and(|cursor| record.id.as_str() <= cursor) {
                continue;
            }
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
            if rows.len() >= max_rows {
                break;
            }
        }
        Ok(rows)
    }

    /// Locality-optimized companion to [`Self::scan_collection_batch_after`].
    /// Paged stores resolve a bounded primary-key range in physical heap-page
    /// order and restore primary-key order before returning it. Other storage
    /// modes use the ordinary bounded scan.
    pub fn scan_collection_locality_batch_after(
        &self,
        collection: &str,
        after_id: Option<&str>,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Vec<Record>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        if max_rows == 0 || max_bytes == 0 {
            return Err(BicDbError::PagedStorage(
                "collection batch limits must be non-zero".to_string(),
            ));
        }
        if let Some(paged) = &self.paged_records {
            let _ = self.collection_state(collection)?;
            let snapshot = paged.latest_snapshot();
            return paged
                .scan_locality_batch_after(&snapshot, collection, after_id, max_rows, max_bytes);
        }
        self.scan_collection_batch_after(collection, after_id, max_rows, max_bytes)
    }

    pub fn scan_collection_record_ids_with_prefix(
        &self,
        collection: &str,
        id_prefix: &str,
    ) -> Result<Vec<String>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        self.scan_collection_record_ids_with_prefix_unchecked(collection, id_prefix)
    }

    /// Prefix id scan with no policy or protection gate, for locator planning
    /// whose ids resolve to rows only through policy-filtered record paths.
    pub fn scan_collection_record_ids_with_prefix_unchecked(
        &self,
        collection: &str,
        id_prefix: &str,
    ) -> Result<Vec<String>> {
        if let Some(paged) = &self.paged_records {
            // Shards are a touched-rows cache in paged mode; the page store is
            // the complete view.
            let _ = self.collection_state(collection)?;
            let snapshot = paged.latest_snapshot();
            let mut ids = paged
                .scan(&snapshot, collection)?
                .filter_map(|record| match record {
                    Ok(record) if record.id.starts_with(id_prefix) => Some(Ok(record.id)),
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                })
                .collect::<Result<Vec<_>>>()?;
            ids.sort();
            return Ok(ids);
        }
        let state = self.collection_state(collection)?;
        let mut ids = state
            .read_all()
            .iter()
            .flat_map(|shard| shard.records.values().map(|entry| entry.record.id.clone()))
            .filter(|id| id.starts_with(id_prefix))
            .collect::<Vec<_>>();
        ids.sort();
        Ok(ids)
    }

    /// Full scan with no policy or protection gate. For engine-internal
    /// integrity work (referential-integrity checks bypass row policies,
    /// as in PostgreSQL) — never for caller-visible reads.
    /// A deterministic uniform sample of at most `limit` records of the
    /// collection (primary-key order) together with the collection's total
    /// row count. A collection at or under `limit` is returned whole. Unlike
    /// [`Self::scan_collection`] this never materializes more than `limit`
    /// records: ANALYZE over a multi-gigabyte table stays bounded.
    pub fn sample_collection(
        &self,
        collection: &str,
        limit: usize,
    ) -> Result<(Vec<Record>, usize)> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        let mut sampler = AnalyzeSampler::new(limit, collection);
        let mut records = Vec::<Record>::new();
        if let Some(paged) = &self.paged_records {
            let _ = self.collection_state(collection)?;
            let snapshot = paged.latest_snapshot();
            for record in paged.scan(&snapshot, collection)? {
                let record = record?;
                match sampler.offer() {
                    Some(slot) if slot == records.len() => records.push(record),
                    Some(slot) => records[slot] = record,
                    None => {}
                }
            }
        } else {
            let state = self.collection_state(collection)?;
            let shards = state.read_all();
            let mut sampled = Vec::<Arc<StoredRecord>>::new();
            for entry in shards.iter().flat_map(|shard| shard.records.values()) {
                match sampler.offer() {
                    Some(slot) if slot == sampled.len() => sampled.push(entry.record.clone()),
                    Some(slot) => sampled[slot] = entry.record.clone(),
                    None => {}
                }
            }
            drop(shards);
            records = sampled
                .iter()
                .map(|stored| stored.to_record())
                .collect::<Result<Vec<_>>>()?;
        }
        records.sort_by(|left, right| left.id.cmp(&right.id));
        Ok((records, sampler.seen()))
    }

    pub fn scan_collection_unchecked(&self, collection: &str) -> Result<Vec<Record>> {
        if let Some(paged) = &self.paged_records {
            let _ = self.collection_state(collection)?;
            let snapshot = paged.latest_snapshot();
            let mut records = paged
                .scan(&snapshot, collection)?
                .collect::<Result<Vec<_>>>()?;
            records.sort_by(|left, right| left.id.cmp(&right.id));
            return Ok(records);
        }
        let state = self.collection_state(collection)?;
        let mut records = state
            .read_all()
            .iter()
            .flat_map(|shard| shard.records.values())
            .map(|entry| entry.record.to_record())
            .collect::<Result<Vec<_>>>()?;
        records.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(records)
    }

    pub fn write_attachment_from_reader(
        &mut self,
        collection: &str,
        id: &str,
        field: &str,
        media_type: Option<&str>,
        reader: impl std::io::Read,
    ) -> Result<LargeValueRef> {
        self.ensure_writable("write attachment")?;
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Write)?;
        self.write_attachment_unchecked(collection, id, field, media_type, reader)
    }

    pub fn open_attachment_reader(
        &self,
        collection: &str,
        id: &str,
        field: &str,
    ) -> Result<Option<LargeValueReader>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        let Some(record) = self.get_unchecked(collection, id)? else {
            return Ok(None);
        };
        self.open_attachment_reader_for_record(&record, field)
    }

    /// Atomically allocates a unique, monotonically increasing transaction id.
    /// Safe to call from concurrent transaction begins.
    pub(crate) fn allocate_tx_id(&self) -> TransactionId {
        let mut current = self.next_tx_id.load(AtomicOrdering::SeqCst);
        loop {
            let id = current.max(1);
            let next = id.saturating_add(1);
            match self.next_tx_id.compare_exchange_weak(
                current,
                next,
                AtomicOrdering::SeqCst,
                AtomicOrdering::SeqCst,
            ) {
                Ok(_) => return TransactionId(id),
                Err(observed) => current = observed,
            }
        }
    }

    /// Commits a transaction that was begun and buffered (possibly under shared
    /// access) by applying it through a shared `&BicDb`. Unlike
    /// [`Transaction::commit`], this does not dereference the transaction's
    /// internal back-pointer, so the caller may hold only a shared read guard on
    /// the database: the durable commit step serializes committers on the global
    /// `commit_lock` and mutates per-collection / per-index state through their
    /// own interior locks, so concurrent reads and disjoint-row commits proceed
    /// in parallel.
    ///
    /// Returns the commit sequence; the caller MUST call `tx_log_handle().
    /// write_durable(seq)` AFTER releasing the read lock (so concurrent commits
    /// pipeline and coalesce their durable writes), then call
    /// [`Transaction::finalize_commit_admission`], before acknowledging.
    pub fn commit_buffered_transaction(&self, tx: &mut Transaction) -> Result<u64> {
        self.commit_transaction(tx)
    }

    /// A cloneable handle to the transaction-log writer, for performing the
    /// durable write outside the database write lock (decoupled group commit).
    /// See [`Self::commit_buffered_transaction`].
    pub fn tx_log_handle(&self) -> TxLogHandle {
        self.tx_log.clone()
    }

    /// The oldest tx-id snapshot that any live or future transaction can still
    /// read, below which version-chain entries may be pruned. This is the lower
    /// of the contiguous applied watermark and the minimum registered active
    /// snapshot.
    ///
    /// An active snapshot may be raised above the applied watermark by a
    /// session's read-your-own-write floor. Such a raised snapshot is authority
    /// for that session only: until the applied watermark catches up, a new
    /// session may still start at the lower watermark. Therefore active
    /// snapshots can lower the GC boundary, but must never raise it above
    /// `last_committed_tx`.
    pub(crate) fn gc_watermark(&self) -> u64 {
        let applied = self.last_committed_tx.load(AtomicOrdering::SeqCst);
        self.active_snapshots
            .min_key()
            .map_or(applied, |active| active.min(applied))
    }

    /// Begin a transaction, floored at the last commit this *thread* made to this
    /// database.
    ///
    /// Without the floor, a thread that commits back-to-back can begin its next
    /// transaction at a snapshot below its own previous commit and then falsely
    /// conflict on a row nobody else has touched — see
    /// [`Self::begin_transaction_after`] for why the watermark lags. The session
    /// layer already passes its connection's last `commit_seq` for this reason;
    /// callers of the plain API were left exposed to a race they have no way to
    /// know about.
    ///
    /// The floor is visibility authority, not proof that every lower sequence
    /// had physically applied before a row was read. Point reads therefore
    /// carry their exact observed mutation version into writes. Blind direct
    /// writes retain their established transaction-snapshot boundary. This
    /// distinction prevents an out-of-order lower commit from being hidden
    /// after a real read without making ordinary back-to-back upserts conflict
    /// with themselves.
    ///
    /// Keyed by database id, not just by thread: a thread holding two databases
    /// open would otherwise floor one database's snapshot with the other's
    /// unrelated sequence, which *would* mask real conflicts.
    pub fn begin_transaction(&self) -> Result<Transaction> {
        self.begin_transaction_after(self.last_commit_seq_on_this_thread())
    }

    /// Begin a read-committed transaction owned by the trusted application
    /// runtime. Mutation grants can be issued only on transactions carrying
    /// this immutable actor/plugin binding.
    pub fn begin_application_transaction(&self, actor: MutationActor) -> Result<Transaction> {
        self.begin_application_transaction_with_isolation(
            actor,
            TransactionIsolation::ReadCommitted,
        )
    }

    /// Begin a trusted application transaction at the requested isolation.
    /// Repeatable-read uses BicDB's fixed MVCC snapshot. Serializable adds a
    /// conservative relation read set that is validated under exclusive commit
    /// admission, preventing write skew and predicate phantoms.
    pub fn begin_application_transaction_with_isolation(
        &self,
        actor: MutationActor,
        isolation: TransactionIsolation,
    ) -> Result<Transaction> {
        if actor.actor_id.trim().is_empty()
            || actor.originating_plugin.trim().is_empty()
            || actor.trace_id.trim().is_empty()
        {
            return Err(BicDbError::MutationDenied(
                "application transactions require actor, plugin, and trace identifiers".to_string(),
            ));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default();
        if actor.deadline_unix_ms <= now {
            return Err(BicDbError::QueryTimedOut);
        }
        // Synchronize a serializable begin with every in-flight commit so its
        // MVCC watermark and relation generations describe one stable point.
        let _admission = (isolation == TransactionIsolation::Serializable)
            .then(|| self.serializable_commit_admission.write());
        let mut transaction = self.begin_transaction()?;
        transaction.mutation_actor = Some(actor);
        transaction.isolation = isolation;
        if isolation == TransactionIsolation::Serializable {
            transaction.serializable_generation_snapshot = self.collection_generations();
        }
        Ok(transaction)
    }

    /// The highest `commit_seq` this thread has committed to this database.
    pub(crate) fn last_commit_seq_on_this_thread(&self) -> u64 {
        THREAD_LAST_COMMIT_SEQ.with(|seqs| {
            seqs.borrow()
                .get(&self.instance_id)
                .copied()
                .unwrap_or_default()
        })
    }

    /// Record a `commit_seq` this thread produced, so its next transaction does
    /// not begin behind it.
    pub(crate) fn note_commit_seq_on_this_thread(&self, commit_seq: u64) {
        THREAD_LAST_COMMIT_SEQ.with(|seqs| {
            let mut seqs = seqs.borrow_mut();
            let entry = seqs.entry(self.instance_id).or_default();
            *entry = (*entry).max(commit_seq);
        });
    }

    /// Like [`Self::begin_transaction`], but raises the visibility snapshot to
    /// at least `min_snapshot` (a `commit_seq`). The session layer passes the
    /// connection's own last commit sequence so a connection can read its own
    /// just-committed writes.
    ///
    /// This matters because the contiguous visibility watermark
    /// (`last_committed_tx`) lags the assigned `commit_seq` by roughly the number
    /// of in-flight concurrent commits: a committer finishes seq C but the
    /// watermark cannot advance past C until every lower seq has also applied. A
    /// session committing back-to-back would otherwise begin its next
    /// transaction below its own last commit. The raised number is visibility
    /// authority only for point reads: a later write after such a read uses the
    /// exact row version it observed. A blind write retains the historical
    /// snapshot-isolation behavior and may use the raised boundary.
    pub fn begin_transaction_after(&self, min_snapshot: u64) -> Result<Transaction> {
        self.begin_transaction_after_with_registration_hook(min_snapshot, || {})
    }

    pub(super) fn begin_transaction_after_with_registration_hook(
        &self,
        min_snapshot: u64,
        before_registration: impl FnOnce(),
    ) -> Result<Transaction> {
        self.ensure_writable("begin write transaction")?;
        let tx_id = self.allocate_tx_id();
        self.tx_states.insert(tx_id.0, TxState::Pending);
        let mut applied_watermark = self.last_committed_tx.load(AtomicOrdering::SeqCst);
        let mut snapshot_tx = TransactionId(applied_watermark.max(min_snapshot));
        before_registration();
        // Register the snapshot so GC keeps every version this transaction can
        // still read. GC may have advanced between the initial watermark load
        // and publication, so confirm that boundary after publishing. No row
        // has been read yet: retry at the newer boundary if it overtook us.
        // A session floor already covering the new boundary needs no retry.
        // The successful registration is deregistered in Transaction::drop.
        loop {
            self.active_snapshots.incr(snapshot_tx.0);
            let current = self.last_committed_tx.load(AtomicOrdering::SeqCst);
            if current <= snapshot_tx.0 {
                break;
            }
            self.active_snapshots.decr_remove(snapshot_tx.0);
            applied_watermark = current;
            snapshot_tx = TransactionId(current.max(min_snapshot));
        }
        Ok(Transaction {
            db: NonNull::from(self),
            id: tx_id,
            snapshot_tx,
            // Everything at or below this unraised boundary was physically
            // applied before construction. It is the conservative fallback
            // when neither a point read nor a resident row high-water mark can
            // provide a more exact mutation boundary.
            applied_snapshot: applied_watermark,
            observed_record_versions: Mutex::new(FxHashMap::default()),
            state: TxState::Pending,
            writes: Vec::new(),
            write_index: FxHashMap::default(),
            enqueue_memory_jobs_on_commit: true,
            prepared_wal: PreparedWal::default(),
            read_images: Mutex::new(ReadImages::default()),
            write_locks: Arc::clone(&self.write_locks),
            locked_keys: Mutex::new(LockedKeys::default()),
            active_snapshots: Arc::clone(&self.active_snapshots),
            tx_states: Arc::clone(&self.tx_states),
            pending_broker_publishes: Mutex::new(Vec::new()),
            pending_record_audit_events: Vec::new(),
            mutation_actor: None,
            deferred_hooks: Vec::new(),
            mutation_grants: BTreeMap::new(),
            next_mutation_grant: 1,
            commit_validators: Vec::new(),
            isolation: TransactionIsolation::ReadCommitted,
            serializable_generation_snapshot: HashMap::new(),
            serializable_reads: BTreeSet::new(),
            bypass_commit_admission: false,
            commit_admission_ticket: None,
            committed_seq: None,
            paged_snapshot: self
                .paged_records
                .as_ref()
                .map(|paged| paged.latest_snapshot()),
            _not_send: PhantomData,
        })
    }

    pub fn snapshot(&self) -> Result<DbSnapshot> {
        self.snapshot_for_tx(TransactionId(
            self.last_committed_tx.load(AtomicOrdering::SeqCst),
        ))
    }

    pub fn collection_generations(&self) -> HashMap<String, u64> {
        self.collections
            .iter()
            .map(|(collection, state)| (collection.clone(), state.read().generation()))
            .collect()
    }

    pub fn collection_generation(&self, collection: &str) -> u64 {
        self.collections
            .get(collection)
            .map(|state| state.read().generation())
            .unwrap_or_default()
    }

    /// Number of collections currently resident. O(1); used by name-resolution
    /// caches to notice collection creation/removal that bypasses the SQL
    /// schema catalog (legacy document-API collections).
    pub fn collection_count(&self) -> usize {
        self.collections.len()
    }

    /// Number of executable indexes currently resident. O(1); used by caches
    /// to notice index creation/removal.
    pub fn index_catalog_len(&self) -> usize {
        self.indexes.len()
    }

    /// Monotonic counter bumped on every executable-index create, drop or
    /// rename. O(1); the validation key for caches derived from index
    /// definitions.
    pub fn index_generation(&self) -> u64 {
        self.index_generation.load(AtomicOrdering::Relaxed)
    }

    /// Process-unique id of this database instance. Memos that live outside
    /// the database (per-thread catalog caches) must key by this, not by the
    /// instance's address: a fresh database allocated where a dropped one
    /// lived starts every generation counter at 0 and would otherwise inherit
    /// the old instance's entries.
    pub fn instance_id(&self) -> u64 {
        self.instance_id
    }

    pub fn transaction_state(&self, tx_id: TransactionId) -> Option<TxState> {
        self.tx_states.get_copied(&tx_id.0)
    }

    pub fn delete(&mut self, collection: &str, id: &str) -> Result<bool> {
        Ok(self.batch_delete(collection, std::iter::once(id))? > 0)
    }

    pub fn batch_delete<I, S>(&mut self, collection: &str, ids: I) -> Result<usize>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.ensure_writable("delete records")?;
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Delete)?;
        self.ensure_collection(collection)?;
        let mut seen_ids = FxHashSet::default();
        let mut existing_ids = Vec::new();
        {
            let state = self.collection_state(collection)?;
            for id in ids {
                let id = id.as_ref();
                if id.is_empty() {
                    return Err(BicDbError::EmptyRecordId);
                }
                if !seen_ids.insert(id.to_string()) {
                    continue;
                }
                if state.shard(id).read().get_record(id).is_some() {
                    existing_ids.push(id.to_string());
                } else if state.paged_lazy {
                    // Lazy paged collection: an untouched row exists only in
                    // the page store; a shard-only check would report "not
                    // present" and skip the delete entirely.
                    if let Some(paged) = &self.paged_records {
                        if paged
                            .get(&paged.latest_snapshot(), collection, id)?
                            .is_some()
                        {
                            existing_ids.push(id.to_string());
                        }
                    }
                }
            }
        }
        if existing_ids.is_empty() {
            return Ok(0);
        }
        let count = existing_ids.len();
        let mut tx = self.begin_transaction()?;
        tx.delete_many(collection, existing_ids)?;
        tx.commit()?;
        Ok(count)
    }

    pub(crate) fn batch_delete_unchecked<I, S>(&mut self, collection: &str, ids: I) -> Result<usize>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.ensure_collection(collection)?;
        let mut seen_ids = FxHashSet::default();
        let mut ids_to_delete = Vec::new();
        for id in ids {
            let id = id.as_ref();
            if id.is_empty() {
                return Err(BicDbError::EmptyRecordId);
            }
            if seen_ids.insert(id.to_string()) {
                ids_to_delete.push(id.to_string());
            }
        }
        if ids_to_delete.is_empty() {
            return Ok(0);
        }
        let collection_mode = self.collection_state(collection)?.meta.mode.clone();

        let removed = {
            let state = self.collection_state(collection)?;
            let mut removed = Vec::new();
            for id in &ids_to_delete {
                let entry = {
                    let shard = state.shard(id).read();
                    shard.get_record(id).map(|entry| Arc::clone(&entry.record))
                };
                if let Some(record) = entry {
                    removed.push((id.clone(), record));
                } else if state.paged_lazy {
                    // Untouched row of a lazy collection: exists only in the
                    // page store. Resolve it there, as a stub the downstream
                    // index bookkeeping can materialize on demand.
                    if let Some(paged) = &self.paged_records {
                        let snapshot = paged.latest_snapshot();
                        if let Some(full) = paged.get(&snapshot, collection, id)? {
                            let fetch: PagedStubFetch = {
                                let paged = Arc::clone(paged);
                                let name: Arc<str> = Arc::from(collection);
                                Arc::new(move |pk: &str| paged.get(&snapshot, &name, pk))
                            };
                            let stub = StoredRecord::evicted_stub(
                                &full,
                                crate::record::EvictedPayload {
                                    fetch,
                                    pk: Arc::from(id.as_str()),
                                },
                            );
                            removed.push((id.clone(), Arc::new(stub)));
                        }
                    }
                }
            }
            removed
        };
        if removed.is_empty() {
            return Ok(0);
        }

        // In server-paged mode the page store is the durable home for rows and
        // durable index postings.  The legacy direct-delete path below writes a
        // segment tombstone and mutates resident shards, neither of which can
        // delete an untouched row that exists only on pages.  It also cannot
        // resolve that row's index RowId, which made SQL/PLpgSQL autocommit
        // deletes fail with `index ... apply missing rowid for record`.
        //
        // Route the already-authorized, existence-filtered ids through the
        // normal transaction path.  That path deletes the page-store row and
        // durable postings atomically, seeds a lazy row's locator before index
        // apply, and preserves the same audit/sync hooks as every other paged
        // mutation.
        if self.paged_records.is_some() {
            let ids = removed.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>();
            let count = ids.len();
            let mut tx = self.begin_transaction()?;
            tx.delete_many(collection, ids)?;
            tx.commit()?;
            return Ok(count);
        }

        let mut index_mutations = removed
            .iter()
            .map(|(_, record)| {
                Ok(IndexRecordMutation {
                    old_record: OldImage::from_stored(Arc::clone(record)),
                    new_record: OldImage::none(),
                    rowid: None,
                    changed: None,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        // Capture each deleted record's locator NOW, before its record/version
        // state (and registry entry) is removed below — the index apply needs it
        // to drop the right entry.
        self.fill_index_mutation_rowids(collection, &mut index_mutations)?;
        self.validate_collection_index_mutations(collection, &index_mutations)?;

        let payloads = removed
            .iter()
            .map(|(id, _)| serde_json::to_vec(&RecordFrame::Delete { id: id.clone() }))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        storage::append_frames(
            &self.segment_path(collection),
            FrameKind::Record,
            &payloads,
            self.config.fsync,
            &self.config.compression,
            &self.encryption,
        )?;

        if self.config.sync_outbox {
            let timestamp = unix_timestamp();
            let ops = removed
                .iter()
                .map(|(id, record)| {
                    Ok(SyncOp {
                        op_id: Uuid::new_v4(),
                        collection: collection.to_string(),
                        record_id: id.clone(),
                        op_type: OpType::Delete,
                        timestamp,
                        hash: record.content_hash()?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            self.sync_log.lock().append_ops(&ops)?;
        }

        let gc_watermark = self.gc_watermark();
        {
            let state = self.collection_state_mut(collection)?;
            let mut rebuild_vector_store = false;
            for (id, record) in &removed {
                let shard = state.shard_mut(id);
                let Some(rowid) = shard.rowid_of(id) else {
                    continue;
                };
                shard.records.remove(&rowid);
                apply_versioned_delete(
                    &mut shard.versions,
                    &mut shard.pk_to_rowid,
                    &mut shard.gc_pending,
                    rowid,
                    TransactionId(0),
                    gc_watermark,
                );
                rebuild_vector_store |= record.vector.is_some();
            }
            if rebuild_vector_store {
                state.rebuild_vector_store();
            }
            state.generation.fetch_add(1, AtomicOrdering::Relaxed);
        }
        self.apply_collection_index_mutations(collection, &mut index_mutations)?;
        self.tombstone_hnsw_records(collection, removed.iter().map(|(id, _)| id.as_str()))?;

        if self.config.audit_events {
            // Captured before appending (and outside the events lock — the
            // vector computation takes it internally).
            let write_context = self.sync_vector()?;
            let write_clock = self.write_clock_value();
            for (_, record) in &removed {
                self.events.lock().append(record_deleted_event(
                    collection,
                    &collection_mode,
                    &record.to_record()?,
                    Some(&write_context),
                    Some(&write_clock),
                )?)?;
            }
        }
        self.refresh_graph_projections()?;

        Ok(removed.len())
    }

    pub fn events(&self) -> parking_lot::MutexGuard<'_, EventStream> {
        self.events.lock()
    }

    pub fn events_mut(&mut self) -> &mut EventStream {
        self.events.get_mut()
    }

    pub fn emit_geofence_triggered(
        &mut self,
        geofence_id: &str,
        device_id: &str,
        location: &Geometry,
        metadata: serde_json::Value,
    ) -> Result<bool> {
        if !self.config.audit_events {
            return Ok(false);
        }
        self.events.lock().append(geofence_triggered_event(
            geofence_id,
            device_id,
            location,
            metadata,
        )?)?;
        Ok(true)
    }

    /// The stateful geofence engine: evaluates one device location update
    /// against every fence in `fences_collection` (records whose intrinsic
    /// geometry is areal), maintains per-device presence in
    /// `<fences>_presence`, and returns enter/dwell/exit transitions. With
    /// audit events on, transitions are also appended to the spatial stream
    /// so broker consumers and mesh peers see them; presence records and
    /// fence layers replicate like any other data — the offline-first
    /// moving-assets story needs no extra machinery.
    pub fn process_device_location(
        &mut self,
        fences_collection: &str,
        device_id: &str,
        lon: f64,
        lat: f64,
        timestamp: i64,
    ) -> Result<Vec<GeofenceTransition>> {
        let point = geo_types::Point::new(lon, lat);
        let fences = self.scan_collection(fences_collection)?;
        let mut inside_now: BTreeSet<String> = BTreeSet::new();
        for fence in &fences {
            let Some(geometry) = &fence.geometry else {
                continue;
            };
            let contains = match geometry {
                Geometry::Polygon(polygon) => polygon_contains_point(polygon, &point),
                Geometry::MultiPolygon(polygons) => polygons
                    .iter()
                    .any(|polygon| polygon_contains_point(polygon, &point)),
                Geometry::Envelope(rect) => {
                    (rect.min().x..=rect.max().x).contains(&lon)
                        && (rect.min().y..=rect.max().y).contains(&lat)
                }
                _ => false,
            };
            if contains {
                inside_now.insert(fence.id.clone());
            }
        }

        let presence_collection = format!("{fences_collection}_presence");
        if !self.collections.contains_key(&presence_collection) {
            self.create_collection(&presence_collection)?;
        }
        let mut entered_at: BTreeMap<String, i64> = self
            .get(&presence_collection, device_id)?
            .and_then(|record| {
                record.metadata.get("fences").and_then(|fences| {
                    serde_json::from_value::<BTreeMap<String, i64>>(fences.clone()).ok()
                })
            })
            .unwrap_or_default();

        let mut transitions = Vec::new();
        for fence_id in &inside_now {
            match entered_at.get(fence_id) {
                None => {
                    entered_at.insert(fence_id.clone(), timestamp);
                    transitions.push(GeofenceTransition {
                        geofence_id: fence_id.clone(),
                        device_id: device_id.to_string(),
                        kind: GeofenceTransitionKind::Enter,
                        timestamp,
                    });
                }
                Some(since) => transitions.push(GeofenceTransition {
                    geofence_id: fence_id.clone(),
                    device_id: device_id.to_string(),
                    kind: GeofenceTransitionKind::Dwell(timestamp.saturating_sub(*since)),
                    timestamp,
                }),
            }
        }
        let exited: Vec<(String, i64)> = entered_at
            .iter()
            .filter(|(fence_id, _)| !inside_now.contains(*fence_id))
            .map(|(fence_id, since)| (fence_id.clone(), *since))
            .collect();
        for (fence_id, since) in exited {
            entered_at.remove(&fence_id);
            transitions.push(GeofenceTransition {
                geofence_id: fence_id.clone(),
                device_id: device_id.to_string(),
                kind: GeofenceTransitionKind::Exit(timestamp.saturating_sub(since)),
                timestamp,
            });
        }

        self.insert(
            &presence_collection,
            Record::new(device_id).with_metadata(json!({
                "fences": entered_at,
                "lon": lon,
                "lat": lat,
                "updated_at": timestamp,
            })),
        )?;

        if self.config.audit_events {
            for transition in &transitions {
                let event_type = match transition.kind {
                    GeofenceTransitionKind::Enter => "GeofenceEntered",
                    GeofenceTransitionKind::Dwell(_) => "GeofenceDwell",
                    GeofenceTransitionKind::Exit(_) => "GeofenceExited",
                };
                self.events.lock().append(Event::new(
                    SPATIAL_AUDIT_STREAM,
                    event_type,
                    json!({
                        "geofence_id": transition.geofence_id,
                        "device_id": transition.device_id,
                        "transition": transition.kind,
                        "lon": lon,
                        "lat": lat,
                        "timestamp": timestamp,
                    }),
                ))?;
            }
        }
        Ok(transitions)
    }

    pub fn emit_device_location_updated(
        &mut self,
        device_id: &str,
        location: &Geometry,
        metadata: serde_json::Value,
    ) -> Result<bool> {
        if !self.config.audit_events {
            return Ok(false);
        }
        self.events.lock().append(device_location_updated_event(
            device_id, location, metadata,
        )?)?;
        Ok(true)
    }

    pub fn build_graph_projection(
        &mut self,
        projection: GraphProjection,
    ) -> Result<GraphProjectionData> {
        validate_graph_projection_name(&projection.name)?;
        for collection in projection.collection_names() {
            self.ensure_existing_unprotected_legacy_access(&collection, SecureOperation::Read)?;
        }
        let graph = self.materialize_graph_projection(projection)?;
        self.persist_graph_projection(&graph)?;
        self.graphs
            .write()
            .insert(graph.name.clone(), graph.clone());
        Ok(graph)
    }

    pub fn rebuild_graph_projection(
        &mut self,
        projection: GraphProjection,
    ) -> Result<GraphProjectionData> {
        self.build_graph_projection(projection)
    }

    pub fn verify_graph_projection(
        &self,
        projection: &GraphProjection,
    ) -> Result<GraphVerifyReport> {
        validate_graph_projection_name(&projection.name)?;
        for collection in projection.collection_names() {
            self.ensure_existing_unprotected_legacy_access(&collection, SecureOperation::Read)?;
        }
        let rebuilt = self.materialize_graph_projection(projection.clone())?;
        let graphs = self.graphs.read();
        let stored = graphs.get(&projection.name);
        let valid = stored.is_some_and(|stored| {
            stored.nodes == rebuilt.nodes
                && stored.edges == rebuilt.edges
                && stored.definition == rebuilt.definition
        });
        Ok(GraphVerifyReport {
            projection: projection.name.clone(),
            stored_nodes: stored.map(|graph| graph.nodes.len()).unwrap_or_default(),
            rebuilt_nodes: rebuilt.nodes.len(),
            stored_edges: stored.map(|graph| graph.edges.len()).unwrap_or_default(),
            rebuilt_edges: rebuilt.edges.len(),
            graph_size_bytes: stored
                .map(GraphProjectionData::graph_size_bytes)
                .unwrap_or_default(),
            valid,
        })
    }

    pub fn graph_projection(&self, name: &str) -> Result<Option<GraphProjectionData>> {
        validate_graph_projection_name(name)?;
        Ok(self.graphs.read().get(name).cloned())
    }

    pub fn graph_nodes(&self) -> Vec<(String, GraphNode)> {
        self.graphs
            .read()
            .values()
            .flat_map(|graph| {
                graph
                    .nodes
                    .values()
                    .cloned()
                    .map(|node| (graph.name.clone(), node))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub fn graph_edges(&self) -> Vec<(String, GraphEdge)> {
        self.graphs
            .read()
            .values()
            .flat_map(|graph| {
                graph
                    .edges
                    .values()
                    .cloned()
                    .map(|edge| (graph.name.clone(), edge))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub fn node_id(&self) -> NodeId {
        self.sync_state.node_id.clone()
    }

    pub fn encryption_mode(&self) -> EncryptionMode {
        self.encryption.mode()
    }

    pub fn encryption_key_version(&self) -> Option<u32> {
        self.encryption.key_version()
    }

    pub fn key_rotation_plan(&self) -> Result<KeyRotationPlan> {
        let metadata = encryption::load_metadata(&self.path)?;
        Ok(encryption::rotation_plan(metadata.as_ref()))
    }

    pub fn rotate_protected_data_field_encryption_key(
        &mut self,
        options: ProtectedDataKeyRotationOptions,
    ) -> Result<ProtectedDataKeyRotationReport> {
        if options.old_key_material.trim().is_empty()
            || options.new_key_material.trim().is_empty()
            || options.new_key_ref.trim().is_empty()
        {
            return Err(BicDbError::EncryptionKeyRequired);
        }

        let mut report = ProtectedDataKeyRotationReport::default();
        if !options.dry_run {
            // Rotation rewrites segment files in place. Committed writes still
            // sitting in the transaction log must be checkpointed into the
            // segments first, both so the segment file exists for WAL-only
            // collections and so a later WAL replay cannot resurrect
            // old-key envelopes over the rotated segment.
            let plan = self.checkpoint_begin()?;
            self.checkpoint_write_segments(&plan)?;
            self.checkpoint_truncate_wal(plan.keep_from())?;
        }
        let collection_names = self.collections.keys().cloned().collect::<Vec<_>>();
        for collection in collection_names {
            let Some(policy) = self
                .collection_state(&collection)?
                .meta
                .policy
                .clone()
                .filter(|policy| policy.columns.values().any(|column| column.encrypted))
            else {
                continue;
            };
            report.protected_collections += 1;
            let mut rotated_policy = policy.clone();
            let mut records = self
                .collection_state(&collection)?
                .read_all()
                .iter()
                .flat_map(|shard| shard.records.values())
                .map(|entry| entry.record.to_record())
                .collect::<Result<Vec<_>>>()?;
            records.sort_by(|left, right| left.id.cmp(&right.id));
            for record in &mut records {
                let rotated = protected_data::rotate_record(
                    &self.sync_state.node_id.to_string(),
                    &self.path,
                    &collection,
                    &mut rotated_policy,
                    record,
                    &options.old_key_material,
                    &options.new_key_material,
                    &options.new_key_ref,
                )?;
                if rotated > 0 {
                    report.records_verified += 1;
                    report.fields_rotated += rotated;
                }
            }
            if options.dry_run {
                continue;
            }

            let segment_path = self.segment_path(&collection);
            let backup_path = segment_path
                .with_extension(format!("seg.phi-rotation-backup-{}", unix_timestamp()));
            fs::copy(&segment_path, &backup_path)?;
            let offsets = rewrite_record_segment(
                &segment_path,
                &records,
                self.config.fsync,
                &self.config.compression,
                &self.encryption,
            )?;
            {
                let state = self.collection_state_mut(&collection)?;
                state.meta.policy = Some(rotated_policy);
                for shard in state.shards_mut() {
                    // Full rewrite from live records: reset the rowid allocator and
                    // registry so locators are re-derived fresh, and drop the now-stale
                    // dirty set (everything lands in the rewritten segment, so clean).
                    shard.records.clear();
                    shard.pk_to_rowid.clear();
                    shard.next_local = 0;
                    shard.dirty_resident.clear();
                    shard.dirty_unresolved.clear();
                }
                for (record, offset) in records.into_iter().zip(offsets) {
                    let id = record.id.clone();
                    let shard = state.shard_mut(&id);
                    let rowid = shard.rowid_or_alloc(&id);
                    shard.records.insert(
                        rowid,
                        RecordEntry {
                            offset,
                            record: Arc::new(record.into()),
                        },
                    );
                }
                for shard in state.shards_mut() {
                    shard.versions = versions_from_records(&shard.records, TransactionId(0));
                }
                state.rebuild_vector_store();
                state.generation.fetch_add(1, AtomicOrdering::Relaxed);
                // Segment was fully rewritten from live records (no garbage).
                state.segment_frame_count = state.record_count() as u64;
            }
            self.rebuild_collection_indexes(&collection)?;
            self.refresh_hnsw_after_upsert(&collection, true)?;
            report.backups.push(backup_path);
        }

        if !options.dry_run && report.protected_collections > 0 {
            self.persist_catalog()?;
            self.refresh_graph_projections()?;
            report.old_key_retired = true;
        }
        Ok(report)
    }

    pub fn verify_integrity(&self) -> Result<IntegrityReport> {
        let metadata = encryption::load_metadata(&self.path)?;
        let mut report = verify_integrity_for(
            &self.path,
            self.config.read_mode,
            &self.encryption,
            metadata.as_ref(),
            self.collections().as_slice(),
        )?;
        if let Some(paged) = &self.paged_records {
            report.paged_storage = Some(paged.verify_integrity(64)?);
        }
        Ok(report)
    }

    pub fn verify_path(
        path: impl AsRef<Path>,
        config: DbConfig,
        encryption_config: Option<EncryptionConfig>,
    ) -> Result<IntegrityReport> {
        let path = path.as_ref().to_path_buf();
        format::verify_open_compatible(&path)?;
        let encryption = encryption::runtime_for_existing(&path, encryption_config)?;
        let metadata = encryption::load_metadata(&path)?;
        let metas = load_catalog(&path.join(DEFAULT_COLLECTION_CATALOG), &encryption)?;
        verify_integrity_for(
            &path,
            config.read_mode,
            &encryption,
            metadata.as_ref(),
            metas.as_slice(),
        )
    }

    pub fn sync(&mut self) -> SyncMesh<'_> {
        SyncMesh { db: self }
    }

    pub fn pending_changes(&self) -> SyncPendingChanges {
        let pending_events = self
            .events
            .lock()
            .read_since(self.sync_state.last_export_offset)
            .len();
        SyncPendingChanges {
            node_id: self.node_id(),
            pending_events,
            from_checkpoint: SyncCheckpoint::new(self.sync_state.last_export_offset),
        }
    }

    pub fn sync_status(&self) -> SyncStatus {
        let total_events = self.events.lock().read_since(0).len();
        let pending_events = self
            .events
            .lock()
            .read_since(self.sync_state.last_export_offset)
            .len();
        SyncStatus {
            node_id: self.node_id(),
            total_events,
            pending_events,
            last_export_at: self.sync_state.last_export_at,
            last_import_at: self.sync_state.last_import_at,
            last_sync_at: self.sync_state.last_sync_at,
            last_export_checkpoint: SyncCheckpoint::new(self.sync_state.last_export_offset),
        }
    }

    pub fn last_sync(&self) -> Option<i64> {
        self.sync_state.last_sync_at
    }

    /// Returns the in-memory high-availability role without touching the
    /// filesystem. Callers that only need to know whether the database is a
    /// standby should prefer this over [`Self::ha_status`], which walks the
    /// entire data directory to compute durable byte totals.
    pub fn ha_role(&self) -> HaRole {
        self.ha_state.role
    }

    pub fn ha_status(&self) -> Result<HaStatus> {
        let durable = storage::total_dir_size(&self.path)?;
        Ok(HaStatus {
            role: self.ha_state.role,
            read_only: self.ha_state.role == HaRole::Standby,
            ready: self.ha_state.last_apply_error.is_none(),
            source_path: self.ha_state.source_path.clone(),
            source_checkpoint_bytes: self.ha_state.source_checkpoint_bytes,
            applied_checkpoint_bytes: self.ha_state.applied_checkpoint_bytes,
            lag_bytes: self
                .ha_state
                .source_checkpoint_bytes
                .saturating_sub(self.ha_state.applied_checkpoint_bytes),
            last_apply_at: self.ha_state.last_apply_at,
            last_apply_error: self.ha_state.last_apply_error.clone(),
            promoted_at: self.ha_state.promoted_at,
            last_durable_checkpoint_bytes: durable,
        })
    }

    pub fn configure_standby_from(
        standby_path: impl AsRef<Path>,
        source_path: impl AsRef<Path>,
    ) -> Result<HaApplyReport> {
        ship_to_standby(source_path.as_ref(), standby_path.as_ref())
    }

    pub fn promote_standby(path: impl AsRef<Path>, force: bool) -> Result<HaStatus> {
        let path = path.as_ref();
        let mut state = load_ha_state(path)?;
        if state.role != HaRole::Standby {
            return Err(BicDbError::HighAvailability(
                "only standby databases can be promoted".to_string(),
            ));
        }
        let lag = state
            .source_checkpoint_bytes
            .saturating_sub(state.applied_checkpoint_bytes);
        if lag > 0 && !force {
            return Err(BicDbError::HighAvailability(format!(
                "standby has {lag} bytes of replication lag; pass force only after fencing the old primary"
            )));
        }
        if let Some(error) = state.last_apply_error.as_ref() {
            if !force {
                return Err(BicDbError::HighAvailability(format!(
                    "standby has apply error `{error}`; pass force only after manual verification"
                )));
            }
        }
        state.role = HaRole::Primary;
        state.promoted_at = Some(unix_timestamp());
        state.last_apply_error = None;
        persist_ha_state(path, &state, true)?;
        let db = BicDb::open(path)?;
        db.ha_status()
    }

    pub fn queue(&mut self, name: &str) -> EventQueue<'_> {
        self.events.get_mut().queue(name)
    }

    /// Durable-broker facade over the event store: consumer groups, ack/nack,
    /// retry with visibility timeouts, and dead-letter queues.
    pub fn broker(&mut self) -> crate::broker::Broker<'_> {
        self.events.get_mut().broker()
    }

    /// Runs `f` against the broker under the event-store lock. Usable from
    /// shared (`&BicDb`) contexts such as concurrent SQL execution; broker
    /// operations are internally serialized by this lock and are durable
    /// immediately (not gated on any SQL transaction).
    pub fn with_broker<T>(&self, f: impl FnOnce(&mut crate::broker::Broker<'_>) -> T) -> T {
        let mut events = self.events.lock();
        f(&mut events.broker())
    }

    pub fn export_events_since(&self, offset: u64) -> Vec<StoredEvent> {
        self.events
            .lock()
            .read_since(offset)
            .into_iter()
            .filter(|event| !self.event_targets_mesh_ineligible_collection(event))
            .collect()
    }

    pub(crate) fn backup_event_bounds(&self) -> (Option<i64>, Option<i64>, usize) {
        self.events
            .lock()
            .events_iter()
            .filter(|event| !self.event_targets_protected_collection(event))
            .fold((None, None, 0_usize), |(minimum, maximum, count), event| {
                let timestamp = event.event.timestamp;
                (
                    Some(minimum.map_or(timestamp, |value: i64| value.min(timestamp))),
                    Some(maximum.map_or(timestamp, |value: i64| value.max(timestamp))),
                    count.saturating_add(1),
                )
            })
    }

    pub fn import_events<I>(&mut self, events: I) -> Result<usize>
    where
        I: IntoIterator<Item = StoredEvent>,
    {
        let mut imported = 0;
        for event in events {
            if self.event_targets_mesh_ineligible_collection(&event) {
                return Err(BicDbError::Authorization(
                    "raw event import cannot target an unknown, protected, or mesh-disabled collection"
                        .to_string(),
                ));
            }
            if self.events.lock().append_imported(event.event)? {
                imported += 1;
            }
        }
        if imported > 0 {
            self.refresh_graph_projections()?;
        }
        Ok(imported)
    }

    pub fn export_sync_bundle_since(&mut self, checkpoint: SyncCheckpoint) -> Result<SyncBundle> {
        let events = self
            .events
            .lock()
            .read_since(checkpoint.event_offset)
            .into_iter()
            .filter(|event| !self.event_targets_mesh_ineligible_collection(event))
            .collect::<Vec<_>>();
        let next_offset = events
            .iter()
            .map(|event| event.offset.saturating_add(1))
            .max()
            .unwrap_or(checkpoint.event_offset);
        let bundle_events = events
            .iter()
            .map(|event| {
                let mut entry = sync_mesh::bundle_event_for_stored_based(
                    &self.sync_state.node_id,
                    self.sync_state.origin_position_base,
                    event,
                )?;
                self.sign_own_envelope(&mut entry);
                Ok(entry)
            })
            .collect::<Result<Vec<_>>>()?;
        let bundle = SyncBundle::new(
            self.node_id(),
            checkpoint,
            SyncCheckpoint::new(next_offset),
            bundle_events,
        )?;
        Ok(bundle)
    }

    /// Per-origin coverage over the exportable event space: for every
    /// authoring node (this one included), the highest origin sequence held.
    /// Events targeting protected collections never leave this node, so they
    /// are excluded here too — the vector describes exactly what a peer could
    /// receive. Derived from the durable log (which makes it correct after a
    /// crash mid-import: only durably appended events count), maintained
    /// incrementally: each call folds in only the event tail appended since
    /// the previous call.
    pub fn sync_vector(&self) -> Result<SyncVector> {
        let mut cache = self.sync_vector_cache.lock();
        let events = self.events.lock();
        for stored in events.events_iter() {
            if stored.offset < cache.next_offset {
                continue;
            }
            cache.next_offset = stored.offset.saturating_add(1);
            if self.event_targets_mesh_ineligible_collection(stored) {
                continue;
            }
            let mut entry = sync_mesh::bundle_event_for_stored_based(
                &self.sync_state.node_id,
                self.sync_state.origin_position_base,
                stored,
            )?;
            self.sign_own_envelope(&mut entry);
            cache.vector.observe_envelope(&entry.envelope);
        }
        Ok(cache.vector.clone())
    }

    /// Event-log rewrites (compaction) may reassign offsets; the incremental
    /// vector cache must restart from the beginning afterwards.
    pub(crate) fn invalidate_sync_vector_cache(&self) {
        *self.sync_vector_cache.lock() = SyncVectorCache::default();
    }

    /// Signs a locally-authored envelope when mesh signing is on. Foreign
    /// envelopes keep their origin's signature untouched — a relay never
    /// signs on behalf of an author.
    pub(crate) fn sign_own_envelope(&self, entry: &mut sync_mesh::SyncBundleEvent) {
        let Some(signer) = self.mesh_signer.as_ref() else {
            return;
        };
        if entry.envelope.node_id != self.sync_state.node_id || entry.envelope.signature.is_some() {
            return;
        }
        let message = sync_mesh::envelope_signing_message(entry);
        let signature = ed25519_dalek::Signer::sign(signer, &message);
        entry.envelope.signature = Some(hex::encode(signature.to_bytes()));
    }

    /// Mesh delta exchange: everything this node holds that `peer`'s vector
    /// does not cover, regardless of which node authored it. Imported foreign
    /// events are re-exported with their origin envelopes intact, so relaying
    /// through intermediate nodes (store-and-forward) preserves attribution
    /// and lets any pair of nodes converge without a shared hub. The returned
    /// bundle carries this node's own vector as `source_vector`.
    pub fn export_sync_bundle_delta(&mut self, peer: &SyncVector) -> Result<SyncBundle> {
        let mut own_vector = SyncVector::default();
        let mut bundle_events = Vec::new();
        let mut next_offset = 0_u64;
        {
            let events = self.events.lock();
            for stored in events.events_iter() {
                next_offset = next_offset.max(stored.offset.saturating_add(1));
                if self.event_targets_mesh_ineligible_collection(stored) {
                    continue;
                }
                let mut entry = sync_mesh::bundle_event_for_stored_based(
                    &self.sync_state.node_id,
                    self.sync_state.origin_position_base,
                    stored,
                )?;
                self.sign_own_envelope(&mut entry);
                own_vector.observe_envelope(&entry.envelope);
                if peer.covers(&entry.envelope.node_id, entry.envelope.sequence) {
                    continue;
                }
                bundle_events.push(entry);
            }
        }
        let bundle = SyncBundle::new(
            self.node_id(),
            SyncCheckpoint::default(),
            SyncCheckpoint::new(next_offset),
            bundle_events,
        )?
        .with_source_vector(own_vector);
        Ok(bundle)
    }

    pub fn write_sync_bundle_since(
        &mut self,
        checkpoint: SyncCheckpoint,
        path: impl AsRef<Path>,
    ) -> Result<SyncExportReport> {
        let bundle = self.export_sync_bundle_since(checkpoint)?;
        bundle.write_atomic(path.as_ref(), self.config.fsync)?;
        self.mark_sync_bundle_exported(&bundle)?;
        Ok(SyncExportReport {
            bundle_id: bundle.bundle_id,
            source_node_id: bundle.source_node_id,
            event_count: bundle.event_count,
            from_checkpoint: bundle.from_checkpoint,
            next_checkpoint: bundle.next_checkpoint,
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn write_encrypted_sync_bundle_since(
        &mut self,
        checkpoint: SyncCheckpoint,
        path: impl AsRef<Path>,
        bundle_encryption: EncryptionConfig,
    ) -> Result<SyncExportReport> {
        let bundle = self.export_sync_bundle_since(checkpoint)?;
        bundle.write_encrypted_atomic(path.as_ref(), self.config.fsync, bundle_encryption)?;
        self.mark_sync_bundle_exported(&bundle)?;
        Ok(SyncExportReport {
            bundle_id: bundle.bundle_id,
            source_node_id: bundle.source_node_id,
            event_count: bundle.event_count,
            from_checkpoint: bundle.from_checkpoint,
            next_checkpoint: bundle.next_checkpoint,
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn import_sync_bundle(&mut self, bundle: SyncBundle) -> Result<SyncImportReport> {
        self.import_sync_bundle_inner(bundle, self.config.require_signed_imports)
    }

    /// Imports with signature verification forced on regardless of the
    /// offline-import compatibility setting. Network transports use this
    /// after authenticating the directly connected peer.
    pub fn import_sync_bundle_strict(&mut self, bundle: SyncBundle) -> Result<SyncImportReport> {
        self.import_sync_bundle_inner(bundle, true)
    }

    pub(crate) fn import_sync_bundle_inner(
        &mut self,
        bundle: SyncBundle,
        require_signed: bool,
    ) -> Result<SyncImportReport> {
        bundle.verify()?;
        // A write context is evidence only for origin positions this receiver
        // already holds or has seen earlier in this same bundle. This binds
        // causal dominance to observable history and prevents a signed but
        // malicious writer from claiming MAX watermarks to erase concurrent
        // victims from the frontier.
        let mut evidenced_context = self.sync_vector()?;
        for entry in &bundle.events {
            self.verify_envelope_signature(entry, require_signed)?;
            let event = event_with_sync_metadata(entry.event.clone(), &entry.envelope)?;
            let stored = StoredEvent { offset: 0, event };
            if self.event_targets_mesh_ineligible_collection(&stored) {
                return Err(BicDbError::Authorization(
                    "sync import cannot target an unknown, protected, or mesh-disabled collection"
                        .to_string(),
                ));
            }
            if let Some(mutation) = AuditMutation::from_event_based(
                &self.sync_state.node_id,
                self.sync_state.origin_position_base,
                &stored,
            )? {
                if let Some(lock) = self.collections.get(&mutation.collection) {
                    let state = lock.read();
                    if state.meta.mode != mutation.collection_mode {
                        return Err(BicDbError::SyncBundle(format!(
                            "mesh collection `{}` mode does not match the provisioned schema",
                            mutation.collection
                        )));
                    }
                }
                if let Some(context) = mutation.context.as_ref() {
                    for (origin, claimed_sequence) in &context.origins {
                        let evidenced_sequence = evidenced_context.origins.get(origin).copied();
                        if evidenced_sequence.is_none_or(|sequence| sequence < *claimed_sequence) {
                            return Err(BicDbError::SyncBundle(format!(
                                "unproven write_context for origin `{origin}` at sequence {claimed_sequence}"
                            )));
                        }
                    }
                }
            }
            evidenced_context.observe_envelope(&entry.envelope);
        }
        let mut imported_events = 0;
        for entry in &bundle.events {
            let event = event_with_sync_metadata(entry.event.clone(), &entry.envelope)?;
            if self.events.lock().append_imported(event)? {
                imported_events += 1;
            }
        }
        let duplicate_events = bundle.event_count.saturating_sub(imported_events);
        let merge = self.reconcile_record_audit_events()?;
        let now = sync_mesh::unix_timestamp();
        self.sync_state.last_import_at = Some(now);
        self.sync_state.last_sync_at = Some(now);
        self.sync_state.imported_events = self
            .sync_state
            .imported_events
            .saturating_add(imported_events as u64);
        self.sync_state.conflicts_resolved = self
            .sync_state
            .conflicts_resolved
            .saturating_add(merge.conflicts_resolved as u64);
        self.persist_sync_state()?;
        self.refresh_graph_projections()?;

        Ok(SyncImportReport {
            bundle_id: bundle.bundle_id,
            source_node_id: bundle.source_node_id,
            imported_events,
            duplicate_events,
            records_merged: merge.records_merged,
            conflicts_resolved: merge.conflicts_resolved,
            last_sync_at: now,
        })
    }

    pub fn import_sync_bundle_file(&mut self, path: impl AsRef<Path>) -> Result<SyncImportReport> {
        let bundle = SyncBundle::read(path)?;
        self.import_sync_bundle(bundle)
    }

    pub fn import_sync_bundle_file_auto(
        &mut self,
        path: impl AsRef<Path>,
        bundle_encryption: Option<EncryptionConfig>,
    ) -> Result<SyncImportReport> {
        let bundle = SyncBundle::read_auto(path, bundle_encryption)?;
        self.import_sync_bundle(bundle)
    }

    /// Enforces origin authenticity on one imported envelope. A signature
    /// from a pinned origin must verify (bad = refuse the whole bundle — an
    /// alarm, not a skip). Under `require_signed_imports`, unsigned events
    /// and unpinned origins are refused too. Own-origin events are always
    /// checked against our own key when signing is on: nobody forges *us*
    /// back to ourselves.
    pub(crate) fn verify_envelope_signature(
        &self,
        entry: &sync_mesh::SyncBundleEvent,
        require_signed: bool,
    ) -> Result<()> {
        let envelope = &entry.envelope;
        let origin = envelope.node_id.to_string();
        let pinned = self
            .sync_state
            .known_node_keys
            .get(&origin)
            .cloned()
            .or_else(|| {
                (envelope.node_id == self.sync_state.node_id)
                    .then(|| self.mesh_verifying_key())
                    .flatten()
            });
        match (&envelope.signature, pinned) {
            (Some(signature_hex), Some(key_hex)) => {
                let key_bytes: [u8; 32] = hex::decode(&key_hex)
                    .ok()
                    .and_then(|bytes| bytes.try_into().ok())
                    .ok_or_else(|| {
                        BicDbError::SyncBundle(format!("pinned key for {origin} is malformed"))
                    })?;
                let verifying_key = VerifyingKey::from_bytes(&key_bytes).map_err(|_| {
                    BicDbError::SyncBundle(format!("pinned key for {origin} is invalid"))
                })?;
                let signature_bytes: [u8; 64] = hex::decode(signature_hex)
                    .ok()
                    .and_then(|bytes| bytes.try_into().ok())
                    .ok_or_else(|| {
                        BicDbError::SyncBundle(format!(
                            "event {} carries a malformed signature",
                            envelope.event_id
                        ))
                    })?;
                let signature = Signature::from_bytes(&signature_bytes);
                verifying_key
                    // Strict verification matters here specifically: this
                    // signature IS the origin's identity on a replicated
                    // event, and signature bytes participate in dedup and
                    // replay reasoning. A non-canonical re-encoding of an
                    // accepted signature must not read as a different,
                    // also-valid envelope.
                    .verify_strict(&sync_mesh::envelope_signing_message(entry), &signature)
                    .map_err(|_| {
                        BicDbError::SyncBundle(format!(
                            "event {} from {origin} fails origin signature verification",
                            envelope.event_id
                        ))
                    })
            }
            (Some(_), None) if require_signed => Err(BicDbError::SyncBundle(format!(
                "signed event from unpinned origin {origin} refused under require_signed_imports"
            ))),
            (None, Some(_)) => Err(BicDbError::SyncBundle(format!(
                "unsigned event {} from pinned origin {origin} refused",
                envelope.event_id
            ))),
            (None, _) if require_signed => Err(BicDbError::SyncBundle(format!(
                "unsigned event {} from {origin} refused under require_signed_imports",
                envelope.event_id
            ))),
            _ => Ok(()),
        }
    }

    /// This node's hex ed25519 verifying key, when mesh signing is on —
    /// what peers pin.
    pub fn mesh_verifying_key(&self) -> Option<String> {
        self.mesh_signer
            .as_ref()
            .map(|signer| hex::encode(signer.verifying_key().to_bytes()))
    }

    /// Sign a session-bound coverage proof. Binding both hello nonces and both
    /// node identities prevents a captured vector from being replayed in a
    /// different session or reflected back at its signer.
    pub fn sign_mesh_coverage(
        &self,
        protocol: u32,
        peer: &NodeId,
        initiator_nonce: &str,
        responder_nonce: &str,
        vector: &SyncVector,
    ) -> Result<String> {
        let signer = self.mesh_signer.as_ref().ok_or_else(|| {
            BicDbError::SyncBundle("mesh coverage signing requires mesh_signing".to_string())
        })?;
        let message = mesh_coverage_signing_message(
            protocol,
            &self.sync_state.node_id,
            peer,
            initiator_nonce,
            responder_nonce,
            vector,
        )?;
        Ok(hex::encode(
            ed25519_dalek::Signer::sign(signer, &message).to_bytes(),
        ))
    }

    pub fn verify_mesh_coverage(
        &self,
        protocol: u32,
        peer: &NodeId,
        peer_key_hex: &str,
        initiator_nonce: &str,
        responder_nonce: &str,
        vector: &SyncVector,
        signature_hex: &str,
    ) -> Result<()> {
        self.require_pinned_node_key(peer, peer_key_hex)?;
        let key: [u8; 32] = hex::decode(peer_key_hex)
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| BicDbError::SyncBundle("peer signing key is invalid".to_string()))?;
        let signature = hex::decode(signature_hex)
            .ok()
            .and_then(|bytes| Signature::from_slice(&bytes).ok())
            .ok_or_else(|| BicDbError::SyncBundle("coverage signature is invalid".to_string()))?;
        let message = mesh_coverage_signing_message(
            protocol,
            peer,
            &self.sync_state.node_id,
            initiator_nonce,
            responder_nonce,
            vector,
        )?;
        VerifyingKey::from_bytes(&key)
            .map_err(|_| BicDbError::SyncBundle("peer signing key is invalid".to_string()))?
            .verify_strict(&message, &signature)
            .map_err(|_| {
                BicDbError::SyncBundle("coverage signature verification failed".to_string())
            })
    }

    /// Trust-on-first-use pin: associates `node` with a verifying key. A
    /// second pin with the same key is a no-op; a conflicting key is an
    /// error — key changes are alarms until certificate chains (Phase 2)
    /// provide legitimate rotation.
    pub fn pin_node_key(&mut self, node: &NodeId, verifying_key_hex: &str) -> Result<()> {
        let key_bytes: [u8; 32] = hex::decode(verifying_key_hex)
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| {
                BicDbError::SyncBundle(format!(
                    "verifying key for node {node} must be exactly 32 hex-encoded bytes"
                ))
            })?;
        VerifyingKey::from_bytes(&key_bytes).map_err(|_| {
            BicDbError::SyncBundle(format!("verifying key for node {node} is invalid"))
        })?;
        match self.sync_state.known_node_keys.get(&node.to_string()) {
            Some(existing) if existing == verifying_key_hex => Ok(()),
            Some(_) => Err(BicDbError::SyncBundle(format!(
                "node {node} announced a key conflicting with its pin; refusing (possible impersonation)"
            ))),
            None => {
                self.sync_state
                    .known_node_keys
                    .insert(node.to_string(), verifying_key_hex.to_string());
                self.persist_sync_state()
            }
        }
    }

    pub fn pinned_node_key(&self, node: &NodeId) -> Option<String> {
        self.sync_state
            .known_node_keys
            .get(&node.to_string())
            .cloned()
    }

    /// Verifies an out-of-band provisioned peer key without creating or
    /// changing trust state. Network sessions call this before exchanging
    /// vectors or exporting any events.
    pub fn require_pinned_node_key(&self, node: &NodeId, verifying_key_hex: &str) -> Result<()> {
        match self.sync_state.known_node_keys.get(&node.to_string()) {
            Some(existing) if existing == verifying_key_hex => Ok(()),
            Some(_) => Err(BicDbError::SyncBundle(format!(
                "node {node} announced a key conflicting with its provisioned pin; refusing"
            ))),
            None => Err(BicDbError::SyncBundle(format!(
                "node {node} has no provisioned key pin; refusing unauthenticated mesh session"
            ))),
        }
    }

    /// Records what a peer's coverage looked like (from a session hello or a
    /// bundle's `source_vector`), merged max-wise since vectors only grow.
    /// Feeds [`Self::mesh_peer_status`]; never consulted for correctness.
    pub fn record_peer_vector(&mut self, peer: &NodeId, vector: &SyncVector) -> Result<()> {
        if *peer == self.sync_state.node_id {
            return Ok(());
        }
        self.sync_vector()?.ensure_matching_equal_prefixes(vector)?;
        if let Some(previous) = self.sync_state.peer_vectors.get(&peer.to_string()) {
            previous.ensure_matching_equal_prefixes(vector)?;
        }
        self.sync_state
            .peer_vectors
            .entry(peer.to_string())
            .or_default()
            .merge(vector);
        self.persist_sync_state()
    }

    /// Per-peer mesh sync status: for every peer this node has ever synced
    /// with, how many exportable events it holds that the peer's last known
    /// vector did not cover. One pass over the event log for all peers.
    pub fn mesh_peer_status(&self) -> Result<Vec<PeerSyncStatus>> {
        let peers: Vec<(NodeId, SyncVector)> = self
            .sync_state
            .peer_vectors
            .iter()
            .filter_map(|(node, vector)| {
                node.parse::<NodeId>()
                    .ok()
                    .map(|node| (node, vector.clone()))
            })
            .collect();
        let mut pending = vec![0_usize; peers.len()];
        {
            let events = self.events.lock();
            for stored in events.events_iter() {
                if self.event_targets_mesh_ineligible_collection(stored) {
                    continue;
                }
                let envelope = sync_mesh::envelope_for_event_based(
                    &self.sync_state.node_id,
                    self.sync_state.origin_position_base,
                    stored,
                )?;
                for (index, (_, vector)) in peers.iter().enumerate() {
                    if !vector.covers(&envelope.node_id, envelope.sequence) {
                        pending[index] += 1;
                    }
                }
            }
        }
        Ok(peers
            .into_iter()
            .zip(pending)
            .map(
                |((node_id, last_known_vector), pending_events_for_peer)| PeerSyncStatus {
                    node_id,
                    last_known_vector,
                    pending_events_for_peer,
                },
            )
            .collect())
    }

    pub(crate) fn write_clock_value(&self) -> serde_json::Value {
        json!({
            "session": self.runtime_session_id.to_string(),
            "monotonic_ms": self.opened_at.elapsed().as_millis() as u64,
        })
    }

    /// Records a live-session clock measurement as a replicated event:
    /// "peer's clock is `offset_ms` ahead of mine, ± `error_ms`". Because
    /// observations replicate like any other event, every converged replica
    /// derives the same clock table, keeping timing-informed resolution
    /// deterministic. Rate-limited: a fresh observation for the same pair is
    /// only appended when it moves beyond the previous error bar or the
    /// previous one is over an hour old. Returns whether an event was
    /// appended.
    pub fn record_clock_observation(
        &mut self,
        peer: &NodeId,
        offset_ms: i64,
        error_ms: i64,
    ) -> Result<bool> {
        let observer = self.node_id();
        if *peer == observer {
            return Ok(false);
        }
        let table = self.clock_pair_table()?;
        if let Some(existing) = table.between(&observer, peer) {
            // 250ms floor: sub-quarter-second wobble is irrelevant to a
            // resolver whose minimum margin is two seconds, and without the
            // floor every session would mint a new observation from
            // measurement jitter alone.
            let moved =
                (existing.offset_ms - offset_ms).abs() > existing.error_ms.max(error_ms).max(250);
            let stale = sync_mesh::unix_timestamp() - existing.timestamp > 3_600;
            // Precision upgrades only matter while the existing error is
            // above the floor: refining 5ms to 1ms changes nothing for a
            // resolver whose minimum margin is two seconds, and re-appending
            // for it would make session behavior scheduling-dependent.
            let much_tighter = existing.error_ms > 250 && error_ms < existing.error_ms / 4;
            if !moved && !stale && !much_tighter {
                return Ok(false);
            }
        }
        self.events.lock().append(Event::new(
            CLOCK_OBSERVATION_STREAM,
            "ClockObserved",
            json!({
                "observer": observer.to_string(),
                "peer": peer.to_string(),
                "offset_ms": offset_ms,
                "error_ms": error_ms.max(0),
            }),
        ))?;
        Ok(true)
    }

    /// Builds the deterministic pairwise clock table from replicated
    /// observation events. Only observations whose envelope origin matches
    /// the claimed observer count — a relay cannot attribute a measurement
    /// to a node that never made it (full spoof-proofing arrives with
    /// signed frames).
    pub(crate) fn clock_pair_table(&self) -> Result<ClockPairTable> {
        let mut table = ClockPairTable::default();
        let events = self.events.lock();
        for stored in events.events_iter() {
            if stored.event.stream != CLOCK_OBSERVATION_STREAM {
                continue;
            }
            let envelope = sync_mesh::envelope_for_event_based(
                &self.sync_state.node_id,
                self.sync_state.origin_position_base,
                stored,
            )?;
            let (Some(observer), Some(peer)) = (
                stored
                    .event
                    .payload
                    .get("observer")
                    .and_then(Value::as_str)
                    .and_then(|value| value.parse::<NodeId>().ok()),
                stored
                    .event
                    .payload
                    .get("peer")
                    .and_then(Value::as_str)
                    .and_then(|value| value.parse::<NodeId>().ok()),
            ) else {
                continue;
            };
            if envelope.node_id != observer {
                continue;
            }
            let (Some(offset_ms), Some(error_ms)) = (
                stored
                    .event
                    .payload
                    .get("offset_ms")
                    .and_then(Value::as_i64),
                stored.event.payload.get("error_ms").and_then(Value::as_i64),
            ) else {
                continue;
            };
            table.observe(
                ClockPairObservation {
                    observer,
                    offset_ms,
                    error_ms: error_ms.max(0),
                    timestamp: stored.event.timestamp,
                    event_id: stored.event.id,
                },
                &peer,
            );
        }
        Ok(table)
    }

    /// Whether this record's latest writes are genuinely concurrent — none of
    /// them had seen the others. `None` means either no conflict or causal
    /// resolution: a write whose context covers all rivals clears the
    /// conflict on every replica. Deterministic across converged replicas.
    pub fn record_conflict(
        &self,
        collection: &str,
        record_id: &str,
    ) -> Result<Option<RecordConflict>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        let mut mutations = Vec::new();
        for stored in self.events.lock().read(RECORD_AUDIT_STREAM) {
            let Some(mutation) = AuditMutation::from_event_based(
                &self.sync_state.node_id,
                self.sync_state.origin_position_base,
                &stored,
            )?
            else {
                continue;
            };
            if mutation.collection == collection && mutation.record_id == record_id {
                mutations.push(mutation);
            }
        }
        Ok(record_conflict_from_mutations(
            collection, record_id, &mutations,
        ))
    }

    /// Every unresolved concurrent record in the collection — the review
    /// queue an application shows humans. Deterministic across converged
    /// replicas.
    pub fn list_record_conflicts(&self, collection: &str) -> Result<Vec<RecordConflict>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        let mut mutations_by_record = BTreeMap::<String, Vec<AuditMutation>>::new();
        for stored in self.events.lock().read(RECORD_AUDIT_STREAM) {
            let Some(mutation) = AuditMutation::from_event_based(
                &self.sync_state.node_id,
                self.sync_state.origin_position_base,
                &stored,
            )?
            else {
                continue;
            };
            if mutation.collection == collection {
                mutations_by_record
                    .entry(mutation.record_id.clone())
                    .or_default()
                    .push(mutation);
            }
        }
        Ok(mutations_by_record
            .iter()
            .filter_map(|(record_id, mutations)| {
                record_conflict_from_mutations(collection, record_id, mutations)
            })
            .collect())
    }

    pub fn snapshot_at(&self, timestamp: i64) -> Result<DbSnapshot> {
        let mut snapshot = DbSnapshot::default();

        for stored in self.events.lock().read(RECORD_AUDIT_STREAM) {
            if stored.event.timestamp > timestamp {
                continue;
            }

            let collection = event_payload_string(&stored.event, "collection")?.to_string();
            let record_id = event_payload_string(&stored.event, "record_id")?.to_string();
            match stored.event.event_type.as_str() {
                "RecordCreated" | "RecordUpdated" => {
                    let record = stored
                        .event
                        .payload
                        .get("record")
                        .cloned()
                        .ok_or_else(|| {
                            BicDbError::ProjectionError(
                                "record audit event is missing `record`".to_string(),
                            )
                        })
                        .and_then(|value| serde_json::from_value(value).map_err(Into::into))?;
                    snapshot
                        .records
                        .entry(collection)
                        .or_default()
                        .insert(record_id, record);
                }
                "RecordDeleted" => {
                    if let Some(records) = snapshot.records.get_mut(&collection) {
                        records.remove(&record_id);
                    }
                }
                _ => {}
            }
        }

        Ok(snapshot)
    }

    pub(crate) fn restore_audit_snapshot_bounded(
        &self,
        timestamp: i64,
        max_records_per_batch: usize,
        max_events_per_batch: usize,
        max_event_bytes_per_batch: usize,
    ) -> Result<usize> {
        if max_records_per_batch == 0 || max_events_per_batch == 0 || max_event_bytes_per_batch == 0
        {
            return Err(BicDbError::Backup(
                "PITR replay batch limits must be greater than zero".to_string(),
            ));
        }

        let mut after_offset = None;
        let mut cleared_collections = BTreeSet::new();
        loop {
            let batch = self.pitr_audit_batch(
                after_offset,
                timestamp,
                max_events_per_batch,
                max_event_bytes_per_batch,
            )?;

            for action in &batch.actions {
                if cleared_collections.insert(action.collection.clone()) {
                    loop {
                        let removed = self.pitr_clear_collection_batch(
                            &action.collection,
                            max_records_per_batch,
                        )?;
                        if removed == 0 {
                            break;
                        }
                    }
                }
            }
            self.pitr_apply_actions(batch.actions)?;

            after_offset = batch.last_scanned_offset.or(after_offset);
            if batch.exhausted {
                break;
            }
            if batch.last_scanned_offset.is_none() {
                return Err(BicDbError::Backup(
                    "PITR audit replay made no progress within its configured byte limit"
                        .to_string(),
                ));
            }
        }

        cleared_collections
            .into_iter()
            .try_fold(0_usize, |total, collection| {
                self.collection_record_count(&collection).and_then(|count| {
                    total.checked_add(count).ok_or_else(|| {
                        BicDbError::Backup("PITR restored record count overflow".to_string())
                    })
                })
            })
    }

    pub(crate) fn pitr_audit_batch(
        &self,
        after_offset: Option<u64>,
        timestamp: i64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<PitrAuditBatch> {
        let events = self.events.lock();
        let mut source = events
            .events_iter()
            .filter(|stored| after_offset.is_none_or(|offset| stored.offset > offset))
            .peekable();
        let mut actions = Vec::new();
        let mut scanned = 0_usize;
        let mut charged_bytes = 0_usize;
        let mut last_scanned_offset = None;
        let mut deferred_event = false;

        while scanned < max_events {
            let Some(stored) = source.next() else {
                break;
            };
            if stored.event.stream == RECORD_AUDIT_STREAM {
                let collection = event_payload_string(&stored.event, "collection")?.to_string();
                let has_target_operation = stored.event.timestamp <= timestamp
                    && matches!(
                        stored.event.event_type.as_str(),
                        "RecordCreated" | "RecordUpdated" | "RecordDeleted"
                    );
                let charge = if has_target_operation {
                    serialized_size_bounded(stored, max_bytes)?
                } else {
                    collection.len().saturating_add(128)
                };
                let next_charge = charged_bytes.checked_add(charge).ok_or_else(|| {
                    BicDbError::Backup("PITR replay byte count overflow".to_string())
                })?;
                if next_charge > max_bytes {
                    if actions.is_empty() {
                        return Err(BicDbError::Backup(format!(
                            "one PITR audit event exceeds the configured {max_bytes}-byte batch limit"
                        )));
                    }
                    deferred_event = true;
                    break;
                }
                charged_bytes = next_charge;
                let operation = if has_target_operation {
                    match stored.event.event_type.as_str() {
                        "RecordCreated" | "RecordUpdated" => {
                            let record = stored
                                .event
                                .payload
                                .get("record")
                                .cloned()
                                .ok_or_else(|| {
                                    BicDbError::ProjectionError(
                                        "record audit event is missing `record`".to_string(),
                                    )
                                })
                                .and_then(|value| {
                                    serde_json::from_value(value).map_err(Into::into)
                                })?;
                            Some(PitrAuditOperation::Upsert(record))
                        }
                        "RecordDeleted" => Some(PitrAuditOperation::Delete(
                            event_payload_string(&stored.event, "record_id")?.to_string(),
                        )),
                        _ => None,
                    }
                } else {
                    None
                };
                actions.push(PitrAuditAction {
                    collection,
                    operation,
                });
            }
            scanned = scanned.saturating_add(1);
            last_scanned_offset = Some(stored.offset);
        }

        Ok(PitrAuditBatch {
            actions,
            last_scanned_offset,
            exhausted: !deferred_event && source.peek().is_none(),
        })
    }
}
