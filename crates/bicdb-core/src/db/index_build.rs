//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;

impl BicDb {
    pub(crate) fn remove_physical_block_namespace(
        &self,
        paged: &Arc<crate::paged_collection::PagedRecords>,
        physical: &str,
        order: crate::fts_build::RunOrder,
    ) -> Result<()> {
        let prefix = match order {
            crate::fts_build::RunOrder::Pk => {
                let posting = crate::paged_collection::index_numeric_posting_block_prefix(physical);
                self.remove_raw_prefix(paged, &posting)?;
                let dictionary =
                    crate::paged_collection::index_full_text_dictionary_prefix(physical);
                return self.remove_raw_prefix(paged, &dictionary);
            }
            crate::fts_build::RunOrder::Impact => {
                crate::paged_collection::index_numeric_impact_block_prefix(physical)
            }
        };
        self.remove_raw_prefix(paged, &prefix)
    }

    pub(crate) fn remove_raw_prefix(
        &self,
        paged: &Arc<crate::paged_collection::PagedRecords>,
        prefix: &[u8],
    ) -> Result<()> {
        const CLEANUP_BATCH_ENTRIES: usize = 8_192;
        const CLEANUP_BATCH_BYTES: usize = 64 * 1024 * 1024;

        let mut after = None::<Vec<u8>>;
        loop {
            let snapshot = paged.latest_snapshot();
            let keys = paged.raw_keys_with_prefix_batch_after(
                &snapshot,
                prefix,
                after.as_deref(),
                CLEANUP_BATCH_ENTRIES,
                CLEANUP_BATCH_BYTES,
            )?;
            if keys.is_empty() {
                break;
            }
            let next_after = keys.last().cloned().expect("non-empty cleanup batch");
            self.in_paged_transaction(|paged, xid| {
                for key in &keys {
                    paged.delete_raw(xid, key)?;
                }
                Ok(())
            })?;
            after = Some(next_after);
        }
        Ok(())
    }

    pub(crate) fn publish_full_text_generation(
        &self,
        logical: &str,
        physical: &str,
    ) -> Result<Option<String>> {
        let mut catalog = self.fts_generations.lock();
        let old = catalog
            .indexes
            .insert(logical.to_string(), physical.to_string());
        let bytes = serde_json::to_vec_pretty(&*catalog)?;
        if let Err(error) = storage::write_atomic(
            &self.path.join(DEFAULT_FTS_GENERATION_CATALOG),
            &bytes,
            self.config.fsync,
        ) {
            match old.as_ref() {
                Some(old) => {
                    catalog.indexes.insert(logical.to_string(), old.clone());
                }
                None => {
                    catalog.indexes.remove(logical);
                }
            }
            return Err(error);
        }
        if let Some(paged) = &self.paged_records {
            paged.set_index_alias(logical, physical);
        }
        Ok(old.or_else(|| Some(logical.to_string())))
    }

    pub(crate) fn remove_physical_index_entries(
        &self,
        paged: &Arc<crate::paged_collection::PagedRecords>,
        physical: &str,
    ) -> Result<()> {
        // The packed segment goes with the generation. Readers holding the
        // old Arc keep their open file handles across the unlink.
        paged.drop_fts_segment(physical);
        for prefix in crate::paged_collection::index_purge_prefixes(physical) {
            self.remove_raw_prefix(paged, &prefix)?;
        }
        Ok(())
    }

    /// Emit completed blocks from every term's carried run; keep each term's
    /// trailing partial range unless `freeze_partials`. Returns the number of
    /// postings emitted (for the caller's carry accounting).
    pub(crate) fn flush_direct_block_runs(
        &self,
        name: &str,
        carry: &mut std::collections::BTreeMap<Vec<u8>, Vec<crate::paged_collection::BlockPosting>>,
        freeze_partials: bool,
    ) -> Result<usize> {
        let mut writes: Vec<(Vec<u8>, String, Vec<u8>)> = Vec::new();
        let mut emitted = 0usize;
        for (encoded_key, postings) in carry.iter_mut() {
            let ranges = posting_block_ranges(postings, FTS_BLOCK_DOC_CAP, FTS_BLOCK_BYTE_TARGET);
            let emit = if freeze_partials {
                ranges.len()
            } else {
                ranges.len().saturating_sub(1)
            };
            if emit == 0 {
                continue;
            }
            for range in &ranges[..emit] {
                let (last_pk, block) = encode_posting_run(&postings[range.clone()]);
                writes.push((encoded_key.clone(), last_pk, block));
            }
            let consumed = ranges[emit - 1].end;
            emitted += consumed;
            postings.drain(..consumed);
        }
        carry.retain(|_, postings| !postings.is_empty());
        if writes.is_empty() {
            return Ok(0);
        }
        self.in_paged_transaction(|paged, xid| {
            for (encoded_key, last_pk, block) in &writes {
                paged.put_posting_block(xid, name, encoded_key, last_pk, block)?;
            }
            Ok(())
        })?;
        Ok(emitted)
    }

    /// Delete every v3/v5 block of an index: the legacy-projection unwind,
    /// and the defensive sweep a direct build runs first so a re-backfill
    /// (crashed predecessor, REINDEX) never appends onto stale blocks.
    pub(crate) fn remove_direct_blocks(
        &self,
        paged: &Arc<crate::paged_collection::PagedRecords>,
        name: &str,
    ) -> Result<()> {
        let snapshot = paged.latest_snapshot();
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for prefix in crate::paged_collection::index_block_prefixes(name) {
            keys.extend(paged.keys_with_raw_prefix(&snapshot, &prefix)?);
        }
        for chunk in keys.chunks(8_192) {
            self.in_paged_transaction(|paged, xid| {
                for key in chunk {
                    paged.delete_raw(xid, key)?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    /// Remove every durable entry of a dropped index from the page store.
    pub(crate) fn drop_paged_index_entries(&self, index_name: &str) -> Result<()> {
        let Some(paged) = &self.paged_records else {
            return Ok(());
        };
        let paged = Arc::clone(paged);
        let physical = paged.resolve_index_name(index_name);
        self.remove_physical_index_entries(&paged, &physical)?;
        if physical != index_name {
            self.remove_physical_index_entries(&paged, index_name)?;
        }
        let mut catalog = self.fts_generations.lock();
        if catalog.indexes.remove(index_name).is_some() {
            let bytes = serde_json::to_vec_pretty(&*catalog)?;
            storage::write_atomic(
                &self.path.join(DEFAULT_FTS_GENERATION_CATALOG),
                &bytes,
                self.config.fsync,
            )?;
            paged.remove_index_alias(index_name);
        }
        Ok(())
    }

    /// Populate a lazy paged collection's shards with eviction stubs for every
    /// row in the page store, and clear its lazy flag. No-op for embedded mode
    /// or already-materialized collections.
    pub(crate) fn materialize_lazy_paged_collection(&mut self, collection: &str) -> Result<()> {
        let Some(paged) = &self.paged_records else {
            return Ok(());
        };
        {
            let state = self.collection_state(collection)?;
            if !state.paged_lazy {
                return Ok(());
            }
        }
        let paged = Arc::clone(paged);
        let as_of = paged.latest_snapshot();
        let fetch: PagedStubFetch = {
            let paged = Arc::clone(&paged);
            let name: Arc<str> = Arc::from(collection);
            let as_of = as_of.clone();
            Arc::new(move |pk: &str| paged.get(&as_of, &name, pk))
        };
        let mut stubs: Vec<Arc<StoredRecord>> = Vec::new();
        for record in paged.scan_identities(&as_of, collection)? {
            let record = record?;
            stubs.push(Arc::new(StoredRecord::evicted_stub(
                &record,
                crate::record::EvictedPayload {
                    fetch: Arc::clone(&fetch),
                    pk: Arc::from(record.id.as_str()),
                },
            )));
        }
        // The session's shards stay the base — their rowids are already handed
        // out (version chains, pending transactions), so the scan only adds
        // rows this session has never touched, under freshly allocated rowids.
        // Merging the other way around would collide rowid namespaces.
        let mut state = self.collection_state_write(collection)?;
        for stub in stubs {
            let shard = state.shard_mut(&stub.id);
            if shard.rowid_of(&stub.id).is_some() {
                continue;
            }
            let rowid = shard.rowid_or_alloc(&stub.id);
            let timestamp = stub.timestamp.unwrap_or_default();
            shard.records.insert(
                rowid,
                RecordEntry {
                    offset: 0,
                    record: stub.clone(),
                },
            );
            shard.versions.insert(
                rowid,
                smallvec::smallvec![VersionedRecord {
                    created_tx: TransactionId(0),
                    deleted_tx: None,
                    timestamp,
                    record: stub,
                }],
            );
        }
        state.paged_lazy = false;
        state.rebuild_vector_store();
        state.generation.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(())
    }

    pub fn create_spatial_index(&mut self, collection: &str, field: &str) -> Result<()> {
        let index_field = spatial_field_from_name(field)?;
        let name = spatial_index_name(collection, field);
        self.create_index(IndexDefinition {
            name,
            collection: collection.to_string(),
            fields: vec![index_field],
            unique: false,
            kind: IndexKind::Spatial,
            predicate: None,
            exclusion: None,
        })
    }

    /// Bulk-pack a spatial index into an immutable durable node tree
    /// (Hilbert ordering; see [`Self::pack_spatial_index_with_strategy`]).
    pub fn pack_spatial_index(&mut self, name: &str) -> Result<SpatialPackReport> {
        self.pack_spatial_index_with_strategy(name, SpatialPackStrategy::Hilbert)
    }

    /// Rebuild a spatial index as a PACKED durable base: stream every
    /// (pk, rect) from the page store, spatially order them (Hilbert or STR),
    /// pack leaves to capacity, build parent MBR levels bottom-up, and write
    /// the finished nodes as immutable values in the durable index keyspace.
    /// The new generation is built off to the side and activated by one
    /// atomic meta swap; the retired generation is garbage-collected after.
    ///
    /// After packing, reopen loads the meta plus the (small) durable delta
    /// tail instead of streaming the corpus, and commits maintain the delta
    /// transactionally — packed base + resident delta answer every query.
    /// Re-packing folds the accumulated delta back into a fresh base.
    /// Migrate a v2 paged B-tree index to v3 (intern-keyed) entries, in
    /// bounded self-checkpointing batches. Safe to interrupt and re-run; the
    /// durable format record flips only when no v2 entry remains. Returns the
    /// total entries rewritten by THIS run.
    pub fn rekey_index(&mut self, name: &str) -> Result<u64> {
        self.ensure_writable("rekey index")?;
        let definition = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read()
            .definition
            .clone();
        if definition.kind != IndexKind::BTree {
            return Err(BicDbError::Index(format!(
                "index `{name}` is not a B-tree index; rekey applies to ordered entries only"
            )));
        }
        let Some(paged) = self.paged_records.clone() else {
            return Err(BicDbError::Index(format!(
                "rekeying `{name}` requires paged storage (server_paged)"
            )));
        };
        const REKEY_BATCH_ENTRIES: usize = 8_192;
        let mut total = 0u64;
        loop {
            let rewritten = paged.rekey_ordered_index_batch(
                name,
                &definition.collection,
                REKEY_BATCH_ENTRIES,
            )?;
            if rewritten == 0 {
                break;
            }
            total += rewritten;
        }
        let (xid, _) = paged.begin();
        paged.set_index_entry_format(
            xid,
            name,
            &definition.collection,
            crate::paged_collection::IndexEntryFormat::V3,
        )?;
        paged.commit(xid)?;
        Ok(total)
    }

    pub fn pack_spatial_index_with_strategy(
        &mut self,
        name: &str,
        strategy: SpatialPackStrategy,
    ) -> Result<SpatialPackReport> {
        self.ensure_writable("pack spatial index")?;
        let definition = self
            .indexes
            .get(name)
            .map(|index| index.read().definition.clone())
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?;
        if definition.kind != IndexKind::Spatial {
            return Err(BicDbError::Index(format!(
                "index `{name}` is not a spatial index"
            )));
        }
        if definition.predicate.is_some()
            || !matches!(definition.fields.first(), Some(IndexField::Geometry))
        {
            return Err(BicDbError::Index(format!(
                "packing `{name}` requires a first-class geometry field and no predicate"
            )));
        }
        let Some(paged) = self.paged_records.clone() else {
            return Err(BicDbError::Index(format!(
                "packing `{name}` requires paged storage (server_paged)"
            )));
        };
        let field = definition.fields.first().cloned().expect("checked above");
        match strategy {
            // Hilbert packs through the resumable external-sort pipeline:
            // bounded memory (one run buffer), sorted run files on disk, and
            // a checkpoint that survives a crash — the front half never holds
            // the corpus. Fixed geographic Hilbert bounds make the scan
            // single-pass.
            SpatialPackStrategy::Hilbert => {
                self.pack_spatial_index_external(&definition, &field, &paged)
            }
            // STR needs global slab partitioning, which wants the whole
            // entry set — it stays the in-memory benchmark alternative and
            // is not resumable. Corpora too large for that belong on
            // Hilbert.
            SpatialPackStrategy::Str => {
                self.pack_spatial_index_resident(&definition, &field, &paged)
            }
        }
    }

    /// STR pack: collect-all, order, pack — bounded only by the corpus.
    pub(crate) fn pack_spatial_index_resident(
        &mut self,
        definition: &IndexDefinition,
        field: &IndexField,
        paged: &Arc<crate::paged_collection::PagedRecords>,
    ) -> Result<SpatialPackReport> {
        let name = definition.name.clone();
        // A strategy switch abandons any Hilbert build in flight.
        if let Some(workspace) =
            crate::spatial_pack_build::SpatialPackWorkspace::load(&self.path, &name)?
        {
            workspace.discard()?;
        }
        let published_generation = self.published_spatial_generation(paged, &name)?;
        // Sweep crashed leftovers BEFORE choosing the generation number, so
        // a reused number can never inherit stale higher-numbered nodes.
        sweep_stale_spatial_generations(paged, &name, published_generation.as_slice())?;
        let generation = published_generation.first().copied().unwrap_or(0) + 1;

        let snapshot = paged.latest_snapshot();
        let mut entries = Vec::new();
        for record in paged.scan_identities(&snapshot, &definition.collection)? {
            let record = record?;
            if let Some(geometry) = record_spatial_geometry(&record, field)? {
                entries.push(packed_entry_from_index_entry(&spatial_index_entry(
                    record.id.clone(),
                    &geometry,
                )?));
            }
        }

        let db: &Self = &*self;
        let mut pending: Vec<(u64, Vec<u8>)> = Vec::new();
        let flush = |pending: &mut Vec<(u64, Vec<u8>)>| -> Result<()> {
            if pending.is_empty() {
                return Ok(());
            }
            db.in_paged_transaction(|paged, xid| {
                for (node, bytes) in pending.iter() {
                    paged.put_spatial_node(xid, &name, generation, *node, bytes)?;
                }
                Ok(())
            })?;
            pending.clear();
            Ok(())
        };
        let build_result = spatial_packed::build_packed_tree(
            entries,
            SpatialPackStrategy::Str,
            &mut |node, bytes| {
                pending.push((node, bytes.to_vec()));
                if pending.len() >= SPATIAL_PACK_NODES_PER_TXN {
                    flush(&mut pending)?;
                }
                Ok(())
            },
        );
        let build_result = build_result.and_then(|meta| {
            flush(&mut pending)?;
            Ok(meta)
        });
        let meta = match build_result {
            Ok(meta) => meta,
            Err(error) => {
                // Best-effort GC; anything left is swept at open, by the
                // next pack, or by DROP INDEX.
                let physical = paged.resolve_index_name(&name);
                let _ = self.remove_raw_prefix(
                    paged,
                    &crate::paged_collection::spatial_generation_prefix(&physical, generation),
                );
                return Err(error);
            }
        };

        // `&mut self` excludes writers for the whole call, so the live delta
        // list equals the delta the scan reflected.
        let delta_pks = paged
            .scan_spatial_delta(&paged.latest_snapshot(), &name)?
            .map(|entry| entry.map(|(pk, _)| pk))
            .collect::<Result<Vec<_>>>()?;
        self.finish_spatial_pack(definition, paged, meta, generation, &delta_pks, false)
    }

    /// Hilbert pack: resumable external sort. See `spatial_pack_build` for
    /// the phase machine and the xmax-chain resume-soundness argument.
    pub(crate) fn pack_spatial_index_external(
        &mut self,
        definition: &IndexDefinition,
        field: &IndexField,
        paged: &Arc<crate::paged_collection::PagedRecords>,
    ) -> Result<SpatialPackReport> {
        use crate::spatial_pack_build::{
            crash_after_runs, run_entry_budget, SpatialPackCheckpoint, SpatialPackPhase,
            SpatialPackWorkspace,
        };
        let name = definition.name.clone();
        let budget = run_entry_budget();
        let published_generation = self.published_spatial_generation(paged, &name)?;

        // Resume whenever the workspace matches (collection, strategy, run
        // budget). Soundness does not need a no-writes guarantee: from the
        // moment a workspace exists, commits write durable delta rows for
        // this index (`spatial_delta_durable`), and a delta row masks its pk
        // against the packed base at the id level — so runs and resumed
        // scans may mix snapshots freely; any row touched since the original
        // scan is served from the delta, never from a stale base entry. The
        // publish folds ONLY the delta rows captured at workspace creation.
        let mut resumed = false;
        let mut workspace = match SpatialPackWorkspace::load(&self.path, &name)? {
            Some(workspace)
                if workspace.checkpoint.collection == definition.collection
                    && workspace.checkpoint.strategy == SpatialPackStrategy::Hilbert.label()
                    && workspace.checkpoint.run_entry_budget == budget =>
            {
                resumed = true;
                Some(workspace)
            }
            Some(workspace) => {
                workspace.discard()?;
                None
            }
            None => None,
        };
        let mut workspace = match workspace.take() {
            Some(workspace) => workspace,
            None => {
                // Fresh build: clear crashed leftovers first so the chosen
                // generation cannot collide with stale nodes, then capture
                // the delta rows this pack will fold.
                sweep_stale_spatial_generations(paged, &name, published_generation.as_slice())?;
                let snapshot = paged.latest_snapshot();
                let delta_rows = paged
                    .scan_spatial_delta(&snapshot, &name)?
                    .map(|entry| entry.map(|(pk, value)| (pk, hex_bytes(&value))))
                    .collect::<Result<Vec<_>>>()?;
                SpatialPackWorkspace::create(
                    &self.path,
                    &name,
                    self.config.fsync,
                    SpatialPackCheckpoint {
                        version: 1,
                        collection: definition.collection.clone(),
                        strategy: SpatialPackStrategy::Hilbert.label().to_string(),
                        generation: published_generation.first().copied().unwrap_or(0) + 1,
                        run_entry_budget: budget,
                        phase: SpatialPackPhase::Scan,
                        last_pk: None,
                        runs: Vec::new(),
                        entries: 0,
                        delta_rows,
                        expected_xmax: snapshot.xmax,
                    },
                )?
            }
        };
        // From here on, every commit against this index writes durable delta
        // rows — the masking layer the resume argument above relies on.
        if let Some(index) = self.indexes.get(&name) {
            index.write().spatial_delta_durable = true;
        }

        // SCAN: stream identities from the checkpoint cursor, key on the
        // fixed-bounds Hilbert curve, spill sorted runs. Writes nothing to
        // the paged store, so the xmax chain stays intact run over run.
        if workspace.checkpoint.phase == SpatialPackPhase::Scan {
            let snapshot = paged.latest_snapshot();
            let crash_after = crash_after_runs();
            let mut buffer: Vec<(u64, PackedSpatialEntry)> =
                Vec::with_capacity(budget.min(1 << 20));
            let mut cursor = workspace.checkpoint.last_pk.clone();
            let scan = paged.scan_identities_after(
                &snapshot,
                &definition.collection,
                cursor.as_deref(),
            )?;
            for record in scan {
                let record = record?;
                let record_id = record.id.clone();
                if let Some(geometry) = record_spatial_geometry(&record, field)? {
                    let entry = packed_entry_from_index_entry(&spatial_index_entry(
                        record.id.clone(),
                        &geometry,
                    )?);
                    if entry.record_id.len() > usize::from(u16::MAX) {
                        return Err(BicDbError::Index(format!(
                            "packed spatial index cannot store record id of {} bytes (max {})",
                            entry.record_id.len(),
                            u16::MAX
                        )));
                    }
                    let key = spatial_packed::hilbert_key_lonlat(
                        (entry.min[0] + entry.max[0]) / 2.0,
                        (entry.min[1] + entry.max[1]) / 2.0,
                    );
                    buffer.push((key, entry));
                }
                cursor = Some(record_id);
                if buffer.len() >= budget {
                    workspace.write_run(&mut buffer, cursor.clone())?;
                    if crash_after.is_some_and(|after| workspace.checkpoint.runs.len() >= after) {
                        return Err(BicDbError::Index(
                            "spatial pack crash injection".to_string(),
                        ));
                    }
                }
            }
            workspace.write_run(&mut buffer, cursor)?;
            workspace.checkpoint.phase = SpatialPackPhase::Pack;
            workspace.save()?;
        }

        // MERGE + PACK: k-way merge the runs straight into the streaming
        // node builder. A restart here redoes only this phase — pure
        // sequential I/O over the runs, no sort, deterministic node set, so
        // rewriting from node 1 is an idempotent overwrite.
        let generation = workspace.checkpoint.generation;
        let merge = crate::spatial_pack_build::RunMerge::open(&workspace.run_paths())?;
        let db: &Self = &*self;
        let paged_for_flush = Arc::clone(paged);
        let name_for_flush = name.clone();
        let mut pending: Vec<(u64, Vec<u8>)> = Vec::new();
        let mut flush = |pending: &mut Vec<(u64, Vec<u8>)>,
                         workspace: &mut SpatialPackWorkspace|
         -> Result<()> {
            if pending.is_empty() {
                return Ok(());
            }
            db.in_paged_transaction(|paged, xid| {
                for (node, bytes) in pending.iter() {
                    paged.put_spatial_node(xid, &name_for_flush, generation, *node, bytes)?;
                }
                Ok(())
            })?;
            pending.clear();
            // Re-arm the drift detector after our own commit, so a
            // pack-phase restart can prove the interval was ours alone.
            workspace.checkpoint.expected_xmax = paged_for_flush.latest_snapshot().xmax;
            workspace.save()
        };
        let meta = {
            let workspace = &mut workspace;
            let build = spatial_packed::build_packed_tree_from_sorted(
                merge,
                SpatialPackStrategy::Hilbert,
                &mut |node, bytes| {
                    pending.push((node, bytes.to_vec()));
                    if pending.len() >= SPATIAL_PACK_NODES_PER_TXN {
                        flush(&mut pending, workspace)?;
                    }
                    Ok(())
                },
            );
            let meta = build?;
            flush(&mut pending, workspace)?;
            meta
        };

        // Fold only captured delta rows that are byte-identical NOW: a row
        // rewritten since capture may describe state the runs did not scan,
        // and its masking must survive the publish. Unfolded rows are pure
        // overhead (the next uninterrupted re-pack folds them), never wrong.
        let snapshot = paged.latest_snapshot();
        let mut fold_pks = Vec::with_capacity(workspace.checkpoint.delta_rows.len());
        for (pk, captured_hex) in &workspace.checkpoint.delta_rows {
            let current = paged.spatial_delta_value(&snapshot, &name, pk)?;
            if current.is_some_and(|value| hex_bytes(&value) == *captured_hex) {
                fold_pks.push(pk.clone());
            }
        }
        let report =
            self.finish_spatial_pack(definition, paged, meta, generation, &fold_pks, resumed)?;
        workspace.discard()?;
        Ok(report)
    }

    /// The published packed generation, as a 0-or-1-element slice-able vec.
    pub(crate) fn published_spatial_generation(
        &self,
        paged: &Arc<crate::paged_collection::PagedRecords>,
        name: &str,
    ) -> Result<Vec<u64>> {
        Ok(paged
            .spatial_meta(&paged.latest_snapshot(), name)?
            .map(|bytes| PackedSpatialMeta::decode(&bytes))
            .transpose()?
            .map(|meta| vec![meta.generation])
            .unwrap_or_default())
    }

    /// Shared pack tail: atomic publish (meta swap + delta fold in ONE
    /// transaction), resident-state swap, retired-generation GC, audit
    /// event, report.
    pub(crate) fn finish_spatial_pack(
        &mut self,
        definition: &IndexDefinition,
        paged: &Arc<crate::paged_collection::PagedRecords>,
        mut meta: PackedSpatialMeta,
        generation: u64,
        delta_pks: &[String],
        resumed: bool,
    ) -> Result<SpatialPackReport> {
        let name = definition.name.clone();
        meta.generation = generation;
        self.in_paged_transaction(|paged, xid| {
            for pk in delta_pks {
                paged.delete_spatial_delta(xid, &name, pk)?;
            }
            paged.put_spatial_meta(xid, &name, &meta.encode())
        })?;

        // Swap the resident state. The delta is NOT necessarily empty: a
        // resumed build may have accumulated post-scan writes whose delta
        // rows the publish deliberately did not fold (they are what masked
        // the mixed-snapshot runs) — reload them so queries keep serving the
        // current truth for those pks.
        {
            let snapshot = paged.latest_snapshot();
            let mut tombstones = FxHashSet::default();
            let mut delta = Vec::new();
            for entry in paged.scan_spatial_delta(&snapshot, &name)? {
                let (pk, value) = entry?;
                if let Some((min, max, point)) = spatial_packed::decode_delta(&value)? {
                    delta.push(SpatialIndexEntry {
                        record_id: pk.clone(),
                        envelope: AABB::from_corners(min, max),
                        point,
                    });
                }
                tombstones.insert(pk);
            }
            let index = self
                .indexes
                .get(&name)
                .ok_or_else(|| BicDbError::Index(format!("index `{name}` disappeared")))?;
            let mut state = index.write();
            state.packed_spatial = Some(meta.clone());
            state.spatial = Some(RTree::bulk_load(delta));
            state.spatial_tombstones = tombstones;
            state.spatial_delta_durable = true;
        }

        // Retired and crash-orphaned generations stay queryable to old MVCC
        // snapshots until this sweep; new snapshots only ever see `meta`.
        sweep_stale_spatial_generations(paged, &name, &[generation])?;

        if self.config.audit_events {
            let event =
                spatial_index_updated_event(definition, "packed", meta.entry_count as usize)?;
            self.events.lock().append(event)?;
        }

        Ok(SpatialPackReport {
            index_name: definition.name.clone(),
            collection: definition.collection.clone(),
            strategy: meta.strategy,
            generation,
            entry_count: meta.entry_count,
            node_count: meta.node_count,
            height: meta.height,
            resumed,
        })
    }

    /// Create a spatial index and go STRAIGHT to the packed build, skipping
    /// the resident tree entirely: the definition is registered with an
    /// empty state (persisted, so a crash resumes instead of restarting) and
    /// the bounded-memory external pack runs immediately. This is the only
    /// index-creation path whose peak memory does not scale with the corpus
    /// — `create_spatial_index` on 74M rows builds a ~14 GiB resident tree;
    /// this builds none.
    ///
    /// Until the pack completes, queries on the index see only the (empty)
    /// resident state — the same contract as any index mid-build. A crash
    /// leaves the definition + workspace behind; running the pack again
    /// resumes from the checkpoint.
    pub fn create_packed_spatial_index(
        &mut self,
        collection: &str,
        field: &str,
        strategy: SpatialPackStrategy,
    ) -> Result<SpatialPackReport> {
        self.ensure_writable("create packed spatial index")?;
        let index_field = spatial_field_from_name(field)?;
        if index_field != IndexField::Geometry {
            return Err(BicDbError::Index(
                "packed spatial indexes require the first-class geometry field".to_string(),
            ));
        }
        let name = spatial_index_name(collection, field);
        let definition = IndexDefinition {
            name: name.clone(),
            collection: collection.to_string(),
            fields: vec![index_field],
            unique: false,
            kind: IndexKind::Spatial,
            predicate: None,
            exclusion: None,
        };
        validate_index_definition(&definition)?;
        self.ensure_collection(collection)?;
        self.validate_secure_index_definition(&definition)?;
        let Some(paged) = self.paged_records.clone() else {
            return Err(BicDbError::Index(format!(
                "packing `{name}` requires paged storage (server_paged)"
            )));
        };
        if self.indexes.contains_key(&name) {
            // Already defined (possibly by a crashed earlier attempt):
            // packing it resumes or restarts as the workspace dictates.
            return self.pack_spatial_index_with_strategy(&name, strategy);
        }
        // Clean slate: a crashed DROP can leave [0,0,13] leftovers whose
        // meta would resurrect on reopen; a brand-new index must own its
        // namespace outright.
        let physical = paged.resolve_index_name(&name);
        self.remove_raw_prefix(
            &paged,
            &crate::paged_collection::index_spatial_prefix(&physical),
        )?;
        if let Some(workspace) =
            crate::spatial_pack_build::SpatialPackWorkspace::load(&self.path, &name)?
        {
            workspace.discard()?;
        }
        self.index_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.indexes.insert(
            name.clone(),
            RwLock::new(IndexState {
                definition,
                store: new_index_store(),
                spatial: Some(RTree::new()),
                packed_spatial: None,
                spatial_tombstones: FxHashSet::default(),
                // Durable delta from the very first commit: post-crash
                // writes must mask the interrupted build's runs.
                spatial_delta_durable: true,
                paged_read_through: false,
                full_text_build_incomplete: false,
            }),
        );
        self.persist_index_catalog()?;
        self.schema_compatibility.invalidate();
        self.pack_spatial_index_with_strategy(&name, strategy)
    }

    pub fn drop_index(&mut self, name: &str) -> Result<bool> {
        self.ensure_writable("drop index")?;
        // The packed spatial META dies FIRST, in its own committed
        // transaction, before the index leaves the durable catalog. The
        // reverse order has a resurrection window: crash after the catalog
        // persist but before the [0,0,13] purge, then a same-name re-create
        // + reopen would adopt the stale meta as authoritative — serving the
        // dropped index's corpus silently. With the meta gone the leftover
        // nodes/delta are only a space leak, reclaimed by the create-time
        // purge or the next completed drop.
        if self.indexes.contains_key(name) {
            if let Some(paged) = self.paged_records.clone() {
                if paged
                    .spatial_meta(&paged.latest_snapshot(), name)?
                    .is_some()
                {
                    self.in_paged_transaction(|paged, xid| {
                        paged.delete_spatial_meta(xid, name)?;
                        Ok(())
                    })?;
                }
            }
        }
        self.index_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let existed = self.indexes.remove(name).is_some();
        if existed {
            self.persist_index_catalog()?;
            let mut catalog = self.load_index_maintenance_catalog()?;
            if catalog.indexes.remove(name).is_some() {
                self.persist_index_maintenance_catalog(&catalog)?;
            }
            // The durable entries too, or the reserved keyspace accumulates
            // dead indexes forever.
            self.drop_paged_index_entries(name)?;
        }
        // A published index can also have an unpublished replacement
        // generation. DROP abandons that resumable build as well; otherwise
        // its completed runs and physical keyspace would become orphaned.
        let abandoned_build = self.discard_full_text_build(name)?;
        let changed = existed || abandoned_build;
        if changed {
            self.schema_compatibility.invalidate();
        }
        Ok(changed)
    }

    pub fn rename_index(&mut self, old_name: &str, new_name: &str) -> Result<bool> {
        self.ensure_writable("rename index")?;
        let Some(existing_name) = self
            .indexes
            .keys()
            .find(|name| name.eq_ignore_ascii_case(old_name))
            .cloned()
        else {
            return Ok(false);
        };
        if existing_name.eq_ignore_ascii_case(new_name) {
            return Ok(true);
        }
        if self
            .indexes
            .keys()
            .any(|name| name.eq_ignore_ascii_case(new_name))
        {
            return Err(BicDbError::Index(format!(
                "index `{new_name}` already exists"
            )));
        }

        // A packed spatial index's durable keyspace (meta/nodes/delta in
        // [0,0,13]) is keyed by the index NAME with no alias indirection.
        // Renaming would leave every durable key under the old name: all
        // queries would fail with "missing node", DROP would leak the old
        // keyspace, and reopen would silently abandon the base. Refuse until
        // an alias layer exists; the workaround is DROP + CREATE + PACK.
        if let Some(paged) = &self.paged_records {
            if paged
                .spatial_meta(&paged.latest_snapshot(), &existing_name)?
                .is_some()
            {
                return Err(BicDbError::Index(format!(
                    "index `{existing_name}` has a packed spatial base and cannot be renamed; \
                     drop and re-create it under the new name, then pack again"
                )));
            }
        }

        self.index_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let mut state = self.indexes.remove(&existing_name).ok_or_else(|| {
            BicDbError::Index(format!("index `{existing_name}` disappeared during rename"))
        })?;
        let collection = state.get_mut().definition.collection.clone();
        state.get_mut().definition.name = new_name.to_string();
        self.index_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.indexes.insert(new_name.to_string(), state);
        self.persist_index_catalog()?;
        {
            let mut generations = self.fts_generations.lock();
            if let Some(physical) = generations.indexes.remove(&existing_name) {
                generations
                    .indexes
                    .insert(new_name.to_string(), physical.clone());
                let bytes = serde_json::to_vec_pretty(&*generations)?;
                storage::write_atomic(
                    &self.path.join(DEFAULT_FTS_GENERATION_CATALOG),
                    &bytes,
                    self.config.fsync,
                )?;
                if let Some(paged) = &self.paged_records {
                    paged.remove_index_alias(&existing_name);
                    paged.set_index_alias(new_name, &physical);
                }
            }
        }

        let mut maintenance = self.load_index_maintenance_catalog()?;
        if let Some(status) = maintenance.indexes.remove(&existing_name) {
            maintenance.indexes.insert(new_name.to_string(), status);
            self.persist_index_maintenance_catalog(&maintenance)?;
        }

        if let Some(stats) = self.planner_stats.tables.get_mut(&collection) {
            if let Some(mut index_stats) = stats.indexes.remove(&existing_name) {
                index_stats.index_name = new_name.to_string();
                stats.indexes.insert(new_name.to_string(), index_stats);
                self.persist_planner_stats()?;
            }
        }

        self.schema_compatibility.invalidate();
        Ok(true)
    }

    /// A spatial definition whose backfill can stream identity rows from the
    /// page store instead of materializing the collection resident:
    /// server_paged, lazy collection, first-class geometry field, and no
    /// predicate (predicates read metadata, which identity scans skip).
    ///
    /// Existence reason: a resident spatial build materializes every row —
    /// measured at 50+ GiB for a 74M-row collection, which cannot fit. The
    /// R-tree's entries are (pk, rect); nothing about them needs the rows.
    pub(crate) fn spatial_index_streams_from_paged(&self, definition: &IndexDefinition) -> bool {
        self.paged_records.is_some()
            && definition.kind == IndexKind::Spatial
            && definition.predicate.is_none()
            && matches!(definition.fields.first(), Some(IndexField::Geometry))
            && self
                .collections
                .get(&definition.collection)
                .is_some_and(|state| state.read().paged_lazy)
    }

    /// The resident state for a build/rebuild: streamed from the page store
    /// for spatial indexes on lazy collections, recomputed from resident
    /// shards for everything else.
    pub(crate) fn index_state_for_build(&self, definition: IndexDefinition) -> Result<IndexState> {
        if self.spatial_index_streams_from_paged(&definition) {
            let paged = self.paged_records.as_ref().expect("checked by caller");
            return Ok(paged_spatial_index_state(&definition, paged)?.0);
        }
        build_index_state(definition, &self.collections)
    }

    pub fn rebuild_index(&mut self, name: &str) -> Result<IndexVerifyReport> {
        self.ensure_writable("rebuild index")?;
        let definition = self
            .indexes
            .get(name)
            .map(|index| index.read().definition.clone())
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?;
        // A spatial index with a published packed base rebuilds by
        // RE-PACKING: the durable node tree is the index, so rebuilding only
        // the resident side would leave stale packed entries authoritative.
        if definition.kind == IndexKind::Spatial {
            if let Some(paged) = &self.paged_records {
                if let Some(bytes) = paged.spatial_meta(&paged.latest_snapshot(), name)? {
                    let strategy = PackedSpatialMeta::decode(&bytes)?.strategy;
                    self.pack_spatial_index_with_strategy(name, strategy)?;
                    return self.verify_index(name);
                }
            }
        }
        let state = self.index_state_for_build(definition)?;
        let report = verify_index_state(&state, &state, &self.collections)?;
        let event = if self.config.audit_events && state.definition.kind == IndexKind::Spatial {
            Some(spatial_index_updated_event(
                &state.definition,
                "rebuilt",
                index_state_record_count(&state),
            )?)
        } else {
            None
        };
        self.index_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.indexes.insert(name.to_string(), RwLock::new(state));
        if let Some(event) = event {
            self.events.lock().append(event)?;
        }
        Ok(report)
    }

    pub fn rebuild_index_online(&mut self, name: &str) -> Result<IndexMaintenanceReport> {
        self.ensure_writable("rebuild index online")?;
        let interrupted_previous = self.index_maintenance_interrupted(name)?;
        let definition = self
            .indexes
            .get(name)
            .map(|index| index.read().definition.clone())
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?;
        self.rebuild_index_online_from_definition(definition, "rebuild", interrupted_previous)
    }

    pub fn rebuild_all_indexes_online(&mut self) -> Result<IndexMaintenanceAllReport> {
        self.ensure_writable("rebuild all indexes online")?;
        let names = self
            .index_definitions()
            .into_iter()
            .map(|definition| definition.name)
            .collect::<Vec<_>>();
        let mut reports = Vec::new();
        for name in names {
            reports.push(self.rebuild_index_online(&name)?);
        }
        let collections = self.hnsw_indexes.read().keys().cloned().collect::<Vec<_>>();
        let mut vector_reports = Vec::new();
        for collection in collections {
            vector_reports.push(self.rebuild_vector_index(&collection)?);
        }
        let valid = reports.iter().all(|report| report.verification.valid)
            && vector_reports.iter().all(|report| report.valid);
        Ok(IndexMaintenanceAllReport {
            operation: "rebuild_all".to_string(),
            valid,
            reports,
            vector_reports,
        })
    }

    pub(crate) fn rebuild_index_online_from_definition(
        &mut self,
        definition: IndexDefinition,
        operation: &str,
        interrupted_previous: bool,
    ) -> Result<IndexMaintenanceReport> {
        let start = Instant::now();
        let scan_start = Instant::now();
        let expected_records = self
            .collections
            .get(&definition.collection)
            .map(|state| state.read().record_count())
            .unwrap_or_default();
        self.persist_index_maintenance_status(
            &definition,
            IndexMaintenanceStatus {
                collection: definition.collection.clone(),
                kind: definition.kind.clone(),
                status: "building".to_string(),
                progress_percent: 1,
                stale: true,
                updated_unix_ms: unix_timestamp_millis(),
                ..IndexMaintenanceStatus::default()
            },
        )?;
        // A spatial index with a published packed base rebuilds by
        // RE-PACKING, exactly like the synchronous rebuild path: swapping in
        // a resident-only state would silently stop durable delta
        // maintenance while the stale packed meta stays published — a later
        // reopen would then serve pre-rebuild data and lose every spatial
        // mutation made since this rebuild.
        let packed_strategy = if definition.kind == IndexKind::Spatial {
            match &self.paged_records {
                Some(paged) => paged
                    .spatial_meta(&paged.latest_snapshot(), &definition.name)?
                    .map(|bytes| PackedSpatialMeta::decode(&bytes))
                    .transpose()?
                    .map(|meta| meta.strategy),
                None => None,
            }
        } else {
            None
        };
        let scan_ms;
        let swap_ms;
        let indexed_records;
        if let Some(strategy) = packed_strategy {
            let pack = self.pack_spatial_index_with_strategy(&definition.name, strategy)?;
            scan_ms = scan_start.elapsed().as_millis();
            swap_ms = 0;
            indexed_records = pack.entry_count as usize;
        } else {
            let state = self.index_state_for_build(definition.clone())?;
            scan_ms = scan_start.elapsed().as_millis();
            let swap_start = Instant::now();
            indexed_records = index_state_record_count(&state);
            let event = if self.config.audit_events && state.definition.kind == IndexKind::Spatial {
                Some(spatial_index_updated_event(
                    &state.definition,
                    operation,
                    indexed_records,
                )?)
            } else {
                None
            };
            // Every mutation of `indexes` bumps the generation: it is the
            // validation key for caches derived from index definitions.
            self.index_generation
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.indexes
                .insert(definition.name.clone(), RwLock::new(state));
            self.persist_index_catalog()?;
            if let Some(event) = event {
                self.events.lock().append(event)?;
            }
            swap_ms = swap_start.elapsed().as_millis();
        }
        let mut verification = self.verify_index(&definition.name)?;
        verification.last_verified_unix_ms = Some(unix_timestamp_millis());
        let build_time_ms = start.elapsed().as_millis();
        let report = IndexMaintenanceReport {
            index_name: definition.name.clone(),
            collection: definition.collection.clone(),
            kind: definition.kind.clone(),
            operation: operation.to_string(),
            status: if verification.valid {
                "complete"
            } else {
                "corrupt"
            }
            .to_string(),
            online: true,
            restartable: true,
            interrupted_previous,
            progress_percent: 100,
            records_scanned: expected_records,
            records_indexed: indexed_records,
            size_bytes: verification.size_bytes,
            build_time_ms,
            lock_phases: vec![
                IndexLockPhaseReport {
                    phase: "catalog-checkpoint".to_string(),
                    bounded: true,
                    duration_ms: 0,
                },
                IndexLockPhaseReport {
                    phase: "snapshot-scan".to_string(),
                    bounded: true,
                    duration_ms: scan_ms,
                },
                IndexLockPhaseReport {
                    phase: "catalog-swap".to_string(),
                    bounded: true,
                    duration_ms: swap_ms,
                },
            ],
            stale: !verification.valid,
            corrupt: !verification.valid,
            last_verified_unix_ms: verification.last_verified_unix_ms,
            verification,
        };
        self.persist_index_maintenance_status(
            &definition,
            IndexMaintenanceStatus {
                collection: definition.collection.clone(),
                kind: definition.kind.clone(),
                status: report.status.clone(),
                progress_percent: report.progress_percent,
                size_bytes: report.size_bytes,
                build_time_ms: report.build_time_ms,
                stale: report.stale,
                corrupt: report.corrupt,
                last_verified_unix_ms: report.last_verified_unix_ms,
                updated_unix_ms: unix_timestamp_millis(),
            },
        )?;
        Ok(report)
    }

    pub fn verify_index(&self, name: &str) -> Result<IndexVerifyReport> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        // Registry-mode collections keep no resident rows, so "recompute from
        // the collection" means recompute from the page store. A full-decode
        // scan per verify is the honest cost of an integrity check.
        let registry_mode = self
            .collections
            .get(&state.definition.collection)
            .map(|collection| collection.read().paged_lazy)
            .unwrap_or(false);
        // A streamed spatial index on a lazy collection: verify by streaming
        // identity rows and probing the LIVE tree for each expected entry —
        // an envelope-guided descent per row, O(1) extra memory. Building a
        // second expected tree would double the index's footprint exactly
        // when it is largest (measured: the create-path verify pushed a 74M-
        // entry build from ~14 GiB to 44+ GiB).
        if self.spatial_index_streams_from_paged(&state.definition)
            || state.packed_spatial.is_some()
        {
            if let Some(paged) = &self.paged_records {
                let field = state.definition.fields.first().ok_or_else(|| {
                    BicDbError::Index("spatial index requires a field".to_string())
                })?;
                let tree = state.spatial.as_ref();
                let snapshot = paged.latest_snapshot();
                let mut streamed_entries = 0usize;
                let mut missing_entries = 0usize;
                for record in paged.scan_identities(&snapshot, &state.definition.collection)? {
                    let record = record?;
                    let Some(geometry) = record_spatial_geometry(&record, field)? else {
                        continue;
                    };
                    streamed_entries += 1;
                    let entry = spatial_index_entry(record.id.clone(), &geometry)?;
                    // Resident tree first (full index, or the delta in packed
                    // mode), then the packed base behind its tombstone mask.
                    let present = tree.is_some_and(|tree| {
                        tree.locate_in_envelope_intersecting(&entry.envelope)
                            .any(|existing| *existing == entry)
                    }) || self.packed_spatial_contains(&state, &entry)?;
                    if !present {
                        missing_entries += 1;
                    }
                }
                let indexed_records = tree.map(RTree::size).unwrap_or_default()
                    + self.packed_spatial_live_count(&state)?;
                let matched = streamed_entries - missing_entries;
                let stale_entries = indexed_records.saturating_sub(matched);
                let wrong_entries = if missing_entries == 0 && stale_entries == 0 {
                    0
                } else {
                    missing_entries.max(stale_entries)
                };
                let mut report = IndexVerifyReport {
                    index_name: state.definition.name.clone(),
                    collection: state.definition.collection.clone(),
                    kind: state.definition.kind.clone(),
                    indexed_records,
                    expected_records: streamed_entries,
                    missing_entries,
                    stale_entries,
                    duplicate_entries: 0,
                    wrong_entries,
                    size_bytes: index_state_size_bytes(&state),
                    last_verified_unix_ms: None,
                    valid: missing_entries == 0
                        && stale_entries == 0
                        && indexed_records == streamed_entries,
                };
                if let Some(status) = self.index_maintenance_status(name)? {
                    report.last_verified_unix_ms = status.last_verified_unix_ms;
                }
                return Ok(report);
            }
        }
        if (registry_mode
            && matches!(
                state.definition.kind,
                IndexKind::BTree | IndexKind::FullText
            ))
            || state.paged_read_through
        {
            if let Some(paged) = &self.paged_records {
                let (expected, live_rows) =
                    build_index_state_for_verify_from_paged(&state.definition, self, paged)?;
                // A read-through index keeps no resident postings; the ACTUAL
                // side of the comparison is the durable entries themselves,
                // materialized just for this verification.
                let actual_state;
                let actual: &IndexState = if state.paged_read_through {
                    let snapshot = paged.latest_snapshot();
                    if state.definition.kind == IndexKind::FullText
                        && paged.index_has_any_posting_blocks(&snapshot, &state.definition.name)?
                    {
                        // Folded index: the ACTUAL entries are blocks + tail
                        // minus tombstones, assembled term by term.
                        let collection_state =
                            self.collection_state(&state.definition.collection)?;
                        let index = IndexState {
                            definition: state.definition.clone(),
                            store: new_index_store(),
                            spatial: None,
                            packed_spatial: None,
                            spatial_tombstones: FxHashSet::default(),
                            spatial_delta_durable: false,
                            paged_read_through: true,
                            full_text_build_incomplete: false,
                        };
                        let mut entries: Vec<(Vec<u8>, RowId)> = Vec::new();
                        let mut resolve = |pk: &str| -> Option<RowId> {
                            collection_state.shard(pk).read().rowid_of(pk)
                        };
                        let mut tail_terms: FxHashSet<Vec<u8>> = FxHashSet::default();
                        for entry in paged.scan_index(&snapshot, &state.definition.name)? {
                            let (encoded_key, pk) = entry?;
                            tail_terms.insert(encoded_key.clone());
                            if let Some(rowid) = resolve(&pk) {
                                entries.push((encoded_key, rowid));
                            }
                        }
                        for block in
                            paged.scan_all_posting_blocks(&snapshot, &state.definition.name)?
                        {
                            let (encoded_key, bytes) = block?;
                            let (postings, _) =
                                crate::paged_collection::decode_posting_block(&bytes)?;
                            let tombstones: FxHashSet<String> = paged
                                .scan_posting_tombstones(
                                    &snapshot,
                                    &state.definition.name,
                                    &encoded_key,
                                )?
                                .collect::<Result<_>>()?;
                            let tail_pks: FxHashSet<String> = if tail_terms.contains(&encoded_key) {
                                paged
                                    .scan_index_exact(
                                        &snapshot,
                                        &state.definition.name,
                                        &encoded_key,
                                    )?
                                    .map(|entry| entry.map(|(pk, _)| pk))
                                    .collect::<Result<_>>()?
                            } else {
                                FxHashSet::default()
                            };
                            for posting in postings {
                                if tombstones.contains(&posting.pk)
                                    || tail_pks.contains(&posting.pk)
                                {
                                    continue;
                                }
                                if let Some(rowid) = resolve(&posting.pk) {
                                    entries.push((encoded_key.clone(), rowid));
                                }
                            }
                        }
                        for block in paged
                            .scan_all_numeric_posting_blocks(&snapshot, &state.definition.name)?
                        {
                            let (encoded_key, _, bytes) = block?;
                            let postings =
                                crate::paged_collection::decode_numeric_posting_block(&bytes)?;
                            let tombstones: FxHashSet<String> = paged
                                .scan_posting_tombstones(
                                    &snapshot,
                                    &state.definition.name,
                                    &encoded_key,
                                )?
                                .collect::<Result<_>>()?;
                            let tail_pks: FxHashSet<String> = if tail_terms.contains(&encoded_key) {
                                paged
                                    .scan_index_exact(
                                        &snapshot,
                                        &state.definition.name,
                                        &encoded_key,
                                    )?
                                    .map(|entry| entry.map(|(pk, _)| pk))
                                    .collect::<Result<_>>()?
                            } else {
                                FxHashSet::default()
                            };
                            for posting in postings {
                                let pk = paged
                                    .full_text_pk_for_document_id(
                                        &snapshot,
                                        &state.definition.name,
                                        posting.document_id,
                                    )?
                                    .ok_or_else(|| BicDbError::Corruption {
                                        path: PathBuf::from(DEFAULT_PAGED_DIR),
                                        message: format!(
                                            "full-text document id {} has no primary-key mapping",
                                            posting.document_id
                                        ),
                                    })?;
                                if tombstones.contains(&pk) || tail_pks.contains(&pk) {
                                    continue;
                                }
                                if let Some(rowid) = resolve(&pk) {
                                    entries.push((encoded_key.clone(), rowid));
                                }
                            }
                        }
                        index.store.bulk_load(entries);
                        actual_state = index;
                        &actual_state
                    } else {
                        actual_state = match load_paged_btree_index(
                            state.definition.clone(),
                            &self.collections,
                            paged,
                        )? {
                            LoadedPagedIndex::Loaded(actual) => actual,
                            LoadedPagedIndex::NoDurableEntries(definition) => IndexState {
                                definition,
                                store: new_index_store(),
                                spatial: None,
                                packed_spatial: None,
                                spatial_tombstones: FxHashSet::default(),
                                spatial_delta_durable: false,
                                paged_read_through: true,
                                full_text_build_incomplete: false,
                            },
                        };
                        &actual_state
                    }
                } else {
                    &state
                };
                let mut report = verify_index_state(actual, &expected, &self.collections)?;
                // The resident record count is meaningless for a registry-mode
                // collection (no rows are resident); the page-store scan that
                // built `expected` is the authoritative live count.
                report.expected_records = live_rows;
                report.valid = report.indexed_records == live_rows
                    && report.missing_entries == 0
                    && report.stale_entries == 0
                    && report.duplicate_entries == 0
                    && report.wrong_entries == 0;
                if let Some(status) = self.index_maintenance_status(name)? {
                    report.last_verified_unix_ms = status.last_verified_unix_ms;
                }
                return Ok(report);
            }
        }
        let expected = build_index_state_for_verify(state.definition.clone(), &self.collections)?;
        let mut report = verify_index_state(&state, &expected, &self.collections)?;
        if let Some(status) = self.index_maintenance_status(name)? {
            report.last_verified_unix_ms = status.last_verified_unix_ms;
        }
        Ok(report)
    }

    pub fn verify_all_indexes(&mut self) -> Result<IndexMaintenanceAllReport> {
        let definitions = self.index_definitions();
        let mut reports = Vec::new();
        for definition in definitions {
            let start = Instant::now();
            let mut verification = self.verify_index(&definition.name)?;
            verification.last_verified_unix_ms = Some(unix_timestamp_millis());
            let status = if verification.valid {
                "valid"
            } else {
                "corrupt"
            };
            self.persist_index_maintenance_status(
                &definition,
                IndexMaintenanceStatus {
                    collection: definition.collection.clone(),
                    kind: definition.kind.clone(),
                    status: status.to_string(),
                    progress_percent: 100,
                    size_bytes: verification.size_bytes,
                    build_time_ms: 0,
                    stale: !verification.valid,
                    corrupt: !verification.valid,
                    last_verified_unix_ms: verification.last_verified_unix_ms,
                    updated_unix_ms: unix_timestamp_millis(),
                },
            )?;
            reports.push(IndexMaintenanceReport {
                index_name: definition.name.clone(),
                collection: definition.collection.clone(),
                kind: definition.kind.clone(),
                operation: "verify".to_string(),
                status: status.to_string(),
                online: true,
                restartable: true,
                interrupted_previous: false,
                progress_percent: 100,
                records_scanned: verification.expected_records,
                records_indexed: verification.indexed_records,
                size_bytes: verification.size_bytes,
                build_time_ms: start.elapsed().as_millis(),
                lock_phases: vec![IndexLockPhaseReport {
                    phase: "snapshot-verify".to_string(),
                    bounded: true,
                    duration_ms: start.elapsed().as_millis(),
                }],
                stale: !verification.valid,
                corrupt: !verification.valid,
                last_verified_unix_ms: verification.last_verified_unix_ms,
                verification,
            });
        }
        let mut vector_reports = Vec::new();
        let collections = self.hnsw_indexes.read().keys().cloned().collect::<Vec<_>>();
        for collection in collections {
            vector_reports.push(self.verify_vector_index(&collection)?);
        }
        let valid = reports.iter().all(|report| report.verification.valid)
            && vector_reports.iter().all(|report| report.valid);
        Ok(IndexMaintenanceAllReport {
            operation: "verify_all".to_string(),
            valid,
            reports,
            vector_reports,
        })
    }

    pub fn index_definitions(&self) -> Vec<IndexDefinition> {
        let mut indexes = self
            .indexes
            .values()
            .map(|index| index.read().definition.clone())
            .collect::<Vec<_>>();
        indexes.sort_by(|left, right| left.name.cmp(&right.name));
        indexes
    }

    /// Per-index live entry diagnostics: `(name, key_count, total_entries)`.
    /// `total_entries` is the number of (key, record-id) pairs the in-memory store
    /// holds; `key_count` the number of distinct keys. A `total_entries` that keeps
    /// climbing for an index whose underlying live row set is bounded (e.g. a
    /// new-order index that is inserted-then-deleted) signals the store is retaining
    /// superseded entries — the in-run index-bloat decay signal. Sorted by name.
    pub fn index_entry_diagnostics(&self) -> Vec<(String, usize, usize)> {
        let mut out = self
            .indexes
            .iter()
            .map(|(name, index)| {
                let guard = index.read();
                (
                    name.clone(),
                    guard.store.key_count(),
                    guard.store.total_entries(),
                )
            })
            .collect::<Vec<_>>();
        out.sort_by(|left, right| left.0.cmp(&right.0));
        out
    }

    pub fn index_stats(&self) -> Result<IndexCatalogStats> {
        Ok(IndexCatalogStats {
            index_count: self.indexes.len(),
            size_bytes: match fs::metadata(self.path.join(DEFAULT_INDEX_CATALOG)) {
                Ok(metadata) => metadata.len(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                Err(error) => return Err(error.into()),
            },
        })
    }

    pub fn analyze(&mut self) -> Result<PlannerStatsCatalog> {
        self.ensure_writable("analyze")?;
        let collections = self.collections.keys().cloned().collect::<Vec<_>>();
        self.planner_stats.tables.clear();
        for collection in collections {
            let stats = self.collect_table_statistics(&collection)?;
            self.planner_stats.tables.insert(collection, stats);
        }
        self.persist_planner_stats()?;
        Ok(self.planner_stats.clone())
    }

    pub fn analyze_collection(&mut self, collection: &str) -> Result<TableStatistics> {
        self.ensure_writable("analyze")?;
        self.ensure_collection(collection)?;
        let stats = self.collect_table_statistics(collection)?;
        self.planner_stats
            .tables
            .insert(collection.to_string(), stats.clone());
        self.persist_planner_stats()?;
        Ok(stats)
    }

    pub fn planner_stats(&self) -> &PlannerStatsCatalog {
        &self.planner_stats
    }

    pub fn table_statistics(&self, collection: &str) -> Option<&TableStatistics> {
        self.planner_stats.tables.get(collection)
    }

    pub fn replace_table_statistics(&mut self, stats: TableStatistics) -> Result<()> {
        self.ensure_writable("replace table statistics")?;
        self.ensure_collection(&stats.collection)?;
        self.planner_stats
            .tables
            .insert(stats.collection.clone(), stats);
        self.persist_planner_stats()
    }

    pub(crate) fn rebuild_vector_index_with_config(
        &self,
        collection: &str,
        config: HnswIndexConfig,
    ) -> Result<HnswIndexVerifyReport> {
        self.ensure_collection(collection)?;
        let vectors = self.hnsw_vectors_for_collection(collection)?;
        let index = HnswIndex::build(collection, config, vectors.clone())?;
        self.persist_hnsw_index(collection, &index)?;
        self.hnsw_indexes
            .write()
            .insert(collection.to_string(), index);
        self.verify_vector_index(collection)
    }

    pub(crate) fn hnsw_vectors_for_collection(&self, collection: &str) -> Result<Vec<HnswVector>> {
        let state = self.collection_state(collection)?;
        // Lazy paged: the identity projection on pages is the vector source
        // (resident shards hold nothing to enumerate).
        if state.paged_lazy {
            if let Some(paged) = &self.paged_records {
                let snapshot = paged.latest_snapshot();
                let mut vectors = Vec::new();
                for record in paged.scan_identities(&snapshot, collection)? {
                    let record = record?;
                    if let Some(vector) = record.vector {
                        vectors.push(HnswVector {
                            record_id: record.id,
                            vector,
                        });
                    }
                }
                vectors.sort_by(|left, right| left.record_id.cmp(&right.record_id));
                return Ok(vectors);
            }
        }
        let mut vectors = state
            .read_all()
            .iter()
            .flat_map(|shard| shard.records.values())
            .filter_map(|entry| {
                entry.record.vector.as_ref().map(|vector| HnswVector {
                    record_id: entry.record.id.clone(),
                    vector: vector.clone(),
                })
            })
            .collect::<Vec<_>>();
        vectors.sort_by(|left, right| left.record_id.cmp(&right.record_id));
        Ok(vectors)
    }

    pub(crate) fn vector_hits_to_results(
        &self,
        collection: &str,
        hits: Vec<HnswSearchHit>,
    ) -> Result<Vec<VectorSearchResult>> {
        let state = self.collection_state(collection)?;
        let lazy = state.paged_lazy;
        hits.into_iter()
            .filter_map(|hit| {
                let resident = {
                    let shard = state.shard(&hit.record_id).read();
                    shard
                        .get_record(&hit.record_id)
                        .map(|entry| entry.record.to_record())
                };
                match resident {
                    Some(record) => Some(record.map(|record| VectorSearchResult {
                        record,
                        score: hit.score,
                    })),
                    // Lazy paged: rows live on pages, not in shards — resolve
                    // through the ordinary lazy read. A miss there means the
                    // graph references a row deleted since the index was
                    // refreshed; dropping the hit mirrors resident behavior.
                    None if lazy => match self.get(collection, &hit.record_id) {
                        Ok(Some(record)) => Some(Ok(VectorSearchResult {
                            record: record.as_ref().clone(),
                            score: hit.score,
                        })),
                        Ok(None) => None,
                        Err(error) => Some(Err(error)),
                    },
                    None => None,
                }
            })
            .collect()
    }

    pub(crate) fn hnsw_index_path(&self, collection: &str) -> PathBuf {
        hnsw_index_path_for(&self.path, collection)
    }

    pub(crate) fn hnsw_index_size_bytes(&self, collection: &str) -> Result<u64> {
        match fs::metadata(self.hnsw_index_path(collection)) {
            Ok(metadata) => Ok(metadata.len()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn persist_hnsw_index(&self, collection: &str, index: &HnswIndex) -> Result<()> {
        let path = self.hnsw_index_path(collection);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(&index.to_persisted())?;
        if self.encryption.bound_object_cipher().is_some() {
            storage::write_frame_atomic(
                &path,
                FrameKind::Search,
                &bytes,
                self.config.fsync,
                &self.encryption,
            )?;
        } else {
            storage::write_atomic(&path, &bytes, self.config.fsync)?;
        }
        Ok(())
    }

    pub(crate) fn persist_existing_hnsw_index(&self, collection: &str) -> Result<()> {
        let hnsw_indexes = self.hnsw_indexes.read();
        if let Some(index) = hnsw_indexes.get(collection) {
            self.persist_hnsw_index(collection, index)?;
        }
        Ok(())
    }

    pub(crate) fn refresh_hnsw_after_upsert(&self, collection: &str, rebuild: bool) -> Result<()> {
        if !rebuild || !self.hnsw_indexes.read().contains_key(collection) {
            return Ok(());
        }
        let _ = self.rebuild_vector_index(collection)?;
        Ok(())
    }

    pub(crate) fn tombstone_hnsw_record(&self, collection: &str, record_id: &str) -> Result<()> {
        self.tombstone_hnsw_records(collection, std::iter::once(record_id))
    }

    pub(crate) fn tombstone_hnsw_records<'a, I>(
        &self,
        collection: &str,
        record_ids: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut changed = false;
        if let Some(index) = self.hnsw_indexes.write().get_mut(collection) {
            for record_id in record_ids {
                index.tombstone(record_id);
                changed = true;
            }
        }
        if changed {
            self.persist_existing_hnsw_index(collection)?;
        }
        Ok(())
    }

    pub(crate) fn materialize_graph_projection(
        &self,
        projection: GraphProjection,
    ) -> Result<GraphProjectionData> {
        let mut records_by_collection = HashMap::new();
        for collection in projection.collection_names() {
            let records = match self.collections.get(&collection) {
                Some(state) => {
                    let state = state.read();
                    let mut records = state
                        .read_all()
                        .iter()
                        .flat_map(|shard| shard.records.values())
                        .map(|entry| entry.record.to_record())
                        .collect::<Result<Vec<_>>>()?;
                    records.sort_by(|left, right| left.id.cmp(&right.id));
                    records
                }
                None => Vec::new(),
            };
            records_by_collection.insert(collection, records);
        }

        let mut events_by_stream = HashMap::new();
        for stream in projection.stream_names() {
            events_by_stream.insert(stream.clone(), self.events.lock().read(&stream));
        }

        GraphProjectionData::build(projection, &records_by_collection, &events_by_stream)
    }

    pub(crate) fn graph_projection_path(&self, projection: &str) -> PathBuf {
        graph_projection_path_for(&self.path, projection)
    }

    pub(crate) fn persist_graph_projection(&self, graph: &GraphProjectionData) -> Result<()> {
        let path = self.graph_projection_path(&graph.name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(graph)?;
        if self.encryption.bound_object_cipher().is_some() {
            storage::write_frame_atomic(
                &path,
                FrameKind::Index,
                &bytes,
                self.config.fsync,
                &self.encryption,
            )?;
        } else {
            storage::write_atomic(&path, &bytes, self.config.fsync)?;
        }
        Ok(())
    }

    pub(crate) fn refresh_graph_projections(&self) -> Result<()> {
        if self.graphs.read().is_empty() {
            return Ok(());
        }
        let definitions = self
            .graphs
            .read()
            .values()
            .map(|graph| graph.definition.clone())
            .collect::<Vec<_>>();
        for definition in definitions {
            let graph = self.materialize_graph_projection(definition)?;
            self.persist_graph_projection(&graph)?;
            self.graphs.write().insert(graph.name.clone(), graph);
        }
        Ok(())
    }

    /// Like [`Self::lookup_index`] but yields the raw physical [`RowId`] locators
    /// (the index payload) — no rowid->pk conversion, no String allocation. The
    /// PG-TID read-path primitive; pair with [`Self::get_by_rowid`].
    pub fn lookup_index_rowids(&self, name: &str, prefix: &[IndexValue]) -> Result<Vec<RowId>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if prefix.is_empty() || prefix.len() > state.definition.fields.len() {
            return Err(BicDbError::Index(format!(
                "invalid lookup prefix for index `{name}`"
            )));
        }
        let encoded_prefix = encode_index_key(prefix);
        let mut rowids: Vec<RowId> = Vec::new();
        if state.paged_read_through && state.definition.kind == IndexKind::BTree {
            let paged = self
                .paged_records
                .as_ref()
                .ok_or_else(|| BicDbError::Index(format!("index `{name}` lost its page store")))?;
            let snapshot = paged.latest_snapshot();
            let hints_on = paged_tid_hints_enabled();
            let target = &state.definition.collection;
            let pks = if prefix.len() == state.definition.fields.len() {
                paged
                    .scan_index_exact_refs(&snapshot, name, &encoded_prefix)?
                    .map(|entry| {
                        let (entry_ref, value) = entry?;
                        resolve_paged_entry_pk(
                            paged, &snapshot, target, name, entry_ref, &value, hints_on,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                paged
                    .scan_index_encoded_prefix_refs(&snapshot, name, &encoded_prefix)?
                    .map(|entry| {
                        let (_, entry_ref, value) = entry?;
                        resolve_paged_entry_pk(
                            paged, &snapshot, target, name, entry_ref, &value, hints_on,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?
            };
            let collection = self.collection_state(&state.definition.collection)?;
            for pk in pks {
                let rowid = collection.shard(&pk).read().rowid_of(&pk).ok_or_else(|| {
                    BicDbError::Index(format!(
                        "durable index `{name}` references missing row `{pk}`"
                    ))
                })?;
                rowids.push(rowid);
            }
            return Ok(rowids);
        }
        // Full-key lookup (the common `WHERE pk = x` case): a single point lookup.
        if prefix.len() == state.definition.fields.len() {
            state.store.collect_exact(&encoded_prefix, &mut rowids);
            index_trace::record(&index_trace::POINT);
            return Ok(rowids);
        }
        // Prefix scan: the encoding is prefix-preserving, so a byte-prefix match
        // selects exactly the keys whose leading fields equal `prefix`.
        // scan_prefix stops at the prefix boundary itself and, for a sharded
        // store, routes to the single shard holding this leading field.
        state.store.collect_prefix(&encoded_prefix, &mut rowids);
        index_trace::record_scan(&index_trace::PREFIX, rowids.len());
        Ok(rowids)
    }

    pub fn lookup_index(&self, name: &str, prefix: &[IndexValue]) -> Result<Vec<String>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.paged_read_through && state.definition.kind == IndexKind::BTree {
            if prefix.is_empty() || prefix.len() > state.definition.fields.len() {
                return Err(BicDbError::Index(format!(
                    "invalid lookup prefix for index `{name}`"
                )));
            }
            let paged = self
                .paged_records
                .as_ref()
                .ok_or_else(|| BicDbError::Index(format!("index `{name}` lost its page store")))?;
            let snapshot = paged.latest_snapshot();
            let encoded = encode_index_key(prefix);
            let hints_on = paged_tid_hints_enabled();
            let target = &state.definition.collection;
            let mut pks = if prefix.len() == state.definition.fields.len() {
                paged
                    .scan_index_exact_refs(&snapshot, name, &encoded)?
                    .map(|entry| {
                        let (entry_ref, value) = entry?;
                        resolve_paged_entry_pk(
                            paged, &snapshot, target, name, entry_ref, &value, hints_on,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                paged
                    .scan_index_encoded_prefix_refs(&snapshot, name, &encoded)?
                    .map(|entry| {
                        let (_, entry_ref, value) = entry?;
                        resolve_paged_entry_pk(
                            paged, &snapshot, target, name, entry_ref, &value, hints_on,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?
            };
            pks.sort();
            pks.dedup();
            return Ok(pks);
        }
        let collection = state.definition.collection.clone();
        drop(state);
        let rowids = self.lookup_index_rowids(name, prefix)?;
        Ok(self.rowids_to_pks_sorted(&collection, &rowids))
    }

    /// Refuses a query against a full-text index whose first build never
    /// completed.
    ///
    /// Such an index has an empty resident store and nothing durable to read
    /// through, so every lookup returned zero rows -- a result a caller cannot
    /// distinguish from "no documents matched". Rebuild it instead.
    fn ensure_full_text_build_complete(name: &str, state: &IndexState) -> Result<()> {
        if state.full_text_build_incomplete {
            return Err(BicDbError::Index(format!(
                "full-text index `{name}` has no completed build (an earlier build \
                 was interrupted); rebuild it before querying"
            )));
        }
        Ok(())
    }

    pub fn lookup_full_text_term(
        &self,
        name: &str,
        term: &str,
        prefix: bool,
    ) -> Result<Vec<String>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText {
            return Err(BicDbError::Index(format!(
                "index `{name}` is not a full-text index"
            )));
        }
        Self::ensure_full_text_build_complete(name, &state)?;
        if !full_text_term_is_indexable(term) {
            return Ok(Vec::new());
        }
        let collection = state.definition.collection.clone();
        // Read-through: bounded range scans over the durable keyspace, pks
        // come straight from the entry keys — no resident postings, no
        // registry round-trip. Term-prefix lookups map to an encoded-key
        // prefix because the string encoding (tag, 0x00-escaped bytes) is
        // byte-prefix-preserving up to its terminator.
        if state.paged_read_through {
            let paged = self.paged_records.as_ref().ok_or_else(|| {
                BicDbError::Index(format!(
                    "index `{name}` is read-through but the page store is gone"
                ))
            })?;
            let snapshot = paged.latest_snapshot();
            let mut pks: Vec<String> = if prefix {
                let mut encoded_prefix = vec![4u8];
                for &byte in term.as_bytes() {
                    encoded_prefix.push(byte);
                    if byte == 0 {
                        encoded_prefix.push(0xFF);
                    }
                }
                let mut pks = Vec::new();
                let mut tail: FxHashSet<(Vec<u8>, String)> = FxHashSet::default();
                for entry in paged.scan_index_encoded_prefix(&snapshot, name, &encoded_prefix)? {
                    let (encoded_key, pk, _) = entry?;
                    if matches!(
                        decode_index_key(&encoded_key).as_slice(),
                        [IndexValue::String(value)] if value.starts_with(term)
                    ) {
                        tail.insert((encoded_key, pk.clone()));
                        pks.push(pk);
                    }
                }
                // Folded terms have no v1 entries: their postings live in
                // blocks, minus per-term tombstones, tail overriding per pk.
                if paged.index_has_posting_blocks(&snapshot, name)? {
                    let mut current_term: Option<Vec<u8>> = None;
                    let mut term_matches = false;
                    let mut tombstones: FxHashSet<String> = FxHashSet::default();
                    for entry in paged.scan_posting_blocks_encoded_prefix(
                        &snapshot,
                        name,
                        &encoded_prefix,
                    )? {
                        let (encoded_key, _, bytes) = entry?;
                        if current_term.as_deref() != Some(&encoded_key[..]) {
                            term_matches = matches!(
                                decode_index_key(&encoded_key).as_slice(),
                                [IndexValue::String(value)] if value.starts_with(term)
                            );
                            tombstones = if term_matches {
                                paged
                                    .scan_posting_tombstones(&snapshot, name, &encoded_key)?
                                    .collect::<Result<_>>()?
                            } else {
                                FxHashSet::default()
                            };
                            current_term = Some(encoded_key.clone());
                        }
                        if !term_matches {
                            continue;
                        }
                        let (block_postings, _) =
                            crate::paged_collection::decode_posting_block(&bytes)?;
                        for posting in block_postings {
                            if !tombstones.contains(&posting.pk)
                                && !tail.contains(&(encoded_key.clone(), posting.pk.clone()))
                            {
                                pks.push(posting.pk);
                            }
                        }
                    }
                }
                if paged.index_has_numeric_posting_blocks(&snapshot, name)? {
                    let mut current_term: Option<Vec<u8>> = None;
                    let mut term_matches = false;
                    let mut tombstones: FxHashSet<String> = FxHashSet::default();
                    for entry in paged.scan_numeric_posting_blocks_encoded_prefix(
                        &snapshot,
                        name,
                        &encoded_prefix,
                    )? {
                        let (encoded_key, _, bytes) = entry?;
                        if current_term.as_deref() != Some(&encoded_key[..]) {
                            term_matches = matches!(
                                decode_index_key(&encoded_key).as_slice(),
                                [IndexValue::String(value)] if value.starts_with(term)
                            );
                            tombstones = if term_matches {
                                paged
                                    .scan_posting_tombstones(&snapshot, name, &encoded_key)?
                                    .collect::<Result<_>>()?
                            } else {
                                FxHashSet::default()
                            };
                            current_term = Some(encoded_key.clone());
                        }
                        if !term_matches {
                            continue;
                        }
                        for posting in
                            crate::paged_collection::decode_numeric_posting_block(&bytes)?
                        {
                            let pk = paged
                                .full_text_pk_for_document_id(&snapshot, name, posting.document_id)?
                                .ok_or_else(|| BicDbError::Corruption {
                                    path: PathBuf::from(DEFAULT_PAGED_DIR),
                                    message: format!(
                                        "full-text document id {} has no primary-key mapping",
                                        posting.document_id
                                    ),
                                })?;
                            if !tombstones.contains(&pk)
                                && !tail.contains(&(encoded_key.clone(), pk.clone()))
                            {
                                pks.push(pk);
                            }
                        }
                    }
                }
                index_trace::record_scan(&index_trace::PREFIX, pks.len());
                pks
            } else {
                let encoded = encode_index_key(&[IndexValue::String(term.to_string())]);
                let mut pks = paged
                    .scan_index_exact(&snapshot, name, &encoded)?
                    .map(|entry| entry.map(|(pk, _)| pk))
                    .collect::<Result<Vec<_>>>()?;
                if paged.index_has_posting_blocks(&snapshot, name)? {
                    let tombstones: FxHashSet<String> = paged
                        .scan_posting_tombstones(&snapshot, name, &encoded)?
                        .collect::<Result<_>>()?;
                    let tail: FxHashSet<String> = pks.iter().cloned().collect();
                    for block in paged.scan_posting_blocks(&snapshot, name, &encoded)? {
                        let (_, bytes) = block?;
                        let (block_postings, _) =
                            crate::paged_collection::decode_posting_block(&bytes)?;
                        for posting in block_postings {
                            if !tail.contains(&posting.pk) && !tombstones.contains(&posting.pk) {
                                pks.push(posting.pk);
                            }
                        }
                    }
                }
                if paged.index_has_numeric_posting_blocks(&snapshot, name)? {
                    let tombstones: FxHashSet<String> = paged
                        .scan_posting_tombstones(&snapshot, name, &encoded)?
                        .collect::<Result<_>>()?;
                    let tail: FxHashSet<String> = pks.iter().cloned().collect();
                    for block in paged.scan_numeric_posting_blocks(&snapshot, name, &encoded)? {
                        let (_, bytes) = block?;
                        for posting in
                            crate::paged_collection::decode_numeric_posting_block(&bytes)?
                        {
                            let pk = paged
                                .full_text_pk_for_document_id(&snapshot, name, posting.document_id)?
                                .ok_or_else(|| BicDbError::Corruption {
                                    path: PathBuf::from(DEFAULT_PAGED_DIR),
                                    message: format!(
                                        "full-text document id {} has no primary-key mapping",
                                        posting.document_id
                                    ),
                                })?;
                            if !tail.contains(&pk) && !tombstones.contains(&pk) {
                                pks.push(pk);
                            }
                        }
                    }
                }
                index_trace::record(&index_trace::POINT);
                pks
            };
            pks.sort_unstable();
            pks.dedup();
            return Ok(pks);
        }
        let mut rowids = Vec::new();
        if prefix {
            state.store.scan_from(&[], &mut |key, ids| {
                if matches!(decode_index_key(key).as_slice(), [IndexValue::String(value)] if value.starts_with(term)) {
                    rowids.extend(ids);
                }
                true
            });
            index_trace::record_scan(&index_trace::PREFIX, rowids.len());
        } else {
            state.store.collect_exact(
                &encode_index_key(&[IndexValue::String(term.to_string())]),
                &mut rowids,
            );
            index_trace::record(&index_trace::POINT);
        }
        rowids.sort_unstable();
        rowids.dedup();
        Ok(self.rowids_to_pks_sorted(&collection, &rowids))
    }

    /// Budgeted boolean/candidate lookup. Durable posting blocks are charged
    /// during decode; resident fallback results are charged before returning.
    pub fn lookup_full_text_term_budgeted(
        &self,
        name: &str,
        term: &str,
        prefix: bool,
        budget: &mut FtsQueryBudget,
    ) -> Result<Vec<String>> {
        let read_through = {
            let state = self
                .indexes
                .get(name)
                .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
                .read();
            Self::ensure_full_text_build_complete(name, &state)?;
            state.paged_read_through
        };
        let pks = if read_through {
            self.full_text_term_postings_budgeted(name, term, prefix, budget)?
                .into_iter()
                .map(|(_, pk, _)| pk)
                .collect::<Vec<_>>()
        } else {
            self.lookup_full_text_term(name, term, prefix)?
        };
        budget.charge_candidates(pks.len() as u64)?;
        Ok(pks)
    }

    /// Postings for one term (or term prefix) of a READ-THROUGH full-text
    /// index: `(matched term, pk, posting payload)`. The payload is the
    /// binary posting value written at commit time (positions/weights plus
    /// the document scalars ranking needs) — see
    /// [`decode_fts_posting_payload`]. Errors when the index is not serving
    /// read-through (embedded mode or the pre-upgrade resident fallback):
    /// callers fall back to ranking from row text.
    /// Diagnostic layout of a full-text index's durable keyspaces:
    /// `(v1 tail entries, v3 posting blocks, v5 impact blocks, sentinel)`.
    /// Ops introspection and test engagement proof — a direct-built index
    /// shows an empty tail; a per-posting one shows no blocks until folded.
    pub fn full_text_index_layout(&self, name: &str) -> Result<(usize, usize, usize, bool)> {
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;
        let diagnostic_name = if self.fts_generations.lock().indexes.contains_key(name) {
            name.to_string()
        } else {
            crate::fts_build::FtsBuildWorkspace::open_existing(
                &self.path.join(DEFAULT_FTS_BUILD_DIR),
                name,
                self.config.fsync,
            )?
            .map(|workspace| workspace.physical_index().to_string())
            .unwrap_or_else(|| name.to_string())
        };
        let snapshot = paged.latest_snapshot();
        let mut tail_entries = 0usize;
        for entry in paged.scan_index(&snapshot, &diagnostic_name)? {
            entry?;
            tail_entries += 1;
        }
        let mut blocks = 0usize;
        for entry in paged.scan_all_posting_blocks(&snapshot, &diagnostic_name)? {
            entry?;
            blocks += 1;
        }
        for entry in paged.scan_all_numeric_posting_blocks(&snapshot, &diagnostic_name)? {
            entry?;
            blocks += 1;
        }
        let mut impact_blocks = 0usize;
        let mut term_keys: Vec<Vec<u8>> = Vec::new();
        for entry in paged.scan_all_posting_blocks(&snapshot, &diagnostic_name)? {
            let (encoded_key, _) = entry?;
            if term_keys
                .last()
                .map(|last| last != &encoded_key)
                .unwrap_or(true)
            {
                term_keys.push(encoded_key);
            }
        }
        for entry in paged.scan_all_numeric_posting_blocks(&snapshot, &diagnostic_name)? {
            let (encoded_key, _, _) = entry?;
            if term_keys
                .last()
                .map(|last| last != &encoded_key)
                .unwrap_or(true)
            {
                term_keys.push(encoded_key);
            }
        }
        term_keys.sort();
        term_keys.dedup();
        for encoded_key in &term_keys {
            impact_blocks += paged
                .scan_impact_blocks(&snapshot, &diagnostic_name, encoded_key)?
                .count();
            impact_blocks += paged
                .scan_numeric_impact_blocks(&snapshot, &diagnostic_name, encoded_key)?
                .count();
        }
        let sentinel = paged.index_v2_complete(&snapshot, &diagnostic_name)?;
        Ok((tail_entries, blocks, impact_blocks, sentinel))
    }

    /// Number of doc-terms blobs an index holds (diagnostics/tests).
    pub fn full_text_doc_terms_count(&self, name: &str) -> Result<usize> {
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;
        let snapshot = paged.latest_snapshot();
        let mut count = 0usize;
        for entry in paged.scan_doc_terms(&snapshot, name)? {
            entry?;
            count += 1;
        }
        Ok(count)
    }

    pub fn full_text_term_postings(
        &self,
        name: &str,
        term: &str,
        prefix: bool,
    ) -> Result<Vec<(String, String, Vec<u8>)>> {
        self.full_text_term_postings_budgeted(name, term, prefix, &mut FtsQueryBudget::unlimited())
    }

    /// Budgeted, cancellable variant of [`Self::full_text_term_postings`]:
    /// charges every posting block decoded and every posting materialized
    /// against the budget, checking cancellation between batches. Broad
    /// terms fail fast with `query_budget_exceeded` instead of resolving an
    /// enormous posting list to completion.
    pub fn full_text_term_postings_budgeted(
        &self,
        name: &str,
        term: &str,
        prefix: bool,
        budget: &mut FtsQueryBudget,
    ) -> Result<Vec<(String, String, Vec<u8>)>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        Self::ensure_full_text_build_complete(name, &state)?;
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Err(BicDbError::Index(format!(
                "index `{name}` does not serve read-through postings"
            )));
        }
        if !full_text_term_is_indexable(term) {
            return Ok(Vec::new());
        }
        drop(state);
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;
        let snapshot = paged.latest_snapshot();
        let mut postings = Vec::new();
        if prefix {
            let mut encoded_prefix = vec![4u8];
            for &byte in term.as_bytes() {
                encoded_prefix.push(byte);
                if byte == 0 {
                    encoded_prefix.push(0xFF);
                }
            }
            let mut tail: FxHashSet<(Vec<u8>, String)> = FxHashSet::default();
            for entry in paged.scan_index_encoded_prefix(&snapshot, name, &encoded_prefix)? {
                let (encoded_key, pk, payload) = entry?;
                if let [IndexValue::String(matched)] = decode_index_key(&encoded_key).as_slice() {
                    if matched.starts_with(term) {
                        tail.insert((encoded_key, pk.clone()));
                        budget.charge_postings(1)?;
                        postings.push((matched.clone(), pk, payload));
                    }
                }
            }
            // Blocks carry the folded postings; the tail overrides per
            // (term, pk) and tombstones hide folded deletes.
            if paged.index_has_posting_blocks(&snapshot, name)? {
                let mut current_term: Option<Vec<u8>> = None;
                let mut matched_term: Option<String> = None;
                let mut tombstones: FxHashSet<String> = FxHashSet::default();
                for entry in
                    paged.scan_posting_blocks_encoded_prefix(&snapshot, name, &encoded_prefix)?
                {
                    let (encoded_key, _, bytes) = entry?;
                    if current_term.as_deref() != Some(&encoded_key[..]) {
                        matched_term = match decode_index_key(&encoded_key).as_slice() {
                            [IndexValue::String(matched)] if matched.starts_with(term) => {
                                Some(matched.clone())
                            }
                            _ => None,
                        };
                        tombstones = if matched_term.is_some() {
                            paged
                                .scan_posting_tombstones(&snapshot, name, &encoded_key)?
                                .collect::<Result<_>>()?
                        } else {
                            FxHashSet::default()
                        };
                        current_term = Some(encoded_key.clone());
                    }
                    let Some(matched) = &matched_term else {
                        continue;
                    };
                    let (block_postings, _) = {
                        budget.charge_posting_blocks(1)?;
                        crate::paged_collection::decode_posting_block(&bytes)?
                    };
                    for posting in block_postings {
                        if tombstones.contains(&posting.pk)
                            || tail.contains(&(encoded_key.clone(), posting.pk.clone()))
                        {
                            continue;
                        }
                        budget.charge_postings(1)?;
                        postings.push((
                            matched.clone(),
                            posting.pk,
                            encode_fts_posting_payload(
                                posting.doc_length,
                                posting.doc_distinct,
                                &posting.packed_positions,
                            ),
                        ));
                    }
                }
            }
            if paged.index_has_numeric_posting_blocks(&snapshot, name)? {
                let mut current_term: Option<Vec<u8>> = None;
                let mut matched_term: Option<String> = None;
                let mut tombstones: FxHashSet<String> = FxHashSet::default();
                for entry in paged.scan_numeric_posting_blocks_encoded_prefix(
                    &snapshot,
                    name,
                    &encoded_prefix,
                )? {
                    let (encoded_key, _, bytes) = entry?;
                    if current_term.as_deref() != Some(&encoded_key[..]) {
                        matched_term = match decode_index_key(&encoded_key).as_slice() {
                            [IndexValue::String(matched)] if matched.starts_with(term) => {
                                Some(matched.clone())
                            }
                            _ => None,
                        };
                        tombstones = if matched_term.is_some() {
                            paged
                                .scan_posting_tombstones(&snapshot, name, &encoded_key)?
                                .collect::<Result<_>>()?
                        } else {
                            FxHashSet::default()
                        };
                        current_term = Some(encoded_key.clone());
                    }
                    let Some(matched) = &matched_term else {
                        continue;
                    };
                    for posting in {
                        budget.charge_posting_blocks(1)?;
                        crate::paged_collection::decode_numeric_posting_block(&bytes)?
                    } {
                        let pk = paged
                            .full_text_pk_for_document_id(&snapshot, name, posting.document_id)?
                            .ok_or_else(|| BicDbError::Corruption {
                                path: PathBuf::from(DEFAULT_PAGED_DIR),
                                message: format!(
                                    "full-text document id {} has no primary-key mapping",
                                    posting.document_id
                                ),
                            })?;
                        if tombstones.contains(&pk)
                            || tail.contains(&(encoded_key.clone(), pk.clone()))
                        {
                            continue;
                        }
                        budget.charge_postings(1)?;
                        postings.push((
                            matched.clone(),
                            pk,
                            encode_fts_posting_payload(
                                posting.doc_length,
                                posting.doc_distinct,
                                &posting.packed_positions,
                            ),
                        ));
                    }
                }
            }
        } else {
            let encoded = encode_index_key(&[IndexValue::String(term.to_string())]);
            let mut tail: FxHashMap<String, Vec<u8>> = FxHashMap::default();
            for entry in paged.scan_index_exact(&snapshot, name, &encoded)? {
                let (pk, payload) = entry?;
                tail.insert(pk, payload);
            }
            if paged.index_has_posting_blocks(&snapshot, name)? {
                let tombstones: FxHashSet<String> = paged
                    .scan_posting_tombstones(&snapshot, name, &encoded)?
                    .collect::<Result<_>>()?;
                for block in paged.scan_posting_blocks(&snapshot, name, &encoded)? {
                    let (_, bytes) = block?;
                    let (block_postings, _) = {
                        budget.charge_posting_blocks(1)?;
                        crate::paged_collection::decode_posting_block(&bytes)?
                    };
                    for posting in block_postings {
                        if tail.contains_key(&posting.pk) || tombstones.contains(&posting.pk) {
                            continue;
                        }
                        budget.charge_postings(1)?;
                        postings.push((
                            term.to_string(),
                            posting.pk,
                            encode_fts_posting_payload(
                                posting.doc_length,
                                posting.doc_distinct,
                                &posting.packed_positions,
                            ),
                        ));
                    }
                }
            }
            if paged.index_has_numeric_posting_blocks(&snapshot, name)? {
                let tombstones: FxHashSet<String> = paged
                    .scan_posting_tombstones(&snapshot, name, &encoded)?
                    .collect::<Result<_>>()?;
                for block in paged.scan_numeric_posting_blocks(&snapshot, name, &encoded)? {
                    let (_, bytes) = block?;
                    for posting in {
                        budget.charge_posting_blocks(1)?;
                        crate::paged_collection::decode_numeric_posting_block(&bytes)?
                    } {
                        let pk = paged
                            .full_text_pk_for_document_id(&snapshot, name, posting.document_id)?
                            .ok_or_else(|| BicDbError::Corruption {
                                path: PathBuf::from(DEFAULT_PAGED_DIR),
                                message: format!(
                                    "full-text document id {} has no primary-key mapping",
                                    posting.document_id
                                ),
                            })?;
                        if tail.contains_key(&pk) || tombstones.contains(&pk) {
                            continue;
                        }
                        budget.charge_postings(1)?;
                        postings.push((
                            term.to_string(),
                            pk,
                            encode_fts_posting_payload(
                                posting.doc_length,
                                posting.doc_distinct,
                                &posting.packed_positions,
                            ),
                        ));
                    }
                }
            }
            for (pk, payload) in tail {
                budget.charge_postings(1)?;
                postings.push((term.to_string(), pk, payload));
            }
        }
        Ok(postings)
    }

    /// Block-max ranked scan for a FOLDED term: blocks are visited in
    /// DESCENDING max-impact order (one header-only pass sorts them), each
    /// decoded block's postings stream through `visit(block_max, pk,
    /// payload)`, and `visit` returning false stops — the same early
    /// termination as the per-posting impact scan, at block granularity,
    /// with the tail and tombstones merged so folded and unfolded postings
    /// rank together. Returns Ok(false) when the index has no blocks.
    /// Drive `visit` over one term's postings in descending block-max-impact
    /// order, gating whole BLOCKS on their header bounds first.
    pub fn full_text_block_impact_scan(
        &self,
        name: &str,
        term: &str,
        budget: &mut FtsQueryBudget,
        mut gate: impl FnMut(u16, Option<f32>) -> BlockGate,
        mut visit: impl FnMut(u16, &str, u32, u32, &[u16]) -> Result<bool>,
    ) -> Result<bool> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Ok(false);
        }
        if !full_text_term_is_indexable(term) {
            return Ok(true);
        }
        drop(state);
        let Some(paged) = self.paged_records.as_ref() else {
            return Ok(false);
        };
        let snapshot = paged.latest_snapshot();
        let has_numeric_blocks = paged.index_has_numeric_posting_blocks(&snapshot, name)?;
        if !has_numeric_blocks && !paged.index_has_posting_blocks(&snapshot, name)? {
            return Ok(false);
        }
        let encoded = encode_index_key(&[IndexValue::String(term.to_string())]);
        // Tail postings rank first at MAX bound (their true impact is
        // unknown without decoding — the tail is small by construction).
        let mut tail: Vec<(String, Vec<u8>)> = Vec::new();
        for entry in paged.scan_index_exact(&snapshot, name, &encoded)? {
            let (pk, payload) = entry?;
            tail.push((pk, payload));
        }
        let tombstones: FxHashSet<String> = paged
            .scan_posting_tombstones(&snapshot, name, &encoded)?
            .collect::<Result<_>>()?;
        let tail_pks: FxHashSet<String> = tail.iter().map(|(pk, _)| pk.clone()).collect();
        let mut packed_scratch: Vec<u16> = Vec::new();
        for (pk, payload) in &tail {
            let Some((doc_length, doc_distinct)) =
                decode_fts_posting_payload_into(payload, &mut packed_scratch)
            else {
                // Legacy payload-less posting: the caller cannot rank it from
                // the index — bow out so the bulk path ranks from row text.
                return Ok(false);
            };
            if !visit(u16::MAX, pk, doc_length, doc_distinct, &packed_scratch)? {
                return Ok(true);
            }
        }
        // The impact-ordered copy streams blocks in descending max-impact
        // order directly — no header pass, and termination can actually skip
        // because each block is an impact run. `visit_posting_block` hands
        // out borrows of two scratch buffers: NOTHING allocates per posting.
        let mut pk_scratch: Vec<u8> = Vec::new();
        // Direct-built / freshly folded indexes have no tail and no
        // tombstones: keep two FxHashSet probes out of the per-posting path.
        let churn = !tombstones.is_empty() || !tail_pks.is_empty();
        let mut compact_base_cache = std::collections::BTreeMap::<
            u64,
            Vec<crate::paged_collection::NumericBlockPosting>,
        >::new();
        if has_numeric_blocks {
            for block in paged.scan_numeric_impact_blocks(&snapshot, name, &encoded)? {
                let (_, bytes) = block?;
                budget.charge_posting_blocks(1)?;
                if let Some(header) = crate::paged_collection::compact_impact_block_header(&bytes) {
                    match gate(header.max_impact, Some(header.max_rank)) {
                        BlockGate::Stop => break,
                        BlockGate::Skip => continue,
                        BlockGate::Scan => {}
                    }
                    let max_impact = header.max_impact;
                    let kept_walking = crate::paged_collection::visit_compact_impact_block(
                        &bytes,
                        |document_id| {
                            budget.charge_postings(1)?;
                            let mut posting = compact_base_cache
                                .range(document_id..)
                                .next()
                                .and_then(|(_, postings)| {
                                    postings
                                        .iter()
                                        .find(|posting| posting.document_id == document_id)
                                })
                                .cloned();
                            if posting.is_none() {
                                let (last_document_id, base) = paged
                                        .numeric_posting_block_for_with_key(
                                            &snapshot,
                                            name,
                                            &encoded,
                                            document_id,
                                        )?
                                        .ok_or_else(|| BicDbError::Corruption {
                                            path: PathBuf::from(DEFAULT_PAGED_DIR),
                                            message: format!(
                                                "compact impact document id {document_id} has no base posting block"
                                            ),
                                        })?;
                                let postings =
                                    crate::paged_collection::decode_numeric_posting_block(&base)?;
                                posting = postings
                                    .iter()
                                    .find(|posting| posting.document_id == document_id)
                                    .cloned();
                                compact_base_cache.insert(last_document_id, postings);
                                while compact_base_cache.len() > 8 {
                                    let Some(first) = compact_base_cache.keys().next().copied()
                                    else {
                                        break;
                                    };
                                    compact_base_cache.remove(&first);
                                }
                            }
                            let posting = posting.ok_or_else(|| BicDbError::Corruption {
                                path: PathBuf::from(DEFAULT_PAGED_DIR),
                                message: format!(
                                    "compact impact document id {document_id} has no base posting"
                                ),
                            })?;
                            let pk = paged
                                    .full_text_pk_for_document_id(&snapshot, name, document_id)?
                                    .ok_or_else(|| BicDbError::Corruption {
                                        path: PathBuf::from(DEFAULT_PAGED_DIR),
                                        message: format!(
                                            "full-text document id {document_id} has no primary-key mapping"
                                        ),
                                    })?;
                            if churn && (tombstones.contains(&pk) || tail_pks.contains(&pk)) {
                                return Ok(true);
                            }
                            visit(
                                max_impact,
                                &pk,
                                posting.doc_length,
                                posting.doc_distinct,
                                &posting.packed_positions,
                            )
                        },
                    )?;
                    if !kept_walking {
                        break;
                    }
                    continue;
                }
                let header = crate::paged_collection::numeric_posting_block_header(&bytes)
                    .ok_or_else(|| {
                        BicDbError::PagedStorage("corrupt numeric posting block".to_string())
                    })?;
                match gate(header.max_impact, Some(header.max_rank)) {
                    BlockGate::Stop => break,
                    BlockGate::Skip => continue,
                    BlockGate::Scan => {}
                }
                let max_impact = header.max_impact;
                let mut positions = Vec::new();
                let kept_walking = crate::paged_collection::visit_numeric_posting_block(
                    &bytes,
                    &mut positions,
                    |document_id, doc_length, doc_distinct, packed| {
                        budget.charge_postings(1)?;
                        let pk = paged
                            .full_text_pk_for_document_id(&snapshot, name, document_id)?
                            .ok_or_else(|| BicDbError::Corruption {
                                path: PathBuf::from(DEFAULT_PAGED_DIR),
                                message: format!(
                                    "full-text document id {document_id} has no primary-key mapping"
                                ),
                            })?;
                        if churn && (tombstones.contains(&pk) || tail_pks.contains(&pk)) {
                            return Ok(true);
                        }
                        visit(max_impact, &pk, doc_length, doc_distinct, packed)
                    },
                )?;
                if !kept_walking {
                    break;
                }
            }
            return Ok(true);
        }
        for block in paged.scan_impact_blocks(&snapshot, name, &encoded)? {
            let (_, bytes) = block?;
            budget.charge_posting_blocks(1)?;
            let header = crate::paged_collection::posting_block_header(&bytes)
                .ok_or_else(|| BicDbError::PagedStorage("corrupt posting block".to_string()))?;
            // BLOCK-MAX GATE: the caller can retire the whole block from its
            // header — Stop when the bucket bound proves the top-k final,
            // Skip when the exact max rank cannot beat (or only ties) the
            // kth. Neither reads a posting.
            match gate(header.max_impact, header.max_rank) {
                BlockGate::Stop => break,
                BlockGate::Skip => continue,
                BlockGate::Scan => {}
            }
            let max_impact = header.max_impact;
            let (kept_walking, _) = crate::paged_collection::visit_posting_block(
                &bytes,
                &mut pk_scratch,
                &mut packed_scratch,
                |pk, doc_length, doc_distinct, packed| {
                    budget.charge_postings(1)?;
                    if churn && (tombstones.contains(pk) || tail_pks.contains(pk)) {
                        return Ok(true);
                    }
                    visit(max_impact, pk, doc_length, doc_distinct, packed)
                },
            )?;
            if !kept_walking {
                break;
            }
        }
        Ok(true)
    }

    /// Drive `visit(bucket, pk)` over one term's postings in DESCENDING
    /// impact order (v2 keyspace). Returns Ok(false) without visiting when
    /// the ordering cannot be trusted: index not read-through, or the v2
    /// completeness sentinel is absent (pre-upgrade index — incremental
    /// writes add v2 twins, but only a full backfill covers every document).
    /// `visit` returns false to stop early — that early stop is the point.
    pub fn full_text_impact_scan(
        &self,
        name: &str,
        term: &str,
        budget: &mut FtsQueryBudget,
        mut visit: impl FnMut(u16, &str, u32, u32, &[u16]) -> Result<bool>,
    ) -> Result<bool> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Ok(false);
        }
        if !full_text_term_is_indexable(term) {
            return Ok(true);
        }
        drop(state);
        let Some(paged) = self.paged_records.as_ref() else {
            return Ok(false);
        };
        let snapshot = paged.latest_snapshot();
        if !paged.index_v2_complete(&snapshot, name)? {
            return Ok(false);
        }
        // Folded indexes serve ranked queries from posting blocks (block-max
        // pruning, next stage); the v2 tail alone no longer covers the term.
        if paged.index_has_any_posting_blocks(&snapshot, name)? {
            return Ok(false);
        }
        let encoded = encode_index_key(&[IndexValue::String(term.to_string())]);
        let mut packed_scratch: Vec<u16> = Vec::new();
        for entry in paged.scan_index_exact_v2(&snapshot, name, &encoded)? {
            let (bucket, pk, payload) = entry?;
            budget.charge_postings(1)?;
            let Some((doc_length, doc_distinct)) =
                decode_fts_posting_payload_into(&payload, &mut packed_scratch)
            else {
                // Legacy payload-less posting: cannot rank from the index.
                return Ok(false);
            };
            if !visit(bucket, &pk, doc_length, doc_distinct, &packed_scratch)? {
                break;
            }
        }
        Ok(true)
    }

    /// Stream one exact term's postings in ASCENDING pk order with the same
    /// layering as [`Self::full_text_term_postings`] (tail overrides blocks
    /// per pk, tombstones hide folded postings) — but zero allocation per
    /// posting: pk and positions are borrows of scratch buffers. `visit`
    /// returning false stops the stream. Returns Ok(false) without visiting
    /// when the index cannot serve read-through postings or a legacy
    /// payload-less posting is hit (callers fall back to the bulk path).
    pub fn full_text_term_postings_stream(
        &self,
        name: &str,
        term: &str,
        mut visit: impl FnMut(&str, u32, u32, &[u16]) -> Result<bool>,
    ) -> Result<bool> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Ok(false);
        }
        if !full_text_term_is_indexable(term) {
            return Ok(true);
        }
        drop(state);
        let Some(paged) = self.paged_records.as_ref() else {
            return Ok(false);
        };
        let snapshot = paged.latest_snapshot();
        let encoded = encode_index_key(&[IndexValue::String(term.to_string())]);
        // Tail first: pk-ordered scan, decoded up front (small by
        // construction — everything since the last fold or direct build).
        let mut tail: Vec<(String, Vec<u8>)> = Vec::new();
        for entry in paged.scan_index_exact(&snapshot, name, &encoded)? {
            let (pk, payload) = entry?;
            tail.push((pk, payload));
        }
        let mut packed_scratch: Vec<u16> = Vec::new();
        let has_numeric_blocks = paged.index_has_numeric_posting_blocks(&snapshot, name)?;
        if !has_numeric_blocks && !paged.index_has_posting_blocks(&snapshot, name)? {
            for (pk, payload) in &tail {
                let Some((doc_length, doc_distinct)) =
                    decode_fts_posting_payload_into(payload, &mut packed_scratch)
                else {
                    return Ok(false);
                };
                if !visit(pk, doc_length, doc_distinct, &packed_scratch)? {
                    return Ok(true);
                }
            }
            return Ok(true);
        }
        let tombstones: FxHashSet<String> = paged
            .scan_posting_tombstones(&snapshot, name, &encoded)?
            .collect::<Result<_>>()?;
        if has_numeric_blocks {
            let mut next_tail = 0usize;
            let mut positions = Vec::new();
            let mut legacy = false;
            for block in paged.scan_numeric_posting_blocks(&snapshot, name, &encoded)? {
                let (_, bytes) = block?;
                let kept_walking = crate::paged_collection::visit_numeric_posting_block(
                    &bytes,
                    &mut positions,
                    |document_id, doc_length, doc_distinct, packed| {
                        let pk = paged
                            .full_text_pk_for_document_id(&snapshot, name, document_id)?
                            .ok_or_else(|| BicDbError::Corruption {
                                path: PathBuf::from(DEFAULT_PAGED_DIR),
                                message: format!(
                                    "full-text document id {document_id} has no primary-key mapping"
                                ),
                            })?;
                        while next_tail < tail.len() && tail[next_tail].0 < pk {
                            let (tail_pk, payload) = &tail[next_tail];
                            next_tail += 1;
                            let mut tail_packed = Vec::new();
                            let Some((length, distinct)) =
                                decode_fts_posting_payload_into(payload, &mut tail_packed)
                            else {
                                legacy = true;
                                return Ok(false);
                            };
                            if !visit(tail_pk, length, distinct, &tail_packed)? {
                                return Ok(false);
                            }
                        }
                        if next_tail < tail.len() && tail[next_tail].0 == pk {
                            return Ok(true);
                        }
                        if tombstones.contains(&pk) {
                            return Ok(true);
                        }
                        visit(&pk, doc_length, doc_distinct, packed)
                    },
                )?;
                if !kept_walking {
                    return Ok(!legacy);
                }
            }
            for (tail_pk, payload) in &tail[next_tail..] {
                let Some((length, distinct)) =
                    decode_fts_posting_payload_into(payload, &mut packed_scratch)
                else {
                    return Ok(false);
                };
                if !visit(tail_pk, length, distinct, &packed_scratch)? {
                    break;
                }
            }
            return Ok(true);
        }
        // Two-pointer merge: blocks stream pk-ascending; tail is pk-ascending.
        let mut next_tail = 0usize;
        let mut pk_scratch: Vec<u8> = Vec::new();
        let mut legacy = false;
        for block in paged.scan_posting_blocks(&snapshot, name, &encoded)? {
            let (_, bytes) = block?;
            let (kept_walking, _) = crate::paged_collection::visit_posting_block(
                &bytes,
                &mut pk_scratch,
                &mut packed_scratch,
                |pk, doc_length, doc_distinct, packed| {
                    // Emit tail postings that sort before this block pk.
                    while next_tail < tail.len() && tail[next_tail].0.as_str() < pk {
                        let (tail_pk, payload) = &tail[next_tail];
                        next_tail += 1;
                        let mut tail_packed: Vec<u16> = Vec::new();
                        let Some((length, distinct)) =
                            decode_fts_posting_payload_into(payload, &mut tail_packed)
                        else {
                            legacy = true;
                            return Ok(false);
                        };
                        if !visit(tail_pk, length, distinct, &tail_packed)? {
                            return Ok(false);
                        }
                    }
                    if next_tail < tail.len() && tail[next_tail].0.as_str() == pk {
                        // Tail overrides the folded posting for this pk.
                        return Ok(true);
                    }
                    if tombstones.contains(pk) {
                        return Ok(true);
                    }
                    visit(pk, doc_length, doc_distinct, packed)
                },
            )?;
            if !kept_walking {
                return Ok(!legacy);
            }
        }
        for (tail_pk, payload) in &tail[next_tail..] {
            let Some((length, distinct)) =
                decode_fts_posting_payload_into(payload, &mut packed_scratch)
            else {
                return Ok(false);
            };
            if !visit(tail_pk, length, distinct, &packed_scratch)? {
                return Ok(true);
            }
        }
        Ok(true)
    }
}
