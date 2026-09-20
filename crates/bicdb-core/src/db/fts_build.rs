//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;

impl BicDb {
    /// Continue an interrupted full-text build rather than refusing the retry.
    ///
    /// Returns `None` when this is not a resumable situation — no workspace, a
    /// finished one, a different index kind, or a catalog entry describing a
    /// DIFFERENT index under the same name, which is a genuine collision and
    /// must still be reported as one.
    pub(crate) fn resume_incomplete_full_text_build(
        &mut self,
        definition: &IndexDefinition,
    ) -> Result<Option<IndexMaintenanceReport>> {
        if definition.kind != IndexKind::FullText || !self.is_server_paged() {
            return Ok(None);
        }
        let root = self.path.join(DEFAULT_FTS_BUILD_DIR);
        // `open_existing` already refuses a mismatched checkpoint version, a
        // different logical index, and a `Complete` phase, so `Some` here means
        // genuinely unfinished work for THIS index.
        if crate::fts_build::FtsBuildWorkspace::open_existing(
            &root,
            &definition.name,
            self.config.fsync,
        )?
        .is_none()
        {
            return Ok(None);
        }
        let same_definition = self
            .indexes
            .get(&definition.name)
            .is_some_and(|index| index.read().definition == *definition);
        if !same_definition {
            return Ok(None);
        }

        let started = Instant::now();
        // Re-enter the same backfill the original CREATE ran. The build opens
        // its existing workspace and continues from the recorded phase; this
        // function deliberately adds no build logic of its own, so resume and
        // first-run cannot drift apart.
        self.backfill_paged_index_entries(definition)?;
        if let Some(paged) = &self.paged_records {
            let snapshot = paged.latest_snapshot();
            if paged.index_has_any_posting_blocks(&snapshot, &definition.name)? {
                if let Some(state) = self.indexes.get(&definition.name) {
                    state.write().paged_read_through = true;
                }
            }
        }
        self.schema_compatibility.invalidate();
        let verification = IndexVerifyReport {
            index_name: definition.name.clone(),
            collection: definition.collection.clone(),
            kind: IndexKind::FullText,
            indexed_records: 0,
            expected_records: 0,
            missing_entries: 0,
            stale_entries: 0,
            duplicate_entries: 0,
            wrong_entries: 0,
            size_bytes: 0,
            last_verified_unix_ms: Some(unix_timestamp_millis()),
            valid: true,
        };
        Ok(Some(IndexMaintenanceReport {
            index_name: definition.name.clone(),
            collection: definition.collection.clone(),
            kind: IndexKind::FullText,
            operation: "create".to_string(),
            status: "resumed".to_string(),
            online: true,
            restartable: true,
            // The signal an operator needs: this CREATE continued a previous
            // run rather than starting one.
            interrupted_previous: true,
            progress_percent: 100,
            records_scanned: 0,
            records_indexed: 0,
            size_bytes: 0,
            build_time_ms: started.elapsed().as_millis(),
            lock_phases: Vec::new(),
            stale: false,
            corrupt: false,
            last_verified_unix_ms: Some(unix_timestamp_millis()),
            verification,
        }))
    }

    pub(crate) fn reconcile_published_paged_btree_build(
        &self,
        definition: &IndexDefinition,
    ) -> Result<Option<PagedBTreeBuildState>> {
        let limits = self.paged_btree_build_limits();
        let path = self.paged_btree_build_path(&definition.name);
        if !path.exists() {
            return Ok(None);
        }
        let mut state = self.load_paged_btree_build(&definition.name, limits)?;
        if state.phase != PagedBTreeBuildPhase::Publishing || state.definition != *definition {
            return Ok(None);
        }
        let catalog_matches = self.indexes.get(&definition.name).is_some_and(|index| {
            let index = index.read();
            index.definition == *definition && index.paged_read_through
        });
        let alias_matches =
            self.fts_generations.lock().indexes.get(&definition.name) == Some(&state.physical_name);
        if !catalog_matches || !alias_matches {
            return Ok(None);
        }
        state.phase = PagedBTreeBuildPhase::Complete;
        state.updated_at_ms = state
            .updated_at_ms
            .max(unix_timestamp_millis().max(0) as u64);
        state.refresh_checksum()?;
        self.save_paged_btree_build(&state)?;
        Ok(Some(state))
    }

    pub(crate) fn paged_btree_build_path(&self, index: &str) -> PathBuf {
        let digest = hex::encode(sha2::Sha256::digest(index.as_bytes()));
        self.path
            .join(DEFAULT_PAGED_BTREE_BUILD_DIR)
            .join(format!("{digest}.json"))
    }

    /// Prepare a crash-resumable paged B-tree generation without making the
    /// logical index visible. Reopening with the same definition returns the
    /// existing durable checkpoint.
    pub fn begin_paged_btree_build(
        &mut self,
        definition: IndexDefinition,
        limits: PagedBTreeBuildLimits,
        now_ms: u64,
    ) -> Result<PagedBTreeBuildState> {
        self.ensure_writable("begin paged B-tree build")?;
        limits.validate()?;
        validate_index_definition(&definition)?;
        self.ensure_collection(&definition.collection)?;
        self.validate_secure_index_definition(&definition)?;
        if definition.kind != IndexKind::BTree || self.paged_records.is_none() {
            return Err(BicDbError::Index(
                "restartable paged B-tree builds require a B-tree index and server_paged storage"
                    .to_string(),
            ));
        }
        if definition.exclusion.is_some() {
            return Err(BicDbError::Index(
                "paged B-tree build does not yet support exclusion indexes".to_string(),
            ));
        }
        if self.indexes.contains_key(&definition.name) {
            return Err(BicDbError::Index(format!(
                "index `{}` already exists",
                definition.name
            )));
        }
        self.ensure_no_pending_transactions()?;
        let root = self.path.join(DEFAULT_PAGED_BTREE_BUILD_DIR);
        if let Ok(metadata) = fs::symlink_metadata(&root) {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(BicDbError::Index(
                    "refusing unsafe paged B-tree build directory".to_string(),
                ));
            }
        } else {
            fs::create_dir_all(&root)?;
        }
        let path = self.paged_btree_build_path(&definition.name);
        if path.exists() {
            let existing = self.load_paged_btree_build(&definition.name, limits)?;
            // A COMPLETE state for an index that is NOT in the catalog is
            // orphaned provenance: the build published once and the index was
            // since dropped (directly, or by a table rename that re-derives
            // its primary-key index under a new collection). Adopting it
            // would "complete" instantly without re-registering the logical
            // index, and a definition mismatch would refuse the create
            // outright. Either way the file no longer describes live work:
            // discard it and build fresh. An IN-FLIGHT state is still adopted
            // or refused exactly as before — that is the resumable contract.
            if existing.phase == PagedBTreeBuildPhase::Complete
                && !self.indexes.contains_key(&definition.name)
            {
                fs::remove_file(&path)?;
            } else if existing.definition == definition {
                return Ok(existing);
            } else {
                return Err(BicDbError::Index(format!(
                    "another paged B-tree build for `{}` is active",
                    definition.name
                )));
            }
        }
        let run_id = Uuid::now_v7();
        let physical_name = format!("{}.__btree.{}", definition.name, run_id);
        // A brand-new build owns a fresh physical namespace: publish its
        // entry format BEFORE any entry exists, so every writer (this build,
        // and live commits maintaining the build's delta) agrees.
        if paged_v3_entries_enabled() && definition.kind == IndexKind::BTree {
            if let Some(paged) = &self.paged_records {
                let (xid, _) = paged.begin();
                paged.set_index_entry_format(
                    xid,
                    &physical_name,
                    &definition.collection,
                    crate::paged_collection::IndexEntryFormat::V3,
                )?;
                paged.commit(xid)?;
            }
        }
        let mut state = PagedBTreeBuildState {
            format_version: PAGED_BTREE_BUILD_FORMAT_VERSION,
            run_id,
            phase: PagedBTreeBuildPhase::Building,
            physical_name,
            definition,
            limits,
            starting_commit_seq: self.last_commit_seq(),
            resume_after_id: None,
            verify_after_id: None,
            count_after_key_hex: None,
            count_after_pk: None,
            expected_entries: 0,
            verified_entries: 0,
            physical_entries: 0,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            checksum_sha256: String::new(),
        };
        state.refresh_checksum()?;
        self.save_paged_btree_build(&state)?;
        Ok(state)
    }

    pub fn load_paged_btree_build(
        &self,
        index: &str,
        limits: PagedBTreeBuildLimits,
    ) -> Result<PagedBTreeBuildState> {
        limits.validate()?;
        let path = self.paged_btree_build_path(index);
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() == 0
            || metadata.len() > limits.max_state_bytes
        {
            return Err(BicDbError::Index(
                "paged B-tree build state is unsafe or outside its bound".to_string(),
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
        let file = File::open(&path)?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(limits.max_state_bytes.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 != metadata.len() || bytes.len() as u64 > limits.max_state_bytes {
            return Err(BicDbError::Index(
                "paged B-tree build state changed or grew while reading".to_string(),
            ));
        }
        let state: PagedBTreeBuildState = serde_json::from_slice(&bytes)?;
        if state.limits != limits || state.definition.name != index {
            return Err(BicDbError::Index(
                "paged B-tree build identity or limits changed across restart".to_string(),
            ));
        }
        state.validate()?;
        Ok(state)
    }

    pub(crate) fn save_paged_btree_build(&self, state: &PagedBTreeBuildState) -> Result<()> {
        state.validate()?;
        let bytes = serde_json::to_vec(state)?;
        if bytes.len() as u64 > state.limits.max_state_bytes {
            return Err(BicDbError::Index(
                "paged B-tree build state exceeds its write bound".to_string(),
            ));
        }
        storage::write_atomic(
            &self.paged_btree_build_path(&state.definition.name),
            &bytes,
            self.config.fsync,
        )
    }

    /// Advance exactly one bounded scan/verification batch or one publication
    /// phase. The source commit sequence is checked before and after work, so a
    /// changed table can never promote a stale shadow generation.
    pub fn advance_paged_btree_build_governed(
        &mut self,
        index: &str,
        limits: PagedBTreeBuildLimits,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<PagedBTreeBuildAdvance> {
        let mut state = self.load_paged_btree_build(index, limits)?;
        if state.phase == PagedBTreeBuildPhase::Complete {
            return Ok(PagedBTreeBuildAdvance::Complete(state));
        }
        if now_ms < state.updated_at_ms {
            return Err(BicDbError::Index(
                "paged B-tree build clock regressed".to_string(),
            ));
        }
        if demand.memory_bytes < limits.max_batch_bytes as u64 {
            return Err(BicDbError::ResourceGovernance(format!(
                "paged B-tree batch may use {} bytes but demand declares {}",
                limits.max_batch_bytes, demand.memory_bytes
            )));
        }
        self.ensure_no_pending_transactions()?;
        if self.last_commit_seq() != state.starting_commit_seq {
            return Err(BicDbError::Index(
                "database changed during paged B-tree build; discard and restart the run"
                    .to_string(),
            ));
        }
        let _permit = governor.try_admit(ResourceLane::IndexBuild, demand, now_ms)?;
        let paged = Arc::clone(self.paged_records.as_ref().ok_or_else(|| {
            BicDbError::Index("paged B-tree build lost its page store".to_string())
        })?);
        let snapshot = paged.latest_snapshot();
        match state.phase {
            PagedBTreeBuildPhase::Building => {
                let rows = paged.scan_batch_after_with_heads(
                    &snapshot,
                    &state.definition.collection,
                    state.resume_after_id.as_deref(),
                    limits.max_batch_rows,
                    limits.max_batch_bytes,
                )?;
                if rows.is_empty() {
                    state.phase = PagedBTreeBuildPhase::VerifyingRows;
                } else {
                    let entry_format = paged.entry_format_cached(&state.physical_name)?;
                    let def = PagedDurableIndexDef {
                        name: state.physical_name.clone(),
                        fields: state.definition.fields.clone(),
                        kind: IndexKind::BTree,
                        predicate: state.definition.predicate.clone(),
                        entry_format,
                        // The build scans a write-fenced collection; there is
                        // nothing concurrent to guard against, and a
                        // pre-existing duplicate must surface as the build's
                        // verification error, not an apply abort.
                        unique: false,
                    };
                    let indexed = rows
                        .iter()
                        .map(|(record, _, _)| {
                            record_matches_index_predicate(record, &state.definition)
                        })
                        .collect::<Result<Vec<_>>>()?
                        .into_iter()
                        .filter(|matches| *matches)
                        .count() as u64;
                    self.in_paged_transaction(|paged, xid| {
                        for (record, head, head_xmin) in &rows {
                            apply_paged_index_upsert(
                                paged,
                                xid,
                                &state.definition.collection,
                                std::slice::from_ref(&def),
                                None,
                                record,
                                Some((*head, *head_xmin)),
                            )?;
                        }
                        Ok(())
                    })?;
                    state.expected_entries = state.expected_entries.saturating_add(indexed);
                    state.resume_after_id = rows.last().map(|(record, _, _)| record.id.clone());
                }
            }
            PagedBTreeBuildPhase::VerifyingRows => {
                let rows = paged.scan_batch_after(
                    &snapshot,
                    &state.definition.collection,
                    state.verify_after_id.as_deref(),
                    limits.max_batch_rows,
                    limits.max_batch_bytes,
                )?;
                if rows.is_empty() {
                    if state.verified_entries != state.expected_entries {
                        return Err(BicDbError::Index(
                            "paged B-tree source verification count mismatch".to_string(),
                        ));
                    }
                    state.phase = PagedBTreeBuildPhase::CountingEntries;
                } else {
                    for record in &rows {
                        if !record_matches_index_predicate(record, &state.definition)? {
                            continue;
                        }
                        let key =
                            encode_index_key(&record_index_key(record, &state.definition.fields));
                        // Point probe for the exact (key, pk) entry — one
                        // B-tree descent. Scanning the key's whole run here
                        // made verification O(rows-per-key) per row, which on
                        // a low-cardinality index turned the build quadratic:
                        // measured 2.7 s per 4096-row verify batch at 100k
                        // rows over 36 distinct keys.
                        let present = paged
                            .get_index_entry(&snapshot, &state.physical_name, &key, &record.id)?
                            .is_some();
                        if !present {
                            return Err(BicDbError::Index(format!(
                                "paged B-tree verification found missing row `{}`",
                                record.id
                            )));
                        }
                        state.verified_entries = state.verified_entries.saturating_add(1);
                    }
                    state.verify_after_id = rows.last().map(|record| record.id.clone());
                }
            }
            PagedBTreeBuildPhase::CountingEntries => {
                // The cursor is the RAW store key of the last counted entry —
                // format-agnostic, so v3 (intern-keyed) entries resume
                // exactly. `count_after_pk` is legacy state (pre-v3 builds);
                // a build that stored it resumes correctly because raw keys
                // strictly extend the (key, pk) ordering it encoded.
                let after_raw = state
                    .count_after_key_hex
                    .as_deref()
                    .map(hex::decode)
                    .transpose()
                    .map_err(|_| {
                        BicDbError::Index("invalid paged B-tree count cursor".to_string())
                    })?;
                let entries = paged.scan_index_batch_after_raw(
                    &snapshot,
                    &state.physical_name,
                    after_raw.as_deref(),
                    limits.max_batch_rows,
                    limits.max_batch_bytes,
                )?;
                if entries.is_empty() {
                    if state.physical_entries != state.expected_entries {
                        return Err(BicDbError::Index(format!(
                            "paged B-tree physical count {} differs from expected {}",
                            state.physical_entries, state.expected_entries
                        )));
                    }
                    state.phase = PagedBTreeBuildPhase::Publishing;
                } else {
                    let mut previous: Option<Vec<u8>> = after_raw
                        .as_deref()
                        .map(|raw| {
                            crate::paged_collection::decode_index_entry_key_any(
                                &state.physical_name,
                                raw,
                            )
                            .map(|(encoded, _)| encoded)
                        })
                        .transpose()
                        .unwrap_or(None);
                    for (_, encoded, _) in &entries {
                        if state.definition.unique
                            && previous.as_deref() == Some(encoded.as_slice())
                            && !index_key_contains_null(&decode_index_key(encoded))
                        {
                            return Err(BicDbError::Index(format!(
                                "unique index `{}` has duplicate keys",
                                state.definition.name
                            )));
                        }
                        previous = Some(encoded.clone());
                    }
                    state.physical_entries =
                        state.physical_entries.saturating_add(entries.len() as u64);
                    if let Some((raw, _, _)) = entries.last() {
                        state.count_after_key_hex = Some(hex::encode(raw));
                        // Empty sentinel keeps the paired-cursor invariant.
                        state.count_after_pk = Some(String::new());
                    }
                }
            }
            PagedBTreeBuildPhase::Publishing => {
                if self.last_commit_seq() != state.starting_commit_seq {
                    return Err(BicDbError::Index(
                        "database changed before paged B-tree publication".to_string(),
                    ));
                }
                // The alias is inert until the logical definition catalog is
                // durably written; indexes.json is therefore the visibility
                // commit marker. A crash before it leaves only resumable shadow
                // state, while an older logical index remains untouched.
                self.publish_full_text_generation(&state.definition.name, &state.physical_name)?;
                self.index_generation
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.indexes.insert(
                    state.definition.name.clone(),
                    RwLock::new(IndexState {
                        definition: state.definition.clone(),
                        store: new_index_store(),
                        spatial: None,
                        packed_spatial: None,
                        spatial_tombstones: FxHashSet::default(),
                        spatial_delta_durable: false,
                        paged_read_through: true,
                        full_text_build_incomplete: false,
                    }),
                );
                if let Err(error) = self.persist_index_catalog() {
                    self.index_generation
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.indexes.remove(&state.definition.name);
                    return Err(error);
                }
                self.schema_compatibility.invalidate();
                state.phase = PagedBTreeBuildPhase::Complete;
            }
            PagedBTreeBuildPhase::Complete => unreachable!(),
        }
        if self.last_commit_seq() != state.starting_commit_seq {
            return Err(BicDbError::Index(
                "database changed while advancing paged B-tree build".to_string(),
            ));
        }
        state.updated_at_ms = now_ms;
        state.refresh_checksum()?;
        self.save_paged_btree_build(&state)?;
        if state.phase == PagedBTreeBuildPhase::Complete {
            Ok(PagedBTreeBuildAdvance::Complete(state))
        } else {
            Ok(PagedBTreeBuildAdvance::Progress(state))
        }
    }

    /// Validate and remove an incomplete build and its unreachable physical
    /// namespace. Published indexes are never removed by this operation.
    pub fn discard_paged_btree_build(
        &self,
        index: &str,
        limits: PagedBTreeBuildLimits,
    ) -> Result<bool> {
        let path = self.paged_btree_build_path(index);
        if !path.exists() {
            return Ok(false);
        }
        let state = self.load_paged_btree_build(index, limits)?;
        if state.phase == PagedBTreeBuildPhase::Complete {
            return Err(BicDbError::Index(
                "refusing to discard a published paged B-tree build".to_string(),
            ));
        }
        if self.indexes.contains_key(index) {
            return Err(BicDbError::Index(
                "refusing to discard a paged B-tree generation referenced by a published catalog"
                    .to_string(),
            ));
        }
        let paged = Arc::clone(self.paged_records.as_ref().ok_or_else(|| {
            BicDbError::Index("paged B-tree build lost its page store".to_string())
        })?);
        // The generation alias is intentionally durable before the logical
        // index catalog. When publication fails between those writes, the
        // alias is inert but must be atomically removed before its physical
        // namespace can be discarded.
        {
            let mut aliases = self.fts_generations.lock();
            if aliases.indexes.get(index) == Some(&state.physical_name) {
                let removed = aliases.indexes.remove(index);
                let bytes = serde_json::to_vec_pretty(&*aliases)?;
                if let Err(error) = storage::write_atomic(
                    &self.path.join(DEFAULT_FTS_GENERATION_CATALOG),
                    &bytes,
                    self.config.fsync,
                ) {
                    if let Some(physical) = removed {
                        aliases.indexes.insert(index.to_string(), physical);
                    }
                    return Err(error);
                }
                paged.remove_index_alias(index);
            }
        }
        self.remove_physical_index_entries(&paged, &state.physical_name)?;
        fs::remove_file(path)?;
        Ok(true)
    }

    /// Write durable index entries for every existing row of a fresh B-tree
    /// index. One paged transaction: an index either exists completely in the
    /// page store or not at all — a crash mid-backfill must not leave a
    /// half-populated keyspace that later reads would trust.
    /// Bulk-write doc-terms blobs for a full-text index — the CREATE INDEX
    /// pre-pass. The blobs, not the rows, become the corpus the direct build
    /// reads, which is what lets CREATE INDEX stop rewriting every row with
    /// an embedded JSON projection. Chunked transactions; no-op in embedded
    /// mode (the resident build reads in-row projections there).
    pub fn put_fts_doc_terms_batch(&self, index: &str, blobs: &[(String, Vec<u8>)]) -> Result<()> {
        if self.paged_records.is_none() {
            return Ok(());
        }
        for chunk in blobs.chunks(4_096) {
            self.in_paged_transaction(|paged, xid| {
                for (pk, blob) in chunk {
                    paged.put_doc_terms(xid, index, pk, blob)?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    pub(crate) fn put_fts_document_id_batch(
        &self,
        index: &str,
        first_document_id: u64,
        blobs: &[(String, Vec<u8>)],
    ) -> Result<()> {
        if self.paged_records.is_none() {
            return Ok(());
        }
        for (chunk_index, chunk) in blobs.chunks(4_096).enumerate() {
            let chunk_first = first_document_id + (chunk_index * 4_096) as u64;
            self.in_paged_transaction(|paged, xid| {
                for (offset, (pk, blob)) in chunk.iter().enumerate() {
                    let document_id = chunk_first + offset as u64;
                    paged.put_full_text_document_id(xid, index, document_id, pk)?;
                    let statistics = crate::paged_collection::decode_doc_term_statistics(blob)?;
                    paged.put_full_text_document_statistics(
                        xid,
                        index,
                        document_id,
                        &statistics,
                    )?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    /// Open or resume the disk-backed build generation for one logical GIN
    /// index. The signature is the SQL source expression; changing it safely
    /// discards incompatible temporary runs.
    pub fn prepare_full_text_build(
        &self,
        index: &str,
        collection: &str,
        source_signature: &str,
    ) -> Result<FullTextBuildProgress> {
        if self.paged_records.is_none() {
            return Ok(FullTextBuildProgress {
                needs_tokenization: false,
                resume_after: None,
                memory_bytes: self.config.fts_build_memory_bytes,
                workers: self.effective_full_text_build_workers(),
            });
        }
        let workspace = crate::fts_build::FtsBuildWorkspace::open_or_create(
            &self.path.join(DEFAULT_FTS_BUILD_DIR),
            index,
            collection,
            source_signature,
            self.config.fsync,
        )?;
        if workspace.checkpoint().progressive_subs > 0
            && workspace.checkpoint().progressive_old_generation.is_some()
        {
            // Direct/sealed ingestion bypasses CREATE INDEX's resume path.
            // Restore the first progressive generation after a process
            // restart so an external loader and a snapshot reader observe
            // the same partial index the original writer published.
            let physical = workspace.physical_index().to_string();
            let paged = self.paged_records.as_ref().expect("checked above");
            paged.register_fts_segment(&physical)?;
            self.fts_block_cache.purge_generation(&physical);
            self.publish_full_text_generation(index, &physical)?;
            if let Some(state) = self.indexes.get(index) {
                state.write().paged_read_through = true;
            }
        }
        Ok(FullTextBuildProgress {
            needs_tokenization: workspace.phase() == crate::fts_build::FtsBuildPhase::Tokenizing,
            resume_after: workspace.checkpoint().last_pk.clone(),
            memory_bytes: self.config.fts_build_memory_bytes,
            workers: self.effective_full_text_build_workers(),
        })
    }

    pub fn prepare_full_text_build_governed(
        &self,
        index: &str,
        collection: &str,
        source_signature: &str,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<FullTextBuildProgress> {
        let permit = self.admit_index_build(governor, demand, now_ms)?;
        let result = self.prepare_full_text_build(index, collection, source_signature);
        drop(permit);
        result
    }

    /// Persist one tokenized batch and its two sorted posting runs. Doc terms
    /// are written first; if the process dies before the checkpoint advances,
    /// replaying the batch simply overwrites the same document blobs and runs.
    pub fn append_full_text_build_batch(
        &self,
        index: &str,
        blobs: &[(String, Vec<u8>)],
    ) -> Result<()> {
        self.append_full_text_build_batch_inner(index, blobs, None, true)
    }

    pub fn append_full_text_build_batch_governed(
        &self,
        index: &str,
        blobs: &[(String, Vec<u8>)],
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<()> {
        let permit = self.admit_index_build(governor, demand, now_ms)?;
        let result = self.append_full_text_build_batch(index, blobs);
        drop(permit);
        result
    }

    /// Append already-analyzed documents directly to a resumable FTS build.
    ///
    /// This path never creates or updates a collection row. Terms are grouped
    /// by native BM25F field, optional retrieval text is compressed into the
    /// unpublished generation, and the normal bounded posting-run checkpoint
    /// remains the commit point. Documents must arrive in strictly ascending
    /// primary-key order so replay can resume after `FullTextBuildProgress`.
    pub fn append_full_text_documents(
        &self,
        index: &str,
        documents: &[crate::fts_format::FullTextDocumentInput],
    ) -> Result<()> {
        self.append_full_text_documents_inner(index, documents, true)
    }

    /// Append already-analyzed documents for an immutable, sealed generation.
    ///
    /// Unlike [`Self::append_full_text_documents`], this does not retain the
    /// per-document reverse-term blobs needed to retract or replace individual
    /// documents later. The completed generation remains fully searchable,
    /// including positions and stored retrieval text, but it must be replaced
    /// as a whole rather than mutated in place.
    pub fn append_sealed_full_text_documents(
        &self,
        index: &str,
        documents: &[crate::fts_format::FullTextDocumentInput],
    ) -> Result<()> {
        if !self.config.fts_packed_segments {
            return Err(BicDbError::Index(
                "sealed direct full-text documents require packed segments".to_string(),
            ));
        }
        self.append_full_text_documents_inner(index, documents, false)
    }

    pub(crate) fn append_full_text_documents_inner(
        &self,
        index: &str,
        documents: &[crate::fts_format::FullTextDocumentInput],
        retain_document_terms: bool,
    ) -> Result<()> {
        if documents.is_empty() {
            return Ok(());
        }
        let workspace = crate::fts_build::FtsBuildWorkspace::open_existing(
            &self.path.join(DEFAULT_FTS_BUILD_DIR),
            index,
            self.config.fsync,
        )?
        .ok_or_else(|| BicDbError::Index(format!("full-text build `{index}` is not prepared")))?;
        if documents
            .windows(2)
            .any(|pair| pair[0].primary_key >= pair[1].primary_key)
            || workspace
                .checkpoint()
                .last_pk
                .as_ref()
                .is_some_and(|last| documents[0].primary_key <= *last)
        {
            return Err(BicDbError::Index(
                "direct full-text documents must use strictly ascending, non-replayed primary keys"
                    .to_string(),
            ));
        }
        drop(workspace);

        let mut blobs = Vec::with_capacity(documents.len());
        let mut stored = Vec::with_capacity(documents.len());
        let mut batch_bytes = 0usize;
        let batch_budget = (self.config.fts_build_memory_bytes / 2).max(64 * 1024);
        for document in documents {
            let mut terms = std::collections::BTreeMap::<String, Vec<u16>>::new();
            let mut field_lengths = [0u32; 4];
            for field in &document.fields {
                let slot = field.field.slot();
                for input in &field.terms {
                    if input.term.is_empty() || !full_text_term_is_indexable(&input.term) {
                        continue;
                    }
                    let packed = terms.entry(input.term.clone()).or_default();
                    if input.positions.is_empty() {
                        packed.push((slot as u16) << 14);
                        field_lengths[slot] = field_lengths[slot].saturating_add(1);
                    } else {
                        for &position in &input.positions {
                            if position > 0x3FFF {
                                return Err(BicDbError::Index(format!(
                                    "full-text position {position} exceeds the 14-bit field-local limit"
                                )));
                            }
                            packed.push(((slot as u16) << 14) | position);
                            field_lengths[slot] = field_lengths[slot].saturating_add(1);
                        }
                    }
                }
            }
            let mut terms = terms.into_iter().collect::<Vec<_>>();
            for (_, positions) in &mut terms {
                positions.sort_unstable();
            }
            let document_length = field_lengths.into_iter().fold(0u32, u32::saturating_add);
            let distinct_terms = u32::try_from(terms.len()).map_err(|_| {
                BicDbError::Index("full-text document has too many distinct terms".to_string())
            })?;
            for filter in &document.filters {
                let term = crate::fts_format::full_text_filter_term(&filter.name, &filter.value)?;
                match terms.binary_search_by(|(candidate, _)| candidate.cmp(&term)) {
                    Ok(_) => {}
                    Err(position) => terms.insert(position, (term, Vec::new())),
                }
            }
            let blob = crate::paged_collection::encode_doc_terms_with_field_lengths(
                document_length,
                distinct_terms,
                field_lengths,
                &terms,
            );
            let stored_text = document
                .stored_text
                .as_deref()
                .map(crate::fts_format::encode_stored_text)
                .transpose()?;
            batch_bytes = batch_bytes
                .saturating_add(document.primary_key.len())
                .saturating_add(blob.len())
                .saturating_add(stored_text.as_ref().map_or(0, Vec::len))
                .saturating_add(
                    document
                        .filters
                        .iter()
                        .map(|filter| filter.name.len().saturating_add(filter.value.len()))
                        .sum::<usize>(),
                )
                .saturating_add(64);
            if !blobs.is_empty() && batch_bytes > batch_budget {
                return Err(BicDbError::Index(format!(
                    "direct full-text batch uses more than the {batch_budget}-byte ingestion budget; submit a smaller primary-key range"
                )));
            }
            blobs.push((document.primary_key.clone(), blob));
            stored.push(stored_text);
        }
        self.append_full_text_build_batch_inner(index, &blobs, Some(&stored), retain_document_terms)
    }

    pub fn append_full_text_documents_governed(
        &self,
        index: &str,
        documents: &[crate::fts_format::FullTextDocumentInput],
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<()> {
        let permit = self.admit_index_build(governor, demand, now_ms)?;
        let result = self.append_full_text_documents(index, documents);
        drop(permit);
        result
    }

    pub fn append_sealed_full_text_documents_governed(
        &self,
        index: &str,
        documents: &[crate::fts_format::FullTextDocumentInput],
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<()> {
        let permit = self.admit_index_build(governor, demand, now_ms)?;
        let result = self.append_sealed_full_text_documents(index, documents);
        drop(permit);
        result
    }

    pub(crate) fn append_full_text_build_batch_inner(
        &self,
        index: &str,
        blobs: &[(String, Vec<u8>)],
        stored_text: Option<&[Option<Vec<u8>>]>,
        retain_document_terms: bool,
    ) -> Result<()> {
        if blobs.is_empty() || self.paged_records.is_none() {
            return Ok(());
        }
        if stored_text.is_some_and(|stored| stored.len() != blobs.len()) {
            return Err(BicDbError::Index(
                "stored-text batch does not match document batch".to_string(),
            ));
        }
        let stage = Instant::now();
        let mut workspace = crate::fts_build::FtsBuildWorkspace::open_existing(
            &self.path.join(DEFAULT_FTS_BUILD_DIR),
            index,
            self.config.fsync,
        )?
        .ok_or_else(|| BicDbError::Index(format!("full-text build `{index}` is not prepared")))?;
        FTS_BUILD_OPEN_NANOS.fetch_add(stage.elapsed().as_nanos() as u64, AtomicOrdering::Relaxed);
        let physical = workspace.physical_index().to_string();
        let first_document_id = workspace.checkpoint().document_count;
        let stage = Instant::now();
        if retain_document_terms {
            self.put_fts_doc_terms_batch(&physical, blobs)?;
        }
        self.put_fts_document_id_batch(&physical, first_document_id, blobs)?;
        if let Some(stored_text) = stored_text {
            self.put_fts_stored_text_batch(&physical, first_document_id, stored_text)?;
        }
        FTS_BUILD_PUT_NANOS.fetch_add(stage.elapsed().as_nanos() as u64, AtomicOrdering::Relaxed);
        let stage = Instant::now();
        workspace.append_document_batch(
            blobs,
            self.config.fts_build_memory_bytes / 2,
            self.effective_full_text_build_workers(),
        )?;
        if self.config.fts_progressive && self.config.fts_packed_segments {
            let ready = workspace.checkpoint().document_count
                - workspace.checkpoint().progressive_docs
                >= self.config.fts_progressive_interval_docs;
            if ready {
                let definition = self
                    .indexes
                    .get(index)
                    .ok_or_else(|| BicDbError::Index(format!("index `{index}` does not exist")))?
                    .read()
                    .definition
                    .clone();
                let paged = Arc::clone(
                    self.paged_records
                        .as_ref()
                        .expect("direct full-text builds require server-paged storage"),
                );
                self.publish_progressive_sub(&paged, &mut workspace, &definition)?;
            }
        }
        FTS_BUILD_APPEND_NANOS
            .fetch_add(stage.elapsed().as_nanos() as u64, AtomicOrdering::Relaxed);
        if let (Ok(after_pk), Some((last_pk, _))) = (
            std::env::var("BICDB_TEST_FTS_BUILD_ABORT_AFTER_PK"),
            blobs.last(),
        ) {
            if after_pk == *last_pk {
                return Err(BicDbError::Index(format!(
                    "injected full-text build abort after pk `{last_pk}` (test)"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn put_fts_stored_text_batch(
        &self,
        index: &str,
        first_document_id: u64,
        stored_text: &[Option<Vec<u8>>],
    ) -> Result<()> {
        let Some(_) = self.paged_records else {
            return Ok(());
        };
        for (chunk_index, chunk) in stored_text.chunks(4_096).enumerate() {
            let chunk_first = first_document_id + (chunk_index * 4_096) as u64;
            self.in_paged_transaction(|paged, xid| {
                for (offset, value) in chunk.iter().enumerate() {
                    if let Some(value) = value {
                        paged.put_full_text_stored_text(
                            xid,
                            index,
                            chunk_first + offset as u64,
                            value,
                        )?;
                    }
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    pub fn finish_full_text_tokenization(&self, index: &str) -> Result<()> {
        let Some(mut workspace) = crate::fts_build::FtsBuildWorkspace::open_existing(
            &self.path.join(DEFAULT_FTS_BUILD_DIR),
            index,
            self.config.fsync,
        )?
        else {
            return Err(BicDbError::Index(format!(
                "full-text build `{index}` is not prepared"
            )));
        };
        workspace.finish_tokenizing()
    }

    pub fn finish_full_text_tokenization_governed(
        &self,
        index: &str,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<()> {
        let permit = self.admit_index_build(governor, demand, now_ms)?;
        let result = self.finish_full_text_tokenization(index);
        drop(permit);
        result
    }

    pub(crate) fn effective_full_text_build_workers(&self) -> usize {
        self.config
            .fts_build_workers
            .min((self.config.fts_build_memory_bytes / (64 * 1024)).max(1))
            .max(1)
    }

    /// Explicitly abandon a resumable build and reclaim its unpublished
    /// generation. Completed runs are otherwise retained after failure so a
    /// repeated CREATE can resume.
    pub fn discard_full_text_build(&self, index: &str) -> Result<bool> {
        let Some(workspace) = crate::fts_build::FtsBuildWorkspace::open_existing(
            &self.path.join(DEFAULT_FTS_BUILD_DIR),
            index,
            self.config.fsync,
        )?
        else {
            return Ok(false);
        };
        let physical = workspace.physical_index().to_string();
        workspace.remove()?;
        if let Some(paged) = &self.paged_records {
            self.remove_physical_index_entries(paged, &physical)?;
        }
        Ok(true)
    }

    /// Finish an already-tokenized replacement generation. This is primarily
    /// the engine hook used by resumable rebuild orchestration; the currently
    /// published generation remains readable until this returns successfully.
    pub fn complete_prepared_full_text_build(&self, index: &str) -> Result<()> {
        let definition = self
            .indexes
            .get(index)
            .map(|state| state.read().definition.clone())
            .ok_or_else(|| BicDbError::Index(format!("index `{index}` not found")))?;
        if definition.kind != IndexKind::FullText {
            return Err(BicDbError::Index(format!(
                "index `{index}` is not full-text"
            )));
        }
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{index}` has no page store")))?;
        self.complete_external_full_text_build(paged, &definition)?;
        if paged.index_has_any_posting_blocks(&paged.latest_snapshot(), index)? {
            if let Some(state) = self.indexes.get(index) {
                state.write().paged_read_through = true;
            }
        }
        Ok(())
    }

    /// Return BicDB's authoritative lifecycle view for one FTS build.
    ///
    /// The report composes the durable build checkpoint with the generation
    /// that is actually serving. No external completion marker participates,
    /// so a stale supervisor file cannot make a completed build look active or
    /// an unpublished replacement look complete.
    pub fn full_text_build_lifecycle(&self, index: &str) -> Result<FullTextBuildLifecycleStatus> {
        use crate::fts_build::{FtsBuildPhase, FtsBuildWorkspace};

        let catalog = self.indexes.get(index).map(|state| {
            let state = state.read();
            (state.definition.kind.clone(), state.paged_read_through)
        });
        if catalog
            .as_ref()
            .is_some_and(|(kind, _)| *kind != IndexKind::FullText)
        {
            return Err(BicDbError::Index(format!(
                "index `{index}` is not full-text"
            )));
        }
        let published = match catalog.as_ref() {
            Some((IndexKind::FullText, true)) => Some(self.full_text_published_generation(index)?),
            _ => None,
        };
        let workspace = FtsBuildWorkspace::open_existing(
            &self.path.join(DEFAULT_FTS_BUILD_DIR),
            index,
            self.config.fsync,
        )?;

        let Some(workspace) = workspace else {
            let (state, reason_code) = if published.is_some() {
                (
                    FullTextBuildLifecycleState::Published,
                    "published_generation_active",
                )
            } else if catalog.is_some() {
                (
                    FullTextBuildLifecycleState::Idle,
                    "no_active_build_workspace",
                )
            } else {
                (FullTextBuildLifecycleState::NotFound, "index_not_found")
            };
            return Ok(FullTextBuildLifecycleStatus {
                schema_version: 1,
                logical_index: index.to_string(),
                state,
                reason_code: reason_code.to_string(),
                recommended_action: FullTextBuildRecommendedAction::None,
                serving: published.is_some(),
                resumable: false,
                checkpoint_updated_unix_ms: None,
                staged_physical_index: None,
                progressive_documents_staged: 0,
                progress: None,
                published,
            });
        };

        let checkpoint_updated_unix_ms = workspace.checkpoint_updated_unix_ms();
        let staged_physical_index = Some(workspace.physical_index().to_string());
        let progressive_documents_staged = workspace.checkpoint().progressive_docs;
        let row_backfill = workspace.source_signature().starts_with("core:");
        let (state, reason_code, recommended_action) = if catalog.is_none() {
            (
                FullTextBuildLifecycleState::Blocked,
                "catalog_definition_missing",
                FullTextBuildRecommendedAction::None,
            )
        } else {
            match workspace.phase() {
                FtsBuildPhase::Tokenizing if row_backfill => (
                    FullTextBuildLifecycleState::Tokenizing,
                    "row_backfill_resumable",
                    FullTextBuildRecommendedAction::Reconcile,
                ),
                FtsBuildPhase::Tokenizing => (
                    FullTextBuildLifecycleState::AwaitingInput,
                    "direct_ingest_awaiting_documents_or_finish",
                    FullTextBuildRecommendedAction::AppendDocuments,
                ),
                FtsBuildPhase::MergePk | FtsBuildPhase::MergeImpact => (
                    FullTextBuildLifecycleState::Finalizing,
                    "durable_tokenization_ready_to_finalize",
                    FullTextBuildRecommendedAction::Reconcile,
                ),
                FtsBuildPhase::Publishing => (
                    FullTextBuildLifecycleState::Publishing,
                    "publication_resume_required",
                    FullTextBuildRecommendedAction::Reconcile,
                ),
                FtsBuildPhase::Complete => (
                    FullTextBuildLifecycleState::Published,
                    "published_generation_active",
                    FullTextBuildRecommendedAction::None,
                ),
            }
        };
        let progress = self.full_text_build_progress(index)?;
        Ok(FullTextBuildLifecycleStatus {
            schema_version: 1,
            logical_index: index.to_string(),
            state,
            reason_code: reason_code.to_string(),
            recommended_action,
            serving: published.is_some(),
            resumable: state != FullTextBuildLifecycleState::Blocked,
            checkpoint_updated_unix_ms,
            staged_physical_index,
            progressive_documents_staged,
            progress,
            published,
        })
    }

    /// Discover every catalogued or checkpointed FTS lifecycle.
    ///
    /// Including checkpoint-only names is important: a damaged or
    /// interrupted catalog publish must appear as `blocked`, not disappear
    /// from an operator's fleet inventory.
    pub fn full_text_build_lifecycles(&self) -> Result<Vec<FullTextBuildLifecycleStatus>> {
        let mut names = self
            .indexes
            .iter()
            .filter_map(|(name, state)| {
                (state.read().definition.kind == IndexKind::FullText).then(|| name.clone())
            })
            .collect::<Vec<_>>();
        names.extend(crate::fts_build::FtsBuildWorkspace::existing_indexes(
            &self.path.join(DEFAULT_FTS_BUILD_DIR),
        )?);
        names.sort();
        names.dedup();
        names
            .into_iter()
            .map(|name| self.full_text_build_lifecycle(&name))
            .collect()
    }

    /// Reconcile one interrupted FTS build from BicDB's durable facts.
    ///
    /// Repeated calls are safe. Row-backed tokenization advances by at most
    /// `budget_documents`; direct/sealed ingestion remains awaiting caller
    /// input until the caller explicitly finishes tokenization. Once durable
    /// tokenization is complete, reconciliation resumes merge/publication and
    /// atomically swaps the serving generation.
    pub fn reconcile_full_text_build(
        &mut self,
        index: &str,
        budget_documents: u64,
    ) -> Result<FullTextBuildReconcileReport> {
        use crate::fts_build::{FtsBuildPhase, FtsBuildWorkspace};

        let before = self.full_text_build_lifecycle(index)?;
        let root = self.path.join(DEFAULT_FTS_BUILD_DIR);
        let Some(workspace) = FtsBuildWorkspace::open_existing(&root, index, self.config.fsync)?
        else {
            let outcome = if before.state == FullTextBuildLifecycleState::Published {
                FullTextBuildReconcileOutcome::AlreadyPublished
            } else {
                FullTextBuildReconcileOutcome::NoBuild
            };
            return Ok(FullTextBuildReconcileReport {
                outcome,
                documents_indexed: 0,
                after: before.clone(),
                before,
            });
        };
        if before.state == FullTextBuildLifecycleState::Blocked {
            return Ok(FullTextBuildReconcileReport {
                outcome: FullTextBuildReconcileOutcome::Blocked,
                documents_indexed: 0,
                after: before.clone(),
                before,
            });
        }
        if workspace.phase() == FtsBuildPhase::Tokenizing
            && !workspace.source_signature().starts_with("core:")
        {
            return Ok(FullTextBuildReconcileReport {
                outcome: FullTextBuildReconcileOutcome::AwaitingInput,
                documents_indexed: 0,
                after: before.clone(),
                before,
            });
        }
        let phase = workspace.phase();
        drop(workspace);

        self.ensure_writable("reconcile full-text build")?;
        let mut documents_indexed = 0;
        let outcome = if phase == FtsBuildPhase::Tokenizing {
            let definition = self
                .indexes
                .get(index)
                .map(|state| state.read().definition.clone())
                .ok_or_else(|| BicDbError::Index(format!("index `{index}` not found")))?;
            match self.full_text_build_step(definition, budget_documents.max(1))? {
                FullTextBuildStep::InProgress {
                    documents_indexed: count,
                } => {
                    documents_indexed = count;
                    FullTextBuildReconcileOutcome::Progressed
                }
                FullTextBuildStep::Complete => FullTextBuildReconcileOutcome::Published,
            }
        } else {
            self.complete_prepared_full_text_build(index)?;
            FullTextBuildReconcileOutcome::Published
        };
        let after = self.full_text_build_lifecycle(index)?;
        Ok(FullTextBuildReconcileReport {
            outcome,
            documents_indexed,
            before,
            after,
        })
    }

    /// Resource-governed variant of [`Self::reconcile_full_text_build`] for
    /// long-running supervisors and multi-tenant serving processes.
    pub fn reconcile_full_text_build_governed(
        &mut self,
        index: &str,
        budget_documents: u64,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<FullTextBuildReconcileReport> {
        let permit = self.admit_index_build(governor, demand, now_ms)?;
        let result = self.reconcile_full_text_build(index, budget_documents);
        drop(permit);
        result
    }

    pub fn complete_prepared_full_text_build_governed(
        &self,
        index: &str,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<()> {
        let permit = self.admit_index_build(governor, demand, now_ms)?;
        let result = self.complete_prepared_full_text_build(index);
        drop(permit);
        result
    }

    pub(crate) fn admit_index_build(
        &self,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<ResourcePermit> {
        let configured_memory = u64::try_from(self.config.fts_build_memory_bytes)
            .map_err(|_| BicDbError::ResourceGovernance("FTS memory bound is invalid".into()))?;
        if demand.memory_bytes < configured_memory {
            return Err(BicDbError::ResourceGovernance(format!(
                "index-build demand declares {} resident bytes but the configured builder may use {configured_memory}",
                demand.memory_bytes
            )));
        }
        governor.try_admit(ResourceLane::IndexBuild, demand, now_ms)
    }

    /// Encode a doc-terms blob from a projection's parts (session-side
    /// CREATE INDEX pre-pass; terms MUST be sorted by term).
    pub fn encode_fts_doc_terms(
        doc_length: u32,
        doc_distinct: u32,
        terms: &[(String, Vec<u16>)],
    ) -> Vec<u8> {
        crate::paged_collection::encode_doc_terms(doc_length, doc_distinct, terms)
    }

    /// `(indexed_rows, distinct_terms)` of a read-through full-text index,
    /// from the durable keyspaces: rows = doc-terms blobs + distinct tail
    /// pks not covered by a blob; terms = distinct tail terms + block terms.
    pub(crate) fn read_through_fts_statistics(&self, name: &str) -> Result<(usize, usize)> {
        let Some(paged) = &self.paged_records else {
            return Ok((0, 0));
        };
        let snapshot = paged.latest_snapshot();
        if let Some(statistics) = paged.full_text_collection_statistics(&snapshot, name)? {
            return Ok((
                statistics.document_count as usize,
                statistics.term_count as usize,
            ));
        }
        let mut pks: FxHashSet<String> = FxHashSet::default();
        for entry in paged.scan_doc_terms(&snapshot, name)? {
            let (pk, _) = entry?;
            pks.insert(pk);
        }
        let mut terms: FxHashSet<Vec<u8>> = FxHashSet::default();
        for entry in paged.scan_index(&snapshot, name)? {
            let (encoded_key, pk) = entry?;
            terms.insert(encoded_key);
            pks.insert(pk);
        }
        for entry in paged.scan_all_posting_blocks(&snapshot, name)? {
            let (encoded_key, _) = entry?;
            terms.insert(encoded_key);
        }
        for entry in paged.scan_all_numeric_posting_blocks(&snapshot, name)? {
            let (encoded_key, _, _) = entry?;
            terms.insert(encoded_key);
        }
        Ok((pks.len(), terms.len()))
    }

    /// `(indexed rows, distinct keys)` for a read-through B-tree index,
    /// counted from the durable keyspace: the resident store is empty, so
    /// planner statistics must come from the entries themselves. One bounded
    /// namespace walk at the latest snapshot; ANALYZE-frequency cost.
    pub(crate) fn read_through_btree_statistics(&self, name: &str) -> Result<(usize, usize)> {
        let Some(paged) = &self.paged_records else {
            return Ok((0, 0));
        };
        let snapshot = paged.latest_snapshot();
        let mut entries = 0usize;
        let mut distinct_keys = 0usize;
        let mut previous_key: Option<Vec<u8>> = None;
        for entry in paged.scan_index(&snapshot, name)? {
            let (encoded_key, _) = entry?;
            entries += 1;
            // Entries sort by (encoded key, pk), so distinct keys are the
            // boundaries of equal-key runs — no hash set proportional to the
            // key universe.
            if previous_key.as_deref() != Some(encoded_key.as_slice()) {
                distinct_keys += 1;
                previous_key = Some(encoded_key);
            }
        }
        Ok((entries, distinct_keys))
    }

    pub(crate) fn backfill_paged_index_entries(&self, definition: &IndexDefinition) -> Result<()> {
        if !matches!(definition.kind, IndexKind::BTree | IndexKind::FullText) {
            return Ok(());
        }
        let Some(paged) = &self.paged_records else {
            return Ok(());
        };
        let paged = Arc::clone(paged);
        if paged_v3_entries_enabled() && definition.kind == IndexKind::BTree {
            let (xid, _) = paged.begin();
            paged.set_index_entry_format(
                xid,
                &definition.name,
                &definition.collection,
                crate::paged_collection::IndexEntryFormat::V3,
            )?;
            paged.commit(xid)?;
        }
        let entry_format = paged.entry_format_cached(&definition.name)?;
        let defs = [PagedDurableIndexDef {
            name: definition.name.clone(),
            fields: definition.fields.clone(),
            kind: definition.kind.clone(),
            predicate: definition.predicate.clone(),
            entry_format,
            // Backfill of pre-existing rows: duplicates among them are the
            // index creation's validation error, not an apply abort.
            unique: false,
        }];
        // Full-text backfill goes DIRECT TO BLOCKS: per-posting v1/v2 entries
        // cost ~20x the block representation in disk and force a fold + GC
        // chain to reclaim. Only legacy array projections (no positions) still
        // take the per-posting path below.
        if definition.kind == IndexKind::FullText
            && self.backfill_full_text_direct_blocks(&paged, definition, &defs[0])?
        {
            return Ok(());
        }
        let snapshot = paged.latest_snapshot();
        let rows: Vec<(Record, bicdb_page::TupleLocator, bicdb_page::Xid)> = paged
            .scan_with_heads(&snapshot, &definition.collection)?
            .collect::<Result<Vec<_>>>()?;
        self.in_paged_transaction(|paged, xid| {
            for (record, head, head_xmin) in &rows {
                apply_paged_index_upsert(
                    paged,
                    xid,
                    &definition.collection,
                    &defs,
                    None,
                    record,
                    Some((*head, *head_xmin)),
                )?;
            }
            // Full FTS backfill => the v2 impact ordering now covers every
            // document; the sentinel is what authorizes early termination.
            if definition.kind == IndexKind::FullText {
                paged.put_index_v2_sentinel(xid, &definition.name)?;
            }
            Ok(())
        })?;
        Ok(())
    }

    /// Build a full-text index through bounded, restartable external posting
    /// runs. Both posting orders are sorted on temporary disk, merged into an
    /// immutable physical generation, and atomically promoted.
    ///
    /// Returns `Ok(false)` — after removing anything it wrote — when the
    /// projection turns out to be a legacy term array (payload-less postings
    /// carry no positions, and blocks cannot represent "no payload"): the
    /// caller falls back to the per-posting path.
    #[allow(unreachable_code)]
    pub(crate) fn backfill_full_text_direct_blocks(
        &self,
        paged: &Arc<crate::paged_collection::PagedRecords>,
        definition: &IndexDefinition,
        def: &PagedDurableIndexDef,
    ) -> Result<bool> {
        return Ok(matches!(
            self.prepare_and_complete_external_full_text_build(paged, definition, def, None)?,
            ExternalFullTextBuildOutcome::Complete
        ));
        const CHUNK_ROWS: usize = 8_192;
        const CARRY_BUDGET_POSTINGS: usize = 750_000;
        let chunk_rows = std::env::var("BICDB_FTS_BACKFILL_CHUNK_ROWS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|&value| value > 0)
            .unwrap_or(CHUNK_ROWS);
        let carry_budget = std::env::var("BICDB_FTS_BACKFILL_CARRY_BUDGET")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|&value| value > 0)
            .unwrap_or(CARRY_BUDGET_POSTINGS);
        let fields = full_text_fields(def)?;
        let name = definition.name.as_str();
        // A crashed earlier backfill (or a REINDEX) may have left blocks
        // behind; appending onto them would duplicate postings.
        self.remove_direct_blocks(paged, name)?;
        // Reclaim before building: the projection materialization that runs
        // just before this backfill rewrote every row, leaving the whole
        // pre-materialization corpus as dead versions. Vacuum frees those
        // fully-dead pages to the free list, and the block writes below are
        // then absorbed by the holes instead of growing the file — this is
        // what keeps CREATE INDEX from doubling the store on bulk imports.
        // (Checkpoint first bounds the WAL; checkpoint last truncates any
        // trailing free run.)
        let stage = Instant::now();
        paged.checkpoint()?;
        fts_build_trace(name, "preamble-checkpoint", stage);
        let stage = Instant::now();
        vacuum_paged_records_to_completion(paged.as_ref())?;
        fts_build_trace(name, "preamble-vacuum", stage);
        let stage = Instant::now();
        paged.sweep_dead_index_entries(usize::MAX)?;
        fts_build_trace(name, "preamble-sweep", stage);
        let stage = Instant::now();
        paged.checkpoint()?;
        fts_build_trace(name, "preamble-checkpoint2", stage);

        let mut carry: std::collections::BTreeMap<
            Vec<u8>,
            Vec<crate::paged_collection::BlockPosting>,
        > = std::collections::BTreeMap::new();
        let mut carried = 0usize;
        let mut legacy = false;
        let mut flushes = 0usize;
        let snapshot = paged.latest_snapshot();
        let mut push_doc = |carry: &mut std::collections::BTreeMap<
            Vec<u8>,
            Vec<crate::paged_collection::BlockPosting>,
        >,
                            carried: &mut usize,
                            pk: &str,
                            doc_length: u32,
                            doc_distinct: u32,
                            term: &str,
                            packed: Vec<u16>| {
            carry
                .entry(full_text_term_entry_key(term))
                .or_default()
                .push(crate::paged_collection::BlockPosting {
                    pk: pk.to_string(),
                    doc_length,
                    doc_distinct,
                    packed_positions: packed,
                });
            *carried += 1;
        };
        if paged.index_has_doc_terms(&snapshot, name)? {
            // The CREATE INDEX pre-pass wrote a doc-terms blob per row: the
            // corpus is those compact blobs in pk order, not the rows —
            // no row is read, let alone rewritten. Chunked resume so block
            // commits never race an open cursor.
            let mut resume: Option<String> = None;
            loop {
                let mut chunk: Vec<(String, Vec<u8>)> = Vec::with_capacity(chunk_rows);
                {
                    let iterator: Box<dyn Iterator<Item = Result<(String, Vec<u8>)>>> =
                        match &resume {
                            None => Box::new(paged.scan_doc_terms(&snapshot, name)?),
                            Some(after) => {
                                Box::new(paged.scan_doc_terms_after(&snapshot, name, after)?)
                            }
                        };
                    for entry in iterator.take(chunk_rows) {
                        chunk.push(entry?);
                    }
                }
                if chunk.is_empty() {
                    break;
                }
                resume = chunk.last().map(|(pk, _)| pk.clone());
                let exhausted = chunk.len() < chunk_rows;
                for (pk, blob) in &chunk {
                    let (doc_length, doc_distinct, terms) =
                        crate::paged_collection::decode_doc_terms(blob)?;
                    for (term, positions) in terms {
                        push_doc(
                            &mut carry,
                            &mut carried,
                            pk,
                            doc_length,
                            doc_distinct,
                            &term,
                            positions,
                        );
                    }
                }
                let freeze_partials = carried > carry_budget;
                carried -= self.flush_direct_block_runs(name, &mut carry, freeze_partials)?;
                flushes += 1;
                if flushes == 1
                    && std::env::var("BICDB_TEST_FTS_BACKFILL_ABORT").as_deref() == Ok(name)
                {
                    return Err(BicDbError::Index(format!(
                        "injected backfill abort for `{name}` (test)"
                    )));
                }
                if exhausted {
                    break;
                }
            }
        } else {
            paged.for_each_batch(&snapshot, &definition.collection, chunk_rows, |batch| {
                for record in &batch {
                    for (term, payload) in record_full_text_postings(record, fields)? {
                        let Some((doc_length, doc_distinct, packed)) =
                            decode_fts_posting_payload(&payload)
                        else {
                            legacy = true;
                            return Ok(false);
                        };
                        push_doc(
                            &mut carry,
                            &mut carried,
                            &record.id,
                            doc_length,
                            doc_distinct,
                            &term,
                            packed,
                        );
                    }
                }
                let freeze_partials = carried > carry_budget;
                carried -= self.flush_direct_block_runs(name, &mut carry, freeze_partials)?;
                flushes += 1;
                // Crash-window fault injection: die after the first chunk's
                // blocks are durably committed but before the sentinel.
                if flushes == 1
                    && std::env::var("BICDB_TEST_FTS_BACKFILL_ABORT").as_deref() == Ok(name)
                {
                    return Err(BicDbError::Index(format!(
                        "injected backfill abort for `{name}` (test)"
                    )));
                }
                Ok(true)
            })?;
        }
        if legacy {
            // Undo any blocks a mixed corpus let through before the first
            // payload-less posting surfaced.
            self.remove_direct_blocks(paged, name)?;
            return Ok(false);
        }
        self.flush_direct_block_runs(name, &mut carry, true)?;
        drop(carry);

        // Second pass: the impact-ordered v5 copy, term by term, read from the
        // compressed pk-ordered blocks just written. Same output as the fold's
        // impact stage. Term keys are collected first so no cursor is held
        // open across the write transactions.
        let snapshot = paged.latest_snapshot();
        let mut term_keys: Vec<Vec<u8>> = Vec::new();
        for entry in paged.scan_all_posting_blocks(&snapshot, name)? {
            let (encoded_key, _) = entry?;
            if term_keys
                .last()
                .map(|last| last != &encoded_key)
                .unwrap_or(true)
            {
                term_keys.push(encoded_key);
            }
        }
        for encoded_key in &term_keys {
            let mut postings: Vec<crate::paged_collection::BlockPosting> = Vec::new();
            for block in paged.scan_posting_blocks(&snapshot, name, encoded_key)? {
                let (_, bytes) = block?;
                let (decoded, _) = crate::paged_collection::decode_posting_block(&bytes)?;
                postings.extend(decoded);
            }
            postings.sort_by(|left, right| {
                fts_impact_bucket(&right.packed_positions)
                    .cmp(&fts_impact_bucket(&left.packed_positions))
                    .then_with(|| left.pk.cmp(&right.pk))
            });
            let impact_blocks =
                build_posting_blocks(&postings, FTS_BLOCK_DOC_CAP, FTS_BLOCK_BYTE_TARGET);
            self.in_paged_transaction(|paged, xid| {
                for (seq, (_, block)) in impact_blocks.iter().enumerate() {
                    paged.put_impact_block(xid, name, encoded_key, seq as u32, block)?;
                }
                Ok(())
            })?;
        }
        // The blocks now cover every document; the sentinel authorizes
        // early-terminating impact scans exactly as a per-posting backfill
        // would have.
        self.in_paged_transaction(|paged, xid| paged.put_index_v2_sentinel(xid, name))?;
        Ok(true)
    }

    /// Run one bounded slice of the external full-text build for
    /// `definition`, registering the index on the first call. Combined with
    /// `fts_progressive`, this is the online-build loop: each step ingests
    /// up to `budget_documents`, sub-segments publish on their interval and
    /// become searchable, and the caller runs queries between steps. The
    /// final step performs the merge and atomic publish; the finished index
    /// is identical to one built by `create_index`.
    pub fn full_text_build_step(
        &mut self,
        definition: IndexDefinition,
        budget_documents: u64,
    ) -> Result<FullTextBuildStep> {
        self.ensure_writable("full-text build step")?;
        if definition.kind != IndexKind::FullText {
            return Err(BicDbError::Index(format!(
                "index `{}` is not full-text",
                definition.name
            )));
        }
        if !self.is_server_paged() {
            return Err(BicDbError::Index(
                "stepped full-text builds require server-paged storage".to_string(),
            ));
        }
        if !self.indexes.contains_key(&definition.name) {
            self.index_generation
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.indexes.insert(
                definition.name.clone(),
                RwLock::new(IndexState {
                    definition: definition.clone(),
                    store: new_index_store(),
                    spatial: None,
                    packed_spatial: None,
                    spatial_tombstones: FxHashSet::default(),
                    spatial_delta_durable: false,
                    paged_read_through: false,
                    full_text_build_incomplete: false,
                }),
            );
            if let Err(error) = self.persist_index_catalog() {
                self.index_generation
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.indexes.remove(&definition.name);
                return Err(error);
            }
        }
        let paged = Arc::clone(self.paged_records.as_ref().expect("server-paged checked"));
        let entry_format = paged.entry_format_cached(&definition.name)?;
        let def = PagedDurableIndexDef {
            name: definition.name.clone(),
            fields: definition.fields.clone(),
            kind: definition.kind.clone(),
            predicate: definition.predicate.clone(),
            entry_format,
            unique: false,
        };
        match self.prepare_and_complete_external_full_text_build(
            &paged,
            &definition,
            &def,
            Some(budget_documents.max(1)),
        )? {
            ExternalFullTextBuildOutcome::Paused { documents_indexed } => {
                Ok(FullTextBuildStep::InProgress { documents_indexed })
            }
            ExternalFullTextBuildOutcome::Complete => {
                let snapshot = paged.latest_snapshot();
                if paged.index_has_any_posting_blocks(&snapshot, &definition.name)? {
                    if let Some(state) = self.indexes.get(&definition.name) {
                        state.write().paged_read_through = true;
                    }
                }
                self.schema_compatibility.invalidate();
                Ok(FullTextBuildStep::Complete)
            }
            ExternalFullTextBuildOutcome::LegacyFallback => Err(BicDbError::Index(
                "this projection predates block postings and cannot be step-built; \
                 use CREATE INDEX"
                    .to_string(),
            )),
        }
    }

    pub(crate) fn prepare_and_complete_external_full_text_build(
        &self,
        paged: &Arc<crate::paged_collection::PagedRecords>,
        definition: &IndexDefinition,
        def: &PagedDurableIndexDef,
        step_budget_documents: Option<u64>,
    ) -> Result<ExternalFullTextBuildOutcome> {
        use crate::fts_build::{FtsBuildPhase, FtsBuildWorkspace};

        let root = self.path.join(DEFAULT_FTS_BUILD_DIR);
        let mut workspace =
            match FtsBuildWorkspace::open_existing(&root, &definition.name, self.config.fsync)? {
                Some(workspace) => workspace,
                None => FtsBuildWorkspace::open_or_create(
                    &root,
                    &definition.name,
                    &definition.collection,
                    &format!("core:{:?}", def.fields),
                    self.config.fsync,
                )?,
            };
        if workspace.checkpoint().progressive_subs > 0
            && workspace.checkpoint().progressive_old_generation.is_some()
        {
            // A resumed first progressive build has already made its partial
            // generation visible because no older generation existed. A
            // replacement records no displaced generation and remains
            // staging-only until the final atomic publish.
            let resumed_physical = workspace.physical_index().to_string();
            paged.register_fts_segment(&resumed_physical)?;
            self.fts_block_cache.purge_generation(&resumed_physical);
            self.publish_full_text_generation(&definition.name, &resumed_physical)?;
            if let Some(state) = self.indexes.get(&definition.name) {
                state.write().paged_read_through = true;
            }
        }
        if workspace.phase() == FtsBuildPhase::Tokenizing {
            let fields = full_text_fields(def)?;
            let snapshot = paged.latest_snapshot();
            let batch_rows = (self.config.fts_build_memory_bytes / (64 * 1024)).clamp(64, 8_192);
            // A stepped build must be able to stop near its budget: batches
            // are its quantum.
            let batch_rows = match step_budget_documents {
                Some(budget) => batch_rows
                    .min(usize::try_from(budget).unwrap_or(batch_rows))
                    .max(1),
                None => batch_rows,
            };
            let resume = workspace.checkpoint().last_pk.clone();
            let workers = self.effective_full_text_build_workers();
            let phase_started = Instant::now();
            let mut legacy = false;
            let mut paused = false;
            let mut stepped_documents = 0u64;
            // The batch consumer: parallel tokenize, then the SEQUENTIAL
            // tail (doc-terms blobs, document ids, run append + checkpoint)
            // exactly as before — crash-resume semantics are unchanged.
            let mut consume_batch =
                |workspace: &mut FtsBuildWorkspace, batch: Vec<Record>| -> Result<bool> {
                    let batch_documents = batch.len();
                    let Some(blobs) = tokenize_full_text_batch(&batch, fields, workers)? else {
                        legacy = true;
                        return Ok(false);
                    };
                    let physical = workspace.physical_index().to_string();
                    let first_document_id = workspace.checkpoint().document_count;
                    // No doc-terms blobs for row-backed builds. The blob was
                    // the reverse mapping for UPDATE/DELETE retraction, but
                    // `full_text_diff_postings` derives the same postings
                    // from the row itself (projection object, pre-tokenized
                    // array, or raw text through the same tokenizer this
                    // build just used) — the blob arm was unreachable for
                    // any row whose field is present, and an absent field
                    // indexed nothing, so there is nothing to retract.
                    // Direct-document ingestion keeps its blobs: those terms
                    // are caller-tokenized and no row exists to rederive
                    // them from. Measured on the retained corpus the blobs
                    // were 0.82 GB — a fifth of the whole database — spent
                    // on a copy of information the rows already hold.
                    self.put_fts_document_id_batch(&physical, first_document_id, &blobs)?;
                    workspace.append_document_batch(
                        &blobs,
                        self.config.fts_build_memory_bytes / 2,
                        workers,
                    )?;
                    if self.config.fts_progressive && self.config.fts_packed_segments {
                        let ready = workspace.checkpoint().document_count
                            - workspace.checkpoint().progressive_docs
                            >= self.config.fts_progressive_interval_docs;
                        if ready {
                            self.publish_progressive_sub(paged, workspace, definition)?;
                        }
                    }
                    stepped_documents += batch_documents as u64;
                    if let Some(budget) = step_budget_documents {
                        if stepped_documents >= budget {
                            paused = true;
                            return Ok(false);
                        }
                    }
                    Ok(true)
                };
            if std::env::var("BICDB_FTS_BUILD_SERIAL").as_deref() == Ok("1") {
                paged.for_each_batch_after(
                    &snapshot,
                    &definition.collection,
                    batch_rows,
                    resume.as_deref(),
                    |batch| consume_batch(&mut workspace, batch),
                )?;
            } else {
                // Pipelined scan: a producer thread reads batches ahead
                // through a bounded channel while this thread tokenizes and
                // appends, and a read-ahead pump services the next-leaf /
                // overflow-chain requests the scan cursor enqueues — without
                // it those requests rot in the queue (nothing drives
                // `read_ahead_step` inside an embedded build) and every page
                // is a synchronous foreground read. Together they overlap
                // page I/O with tokenization instead of alternating them.
                let (sender, receiver) = std::sync::mpsc::sync_channel::<Vec<Record>>(2);
                let pump_stop = AtomicU8::new(0);
                let scan_paged = Arc::clone(&paged);
                let pump_paged = Arc::clone(&paged);
                let scan_snapshot = snapshot.clone();
                let collection = definition.collection.clone();
                let (consumed, scanned) = std::thread::scope(|scope| {
                    let pump_stop = &pump_stop;
                    let pump = scope.spawn(move || {
                        // Registering makes the pool accept the scan's
                        // speculative requests for exactly as long as this
                        // pump is alive to load them.
                        let _driver = ReadAheadDriverRegistration::new(Arc::clone(&pump_paged));
                        while pump_stop.load(AtomicOrdering::Acquire) == 0 {
                            match pump_paged.read_ahead_step(crate::ReadAheadLimits::default()) {
                                Ok(report) if report.pages_loaded == 0 => {
                                    std::thread::sleep(Duration::from_micros(250));
                                }
                                Ok(_) => {}
                                // Admission declines and transient read
                                // errors surface on the foreground read
                                // path if real; the pump only backs off.
                                Err(_) => std::thread::sleep(Duration::from_millis(5)),
                            }
                        }
                    });
                    let scan = scope.spawn(move || {
                        scan_paged.for_each_locality_batch_after_cancellable(
                            &scan_snapshot,
                            &collection,
                            batch_rows,
                            resume.as_deref(),
                            &CancellationToken::uncancelable(),
                            // A closed channel means the consumer stopped
                            // (legacy bail or error): end the scan cleanly.
                            |batch| Ok(sender.send(batch).is_ok()),
                        )
                    });
                    let consumed = (|| -> Result<()> {
                        for batch in &receiver {
                            if !consume_batch(&mut workspace, batch)? {
                                break;
                            }
                        }
                        Ok(())
                    })();
                    // Unblock a producer parked on a full channel, then join
                    // BOTH threads before propagating any error.
                    drop(receiver);
                    let scanned = scan.join().map_err(|_| {
                        BicDbError::Index("full-text build scan thread panicked".to_string())
                    });
                    pump_stop.store(1, AtomicOrdering::Release);
                    let _ = pump.join();
                    (consumed, scanned)
                });
                consumed?;
                scanned??;
            }
            fts_build_trace(&definition.name, "scan+tokenize", phase_started);
            if legacy {
                let physical = workspace.physical_index().to_string();
                workspace.remove()?;
                self.remove_physical_index_entries(paged, &physical)?;
                return Ok(ExternalFullTextBuildOutcome::LegacyFallback);
            }
            if paused {
                // The step budget ran out mid-tokenize. Everything this step
                // ingested is checkpointed; under the progressive flag its
                // published sub-segments are already serving queries.
                return Ok(ExternalFullTextBuildOutcome::Paused {
                    documents_indexed: stepped_documents,
                });
            }
            let finish_started = Instant::now();
            workspace.finish_tokenizing()?;
            fts_build_trace(&definition.name, "finish_tokenizing", finish_started);
        }
        drop(workspace);
        if self.complete_external_full_text_build(paged, definition)? {
            Ok(ExternalFullTextBuildOutcome::Complete)
        } else {
            Ok(ExternalFullTextBuildOutcome::LegacyFallback)
        }
    }

    pub(crate) fn complete_external_full_text_build(
        &self,
        paged: &Arc<crate::paged_collection::PagedRecords>,
        definition: &IndexDefinition,
    ) -> Result<bool> {
        use crate::fts_build::{FtsBuildPhase, FtsBuildWorkspace, RunOrder};

        let root = self.path.join(DEFAULT_FTS_BUILD_DIR);
        let mut workspace =
            FtsBuildWorkspace::open_existing(&root, &definition.name, self.config.fsync)?
                .ok_or_else(|| {
                    BicDbError::Index(format!(
                        "full-text build `{}` has no checkpoint",
                        definition.name
                    ))
                })?;
        if workspace.phase() == FtsBuildPhase::Tokenizing {
            return Err(BicDbError::Index(format!(
                "full-text build `{}` has not finished tokenizing",
                definition.name
            )));
        }
        let physical = workspace.physical_index().to_string();

        if workspace.phase() == FtsBuildPhase::MergeImpact {
            // A workspace checkpointed by the two-pass era: its impact runs
            // are gone from the ingest, so resume by redoing the single-pass
            // merge — idempotent (namespaces cleared, segment files
            // truncated on begin).
            workspace.set_phase(FtsBuildPhase::MergePk)?;
        }
        if workspace.phase() == FtsBuildPhase::MergePk {
            let phase_started = Instant::now();
            self.remove_physical_block_namespace(paged, &physical, RunOrder::Pk)?;
            self.remove_physical_block_namespace(paged, &physical, RunOrder::Impact)?;
            self.merge_external_posting_blocks(&workspace, &physical, RunOrder::Pk)?;
            fts_build_trace(&definition.name, "merge", phase_started);
            workspace.set_phase(FtsBuildPhase::Publishing)?;
            let legacy_abort = std::env::var("BICDB_TEST_FTS_BACKFILL_ABORT").as_deref()
                == Ok(definition.name.as_str());
            if legacy_abort {
                return Err(BicDbError::Index(format!(
                    "injected backfill abort for `{}` (test)",
                    definition.name
                )));
            }
            if test_fts_build_abort_matches(&definition.name, "merge_pk") {
                return Err(BicDbError::Index(format!(
                    "injected full-text build abort after pk merge for `{}` (test)",
                    definition.name
                )));
            }
            if test_fts_build_abort_matches(&definition.name, "merge_impact") {
                return Err(BicDbError::Index(format!(
                    "injected full-text build abort after impact merge for `{}` (test)",
                    definition.name
                )));
            }
        }
        if workspace.phase() == FtsBuildPhase::Publishing {
            let publish_started = Instant::now();
            if self.config.fts_packed_segments {
                // Publish the packed segment BEFORE the generation flip: the
                // alias still points at the old generation, so the segment is
                // invisible until the flip makes it authoritative. An empty
                // build (index-first, no rows) removes its empty segment so
                // an index-first index behaves exactly as before.
                let root = paged.fts_segments_root().to_path_buf();
                let term_count =
                    crate::fts_segment::publish_segment(&root, &physical, self.config.fsync)?;
                if term_count > 0 {
                    paged.register_fts_segment(&physical)?;
                } else {
                    paged.drop_fts_segment(&physical);
                }
            }
            let snapshot = paged.latest_snapshot();
            let initial_empty = workspace.checkpoint().next_run == 0
                && !self
                    .fts_generations
                    .lock()
                    .indexes
                    .contains_key(&definition.name)
                && !paged.index_has_entries(&snapshot, &definition.name)?
                && !paged.index_has_any_posting_blocks(&snapshot, &definition.name)?
                && !paged.index_has_doc_terms(&snapshot, &definition.name)?;
            let old = if initial_empty {
                // Keep an index-first empty index in its logical namespace.
                // Subsequent transactional inserts then remain compatible
                // with pre-generation databases and raw page-store tooling.
                self.in_paged_transaction(|paged, xid| {
                    paged.put_index_v2_sentinel(xid, &definition.name)
                })?;
                None
            } else {
                self.in_paged_transaction(|paged, xid| {
                    paged.put_index_v2_sentinel(xid, &physical)
                })?;
                self.publish_full_text_generation(&definition.name, &physical)?
            };
            // A progressive build already flipped at its first sub; the true
            // displaced generation was recorded then. Delete the disposable
            // sub-segments only now, after the final manifest made the
            // single segment authoritative and the registry re-opened it.
            let progressive_old = workspace
                .checkpoint()
                .progressive_old_generation
                .clone()
                .filter(|old| old != &physical);
            if workspace.checkpoint().progressive_subs > 0 {
                crate::fts_segment::remove_progressive_subs(paged.fts_segments_root(), &physical);
                paged.register_fts_segment(&physical)?;
                self.fts_block_cache.purge_generation(&physical);
            }
            workspace.set_phase(FtsBuildPhase::Complete)?;
            workspace.remove()?;
            if let Some(old) = old.filter(|old| old != &physical).or(progressive_old) {
                // Publication already succeeded. Old-generation reclamation
                // is deliberately after the swap, so a cleanup interruption
                // can waste disk but can never make the logical index invalid.
                let _ = self.remove_physical_index_entries(paged, &old);
            }
            fts_build_trace(&definition.name, "publish", publish_started);
            if std::env::var("BICDB_FTS_BUILD_TRACE").as_deref() == Ok("1") {
                eprintln!(
                    "fts-build {}: batch stages — workspace-open {:.2}s, blob-puts {:.2}s, run-append {:.2}s",
                    definition.name,
                    FTS_BUILD_OPEN_NANOS.load(AtomicOrdering::Relaxed) as f64 / 1e9,
                    FTS_BUILD_PUT_NANOS.load(AtomicOrdering::Relaxed) as f64 / 1e9,
                    FTS_BUILD_APPEND_NANOS.load(AtomicOrdering::Relaxed) as f64 / 1e9,
                );
            }
        }
        Ok(true)
    }

    /// Publish one progressive sub-segment covering the runs spilled since
    /// the previous one, and — on the first — flip the index visible.
    /// Partial coverage is the FEATURE: the index answers over everything
    /// ingested so far, hours before the final merge, and the final publish
    /// swaps in the ordinary single segment and deletes the subs.
    pub(crate) fn publish_progressive_sub(
        &self,
        paged: &Arc<crate::paged_collection::PagedRecords>,
        workspace: &mut crate::fts_build::FtsBuildWorkspace,
        definition: &IndexDefinition,
    ) -> Result<()> {
        let physical = workspace.physical_index().to_string();
        let checkpoint = workspace.checkpoint();
        let from_run = checkpoint.progressive_next_run;
        let next_run = checkpoint.next_run;
        let document_count = checkpoint.document_count;
        let sub_index = checkpoint.progressive_subs;
        let paths = workspace.run_paths_in(from_run, next_run);
        if paths.is_empty() {
            return Ok(());
        }
        let sub_root = paged.fts_segments_root().join(&physical);
        let sub_name = format!("sub-{sub_index:04}");
        let mut worker = SegmentRangeWorker {
            pk: crate::fts_segment::FtsSegmentWriter::begin_pk(
                &sub_root,
                &sub_name,
                self.config.fsync,
                document_count,
            )?,
            impact: crate::fts_segment::FtsSegmentWriter::begin_impact_single_pass(
                &sub_root,
                &sub_name,
                self.config.fsync,
            )?,
            term_impacts: TermImpactAccumulator::new(
                workspace.scratch_path(&format!("progressive-impacts-{sub_index}.tmp")),
                (self.config.fts_build_memory_bytes / 4).max(4 * 1024 * 1024),
            ),
            current_term: None,
            block: Vec::with_capacity(FTS_BLOCK_DOC_CAP),
            block_bytes: 0,
            block_max_impact: 0,
            block_max_rank: f32::NEG_INFINITY,
            term_document_frequency: 0,
            term_collection_frequency: 0,
            term_maximum_contribution: 0.0,
            term_block_count: 0,
            term_first_document_id: None,
            term_last_document_id: None,
            terms_finished: 0,
        };
        crate::fts_build::stream_term_range(&paths, None, None, |event| worker.on_event(event))?;
        let terms = worker.finish_publish()?;
        let published =
            crate::fts_segment::publish_segment(&sub_root, &sub_name, self.config.fsync)?;
        debug_assert_eq!(published, terms);
        // Statistics before visibility: BM25 needs document counts the
        // moment the first query lands. Term count is the sum across subs —
        // an overcount where terms repeat, which the planner tolerates and
        // the final publish corrects.
        self.write_full_text_collection_statistics(
            workspace,
            &physical,
            checkpoint.progressive_terms + terms,
        )?;
        paged.register_fts_segment(&physical)?;
        // Same generation name, new readable content: everything cached
        // under it — posting blocks AND dictionary statistics — is stale.
        self.fts_block_cache.purge_generation(&physical);
        let has_published_generation = self
            .fts_generations
            .lock()
            .indexes
            .contains_key(&definition.name);
        let old = if sub_index == 0 && !has_published_generation {
            let old = self.publish_full_text_generation(&definition.name, &physical)?;
            if let Some(state) = self.indexes.get(&definition.name) {
                state.write().paged_read_through = true;
            }
            old.filter(|old| old != &physical)
        } else {
            None
        };
        workspace.record_progressive_sub(next_run, document_count, terms, old)?;
        if std::env::var("BICDB_FTS_BUILD_TRACE").as_deref() == Ok("1") {
            eprintln!(
                "fts-build {}: progressive sub {sub_index} published — {} documents searchable",
                definition.name, document_count
            );
        }
        Ok(())
    }

    /// Merge disjoint term ranges concurrently into per-part segment files,
    /// then assemble them into the exact single-segment layout the
    /// sequential merge produces. Workers share nothing; the term ranges
    /// partition the dictionary, parts concatenate in term order, and the
    /// assembly rebases part-local offsets — the output is byte-identical.
    pub(crate) fn merge_parallel_ranges(
        &self,
        workspace: &crate::fts_build::FtsBuildWorkspace,
        physical: &str,
        paths: &[std::path::PathBuf],
        splits: &[Vec<u8>],
    ) -> Result<()> {
        let root = self
            .paged_records
            .as_ref()
            .ok_or_else(|| {
                BicDbError::Index(format!("full-text build `{physical}` has no page store"))
            })?
            .fts_segments_root()
            .to_path_buf();
        let document_count = workspace.checkpoint().document_count;
        let parts = splits.len() + 1;
        // Stale parts from an interrupted previous attempt of this phase.
        if let Ok(entries) = std::fs::read_dir(root.join(physical)) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.contains(".part") && (name.ends_with(".dat") || name.ends_with(".tmp")) {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
        let mut workers: Vec<SegmentRangeWorker> = (0..parts)
            .map(|part| {
                Ok(SegmentRangeWorker {
                    pk: crate::fts_segment::FtsSegmentWriter::begin_pk_part(
                        &root,
                        physical,
                        self.config.fsync,
                        document_count,
                        part,
                    )?,
                    impact: crate::fts_segment::FtsSegmentWriter::begin_impact_part(
                        &root,
                        physical,
                        self.config.fsync,
                        part,
                    )?,
                    term_impacts: TermImpactAccumulator::new(
                        workspace.scratch_path(&format!("impact-triples.part{part}.tmp")),
                        (self.config.fts_build_memory_bytes / (4 * parts.max(1)))
                            .max(4 * 1024 * 1024),
                    ),
                    current_term: None,
                    block: Vec::with_capacity(FTS_BLOCK_DOC_CAP),
                    block_bytes: 0,
                    block_max_impact: 0,
                    block_max_rank: f32::NEG_INFINITY,
                    term_document_frequency: 0,
                    term_collection_frequency: 0,
                    term_maximum_contribution: 0.0,
                    term_block_count: 0,
                    term_first_document_id: None,
                    term_last_document_id: None,
                    terms_finished: 0,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let outputs = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(parts);
            for (part, mut worker) in workers.drain(..).enumerate() {
                let lower = if part == 0 {
                    None
                } else {
                    Some(splits[part - 1].as_slice())
                };
                let upper = splits.get(part).map(Vec::as_slice);
                handles.push(scope.spawn(move || -> Result<(Vec<u64>, Vec<u64>, u64)> {
                    crate::fts_build::stream_term_range(paths, lower, upper, |event| {
                        worker.on_event(event)
                    })?;
                    worker.finish()
                }));
            }
            handles
                .into_iter()
                .map(|handle| {
                    handle.join().map_err(|_| {
                        BicDbError::Index("full-text range-merge worker panicked".to_string())
                    })?
                })
                .collect::<Result<Vec<_>>>()
        })?;

        // Every part saw every document's stats for the terms it owned;
        // merge by first-nonzero. Disagreement would mean the runs carried
        // inconsistent per-document fields.
        let documents = usize::try_from(document_count)
            .map_err(|_| BicDbError::Index("document count exceeds this platform".to_string()))?;
        let mut docs = vec![0u64; documents];
        let mut dictionary_term_count = 0u64;
        for (pk_docs, _impact_docs, terms) in &outputs {
            dictionary_term_count += terms;
            for (slot, packed) in pk_docs.iter().enumerate() {
                if *packed != 0 {
                    debug_assert!(
                        docs[slot] == 0 || docs[slot] == *packed,
                        "parts disagree on document statistics"
                    );
                    docs[slot] = *packed;
                }
            }
        }
        crate::fts_segment::assemble_parts(&root, physical, parts, docs, self.config.fsync)?;
        if std::env::var("BICDB_FTS_BUILD_TRACE").as_deref() == Ok("1") {
            eprintln!(
                "fts-build {physical}: parallel merge over {parts} ranges, {} terms",
                dictionary_term_count
            );
        }
        self.write_full_text_collection_statistics(workspace, physical, dictionary_term_count)
    }

    pub(crate) fn write_full_text_collection_statistics(
        &self,
        workspace: &crate::fts_build::FtsBuildWorkspace,
        physical: &str,
        dictionary_term_count: u64,
    ) -> Result<()> {
        let checkpoint = workspace.checkpoint();
        let document_count = checkpoint.document_count;
        let total_document_length = checkpoint.total_document_length;
        let statistics = crate::fts_format::FullTextCollectionStatistics {
            format_version: crate::fts_format::FTS_GENERATION_FORMAT_VERSION,
            document_count,
            total_document_length,
            average_document_length: if document_count == 0 {
                0.0
            } else {
                total_document_length as f64 / document_count as f64
            },
            field_total_lengths: checkpoint.field_total_lengths,
            term_count: dictionary_term_count,
            next_document_id: document_count,
        };
        self.in_paged_transaction(|paged, xid| {
            paged.put_full_text_collection_statistics(xid, physical, &statistics)
        })?;
        Ok(())
    }

    pub(crate) fn merge_external_posting_blocks(
        &self,
        workspace: &crate::fts_build::FtsBuildWorkspace,
        physical: &str,
        order: crate::fts_build::RunOrder,
    ) -> Result<()> {
        use crate::fts_build::RunOrder;
        debug_assert!(matches!(order, RunOrder::Pk), "the merge is single-pass");
        // Parallel range merge: segment mode only (range workers are pure
        // file writers), opted out with BICDB_FTS_PARALLEL_MERGE=0.
        if self.config.fts_packed_segments
            && std::env::var("BICDB_FTS_PARALLEL_MERGE").as_deref() != Ok("0")
        {
            let workers = self.effective_full_text_build_workers();
            if let Some((paths, splits)) =
                workspace.plan_term_ranges(workers, self.config.fts_build_memory_bytes)?
            {
                return self.merge_parallel_ranges(workspace, physical, &paths, &splits);
            }
        }

        enum Suffix {
            Document(u64),
            Impact(u32),
            Dictionary(crate::fts_format::FullTextTermStatistics),
        }
        // Physical-format v2: blocks stream into a packed segment instead of
        // becoming millions of keyed rows. The bytes are identical; only
        // where they live changes.
        let sinks: Option<(
            std::cell::RefCell<crate::fts_segment::FtsSegmentWriter>,
            std::cell::RefCell<crate::fts_segment::FtsSegmentWriter>,
        )> = if self.config.fts_packed_segments {
            let root = self
                .paged_records
                .as_ref()
                .ok_or_else(|| {
                    BicDbError::Index(format!("full-text build `{physical}` has no page store"))
                })?
                .fts_segments_root()
                .to_path_buf();
            // ONE pass writes both orders: pk blocks stream out as they cut,
            // and each term's impact sidecar is generated at term end from
            // the accumulated (bucket, rank, id) triples — the second spill,
            // sort and merge that used to produce it are gone.
            Some((
                std::cell::RefCell::new(crate::fts_segment::FtsSegmentWriter::begin_pk(
                    &root,
                    physical,
                    self.config.fsync,
                    workspace.checkpoint().document_count,
                )?),
                std::cell::RefCell::new(
                    crate::fts_segment::FtsSegmentWriter::begin_impact_single_pass(
                        &root,
                        physical,
                        self.config.fsync,
                    )?,
                ),
            ))
        } else {
            None
        };
        let segment_sink = sinks.as_ref().map(|(pk, _)| pk);
        let impact_sink = sinks.as_ref().map(|(_, impact)| impact);
        let mut writes: Vec<(Vec<u8>, Suffix, Vec<u8>)> = Vec::with_capacity(512);
        let mut term_impacts = TermImpactAccumulator::new(
            workspace.scratch_path("impact-triples.tmp"),
            (self.config.fts_build_memory_bytes / 4).max(4 * 1024 * 1024),
        );
        let mut current_term: Option<Vec<u8>> = None;
        let mut block: Vec<crate::paged_collection::NumericBlockPosting> =
            Vec::with_capacity(FTS_BLOCK_DOC_CAP);
        let mut block_bytes = 0usize;
        let mut block_max_impact = 0u16;
        let mut block_max_rank = f32::NEG_INFINITY;
        let mut impact_seq = 0u32;
        let mut term_document_frequency = 0u64;
        let mut term_collection_frequency = 0u64;
        let mut term_maximum_contribution = 0.0f32;
        let mut term_block_count = 0u32;
        let mut term_stored_bytes = 0u64;
        let mut dictionary_term_count = 0u64;
        let mut term_first_document_id = None::<u64>;
        let mut term_last_document_id = None::<u64>;

        let write_nanos = std::cell::Cell::new(0u128);
        let write_bytes = std::cell::Cell::new(0u64);
        let write_entries = std::cell::Cell::new(0u64);
        let flush_writes = |writes: &mut Vec<(Vec<u8>, Suffix, Vec<u8>)>| -> Result<()> {
            if writes.is_empty() {
                return Ok(());
            }
            let flush_started = Instant::now();
            write_entries.set(write_entries.get() + writes.len() as u64);
            write_bytes.set(
                write_bytes.get()
                    + writes
                        .iter()
                        .map(|(_, _, bytes)| bytes.len() as u64)
                        .sum::<u64>(),
            );
            let flushed = self.in_paged_transaction(|paged, xid| {
                for (term, suffix, bytes) in writes.iter() {
                    match suffix {
                        Suffix::Document(last_document_id) => {
                            paged.put_numeric_posting_block(
                                xid,
                                physical,
                                term,
                                *last_document_id,
                                bytes,
                            )?;
                        }
                        Suffix::Impact(seq) => {
                            paged.put_numeric_impact_block(xid, physical, term, *seq, bytes)?;
                        }
                        Suffix::Dictionary(statistics) => {
                            paged.put_full_text_term_statistics(xid, physical, term, statistics)?;
                        }
                    }
                }
                Ok(())
            });
            write_nanos.set(write_nanos.get() + flush_started.elapsed().as_nanos());
            flushed?;
            writes.clear();
            Ok(())
        };

        let flush_block = |term: &Vec<u8>,
                           block: &mut Vec<crate::paged_collection::NumericBlockPosting>,
                           block_max_impact: &mut u16,
                           block_max_rank: &mut f32,
                           impact_seq: &mut u32,
                           term_block_count: &mut u32,
                           term_stored_bytes: &mut u64,
                           writes: &mut Vec<(Vec<u8>, Suffix, Vec<u8>)>|
         -> Result<()> {
            if block.is_empty() {
                return Ok(());
            }
            // Format 2: the pk pass hands the DECODED postings to the segment
            // writer, which stores the slim form. The legacy encoder is not
            // run at all here — it runs on READ, regenerating bit-identical
            // v1 bytes from the slim block plus docs.dat.
            if let (Some(sink), RunOrder::Pk) = (&segment_sink, order) {
                let stored = sink.borrow_mut().append_pk_postings(
                    term,
                    block,
                    *block_max_impact,
                    *block_max_rank,
                )?;
                *term_block_count = term_block_count.saturating_add(1);
                *term_stored_bytes = term_stored_bytes.saturating_add(stored);
                block.clear();
                *block_max_impact = 0;
                *block_max_rank = f32::NEG_INFINITY;
                return Ok(());
            }
            let (suffix, bytes) = match order {
                RunOrder::Pk => {
                    let (last_document_id, bytes) = encode_numeric_posting_run(block);
                    (Suffix::Document(last_document_id), bytes)
                }
                // The single-pass merge generates impact blocks per term in
                // `finish_term`; posting runs are only ever pk-ordered here.
                RunOrder::Impact => unreachable!("the merge is single-pass"),
            };
            *term_block_count = term_block_count.saturating_add(1);
            *term_stored_bytes = term_stored_bytes.saturating_add(bytes.len() as u64);
            if let Some(sink) = &segment_sink {
                match &suffix {
                    // pk blocks were diverted above, before encoding.
                    Suffix::Document(_) => unreachable!("pk blocks divert pre-encode"),
                    // Impact suffixes are never produced here: the merge is
                    // single-pass and emits sidecars in finish_term.
                    Suffix::Impact(_) => unreachable!("impact blocks emit in finish_term"),
                    Suffix::Dictionary(_) => unreachable!("blocks only"),
                }
            } else {
                writes.push((term.clone(), suffix, bytes));
            }
            block.clear();
            *block_max_impact = 0;
            *block_max_rank = f32::NEG_INFINITY;
            Ok(())
        };

        let finish_term = |term: &Vec<u8>,
                           block: &mut Vec<crate::paged_collection::NumericBlockPosting>,
                           block_max_impact: &mut u16,
                           block_max_rank: &mut f32,
                           impact_seq: &mut u32,
                           term_document_frequency: &mut u64,
                           term_collection_frequency: &mut u64,
                           term_maximum_contribution: &mut f32,
                           term_block_count: &mut u32,
                           term_stored_bytes: &mut u64,
                           term_first_document_id: &mut Option<u64>,
                           term_last_document_id: &mut Option<u64>,
                           dictionary_term_count: &mut u64,
                           term_impacts: &mut TermImpactAccumulator,
                           writes: &mut Vec<(Vec<u8>, Suffix, Vec<u8>)>|
         -> Result<()> {
            flush_block(
                term,
                block,
                block_max_impact,
                block_max_rank,
                impact_seq,
                term_block_count,
                term_stored_bytes,
                writes,
            )?;
            // Generate the term's impact sidecar from the accumulated
            // triples — the work the second merge pass used to do, now in
            // the only pass. Blocks cut exactly where the two-pass build
            // cut them; a one-block sidecar over a one-block pk term is
            // elided as before.
            let pk_blocks = *term_block_count;
            let mut impact_blocks_stored = 0u32;
            let mut impact_bytes_stored = 0u64;
            if let Some(sink) = &impact_sink {
                let mut pending: Option<(Vec<u64>, u16, f32)> = None;
                let mut emitted = 0usize;
                {
                    let mut writer = sink.borrow_mut();
                    term_impacts.finish(
                        FTS_BLOCK_DOC_CAP,
                        FTS_BLOCK_BYTE_TARGET,
                        |ids, max_impact, max_rank| {
                            emitted += 1;
                            if emitted == 1 && pk_blocks == 1 {
                                pending = Some((ids.to_vec(), max_impact, max_rank));
                                return Ok(());
                            }
                            if let Some((held_ids, held_impact, held_rank)) = pending.take() {
                                writer.append_impact_ids(
                                    term,
                                    &held_ids,
                                    held_impact,
                                    held_rank,
                                )?;
                            }
                            writer.append_impact_ids(term, ids, max_impact, max_rank)?;
                            Ok(())
                        },
                    )?;
                    if pending.is_some() {
                        // Exactly one block over a one-block pk term: elide.
                        writer.record_elided_impact_term(term)?;
                    } else {
                        writer.end_impact_term(term)?;
                    }
                }
            } else {
                let mut sequence = 0u32;
                term_impacts.finish(
                    FTS_BLOCK_DOC_CAP,
                    FTS_BLOCK_BYTE_TARGET,
                    |ids, max_impact, max_rank| {
                        let bytes = crate::paged_collection::encode_compact_impact_block_from_ids(
                            ids, max_impact, max_rank,
                        );
                        impact_bytes_stored += bytes.len() as u64;
                        impact_blocks_stored += 1;
                        writes.push((term.clone(), Suffix::Impact(sequence), bytes));
                        sequence += 1;
                        Ok(())
                    },
                )?;
            }
            if let Some(sink) = &segment_sink {
                let mut writer = sink.borrow_mut();
                *dictionary_term_count = dictionary_term_count.saturating_add(1);
                writer.end_pk_term(
                    term,
                    &crate::fts_segment::PkTermStats {
                        document_frequency: *term_document_frequency,
                        collection_frequency: *term_collection_frequency,
                        maximum_contribution: *term_maximum_contribution,
                        first_doc_id: *term_first_document_id,
                        last_doc_id: *term_last_document_id,
                    },
                )?;
                *term_document_frequency = 0;
                *term_collection_frequency = 0;
                *term_maximum_contribution = 0.0;
                *term_block_count = 0;
                *term_stored_bytes = 0;
                *term_first_document_id = None;
                *term_last_document_id = None;
                return Ok(());
            }
            *dictionary_term_count = dictionary_term_count.saturating_add(1);
            let statistics = crate::fts_format::FullTextTermStatistics {
                document_frequency: *term_document_frequency,
                collection_frequency: *term_collection_frequency,
                posting_block_count: *term_block_count,
                impact_block_count: impact_blocks_stored,
                posting_bytes: *term_stored_bytes,
                impact_bytes: impact_bytes_stored,
                first_doc_id: *term_first_document_id,
                last_doc_id: *term_last_document_id,
                maximum_contribution: *term_maximum_contribution,
            };
            writes.push((term.clone(), Suffix::Dictionary(statistics), Vec::new()));
            *term_document_frequency = 0;
            *term_collection_frequency = 0;
            *term_maximum_contribution = 0.0;
            *term_block_count = 0;
            *term_stored_bytes = 0;
            *term_first_document_id = None;
            *term_last_document_id = None;
            Ok(())
        };

        workspace.stream_merged_terms(
            self.effective_full_text_build_workers(),
            self.config.fts_build_memory_bytes,
            |event| match event {
                crate::fts_build::TermStreamEvent::Term(term) => {
                    if let Some(previous) = current_term.as_ref() {
                        finish_term(
                            previous,
                            &mut block,
                            &mut block_max_impact,
                            &mut block_max_rank,
                            &mut impact_seq,
                            &mut term_document_frequency,
                            &mut term_collection_frequency,
                            &mut term_maximum_contribution,
                            &mut term_block_count,
                            &mut term_stored_bytes,
                            &mut term_first_document_id,
                            &mut term_last_document_id,
                            &mut dictionary_term_count,
                            &mut term_impacts,
                            &mut writes,
                        )?;
                        block_bytes = 0;
                        impact_seq = 0;
                    }
                    match current_term.as_mut() {
                        Some(buffer) => {
                            buffer.clear();
                            buffer.extend_from_slice(term);
                        }
                        None => current_term = Some(term.to_vec()),
                    }
                    Ok(())
                }
                crate::fts_build::TermStreamEvent::Posting {
                    posting,
                    impact_bucket,
                    impact_rank,
                } => {
                    let posting_bytes = 8 + posting.packed_positions.len() * 2 + 12;
                    if !block.is_empty()
                        && (block.len() >= FTS_BLOCK_DOC_CAP
                            || block_bytes.saturating_add(posting_bytes) > FTS_BLOCK_BYTE_TARGET)
                    {
                        flush_block(
                            current_term.as_ref().expect("term exists"),
                            &mut block,
                            &mut block_max_impact,
                            &mut block_max_rank,
                            &mut impact_seq,
                            &mut term_block_count,
                            &mut term_stored_bytes,
                            &mut writes,
                        )?;
                        block_bytes = 0;
                    }
                    term_document_frequency = term_document_frequency.saturating_add(1);
                    term_collection_frequency = term_collection_frequency
                        .saturating_add(posting.packed_positions.len() as u64);
                    term_first_document_id = Some(
                        term_first_document_id
                            .map_or(posting.document_id, |first| first.min(posting.document_id)),
                    );
                    term_last_document_id = Some(
                        term_last_document_id
                            .map_or(posting.document_id, |last| last.max(posting.document_id)),
                    );
                    term_maximum_contribution = term_maximum_contribution.max(impact_rank);
                    block_max_impact = block_max_impact.max(impact_bucket);
                    block_max_rank = block_max_rank.max(impact_rank);
                    block_bytes = block_bytes.saturating_add(posting_bytes);
                    term_impacts.push(ImpactTriple {
                        bucket: impact_bucket,
                        rank: impact_rank,
                        document_id: posting.document_id,
                        // The two-pass build estimated sidecar cuts from its
                        // impact-run records, which carry no positions: the
                        // estimator saw `8 + 0*2 + 12` for every posting. Use
                        // the same constant so blocks cut at the same points.
                        estimated_bytes: 8 + 12,
                    })?;
                    block.push(posting);
                    if writes.len() >= 4096 {
                        flush_writes(&mut writes)?;
                    }
                    Ok(())
                }
            },
        )?;
        if let Some(term) = current_term.as_ref() {
            finish_term(
                term,
                &mut block,
                &mut block_max_impact,
                &mut block_max_rank,
                &mut impact_seq,
                &mut term_document_frequency,
                &mut term_collection_frequency,
                &mut term_maximum_contribution,
                &mut term_block_count,
                &mut term_stored_bytes,
                &mut term_first_document_id,
                &mut term_last_document_id,
                &mut dictionary_term_count,
                &mut term_impacts,
                &mut writes,
            )?;
        }
        flush_writes(&mut writes)?;
        if let Some((pk_sink, impact_sink)) = sinks {
            pk_sink.into_inner().finish_phase()?;
            impact_sink.into_inner().finish_phase()?;
        }
        if std::env::var("BICDB_FTS_BUILD_TRACE").as_deref() == Ok("1") {
            eprintln!(
                "fts-build {physical}: merge_{} wrote {} entries / {} bytes in {:.3}s of commit time",
                match order {
                    RunOrder::Pk => "pk",
                    RunOrder::Impact => "impact",
                },
                write_entries.get(),
                write_bytes.get(),
                write_nanos.get() as f64 / 1e9,
            );
        }
        if order == RunOrder::Pk {
            self.write_full_text_collection_statistics(workspace, physical, dictionary_term_count)?;
        }
        Ok(())
    }
}
