//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;

impl BicDb {
    pub(crate) fn spatial_point_scan(
        &self,
        collection: &str,
        field: &IndexField,
        lon: f64,
        lat: f64,
        radius_meters: Option<f64>,
    ) -> Result<Vec<SpatialQueryResult>> {
        let state = self.collection_state(collection)?;
        let mut results = Vec::new();
        for shard in state.read_all() {
            for entry in shard.records.values() {
                let record = entry.record.to_record()?;
                let Some(point) = spatial_record_point(&record, field)? else {
                    continue;
                };
                let distance_meters = haversine_meters(lon, lat, point[0], point[1]);
                if radius_meters.is_none_or(|meters| distance_meters <= meters) {
                    results.push(SpatialQueryResult {
                        record,
                        distance_meters,
                    });
                }
            }
        }
        Ok(results)
    }

    pub(crate) fn ensure_spatial_field_present(
        &self,
        collection: &str,
        field: &IndexField,
    ) -> Result<()> {
        let state = self.collection_state(collection)?;
        if state.record_count() == 0 {
            return Ok(());
        }
        for shard in state.read_all() {
            for entry in shard.records.values() {
                if record_spatial_geometry(&entry.record.to_record()?, field)?.is_some() {
                    return Ok(());
                }
            }
        }
        Err(BicDbError::Index(format!(
            "spatial field `{}` not found in collection `{collection}`",
            spatial_field_name(field)
        )))
    }

    pub(crate) fn ensure_spatial_point_query(
        &self,
        collection: &str,
        field: &str,
        lon: f64,
        lat: f64,
        limit: Option<usize>,
        radius_meters: Option<f64>,
    ) -> Result<()> {
        self.ensure_collection(collection)?;
        let _ = spatial_field_from_name(field)?;
        if !lon.is_finite() || !lat.is_finite() {
            return Err(BicDbError::Index(
                "spatial query requires finite lon/lat".to_string(),
            ));
        }
        if limit.is_some_and(|limit| limit == 0) {
            return Ok(());
        }
        if radius_meters.is_some_and(|meters| meters < 0.0 || !meters.is_finite()) {
            return Err(BicDbError::Index(
                "spatial radius query requires non-negative finite meters".to_string(),
            ));
        }
        Ok(())
    }

    /// FOLD a full-text index's per-posting tail into compressed posting
    /// BLOCKS (Search-core stage: block postings). Per term, atomically in
    /// one paged transaction: existing blocks + v1 tail postings (tail wins
    /// per pk) minus tombstones are re-blocked (front-coded pks, varint
    /// positions, per-block max impact) and the folded v1/v2 entries and
    /// consumed tombstones are deleted. The per-posting keyspaces remain the
    /// transactional TAIL for writes after the fold; reads merge
    /// blocks + tail - tombstones. Returns (terms folded, blocks written).
    pub fn compact_full_text_index(&self, name: &str) -> Result<(usize, usize)> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Err(BicDbError::Index(format!(
                "index `{name}` is not a read-through full-text index"
            )));
        }
        drop(state);
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;
        let paged = Arc::clone(paged);
        if paged.index_has_numeric_posting_blocks(&paged.latest_snapshot(), name)? {
            return self.compact_numeric_full_text_index(name, &paged);
        }

        // Terms with anything to fold: a v1 tail entry or a tombstone.
        let snapshot = paged.latest_snapshot();
        let mut terms: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        for entry in paged.scan_index(&snapshot, name)? {
            let (encoded_key, _) = entry?;
            terms.insert(encoded_key);
        }
        // v2 twins are written alongside v1 since 0.9.51, so the v1 scan
        // already covers every tail term.
        let mut tombstone_terms: std::collections::BTreeSet<Vec<u8>> =
            std::collections::BTreeSet::new();
        for entry in paged.scan_index_tombstone_terms(&snapshot, name)? {
            tombstone_terms.insert(entry?);
        }
        terms.extend(tombstone_terms.iter().cloned());

        let mut folded_terms = 0usize;
        let mut blocks_written = 0usize;
        for encoded_key in terms {
            let snapshot = paged.latest_snapshot();
            // Assemble the term's full posting set: blocks first, then the
            // tail OVERRIDES per pk, then tombstones remove.
            let mut merged: std::collections::BTreeMap<
                String,
                crate::paged_collection::BlockPosting,
            > = std::collections::BTreeMap::new();
            let mut old_blocks: Vec<String> = Vec::new();
            for block in paged.scan_posting_blocks(&snapshot, name, &encoded_key)? {
                let (last_pk, bytes) = block?;
                old_blocks.push(last_pk);
                let (postings, _) = crate::paged_collection::decode_posting_block(&bytes)?;
                for posting in postings {
                    merged.insert(posting.pk.clone(), posting);
                }
            }
            let mut tail_pairs: Vec<(String, Option<(u32, u32, Vec<u16>)>)> = Vec::new();
            for entry in paged.scan_index_exact(&snapshot, name, &encoded_key)? {
                let (pk, payload) = entry?;
                tail_pairs.push((pk, decode_fts_posting_payload(&payload)));
            }
            for (pk, decoded) in &tail_pairs {
                let Some((doc_length, doc_distinct, packed)) = decoded else {
                    // Legacy payload-less tail (pre-0.9.48 postings): folding
                    // would lose positions — skip this term entirely.
                    tail_pairs.clear();
                    merged.clear();
                    break;
                };
                merged.insert(
                    pk.clone(),
                    crate::paged_collection::BlockPosting {
                        pk: pk.clone(),
                        doc_length: *doc_length,
                        doc_distinct: *doc_distinct,
                        packed_positions: packed.clone(),
                    },
                );
            }
            if tail_pairs.is_empty() && merged.is_empty() && old_blocks.is_empty() {
                continue;
            }
            let mut tombstoned: Vec<String> = Vec::new();
            for pk in paged.scan_posting_tombstones(&snapshot, name, &encoded_key)? {
                let pk = pk?;
                merged.remove(&pk);
                tombstoned.push(pk);
            }

            // Re-block, twice: once in pk order (probes, boolean merges) and
            // once in (impact DESC, pk) order (the ranked block-max scan —
            // pk-ordered blocks interleave impact classes, so every block max
            // ties and nothing can be skipped).
            let merged: Vec<crate::paged_collection::BlockPosting> = merged.into_values().collect();
            let new_blocks =
                build_posting_blocks(&merged, FTS_BLOCK_DOC_CAP, FTS_BLOCK_BYTE_TARGET);
            let mut impact_sorted = merged;
            impact_sorted.sort_by(|left, right| {
                fts_impact_bucket(&right.packed_positions)
                    .cmp(&fts_impact_bucket(&left.packed_positions))
                    .then_with(|| left.pk.cmp(&right.pk))
            });
            let impact_blocks =
                build_posting_blocks(&impact_sorted, FTS_BLOCK_DOC_CAP, FTS_BLOCK_BYTE_TARGET);

            // Atomic swap for this term.
            let tail_pks: Vec<String> = tail_pairs.iter().map(|(pk, _)| pk.clone()).collect();
            let tail_buckets: Vec<Option<u16>> = tail_pairs
                .iter()
                .map(|(_, decoded)| {
                    decoded
                        .as_ref()
                        .map(|(_, _, packed)| fts_impact_bucket(packed))
                })
                .collect();
            let old_impact_seqs: Vec<u32> = paged
                .scan_impact_blocks(&snapshot, name, &encoded_key)?
                .map(|entry| entry.map(|(seq, _)| seq))
                .collect::<Result<Vec<_>>>()?;
            self.in_paged_transaction(|paged, xid| {
                for last_pk in &old_blocks {
                    paged.delete_posting_block(xid, name, &encoded_key, last_pk)?;
                }
                for seq in &old_impact_seqs {
                    paged.delete_impact_block(xid, name, &encoded_key, *seq)?;
                }
                for (seq, (_, block)) in impact_blocks.iter().enumerate() {
                    paged.put_impact_block(xid, name, &encoded_key, seq as u32, block)?;
                }
                for (pk, bucket) in tail_pks.iter().zip(&tail_buckets) {
                    paged.delete_index_entry(xid, name, &encoded_key, pk)?;
                    if let Some(bucket) = bucket {
                        paged.delete_index_entry_v2(xid, name, &encoded_key, *bucket, pk)?;
                    }
                }
                for pk in &tombstoned {
                    paged.delete_posting_tombstone(xid, name, &encoded_key, pk)?;
                }
                for (last_pk, block) in &new_blocks {
                    paged.put_posting_block(xid, name, &encoded_key, last_pk, block)?;
                }
                Ok(())
            })?;
            folded_terms += 1;
            blocks_written += new_blocks.len();
        }
        // Reclaim the folded per-posting entries: until vacuumed, every read
        // of a folded term pays a visibility walk over the dead tail
        // (~285 ms per 100k reclaimed entries, measured). Safe since B11a
        // (writes tolerate reclaimed chain heads) and B11b (slot directories
        // keep trailing dead slots, so generations stay monotonic and stale
        // locators can never validate against a new tenant).
        paged.checkpoint()?;
        vacuum_paged_records_to_completion(paged.as_ref())?;
        // Vacuum retires the versions; the sweep retires the KEYS. Without
        // it every read of a folded term still walks the dead tail's B-tree
        // entries (~180 ms per 100k keys, measured).
        paged.sweep_dead_index_entries(usize::MAX)?;
        Ok((folded_terms, blocks_written))
    }

    pub(crate) fn compact_numeric_full_text_index(
        &self,
        name: &str,
        paged: &Arc<crate::paged_collection::PagedRecords>,
    ) -> Result<(usize, usize)> {
        let snapshot = paged.latest_snapshot();
        let mut collection_statistics = paged
            .full_text_collection_statistics(&snapshot, name)?
            .ok_or_else(|| {
                BicDbError::Index(format!(
                    "numeric full-text index `{name}` has no collection statistics"
                ))
            })?;
        let mut terms = std::collections::BTreeSet::new();
        for entry in paged.scan_index(&snapshot, name)? {
            let (encoded_key, _) = entry?;
            terms.insert(encoded_key);
        }
        for entry in paged.scan_index_tombstone_terms(&snapshot, name)? {
            terms.insert(entry?);
        }

        let mut folded_terms = 0usize;
        let mut blocks_written = 0usize;
        for encoded_key in terms {
            let snapshot = paged.latest_snapshot();
            let previous_statistics =
                paged.full_text_term_statistics(&snapshot, name, &encoded_key)?;
            let mut merged = std::collections::BTreeMap::<
                u64,
                crate::paged_collection::NumericBlockPosting,
            >::new();
            let mut old_blocks = Vec::new();
            for block in paged.scan_numeric_posting_blocks(&snapshot, name, &encoded_key)? {
                let (last_document_id, bytes) = block?;
                old_blocks.push(last_document_id);
                for posting in crate::paged_collection::decode_numeric_posting_block(&bytes)? {
                    merged.insert(posting.document_id, posting);
                }
            }

            let mut tail = Vec::<(String, u16, u64)>::new();
            let mut new_mappings =
                Vec::<(u64, String, crate::fts_format::FullTextDocumentStatistics)>::new();
            let mut legacy = false;
            for entry in paged.scan_index_exact(&snapshot, name, &encoded_key)? {
                let (pk, payload) = entry?;
                let Some((doc_length, doc_distinct, packed_positions)) =
                    decode_fts_posting_payload(&payload)
                else {
                    legacy = true;
                    break;
                };
                let document_id = match paged.full_text_document_id_for_pk(&snapshot, name, &pk)? {
                    Some(document_id) => document_id,
                    None => {
                        let document_id = collection_statistics.next_document_id;
                        collection_statistics.next_document_id = collection_statistics
                            .next_document_id
                            .checked_add(1)
                            .ok_or_else(|| {
                                BicDbError::Index(
                                    "full-text document-id space exhausted".to_string(),
                                )
                            })?;
                        collection_statistics.document_count =
                            collection_statistics.document_count.saturating_add(1);
                        collection_statistics.total_document_length = collection_statistics
                            .total_document_length
                            .saturating_add(u64::from(doc_length));
                        let statistics = paged
                            .get_doc_terms(&snapshot, name, &pk)?
                            .map(|blob| crate::paged_collection::decode_doc_term_statistics(&blob))
                            .transpose()?
                            .unwrap_or(crate::fts_format::FullTextDocumentStatistics {
                                document_length: doc_length,
                                distinct_terms: doc_distinct,
                                field_lengths: [doc_length, 0, 0, 0],
                            });
                        for (total, length) in collection_statistics
                            .field_total_lengths
                            .iter_mut()
                            .zip(statistics.field_lengths)
                        {
                            *total = total.saturating_add(u64::from(length));
                        }
                        new_mappings.push((document_id, pk.clone(), statistics));
                        document_id
                    }
                };
                let bucket = fts_impact_bucket(&packed_positions);
                merged.insert(
                    document_id,
                    crate::paged_collection::NumericBlockPosting {
                        document_id,
                        doc_length,
                        doc_distinct,
                        packed_positions,
                    },
                );
                tail.push((pk, bucket, document_id));
            }
            if legacy {
                continue;
            }

            let mut tombstones = Vec::new();
            for pk in paged.scan_posting_tombstones(&snapshot, name, &encoded_key)? {
                let pk = pk?;
                if let Some(document_id) =
                    paged.full_text_document_id_for_pk(&snapshot, name, &pk)?
                {
                    merged.remove(&document_id);
                }
                tombstones.push(pk);
            }

            let postings: Vec<_> = merged.into_values().collect();
            let posting_blocks =
                build_numeric_posting_blocks(&postings, FTS_BLOCK_DOC_CAP, FTS_BLOCK_BYTE_TARGET);
            let mut impact_postings = postings.clone();
            impact_postings.sort_by(|left, right| {
                fts_impact_bucket(&right.packed_positions)
                    .cmp(&fts_impact_bucket(&left.packed_positions))
                    .then_with(|| left.document_id.cmp(&right.document_id))
            });
            let impact_blocks = build_compact_impact_blocks(
                &impact_postings,
                FTS_BLOCK_DOC_CAP,
                FTS_BLOCK_BYTE_TARGET,
            );
            let old_impact_blocks: Vec<u32> = paged
                .scan_numeric_impact_blocks(&snapshot, name, &encoded_key)?
                .map(|entry| entry.map(|(seq, _)| seq))
                .collect::<Result<_>>()?;

            let term_statistics = (!postings.is_empty()).then(|| {
                let mut statistics = crate::fts_format::FullTextTermStatistics {
                    document_frequency: postings.len() as u64,
                    collection_frequency: postings
                        .iter()
                        .map(|posting| posting.packed_positions.len() as u64)
                        .sum(),
                    posting_block_count: posting_blocks.len() as u32,
                    impact_block_count: impact_blocks.len() as u32,
                    posting_bytes: posting_blocks
                        .iter()
                        .map(|(_, bytes)| bytes.len() as u64)
                        .sum(),
                    impact_bytes: impact_blocks
                        .iter()
                        .map(|(_, bytes)| bytes.len() as u64)
                        .sum(),
                    first_doc_id: postings.first().map(|posting| posting.document_id),
                    last_doc_id: postings.last().map(|posting| posting.document_id),
                    maximum_contribution: 0.0,
                };
                for posting in &postings {
                    statistics.maximum_contribution = statistics.maximum_contribution.max(
                        fts_rank_single_term(&posting.packed_positions, [0.1, 0.2, 0.4, 1.0]),
                    );
                }
                statistics
            });
            match (previous_statistics.is_some(), term_statistics.is_some()) {
                (false, true) => {
                    collection_statistics.term_count =
                        collection_statistics.term_count.saturating_add(1);
                }
                (true, false) => {
                    collection_statistics.term_count =
                        collection_statistics.term_count.saturating_sub(1);
                }
                _ => {}
            }
            collection_statistics.average_document_length =
                if collection_statistics.document_count == 0 {
                    0.0
                } else {
                    collection_statistics.total_document_length as f64
                        / collection_statistics.document_count as f64
                };

            self.in_paged_transaction(|paged, xid| {
                for last_document_id in &old_blocks {
                    paged.delete_numeric_posting_block(
                        xid,
                        name,
                        &encoded_key,
                        *last_document_id,
                    )?;
                }
                for seq in &old_impact_blocks {
                    paged.delete_numeric_impact_block(xid, name, &encoded_key, *seq)?;
                }
                for (document_id, pk, statistics) in &new_mappings {
                    paged.put_full_text_document_id(xid, name, *document_id, pk)?;
                    paged.put_full_text_document_statistics(xid, name, *document_id, statistics)?;
                }
                for (last_document_id, bytes) in &posting_blocks {
                    paged.put_numeric_posting_block(
                        xid,
                        name,
                        &encoded_key,
                        *last_document_id,
                        bytes,
                    )?;
                }
                for (seq, (_, bytes)) in impact_blocks.iter().enumerate() {
                    paged.put_numeric_impact_block(xid, name, &encoded_key, seq as u32, bytes)?;
                }
                for (pk, bucket, _) in &tail {
                    paged.delete_index_entry(xid, name, &encoded_key, pk)?;
                    paged.delete_index_entry_v2(xid, name, &encoded_key, *bucket, pk)?;
                }
                for pk in &tombstones {
                    paged.delete_posting_tombstone(xid, name, &encoded_key, pk)?;
                }
                if let Some(statistics) = &term_statistics {
                    paged.put_full_text_term_statistics(xid, name, &encoded_key, statistics)?;
                } else {
                    paged.delete_full_text_term_statistics(xid, name, &encoded_key)?;
                }
                paged.put_full_text_collection_statistics(xid, name, &collection_statistics)
            })?;
            folded_terms += 1;
            blocks_written += posting_blocks.len();
        }
        paged.checkpoint()?;
        vacuum_paged_records_to_completion(paged.as_ref())?;
        paged.sweep_dead_index_entries(usize::MAX)?;
        Ok((folded_terms, blocks_written))
    }

    pub fn compact(&mut self) -> Result<CompactionReport> {
        self.compact_with_options(CompactionOptions::default())
    }

    pub fn compact_with_options_governed(
        &mut self,
        options: CompactionOptions,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<CompactionReport> {
        let permit = governor.try_admit(ResourceLane::Compaction, demand, now_ms)?;
        let result = self.compact_with_options(options);
        drop(permit);
        result
    }

    /// Create or reopen an offline, collection-at-a-time compaction run. The
    /// database commit sequence is pinned for the lifetime of the run; writes
    /// between ticks fail closed before log truncation can occur.
    pub fn begin_incremental_compaction(
        &mut self,
        options: CompactionOptions,
        limits: IncrementalCompactionLimits,
        now_ms: u64,
    ) -> Result<IncrementalCompactionState> {
        limits.validate()?;
        if options.allow_pending {
            return Err(BicDbError::Compaction(
                "incremental compaction requires an offline, write-stable database".to_string(),
            ));
        }
        self.ensure_no_pending_transactions()?;
        let path = self.path.join(DEFAULT_INCREMENTAL_COMPACTION_STATE);
        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(BicDbError::Compaction(
                        "refusing symlink incremental compaction state".to_string(),
                    ));
                }
                let existing = self.load_incremental_compaction(limits)?;
                if existing.phase != IncrementalCompactionPhase::Complete {
                    if existing.options == options {
                        return Ok(existing);
                    }
                    return Err(BicDbError::Compaction(
                        "another incremental compaction run is active".to_string(),
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let mut collections = self
            .collections()
            .into_iter()
            .map(|collection| collection.name)
            .collect::<Vec<_>>();
        collections.sort();
        if collections.len() > limits.max_collections {
            return Err(BicDbError::Compaction(
                "database collection count exceeds incremental compaction limit".to_string(),
            ));
        }
        let transaction_log_bytes_before = file_len(&self.tx_log_path())?;
        let event_log_bytes_before = file_len(&event_segment_path_for(&self.path))?;
        let sync_log_bytes_before = file_len(&self.path.join("sync.log"))?;
        let run_id = Uuid::now_v7();
        let mut state = IncrementalCompactionState {
            format_version: INCREMENTAL_COMPACTION_FORMAT_VERSION,
            run_id,
            phase: IncrementalCompactionPhase::RewritingCollections,
            options,
            limits,
            collections: collections.clone(),
            next_collection: 0,
            starting_commit_seq: self.last_commit_seq(),
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            report: CompactionReport {
                checkpoint_id: Some(run_id.to_string()),
                transaction_log_bytes_before,
                event_log_bytes_before,
                sync_log_bytes_before,
                index_metadata_bytes: file_len(&self.path.join(DEFAULT_INDEX_CATALOG))?,
                sidecar_bytes: self.sidecar_bytes()?,
                ..CompactionReport::default()
            },
            checksum_sha256: String::new(),
        };
        state.refresh_checksum()?;
        self.write_compaction_checkpoint(
            &run_id.to_string(),
            CompactionCheckpointPhase::Started,
            &collections,
            transaction_log_bytes_before,
            event_log_bytes_before,
            sync_log_bytes_before,
        )?;
        self.save_incremental_compaction(&state)?;
        Ok(state)
    }

    pub fn load_incremental_compaction(
        &self,
        limits: IncrementalCompactionLimits,
    ) -> Result<IncrementalCompactionState> {
        limits.validate()?;
        let path = self.path.join(DEFAULT_INCREMENTAL_COMPACTION_STATE);
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() == 0
            || metadata.len() > limits.max_state_bytes
        {
            return Err(BicDbError::Compaction(
                "incremental compaction state is unsafe or outside its bound".to_string(),
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
        let length = metadata.len();
        let mut bytes = Vec::with_capacity(length as usize);
        file.take(limits.max_state_bytes.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 != length || bytes.len() as u64 > limits.max_state_bytes {
            return Err(BicDbError::Compaction(
                "incremental compaction state changed or grew while reading".to_string(),
            ));
        }
        let state: IncrementalCompactionState = serde_json::from_slice(&bytes)?;
        if state.limits != limits {
            return Err(BicDbError::Compaction(
                "incremental compaction limits changed across restart".to_string(),
            ));
        }
        state.validate()?;
        Ok(state)
    }

    /// Advance exactly one collection or one finalization phase under the
    /// compaction resource lane, then atomically checkpoint the durable state.
    pub fn advance_incremental_compaction_governed(
        &mut self,
        limits: IncrementalCompactionLimits,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<IncrementalCompactionAdvance> {
        let mut state = self.load_incremental_compaction(limits)?;
        if state.phase == IncrementalCompactionPhase::Complete {
            return Ok(IncrementalCompactionAdvance::Complete(state.report));
        }
        if now_ms < state.updated_at_ms {
            return Err(BicDbError::Compaction(
                "incremental compaction clock regressed".to_string(),
            ));
        }
        self.ensure_no_pending_transactions()?;
        if self.last_commit_seq() != state.starting_commit_seq {
            return Err(BicDbError::Compaction(
                "database changed during incremental compaction; discard and restart the run"
                    .to_string(),
            ));
        }
        let _permit = governor.try_admit(ResourceLane::Compaction, demand, now_ms)?;
        match state.phase {
            IncrementalCompactionPhase::RewritingCollections => {
                if let Some(collection) = state.collections.get(state.next_collection).cloned() {
                    let report = self.compact_collection_inner(&collection)?;
                    throttle_compaction_io(&state.options, report.bytes_before)?;
                    state.report.live_records = state
                        .report
                        .live_records
                        .saturating_add(report.live_records);
                    state.report.bytes_before = state
                        .report
                        .bytes_before
                        .saturating_add(report.bytes_before);
                    state.report.bytes_after =
                        state.report.bytes_after.saturating_add(report.bytes_after);
                    state.report.bytes_reclaimed = state
                        .report
                        .bytes_reclaimed
                        .saturating_add(report.bytes_reclaimed);
                    state.report.collections.push(report);
                    state.next_collection += 1;
                } else {
                    state.report.bytes_scanned = state
                        .report
                        .bytes_before
                        .saturating_add(state.report.transaction_log_bytes_before)
                        .saturating_add(state.report.event_log_bytes_before)
                        .saturating_add(state.report.sync_log_bytes_before)
                        .saturating_add(state.report.index_metadata_bytes)
                        .saturating_add(state.report.sidecar_bytes);
                    state.report.dead_records = estimate_dead_records(&state.report.collections);
                    let skip_logs = !state.options.force
                        && state.options.reclaim_threshold_percent > 0
                        && reclaim_percent(state.report.bytes_before, state.report.bytes_reclaimed)
                            < state.options.reclaim_threshold_percent;
                    if skip_logs {
                        state.report.transaction_log_bytes_after =
                            state.report.transaction_log_bytes_before;
                        state.report.event_log_bytes_after = state.report.event_log_bytes_before;
                        state.report.sync_log_bytes_after = state.report.sync_log_bytes_before;
                        state.phase = IncrementalCompactionPhase::Complete;
                        self.write_incremental_compatibility_checkpoint(
                            &state,
                            CompactionCheckpointPhase::Committed,
                        )?;
                    } else {
                        state.phase = IncrementalCompactionPhase::CheckpointingLogs;
                        self.write_incremental_compatibility_checkpoint(
                            &state,
                            CompactionCheckpointPhase::RecordsRewritten,
                        )?;
                    }
                }
            }
            IncrementalCompactionPhase::CheckpointingLogs => {
                self.tx_log.flush_pending()?;
                truncate_log_file(&self.tx_log_path(), self.config.fsync)?;
                self.tx_log.invalidate();
                self.tx_states
                    .retain(|_, value| *value == TxState::Committed);
                let (event_before, event_after, frozen_end) = {
                    let mut events = self.events.lock();
                    events.broker().trim_configured()?;
                    let frozen_end = self.freeze_local_event_positions(&mut events)?;
                    let (before, after) = events.compact()?;
                    (before, after, frozen_end)
                };
                self.invalidate_sync_vector_cache();
                let (sync_before, sync_after) = self.sync_log.get_mut().compact()?;
                self.sync_state.last_export_offset = 0;
                self.sync_state.last_reconciled_offset = 0;
                self.sync_state.origin_position_base = self
                    .sync_state
                    .origin_position_base
                    .saturating_add(frozen_end);
                state.report.event_log_bytes_before = event_before;
                state.report.event_log_bytes_after = event_after;
                state.report.sync_log_bytes_before = sync_before;
                state.report.sync_log_bytes_after = sync_after;
                state.report.transaction_log_bytes_after = file_len(&self.tx_log_path())?;
                state.phase = IncrementalCompactionPhase::Publishing;
                self.write_incremental_compatibility_checkpoint(
                    &state,
                    CompactionCheckpointPhase::LogsCheckpointed,
                )?;
            }
            IncrementalCompactionPhase::Publishing => {
                self.persist_catalog()?;
                self.persist_index_catalog()?;
                self.persist_sync_state()?;
                self.write_incremental_compatibility_checkpoint(
                    &state,
                    CompactionCheckpointPhase::Committed,
                )?;
                if self.config.snapshot {
                    if let Err(error) = self.write_startup_snapshot() {
                        eprintln!("[snapshot] incremental compaction write failed: {error}");
                    }
                }
                state.phase = IncrementalCompactionPhase::Complete;
            }
            IncrementalCompactionPhase::Complete => unreachable!(),
        }
        state.updated_at_ms = now_ms;
        state.report.duration_ms = now_ms.saturating_sub(state.created_at_ms);
        state.refresh_checksum()?;
        self.save_incremental_compaction(&state)?;
        if state.phase == IncrementalCompactionPhase::Complete {
            Ok(IncrementalCompactionAdvance::Complete(state.report))
        } else {
            Ok(IncrementalCompactionAdvance::Progress {
                phase: state.phase,
                completed_collections: state.next_collection,
                total_collections: state.collections.len(),
            })
        }
    }

    /// Remove a validated incomplete coordinator checkpoint. Already rewritten
    /// collection files remain valid; a new run starts from current committed
    /// state and safely rewrites them again.
    pub fn discard_incremental_compaction(
        &self,
        limits: IncrementalCompactionLimits,
    ) -> Result<bool> {
        let path = self.path.join(DEFAULT_INCREMENTAL_COMPACTION_STATE);
        if !path.exists() {
            return Ok(false);
        }
        self.load_incremental_compaction(limits)?;
        fs::remove_file(&path)?;
        #[cfg(unix)]
        if self.config.fsync {
            File::open(&self.path)?.sync_all()?;
        }
        Ok(true)
    }

    pub(crate) fn save_incremental_compaction(
        &self,
        state: &IncrementalCompactionState,
    ) -> Result<()> {
        state.validate()?;
        let bytes = serde_json::to_vec(state)?;
        if bytes.len() as u64 > state.limits.max_state_bytes {
            return Err(BicDbError::Compaction(
                "incremental compaction state exceeds its write bound".to_string(),
            ));
        }
        storage::write_atomic(
            &self.path.join(DEFAULT_INCREMENTAL_COMPACTION_STATE),
            &bytes,
            self.config.fsync,
        )
    }

    pub(crate) fn write_incremental_compatibility_checkpoint(
        &self,
        state: &IncrementalCompactionState,
        phase: CompactionCheckpointPhase,
    ) -> Result<()> {
        self.write_compaction_checkpoint(
            &state.run_id.to_string(),
            phase,
            &state.collections[..state.next_collection],
            state.report.transaction_log_bytes_before,
            state.report.event_log_bytes_before,
            state.report.sync_log_bytes_before,
        )
    }

    /// Event-horizon trimming: bound record-audit history growth without
    /// breaking sync.
    ///
    /// The record-audit stream is the sync substrate, and state convergence
    /// only ever consumes each record's *winning* event (reconciliation
    /// replays winners; imports resolve last-writer-wins). So per
    /// `(collection, record)`:
    ///
    /// - **superseded events** (older than the winner) are dropped
    ///   unconditionally — any peer still holding one loses LWW against the
    ///   retained winner, so replays converge identically;
    /// - **winning upserts** are always retained — they *are* current state,
    ///   and a fresh device bootstraps from exactly this set;
    /// - **winning deletes** are retained until `acknowledged` passes their
    ///   offset (a lagging peer that still holds the record needs the delete
    ///   to arrive, otherwise its next push would resurrect the record).
    ///   `acknowledged` is the caller's peer horizon: a client passes its
    ///   export watermark (everything the server confirmed), a hub server
    ///   passes the minimum of its clients' pull checkpoints. Peers lagging
    ///   beyond the horizon must re-bootstrap.
    ///
    /// Every other stream — user event-sourcing streams, projections,
    /// broker, spatial audit — is retained untouched.
    ///
    /// Like `compact()`, this rewrites the event segment (offsets change),
    /// so the engine's sync export watermark resets to zero; re-exports are
    /// deduplicated by event id on the receiving side.
    pub fn trim_event_horizon(
        &mut self,
        acknowledged: SyncCheckpoint,
    ) -> Result<EventHorizonReport> {
        self.ensure_no_pending_transactions()?;

        let mut events = self.events.lock();
        let mut winners = BTreeMap::<(String, String), (Uuid, AuditOrder, bool, u64)>::new();
        for stored in events.read(RECORD_AUDIT_STREAM) {
            let Some(mutation) = AuditMutation::from_event_based(
                &self.sync_state.node_id,
                self.sync_state.origin_position_base,
                &stored,
            )?
            else {
                continue;
            };
            let key = (mutation.collection.clone(), mutation.record_id.clone());
            let is_delete = matches!(mutation.action, AuditAction::Delete);
            match winners.get(&key) {
                Some((_, existing_order, _, _)) if *existing_order >= mutation.order => {}
                _ => {
                    winners.insert(
                        key,
                        (stored.event.id, mutation.order, is_delete, stored.offset),
                    );
                }
            }
        }

        let mut report = EventHorizonReport {
            events_before: events.stored_events().len(),
            ..Default::default()
        };
        // Freeze every retained LOCAL event's origin position into its sync
        // metadata before offsets change, and advance the durable position
        // base past everything ever assigned — mesh vectors stay sound
        // through the rewrite (origin positions never move, never repeat).
        let base = self.sync_state.origin_position_base;
        let mut old_log_end: u64 = 0;
        let freeze = |stored: &StoredEvent| -> Result<Event> {
            if sync_mesh::envelope_from_metadata(&stored.event)?.is_some() {
                return Ok(stored.event.clone());
            }
            let envelope =
                sync_mesh::envelope_for_event_based(&self.sync_state.node_id, base, stored)?;
            event_with_sync_metadata(stored.event.clone(), &envelope)
        };
        let mut retained = Vec::with_capacity(events.stored_events().len());
        for stored in events.stored_events() {
            old_log_end = old_log_end.max(stored.offset.saturating_add(1));
            if stored.event.stream != RECORD_AUDIT_STREAM {
                retained.push(freeze(stored)?);
                continue;
            }
            let Some(mutation) = AuditMutation::from_event_based(
                &self.sync_state.node_id,
                self.sync_state.origin_position_base,
                stored,
            )?
            else {
                // Unrecognized record-audit subtype: retain conservatively.
                retained.push(stored.event.clone());
                continue;
            };
            let key = (mutation.collection, mutation.record_id);
            match winners.get(&key) {
                Some((winner_id, _, is_delete, offset)) if *winner_id == stored.event.id => {
                    if *is_delete && *offset <= acknowledged.event_offset {
                        report.deletes_dropped += 1;
                    } else {
                        retained.push(freeze(stored)?);
                    }
                }
                Some(_) => report.superseded_dropped += 1,
                None => retained.push(freeze(stored)?),
            }
        }

        report.events_after = retained.len();
        if report.superseded_dropped == 0 && report.deletes_dropped == 0 {
            return Ok(report);
        }

        let (bytes_before, bytes_after) = events.rewrite_events(retained)?;
        report.bytes_before = bytes_before;
        report.bytes_after = bytes_after;
        drop(events);
        self.invalidate_sync_vector_cache();

        // Offsets changed with the rewrite; reset the export watermark just
        // like compact() does (re-exports dedup by event id downstream), and
        // advance the origin-position base past every position ever
        // assigned so future local events stay monotonic for mesh vectors.
        self.sync_state.last_export_offset = 0;
        self.sync_state.last_reconciled_offset = 0;
        self.sync_state.origin_position_base = base.saturating_add(old_log_end);
        self.persist_sync_state()?;
        Ok(report)
    }

    pub fn compact_with_options(&mut self, options: CompactionOptions) -> Result<CompactionReport> {
        let started = Instant::now();
        let pause_started = Instant::now();
        if !options.allow_pending {
            self.ensure_no_pending_transactions()?;
        }
        let collections = self
            .collections()
            .into_iter()
            .map(|collection| collection.name)
            .collect::<Vec<_>>();
        let tx_log_before = file_len(&self.tx_log_path())?;
        let event_log_before = file_len(&event_segment_path_for(&self.path))?;
        let sync_log_before = file_len(&self.path.join("sync.log"))?;
        let index_metadata_bytes = file_len(&self.path.join(DEFAULT_INDEX_CATALOG))?;
        let sidecar_bytes = self.sidecar_bytes()?;
        let checkpoint_id = Uuid::new_v4().to_string();

        let mut report = CompactionReport {
            checkpoint_id: Some(checkpoint_id.clone()),
            transaction_log_bytes_before: tx_log_before,
            event_log_bytes_before: event_log_before,
            sync_log_bytes_before: sync_log_before,
            index_metadata_bytes,
            sidecar_bytes,
            ..Default::default()
        };
        self.write_compaction_checkpoint(
            &checkpoint_id,
            CompactionCheckpointPhase::Started,
            &collections,
            tx_log_before,
            event_log_before,
            sync_log_before,
        )?;
        for collection in collections {
            let collection_report = self.compact_collection_inner(&collection)?;
            let bytes_before = collection_report.bytes_before;
            report.live_records += collection_report.live_records;
            report.bytes_before += collection_report.bytes_before;
            report.bytes_after += collection_report.bytes_after;
            report.bytes_reclaimed += collection_report.bytes_reclaimed;
            report.collections.push(collection_report);
            throttle_compaction_io(&options, bytes_before)?;
        }
        report.bytes_scanned = report
            .bytes_before
            .saturating_add(tx_log_before)
            .saturating_add(event_log_before)
            .saturating_add(sync_log_before)
            .saturating_add(index_metadata_bytes)
            .saturating_add(sidecar_bytes);
        report.dead_records = estimate_dead_records(&report.collections);
        if !options.force
            && options.reclaim_threshold_percent > 0
            && reclaim_percent(report.bytes_before, report.bytes_reclaimed)
                < options.reclaim_threshold_percent
        {
            self.write_compaction_checkpoint(
                report.checkpoint_id.as_deref().unwrap_or(""),
                CompactionCheckpointPhase::Committed,
                &report
                    .collections
                    .iter()
                    .map(|collection| collection.collection.clone())
                    .collect::<Vec<_>>(),
                tx_log_before,
                event_log_before,
                sync_log_before,
            )?;
            report.transaction_log_bytes_after = tx_log_before;
            report.event_log_bytes_after = event_log_before;
            report.sync_log_bytes_after = sync_log_before;
            report.duration_ms = duration_millis(started.elapsed());
            report.pause_time_ms = duration_millis(pause_started.elapsed());
            return Ok(report);
        }

        let compacted_collections = report
            .collections
            .iter()
            .map(|collection| collection.collection.clone())
            .collect::<Vec<_>>();
        self.write_compaction_checkpoint(
            &checkpoint_id,
            CompactionCheckpointPhase::RecordsRewritten,
            &compacted_collections,
            tx_log_before,
            event_log_before,
            sync_log_before,
        )?;
        // Flush any commits whose durable write was still deferred before we
        // discard the log file, then drop the handle so the next commit reopens
        // the fresh (truncated) log. We hold the write lock, so no new commit can
        // enqueue concurrently.
        self.tx_log.flush_pending()?;
        truncate_log_file(&self.tx_log_path(), self.config.fsync)?;
        self.tx_log.invalidate();
        self.tx_states
            .retain(|_, state| *state == TxState::Committed);
        let (event_before, event_after, frozen_end) = {
            let mut events = self.events.lock();
            // Enforce configured broker retention before compacting so the
            // rewrite drops trimmable queue history in the same pass.
            events.broker().trim_configured()?;
            let frozen_end = self.freeze_local_event_positions(&mut events)?;
            let (before, after) = events.compact()?;
            (before, after, frozen_end)
        };
        self.invalidate_sync_vector_cache();
        let (sync_before, sync_after) = self.sync_log.get_mut().compact()?;
        self.sync_state.last_export_offset = 0;
        self.sync_state.last_reconciled_offset = 0;
        self.sync_state.origin_position_base = self
            .sync_state
            .origin_position_base
            .saturating_add(frozen_end);
        report.event_log_bytes_before = event_before;
        report.event_log_bytes_after = event_after;
        report.sync_log_bytes_before = sync_before;
        report.sync_log_bytes_after = sync_after;
        report.transaction_log_bytes_after = file_len(&self.tx_log_path())?;
        self.write_compaction_checkpoint(
            &checkpoint_id,
            CompactionCheckpointPhase::LogsCheckpointed,
            &compacted_collections,
            tx_log_before,
            event_log_before,
            sync_log_before,
        )?;
        self.persist_catalog()?;
        self.persist_index_catalog()?;
        self.persist_sync_state()?;
        self.write_compaction_checkpoint(
            &checkpoint_id,
            CompactionCheckpointPhase::Committed,
            &compacted_collections,
            tx_log_before,
            event_log_before,
            sync_log_before,
        )?;
        // We are now at a quiescent point: segments hold exactly the live records,
        // the WAL is truncated, and the heap equals that state. Publish the binary
        // startup snapshot LAST (it is the commit marker) so the next open can skip
        // the JSON segment reparse. Opt-in; a write failure is non-fatal (the open
        // path always falls back to segments).
        if self.config.snapshot {
            if let Err(error) = self.write_startup_snapshot() {
                eprintln!("[snapshot] write failed (ignored): {error}");
            }
        }
        report.duration_ms = duration_millis(started.elapsed());
        report.pause_time_ms = duration_millis(pause_started.elapsed());
        Ok(report)
    }

    /// Serialize every live record into the binary startup snapshot. Called at the
    /// end of a full [`Self::compact`]. See [`crate::snapshot`].
    pub(crate) fn write_startup_snapshot(&self) -> Result<()> {
        let watermark = self.last_commit_seq();
        let mut names: Vec<String> = self.collections.keys().cloned().collect();
        names.sort();

        let mut frames: Vec<Vec<u8>> = Vec::new();
        let mut collection_metas = Vec::with_capacity(names.len());
        for name in names {
            let state = self.collection_state(&name)?;
            let guards = state.read_all();
            let records: Vec<&StoredRecord> = guards
                .iter()
                .flat_map(|shard| shard.records.values())
                .map(|entry| entry.record.as_ref())
                .collect();
            let record_count = records.len() as u64;
            let segment_frame_count = state.segment_frame_count;
            frames.extend(crate::snapshot::encode_collection_chunks(&name, &records));
            drop(guards);
            drop(state);
            // File size AFTER the compaction rewrote this segment. On open we compare
            // the live segment size against this; a post-snapshot commit appends to
            // the segment and grows it, so a mismatch means "stale" -> fall back.
            let segment_byte_len = match fs::metadata(self.segment_path(&name)) {
                Ok(meta) => meta.len(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                Err(error) => return Err(error.into()),
            };
            collection_metas.push(crate::snapshot::SnapshotCollection {
                name,
                record_count,
                segment_frame_count,
                segment_byte_len,
            });
        }

        let manifest = crate::snapshot::SnapshotManifest {
            format_version: crate::snapshot::SNAPSHOT_FORMAT_VERSION,
            watermark_commit_seq: watermark,
            created_unix: unix_timestamp(),
            collections: collection_metas,
        };
        crate::snapshot::write_snapshot(
            &self.path,
            &manifest,
            &frames,
            self.config.fsync,
            &self.config.compression,
            &self.encryption,
        )
    }

    pub fn compact_collection(&mut self, collection: &str) -> Result<CompactionReport> {
        self.ensure_no_pending_transactions()?;
        self.ensure_collection(collection)?;
        let started = Instant::now();
        let collection_report = self.compact_collection_inner(collection)?;
        Ok(CompactionReport {
            live_records: collection_report.live_records,
            bytes_scanned: collection_report.bytes_before,
            bytes_before: collection_report.bytes_before,
            bytes_after: collection_report.bytes_after,
            bytes_reclaimed: collection_report.bytes_reclaimed,
            duration_ms: duration_millis(started.elapsed()),
            pause_time_ms: duration_millis(started.elapsed()),
            collections: vec![collection_report],
            ..CompactionReport::default()
        })
    }

    /// Machine-readable resident-memory accounting (see [`crate::residency`]).
    ///
    /// Phase 0 of `docs/server-paged-storage-todo.md`: every later gate in that
    /// roadmap is a bound on memory, and none can be evaluated without first
    /// attributing the bytes. The figures are estimates walked from the live
    /// structures — read the module docs before treating any of them as exact.
    ///
    /// This walks every resident record, so it is a diagnostic, not something to
    /// call per request. It takes only read locks and never blocks writers for
    /// longer than one shard at a time.
    pub fn residency_report(&self) -> Result<ResidencyReport> {
        let mut report = ResidencyReport::default();

        for (name, state) in &self.collections {
            let state = state.read();
            let mut collection = residency::CollectionResidency {
                name: name.clone(),
                ..Default::default()
            };
            for shard in state.read_all() {
                let shard = shard.residency();
                collection.record_count += shard.record_count;
                collection.version_count += shard.version_count;
                collection.rows_bytes += shard.rows_bytes;
                collection.version_chains_bytes += shard.version_chains_bytes;
                collection.primary_key_maps_bytes += shard.primary_key_maps_bytes;
            }
            collection.exact_vectors_bytes = state.vector_store.read().heap_bytes();
            report.collections.push(collection);
        }
        report.collections.sort_by(|a, b| a.name.cmp(&b.name));

        for (name, index) in &self.indexes {
            let index = index.read();
            report.indexes.push(residency::IndexResidency {
                name: name.clone(),
                collection: index.definition.collection.clone(),
                entry_count: index.store.total_entries() as u64,
                store_bytes: index.store.resident_bytes(),
                spatial_bytes: index.spatial.as_ref().map_or(0, |tree| {
                    residency::vec_bytes::<SpatialIndexEntry>(tree.size())
                }),
            });
        }
        report.indexes.sort_by(|a, b| a.name.cmp(&b.name));

        for (collection, index) in self.hnsw_indexes.read().iter() {
            report.hnsw.push(index.residency(collection));
        }
        report.hnsw.sort_by(|a, b| a.collection.cmp(&b.collection));

        report.graphs_bytes = self
            .graphs
            .read()
            .values()
            .map(|graph| std::mem::size_of::<GraphProjectionData>() as u64 + graph.heap_bytes())
            .sum();

        Ok(report.finalize(unix_timestamp()))
    }

    /// Return bounded operational telemetry for the server-paged engine.
    ///
    /// Embedded databases return `None`. This call never scans records, index
    /// entries, page contents, or WAL records and is safe for periodic metrics
    /// collection regardless of total database size.
    pub fn paged_storage_snapshot(&self) -> Result<Option<bicdb_page::PagedStoreSnapshot>> {
        self.paged_records
            .as_ref()
            .map(|paged| paged.storage_snapshot())
            .transpose()
    }

    /// Mirror a committed transaction's record writes into the paged store.
    ///
    /// No-op in `embedded_memory`. In `server_paged` this is what makes the page
    /// engine the durable home for rows: the same writes that updated the
    /// resident state are committed to pages under one transaction, so a crash
    /// recovers them through the page WAL rather than the segment log.
    pub(crate) fn apply_record_writes_to_paged(
        &self,
        collection: &str,
        writes: &[&TxWrite],
    ) -> Result<Option<bicdb_page::Xid>> {
        // B-tree index definitions on this collection: their durable entries
        // are maintained in the SAME paged transaction as the rows, which is
        // what makes the index crash-consistent by construction — recovery
        // keeps or discards row and entries together, so a restart never needs
        // a corpus-wide rebuild to trust them (Phase 4's exit gate).
        let index_defs = self.paged_durable_index_defs(collection)?;
        self.in_paged_transaction(|paged, xid| {
            // Pre-image reads for index maintenance: the row state before this
            // transaction, overlaid with earlier writes in this same batch so
            // an upsert-then-delete of one key within a batch resolves in
            // order.
            let pre_snapshot = bicdb_page::Snapshot {
                xid: 0,
                xmax: xid,
                in_flight: empty_in_flight(),
            };
            let mut batch_state: FxHashMap<&str, Option<Record>> = FxHashMap::default();
            for write in writes {
                let previous = match batch_state.get(write.record_id.as_str()) {
                    Some(previous) => previous.clone(),
                    None if index_defs.is_empty() => None,
                    None => paged.get(&pre_snapshot, collection, &write.record_id)?,
                };
                match write.op {
                    TxWriteOp::Upsert => {
                        let record = write.record()?.ok_or_else(|| BicDbError::Corruption {
                            path: self.tx_log_path(),
                            message: "transaction upsert missing record".to_string(),
                        })?;
                        let locator = paged.put(xid, collection, record)?;
                        apply_paged_index_upsert(
                            paged,
                            xid,
                            collection,
                            &index_defs,
                            previous.as_ref(),
                            record,
                            Some((locator, xid)),
                        )?;
                        batch_state.insert(write.record_id.as_str(), Some(record.as_ref().clone()));
                    }
                    TxWriteOp::Delete => {
                        paged.delete(xid, collection, &write.record_id)?;
                        if let Some(previous) = &previous {
                            apply_paged_index_delete(
                                paged,
                                xid,
                                collection,
                                &index_defs,
                                previous,
                                &write.record_id,
                            )?;
                        }
                        batch_state.insert(write.record_id.as_str(), None);
                    }
                }
            }
            Ok(())
        })
    }

    /// Names and fields of the B-tree indexes on `collection` whose entries are
    /// durably maintained in the page store. B-tree entries are one composite
    /// key per row; full-text entries are one key per TERM of the row's
    /// materialized projection. Spatial, array, and jsonb kinds remain
    /// resident-only for now; their durable form is the rest of Phase 4.
    pub(crate) fn paged_durable_index_defs(
        &self,
        collection: &str,
    ) -> Result<Vec<PagedDurableIndexDef>> {
        if self.paged_records.is_none() {
            return Ok(Vec::new());
        }
        self.indexes
            .values()
            .filter_map(|state| {
                let state = state.read();
                // A spatial index maintains a durable delta tail when packed
                // OR while a packed build is in flight (the delta is what
                // makes resuming that build sound); a plain resident spatial
                // index writes nothing durable.
                (state.definition.collection == collection
                    && (matches!(
                        state.definition.kind,
                        IndexKind::BTree | IndexKind::FullText
                    ) || (state.definition.kind == IndexKind::Spatial
                        && (state.packed_spatial.is_some() || state.spatial_delta_durable))))
                    .then(|| {
                        // The format read must not fail silently: writing v2
                        // entries into a v3 index is corruption, not degradation.
                        let entry_format = match self.paged_records.as_ref() {
                            Some(paged) => paged.entry_format_cached(&state.definition.name)?,
                            None => crate::paged_collection::IndexEntryFormat::V2,
                        };
                        Ok(PagedDurableIndexDef {
                            name: state.definition.name.clone(),
                            fields: state.definition.fields.clone(),
                            kind: state.definition.kind.clone(),
                            predicate: state.definition.predicate.clone(),
                            entry_format,
                            unique: state.definition.unique,
                        })
                    })
            })
            .collect()
    }

    /// Run `apply` against the paged store inside one transaction, committing on
    /// success and aborting on any error.
    ///
    /// No-op in `embedded_memory`, where `apply` is never called.
    ///
    /// Aborting rather than leaving the transaction open matters: an abandoned
    /// paged transaction never commits, so its versions stay invisible forever
    /// while still occupying pages. Every write path into the paged store goes
    /// through here so that discipline is stated once rather than repeated —
    /// getting it wrong in one path is a silent partial write.
    /// Returns the paged transaction id on success (`None` in embedded mode),
    /// which is what eviction stubs pin their as-of reads to.
    pub(crate) fn in_paged_transaction<F>(&self, apply: F) -> Result<Option<bicdb_page::Xid>>
    where
        F: FnOnce(&crate::paged_collection::PagedRecords, bicdb_page::Xid) -> Result<()>,
    {
        let Some(paged) = &self.paged_records else {
            return Ok(None);
        };
        // Held across begin..commit so no other batch's operations interleave
        // with this one — see `PagedStore::write_guard`. Without it, two
        // core-committed batches could interleave puts and commits, producing
        // spurious conflicts, orphaned versions, and stubs pinned to
        // transactions that do not contain their own rows.
        let _guard = paged.write_guard();
        let (xid, _) = paged.begin();
        match apply(paged, xid) {
            Ok(()) => {
                paged.commit(xid)?;
                Ok(Some(xid))
            }
            Err(error) => {
                let _ = paged.abort(xid);
                Err(error)
            }
        }
    }

    /// The eviction context for rows written to `collection` by paged
    /// transaction `xid`: a shared fetch that resolves a primary key to its
    /// record **as of that transaction**, pinned so a superseded version's stub
    /// keeps answering with the value it had when it was current (see
    /// `EvictedPayload`). One allocation per commit batch, shared by every row
    /// in it.
    pub(crate) fn paged_stub_fetch(
        &self,
        collection: &str,
        xid: bicdb_page::Xid,
    ) -> Option<PagedStubFetch> {
        Some(self.paged_apply_ctx(collection, xid)?.post)
    }

    /// Both pinned fetches for one commit batch: `post` (as of the batch's own
    /// paged transaction) and `pre` (the instant before it — the pre-image used
    /// to seed baselines on lazy collections). See [`PagedApplyCtx`].
    pub(crate) fn paged_apply_ctx(
        &self,
        collection: &str,
        xid: bicdb_page::Xid,
    ) -> Option<PagedApplyCtx> {
        let paged = Arc::clone(self.paged_records.as_ref()?);
        let collection: Arc<str> = Arc::from(collection);
        let post_snapshot = bicdb_page::Snapshot {
            xid,
            xmax: xid + 1,
            in_flight: empty_in_flight(),
        };
        // `xmax = xid` excludes the transaction itself: everything committed
        // strictly before it — the pre-image. `xid: 0` (a read outside any
        // transaction), NOT the batch's xid: a snapshot's own transaction sees
        // its own writes, which is precisely what the pre-image must not.
        let pre_snapshot = bicdb_page::Snapshot {
            xid: 0,
            xmax: xid,
            in_flight: empty_in_flight(),
        };
        let post = {
            let paged = Arc::clone(&paged);
            let collection = Arc::clone(&collection);
            Arc::new(move |pk: &str| paged.get(&post_snapshot, &collection, pk)) as PagedStubFetch
        };
        let pre =
            Arc::new(move |pk: &str| paged.get(&pre_snapshot, &collection, pk)) as PagedStubFetch;
        Some(PagedApplyCtx { xid, post, pre })
    }

    pub fn stats(&self) -> Result<DbStats> {
        let mut collections = self
            .collections
            .values()
            .map(|state| {
                let state = state.read();
                let segment_bytes = storage::seek_to_end(&self.segment_path(&state.meta.name))?;
                // In paged mode the page store is the complete view (shards may
                // be a touched-rows cache), so count and logical bytes come
                // from one scan of it. O(rows) I/O per call, bounded by the
                // buffer pool — stats is an operator action, not a hot path.
                if let Some(paged) = &self.paged_records {
                    let snapshot = paged.latest_snapshot();
                    let mut record_count = 0usize;
                    let mut logical_record_bytes = 0u64;
                    for record in paged.scan(&snapshot, &state.meta.name)? {
                        let record = record?;
                        record_count += 1;
                        logical_record_bytes += serde_json::to_vec(&record)?.len() as u64;
                    }
                    return Ok(CollectionStats {
                        name: state.meta.name.clone(),
                        mode: state.meta.mode.clone(),
                        vector_dim: state.meta.vector_dim,
                        record_count,
                        segment_bytes,
                        logical_record_bytes,
                        storage_overhead_bytes: segment_bytes.saturating_sub(logical_record_bytes),
                        last_segment_offset: None,
                    });
                }
                // `wire_bytes_len` rather than serializing the entry directly:
                // the resident entry may be an eviction stub, and the caller
                // asked how big the rows are, not the resident representation.
                let logical_record_bytes = state
                    .read_all()
                    .iter()
                    .flat_map(|shard| shard.records.values())
                    .map(|entry| entry.record.wire_bytes_len())
                    .collect::<std::result::Result<Vec<_>, _>>()?
                    .into_iter()
                    .sum();
                let last_segment_offset = state
                    .read_all()
                    .iter()
                    .flat_map(|shard| shard.records.values())
                    .map(|entry| entry.offset)
                    .max();
                Ok(CollectionStats {
                    name: state.meta.name.clone(),
                    mode: state.meta.mode.clone(),
                    vector_dim: state.meta.vector_dim,
                    record_count: state.record_count(),
                    segment_bytes,
                    logical_record_bytes,
                    storage_overhead_bytes: segment_bytes.saturating_sub(logical_record_bytes),
                    last_segment_offset,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        collections.sort_by(|left, right| left.name.cmp(&right.name));

        let record_count = collections
            .iter()
            .map(|collection| collection.record_count)
            .sum();

        Ok(DbStats {
            path: self.path.clone(),
            collection_count: collections.len(),
            record_count,
            pending_sync_ops: self.pending_sync_ops().len(),
            size_bytes: storage::total_dir_size(&self.path)?,
            collections,
        })
    }

    pub fn flush(&self) -> Result<()> {
        if self.config.fsync {
            return Ok(());
        }
        format::persist_current(&self.path, self.config.fsync)?;
        self.persist_catalog()?;
        self.persist_sync_state()?;
        self.persist_ha_state()?;
        for name in self.collections.keys() {
            storage::sync_file(&self.segment_path(name))?;
        }
        storage::sync_file(&self.tx_log_path())?;
        self.events.lock().flush()?;
        self.sync_log.lock().flush()
    }

    /// Everything [`Self::close`] makes durable, without closing: the paged
    /// store checkpoints (bounding its WAL), the core transaction log is
    /// bounded, and segment/log files flush. Bulk importers call this as a
    /// resume barrier between batches — the previous pattern of
    /// close+reopen per checkpoint paid a full recovery scan every cycle,
    /// and the cost grows with the store.
    pub fn checkpoint_for_resume(&self) -> Result<()> {
        if let Some(paged) = &self.paged_records {
            paged.checkpoint()?;
            let log_path = self.tx_log_path();
            let log_len = file_len(&log_path)?;
            if log_len > 0 {
                copy_transaction_log_tail(&log_path, log_len, self.config.fsync)?;
            }
        }
        self.flush()
    }

    /// Online, checkpoint-consistent backup of a live paged database:
    /// TWO chained artifacts, streamed while writes continue.
    ///
    /// 1. `base_path` — a full archive whose page segments and active WAL
    ///    are copied fuzzily (size-pinned, unhashed) under a backup pin
    ///    that defers WAL truncation and page-tail reclaim. Pages stream
    ///    BEFORE any WAL byte.
    /// 2. `wal_tail_path` — after the base completes, the active WAL is
    ///    sealed and an incremental ships the sealed, immutable segment
    ///    chain. Because the WAL is a physical page-image log with a
    ///    writeback barrier, that chain covers every page state the base
    ///    copy could have observed.
    ///
    /// Restore applies both in order; opening the restored directory
    /// replays the sealed chain and truncates the base's stale active-WAL
    /// copy at its LSN discontinuity — exactly a crash at the seal point.
    /// Requires an extent-segmented store (`bicdb store segment` migrates).
    pub fn create_online_backup(
        &self,
        base_path: impl AsRef<Path>,
        wal_tail_path: impl AsRef<Path>,
        options: crate::backup::BackupCreateOptions,
    ) -> Result<OnlineBackupReport> {
        let Some(paged) = &self.paged_records else {
            return Err(BicDbError::Backup(
                "online backup requires server_paged storage".to_string(),
            ));
        };
        online_backup_pinned(
            paged,
            &self.path,
            base_path.as_ref(),
            wal_tail_path.as_ref(),
            options,
            // Materialize dirty pages and bound the core log first: the base
            // is as close to the pin point as a checkpoint can make it, and
            // the pin keeps the WAL covering everything after it.
            || self.checkpoint_for_resume(),
        )
    }

    /// Continuous WAL archiving: seal the active WAL (the cut), copy every
    /// sealed segment the archive does not already hold into
    /// `destination`, fsync, and release the copied segments for local
    /// checkpoint truncation. Call on a cadence — the archive's freshness
    /// IS the recovery point objective.
    ///
    /// The first call enables archive retention for this handle's
    /// lifetime: sealed segments survive local truncation until archived,
    /// so a stalled archiver grows the local WAL directory (deliberately —
    /// the PostgreSQL `archive_command` trade) rather than gapping the
    /// archive. Roll a restore forward with [`apply_archived_wal`]:
    /// restore an online-backup chain, apply the archive, open.
    pub fn archive_wal_segments(&self, destination: impl AsRef<Path>) -> Result<WalArchiveReport> {
        let Some(paged) = &self.paged_records else {
            return Err(BicDbError::Backup(
                "WAL archiving requires server_paged storage".to_string(),
            ));
        };
        let destination = destination.as_ref();
        fs::create_dir_all(destination)?;
        let store = paged.store();
        store.enable_wal_archive_retention();
        let sealed = store
            .seal_wal_for_backup()
            .map_err(|error| BicDbError::Backup(error.to_string()))?;
        let mut report = WalArchiveReport {
            archived: 0,
            bytes: 0,
            archived_through: None,
            skipped: 0,
        };
        for path in sealed {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| {
                    BicDbError::Backup(format!(
                        "sealed WAL segment {} has no readable name",
                        path.display()
                    ))
                })?
                .to_string();
            let sequence: u64 = name
                .rsplit('.')
                .next()
                .and_then(|suffix| suffix.parse().ok())
                .ok_or_else(|| {
                    BicDbError::Backup(format!("sealed WAL segment {name} has no sequence"))
                })?;
            let target = destination.join(&name);
            let source_len = fs::metadata(&path)?.len();
            match fs::metadata(&target) {
                Ok(existing) if existing.len() == source_len => {
                    report.skipped += 1;
                }
                Ok(existing) => {
                    return Err(BicDbError::Backup(format!(
                        "archive already holds {name} with {} bytes but the store's copy has                          {source_len}; refusing to overwrite an archived segment",
                        existing.len()
                    )));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    // Copy through a temp name + rename so a torn copy can
                    // never be mistaken for an archived segment.
                    let staging = destination.join(format!("{name}.partial"));
                    fs::copy(&path, &staging)?;
                    let copied = File::open(&staging)?;
                    copied.sync_all()?;
                    drop(copied);
                    fs::rename(&staging, &target)?;
                    report.archived += 1;
                    report.bytes = report.bytes.saturating_add(source_len);
                }
                Err(error) => return Err(error.into()),
            }
            report.archived_through = Some(
                report
                    .archived_through
                    .map_or(sequence, |max| max.max(sequence)),
            );
        }
        if let Ok(dir) = File::open(destination) {
            let _ = dir.sync_all();
        }
        if let Some(through) = report.archived_through {
            store.mark_wal_archived_through(through);
        }
        Ok(report)
    }

    /// Begin migrating a live monolithic paged store to `extent_bytes`
    /// segment files WITHOUT downtime. Drive it to completion with repeated
    /// [`Self::advance_extent_migration`] calls (a maintenance loop);
    /// crash-safe and resumable — progress is durable in the superblock.
    pub fn begin_extent_migration(&self, extent_bytes: u64) -> Result<()> {
        let Some(paged) = &self.paged_records else {
            return Err(BicDbError::PagedStorage(
                "extent migration requires server_paged storage".to_string(),
            ));
        };
        paged
            .store()
            .buffer_pool()
            .store()
            .begin_extent_migration(extent_bytes)
            .map_err(|error| BicDbError::PagedStorage(error.to_string()))
    }

    /// One bounded online-migration step; see [`Self::begin_extent_migration`].
    pub fn advance_extent_migration(
        &self,
        max_pages: u64,
    ) -> Result<bicdb_page::ExtentMigrationReport> {
        let Some(paged) = &self.paged_records else {
            return Err(BicDbError::PagedStorage(
                "extent migration requires server_paged storage".to_string(),
            ));
        };
        paged
            .store()
            .buffer_pool()
            .store()
            .advance_extent_migration(max_pages)
            .map_err(|error| BicDbError::PagedStorage(error.to_string()))
    }

    pub fn close(self) -> Result<()> {
        // Paged mode: bound the core transaction log at the session boundary,
        // same contract as the truncation at open (see there for why nothing
        // else bounds it in this mode). Checkpoint the page store first so the
        // log's contents are durably past needing before they are dropped.
        if let Some(paged) = &self.paged_records {
            paged.checkpoint()?;
            let log_path = self.tx_log_path();
            let log_len = file_len(&log_path)?;
            if log_len > 0 {
                copy_transaction_log_tail(&log_path, log_len, self.config.fsync)?;
            }
        }
        self.flush()
    }

    pub fn format_metadata(&self) -> Result<FormatMetadata> {
        format::load_metadata(&self.path)?.ok_or_else(|| {
            BicDbError::FormatCompatibility(format!(
                "database is missing required {DEFAULT_FORMAT_METADATA}"
            ))
        })
    }

    pub fn plan_format_migration(path: impl AsRef<Path>) -> Result<FormatMigrationPlan> {
        format::plan_migration(path.as_ref(), true)
    }

    pub(crate) fn ensure_writable(&self, operation: &str) -> Result<()> {
        if self.ha_state.role == HaRole::Standby
            || (self.config.replication.enabled
                && self.config.replication.mode == ReplicationMode::Standby)
        {
            return Err(BicDbError::ReadOnlyStandby(format!(
                "{operation} is refused on a hot standby/read replica"
            )));
        }
        Ok(())
    }

    pub(crate) fn persist_ha_state(&self) -> Result<()> {
        persist_ha_state(&self.path, &self.ha_state, self.config.fsync)
    }

    pub(crate) fn persist_consensus_state(&self) -> Result<()> {
        persist_consensus_state(&self.path, &self.consensus_state, self.config.fsync)
    }

    /// Applies a transaction (conflict check, in-memory apply) and enqueues its
    /// WAL bytes in commit-sequence order. Returns the commit sequence. The WAL
    /// is NOT fsync'd here; the caller must call `tx_log_handle().write_durable(
    /// seq)` (after releasing the database write lock, so concurrent commits
    /// pipeline and coalesce) before acknowledging the commit.
    pub(crate) fn validate_native_commit_validators(&self, tx: &Transaction) -> Result<()> {
        for validator in &tx.commit_validators {
            match validator {
                NativeCommitValidator::AppendOnly { relation } => {
                    for write in tx
                        .writes
                        .iter()
                        .filter(|write| &write.collection == relation)
                    {
                        if write.op == TxWriteOp::Delete
                            || self.get_unchecked(relation, &write.record_id)?.is_some()
                        {
                            return Err(BicDbError::CommitValidation(format!(
                                "append-only relation `{relation}` rejected mutation of `{}`",
                                write.record_id
                            )));
                        }
                    }
                }
                NativeCommitValidator::ImmutableFields { relation, fields } => {
                    for write in tx
                        .writes
                        .iter()
                        .filter(|write| &write.collection == relation)
                    {
                        let (Some(before), Some(after)) = (
                            self.get_unchecked(relation, &write.record_id)?,
                            write.record()?.map(Arc::as_ref),
                        ) else {
                            continue;
                        };
                        for field in fields {
                            if before.metadata.get(field) != after.metadata.get(field) {
                                return Err(BicDbError::CommitValidation(format!(
                                    "immutable field `{field}` changed on `{relation}`/`{}`",
                                    write.record_id
                                )));
                            }
                        }
                    }
                }
                NativeCommitValidator::TenantWorkspaceImmutable {
                    relation,
                    tenant_field,
                    workspace_field,
                } => {
                    for write in tx
                        .writes
                        .iter()
                        .filter(|write| &write.collection == relation)
                    {
                        let (Some(before), Some(after)) = (
                            self.get_unchecked(relation, &write.record_id)?,
                            write.record()?.map(Arc::as_ref),
                        ) else {
                            continue;
                        };
                        for (boundary, field) in [
                            ("tenant", tenant_field.as_ref()),
                            ("workspace", workspace_field.as_ref()),
                        ] {
                            if field.is_some_and(|field| {
                                before.metadata.get(field) != after.metadata.get(field)
                            }) {
                                return Err(BicDbError::CommitValidation(format!(
                                    "{boundary} field is immutable on `{relation}`/`{}`",
                                    write.record_id
                                )));
                            }
                        }
                    }
                }
                NativeCommitValidator::OptimisticVersion { relation, field } => {
                    for write in tx
                        .writes
                        .iter()
                        .filter(|write| &write.collection == relation)
                    {
                        let (Some(before), Some(after)) = (
                            self.get_unchecked(relation, &write.record_id)?,
                            write.record()?.map(Arc::as_ref),
                        ) else {
                            continue;
                        };
                        let previous = before
                            .metadata
                            .get(field)
                            .and_then(Value::as_u64)
                            .ok_or_else(|| {
                                BicDbError::CommitValidation(format!(
                                    "`{relation}`/`{}` lacks version field `{field}`",
                                    write.record_id
                                ))
                            })?;
                        let next = after
                            .metadata
                            .get(field)
                            .and_then(Value::as_u64)
                            .ok_or_else(|| {
                                BicDbError::CommitValidation(format!(
                                    "`{relation}`/`{}` lacks version field `{field}`",
                                    write.record_id
                                ))
                            })?;
                        if next != previous.saturating_add(1) {
                            return Err(BicDbError::CommitValidation(format!(
                                "`{relation}`/`{}` version must advance from {previous} to {}",
                                write.record_id,
                                previous.saturating_add(1)
                            )));
                        }
                    }
                }
                NativeCommitValidator::LedgerBalanced {
                    relation,
                    subject_field,
                    debit_field,
                    credit_field,
                } => {
                    let records = resulting_records_for_relation(self, tx, relation)?;
                    let mut balances = BTreeMap::<String, (f64, f64)>::new();
                    for record in records {
                        let Some(subject) =
                            record.metadata.get(subject_field).and_then(Value::as_str)
                        else {
                            continue;
                        };
                        let debit = json_number(record.metadata.get(debit_field)).unwrap_or(0.0);
                        let credit = json_number(record.metadata.get(credit_field)).unwrap_or(0.0);
                        let entry = balances.entry(subject.to_string()).or_default();
                        entry.0 += debit;
                        entry.1 += credit;
                    }
                    for (subject, (debit, credit)) in balances {
                        if (debit - credit).abs() > 1e-9 {
                            return Err(BicDbError::CommitValidation(format!(
                                "ledger `{relation}` is unbalanced for `{subject}`: debit={debit} credit={credit}"
                            )));
                        }
                    }
                }
                NativeCommitValidator::AggregateInvariant {
                    relation,
                    subject_field,
                    value_field,
                    minimum,
                    maximum,
                } => {
                    if !minimum.is_finite() || !maximum.is_finite() || minimum > maximum {
                        return Err(BicDbError::CommitValidation(format!(
                            "aggregate validator for `{relation}` has invalid bounds"
                        )));
                    }
                    let records = resulting_records_for_relation(self, tx, relation)?;
                    let mut totals = BTreeMap::<String, f64>::new();
                    for record in records {
                        let Some(subject) =
                            record.metadata.get(subject_field).and_then(Value::as_str)
                        else {
                            continue;
                        };
                        *totals.entry(subject.to_string()).or_default() +=
                            json_number(record.metadata.get(value_field)).unwrap_or(0.0);
                    }
                    for (subject, total) in totals {
                        if total < *minimum || total > *maximum {
                            return Err(BicDbError::CommitValidation(format!(
                                "aggregate invariant failed for `{relation}` subject `{subject}`: {total} not in [{minimum}, {maximum}]"
                            )));
                        }
                    }
                }
                NativeCommitValidator::FinanceGuard {
                    relation,
                    amount_field,
                    maximum_absolute_amount,
                } => {
                    if !maximum_absolute_amount.is_finite() || *maximum_absolute_amount < 0.0 {
                        return Err(BicDbError::CommitValidation(format!(
                            "finance guard for `{relation}` has invalid maximum"
                        )));
                    }
                    for write in tx
                        .writes
                        .iter()
                        .filter(|write| &write.collection == relation)
                    {
                        let Some(record) = write.record()?.map(Arc::as_ref) else {
                            continue;
                        };
                        let amount =
                            json_number(record.metadata.get(amount_field)).ok_or_else(|| {
                                BicDbError::CommitValidation(format!(
                                    "`{relation}`/`{}` lacks numeric `{amount_field}`",
                                    write.record_id
                                ))
                            })?;
                        if !amount.is_finite() || amount.abs() > *maximum_absolute_amount {
                            return Err(BicDbError::CommitValidation(format!(
                                "finance guard rejected `{relation}`/`{}` amount {amount}",
                                write.record_id
                            )));
                        }
                    }
                }
                NativeCommitValidator::ApplicationInvariant { definition } => {
                    validate_carrier_invariant(self, tx, definition)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_serializable_reads(&self, tx: &Transaction) -> Result<()> {
        if tx.isolation != TransactionIsolation::Serializable {
            return Ok(());
        }
        for relation in &tx.serializable_reads {
            let expected = tx
                .serializable_generation_snapshot
                .get(relation)
                .copied()
                .unwrap_or_default();
            let actual = self.collection_generation(relation);
            if actual != expected {
                return Err(BicDbError::TransactionConflict(format!(
                    "serializable relation `{relation}` changed after transaction {} snapshot",
                    tx.id.0
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn commit_transaction(&self, tx: &mut Transaction) -> Result<u64> {
        if tx.state != TxState::Pending {
            return Err(BicDbError::TransactionNotPending);
        }
        self.ensure_writable_by_consensus(tx)?;
        let _serializable_shared = (tx.isolation != TransactionIsolation::Serializable)
            .then(|| self.serializable_commit_admission.read());
        let _serializable_exclusive = (tx.isolation == TransactionIsolation::Serializable)
            .then(|| self.serializable_commit_admission.write());
        // CONCURRENT COMMIT (no global commit_lock). Correctness rests on four
        // independent mechanisms, each guarding exactly what it must:
        //  * same-record W-W: optimistic per-record `write_locks` (a 2nd writer of
        //    a record errors at write time), so two committers never apply the same
        //    record concurrently -> per-shard conflict-check + apply is atomic per
        //    record. The version-chain watermark is bumped during apply, before the write lock
        //    is released below, so a later writer's conflict check sees it.
        //  * unique-index validate+apply atomicity: done under the touched indexes'
        //    write locks, held across the validate-all-then-apply-all section
        //    (`validate_and_apply_index_mutations`), acquired in canonical name
        //    order so concurrent committers cannot deadlock.
        //  * WAL ordering: `tx_log.enqueue` is keyed by commit_seq into a BTreeMap
        //    and `drain_contiguous` only emits a gap-free prefix, so seqs may be
        //    assigned/enqueued out of order; a plain atomic fetch_add suffices.
        //  * MVCC visibility: `mark_committed_seq` advances the watermark only
        //    across the contiguous applied prefix, so a snapshot never observes a
        //    seq before every lower seq is fully applied (records included).
        let mut timer = commit_trace::Timer::start();
        // Repair-carrying writes lock and (if the row moved past the snapshot)
        // rebuild their records FIRST, so every later stage — index mutations,
        // conflict detection, WAL encode, apply — sees the final record.
        self.repair_conflicting_delta_writes(tx)?;
        let writes_by_collection = tx_writes_by_collection(&tx.writes);
        let structural_schema_records_changed = writes_by_collection
            .keys()
            .any(|collection| CLUSTER_SCHEMA_CATALOG_COLLECTIONS.contains(collection));
        let mut index_mutations_by_collection =
            self.tx_index_mutations_by_collection(&writes_by_collection, tx.writes_are_unique())?;
        timer.lap(&commit_trace::INDEX_BUILD);
        // Plan vector dimensions per collection (abort point: dimension mismatch).
        // Tells the apply path whether a collection needs the rare exclusive
        // (dimension-changing) path or the common concurrent read-guard path.
        let dim_plan = self.plan_vector_dims(&writes_by_collection)?;
        // A/B gate: when `BICDB_GLOBAL_COMMIT_LOCK` is set, serialize the entire
        // commit critical section under one global mutex, reproducing the old
        // pre-removal `commit_lock` baseline (conflict check + index validate/apply
        // + seq assignment + record apply + watermark publish all run as one). The
        // guard wraps the existing per-shard/per-index locks (it only serializes,
        // never reorders acquisition) and drops at function scope. When the gate is
        // off (default) the lock is never taken and this is a single relaxed load,
        // leaving the concurrent path byte-identical to before. Held from here so a
        // committer's full apply completes before the next committer begins.
        let has_carrier_invariants = tx.commit_validators.iter().any(|validator| {
            matches!(
                validator,
                NativeCommitValidator::ApplicationInvariant { .. }
            )
        });
        let _commit_guard = if global_commit_lock_enabled() || has_carrier_invariants {
            Some(self.commit_lock.lock())
        } else {
            None
        };
        self.detect_commit_conflicts(tx)?;
        self.validate_serializable_reads(tx)?;
        // Cross-record BicDB application invariants must observe a stable committed set
        // through record apply. Transactions carrying them therefore share the
        // global commit boundary above; ordinary transactions retain the
        // disjoint-write concurrent path.
        self.validate_native_commit_validators(tx)?;
        timer.lap(&commit_trace::CONFLICT);
        // Resolve each mutation's record locator for validation. Existing records
        // (updates/deletes) resolve now; brand-new records are still unallocated
        // (`None`) and validated in pk-space.
        self.fill_all_index_mutation_rowids(&mut index_mutations_by_collection)?;
        // Index uniqueness VALIDATION (abort point: unique violation), under the
        // touched indexes' READ locks. The mutating apply is deferred until after
        // record apply (below), once every record's locator is allocated. This is
        // the ONLY commit-vs-commit serialization point, and only for commits whose
        // index sets overlap; record apply runs fully concurrently.
        let _unique_claims = self.claim_unique_keys(tx, &index_mutations_by_collection)?;
        let _exclusion_claims = self.claim_exclusion_ranges(tx, &index_mutations_by_collection)?;
        self.validate_index_mutations(&index_mutations_by_collection)?;
        let admission = if tx.bypass_commit_admission || tx.writes.is_empty() {
            None
        } else {
            let admission = self.commit_admission.read().clone();
            if admission.is_none() && self.config.require_commit_admission {
                return Err(BicDbError::Cluster(
                    "distributed write rejected: range-quorum commit authority is not ready"
                        .to_string(),
                ));
            }
            admission
        };
        let admission_ticket = if let Some(admission) = admission.as_ref() {
            let mutations = tx
                .writes
                .iter()
                .map(|write| {
                    let record = match write.op {
                        TxWriteOp::Upsert => Some(
                            write
                                .record()?
                                .map(|record| (**record).clone())
                                .ok_or_else(|| {
                                    BicDbError::Cluster(format!(
                                        "distributed upsert {}/{} has no record payload",
                                        write.collection, write.record_id
                                    ))
                                })?,
                        ),
                        TxWriteOp::Delete => None,
                    };
                    Ok(CommitAdmissionMutation {
                        collection: write.collection.clone(),
                        record_id: write.record_id.clone(),
                        record,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Some(admission.admit(&CommitAdmissionIntent {
                transaction_id: tx.id.0,
                mutations,
            })?)
        } else {
            None
        };
        let sync_ops = self.sync_ops_for_commit(&index_mutations_by_collection)?;
        if !sync_ops.is_empty() {
            // TODO: v0.2 should atomically group transaction commits and sync-log events.
            self.sync_log.lock().append_ops(&sync_ops)?;
        }
        let record_audit_events =
            self.record_audit_events_for_commit(&index_mutations_by_collection)?;
        test_commit_pause_after_validate();
        timer.lap(&commit_trace::VALIDATE);
        // Past every abort point: assign the sequence and enqueue the WAL bytes.
        // A plain atomic add is monotonic; the seq-keyed writer queue tolerates
        // out-of-order enqueue. Deferred durable write coalesces in the caller.
        let commit_seq = self.commit_seq.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        test_commit_pause_after_seq_reserved();
        if repair_trace_filter().is_some() {
            for write in &tx.writes {
                if let Some(record) = write.record()?.map(Arc::as_ref) {
                    repair_trace(
                        &write.collection,
                        &write.record_id,
                        tx.id.0,
                        write.statement_snapshot,
                        commit_seq,
                        || {
                            format!(
                                "seq_assigned repair={} meta={}",
                                write.repair.is_some(),
                                record.metadata
                            )
                        },
                    );
                }
            }
        }
        let prepared = std::mem::take(&mut tx.prepared_wal);
        // LOGICAL LOGGING (paged mode): the page store's own WAL carries every
        // row durably before `paged.commit` returns — inside the apply below,
        // BEFORE any client can be acknowledged. Writing the same records to
        // the core log again doubled the write volume of every commit for a
        // replay path paged recovery already covers. The core log keeps only a
        // tiny CommitMaterialized marker: recovery still learns the seq (for
        // `last_commit_seq` continuity) and that there is nothing to replay.
        //
        // The enqueue stays at THIS point — before the apply — deliberately:
        // `write_durable` drains only the contiguous prefix, so a later
        // enqueue would stretch the window in which a concurrent committer's
        // durability flush cannot pass this seq. A crash after the marker is
        // durable but before the paged apply finishes loses only a transaction
        // no client was ever acknowledged for (acknowledgment follows the
        // apply); a FAILED apply goes through `revoke_enqueued_commit`, whose
        // Abort frame overrides the marker in recovery's fold exactly as it
        // overrides a full Commit.
        // EXCEPTION: replication standbys are fed Write frames from this very
        // log (`replication_commits_from_retained_wal`), so a replicating
        // primary must keep logging full records — markers would export empty
        // commits and silently starve standbys.
        let pending_publishes = tx.pending_broker_publishes.lock().clone();
        // The sequence above is already public, but this encoding is fallible.
        // If it fails we must still occupy the queue slot: `drain_contiguous`
        // only advances across an unbroken prefix, so an abandoned sequence
        // would strand every later commit's durability behind a gap that never
        // fills. Nothing was applied and no client is acknowledged, so an empty
        // slot is exactly right — it carries no frames into recovery.
        let encoded: Result<Vec<u8>> = (|| {
            let wal_bytes = if self.paged_records.is_some() && !self.config.replication.enabled {
                let mut payloads = pending_publishes
                    .iter()
                    .map(|publish| {
                        serde_json::to_vec(&TxFrameRef::BrokerPublish {
                            tx_id: tx.id,
                            publish,
                        })
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                payloads.extend(
                    record_audit_events
                        .iter()
                        .map(|event| {
                            serde_json::to_vec(&TxFrameRef::RecordAudit {
                                tx_id: tx.id,
                                event,
                            })
                        })
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                );
                payloads.push(serde_json::to_vec(&TxFrame::CommitMaterialized {
                    tx_id: tx.id,
                    timestamp: unix_timestamp(),
                    commit_seq,
                })?);
                storage::encode_frames_to_bytes(
                    &self.tx_log_path(),
                    FrameKind::Transaction,
                    &payloads,
                    &CompressionConfig::disabled(),
                    &self.encryption,
                )?
            } else {
                self.encode_tx_commit_frames(
                    tx.id,
                    commit_seq,
                    &tx.writes,
                    &pending_publishes,
                    &record_audit_events,
                    prepared,
                )?
            };
            Ok(wal_bytes)
        })();
        let wal_bytes = match encoded {
            Ok(bytes) => bytes,
            Err(error) => {
                self.tx_log.enqueue(commit_seq, Vec::new());
                // This reserved sequence represents no applied mutation, but
                // it is nevertheless complete. Leaving it absent from the
                // applied watermark permanently hides every later successful
                // commit behind an impossible gap.
                self.mark_committed_seq(commit_seq);
                return Err(error);
            }
        };
        self.tx_log.enqueue(commit_seq, wal_bytes);
        timer.lap(&commit_trace::WAL_ENCODE);
        // Record/version/vector apply: per-shard write locks under the shared
        // collection read guard, so disjoint-key commits (even within one
        // collection) apply in parallel. This allocates each brand-new record's
        // rowid (the index payload resolved next).
        let apply_result = if std::env::var_os("BICDB_TEST_FAIL_COMMIT_APPLY").is_some() {
            // Test failpoint: the only way to exercise the revoke path below
            // deterministically. Checked with `var_os` (no allocation) once
            // per commit, not per row.
            Err(BicDbError::PagedStorage(
                "injected commit-apply failure (BICDB_TEST_FAIL_COMMIT_APPLY)".to_string(),
            ))
        } else {
            self.apply_committed_record_writes(
                commit_seq,
                &writes_by_collection,
                &index_mutations_by_collection,
                &dim_plan,
                &mut timer,
            )
        };
        if let Err(error) = apply_result {
            return Err(self.revoke_enqueued_commit(tx.id, commit_seq, error));
        }
        // Now every record is resident: re-resolve locators (brand-new records now
        // resolve to `Some`) and apply the index entries (Pass 2). Returns spatial
        // audit events to append after the index locks drop.
        self.fill_all_index_mutation_rowids(&mut index_mutations_by_collection)?;
        let spatial_events = self.apply_index_mutations(&index_mutations_by_collection)?;
        if !spatial_events.is_empty() {
            let mut events = self.events.lock();
            for event in spatial_events {
                events.append(event)?;
            }
        }
        // The record-audit outbox is part of the transaction WAL above.  Keep
        // it on the transaction until the caller has made that WAL durable;
        // `finalize_commit_admission` then publishes it idempotently.
        tx.pending_record_audit_events = record_audit_events;
        self.refresh_graph_projections()?;
        if structural_schema_records_changed {
            self.schema_compatibility.invalidate();
        }
        timer.lap(&commit_trace::GRAPH);
        // Done: drop the tx_states entry rather than leaving a `Committed`
        // tombstone. Nothing reads a completed tx's state by tx_id (visibility is
        // commit_seq-based via the version chain; conflict detection never consults
        // tx_states; the only reader, `ensure_no_pending_transactions`, treats an
        // absent entry as "not pending"). Keeping committed entries leaked one map
        // entry per transaction forever (1M+ under a sustained workload). tx_states now holds only
        // in-flight transactions, bounded by concurrency.
        self.tx_states.remove(&tx.id.0);
        // Publish visibility only after this seq's records are fully applied; the
        // watermark advances across the contiguous applied prefix (see above).
        self.mark_committed_seq(commit_seq);
        let locked_keys = tx.locked_keys.get_mut().take_all();
        release_owned_write_locks(&self.write_locks, tx.id, &locked_keys);
        tx.state = TxState::Committed;
        tx.committed_seq = Some(commit_seq);
        timer.lap(&commit_trace::BOOKKEEPING);
        commit_trace::tick();
        // So this thread's next `begin_transaction` does not start behind its own
        // commit and self-conflict on a row nobody else touched.
        self.note_commit_seq_on_this_thread(commit_seq);
        if let (Some(admission), Some(ticket)) = (admission, admission_ticket) {
            tx.commit_admission_ticket = Some((admission, ticket));
        }
        Ok(commit_seq)
    }
}
