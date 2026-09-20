//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;

impl BicDb {
    /// SIMD/galloping intersection over the dense document-id postings of a
    /// generation-v2 index. `terms` defines output slots; only
    /// `required_slots` participate in the conjunction, so ranking-only terms
    /// may remain absent. A transactional tail or tombstone returns
    /// `Ok(None)` and lets the caller use the fully layered streaming path.
    pub fn full_text_numeric_conjunctive_postings(
        &self,
        name: &str,
        terms: &[&str],
        required_slots: &[usize],
    ) -> Result<Option<Vec<crate::fts_postings::FullTextConjunctivePosting>>> {
        if terms.is_empty()
            || required_slots.is_empty()
            || required_slots.iter().any(|slot| *slot >= terms.len())
            || terms.iter().any(|term| !full_text_term_is_indexable(term))
        {
            return Ok(None);
        }
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Ok(None);
        }
        drop(state);
        let Some(paged) = self.paged_records.as_ref() else {
            return Ok(None);
        };
        let snapshot = paged.latest_snapshot();
        if !paged.index_has_numeric_posting_blocks(&snapshot, name)? {
            return Ok(None);
        }

        let mut postings_by_term = Vec::with_capacity(terms.len());
        for term in terms {
            let encoded = encode_index_key(&[IndexValue::String((*term).to_string())]);
            let mut tail = paged.scan_index_exact(&snapshot, name, &encoded)?;
            if let Some(entry) = tail.next() {
                entry?;
                return Ok(None);
            }
            let mut tombstones = paged.scan_posting_tombstones(&snapshot, name, &encoded)?;
            if let Some(entry) = tombstones.next() {
                entry?;
                return Ok(None);
            }
            let mut postings = Vec::new();
            for block in paged.scan_numeric_posting_blocks(&snapshot, name, &encoded)? {
                let (_, bytes) = block?;
                postings.extend(crate::paged_collection::decode_numeric_posting_block(
                    &bytes,
                )?);
            }
            postings_by_term.push(postings);
        }

        let first_required = required_slots[0];
        let mut candidates: Vec<u64> = postings_by_term[first_required]
            .iter()
            .map(|posting| posting.document_id)
            .collect();
        let mut scratch = Vec::new();
        for &slot in &required_slots[1..] {
            let ids: Vec<u64> = postings_by_term[slot]
                .iter()
                .map(|posting| posting.document_id)
                .collect();
            crate::fts_postings::intersect_sorted_document_ids_into(
                &candidates,
                &ids,
                &mut scratch,
            );
            std::mem::swap(&mut candidates, &mut scratch);
            if candidates.is_empty() {
                break;
            }
        }

        let mut matches = Vec::with_capacity(candidates.len());
        for document_id in candidates {
            let mut term_positions = Vec::with_capacity(terms.len());
            let mut document_length = 0u32;
            let mut document_distinct_terms = 0u32;
            for (slot, postings) in postings_by_term.iter().enumerate() {
                let found = postings
                    .binary_search_by_key(&document_id, |posting| posting.document_id)
                    .ok()
                    .map(|position| &postings[position]);
                if let Some(posting) = found {
                    if slot == first_required {
                        document_length = posting.doc_length;
                        document_distinct_terms = posting.doc_distinct;
                    }
                    term_positions.push(Some(posting.packed_positions.clone()));
                } else {
                    term_positions.push(None);
                }
            }
            let primary_key = paged
                .full_text_pk_for_document_id(&snapshot, name, document_id)?
                .ok_or_else(|| BicDbError::Corruption {
                    path: PathBuf::from(DEFAULT_PAGED_DIR),
                    message: format!(
                        "full-text document id {document_id} has no primary-key mapping"
                    ),
                })?;
            matches.push(crate::fts_postings::FullTextConjunctivePosting {
                document_id,
                primary_key,
                document_length,
                document_distinct_terms,
                term_positions,
            });
        }
        Ok(Some(matches))
    }

    /// Count the documents matching a conjunction of plain lexemes from the
    /// dense document-id postings of a generation-v2 index. Positions are
    /// never decoded and no primary keys are resolved, so a broad count costs
    /// the posting bytes of its terms rather than a heap fetch per match.
    /// Returns `Ok(None)` — caller falls back to the fully layered scan —
    /// when a transactional tail, a tombstone, or a non-read-through index
    /// makes the blocks non-authoritative for the current snapshot.
    pub fn full_text_numeric_conjunctive_count(
        &self,
        name: &str,
        terms: &[&str],
    ) -> Result<Option<u64>> {
        if terms.is_empty() || terms.iter().any(|term| !full_text_term_is_indexable(term)) {
            return Ok(None);
        }
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Ok(None);
        }
        drop(state);
        let Some(paged) = self.paged_records.as_ref() else {
            return Ok(None);
        };
        let snapshot = paged.latest_snapshot();
        if !paged.index_has_numeric_posting_blocks(&snapshot, name)? {
            return Ok(None);
        }

        let mut ids_by_term = Vec::with_capacity(terms.len());
        for term in terms {
            let encoded = encode_index_key(&[IndexValue::String((*term).to_string())]);
            let mut tail = paged.scan_index_exact(&snapshot, name, &encoded)?;
            if let Some(entry) = tail.next() {
                entry?;
                return Ok(None);
            }
            let mut tombstones = paged.scan_posting_tombstones(&snapshot, name, &encoded)?;
            if let Some(entry) = tombstones.next() {
                entry?;
                return Ok(None);
            }
            let mut ids = Vec::new();
            for block in paged.scan_numeric_posting_blocks(&snapshot, name, &encoded)? {
                let (_, bytes) = block?;
                ids.extend(
                    crate::paged_collection::decode_numeric_posting_block_scores(&bytes)?
                        .into_iter()
                        .map(|posting| posting.document_id),
                );
            }
            if ids.is_empty() {
                crate::fts_format::record_boolean_block_count();
                return Ok(Some(0));
            }
            ids_by_term.push(ids);
        }

        // Intersect rarest-first so the candidate set only shrinks.
        ids_by_term.sort_unstable_by_key(Vec::len);
        let mut ids_by_term = ids_by_term.into_iter();
        let mut candidates = ids_by_term.next().expect("at least one term");
        let mut scratch = Vec::new();
        for ids in ids_by_term {
            crate::fts_postings::intersect_sorted_document_ids_into(
                &candidates,
                &ids,
                &mut scratch,
            );
            std::mem::swap(&mut candidates, &mut scratch);
            if candidates.is_empty() {
                break;
            }
        }
        crate::fts_format::record_boolean_block_count();
        Ok(Some(candidates.len() as u64))
    }

    /// Stream every document matching a conjunction of plain lexemes,
    /// together with each term's packed positions, from dense posting
    /// blocks. Two passes keep memory proportional to the encoded posting
    /// bytes plus one decoded block per term: document ids intersect first
    /// through the position-free decoder, then positions are decoded only
    /// for blocks that contain an intersection member. The visitor returns
    /// `false` to stop early (bounded SELECTs). `Ok(None)` — caller falls
    /// back — under the same authority gates as the conjunctive count, or
    /// when the terms' combined posting count exceeds the scan budget.
    pub fn full_text_numeric_conjunctive_position_scan(
        &self,
        name: &str,
        terms: &[&str],
        query_budget: &mut FtsQueryBudget,
        visit: &mut dyn FnMut(u64, &[Option<&[u16]>]) -> Result<bool>,
    ) -> Result<Option<()>> {
        // Beyond this many postings the retained encoded blocks and the
        // decode work stop being an obvious win over the layered scan.
        const MAX_SCANNED_POSTINGS: u64 = 32_000_000;
        if terms.is_empty() || terms.iter().any(|term| !full_text_term_is_indexable(term)) {
            return Ok(None);
        }
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Ok(None);
        }
        drop(state);
        let Some(paged) = self.paged_records.as_ref() else {
            return Ok(None);
        };
        let snapshot = paged.latest_snapshot();
        if !paged.index_has_numeric_posting_blocks(&snapshot, name)? {
            return Ok(None);
        }

        let mut blocks_by_term = Vec::with_capacity(terms.len());
        let mut ids_by_term = Vec::with_capacity(terms.len());
        let mut budget = 0u64;
        for term in terms {
            let encoded = encode_index_key(&[IndexValue::String((*term).to_string())]);
            let mut tail = paged.scan_index_exact(&snapshot, name, &encoded)?;
            if let Some(entry) = tail.next() {
                entry?;
                return Ok(None);
            }
            let mut tombstones = paged.scan_posting_tombstones(&snapshot, name, &encoded)?;
            if let Some(entry) = tombstones.next() {
                entry?;
                return Ok(None);
            }
            let mut blocks = Vec::new();
            let mut ids = Vec::new();
            for block in paged.scan_numeric_posting_blocks(&snapshot, name, &encoded)? {
                let (last_document_id, bytes) = block?;
                query_budget.charge_posting_blocks(1)?;
                ids.extend(
                    crate::paged_collection::decode_numeric_posting_block_scores(&bytes)?
                        .into_iter()
                        .map(|posting| posting.document_id),
                );
                blocks.push((last_document_id, bytes));
            }
            budget = budget.saturating_add(ids.len() as u64);
            query_budget.charge_postings(ids.len() as u64)?;
            if budget > MAX_SCANNED_POSTINGS {
                return Ok(None);
            }
            if ids.is_empty() {
                crate::fts_format::record_conjunctive_block_scan();
                return Ok(Some(()));
            }
            blocks_by_term.push(blocks);
            ids_by_term.push(ids);
        }

        let mut order = (0..terms.len()).collect::<Vec<_>>();
        order.sort_unstable_by_key(|slot| ids_by_term[*slot].len());
        let mut candidates = std::mem::take(&mut ids_by_term[order[0]]);
        let mut scratch = Vec::new();
        for &slot in &order[1..] {
            crate::fts_postings::intersect_sorted_document_ids_into(
                &candidates,
                &ids_by_term[slot],
                &mut scratch,
            );
            std::mem::swap(&mut candidates, &mut scratch);
            if candidates.is_empty() {
                break;
            }
        }

        struct TermCursor {
            blocks: Vec<(u64, Vec<u8>)>,
            block: usize,
            decoded: Option<Vec<crate::paged_collection::NumericBlockPosting>>,
        }
        let mut cursors = blocks_by_term
            .into_iter()
            .map(|blocks| TermCursor {
                blocks,
                block: 0,
                decoded: None,
            })
            .collect::<Vec<_>>();
        let corrupt = || BicDbError::PagedStorage("corrupt numeric posting block".to_string());
        for document_id in candidates {
            query_budget.charge_candidates(1)?;
            for cursor in &mut cursors {
                while cursor
                    .blocks
                    .get(cursor.block)
                    .is_some_and(|(last, _)| *last < document_id)
                {
                    cursor.block += 1;
                    cursor.decoded = None;
                }
                if cursor.decoded.is_none() {
                    let (_, bytes) = cursor.blocks.get(cursor.block).ok_or_else(corrupt)?;
                    query_budget.charge_posting_blocks(1)?;
                    cursor.decoded = Some(crate::paged_collection::decode_numeric_posting_block(
                        bytes,
                    )?);
                }
            }
            let mut positions: Vec<Option<&[u16]>> = Vec::with_capacity(cursors.len());
            for cursor in &cursors {
                let decoded = cursor.decoded.as_ref().ok_or_else(corrupt)?;
                let posting = decoded
                    .binary_search_by_key(&document_id, |posting| posting.document_id)
                    .ok()
                    .map(|index| decoded[index].packed_positions.as_slice())
                    .ok_or_else(corrupt)?;
                positions.push(Some(posting));
            }
            if !visit(document_id, &positions)? {
                break;
            }
        }
        crate::fts_format::record_conjunctive_block_scan();
        Ok(Some(()))
    }

    /// Human-readable routing report for one full-text index and a set of
    /// query terms: which retrieval path a ranked conjunction would take and
    /// why, derived from the same authority gates the dispatch checks. Built
    /// for production diagnosis over SQL (`bicdb_fts_route(index, terms)`)
    /// — bounded probes only, never a posting scan.
    /// Counts the index's unfolded write-layer entries up to `cap`.
    /// `None` means the tail exceeds the cap (the fold-now signal without
    /// paying for an exact count). Folded postings live in blocks and are
    /// not entries, so this is exactly the fold-hygiene metric.
    pub fn full_text_unfolded_entries_capped(
        &self,
        name: &str,
        cap: usize,
    ) -> Result<Option<usize>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Err(BicDbError::Index(format!(
                "index `{name}` does not serve read-through postings"
            )));
        }
        drop(state);
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;
        let snapshot = paged.latest_snapshot();
        let mut count = 0usize;
        for entry in paged.scan_index_encoded_prefix(&snapshot, name, &[4u8])? {
            entry?;
            count += 1;
            if count > cap {
                return Ok(None);
            }
        }
        Ok(Some(count))
    }

    pub fn full_text_route_report(&self, name: &str, terms: &[&str]) -> Result<String> {
        use std::fmt::Write as _;
        let mut report = String::new();
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        let full_text = state.definition.kind == IndexKind::FullText;
        let read_through = state.paged_read_through;
        drop(state);
        let _ = writeln!(
            report,
            "index {name}: full_text={full_text} read_through={read_through}"
        );
        if !full_text || !read_through {
            let _ = writeln!(
                report,
                "verdict: block paths unavailable — every ranked call uses the resident scan \
                 (recreate the index on a server_paged store to enable read-through postings)"
            );
            return Ok(report);
        }
        let Some(paged) = self.paged_records.as_ref() else {
            let _ = writeln!(report, "verdict: no page storage attached");
            return Ok(report);
        };
        let snapshot = paged.latest_snapshot();
        let has_blocks = paged.index_has_numeric_posting_blocks(&snapshot, name)?;
        match paged.full_text_collection_statistics(&snapshot, name)? {
            Some(statistics) => {
                let _ = writeln!(
                    report,
                    "collection: documents={} average_length={:.1} numeric_blocks={has_blocks}",
                    statistics.document_count, statistics.average_document_length
                );
            }
            None => {
                let _ = writeln!(
                    report,
                    "collection: statistics MISSING (BM25 paths decline) numeric_blocks={has_blocks}"
                );
            }
        }
        let mut unfolded = Vec::new();
        for term in terms {
            if !full_text_term_is_indexable(term) {
                let _ = writeln!(
                    report,
                    "term '{term}': not indexable — declines every block path"
                );
                unfolded.push((*term).to_string());
                continue;
            }
            let encoded = encode_index_key(&[IndexValue::String((*term).to_string())]);
            let tail = match paged.scan_index_exact(&snapshot, name, &encoded)?.next() {
                Some(entry) => {
                    entry?;
                    true
                }
                None => false,
            };
            let tombstones = match paged
                .scan_posting_tombstones(&snapshot, name, &encoded)?
                .next()
            {
                Some(entry) => {
                    entry?;
                    true
                }
                None => false,
            };
            match paged.full_text_term_statistics(&snapshot, name, &encoded)? {
                Some(statistics) => {
                    let _ = writeln!(
                        report,
                        "term '{term}': documents={} blocks={} impact_blocks={} \
                         unfolded_tail={tail} tombstones={tombstones}",
                        statistics.document_frequency,
                        statistics.posting_block_count,
                        statistics.impact_block_count,
                    );
                }
                None => {
                    let _ = writeln!(
                        report,
                        "term '{term}': absent from dictionary (matches nothing) \
                         unfolded_tail={tail} tombstones={tombstones}"
                    );
                }
            }
            if tail || tombstones {
                unfolded.push((*term).to_string());
            }
        }
        if !has_blocks {
            let _ = writeln!(
                report,
                "verdict: NO numeric posting blocks — every block-served path declines; \
                 rebuild the index (rows first, then CREATE INDEX) or fold it"
            );
        } else if !unfolded.is_empty() {
            let _ = writeln!(
                report,
                "verdict: unfolded writes or tombstones on: {} — plain BM25 conjunctions \
                 serve via the TAIL-MERGED block path (write layer capped at 65536 \
                 entries); ts_rank/SQL block paths still decline — fold \
                 (bicdb_fts_fold) to restore every path and shrink the layer",
                unfolded.join(", ")
            );
        } else {
            let _ = writeln!(
                report,
                "verdict bm25 (require_all, {} terms): {}",
                terms.len(),
                if terms.len() >= 2 {
                    "block-max seeking conjunction (fast path)"
                } else {
                    "single-term impact/exhaustive block path"
                }
            );
            let _ = writeln!(
                report,
                "verdict bm25f: EXHAUSTIVE accumulator over every posting of every term — \
                 no seeking path exists for BM25F; cost is O(sum of term document counts)"
            );
        }
        Ok(report)
    }

    /// SURGICAL repair of one row's corrupt version-header stamp: replaces
    /// exactly `field == expected` with the structural committed marker and
    /// checkpoints. Narrower than the transaction-floor advance — refused on
    /// any mismatch, idempotent when already clean. `field` is "xmin" or
    /// "xmax".
    pub fn repair_row_transaction_stamp(
        &self,
        collection: &str,
        pk: &str,
        field: &str,
        expected: u64,
    ) -> Result<String> {
        let stamp_field = match field {
            "xmin" => bicdb_page::paged::HeaderStampField::Xmin,
            "xmax" => bicdb_page::paged::HeaderStampField::Xmax,
            other => {
                return Err(BicDbError::Index(format!(
                    "unknown header field `{other}` (expected xmin or xmax)"
                )));
            }
        };
        let Some(paged) = self.paged_records.as_ref() else {
            return Err(BicDbError::Index(
                "row header repair requires a paged store".to_string(),
            ));
        };
        let outcome = paged.repair_record_header_stamp(collection, pk, stamp_field, expected)?;
        paged.checkpoint_store()?;
        Ok(match outcome {
            bicdb_page::paged::HeaderRepair::Repaired => format!(
                "repaired: {collection}/{pk} {field} {expected} -> 0 (structural committed); checkpointed"
            ),
            bicdb_page::paged::HeaderRepair::AlreadyClean => {
                format!("already clean: {collection}/{pk} {field} is 0; nothing changed")
            }
        })
    }

    /// REPAIR: advance the paged store's transaction floor past a corrupt
    /// future transaction id (see `bicdb_advance_transaction_floor` in SQL
    /// and `PagedStore::advance_transaction_floor` for the safety contract).
    /// Returns `(frozen_xid, next_xid)` after the durable checkpoint, or
    /// `None` when the database has no paged store.
    pub fn advance_transaction_floor(&self, beyond: u64) -> Result<Option<(u64, u64)>> {
        let Some(paged) = self.paged_records.as_ref() else {
            return Ok(None);
        };
        paged.advance_transaction_floor(beyond).map(Some)
    }

    /// Resolve one full-text document id to its primary key.
    pub fn full_text_primary_key_for_document_id(
        &self,
        name: &str,
        document_id: u64,
    ) -> Result<Option<String>> {
        let Some(paged) = self.paged_records.as_ref() else {
            return Ok(None);
        };
        let snapshot = paged.latest_snapshot();
        paged.full_text_pk_for_document_id(&snapshot, name, document_id)
    }

    /// Resolve the primary keys of full-text document ids, in order. Errors
    /// if any id has no mapping — matching documents always have one.
    pub fn full_text_primary_keys_for_document_ids(
        &self,
        name: &str,
        document_ids: &[u64],
    ) -> Result<Vec<String>> {
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index("page storage required".to_string()))?;
        let snapshot = paged.latest_snapshot();
        bounded_parallel_read_map(document_ids, |document_id| {
            paged
                .full_text_pk_for_document_id(&snapshot, name, *document_id)?
                .ok_or_else(|| BicDbError::Corruption {
                    path: PathBuf::from(DEFAULT_PAGED_DIR),
                    message: format!(
                        "full-text document id {document_id} has no primary-key mapping"
                    ),
                })
        })
    }

    /// Pin an independent MVCC snapshot and the currently published physical
    /// generation for one read-through FTS index.
    pub fn full_text_read_session(&self, name: &str) -> Result<FullTextReadSession<'_>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Err(BicDbError::Index(format!(
                "index `{name}` does not serve read-through postings"
            )));
        }
        drop(state);
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;

        // Publication takes this same catalog lock before changing the alias.
        // Beginning the page snapshot while it is held makes these two values
        // an indivisible view: old generation + pre-reclamation snapshot, or
        // new generation + post-publication snapshot.
        let generations = self.fts_generations.lock();
        let physical_index = generations
            .indexes
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_string())
            .into();
        let (xid, snapshot) = paged.begin();
        drop(generations);
        Ok(FullTextReadSession {
            db: self,
            logical_index: name.to_string(),
            physical_index,
            xid,
            snapshot,
            cancellation: None,
        })
    }

    pub fn full_text_open_metrics(&self) -> FullTextOpenMetrics {
        self.fts_open_metrics
    }

    /// Adaptive multi-term Block-Max WAND/MaxScore over document-order
    /// numeric blocks. This path is exact for positive OR queries using
    /// default-weight, normalization-zero `ts_rank`; other ranking modes
    /// return `None` so callers retain their compatibility fallback.
    pub fn full_text_block_max_wand_top_k(
        &self,
        name: &str,
        terms: &[&str],
        weights: [f32; 4],
        keep: usize,
    ) -> Result<Option<Vec<crate::fts_postings::FullTextRankedPosting>>> {
        self.full_text_block_max_wand_top_k_filtered(name, terms, weights, keep, None)
    }

    pub fn full_text_block_max_wand_top_k_filtered(
        &self,
        name: &str,
        terms: &[&str],
        weights: [f32; 4],
        keep: usize,
        filter: Option<&crate::fts_filters::FullTextDocumentFilter>,
    ) -> Result<Option<Vec<crate::fts_postings::FullTextRankedPosting>>> {
        const DEFAULT_WEIGHTS: [f32; 4] = [0.1, 0.2, 0.4, 1.0];
        if terms.len() < 2
            || keep == 0
            || weights != DEFAULT_WEIGHTS
            || terms.iter().any(|term| !full_text_term_is_indexable(term))
        {
            return Ok(None);
        }
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Ok(None);
        }
        drop(state);
        if self.paged_records.is_none() {
            return Ok(None);
        }
        self.full_text_read_session(name)?
            .block_max_wand_top_k(terms, weights, keep, filter)
    }

    /// Block-Max Ranked AND over immutable numeric posting blocks.
    pub fn full_text_block_max_ranked_and_top_k(
        &self,
        name: &str,
        terms: &[&str],
        weights: [f32; 4],
        keep: usize,
    ) -> Result<Option<Vec<crate::fts_postings::FullTextRankedPosting>>> {
        self.full_text_block_max_ranked_and_top_k_filtered(name, terms, weights, keep, None)
    }

    pub fn full_text_block_max_ranked_and_top_k_filtered(
        &self,
        name: &str,
        terms: &[&str],
        weights: [f32; 4],
        keep: usize,
        filter: Option<&crate::fts_filters::FullTextDocumentFilter>,
    ) -> Result<Option<Vec<crate::fts_postings::FullTextRankedPosting>>> {
        const DEFAULT_WEIGHTS: [f32; 4] = [0.1, 0.2, 0.4, 1.0];
        if terms.len() < 2
            || keep == 0
            || weights != DEFAULT_WEIGHTS
            || terms.iter().any(|term| !full_text_term_is_indexable(term))
        {
            return Ok(None);
        }
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Ok(None);
        }
        drop(state);
        if self.paged_records.is_none() {
            return Ok(None);
        }
        self.full_text_read_session(name)?
            .block_max_ranked_and_top_k(terms, weights, keep, filter)
    }

    pub fn full_text_bm25_top_k(
        &self,
        name: &str,
        terms: &[&str],
        parameters: crate::fts_scoring::Bm25Parameters,
        keep: usize,
        require_all: bool,
    ) -> Result<Option<Vec<crate::fts_postings::FullTextRankedPosting>>> {
        self.full_text_bm25_top_k_filtered(name, terms, parameters, keep, require_all, None)
    }

    /// Budgeted, cancellable ranked search. The block-max seeking paths are
    /// bounded by construction (they visit only the blocks the scores
    /// require); the danger is the merged/exhaustive fallback, which scans
    /// every posting of every term. This variant pre-flights that route:
    /// when the summed term document frequencies exceed the candidate
    /// budget, it fails fast with `query_budget_exceeded` instead of
    /// silently scanning the corpus. Cancellation is also checked during
    /// posting-fetch batches, impact scans, write-layer and materialization work.
    pub fn full_text_bm25_top_k_budgeted(
        &self,
        name: &str,
        terms: &[&str],
        parameters: crate::fts_scoring::Bm25Parameters,
        keep: usize,
        require_all: bool,
        budget: &mut FtsQueryBudget,
    ) -> Result<Option<Vec<crate::fts_postings::FullTextRankedPosting>>> {
        budget.cancellation.check()?;
        if let Some(limit) = budget.limits.max_candidates {
            // Conjunctions are bounded by the rarest term; disjunctions by
            // the sum of all terms.
            let cap = usize::try_from(limit.saturating_add(1)).unwrap_or(usize::MAX);
            let mut rarest: Option<u64> = None;
            let mut total: u64 = 0;
            for term in terms {
                let count = self
                    .full_text_term_count_capped(name, term, cap)?
                    .unwrap_or(cap) as u64;
                rarest = Some(rarest.map_or(count, |best| best.min(count)));
                total = total.saturating_add(count);
            }
            let bound = if require_all {
                rarest.unwrap_or(0)
            } else {
                total
            };
            if bound > limit {
                return Err(BicDbError::QueryBudgetExceeded {
                    resource: "candidates",
                    limit,
                });
            }
            budget.charge_candidates(bound)?;
        }
        let result = self.full_text_bm25_top_k_filtered_cancellable(
            name,
            terms,
            parameters,
            keep,
            require_all,
            None,
            &budget.cancellation,
        )?;
        budget.cancellation.check()?;
        Ok(result)
    }

    /// Budgeted probe: inputs are charged as candidates and hits as
    /// postings, so a probe fed by an over-budget driver list can never
    /// silently expand the query's footprint.
    pub fn full_text_posting_probe_many_budgeted(
        &self,
        name: &str,
        term: &str,
        pks: &[String],
        budget: &mut FtsQueryBudget,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        budget.charge_candidates(pks.len() as u64)?;
        let hits = self.full_text_posting_probe_many(name, term, pks)?;
        budget.charge_postings(hits.len() as u64)?;
        Ok(hits)
    }

    pub fn full_text_bm25_top_k_filtered(
        &self,
        name: &str,
        terms: &[&str],
        parameters: crate::fts_scoring::Bm25Parameters,
        keep: usize,
        require_all: bool,
        filter: Option<&crate::fts_filters::FullTextDocumentFilter>,
    ) -> Result<Option<Vec<crate::fts_postings::FullTextRankedPosting>>> {
        self.full_text_bm25_top_k_impl(
            name,
            terms,
            FullTextBm25Mode::Bm25(parameters),
            keep,
            require_all,
            filter,
            None,
        )
    }

    /// Cooperative cancellation at posting-fetch batches, impact rounds,
    /// write-layer entries and ranked-hit materialization. No index mutation.
    pub fn full_text_bm25_top_k_filtered_cancellable(
        &self,
        name: &str,
        terms: &[&str],
        parameters: crate::fts_scoring::Bm25Parameters,
        keep: usize,
        require_all: bool,
        filter: Option<&crate::fts_filters::FullTextDocumentFilter>,
        cancellation: &CancellationToken,
    ) -> Result<Option<Vec<crate::fts_postings::FullTextRankedPosting>>> {
        cancellation.check()?;
        let result = self.full_text_bm25_top_k_impl(
            name,
            terms,
            FullTextBm25Mode::Bm25(parameters),
            keep,
            require_all,
            filter,
            Some(cancellation),
        )?;
        cancellation.check()?;
        Ok(result)
    }

    pub fn full_text_bm25f_top_k(
        &self,
        name: &str,
        terms: &[&str],
        parameters: crate::fts_scoring::Bm25fParameters,
        keep: usize,
        require_all: bool,
    ) -> Result<Option<Vec<crate::fts_postings::FullTextRankedPosting>>> {
        self.full_text_bm25f_top_k_filtered(name, terms, parameters, keep, require_all, None)
    }

    pub fn full_text_bm25f_top_k_filtered(
        &self,
        name: &str,
        terms: &[&str],
        parameters: crate::fts_scoring::Bm25fParameters,
        keep: usize,
        require_all: bool,
        filter: Option<&crate::fts_filters::FullTextDocumentFilter>,
    ) -> Result<Option<Vec<crate::fts_postings::FullTextRankedPosting>>> {
        self.full_text_bm25_top_k_impl(
            name,
            terms,
            FullTextBm25Mode::Bm25f(parameters),
            keep,
            require_all,
            filter,
            None,
        )
    }

    pub(crate) fn full_text_bm25_top_k_impl(
        &self,
        name: &str,
        terms: &[&str],
        mode: FullTextBm25Mode,
        keep: usize,
        require_all: bool,
        filter: Option<&crate::fts_filters::FullTextDocumentFilter>,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Option<Vec<crate::fts_postings::FullTextRankedPosting>>> {
        // Count this query in flight for as long as it runs, so concurrent
        // queries see each other and each asks for a share of the machine
        // rather than all of it.
        let _inflight = RankedInflight::enter();
        if terms.is_empty()
            || keep == 0
            || terms.iter().any(|term| !full_text_term_is_indexable(term))
        {
            return Ok(None);
        }
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Ok(None);
        }
        drop(state);
        let Some(paged) = self.paged_records.as_ref() else {
            return Ok(None);
        };
        let snapshot = paged.latest_snapshot();
        if !paged.index_has_numeric_posting_blocks(&snapshot, name)? {
            return Ok(None);
        }
        if terms.len() == 1 || (require_all && terms.len() >= 2) {
            if let FullTextBm25Mode::Bm25(parameters) = mode {
                let mut session = self.full_text_read_session(name)?;
                session.cancellation = cancellation.cloned();
                if let Some(ranked) =
                    session.block_max_bm25_and_top_k(terms, parameters, keep, filter)?
                {
                    return Ok(Some(ranked));
                }
                if terms.len() >= 2 {
                    if let Some(ranked) =
                        session.impact_ordered_bm25_and_top_k(terms, parameters, keep, filter)?
                    {
                        return Ok(Some(ranked));
                    }
                }
                // Unfolded writes or tombstones declined the sealed-only
                // path; layer them over the blocks instead of falling all
                // the way back to the caller.
                if let Some(ranked) =
                    session.tail_merged_bm25_and_top_k(terms, parameters, keep, filter)?
                {
                    return Ok(Some(ranked));
                }
            }
        }
        let collection_statistics = paged
            .full_text_collection_statistics(&snapshot, name)?
            .ok_or_else(|| {
                BicDbError::Index(format!(
                    "full-text index `{name}` has no collection statistics"
                ))
            })?;
        let average_field_lengths = std::array::from_fn(|field| {
            if collection_statistics.document_count == 0 {
                0.0
            } else {
                collection_statistics.field_total_lengths[field] as f64
                    / collection_statistics.document_count as f64
            }
        });

        struct Accumulator {
            score: f32,
            matched_terms: usize,
            document_length: u32,
            document_distinct_terms: u32,
            term_positions: Vec<Option<Vec<u16>>>,
        }
        let mut documents: FxHashMap<u64, Accumulator> = FxHashMap::default();
        let mut document_statistics: FxHashMap<u64, crate::fts_format::FullTextDocumentStatistics> =
            FxHashMap::default();
        for (slot, term) in terms.iter().enumerate() {
            if let Some(token) = cancellation {
                token.check()?;
            }
            let encoded = encode_index_key(&[IndexValue::String((*term).to_string())]);
            let mut tail = paged.scan_index_exact(&snapshot, name, &encoded)?;
            if let Some(entry) = tail.next() {
                entry?;
                return Ok(None);
            }
            let mut tombstones = paged.scan_posting_tombstones(&snapshot, name, &encoded)?;
            if let Some(entry) = tombstones.next() {
                entry?;
                return Ok(None);
            }
            let term_statistics = paged
                .full_text_term_statistics(&snapshot, name, &encoded)?
                .unwrap_or_default();
            let idf = crate::fts_scoring::bm25_inverse_document_frequency(
                collection_statistics.document_count,
                term_statistics.document_frequency,
            );
            for block in paged.scan_numeric_posting_blocks(&snapshot, name, &encoded)? {
                if let Some(token) = cancellation {
                    token.check()?;
                }
                let (_, bytes) = block?;
                for posting in crate::paged_collection::decode_numeric_posting_block(&bytes)? {
                    if filter.is_some_and(|filter| !filter.contains(posting.document_id)) {
                        crate::fts_format::record_filter_rejections(1);
                        continue;
                    }
                    let score = match mode {
                        FullTextBm25Mode::Bm25(parameters) => crate::fts_scoring::bm25_term_score(
                            posting.packed_positions.len().max(1) as u32,
                            idf,
                            posting.doc_length,
                            collection_statistics.average_document_length,
                            parameters,
                        ),
                        FullTextBm25Mode::Bm25f(parameters) => {
                            if !document_statistics.contains_key(&posting.document_id) {
                                let Some(statistics) = paged.full_text_document_statistics(
                                    &snapshot,
                                    name,
                                    posting.document_id,
                                )?
                                else {
                                    return Ok(None);
                                };
                                document_statistics.insert(posting.document_id, statistics);
                            }
                            let statistics = &document_statistics[&posting.document_id];
                            let mut field_term_frequencies = [0u32; 4];
                            if posting.packed_positions.is_empty() {
                                field_term_frequencies[0] = 1;
                            } else {
                                for packed in &posting.packed_positions {
                                    let field = ((packed >> 14) & 0x3) as usize;
                                    field_term_frequencies[field] =
                                        field_term_frequencies[field].saturating_add(1);
                                }
                            }
                            crate::fts_scoring::bm25f_term_score(
                                field_term_frequencies,
                                idf,
                                statistics.field_lengths,
                                average_field_lengths,
                                parameters,
                            )
                        }
                    };
                    let document =
                        documents
                            .entry(posting.document_id)
                            .or_insert_with(|| Accumulator {
                                score: 0.0,
                                matched_terms: 0,
                                document_length: posting.doc_length,
                                document_distinct_terms: posting.doc_distinct,
                                term_positions: vec![None; terms.len()],
                            });
                    document.score += score;
                    document.matched_terms += 1;
                    document.term_positions[slot] = Some(posting.packed_positions);
                }
            }
        }

        let mut ranked: Vec<(u64, Accumulator)> = documents
            .into_iter()
            .filter(|(_, document)| !require_all || document.matched_terms == terms.len())
            .collect();
        if let Some(token) = cancellation {
            token.check()?;
        }
        ranked.sort_unstable_by(|left, right| {
            right
                .1
                .score
                .partial_cmp(&left.1.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.0.cmp(&right.0))
        });
        ranked.truncate(keep);
        let mut results = Vec::with_capacity(ranked.len());
        for (document_id, document) in ranked {
            if let Some(token) = cancellation {
                token.check()?;
            }
            let primary_key = paged
                .full_text_pk_for_document_id(&snapshot, name, document_id)?
                .ok_or_else(|| BicDbError::Corruption {
                    path: PathBuf::from(DEFAULT_PAGED_DIR),
                    message: format!(
                        "full-text document id {document_id} has no primary-key mapping"
                    ),
                })?;
            results.push(crate::fts_postings::FullTextRankedPosting {
                document_id,
                primary_key,
                score: document.score,
                document_length: document.document_length,
                document_distinct_terms: document.document_distinct_terms,
                term_positions: document.term_positions,
            });
        }
        Ok(Some(results))
    }

    /// Materialize a dense FTS-generation filter from primary keys.
    ///
    /// The bitset is indexed by the generation's numeric document ids, so
    /// ranked retrieval can reject non-matching documents before scoring or
    /// primary-key materialization.
    pub fn full_text_document_filter_from_primary_keys(
        &self,
        name: &str,
        primary_keys: &[String],
    ) -> Result<crate::fts_filters::FullTextDocumentFilter> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Err(BicDbError::Index(format!(
                "index `{name}` does not serve read-through postings"
            )));
        }
        drop(state);
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;
        let snapshot = paged.latest_snapshot();
        let statistics = paged
            .full_text_collection_statistics(&snapshot, name)?
            .ok_or_else(|| {
                BicDbError::Index(format!(
                    "full-text index `{name}` has no collection statistics"
                ))
            })?;
        let mut document_ids = Vec::with_capacity(primary_keys.len());
        for primary_key in primary_keys {
            if let Some(document_id) =
                paged.full_text_document_id_for_pk(&snapshot, name, primary_key)?
            {
                document_ids.push(document_id);
            }
        }
        Ok(
            crate::fts_filters::FullTextDocumentFilter::from_document_ids(
                statistics.document_count,
                document_ids,
            ),
        )
    }

    /// Resolve one exact filter value attached to sealed direct-build
    /// documents into a generation-pinned ranked-search filter.
    pub fn full_text_document_filter_from_sealed_value(
        &self,
        full_text_index: &str,
        filter_name: &str,
        filter_value: &str,
    ) -> Result<crate::fts_filters::FullTextDocumentFilter> {
        self.full_text_read_session(full_text_index)?
            .document_filter_from_sealed_value(filter_name, filter_value)
    }

    /// Resolve a native secondary-index predicate directly into an FTS
    /// generation bitset. Both indexes must cover the same collection.
    pub fn full_text_document_filter_from_index(
        &self,
        full_text_index: &str,
        filter_index: &str,
        prefix: &[IndexValue],
    ) -> Result<crate::fts_filters::FullTextDocumentFilter> {
        let full_text_collection = self.index_collection(full_text_index)?;
        let filter_definition = self
            .indexes
            .get(filter_index)
            .ok_or_else(|| BicDbError::Index(format!("index `{filter_index}` not found")))?
            .read()
            .definition
            .clone();
        if filter_definition.kind != IndexKind::BTree {
            return Err(BicDbError::Index(format!(
                "filter index `{filter_index}` is not a B-tree index"
            )));
        }
        if prefix.is_empty() || prefix.len() > filter_definition.fields.len() {
            return Err(BicDbError::Index(format!(
                "invalid lookup prefix for index `{filter_index}`"
            )));
        }
        if full_text_collection != filter_definition.collection {
            return Err(BicDbError::Index(format!(
                "filter index `{filter_index}` and full-text index `{full_text_index}` cover different collections"
            )));
        }
        let primary_keys = if let Some(paged) = self.paged_records.as_ref() {
            let snapshot = paged.latest_snapshot();
            let encoded_prefix = encode_index_key(prefix);
            let mut primary_keys = if prefix.len() == filter_definition.fields.len() {
                paged
                    .scan_index_exact_refs(&snapshot, filter_index, &encoded_prefix)?
                    .map(|entry| {
                        let (entry_ref, value) = entry?;
                        resolve_paged_entry_pk(
                            paged,
                            &snapshot,
                            &filter_definition.collection,
                            filter_index,
                            entry_ref,
                            &value,
                            false,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                paged
                    .scan_index_encoded_prefix_refs(&snapshot, filter_index, &encoded_prefix)?
                    .map(|entry| {
                        let (_, entry_ref, value) = entry?;
                        resolve_paged_entry_pk(
                            paged,
                            &snapshot,
                            &filter_definition.collection,
                            filter_index,
                            entry_ref,
                            &value,
                            false,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?
            };
            primary_keys.sort_unstable();
            primary_keys.dedup();
            primary_keys
        } else {
            self.lookup_index(filter_index, prefix)?
        };
        self.full_text_document_filter_from_primary_keys(full_text_index, &primary_keys)
    }

    /// [`Self::full_text_posting_probe_many`], streaming: `visit(pks_index,
    /// doc_length, doc_distinct, packed)` for every hit, no allocation per
    /// probe. `pks` MUST be ascending.
    pub fn full_text_posting_probe_many_stream(
        &self,
        name: &str,
        term: &str,
        pks: &[&str],
        mut visit: impl FnMut(usize, u32, u32, &[u16]) -> Result<()>,
    ) -> Result<()> {
        debug_assert!(pks.windows(2).all(|pair| pair[0] <= pair[1]));
        if !full_text_term_is_indexable(term) {
            return Ok(());
        }
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;
        let snapshot = paged.latest_snapshot();
        let encoded = encode_index_key(&[IndexValue::String(term.to_string())]);
        let mut tail: FxHashMap<String, Vec<u8>> = FxHashMap::default();
        for entry in paged.scan_index_exact(&snapshot, name, &encoded)? {
            let (pk, payload) = entry?;
            tail.insert(pk, payload);
        }
        let mut packed_scratch: Vec<u16> = Vec::new();
        let mut emit_tail = |index: usize,
                             payload: &[u8],
                             packed_scratch: &mut Vec<u16>,
                             visit: &mut dyn FnMut(usize, u32, u32, &[u16]) -> Result<()>|
         -> Result<()> {
            if let Some((length, distinct)) =
                decode_fts_posting_payload_into(payload, packed_scratch)
            {
                visit(index, length, distinct, packed_scratch)?;
            }
            Ok(())
        };
        let has_numeric_blocks = paged.index_has_numeric_posting_blocks(&snapshot, name)?;
        if !has_numeric_blocks && !paged.index_has_posting_blocks(&snapshot, name)? {
            for (index, pk) in pks.iter().enumerate() {
                if let Some(payload) = tail.get(*pk) {
                    emit_tail(index, payload, &mut packed_scratch, &mut visit)?;
                }
            }
            return Ok(());
        }
        let tombstones: FxHashSet<String> = paged
            .scan_posting_tombstones(&snapshot, name, &encoded)?
            .collect::<Result<_>>()?;
        if has_numeric_blocks {
            // Dense ids were assigned in primary-key scan order, so this map
            // preserves the caller's ordering and a two-pointer block walk
            // answers every probe while decoding each relevant block once.
            let mut document_ids = Vec::with_capacity(pks.len());
            for pk in pks {
                document_ids.push(paged.full_text_document_id_for_pk(&snapshot, name, pk)?);
            }
            let mut next = 0usize;
            for block in paged.scan_numeric_posting_blocks(&snapshot, name, &encoded)? {
                if next >= pks.len() {
                    break;
                }
                let (last_document_id, bytes) = block?;
                let postings = crate::paged_collection::decode_numeric_posting_block(&bytes)?;
                let mut posting = 0usize;
                while next < pks.len() {
                    if let Some(payload) = tail.get(pks[next]) {
                        emit_tail(next, payload, &mut packed_scratch, &mut visit)?;
                        next += 1;
                        continue;
                    }
                    if tombstones.contains(pks[next]) {
                        next += 1;
                        continue;
                    }
                    let Some(document_id) = document_ids[next] else {
                        next += 1;
                        continue;
                    };
                    if document_id > last_document_id {
                        break;
                    }
                    while posting < postings.len() && postings[posting].document_id < document_id {
                        posting += 1;
                    }
                    if posting < postings.len() && postings[posting].document_id == document_id {
                        let hit = &postings[posting];
                        visit(
                            next,
                            hit.doc_length,
                            hit.doc_distinct,
                            &hit.packed_positions,
                        )?;
                    }
                    next += 1;
                }
            }
            while next < pks.len() {
                if let Some(payload) = tail.get(pks[next]) {
                    emit_tail(next, payload, &mut packed_scratch, &mut visit)?;
                }
                next += 1;
            }
            return Ok(());
        }
        let mut next = 0usize;
        let mut pk_scratch: Vec<u8> = Vec::new();
        if tail.is_empty() && tombstones.is_empty() {
            // Churn-free (direct-built / freshly folded): the lockstep walk
            // needs no tail overrides and no tombstone hides, so the block
            // walk can SKIP position decode for every non-candidate resident
            // — on a broad term probed by a selective driver that is ~90% of
            // the block.
            for block in paged.scan_posting_blocks(&snapshot, name, &encoded)? {
                if next >= pks.len() {
                    break;
                }
                let (last_pk, bytes) = block?;
                let start = next;
                while next < pks.len() && pks[next] <= last_pk.as_str() {
                    next += 1;
                }
                if start == next {
                    continue;
                }
                let probe = std::cell::Cell::new(start);
                crate::paged_collection::visit_posting_block_filtered(
                    &bytes,
                    &mut pk_scratch,
                    &mut packed_scratch,
                    |pk| {
                        let mut position = probe.get();
                        while position < next && pks[position] < pk {
                            position += 1;
                        }
                        probe.set(position);
                        position < next && pks[position] == pk
                    },
                    |_, doc_length, doc_distinct, packed| {
                        visit(probe.get(), doc_length, doc_distinct, packed)?;
                        probe.set(probe.get() + 1);
                        Ok(true)
                    },
                )?;
            }
            return Ok(());
        }
        for block in paged.scan_posting_blocks(&snapshot, name, &encoded)? {
            if next >= pks.len() {
                break;
            }
            let (last_pk, bytes) = block?;
            let start = next;
            while next < pks.len() && pks[next] <= last_pk.as_str() {
                next += 1;
            }
            if start == next {
                continue;
            }
            // One decode answers every probe the block covers: walk the
            // block and the probe window in lockstep (both pk-ascending).
            let mut probe = start;
            crate::paged_collection::visit_posting_block(
                &bytes,
                &mut pk_scratch,
                &mut packed_scratch,
                |pk, doc_length, doc_distinct, packed| {
                    let mut tail_packed: Vec<u16> = Vec::new();
                    // Probes sorting before this posting can only be tail hits.
                    while probe < next && pks[probe] < pk {
                        if let Some(payload) = tail.get(pks[probe]) {
                            if let Some((length, distinct)) =
                                decode_fts_posting_payload_into(payload, &mut tail_packed)
                            {
                                visit(probe, length, distinct, &tail_packed)?;
                            }
                        }
                        probe += 1;
                    }
                    if probe < next && pks[probe] == pk {
                        if let Some(payload) = tail.get(pk) {
                            if let Some((length, distinct)) =
                                decode_fts_posting_payload_into(payload, &mut tail_packed)
                            {
                                visit(probe, length, distinct, &tail_packed)?;
                            }
                        } else if !tombstones.contains(pk) {
                            visit(probe, doc_length, doc_distinct, packed)?;
                        }
                        probe += 1;
                    }
                    Ok(true)
                },
            )?;
            // Probes past the block's postings but within its window: tail.
            while probe < next {
                if let Some(payload) = tail.get(pks[probe]) {
                    emit_tail(probe, payload, &mut packed_scratch, &mut visit)?;
                }
                probe += 1;
            }
        }
        while next < pks.len() {
            if let Some(payload) = tail.get(pks[next]) {
                emit_tail(next, payload, &mut packed_scratch, &mut visit)?;
            }
            next += 1;
        }
        Ok(())
    }

    /// Bounded posting count for one exact term of a read-through FTS index.
    /// New-format generations answer from one compact dictionary record.
    /// Legacy generations scan at most `cap + 1` entries.
    pub fn full_text_term_count_capped(
        &self,
        name: &str,
        term: &str,
        cap: usize,
    ) -> Result<Option<usize>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Err(BicDbError::Index(format!(
                "index `{name}` does not serve read-through postings"
            )));
        }
        if !full_text_term_is_indexable(term) {
            return Ok(Some(0));
        }
        drop(state);
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;
        let encoded = encode_index_key(&[IndexValue::String(term.to_string())]);
        let snapshot = paged.latest_snapshot();
        if let Some(statistics) = paged.full_text_term_statistics(&snapshot, name, &encoded)? {
            return Ok((statistics.document_frequency <= cap as u64)
                .then_some(statistics.document_frequency as usize));
        }
        let Some(mut count) = paged.index_posting_keys_capped(name, &encoded, cap)? else {
            crate::fts_format::record_planning_key_count(cap.saturating_add(1));
            return Ok(None);
        };
        crate::fts_format::record_planning_key_count(count);
        if paged.index_has_posting_blocks(&snapshot, name)? {
            for block in paged.scan_posting_blocks(&snapshot, name, &encoded)? {
                let (_, bytes) = block?;
                let docs = crate::paged_collection::posting_block_doc_count(&bytes)?;
                crate::fts_format::record_planning_key_count(docs);
                count += docs;
                if count > cap {
                    return Ok(None);
                }
            }
        }
        if paged.index_has_numeric_posting_blocks(&snapshot, name)? {
            for block in paged.scan_numeric_posting_blocks(&snapshot, name, &encoded)? {
                let (_, bytes) = block?;
                let docs = crate::paged_collection::numeric_posting_block_header(&bytes)
                    .ok_or_else(|| {
                        BicDbError::PagedStorage("corrupt numeric posting block".to_string())
                    })?
                    .doc_count;
                crate::fts_format::record_planning_key_count(docs);
                count += docs;
                if count > cap {
                    return Ok(None);
                }
            }
        }
        Ok(Some(count))
    }

    /// Compact O(1)-record term statistics for planner and ranking use.
    pub fn full_text_term_statistics(
        &self,
        name: &str,
        term: &str,
    ) -> Result<Option<crate::fts_format::FullTextTermStatistics>> {
        if !full_text_term_is_indexable(term) {
            return Ok(None);
        }
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Err(BicDbError::Index(format!(
                "index `{name}` does not serve read-through postings"
            )));
        }
        drop(state);
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;
        paged.full_text_term_statistics(
            &paged.latest_snapshot(),
            name,
            &full_text_term_entry_key(term),
        )
    }

    /// Collection-wide corpus statistics stored once per physical generation.
    pub fn full_text_collection_statistics(
        &self,
        name: &str,
    ) -> Result<Option<crate::fts_format::FullTextCollectionStatistics>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Err(BicDbError::Index(format!(
                "index `{name}` does not serve read-through postings"
            )));
        }
        drop(state);
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;
        paged.full_text_collection_statistics(&paged.latest_snapshot(), name)
    }

    /// Identify the durable physical generation currently serving a logical
    /// read-through FTS index. An unpublished replacement workspace is never
    /// reported here.
    pub fn full_text_published_generation(
        &self,
        name: &str,
    ) -> Result<FullTextPublishedGeneration> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Err(BicDbError::Index(format!(
                "index `{name}` does not serve read-through postings"
            )));
        }
        drop(state);
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;
        let physical_index = self
            .fts_generations
            .lock()
            .indexes
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_string());
        let statistics = paged
            .full_text_collection_statistics(&paged.latest_snapshot(), &physical_index)?
            .ok_or_else(|| {
                BicDbError::Index(format!(
                    "published full-text generation `{physical_index}` has no collection statistics"
                ))
            })?;
        let manifest = paged
            .fts_segments_root()
            .join(&physical_index)
            .join("manifest.json");
        let segment_manifest_sha256 = manifest
            .exists()
            .then(|| sha256_file(&manifest))
            .transpose()?;
        Ok(FullTextPublishedGeneration {
            logical_index: name.to_string(),
            physical_index,
            document_count: statistics.document_count,
            format_version: statistics.format_version,
            segment_manifest_sha256,
        })
    }

    /// Return optional retrieval text stored directly in the published FTS
    /// generation. No source collection row is required.
    pub fn full_text_stored_text(&self, name: &str, primary_key: &str) -> Result<Option<Vec<u8>>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText {
            return Err(BicDbError::Index(format!(
                "index `{name}` is not full-text"
            )));
        }
        drop(state);
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;
        let snapshot = paged.latest_snapshot();
        let Some(document_id) = paged.full_text_document_id_for_pk(&snapshot, name, primary_key)?
        else {
            return Ok(None);
        };
        paged
            .full_text_stored_text(&snapshot, name, document_id)?
            .as_deref()
            .map(crate::fts_format::decode_stored_text)
            .transpose()
    }

    /// Return one bounded page of retrieval text by numeric document id from
    /// the currently published physical generation.
    ///
    /// The generation catalog lock and one MVCC snapshot remain pinned for the
    /// complete range scan, so publication cannot switch this logical index
    /// between validation and the returned rows.
    pub fn full_text_stored_text_page(
        &self,
        name: &str,
        expected_physical_generation: &str,
        after_document_id: Option<u64>,
        limit: usize,
    ) -> Result<FullTextStoredTextPage> {
        if !(1..=4_096).contains(&limit) {
            return Err(BicDbError::Index(format!(
                "full-text stored-text page limit must be between 1 and 4096; got {limit}"
            )));
        }
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText || !state.paged_read_through {
            return Err(BicDbError::Index(format!(
                "index `{name}` does not serve read-through postings"
            )));
        }
        drop(state);
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;
        let generations = self.fts_generations.lock();
        let physical_generation = generations
            .indexes
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_string());
        if physical_generation != expected_physical_generation {
            return Err(BicDbError::Index(format!(
                "published full-text generation mismatch for `{name}`: expected `{expected_physical_generation}`, found `{physical_generation}`"
            )));
        }
        let snapshot = paged.latest_snapshot();
        let (rows, next_after_document_id) = paged.full_text_stored_text_page(
            &snapshot,
            &physical_generation,
            after_document_id,
            limit,
        )?;
        let current_generation = generations
            .indexes
            .get(name)
            .map(String::as_str)
            .unwrap_or(name);
        if current_generation != physical_generation {
            return Err(BicDbError::Index(format!(
                "published full-text generation changed while reading `{name}`"
            )));
        }
        Ok(FullTextStoredTextPage {
            physical_generation,
            rows,
            next_after_document_id,
        })
    }

    /// Count live logical bytes by page-store namespace for one published FTS
    /// index. This deliberately does no term/posting decoding.
    pub fn full_text_storage_accounting(
        &self,
        name: &str,
    ) -> Result<crate::fts_format::FullTextStorageAccounting> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::FullText {
            return Err(BicDbError::Index(format!(
                "index `{name}` is not full-text"
            )));
        }
        let collection = state.definition.collection.clone();
        drop(state);
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;
        let snapshot = paged.latest_snapshot();
        let physical = paged.resolve_index_name(name);
        let count = |prefix: Vec<u8>| paged.raw_prefix_accounting(&snapshot, &prefix);
        let mut postings = count(crate::paged_collection::index_numeric_posting_block_prefix(
            &physical,
        ))?;
        postings.merge(count(crate::paged_collection::index_posting_block_prefix(
            &physical,
        ))?);
        postings.merge(count(crate::paged_collection::index_tail_prefix(
            &physical,
        ))?);
        postings.merge(count(crate::paged_collection::index_tombstone_prefix(
            &physical,
        ))?);
        let mut impact_metadata = count(
            crate::paged_collection::index_numeric_impact_block_prefix(&physical),
        )?;
        impact_metadata.merge(count(crate::paged_collection::index_impact_block_prefix(
            &physical,
        ))?);
        impact_metadata.merge(count(crate::paged_collection::index_impact_tail_prefix(
            &physical,
        ))?);
        // A packed segment's bytes live in files, not keyed rows. Fold them
        // into the same namespaces so the accounting stays honest across
        // formats: postings and impact bytes are VALUE bytes with ZERO key
        // bytes — which is the entire point of the format — and the term
        // directory stands in for the dictionary.
        let mut segment_postings = crate::fts_format::StorageNamespaceAccounting::default();
        let mut segment_impacts = crate::fts_format::StorageNamespaceAccounting::default();
        let mut segment_dictionary = crate::fts_format::StorageNamespaceAccounting::default();
        if let Some(set) = paged.fts_segment_for(&physical)? {
            let term_count = set.term_count();
            for reader in set.readers() {
                let (postings_bytes, impact_bytes, terms_bytes) = reader.file_bytes();
                segment_postings.value_bytes += postings_bytes;
                segment_impacts.value_bytes += impact_bytes;
                segment_dictionary.value_bytes += terms_bytes;
            }
            segment_postings.entries = term_count;
            segment_impacts.entries = term_count;
            segment_dictionary.entries = term_count;
        }
        Ok(crate::fts_format::FullTextStorageAccounting {
            logical_index: name.to_string(),
            physical_index: physical.clone(),
            row_data: count(crate::paged_collection::collection_storage_prefix(
                &collection,
            ))?,
            document_terms: count(crate::paged_collection::index_document_terms_prefix(
                &physical,
            ))?,
            term_dictionary: {
                let mut merged = count(
                    crate::paged_collection::index_full_text_dictionary_prefix(&physical),
                )?;
                merged.merge(segment_dictionary);
                merged
            },
            document_ids: count(crate::paged_collection::index_document_ids_prefix(
                &physical,
            ))?,
            postings: {
                let mut merged = postings;
                merged.merge(segment_postings);
                merged
            },
            impact_metadata: {
                let mut merged = impact_metadata;
                merged.merge(segment_impacts);
                merged
            },
            document_statistics: count(crate::paged_collection::index_document_statistics_prefix(
                &physical,
            ))?,
            stored_text: count(crate::paged_collection::index_stored_text_prefix(&physical))?,
        })
    }

    /// Decompose the KEY bytes of one FTS index by structural component.
    ///
    /// Returns `(label, forensics)` per namespace. The point is to separate
    /// bytes that carry information from bytes the subtree already implies.
    pub fn full_text_key_forensics(
        &self,
        name: &str,
    ) -> Result<Vec<(String, crate::fts_format::KeyForensics)>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        let collection = state.definition.collection.clone();
        drop(state);
        let physical = self
            .fts_generations
            .lock()
            .indexes
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_string());
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index("key forensics requires page storage".into()))?;
        let snapshot = paged.latest_snapshot();
        let name_len = physical.len();
        let mut probe = |label: &str,
                         prefix: Vec<u8>,
                         term_and_suffix: bool|
         -> Result<(String, crate::fts_format::KeyForensics)> {
            Ok((
                label.to_string(),
                paged.raw_prefix_key_forensics(&snapshot, &prefix, name_len, term_and_suffix)?,
            ))
        };
        let _ = &collection;
        Ok(vec![
            probe(
                "postings (numeric blocks)",
                crate::paged_collection::index_numeric_posting_block_prefix(&physical),
                true,
            )?,
            probe(
                "impact blocks (numeric)",
                crate::paged_collection::index_numeric_impact_block_prefix(&physical),
                true,
            )?,
            probe(
                "term dictionary",
                crate::paged_collection::index_full_text_dictionary_prefix(&physical),
                false,
            )?,
            probe(
                "document terms",
                crate::paged_collection::index_document_terms_prefix(&physical),
                false,
            )?,
            probe(
                "document ids",
                crate::paged_collection::index_document_ids_prefix(&physical),
                false,
            )?,
        ])
    }

    /// Point probe: the posting payload of (term, pk) in a read-through FTS
    /// index, one B-tree descent. `None` = the document does not contain the
    /// term.
    pub fn full_text_posting_probe(
        &self,
        name: &str,
        term: &str,
        pk: &str,
    ) -> Result<Option<Vec<u8>>> {
        if !full_text_term_is_indexable(term) {
            return Ok(None);
        }
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;
        let snapshot = paged.latest_snapshot();
        let encoded = encode_index_key(&[IndexValue::String(term.to_string())]);
        // Precedence: tail write > tombstone > folded block.
        if let Some(payload) = paged.get_index_entry(&snapshot, name, &encoded, pk)? {
            return Ok(Some(payload));
        }
        let has_numeric_blocks = paged.index_has_numeric_posting_blocks(&snapshot, name)?;
        if !has_numeric_blocks && !paged.index_has_posting_blocks(&snapshot, name)? {
            return Ok(None);
        }
        if paged.posting_tombstone_exists(&snapshot, name, &encoded, pk)? {
            return Ok(None);
        }
        if has_numeric_blocks {
            let Some(document_id) = paged.full_text_document_id_for_pk(&snapshot, name, pk)? else {
                return Ok(None);
            };
            let Some(bytes) =
                paged.numeric_posting_block_for(&snapshot, name, &encoded, document_id)?
            else {
                return Ok(None);
            };
            let postings = crate::paged_collection::decode_numeric_posting_block(&bytes)?;
            return Ok(postings
                .into_iter()
                .find(|posting| posting.document_id == document_id)
                .map(|posting| {
                    encode_fts_posting_payload(
                        posting.doc_length,
                        posting.doc_distinct,
                        &posting.packed_positions,
                    )
                }));
        }
        let Some(bytes) = paged.posting_block_for(&snapshot, name, &encoded, pk)? else {
            return Ok(None);
        };
        let (block_postings, _) = crate::paged_collection::decode_posting_block(&bytes)?;
        Ok(block_postings
            .into_iter()
            .find(|posting| posting.pk == pk)
            .map(|posting| {
                encode_fts_posting_payload(
                    posting.doc_length,
                    posting.doc_distinct,
                    &posting.packed_positions,
                )
            }))
    }

    /// Probe MANY pks against one term with the same precedence as
    /// [`Self::full_text_posting_probe`] (tail write > tombstone > folded
    /// block), returning only the hits as `(pk, payload)`.
    ///
    /// `pks` MUST be ascending: the term's posting blocks are walked ONCE in
    /// lockstep with the pks, so each block is decoded at most once — the
    /// per-probe variant decodes a full block per pk, which is what made
    /// probe-driven AND intersection pay a blocks-era decode tax.
    pub fn full_text_posting_probe_many(
        &self,
        name: &str,
        term: &str,
        pks: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>> {
        debug_assert!(pks.windows(2).all(|pair| pair[0] <= pair[1]));
        if !full_text_term_is_indexable(term) {
            return Ok(Vec::new());
        }
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` has no page store")))?;
        let snapshot = paged.latest_snapshot();
        let encoded = encode_index_key(&[IndexValue::String(term.to_string())]);
        let mut tail: FxHashMap<String, Vec<u8>> = FxHashMap::default();
        for entry in paged.scan_index_exact(&snapshot, name, &encoded)? {
            let (pk, payload) = entry?;
            tail.insert(pk, payload);
        }
        let mut hits: Vec<(String, Vec<u8>)> = Vec::new();
        let has_numeric_blocks = paged.index_has_numeric_posting_blocks(&snapshot, name)?;
        if has_numeric_blocks {
            let refs: Vec<&str> = pks.iter().map(String::as_str).collect();
            self.full_text_posting_probe_many_stream(
                name,
                term,
                &refs,
                |index, doc_length, doc_distinct, packed| {
                    hits.push((
                        pks[index].clone(),
                        encode_fts_posting_payload(doc_length, doc_distinct, packed),
                    ));
                    Ok(())
                },
            )?;
            return Ok(hits);
        }
        if !paged.index_has_posting_blocks(&snapshot, name)? {
            for pk in pks {
                if let Some(payload) = tail.get(pk) {
                    hits.push((pk.clone(), payload.clone()));
                }
            }
            return Ok(hits);
        }
        let tombstones: FxHashSet<String> = paged
            .scan_posting_tombstones(&snapshot, name, &encoded)?
            .collect::<Result<_>>()?;
        let mut next = 0usize;
        for block in paged.scan_posting_blocks(&snapshot, name, &encoded)? {
            if next >= pks.len() {
                break;
            }
            let (last_pk, bytes) = block?;
            // The pks this block can answer: everything up to its last pk.
            let start = next;
            while next < pks.len() && pks[next].as_str() <= last_pk.as_str() {
                next += 1;
            }
            if start == next {
                continue;
            }
            let (block_postings, _) = crate::paged_collection::decode_posting_block(&bytes)?;
            let by_pk: FxHashMap<&str, &crate::paged_collection::BlockPosting> = block_postings
                .iter()
                .map(|posting| (posting.pk.as_str(), posting))
                .collect();
            for pk in &pks[start..next] {
                if let Some(payload) = tail.get(pk) {
                    hits.push((pk.clone(), payload.clone()));
                    continue;
                }
                if tombstones.contains(pk) {
                    continue;
                }
                if let Some(posting) = by_pk.get(pk.as_str()) {
                    hits.push((
                        pk.clone(),
                        encode_fts_posting_payload(
                            posting.doc_length,
                            posting.doc_distinct,
                            &posting.packed_positions,
                        ),
                    ));
                }
            }
        }
        // pks beyond the last block can still be tail writes.
        for pk in &pks[next..] {
            if let Some(payload) = tail.get(pk) {
                hits.push((pk.clone(), payload.clone()));
            }
        }
        Ok(hits)
    }

    pub fn lookup_jsonb_term(&self, name: &str, term: &str) -> Result<Vec<String>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::Jsonb {
            return Err(BicDbError::Index(format!(
                "index `{name}` is not a JSONB index"
            )));
        }
        let collection = state.definition.collection.clone();
        let mut rowids = Vec::new();
        state.store.collect_exact(
            &encode_index_key(&[IndexValue::String(term.to_string())]),
            &mut rowids,
        );
        index_trace::record(&index_trace::POINT);
        rowids.sort_unstable();
        rowids.dedup();
        Ok(self.rowids_to_pks_sorted(&collection, &rowids))
    }

    pub fn lookup_array_term(&self, name: &str, term: &str) -> Result<Vec<String>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::Array {
            return Err(BicDbError::Index(format!(
                "index `{name}` is not an array index"
            )));
        }
        let collection = state.definition.collection.clone();
        let mut rowids = Vec::new();
        state.store.collect_exact(
            &encode_index_key(&[IndexValue::String(term.to_string())]),
            &mut rowids,
        );
        index_trace::record(&index_trace::POINT);
        rowids.sort_unstable();
        rowids.dedup();
        Ok(self.rowids_to_pks_sorted(&collection, &rowids))
    }

    pub fn lookup_index_exact(&self, name: &str, key: &[IndexValue]) -> Result<Vec<String>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if key.is_empty() || key.len() != state.definition.fields.len() {
            return Err(BicDbError::Index(format!(
                "invalid exact lookup key for index `{name}`"
            )));
        }
        if state.paged_read_through && state.definition.kind == IndexKind::BTree {
            drop(state);
            return self.lookup_index(name, key);
        }
        let collection = state.definition.collection.clone();
        let mut rowids: Vec<RowId> = Vec::new();
        state
            .store
            .collect_exact(&encode_index_key(key), &mut rowids);
        index_trace::record(&index_trace::POINT);
        Ok(self.rowids_to_pks_sorted(&collection, &rowids))
    }

    pub fn lookup_index_extreme(
        &self,
        name: &str,
        prefix: &[IndexValue],
        greatest: bool,
    ) -> Result<Option<(Vec<IndexValue>, Vec<String>)>> {
        self.lookup_index_extreme_with_filters(name, prefix, greatest, &[])
    }

    /// The immediate successor of `prefix` in bytewise order: the smallest
    /// byte string greater than every string starting with `prefix`. `None`
    /// when no bound exists (prefix is all 0xFF). Used as the exclusive
    /// upper bound for descending scans over a prefix range.
    pub(crate) fn encoded_prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
        let mut bound = prefix.to_vec();
        while bound.last() == Some(&0xFF) {
            bound.pop();
        }
        let last = bound.last_mut()?;
        *last += 1;
        Some(bound)
    }

    pub fn lookup_index_extreme_with_filters(
        &self,
        name: &str,
        prefix: &[IndexValue],
        greatest: bool,
        filters: &[(usize, IndexValue)],
    ) -> Result<Option<(Vec<IndexValue>, Vec<String>)>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if prefix.len() >= state.definition.fields.len() {
            return Err(BicDbError::Index(format!(
                "invalid extreme lookup prefix for index `{name}`"
            )));
        }
        if filters
            .iter()
            .any(|(idx, _)| *idx >= state.definition.fields.len())
        {
            return Err(BicDbError::Index(format!(
                "invalid extreme lookup filter for index `{name}`"
            )));
        }

        let collection = state.definition.collection.clone();
        let encoded_prefix = encode_index_key(prefix);
        if state.paged_read_through && state.definition.kind == IndexKind::BTree {
            let paged = self
                .paged_records
                .as_ref()
                .ok_or_else(|| BicDbError::Index(format!("index `{name}` lost its page store")))?;
            let snapshot = paged.latest_snapshot();
            // Scan in the extreme's direction and stop at the boundary of the
            // first filter-passing key: O(entries-until-stop), not O(index).
            // Descending starts one root descent below the prefix successor.
            let bound = Self::encoded_prefix_successor(&encoded_prefix);
            let mut cursor: Box<
                dyn Iterator<
                    Item = Result<(Vec<u8>, crate::paged_collection::IndexEntryRef, Vec<u8>)>,
                >,
            > = match (greatest, prefix.is_empty()) {
                (true, true) => Box::new(paged.scan_index_rev_refs(&snapshot, name)?),
                (true, false) => match bound.as_deref() {
                    Some(bound) => {
                        Box::new(paged.scan_index_rev_below_refs(&snapshot, name, bound)?)
                    }
                    None => Box::new(paged.scan_index_rev_refs(&snapshot, name)?),
                },
                (false, _) => {
                    Box::new(paged.scan_index_from_refs(&snapshot, name, &encoded_prefix)?)
                }
            };
            let hints_on = paged_tid_hints_enabled();
            let mut selected: Option<(Vec<u8>, Vec<String>)> = None;
            for entry in &mut cursor {
                let (encoded_key, entry_ref, entry_value) = entry?;
                let pk = resolve_paged_entry_pk(
                    paged,
                    &snapshot,
                    &state.definition.collection,
                    name,
                    entry_ref,
                    &entry_value,
                    hints_on,
                )?;
                if !prefix.is_empty() && !encoded_key.starts_with(&encoded_prefix) {
                    // Directional scans leave the prefix range exactly once.
                    break;
                }
                let key = decode_index_key(&encoded_key);
                if filters
                    .iter()
                    .any(|(idx, expected)| key.get(*idx) != Some(expected))
                {
                    continue;
                }
                match selected.as_mut() {
                    Some((key, pks)) if *key == encoded_key => pks.push(pk),
                    Some(_) => break,
                    None => selected = Some((encoded_key, vec![pk])),
                }
            }
            drop(cursor);
            index_trace::record(&index_trace::EXTREME);
            return Ok(selected.map(|(encoded, mut pks)| {
                pks.sort();
                pks.dedup();
                (decode_index_key(&encoded), pks)
            }));
        }
        let mut best: Option<(Vec<IndexValue>, Vec<RowId>)> = None;
        let mut visit = |encoded_key: &[u8], key_ids: &mut dyn Iterator<Item = RowId>| {
            if !prefix.is_empty() && !encoded_key.starts_with(&encoded_prefix) {
                return false;
            }
            let key = decode_index_key(encoded_key);
            if filters
                .iter()
                .any(|(idx, expected)| key.get(*idx) != Some(expected))
            {
                return true; // keep scanning past a filtered-out key
            }
            best = Some((key, key_ids.collect::<Vec<RowId>>()));
            // Scan runs in the extreme's direction (descending for `greatest`,
            // ascending for least), so the FIRST filter-passing key is the answer —
            // stop, instead of cloning every remaining matching key's id set.
            false
        };
        // A fixed leading-field prefix routes to a single shard (no merge); an
        // empty prefix is a global extreme spanning the whole (cross-shard) store.
        match (greatest, prefix.is_empty()) {
            (true, true) => state.store.scan_rev(&mut visit),
            (true, false) => state.store.scan_prefix_rev(&encoded_prefix, &mut visit),
            (false, true) => state.store.scan_from(&encoded_prefix, &mut visit),
            (false, false) => state.store.scan_prefix(&encoded_prefix, &mut visit),
        }
        index_trace::record(&index_trace::EXTREME);
        Ok(best.map(|(key, rowids)| (key, self.rowids_to_pks(&collection, &rowids))))
    }

    pub fn range_index(
        &self,
        name: &str,
        lower: Option<&IndexValue>,
        upper: Option<&IndexValue>,
    ) -> Result<Vec<String>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.fields.len() != 1 {
            return Err(BicDbError::Index(format!(
                "range scans require a single-field index, got `{name}`"
            )));
        }
        let collection = state.definition.collection.clone();
        if state.paged_read_through && state.definition.kind == IndexKind::BTree {
            let paged = self
                .paged_records
                .as_ref()
                .ok_or_else(|| BicDbError::Index(format!("index `{name}` lost its page store")))?;
            let snapshot = paged.latest_snapshot();
            let mut pks = Vec::new();
            for entry in paged.scan_index(&snapshot, name)? {
                let (encoded, pk) = entry?;
                let value = decode_index_field(&encoded, 0);
                if !(lower.is_some_and(|lower| &value < lower)
                    || upper.is_some_and(|upper| &value > upper))
                {
                    pks.push(pk);
                }
            }
            pks.sort();
            pks.dedup();
            return Ok(pks);
        }
        let mut rowids: Vec<RowId> = Vec::new();
        state.store.scan_from(&[], &mut |encoded_key, key_ids| {
            let value = decode_index_field(encoded_key, 0);
            if !(lower.is_some_and(|lower| &value < lower)
                || upper.is_some_and(|upper| &value > upper))
            {
                rowids.extend(key_ids);
            }
            true
        });
        index_trace::record_scan(&index_trace::RANGE, rowids.len());
        Ok(self.rowids_to_pks(&collection, &rowids))
    }

    pub fn range_index_with_prefix(
        &self,
        name: &str,
        prefix: &[IndexValue],
        lower: Option<&IndexValue>,
        upper: Option<&IndexValue>,
    ) -> Result<Vec<String>> {
        self.range_index_with_prefix_filters(name, prefix, lower, upper, &[])
    }

    /// Like [`Self::range_index_with_prefix_filters`] but yields raw [`RowId`]
    /// locators (no rowid->pk conversion). Pair with [`Self::get_by_rowid`].
    pub fn range_index_with_prefix_filters_rowids(
        &self,
        name: &str,
        prefix: &[IndexValue],
        lower: Option<&IndexValue>,
        upper: Option<&IndexValue>,
        filters: &[(usize, IndexValue)],
    ) -> Result<Vec<RowId>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if prefix.len() >= state.definition.fields.len()
            || (prefix.is_empty() && lower.is_none() && upper.is_none())
        {
            return Err(BicDbError::Index(format!(
                "invalid range lookup prefix for index `{name}`"
            )));
        }
        if filters
            .iter()
            .any(|(idx, _)| *idx >= state.definition.fields.len())
        {
            return Err(BicDbError::Index(format!(
                "invalid range lookup filter for index `{name}`"
            )));
        }
        let range_field_idx = prefix.len();
        let mut rowids: Vec<RowId> = Vec::new();
        let mut start = prefix.to_vec();
        if let Some(lower) = lower {
            start.push(lower.clone());
        }
        let encoded_start = encode_index_key(&start);
        if state.paged_read_through && state.definition.kind == IndexKind::BTree {
            // Read-through: the resident store is empty for this index, so
            // scanning it would silently return zero rows. Range over the
            // durable keyspace from the encoded start — the entry escaping is
            // order-preserving, so this is the same bounded scan the resident
            // path performs — and map each pk to its rowid through the
            // registry.
            let paged = self
                .paged_records
                .as_ref()
                .ok_or_else(|| BicDbError::Index(format!("index `{name}` lost its page store")))?;
            let snapshot = paged.latest_snapshot();
            let mut pks = Vec::new();
            let hints_on = paged_tid_hints_enabled();
            let target = &state.definition.collection;
            for entry in paged.scan_index_from_refs(&snapshot, name, &encoded_start)? {
                let (encoded, entry_ref, entry_value) = entry?;
                let key = decode_index_key(&encoded);
                if !prefix.is_empty() && !key.starts_with(prefix) {
                    break;
                }
                let value = &key[range_field_idx];
                if lower.is_some_and(|lower| value < lower) {
                    continue;
                }
                if upper.is_some_and(|upper| value > upper) {
                    break;
                }
                if filters
                    .iter()
                    .any(|(idx, expected)| key.get(*idx) != Some(expected))
                {
                    continue;
                }
                pks.push(resolve_paged_entry_pk(
                    paged,
                    &snapshot,
                    target,
                    name,
                    entry_ref,
                    &entry_value,
                    hints_on,
                )?);
            }
            let collection = self.collection_state(&state.definition.collection)?;
            for pk in pks {
                let rowid = collection.shard(&pk).read().rowid_of(&pk).ok_or_else(|| {
                    BicDbError::Index(format!(
                        "durable index `{name}` references missing row `{pk}`"
                    ))
                })?;
                rowids.push(rowid);
            }
            index_trace::record_scan(&index_trace::RANGE, rowids.len());
            return Ok(rowids);
        }
        state
            .store
            .scan_from(&encoded_start, &mut |encoded_key, key_ids| {
                let key = decode_index_key(encoded_key);
                if !prefix.is_empty() && !key.starts_with(prefix) {
                    return false;
                }
                let value = &key[range_field_idx];
                if lower.is_some_and(|lower| value < lower) {
                    return true;
                }
                if upper.is_some_and(|upper| value > upper) {
                    return false;
                }
                if filters
                    .iter()
                    .any(|(idx, expected)| key.get(*idx) != Some(expected))
                {
                    return true;
                }
                rowids.extend(key_ids);
                true
            });
        index_trace::record_scan(&index_trace::RANGE, rowids.len());
        Ok(rowids)
    }

    pub fn range_index_with_prefix_filters(
        &self,
        name: &str,
        prefix: &[IndexValue],
        lower: Option<&IndexValue>,
        upper: Option<&IndexValue>,
        filters: &[(usize, IndexValue)],
    ) -> Result<Vec<String>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.paged_read_through && state.definition.kind == IndexKind::BTree {
            if prefix.len() >= state.definition.fields.len()
                || (prefix.is_empty() && lower.is_none() && upper.is_none())
            {
                return Err(BicDbError::Index(format!(
                    "invalid range lookup prefix for index `{name}`"
                )));
            }
            let range_field = prefix.len();
            let paged = self
                .paged_records
                .as_ref()
                .ok_or_else(|| BicDbError::Index(format!("index `{name}` lost its page store")))?;
            let snapshot = paged.latest_snapshot();
            // Bounded: descend to the encoded start and stop at the first
            // entry past the prefix or upper bound, instead of walking the
            // whole index namespace.
            let mut start = prefix.to_vec();
            if let Some(lower) = lower {
                start.push(lower.clone());
            }
            let encoded_start = encode_index_key(&start);
            let mut pks = Vec::new();
            for entry in paged.scan_index_from_refs(&snapshot, name, &encoded_start)? {
                let (encoded, entry_ref, entry_value) = entry?;
                let pk = resolve_paged_entry_pk(
                    paged,
                    &snapshot,
                    &state.definition.collection,
                    name,
                    entry_ref,
                    &entry_value,
                    paged_tid_hints_enabled(),
                )?;
                let key = decode_index_key(&encoded);
                if !prefix.is_empty() && !key.starts_with(prefix) {
                    break;
                }
                if upper.is_some_and(|upper| &key[range_field] > upper) {
                    break;
                }
                if lower.is_some_and(|lower| &key[range_field] < lower)
                    || filters
                        .iter()
                        .any(|(idx, expected)| key.get(*idx) != Some(expected))
                {
                    continue;
                }
                pks.push(pk);
            }
            pks.sort();
            pks.dedup();
            return Ok(pks);
        }
        drop(state);
        let collection = self.index_collection(name)?;
        let rowids =
            self.range_index_with_prefix_filters_rowids(name, prefix, lower, upper, filters)?;
        Ok(self.rowids_to_pks(&collection, &rowids))
    }

    pub fn ordered_index_records(
        &self,
        name: &str,
        descending: bool,
        limit: Option<usize>,
    ) -> Result<Vec<String>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.fields.len() != 1 {
            return Err(BicDbError::Index(format!(
                "ordered scans require a single-field index, got `{name}`"
            )));
        }

        let collection = state.definition.collection.clone();
        if state.paged_read_through && state.definition.kind == IndexKind::BTree {
            if limit == Some(0) {
                return Ok(Vec::new());
            }
            let paged = self
                .paged_records
                .as_ref()
                .ok_or_else(|| BicDbError::Index(format!("index `{name}` lost its page store")))?;
            let snapshot = paged.latest_snapshot();
            // Both directions stop after `limit` entries; descending runs the
            // reverse durable cursor instead of forward-scanning the whole
            // namespace to keep its tail.
            let mut pks = Vec::new();
            let mut cursor: Box<dyn Iterator<Item = Result<(Vec<u8>, String)>>> = if descending {
                Box::new(paged.scan_index_rev(&snapshot, name)?)
            } else {
                Box::new(paged.scan_index(&snapshot, name)?)
            };
            for entry in &mut cursor {
                let (_, pk) = entry?;
                pks.push(pk);
                if limit.is_some_and(|limit| pks.len() >= limit) {
                    break;
                }
            }
            drop(cursor);
            index_trace::record_scan(&index_trace::ORDERED, pks.len());
            return Ok(pks);
        }
        let mut rowids: Vec<RowId> = Vec::new();
        let visit = &mut |_key: &[u8], key_ids: &mut dyn Iterator<Item = RowId>| {
            // Match prior behaviour: per-key id ties are also reversed when the
            // overall scan is descending.
            let mut batch: Vec<RowId> = key_ids.collect();
            if descending {
                batch.reverse();
            }
            for id in batch {
                rowids.push(id);
                if limit.is_some_and(|limit| rowids.len() >= limit) {
                    return false;
                }
            }
            true
        };
        if descending {
            state.store.scan_rev(visit);
        } else {
            state.store.scan_from(&[], visit);
        }
        index_trace::record_scan(&index_trace::ORDERED, rowids.len());
        Ok(self.rowids_to_pks(&collection, &rowids))
    }

    /// Up to `limit` record ids whose index key starts with `prefix`, in ascending
    /// (`descending=false`) or descending key order. The bounded driver for
    /// `WHERE <equality prefix> ORDER BY <next field> LIMIT k` (e.g. a customer's
    /// latest order): it stops after `limit` ids instead of materializing every
    /// prefix match, so cost is O(limit) rather than O(matches). `prefix` fixes the
    /// leading index fields, so a sharded store routes to a single shard.
    pub fn prefix_ordered_index_records(
        &self,
        name: &str,
        prefix: &[IndexValue],
        descending: bool,
        limit: usize,
    ) -> Result<Vec<String>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if prefix.is_empty() || prefix.len() >= state.definition.fields.len() {
            return Err(BicDbError::Index(format!(
                "invalid prefix-ordered scan for index `{name}`"
            )));
        }
        let collection = state.definition.collection.clone();
        let encoded_prefix = encode_index_key(prefix);
        if state.paged_read_through && state.definition.kind == IndexKind::BTree {
            if limit == 0 {
                return Ok(Vec::new());
            }
            let paged = self
                .paged_records
                .as_ref()
                .ok_or_else(|| BicDbError::Index(format!("index `{name}` lost its page store")))?;
            let snapshot = paged.latest_snapshot();
            // Both directions stop after `limit` entries; descending runs the
            // reverse durable cursor from just below the prefix successor
            // instead of forward-scanning the whole prefix range for its tail.
            let mut pks = Vec::with_capacity(limit.min(4096));
            let bound = Self::encoded_prefix_successor(&encoded_prefix);
            let mut cursor: Box<
                dyn Iterator<
                    Item = Result<(Vec<u8>, crate::paged_collection::IndexEntryRef, Vec<u8>)>,
                >,
            > = if descending {
                match bound.as_deref() {
                    Some(bound) => {
                        Box::new(paged.scan_index_rev_below_refs(&snapshot, name, bound)?)
                    }
                    None => Box::new(paged.scan_index_rev_refs(&snapshot, name)?),
                }
            } else {
                Box::new(paged.scan_index_encoded_prefix_refs(&snapshot, name, &encoded_prefix)?)
            };
            let hints_on = paged_tid_hints_enabled();
            for entry in &mut cursor {
                let (encoded, entry_ref, entry_value) = entry?;
                if !encoded.starts_with(&encoded_prefix) {
                    // A directional scan leaves the prefix range exactly once.
                    break;
                }
                pks.push(resolve_paged_entry_pk(
                    paged,
                    &snapshot,
                    &collection,
                    name,
                    entry_ref,
                    &entry_value,
                    hints_on,
                )?);
                if pks.len() >= limit {
                    break;
                }
            }
            drop(cursor);
            index_trace::record_scan(&index_trace::ORDERED, pks.len());
            return Ok(pks);
        }
        let mut rowids: Vec<RowId> = Vec::new();
        if limit == 0 {
            return Ok(Vec::new());
        }
        let visit = &mut |_key: &[u8], key_ids: &mut dyn Iterator<Item = RowId>| {
            // Reverse per-key id ties on a descending scan, mirroring
            // ordered_index_records so tie ordering is consistent.
            let mut batch: Vec<RowId> = key_ids.collect();
            if descending {
                batch.reverse();
            }
            for id in batch {
                rowids.push(id);
                if rowids.len() >= limit {
                    return false;
                }
            }
            true
        };
        if descending {
            state.store.scan_prefix_rev(&encoded_prefix, visit);
        } else {
            state.store.scan_prefix(&encoded_prefix, visit);
        }
        index_trace::record_scan(&index_trace::ORDERED, rowids.len());
        Ok(self.rowids_to_pks(&collection, &rowids))
    }

    pub fn spatial_radius_index(
        &self,
        name: &str,
        lon: f64,
        lat: f64,
        meters: f64,
    ) -> Result<Vec<String>> {
        if meters < 0.0 || !lon.is_finite() || !lat.is_finite() || !meters.is_finite() {
            return Err(BicDbError::Index(
                "spatial radius lookup requires finite lon/lat and non-negative meters".to_string(),
            ));
        }
        let index = self.spatial_index_state(name)?;
        let envelope = lon_lat_radius_envelope(lon, lat, meters);
        let mut ids = Vec::new();
        for entry in self.spatial_candidates_in_envelope(&index, &envelope)? {
            let Some(point) = entry.point else {
                return Err(BicDbError::Index(format!(
                    "spatial radius lookup on `{name}` supports only point geometries"
                )));
            };
            if haversine_meters(lon, lat, point[0], point[1]) <= meters {
                ids.push(entry.record_id);
            }
        }
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    pub fn nearest(
        &self,
        collection: &str,
        field: &str,
        lon: f64,
        lat: f64,
        limit: usize,
    ) -> Result<Vec<SpatialQueryResult>> {
        self.ensure_spatial_point_query(collection, field, lon, lat, Some(limit), None)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let index_field = spatial_field_from_name(field)?;
        self.ensure_spatial_field_present(collection, &index_field)?;
        let ids = if let Some(index_name) = self.spatial_index_for(collection, &index_field) {
            self.spatial_nearest_index(&index_name, lon, lat, limit)?
        } else {
            Vec::new()
        };
        if !ids.is_empty() {
            return self.spatial_results_from_ids(collection, &index_field, lon, lat, ids);
        }

        let mut results = self.spatial_point_scan(collection, &index_field, lon, lat, None)?;
        results.sort_by(|left, right| {
            left.distance_meters
                .total_cmp(&right.distance_meters)
                .then_with(|| left.record.id.cmp(&right.record.id))
        });
        results.truncate(limit);
        Ok(results)
    }

    pub fn within_radius(
        &self,
        collection: &str,
        field: &str,
        lon: f64,
        lat: f64,
        meters: f64,
    ) -> Result<Vec<SpatialQueryResult>> {
        self.ensure_spatial_point_query(collection, field, lon, lat, None, Some(meters))?;
        let index_field = spatial_field_from_name(field)?;
        self.ensure_spatial_field_present(collection, &index_field)?;
        let ids = if let Some(index_name) = self.spatial_index_for(collection, &index_field) {
            self.spatial_radius_index(&index_name, lon, lat, meters)?
        } else {
            Vec::new()
        };
        let mut results = if !ids.is_empty() {
            self.spatial_results_from_ids(collection, &index_field, lon, lat, ids)?
        } else {
            self.spatial_point_scan(collection, &index_field, lon, lat, Some(meters))?
        };
        results.retain(|result| result.distance_meters <= meters);
        results.sort_by(|left, right| {
            left.distance_meters
                .total_cmp(&right.distance_meters)
                .then_with(|| left.record.id.cmp(&right.record.id))
        });
        Ok(results)
    }

    pub fn shortest_path(
        &self,
        graph: &str,
        start: &Geometry,
        end: &Geometry,
    ) -> Result<RoutePath> {
        self.shortest_path_with_algorithm(graph, start, end, RouteAlgorithm::Dijkstra)
    }

    pub fn shortest_path_with_events(
        &mut self,
        graph: &str,
        start: &Geometry,
        end: &Geometry,
    ) -> Result<RoutePath> {
        let path =
            self.shortest_path_with_algorithm(graph, start, end, RouteAlgorithm::Dijkstra)?;
        self.emit_route_path_computed("shortest_path", RouteAlgorithm::Dijkstra, &path)?;
        Ok(path)
    }

    pub fn shortest_path_astar(
        &self,
        graph: &str,
        start: &Geometry,
        end: &Geometry,
    ) -> Result<RoutePath> {
        self.shortest_path_with_algorithm(graph, start, end, RouteAlgorithm::AStar)
    }

    pub fn shortest_path_astar_with_events(
        &mut self,
        graph: &str,
        start: &Geometry,
        end: &Geometry,
    ) -> Result<RoutePath> {
        let path = self.shortest_path_with_algorithm(graph, start, end, RouteAlgorithm::AStar)?;
        self.emit_route_path_computed("shortest_path", RouteAlgorithm::AStar, &path)?;
        Ok(path)
    }

    pub fn route_distance(&self, graph: &str, start: &Geometry, end: &Geometry) -> Result<f64> {
        Ok(self.shortest_path(graph, start, end)?.distance_m)
    }

    /// Travel time in seconds between two points over a road graph, using
    /// profile-costed Dijkstra (the reachability primitive behind
    /// `travel_time(...)` in SQL).
    pub fn travel_time_seconds(
        &self,
        graph: &str,
        start: &Geometry,
        end: &Geometry,
        profile: RouteProfile,
    ) -> Result<f64> {
        let start = routing_point("start", start)?;
        let end = routing_point("end", end)?;
        let road_graph = self.load_road_graph(graph)?;
        let start_snap = road_graph.nearest_node(start.0, start.1, "start")?;
        let end_snap = road_graph.nearest_node(end.0, end.1, "end")?;
        let times = petgraph::algo::dijkstra(
            &road_graph.graph,
            start_snap.index,
            Some(end_snap.index),
            |edge| profile.edge_seconds(edge.weight()),
        );
        times.get(&end_snap.index).copied().ok_or_else(|| {
            BicDbError::Routing(format!(
                "no route in road graph `{graph}` between snapped nodes"
            ))
        })
    }

    /// Many-to-many travel-time matrix: one single-source Dijkstra per
    /// origin. `None` marks unreachable pairs.
    pub fn travel_time_matrix(
        &self,
        graph: &str,
        origins: &[Geometry],
        destinations: &[Geometry],
        profile: RouteProfile,
    ) -> Result<Vec<Vec<Option<f64>>>> {
        let road_graph = self.load_road_graph(graph)?;
        let mut destination_indexes = Vec::with_capacity(destinations.len());
        for destination in destinations {
            let (lon, lat) = routing_point("destination", destination)?;
            destination_indexes.push(road_graph.nearest_node(lon, lat, "destination")?.index);
        }
        let mut matrix = Vec::with_capacity(origins.len());
        for origin in origins {
            let (lon, lat) = routing_point("origin", origin)?;
            let origin_index = road_graph.nearest_node(lon, lat, "origin")?.index;
            let times = petgraph::algo::dijkstra(&road_graph.graph, origin_index, None, |edge| {
                profile.edge_seconds(edge.weight())
            });
            matrix.push(
                destination_indexes
                    .iter()
                    .map(|index| times.get(index).copied())
                    .collect(),
            );
        }
        Ok(matrix)
    }

    /// The set of road nodes reachable within `max_seconds`, returned as a
    /// convex-hull polygon — the catchment behind "reachable in 15
    /// minutes". Concave/cell-based hulls are a later refinement; convex is
    /// deterministic and conservative (never understates reach).
    pub fn isochrone(
        &self,
        graph: &str,
        origin: &Geometry,
        max_seconds: f64,
        profile: RouteProfile,
    ) -> Result<Geometry> {
        if !(max_seconds > 0.0) || !max_seconds.is_finite() {
            return Err(BicDbError::Routing(
                "isochrone requires a positive time budget".to_string(),
            ));
        }
        let (lon, lat) = routing_point("origin", origin)?;
        let road_graph = self.load_road_graph(graph)?;
        let origin_index = road_graph.nearest_node(lon, lat, "origin")?.index;
        let times = petgraph::algo::dijkstra(&road_graph.graph, origin_index, None, |edge| {
            profile.edge_seconds(edge.weight())
        });
        // Each reached node contributes a ~50m square of standing room, so
        // the hull is well-formed even for sparse or collinear road chains.
        let mut reached: Vec<geo_types::Coord<f64>> = Vec::new();
        for (index, seconds) in &times {
            if *seconds > max_seconds {
                continue;
            }
            let node = &road_graph.graph[*index];
            let pad_lat = 50.0 / 111_320.0;
            let pad_lon = pad_lat / node.lat.to_radians().cos().abs().max(0.01);
            for (dx, dy) in [(-1.0, -1.0), (1.0, -1.0), (1.0, 1.0), (-1.0, 1.0)] {
                reached.push(geo_types::Coord {
                    x: node.lon + dx * pad_lon,
                    y: node.lat + dy * pad_lat,
                });
            }
        }
        if reached.is_empty() {
            return Err(BicDbError::Routing(
                "isochrone reaches no nodes; increase the time budget".to_string(),
            ));
        }
        let hull = convex_hull_ring(reached);
        if hull.len() < 4 {
            return Err(BicDbError::Routing(
                "isochrone hull degenerated unexpectedly".to_string(),
            ));
        }
        Ok(Geometry::Polygon(geo_types::Polygon::new(
            geo_types::LineString::new(hull),
            Vec::new(),
        )))
    }

    pub fn route_distance_with_events(
        &mut self,
        graph: &str,
        start: &Geometry,
        end: &Geometry,
    ) -> Result<f64> {
        let path =
            self.shortest_path_with_algorithm(graph, start, end, RouteAlgorithm::Dijkstra)?;
        self.emit_route_path_computed("route_distance", RouteAlgorithm::Dijkstra, &path)?;
        Ok(path.distance_m)
    }

    pub fn import_osm_pbf(
        &mut self,
        pbf_path: impl AsRef<Path>,
        bbox: OsmImportBbox,
    ) -> Result<OsmImportReport> {
        self.import_osm_pbf_as("roads", pbf_path, bbox)
    }

    // The API stays present without `osm-import` so callers degrade at
    // runtime (like a missing model file) instead of failing to compile.
    #[cfg(not(feature = "osm-import"))]
    pub fn import_osm_pbf_as(
        &mut self,
        graph: &str,
        _pbf_path: impl AsRef<Path>,
        bbox: OsmImportBbox,
    ) -> Result<OsmImportReport> {
        validate_collection_name(graph)?;
        bbox.validate()?;
        Err(BicDbError::Routing(
            "OSM PBF import requires a build with the `osm-import` feature".to_string(),
        ))
    }

    #[cfg(feature = "osm-import")]
    pub fn import_osm_pbf_as(
        &mut self,
        graph: &str,
        pbf_path: impl AsRef<Path>,
        bbox: OsmImportBbox,
    ) -> Result<OsmImportReport> {
        validate_collection_name(graph)?;
        bbox.validate()?;

        let pbf_path = pbf_path.as_ref();
        let nodes = osm_nodes_in_bbox(pbf_path, bbox)?;
        let ways = osm_road_ways_in_bbox(pbf_path, &nodes)?;

        let nodes_collection = format!("{graph}_nodes");
        let edges_collection = format!("{graph}_edges");
        if self.collections.contains_key(&nodes_collection)
            || self.collections.contains_key(&edges_collection)
        {
            return Err(BicDbError::Routing(format!(
                "road graph `{graph}` would overwrite an existing collection; choose a new graph name"
            )));
        }
        self.create_collection(&nodes_collection)?;
        self.create_collection(&edges_collection)?;

        let mut used_nodes = BTreeSet::new();
        let mut edge_records = Vec::new();
        for way in &ways {
            for edge in osm_way_edges(way, &nodes) {
                used_nodes.insert(edge.from.clone());
                used_nodes.insert(edge.to.clone());
                edge_records.push(edge.into_record());
            }
        }

        let node_records = used_nodes
            .iter()
            .filter_map(|node_id| nodes.get(node_id))
            .map(OsmNode::to_record)
            .collect::<Vec<_>>();
        let node_count = node_records.len();
        let edge_count = edge_records.len();

        self.batch_insert(&nodes_collection, node_records)?;
        self.batch_insert(&edges_collection, edge_records)?;

        Ok(OsmImportReport {
            graph: graph.to_string(),
            bbox,
            road_ways: ways.len(),
            nodes: node_count,
            edges: edge_count,
        })
    }

    pub fn nearest_neighbor_route(
        &self,
        graph: &str,
        stops: &[Geometry],
    ) -> Result<OptimizedRoute> {
        let matrix = self.route_distance_matrix(graph, stops)?;
        let order = nearest_neighbor_order(&matrix.distances);
        Ok(route_optimization_result(
            graph,
            &matrix.distance_mode,
            "nearest_neighbor",
            stops,
            order,
            &matrix.distances,
        ))
    }

    pub fn nearest_neighbor_route_with_events(
        &mut self,
        graph: &str,
        stops: &[Geometry],
    ) -> Result<OptimizedRoute> {
        let route = self.nearest_neighbor_route(graph, stops)?;
        self.emit_optimized_route_computed("nearest_neighbor_route", &route)?;
        Ok(route)
    }

    pub fn optimize_route(&self, graph: &str, stops: &[Geometry]) -> Result<OptimizedRoute> {
        let matrix = self.route_distance_matrix(graph, stops)?;
        let nearest = two_opt_order(nearest_neighbor_order(&matrix.distances), &matrix.distances);
        let input = two_opt_order((0..stops.len()).collect(), &matrix.distances);
        let order = if route_order_distance(&input, &matrix.distances)
            < route_order_distance(&nearest, &matrix.distances)
        {
            input
        } else {
            nearest
        };
        Ok(route_optimization_result(
            graph,
            &matrix.distance_mode,
            "nearest_neighbor_2opt",
            stops,
            order,
            &matrix.distances,
        ))
    }

    pub fn optimize_route_with_events(
        &mut self,
        graph: &str,
        stops: &[Geometry],
    ) -> Result<OptimizedRoute> {
        let route = self.optimize_route(graph, stops)?;
        self.emit_optimized_route_computed("optimize_route", &route)?;
        Ok(route)
    }

    pub(crate) fn emit_route_path_computed(
        &mut self,
        operation: &str,
        algorithm: RouteAlgorithm,
        path: &RoutePath,
    ) -> Result<()> {
        if self.config.audit_events {
            self.events
                .lock()
                .append(route_path_computed_event(operation, algorithm, path)?)?;
        }
        Ok(())
    }

    pub(crate) fn emit_optimized_route_computed(
        &mut self,
        operation: &str,
        route: &OptimizedRoute,
    ) -> Result<()> {
        if self.config.audit_events {
            self.events
                .lock()
                .append(optimized_route_computed_event(operation, route)?)?;
        }
        Ok(())
    }

    pub(crate) fn shortest_path_with_algorithm(
        &self,
        graph: &str,
        start: &Geometry,
        end: &Geometry,
        algorithm: RouteAlgorithm,
    ) -> Result<RoutePath> {
        let start = routing_point("start", start)?;
        let end = routing_point("end", end)?;
        let road_graph = self.load_road_graph(graph)?;
        let start_snap = road_graph.nearest_node(start.0, start.1, "start")?;
        let end_snap = road_graph.nearest_node(end.0, end.1, "end")?;
        let (distance_m, path) = match algorithm {
            RouteAlgorithm::Dijkstra => {
                road_graph.dijkstra_path(start_snap.index, end_snap.index)?
            }
            RouteAlgorithm::AStar => road_graph.astar_path(start_snap.index, end_snap.index)?,
        };
        let node_ids = path
            .iter()
            .map(|index| road_graph.graph[*index].id.clone())
            .collect::<Vec<_>>();
        Ok(RoutePath {
            graph: graph.to_string(),
            start: start_snap.into_public(&road_graph),
            end: end_snap.into_public(&road_graph),
            node_ids,
            distance_m,
        })
    }

    pub(crate) fn route_distance_matrix(
        &self,
        graph: &str,
        stops: &[Geometry],
    ) -> Result<RouteDistanceMatrix> {
        if stops.len() < 2 {
            return Err(BicDbError::Routing(
                "route optimization requires at least two stops".to_string(),
            ));
        }

        let points = stops
            .iter()
            .enumerate()
            .map(|(index, stop)| routing_point(&format!("stop {index}"), stop))
            .collect::<Result<Vec<_>>>()?;

        let use_graph = self.collections.contains_key(&format!("{graph}_nodes"))
            && self.collections.contains_key(&format!("{graph}_edges"));
        let road_graph = if use_graph {
            Some(self.load_road_graph(graph)?)
        } else {
            None
        };
        let mut distances = vec![vec![0.0; stops.len()]; stops.len()];
        for left in 0..stops.len() {
            for right in 0..stops.len() {
                if left == right {
                    continue;
                }
                distances[left][right] = if let Some(road_graph) = &road_graph {
                    let left_snap =
                        road_graph.nearest_node(points[left].0, points[left].1, "route stop")?;
                    let right_snap =
                        road_graph.nearest_node(points[right].0, points[right].1, "route stop")?;
                    road_graph
                        .dijkstra_path(left_snap.index, right_snap.index)?
                        .0
                } else {
                    haversine_meters(
                        points[left].0,
                        points[left].1,
                        points[right].0,
                        points[right].1,
                    )
                };
            }
        }

        Ok(RouteDistanceMatrix {
            distance_mode: if road_graph.is_some() {
                "route_graph".to_string()
            } else {
                "haversine".to_string()
            },
            distances,
        })
    }

    pub(crate) fn load_road_graph(&self, graph: &str) -> Result<RoadGraph> {
        validate_collection_name(graph)?;
        let nodes_collection = format!("{graph}_nodes");
        let edges_collection = format!("{graph}_edges");
        let nodes = self.scan_collection(&nodes_collection)?;
        let edges = self.scan_collection(&edges_collection)?;
        RoadGraph::build(graph, nodes, edges)
    }

    pub fn spatial_nearest_index(
        &self,
        name: &str,
        lon: f64,
        lat: f64,
        limit: usize,
    ) -> Result<Vec<String>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if !lon.is_finite() || !lat.is_finite() {
            return Err(BicDbError::Index(
                "spatial nearest lookup requires finite lon/lat".to_string(),
            ));
        }
        let index = self.spatial_index_state(name)?;
        let non_point = || {
            BicDbError::Index(format!(
                "spatial nearest lookup on `{name}` supports only point geometries"
            ))
        };
        // Bounded k-NN over both halves of the index: the resident tree (the
        // whole index without a packed base, the since-pack delta with one)
        // through rstar's best-first iterator, then the packed base through
        // its own best-first descent. Neither half touches entries beyond
        // the k-th distance plus ties, so cost tracks the result, not the
        // corpus.
        let mut threshold = NearestThreshold::new(limit);
        let mut candidates: Vec<(f64, String)> = Vec::new();
        if let Some(tree) = index.spatial.as_ref() {
            for (entry, planar_d2) in tree.nearest_neighbor_iter_with_distance_2(&[lon, lat]) {
                if let Some(kth) = threshold.kth() {
                    // rstar orders by PLANAR distance while we rank by
                    // haversine. Everything still unseen is planar-farther
                    // than this entry; once that exceeds the far corner of
                    // the envelope covering the k-th haversine distance,
                    // nothing unseen can rank inside the top k.
                    if planar_d2 > haversine_stop_planar_d2(lon, lat, kth) {
                        break;
                    }
                }
                let Some(point) = entry.point else {
                    return Err(non_point());
                };
                let meters = haversine_meters(lon, lat, point[0], point[1]);
                candidates.push((meters, entry.record_id.clone()));
                threshold.offer(meters);
            }
        }
        if let Some(meta) = &index.packed_spatial {
            let Some(paged) = &self.paged_records else {
                return Err(BicDbError::Index(format!(
                    "packed spatial index `{name}` requires paged storage"
                )));
            };
            let snapshot = paged.latest_snapshot();
            let cutoff = std::cell::Cell::new(threshold.kth().unwrap_or(f64::INFINITY));
            spatial_packed::search_nearest(
                meta,
                &mut |node| {
                    paged
                        .spatial_node(&snapshot, name, meta.generation, node)?
                        .ok_or_else(|| {
                            BicDbError::Index(format!(
                                "packed spatial index `{name}` generation {} is missing node {node}",
                                meta.generation
                            ))
                        })
                },
                &mut |min, max| haversine_rect_min_meters(lon, lat, min, max),
                &|| cutoff.get(),
                &mut |meters, entry| {
                    if index.spatial_tombstones.contains(&entry.record_id) {
                        return Ok(());
                    }
                    if entry.point.is_none() {
                        return Err(non_point());
                    }
                    // For a point entry the MBR is degenerate, so the rect
                    // lower bound IS the exact haversine distance.
                    candidates.push((meters, entry.record_id));
                    threshold.offer(meters);
                    cutoff.set(threshold.kth().unwrap_or(f64::INFINITY));
                    Ok(())
                },
            )?;
        }
        candidates.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        candidates.truncate(limit);
        Ok(candidates
            .into_iter()
            .map(|(_, record_id)| record_id)
            .collect())
    }

    pub fn spatial_nearest_envelope_index(
        &self,
        name: &str,
        x: f64,
        y: f64,
        limit: usize,
    ) -> Result<Vec<String>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if !x.is_finite() || !y.is_finite() {
            return Err(BicDbError::Index(
                "spatial nearest lookup requires finite coordinates".to_string(),
            ));
        }
        let index = self.spatial_index_state(name)?;
        // Bounded k-NN by planar envelope distance — the same ordering the
        // old drain-and-sort produced, but rstar's best-first iterator and
        // the packed tree's best-first descent stop past the k-th distance
        // (ties included) instead of touching every entry.
        let mut threshold = NearestThreshold::new(limit);
        let mut candidates: Vec<(f64, String)> = Vec::new();
        if let Some(tree) = index.spatial.as_ref() {
            for (entry, distance_2) in tree.nearest_neighbor_iter_with_distance_2(&[x, y]) {
                if let Some(kth) = threshold.kth() {
                    if distance_2 > kth {
                        break;
                    }
                }
                candidates.push((distance_2, entry.record_id.clone()));
                threshold.offer(distance_2);
            }
        }
        if let Some(meta) = &index.packed_spatial {
            let Some(paged) = &self.paged_records else {
                return Err(BicDbError::Index(format!(
                    "packed spatial index `{name}` requires paged storage"
                )));
            };
            let snapshot = paged.latest_snapshot();
            let cutoff = std::cell::Cell::new(threshold.kth().unwrap_or(f64::INFINITY));
            spatial_packed::search_nearest(
                meta,
                &mut |node| {
                    paged
                        .spatial_node(&snapshot, name, meta.generation, node)?
                        .ok_or_else(|| {
                            BicDbError::Index(format!(
                                "packed spatial index `{name}` generation {} is missing node {node}",
                                meta.generation
                            ))
                        })
                },
                &mut |min, max| AABB::from_corners(min, max).distance_2(&[x, y]),
                &|| cutoff.get(),
                &mut |distance_2, entry| {
                    if index.spatial_tombstones.contains(&entry.record_id) {
                        return Ok(());
                    }
                    candidates.push((distance_2, entry.record_id));
                    threshold.offer(distance_2);
                    cutoff.set(threshold.kth().unwrap_or(f64::INFINITY));
                    Ok(())
                },
            )?;
        }
        candidates.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        candidates.truncate(limit);
        let mut ids = candidates
            .into_iter()
            .map(|(_, record_id)| record_id)
            .collect::<Vec<_>>();
        ids.dedup();
        Ok(ids)
    }

    pub fn spatial_intersects_index(
        &self,
        name: &str,
        min_lon: f64,
        min_lat: f64,
        max_lon: f64,
        max_lat: f64,
    ) -> Result<Vec<String>> {
        if [min_lon, min_lat, max_lon, max_lat]
            .iter()
            .any(|value| !value.is_finite())
            || min_lon > max_lon
            || min_lat > max_lat
        {
            return Err(BicDbError::Index(
                "spatial envelope lookup requires a valid finite envelope".to_string(),
            ));
        }
        let index = self.spatial_index_state(name)?;
        let envelope = AABB::from_corners([min_lon, min_lat], [max_lon, max_lat]);
        let mut ids = self
            .spatial_candidates_in_envelope(&index, &envelope)?
            .into_iter()
            .map(|entry| entry.record_id)
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    /// Envelope hits from a packed spatial base, tombstone-filtered: a
    /// recursive page-pruning descent over the immutable durable nodes.
    pub(crate) fn packed_spatial_envelope_hits(
        &self,
        state: &IndexState,
        envelope: &AABB<[f64; 2]>,
    ) -> Result<Vec<SpatialIndexEntry>> {
        let Some(meta) = &state.packed_spatial else {
            return Ok(Vec::new());
        };
        let name = &state.definition.name;
        let Some(paged) = &self.paged_records else {
            return Err(BicDbError::Index(format!(
                "packed spatial index `{name}` requires paged storage"
            )));
        };
        let snapshot = paged.latest_snapshot();
        let mut hits = Vec::new();
        spatial_packed::search_envelope(
            meta,
            envelope.lower(),
            envelope.upper(),
            &mut |node| {
                paged
                    .spatial_node(&snapshot, name, meta.generation, node)?
                    .ok_or_else(|| {
                        BicDbError::Index(format!(
                            "packed spatial index `{name}` generation {} is missing node {node}",
                            meta.generation
                        ))
                    })
            },
            &mut |entry| {
                if !state.spatial_tombstones.contains(&entry.record_id) {
                    hits.push(index_entry_from_packed(entry));
                }
                Ok(())
            },
        )?;
        Ok(hits)
    }

    /// Whether the (non-tombstoned) packed base holds exactly `entry`.
    pub(crate) fn packed_spatial_contains(
        &self,
        state: &IndexState,
        entry: &SpatialIndexEntry,
    ) -> Result<bool> {
        if state.packed_spatial.is_none() || state.spatial_tombstones.contains(&entry.record_id) {
            return Ok(false);
        }
        Ok(self
            .packed_spatial_envelope_hits(state, &entry.envelope)?
            .iter()
            .any(|existing| existing == entry))
    }

    /// Live (non-tombstoned) entries in the packed base — a full node sweep,
    /// the honest cost of verify's ACTUAL-side count.
    pub(crate) fn packed_spatial_live_count(&self, state: &IndexState) -> Result<usize> {
        let Some(meta) = &state.packed_spatial else {
            return Ok(0);
        };
        let name = &state.definition.name;
        let Some(paged) = &self.paged_records else {
            return Err(BicDbError::Index(format!(
                "packed spatial index `{name}` requires paged storage"
            )));
        };
        let snapshot = paged.latest_snapshot();
        let mut live = 0usize;
        spatial_packed::for_each_entry(
            meta,
            &mut |node| {
                paged
                    .spatial_node(&snapshot, name, meta.generation, node)?
                    .ok_or_else(|| {
                        BicDbError::Index(format!(
                            "packed spatial index `{name}` generation {} is missing node {node}",
                            meta.generation
                        ))
                    })
            },
            &mut |entry| {
                if !state.spatial_tombstones.contains(&entry.record_id) {
                    live += 1;
                }
                Ok(())
            },
        )?;
        Ok(live)
    }

    /// Every candidate intersecting `envelope`: resident tree (the full index
    /// without a packed base, the delta with one) plus packed base hits.
    pub(crate) fn spatial_candidates_in_envelope(
        &self,
        state: &IndexState,
        envelope: &AABB<[f64; 2]>,
    ) -> Result<Vec<SpatialIndexEntry>> {
        let mut hits: Vec<SpatialIndexEntry> = state
            .spatial
            .as_ref()
            .map(|tree| {
                tree.locate_in_envelope_intersecting(envelope)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        if state.packed_spatial.is_some() {
            hits.extend(self.packed_spatial_envelope_hits(state, envelope)?);
        }
        Ok(hits)
    }

    pub(crate) fn spatial_index_state(
        &self,
        name: &str,
    ) -> Result<RwLockReadGuard<'_, IndexState>> {
        let state = self
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
            .read();
        if state.definition.kind != IndexKind::Spatial {
            return Err(BicDbError::Index(format!(
                "index `{name}` is not a spatial index"
            )));
        }
        Ok(state)
    }

    pub(crate) fn spatial_index_for(&self, collection: &str, field: &IndexField) -> Option<String> {
        self.indexes
            .values()
            .filter_map(|index| {
                let index = index.read();
                if index.definition.kind == IndexKind::Spatial
                    && index.definition.collection == collection
                    && index.definition.fields.as_slice() == [field.clone()]
                {
                    Some(index.definition.name.clone())
                } else {
                    None
                }
            })
            .min()
    }

    pub(crate) fn spatial_results_from_ids(
        &self,
        collection: &str,
        field: &IndexField,
        lon: f64,
        lat: f64,
        ids: Vec<String>,
    ) -> Result<Vec<SpatialQueryResult>> {
        let state = self.collection_state(collection)?;
        let mut results = Vec::new();
        for id in ids {
            // Lazy collections keep no resident rows; a miss falls back to
            // the page store, the same read path every other lazy access
            // uses. Dropping the row instead would make every hit from a
            // streamed spatial index silently vanish.
            let resident = state
                .shard(&id)
                .read()
                .get_record(&id)
                .map(|entry| entry.record.to_record())
                .transpose()?;
            let record = match resident {
                Some(record) => record,
                None if state.paged_lazy => {
                    let Some(paged) = &self.paged_records else {
                        continue;
                    };
                    let snapshot = paged.latest_snapshot();
                    match paged.get(&snapshot, collection, &id)? {
                        Some(record) => record,
                        None => continue,
                    }
                }
                None => continue,
            };
            let Some(point) = spatial_record_point(&record, field)? else {
                continue;
            };
            results.push(SpatialQueryResult {
                distance_meters: haversine_meters(lon, lat, point[0], point[1]),
                record,
            });
        }
        Ok(results)
    }
}
