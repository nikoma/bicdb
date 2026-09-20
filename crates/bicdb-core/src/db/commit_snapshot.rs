//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;

impl BicDb {
    pub(crate) fn persist_record_audit_publication(&self, tx_id: TransactionId) -> Result<()> {
        let payload = serde_json::to_vec(&TxFrame::RecordAuditPublished { tx_id })?;
        let framed = storage::encode_frames_to_bytes(
            &self.tx_log_path(),
            FrameKind::Transaction,
            &[payload],
            &CompressionConfig::disabled(),
            &self.encryption,
        )?;
        self.tx_log.append_auxiliary(&framed)
    }

    /// Undo an enqueued commit whose apply failed, so the failure neither
    /// wedges the database nor comes back from the dead.
    ///
    /// By the time the apply runs, this transaction's Write+Commit frames are
    /// already in the WAL queue — and possibly already durable, because any
    /// CONCURRENT committer's `write_durable` drains the contiguous queue,
    /// including our frames. So the frames cannot simply be dequeued. Two
    /// consequences had to be prevented:
    ///
    /// - **Resurrection.** Recovery would replay the Commit frames and apply a
    ///   transaction whose client was told it failed. The tx-log format
    ///   already has the answer: a later `TxFrame::Abort` overrides an earlier
    ///   `Commit` in recovery's state fold (`commit_seq_by_tx.remove`). One
    ///   Abort frame, enqueued at a fresh commit_seq and written durably
    ///   before the error returns, revokes the commit in every replay.
    ///
    /// - **The watermark wedge.** `applied_watermark` advances only across the
    ///   contiguous prefix of applied seqs; a seq that never marks blocks it
    ///   forever, leaving every LATER commit permanently invisible to new
    ///   snapshots. Both the failed seq and the revoke seq are marked here.
    ///
    /// Marking the failed seq makes any PARTIALLY applied collections visible
    /// (an apply that failed on its second collection leaves its first
    /// applied; nothing can un-apply resident state). That is the accepted
    /// cost: the realistic mid-batch failure is storage-level, where a loudly
    /// reported partial beats a silently wedged database — and the revoke
    /// frame guarantees a restart converges to "transaction absent".
    pub(crate) fn revoke_enqueued_commit(
        &self,
        tx_id: TransactionId,
        commit_seq: u64,
        error: BicDbError,
    ) -> BicDbError {
        let revoke_seq = self.commit_seq.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        let framed = serde_json::to_vec(&TxFrame::Abort {
            tx_id,
            timestamp: unix_timestamp(),
        })
        .map_err(BicDbError::from)
        .and_then(|abort_bytes| {
            storage::encode_frames_to_bytes(
                &self.tx_log_path(),
                FrameKind::Transaction,
                &[abort_bytes],
                &CompressionConfig::disabled(),
                &self.encryption,
            )
        });
        match framed {
            Ok(framed) => self.tx_log.enqueue(revoke_seq, framed),
            Err(encode_error) => eprintln!(
                "bicdb: FAILED to encode revoke frame for commit_seq {commit_seq}: \
                 {encode_error}; a crash before compaction may replay the failed commit"
            ),
        }
        self.mark_committed_seq(commit_seq);
        self.mark_committed_seq(revoke_seq);
        // The revoke must be durable BEFORE the caller sees the error: a crash
        // in between would leave durable Commit frames with no Abort.
        if let Err(flush_error) = self.tx_log_handle().write_durable(revoke_seq) {
            eprintln!(
                "bicdb: FAILED to flush revoke frame for commit_seq {commit_seq}: {flush_error};                  a crash before the next durable write may replay the failed commit"
            );
        }
        eprintln!(
            "bicdb: commit_seq {commit_seq} failed during apply and was revoked              (transaction {}): {error}",
            tx_id.0
        );
        error
    }

    /// Mark `commit_seq` as fully applied and publish the resulting contiguous
    /// visibility watermark to `last_committed_tx`. The watermark only advances
    /// across the unbroken applied prefix, so readers never observe a snapshot whose
    /// lower-seq commits are not yet applied.
    pub(crate) fn mark_committed_seq(&self, commit_seq: u64) {
        let contiguous = self.applied_watermark.lock().mark(commit_seq);
        // `contiguous` is monotonic non-decreasing across callers; a plain store is
        // sufficient and stays >= any value a concurrent committer could publish.
        self.last_committed_tx
            .fetch_max(contiguous, AtomicOrdering::SeqCst);
    }

    /// Encodes a committed transaction's WAL frames into a single byte buffer
    /// (no write). An empty buffer is returned for a no-write commit (which still
    /// occupies a sequence and is enqueued so the writer queue stays gap-free).
    pub(crate) fn encode_tx_commit_frames(
        &self,
        tx_id: TransactionId,
        commit_seq: u64,
        writes: &[TxWrite],
        broker_publishes: &[crate::broker::PendingBrokerPublish],
        record_audit_events: &[Event],
        prepared: PreparedWal,
    ) -> Result<Vec<u8>> {
        if writes.is_empty() && broker_publishes.is_empty() && record_audit_events.is_empty() {
            return Ok(Vec::new());
        }
        let prepared = match prepared {
            PreparedWal::PlainFrames { count, mut bytes }
                if count == writes.len() && !self.encryption.is_enabled() =>
            {
                for publish in broker_publishes {
                    storage::append_plain_json_frame(
                        &mut bytes,
                        FrameKind::Transaction,
                        &TxFrameRef::BrokerPublish { tx_id, publish },
                    )?;
                }
                for event in record_audit_events {
                    storage::append_plain_json_frame(
                        &mut bytes,
                        FrameKind::Transaction,
                        &TxFrameRef::RecordAudit { tx_id, event },
                    )?;
                }
                storage::append_plain_json_frame(
                    &mut bytes,
                    FrameKind::Transaction,
                    &TxFrameRef::Commit {
                        tx_id,
                        timestamp: unix_timestamp(),
                        commit_seq,
                    },
                )?;
                return Ok(bytes);
            }
            PreparedWal::Payloads(payloads) => payloads,
            _ => Vec::new(), // A stale/incompatible preparation uses the normal fallback.
        };
        // Reuse the Write-frame payloads serialized off the critical section by
        // `prepare_wal_payloads`; otherwise serialize them inline. TxFrameRef
        // serializes identically to TxFrame (see
        // tx_frame_ref_matches_owned_serialization).
        let mut payloads = if prepared.len() == writes.len() {
            prepared
        } else {
            let mut payloads = Vec::with_capacity(writes.len() + broker_publishes.len() + 1);
            for write in writes {
                payloads.push(serde_json::to_vec(&TxFrameRef::Write { tx_id, write })?);
            }
            payloads
        };
        for publish in broker_publishes {
            payloads.push(serde_json::to_vec(&TxFrameRef::BrokerPublish {
                tx_id,
                publish,
            })?);
        }
        for event in record_audit_events {
            payloads.push(serde_json::to_vec(&TxFrameRef::RecordAudit {
                tx_id,
                event,
            })?);
        }
        payloads.push(serde_json::to_vec(&TxFrameRef::Commit {
            tx_id,
            timestamp: unix_timestamp(),
            commit_seq,
        })?);
        storage::encode_frames_to_bytes(
            &self.tx_log_path(),
            FrameKind::Transaction,
            &payloads,
            &CompressionConfig::disabled(),
            &self.encryption,
        )
    }

    pub(crate) fn apply_imported_replication_commit(
        &mut self,
        tx_id: u64,
        commit_seq: u64,
        writes: &[TxWrite],
    ) -> Result<()> {
        let _commit_guard = self.commit_lock.lock();
        let current = self.commit_seq.load(AtomicOrdering::SeqCst);
        if commit_seq <= current {
            return Ok(());
        }
        if commit_seq != current.saturating_add(1) {
            return Err(replication_error(format!(
                "replication importer expected commit_seq {}, got {commit_seq}",
                current.saturating_add(1)
            )));
        }

        for write in writes {
            if !self.collections.contains_key(&write.collection) {
                validate_collection_name(&write.collection)?;
                let meta = CollectionMeta {
                    name: write.collection.clone(),
                    vector_dim: None,
                    mode: CollectionMode::Standard,
                    policy: None,
                    mutation_policy: None,
                    mesh_sync_enabled: false,
                };
                self.collections.insert(
                    write.collection.clone(),
                    RwLock::new(CollectionState::new(meta)),
                );
                self.persist_catalog()?;
            }
        }

        let writes_by_collection = tx_writes_by_collection(writes);
        let mut index_mutations_by_collection = self.tx_index_mutations_by_collection(
            &writes_by_collection,
            tx_writes_are_unique(writes),
        )?;
        let dim_plan = self.plan_vector_dims(&writes_by_collection)?;
        self.fill_all_index_mutation_rowids(&mut index_mutations_by_collection)?;
        self.validate_index_mutations(&index_mutations_by_collection)?;

        let wal_bytes = self.encode_tx_commit_frames(
            TransactionId(tx_id),
            commit_seq,
            writes,
            &[],
            &[],
            PreparedWal::default(),
        )?;
        self.tx_log.enqueue(commit_seq, wal_bytes);

        let mut timer = commit_trace::Timer::start();
        self.apply_committed_record_writes(
            commit_seq,
            &writes_by_collection,
            &index_mutations_by_collection,
            &dim_plan,
            &mut timer,
        )?;
        self.fill_all_index_mutation_rowids(&mut index_mutations_by_collection)?;
        let spatial_events = self.apply_index_mutations(&index_mutations_by_collection)?;
        if !spatial_events.is_empty() {
            let mut events = self.events.lock();
            for event in spatial_events {
                events.append(event)?;
            }
        }
        self.refresh_graph_projections()?;
        self.tx_log.write_durable(commit_seq)?;
        self.commit_seq.store(commit_seq, AtomicOrdering::SeqCst);
        self.mark_committed_seq(commit_seq);
        Ok(())
    }

    pub(crate) fn rollback_transaction(&mut self, tx: &mut Transaction) -> Result<()> {
        if tx.state != TxState::Pending {
            return Err(BicDbError::TransactionNotPending);
        }
        // Done: drop the tx_states entry rather than leaving an `Aborted` tombstone
        // (see the commit path). An absent entry is correctly "not pending".
        self.tx_states.remove(&tx.id.0);
        let locked_keys = tx.locked_keys.get_mut().take_all();
        release_owned_write_locks(&self.write_locks, tx.id, &locked_keys);
        tx.state = TxState::Aborted;
        Ok(())
    }

    /// Atomically claim every unique-index key this commit introduces, at the
    /// PRE-WAL abort point. A key already claimed by another in-flight
    /// committer (or resident under another record — the subsequent
    /// validation checks that) aborts cleanly here, so the post-validation
    /// commit section can never fail on uniqueness: the old failure mode
    /// half-applied the losing commit after its WAL frames were enqueued and
    /// froze the applied watermark permanently. Claims release when the guard
    /// drops at the end of the commit (success or error alike).
    pub(crate) fn claim_unique_keys<'db>(
        &'db self,
        tx: &Transaction,
        index_mutations_by_collection: &BTreeMap<String, Vec<IndexRecordMutation>>,
    ) -> Result<UniqueClaimsGuard<'db>> {
        let mut guard = UniqueClaimsGuard {
            db: self,
            claims: Vec::new(),
        };
        for index in self.indexes.values() {
            let state = index.read();
            if !state.definition.unique {
                continue;
            }
            let Some(mutations) = index_mutations_by_collection.get(&state.definition.collection)
            else {
                continue;
            };
            for mutation in mutations {
                // Same record, changed keys known and none of them indexed
                // here: the same key stays claimed by the same row.
                if mutation.leaves_index_untouched(&state.definition) {
                    continue;
                }
                if mutation.new_record.matches_predicate(&state.definition)? != Some(true) {
                    continue;
                }
                let key = mutation
                    .new_record
                    .index_key(&state.definition.fields)?
                    .expect("post-image present");
                if index_key_contains_null(&key) {
                    continue;
                }
                // Same record keeping the same key introduces nothing new.
                if mutation.old_record.id() == mutation.new_record.id()
                    && mutation.old_record.matches_predicate(&state.definition)? == Some(true)
                    && mutation
                        .old_record
                        .index_key(&state.definition.fields)?
                        .as_ref()
                        == Some(&key)
                {
                    continue;
                }
                let encoded = mutation
                    .new_record
                    .encoded_index_key(&state.definition.fields)?
                    .expect("post-image key encodes");
                let claim_key = (state.definition.name.clone(), encoded.to_vec());
                let mut shard = self.unique_claims.shard_for(&claim_key);
                match shard.get(&claim_key) {
                    Some(owner) if *owner != tx.id.0 => {
                        drop(shard);
                        return Err(BicDbError::Index(format!(
                            "unique index `{}` has duplicate keys",
                            state.definition.name
                        )));
                    }
                    Some(_) => {}
                    None => {
                        shard.insert(claim_key.clone(), tx.id.0);
                        drop(shard);
                        guard.claims.push(claim_key);
                    }
                }
            }
        }
        Ok(guard)
    }

    pub(crate) fn claim_exclusion_ranges<'db>(
        &'db self,
        tx: &Transaction,
        index_mutations_by_collection: &BTreeMap<String, Vec<IndexRecordMutation>>,
    ) -> Result<ExclusionClaimsGuard<'db>> {
        let mut pending = Vec::new();
        for index in self.indexes.values() {
            let state = index.read();
            if state.definition.exclusion.is_none() {
                continue;
            }
            let Some(mutations) = index_mutations_by_collection.get(&state.definition.collection)
            else {
                continue;
            };
            for mutation in mutations {
                let Some(new_record) = mutation.new_record.record()? else {
                    continue;
                };
                let Some(tuple) = record_exclusion_tuple(new_record, &state.definition)? else {
                    continue;
                };
                if let Some(old_record) = mutation.old_record.record()? {
                    if record_exclusion_tuple(old_record, &state.definition)?.as_ref()
                        == Some(&tuple)
                    {
                        continue;
                    }
                }
                pending.push(ActiveExclusionClaim {
                    transaction_id: tx.id.0,
                    index_name: state.definition.name.clone(),
                    tuple,
                });
            }
        }
        let mut active = self.exclusion_claims.lock();
        for claim in &pending {
            if active.iter().any(|existing| {
                existing.transaction_id != claim.transaction_id
                    && existing.index_name == claim.index_name
                    && exclusion_tuples_conflict(&existing.tuple, &claim.tuple)
            }) {
                return Err(BicDbError::Index(format!(
                    "exclusion constraint `{}` conflicts with an in-flight write",
                    claim.index_name
                )));
            }
        }
        active.extend(pending);
        drop(active);
        Ok(ExclusionClaimsGuard {
            db: self,
            transaction_id: tx.id.0,
        })
    }

    pub(crate) fn validate_exclusion_record_mutations(
        &self,
        definition: &IndexDefinition,
        mutations: &[IndexRecordMutation],
    ) -> Result<()> {
        let changed_ids = mutations
            .iter()
            .flat_map(|mutation| {
                mutation
                    .old_record
                    .id()
                    .into_iter()
                    .chain(mutation.new_record.id())
            })
            .collect::<BTreeSet<_>>();
        let candidates = mutations
            .iter()
            .map(|mutation| mutation.new_record.record())
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .map(|record| {
                Ok(record_exclusion_tuple(record, definition)?
                    .map(|tuple| (record.id.as_str(), tuple)))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        for (index, (left_id, left)) in candidates.iter().enumerate() {
            if candidates[index + 1..].iter().any(|(right_id, right)| {
                left_id != right_id && exclusion_tuples_conflict(left, right)
            }) {
                return Err(BicDbError::Index(format!(
                    "exclusion constraint `{}` conflicts within the write batch",
                    definition.name
                )));
            }
        }
        for existing in self.scan_collection_unchecked(&definition.collection)? {
            if changed_ids.contains(existing.id.as_str()) {
                continue;
            }
            let Some(existing_tuple) = record_exclusion_tuple(&existing, definition)? else {
                continue;
            };
            if candidates
                .iter()
                .any(|(_, candidate)| exclusion_tuples_conflict(candidate, &existing_tuple))
            {
                return Err(BicDbError::Index(format!(
                    "exclusion constraint `{}` conflicts with record `{}`",
                    definition.name, existing.id
                )));
            }
        }
        Ok(())
    }

    /// Commit-time half of the delta-repair protocol (see [`RepairPlan`]).
    /// For every repair-carrying write: take its row lock (repair writers skip
    /// the statement-time lock, so contention here is commit-vs-commit and the
    /// hold is microseconds — except when a statement-time locker such as
    /// NewOrder's district update holds the row, where we wait it out), then if
    /// the row changed past the write's snapshot rebuild the record as
    /// `latest + deltas` and stamp the write with the observed snapshot so
    /// `detect_commit_conflicts` accepts it. Locks are acquired in
    /// (collection, record_id) order so concurrent repairers cannot deadlock;
    /// they release with the transaction's other locks after apply.
    pub(crate) fn repair_conflicting_delta_writes(&self, tx: &mut Transaction) -> Result<()> {
        if tx.writes.iter().all(|write| write.repair.is_none()) {
            return Ok(());
        }
        let mut order: Vec<usize> = (0..tx.writes.len())
            .filter(|&idx| tx.writes[idx].repair.is_some())
            .collect();
        order.sort_by(|&a, &b| {
            (
                tx.writes[a].collection.as_str(),
                tx.writes[a].record_id.as_str(),
            )
                .cmp(&(
                    tx.writes[b].collection.as_str(),
                    tx.writes[b].record_id.as_str(),
                ))
        });
        let mut repaired_any = false;
        for idx in order {
            let collection = tx.writes[idx].collection.clone();
            let record_id = tx.writes[idx].record_id.clone();
            // Generous budget: a statement-time locker holds for the rest of
            // its procedure (~ms); event-driven waits keep the cost bounded.
            self.lock_tx_record_with_attempts(
                tx.id,
                &collection,
                &record_id,
                4096,
                true,
                false,
                &tx.locked_keys,
            )?;
            let latest = {
                let state = self.collection_state(&collection)?;
                let shard = state.shard(&record_id).read();
                let Some(rowid) = shard.rowid_of(&record_id) else {
                    continue;
                };
                let max_tx = version_chain_max_tx(&shard.versions, rowid);
                // Freshness must be judged against the un-floored APPLIED
                // watermark, not the (possibly floored) snapshot: a commit seq
                // in the (applied, snapshot] gap may not have been applied yet
                // when this transaction's statement read the row, so the
                // buffered absolute record can predate it even though the seq
                // is "within snapshot". (Observed in the wild as a payment
                // regressing d_next_o_id and minting duplicate order ids.)
                if max_tx <= tx.applied_snapshot {
                    // Genuinely fresh — and the lock we now hold keeps it
                    // fresh through apply.
                    repair_trace(
                        &collection,
                        &record_id,
                        tx.id.0,
                        tx.applied_snapshot,
                        max_tx,
                        || "fresh".to_owned(),
                    );
                    if let Some(entry) = shard.records.get(&rowid) {
                        let write = &mut tx.writes[idx];
                        if write.stored.is_some() {
                            // No delta rebase was needed, but the row is now
                            // locked and final just like a rebuilt repair.
                            // Retain its truthful pre-image and allow narrow
                            // compact WAL instead of carrying an unresolved
                            // plan into the full-frame-only path.
                            write.previous_stored = Some(Arc::clone(&entry.record));
                            write.repair = None;
                            repaired_any = true;
                        }
                    }
                    continue;
                }
                let Some(entry) = shard.records.get(&rowid) else {
                    continue;
                };
                repair_trace(
                    &collection,
                    &record_id,
                    tx.id.0,
                    tx.applied_snapshot,
                    max_tx,
                    || format!("rebuild latest_meta={}", entry.record.metadata.get()),
                );
                (Arc::clone(&entry.record), max_tx)
            };
            let (latest, max_tx) = latest;
            let write = &tx.writes[idx];
            let Some(plan) = write.repair.clone() else {
                continue;
            };
            if let Some(old) = write.stored.as_deref() {
                if let Some(mut repaired) = apply_stored_repair_deltas(&latest, &plan) {
                    repaired.timestamp = old.timestamp;
                    // Retain the same conservative index-key validation as
                    // the parsed path. Unsupported key shapes fall back below.
                    let mut supported = true;
                    for index in self.indexes.values() {
                        let definition = &index.read().definition;
                        if definition.collection != collection {
                            continue;
                        }
                        match (stored_index_key(old, &definition.fields),
                               stored_index_key(&repaired, &definition.fields)) {
                            (Some(before), Some(after)) if before == after => {}
                            (Some(_), Some(_)) => return Err(BicDbError::TransactionConflict(format!(
                                "{collection}:{record_id} changed after transaction {} snapshot (index key moved)",
                                tx.id.0
                            ))),
                            _ => { supported = false; break; }
                        }
                    }
                    if supported {
                        let write = &mut tx.writes[idx];
                        write.record = OnceLock::new();
                        write.stored = Some(Arc::new(repaired));
                        write.statement_snapshot = max_tx;
                        // The delta is now consumed into an absolute row and
                        // its lock remains held through apply. Only finalized
                        // repairs may become compact WAL patches; unresolved
                        // repair plans still force full frames.
                        write.repair = None;
                        write.previous = None;
                        // This is the actual pre-image under our row lock,
                        // allowing compact WAL to retain its idempotent
                        // before/after validation even for repaired writes.
                        write.previous_stored = Some(latest);
                        repaired_any = true;
                        continue;
                    }
                }
            }
            let Some(old) = write.record()?.map(Arc::as_ref) else {
                continue;
            };
            let mut repaired = apply_repair_deltas(latest.to_record()?, &plan).ok_or_else(|| {
                BicDbError::TransactionConflict(format!(
                    "{collection}:{record_id} changed after transaction {} snapshot (not repairable)",
                    tx.id.0
                ))
            })?;
            repaired.timestamp = old.timestamp;
            // Defense in depth: a repair must not move any index key, or the
            // precomputed index mutations and unique validation would diverge
            // from the applied record. The SQL layer already refuses to build
            // repair plans for indexed columns; verify anyway and fall back to
            // the classic conflict (statement replay) if violated.
            for index in self.indexes.values() {
                let definition = &index.read().definition;
                if definition.collection != collection {
                    continue;
                }
                if record_index_key(old, &definition.fields)
                    != record_index_key(&repaired, &definition.fields)
                {
                    return Err(BicDbError::TransactionConflict(format!(
                        "{collection}:{record_id} changed after transaction {} snapshot (index key moved)",
                        tx.id.0
                    )));
                }
            }
            tx.writes[idx].record = OnceLock::from(Some(Arc::new(repaired)));
            tx.writes[idx].statement_snapshot = max_tx;
            // The compacted form and the remembered pre-image described the
            // pre-repair record; commit rebuilds both from the repaired one.
            tx.writes[idx].stored = None;
            tx.writes[idx].previous = None;
            tx.writes[idx].previous_stored = None;
            repaired_any = true;
        }
        if repaired_any {
            // Any pre-encoded WAL payloads captured the pre-repair records.
            tx.prepared_wal.clear();
        }
        // Every repair row is now locked and finalized. Encode once from the
        // compact rows, including when all plans were fresh (and the eager
        // preparation was intentionally deferred). Do not fall through to the
        // full-record serializer for an entire Payment after one row repair.
        tx.prepare_final_wal_payloads();
        Ok(())
    }

    pub(crate) fn detect_commit_conflicts(&self, tx: &Transaction) -> Result<()> {
        for write in &tx.writes {
            let Some(state) = self
                .collections
                .get(&write.collection)
                .map(|lock| lock.read())
            else {
                return Err(BicDbError::CollectionNotFound(write.collection.clone()));
            };
            // A conflict means some version of this record was created or deleted
            // after the transaction's snapshot. Chains are normally one inline
            // entry, so derive their exact high-water mark without maintaining a
            // redundant per-row hash table.
            let shard = state.shard(&write.record_id).read();
            // A primary key absent from the registry was never mutated post-baseline,
            // so it has no high-water mark and cannot conflict.
            if let Some(rowid) = shard.rowid_of(&write.record_id) {
                let max_tx = version_chain_max_tx(&shard.versions, rowid);
                let mut conflict_snapshot = if write.statement_snapshot == 0 {
                    // `snapshot_tx` may be raised above the contiguous applied
                    // watermark solely for read-your-writes. A lower sequence
                    // in that gap can apply after this transaction read the
                    // row; treating the raised number as observed would mask a
                    // real lost update. Write buffering normally replaces zero
                    // with a point-read or lock-protected row boundary; retain
                    // the truthful applied watermark as a defensive fallback.
                    tx.applied_snapshot
                } else {
                    write.statement_snapshot
                };
                if max_tx > conflict_snapshot {
                    // A sibling write to the same record may carry a newer
                    // READ COMMITTED statement snapshot (the recheck refreshes
                    // only the first write to a record; later writes in the
                    // same transaction skip it and keep snapshot 0). The record
                    // was observed as of the newest such snapshot, so judge the
                    // whole record against it. Scanned only on a would-be
                    // conflict to keep the common no-conflict path linear and
                    // allocation-free.
                    let refreshed = tx
                        .writes
                        .iter()
                        .filter(|other| {
                            other.collection == write.collection
                                && other.record_id == write.record_id
                        })
                        .map(|other| other.statement_snapshot)
                        .max()
                        .unwrap_or(0);
                    conflict_snapshot = conflict_snapshot.max(refreshed);
                }
                if max_tx > conflict_snapshot {
                    return Err(BicDbError::TransactionConflict(format!(
                        "{}:{} changed after transaction {} snapshot",
                        write.collection, write.record_id, tx.id.0
                    )));
                }
            }
        }
        Ok(())
    }

    /// Plan per-collection vector-dimension handling for a commit. Validates each
    /// upsert record against the collection's current dimension (abort point:
    /// `DimensionMismatch`) and returns, per collection, whether the dimension
    /// would change (a `None -> Some` first-vector transition). A changing
    /// dimension forces the rare exclusive apply path; everything else (including
    /// ordinary non-vector workloads) takes the concurrent read-guard path.
    pub(crate) fn plan_vector_dims(
        &self,
        writes_by_collection: &BTreeMap<&str, Vec<&TxWrite>>,
    ) -> Result<BTreeMap<String, bool>> {
        let mut plan = BTreeMap::new();
        for (&collection, writes) in writes_by_collection {
            self.ensure_collection(collection)?;
            let old_dim = self.collection_state(collection)?.meta.vector_dim;
            let mut next_dim = old_dim;
            for write in writes {
                if matches!(write.op, TxWriteOp::Upsert) {
                    validate_write_record(collection, write, &mut next_dim)?;
                }
            }
            plan.insert(collection.to_owned(), next_dim != old_dim);
        }
        Ok(plan)
    }

    pub(crate) fn record_audit_events_for_commit(
        &self,
        index_mutations_by_collection: &BTreeMap<String, Vec<IndexRecordMutation>>,
    ) -> Result<Vec<Event>> {
        if !self.config.audit_events {
            return Ok(Vec::new());
        }

        // Captured once per commit, before these events append: the context
        // deliberately covers everything visible prior to this write.
        let write_context = self.sync_vector()?;
        let write_clock = self.write_clock_value();
        let mut events = Vec::new();
        for (collection, mutations) in index_mutations_by_collection {
            let collection_mode = self.collection_state(collection)?.meta.mode.clone();
            for mutation in mutations {
                match (mutation.old_record.record()?, mutation.new_record.record()?) {
                    (_, Some(record)) => {
                        let op_type = if mutation.old_record.is_some() {
                            OpType::Update
                        } else {
                            OpType::Insert
                        };
                        events.push(record_upsert_audit_event(
                            collection,
                            &collection_mode,
                            record,
                            op_type,
                            Some(&write_context),
                            Some(&write_clock),
                        )?);
                        let prepared = PreparedMutation {
                            record: record.as_ref().clone(),
                            op_type,
                        };
                        if let Some(event) = spatial_record_inserted_event(
                            collection,
                            &collection_mode,
                            &prepared,
                            &self.indexes,
                        )? {
                            events.push(event);
                        }
                    }
                    (Some(record), None) => {
                        events.push(record_deleted_event(
                            collection,
                            &collection_mode,
                            record,
                            Some(&write_context),
                            Some(&write_clock),
                        )?);
                    }
                    (None, None) => {}
                }
            }
        }
        Ok(events)
    }

    pub(crate) fn sync_ops_for_commit(
        &self,
        index_mutations_by_collection: &BTreeMap<String, Vec<IndexRecordMutation>>,
    ) -> Result<Vec<SyncOp>> {
        if !self.config.sync_outbox {
            return Ok(Vec::new());
        }

        let timestamp = unix_timestamp();
        let mut ops = Vec::new();
        for (collection, mutations) in index_mutations_by_collection {
            for mutation in mutations {
                match (mutation.old_record.record()?, mutation.new_record.record()?) {
                    (_, Some(record)) => {
                        let op_type = if mutation.old_record.is_some() {
                            OpType::Update
                        } else {
                            OpType::Insert
                        };
                        ops.push(SyncOp {
                            op_id: Uuid::new_v4(),
                            collection: collection.clone(),
                            record_id: record.id.clone(),
                            op_type,
                            timestamp,
                            hash: record.content_hash()?,
                        });
                    }
                    (Some(record), None) => {
                        ops.push(SyncOp {
                            op_id: Uuid::new_v4(),
                            collection: collection.clone(),
                            record_id: record.id.clone(),
                            op_type: OpType::Delete,
                            timestamp,
                            hash: record.content_hash()?,
                        });
                    }
                    (None, None) => {}
                }
            }
        }
        Ok(ops)
    }

    /// The atomic index section of a commit. Acquires the write lock of every
    /// index belonging to a written collection — in canonical (name-sorted) order,
    /// so concurrent committers acquire any shared subset in the same order and
    /// cannot deadlock — then validates uniqueness across ALL of them before
    /// applying ANY mutation. Holding the locks across validate-then-apply is what
    /// makes "no duplicate unique key" hold without a global commit lock: two
    /// committers inserting the same unique key into different records cannot both
    /// pass validation and then both insert. Returns spatial-index audit events to
    /// append by the caller AFTER the locks are dropped (so event I/O is off the
    /// critical section). Record/version apply happens later, outside these locks.
    /// Pass 1 (abort point): validate uniqueness for every touched index under its
    /// READ lock — concurrent across committers, instead of the old
    /// hold-every-index-write-lock convoy. This runs BEFORE record apply, so a
    /// brand-new record's rowid is not yet allocated; `validate_btree_record_mutations`
    /// handles that in pk-space. The mutating Pass 2 ([`Self::apply_index_mutations`])
    /// runs AFTER record apply, when every record's rowid (the index payload) is
    /// resolved.
    ///
    /// Correctness of the split validate->apply window:
    ///  * Uniqueness: `apply_btree_record_mutation` re-checks via `insert_if_unique`
    ///    under the write lock, so a key another committer inserts between this
    ///    commit's validate and apply is still rejected (the read-lock validate is
    ///    an early-out, not the authority).
    ///  * Atomicity: a half-applied index is harmless — index entries point at
    ///    record locators, and MVCC visibility (the contiguous watermark, published
    ///    after both passes) filters any entry whose record version is not yet
    ///    committed, so an orphan entry is never observable.
    pub(crate) fn validate_index_mutations(
        &self,
        index_mutations_by_collection: &BTreeMap<String, Vec<IndexRecordMutation>>,
    ) -> Result<()> {
        for index in self.indexes.values() {
            let guard = index.read();
            let Some(mutations) = index_mutations_by_collection.get(&guard.definition.collection)
            else {
                continue;
            };
            if guard.definition.exclusion.is_some() {
                self.validate_exclusion_record_mutations(&guard.definition, mutations)?;
            }
            match guard.definition.kind {
                IndexKind::BTree if guard.paged_read_through => {
                    self.validate_paged_btree_record_mutations(&guard.definition, mutations)?
                }
                IndexKind::BTree => guard.validate_btree_record_mutations(mutations)?,
                IndexKind::FullText | IndexKind::Jsonb | IndexKind::Array => {}
                IndexKind::Spatial => {
                    let mut candidate = (*guard).clone();
                    for mutation in mutations {
                        candidate.apply_record_mutation(mutation)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Validate a read-through unique index against its durable PK postings.
    /// The caller already holds this commit's unforgeable unique-key claims,
    /// so another committer cannot pass the same key between this check and
    /// paged apply. Batch mutations are replayed in statement order in PK
    /// space, exactly like the resident validator does in RowId space.
    pub(crate) fn validate_paged_btree_record_mutations(
        &self,
        definition: &IndexDefinition,
        mutations: &[IndexRecordMutation],
    ) -> Result<()> {
        if !definition.unique {
            return Ok(());
        }
        let paged = self.paged_records.as_ref().ok_or_else(|| {
            BicDbError::Index(format!(
                "read-through unique index `{}` lost its page store",
                definition.name
            ))
        })?;
        let snapshot = paged.latest_snapshot();
        let mut touched = BTreeMap::<Vec<u8>, BTreeSet<String>>::new();
        for mutation in mutations {
            if let Some(true) = mutation.old_record.matches_predicate(definition)? {
                {
                    let values = mutation
                        .old_record
                        .index_key(&definition.fields)?
                        .expect("pre-image present");
                    if !index_key_contains_null(&values) {
                        let encoded = encode_index_key(&values);
                        if !touched.contains_key(&encoded) {
                            let pks = paged
                                .scan_index_exact_refs(&snapshot, &definition.name, &encoded)?
                                .map(|entry| {
                                    let (entry_ref, value) = entry?;
                                    resolve_paged_entry_pk(
                                        paged,
                                        &snapshot,
                                        &definition.collection,
                                        &definition.name,
                                        entry_ref,
                                        &value,
                                        false,
                                    )
                                })
                                .collect::<Result<BTreeSet<_>>>()?;
                            touched.insert(encoded.clone(), pks);
                        }
                        touched
                            .get_mut(&encoded)
                            .expect("inserted durable unique key")
                            .remove(mutation.old_record.id().expect("pre-image present"));
                    }
                }
            }
            if mutation.new_record.matches_predicate(definition)? == Some(true) {
                let values = mutation
                    .new_record
                    .index_key(&definition.fields)?
                    .expect("post-image present");
                let new_id = mutation.new_record.id().expect("post-image present");
                if index_key_contains_null(&values) {
                    continue;
                }
                let encoded = encode_index_key(&values);
                if !touched.contains_key(&encoded) {
                    let pks = paged
                        .scan_index_exact_refs(&snapshot, &definition.name, &encoded)?
                        .map(|entry| {
                            let (entry_ref, value) = entry?;
                            resolve_paged_entry_pk(
                                paged,
                                &snapshot,
                                &definition.collection,
                                &definition.name,
                                entry_ref,
                                &value,
                                false,
                            )
                        })
                        .collect::<Result<BTreeSet<_>>>()?;
                    touched.insert(encoded.clone(), pks);
                }
                let pks = touched
                    .get_mut(&encoded)
                    .expect("inserted durable unique key");
                if pks.iter().any(|pk| pk != new_id) {
                    return Err(BicDbError::Index(format!(
                        "unique index `{}` has duplicate keys",
                        definition.name
                    )));
                }
                pks.insert(new_id.to_string());
            }
        }
        Ok(())
    }

    /// Pass 2: apply index entries, AFTER record apply (so `mutation.rowid` is
    /// resolved). A BTree index applies through the store's interior (per-shard)
    /// locks under the index's shared READ guard, so committers touching different
    /// shards of the same index — different leading-field values, e.g. different
    /// logical partitions — apply CONCURRENTLY instead of convoying on one per-index
    /// write lock (3b's collapse). Only the rare Spatial index, whose R-tree lives
    /// in the IndexState itself, needs the exclusive WRITE guard. Returns
    /// spatial-index audit events to append after the locks are dropped.
    pub(crate) fn apply_index_mutations(
        &self,
        index_mutations_by_collection: &BTreeMap<String, Vec<IndexRecordMutation>>,
    ) -> Result<Vec<Event>> {
        let mut events = Vec::new();
        for index in self.indexes.values() {
            let kind = index.read().definition.kind.clone();
            match kind {
                IndexKind::BTree | IndexKind::FullText | IndexKind::Jsonb | IndexKind::Array => {
                    let guard = index.read();
                    let Some(mutations) =
                        index_mutations_by_collection.get(&guard.definition.collection)
                    else {
                        continue;
                    };
                    // Read-through FTS keeps no resident postings to
                    // maintain — the durable entries were written in this
                    // commit's paged apply, and lookups read them directly.
                    if guard.paged_read_through {
                        continue;
                    }
                    for mutation in mutations {
                        let result = match kind {
                            IndexKind::BTree => guard.apply_btree_record_mutation(mutation),
                            IndexKind::FullText => guard.apply_full_text_record_mutation(mutation),
                            IndexKind::Jsonb | IndexKind::Array => {
                                guard.apply_full_text_record_mutation(mutation)
                            }
                            IndexKind::Spatial => unreachable!(),
                        };
                        if let Err(error) = result {
                            let record_id = mutation
                                .new_record
                                .id()
                                .or(mutation.old_record.id())
                                .unwrap_or("<unknown>");
                            return Err(match error {
                                BicDbError::Index(message) => BicDbError::Index(format!(
                                    "index `{}` on collection `{}` failed for record `{record_id}`: {message}",
                                    guard.definition.name, guard.definition.collection,
                                )),
                                error => error,
                            });
                        }
                    }
                }
                IndexKind::Spatial => {
                    let mut guard = index.write();
                    let Some(mutations) =
                        index_mutations_by_collection.get(&guard.definition.collection)
                    else {
                        continue;
                    };
                    let mut changed = false;
                    for mutation in mutations {
                        changed |= guard.apply_record_mutation(mutation)?;
                    }
                    if self.config.audit_events && changed {
                        events.push(spatial_index_updated_event(
                            &guard.definition,
                            "materially_updated",
                            index_state_record_count(&guard),
                        )?);
                    }
                }
            }
        }
        Ok(events)
    }

    /// Apply a commit's record/version/vector mutations. Per collection, the common
    /// path takes the shared collection READ guard and mutates each written record's
    /// shard under that shard's write lock, so disjoint-key commits (even within one
    /// collection) apply concurrently; same-record commits are already serialized by
    /// the optimistic per-record `write_locks`. The rare exclusive path (a vector
    /// dimension changing) re-validates and sets the dimension under the collection
    /// write guard. Index mutations were already applied by
    /// [`Self::validate_and_apply_index_mutations`]; this only consults their
    /// `old_record` to decide whether a vectored value was replaced.
    pub(crate) fn apply_committed_record_writes(
        &self,
        commit_seq: u64,
        writes_by_collection: &BTreeMap<&str, Vec<&TxWrite>>,
        index_mutations_by_collection: &BTreeMap<String, Vec<IndexRecordMutation>>,
        dim_plan: &BTreeMap<String, bool>,
        timer: &mut commit_trace::Timer,
    ) -> Result<()> {
        // MVCC version stamps are commit_seq values (see VersionedRecord).
        let tx_id = TransactionId(commit_seq);
        // One watermark for the whole commit: prune version-chain entries no live
        // snapshot can still read as we re-touch each written record below.
        let gc_watermark = self.gc_watermark();
        let tx_log_path = self.tx_log_path();
        let mut persist_vector_dims = false;
        for (&collection, writes) in writes_by_collection {
            let index_mutations =
                index_mutations_by_collection
                    .get(collection)
                    .ok_or_else(|| BicDbError::Corruption {
                        path: self.tx_log_path(),
                        message: format!(
                            "transaction commit missing prepared index mutations for `{collection}`"
                        ),
                    })?;
            if index_mutations.len() != writes.len() {
                return Err(BicDbError::Corruption {
                    path: self.tx_log_path(),
                    message: format!(
                        "transaction commit index mutation count mismatch for `{collection}`"
                    ),
                });
            }
            let dim_changed = dim_plan.get(collection).copied().unwrap_or(false);
            // Durable record storage for `server_paged`, applied BEFORE the
            // resident state. The order is what makes eviction stubs sound: a
            // stub becomes readable the moment it enters a shard, and its fetch
            // resolves against the paged transaction committed here — so the
            // bytes must already be in the page store when the stub appears.
            // (The reverse order was safe only while shards held full copies.)
            // A failure here fails the whole apply with the resident state
            // untouched; the commit is already durable in the core WAL either
            // way, and reopen replays it.
            let paged_xid = self.apply_record_writes_to_paged(collection, writes)?;
            let paged_ctx = paged_xid.and_then(|xid| self.paged_apply_ctx(collection, xid));
            let outcome = if dim_changed {
                // Rare exclusive path: a None -> Some dimension transition. Take the
                // collection write guard and re-validate the dimension, because a
                // concurrent committer may have set it between plan and apply.
                let mut state = self.collection_state_write(collection)?;
                let dim_before = state.meta.vector_dim;
                let mut authoritative = state.meta.vector_dim;
                for write in writes {
                    if matches!(write.op, TxWriteOp::Upsert) {
                        validate_write_record(collection, write, &mut authoritative)?;
                    }
                }
                state.meta.vector_dim = authoritative;
                persist_vector_dims |= authoritative != dim_before;
                let state: &CollectionState = &state;
                let outcome = apply_record_writes_to_state(
                    state,
                    writes,
                    index_mutations,
                    tx_id,
                    gc_watermark,
                    &tx_log_path,
                    paged_ctx.as_ref(),
                )?;
                if outcome.rebuild_vector_store {
                    state.rebuild_vector_store_shared();
                }
                state.bump_generation();
                outcome
            } else {
                let state = self.collection_state(collection)?;
                let outcome = apply_record_writes_to_state(
                    &state,
                    writes,
                    index_mutations,
                    tx_id,
                    gc_watermark,
                    &tx_log_path,
                    paged_ctx.as_ref(),
                )?;
                if outcome.rebuild_vector_store {
                    state.rebuild_vector_store_shared();
                }
                state.bump_generation();
                outcome
            };
            timer.lap(&commit_trace::APPLY_RECORDS);
            self.refresh_hnsw_after_upsert(collection, outcome.refresh_hnsw)?;
            for record_id in outcome.hnsw_tombstones {
                self.tombstone_hnsw_record(collection, &record_id)?;
            }
        }
        if persist_vector_dims {
            // A None -> Some vector-dimension transition must reach the durable
            // catalog: the paged open path decides residency (lazy registry vs
            // materialized stubs) from the PERSISTED `vector_dim`, and a vector
            // collection that reopens as lazy has no resident vectors, which
            // tombstones every node of a loaded HNSW index. The non-transactional
            // insert path already persists here; this is its transactional twin.
            // Once per collection lifetime, so the catalog write is not a
            // commit-path cost. (Historically masked in paged mode by full
            // Write-frame replay rematerializing records at open; logical
            // commit markers removed that accidental cover.)
            self.persist_catalog()?;
            self.schema_compatibility.invalidate();
        }

        Ok(())
    }

    pub(crate) fn lock_tx_record(
        &self,
        tx_id: TransactionId,
        collection: &str,
        record_id: &str,
        locked_keys: &Mutex<LockedKeys>,
    ) -> Result<()> {
        // A writer holding this record is mid-transaction; SQL row-lock
        // semantics block the second writer until commit, so the wait budget
        // covers multi-statement transactions (event-driven 250µs waits,
        // ~5s total by default) instead of failing after a ~2ms spin. The
        // bounded deadline doubles as deadlock protection.
        self.lock_tx_record_with_attempts(
            tx_id,
            collection,
            record_id,
            write_lock_attempts(),
            true,
            false,
            locked_keys,
        )
    }

    pub(crate) fn lock_tx_record_for_read_committed_update(
        &self,
        tx_id: TransactionId,
        collection: &str,
        record_id: &str,
        locked_keys: &Mutex<LockedKeys>,
    ) -> Result<()> {
        self.lock_tx_record_for_read_committed_update_with_first_wait(
            tx_id,
            collection,
            record_id,
            locked_keys,
            read_committed_first_lock_wait_enabled(),
        )
    }

    pub(crate) fn lock_tx_record_for_read_committed_update_with_first_wait(
        &self,
        tx_id: TransactionId,
        collection: &str,
        record_id: &str,
        locked_keys: &Mutex<LockedKeys>,
        first_wait: bool,
    ) -> Result<()> {
        // Opt-in experiment: extend the existing first-row reservation policy
        // to other READ COMMITTED updates. The serial SQL executor cannot form
        // a row-lock cycle before acquiring its first lock. Once any lock is
        // held, retain the normal loser policy and configured attempt budget.
        if first_wait && locked_keys.lock().len() == 0 {
            return self.lock_tx_first_record_for_reservation(
                tx_id,
                collection,
                record_id,
                locked_keys,
            );
        }
        self.lock_tx_record_with_attempts(
            tx_id,
            collection,
            record_id,
            read_committed_update_lock_attempts(),
            read_committed_update_lock_notify_enabled(),
            false,
            locked_keys,
        )
    }

    /// Wait for a transaction's first row lock without applying wound/wait-die.
    /// With no locks already held this wait cannot be part of a lock cycle, so
    /// aborting either participant only throws away useful work. SQL uses this
    /// for single-row counter reservations whose RETURNING value becomes the
    /// key of later inserts (order/invoice/ticket allocation is the common
    /// shape). Once a transaction owns any other row, fall back to the normal
    /// deadlock-safe loser policy.
    pub(crate) fn lock_tx_first_record_for_reservation(
        &self,
        tx_id: TransactionId,
        collection: &str,
        record_id: &str,
        locked_keys: &Mutex<LockedKeys>,
    ) -> Result<()> {
        let cycle_free_wait = locked_keys.lock().len() == 0;
        self.lock_tx_record_with_attempts(
            tx_id,
            collection,
            record_id,
            write_lock_attempts(),
            true,
            cycle_free_wait,
            locked_keys,
        )
    }

    pub(crate) fn lock_tx_record_with_attempts(
        &self,
        tx_id: TransactionId,
        collection: &str,
        record_id: &str,
        max_attempts: usize,
        notify_wait: bool,
        cycle_free_wait: bool,
        locked_keys: &Mutex<LockedKeys>,
    ) -> Result<()> {
        let key = (collection.to_string(), record_id.to_string());
        let shard_idx = self.write_locks.shard_idx(&key);
        let mut last_owner = None;
        let mut wait_edge: Option<wait_graph::WaitEdge<'_>> = None;
        let mut key_wait: Option<row_lock_waiters::KeyWait<'_>> = None;
        for attempt in 0..=max_attempts {
            // Route to the key's shard; the check-then-insert stays atomic under
            // the single shard lock, so two writers of the same record still
            // serialize.
            let mut shard = self.write_locks.shards[shard_idx].lock();
            match shard.get(&key).copied() {
                Some(owner) if owner != tx_id => {
                    last_owner = Some(owner);
                    // Age-based policies prevent multi-record cycles by only
                    // allowing waits in one transaction-id direction. The
                    // optional graph policy instead checks registered waits
                    // below. A rejected caller must release its locks on
                    // rollback, as with the existing conflict path.
                    let requester_is_older = tx_id.0 < owner.0;
                    let policy = row_lock_policy();
                    let loses_immediately = match policy {
                        RowLockPolicy::OldDies => requester_is_older,
                        RowLockPolicy::WaitDie => !requester_is_older,
                        RowLockPolicy::WaitGraph => false,
                    };
                    if loses_immediately && !cycle_free_wait {
                        break;
                    }
                    if attempt == max_attempts {
                        break;
                    }
                    // Optional graph policy avoids rejecting an acyclic wait
                    // solely because of transaction age. Keep one live edge
                    // per waiter/owner, replacing it if ownership changes.
                    if matches!(policy, RowLockPolicy::WaitGraph)
                        && wait_edge.as_ref().is_none_or(|edge| edge.owner() != owner)
                    {
                        drop(wait_edge.take());
                        wait_edge = self.wait_graph.wait_for(tx_id, owner);
                        if wait_edge.is_none() {
                            break;
                        }
                    }
                    if attempt < 8 {
                        drop(shard);
                        std::hint::spin_loop();
                    } else if notify_wait {
                        if key_wait.is_none() {
                            key_wait =
                                self.write_locks
                                    .register_if_enabled(&key, shard_idx, max_attempts);
                        }
                        if let Some(wait) = &key_wait {
                            if !wait.wait_for(&mut shard) {
                                break;
                            }
                        } else {
                            let _ = self.write_locks.waiters[shard_idx]
                                .wait_for(&mut shard, Duration::from_micros(250));
                        }
                    } else {
                        drop(shard);
                        std::thread::sleep(Duration::from_micros(25));
                    }
                }
                _ => {
                    drop(wait_edge.take());
                    drop(key_wait.take());
                    shard.insert(key.clone(), tx_id);
                    drop(shard);
                    locked_keys.lock().insert(key);
                    return Ok(());
                }
            }
        }
        lock_fail_trace(collection, record_id);
        Err(BicDbError::TransactionConflict(format!(
            "{}:{} is already locked by transaction {}",
            collection,
            record_id,
            last_owner.map(|owner| owner.0).unwrap_or_default()
        )))
    }

    pub(crate) fn snapshot_for_tx(&self, tx_id: TransactionId) -> Result<DbSnapshot> {
        let mut snapshot = DbSnapshot {
            tx_id,
            records: BTreeMap::new(),
        };
        for (collection, state) in &self.collections {
            let state = state.read();
            let mut records = BTreeMap::new();
            for shard in state.read_all() {
                for versions in shard.versions.values() {
                    if let Some(version) = visible_version(versions, tx_id) {
                        let record = version.record.to_record()?;
                        records.insert(record.id.clone(), record);
                    }
                }
            }
            snapshot.records.insert(collection.clone(), records);
        }
        Ok(snapshot)
    }

    pub(crate) fn tx_log_path(&self) -> PathBuf {
        self.path.join(DEFAULT_TRANSACTION_LOG)
    }

    pub(crate) fn prepare_mutations(
        &mut self,
        collection: &str,
        records: Vec<Record>,
    ) -> Result<Vec<PreparedMutation>> {
        let (old_dim, policy) = {
            let state = self.collection_state(collection)?;
            (state.meta.vector_dim, state.meta.policy.clone())
        };
        let mut next_dim = old_dim;
        let mut ids_in_batch = FxHashSet::default();
        let mut prepared = Vec::with_capacity(records.len());

        for mut record in records {
            if let Some(policy) = policy.as_ref() {
                protected_data::prepare_record(
                    &self.sync_state.node_id.to_string(),
                    collection,
                    policy,
                    &mut record,
                )?;
            }
            validate_record(collection, &record, &mut next_dim)?;
            let existing_record = self
                .collection_state(collection)?
                .shard(&record.id)
                .read()
                .contains_record(&record.id);
            let op_type = if existing_record || ids_in_batch.contains(&record.id) {
                OpType::Update
            } else {
                OpType::Insert
            };

            ids_in_batch.insert(record.id.clone());
            prepared.push(PreparedMutation { record, op_type });
        }

        if next_dim != old_dim {
            self.collection_state_mut(collection)?.meta.vector_dim = next_dim;
            self.persist_catalog()?;
        }

        Ok(prepared)
    }

    pub(crate) fn persist_catalog(&self) -> Result<()> {
        let mut collections = self.collections();
        collections.sort_by(|left, right| left.name.cmp(&right.name));
        let catalog = CollectionCatalog { collections };
        let bytes = serde_json::to_vec_pretty(&catalog)?;
        write_sidecar(
            &self.path.join(DEFAULT_COLLECTION_CATALOG),
            FrameKind::Index,
            &bytes,
            self.config.fsync,
            &self.encryption,
        )
    }

    pub(crate) fn persist_index_catalog(&self) -> Result<()> {
        let mut indexes = self.index_definitions();
        indexes.sort_by(|left, right| left.name.cmp(&right.name));
        let catalog = IndexCatalog { indexes };
        let bytes = serde_json::to_vec_pretty(&catalog)?;
        write_sidecar(
            &self.path.join(DEFAULT_INDEX_CATALOG),
            FrameKind::Index,
            &bytes,
            self.config.fsync,
            &self.encryption,
        )
    }

    /// Recompute every index's `size_bytes` (and entry count) from the live
    /// structures and rewrite the maintenance sidecar.
    ///
    /// `size_bytes` was only ever written at CREATE INDEX / REBUILD / VERIFY.
    /// Building an index before its rows land — the ordinary restore and
    /// schema-first order — therefore recorded 0 and never revisited it, so
    /// `index-maintenance.json` reported `size_bytes: 0` for every index
    /// forever and index memory could not be attributed. Ordinary DML
    /// deliberately does not touch the sidecar (it would be a file write per
    /// commit); this is the on-demand refresh instead.
    pub fn refresh_index_maintenance_sizes(&self) -> Result<usize> {
        let mut catalog = self.load_index_maintenance_catalog()?;
        let mut refreshed = 0usize;
        for (name, index) in &self.indexes {
            let state = index.read();
            let size_bytes = index_state_size_bytes(&state);
            let Some(status) = catalog.indexes.get_mut(name) else {
                continue;
            };
            if status.size_bytes != size_bytes {
                status.size_bytes = size_bytes;
                status.updated_unix_ms = unix_timestamp_millis();
                refreshed += 1;
            }
        }
        if refreshed > 0 {
            self.persist_index_maintenance_catalog(&catalog)?;
        }
        Ok(refreshed)
    }

    pub(crate) fn load_index_maintenance_catalog(&self) -> Result<IndexMaintenanceCatalog> {
        let path = self.path.join(DEFAULT_INDEX_MAINTENANCE);
        match read_optional_sidecar(&path, FrameKind::Index, &self.encryption)? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
            None => Ok(IndexMaintenanceCatalog::default()),
        }
    }

    pub(crate) fn persist_index_maintenance_catalog(
        &self,
        catalog: &IndexMaintenanceCatalog,
    ) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(catalog)?;
        write_sidecar(
            &self.path.join(DEFAULT_INDEX_MAINTENANCE),
            FrameKind::Index,
            &bytes,
            self.config.fsync,
            &self.encryption,
        )
    }

    pub(crate) fn persist_index_maintenance_status(
        &self,
        definition: &IndexDefinition,
        status: IndexMaintenanceStatus,
    ) -> Result<()> {
        let mut catalog = self.load_index_maintenance_catalog()?;
        catalog.indexes.insert(definition.name.clone(), status);
        self.persist_index_maintenance_catalog(&catalog)
    }

    pub(crate) fn index_maintenance_status(
        &self,
        name: &str,
    ) -> Result<Option<IndexMaintenanceStatus>> {
        Ok(self
            .load_index_maintenance_catalog()?
            .indexes
            .get(name)
            .cloned())
    }

    pub(crate) fn index_maintenance_interrupted(&self, name: &str) -> Result<bool> {
        Ok(self
            .index_maintenance_status(name)?
            .map(|status| status.status == "building")
            .unwrap_or(false))
    }

    pub(crate) fn persist_planner_stats(&self) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(&self.planner_stats)?;
        write_sidecar(
            &self.path.join(DEFAULT_PLANNER_STATS),
            FrameKind::Index,
            &bytes,
            self.config.fsync,
            &self.encryption,
        )
    }

    pub(crate) fn collect_table_statistics(&self, collection: &str) -> Result<TableStatistics> {
        let state = self.collection_state(collection)?;
        // A bounded reservoir sample of the resident records feeds the
        // whole-collection `collect_column_statistics(&RecordMap, ..)` API;
        // tables at or under the sample limit are analyzed exactly. Only the
        // sampled entries are cloned (an `Arc` bump each), never the map.
        let shards = state.read_all();
        let total_rows: usize = shards.iter().map(|shard| shard.records.len()).sum();
        let mut sampler = AnalyzeSampler::new(analyze_sample_limit(), collection);
        let mut sampled = Vec::<(RowId, RecordEntry)>::new();
        for (key, entry) in shards.iter().flat_map(|shard| shard.records.iter()) {
            match sampler.offer() {
                Some(slot) if slot == sampled.len() => sampled.push((key.clone(), entry.clone())),
                Some(slot) => sampled[slot] = (key.clone(), entry.clone()),
                None => {}
            }
        }
        drop(shards);
        let all_records: RecordMap = sampled.into_iter().collect();
        let mut fields = BTreeMap::<String, IndexField>::new();
        fields.insert("id".to_string(), IndexField::Id);
        fields.insert("timestamp".to_string(), IndexField::Timestamp);
        fields.insert("geometry".to_string(), IndexField::Geometry);

        for entry in all_records.values() {
            let metadata = entry.record.metadata_value()?;
            if let Some(object) = metadata.as_object() {
                for key in object.keys() {
                    fields
                        .entry(key.clone())
                        .or_insert_with(|| IndexField::MetadataPath(vec![key.clone()]));
                }
            }
        }
        for index in self.indexes.values() {
            let index = index.read();
            if index.definition.collection != collection {
                continue;
            }
            for field in &index.definition.fields {
                fields
                    .entry(index_field_stats_name(field))
                    .or_insert_with(|| field.clone());
            }
        }

        let mut columns = BTreeMap::new();
        for (name, field) in fields {
            columns.insert(
                name,
                collect_column_statistics(&all_records, field, total_rows),
            );
        }

        let mut indexes = BTreeMap::new();
        for index in self.indexes.values() {
            let index = index.read();
            if index.definition.collection != collection {
                continue;
            }
            // READ-THROUGH full-text indexes keep no resident postings; the
            // durable keyspaces are the source of truth. Indexed rows =
            // distinct pks across the tail + doc-terms blobs (a bulk-built
            // row appears exactly once, as its blob); distinct keys = tail
            // terms + block terms.
            let read_through_fts =
                index.definition.kind == IndexKind::FullText && index.paged_read_through;
            let read_through_btree =
                index.definition.kind == IndexKind::BTree && index.paged_read_through;
            let (indexed_rows, distinct_keys) = if read_through_fts {
                self.read_through_fts_statistics(&index.definition.name)?
            } else if read_through_btree {
                // The resident store is empty for a read-through B-tree;
                // without this branch the planner sees zero keys and prices
                // every index plan as if the index were useless.
                self.read_through_btree_statistics(&index.definition.name)?
            } else {
                (
                    index_state_record_count(&index),
                    match index.definition.kind {
                        IndexKind::BTree
                        | IndexKind::FullText
                        | IndexKind::Jsonb
                        | IndexKind::Array => index.store.key_count(),
                        IndexKind::Spatial => index.spatial.as_ref().map(RTree::size).unwrap_or(0),
                    },
                )
            };
            indexes.insert(
                index.definition.name.clone(),
                IndexStatistics {
                    index_name: index.definition.name.clone(),
                    fields: index.definition.fields.clone(),
                    indexed_rows,
                    distinct_keys,
                },
            );
        }

        Ok(TableStatistics {
            collection: collection.to_string(),
            row_count: total_rows,
            columns,
            indexes,
            analyzed_at_unix_ms: unix_timestamp_millis(),
        })
    }

    pub(crate) fn rebuild_collection_indexes(&mut self, collection: &str) -> Result<()> {
        let names = self
            .indexes
            .values()
            .filter(|index| index.read().definition.collection == collection)
            .map(|index| index.read().definition.name.clone())
            .collect::<Vec<_>>();
        let mut events = Vec::new();
        for name in names {
            let definition = self
                .indexes
                .get(&name)
                .map(|index| index.read().definition.clone())
                .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?;
            let state = self.index_state_for_build(definition)?;
            if self.config.audit_events && state.definition.kind == IndexKind::Spatial {
                events.push(spatial_index_updated_event(
                    &state.definition,
                    "materially_updated",
                    index_state_record_count(&state),
                )?);
            }
            self.index_generation
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.indexes.insert(name, RwLock::new(state));
        }
        for event in events {
            self.events.lock().append(event)?;
        }
        Ok(())
    }

    pub(crate) fn validate_collection_index_mutations(
        &self,
        collection: &str,
        mutations: &[IndexRecordMutation],
    ) -> Result<()> {
        for index in self.indexes.values() {
            let index = index.read();
            if index.definition.collection != collection {
                continue;
            }
            if index.definition.exclusion.is_some() {
                self.validate_exclusion_record_mutations(&index.definition, mutations)?;
            }
            match index.definition.kind {
                IndexKind::BTree if index.paged_read_through => {
                    self.validate_paged_btree_record_mutations(&index.definition, mutations)?
                }
                IndexKind::BTree => index.validate_btree_record_mutations(mutations)?,
                IndexKind::FullText | IndexKind::Jsonb | IndexKind::Array => {}
                IndexKind::Spatial => {
                    let mut candidate = index.clone();
                    for mutation in mutations {
                        candidate.apply_record_mutation(mutation)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn apply_collection_index_mutations(
        &self,
        collection: &str,
        mutations: &mut [IndexRecordMutation],
    ) -> Result<()> {
        // These callers (legacy/replicated/delete) apply records BEFORE the index,
        // so every record's locator is now resolvable — fill it for the BTree apply.
        self.fill_index_mutation_rowids(collection, mutations)?;
        let names = self
            .indexes
            .values()
            .filter(|index| index.read().definition.collection == collection)
            .map(|index| index.read().definition.name.clone())
            .collect::<Vec<_>>();
        let mut events = Vec::new();
        for name in names {
            let mut index = self
                .indexes
                .get(&name)
                .ok_or_else(|| BicDbError::Index(format!("index `{name}` not found")))?
                .write();
            let mut changed = false;
            for mutation in mutations.iter() {
                changed |= index.apply_record_mutation(mutation)?;
            }
            if self.config.audit_events && changed && index.definition.kind == IndexKind::Spatial {
                events.push(spatial_index_updated_event(
                    &index.definition,
                    "materially_updated",
                    index_state_record_count(&index),
                )?);
            }
        }
        for event in events {
            self.events.lock().append(event)?;
        }
        Ok(())
    }

    pub(crate) fn prepared_index_mutations(
        &self,
        collection: &str,
        prepared: &[PreparedMutation],
    ) -> Result<Vec<IndexRecordMutation>> {
        let state = self.collection_state(collection)?;
        let mut current_records: FxHashMap<String, Arc<Record>> = FxHashMap::default();
        for mutation in prepared {
            let resident = {
                let shard = state.shard(&mutation.record.id).read();
                shard
                    .get_record(&mutation.record.id)
                    .map(|entry| entry.record.clone())
            };
            let current = match resident {
                Some(stored) => match stored.to_record() {
                    Ok(record) => Some(record),
                    Err(_) => None,
                },
                // Lazy paged collection: the current row may exist only in the
                // page store. Without this, an update's index mutation carried
                // old_record = None, so the resident index kept the stale
                // entry alongside the new one.
                None if state.paged_lazy => match &self.paged_records {
                    Some(paged) => {
                        paged.get(&paged.latest_snapshot(), collection, &mutation.record.id)?
                    }
                    None => None,
                },
                None => None,
            };
            if let Some(record) = current {
                current_records.insert(mutation.record.id.clone(), Arc::new(record));
            }
        }
        let mut mutations = Vec::with_capacity(prepared.len());
        for prepared in prepared {
            let old_record =
                OldImage::from_record(current_records.get(&prepared.record.id).cloned());
            let new_record = Arc::new(prepared.record.clone());
            current_records.insert(prepared.record.id.clone(), Arc::clone(&new_record));
            mutations.push(IndexRecordMutation {
                old_record,
                new_record: OldImage::from_record(Some(new_record)),
                rowid: None,
                changed: None,
            });
        }
        Ok(mutations)
    }

    pub(crate) fn delete_index_mutation(
        &self,
        collection: &str,
        record_id: &str,
    ) -> Result<Option<IndexRecordMutation>> {
        let state = self.collection_state(collection)?;
        let resident = {
            let shard = state.shard(record_id).read();
            shard
                .get_record(record_id)
                .map(|entry| entry.record.clone())
        };
        let old = match resident {
            Some(stored) => Some(stored.to_record()?),
            // Lazy paged collection: the row may exist only in the page store.
            None if state.paged_lazy => match &self.paged_records {
                Some(paged) => paged.get(&paged.latest_snapshot(), collection, record_id)?,
                None => None,
            },
            None => None,
        };
        Ok(old.map(|record| IndexRecordMutation {
            old_record: OldImage::from_record(Some(Arc::new(record))),
            new_record: OldImage::none(),
            rowid: None,
            changed: None,
        }))
    }

    pub(crate) fn tx_index_mutations(
        &self,
        collection: &str,
        writes: &[&TxWrite],
        writes_are_unique: bool,
    ) -> Result<Vec<IndexRecordMutation>> {
        let state = self.collection_state(collection)?;
        if writes_are_unique {
            let mut mutations = Vec::with_capacity(writes.len());
            for write in writes {
                let resident = {
                    let shard = state.shard(&write.record_id).read();
                    shard
                        .get_record(&write.record_id)
                        .map(|entry| entry.record.clone())
                };
                let old_record = match resident {
                    Some(stored) => match &write.previous {
                        Some((read_stored, read_record)) if Arc::ptr_eq(read_stored, &stored) => {
                            OldImage::from_record(Some(Arc::clone(read_record)))
                        }
                        _ => OldImage::from_stored(stored),
                    },
                    None if state.paged_lazy => match &self.paged_records {
                        Some(paged) => paged
                            .get(&paged.latest_snapshot(), collection, &write.record_id)?
                            .map(|record| OldImage::from_record(Some(Arc::new(record))))
                            .unwrap_or_else(OldImage::none),
                        None => OldImage::none(),
                    },
                    None => OldImage::none(),
                };
                let new_record = match write.op {
                    TxWriteOp::Upsert => match &write.stored {
                        Some(stored) => OldImage::from_stored(Arc::clone(stored)),
                        None => OldImage::from_record(Some(write.record()?.cloned().ok_or_else(
                            || BicDbError::Corruption {
                                path: self.tx_log_path(),
                                message: "transaction upsert missing record".to_string(),
                            },
                        )?)),
                    },
                    TxWriteOp::Delete => OldImage::none(),
                };
                let changed = match write.op {
                    TxWriteOp::Upsert if old_record.is_some() => write.changed.clone(),
                    _ => None,
                };
                mutations.push(IndexRecordMutation {
                    old_record,
                    new_record,
                    rowid: None,
                    changed,
                });
            }
            return Ok(mutations);
        }
        let mut current_records: FxHashMap<String, OldImage> = FxHashMap::default();
        for write in writes {
            let resident = {
                let shard = state.shard(&write.record_id).read();
                shard
                    .get_record(&write.record_id)
                    .map(|entry| entry.record.clone())
            };
            let current = match resident {
                // The row is still the exact StoredRecord this transaction read
                // and parsed: reuse that parse. Otherwise keep the resident row
                // unparsed — B-tree key diffs read its cells, and only the
                // consumers that need a Value parse it (once, memoized).
                Some(stored) => Some(match &write.previous {
                    Some((read_stored, read_record)) if Arc::ptr_eq(read_stored, &stored) => {
                        OldImage::from_record(Some(Arc::clone(read_record)))
                    }
                    _ => OldImage::from_stored(stored),
                }),
                // Same lazy fallback as `prepared_index_mutations`, same reason.
                None if state.paged_lazy => match &self.paged_records {
                    Some(paged) => paged
                        .get(&paged.latest_snapshot(), collection, &write.record_id)?
                        .map(|record| OldImage::from_record(Some(Arc::new(record)))),
                    None => None,
                },
                None => None,
            };
            if let Some(image) = current {
                current_records.insert(write.record_id.clone(), image);
            }
        }
        let mut mutations = Vec::with_capacity(writes.len());
        for write in writes {
            let old_record = current_records
                .get(&write.record_id)
                .cloned()
                .unwrap_or_else(OldImage::none);
            let new_record = match write.op {
                TxWriteOp::Upsert => {
                    // The prepared stored form when the write carries one
                    // (index keys from cells, no Value tree); the parsed
                    // record otherwise.
                    let image = match &write.stored {
                        Some(stored) => OldImage::from_stored(Arc::clone(stored)),
                        None => OldImage::from_record(Some(write.record()?.cloned().ok_or_else(
                            || BicDbError::Corruption {
                                path: self.tx_log_path(),
                                message: "transaction upsert missing record".to_string(),
                            },
                        )?)),
                    };
                    current_records.insert(write.record_id.clone(), image.clone());
                    image
                }
                TxWriteOp::Delete => {
                    current_records.remove(&write.record_id);
                    OldImage::none()
                }
            };
            let changed = match write.op {
                TxWriteOp::Upsert if old_record.is_some() => write.changed.clone(),
                _ => None,
            };
            mutations.push(IndexRecordMutation {
                old_record,
                new_record,
                rowid: None,
                changed,
            });
        }
        Ok(mutations)
    }

    pub(crate) fn tx_index_mutations_by_collection(
        &self,
        writes_by_collection: &BTreeMap<&str, Vec<&TxWrite>>,
        writes_are_unique: bool,
    ) -> Result<BTreeMap<String, Vec<IndexRecordMutation>>> {
        let mut index_mutations_by_collection = BTreeMap::new();
        for (&collection, writes) in writes_by_collection {
            index_mutations_by_collection.insert(
                collection.to_owned(),
                self.tx_index_mutations(collection, writes, writes_are_unique)?,
            );
        }
        Ok(index_mutations_by_collection)
    }

    /// Resolve each mutation's record locator (`rowid`) from the collection's
    /// registry — the index payload. Called twice in the commit path: before
    /// uniqueness validation (a brand-new record resolves to `None`, handled in
    /// pk-space there) and again after record apply (every record now resolves to
    /// `Some`, so index entries are inserted under the right locator). Cheap: a
    /// per-record shard READ lock and one hash lookup, no allocation.
    pub(crate) fn fill_index_mutation_rowids(
        &self,
        collection: &str,
        mutations: &mut [IndexRecordMutation],
    ) -> Result<()> {
        let state = self.collection_state(collection)?;
        for mutation in mutations {
            // Resolve only once: a rowid captured before record apply (e.g. a
            // delete's locator, which would otherwise be GC'd away with its
            // version chain) must survive the post-apply re-resolution. Pure new
            // inserts start `None` here and are resolved after their record applies.
            if mutation.rowid.is_some() {
                continue;
            }
            let pk = mutation.new_record.id().or(mutation.old_record.id());
            mutation.rowid = pk.and_then(|pk| state.shard(pk).read().rowid_of(pk));
        }
        Ok(())
    }

    /// Apply [`Self::fill_index_mutation_rowids`] to every collection's mutations.
    pub(crate) fn fill_all_index_mutation_rowids(
        &self,
        index_mutations_by_collection: &mut BTreeMap<String, Vec<IndexRecordMutation>>,
    ) -> Result<()> {
        for (collection, mutations) in index_mutations_by_collection.iter_mut() {
            self.fill_index_mutation_rowids(collection, mutations)?;
        }
        Ok(())
    }

    /// Phase 0 of the background (fuzzy) checkpoint: capture the WAL truncation
    /// boundary. Returns the current on-disk log length, which (right after a flush,
    /// at a frame boundary) separates all already-committed frames below it from
    /// future commits appended above it. Everything below is about to be
    /// materialized into segments by [`Self::checkpoint_write_segments`]; phase 2
    /// then drops exactly that prefix.
    ///
    /// MUST be called under the EXCLUSIVE write lock so the flush and the length
    /// read are atomic w.r.t. committers -- otherwise a commit could append between
    /// them and the boundary would cut a not-yet-materialized commit, losing it.
    /// Rewrite a collection's segment (compacting garbage) instead of appending
    /// once its frame count would exceed this multiple of the live record count.
    /// 2 => a segment holds at most ~2x live frames, so recovery reads at most ~2x.
    const GARBAGE_FACTOR: u64 = 2;
    /// Don't bother rewriting segments below this many frames -- the garbage is
    /// trivially small and a rewrite would just churn. Avoids thrashing tiny/hot
    /// collections where the live count momentarily dips (e.g. to 0 or 1).
    const MIN_COMPACT_FRAMES: u64 = 256;

    pub fn checkpoint_begin(&self) -> Result<CheckpointPlan> {
        self.tx_log.flush_pending()?;
        let keep_from = self.wal_bytes();
        // Snapshot-and-clear each collection's dirty set. Under the exclusive write
        // lock no commit runs, so the boundary and the dirty sets form a consistent
        // cut: everything dirty (changed since the last checkpoint) is captured
        // here for materialization, and commits after this point start a fresh
        // dirty set and are retained in the WAL suffix.
        let mut dirty = FxHashMap::default();
        let collections: Vec<String> = self.collections.keys().cloned().collect();
        for collection in collections {
            let state = self.collection_state_write(&collection)?;
            let mut taken = Vec::new();
            // Detach the existing hash tables in O(shards), not O(dirty rows).
            // Resolving millions of RowIds to allocated string keys here used
            // to hold the exclusive database lock for seconds.
            for (index, shard) in state.shards.iter().enumerate() {
                let mut shard = shard.write();
                if !shard.dirty_resident.is_empty() || !shard.dirty_unresolved.is_empty() {
                    taken.push(CheckpointDirtyShard {
                        index,
                        resident: std::mem::take(&mut shard.dirty_resident),
                        unresolved: std::mem::take(&mut shard.dirty_unresolved),
                    });
                }
            }
            if !taken.is_empty() {
                dirty.insert(collection, taken);
            }
        }
        Ok(CheckpointPlan { keep_from, dirty })
    }

    /// Phase 1 of the background (fuzzy) checkpoint: serialize the committed record
    /// state into segments WITHOUT the exclusive write lock, so committers keep
    /// running. Call under a shared read lock (concurrent with the shared commit
    /// path), AFTER [`Self::checkpoint_begin`]. Every commit below the begin
    /// boundary completed before this runs, so it is in the records map and is
    /// captured here; commits at/above the boundary may or may not be captured but
    /// are retained in the WAL by phase 2 for idempotent replay, so the fuzzy
    /// (per-collection, point-in-time-varying) snapshot is still recovered exactly.
    ///
    /// Per collection we snapshot under a brief read lock, release it, then write
    /// the segment, so a long write never blocks that collection's committers.
    /// Normally we APPEND only the dirty delta (O(delta)); but appended garbage
    /// (superseded versions + tombstones) accumulates, so once the segment frame
    /// count would exceed [`Self::GARBAGE_FACTOR`]x the live record count we instead
    /// fully REWRITE the segment from the live records, compacting the garbage away
    /// and bounding both segment size and recovery read-amplification. Either way
    /// the write is atomic (append fsync, or rewrite temp + rename), so a crash
    /// leaves a complete old or new segment.
    pub fn checkpoint_write_segments(&self, plan: &CheckpointPlan) -> Result<()> {
        // The fuzzy checkpoint advances segments past the last full-compact snapshot,
        // so that binary snapshot is now stale. Drop its commit marker; the next open
        // falls back to segments until a full `compact` re-publishes a fresh snapshot.
        crate::snapshot::invalidate(&self.path)?;
        // What phase 1 should do for one collection, decided under the read lock and
        // executed (I/O) after releasing it. Records are snapshotted as
        // `StoredRecord` clones (a flat copy of the raw metadata JSON) and
        // serialized via `RecordFrameRef` — never through `to_record()`, whose
        // parse-to-`Value` round trip would dominate the checkpoint's CPU and
        // memory-bandwidth cost.
        enum SegmentWrite {
            /// Append these delta frames; new frame count = old + delta.len().
            Append(Vec<StoredDeltaFrame>, u64),
            /// Rewrite the whole segment from these live records; new count = len.
            Rewrite(Vec<Arc<StoredRecord>>),
        }
        enum StoredDeltaFrame {
            Upsert(Arc<StoredRecord>),
            Delete(String),
        }
        for (collection, dirty_shards) in &plan.dirty {
            // Decide append-vs-rewrite and snapshot the needed records under a brief
            // read lock (excludes the per-collection commit write lock, so the clone
            // is a consistent point-in-time snapshot of this collection).
            let action = {
                let state = self.collection_state(collection)?;
                let live = state.record_count() as u64;
                let delta = dirty_shards
                    .iter()
                    .map(|shard| shard.resident.len() + shard.unresolved.len())
                    .sum::<usize>() as u64;
                let prospective = state.segment_frame_count + delta;
                if prospective > Self::MIN_COMPACT_FRAMES
                    && prospective > live.saturating_mul(Self::GARBAGE_FACTOR)
                {
                    SegmentWrite::Rewrite(
                        state
                            .read_all()
                            .iter()
                            .flat_map(|shard| shard.records.values())
                            .map(|entry| entry.record.clone())
                            .collect(),
                    )
                } else {
                    let mut frames = Vec::with_capacity(delta as usize);
                    for dirty in dirty_shards {
                        let mut resident = dirty.resident.iter();
                        loop {
                            // Bound each shard-lock hold while cloning cheap
                            // record Arcs. Committers can run between chunks.
                            let shard = state.shards[dirty.index].read();
                            let mut visited = 0;
                            for rowid in resident.by_ref().take(256) {
                                visited += 1;
                                if let Some(entry) = shard.records.get(rowid) {
                                    frames.push(StoredDeltaFrame::Upsert(entry.record.clone()));
                                }
                                // A resident locator removed after phase 0 may
                                // be skipped: that deletion remains in the WAL
                                // suffix. RowIds are never reused in-process.
                            }
                            drop(shard);
                            if visited < 256 {
                                break;
                            }
                        }
                        for id in &dirty.unresolved {
                            let shard = state.shards[dirty.index].read();
                            match shard.rowid_of(id) {
                                Some(rowid) => {
                                    if let Some(entry) = shard.records.get(&rowid) {
                                        if !dirty.resident.contains(&rowid) {
                                            frames.push(StoredDeltaFrame::Upsert(
                                                entry.record.clone(),
                                            ));
                                        }
                                    } else {
                                        frames.push(StoredDeltaFrame::Delete(id.clone()));
                                    }
                                }
                                None => frames.push(StoredDeltaFrame::Delete(id.clone())),
                            }
                        }
                    }
                    SegmentWrite::Append(frames, state.segment_frame_count)
                }
            };
            // Do the I/O outside the lock, then record the new frame count. Only the
            // (single) checkpoint thread mutates segment_frame_count, so reading it
            // above and writing it below cannot race.
            let new_count = match action {
                SegmentWrite::Append(frames, base) => {
                    let payloads = frames
                        .iter()
                        .map(|frame| match frame {
                            StoredDeltaFrame::Upsert(record) => {
                                serde_json::to_vec(&RecordFrameRef::Upsert {
                                    record: record.as_ref(),
                                })
                            }
                            StoredDeltaFrame::Delete(id) => {
                                serde_json::to_vec(&RecordFrameRef::Delete { id })
                            }
                        })
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    // Append the delta to the existing segment (created on first
                    // append). Recovery replays frames in order, last-write-wins, so
                    // appended frames supersede older ones for the same record id.
                    storage::append_frames(
                        &self.segment_path(collection),
                        FrameKind::Record,
                        &payloads,
                        self.config.fsync,
                        &self.config.compression,
                        &self.encryption,
                    )?;
                    base + frames.len() as u64
                }
                SegmentWrite::Rewrite(records) => {
                    let count = records.len() as u64;
                    rewrite_stored_record_segment(
                        &self.segment_path(collection),
                        &records,
                        self.config.fsync,
                        &self.config.compression,
                        &self.encryption,
                    )?;
                    count
                }
            };
            if let Ok(mut state) = self.collection_state_write(collection) {
                state.segment_frame_count = new_count;
            }
        }
        Ok(())
    }

    /// Undo a [`Self::checkpoint_begin`] whose phase 1/2 failed: re-mark the
    /// snapshotted dirty ids so the next checkpoint re-materializes them. Required
    /// because `checkpoint_begin` clears the dirty sets up front; without this a
    /// failed checkpoint would drop records that a later WAL truncation then loses.
    /// Unions (does not replace) so ids re-dirtied since `begin` are preserved.
    pub fn checkpoint_abort(&self, plan: CheckpointPlan) {
        for (collection, dirty_shards) in plan.dirty {
            if let Ok(state) = self.collection_state_write(&collection) {
                for dirty in dirty_shards {
                    let mut shard = state.shards[dirty.index].write();
                    shard.dirty_resident.extend(dirty.resident);
                    shard.dirty_unresolved.extend(dirty.unresolved);
                }
            }
        }
    }

    /// Phase 2 of the background (fuzzy) checkpoint: under the EXCLUSIVE write lock
    /// (no commit can run), drop the WAL prefix below `keep_from_offset` (the
    /// [`Self::checkpoint_begin`] boundary) now that everything below it is durable
    /// in segments (written by phase 1). Ordering is the crash-safety contract:
    /// segments were materialized + fsync'd in phase 1 BEFORE this truncation, so a
    /// crash between the phases recovers from segments + the still-full WAL, and the
    /// truncation itself is atomic (temp + rename) -- a crash leaves either the full
    /// or the suffix log, both of which reconstruct the committed state together
    /// with the segments. Returns the WAL size before/after for reporting.
    pub fn checkpoint_truncate_wal(&mut self, keep_from_offset: u64) -> Result<(u64, u64)> {
        let before = self.wal_bytes();
        // Catalog/index catalog must be durable and consistent with the segments
        // before we drop the log frames that would otherwise re-create them.
        self.persist_catalog()?;
        self.persist_index_catalog()?;
        // Flush any commits that landed since `begin`, then drop the prefix below
        // the boundary by copying only the retained tail bytes (no frame parsing).
        self.tx_log.flush_pending()?;
        copy_transaction_log_tail(&self.tx_log_path(), keep_from_offset, self.config.fsync)?;
        // Drop the stale log handle so the next commit reopens the truncated log.
        self.tx_log.invalidate();
        let after = self.wal_bytes();
        Ok((before, after))
    }

    pub(crate) fn ensure_no_pending_transactions(&self) -> Result<()> {
        let has_write_locks = !self.write_locks.is_empty();
        let has_pending = self
            .tx_states
            .values_any(|state| *state == TxState::Pending);
        if has_write_locks || has_pending {
            return Err(BicDbError::Compaction(
                "cannot compact while transactions are pending".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn write_compaction_checkpoint(
        &self,
        checkpoint_id: &str,
        phase: CompactionCheckpointPhase,
        collections: &[String],
        transaction_log_bytes_before: u64,
        event_log_bytes_before: u64,
        sync_log_bytes_before: u64,
    ) -> Result<()> {
        let now = unix_timestamp();
        let manifest = CompactionCheckpointManifest {
            checkpoint_id: checkpoint_id.to_string(),
            phase,
            collections: collections.to_vec(),
            created_at: now,
            updated_at: now,
            transaction_log_bytes_before,
            event_log_bytes_before,
            sync_log_bytes_before,
        };
        let bytes = serde_json::to_vec_pretty(&manifest)?;
        storage::write_atomic(
            &self.path.join(DEFAULT_COMPACTION_CHECKPOINT),
            &bytes,
            self.config.fsync,
        )
    }

    pub(crate) fn sidecar_bytes(&self) -> Result<u64> {
        let mut total = 0_u64;
        for name in [DEFAULT_VECTOR_INDEX_DIR, DEFAULT_GRAPH_DIR, "analytics"] {
            let path = self.path.join(name);
            if path.exists() {
                total = total.saturating_add(storage::total_dir_size(&path)?);
            }
        }
        Ok(total)
    }

    pub(crate) fn compact_collection_inner(
        &mut self,
        collection: &str,
    ) -> Result<CollectionCompactionReport> {
        // In paged mode segments carry no rows, so there is nothing to rewrite —
        // and rewriting would serialize every row back INTO a segment, quietly
        // re-creating the on-disk duplication this mode exists to end. The page
        // store bounds its own garbage (WAL checkpoints + vacuum). The derived
        // structures compaction is also responsible for are still rebuilt.
        if self.paged_records.is_some() {
            self.rebuild_collection_indexes(collection)?;
            if self.hnsw_indexes.read().contains_key(collection) {
                let _ = self.rebuild_vector_index(collection)?;
            }
            self.refresh_graph_projections()?;
            let live_records = {
                let state = self.collection_state(collection)?;
                state.rebuild_vector_store_shared();
                state.record_count()
            };
            return Ok(CollectionCompactionReport {
                collection: collection.to_string(),
                live_records,
                bytes_before: 0,
                bytes_after: 0,
                bytes_reclaimed: 0,
            });
        }
        // This rewrites the collection's segment, so any existing binary snapshot
        // (a derived cache of a prior state) is now stale: drop its commit marker.
        // A full `compact` re-publishes a fresh snapshot at the end; a lone
        // single-collection compact leaves none, so the next open falls back.
        crate::snapshot::invalidate(&self.path)?;
        let segment_path = self.segment_path(collection);
        let bytes_before = match fs::metadata(&segment_path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };

        // Resident handles only (an `Arc` bump each); the records are decoded
        // batch by batch inside the rewrite, never all at once.
        let stored = {
            let state = self.collection_state(collection)?;
            let mut stored = state
                .read_all()
                .iter()
                .flat_map(|shard| shard.records.values())
                .map(|entry| entry.record.clone())
                .collect::<Vec<_>>();
            stored.sort_by(|left, right| left.id.cmp(&right.id));
            stored
        };

        let offsets = rewrite_record_segment_streamed(
            &segment_path,
            &stored,
            self.config.fsync,
            &self.config.compression,
            &self.encryption,
        )?;

        {
            let state = self.collection_state_mut(collection)?;
            for (record, offset) in stored.iter().zip(offsets) {
                if let Some(entry) = state.shard_mut(&record.id).get_record_mut(&record.id) {
                    entry.offset = offset;
                }
            }
            state.rebuild_vector_store();
            // The segment now holds exactly the live records with no garbage, and
            // every record is materialized, so reset the checkpoint accounting.
            state.segment_frame_count = stored.len() as u64;
            for shard in state.shards_mut() {
                shard.dirty_resident.clear();
                shard.dirty_unresolved.clear();
            }
        }

        self.rebuild_collection_indexes(collection)?;
        if self.hnsw_indexes.read().contains_key(collection) {
            let _ = self.rebuild_vector_index(collection)?;
        }
        self.refresh_graph_projections()?;

        let bytes_after = match fs::metadata(&segment_path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };

        Ok(CollectionCompactionReport {
            collection: collection.to_string(),
            live_records: stored.len(),
            bytes_before,
            bytes_after,
            bytes_reclaimed: bytes_before.saturating_sub(bytes_after),
        })
    }

    pub(crate) fn persist_sync_state(&self) -> Result<()> {
        self.sync_state
            .persist(self.path.join(DEFAULT_SYNC_STATE), self.config.fsync)
    }

    pub(crate) fn mark_sync_bundle_exported(&mut self, bundle: &SyncBundle) -> Result<()> {
        self.mark_sync_exported(bundle.next_checkpoint, bundle.event_count)
    }

    /// Advance the engine-owned export watermark after a bundle exported via
    /// [`Self::export_sync_bundle_since`] was durably delivered. Unlike an
    /// externally stored checkpoint, this watermark lives in `sync_state.json`
    /// and is reset by `compact()` together with the event-offset rewrite, so
    /// callers that drive sync from it stay correct across compaction
    /// (re-exports deduplicate by event id on the receiving side).
    pub fn mark_sync_exported(
        &mut self,
        next_checkpoint: SyncCheckpoint,
        event_count: usize,
    ) -> Result<()> {
        let now = sync_mesh::unix_timestamp();
        self.sync_state.last_export_offset = next_checkpoint.event_offset;
        self.sync_state.last_export_at = Some(now);
        self.sync_state.last_sync_at = Some(now);
        self.sync_state.exported_events = self
            .sync_state
            .exported_events
            .saturating_add(event_count as u64);
        self.persist_sync_state()
    }

    pub(crate) fn ensure_collection(&self, collection: &str) -> Result<()> {
        self.collection_state(collection).map(|_| ())
    }

    pub(crate) fn validate_secure_index_definition(
        &self,
        definition: &IndexDefinition,
    ) -> Result<()> {
        let state = self.collection_state(&definition.collection)?;
        let Some(policy) = state.meta.policy.as_ref() else {
            return Ok(());
        };
        for field in &definition.fields {
            validate_secure_index_field(&definition.collection, definition.unique, policy, field)?;
        }
        Ok(())
    }

    pub(crate) fn validate_secure_filter(
        policy: &CollectionPolicy,
        filter: Option<&JsonFilter>,
    ) -> Result<()> {
        let Some(filter) = filter else {
            return Ok(());
        };
        for key in filter.equals_keys() {
            let path = vec![key.to_string()];
            if protected_data::encrypted_field(policy, &path).is_some() {
                return Err(BicDbError::Authorization(format!(
                    "encrypted field `{key}` cannot be filtered directly; use an approved blind-index lookup"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn ensure_unprotected_legacy_access(
        &self,
        collection: &str,
        operation: SecureOperation,
    ) -> Result<()> {
        let state = self.collection_state(collection)?;
        if !matches!(operation, SecureOperation::Read)
            && state
                .meta
                .mutation_policy
                .as_ref()
                .is_some_and(|policy| policy.grants_required)
        {
            return Err(BicDbError::MutationDenied(format!(
                "collection `{collection}` requires an invocation-scoped MutationGrant for {operation:?}"
            )));
        }
        if state.meta.policy.is_some() {
            return Err(BicDbError::Authorization(format!(
                "collection `{collection}` is protected; use db.secure(ctx) for {operation:?}"
            )));
        }
        Ok(())
    }

    pub(crate) fn ensure_existing_unprotected_legacy_access(
        &self,
        collection: &str,
        operation: SecureOperation,
    ) -> Result<()> {
        let Some(state) = self.collections.get(collection).map(|lock| lock.read()) else {
            return Ok(());
        };
        if !matches!(operation, SecureOperation::Read)
            && state
                .meta
                .mutation_policy
                .as_ref()
                .is_some_and(|policy| policy.grants_required)
        {
            return Err(BicDbError::MutationDenied(format!(
                "collection `{collection}` requires an invocation-scoped MutationGrant for {operation:?}"
            )));
        }
        if state.meta.policy.is_some() {
            return Err(BicDbError::Authorization(format!(
                "collection `{collection}` is protected; use db.secure(ctx) for {operation:?}"
            )));
        }
        Ok(())
    }

    pub(crate) fn ensure_no_native_mutation_grant_required(
        &self,
        collection: &str,
        operation: SecureOperation,
    ) -> Result<()> {
        if !matches!(operation, SecureOperation::Read)
            && self
                .collection_state(collection)?
                .meta
                .mutation_policy
                .as_ref()
                .is_some_and(|policy| policy.grants_required)
        {
            return Err(BicDbError::MutationDenied(format!(
                "collection `{collection}` requires an invocation-scoped MutationGrant for {operation:?}"
            )));
        }
        Ok(())
    }

    pub(crate) fn authorize_collection(
        &mut self,
        collection: &str,
        operation: SecureOperation,
        ctx: &SecurityContext,
    ) -> Result<Option<CollectionPolicy>> {
        let Some(policy) = self.collection_state(collection)?.meta.policy.clone() else {
            return Ok(None);
        };
        if let Some(bypass) = ctx.bypass_policy.as_ref() {
            if bypass.reason.trim().is_empty() {
                return Err(BicDbError::Authorization(
                    "admin bypass requires a non-empty reason".to_string(),
                ));
            }
            self.record_security_event(collection, operation, ctx, Some(&bypass.reason))?;
            return Ok(Some(policy));
        }
        if ctx.user_id.trim().is_empty() || ctx.tenant_id.trim().is_empty() {
            return Err(BicDbError::Authorization(format!(
                "{operation:?} on `{collection}` requires user_id and tenant_id"
            )));
        }
        let required_roles = match operation {
            SecureOperation::Read => &policy.read_roles,
            SecureOperation::Write => &policy.write_roles,
            SecureOperation::Delete => &policy.delete_roles,
        };
        if !required_roles.is_empty() && ctx.roles.is_disjoint(required_roles) {
            return Err(BicDbError::Authorization(format!(
                "{operation:?} on `{collection}` requires one of roles {:?}",
                required_roles
            )));
        }
        Ok(Some(policy))
    }

    pub(crate) fn authorize_read_without_audit(
        collection: &str,
        policy: &CollectionPolicy,
        ctx: &SecurityContext,
    ) -> Result<()> {
        if let Some(bypass) = ctx.bypass_policy.as_ref() {
            if bypass.reason.trim().is_empty() {
                return Err(BicDbError::Authorization(
                    "admin bypass requires a non-empty reason".to_string(),
                ));
            }
            return Ok(());
        }
        if ctx.user_id.trim().is_empty() || ctx.tenant_id.trim().is_empty() {
            return Err(BicDbError::Authorization(format!(
                "read on `{collection}` requires user_id and tenant_id"
            )));
        }
        if !policy.read_roles.is_empty() && ctx.roles.is_disjoint(&policy.read_roles) {
            return Err(BicDbError::Authorization(format!(
                "read on `{collection}` requires one of roles {:?}",
                policy.read_roles
            )));
        }
        Ok(())
    }

    pub(crate) fn record_security_event(
        &mut self,
        collection: &str,
        operation: SecureOperation,
        ctx: &SecurityContext,
        bypass_reason: Option<&str>,
    ) -> Result<()> {
        let Some(reason) = bypass_reason else {
            return Ok(());
        };
        self.events.lock().append(Event::new(
            "bicdb.security",
            "PolicyBypass",
            json!({
                "user_id": ctx.user_id,
                "client_id": ctx.client_id,
                "tenant_id": ctx.tenant_id,
                "workspace_id": ctx.workspace_id,
                "roles": ctx.roles,
                "scopes": ctx.scopes,
                "session_id": ctx.session_id,
                "authentication_strength": ctx.authentication_strength.as_str(),
                "collection": collection,
                "operation": format!("{operation:?}").to_ascii_lowercase(),
                "reason": reason,
            }),
        ))?;
        Ok(())
    }

    pub(crate) fn record_visible_to_policy(
        policy: &CollectionPolicy,
        ctx: &SecurityContext,
        record: &Record,
    ) -> Result<bool> {
        if ctx.bypass_policy.is_some() {
            return Ok(true);
        }
        let Some(value) = record.metadata.get(&policy.tenant_field) else {
            return Err(BicDbError::Authorization(format!(
                "record `{}` is missing tenant field `{}`",
                record.id, policy.tenant_field
            )));
        };
        Ok(value.as_str() == Some(ctx.tenant_id.as_str()))
    }

    pub(crate) fn validate_record_tenant(
        policy: &CollectionPolicy,
        ctx: &SecurityContext,
        record: &Record,
    ) -> Result<()> {
        if ctx.bypass_policy.is_some() {
            return Ok(());
        }
        let Some(value) = record.metadata.get(&policy.tenant_field) else {
            return Err(BicDbError::Authorization(format!(
                "write to tenant-owned collection requires `{}`",
                policy.tenant_field
            )));
        };
        if value.as_str() != Some(ctx.tenant_id.as_str()) {
            return Err(BicDbError::Authorization(format!(
                "record tenant field `{}` does not match caller tenant",
                policy.tenant_field
            )));
        }
        Ok(())
    }

    pub(crate) fn event_targets_protected_collection(&self, event: &StoredEvent) -> bool {
        if event.event.stream != RECORD_AUDIT_STREAM {
            return false;
        }
        let Some(collection) = event
            .event
            .payload
            .get("collection")
            .and_then(|value| value.as_str())
        else {
            return false;
        };
        self.collections
            .get(collection)
            .map(|lock| lock.read().meta.policy.is_some())
            .unwrap_or(false)
    }

    /// Whether a record-audit event is outside this node's explicit mesh
    /// schema. Non-record events are unaffected; collection mutations fail
    /// closed unless the target exists and is opted in. The legacy switch is
    /// intentionally explicit and never overrides a collection policy.
    pub(crate) fn event_targets_mesh_ineligible_collection(&self, event: &StoredEvent) -> bool {
        if event.event.stream != RECORD_AUDIT_STREAM {
            return false;
        }
        let Some(collection) = event
            .event
            .payload
            .get("collection")
            .and_then(Value::as_str)
        else {
            return true;
        };
        self.collections
            .get(collection)
            .map(|lock| {
                let state = lock.read();
                let meta = &state.meta;
                meta.policy.is_some()
                    || (!self.config.allow_unsafe_legacy_mesh_collections
                        && !meta.mesh_sync_enabled)
            })
            .unwrap_or(!self.config.allow_unsafe_legacy_mesh_collections)
    }

    /// Shared access to a collection's state. Takes the per-collection read lock;
    /// the returned guard derefs to `&CollectionState`. Safe to call concurrently
    /// while the durable commit step holds the write lock on a *different*
    /// collection (or briefly blocks on the same one).
    pub(crate) fn collection_state(
        &self,
        collection: &str,
    ) -> Result<RwLockReadGuard<'_, CollectionState>> {
        self.collections
            .get(collection)
            .map(|lock| lock.read())
            .ok_or_else(|| BicDbError::CollectionNotFound(collection.to_string()))
    }

    /// Exclusive access via `&mut self`: the caller already holds the exclusive
    /// outer `RwLock<BicDb>`, so the per-collection lock is uncontended and we use
    /// `get_mut` (no runtime locking). Used by DDL/admin and the `&mut self`
    /// commit path.
    pub(crate) fn collection_state_mut(
        &mut self,
        collection: &str,
    ) -> Result<&mut CollectionState> {
        self.collections
            .get_mut(collection)
            .map(|lock| lock.get_mut())
            .ok_or_else(|| BicDbError::CollectionNotFound(collection.to_string()))
    }

    /// Shared-path exclusive access to a single collection's state: takes the
    /// per-collection write lock through a shared `&self`. Used by the concurrent
    /// commit apply once the outer lock is downgraded to shared; serialized
    /// against other committers by the global `commit_lock`.
    pub(crate) fn collection_state_write(
        &self,
        collection: &str,
    ) -> Result<RwLockWriteGuard<'_, CollectionState>> {
        self.collections
            .get(collection)
            .map(|lock| lock.write())
            .ok_or_else(|| BicDbError::CollectionNotFound(collection.to_string()))
    }

    pub(crate) fn segment_path(&self, collection: &str) -> PathBuf {
        segment_path_for(&self.path, collection)
    }

    /// Replays the audit stream and converges each record onto one winner.
    ///
    /// Ordering is causal-first: mutations dominated by a later write that
    /// had provably seen them (write context covers their origin sequence)
    /// are excluded outright — device clocks cannot override causality. Only
    /// the genuinely concurrent frontier falls through to the deterministic
    /// `AuditOrder` tiebreak, and a frontier wider than one is counted (and
    /// surfaced via [`BicDb::record_conflict`]) as unresolved concurrency:
    /// the projection is deterministic, not a claim about which write is
    /// semantically correct. Any subsequent write whose context covers the
    /// whole frontier resolves the conflict on every replica.
    /// Freezes every locally-authored event's origin position into its sync
    /// metadata (full rewrite when any local event exists) and returns the
    /// pre-rewrite log end. Callers performing an offset-changing rewrite
    /// advance `origin_position_base` by the returned end afterwards, so
    /// mesh vectors survive compaction without resyncs.
    pub(crate) fn freeze_local_event_positions(
        &self,
        events: &mut crate::event::EventStream,
    ) -> Result<u64> {
        let node_id = self.sync_state.node_id.clone();
        let base = self.sync_state.origin_position_base;
        let mut old_end: u64 = 0;
        let mut any_local = false;
        let mut stamped = Vec::with_capacity(events.stored_events().len());
        for stored in events.stored_events() {
            old_end = old_end.max(stored.offset.saturating_add(1));
            if sync_mesh::envelope_from_metadata(&stored.event)?.is_some() {
                stamped.push(stored.event.clone());
            } else {
                any_local = true;
                let envelope = sync_mesh::envelope_for_event_based(&node_id, base, stored)?;
                stamped.push(event_with_sync_metadata(stored.event.clone(), &envelope)?);
            }
        }
        if any_local {
            events.rewrite_events(stamped)?;
        }
        Ok(old_end)
    }

    pub(crate) fn reconcile_record_audit_events(&mut self) -> Result<AuditMergeReport> {
        // Pass 1: which records did events at or above the reconciliation
        // high-water mark touch? Only their histories need re-resolution;
        // everything below the mark is already folded into record state.
        let mut touched = BTreeSet::<(String, String)>::new();
        let mut log_end = self.sync_state.last_reconciled_offset;
        {
            let events = self.events.lock();
            for stored in events.events_iter() {
                log_end = log_end.max(stored.offset.saturating_add(1));
                if stored.offset < self.sync_state.last_reconciled_offset {
                    continue;
                }
                if stored.event.stream != RECORD_AUDIT_STREAM {
                    continue;
                }
                if !matches!(
                    stored.event.event_type.as_str(),
                    "RecordCreated" | "RecordUpdated" | "RecordDeleted"
                ) {
                    continue;
                }
                let (Some(collection), Some(record_id)) = (
                    stored
                        .event
                        .payload
                        .get("collection")
                        .and_then(Value::as_str),
                    stored
                        .event
                        .payload
                        .get("record_id")
                        .and_then(Value::as_str),
                ) else {
                    continue;
                };
                touched.insert((collection.to_string(), record_id.to_string()));
            }
        }
        if touched.is_empty() {
            self.sync_state.last_reconciled_offset = log_end;
            return Ok(AuditMergeReport::default());
        }

        // Pass 2: stream the full log again, materializing mutation history
        // only for the touched records — memory stays bounded by what
        // actually changed, not by total history.
        let mut mutations_by_record = BTreeMap::<(String, String), Vec<AuditMutation>>::new();
        {
            let events = self.events.lock();
            for stored in events.events_iter() {
                if stored.event.stream != RECORD_AUDIT_STREAM {
                    continue;
                }
                let (Some(collection), Some(record_id)) = (
                    stored
                        .event
                        .payload
                        .get("collection")
                        .and_then(Value::as_str),
                    stored
                        .event
                        .payload
                        .get("record_id")
                        .and_then(Value::as_str),
                ) else {
                    continue;
                };
                if !touched.contains(&(collection.to_string(), record_id.to_string())) {
                    continue;
                }
                let Some(mutation) = AuditMutation::from_event_based(
                    &self.sync_state.node_id,
                    self.sync_state.origin_position_base,
                    stored,
                )?
                else {
                    continue;
                };
                let key = (mutation.collection.clone(), mutation.record_id.clone());
                mutations_by_record.entry(key).or_default().push(mutation);
            }
        }
        self.sync_state.last_reconciled_offset = log_end;

        let mut records_merged = 0;
        let mut conflicts_resolved = 0;
        for mutations in mutations_by_record.values() {
            let frontier = audit_concurrent_frontier(mutations);
            // Peer-controlled wall-clock observations are diagnostics only.
            // A pinned but malicious peer controls its timing stamps, so they
            // must never suppress a conflict or select its winner.
            if frontier.len() > 1 {
                conflicts_resolved += 1;
            }
            let Some(mutation) = frontier.into_iter().max_by(|a, b| a.order.cmp(&b.order)) else {
                continue;
            };
            match &mutation.action {
                AuditAction::Upsert(record) => {
                    if self
                        .get(&mutation.collection, &mutation.record_id)
                        .ok()
                        .flatten()
                        == Some(record.clone().into())
                    {
                        continue;
                    }
                    if self.apply_replicated_upsert(
                        &mutation.collection,
                        mutation.collection_mode.clone(),
                        record.clone(),
                    )? {
                        records_merged += 1;
                    }
                }
                AuditAction::Delete => {
                    if self
                        .get(&mutation.collection, &mutation.record_id)
                        .ok()
                        .flatten()
                        .is_none()
                    {
                        continue;
                    }
                    if self.apply_replicated_delete(&mutation.collection, &mutation.record_id)? {
                        records_merged += 1;
                    }
                }
            }
        }

        Ok(AuditMergeReport {
            records_merged,
            conflicts_resolved,
        })
    }

    pub(crate) fn apply_replicated_upsert(
        &mut self,
        collection: &str,
        mode: CollectionMode,
        record: Record,
    ) -> Result<bool> {
        if !self.collections.contains_key(collection) {
            if !self.config.allow_unsafe_legacy_mesh_collections {
                return Err(BicDbError::Authorization(format!(
                    "mesh collection `{collection}` must be provisioned and explicitly enabled"
                )));
            }
            self.create_collection_with_mode(collection, mode.clone())?;
        }
        {
            let state = self.collection_state(collection)?;
            if state.meta.policy.is_some()
                || (!self.config.allow_unsafe_legacy_mesh_collections
                    && !state.meta.mesh_sync_enabled)
            {
                return Err(BicDbError::Authorization(format!(
                    "mesh collection `{collection}` is protected or not enabled"
                )));
            }
            if state.meta.mode != mode {
                return Err(BicDbError::SyncBundle(format!(
                    "mesh collection `{collection}` mode does not match the provisioned schema"
                )));
            }
        }
        let mut prepared = self.prepare_mutations(collection, vec![record])?;
        let Some(mutation) = prepared.pop() else {
            return Ok(false);
        };
        let mut index_mutations =
            self.prepared_index_mutations(collection, std::slice::from_ref(&mutation))?;
        self.validate_collection_index_mutations(collection, &index_mutations)?;
        // Durable record storage for `server_paged`. Replication and offline sync
        // reach records through this path rather than through a transaction, so
        // without this the imported row would be invisible in paged mode.
        // Applied BEFORE the resident insert below, so the eviction stub's
        // fetch already resolves when the stub becomes readable. In paged mode
        // the segment append is skipped entirely — the page store is the
        // durable home for rows, and segments carry none.
        let replicated_index_defs = self.paged_durable_index_defs(collection)?;
        let paged_xid = self.in_paged_transaction(|paged, xid| {
            let pre_snapshot = bicdb_page::Snapshot {
                xid: 0,
                xmax: xid,
                in_flight: empty_in_flight(),
            };
            let previous = if replicated_index_defs.is_empty() {
                None
            } else {
                paged.get(&pre_snapshot, collection, &mutation.record.id)?
            };
            let locator = paged.put(xid, collection, &mutation.record)?;
            apply_paged_index_upsert(
                paged,
                xid,
                collection,
                &replicated_index_defs,
                previous.as_ref(),
                &mutation.record,
                Some((locator, xid)),
            )?;
            Ok(())
        })?;
        let offset = if paged_xid.is_none() {
            let payload = serde_json::to_vec(&RecordFrame::Upsert {
                record: mutation.record.clone(),
            })?;
            let offsets = storage::append_frames(
                &self.segment_path(collection),
                FrameKind::Record,
                &[payload],
                self.config.fsync,
                &self.config.compression,
                &self.encryption,
            )?;
            offsets[0]
        } else {
            0
        };
        let stub_fetch = paged_xid.and_then(|xid| self.paged_stub_fetch(collection, xid));
        let state = self.collection_state_mut(collection)?;
        let record_id = mutation.record.id.clone();
        let old_record_had_vector = index_mutations
            .first()
            .is_some_and(|mutation| mutation.old_record.has_vector());
        {
            let stored = match &stub_fetch {
                Some(fetch) => Arc::new(StoredRecord::evicted_stub(
                    &mutation.record,
                    crate::record::EvictedPayload {
                        fetch: Arc::clone(fetch),
                        pk: Arc::from(record_id.as_str()),
                    },
                )),
                None => Arc::new(StoredRecord::from_record(&mutation.record)?),
            };
            let shard = state.shard_mut(&record_id);
            let rowid = shard.rowid_or_alloc(&record_id);
            shard.records.insert(
                rowid,
                RecordEntry {
                    offset,
                    record: stored,
                },
            );
        }
        if old_record_had_vector {
            state.rebuild_vector_store();
        } else {
            let pushed = state
                .shard_mut(&record_id)
                .get_record(&record_id)
                .map(|entry| entry.record.clone());
            if let Some(record) = pushed {
                state.vector_store.get_mut().push_record(&record);
            }
        }
        state.generation.fetch_add(1, AtomicOrdering::Relaxed);
        self.apply_collection_index_mutations(collection, &mut index_mutations)?;
        self.refresh_hnsw_after_upsert(collection, true)?;
        Ok(true)
    }

    pub(crate) fn apply_replicated_delete(
        &mut self,
        collection: &str,
        record_id: &str,
    ) -> Result<bool> {
        if !self.collections.contains_key(collection) {
            return if self.config.allow_unsafe_legacy_mesh_collections {
                Ok(false)
            } else {
                Err(BicDbError::Authorization(format!(
                    "mesh collection `{collection}` must be provisioned and explicitly enabled"
                )))
            };
        }
        {
            let state = self.collection_state(collection)?;
            if state.meta.policy.is_some()
                || (!self.config.allow_unsafe_legacy_mesh_collections
                    && !state.meta.mesh_sync_enabled)
            {
                return Err(BicDbError::Authorization(format!(
                    "mesh collection `{collection}` is protected or not enabled"
                )));
            }
        }
        let mut index_mutations = self
            .delete_index_mutation(collection, record_id)?
            .into_iter()
            .collect::<Vec<_>>();
        // Capture the deleted record's locator before it is removed below.
        self.fill_index_mutation_rowids(collection, &mut index_mutations)?;
        self.validate_collection_index_mutations(collection, &index_mutations)?;
        // Durable record storage for `server_paged` — see the matching comment in
        // `apply_replicated_upsert`. In paged mode this, not the shard below, is
        // what decides whether the record was really there to delete, and no
        // segment tombstone is written: segments carry no rows in that mode.
        let mut removed_from_paged = false;
        let replicated_index_defs = self.paged_durable_index_defs(collection)?;
        let paged_xid = self.in_paged_transaction(|paged, xid| {
            let pre_snapshot = bicdb_page::Snapshot {
                xid: 0,
                xmax: xid,
                in_flight: empty_in_flight(),
            };
            let previous = if replicated_index_defs.is_empty() {
                None
            } else {
                paged.get(&pre_snapshot, collection, record_id)?
            };
            removed_from_paged = paged.delete(xid, collection, record_id)?;
            if let Some(previous) = &previous {
                apply_paged_index_delete(
                    paged,
                    xid,
                    collection,
                    &replicated_index_defs,
                    previous,
                    record_id,
                )?;
            }
            Ok(())
        })?;
        if paged_xid.is_none() {
            let payload = serde_json::to_vec(&RecordFrame::Delete {
                id: record_id.to_string(),
            })?;
            storage::append_frames(
                &self.segment_path(collection),
                FrameKind::Record,
                &[payload],
                self.config.fsync,
                &self.config.compression,
                &self.encryption,
            )?;
        }
        let state = self.collection_state_mut(collection)?;
        let removed = {
            let shard = state.shard_mut(record_id);
            match shard.rowid_of(record_id) {
                Some(rowid) => {
                    let removed = shard.records.remove(&rowid);
                    // This replicated path keeps no version chain, so the key is gone
                    // immediately; drop its registry entry.
                    shard.forget_if_empty(record_id, rowid);
                    removed
                }
                None => None,
            }
        };
        if removed
            .as_ref()
            .is_some_and(|entry| entry.record.vector.is_some())
        {
            state.rebuild_vector_store();
        }
        if removed.is_some() {
            state.generation.fetch_add(1, AtomicOrdering::Relaxed);
        }
        self.apply_collection_index_mutations(collection, &mut index_mutations)?;
        self.tombstone_hnsw_record(collection, record_id)?;
        Ok(removed.is_some() || removed_from_paged)
    }

    pub(crate) fn write_attachment_unchecked(
        &mut self,
        collection: &str,
        id: &str,
        field: &str,
        media_type: Option<&str>,
        reader: impl std::io::Read,
    ) -> Result<LargeValueRef> {
        if field.trim().is_empty() {
            return Err(BicDbError::LargeValue(
                "attachment metadata field must not be empty".to_string(),
            ));
        }
        let mut record = self
            .get_unchecked(collection, id)?
            .ok_or_else(|| {
                BicDbError::LargeValue(format!(
                    "attachment target record `{id}` in `{collection}` does not exist"
                ))
            })?
            .as_ref()
            .clone();
        let descriptor = large_value::write_from_reader(
            &self.path,
            reader,
            &self.encryption,
            self.config.fsync,
            media_type.map(str::to_string),
        )?;
        let Some(metadata) = record.metadata.as_object_mut() else {
            return Err(BicDbError::LargeValue(format!(
                "attachment target record `{id}` metadata is not an object"
            )));
        };
        metadata.insert(field.to_string(), serde_json::to_value(&descriptor)?);
        let mut tx = self.begin_transaction()?;
        tx.write_upserts_unchecked(collection, vec![record])?;
        tx.commit()?;
        Ok(descriptor)
    }

    pub(crate) fn open_attachment_reader_for_record(
        &self,
        record: &Record,
        field: &str,
    ) -> Result<Option<LargeValueReader>> {
        let Some(value) = record.metadata.get(field) else {
            return Ok(None);
        };
        let descriptor = LargeValueRef::from_value(value)?;
        large_value::open_reader(&self.path, &descriptor, &self.encryption).map(Some)
    }
}
