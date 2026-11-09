//! Checked current-row capabilities for signed READ COMMITTED arbitration.
use super::*;

/// A row reserved by one live transaction. Raw current data and the checked
/// version stay private; reads project through that transaction's native actor
/// policy, and writes recheck the capability before checking their actual grant.
pub struct ReadCommittedRowReservation {
    transaction: TransactionId,
    health: Arc<bicdb_page::DurabilityHealth>,
    collection_incarnation: Arc<()>,
    collection: String,
    id: String,
    version: Option<u64>,
    lock_epoch: u64,
    pending_write: Option<usize>,
    raw: Option<Arc<Record>>,
}
impl std::fmt::Debug for ReadCommittedRowReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadCommittedRowReservation")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
pub(super) struct RowReservationEpochs {
    next: u64,
    rows: FxHashMap<String, FxHashMap<String, u64>>,
}

impl Transaction {
    pub(super) fn invalidate_row_reservations(&self, released: &[RecordLockKey]) {
        if !self.row_reservations_active.load(AtomicOrdering::Acquire) {
            return;
        }
        let mut epochs = self.row_reservation_epochs.lock();
        for (collection, id) in released {
            if let Some(rows) = epochs.rows.get_mut(collection) {
                rows.remove(id);
                if rows.is_empty() {
                    epochs.rows.remove(collection);
                }
            }
        }
    }

    pub(super) fn invalidate_row_reservation_writes(&self, writes: &[TxWrite]) {
        if !self.row_reservations_active.load(AtomicOrdering::Acquire) {
            return;
        }
        let mut epochs = self.row_reservation_epochs.lock();
        for write in writes {
            if let Some(rows) = epochs.rows.get_mut(&write.collection) {
                rows.remove(&write.record_id);
                if rows.is_empty() {
                    epochs.rows.remove(&write.collection);
                }
            }
        }
    }

    fn register_row_reservation(&self, collection: &str, id: &str) -> Result<u64> {
        let mut epochs = self.row_reservation_epochs.lock();
        if let Some(epoch) = epochs
            .rows
            .get(collection)
            .and_then(|rows| rows.get(id))
            .copied()
        {
            return Ok(epoch);
        }
        let epoch = epochs.next.checked_add(1).ok_or_else(|| {
            BicDbError::TransactionConflict("row reservation epochs exhausted".into())
        })?;
        epochs.next = epoch;
        epochs
            .rows
            .entry(collection.to_owned())
            .or_default()
            .insert(id.to_owned(), epoch);
        self.row_reservations_active
            .store(true, AtomicOrdering::Release);
        Ok(epoch)
    }

    /// Reserve a current row for an application transaction's authorized read.
    /// A supplied cancellation token allows waiting without an outer exclusive
    /// DB guard; `None` refuses a busy row immediately. This consumes no mutation
    /// grant or audit budget: the eventual actual INSERT/UPDATE checks those.
    pub fn reserve_authorized_read_committed_row(
        &mut self,
        collection: &str,
        id: &str,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ReadCommittedRowReservation> {
        self.db_ref().check_recovery_state()?;
        let result = self.reserve_authorized_read_committed_row_inner(collection, id, cancellation);
        self.fenced(result)
    }

    fn reserve_authorized_read_committed_row_inner(
        &mut self,
        collection: &str,
        id: &str,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ReadCommittedRowReservation> {
        let db = self.db_ref();
        db.check_recovery_state()?;
        #[cfg(debug_assertions)]
        bicdb_page::test_faults::io_hook_point("read_committed.reservation.begin", &db.path);
        if self.state != TxState::Pending {
            return Err(BicDbError::TransactionNotPending);
        }
        if self.isolation != TransactionIsolation::ReadCommitted {
            return Err(BicDbError::TransactionConflict(
                "current row reservations require READ COMMITTED".into(),
            ));
        }
        // Check collection read authority even when its target is absent.
        self.project_reserved_authorized_record(collection, None)?;
        let actor = self
            .mutation_actor
            .as_ref()
            .expect("read authority checked actor");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let remaining = actor
            .deadline_unix_ms
            .checked_sub(now)
            .filter(|value| *value > 0)
            .ok_or(BicDbError::QueryTimedOut)?;
        if let Some(parent) = cancellation {
            let deadline = std::time::Instant::now()
                .checked_add(Duration::from_millis(remaining as u64))
                .ok_or(BicDbError::QueryTimedOut)?;
            let token = parent.child_with_deadline(deadline);
            db.lock_tx_record_cancellable(self.id, collection, id, &self.locked_keys, &token)?;
        } else {
            db.lock_tx_record_with_attempts(
                self.id,
                collection,
                id,
                0,
                false,
                false,
                &self.locked_keys,
            )?;
        }
        let epoch = self.register_row_reservation(collection, id)?;
        let pending_write = self
            .write_index
            .get(collection)
            .and_then(|rows| rows.get(id))
            .copied();
        let raw = if pending_write.is_some() {
            self.get_visible_unchecked(collection, id)?
        } else {
            db.get_unchecked(collection, id)?
        };
        let token = ReadCommittedRowReservation {
            transaction: self.id,
            health: Arc::clone(&db.durability),
            collection_incarnation: Arc::clone(&db.collection_state(collection)?.incarnation),
            collection: collection.to_owned(),
            id: id.to_owned(),
            version: Self::latest_committed_record_sequence(db, collection, id)?,
            lock_epoch: epoch,
            pending_write,
            raw,
        };
        // Projection validates present-row policy before returning a capability,
        // but never publishes its private unprojected preimage.
        self.project_reserved_authorized_record(collection, token.raw.clone())?;
        db.check_recovery_state()?;
        Ok(token)
    }

    fn validate_row_reservation(&self, token: &ReadCommittedRowReservation) -> Result<()> {
        let db = self.db_ref();
        db.check_recovery_state()?;
        #[cfg(debug_assertions)]
        bicdb_page::test_faults::io_hook_point("read_committed.reservation.validate", &db.path);
        if self.state != TxState::Pending {
            return Err(BicDbError::TransactionNotPending);
        }
        if self.id != token.transaction
            || !Arc::ptr_eq(&db.durability, &token.health)
            || !Arc::ptr_eq(
                &db.collection_state(&token.collection)?.incarnation,
                &token.collection_incarnation,
            )
            || self
                .row_reservation_epochs
                .lock()
                .rows
                .get(&token.collection)
                .and_then(|rows| rows.get(&token.id))
                .copied()
                != Some(token.lock_epoch)
            || !self.holds_record_lock(&token.collection, &token.id)
            || self
                .write_index
                .get(&token.collection)
                .and_then(|rows| rows.get(&token.id))
                .copied()
                != token.pending_write
            || Self::latest_committed_record_sequence(db, &token.collection, &token.id)?
                != token.version
        {
            return Err(BicDbError::TransactionConflict(
                "current row reservation is no longer valid".into(),
            ));
        }
        let current = if token.pending_write.is_some() {
            self.get_visible_unchecked(&token.collection, &token.id)?
        } else {
            db.get_unchecked(&token.collection, &token.id)?
        };
        if current.as_deref() != token.raw.as_deref() {
            return Err(BicDbError::TransactionConflict(
                "reserved row contents changed".into(),
            ));
        }
        db.check_recovery_state()
    }

    fn project_reserved_authorized_record(
        &self,
        collection: &str,
        raw: Option<Arc<Record>>,
    ) -> Result<Option<Arc<Record>>> {
        let db = self.db_ref();
        db.check_recovery_state()?;
        let actor = self.mutation_actor.as_ref().ok_or_else(|| {
            BicDbError::Authorization(
                "authorized row reservations require an application transaction".into(),
            )
        })?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        if actor.deadline_unix_ms <= now {
            return Err(BicDbError::QueryTimedOut);
        }
        let state = db.collection_state(collection)?;
        let Some(policy) = &state.meta.policy else {
            return Ok(raw);
        };
        let context = mutation_security_context(actor)?;
        BicDb::authorize_read_without_audit(collection, policy, &context)?;
        let Some(raw) = raw else { return Ok(None) };
        if !BicDb::record_visible_to_policy(policy, &context, &raw)? {
            return Ok(None);
        }
        let record = protected_data::project_record(
            &db.sync_state.node_id.to_string(),
            &db.path,
            collection,
            policy,
            &context.roles,
            &raw,
            false,
        )?;
        db.check_recovery_state()?;
        Ok(Some(Arc::new(record)))
    }

    /// Read a reserved row through current native policy. A fence, replaced
    /// backing, released/reacquired row lock, foreign transaction or changed
    /// pending/current image is an error. Unrelated lock releases stay valid.
    pub fn read_authorized_reserved_row(
        &self,
        token: &ReadCommittedRowReservation,
    ) -> Result<Option<Arc<Record>>> {
        self.db_ref().check_recovery_state()?;
        let result = (|| {
            self.validate_row_reservation(token)?;
            self.project_reserved_authorized_record(&token.collection, token.raw.clone())
        })();
        self.fenced(result)
    }

    /// Apply the actual upsert action using a core-checked current preimage.
    /// The capability is consumed; grant columns, version, immutability, tenant,
    /// row/statement budgets and audit are checked once for that actual action.
    pub fn upsert_reserved_with_grant(
        &mut self,
        grant: MutationGrantId,
        token: ReadCommittedRowReservation,
        record: Record,
    ) -> Result<()> {
        self.db_ref().check_recovery_state()?;
        let result = self.upsert_reserved_with_grant_inner(grant, token, record);
        self.fenced(result)
    }

    fn upsert_reserved_with_grant_inner(
        &mut self,
        grant: MutationGrantId,
        token: ReadCommittedRowReservation,
        mut record: Record,
    ) -> Result<()> {
        self.validate_row_reservation(&token)?;
        if record.id != token.id {
            return Err(BicDbError::TransactionConflict(
                "reserved row identity differs from mutation".into(),
            ));
        }
        let projected =
            self.project_reserved_authorized_record(&token.collection, token.raw.clone())?;
        if token.raw.is_some() && projected.is_none() {
            return Err(BicDbError::Authorization(
                "reserved row is not visible to the mutation actor".into(),
            ));
        }
        let operation = if token.raw.is_some() {
            MutationOperation::Update
        } else {
            MutationOperation::Insert
        };
        if let (Some(raw), Some(projected)) = (&token.raw, &projected) {
            // Preserve fields hidden/masked by read policy. Only changes from
            // the authorized projection are applied to the raw preimage; actual
            // grant validation below still authorizes every resulting change.
            let mut merged = raw.as_ref().clone();
            let old = projected.metadata.as_object();
            let new = record.metadata.as_object();
            let fields: BTreeSet<_> = old
                .into_iter()
                .flatten()
                .map(|(field, _)| field)
                .chain(new.into_iter().flatten().map(|(field, _)| field))
                .collect();
            let metadata = merged.metadata.as_object_mut().ok_or_else(|| {
                BicDbError::CommitValidation("reserved row metadata is not an object".into())
            })?;
            for field in fields {
                if old.and_then(|values| values.get(field))
                    != new.and_then(|values| values.get(field))
                {
                    match new.and_then(|values| values.get(field)) {
                        Some(value) => {
                            metadata.insert(field.clone(), value.clone());
                        }
                        None => {
                            metadata.remove(field);
                        }
                    }
                }
            }
            if projected.vector != record.vector {
                merged.vector = record.vector.clone();
            }
            if projected.geometry != record.geometry {
                merged.geometry = record.geometry.clone();
            }
            if projected.timestamp != record.timestamp {
                merged.timestamp = record.timestamp;
            }
            if projected.payload != record.payload {
                merged.payload = record.payload.clone();
            }
            record = merged;
        }
        let before = token.raw.clone();
        let audit = self.authorize_mutation_with_preimage(
            grant,
            &token.collection,
            operation,
            &[record.clone()],
            Some((&token.id, before.clone())),
        )?;
        let result = self.write_upserts_with_reservation_preimages(
            &token.collection,
            [(
                record,
                token.version.unwrap_or(0),
                Some(OldImage::from_record(before)),
            )],
            false,
        );
        self.fenced(result)?;
        self.enqueue_mutation_audit(audit);
        Ok(())
    }
}
