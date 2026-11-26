//! Two-phase commit: PREPARE TRANSACTION, COMMIT PREPARED, ROLLBACK PREPARED.
//!
//! See `docs/postgresql-two-phase-commit.md`. In short:
//!
//! * PREPARE runs COMMIT's validation (delta repair, write/write conflicts,
//!   unique keys against committed data, the SSI decision), reserves every
//!   unique key it introduces exclusively, and then commits one row of
//!   [`PREPARED_XACTS_COLLECTION`] through the ordinary (durable) commit
//!   path. That row holds the write set and every lock and reservation key.
//!   The transaction's locks, reservations, catalog observations and SSI
//!   participant then move, unreleased, into the in-memory [`Registry`].
//! * COMMIT PREPARED resumes a transaction under the same id from the entry,
//!   adds the deletion of the prepare row and commits it normally: the data
//!   and the end of the prepared state are one atomic commit.
//! * ROLLBACK PREPARED durably deletes the row, then releases everything.
//! * Opening a store restores every row before any work is accepted.
use super::*;

/// Durable prepare records, one per gid. Never mesh-synced, audited or
/// reachable from SQL; it reaches standbys through ordinary replication.
pub const PREPARED_XACTS_COLLECTION: &str = "__bicdb_prepared_xacts";

/// PostgreSQL's GIDSIZE (200) includes the terminating NUL.
const GID_MAX_BYTES: usize = 199;
const RECORD_FORMAT: u32 = 1;

/// One prepared transaction, as `pg_prepared_xacts` reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedTransaction {
    pub gid: String,
    pub transaction: u64,
    pub prepared_at_micros: i64,
    pub owner: String,
    pub database: String,
}

/// Who asks to finish a prepared transaction (PostgreSQL's `GetUserId()`
/// and `MyDatabaseId`).
#[derive(Clone, Copy, Debug)]
pub struct PreparedTransactionRequester<'a> {
    pub user: &'a str,
    pub superuser: bool,
    pub database: &'a str,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedWrite {
    collection: String,
    op: TxWriteOp,
    record_id: String,
    record: Option<Arc<StoredRecord>>,
    timestamp: i64,
    /// The snapshot the write was validated against at PREPARE. The row is
    /// locked since, so its latest committed version cannot pass it.
    snapshot: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedPrepare {
    format: u32,
    gid: String,
    owner: String,
    database: String,
    prepared_at_micros: i64,
    transaction: u64,
    #[serde(default)]
    repeatable_read: bool,
    #[serde(default)]
    serializable: bool,
    writes: Vec<PersistedWrite>,
    locks: Vec<RecordLockKey>,
}

#[derive(Debug)]
struct Entry {
    tx_id: TransactionId,
    info: PreparedTransaction,
    isolation: TransactionIsolation,
    writes: Vec<PersistedWrite>,
    locks: Vec<RecordLockKey>,
    bindings: Arc<catalog_admission::Bindings>,
    serializable: Option<sql_serializable::Registration>,
    busy: bool,
}

#[derive(Debug, Default)]
struct State {
    entries: BTreeMap<String, Entry>,
    /// Gids whose PREPARE is writing its record.
    preparing: BTreeSet<String>,
}

#[derive(Debug, Default)]
pub(super) struct Registry {
    max: AtomicUsize,
    state: Mutex<State>,
}

fn error(sqlstate: &'static str, message: impl Into<String>, hint: Option<String>) -> BicDbError {
    BicDbError::PreparedTransaction {
        sqlstate,
        message: message.into(),
        hint,
    }
}

fn unsupported(what: &str) -> BicDbError {
    error(
        "0A000",
        format!("cannot PREPARE a transaction that {what}"),
        None,
    )
}

fn record_id(gid: &str) -> String {
    format!("g{}", hex::encode(gid.as_bytes()))
}

fn not_found(gid: &str) -> BicDbError {
    error(
        "42704",
        format!("prepared transaction with identifier \"{gid}\" does not exist"),
        None,
    )
}

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_micros() as i64)
        .unwrap_or_default()
}

/// The server's `max_prepared_transactions`, as `SHOW` and
/// `current_setting` report it. A postmaster-level setting: one value per
/// process, the last one a store was configured with.
static CONFIGURED_MAX: AtomicUsize = AtomicUsize::new(0);

pub fn configured_max_prepared_transactions() -> usize {
    CONFIGURED_MAX.load(AtomicOrdering::Acquire)
}

/// The deliberately-excluded internal collection: neither audited nor synced.
pub(crate) fn is_prepared_xacts_collection(collection: &str) -> bool {
    collection == PREPARED_XACTS_COLLECTION
}

impl BicDb {
    /// `max_prepared_transactions`. 0 (the default, as in PostgreSQL)
    /// disables PREPARE TRANSACTION.
    pub fn max_prepared_transactions(&self) -> usize {
        self.prepared_xacts.max.load(AtomicOrdering::Acquire)
    }

    /// Set `max_prepared_transactions` (server start). Creates the internal
    /// record collection on a writable primary when enabling.
    pub fn set_max_prepared_transactions(&mut self, max: usize) -> Result<()> {
        if max > 0
            && !self.collections.contains_key(PREPARED_XACTS_COLLECTION)
            && !self.is_read_only_standby()
            && self.ensure_writable("probe").is_ok()
        {
            self.create_collection(PREPARED_XACTS_COLLECTION)?;
        }
        self.prepared_xacts.max.store(max, AtomicOrdering::Release);
        CONFIGURED_MAX.store(max, AtomicOrdering::Release);
        Ok(())
    }

    /// Every prepared transaction, from the durable records (so a standby
    /// lists its primary's prepared transactions too).
    pub fn prepared_transactions(&self) -> Result<Vec<PreparedTransaction>> {
        if !self.collections.contains_key(PREPARED_XACTS_COLLECTION) {
            return Ok(Vec::new());
        }
        let live = {
            let state = self.prepared_xacts.state.lock();
            state
                .entries
                .iter()
                .map(|(gid, entry)| (gid.clone(), entry.tx_id.0))
                .collect::<BTreeMap<_, _>>()
        };
        let mut out = Vec::new();
        for persisted in self.read_prepare_records()? {
            out.push(PreparedTransaction {
                transaction: live
                    .get(&persisted.gid)
                    .copied()
                    .unwrap_or(persisted.transaction),
                gid: persisted.gid,
                prepared_at_micros: persisted.prepared_at_micros,
                owner: persisted.owner,
                database: persisted.database,
            });
        }
        out.sort_by(|a, b| a.gid.cmp(&b.gid));
        Ok(out)
    }

    fn read_prepare_records(&self) -> Result<Vec<PersistedPrepare>> {
        let reader = self.begin_transaction()?;
        let rows = reader.scan_collection(PREPARED_XACTS_COLLECTION)?;
        drop(reader);
        rows.into_iter()
            .map(|record| {
                let persisted: PersistedPrepare = serde_json::from_value(record.metadata)?;
                if persisted.format != RECORD_FORMAT {
                    return Err(BicDbError::FormatCompatibility(format!(
                        "unsupported prepared transaction record format {}",
                        persisted.format
                    )));
                }
                Ok(persisted)
            })
            .collect()
    }

    /// Restrictions PREPARE enforces before touching any state.
    fn check_preparable(&self, tx: &Transaction) -> Result<()> {
        if !std::ptr::eq(tx.db.as_ptr(), self) || !Arc::ptr_eq(&tx.tx_states, &self.tx_states) {
            return Err(BicDbError::Unsupported(
                "transaction belongs to a different database instance".into(),
            ));
        }
        if tx.state != TxState::Pending || tx.prepared_detached {
            return Err(BicDbError::TransactionNotPending);
        }
        if tx.has_deferred_hooks() {
            return Err(BicDbError::TransactionConflict(
                "pending SQL constraint effects must be validated before PREPARE".into(),
            ));
        }
        if self.commit_admission.read().is_some()
            || self.config.require_commit_admission
            || self
                .range_image_native_birth_active
                .load(AtomicOrdering::Acquire)
            || self.actual_native_writer_enforcement_version()? != 0
            || tx.range_mutation_claim.is_some()
            || tx.commit_admission_ticket.is_some()
            || tx.bypass_commit_admission
        {
            return Err(unsupported(
                "runs under a distributed commit authority (two-phase commit is not supported there)",
            ));
        }
        if tx.mutation_actor.is_some()
            || !tx.mutation_grants.is_empty()
            || !tx.commit_validators.is_empty()
            || !tx.mutation_invariant_bundles.is_empty()
            || tx.validate_existing_application_invariants
            || !tx.native_catalog_allocations.is_empty()
            || tx.catalog_actor_scope.is_some()
            || tx.imported_projection
            || !tx.imported_record_audit_events.is_empty()
            || tx.native_image_row_grant.is_some()
        {
            return Err(unsupported("carries application-runtime authority"));
        }
        if !tx.pending_broker_publishes.lock().is_empty() {
            return Err(unsupported("publishes broker messages on commit"));
        }
        if tx.isolation == TransactionIsolation::Serializable {
            return Err(unsupported(
                "uses native relation-level serializable isolation",
            ));
        }
        for write in &tx.writes {
            if is_prepared_xacts_collection(&write.collection)
                || is_flashback_history_collection(&write.collection)
            {
                return Err(unsupported("writes an internal collection"));
            }
            let state = self.collection_state(&write.collection)?;
            if state.meta.policy.is_some() {
                return Err(unsupported("writes a protected collection"));
            }
        }
        let written = tx
            .writes
            .iter()
            .map(|write| write.collection.as_str())
            .collect::<BTreeSet<_>>();
        for index in self.indexes.values() {
            let state = index.read();
            if state.definition.exclusion.is_some()
                && written.contains(state.definition.collection.as_str())
            {
                return Err(unsupported("writes a table with an exclusion constraint"));
            }
        }
        Ok(())
    }

    /// COMMIT's validation, without its decision. Leaves every claim this
    /// transaction will need held by the transaction itself.
    fn validate_for_prepare(&self, tx: &mut Transaction) -> Result<()> {
        let _serializable_shared = self.serializable_commit_admission.read();
        self.check_recovery_state()?;
        self.validate_transaction_collection_incarnations(tx)?;
        // Resolve delta writes now (they lock their rows): the persisted
        // images must be absolute.
        self.repair_conflicting_delta_writes(tx)?;
        for write in &mut tx.writes {
            write.repair = None;
        }
        self.detect_commit_conflicts(tx)?;
        let writes_by_collection = tx_writes_by_collection(&tx.writes);
        let mut index_mutations =
            self.tx_index_mutations_by_collection(&writes_by_collection, tx.writes_are_unique())?;
        self.plan_vector_dims(&writes_by_collection)?;
        self.fill_all_index_mutation_rowids(&mut index_mutations)?;
        {
            let _unique_claims = self.claim_unique_keys(tx, &index_mutations)?;
            self.validate_index_mutations(&index_mutations)?;
        }
        // Hold every unique key this transaction introduces exclusively: no
        // other writer can commit it before COMMIT/ROLLBACK PREPARED.
        let unique_indexes = self
            .indexes
            .iter()
            .filter_map(|(name, index)| {
                let state = index.read();
                state
                    .definition
                    .unique
                    .then(|| (name.clone(), state.definition.collection.clone()))
            })
            .collect::<Vec<_>>();
        if !unique_indexes.is_empty() {
            let records = tx
                .writes
                .iter()
                .filter(|write| write.op == TxWriteOp::Upsert)
                .map(|write| {
                    Ok((
                        write.collection.clone(),
                        write.record()?.map(|record| Arc::clone(record)),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            for (collection, record) in records {
                let Some(record) = record else { continue };
                for (index, index_collection) in &unique_indexes {
                    if *index_collection == collection {
                        tx.reserve_unique_index_record_for_integrity_check(index, &record)?;
                    }
                }
            }
        }
        if let Some(mut registry) = self.postgres_commit_registry(tx) {
            registry.prepare(self, tx, &index_mutations)?.prepared();
        }
        Ok(())
    }

    /// End `tx` by preparing it as `gid`. On success the transaction is
    /// detached: the caller drops it, and nothing it held is released.
    /// Returns the durable commit sequence of the prepare record. On error
    /// the caller rolls `tx` back as after a failed COMMIT.
    pub fn prepare_transaction(
        &self,
        tx: &mut Transaction,
        gid: &str,
        owner: &str,
        database: &str,
    ) -> Result<u64> {
        if self.max_prepared_transactions() == 0 {
            return Err(error(
                "55000",
                "prepared transactions are disabled",
                Some("Set \"max_prepared_transactions\" to a nonzero value.".into()),
            ));
        }
        if gid.len() > GID_MAX_BYTES {
            return Err(error(
                "22023",
                format!("transaction identifier \"{gid}\" is too long"),
                None,
            ));
        }
        if self.ensure_writable("prepare transaction").is_err() {
            self.check_recovery_state()?;
            return Err(error(
                "25006",
                "cannot execute PREPARE TRANSACTION during recovery",
                None,
            ));
        }
        if !self.collections.contains_key(PREPARED_XACTS_COLLECTION) {
            return Err(error(
                "55000",
                "prepared transactions are not initialized on this database",
                Some("Restart the server with \"max_prepared_transactions\" set.".into()),
            ));
        }
        self.check_preparable(tx)?;
        {
            let mut state = self.prepared_xacts.state.lock();
            if state.entries.contains_key(gid) || state.preparing.contains(gid) {
                return Err(error(
                    "42710",
                    format!("transaction identifier \"{gid}\" is already in use"),
                    None,
                ));
            }
            let max = self.max_prepared_transactions();
            if state.entries.len() + state.preparing.len() >= max {
                return Err(error(
                    "53200",
                    "maximum number of prepared transactions reached",
                    Some(format!(
                        "Increase max_prepared_transactions (currently {max})."
                    )),
                ));
            }
            state.preparing.insert(gid.to_string());
        }
        let result = self.prepare_transaction_inner(tx, gid, owner, database);
        self.prepared_xacts.state.lock().preparing.remove(gid);
        result
    }

    fn prepare_transaction_inner(
        &self,
        tx: &mut Transaction,
        gid: &str,
        owner: &str,
        database: &str,
    ) -> Result<u64> {
        self.validate_for_prepare(tx)?;
        let mut writes = Vec::with_capacity(tx.writes.len());
        {
            let observed = tx.observed_record_versions.lock();
            for write in &tx.writes {
                let base = if write.statement_snapshot == 0 {
                    observed
                        .get(&write.collection)
                        .and_then(|rows| rows.get(&write.record_id))
                        .copied()
                        .unwrap_or(tx.applied_snapshot)
                } else {
                    write.statement_snapshot
                };
                let record = match write.op {
                    TxWriteOp::Delete => None,
                    TxWriteOp::Upsert => Some(match &write.stored {
                        Some(stored) => Arc::clone(stored),
                        None => Arc::new(StoredRecord::from_record(write.record()?.ok_or_else(
                            || {
                                BicDbError::CommitValidation(format!(
                                    "prepared upsert {}/{} has no record",
                                    write.collection, write.record_id
                                ))
                            },
                        )?)?),
                    }),
                };
                writes.push(PersistedWrite {
                    collection: write.collection.clone(),
                    op: write.op,
                    record_id: write.record_id.clone(),
                    record,
                    timestamp: write.timestamp,
                    snapshot: base,
                });
            }
        }
        // A key written several times was judged against its newest snapshot.
        let mut newest: BTreeMap<(&str, &str), u64> = BTreeMap::new();
        for write in &tx.writes {
            let entry = newest
                .entry((write.collection.as_str(), write.record_id.as_str()))
                .or_default();
            *entry = (*entry).max(write.statement_snapshot);
        }
        for write in &mut writes {
            if let Some(snapshot) =
                newest.get(&(write.collection.as_str(), write.record_id.as_str()))
            {
                write.snapshot = write.snapshot.max(*snapshot);
            }
        }
        let locks = tx.locked_keys.lock().order.clone();
        let info = PreparedTransaction {
            gid: gid.to_string(),
            transaction: tx.id.0,
            prepared_at_micros: now_micros(),
            owner: owner.to_string(),
            database: database.to_string(),
        };
        let persisted = PersistedPrepare {
            format: RECORD_FORMAT,
            gid: gid.to_string(),
            owner: owner.to_string(),
            database: database.to_string(),
            prepared_at_micros: info.prepared_at_micros,
            transaction: tx.id.0,
            repeatable_read: tx.isolation == TransactionIsolation::RepeatableRead,
            serializable: tx.postgres_serializable.is_some(),
            writes: writes.clone(),
            locks: locks.clone(),
        };
        let record = Record::new(record_id(gid)).with_metadata(serde_json::to_value(&persisted)?);
        let mut prepare = self.begin_transaction()?;
        prepare.insert(PREPARED_XACTS_COLLECTION, record)?;
        let sequence = prepare.commit_with_sequence()?;
        // Durable: detach without releasing anything.
        let entry = Entry {
            tx_id: tx.id,
            info,
            isolation: tx.isolation,
            writes,
            locks,
            bindings: Arc::clone(&tx.collection_incarnations),
            serializable: tx.postgres_serializable.take(),
            busy: false,
        };
        tx.locked_keys.get_mut().take_all();
        tx.writes.clear();
        tx.rebuild_write_index();
        tx.prepared_detached = true;
        tx.state = TxState::Committed;
        tx.committed_seq = Some(sequence);
        self.prepared_xacts
            .state
            .lock()
            .entries
            .insert(gid.to_string(), entry);
        Ok(sequence)
    }

    /// Mark `gid` busy for its resolution after the PostgreSQL checks.
    fn claim_prepared(&self, gid: &str, requester: PreparedTransactionRequester<'_>) -> Result<()> {
        self.check_recovery_state()?;
        if self.ensure_writable("finish prepared transaction").is_err() {
            self.check_recovery_state()?;
            return Err(error(
                "25006",
                "cannot execute COMMIT PREPARED or ROLLBACK PREPARED during recovery",
                None,
            ));
        }
        let mut state = self.prepared_xacts.state.lock();
        let entry = state.entries.get_mut(gid).ok_or_else(|| not_found(gid))?;
        if entry.busy {
            return Err(error(
                "55000",
                format!("prepared transaction with identifier \"{gid}\" is busy"),
                None,
            ));
        }
        if entry.info.owner != requester.user && !requester.superuser {
            return Err(error(
                "42501",
                "permission denied to finish prepared transaction",
                Some("Must be superuser or the user that prepared the transaction.".into()),
            ));
        }
        if entry.info.database != requester.database {
            return Err(error(
                "0A000",
                "prepared transaction belongs to another database",
                Some(
                    "Connect to the database where the transaction was prepared to finish it."
                        .into(),
                ),
            ));
        }
        entry.busy = true;
        Ok(())
    }

    fn release_prepared_claim(&self, gid: &str) {
        if let Some(entry) = self.prepared_xacts.state.lock().entries.get_mut(gid) {
            entry.busy = false;
        }
    }

    /// COMMIT PREPARED: commit `gid`'s writes and end its prepared state in
    /// one durable commit. Returns its commit sequence.
    pub fn commit_prepared(
        &self,
        gid: &str,
        requester: PreparedTransactionRequester<'_>,
    ) -> Result<u64> {
        self.claim_prepared(gid, requester)?;
        let (tx_id, isolation, writes, locks, bindings, serializable) = {
            let mut state = self.prepared_xacts.state.lock();
            let entry = state.entries.get_mut(gid).expect("claimed entry");
            (
                entry.tx_id,
                entry.isolation,
                entry.writes.clone(),
                entry.locks.clone(),
                Arc::clone(&entry.bindings),
                entry.serializable.take(),
            )
        };
        let mut tx =
            self.resume_prepared(tx_id, isolation, &writes, &locks, bindings, serializable);
        let result = tx
            .delete(PREPARED_XACTS_COLLECTION, &record_id(gid))
            .and_then(|()| self.commit_transaction(&mut tx));
        match result {
            Ok(sequence) => {
                let finished = self
                    .tx_log
                    .write_durable(sequence)
                    .and_then(|()| tx.finalize_commit_admission());
                tx.finalize_committed_broker_outbox();
                let jobs = tx.finalize_committed_memory_jobs();
                self.prepared_xacts.state.lock().entries.remove(gid);
                finished?;
                jobs?;
                Ok(sequence)
            }
            Err(failure) if tx.state == TxState::Committed => {
                self.prepared_xacts.state.lock().entries.remove(gid);
                Err(failure)
            }
            Err(failure) => {
                // Nothing was decided: return everything to the entry and
                // release only what this attempt acquired itself.
                let held = locks.iter().collect::<FxHashSet<_>>();
                let acquired = tx.locked_keys.get_mut().take_all();
                let extra = acquired
                    .into_iter()
                    .filter(|key| !held.contains(key))
                    .collect::<Vec<_>>();
                release_owned_write_locks(&self.write_locks, tx.id, &extra);
                let serializable = tx.postgres_serializable.take();
                tx.prepared_detached = true;
                drop(tx);
                let mut state = self.prepared_xacts.state.lock();
                if let Some(entry) = state.entries.get_mut(gid) {
                    entry.serializable = serializable;
                    entry.busy = false;
                }
                Err(failure)
            }
        }
    }

    /// ROLLBACK PREPARED: durably end `gid`'s prepared state, then release
    /// its locks and reservations. Returns the commit sequence of the end.
    pub fn rollback_prepared(
        &self,
        gid: &str,
        requester: PreparedTransactionRequester<'_>,
    ) -> Result<u64> {
        self.claim_prepared(gid, requester)?;
        let ended = (|| {
            let mut end = self.begin_transaction()?;
            end.delete(PREPARED_XACTS_COLLECTION, &record_id(gid))?;
            end.commit_with_sequence()
        })();
        let sequence = match ended {
            Ok(sequence) => sequence,
            Err(failure) => {
                self.release_prepared_claim(gid);
                return Err(failure);
            }
        };
        let entry = self
            .prepared_xacts
            .state
            .lock()
            .entries
            .remove(gid)
            .expect("claimed entry");
        self.tx_states.remove(&entry.tx_id.0);
        release_owned_write_locks(&self.write_locks, entry.tx_id, &entry.locks);
        self.catalog_admission.finish(entry.tx_id, TxState::Aborted);
        // Dropping the participant retires it from the SSI graph.
        drop(entry);
        Ok(sequence)
    }

    /// A pending transaction under a prepared transaction's id, owning its
    /// locks, reservations, catalog observations and SSI participant.
    fn resume_prepared(
        &self,
        tx_id: TransactionId,
        isolation: TransactionIsolation,
        writes: &[PersistedWrite],
        locks: &[RecordLockKey],
        bindings: Arc<catalog_admission::Bindings>,
        serializable: Option<sql_serializable::Registration>,
    ) -> Transaction {
        // Register a current snapshot exactly as `begin_transaction` does.
        let mut snapshot = self.last_committed_tx.load(AtomicOrdering::SeqCst);
        loop {
            self.active_snapshots.incr(snapshot);
            let current = self.last_committed_tx.load(AtomicOrdering::SeqCst);
            if current <= snapshot {
                break;
            }
            self.active_snapshots.decr_remove(snapshot);
            snapshot = current;
        }
        let mut tx =
            self.new_transaction_value(tx_id, TransactionId(snapshot), snapshot, bindings, None);
        tx.isolation = isolation;
        tx.postgres_serializable = serializable;
        {
            let held = tx.locked_keys.get_mut();
            for key in locks {
                held.insert(key.clone());
            }
        }
        tx.writes = writes
            .iter()
            .map(|write| TxWrite {
                collection: write.collection.clone(),
                op: write.op,
                record_id: write.record_id.clone(),
                record: OnceLock::new(),
                timestamp: write.timestamp,
                statement_snapshot: write.snapshot,
                repair: None,
                stored: write.record.clone(),
                previous: None,
                previous_stored: None,
                changed: None,
            })
            .collect();
        tx.rebuild_write_index();
        tx
    }

    /// Restore every durable prepare record (opening a store). Before any
    /// other transaction can begin, each takes back its row locks and
    /// constraint reservations under a fresh id, pins its collections
    /// against catalog changes and, when SERIALIZABLE, rejoins the SSI graph.
    pub(super) fn restore_prepared_transactions(&mut self) -> Result<()> {
        let max = std::env::var("BICDB_MAX_PREPARED_TRANSACTIONS")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(self.config.max_prepared_transactions);
        if max > 0 {
            self.set_max_prepared_transactions(max)?;
        }
        if !self.collections.contains_key(PREPARED_XACTS_COLLECTION) || self.is_read_only_standby()
        {
            // A standby lists the records but resolves nothing until it
            // is promoted, which reopens the store.
            return Ok(());
        }
        for persisted in self.read_prepare_records()? {
            let tx_id = self.allocate_tx_id()?;
            self.tx_states.insert(tx_id.0, TxState::Pending);
            let mut locks = Vec::with_capacity(persisted.locks.len());
            for key in &persisted.locks {
                if !self.write_locks.integrity.restore(tx_id, key) {
                    let shard = self.write_locks.shard_idx(key);
                    self.write_locks.shards[shard]
                        .lock()
                        .insert(key.clone(), tx_id);
                }
                locks.push(key.clone());
            }
            let bindings: Arc<catalog_admission::Bindings> = Arc::default();
            let mut written = Vec::new();
            for write in &persisted.writes {
                if let Some(state) = self.collections.get(&write.collection) {
                    let incarnation = Arc::clone(&state.read().incarnation);
                    catalog_admission::CatalogAdmission::bind_restored(
                        &bindings,
                        &write.collection,
                        incarnation,
                    )?;
                    written.push((write.collection.clone(), write.record_id.clone()));
                }
            }
            self.catalog_admission.register(tx_id, &bindings, None)?;
            let serializable = if persisted.serializable {
                Some(self.restore_prepared_serializable(tx_id.0, &written)?)
            } else {
                None
            };
            let isolation = if persisted.repeatable_read || persisted.serializable {
                TransactionIsolation::RepeatableRead
            } else {
                TransactionIsolation::ReadCommitted
            };
            let gid = persisted.gid.clone();
            let entry = Entry {
                tx_id,
                info: PreparedTransaction {
                    gid: persisted.gid,
                    transaction: tx_id.0,
                    prepared_at_micros: persisted.prepared_at_micros,
                    owner: persisted.owner,
                    database: persisted.database,
                },
                isolation,
                writes: persisted.writes,
                locks,
                bindings,
                serializable,
                busy: false,
            };
            self.prepared_xacts.state.lock().entries.insert(gid, entry);
        }
        Ok(())
    }
}
