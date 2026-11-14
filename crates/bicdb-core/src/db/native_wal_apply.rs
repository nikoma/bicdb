//! Constant-size local WAL evidence. This is neither a range certificate nor
//! an image/readiness permit. Only an actual committed transaction can mint it.
use super::*;
use serde::ser::{SerializeSeq, SerializeStruct};

const MAX_NATIVE_WAL_PINS: usize = 65_536;
#[derive(Debug, Default)]
struct RetentionWindow {
    active: usize,
    since: Option<Instant>,
    last_reported: Option<Instant>,
}
#[derive(Debug, Default)]
pub(super) struct NativeWalRetentionState {
    window: Mutex<RetentionWindow>,
    pub(super) active: AtomicUsize,
}
#[derive(Debug)]
struct RetentionLease {
    state: Arc<NativeWalRetentionState>,
}
/// Clones share ONE owned lease; the count falls only after the transaction
/// and every private receipt have released it.
#[derive(Clone, Debug)]
pub(super) struct NativeWalRetentionPin(Arc<RetentionLease>);
impl Drop for RetentionLease {
    fn drop(&mut self) {
        let mut window = self.state.window.lock();
        debug_assert!(window.active > 0);
        window.active -= 1;
        self.state
            .active
            .store(window.active, AtomicOrdering::Release);
        if window.active == 0 {
            window.since = None;
            window.last_reported = None;
        }
    }
}
pub(super) struct NativeWalCutGuard<'a> {
    _window: parking_lot::MutexGuard<'a, RetentionWindow>,
}
impl TxLogHandle {
    pub(super) fn pin_native_wal_proof(&self) -> Result<NativeWalRetentionPin> {
        self.pin_native_wal_proof_with_limit(MAX_NATIVE_WAL_PINS)
    }
    fn pin_native_wal_proof_with_limit(&self, maximum: usize) -> Result<NativeWalRetentionPin> {
        self.check_health()?;
        let mut window = self.native_retention.window.try_lock().ok_or_else(|| {
            BicDbError::HighAvailability(
                "native WAL rotation is in progress; retry before admission".into(),
            )
        })?;
        if window.active >= maximum {
            return Err(BicDbError::HighAvailability(format!("native WAL proof admission reached {maximum} active leases; continuous retention age {:?}", window.since.map(|since| since.elapsed()))));
        }
        if window.active == 0 {
            window.since = Some(Instant::now());
        }
        window.active += 1;
        self.native_retention
            .active
            .store(window.active, AtomicOrdering::Release);
        Ok(NativeWalRetentionPin(Arc::new(RetentionLease {
            state: Arc::clone(&self.native_retention),
        })))
    }
    pub(super) fn native_wal_cut_guard(&self) -> Result<Option<NativeWalCutGuard<'_>>> {
        self.check_health()?;
        let mut window = self.native_retention.window.try_lock().ok_or_else(|| {
            BicDbError::HighAvailability(
                "another native WAL admission/rotation is in progress".into(),
            )
        })?;
        if window.active != 0 {
            let report = window
                .last_reported
                .is_none_or(|last| last.elapsed() >= Duration::from_secs(30));
            let active = window.active;
            let age = window.since.map(|since| since.elapsed().as_millis());
            if report {
                window.last_reported = Some(Instant::now());
            }
            drop(window);
            if report {
                bicdb_page::health::log_event(
                    &json!({"event":"range_image.native_wal_cut_deferred", "severity":"warn", "active_native_proof_leases":active, "continuous_retention_ms":age}),
                );
            }
            return Ok(None);
        }
        Ok(Some(NativeWalCutGuard { _window: window }))
    }
}

#[derive(Debug)]
struct NativeWalSyncReceipt {
    writer_nonce: Uuid,
    through_sequence: u64,
    generation: u64,
    pin: NativeWalRetentionPin,
}

/// Local, current-open evidence from a committed transaction and an actual
/// whole-file transaction-log sync. Private fields/no serde prevent a caller
/// from replacing it with a sequence number or an earlier open's receipt.
#[derive(Debug)]
pub struct NativeWalApplyReceipt {
    database_incarnation: String,
    transaction_id: u64,
    sequence: u64,
    mutations_sha256: String,
    sync: NativeWalSyncReceipt,
}
impl NativeWalApplyReceipt {
    pub fn database_incarnation(&self) -> &str {
        &self.database_incarnation
    }
    pub fn transaction_id(&self) -> u64 {
        self.transaction_id
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn mutations_sha256(&self) -> &str {
        &self.mutations_sha256
    }
}

#[derive(Default)]
struct ShaWriter(sha2::Sha256);
impl Write for ShaWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn mutation_hash(value: &impl Serialize) -> Result<String> {
    let mut writer = ShaWriter::default();
    writer.write_all(b"bicdb.native-applied-mutations.v1\0")?;
    serde_json::to_writer(&mut writer, value)?;
    Ok(hex::encode(writer.0.finalize()))
}

pub(crate) fn admission_mutations_sha256(mutations: &[CommitAdmissionMutation]) -> Result<String> {
    mutation_hash(&mutations)
}
pub(super) fn actual_transaction_mutations_sha256(transaction: &Transaction) -> Result<String> {
    actual_writes_mutations_sha256(&transaction.writes)
}
/// The same canonical ordered mutation hash for bounded decoded WAL writes.
/// This is integrity DATA, never an applied receipt or transaction grant.
pub(super) fn actual_writes_mutations_sha256(writes: &[TxWrite]) -> Result<String> {
    mutation_hash(&ActualMutations(writes))
}
struct ActualMutations<'a>(&'a [TxWrite]);
impl Serialize for ActualMutations<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for write in self.0 {
            struct Mutation<'a>(&'a TxWrite);
            impl Serialize for Mutation<'_> {
                fn serialize<S: serde::Serializer>(
                    &self,
                    serializer: S,
                ) -> std::result::Result<S::Ok, S::Error> {
                    let mut mutation = serializer.serialize_struct("CommitAdmissionMutation", 3)?;
                    mutation.serialize_field("collection", &self.0.collection)?;
                    mutation.serialize_field("record_id", &self.0.record_id)?;
                    let record: Option<&Record> = match self.0.op {
                        TxWriteOp::Upsert => Some(
                            self.0
                                .record()
                                .map_err(serde::ser::Error::custom)?
                                .ok_or_else(|| {
                                    serde::ser::Error::custom("applied upsert has no actual record")
                                })?
                                .as_ref(),
                        ),
                        TxWriteOp::Delete => None,
                    };
                    mutation.serialize_field("record", &record)?;
                    mutation.end()
                }
            }
            sequence.serialize_element(&Mutation(write))?;
        }
        sequence.end()
    }
}

impl TxLogHandle {
    /// Caller must have completed ordinary WAL delivery before invoking the
    /// native proof callback. Do not wait for missing commit slots under a DB
    /// or service latch. Explicit full-file sync closes the buffered-prefix /
    /// recovered-prefix gap; an O_DSYNC append maximum is not used as proof.
    fn sync_native_through(
        &self,
        sequence: u64,
        pin: &NativeWalRetentionPin,
    ) -> Result<NativeWalSyncReceipt> {
        if !Arc::ptr_eq(&self.native_retention, &pin.0.state) {
            return Err(BicDbError::HighAvailability(
                "native WAL pin belongs to another writer".into(),
            ));
        }
        self.check_health()?;
        if self.written_seq.load(AtomicOrdering::Acquire) < sequence {
            return Err(BicDbError::HighAvailability(
                "native WAL receipt requested before actual WAL delivery".into(),
            ));
        }
        let mut state = self.lock_write();
        self.check_health()?;
        if state.written_seq < sequence {
            return Err(BicDbError::HighAvailability(
                "native WAL delivery changed under writer gate".into(),
            ));
        }
        if self.native_synced_through.load(AtomicOrdering::Acquire) < sequence {
            if state.file.is_none() {
                state.file = Some(storage::open_append_handle(&self.path, self.fsync)?);
            }
            let file = state
                .file
                .as_mut()
                .expect("native sync opened append descriptor");
            if let Err(error) = storage::sync_append_handle_data(file, &self.path) {
                return Err(self.fail("transaction_log.native_sync", error));
            }
            self.sync_calls.fetch_add(1, AtomicOrdering::Relaxed);
            // This publication follows an ACTUAL successful whole-file sync,
            // not a flag, elapsed timer or optimistic counter increment.
            self.native_synced_through
                .store(state.written_seq, AtomicOrdering::Release);
        }
        Ok(NativeWalSyncReceipt {
            writer_nonce: self.native_sync_nonce,
            through_sequence: self.native_synced_through.load(AtomicOrdering::Acquire),
            generation: self.native_log_generation.load(AtomicOrdering::Acquire),
            pin: pin.clone(),
        })
    }
}
impl Transaction {
    pub(super) fn mint_native_wal_apply_receipt(&self) -> Result<NativeWalApplyReceipt> {
        if self.state != TxState::Committed {
            return Err(BicDbError::TransactionNotPending);
        }
        let db = unsafe { self.db.as_ref() };
        db.check_recovery_state()?;
        let sequence = self.committed_seq.ok_or_else(|| {
            BicDbError::HighAvailability(
                "committed native apply has no actual local sequence".into(),
            )
        })?;
        let mutations_sha256 = mutation_hash(&ActualMutations(&self.writes))?;
        let pin = self.native_wal_retention.as_ref().ok_or_else(|| {
            BicDbError::HighAvailability(
                "native WAL proof had no pre-admission retention lease".into(),
            )
        })?;
        let sync = db.tx_log.sync_native_through(sequence, pin)?;
        Ok(NativeWalApplyReceipt {
            database_incarnation: db.incarnation.clone(),
            transaction_id: self.id.0,
            sequence,
            mutations_sha256,
            sync,
        })
    }
}
impl BicDb {
    /// Diagnostic only; no durability, admission or row-outcome authority.
    #[cfg(test)]
    pub(crate) fn test_native_wal_retention_active(&self) -> usize {
        self.tx_log
            .native_retention
            .active
            .load(AtomicOrdering::Acquire)
    }
    pub(crate) fn validate_native_wal_apply_receipt(
        &self,
        receipt: &NativeWalApplyReceipt,
    ) -> Result<()> {
        self.check_recovery_state()?;
        if self.incarnation != receipt.database_incarnation
            || self.tx_log.native_sync_nonce != receipt.sync.writer_nonce
            || !Arc::ptr_eq(&self.tx_log.native_retention, &receipt.sync.pin.0.state)
            || self
                .tx_log
                .native_log_generation
                .load(AtomicOrdering::Acquire)
                != receipt.sync.generation
            || receipt.sync.through_sequence < receipt.sequence
            || self
                .tx_log
                .native_synced_through
                .load(AtomicOrdering::Acquire)
                < receipt.sequence
        {
            return Err(BicDbError::HighAvailability(
                "native WAL apply receipt belongs to a different DB/open or unsynced prefix".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn database(root: &Path, fsync: bool) -> BicDb {
        let mut db = BicDb::open_with_config(
            root,
            DbConfig::default()
                .with_fsync(fsync)
                .with_storage_mode(StorageMode::ServerPaged)
                .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
        )
        .unwrap();
        db.create_collection("items").unwrap();
        // Select the private native-WAL path; birth/genesis itself is covered
        // by separate actual-factory tests. This is not an image authority.
        db.range_image_native_birth_active
            .store(true, AtomicOrdering::Release);
        db
    }
    #[test]
    fn native_wal_apply_receipt_requires_actual_wal_delivery_then_exact_actual_mutations() {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path(), true);
        let mut tx = db.begin_transaction().unwrap();
        let record = Record::new("one").with_metadata(json!({"a":1}));
        tx.insert("items", record.clone()).unwrap();
        assert!(tx.mint_native_wal_apply_receipt().is_err());
        let sequence = db.commit_buffered_transaction(&mut tx).unwrap();
        // Current core writes its decision before materialization even via
        // the buffered API; the extra native whole-file barrier is still new.
        assert!(db.tx_log.stats().written_seq >= sequence);
        assert_eq!(
            db.tx_log
                .native_synced_through
                .load(AtomicOrdering::Acquire),
            0
        );
        db.tx_log_handle().write_durable(sequence).unwrap();
        let receipt = tx.mint_native_wal_apply_receipt().unwrap();
        db.validate_native_wal_apply_receipt(&receipt).unwrap();
        assert_eq!(receipt.sequence(), sequence);
        assert_eq!(receipt.transaction_id(), tx.id().0);
        assert_eq!(
            receipt.mutations_sha256(),
            admission_mutations_sha256(&[CommitAdmissionMutation {
                collection: "items".into(),
                record_id: "one".into(),
                record: Some(record)
            }])
            .unwrap()
        );
        let other_root = tempfile::tempdir().unwrap();
        let other = database(other_root.path(), true);
        assert!(other.validate_native_wal_apply_receipt(&receipt).is_err());
    }
    #[test]
    fn native_wal_written_counter_cannot_substitute_for_successful_whole_file_sync() {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path(), false);
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("items", Record::new("buffered")).unwrap();
        let sequence = db.commit_buffered_transaction(&mut tx).unwrap();
        db.tx_log_handle().write_durable(sequence).unwrap();
        assert!(db.tx_log.stats().written_seq >= sequence);
        let _fault = bicdb_page::test_faults::arm_io_failure(
            "transaction_log.native_sync",
            root.path(),
            1,
            true,
        );
        assert!(tx.mint_native_wal_apply_receipt().is_err());
        assert_eq!(
            db.tx_log
                .native_synced_through
                .load(AtomicOrdering::Acquire),
            0
        );
        assert!(db.check_recovery_state().is_err());
        assert!(db.tx_log.native_wal_cut_guard().is_err()); // retained pin must not hide a fence
        assert!(db.tx_log.pin_native_wal_proof().is_err());
        assert_eq!(tx.state, TxState::Committed); // no invented rollback
    }
    #[test]
    fn native_wal_reopen_does_not_reuse_prior_open_receipt_or_infer_sync_from_recovered_counter() {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path(), true);
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("items", Record::new("one")).unwrap();
        let sequence = db.commit_buffered_transaction(&mut tx).unwrap();
        db.tx_log_handle().write_durable(sequence).unwrap();
        let receipt = tx.mint_native_wal_apply_receipt().unwrap();
        drop(tx);
        drop(db);
        let reopened = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(StorageMode::ServerPaged)
                .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
        )
        .unwrap();
        assert!(reopened.current_commit_seq() >= sequence);
        assert_eq!(
            reopened
                .tx_log
                .native_synced_through
                .load(AtomicOrdering::Acquire),
            0
        );
        assert!(reopened
            .validate_native_wal_apply_receipt(&receipt)
            .is_err());
    }
    #[test]
    fn native_wal_retention_blocks_checkpoint_until_callback_and_receipt_owners_release() {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path(), true);
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("items", Record::new("retained")).unwrap();
        let sequence = db.commit_buffered_transaction(&mut tx).unwrap();
        db.tx_log_handle().write_durable(sequence).unwrap();
        let before = fs::read(db.tx_log_path()).unwrap();
        assert!(!before.is_empty());
        db.checkpoint_for_resume().unwrap();
        assert_eq!(fs::read(db.tx_log_path()).unwrap(), before);
        let receipt = tx.mint_native_wal_apply_receipt().unwrap();
        drop(tx);
        db.checkpoint_for_resume().unwrap();
        assert_eq!(fs::read(db.tx_log_path()).unwrap(), before);
        db.validate_native_wal_apply_receipt(&receipt).unwrap();
        drop(receipt);
        assert_eq!(
            db.tx_log
                .native_retention
                .active
                .load(AtomicOrdering::Acquire),
            0
        );
        db.checkpoint_for_resume().unwrap();
        assert_eq!(
            db.tx_log
                .native_synced_through
                .load(AtomicOrdering::Acquire),
            0
        );
    }
    #[test]
    fn native_wal_retention_admission_is_bounded_and_drop_releases_exactly_one_lease() {
        let root = tempfile::tempdir().unwrap();
        let log = TxLogHandle::new(root.path().join("transactions.log"), 0, true);
        let one = log.pin_native_wal_proof_with_limit(2).unwrap();
        let copy = one.clone();
        let two = log.pin_native_wal_proof_with_limit(2).unwrap();
        assert!(log.pin_native_wal_proof_with_limit(2).is_err());
        drop(one);
        assert_eq!(log.native_retention.active.load(AtomicOrdering::Acquire), 2);
        drop(copy);
        drop(two);
        assert_eq!(log.native_retention.active.load(AtomicOrdering::Acquire), 0);
        let cut = log.native_wal_cut_guard().unwrap().unwrap();
        assert!(log.pin_native_wal_proof().is_err());
        drop(cut);
        log.pin_native_wal_proof().unwrap();
    }
    #[derive(Debug, Default)]
    struct RecordingAdmission {
        native: AtomicUsize,
        legacy: AtomicUsize,
        mutations: Mutex<Option<String>>,
    }
    impl CommitAdmission for RecordingAdmission {
        fn admit(&self, intent: &CommitAdmissionIntent) -> Result<CommitAdmissionTicket> {
            *self.mutations.lock() = Some(admission_mutations_sha256(&intent.mutations)?);
            Ok(CommitAdmissionTicket {
                authority: "native-wal-test".into(),
                command_id: "one".into(),
            })
        }
        fn local_applied(&self, _ticket: &CommitAdmissionTicket, _sequence: u64) -> Result<()> {
            self.legacy.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(())
        }
        fn local_applied_with_receipt(
            &self,
            _ticket: &CommitAdmissionTicket,
            receipt: &NativeWalApplyReceipt,
        ) -> Result<()> {
            assert_eq!(
                self.mutations.lock().as_deref(),
                Some(receipt.mutations_sha256())
            );
            self.native.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(())
        }
    }
    #[test]
    fn native_wal_callback_failure_is_unknown_after_commit_and_healthy_callback_releases_pin() {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path(), true);
        let authority = Arc::new(RecordingAdmission::default());
        db.install_commit_admission(authority.clone());
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("items", Record::new("one")).unwrap();
        let sequence = db.commit_buffered_transaction(&mut tx).unwrap();
        db.tx_log_handle().write_durable(sequence).unwrap();
        tx.finalize_commit_admission().unwrap();
        assert_eq!(authority.native.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(authority.legacy.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(
            db.tx_log
                .native_retention
                .active
                .load(AtomicOrdering::Acquire),
            0
        );
        drop(tx);
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("items", Record::new("two")).unwrap();
        let sequence = db.commit_buffered_transaction(&mut tx).unwrap();
        db.tx_log_handle().write_durable(sequence).unwrap();
        let _fault = bicdb_page::test_faults::arm_io_failure(
            "transaction_log.native_sync",
            root.path(),
            1,
            true,
        );
        assert!(
            matches!(tx.finalize_commit_admission(), Err(BicDbError::CommitOutcomeUnknown { sequence:Some(observed), .. }) if observed==sequence)
        );
        assert_eq!(tx.state, TxState::Committed);
        assert_eq!(authority.native.load(AtomicOrdering::Relaxed), 1); // no success callback on failure
        drop(tx);
        assert_eq!(
            db.tx_log
                .native_retention
                .active
                .load(AtomicOrdering::Acquire),
            0
        );
    }
    #[test]
    fn native_wal_legacy_buffered_callback_does_not_gain_or_pay_for_native_proof() {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path(), false);
        db.range_image_native_birth_active
            .store(false, AtomicOrdering::Release);
        let authority = Arc::new(RecordingAdmission::default());
        db.install_commit_admission(authority.clone());
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("items", Record::new("legacy")).unwrap();
        tx.commit().unwrap();
        assert_eq!(authority.legacy.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(authority.native.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(
            db.tx_log
                .native_synced_through
                .load(AtomicOrdering::Acquire),
            0
        );
    }
    #[test]
    fn native_wal_sync_receipt_refuses_actual_unwritten_delivery_without_guessing() {
        let root = tempfile::tempdir().unwrap();
        let log = TxLogHandle::new(root.path().join("transactions.log"), 0, true);
        let pin = log.pin_native_wal_proof().unwrap();
        log.enqueue(1, b"queued-not-written".to_vec());
        assert!(log.sync_native_through(1, &pin).is_err());
        assert_eq!(log.native_synced_through.load(AtomicOrdering::Acquire), 0);
        assert!(!log.path.exists());
    }
    #[test]
    fn native_wal_directory_sync_failure_cannot_publish_a_native_receipt() {
        let root = tempfile::tempdir().unwrap();
        let db = database(root.path(), true);
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("items", Record::new("directory")).unwrap();
        let sequence = db.commit_buffered_transaction(&mut tx).unwrap();
        let _fault = bicdb_page::test_faults::arm_io_failure(
            "transaction_log.native_directory_sync",
            root.path(),
            1,
            true,
        );
        assert!(tx.mint_native_wal_apply_receipt().is_err());
        assert_eq!(
            db.tx_log
                .native_synced_through
                .load(AtomicOrdering::Acquire),
            0
        );
        assert_eq!(tx.committed_seq, Some(sequence));
        assert_eq!(tx.state, TxState::Committed);
        assert!(db.tx_log.native_wal_cut_guard().is_err());
    }
}
