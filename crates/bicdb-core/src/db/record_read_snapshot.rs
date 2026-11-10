//! Owned fixed read views for transaction-scoped host descriptors.
use super::*;

/// A fixed read view of one record-id prefix, including the capturing
/// transaction's pending writes as they were when captured. Clones share one
/// pin; the final drop releases it. No handle retains a database pointer.
///
/// Hosts must bound the number and aggregate retained bytes of live handles.
/// Captured pending rows count against the caller's explicit memory limit.
#[derive(Clone, Debug)]
pub struct RecordReadSnapshot(Arc<RecordReadSnapshotState>);

#[derive(Debug)]
struct RecordReadSnapshotState {
    instance_id: u64,
    collection: String,
    id_prefix: String,
    snapshot_tx: TransactionId,
    paged_snapshot: Option<bicdb_page::Snapshot>,
    active_snapshots: Arc<ActiveSnapshots>,
    overlay: FxHashMap<String, Option<Arc<Record>>>,
    retained_bytes: usize,
}

impl Drop for RecordReadSnapshotState {
    fn drop(&mut self) {
        self.active_snapshots.decr_remove(self.snapshot_tx.0);
    }
}

impl RecordReadSnapshot {
    pub fn retained_bytes(&self) -> usize {
        self.0.retained_bytes
    }

    /// Read through ordinary native collection protection. SQL hosts that
    /// enforce their own object privileges use the explicitly unchecked form.
    pub fn get(&self, db: &BicDb, id: &str) -> Result<Option<Arc<Record>>> {
        db.check_recovery_state()?;
        db.ensure_unprotected_legacy_access(&self.0.collection, SecureOperation::Read)?;
        self.get_visible_record_unchecked(db, id)
    }

    /// Fixed snapshot read with no policy/protection gate. The host must check
    /// its descriptor's privileges before invoking this method, as for native
    /// transaction locator reads. `db` must be the capturing database instance
    /// and held under the caller's normal read guard.
    pub fn get_visible_record_unchecked(
        &self,
        db: &BicDb,
        id: &str,
    ) -> Result<Option<Arc<Record>>> {
        db.check_recovery_state()?;
        let result = self.read_record_unchecked(db, id);
        // A failure may fence the database while an old row or cached pending
        // value is being read. Never expose that value after the fence.
        db.check_recovery_state()?;
        result
    }

    fn read_record_unchecked(&self, db: &BicDb, id: &str) -> Result<Option<Arc<Record>>> {
        if db.instance_id() != self.0.instance_id {
            return Err(BicDbError::Unsupported(
                "read snapshot belongs to a different database instance".into(),
            ));
        }
        if !id.starts_with(&self.0.id_prefix) {
            return Err(BicDbError::InvalidRecordId(
                "record lies outside the captured snapshot prefix".into(),
            ));
        }
        db.ensure_collection(&self.0.collection)?;
        #[cfg(test)]
        tests::after_collection_gate();
        if let Some(record) = self.0.overlay.get(id) {
            return Ok(record.clone());
        }
        read_record_at_snapshot_unchecked(
            db,
            &self.0.collection,
            id,
            self.0.snapshot_tx,
            &self.0.paged_snapshot,
        )
        .map(|(record, _, _)| record)
    }
}

impl Transaction {
    /// Capture a fixed read view for host descriptors. Its prefix scope bounds
    /// the pending-row overlay independently of unrelated writes. Subsequent
    /// writes, savepoint rollback, refresh, commit, and drop of this transaction
    /// do not change the captured read view. The caller owns descriptor lifetime
    /// and must close descriptors at its transaction/subtransaction boundaries.
    ///
    /// Like `get_visible_record_unchecked`, capture performs no policy gate;
    /// hosts must authorize the object before capturing/reading its rows.
    pub fn capture_record_prefix_snapshot_unchecked(
        &self,
        collection: &str,
        id_prefix: &str,
        max_retained_bytes: usize,
    ) -> Result<RecordReadSnapshot> {
        if self.state != TxState::Pending {
            return Err(BicDbError::TransactionNotPending);
        }
        let db = self.db_ref();
        db.check_recovery_state()?;
        db.ensure_collection(collection)?;
        let mut retained_bytes = std::mem::size_of::<RecordReadSnapshotState>()
            .saturating_add(collection.len())
            .saturating_add(id_prefix.len())
            .saturating_add(self.paged_snapshot.as_ref().map_or(0, |snapshot| {
                // A cloned page snapshot shares its in-flight set but keeps
                // that allocation alive independently of the transaction.
                snapshot.in_flight.len().saturating_mul(64)
            }));
        let check = |bytes: usize| -> Result<()> {
            crate::query_budget::observe(bytes);
            crate::query_budget::check()?;
            if bytes > max_retained_bytes {
                return Err(BicDbError::QueryMemoryLimit(format!(
                    "read snapshot retains {bytes} bytes; limit is {max_retained_bytes}"
                )));
            }
            Ok(())
        };
        check(retained_bytes)?;
        let mut overlay = FxHashMap::default();
        for write in self.pending_writes_for_collection(collection) {
            if !write.record_id.starts_with(id_prefix) {
                continue;
            }
            let record = match write.op {
                TxWriteOp::Upsert => write.record()?.cloned(),
                TxWriteOp::Delete => None,
            };
            // Account conservatively even though payloads are Arc-shared with
            // the transaction. The descriptor may outlive that transaction.
            retained_bytes = retained_bytes.saturating_add(write.record_id.len() + 128);
            if let Some(record) = &record {
                retained_bytes = retained_bytes
                    .saturating_add(std::mem::size_of::<Record>())
                    .saturating_add(record.id.len())
                    .saturating_add(serde_json::to_vec(record)?.len().saturating_mul(8));
            }
            check(retained_bytes)?;
            overlay.insert(write.record_id.clone(), record);
        }
        // The source transaction already pins this boundary while its overlay
        // is copied. Register the independent pin before returning the handle.
        self.active_snapshots.incr(self.snapshot_tx.0);
        let snapshot = RecordReadSnapshot(Arc::new(RecordReadSnapshotState {
            instance_id: db.instance_id(),
            collection: collection.into(),
            id_prefix: id_prefix.into(),
            snapshot_tx: self.snapshot_tx,
            paged_snapshot: self.paged_snapshot.clone(),
            active_snapshots: self.active_snapshots.clone(),
            overlay,
            retained_bytes,
        }));
        // On a racing durability failure this error also drops the new pin.
        db.check_recovery_state()?;
        Ok(snapshot)
    }
}

// Shared by transactions (which also record write-conflict observations) and
// descriptor snapshots (which retain a fixed read view without a DB pointer).
pub(super) fn read_record_at_snapshot_unchecked(
    db: &BicDb,
    collection: &str,
    id: &str,
    snapshot_tx: TransactionId,
    paged_snapshot: &Option<bicdb_page::Snapshot>,
) -> Result<(Option<Arc<Record>>, u64, Option<Arc<StoredRecord>>)> {
    db.ensure_collection(collection)?;
    let result = {
        let state = db.collection_state(collection)?;
        let shard = state.shard(id).read();
        // Lazy paged collections: a row is "touched this session" exactly
        // when it has a version chain; only then is the chain authoritative
        // (including an "invisible" verdict). A registry rowid alone (the
        // indexed-collection bootstrap allocates one per row without any
        // chain) says nothing about content, so it must not suppress the
        // page-store fallback.
        let versions = shard
            .rowid_of(id)
            .and_then(|rowid| shard.versions.get(&rowid));
        match versions {
            Some(versions) => {
                let visible = visible_version(versions, snapshot_tx);
                // Exact state this read observed. Future mutations above
                // the snapshot are deliberately excluded.
                let observed = visible
                    .map(|version| version.created_tx.0)
                    .unwrap_or_else(|| {
                        versions
                            .iter()
                            .flat_map(|version| {
                                std::iter::once(version.created_tx.0)
                                    .chain(version.deleted_tx.map(|tx| tx.0))
                            })
                            .filter(|version| *version <= snapshot_tx.0)
                            .max()
                            .unwrap_or_default()
                    });
                let stored = visible.map(|version| Arc::clone(&version.record));
                let record = stored
                    .as_deref()
                    .map(StoredRecord::to_record)
                    .transpose()?
                    .map(Arc::new);
                (record, observed, stored)
            }
            None if state.paged_lazy => {
                drop(shard);
                let record = match (paged_snapshot, &db.paged_records) {
                    (Some(snapshot), Some(paged)) => {
                        paged.get(snapshot, collection, id)?.map(Arc::new)
                    }
                    _ => None,
                };
                // An untouched paged baseline predates core commit
                // sequences; its first resident mutation conflicts with 0.
                (record, 0, None)
            }
            None => (None, 0, None),
        }
    };
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    thread_local! {
        static AFTER_COLLECTION_GATE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
            std::cell::RefCell::new(None);
    }

    pub(super) fn after_collection_gate() {
        let callback = AFTER_COLLECTION_GATE.with(|slot| slot.borrow_mut().take());
        if let Some(callback) = callback {
            callback();
        }
    }

    #[test]
    fn descriptor_snapshot_fence_race_returns_no_cached_rows() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let mut db = open(root.path(), mode);
            db.create_collection("pages").unwrap();
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("pages", Record::new("obj/pending")).unwrap();
            let snapshot = tx
                .capture_record_prefix_snapshot_unchecked("pages", "obj/", 4096)
                .unwrap();
            let health = db.durability_health_for_test();
            // Deterministically fence after collection lookup's health check
            // but before a cached pending row is returned. Ordinary pre-fenced
            // tests also pass without the descriptor's final health check.
            AFTER_COLLECTION_GATE.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || {
                    health
                        .record_failure("test.descriptor_fence_race", &"uncertain durable outcome");
                }))
            });
            assert!(
                matches!(
                    snapshot.get_visible_record_unchecked(&db, "obj/pending"),
                    Err(BicDbError::WriteFailed { .. })
                ),
                "cached row escaped the racing durability fence"
            );
            drop(tx);
            drop(snapshot);
            assert_eq!(db.active_snapshots.len(), 0);
        }
    }

    fn open(path: &Path, mode: StorageMode) -> BicDb {
        BicDb::open_with_config(
            path,
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode),
        )
        .unwrap()
    }

    #[test]
    fn descriptor_snapshot_freezes_own_pending_writes_and_deletes() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let mut db = open(root.path(), mode);
            db.create_collection("pages").unwrap();
            db.insert("pages", Record::new("obj/0").with_payload(b"old".to_vec()))
                .unwrap();
            db.insert("pages", Record::new("obj/1")).unwrap();
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("pages", Record::new("obj/0").with_payload(b"own".to_vec()))
                .unwrap();
            tx.delete("pages", "obj/1").unwrap();
            let snapshot = tx
                .capture_record_prefix_snapshot_unchecked("pages", "obj/", 1024 * 1024)
                .unwrap();
            let mark = tx.rollback_mark();
            tx.insert(
                "pages",
                Record::new("obj/0").with_payload(b"later".to_vec()),
            )
            .unwrap();
            tx.insert("pages", Record::new("obj/2")).unwrap();
            tx.rollback_to_mark(mark).unwrap();
            db.rollback_transaction(&mut tx).unwrap();
            drop(tx);
            db.insert(
                "pages",
                Record::new("obj/0").with_payload(b"committed".to_vec()),
            )
            .unwrap();
            assert_eq!(
                snapshot
                    .get(&db, "obj/0")
                    .unwrap()
                    .unwrap()
                    .payload
                    .as_deref(),
                Some(b"own".as_slice())
            );
            assert!(snapshot.get(&db, "obj/1").unwrap().is_none());
            assert!(snapshot.get(&db, "obj/2").unwrap().is_none());
            assert!(snapshot.get(&db, "another/0").is_err());
            assert!(snapshot.retained_bytes() > 0);
            drop(snapshot);
            assert_eq!(db.active_snapshots.len(), 0);
        }
    }

    #[test]
    fn descriptor_pin_survives_transaction_refresh_drop_and_version_gc() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            // Reopen a populated store so paged mode exercises its lazy base.
            let mut db = open(root.path(), mode.clone());
            db.create_collection("pages").unwrap();
            db.insert("pages", Record::new("obj/0").with_timestamp(1))
                .unwrap();
            db.checkpoint_for_resume().unwrap();
            drop(db);
            let mut db = open(root.path(), mode);
            let mut tx = db.begin_transaction().unwrap();
            let snapshot = tx
                .capture_record_prefix_snapshot_unchecked("pages", "obj/", 1024 * 1024)
                .unwrap();
            for value in 2..40 {
                db.insert("pages", Record::new("obj/0").with_timestamp(value))
                    .unwrap();
            }
            tx.refresh_read_committed_snapshot().unwrap();
            assert_eq!(
                tx.get("pages", "obj/0").unwrap().unwrap().timestamp,
                Some(39)
            );
            db.rollback_transaction(&mut tx).unwrap();
            drop(tx);
            db.checkpoint_for_resume().unwrap();
            let clone = snapshot.clone();
            drop(snapshot);
            assert_eq!(clone.get(&db, "obj/0").unwrap().unwrap().timestamp, Some(1));
            assert_eq!(db.active_snapshots.len(), 1);
            drop(clone);
            assert_eq!(db.active_snapshots.len(), 0);
        }
    }

    #[test]
    fn descriptor_snapshot_rejects_reopened_database_and_drops_after_close() {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), StorageMode::EmbeddedMemory);
        db.create_collection("pages").unwrap();
        let mut tx = db.begin_transaction().unwrap();
        let snapshot = tx
            .capture_record_prefix_snapshot_unchecked("pages", "obj/", 1024)
            .unwrap();
        db.rollback_transaction(&mut tx).unwrap();
        drop(tx);
        drop(db);
        let reopened = open(root.path(), StorageMode::EmbeddedMemory);
        assert!(matches!(
            snapshot.get(&reopened, "obj/0"),
            Err(BicDbError::Unsupported(_))
        ));
        drop(reopened);
        // The owned registration must not dereference the already-dropped DB.
        drop(snapshot);
    }

    #[test]
    fn descriptor_snapshot_fails_closed_after_database_durability_failure() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let mut db = open(root.path(), mode);
            db.create_collection("pages").unwrap();
            db.insert("pages", Record::new("obj/committed")).unwrap();
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("pages", Record::new("obj/pending")).unwrap();
            let snapshot = tx
                .capture_record_prefix_snapshot_unchecked("pages", "obj/", 4096)
                .unwrap();
            db.durability_health_for_test()
                .record_failure("test.descriptor_fence", &"uncertain durable outcome");
            for id in ["obj/committed", "obj/pending", "obj/missing"] {
                assert!(
                    matches!(snapshot.get(&db, id), Err(BicDbError::WriteFailed { .. })),
                    "unfenced descriptor read: {id}"
                );
                assert!(
                    matches!(
                        snapshot.get_visible_record_unchecked(&db, id),
                        Err(BicDbError::WriteFailed { .. })
                    ),
                    "unfenced internal descriptor read: {id}"
                );
            }
            assert!(matches!(
                tx.capture_record_prefix_snapshot_unchecked("pages", "obj/", 4096),
                Err(BicDbError::WriteFailed { .. })
            ));
            drop(tx);
            drop(snapshot);
            assert_eq!(db.active_snapshots.len(), 0);
        }
    }

    #[test]
    fn descriptor_snapshot_enforces_budget_without_leaking_pins() {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), StorageMode::EmbeddedMemory);
        db.create_collection("pages").unwrap();
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("pages", Record::new("obj/0").with_payload(vec![7; 4096]))
            .unwrap();
        assert!(matches!(
            tx.capture_record_prefix_snapshot_unchecked("pages", "obj/", 1024),
            Err(BicDbError::QueryMemoryLimit(_))
        ));
        // A scoped descriptor does not retain another object's buffered pages.
        let snapshot = tx
            .capture_record_prefix_snapshot_unchecked("pages", "other/", 1024)
            .unwrap();
        assert!(snapshot.retained_bytes() < 1024);
        drop(snapshot);
        db.rollback_transaction(&mut tx).unwrap();
        assert!(matches!(
            tx.capture_record_prefix_snapshot_unchecked("pages", "obj/", 1024),
            Err(BicDbError::TransactionNotPending)
        ));
        drop(tx);
        assert_eq!(db.active_snapshots.len(), 0);
    }
}
