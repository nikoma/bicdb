//! Transaction-local read versions for an existing persistent BLOB value.
//!
//! SQL storage/owner lookup and authorization are separate integration steps.
//! These observations grant no mutation permission and retain no database handle.
use super::*;
use bicdb_core::{
    Transaction, TransactionOutcome, TransactionOutcomeObserver, TransactionWriteReceipt,
};
use std::sync::Mutex;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum OwnerVersion {
    Present { record_id: String, bytes: Arc<[u8]> },
    Deleted,
    Replaced,
}

#[derive(Debug)]
struct Change {
    receipt: TransactionWriteReceipt,
    version: OwnerVersion,
}

#[derive(Debug)]
pub(super) struct History {
    origin: TransactionOutcomeObserver,
    collection: String,
    initial: OwnerVersion,
    changes: Mutex<Vec<Change>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Acquisition {
    Snapshot,
    ForUpdate,
}

/// A particular locator's read version. Assignment clones this state; an
/// append replaces only the destination's state. The shared history supplies
/// origin-commit and rollback resolution, not current-row reads.
#[derive(Clone, Debug)]
pub(super) struct ReadVersion {
    history: Arc<History>,
    acquisition: Acquisition,
    bytes: Arc<[u8]>,
    writer: Option<TransactionWriteReceipt>,
    acquisition_write: Option<TransactionWriteReceipt>,
}

fn invalid_value() -> SqlError {
    errors::database_error(-22922, "55000", "nonexistent LOB value")
}

fn uncertain_outcome() -> SqlError {
    SqlError::data_exception(
        "55000",
        "LOB origin transaction has no resolved commit outcome",
        None,
    )
}

impl History {
    /// The baseline is the first acquired read version of this value in the
    /// origin transaction. It can belong to an uncommitted insertion: its
    /// bytes remain readable after rollback even though its row is absent.
    pub(super) fn existing(
        tx: &Transaction,
        collection: String,
        record_id: String,
        bytes: Arc<[u8]>,
    ) -> Arc<Self> {
        Arc::new(Self {
            origin: tx.observe_outcome(),
            collection,
            initial: OwnerVersion::Present { record_id, bytes },
            changes: Mutex::new(Vec::new()),
        })
    }

    fn changes(&self) -> Result<std::sync::MutexGuard<'_, Vec<Change>>> {
        self.changes.lock().map_err(|_| uncertain_outcome())
    }

    pub(super) fn snapshot(
        self: &Arc<Self>,
        acquisition: Acquisition,
        bytes: Arc<[u8]>,
    ) -> Result<ReadVersion> {
        let acquisition_write = if acquisition == Acquisition::ForUpdate {
            self.changes()?
                .iter()
                .rev()
                .find(|change| change.receipt.rollback_boundary().is_none())
                .map(|change| change.receipt.clone())
        } else {
            None
        };
        Ok(ReadVersion {
            history: self.clone(),
            acquisition,
            bytes,
            writer: None,
            acquisition_write,
        })
    }

    /// Observe an already-buffered owner write. Calling this before a write,
    /// with another transaction, or after a commit cannot create a version.
    pub(super) fn record(
        &self,
        tx: &mut Transaction,
        record_id: &str,
        version: OwnerVersion,
    ) -> Result<TransactionWriteReceipt> {
        if !self.origin.same_transaction(&tx.observe_outcome()) {
            return Err(SqlError::data_exception(
                "55000",
                "LOB history belongs to another transaction",
                None,
            ));
        }
        if matches!(&version, OwnerVersion::Present { record_id: owner, .. } if owner != record_id)
        {
            return Err(SqlError::data_exception(
                "55000",
                "LOB owner does not match the observed record write",
                None,
            ));
        }
        let receipt = tx.observe_record_write(&self.collection, record_id)?;
        let mut changes = self.changes()?;
        changes.retain(|change| change.receipt.rollback_boundary().is_none());
        if changes
            .iter()
            .rev()
            .find(|change| change.receipt.rollback_boundary().is_none())
            .is_some_and(|last| last.receipt.write_len() >= receipt.write_len())
        {
            return Err(SqlError::data_exception(
                "55000",
                "LOB versions must follow distinct buffered owner writes",
                None,
            ));
        }
        changes.push(Change {
            receipt: receipt.clone(),
            version,
        });
        Ok(receipt)
    }

    fn latest(
        &self,
        boundary: Option<bicdb_core::TransactionWriteRollback>,
    ) -> Result<OwnerVersion> {
        let mut changes = self.changes()?;
        changes.retain(|change| change.receipt.rollback_boundary().is_none());
        let selected = changes.iter().rposition(|change| {
            boundary.is_none_or(|boundary| {
                change.receipt.write_len() <= boundary.retained_write_len
                    && change.receipt.id() <= boundary.receipt_limit
            })
        });
        let version = selected
            .map(|index| changes[index].version.clone())
            .unwrap_or_else(|| self.initial.clone());
        if boundary.is_none()
            && matches!(self.origin.outcome(), TransactionOutcome::Committed { .. })
        {
            if let Some(index) = selected {
                let final_change = changes.swap_remove(index);
                changes.clear();
                changes.push(final_change);
                changes.shrink_to_fit();
            } else {
                changes.clear();
                changes.shrink_to_fit();
            }
        }
        Ok(version)
    }

    fn aborted_baseline(&self) -> Result<OwnerVersion> {
        let mut changes = self.changes()?;
        changes.clear();
        changes.shrink_to_fit();
        Ok(self.initial.clone())
    }

    /// Origin-local state only. The caller must independently resolve the
    /// live durable owner, enforce its SQL policies and check its row lock.
    pub(super) fn owner_observation(&self) -> Result<OwnerVersion> {
        match self.origin.outcome() {
            TransactionOutcome::Pending | TransactionOutcome::Committed { .. } => self.latest(None),
            TransactionOutcome::Aborted => self.aborted_baseline(),
            TransactionOutcome::Committing { .. } | TransactionOutcome::RecoveryRequired { .. } => {
                Err(uncertain_outcome())
            }
        }
    }
}

impl PartialEq for ReadVersion {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.history, &other.history)
            && self.acquisition == other.acquisition
            && self.bytes == other.bytes
            && self.writer.as_ref().map(TransactionWriteReceipt::id)
                == other.writer.as_ref().map(TransactionWriteReceipt::id)
            && self
                .acquisition_write
                .as_ref()
                .map(TransactionWriteReceipt::id)
                == other
                    .acquisition_write
                    .as_ref()
                    .map(TransactionWriteReceipt::id)
    }
}
impl Eq for ReadVersion {}

impl ReadVersion {
    pub(super) fn belongs_to(&self, tx: &Transaction) -> bool {
        self.history.origin.same_transaction(&tx.observe_outcome())
    }
    pub(super) fn with_read_bytes(mut self, bytes: Arc<[u8]>) -> Self {
        self.bytes = bytes;
        self
    }
    pub(super) fn after_current_write(
        &self,
        tx: &mut Transaction,
        record_id: &str,
    ) -> Result<Self> {
        let actual = tx.observe_record_write(&self.history.collection, record_id)?;
        let receipt = self
            .history
            .changes()?
            .iter()
            .rev()
            .find(|change| {
                change.receipt.write_len() == actual.write_len()
                    && change.receipt.rollback_boundary().is_none()
            })
            .map(|change| change.receipt.clone())
            .ok_or_else(uncertain_outcome)?;
        self.after_write(receipt)
    }
    /// Advance only the destination after a successfully recorded append.
    pub(super) fn after_write(&self, receipt: TransactionWriteReceipt) -> Result<Self> {
        if !self.history.origin.same_transaction(receipt.transaction())
            || receipt.rollback_boundary().is_some()
            || receipt.transaction().outcome() != TransactionOutcome::Pending
        {
            return Err(SqlError::data_exception(
                "55000",
                "LOB read version requires a surviving origin write",
                None,
            ));
        }
        let bytes = self
            .history
            .changes()?
            .iter()
            .find(|change| change.receipt.id() == receipt.id())
            .and_then(|change| match &change.version {
                OwnerVersion::Present { bytes, .. } => Some(bytes.clone()),
                OwnerVersion::Deleted | OwnerVersion::Replaced => None,
            })
            .ok_or_else(|| {
                SqlError::data_exception(
                    "55000",
                    "LOB destination write is not a registered present value",
                    None,
                )
            })?;
        Ok(Self {
            history: self.history.clone(),
            acquisition: self.acquisition,
            bytes,
            writer: Some(receipt),
            acquisition_write: self.acquisition_write.clone(),
        })
    }

    pub(super) fn read(&self) -> Result<Arc<[u8]>> {
        let version = match self.history.origin.outcome() {
            TransactionOutcome::Pending => match self
                .writer
                .as_ref()
                .or(self.acquisition_write.as_ref())
                .and_then(TransactionWriteReceipt::rollback_boundary)
            {
                Some(boundary) => self.history.latest(Some(boundary))?,
                None => return Ok(self.bytes.clone()),
            },
            TransactionOutcome::Committed { .. } => {
                let final_version = self.history.latest(None)?;
                if self.acquisition == Acquisition::ForUpdate || self.writer.is_some() {
                    final_version
                } else {
                    return Ok(self.bytes.clone());
                }
            }
            TransactionOutcome::Aborted => {
                let baseline = self.history.aborted_baseline()?;
                if self.writer.is_some() || self.acquisition == Acquisition::ForUpdate {
                    baseline
                } else {
                    return Ok(self.bytes.clone());
                }
            }
            TransactionOutcome::Committing { .. } | TransactionOutcome::RecoveryRequired { .. } => {
                return Err(uncertain_outcome())
            }
        };
        match version {
            OwnerVersion::Present { bytes, .. } => Ok(bytes),
            OwnerVersion::Deleted | OwnerVersion::Replaced => Err(invalid_value()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bicdb_core::{DbConfig, Record};

    pub(super) fn database() -> (tempfile::TempDir, BicDb) {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
        db.create_collection("values").unwrap();
        db.insert("values", Record::new("one")).unwrap();
        (root, db)
    }
    pub(super) fn bytes(value: &[u8]) -> Arc<[u8]> {
        Arc::from(value)
    }
    pub(super) fn write(
        history: &History,
        tx: &mut Transaction,
        value: &[u8],
    ) -> TransactionWriteReceipt {
        tx.update("values", Record::new("one").with_payload(value.to_vec()))
            .unwrap();
        history
            .record(
                tx,
                "one",
                OwnerVersion::Present {
                    record_id: "one".into(),
                    bytes: bytes(value),
                },
            )
            .unwrap()
    }
    pub(super) fn read(value: &ReadVersion) -> Vec<u8> {
        value.read().unwrap().to_vec()
    }

    #[test]
    fn copied_writers_keep_separate_pending_reads_then_share_origin_commit() {
        let (_root, mut db) = database();
        let mut tx = db.begin_transaction().unwrap();
        let history = History::existing(&tx, "values".into(), "one".into(), bytes(&[0xAB]));
        let original = history
            .snapshot(Acquisition::ForUpdate, bytes(&[0xAB]))
            .unwrap();
        let alias = original.clone();
        let first = original
            .after_write(write(&history, &mut tx, &[0xAB, 1]))
            .unwrap();
        let second = alias
            .after_write(write(&history, &mut tx, &[0xAB, 2]))
            .unwrap();
        assert_eq!(read(&first), [0xAB, 1]);
        assert_eq!(read(&second), [0xAB, 2]);
        assert_eq!(read(&alias), [0xAB]);
        tx.commit().unwrap();
        assert_eq!(read(&first), [0xAB, 2]);
        assert_eq!(read(&second), [0xAB, 2]);
        assert_eq!(read(&alias), [0xAB, 2]);
    }

    #[test]
    fn rollback_of_alias_write_follows_retained_prefix_without_refreshing_unwritten_aliases() {
        let (_root, mut db) = database();
        let mut tx = db.begin_transaction().unwrap();
        let history = History::existing(&tx, "values".into(), "one".into(), bytes(&[0xAB]));
        let original = history
            .snapshot(Acquisition::ForUpdate, bytes(&[0xAB]))
            .unwrap();
        let outer = tx.rollback_mark();
        let first = original
            .after_write(write(&history, &mut tx, &[0xAB, 1]))
            .unwrap();
        let inner = tx.rollback_mark();
        let second = original
            .after_write(write(&history, &mut tx, &[0xAB, 2]))
            .unwrap();
        tx.rollback_to_mark(inner).unwrap();
        assert_eq!(read(&first), [0xAB, 1]);
        assert_eq!(read(&second), [0xAB, 1]);
        assert_eq!(read(&original), [0xAB]);
        tx.rollback_to_mark(outer).unwrap();
        let replacement = original
            .after_write(write(&history, &mut tx, &[0xAB, 3]))
            .unwrap();
        assert_eq!(read(&first), [0xAB]);
        assert_eq!(read(&second), [0xAB]);
        assert_eq!(read(&replacement), [0xAB, 3]);
        tx.rollback().unwrap();
        assert_eq!(read(&replacement), [0xAB]);
    }

    #[test]
    fn independent_read_snapshot_does_not_follow_committed_writer() {
        let (_root, mut db) = database();
        let mut tx = db.begin_transaction().unwrap();
        let history = History::existing(&tx, "values".into(), "one".into(), bytes(&[0xAB]));
        let reader = history
            .snapshot(Acquisition::Snapshot, bytes(&[0xAB]))
            .unwrap();
        let writer = history
            .snapshot(Acquisition::ForUpdate, bytes(&[0xAB]))
            .unwrap();
        let writer = writer
            .after_write(write(&history, &mut tx, &[0xAB, 1]))
            .unwrap();
        tx.commit().unwrap();
        assert_eq!(read(&reader), [0xAB]);
        assert_eq!(read(&writer), [0xAB, 1]);
    }

    #[test]
    fn committed_invalidation_does_not_discard_precommit_read_bytes() {
        for invalid in [OwnerVersion::Deleted, OwnerVersion::Replaced] {
            let (_root, mut db) = database();
            let mut tx = db.begin_transaction().unwrap();
            let history = History::existing(&tx, "values".into(), "one".into(), bytes(&[0xAB]));
            let value = history
                .snapshot(Acquisition::ForUpdate, bytes(&[0xAB]))
                .unwrap();
            let independent = history
                .snapshot(Acquisition::Snapshot, bytes(&[0xAB]))
                .unwrap();
            tx.delete("values", "one").unwrap();
            history.record(&mut tx, "one", invalid.clone()).unwrap();
            assert_eq!(history.owner_observation().unwrap(), invalid);
            assert_eq!(read(&value), [0xAB]);
            tx.commit().unwrap();
            assert!(matches!(
                value.read().unwrap_err(),
                SqlError::OracleError { code: -22922, .. }
            ));
            assert_eq!(read(&independent), [0xAB]);
        }
    }

    #[test]
    fn unrelated_transactions_and_unregistered_writes_cannot_attach_versions() {
        let (_root, mut db) = database();
        let mut first = db.begin_transaction().unwrap();
        let history = History::existing(&first, "values".into(), "one".into(), bytes(&[0xAB]));
        let reader = history
            .snapshot(Acquisition::ForUpdate, bytes(&[0xAB]))
            .unwrap();
        let mut other = db.begin_transaction().unwrap();
        other.insert("values", Record::new("other")).unwrap();
        assert!(history
            .record(&mut other, "other", OwnerVersion::Deleted)
            .is_err());
        other.rollback().unwrap();
        first.update("values", Record::new("one")).unwrap();
        let unregistered = first.observe_record_write("values", "one").unwrap();
        assert!(reader.after_write(unregistered).is_err());
        first.rollback().unwrap();
    }
}

#[cfg(test)]
mod acquisition_tests {
    use super::tests::{bytes, database, read, write};
    use super::*;
    use bicdb_core::Record;

    #[test]
    fn later_locked_acquisition_follows_rollback_while_independent_read_keeps_its_snapshot() {
        let (_root, mut db) = database();
        let mut tx = db.begin_transaction().unwrap();
        let history = History::existing(&tx, "values".into(), "one".into(), bytes(&[0xAB]));
        let mark = tx.rollback_mark();
        write(&history, &mut tx, &[0xAB, 1]);
        let locked = history
            .snapshot(Acquisition::ForUpdate, bytes(&[0xAB, 1]))
            .unwrap();
        let independent = history
            .snapshot(Acquisition::Snapshot, bytes(&[0xAB, 1]))
            .unwrap();
        tx.rollback_to_mark(mark).unwrap();
        assert_eq!(read(&locked), [0xAB]);
        assert_eq!(read(&independent), [0xAB, 1]);
        tx.rollback().unwrap();
        assert_eq!(read(&locked), [0xAB]);
        assert_eq!(read(&independent), [0xAB, 1]);
    }

    #[test]
    fn original_snapshot_writer_joins_commit_but_its_unwritten_alias_stays_independent() {
        let (_root, mut db) = database();
        let mut tx = db.begin_transaction().unwrap();
        let history = History::existing(&tx, "values".into(), "one".into(), bytes(&[0xAB]));
        let original = history
            .snapshot(Acquisition::Snapshot, bytes(&[0xAB]))
            .unwrap();
        let alias = original.clone();
        let writer = original
            .after_write(write(&history, &mut tx, &[0xAB, 1]))
            .unwrap();
        let locked = history
            .snapshot(Acquisition::ForUpdate, bytes(&[0xAB, 1]))
            .unwrap();
        let locked = locked
            .after_write(write(&history, &mut tx, &[0xAB, 2]))
            .unwrap();
        tx.commit().unwrap();
        assert_eq!(read(&writer), [0xAB, 2]);
        assert_eq!(read(&locked), [0xAB, 2]);
        assert_eq!(read(&alias), [0xAB]);
    }

    #[test]
    fn rolled_back_new_value_keeps_its_read_bytes_without_a_live_owner() {
        let (_root, mut db) = database();
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("values", Record::new("born").with_payload(vec![0xCD]))
            .unwrap();
        let history = History::existing(&tx, "values".into(), "born".into(), bytes(&[0xCD]));
        let value = history
            .snapshot(Acquisition::ForUpdate, bytes(&[0xCD]))
            .unwrap();
        let alias = value.clone();
        tx.rollback().unwrap();
        assert!(db.get("values", "born").unwrap().is_none());
        assert_eq!(read(&value), [0xCD]);
        assert_eq!(read(&alias), [0xCD]);
    }

    #[test]
    fn physical_owner_move_does_not_change_existing_value_read_identity() {
        let (_root, mut db) = database();
        let mut tx = db.begin_transaction().unwrap();
        let history = History::existing(&tx, "values".into(), "one".into(), bytes(&[0xAB]));
        let value = history
            .snapshot(Acquisition::ForUpdate, bytes(&[0xAB]))
            .unwrap();
        tx.delete("values", "one").unwrap();
        tx.insert("values", Record::new("moved").with_payload(vec![0xAB]))
            .unwrap();
        history
            .record(
                &mut tx,
                "moved",
                OwnerVersion::Present {
                    record_id: "moved".into(),
                    bytes: bytes(&[0xAB]),
                },
            )
            .unwrap();
        tx.commit().unwrap();
        assert_eq!(read(&value), [0xAB]);
        assert_eq!(
            history.owner_observation().unwrap(),
            OwnerVersion::Present {
                record_id: "moved".into(),
                bytes: bytes(&[0xAB])
            }
        );
    }
}

/// Per-connection lookup survives short-lived SQL sessions. Initialization is
/// lazy; cloning an uninitialized state establishes a shared registry so a
/// nested reader can register a value before its parent has read any BLOBs.
#[derive(Debug, Default)]
pub(crate) struct State {
    shared: std::sync::OnceLock<Arc<Mutex<Registry>>>,
}

impl Clone for State {
    fn clone(&self) -> Self {
        Self {
            shared: std::sync::OnceLock::from(self.registry().clone()),
        }
    }
}

#[derive(Debug, Default)]
struct Registry {
    origins: Vec<OriginValues>,
}

#[derive(Debug)]
struct OriginValues {
    origin: TransactionOutcomeObserver,
    values: std::collections::HashMap<ValueIdentity, std::sync::Weak<History>>,
    prune_at: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct ValueIdentity {
    pub(super) collection: String,
    pub(super) column: String,
    pub(super) value_id: uuid::Uuid,
}

impl State {
    fn registry(&self) -> &Arc<Mutex<Registry>> {
        self.shared
            .get_or_init(|| Arc::new(Mutex::new(Registry::default())))
    }

    pub(super) fn record_write(
        &mut self,
        tx: &mut Transaction,
        identity: &ValueIdentity,
        record_id: &str,
        version: OwnerVersion,
    ) -> Result<()> {
        let Some(shared) = self.shared.get() else {
            return Ok(());
        };
        let history = {
            let registry = shared.lock().map_err(|_| uncertain_outcome())?;
            registry
                .origins
                .iter()
                .find(|origin| origin.origin.same_transaction(&tx.observe_outcome()))
                .and_then(|origin| origin.values.get(identity))
                .and_then(std::sync::Weak::upgrade)
        };
        if let Some(history) = history {
            history.record(tx, record_id, version)?;
        }
        Ok(())
    }

    pub(super) fn capture(
        &mut self,
        tx: &Transaction,
        identity: ValueIdentity,
        record_id: String,
        bytes: Arc<[u8]>,
        acquisition: Acquisition,
    ) -> Result<ReadVersion> {
        let origin = tx.observe_outcome();
        let history = {
            let mut registry = self.registry().lock().map_err(|_| uncertain_outcome())?;
            let position = registry
                .origins
                .iter()
                .position(|known| known.origin.same_transaction(&origin));
            let index = match position {
                Some(position) => position,
                None => {
                    registry.origins.retain(|known| {
                        known.values.values().any(|value| value.strong_count() != 0)
                    });
                    registry.origins.push(OriginValues {
                        origin,
                        values: std::collections::HashMap::new(),
                        prune_at: 64,
                    });
                    registry.origins.len() - 1
                }
            };
            let values = &mut registry.origins[index];
            if values.values.len() >= values.prune_at {
                values.values.retain(|_, value| value.strong_count() != 0);
                values.prune_at = values.values.len().max(32).saturating_mul(2);
                if values.values.capacity() > values.prune_at.saturating_mul(4) {
                    values.values.shrink_to(values.prune_at);
                }
            }
            values
                .values
                .get(&identity)
                .and_then(std::sync::Weak::upgrade)
                .unwrap_or_else(|| {
                    let history = History::existing(
                        tx,
                        identity.collection.clone(),
                        record_id,
                        bytes.clone(),
                    );
                    values.values.insert(identity, Arc::downgrade(&history));
                    history
                })
        };
        history.snapshot(acquisition, bytes)
    }
}

#[cfg(test)]
mod session_tests {
    use super::tests::{bytes, database, read, write};
    use super::*;

    pub(super) fn identity() -> ValueIdentity {
        ValueIdentity {
            collection: "values".into(),
            column: "payload".into(),
            value_id: uuid::Uuid::new_v4(),
        }
    }

    #[test]
    fn histories_follow_pending_transactions_between_short_lived_sql_sessions() {
        let (_root, mut db) = database();
        let tx = db.begin_transaction().unwrap();
        let key = identity();
        let mut session = SqlSession::new(&mut db).with_pending_transaction(tx);
        let initial = session
            .oracle_lobs
            .capture(
                session.tx.as_ref().unwrap(),
                key.clone(),
                "one".into(),
                bytes(&[0xAB]),
                Acquisition::ForUpdate,
            )
            .unwrap();
        let receipt = write(&initial.history, session.tx.as_mut().unwrap(), &[0xAB, 1]);
        let writer = initial.after_write(receipt).unwrap();
        let tx = session.take_pending_transaction().unwrap();
        let cache = session.into_catalog_cache();
        let mut session = SqlSession::new(&mut db)
            .with_catalog_cache(cache)
            .with_pending_transaction(tx);
        let later = session
            .oracle_lobs
            .capture(
                session.tx.as_ref().unwrap(),
                key.clone(),
                "one".into(),
                bytes(&[0xAB, 1]),
                Acquisition::ForUpdate,
            )
            .unwrap();
        assert!(Arc::ptr_eq(&initial.history, &later.history));
        let mut tx = session.take_pending_transaction().unwrap();
        tx.commit().unwrap();
        let cache = session.into_catalog_cache();
        assert_eq!(read(&writer), [0xAB, 1]);
        let tx = db.begin_transaction().unwrap();
        let mut session = SqlSession::new(&mut db)
            .with_catalog_cache(cache)
            .with_pending_transaction(tx);
        let next = session
            .oracle_lobs
            .capture(
                session.tx.as_ref().unwrap(),
                key,
                "one".into(),
                bytes(&[0xAB, 1]),
                Acquisition::ForUpdate,
            )
            .unwrap();
        assert!(!Arc::ptr_eq(&writer.history, &next.history));
        let receipt = write(&next.history, session.tx.as_mut().unwrap(), &[0xAB, 2]);
        let next = next.after_write(receipt).unwrap();
        let mut tx = session.take_pending_transaction().unwrap();
        tx.commit().unwrap();
        assert_eq!(read(&writer), [0xAB, 1]);
        assert_eq!(read(&next), [0xAB, 2]);
    }

    #[test]
    fn value_histories_are_distinct_per_column_and_expired_entries_are_pruned() {
        let (_root, mut db) = database();
        let tx = db.begin_transaction().unwrap();
        let mut state = State::default();
        let key = identity();
        let mut other = key.clone();
        other.column = "another".into();
        let first = state
            .capture(&tx, key, "one".into(), bytes(&[1]), Acquisition::Snapshot)
            .unwrap();
        let second = state
            .capture(&tx, other, "one".into(), bytes(&[2]), Acquisition::Snapshot)
            .unwrap();
        assert!(!Arc::ptr_eq(&first.history, &second.history));
        for _ in 0..200 {
            drop(
                state
                    .capture(
                        &tx,
                        identity(),
                        "one".into(),
                        bytes(&[]),
                        Acquisition::Snapshot,
                    )
                    .unwrap(),
            );
        }
        let registry = state.registry().lock().unwrap();
        let values = &registry.origins[0];
        assert!(values.values.len() < values.prune_at);
        assert!(values.values.len() < 70);
        assert_eq!(read(&first), [1]);
        assert_eq!(read(&second), [2]);
    }
}

#[cfg(test)]
mod registry_tests {
    use super::session_tests::identity;
    use super::tests::{bytes, database};
    use super::*;

    #[test]
    fn clone_before_first_capture_shares_histories_and_nested_transactions_do_not_reset_parent() {
        let (_root, mut db) = database();
        let parent = db.begin_transaction().unwrap();
        let child = db.begin_transaction().unwrap();
        let mut state = State::default();
        assert!(state.shared.get().is_none());
        let mut nested = state.clone();
        let key = identity();
        let first = nested
            .capture(
                &parent,
                key.clone(),
                "one".into(),
                bytes(&[1]),
                Acquisition::ForUpdate,
            )
            .unwrap();
        let other = nested
            .capture(
                &child,
                key.clone(),
                "one".into(),
                bytes(&[2]),
                Acquisition::ForUpdate,
            )
            .unwrap();
        assert!(!Arc::ptr_eq(&first.history, &other.history));
        let returned = state
            .capture(
                &parent,
                key,
                "one".into(),
                bytes(&[1]),
                Acquisition::ForUpdate,
            )
            .unwrap();
        assert!(Arc::ptr_eq(&first.history, &returned.history));
    }

    #[test]
    fn abandoned_origin_entries_are_removed_without_retaining_the_database() {
        let (_root, mut db) = database();
        let mut state = State::default();
        for _ in 0..80 {
            let tx = db.begin_transaction().unwrap();
            drop(
                state
                    .capture(
                        &tx,
                        identity(),
                        "one".into(),
                        bytes(&[1]),
                        Acquisition::Snapshot,
                    )
                    .unwrap(),
            );
            drop(tx);
        }
        assert_eq!(state.registry().lock().unwrap().origins.len(), 1);
        drop(db);
        assert!(state
            .registry()
            .lock()
            .unwrap()
            .origins
            .iter()
            .all(|origin| origin.origin.outcome() == TransactionOutcome::Aborted));
    }
}

#[cfg(test)]
mod retention_tests {
    use super::tests::{bytes, database, read, write};
    use super::*;

    #[test]
    fn a_surviving_independent_reader_releases_obsolete_commit_versions() {
        let (_root, mut db) = database();
        let mut tx = db.begin_transaction().unwrap();
        let history = History::existing(&tx, "values".into(), "one".into(), bytes(&[0]));
        let independent = history
            .snapshot(Acquisition::Snapshot, bytes(&[0]))
            .unwrap();
        for value in 1..30 {
            write(&history, &mut tx, &[value]);
        }
        assert_eq!(history.changes().unwrap().len(), 29);
        tx.commit().unwrap();
        assert_eq!(read(&independent), [0]);
        assert_eq!(history.changes().unwrap().len(), 1);
        let OwnerVersion::Present {
            bytes: final_bytes, ..
        } = history.owner_observation().unwrap()
        else {
            panic!("missing owner")
        };
        assert_eq!(&*final_bytes, &[29]);
    }

    #[test]
    fn abort_and_rollback_release_discarded_versions_without_reusing_receipts() {
        let (_root, mut db) = database();
        let mut tx = db.begin_transaction().unwrap();
        let history = History::existing(&tx, "values".into(), "one".into(), bytes(&[0]));
        let value = history
            .snapshot(Acquisition::ForUpdate, bytes(&[0]))
            .unwrap();
        let mark = tx.rollback_mark();
        let discarded = write(&history, &mut tx, &[1]);
        let old = value.after_write(discarded.clone()).unwrap();
        tx.rollback_to_mark(mark).unwrap();
        let replacement = write(&history, &mut tx, &[2]);
        assert!(replacement.id() > discarded.id());
        assert_eq!(history.changes().unwrap().len(), 1);
        assert_eq!(read(&old), [0]);
        tx.rollback().unwrap();
        assert_eq!(read(&value), [0]);
        assert!(history.changes().unwrap().is_empty());
    }
}
