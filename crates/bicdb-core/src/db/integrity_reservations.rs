//! Transaction-scoped shared reference and exclusive mutation reservations.
//! Shared/exclusive acquisitions have separate rollback events so rolling back
//! an upgrade restores a pre-savepoint shared reservation instead of losing it.
use super::*;

const EXCLUSIVE_PREFIX: &str = "\0bicdb:constraint:";
const SHARED_PREFIX: &str = "\0bicdb:constraint-shared:";

pub(super) fn is_event(key: &RecordLockKey) -> bool {
    key.0.starts_with(EXCLUSIVE_PREFIX) || key.0.starts_with(SHARED_PREFIX)
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Mode {
    Shared,
    Exclusive,
}

#[derive(Debug, Default)]
struct Owners {
    shared: FxHashSet<TransactionId>,
    exclusive: Option<TransactionId>,
}

#[derive(Debug)]
pub(super) struct Table {
    keys: ShardedMutexMap<RecordLockKey, Owners>,
    changed: Vec<Condvar>,
}

pub(super) struct WaitContext<'a> {
    pub cancellation: &'a CancellationToken,
    pub graph: &'a wait_graph::WaitForGraph,
    pub suspensions: &'a transaction_suspension::Suspensions,
    pub check_health: &'a dyn Fn() -> Result<()>,
}

impl Table {
    pub(super) fn with_shards(count: usize) -> Self {
        let keys = ShardedMutexMap::with_shards(count);
        let changed = (0..keys.shards.len()).map(|_| Condvar::new()).collect();
        Self { keys, changed }
    }

    pub(super) fn reserve(
        &self,
        transaction: TransactionId,
        constraint: &str,
        value: &str,
        mode: Mode,
        locked_keys: &Mutex<LockedKeys>,
    ) -> Result<()> {
        let key = (constraint.to_owned(), value.to_owned());
        let mut shard = self.keys.shards[self.keys.shard_idx(&key)].lock();
        let owners = shard.entry(key).or_default();
        let conflict = owners.exclusive.is_some_and(|owner| owner != transaction)
            || matches!(mode, Mode::Exclusive)
                && owners.shared.iter().any(|owner| *owner != transaction);
        if conflict {
            return Err(BicDbError::TransactionConflict(
                "constraint key is reserved by another transaction".into(),
            ));
        }
        let prefix = match mode {
            Mode::Shared => {
                owners.shared.insert(transaction);
                SHARED_PREFIX
            }
            Mode::Exclusive => {
                owners.exclusive = Some(transaction);
                EXCLUSIVE_PREFIX
            }
        };
        locked_keys
            .lock()
            .insert((format!("{prefix}{constraint}"), value.to_owned()));
        Ok(())
    }

    pub(super) fn reserve_cancellable(
        &self,
        transaction: TransactionId,
        constraint: &str,
        value: &str,
        mode: Mode,
        locked_keys: &Mutex<LockedKeys>,
        context: &WaitContext<'_>,
    ) -> Result<()> {
        (context.check_health)()?;
        context.cancellation.check()?;
        match self.reserve(transaction, constraint, value, mode, locked_keys) {
            Ok(()) => {
                (context.check_health)()?;
                context.cancellation.check()?;
                Ok(())
            }
            Err(BicDbError::TransactionConflict(_)) => {
                self.reserve_inner(transaction, constraint, value, mode, locked_keys, context)
            }
            Err(error) => Err(error),
        }
    }

    fn reserve_inner(
        &self,
        transaction: TransactionId,
        constraint: &str,
        value: &str,
        mode: Mode,
        locked_keys: &Mutex<LockedKeys>,
        context: &WaitContext<'_>,
    ) -> Result<()> {
        let key = (constraint.to_owned(), value.to_owned());
        let shard_index = self.keys.shard_idx(&key);
        let started = Instant::now();
        let mut edges = Vec::new();
        let mut previous_owners = Vec::new();
        loop {
            (context.check_health)()?;
            context.cancellation.check()?;
            let mut shard = self.keys.shards[shard_index].lock();
            let owners = shard.entry(key.clone()).or_default();
            let mut blockers = Vec::new();
            if let Some(owner) = owners.exclusive.filter(|owner| *owner != transaction) {
                blockers.push(owner);
            }
            if matches!(mode, Mode::Exclusive) {
                blockers.extend(
                    owners
                        .shared
                        .iter()
                        .copied()
                        .filter(|owner| *owner != transaction),
                );
            }
            if !blockers.is_empty() {
                if started.elapsed() >= Duration::from_secs(240) {
                    return Err(BicDbError::TransactionConflict(
                        "constraint key wait exceeded its bounded safeguard".into(),
                    ));
                }
                blockers.sort_unstable();
                blockers.dedup();
                if blockers != previous_owners {
                    edges.clear();
                    // Keep ownership stable while registering graph edges,
                    // exactly as the row-lock path does. The graph never
                    // acquires a reservation shard in the reverse direction.
                    for owner in &blockers {
                        if context.suspensions.would_cycle(transaction, *owner) {
                            return Err(BicDbError::TransactionDeadlock(
                                "constraint key waits for suspended execution".into(),
                            ));
                        }
                        let edge =
                            context.graph.wait_for(transaction, *owner).ok_or_else(|| {
                                BicDbError::TransactionDeadlock(
                                    "cycle detected while waiting for a constraint key".into(),
                                )
                            })?;
                        edges.push(edge);
                    }
                    previous_owners = blockers;
                }
                self.changed[shard_index].wait_for(&mut shard, Duration::from_millis(1));
                continue;
            }
            edges.clear();
            let prefix = match mode {
                Mode::Shared => {
                    owners.shared.insert(transaction);
                    SHARED_PREFIX
                }
                Mode::Exclusive => {
                    owners.exclusive = Some(transaction);
                    EXCLUSIVE_PREFIX
                }
            };
            locked_keys
                .lock()
                .insert((format!("{prefix}{constraint}"), value.to_owned()));
            drop(shard);
            (context.check_health)()?;
            context.cancellation.check()?;
            return Ok(());
        }
    }

    /// Returns true for an integrity event, even when its owner is absent.
    /// Never remove another transaction's reservation during rollback.
    pub(super) fn release(&self, transaction: TransactionId, event: &RecordLockKey) -> bool {
        let (constraint, shared) = if let Some(name) = event.0.strip_prefix(SHARED_PREFIX) {
            (name, true)
        } else if let Some(name) = event.0.strip_prefix(EXCLUSIVE_PREFIX) {
            (name, false)
        } else {
            return false;
        };
        let key = (constraint.to_owned(), event.1.clone());
        let shard_index = self.keys.shard_idx(&key);
        let mut shard = self.keys.shards[shard_index].lock();
        if let Some(owners) = shard.get_mut(&key) {
            if shared {
                owners.shared.remove(&transaction);
            } else if owners.exclusive == Some(transaction) {
                owners.exclusive = None;
            }
            if owners.shared.is_empty() && owners.exclusive.is_none() {
                shard.remove(&key);
            }
        }
        drop(shard);
        self.changed[shard_index].notify_all();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database() -> (tempfile::TempDir, BicDb) {
        let dir = tempfile::tempdir().unwrap();
        let db =
            BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
        (dir, db)
    }

    #[test]
    fn readers_coexist_but_mutations_conflict_until_all_readers_release() {
        let (_dir, db) = database();
        let one = db.begin_transaction().unwrap();
        let two = db.begin_transaction().unwrap();
        let mutation = db.begin_transaction().unwrap();
        one.reserve_integrity_key_shared("parent", "1").unwrap();
        two.reserve_integrity_key_shared("parent", "1").unwrap();
        assert!(mutation.reserve_integrity_key("parent", "1").is_err());
        one.commit().unwrap();
        assert!(mutation.reserve_integrity_key("parent", "1").is_err());
        two.rollback().unwrap();
        mutation.reserve_integrity_key("parent", "1").unwrap();
        assert!(db
            .begin_transaction()
            .unwrap()
            .reserve_integrity_key_shared("parent", "1")
            .is_err());
        mutation.rollback().unwrap();
        db.begin_transaction()
            .unwrap()
            .reserve_integrity_key_shared("parent", "1")
            .unwrap();
    }

    #[test]
    fn rolling_back_an_upgrade_restores_the_prior_shared_reservation() {
        let (_dir, db) = database();
        let mut owner = db.begin_transaction().unwrap();
        owner.reserve_integrity_key_shared("p", "1").unwrap();
        let mark = owner.rollback_mark();
        owner.reserve_integrity_key("p", "1").unwrap();
        assert!(db
            .begin_transaction()
            .unwrap()
            .reserve_integrity_key_shared("p", "1")
            .is_err());
        owner.rollback_to_mark(mark).unwrap();
        let reader = db.begin_transaction().unwrap();
        reader.reserve_integrity_key_shared("p", "1").unwrap();
        assert!(db
            .begin_transaction()
            .unwrap()
            .reserve_integrity_key("p", "1")
            .is_err());
        drop(owner);
        assert!(reader.reserve_integrity_key("p", "1").is_ok());
    }

    #[test]
    fn savepoint_suffix_release_preserves_an_earlier_exclusive_reservation() {
        let (_dir, db) = database();
        let mut owner = db.begin_transaction().unwrap();
        owner.reserve_integrity_key("p", "1").unwrap();
        let mark = owner.rollback_mark();
        owner.reserve_integrity_key_shared("p", "1").unwrap();
        owner.rollback_to_mark(mark).unwrap();
        assert!(db
            .begin_transaction()
            .unwrap()
            .reserve_integrity_key_shared("p", "1")
            .is_err());
        owner.rollback().unwrap();
        db.begin_transaction()
            .unwrap()
            .reserve_integrity_key_shared("p", "1")
            .unwrap();
    }

    #[test]
    fn concurrent_readers_and_writer_never_own_incompatible_reservations() {
        let table = Table::with_shards(4);
        for _ in 0..64 {
            let barrier = std::sync::Barrier::new(2);
            let (read, write) = (
                Mutex::new(LockedKeys::default()),
                Mutex::new(LockedKeys::default()),
            );
            std::thread::scope(|scope| {
                let reader = scope.spawn(|| {
                    let result = table.reserve(TransactionId(1), "p", "k", Mode::Shared, &read);
                    barrier.wait();
                    result.is_ok()
                });
                let writer = scope.spawn(|| {
                    let result = table.reserve(TransactionId(2), "p", "k", Mode::Exclusive, &write);
                    barrier.wait();
                    result.is_ok()
                });
                let (read_ok, write_ok) = (reader.join().unwrap(), writer.join().unwrap());
                assert_ne!(read_ok, write_ok);
                for event in read.lock().take_all() {
                    table.release(TransactionId(1), &event);
                }
                for event in write.lock().take_all() {
                    table.release(TransactionId(2), &event);
                }
            });
        }
        assert!(table
            .keys
            .shards
            .iter()
            .all(|shard| shard.lock().is_empty()));
    }
}
