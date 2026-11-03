//! Host execution dependencies between suspended and independently running work.
use super::*;

#[derive(Debug, Default)]
pub(super) struct Suspensions {
    edges: Mutex<BTreeMap<TransactionId, TransactionId>>,
    count: std::sync::atomic::AtomicUsize,
}

/// Keeps a host transaction suspended while another transaction executes.
/// Row-lock acquisition detects a wait back to a suspended ancestor as a
/// deadlock, without releasing any of that ancestor's locks or writes.
#[derive(Debug)]
pub struct TransactionSuspension {
    registry: Arc<Suspensions>,
    parent: TransactionId,
}
impl TransactionSuspension {
    /// Move the dependency when the independently executing work commits and
    /// begins another transaction before returning to its suspended caller.
    pub fn set_child(&self, child: TransactionId) -> Result<()> {
        if child == self.parent {
            return Err(BicDbError::TransactionDeadlock(
                "transaction cannot wait for itself".into(),
            ));
        }
        let mut edges = self.registry.edges.lock();
        edges.insert(self.parent, child);
        Ok(())
    }
}
impl Drop for TransactionSuspension {
    fn drop(&mut self) {
        let mut edges = self.registry.edges.lock();
        edges.remove(&self.parent);
        self.registry
            .count
            .store(edges.len(), AtomicOrdering::Release);
    }
}
impl Suspensions {
    pub(super) fn would_cycle(&self, waiter: TransactionId, owner: TransactionId) -> bool {
        if self.count.load(AtomicOrdering::Acquire) == 0 {
            return false;
        }
        let edges = self.edges.lock();
        let mut current = owner;
        for _ in 0..=edges.len() {
            let Some(next) = edges.get(&current) else {
                return false;
            };
            if *next == waiter {
                return true;
            }
            current = *next;
        }
        false
    }
}
impl BicDb {
    pub fn suspend_transaction_for(
        &self,
        parent: TransactionId,
        child: TransactionId,
    ) -> Result<TransactionSuspension> {
        if parent == child {
            return Err(BicDbError::TransactionDeadlock(
                "transaction cannot wait for itself".into(),
            ));
        }
        let mut edges = self.transaction_suspensions.edges.lock();
        if edges.contains_key(&parent) {
            return Err(BicDbError::TransactionConflict(
                "transaction already has suspended execution".into(),
            ));
        }
        edges.insert(parent, child);
        self.transaction_suspensions
            .count
            .store(edges.len(), AtomicOrdering::Release);
        drop(edges);
        Ok(TransactionSuspension {
            registry: self.transaction_suspensions.clone(),
            parent,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pinned_snapshot_ignores_independent_commit_refresh() {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
        db.create_collection("rows").unwrap();
        let mut parent = db.begin_transaction().unwrap();
        parent.set_snapshot_isolation(true, 0).unwrap();
        let mut child = db.begin_transaction().unwrap();
        child
            .batch_insert("rows", vec![Record::new("child")])
            .unwrap();
        let sequence = child.commit_with_sequence().unwrap();
        parent
            .refresh_read_committed_snapshot_after(sequence)
            .unwrap();
        assert!(parent.get("rows", "child").unwrap().is_none());
        parent.set_snapshot_isolation(false, sequence).unwrap();
        assert!(parent.get("rows", "child").unwrap().is_some());
        parent.rollback().unwrap();
    }

    #[test]
    fn suspended_owner_deadlock_does_not_release_parent_locks() {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
        db.create_collection("rows").unwrap();
        db.insert("rows", Record::new("1")).unwrap();
        let mut parent = db.begin_transaction().unwrap();
        parent
            .batch_insert("rows", vec![Record::new("1").with_timestamp(10)])
            .unwrap();
        let mut child = db.begin_transaction().unwrap();
        let suspended = db.suspend_transaction_for(parent.id(), child.id()).unwrap();
        assert!(db.suspend_transaction_for(parent.id(), child.id()).is_err());
        assert!(matches!(
            child
                .batch_insert("rows", vec![Record::new("1")])
                .unwrap_err(),
            BicDbError::TransactionDeadlock(_)
        ));
        child.rollback().unwrap();
        let mut next = db.begin_transaction().unwrap();
        suspended.set_child(next.id()).unwrap();
        assert!(matches!(
            next.batch_insert("rows", vec![Record::new("1")])
                .unwrap_err(),
            BicDbError::TransactionDeadlock(_)
        ));
        assert_eq!(
            parent.get("rows", "1").unwrap().unwrap().timestamp,
            Some(10)
        );
        next.rollback().unwrap();
        drop(suspended);
        parent.rollback().unwrap();
        let mut after = db.begin_transaction().unwrap();
        after
            .batch_insert("rows", vec![Record::new("1").with_timestamp(20)])
            .unwrap();
        after.commit().unwrap();
    }

    #[test]
    fn durable_commit_sequence_refresh_keeps_pending_parent_writes() {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
        db.create_collection("rows").unwrap();
        let mut parent = db.begin_transaction().unwrap();
        parent
            .batch_insert("rows", vec![Record::new("parent")])
            .unwrap();
        let mut child = db.begin_transaction().unwrap();
        child
            .batch_insert("rows", vec![Record::new("child")])
            .unwrap();
        let sequence = child.commit_with_sequence().unwrap();
        assert!(sequence > 0);
        parent
            .refresh_read_committed_snapshot_after(sequence)
            .unwrap();
        assert!(parent.get("rows", "parent").unwrap().is_some());
        assert!(parent.get("rows", "child").unwrap().is_some());
        parent.rollback().unwrap();
        drop(db);
        let mut db = BicDb::open(root.path()).unwrap();
        assert!(db.get("rows", "parent").unwrap().is_none());
        assert!(db.get("rows", "child").unwrap().is_some());
    }
}
