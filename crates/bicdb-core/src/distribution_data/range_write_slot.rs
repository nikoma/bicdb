//! Current service slot, distinct from the lifetime of a removed Store.
//!
//! This child owns the Store privately. Reads may borrow it through Deref;
//! mutable protocol calls explicitly borrow `current_mut`. Store replacement
//! must use `replace`, which revokes the old binding before moving either
//! object. A retained old Store cannot keep an old receiver binding current.
use super::*;
use crate::distribution_range_consensus::RangeWriteStoreOpenGuard;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Weak;

#[derive(Debug)]
pub(super) struct RangeWriteServiceSlot {
    store: RangeWriteStore,
    binding: Arc<AtomicBool>,
}

/// Necessary current-slot/lifetime refusal only; never a mutation/read grant.
#[derive(Clone, Debug)]
pub(crate) struct RangeWriteServiceSlotGuard {
    binding: Weak<AtomicBool>,
    store: RangeWriteStoreOpenGuard,
}
impl RangeWriteServiceSlotGuard {
    pub(crate) fn require_live(&self) -> Result<()> {
        if !self
            .binding
            .upgrade()
            .is_some_and(|binding| binding.load(Ordering::Acquire))
        {
            return Err(data_error(
                "actual service range Store slot ended or changed",
            ));
        }
        self.store.require_live()
    }
    pub(crate) fn id(&self) -> Uuid {
        self.store.id()
    }
}
impl RangeWriteServiceSlot {
    pub(super) fn new(store: RangeWriteStore) -> Self {
        Self {
            store,
            binding: Arc::new(AtomicBool::new(true)),
        }
    }
    pub(super) fn guard(&self) -> RangeWriteServiceSlotGuard {
        RangeWriteServiceSlotGuard {
            binding: Arc::downgrade(&self.binding),
            store: self.store.listener_open_guard(),
        }
    }
    pub(super) fn current_mut(&mut self) -> &mut RangeWriteStore {
        &mut self.store
    }
    // No production replacement is currently needed. Keeping replacement in
    // the slot makes the revocation ordering explicit for any future caller.
    #[allow(dead_code)]
    pub(super) fn replace(&mut self, store: RangeWriteStore) -> RangeWriteStore {
        let next = Arc::new(AtomicBool::new(true));
        self.binding.store(false, Ordering::Release);
        let old = std::mem::replace(&mut self.store, store);
        self.binding = next;
        old
    }
}
impl Deref for RangeWriteServiceSlot {
    type Target = RangeWriteStore;
    fn deref(&self) -> &Self::Target {
        &self.store
    }
}
impl Drop for RangeWriteServiceSlot {
    fn drop(&mut self) {
        self.binding.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(root: &Path) -> RangeWriteStore {
        RangeWriteStore::open(
            root,
            ClusterId::new("actual-receiver-store-slot").unwrap(),
            ClusterNodeId::new("n0").unwrap(),
            true,
        )
        .unwrap()
    }

    #[test]
    fn native_registered_replica_receiver_store_slot_rejects_retained_old_object() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let mut slot = RangeWriteServiceSlot::new(store(first.path()));
        let original = slot.guard();
        let old_lifetime = slot.store.listener_open_guard();
        original.require_live().unwrap();
        let old = slot.replace(store(second.path()));
        // Different actual roots: no reliance on same-root DirectoryLock.
        old_lifetime.require_live().unwrap();
        assert_eq!(old.recovery_store_open_id(), original.id());
        assert!(original.require_live().is_err());
        let current = slot.guard();
        current.require_live().unwrap();
        assert_ne!(current.id(), original.id());
        drop(slot);
        assert!(current.require_live().is_err());
        old_lifetime.require_live().unwrap();
        drop(old);
        assert!(old_lifetime.require_live().is_err());
    }

    #[test]
    fn native_registered_replica_receiver_store_slot_ended_store_refuses() {
        let root = tempfile::tempdir().unwrap();
        let slot = RangeWriteServiceSlot::new(store(root.path()));
        let guard = slot.guard();
        let lifetime = slot.store.listener_open_guard();
        guard.require_live().unwrap();
        lifetime.require_live().unwrap();
        drop(slot);
        assert!(lifetime.require_live().is_err());
        assert!(guard.require_live().is_err());
    }
}
