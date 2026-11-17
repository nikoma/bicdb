//! A bounded hold of one led range's write admission, taken by this node's
//! own range move around the commit of a learner allocation.
//!
//! A write command captures its range descriptor when it is admitted and its
//! commit callback compares that descriptor with the published one. A command
//! that is admitted before a learner allocation commits and finishes after it
//! is refused by that comparison with its native pending record already
//! durable, and nothing settles such a command while the allocation stands.
//! The hold makes that interleaving impossible on the leader: it is the
//! range's ordinary admission reservation, so it is granted only once no
//! command of the range is between admission and its local callback, and
//! while it is held every new admission and point read of the range is
//! refused before it prepares anything. It admits nothing, proves nothing to
//! any other member and is dropped by its owner.
use super::*;

/// The range's admission reservation, held by a range move. Dropping it
/// releases the reservation and wakes the range's waiters.
pub(crate) struct RangeAllocationHold<Transport: RangeWriteTransport> {
    coordinator: Arc<RangeWriteCoordinator<Transport>>,
    range_id: RangeId,
    reservation_id: String,
}

impl<Transport: RangeWriteTransport> std::fmt::Debug for RangeAllocationHold<Transport> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RangeAllocationHold")
            .field("range_id", &self.range_id)
            .field("reservation_id", &self.reservation_id)
            .finish()
    }
}

impl<Transport: RangeWriteTransport> RangeAllocationHold<Transport> {
    pub(crate) fn range_id(&self) -> RangeId {
        self.range_id
    }
    /// Wait, bounded, until this node's published configuration no longer
    /// holds `before` for the range: from then on an admission that resolved
    /// the old descriptor is refused when it re-resolves under its own
    /// reservation. Returns whether the change was seen.
    pub(crate) fn wait_published_change(
        &self,
        before: &RangeDescriptor,
        wait: Duration,
        parent: &CancellationToken,
    ) -> bool {
        let deadline = Instant::now() + wait;
        loop {
            let changed = self
                .coordinator
                .topology
                .try_read()
                .is_some_and(|topology| {
                    topology.range_by_id(self.range_id).is_none_or(|current| {
                        crate::distribution_native_read::canonical_range(current)
                            != crate::distribution_native_read::canonical_range(before)
                    })
                });
            if changed {
                return true;
            }
            if Instant::now() >= deadline || parent.check().is_err() {
                return false;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl<Transport: RangeWriteTransport> Drop for RangeAllocationHold<Transport> {
    fn drop(&mut self) {
        let mut active = self.coordinator.active_ranges.lock();
        if active.get(&self.range_id) == Some(&self.reservation_id) {
            active.remove(&self.range_id);
            self.coordinator.active_range_changed.notify_all();
        }
    }
}

impl<Transport: RangeWriteTransport> RangeWriteCoordinator<Transport> {
    /// Take the admission reservation of `range_id` for a range move, waiting
    /// at most `wait` for the command that holds it to finish its local
    /// callback. A command that is still there after the wait (one that
    /// awaits recovery keeps its reservation until it is settled) refuses the
    /// hold by name; nothing was changed.
    pub(crate) fn hold_range_for_allocation(
        self: &Arc<Self>,
        range_id: RangeId,
        wait: Duration,
        parent: &CancellationToken,
    ) -> Result<RangeAllocationHold<Transport>> {
        let deadline = Instant::now() + wait;
        let mut active = self.active_ranges.lock();
        while let Some(command_id) = active.get(&range_id) {
            let now = Instant::now();
            if now >= deadline || parent.check().is_err() {
                return Err(range_write_error(format!(
                    "range {range_id} still holds write command {command_id} between its admission and its local completion; the range move admission hold was not taken"
                )));
            }
            // Short slices: the parent is observed while waiting.
            self.active_range_changed.wait_for(
                &mut active,
                deadline
                    .saturating_duration_since(now)
                    .min(Duration::from_millis(50)),
            );
        }
        let reservation_id = format!("range-move:allocation-hold:{}", Uuid::now_v7());
        active.insert(range_id, reservation_id.clone());
        drop(active);
        Ok(RangeAllocationHold {
            coordinator: Arc::clone(self),
            range_id,
            reservation_id,
        })
    }
}
