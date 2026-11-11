//! Owned, range-scoped mutation admission and bounded capture drain.
//!
//! This is a local exclusion primitive, not a quorum/epoch/image certificate.
//! Durable range fencing and every mutator's integration are separate required
//! boundaries. A freeze must never be awaited under a database/service latch.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use uuid::Uuid;

use crate::{BicDbError, CancellationToken, RangeId, Result};

const MAX_GATE_RANGES: usize = 65_536;
const POLL: Duration = Duration::from_millis(25);
const MAX_FREEZE_WAIT: Duration = Duration::from_secs(300);

fn invalid(detail: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("range mutation gate: {}", detail.into()))
}

#[derive(Debug, Default)]
struct RangeState {
    active: BTreeSet<Uuid>,
    pending: Option<(Uuid, Uuid)>,
    frozen: Option<(Uuid, Uuid)>,
    poisoned: bool,
}

#[derive(Debug, Default)]
struct Inner {
    ranges: Mutex<BTreeMap<RangeId, RangeState>>,
    changed: Condvar,
    database_active: AtomicUsize,
    range_active: AtomicUsize,
}

/// One shared instance per live database. Clones share admission, not authority.
#[derive(Clone, Debug, Default)]
pub(crate) struct RangeMutationGate {
    inner: Arc<Inner>,
}

/// Owned rather than a borrowed/non-Send lock guard: a leader keeps this token
/// with the admitted command until its actual local-WAL callback finishes.
#[derive(Debug)]
pub(crate) struct RangeMutationPermit {
    inner: Arc<Inner>,
    range_id: RangeId,
    lease: Uuid,
}

/// Conservative admission for native imports which have no range descriptor.
/// It never grants a frozen-range bypass and is held through actual durability.
#[derive(Debug)]
pub(crate) struct DatabaseMutationPermit {
    inner: Arc<Inner>,
}

/// A private transaction annotation, not an owned admission. A synchronous
/// trusted native helper retains its actual owner; commit revalidates this
/// exact lease and refuses after that owner is dropped or replaced.
#[derive(Debug)]
pub(crate) struct RangeMutationClaim {
    inner: Arc<Inner>,
    range_id: RangeId,
    lease: Uuid,
    frozen_owner: Option<Uuid>,
}

/// Local quiescence only. Its private lease nonce prevents a session identifier
/// from being mistaken for an interchangeable guard. It grants no data quorum.
#[derive(Debug)]
pub(crate) struct FrozenRangeMutationPermit {
    inner: Arc<Inner>,
    range_id: RangeId,
    owner: Uuid,
    lease: Uuid,
}

/// Internal helpers accept the already-owned permit instead of reacquiring a
/// gate after a freeze has begun waiting. Every public entry acquires its own
/// normal permit; only a trusted capture/install path receives the frozen arm.
pub(crate) enum RangeMutationAuthority<'a> {
    Mutation(&'a RangeMutationPermit),
    Frozen(&'a FrozenRangeMutationPermit),
}

impl RangeMutationGate {
    pub(crate) fn try_enter_database(&self) -> Result<DatabaseMutationPermit> {
        let ranges = self.inner.ranges.lock();
        if ranges
            .values()
            .any(|state| state.poisoned || state.pending.is_some() || state.frozen.is_some())
        {
            return Err(invalid(
                "unscoped native mutation conflicts with a frozen/draining range",
            ));
        }
        let active = self.inner.database_active.load(Ordering::Relaxed);
        if active >= MAX_GATE_RANGES {
            return Err(invalid("database mutation permit bound reached"));
        }
        self.inner
            .database_active
            .store(active + 1, Ordering::Relaxed);
        drop(ranges);
        Ok(DatabaseMutationPermit {
            inner: Arc::clone(&self.inner),
        })
    }

    pub(crate) fn validate_claim(&self, claim: &RangeMutationClaim) -> Result<()> {
        if !Arc::ptr_eq(&self.inner, &claim.inner) {
            return Err(invalid("transaction claim belongs to another database"));
        }
        let ranges = self.inner.ranges.lock();
        let state = ranges
            .get(&claim.range_id)
            .ok_or_else(|| invalid("transaction admission owner disappeared"))?;
        let valid = if let Some(owner) = claim.frozen_owner {
            state.frozen == Some((owner, claim.lease))
        } else {
            state.active.contains(&claim.lease) && state.frozen.is_none()
        };
        if state.poisoned || !valid {
            return Err(invalid("transaction admission lease is no longer active"));
        }
        Ok(())
    }
    /// Never waits. Safe at an existing database/service-latch boundary.
    pub(crate) fn try_enter(&self, range_id: RangeId) -> Result<RangeMutationPermit> {
        let mut ranges = self.inner.ranges.lock();
        if !ranges.contains_key(&range_id) && ranges.len() >= MAX_GATE_RANGES {
            return Err(invalid("active range limit reached"));
        }
        if self.inner.range_active.load(Ordering::Relaxed) >= MAX_GATE_RANGES {
            return Err(invalid("total range mutation permit bound reached"));
        }
        let state = ranges.entry(range_id).or_default();
        if state.poisoned || state.pending.is_some() || state.frozen.is_some() {
            return Err(invalid(format!(
                "{range_id} is frozen, draining, or invalid"
            )));
        }
        if state.active.len() >= MAX_GATE_RANGES {
            return Err(invalid("range mutation permit bound reached"));
        }
        let lease = Uuid::now_v7();
        state.active.insert(lease);
        self.inner.range_active.fetch_add(1, Ordering::Relaxed);
        Ok(RangeMutationPermit {
            inner: Arc::clone(&self.inner),
            range_id,
            lease,
        })
    }

    /// Atomically drain already-admitted mutations while preventing newcomers.
    /// Call only without any database/service latch. Cancellation/timeout clears
    /// only this caller's pending request; it never revokes an active mutation.
    pub(crate) fn freeze(
        &self,
        range_id: RangeId,
        owner: Uuid,
        budget: Duration,
        cancellation: &CancellationToken,
    ) -> Result<FrozenRangeMutationPermit> {
        if owner.is_nil() || budget.is_zero() || budget > MAX_FREEZE_WAIT {
            return Err(invalid("invalid freeze owner or budget"));
        }
        let cancellation = cancellation.child_with_deadline(Instant::now() + budget);
        cancellation.check()?;
        let lease = Uuid::now_v7();
        let mut ranges = self.inner.ranges.lock();
        if !ranges.contains_key(&range_id) && ranges.len() >= MAX_GATE_RANGES {
            return Err(invalid("active range limit reached"));
        }
        let state = ranges.entry(range_id).or_default();
        if state.poisoned || state.pending.is_some() || state.frozen.is_some() {
            return Err(invalid(format!(
                "{range_id} already has a capture or is invalid"
            )));
        }
        state.pending = Some((owner, lease));
        // Keep all fallible exits inside this closure so the range-map lock is
        // released before pending-request cleanup reacquires it.
        let result = (|| loop {
            cancellation.check()?;
            let state = ranges
                .get_mut(&range_id)
                .ok_or_else(|| invalid("drain state disappeared"))?;
            if state.poisoned || state.pending != Some((owner, lease)) {
                return Err(invalid("drain identity changed"));
            }
            if state.active.is_empty() && self.inner.database_active.load(Ordering::Relaxed) == 0 {
                state.pending = None;
                state.frozen = Some((owner, lease));
                return Ok(FrozenRangeMutationPermit {
                    inner: Arc::clone(&self.inner),
                    range_id,
                    owner,
                    lease,
                });
            }
            self.inner.changed.wait_for(&mut ranges, POLL);
        })();
        if result.is_err() {
            if let Some(state) = ranges.get_mut(&range_id) {
                if state.pending == Some((owner, lease)) {
                    state.pending = None;
                }
            }
            remove_idle(&mut ranges, range_id);
            self.inner.changed.notify_all();
        }
        result
    }

    /// A nonwaiting alternative for callers which already own outer latches.
    /// Busy means retry outside those latches, never park under a service lock.
    pub(crate) fn try_freeze(
        &self,
        range_id: RangeId,
        owner: Uuid,
    ) -> Result<FrozenRangeMutationPermit> {
        if owner.is_nil() {
            return Err(invalid("invalid freeze owner"));
        }
        let mut ranges = self.inner.ranges.lock();
        if !ranges.contains_key(&range_id) && ranges.len() >= MAX_GATE_RANGES {
            return Err(invalid("active range limit reached"));
        }
        let state = ranges.entry(range_id).or_default();
        if state.poisoned
            || !state.active.is_empty()
            || self.inner.database_active.load(Ordering::Relaxed) != 0
            || state.pending.is_some()
            || state.frozen.is_some()
        {
            return Err(invalid(format!(
                "{range_id} is busy or invalid; retry outside database/service locks"
            )));
        }
        let lease = Uuid::now_v7();
        state.frozen = Some((owner, lease));
        Ok(FrozenRangeMutationPermit {
            inner: Arc::clone(&self.inner),
            range_id,
            owner,
            lease,
        })
    }

    pub(crate) fn validate(
        &self,
        range_id: RangeId,
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<()> {
        match authority {
            RangeMutationAuthority::Mutation(permit) => {
                if !Arc::ptr_eq(&self.inner, &permit.inner) || permit.range_id != range_id {
                    return Err(invalid("mutation permit belongs to another range/database"));
                }
                let ranges = self.inner.ranges.lock();
                let state = ranges
                    .get(&range_id)
                    .ok_or_else(|| invalid("mutation state missing"))?;
                // A previously admitted mutation may complete while a freeze
                // waits. Reacquiring a fresh permit here would deadlock it.
                if state.poisoned || !state.active.contains(&permit.lease) || state.frozen.is_some()
                {
                    return Err(invalid("mutation permit is no longer active"));
                }
                Ok(())
            }
            RangeMutationAuthority::Frozen(permit) => self.validate_frozen(range_id, permit),
        }
    }

    pub(crate) fn validate_frozen(
        &self,
        range_id: RangeId,
        permit: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        if !Arc::ptr_eq(&self.inner, &permit.inner) || permit.range_id != range_id {
            return Err(invalid("frozen permit belongs to another range/database"));
        }
        let ranges = self.inner.ranges.lock();
        let state = ranges
            .get(&range_id)
            .ok_or_else(|| invalid("frozen state missing"))?;
        if state.poisoned
            || !state.active.is_empty()
            || state.frozen != Some((permit.owner, permit.lease))
        {
            return Err(invalid("frozen permit is no longer exclusive"));
        }
        Ok(())
    }
}

impl FrozenRangeMutationPermit {
    pub(crate) fn range_id(&self) -> RangeId {
        self.range_id
    }
    pub(crate) fn owner(&self) -> Uuid {
        self.owner
    }
}

fn remove_idle(ranges: &mut BTreeMap<RangeId, RangeState>, range_id: RangeId) {
    if ranges.get(&range_id).is_some_and(|state| {
        !state.poisoned
            && state.active.is_empty()
            && state.pending.is_none()
            && state.frozen.is_none()
    }) {
        ranges.remove(&range_id);
    }
}

impl RangeMutationAuthority<'_> {
    pub(crate) fn range_id(&self) -> RangeId {
        match self {
            Self::Mutation(permit) => permit.range_id,
            Self::Frozen(permit) => permit.range_id,
        }
    }
    pub(crate) fn claim(&self) -> RangeMutationClaim {
        match self {
            Self::Mutation(permit) => RangeMutationClaim {
                inner: Arc::clone(&permit.inner),
                range_id: permit.range_id,
                lease: permit.lease,
                frozen_owner: None,
            },
            Self::Frozen(permit) => RangeMutationClaim {
                inner: Arc::clone(&permit.inner),
                range_id: permit.range_id,
                lease: permit.lease,
                frozen_owner: Some(permit.owner),
            },
        }
    }
}
impl Drop for DatabaseMutationPermit {
    fn drop(&mut self) {
        let _ranges = self.inner.ranges.lock();
        self.inner.database_active.fetch_sub(1, Ordering::Relaxed);
        self.inner.changed.notify_all();
    }
}
impl Drop for RangeMutationPermit {
    fn drop(&mut self) {
        let mut ranges = self.inner.ranges.lock();
        if let Some(state) = ranges.get_mut(&self.range_id) {
            if state.active.remove(&self.lease) {
                self.inner.range_active.fetch_sub(1, Ordering::Relaxed);
            } else {
                state.poisoned = true;
            }
        }
        remove_idle(&mut ranges, self.range_id);
        self.inner.changed.notify_all();
    }
}

impl Drop for FrozenRangeMutationPermit {
    fn drop(&mut self) {
        let mut ranges = self.inner.ranges.lock();
        if let Some(state) = ranges.get_mut(&self.range_id) {
            if state.frozen == Some((self.owner, self.lease)) {
                state.frozen = None;
            }
        }
        remove_idle(&mut ranges, self.range_id);
        self.inner.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread;

    fn range(id: u64) -> RangeId {
        RangeId::new(id).unwrap()
    }
    fn assert_send_sync<T: Send + Sync>() {}
    fn wait_until_pending(gate: &RangeMutationGate, id: RangeId) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if gate
                .inner
                .ranges
                .lock()
                .get(&id)
                .is_some_and(|state| state.pending.is_some())
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "capture never entered pending state"
            );
            thread::yield_now();
        }
    }

    #[test]
    fn owned_permits_cross_threads_and_capture_drains_the_last_callback() {
        assert_send_sync::<RangeMutationGate>();
        assert_send_sync::<RangeMutationPermit>();
        assert_send_sync::<FrozenRangeMutationPermit>();
        let gate = RangeMutationGate::default();
        let permit = gate.try_enter(range(1)).unwrap();
        let (ready, waiting) = mpsc::channel();
        let copy = gate.clone();
        let drain = thread::spawn(move || {
            ready.send(()).unwrap();
            copy.freeze(
                range(1),
                Uuid::now_v7(),
                Duration::from_secs(2),
                &CancellationToken::uncancelable(),
            )
        });
        waiting.recv().unwrap();
        wait_until_pending(&gate, range(1));
        assert!(gate.try_enter(range(1)).is_err());
        // An existing operation is allowed to finish under a pending freeze.
        gate.validate(range(1), &RangeMutationAuthority::Mutation(&permit))
            .unwrap();
        thread::spawn(move || drop(permit)).join().unwrap();
        let frozen = drain.join().unwrap().unwrap();
        gate.validate_frozen(range(1), &frozen).unwrap();
        assert!(gate.try_enter(range(1)).is_err());
        let other = gate.try_enter(range(2)).unwrap();
        drop(other);
        drop(frozen);
        gate.try_enter(range(1)).unwrap();
    }

    #[test]
    fn cancelled_drain_cleans_its_pending_state_without_revoking_writer() {
        let gate = RangeMutationGate::default();
        let permit = gate.try_enter(range(1)).unwrap();
        let token = CancellationToken::uncancelable().child();
        let cancel = token.clone();
        let copy = gate.clone();
        let drain = thread::spawn(move || {
            copy.freeze(range(1), Uuid::now_v7(), Duration::from_secs(2), &token)
        });
        wait_until_pending(&gate, range(1));
        cancel.cancel();
        assert!(matches!(
            drain.join().unwrap(),
            Err(BicDbError::QueryCanceled)
        ));
        gate.validate(range(1), &RangeMutationAuthority::Mutation(&permit))
            .unwrap();
        let another = gate.try_enter(range(1)).unwrap();
        drop(another);
        drop(permit);
        gate.try_freeze(range(1), Uuid::now_v7()).unwrap();
    }

    #[test]
    fn bounded_busy_attempt_does_not_park_or_leave_a_pending_freeze() {
        let gate = RangeMutationGate::default();
        let permit = gate.try_enter(range(1)).unwrap();
        assert!(gate.try_freeze(range(1), Uuid::now_v7()).is_err());
        let started = Instant::now();
        assert!(matches!(
            gate.freeze(
                range(1),
                Uuid::now_v7(),
                Duration::from_millis(20),
                &CancellationToken::uncancelable()
            ),
            Err(BicDbError::QueryTimedOut)
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(permit);
        let frozen = gate.try_freeze(range(1), Uuid::now_v7()).unwrap();
        drop(frozen);
        assert!(gate.inner.ranges.lock().is_empty());
    }

    #[test]
    fn permits_cannot_be_reused_for_another_range_database_or_freeze() {
        let first = RangeMutationGate::default();
        let second = RangeMutationGate::default();
        let permit = first.try_enter(range(1)).unwrap();
        assert!(second
            .validate(range(1), &RangeMutationAuthority::Mutation(&permit))
            .is_err());
        assert!(first
            .validate(range(2), &RangeMutationAuthority::Mutation(&permit))
            .is_err());
        drop(permit);
        let frozen = first.try_freeze(range(1), Uuid::now_v7()).unwrap();
        assert!(second.validate_frozen(range(1), &frozen).is_err());
        assert!(first.validate_frozen(range(2), &frozen).is_err());
        assert!(first
            .freeze(
                range(1),
                frozen.owner(),
                Duration::from_millis(20),
                &CancellationToken::uncancelable()
            )
            .is_err());
    }
    #[test]
    fn database_mutations_drain_and_cannot_enter_a_range_capture() {
        let gate = RangeMutationGate::default();
        let global = gate.try_enter_database().unwrap();
        assert!(gate.try_freeze(range(1), Uuid::now_v7()).is_err());
        let copy = gate.clone();
        let drain = thread::spawn(move || {
            copy.freeze(
                range(1),
                Uuid::now_v7(),
                Duration::from_secs(2),
                &CancellationToken::uncancelable(),
            )
        });
        wait_until_pending(&gate, range(1));
        assert!(gate.try_enter_database().is_err());
        let unrelated = gate.try_enter(range(2)).unwrap();
        drop(global);
        let frozen = drain.join().unwrap().unwrap();
        assert!(gate.try_enter_database().is_err());
        gate.validate(range(2), &RangeMutationAuthority::Mutation(&unrelated))
            .unwrap();
        drop(frozen);
        gate.try_enter_database().unwrap();
    }

    #[test]
    fn transaction_claim_cannot_survive_its_actual_owner_or_reuse_another_lease() {
        let gate = RangeMutationGate::default();
        let permit = gate.try_enter(range(1)).unwrap();
        let claim = RangeMutationAuthority::Mutation(&permit).claim();
        gate.validate_claim(&claim).unwrap();
        drop(permit);
        let replacement = gate.try_enter(range(1)).unwrap();
        assert!(gate.validate_claim(&claim).is_err());
        drop(replacement);
        let frozen = gate.try_freeze(range(1), Uuid::now_v7()).unwrap();
        let frozen_claim = RangeMutationAuthority::Frozen(&frozen).claim();
        gate.validate_claim(&frozen_claim).unwrap();
        drop(frozen);
        let replacement = gate.try_freeze(range(1), Uuid::now_v7()).unwrap();
        assert!(gate.validate_claim(&frozen_claim).is_err());
        drop(replacement);
    }
}
