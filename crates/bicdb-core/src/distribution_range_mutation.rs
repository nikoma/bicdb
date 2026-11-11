//! Owned, range-scoped mutation admission and bounded capture drain.
//!
//! This is a local exclusion primitive, not a quorum/epoch/image certificate.
//! Durable range fencing and every mutator's integration are separate required
//! boundaries. A freeze must never be awaited under a database/service latch.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
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
    observation_generation: Uuid,
    active: BTreeSet<Uuid>,
    pending: Option<(Uuid, Uuid)>,
    frozen: Option<(Uuid, Uuid)>,
    controlled: BTreeSet<Uuid>,
    frozen_owner_dropped: bool,
    capture_pin: Option<Uuid>,
    poisoned: bool,
}

#[derive(Debug, Default)]
struct Inner {
    observation_database_generation: Mutex<Uuid>,
    ranges: Mutex<BTreeMap<RangeId, RangeState>>,
    serving_generations: Mutex<BTreeMap<RangeId, (Uuid, Weak<AtomicBool>)>>,
    changed: Condvar,
    database_active: AtomicUsize,
    range_active: AtomicUsize,
}

/// One shared instance per live database. Clones share admission, not authority.
#[derive(Clone, Debug, Default)]
pub(crate) struct RangeMutationGate {
    inner: Arc<Inner>,
}

/// Local invalidation only, never quorum or serving authority. A new freeze
/// invalidates this range's old generation before any frozen mutation can run.
#[derive(Debug)]
pub(crate) struct RangeServingGeneration {
    inner: Weak<Inner>,
    range_id: RangeId,
    generation: Uuid,
    live: Arc<AtomicBool>,
}

/// An owned invalidation observation. It holds no writer exclusion and creates
/// no range/session entry. An actual admitted mutation invalidates it even if
/// that mutation later fails before publishing data.
#[derive(Debug)]
pub(crate) struct NativeRangeReadStamp {
    inner: Weak<Inner>,
    range_id: RangeId,
    range_generation: Option<Uuid>,
    database_generation: Uuid,
}
fn require_readable_gate(inner: &Inner, state: Option<&RangeState>) -> Result<()> {
    if inner.database_active.load(Ordering::Relaxed) != 0
        || state.is_some_and(|state| {
            state.poisoned
                || !state.active.is_empty()
                || state.pending.is_some()
                || state.frozen.is_some()
                || !state.controlled.is_empty()
                || state.capture_pin.is_some()
                || state.frozen_owner_dropped
        })
    {
        return Err(invalid(
            "native read observation overlaps an actual mutable admission/freeze",
        ));
    }
    Ok(())
}
impl NativeRangeReadStamp {
    pub(crate) fn require_current(&self) -> Result<()> {
        let inner = self
            .inner
            .upgrade()
            .ok_or_else(|| invalid("native read gate was dropped"))?;
        let ranges = inner.ranges.lock();
        let state = ranges.get(&self.range_id);
        require_readable_gate(&inner, state)?;
        if state.map(|s| s.observation_generation) != self.range_generation
            || *inner.observation_database_generation.lock() != self.database_generation
        {
            return Err(invalid("native read observation generation changed"));
        }
        Ok(())
    }
}
impl RangeServingGeneration {
    pub(crate) fn require_current(&self) -> Result<()> {
        let inner = self
            .inner
            .upgrade()
            .ok_or_else(|| invalid("native serving gate was dropped"))?;
        if !self.live.load(Ordering::Acquire)
            || inner
                .serving_generations
                .lock()
                .get(&self.range_id)
                .is_none_or(|(id, _)| *id != self.generation)
        {
            return Err(invalid("native serving generation was invalidated"));
        }
        Ok(())
    }
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
    controlled_lease: Option<Uuid>,
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

/// An owned controlled writer. It must accompany the actual WAL callback;
/// dropping its freeze owner cannot reopen normal writes before this drains.
#[derive(Debug)]
pub(crate) struct ControlledFrozenMutationPermit {
    inner: Arc<Inner>,
    range_id: RangeId,
    owner: Uuid,
    owner_lease: Uuid,
    lease: Uuid,
}

/// Owned exclusive source reader: while live every controlled/native writer
/// is refused. It is not metadata or data-quorum authority.
#[derive(Debug)]
pub(crate) struct SourceCaptureReadPin {
    inner: Arc<Inner>,
    range_id: RangeId,
    owner: Uuid,
    owner_lease: Uuid,
    lease: Uuid,
}

/// Internal helpers accept owned admission instead of reacquiring a gate
/// after a freeze starts waiting. Frozen alone is READ/quiescence authority;
/// every frozen write uses an owned Controlled token through actual callback.
pub(crate) enum RangeMutationAuthority<'a> {
    Mutation(&'a RangeMutationPermit),
    Frozen(&'a FrozenRangeMutationPermit),
    Controlled(&'a ControlledFrozenMutationPermit),
}

impl RangeMutationGate {
    pub(crate) fn observe_native_read_stamp(
        &self,
        range_id: RangeId,
    ) -> Result<NativeRangeReadStamp> {
        let ranges = self.inner.ranges.lock();
        let state = ranges.get(&range_id);
        require_readable_gate(&self.inner, state)?;
        Ok(NativeRangeReadStamp {
            inner: Arc::downgrade(&self.inner),
            range_id,
            range_generation: state.map(|s| s.observation_generation),
            database_generation: *self.inner.observation_database_generation.lock(),
        })
    }
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
        *self.inner.observation_database_generation.lock() = Uuid::now_v7();
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
                && match claim.controlled_lease {
                    Some(id) => state.capture_pin.is_none() && state.controlled.contains(&id),
                    None => false,
                }
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
        state.observation_generation = Uuid::now_v7();
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
        state.observation_generation = Uuid::now_v7();
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
                self.invalidate_serving_generation(range_id);
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
        state.observation_generation = Uuid::now_v7();
        state.frozen = Some((owner, lease));
        self.invalidate_serving_generation(range_id);
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
            RangeMutationAuthority::Frozen(_) => Err(invalid(
                "frozen is read-only; writes require owned controlled admission",
            )),
            RangeMutationAuthority::Controlled(permit) => {
                self.validate_controlled(range_id, permit)
            }
        }
    }

    pub(crate) fn try_source_capture_read_pin(
        &self,
        range_id: RangeId,
        permit: &FrozenRangeMutationPermit,
    ) -> Result<SourceCaptureReadPin> {
        if !Arc::ptr_eq(&self.inner, &permit.inner) || permit.range_id != range_id {
            return Err(invalid("source reader belongs to another range/DB"));
        }
        let mut ranges = self.inner.ranges.lock();
        let state = ranges
            .get_mut(&range_id)
            .ok_or_else(|| invalid("source reader freeze absent"))?;
        if state.poisoned
            || state.frozen_owner_dropped
            || state.frozen != Some((permit.owner, permit.lease))
            || !state.active.is_empty()
            || !state.controlled.is_empty()
            || state.capture_pin.is_some()
        {
            return Err(invalid(
                "source capture requires exclusive quiescent owned freeze",
            ));
        }
        let lease = Uuid::now_v7();
        state.capture_pin = Some(lease);
        Ok(SourceCaptureReadPin {
            inner: Arc::clone(&self.inner),
            range_id,
            owner: permit.owner,
            owner_lease: permit.lease,
            lease,
        })
    }
    pub(crate) fn validate_source_capture_read_pin(
        &self,
        range_id: RangeId,
        pin: &SourceCaptureReadPin,
    ) -> Result<()> {
        if !Arc::ptr_eq(&self.inner, &pin.inner) || pin.range_id != range_id {
            return Err(invalid("source reader belongs to another range/DB"));
        }
        let ranges = self.inner.ranges.lock();
        let state = ranges
            .get(&range_id)
            .ok_or_else(|| invalid("source reader state absent"))?;
        if state.poisoned
            || state.frozen != Some((pin.owner, pin.owner_lease))
            || state.capture_pin != Some(pin.lease)
            || !state.active.is_empty()
            || !state.controlled.is_empty()
        {
            return Err(invalid("source reader is no longer exclusive"));
        }
        Ok(())
    }

    pub(crate) fn try_controlled_mutation(
        &self,
        permit: &FrozenRangeMutationPermit,
    ) -> Result<ControlledFrozenMutationPermit> {
        if !Arc::ptr_eq(&self.inner, &permit.inner) {
            return Err(invalid("controlled freeze belongs to another DB"));
        }
        let mut ranges = self.inner.ranges.lock();
        let state = ranges
            .get_mut(&permit.range_id)
            .ok_or_else(|| invalid("controlled freeze absent"))?;
        if state.poisoned
            || state.frozen_owner_dropped
            || state.frozen != Some((permit.owner, permit.lease))
            || !state.active.is_empty()
            || state.capture_pin.is_some()
            || state.controlled.len() >= MAX_GATE_RANGES
            || self.inner.range_active.load(Ordering::Relaxed) >= MAX_GATE_RANGES
        {
            return Err(invalid("controlled writer freeze invalid or capacity busy"));
        }
        let lease = Uuid::now_v7();
        state.observation_generation = Uuid::now_v7();
        state.controlled.insert(lease);
        self.inner.range_active.fetch_add(1, Ordering::Relaxed);
        Ok(ControlledFrozenMutationPermit {
            inner: Arc::clone(&self.inner),
            range_id: permit.range_id,
            owner: permit.owner,
            owner_lease: permit.lease,
            lease,
        })
    }
    pub(crate) fn validate_controlled(
        &self,
        range_id: RangeId,
        permit: &ControlledFrozenMutationPermit,
    ) -> Result<()> {
        if !Arc::ptr_eq(&self.inner, &permit.inner) || permit.range_id != range_id {
            return Err(invalid("controlled permit belongs to another range/DB"));
        }
        let ranges = self.inner.ranges.lock();
        let state = ranges
            .get(&range_id)
            .ok_or_else(|| invalid("controlled state absent"))?;
        if state.poisoned
            || state.frozen != Some((permit.owner, permit.owner_lease))
            || !state.controlled.contains(&permit.lease)
            || state.capture_pin.is_some()
        {
            return Err(invalid("controlled lease is no longer active"));
        }
        Ok(())
    }
    pub(crate) fn invalidate_serving_generation(&self, range_id: RangeId) {
        if let Some((_, old)) = self.inner.serving_generations.lock().remove(&range_id) {
            if let Some(old) = old.upgrade() {
                old.store(false, Ordering::Release);
            }
        }
    }
    pub(crate) fn invalidate_all_serving_generations(&self) {
        let mut slots = self.inner.serving_generations.lock();
        for (_, weak) in slots.values() {
            if let Some(live) = weak.upgrade() {
                live.store(false, Ordering::Release);
            }
        }
        slots.clear();
    }
    pub(crate) fn bind_serving_generation(
        &self,
        range_id: RangeId,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<RangeServingGeneration> {
        self.validate_quiescent_frozen(range_id, frozen)?;
        let mut slots = self.inner.serving_generations.lock();
        slots.retain(|_, (_, weak)| weak.strong_count() > 0);
        if !slots.contains_key(&range_id) && slots.len() >= MAX_GATE_RANGES {
            return Err(invalid("native serving generation bound reached"));
        }
        if let Some((_, old)) = slots.remove(&range_id) {
            if let Some(old) = old.upgrade() {
                old.store(false, Ordering::Release);
            }
        }
        let generation = Uuid::now_v7();
        let live = Arc::new(AtomicBool::new(true));
        slots.insert(range_id, (generation, Arc::downgrade(&live)));
        Ok(RangeServingGeneration {
            inner: Arc::downgrade(&self.inner),
            range_id,
            generation,
            live,
        })
    }
    pub(crate) fn validate_quiescent_frozen(
        &self,
        range_id: RangeId,
        permit: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.validate_frozen(range_id, permit)?;
        let ranges = self.inner.ranges.lock();
        let state = ranges
            .get(&range_id)
            .ok_or_else(|| invalid("frozen state absent"))?;
        if !state.controlled.is_empty() {
            return Err(invalid("controlled WAL/native callback is still active"));
        }
        Ok(())
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
            || state.frozen_owner_dropped
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
    pub(crate) fn try_rotate_owner(&mut self, new_owner: Uuid) -> Result<()> {
        if new_owner.is_nil() {
            return Err(invalid("new frozen owner is nil"));
        }
        let mut ranges = self.inner.ranges.lock();
        let state = ranges
            .get_mut(&self.range_id)
            .ok_or_else(|| invalid("frozen state absent"))?;
        if state.poisoned
            || state.frozen_owner_dropped
            || state.frozen != Some((self.owner, self.lease))
            || !state.controlled.is_empty()
            || state.capture_pin.is_some()
            || !state.active.is_empty()
        {
            return Err(invalid(
                "frozen owner rotation waits for actual controlled callbacks",
            ));
        }
        state.frozen = Some((new_owner, self.lease));
        self.owner = new_owner;
        Ok(())
    }
}

fn remove_idle(ranges: &mut BTreeMap<RangeId, RangeState>, range_id: RangeId) {
    if ranges.get(&range_id).is_some_and(|state| {
        !state.poisoned
            && state.active.is_empty()
            && state.pending.is_none()
            && state.frozen.is_none()
            && state.controlled.is_empty()
            && state.capture_pin.is_none()
            && !state.frozen_owner_dropped
    }) {
        ranges.remove(&range_id);
    }
}

impl RangeMutationAuthority<'_> {
    pub(crate) fn range_id(&self) -> RangeId {
        match self {
            Self::Mutation(permit) => permit.range_id,
            Self::Frozen(permit) => permit.range_id,
            Self::Controlled(permit) => permit.range_id,
        }
    }
    pub(crate) fn claim(&self) -> RangeMutationClaim {
        match self {
            Self::Mutation(permit) => RangeMutationClaim {
                inner: Arc::clone(&permit.inner),
                range_id: permit.range_id,
                lease: permit.lease,
                frozen_owner: None,
                controlled_lease: None,
            },
            Self::Frozen(permit) => RangeMutationClaim {
                inner: Arc::clone(&permit.inner),
                range_id: permit.range_id,
                lease: permit.lease,
                frozen_owner: Some(permit.owner),
                controlled_lease: None,
            },
            Self::Controlled(permit) => RangeMutationClaim {
                inner: Arc::clone(&permit.inner),
                range_id: permit.range_id,
                lease: permit.owner_lease,
                frozen_owner: Some(permit.owner),
                controlled_lease: Some(permit.lease),
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
                if state.controlled.is_empty() && state.capture_pin.is_none() {
                    state.frozen = None;
                    state.frozen_owner_dropped = false;
                } else {
                    state.frozen_owner_dropped = true;
                }
            }
        }
        remove_idle(&mut ranges, self.range_id);
        self.inner.changed.notify_all();
    }
}

impl SourceCaptureReadPin {
    pub(crate) fn range_id(&self) -> RangeId {
        self.range_id
    }
    pub(crate) fn owner(&self) -> Uuid {
        self.owner
    }
}
impl Drop for SourceCaptureReadPin {
    fn drop(&mut self) {
        let mut ranges = self.inner.ranges.lock();
        if let Some(state) = ranges.get_mut(&self.range_id) {
            if state.frozen == Some((self.owner, self.owner_lease))
                && state.capture_pin == Some(self.lease)
            {
                state.capture_pin = None;
                if state.frozen_owner_dropped && state.controlled.is_empty() {
                    state.frozen = None;
                    state.frozen_owner_dropped = false;
                }
            } else {
                state.poisoned = true;
            }
        }
        remove_idle(&mut ranges, self.range_id);
        self.inner.changed.notify_all();
    }
}

impl Drop for ControlledFrozenMutationPermit {
    fn drop(&mut self) {
        let mut ranges = self.inner.ranges.lock();
        if let Some(state) = ranges.get_mut(&self.range_id) {
            if state.frozen == Some((self.owner, self.owner_lease))
                && state.controlled.remove(&self.lease)
            {
                self.inner.range_active.fetch_sub(1, Ordering::Relaxed);
                if state.frozen_owner_dropped
                    && state.controlled.is_empty()
                    && state.capture_pin.is_none()
                {
                    state.frozen = None;
                    state.frozen_owner_dropped = false;
                }
            } else {
                state.poisoned = true;
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
    fn source_read_pin_atomically_excludes_all_frozen_writers() {
        let gate = RangeMutationGate::default();
        let id = range(1);
        let mut frozen = gate.try_freeze(id, Uuid::now_v7()).unwrap();
        let direct = RangeMutationAuthority::Frozen(&frozen).claim();
        assert!(
            gate.validate_claim(&direct).is_err(),
            "direct frozen claims are never writers"
        );
        let controlled = gate.try_controlled_mutation(&frozen).unwrap();
        assert!(gate.try_source_capture_read_pin(id, &frozen).is_err());
        drop(controlled);
        let pin = gate.try_source_capture_read_pin(id, &frozen).unwrap();
        gate.validate_frozen(id, &frozen).unwrap();
        gate.validate_source_capture_read_pin(id, &pin).unwrap();
        assert!(gate
            .validate(id, &RangeMutationAuthority::Frozen(&frozen))
            .is_err());
        assert!(gate.try_controlled_mutation(&frozen).is_err());
        assert!(frozen.try_rotate_owner(Uuid::now_v7()).is_err());
        drop(frozen);
        assert!(gate.try_enter(id).is_err());
        gate.validate_source_capture_read_pin(id, &pin).unwrap();
        drop(pin);
        gate.try_enter(id).unwrap();
    }

    #[test]
    fn controlled_callback_keeps_tombstone_frozen_after_owner_drop() {
        let gate = RangeMutationGate::default();
        let id = range(1);
        let frozen = gate.try_freeze(id, Uuid::now_v7()).unwrap();
        let controlled = gate.try_controlled_mutation(&frozen).unwrap();
        let claim = RangeMutationAuthority::Controlled(&controlled).claim();
        assert!(gate.validate_quiescent_frozen(id, &frozen).is_err());
        drop(frozen);
        gate.validate_claim(&claim).unwrap();
        assert!(gate.try_enter(id).is_err());
        assert!(gate.try_enter_database().is_err());
        let (finish, wait) = mpsc::channel();
        let callback = thread::spawn(move || {
            wait.recv_timeout(Duration::from_secs(1)).unwrap();
            drop(controlled);
        });
        assert!(gate.try_freeze(id, Uuid::now_v7()).is_err());
        finish.send(()).unwrap();
        callback.join().unwrap();
        assert!(gate.validate_claim(&claim).is_err());
        gate.try_enter(id).unwrap();
    }
    #[test]
    fn controlled_writer_prevents_rotation_and_quiescent_readiness() {
        let gate = RangeMutationGate::default();
        let id = range(1);
        let mut frozen = gate.try_freeze(id, Uuid::now_v7()).unwrap();
        let old = RangeMutationAuthority::Frozen(&frozen).claim();
        let controlled = gate.try_controlled_mutation(&frozen).unwrap();
        assert!(frozen.try_rotate_owner(Uuid::now_v7()).is_err());
        assert!(gate.validate_quiescent_frozen(id, &frozen).is_err());
        drop(controlled);
        gate.validate_quiescent_frozen(id, &frozen).unwrap();
        frozen.try_rotate_owner(Uuid::now_v7()).unwrap();
        assert!(gate.validate_claim(&old).is_err());
        gate.validate_frozen(id, &frozen).unwrap();
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
        assert!(gate.validate_claim(&frozen_claim).is_err());
        let controlled = gate.try_controlled_mutation(&frozen).unwrap();
        let controlled_claim = RangeMutationAuthority::Controlled(&controlled).claim();
        gate.validate_claim(&controlled_claim).unwrap();
        drop(controlled);
        drop(frozen);
        let replacement = gate.try_freeze(range(1), Uuid::now_v7()).unwrap();
        assert!(gate.validate_claim(&frozen_claim).is_err());
        drop(replacement);
    }
}
