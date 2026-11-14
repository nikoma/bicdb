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
    observation_range_generations: Mutex<BTreeMap<RangeId, Uuid>>,
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
        if inner
            .observation_range_generations
            .lock()
            .get(&self.range_id)
            .copied()
            != self.range_generation
            || *inner.observation_database_generation.lock() != self.database_generation
        {
            return Err(invalid("native read observation generation changed"));
        }
        Ok(())
    }
}
impl RangeServingGeneration {
    pub(crate) fn id(&self) -> Result<Uuid> {
        self.require_current()?;
        Ok(self.generation)
    }
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

/// Actual gate acquired without waiting BEFORE the caller's final bounded
/// authority checks. Fields remain private; this grants only the same owned
/// permit's infallible owner publication, never native WAL or Serving.
pub(crate) struct FrozenRangeOwnerPublication<'a> {
    inner: &'a Arc<Inner>,
    range_id: RangeId,
    original_owner: &'a mut Uuid,
    lease: Uuid,
    next_owner: Uuid,
    state: parking_lot::MappedMutexGuard<'a, RangeState>,
}
impl std::fmt::Debug for FrozenRangeOwnerPublication<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrozenRangeOwnerPublication")
            .field("range", &self.range_id)
            .finish_non_exhaustive()
    }
}
impl FrozenRangeOwnerPublication<'_> {
    /// The actual map state and original permit were validated under this
    /// still-held gate. No fallible operation, allocation, callback or gate
    /// reacquisition occurs between the caller's final checks and this update.
    pub(crate) fn publish(mut self) {
        self.state.frozen = Some((self.next_owner, self.lease));
        *self.original_owner = self.next_owner;
    }
}

/// Read-only original gate publication borrow. Holding this opaque value
/// permits final request checks and same-A view retention without a later
/// mutex wait. It exposes no state, mutable permit, extraction or callback.
/// An actual source read pin may remain: restoring custody is not a writer
/// admission, and the existing quiescent-freeze contract permits that reader.
pub(crate) struct FrozenRangeCurrentPublication<'a> {
    _inner: &'a Arc<Inner>,
    range_id: RangeId,
    _owner: Uuid,
    _lease: Uuid,
    _state: parking_lot::MappedMutexGuard<'a, RangeState>,
}
impl std::fmt::Debug for FrozenRangeCurrentPublication<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrozenRangeCurrentPublication")
            .field("range", &self.range_id)
            .finish_non_exhaustive()
    }
}

fn require_frozen_publication_state(
    state: Option<&RangeState>,
    owner: Uuid,
    lease: Uuid,
) -> Result<()> {
    let state = state.ok_or_else(|| invalid("frozen publication state absent"))?;
    if state.poisoned
        || state.frozen_owner_dropped
        || state.frozen != Some((owner, lease))
        || state.pending.is_some()
        || !state.active.is_empty()
        || !state.controlled.is_empty()
    {
        return Err(invalid(
            "frozen publication is not the actual quiescent owner/lease",
        ));
    }
    Ok(())
}
fn require_frozen_owner_publication_state(
    state: Option<&RangeState>,
    owner: Uuid,
    lease: Uuid,
) -> Result<()> {
    require_frozen_publication_state(state, owner, lease)?;
    if state.is_some_and(|state| state.capture_pin.is_some()) {
        return Err(invalid(
            "frozen owner publication still has an actual source read pin",
        ));
    }
    Ok(())
}

/// Non-owning identity DATA for a retained bounded scan. Holding a Weak to the
/// PERMIT Arc would prevent its legitimate unique N->A rotation. This retains
/// only the gate identity and private lease nonce; it creates no exclusion,
/// mutation claim or permission and cannot be deserialized/reconstructed.
#[derive(Debug)]
pub(crate) struct FrozenRangeMutationIdentity {
    inner: Weak<Inner>,
    range_id: RangeId,
    owner: Uuid,
    lease: Uuid,
}
impl FrozenRangeMutationIdentity {
    /// Compare the original private gate/range/owner/lease while that ACTUAL
    /// gate is already held. Unlike require_same_current this never relocks it.
    pub(crate) fn require_in_owner_publication(
        &self,
        actual: &FrozenRangeOwnerPublication<'_>,
    ) -> Result<()> {
        if !Weak::ptr_eq(&self.inner, &Arc::downgrade(actual.inner))
            || self.range_id != actual.range_id
            || self.owner != *actual.original_owner
            || self.lease != actual.lease
        {
            return Err(invalid(
                "frozen publication changed original private gate/range/owner/lease",
            ));
        }
        // The private map borrow prevents any intervening gate mutation.
        require_frozen_owner_publication_state(Some(&actual.state), self.owner, self.lease)
    }
    pub(crate) fn require_same_current(&self, permit: &FrozenRangeMutationPermit) -> Result<()> {
        if !Weak::ptr_eq(&self.inner, &Arc::downgrade(&permit.inner))
            || self.inner.upgrade().is_none()
            || self.range_id != permit.range_id
            || self.owner != permit.owner
            || self.lease != permit.lease
        {
            return Err(invalid(
                "retained scan changed actual frozen gate/lease/owner",
            ));
        }
        RangeMutationGate {
            inner: permit.inner.clone(),
        }
        .validate_quiescent_frozen(self.range_id, permit)
    }
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

/// Borrowed actual local gate slots for one concrete final arm. Captured
/// BEFORE metadata publication guard, never a quorum or Serving grant.
pub(crate) struct NativeRangeArmPublicationBorrow<'a> {
    _ranges: parking_lot::MutexGuard<'a, BTreeMap<RangeId, RangeState>>,
    _generations: parking_lot::MutexGuard<'a, BTreeMap<RangeId, (Uuid, Weak<AtomicBool>)>>,
}
impl RangeMutationGate {
    /// Memory-only diagnostic of the ACTUAL gate; never a permit, reset or
    /// serving admission. Used to distinguish Option removal from pin release.
    #[cfg(test)]
    pub(crate) fn test_source_capture_pin_state(
        &self,
        range: RangeId,
    ) -> Result<serde_json::Value> {
        let ranges = self
            .inner
            .ranges
            .try_lock()
            .ok_or_else(|| invalid("capture-pin diagnostic gate busy"))?;
        Ok(match ranges.get(&range) {
            Some(state) => {
                serde_json::json!({"capture_pin":state.capture_pin,"frozen":state.frozen,
                "frozen_owner_dropped":state.frozen_owner_dropped,"poisoned":state.poisoned,
                "active":state.active.len(),"controlled":state.controlled.len()})
            }
            None => serde_json::Value::Null,
        })
    }
    pub(crate) fn borrow_native_arm_publication<'a>(
        &'a self,
        range: RangeId,
        generation: &RangeServingGeneration,
        already_armed: bool,
    ) -> Result<NativeRangeArmPublicationBorrow<'a>> {
        if range != generation.range_id
            || !Weak::ptr_eq(&generation.inner, &Arc::downgrade(&self.inner))
        {
            return Err(invalid("native arm changed actual local gate/generation"));
        }
        let ranges = self
            .inner
            .ranges
            .try_lock()
            .ok_or_else(|| invalid("native arm range slots busy"))?;
        if !already_armed {
            require_readable_gate(&self.inner, ranges.get(&range))?;
        }
        let generations = self
            .inner
            .serving_generations
            .try_lock()
            .ok_or_else(|| invalid("native arm generation slots busy"))?;
        if !generation.live.load(Ordering::Acquire)
            || generations
                .get(&range)
                .is_none_or(|(id, _)| *id != generation.generation)
        {
            return Err(invalid("native arm generation invalidated"));
        }
        Ok(NativeRangeArmPublicationBorrow {
            _ranges: ranges,
            _generations: generations,
        })
    }
    // Called only under ranges, by actual mutator admission. Retain stamps
    // after idle RangeState removal to prevent None -> writer -> None ABA.
    // Reads never allocate. Exhaustion refuses new mutation identities.
    fn rotate_native_read_generation(&self, range_id: RangeId) -> Result<()> {
        let mut generations = self.inner.observation_range_generations.lock();
        if !generations.contains_key(&range_id) && generations.len() >= MAX_GATE_RANGES {
            return Err(invalid("native read invalidation range bound reached"));
        }
        generations.insert(range_id, Uuid::now_v7());
        Ok(())
    }
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
            range_generation: self
                .inner
                .observation_range_generations
                .lock()
                .get(&range_id)
                .copied(),
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
        self.rotate_native_read_generation(range_id)?;
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
        self.rotate_native_read_generation(range_id)?;
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
        self.rotate_native_read_generation(range_id)?;
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
        self.rotate_native_read_generation(permit.range_id)?;
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
    /// Acquire the actual gate NONBLOCKING before the caller's final Role,
    /// OWN, frame, process and cancellation checks. No authority is inferred
    /// here; the opaque returned guard supports only this original N->A update.
    pub(crate) fn try_owner_publication(
        &mut self,
        next_owner: Uuid,
    ) -> Result<FrozenRangeOwnerPublication<'_>> {
        if next_owner.is_nil() {
            return Err(invalid("new frozen publication owner is nil"));
        }
        let range_id = self.range_id;
        let lease = self.lease;
        let inner = &self.inner;
        let owner = &mut self.owner;
        let ranges = inner
            .ranges
            .try_lock()
            .ok_or_else(|| invalid("frozen owner publication gate busy"))?;
        require_frozen_owner_publication_state(ranges.get(&range_id), *owner, lease)?;
        let state = parking_lot::MutexGuard::map(ranges, |ranges| {
            ranges
                .get_mut(&range_id)
                .expect("validated actual gate state stays held")
        });
        Ok(FrozenRangeOwnerPublication {
            inner,
            range_id,
            original_owner: owner,
            lease,
            next_owner,
            state,
        })
    }
    /// Hold this original immutable A gate across final memory checks and
    /// actual view retention. Busy/nonquiescent states refuse without waiting.
    pub(crate) fn try_current_publication(&self) -> Result<FrozenRangeCurrentPublication<'_>> {
        let range_id = self.range_id;
        let ranges = self
            .inner
            .ranges
            .try_lock()
            .ok_or_else(|| invalid("frozen current publication gate busy"))?;
        require_frozen_publication_state(ranges.get(&range_id), self.owner, self.lease)?;
        let state = parking_lot::MutexGuard::map(ranges, |ranges| {
            ranges
                .get_mut(&range_id)
                .expect("validated actual current gate state stays held")
        });
        Ok(FrozenRangeCurrentPublication {
            _inner: &self.inner,
            range_id,
            _owner: self.owner,
            _lease: self.lease,
            _state: state,
        })
    }
    pub(crate) fn observe_identity(&self) -> Result<FrozenRangeMutationIdentity> {
        let point = FrozenRangeMutationIdentity {
            inner: Arc::downgrade(&self.inner),
            range_id: self.range_id,
            owner: self.owner,
            lease: self.lease,
        };
        point.require_same_current(self)?;
        Ok(point)
    }
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
    #[test]
    fn frozen_publication_owner_guard_updates_only_original_after_checks() {
        let gate = RangeMutationGate::default();
        let original = Uuid::now_v7();
        let next = Uuid::now_v7();
        let mut frozen = Arc::new(gate.try_freeze(range(1), original).unwrap());
        let identity = frozen.observe_identity().unwrap();
        let actual = Arc::get_mut(&mut frozen).expect("identity must not pin the permit Arc");
        let publication = actual.try_owner_publication(next).unwrap();
        identity.require_in_owner_publication(&publication).unwrap();
        publication.publish();
        assert_eq!(frozen.owner(), next);
        gate.validate_quiescent_frozen(range(1), &frozen).unwrap();
        assert!(identity.require_same_current(&frozen).is_err());
        assert!(gate.try_enter(range(1)).is_err());
    }
    #[test]
    fn frozen_publication_busy_gate_refuses_without_wait_or_owner_change() {
        let gate = RangeMutationGate::default();
        let original = Uuid::now_v7();
        let mut frozen = gate.try_freeze(range(1), original).unwrap();
        let blocked = gate.inner.ranges.lock();
        // This is the actual same-thread mutex hold. A blocking constructor
        // would deadlock; both real try-lock operations must return refusal.
        assert!(
            matches!(frozen.try_owner_publication(Uuid::now_v7()), Err(BicDbError::Cluster(ref e)) if e.contains("gate busy"))
        );
        assert!(
            matches!(frozen.try_current_publication(), Err(BicDbError::Cluster(ref e)) if e.contains("gate busy"))
        );
        drop(blocked);
        assert_eq!(frozen.owner(), original);
        gate.validate_quiescent_frozen(range(1), &frozen).unwrap();
    }
    #[test]
    fn frozen_publication_expired_and_canceled_requests_drop_without_rotation() {
        let gate = RangeMutationGate::default();
        let original = Uuid::now_v7();
        let mut frozen = gate.try_freeze(range(1), original).unwrap();
        let identity = frozen.observe_identity().unwrap();
        let deadline = Instant::now() + Duration::from_millis(3);
        let expired = CancellationToken::new(Arc::new(AtomicBool::new(false)), Some(deadline));
        let publication = frozen.try_owner_publication(Uuid::now_v7()).unwrap();
        while Instant::now() < deadline {
            thread::yield_now(); // genuine monotonic expiry after gate acquisition
        }
        identity.require_in_owner_publication(&publication).unwrap();
        assert!(matches!(expired.check(), Err(BicDbError::QueryTimedOut)));
        drop(publication); // caller refused its final real deadline; no publish
        let canceled = CancellationToken::new(Arc::new(AtomicBool::new(false)), None);
        let publication = frozen.try_owner_publication(Uuid::now_v7()).unwrap();
        canceled.cancel(); // actual token changes AFTER gate acquisition
        identity.require_in_owner_publication(&publication).unwrap();
        assert!(matches!(canceled.check(), Err(BicDbError::QueryCanceled)));
        drop(publication);
        let current = frozen.try_current_publication().unwrap();
        assert!(matches!(canceled.check(), Err(BicDbError::QueryCanceled)));
        drop(current);
        assert_eq!(frozen.owner(), original);
        identity.require_same_current(&frozen).unwrap();
        assert!(gate.try_enter(range(1)).is_err());
    }
    #[test]
    fn frozen_publication_identity_refuses_other_gate_range_and_refreeze() {
        let gate = RangeMutationGate::default();
        let other_gate = RangeMutationGate::default();
        let original = Uuid::now_v7();
        let frozen = gate.try_freeze(range(1), original).unwrap();
        let identity = frozen.observe_identity().unwrap();
        let mut foreign = other_gate.try_freeze(range(1), original).unwrap();
        let guard = foreign.try_owner_publication(Uuid::now_v7()).unwrap();
        assert!(identity.require_in_owner_publication(&guard).is_err());
        drop(guard);
        let mut other_range = gate.try_freeze(range(2), original).unwrap();
        let guard = other_range.try_owner_publication(Uuid::now_v7()).unwrap();
        assert!(identity.require_in_owner_publication(&guard).is_err());
        drop(guard);
        drop(frozen);
        let mut replacement = gate.try_freeze(range(1), original).unwrap();
        let guard = replacement.try_owner_publication(Uuid::now_v7()).unwrap();
        assert!(identity.require_in_owner_publication(&guard).is_err());
        drop(guard);
        assert_eq!(foreign.owner(), original);
        assert_eq!(other_range.owner(), original);
        assert_eq!(replacement.owner(), original);
        gate.validate_quiescent_frozen(range(1), &replacement)
            .unwrap();
    }
    #[test]
    fn frozen_publication_current_allows_read_pin_owner_and_controlled_write_refuse() {
        let gate = RangeMutationGate::default();
        let original = Uuid::now_v7();
        let mut frozen = gate.try_freeze(range(1), original).unwrap();
        let pin = gate.try_source_capture_read_pin(range(1), &frozen).unwrap();
        assert!(frozen.try_owner_publication(Uuid::now_v7()).is_err());
        let current = frozen.try_current_publication().unwrap();
        assert!(gate.inner.ranges.try_lock().is_none());
        drop(current);
        gate.validate_source_capture_read_pin(range(1), &pin)
            .unwrap();
        drop(pin);
        let controlled = gate.try_controlled_mutation(&frozen).unwrap();
        assert!(frozen.try_owner_publication(Uuid::now_v7()).is_err());
        assert!(frozen.try_current_publication().is_err());
        drop(controlled);
        assert!(frozen.try_owner_publication(Uuid::nil()).is_err());
        assert_eq!(frozen.owner(), original);
        let publication = frozen.try_owner_publication(Uuid::now_v7()).unwrap();
        drop(publication);
        gate.validate_quiescent_frozen(range(1), &frozen).unwrap();
    }
    #[test]
    fn frozen_publication_current_guard_holds_original_gate_until_drop() {
        let gate = RangeMutationGate::default();
        let original = Uuid::now_v7();
        let frozen = gate.try_freeze(range(1), original).unwrap();
        let mut other = gate.try_freeze(range(2), original).unwrap();
        let current = frozen.try_current_publication().unwrap();
        assert!(gate.inner.ranges.try_lock().is_none());
        assert!(other.try_owner_publication(Uuid::now_v7()).is_err());
        drop(current);
        assert_eq!(frozen.owner(), original);
        assert_eq!(other.owner(), original);
        gate.validate_quiescent_frozen(range(1), &frozen).unwrap();
        gate.validate_quiescent_frozen(range(2), &other).unwrap();
    }
    #[test]
    fn frozen_scan_identity_does_not_pin_permit_arc_and_rejects_rotated_owner() {
        let gate = RangeMutationGate::default();
        let owner = Uuid::now_v7();
        let mut frozen = Arc::new(gate.try_freeze(range(1), owner).unwrap());
        let identity = frozen.observe_identity().unwrap();
        identity.require_same_current(&frozen).unwrap();
        let exclusive = Arc::get_mut(&mut frozen)
            .expect("readonly scan identity must not retain a strong or Weak permit Arc");
        exclusive.try_rotate_owner(Uuid::now_v7()).unwrap();
        assert!(identity.require_same_current(&frozen).is_err());
    }
    #[test]
    fn frozen_scan_identity_refuses_same_owner_refreeze_and_foreign_gate() {
        let gate = RangeMutationGate::default();
        let owner = Uuid::now_v7();
        let frozen = gate.try_freeze(range(1), owner).unwrap();
        let identity = frozen.observe_identity().unwrap();
        let foreign = RangeMutationGate::default()
            .try_freeze(range(1), owner)
            .unwrap();
        assert!(identity.require_same_current(&foreign).is_err());
        drop(frozen);
        let replacement = gate.try_freeze(range(1), owner).unwrap();
        assert!(identity.require_same_current(&replacement).is_err());
    }

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

#[cfg(test)]
mod native_read_stamp_tests {
    use super::*;
    #[test]
    fn native_read_stamp_never_allocates_and_failed_writer_cannot_aba() {
        let gate = RangeMutationGate::default();
        let range = RangeId::new(1).unwrap();
        let before = gate.observe_native_read_stamp(range).unwrap();
        for _ in 0..32 {
            gate.observe_native_read_stamp(range)
                .unwrap()
                .require_current()
                .unwrap();
        }
        assert!(gate.inner.ranges.lock().is_empty());
        assert!(gate.inner.observation_range_generations.lock().is_empty());
        let permit = gate.try_enter(range).unwrap();
        assert!(before.require_current().is_err());
        assert!(gate.observe_native_read_stamp(range).is_err());
        drop(permit);
        assert!(
            gate.inner.ranges.lock().is_empty(),
            "normal idle cleanup remains intact"
        );
        assert!(
            before.require_current().is_err(),
            "idle removal must not restore None generation"
        );
        assert_eq!(gate.inner.observation_range_generations.lock().len(), 1);
        gate.observe_native_read_stamp(range)
            .unwrap()
            .require_current()
            .unwrap();
    }
    #[test]
    fn native_read_stamp_invalidates_database_freeze_and_controlled_paths() {
        let gate = RangeMutationGate::default();
        let range = RangeId::new(1).unwrap();
        let before = gate.observe_native_read_stamp(range).unwrap();
        let database = gate.try_enter_database().unwrap();
        assert!(before.require_current().is_err());
        drop(database);
        assert!(before.require_current().is_err());
        let before = gate.observe_native_read_stamp(range).unwrap();
        let frozen = gate.try_freeze(range, Uuid::now_v7()).unwrap();
        assert!(before.require_current().is_err());
        assert!(gate.observe_native_read_stamp(range).is_err());
        let controlled = gate.try_controlled_mutation(&frozen).unwrap();
        assert!(gate.observe_native_read_stamp(range).is_err());
        drop(controlled);
        drop(frozen);
        assert!(before.require_current().is_err());
        gate.observe_native_read_stamp(range)
            .unwrap()
            .require_current()
            .unwrap();
    }
}
