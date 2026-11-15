//! One-way retirement of obsolete original-phase references. The original
//! participant/status/native outcome stays host-owned; no local release or
//! Serving capability is produced by this transition.
use super::*;
/// Only this module can borrow the actual original slot. A caller cannot pass
/// a replacement Option or a clone of the original owner to the host sink.
pub(crate) struct NativeRemoteOldRetirementSlot<'a> {
    participant: &'a NativeRemoteNewParticipant,
    original: &'a mut Option<Arc<NativeJointOldObservation>>,
}
impl NativeRemoteOldRetirementSlot<'_> {
    pub(crate) fn participant(&self) -> &NativeRemoteNewParticipant {
        self.participant
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.original.is_none()
    }
    pub(crate) fn require_actual(&self, actual: &Arc<NativeJointOldObservation>) -> Result<()> {
        if self
            .original
            .as_ref()
            .is_none_or(|old| !Arc::ptr_eq(old, actual))
        {
            return Err(invalid(
                "ALL Open host entry differs from actual borrowed original OLD slot",
            ));
        }
        Ok(())
    }
    pub(crate) fn take_and_retire(&mut self) -> Option<Arc<NativeJointOldObservation>> {
        let original = self.original.take();
        self.participant
            .original_retired
            .store(true, Ordering::Release);
        original
    }
}

impl NativeRemoteNewParticipant {
    pub(crate) fn retire_original_after_all_open(
        &self,
        phase: &NativeRemoteAllOpenRetirementAdmission<'_>,
    ) -> Result<()> {
        phase.require_current()?;
        phase.require_original_participant(self)?;
        let survivor = phase
            .manifest()
            .new_members
            .contains_key(&self.snapshot.node_id);
        if survivor {
            self.require_all_open_known_decision(phase)?;
        }
        match &self.original {
            OriginalParticipant::Old {
                actual, admission, ..
            } => {
                let mut original = actual
                    .try_lock()
                    .ok_or_else(|| invalid("ALL Open original OLD slot busy"))?;
                admission.retire_remote_old_original_after_all_open(
                    &mut NativeRemoteOldRetirementSlot {
                        participant: self,
                        original: &mut original,
                    },
                    phase,
                )
            }
            OriginalParticipant::Registered {
                frozen,
                pin,
                owner,
                target,
                service,
            } => {
                if !survivor {
                    return Err(invalid("Registered target is not an actual final member"));
                }
                let owner = owner
                    .upgrade()
                    .ok_or_else(|| invalid("ALL Open original Registered owner ended"))?;
                owner.require_first_policy_attempt_lifetime()?;
                let mut original = frozen
                    .try_lock()
                    .ok_or_else(|| invalid("ALL Open original Registered freeze busy"))?;
                if self.original_retired.load(Ordering::Acquire) {
                    if original.is_some() {
                        return Err(invalid(
                            "ALL Open retired target unexpectedly recovered a freeze",
                        ));
                    }
                    return Ok(());
                }
                let freeze = original
                    .as_ref()
                    .cloned()
                    .ok_or_else(|| invalid("ALL Open original target freeze absent"))?;
                // Actual original hook/open/native identity preflight before
                // the final memory gate. Generic target current/absence does
                // not apply to a target that already has protected Open.
                {
                    let service = service
                        .try_lock()
                        .ok_or_else(|| invalid("ALL Open target service busy"))?;
                    let database = service.database();
                    let db = database
                        .try_read()
                        .ok_or_else(|| invalid("ALL Open target DB busy"))?;
                    target.require_original_point(&db, freeze.try_as_frozen()?)?;
                    service.with_writer_data_domain_store(&db, |store| {
                        phase.require_database_store(&db, store)
                    })?;
                }
                let pin = pin
                    .try_lock()
                    .ok_or_else(|| invalid("ALL Open original target pin busy"))?;
                if pin.is_some()
                    || !self.progress.target_export.load(Ordering::Acquire)
                    || !self.progress.target_materialization.load(Ordering::Acquire)
                {
                    return Err(invalid(
                        "ALL Open cannot retire an outstanding actual target read pin",
                    ));
                }
                phase.require_original_frozen_owner(
                    &self.snapshot.node_id,
                    freeze.try_as_frozen()?.owner(),
                )?;
                let current = freeze.try_as_frozen()?.try_current_publication()?;
                let removed = {
                    let _version = phase.publication_guard()?;
                    phase.require_memory_current()?;
                    self.require_lifetime_memory()?;
                    let removed = original.take();
                    self.original_retired.store(true, Ordering::Release);
                    removed
                };
                drop(current);
                drop(pin);
                drop(original);
                drop(removed);
                drop(freeze);
                Ok(())
            }
        }
    }
}
