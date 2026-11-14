//! Genuine N→A transfer for the original Registered remote target. No WAL.
use super::*;
use crate::distribution_native_host::{
    NativeRegisteredRemoteInstallPreflight, NativeRegisteredRemoteTransferRequest,
};
use crate::distribution_range_image::NativeRemoteImageStep;

pub(super) struct RegisteredRemoteFreezeCustody {
    scope: RecoveryFenceScope,
    preflight: Arc<NativeRegisteredRemoteInstallPreflight>,
    frozen: FrozenRangeMutationPermit,
}
impl std::fmt::Debug for RegisteredRemoteFreezeCustody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredRemoteFreezeCustody")
            .field("range", &self.scope.range_id)
            .field("attempt", &self.preflight.attempt_id())
            .finish_non_exhaustive()
    }
}
/// No Clone, no raw permit extraction, no automatic A→N return. The actual
/// runtime independently retains the same cell, including the exact preflight,
/// so dropping an RPC/lease cannot release or erase this transferred owner.
pub(crate) struct RegisteredRemoteImageInstallFreeze {
    runtime: Arc<RangeRecoveryRuntime>,
    custody: Option<Arc<RegisteredRemoteFreezeCustody>>,
}
impl std::fmt::Debug for RegisteredRemoteImageInstallFreeze {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.custody, f)
    }
}
impl RegisteredRemoteImageInstallFreeze {
    fn custody(&self) -> Result<&Arc<RegisteredRemoteFreezeCustody>> {
        self.custody.as_ref().ok_or_else(|| {
            invalid("Registered actual A freeze already consumed by original joint release")
        })
    }
    pub(crate) fn as_frozen(&self) -> &FrozenRangeMutationPermit {
        &self
            .custody
            .as_ref()
            .expect("original A consumed only by actual release")
            .frozen
    }
    pub(crate) fn preflight(&self) -> &Arc<NativeRegisteredRemoteInstallPreflight> {
        &self
            .custody
            .as_ref()
            .expect("original A consumed only by actual release")
            .preflight
    }
    pub(crate) fn try_as_frozen(&self) -> Result<&FrozenRangeMutationPermit> {
        let custody = self.custody()?;
        custody.preflight.require_original_process()?;
        if custody.frozen.owner() != custody.preflight.attempt_id() {
            return Err(invalid("Registered remote lease changed actual A owner"));
        }
        Ok(&custody.frozen)
    }
    pub(crate) fn require_retained(&self) -> Result<()> {
        let custody = self.custody()?;
        custody.preflight.require_original_process()?;
        let sessions = self
            .runtime
            .sessions
            .try_lock()
            .ok_or_else(|| invalid("Registered transferred custody busy"))?;
        let range = custody.scope.range_id;
        let original = sessions
            .current
            .get(&range)
            .ok_or_else(|| invalid("Registered original session ended"))?;
        if !original.scope.same_session(&custody.scope)
            || original.frozen.is_some()
            || sessions
                .registered_remote
                .get(&range)
                .is_none_or(|actual| !Arc::ptr_eq(actual, custody))
        {
            return Err(invalid("Registered exact transferred custody changed"));
        }
        self.runtime
            .gate
            .validate_quiescent_frozen(range, &custody.frozen)
    }
    /// The actual original runtime A can leave custody only after a distinct
    /// complete fresh joint release admission and exact actual image/native
    /// activation checks. No A->N rotation, Drop-based release or new permit.
    pub(crate) fn release_after_registered_joint_activation(
        &mut self,
        target: &Arc<crate::distribution_native_host::NativeRegisteredRemoteJointTarget>,
        admission: &crate::distribution_range_handoff::NativeJointReleaseAdmission,
    ) -> Result<()> {
        admission.require_live()?;
        self.require_retained()?;
        let custody = self.custody()?;
        target.require_original_preflight(&custody.preflight)?;
        admission.require_recipient(&self.runtime.service, admission.native())?;
        let service = self
            .runtime
            .service
            .try_lock()
            .ok_or_else(|| invalid("Registered joint release actual service busy"))?;
        service.require_fresh_registered_joint_image_release(target, &custody.frozen, admission)?;
        let mut sessions = self
            .runtime
            .sessions
            .try_lock()
            .ok_or_else(|| invalid("Registered joint release runtime busy"))?;
        let range = custody.scope.range_id;
        let original = sessions
            .current
            .get(&range)
            .ok_or_else(|| invalid("Registered release lost original runtime session"))?;
        if !original.scope.same_session(&custody.scope)
            || original.frozen.is_some()
            || original.native_prepared.is_some()
            || original.image_preflight.is_some()
            || sessions.active.contains(&range)
            || sessions.installing.contains_key(&range)
            || sessions.replacing.contains_key(&range)
            || sessions
                .registered_remote
                .get(&range)
                .is_none_or(|actual| !Arc::ptr_eq(actual, custody))
            || Arc::strong_count(custody) != 2
        {
            return Err(invalid(
                "Registered release still has changed/busy actual runtime custody",
            ));
        }
        admission.require_live()?;
        // Full actual physical/native/owner proof remains lexical with the
        // actual service and runtime session guards. No MStore acquisition or
        // filesystem operation occurs under the short version publication gate.
        let database_arc = service.database();
        let database = database_arc
            .try_read()
            .ok_or_else(|| invalid("Registered release actual DB busy"))?;
        let physical = admission.native().historical_cut_scope(&database)?;
        admission.require_local_with_scope(
            &service,
            &physical,
            crate::distribution_data::NativeJointReleaseOwnerView::Frozen(&custody.frozen),
        )?;
        let retired = {
            let _version = admission.native_publication_guard()?;
            // Every fallible operation precedes actual consumption. Original
            // host activation survives; no destructor runs under final gate.
            let runtime_custody = sessions.registered_remote.remove(&range);
            let original_session = sessions.current.remove(&range);
            let lease_custody = self.custody.take();
            (runtime_custody, original_session, lease_custody)
        };
        drop(retired);
        Ok(())
    }
}

impl RangeRecoveryRuntime {
    pub(crate) fn transfer_registered_remote_install(
        self: &Arc<Self>,
        preflight: Arc<NativeRegisteredRemoteInstallPreflight>,
        request: &NativeRegisteredRemoteTransferRequest,
        frame: &NativeRemoteImageStep<'_>,
        parent: &CancellationToken,
    ) -> Result<RegisteredRemoteImageInstallFreeze> {
        parent.check()?;
        preflight.require_runtime(self)?;
        preflight.require_request(request, frame)?;
        let scope = preflight.scope().clone();
        if preflight.attempt_id() == scope.freeze_owner()? {
            return Err(invalid("Registered attempt cannot reuse original N owner"));
        }
        let _operation = self.reserve(scope.range_id)?;
        let mut sessions = self
            .sessions
            .try_lock()
            .ok_or_else(|| invalid("Registered transfer session busy"))?;
        if sessions.installing.contains_key(&scope.range_id)
            || sessions.replacing.contains_key(&scope.range_id)
            || sessions.registered_remote.contains_key(&scope.range_id)
        {
            return Err(invalid(
                "Registered range already has another actual installer owner",
            ));
        }
        let session = sessions
            .current
            .get_mut(&scope.range_id)
            .ok_or_else(|| invalid("Registered original runtime session missing"))?;
        if !session.scope.same_session(&scope)
            || session.native_prepared.is_some()
            || session.image_preflight.is_some()
        {
            return Err(invalid(
                "Registered transfer changed original CLOSED runtime session",
            ));
        }
        let arc = session
            .frozen
            .as_mut()
            .ok_or_else(|| invalid("Registered actual freeze already leased"))?;
        let frozen = Arc::get_mut(arc).ok_or_else(|| {
            invalid("Registered transfer waits for actual Frozen callbacks to drain")
        })?;
        self.gate
            .validate_quiescent_frozen(scope.range_id, frozen)?;
        preflight.require_before_rotation(request, frame, frozen)?;
        #[cfg(test)]
        before_rotation::fire();
        // Revalidate after the adversarial actual boundary as well. No hook
        // or writer state may change between preflight and owner rotation.
        preflight.require_before_rotation(request, frame, frozen)?;
        preflight.require_request(request, frame)?;
        parent.check()?;
        frozen.try_rotate_owner(preflight.attempt_id())?;
        // No fallible work after rotation. Host retained the exact attempt
        // before entering this method; runtime now retains the actual A permit.
        let frozen = Arc::try_unwrap(session.frozen.take().unwrap())
            .expect("unique Registered freeze checked under actual session guard");
        let custody = Arc::new(RegisteredRemoteFreezeCustody {
            scope: scope.clone(),
            preflight,
            frozen,
        });
        sessions
            .registered_remote
            .insert(scope.range_id, custody.clone());
        Ok(RegisteredRemoteImageInstallFreeze {
            runtime: self.clone(),
            custody: Some(custody),
        })
    }
}
#[cfg(test)]
mod before_rotation {
    use std::cell::RefCell;
    type Cut = Box<dyn FnOnce()>;
    thread_local! { static CUT: RefCell<Option<Cut>> = const { RefCell::new(None) }; }
    pub(crate) struct Guard(Option<Cut>);
    impl Drop for Guard {
        fn drop(&mut self) {
            CUT.with(|c| {
                c.replace(self.0.take());
            });
        }
    }
    pub(crate) fn install(cut: Cut) -> Guard {
        Guard(CUT.with(|c| c.replace(Some(cut))))
    }
    pub(super) fn fire() {
        if let Some(cut) = CUT.with(|c| c.borrow_mut().take()) {
            cut();
        }
    }
}
#[cfg(test)]
pub(crate) use before_rotation::install as test_before_registered_remote_rotation;
