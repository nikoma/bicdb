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
    custody: Arc<RegisteredRemoteFreezeCustody>,
}
impl std::fmt::Debug for RegisteredRemoteImageInstallFreeze {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.custody.as_ref(), f)
    }
}
impl RegisteredRemoteImageInstallFreeze {
    pub(crate) fn as_frozen(&self) -> &FrozenRangeMutationPermit {
        &self.custody.frozen
    }
    pub(crate) fn preflight(&self) -> &Arc<NativeRegisteredRemoteInstallPreflight> {
        &self.custody.preflight
    }
    pub(crate) fn try_as_frozen(&self) -> Result<&FrozenRangeMutationPermit> {
        self.custody.preflight.require_original_process()?;
        if self.custody.frozen.owner() != self.custody.preflight.attempt_id() {
            return Err(invalid("Registered remote lease changed actual A owner"));
        }
        Ok(&self.custody.frozen)
    }
    pub(crate) fn require_retained(&self) -> Result<()> {
        self.custody.preflight.require_original_process()?;
        let sessions = self
            .runtime
            .sessions
            .try_lock()
            .ok_or_else(|| invalid("Registered transferred custody busy"))?;
        let range = self.custody.scope.range_id;
        let original = sessions
            .current
            .get(&range)
            .ok_or_else(|| invalid("Registered original session ended"))?;
        if !original.scope.same_session(&self.custody.scope)
            || original.frozen.is_some()
            || sessions
                .registered_remote
                .get(&range)
                .is_none_or(|actual| !Arc::ptr_eq(actual, &self.custody))
        {
            return Err(invalid("Registered exact transferred custody changed"));
        }
        self.runtime
            .gate
            .validate_quiescent_frozen(range, &self.custody.frozen)
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
            custody,
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
