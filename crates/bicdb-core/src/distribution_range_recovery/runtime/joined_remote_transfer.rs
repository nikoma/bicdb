//! Original Joined N -> A custody. No native/page WAL, row apply or Serving.
//! Physical preparation stays outside sessions; final use is memory only.
use super::*;
use crate::distribution_native_host::{
    NativeJoinedRangeReceiveRequest, NativeJoinedRemoteInstallPreflight,
};
use crate::distribution_range_image::NativeRemoteImageStep;

pub(super) struct JoinedRemoteFreezeCustody {
    scope: RecoveryFenceScope,
    preflight: Arc<NativeJoinedRemoteInstallPreflight>,
    // Allocate the cell before the short publication boundary. It is set
    // exactly once by the actual N take, never from a caller-provided permit.
    frozen: std::sync::OnceLock<FrozenRangeMutationPermit>,
}
impl std::fmt::Debug for JoinedRemoteFreezeCustody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinedRemoteFreezeCustody")
            .field("range", &self.scope.range_id)
            .field("attempt", &self.preflight.attempt_id())
            .field("transferred", &self.frozen.get().is_some())
            .finish_non_exhaustive()
    }
}
/// The runtime independently retains the actual preflight and A. This view
/// cannot clone/extract a permit or return A to N on Drop. Only a future genuine
/// Joined activation/release consumer may remove the runtime-owned exclusion.
pub(crate) struct JoinedRemoteImageInstallFreeze {
    runtime: Arc<RangeRecoveryRuntime>,
    custody: Option<Arc<JoinedRemoteFreezeCustody>>,
}
impl std::fmt::Debug for JoinedRemoteImageInstallFreeze {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.custody, f)
    }
}
impl JoinedRemoteImageInstallFreeze {
    fn custody(&self) -> Result<&Arc<JoinedRemoteFreezeCustody>> {
        self.custody.as_ref().ok_or_else(|| {
            invalid("Joined original A custody already consumed by its real joint release")
        })
    }
    pub(crate) fn preflight(&self) -> &Arc<NativeJoinedRemoteInstallPreflight> {
        &self
            .custody
            .as_ref()
            .expect("actual Joined A view consumed")
            .preflight
    }
    pub(crate) fn as_frozen(&self) -> &FrozenRangeMutationPermit {
        self.custody
            .as_ref()
            .expect("actual Joined A view consumed")
            .frozen
            .get()
            .expect("actual Joined A transfer incomplete")
    }
    pub(crate) fn try_as_frozen(&self) -> Result<&FrozenRangeMutationPermit> {
        let custody = self.custody()?;
        custody.preflight.require_original_process()?;
        let frozen = custody
            .frozen
            .get()
            .ok_or_else(|| invalid("Joined original A transfer has no actual consumed N"))?;
        if frozen.owner() != custody.preflight.attempt_id() {
            return Err(invalid("Joined original freeze changed actual A owner"));
        }
        Ok(frozen)
    }
    pub(crate) fn require_retained(&self) -> Result<()> {
        let custody = self.custody()?;
        custody.preflight.require_original_process()?;
        let sessions = self
            .runtime
            .sessions
            .try_lock()
            .ok_or_else(|| invalid("Joined actual A custody busy"))?;
        let range = custody.scope.range_id;
        let original = sessions
            .current
            .get(&range)
            .ok_or_else(|| invalid("Joined original runtime session absent"))?;
        if !original.scope.same_session(&custody.scope)
            || original.frozen.is_some()
            || sessions
                .joined_remote
                .get(&range)
                .is_none_or(|actual| !Arc::ptr_eq(actual, custody))
        {
            return Err(invalid("Joined original A runtime custody changed"));
        }
        let frozen = custody
            .frozen
            .get()
            .ok_or_else(|| invalid("Joined actual A custody has not consumed its unique N"))?;
        self.runtime.gate.validate_quiescent_frozen(range, frozen)
    }
}
impl RangeRecoveryRuntime {
    /// Reobtain only the SAME runtime-owned A if no host view remains. This
    /// transfers no N, grants no row/native operation and never creates an A.
    fn reobtain_joined_remote_install(
        self: &Arc<Self>,
        preflight: &Arc<NativeJoinedRemoteInstallPreflight>,
        request: &NativeJoinedRangeReceiveRequest,
        frame: &NativeRemoteImageStep<'_>,
        parent: &CancellationToken,
    ) -> Result<Option<JoinedRemoteImageInstallFreeze>> {
        preflight.require_request(request, frame)?;
        preflight.require_metadata_current(request)?;
        let mut sessions = self
            .sessions
            .try_lock()
            .ok_or_else(|| invalid("Joined actual A reacquisition busy"))?;
        let range = preflight.scope().range_id;
        let Some(actual) = sessions.joined_remote.get(&range) else {
            return Ok(None);
        };
        if !Arc::ptr_eq(&actual.preflight, preflight) {
            return Err(invalid(
                "Joined retry cannot replace its retained original A",
            ));
        }
        let original = sessions
            .current
            .get(&range)
            .ok_or_else(|| invalid("Joined retained original runtime session absent"))?;
        if !original.scope.same_session(&actual.scope) {
            return Err(invalid("Joined retained original runtime scope changed"));
        }
        if actual.frozen.get().is_none() {
            // A stopped pre-publication worker may leave its allocated cell.
            // No consumed permit exists: retain the SAME original session/N.
            if sessions.active.contains(&range) || original.frozen.is_none() {
                return Err(invalid(
                    "Joined original transfer remains in flight/unknown",
                ));
            }
            let unused = sessions.joined_remote.remove(&range);
            drop(sessions);
            drop(unused);
            return Ok(None);
        }
        if original.frozen.is_some() || Arc::strong_count(actual) != 1 {
            return Err(invalid(
                "Joined original A view is still owned; resume that host attempt",
            ));
        }
        request.require_request_memory()?;
        frame.require_request_live()?;
        parent.check()?;
        let _version = request
            .member()
            .publication_version()
            .try_publication_guard()?;
        self.gate
            .validate_quiescent_frozen(range, actual.frozen.get().unwrap())?;
        // No new capability is manufactured: clone the sole actual custody
        // cell under its map guard, then the host must retain this same view.
        Ok(Some(JoinedRemoteImageInstallFreeze {
            runtime: self.clone(),
            custody: Some(actual.clone()),
        }))
    }

    pub(crate) fn transfer_joined_remote_install(
        self: &Arc<Self>,
        preflight: Arc<NativeJoinedRemoteInstallPreflight>,
        request: &NativeJoinedRangeReceiveRequest,
        frame: &NativeRemoteImageStep<'_>,
        parent: &CancellationToken,
    ) -> Result<JoinedRemoteImageInstallFreeze> {
        parent.check()?;
        preflight.require_runtime(self)?;
        preflight.require_request(request, frame)?;
        if let Some(original) =
            self.reobtain_joined_remote_install(&preflight, request, frame, parent)?
        {
            return Ok(original);
        }
        let scope = preflight.scope().clone();
        let attempt = preflight.attempt_id();
        if attempt == scope.freeze_owner()? {
            return Err(invalid("Joined A cannot reuse original N owner"));
        }
        let _operation = self.reserve(scope.range_id)?;
        let temporary_n = self.source_capture_freeze(&scope)?;
        let custody = Arc::new(JoinedRemoteFreezeCustody {
            scope: scope.clone(),
            preflight: preflight.clone(),
            frozen: Default::default(),
        });
        // This helper holds actual physical guards but no session/MStore lock.
        // It drops temporary_n before invoking this final memory-only callback.
        preflight.with_rotation_point(request, frame, temporary_n, |point| {
            let mut sessions = self
                .sessions
                .try_lock()
                .ok_or_else(|| invalid("Joined actual transfer session busy"))?;
            if sessions.installing.contains_key(&scope.range_id)
                || sessions.replacing.contains_key(&scope.range_id)
                || sessions.registered_remote.contains_key(&scope.range_id)
                || sessions.joined_remote.contains_key(&scope.range_id)
            {
                return Err(invalid("Joined range already retains another installer"));
            }
            // Preallocate the BTreeMap node BEFORE the short version guard.
            // On a pre-rotation error, remove only this still-empty cell.
            sessions
                .joined_remote
                .insert(scope.range_id, custody.clone());
            let result = (|| -> Result<()> {
                let _version = point.publication_guard()?;
                let session = sessions
                    .current
                    .get_mut(&scope.range_id)
                    .ok_or_else(|| invalid("Joined original CLOSED session absent"))?;
                if !session.scope.same_session(&scope)
                    || session.native_prepared.is_some()
                    || session.image_preflight.is_some()
                {
                    return Err(invalid("Joined original CLOSED session changed"));
                }
                let original = session
                    .frozen
                    .as_mut()
                    .ok_or_else(|| invalid("Joined original N is already leased"))?;
                let frozen = Arc::get_mut(original)
                    .ok_or_else(|| invalid("Joined original N still has callback owners"))?;
                point.require_current_memory(frozen)?;
                parent.check()?;
                frozen.try_rotate_owner(attempt)?;
                // All fallible validation precedes the actual take. The fresh
                // preallocated cell/map retain A before returning or unwinding.
                let frozen = Arc::try_unwrap(session.frozen.take().unwrap())
                    .expect("actual unique Joined N checked under session ownership");
                custody
                    .frozen
                    .set(frozen)
                    .expect("fresh Joined custody cell is set only by its unique N take");
                Ok(())
            })();
            let unused = if result.is_err() && custody.frozen.get().is_none() {
                sessions.joined_remote.remove(&scope.range_id)
            } else {
                None
            };
            drop(sessions);
            drop(unused);
            result?;
            Ok(JoinedRemoteImageInstallFreeze {
                runtime: self.clone(),
                custody: Some(custody.clone()),
            })
        })
    }
}
