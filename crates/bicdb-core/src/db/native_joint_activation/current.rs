//! Actual dropped-owner/open continuity, NEVER a metadata/serving permission.
//! An observation holds only Weak ownership of the actual installed guard.
use super::*;

pub(crate) struct NativeJointOpenContinuity {
    guard: std::sync::Weak<NativeJointActivatedOpenGuard>,
    store_open_id: Uuid,
    activation_sha256: String,
    armed: bool,
}
impl NativeJointActivatedOpenGuard {
    pub(crate) fn observe_actual_continuity(
        self: &Arc<Self>,
        store_open: Uuid,
        activation_sha: &str,
    ) -> Result<NativeJointOpenContinuity> {
        self.require_binding(store_open, activation_sha)?;
        let armed = self.armed.load(Ordering::Acquire);
        // Unarmed is a CLOSED post-drop retry fact. Check actual drain locally;
        // do not retain an application-mutation stamp for the whole phase.
        // An armed guard remains valid while legal ordinary writes progress.
        if !armed {
            self.gate
                .observe_native_read_stamp(self.range.id)?
                .require_current()?;
        }
        self.require_binding(store_open, activation_sha)?;
        Ok(NativeJointOpenContinuity {
            guard: Arc::downgrade(self),
            store_open_id: store_open,
            activation_sha256: activation_sha.into(),
            armed,
        })
    }
}
impl NativeJointOpenContinuity {
    pub(crate) fn armed(&self) -> bool {
        self.armed
    }
    pub(crate) fn require_current(
        &self,
        actual: &Arc<NativeJointActivatedOpenGuard>,
        store_open: Uuid,
        activation_sha: &str,
    ) -> Result<()> {
        let original = self
            .guard
            .upgrade()
            .ok_or_else(|| BicDbError::Cluster("joint actual open guard ended".into()))?;
        if !Arc::ptr_eq(actual, &original)
            || self.store_open_id != store_open
            || self.activation_sha256 != activation_sha
            || self.armed != actual.armed.load(Ordering::Acquire)
        {
            return Err(BicDbError::Cluster(
                "joint actual open/armed state changed or was replaced".into(),
            ));
        }
        actual.require_binding(store_open, activation_sha)
    }
}
