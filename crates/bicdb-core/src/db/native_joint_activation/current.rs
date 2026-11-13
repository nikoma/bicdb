//! Actual dropped-owner/open continuity, NEVER a metadata/serving permission.
//! An observation holds only Weak ownership of the actual installed guard.
use super::*;

#[derive(Debug)]
pub(crate) struct NativeJointOpenContinuity {
    guard: std::sync::Weak<NativeJointActivatedOpenGuard>,
    store_open_id: Uuid,
    activation_sha256: String,
    armed: bool,
}
impl NativeJointActivatedOpenGuard {
    pub(crate) fn observe_owned_frozen_generation(
        self: &Arc<Self>,
        store_open: Uuid,
        activation_sha: &str,
    ) -> Result<NativeJointOpenContinuity> {
        self.require_binding(store_open, activation_sha)?;
        if self.armed.load(Ordering::Acquire) {
            return Err(BicDbError::Cluster(
                "armed joint member cannot count a retained Frozen owner".into(),
            ));
        }
        Ok(NativeJointOpenContinuity {
            guard: Arc::downgrade(self),
            store_open_id: store_open,
            activation_sha256: activation_sha.into(),
            armed: false,
        })
    }
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
    pub(crate) fn actual_released_publication_point(
        &self,
    ) -> Result<(Arc<NativeJointActivatedOpenGuard>, Uuid, String)> {
        self.released_generation()?;
        let actual = self
            .guard
            .upgrade()
            .ok_or_else(|| BicDbError::Cluster("actual released publication guard ended".into()))?;
        Ok((actual, self.store_open_id, self.activation_sha256.clone()))
    }
    /// Identity of the actual retained, armed generation; a number alone is
    /// never an open permission or a replacement for this weak observation.
    pub(crate) fn released_generation(&self) -> Result<Uuid> {
        self.require_observed_lifetime()?;
        let actual = self
            .guard
            .upgrade()
            .ok_or_else(|| BicDbError::Cluster("joint observed guard ended".into()))?;
        if !self.armed {
            return Err(BicDbError::Cluster(
                "joint observed owner has not released".into(),
            ));
        }
        actual.require_current(self.store_open_id, &self.activation_sha256)?;
        actual.generation.id()
    }
    pub(crate) fn armed(&self) -> bool {
        self.armed
    }
    /// Cheap actual lifetime/generation recheck between complete service
    /// sweeps. This does not replace the actual Store-slot comparison below.
    pub(crate) fn require_observed_lifetime(&self) -> Result<()> {
        let actual = self
            .guard
            .upgrade()
            .ok_or_else(|| BicDbError::Cluster("joint observed guard ended".into()))?;
        if self.armed && !actual.armed.load(Ordering::Acquire) {
            return Err(BicDbError::Cluster(
                "joint armed generation regressed".into(),
            ));
        }
        actual.require_binding(self.store_open_id, &self.activation_sha256)
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
            || (self.armed && !actual.armed.load(Ordering::Acquire))
        {
            return Err(BicDbError::Cluster(
                "joint actual open/armed state changed or was replaced".into(),
            ));
        }
        actual.require_binding(store_open, activation_sha)
    }
}
