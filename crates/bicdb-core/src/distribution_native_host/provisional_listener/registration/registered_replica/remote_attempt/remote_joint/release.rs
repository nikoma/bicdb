//! Host-retained original Registered A -> actual dropped/Open continuation.
//! Every error preserves the exact original activation and actual release
//! progress; no API can put A back or mint another freeze.
use super::*;
use crate::distribution_data::NativeJointReleaseOwnerView;
use crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt;
use crate::distribution_range_handoff::NativeJointReleaseAdmission;
use crate::distribution_range_image_install::NativeImageInstallFreeze;

pub(super) struct RegisteredJointReleaseState {
    frozen: Option<Arc<NativeImageInstallFreeze>>,
    activation: Arc<NativeJointCatalogueActivationReceipt>,
}
pub(crate) struct NativeRegisteredRemoteJointReleaseOwner {
    owner: Arc<NativeRegisteredRemoteJointOwner>,
}
impl NativeRegisteredRemoteJointOwner {
    pub(crate) fn retain_release(
        self: &Arc<Self>,
        frozen: &Arc<NativeImageInstallFreeze>,
        activation: Arc<NativeJointCatalogueActivationReceipt>,
    ) -> Result<NativeRegisteredRemoteJointReleaseOwner> {
        let mut release = self
            .release
            .try_lock()
            .ok_or_else(|| invalid("Registered release continuation busy"))?;
        if let Some(old) = release.as_ref() {
            if old.activation.activation_sha256()? != activation.activation_sha256()?
                || old.activation.store_open_id() != activation.store_open_id()
                || old.activation.native().native_database_incarnation()
                    != activation.native().native_database_incarnation()
            {
                return Err(invalid(
                    "Registered release retry changed original actual activation",
                ));
            }
            return Ok(NativeRegisteredRemoteJointReleaseOwner {
                owner: self.clone(),
            });
        }
        let mut original = self
            .frozen
            .try_lock()
            .ok_or_else(|| invalid("Registered original A custody busy"))?;
        let retained = original.as_ref().ok_or_else(|| {
            invalid("Registered original A already moved without release continuation")
        })?;
        if !Arc::ptr_eq(retained, frozen) || !frozen.requires_registered_remote_release() {
            return Err(invalid(
                "Registered release changed original actual freeze Arc",
            ));
        }
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("Registered release original service busy"))?;
        activation.require_live()?;
        service.require_registered_joint_image_release(
            &self.target,
            frozen.try_as_frozen()?,
            &activation,
        )?;
        activation.require_live()?;
        // Infallible host custody transfer after every fallible check. A later
        // handle/arm failure leaves this ORIGINAL host state recoverable.
        *release = Some(RegisteredJointReleaseState {
            frozen: original.take(),
            activation,
        });
        Ok(NativeRegisteredRemoteJointReleaseOwner {
            owner: self.clone(),
        })
    }
}
impl NativeRegisteredRemoteJointReleaseOwner {
    pub(crate) fn require_same_service(
        &self,
        service: &Arc<parking_lot::Mutex<crate::distribution_data::ClusterDataNodeService>>,
    ) -> Result<()> {
        if !Arc::ptr_eq(service, &self.owner.service) {
            return Err(invalid(
                "Registered release changed actual original service",
            ));
        }
        Ok(())
    }
    pub(crate) fn with_current_release_owner<T>(
        &self,
        native: &crate::db::NativeJointServingBridgeReceipt,
        operation: impl for<'a> FnOnce(
            &Arc<parking_lot::Mutex<crate::distribution_data::ClusterDataNodeService>>,
            NativeJointReleaseOwnerView<'a>,
        ) -> Result<T>,
    ) -> Result<T> {
        let release = self
            .owner
            .release
            .try_lock()
            .ok_or_else(|| invalid("Registered actual release continuation busy"))?;
        let state = release
            .as_ref()
            .ok_or_else(|| invalid("Registered actual release continuation absent"))?;
        if state.activation.native_bridge_sha256() != native.checksum_sha256()
            || state.activation.native().native_database_incarnation()
                != native.native_database_incarnation()
        {
            return Err(invalid(
                "Registered release changed original protected native/open",
            ));
        }
        if let Some(frozen) = &state.frozen {
            operation(
                &self.owner.service,
                NativeJointReleaseOwnerView::Frozen(frozen.try_as_frozen()?),
            )
        } else {
            operation(
                &self.owner.service,
                NativeJointReleaseOwnerView::Dropped(&state.activation),
            )
        }
    }
    pub(crate) fn release_with_fresh_phase(
        &mut self,
        admission: &NativeJointReleaseAdmission,
    ) -> Result<()> {
        self.release_fresh_with(admission, || Ok(()))
    }
    #[cfg(test)]
    pub(crate) fn test_release_with_fresh_phase_after_drop(
        &mut self,
        admission: &NativeJointReleaseAdmission,
    ) -> Result<()> {
        self.release_fresh_with(admission, || {
            Err(invalid(
                "injected Registered error after actual A drop before arm",
            ))
        })
    }
    fn release_fresh_with(
        &mut self,
        admission: &NativeJointReleaseAdmission,
        before_arm: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        admission.require_recipient(&self.owner.service, admission.native())?;
        let mut release = self
            .owner
            .release
            .try_lock()
            .ok_or_else(|| invalid("Registered release state busy"))?;
        let state = release
            .as_mut()
            .ok_or_else(|| invalid("Registered actual release state absent"))?;
        if state.activation.native_bridge_sha256() != admission.native().checksum_sha256() {
            return Err(invalid(
                "Registered fresh release changed original native activation",
            ));
        }
        if let Some(frozen) = state.frozen.as_mut() {
            if Arc::strong_count(frozen) != 1 {
                return Err(invalid("Registered release waits for actual materialization/preparation owners to drain"));
            }
            let original = Arc::get_mut(frozen)
                .ok_or_else(|| invalid("Registered original A acquired another owner"))?;
            let NativeImageInstallFreeze::RegisteredRemote(lease) = original else {
                return Err(invalid("Registered release refuses a legacy freeze cast"));
            };
            lease.release_after_registered_joint_activation(&self.owner.target, admission)?;
            let dropped = state.frozen.take();
            drop(dropped);
        }
        before_arm()?;
        let service = self.owner.service.try_lock().ok_or_else(|| {
            invalid("Registered arm busy; original dropped/activation custody retained")
        })?;
        admission.require_live()?;
        service.arm_fresh_native_joint_release(admission, &state.activation)
    }
}
