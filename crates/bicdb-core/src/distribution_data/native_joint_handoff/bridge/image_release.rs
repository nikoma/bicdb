//! Retain the real image target owner through the last joint release. An OLD
//! runtime session cannot stand in for this nonextractable installer lease.
use super::*;
use crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt;

/// Same-open retry custody only. The original API keeps its receipt deadline;
/// a distinct fresh full-member phase can validate that receipt as historical
/// audit without reviving it or creating a replacement freeze/open grant.
pub(crate) struct NativeJointImageReleaseOwner {
    service: Arc<parking_lot::Mutex<ClusterDataNodeService>>,
    image: Arc<NativeImageCatalogueFinalization>,
    frozen: Option<Arc<NativeImageInstallFreeze>>,
    activation: Arc<NativeJointCatalogueActivationReceipt>,
}

impl ActualServiceNativeJointPreparedEvidence {
    pub(crate) fn retain_image_release_owner(
        &self,
        service: Arc<parking_lot::Mutex<ClusterDataNodeService>>,
        activation: Arc<NativeJointCatalogueActivationReceipt>,
    ) -> Result<NativeJointImageReleaseOwner> {
        self.require_original_writer_lifetime()?;
        let NativeJointServingLease::Image { image, frozen } = &self.lease else {
            return Err(data_error(
                "joint image release requires the actual Image serving lease",
            ));
        };
        if self.committed.data() != activation.native().data()
            || self.prepared.local_node_id() != activation.native().local_node_id()
        {
            return Err(data_error(
                "joint image release changed its actual prepared decision/member",
            ));
        }
        {
            let actual = service
                .try_lock()
                .ok_or_else(|| data_error("joint image release service busy"))?;
            {
                let database = checked_database_read(&actual.db, &actual.storage_policy)?;
                self.require_original_writer(&database, &actual.range_writes)?;
            }
            actual.require_native_joint_image_release(image, frozen.as_frozen(), &activation)?;
            self.require_original_writer_lifetime()?;
        }
        Ok(NativeJointImageReleaseOwner {
            service,
            image: image.clone(),
            frozen: Some(frozen.clone()),
            activation,
        })
    }
}

impl NativeJointImageReleaseOwner {
    pub(crate) fn require_same_service(
        &self,
        service: &Arc<parking_lot::Mutex<ClusterDataNodeService>>,
    ) -> Result<()> {
        if !Arc::ptr_eq(&self.service, service) {
            return Err(data_error(
                "joint image continuation belongs to another actual service",
            ));
        }
        Ok(())
    }
    pub(crate) fn with_current_release_owner<T>(
        &self,
        native: &crate::db::NativeJointServingBridgeReceipt,
        operation: impl for<'a> FnOnce(
            &Arc<parking_lot::Mutex<ClusterDataNodeService>>,
            super::NativeJointReleaseOwnerView<'a>,
        ) -> Result<T>,
    ) -> Result<T> {
        if self.activation.native_bridge_sha256() != native.checksum_sha256()
            || self.activation.native().native_database_incarnation()
                != native.native_database_incarnation()
        {
            return Err(data_error(
                "joint image continuation changed original protected activation/open",
            ));
        }
        if let Some(frozen) = &self.frozen {
            operation(
                &self.service,
                super::NativeJointReleaseOwnerView::Frozen(frozen.try_as_frozen()?),
            )
        } else {
            operation(
                &self.service,
                super::NativeJointReleaseOwnerView::Dropped(&self.activation),
            )
        }
    }
    pub(crate) fn release_with_fresh_phase(
        &mut self,
        admission: &crate::distribution_range_handoff::NativeJointReleaseAdmission,
    ) -> Result<()> {
        self.release_fresh_with(admission, || Ok(()))
    }
    #[cfg(test)]
    pub(crate) fn test_release_with_fresh_phase_after_drop(
        &mut self,
        admission: &crate::distribution_range_handoff::NativeJointReleaseAdmission,
    ) -> Result<()> {
        self.release_fresh_with(admission, || {
            Err(data_error(
                "injected fresh image error AFTER actual drop BEFORE arm",
            ))
        })
    }
    fn release_fresh_with(
        &mut self,
        admission: &crate::distribution_range_handoff::NativeJointReleaseAdmission,
        before_arm: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        admission.require_recipient(&self.service, self.activation.native())?;
        if let Some(frozen) = self.frozen.as_mut() {
            if Arc::strong_count(frozen) != 1 {
                return Err(data_error(
                    "fresh joint image release waits for actual owner drain",
                ));
            }
            let actual = Arc::get_mut(frozen)
                .ok_or_else(|| data_error("fresh joint image owner changed"))?;
            if actual.requires_registered_remote_release() {
                return Err(data_error(
                    "Registered remote image release protocol is not implemented",
                ));
            }
            if let NativeImageInstallFreeze::Recovery(lease) = actual {
                lease.release_after_fresh_joint_activation(&self.image, admission)?;
            } else {
                let service = self
                    .service
                    .try_lock()
                    .ok_or_else(|| data_error("fresh joint image release service busy"))?;
                service.require_fresh_native_joint_image_release(
                    &self.image,
                    actual.try_as_frozen()?,
                    admission,
                )?;
                admission.require_live()?;
            }
            // None means the ACTUAL original owner was consumed. It cannot be
            // replaced with another Frozen, even after a subsequent failure.
            let dropped = self.frozen.take();
            drop(dropped);
        }
        before_arm()?;
        let service = self.service.try_lock().ok_or_else(|| {
            data_error("fresh image arm busy; original same-open custody retained")
        })?;
        admission.require_live()?;
        service.arm_fresh_native_joint_release(admission, &self.activation)
    }
    /// Busy/cancel/error leaves this object usable for the SAME original
    /// activation. All actual preparation/publication owners must drain first.
    pub(crate) fn release(&mut self, parent: &CancellationToken) -> Result<()> {
        self.release_with(parent, || Ok(()))
    }
    #[cfg(test)]
    pub(crate) fn test_release_after_actual_drop_error(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.release_with(parent, || {
            Err(data_error(
                "injected error after actual image owner release before arm",
            ))
        })
    }
    fn release_with(
        &mut self,
        parent: &CancellationToken,
        before_arm: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        parent.check()?;
        self.activation.require_live()?;
        if let Some(frozen) = self.frozen.as_mut() {
            if Arc::strong_count(frozen) != 1 {
                return Err(data_error(
                    "joint image release waits for actual serving/image owners to drain",
                ));
            }
            {
                let service = self
                    .service
                    .try_lock()
                    .ok_or_else(|| data_error("joint image release service busy"))?;
                service.require_native_joint_image_release(
                    &self.image,
                    frozen.as_frozen(),
                    &self.activation,
                )?;
            }
            parent.check()?;
            self.activation.require_live()?;
            let actual = Arc::get_mut(frozen)
                .ok_or_else(|| data_error("joint image lease acquired another owner"))?;
            if actual.requires_registered_remote_release() {
                return Err(data_error(
                    "Registered remote image release protocol is not implemented",
                ));
            }
            if let NativeImageInstallFreeze::Recovery(lease) = actual {
                // This consumes only an actually activated joint image. Drop
                // must not return its A-owned freeze to the old recovery map.
                lease.release_after_joint_activation(&self.image, &self.activation, parent)?;
            }
            // Publish retry state before dropping the actual Standalone owner.
            // A subsequent arm failure retains the original proof in `self`.
            let released = self.frozen.take();
            drop(released);
        }
        before_arm()?;
        let service = self.service.try_lock().ok_or_else(|| {
            data_error("joint image arm service busy; original activation retained")
        })?;
        parent.check()?;
        self.activation.require_live()?;
        service.arm_local_native_joint_open(&self.activation)
    }
}

impl ClusterDataNodeService {
    /// Actual physical/canonical binding AFTER activation has atomically
    /// retired the exact image intent. No old-role or decoded audit fallback.
    pub(crate) fn require_native_joint_image_release(
        &self,
        image: &NativeImageCatalogueFinalization,
        frozen: &FrozenRangeMutationPermit,
        activation: &NativeJointCatalogueActivationReceipt,
    ) -> Result<()> {
        self.require_local_native_joint_activation(activation)?;
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        image.require_materialized_fact_current(&database, &self.range_writes, frozen)?;
        let base = image.base();
        let data = activation.native().data();
        let canonical = |range: &crate::RangeDescriptor| {
            let mut range = range.clone();
            range.approximate_bytes = 0;
            range.approximate_qps = 0;
            range.replicas.sort_by_key(|replica| replica.id);
            range
        };
        if self.node_id != *image.native_admission().local_node_id()
            || self.node_id != *activation.native().local_node_id()
            || data.old_members().contains_key(&self.node_id)
            || !data.new_members().contains_key(&self.node_id)
            || base.session_id() != data.promise().session_id
            || base.nonce() != data.promise().nonce
            || canonical(base.capture_range()) != *data.old_range()
            || base.intended_activation_range() != data.activation_range()
            || base.intended_activation_members() != data.new_members()
            || base.range_index() != data.range_index()
            || base.logical_genesis_sha256() != data.logical_genesis_sha256()
            || base.logical_history_sha256() != data.logical_history_sha256()
            || base.target_snapshot_bound().get()
                != activation.native().actual_local_visibility_sequence()
        {
            return Err(data_error(
                "joint image release changed actual target birth/image/configuration/history",
            ));
        }
        database
            .range_mutation_gate()
            .validate_quiescent_frozen(data.old_range().id, frozen)
    }
}

impl ClusterDataNodeService {
    pub(crate) fn require_fresh_native_joint_image_release(
        &self,
        image: &NativeImageCatalogueFinalization,
        frozen: &FrozenRangeMutationPermit,
        admission: &crate::distribution_range_handoff::NativeJointReleaseAdmission,
    ) -> Result<()> {
        self.require_fresh_native_joint_release(
            admission,
            super::NativeJointReleaseOwnerView::Frozen(frozen),
        )?;
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        image.require_materialized_fact_current(&database, &self.range_writes, frozen)?;
        let base = image.base();
        let native = admission.native();
        let data = native.data();
        let canonical = |r: &crate::RangeDescriptor| {
            let mut r = r.clone();
            r.approximate_bytes = 0;
            r.approximate_qps = 0;
            r.replicas.sort_by_key(|r| r.id);
            r
        };
        if self.node_id != *image.native_admission().local_node_id()
            || self.node_id != *native.local_node_id()
            || data.old_members().contains_key(&self.node_id)
            || !data.new_members().contains_key(&self.node_id)
            || base.session_id() != data.promise().session_id
            || base.nonce() != data.promise().nonce
            || canonical(base.capture_range()) != *data.old_range()
            || base.intended_activation_range() != data.activation_range()
            || base.intended_activation_members() != data.new_members()
            || base.range_index() != data.range_index()
            || base.logical_genesis_sha256() != data.logical_genesis_sha256()
            || base.logical_history_sha256() != data.logical_history_sha256()
            || base.target_snapshot_bound().get() != native.actual_local_visibility_sequence()
        {
            return Err(data_error(
                "fresh joint image release changed actual image/native/target/point/B binding",
            ));
        }
        database
            .range_mutation_gate()
            .validate_quiescent_frozen(data.old_range().id, frozen)?;
        admission.require_live()
    }
}
