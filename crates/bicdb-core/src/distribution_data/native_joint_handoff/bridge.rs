//! Same-service NEW bridge preparation. Metadata acquisition and drain happen
//! outside the latches. Neither this evidence nor its lease grants serving.
use super::*;
use crate::distribution_range_consensus::NativeJointBridgeCatalogue;
use crate::distribution_range_handoff::CommittedNativeJointDecisionObservation;
use crate::distribution_range_image_install::{
    NativeImageCatalogueFinalization, NativeImageInstallFreeze,
};

mod image_release;
pub(crate) use image_release::NativeJointImageReleaseOwner;

/// Keep the actual owner alive, including an installer's nonextractable
/// recovery lease. No raw Frozen is created from a metadata plan or number.
#[derive(Clone)]
pub(crate) enum NativeJointServingLease {
    Survivor(Arc<FrozenRangeMutationPermit>),
    Image {
        image: Arc<NativeImageCatalogueFinalization>,
        frozen: Arc<NativeImageInstallFreeze>,
    },
}
impl NativeJointServingLease {
    pub(crate) fn frozen(&self) -> &FrozenRangeMutationPermit {
        match self {
            Self::Survivor(frozen) => frozen,
            Self::Image { frozen, .. } => frozen.as_frozen(),
        }
    }
    fn image(&self) -> Option<&NativeImageCatalogueFinalization> {
        match self {
            Self::Survivor(_) => None,
            Self::Image { image, .. } => Some(image),
        }
    }
}
pub(crate) struct ActualServiceNativeJointPreparedEvidence {
    committed: CommittedNativeJointDecisionObservation,
    prepared: crate::db::NativeJointServingPreparation,
    catalogue: NativeJointBridgeCatalogue,
    lease: NativeJointServingLease,
    pin: parking_lot::Mutex<Option<SourceCaptureReadPin>>,
}
impl ActualServiceNativeJointPreparedEvidence {
    pub(crate) fn committed(&self) -> &CommittedNativeJointDecisionObservation {
        &self.committed
    }
    pub(crate) fn prepared(&self) -> &crate::db::NativeJointServingPreparation {
        &self.prepared
    }
    pub(crate) fn require_current(&self, service: &ClusterDataNodeService) -> Result<()> {
        let database = checked_database_read(&service.db, &service.storage_policy)?;
        service.require_native_catalog_database(&database)?;
        service.require_native_writer_host_for_range(
            &database,
            self.committed.data().activation_range(),
        )?;
        if self.prepared.local_node_id() != &service.node_id {
            return Err(data_error(
                "joint bridge preparation belongs to another service",
            ));
        }
        self.prepared.require_current(
            &database,
            &service.range_writes,
            &self.committed,
            &self.catalogue,
            self.lease.image(),
            self.lease.frozen(),
        )?;
        let pin = self
            .pin
            .try_lock()
            .ok_or_else(|| data_error("joint bridge pin transition busy"))?;
        let gate = database.range_mutation_gate();
        if let Some(pin) = pin.as_ref() {
            gate.validate_source_capture_read_pin(self.committed.data().old_range().id, pin)?;
        } else {
            gate.validate_quiescent_frozen(
                self.committed.data().old_range().id,
                self.lease.frozen(),
            )?;
        }
        Ok(())
    }
    /// Release only the owned read pin. The underlying real freeze and all
    /// permanent promises/install refusals remain retained by this evidence.
    pub(crate) fn release_read_pin(&self, service: &ClusterDataNodeService) -> Result<()> {
        self.require_current(service)?;
        self.pin
            .try_lock()
            .ok_or_else(|| data_error("joint bridge pin busy"))?
            .take();
        self.require_current(service)
    }
}
impl ClusterDataNodeService {
    pub(crate) fn persist_local_native_joint_activation(
        &mut self,
        admission: &crate::distribution_range_handoff::NativeJointOpenAdmission,
        actual: &ActualServiceNativeJointPreparedEvidence,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt> {
        parent.check()?;
        admission.require_external_publications_current()?;
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.storage_policy.require_database(&database)?;
        self.range_writes
            .refsync_native_joint_catalogue_activation(&database, admission)?;
        self.require_native_catalog_database(&database)?;
        self.require_native_writer_host_for_range(&database, admission.data().activation_range())?;
        if actual.prepared.local_node_id() != &self.node_id {
            return Err(data_error(
                "joint catalogue activation targets another actual service",
            ));
        }
        let receipt = self
            .range_writes
            .persist_native_joint_catalogue_activation(
                &database,
                admission,
                &actual.committed,
                &actual.catalogue,
                actual.lease.image(),
                actual.lease.frozen(),
                parent,
            )?;
        self.range_writes.install_native_joint_activated_open(
            &database,
            &receipt,
            actual.lease.frozen(),
        )?;
        Ok(receipt)
    }
    pub(crate) fn require_local_native_joint_activation(
        &self,
        receipt: &crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt,
    ) -> Result<()> {
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        self.require_native_writer_host_for_range(&database, receipt.range())?;
        receipt.require_current(&database, &self.range_writes)
    }
    pub(crate) fn arm_local_native_joint_open(
        &self,
        receipt: &crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt,
    ) -> Result<()> {
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        self.require_native_writer_host_for_range(&database, receipt.range())?;
        self.range_writes
            .arm_native_joint_activated_open(&database, receipt)
    }
    pub(crate) fn require_actual_native_joint_bridge(
        &self,
        receipt: &crate::db::NativeJointServingBridgeReceipt,
    ) -> Result<()> {
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        self.require_native_writer_host_for_range(&database, receipt.data().activation_range())?;
        if receipt.local_node_id() != &self.node_id {
            return Err(data_error(
                "protected joint bridge belongs to another actual service",
            ));
        }
        receipt.require_current(&database)
    }
    pub(crate) fn publish_local_native_joint_bridge(
        &self,
        admission: &crate::distribution_range_handoff::NativeJointActivationAdmission,
        actual: &ActualServiceNativeJointPreparedEvidence,
        parent: &CancellationToken,
    ) -> Result<crate::db::NativeJointServingBridgeReceipt> {
        parent.check()?;
        admission.require_external_preparations_current()?;
        actual.require_current(self)?;
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        self.require_native_writer_host_for_range(&database, admission.data().activation_range())?;
        if actual.prepared.local_node_id() != &self.node_id {
            return Err(data_error(
                "joint native bridge publishes on another actual service",
            ));
        }
        database.publish_native_joint_serving_bridge(
            admission,
            &actual.committed,
            &actual.prepared,
            &self.range_writes,
            &actual.catalogue,
            actual.lease.image(),
            actual.lease.frozen(),
            parent,
        )
    }
    pub(crate) fn prepare_local_native_joint_serving(
        &self,
        committed: CommittedNativeJointDecisionObservation,
        lease: NativeJointServingLease,
        parent: &CancellationToken,
    ) -> Result<ActualServiceNativeJointPreparedEvidence> {
        parent.check()?;
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        committed.require_current(&database)?;
        if matches!(&lease, NativeJointServingLease::Survivor(_))
            && lease.frozen().owner() != committed.data().promise().session_id
        {
            return Err(data_error(
                "joint survivor preparation does not own the exact permanent session",
            ));
        }

        self.require_native_writer_host_for_range(&database, committed.data().activation_range())?;
        if committed.native_receipt().local_node_id() != &self.node_id {
            return Err(data_error(
                "joint preparation uses another service's actual committed evidence",
            ));
        }
        let catalogue = self.range_writes.observe_native_joint_bridge_catalogue(
            &database,
            &committed,
            lease.image(),
            lease.frozen(),
        )?;
        let prepared = database.prepare_native_joint_serving(
            &committed,
            &self.range_writes,
            &catalogue,
            lease.image(),
            lease.frozen(),
            parent,
        )?;
        let pin = database
            .range_mutation_gate()
            .try_source_capture_read_pin(committed.data().old_range().id, lease.frozen())?;
        drop(database);
        let evidence = ActualServiceNativeJointPreparedEvidence {
            committed,
            prepared,
            catalogue,
            lease,
            pin: parking_lot::Mutex::new(Some(pin)),
        };
        evidence.require_current(self)?;
        parent.check()?;
        Ok(evidence)
    }
}
