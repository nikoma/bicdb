//! Actual finalized target image + protected complete joint decision. This is
//! CLOSED policy-install provenance, never Prepared/Ready/APP/Strong authority.
use super::*;
use crate::distribution_consensus::HostMetadataReadObservation;
use crate::distribution_native_writer::SealedHostWriterEnrollment;
use crate::distribution_range_image_install::{
    NativeImageCatalogueFinalization, NativeImageInstallFreeze,
};
use crate::distribution_range_mutation::SourceCaptureReadPin;

/// No DTO/parts/Clone constructor. Actual image and exclusion owners remain
/// owned until transferred to the real joint-preparation/release producer.
pub(crate) struct NativeAssignedImagePolicyEvidence {
    image: Arc<NativeImageCatalogueFinalization>,
    frozen: Arc<NativeImageInstallFreeze>,
    pin: SourceCaptureReadPin,
    decision: crate::db::NativeJointDecisionReceipt,
    database: ClusterServiceOpenGuard,
    slot: RangeWriteServiceSlotGuard,
    metadata_context: Uuid,
    metadata_term: u64,
    metadata_index: u64,
    parent: CancellationToken,
}
impl NativeAssignedImagePolicyEvidence {
    pub(crate) fn range(&self) -> &RangeDescriptor {
        self.decision.data().activation_range()
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.parent.check()?;
        self.database.require_live()?;
        self.slot.require_live()
    }
    /// Called without an inherited native journal. The real retained image
    /// pin excludes controlled range mutation; policy metadata is not a range
    /// materialization and cannot replace these current image/decision reads.
    pub(crate) fn require_current(
        &self,
        service: &ClusterDataNodeService,
        database: &BicDb,
        policy: &SealedHostWriterEnrollment,
    ) -> Result<()> {
        self.require_service_identity(service, database, policy)?;
        let gate = database.range_mutation_gate();
        gate.validate_source_capture_read_pin(self.range().id, &self.pin)?;
        self.image.require_materialized_fact_current(
            database,
            &service.range_writes,
            self.frozen.try_as_frozen()?,
        )?;
        self.decision.require_current(database)?;
        let base = self.image.base();
        let data = self.decision.data();
        data.require_actual_local_point(
            self.image.native_admission(),
            base.native_receipt_sha256(),
            base.range_index(),
            base.logical_genesis_sha256(),
            base.logical_history_sha256(),
            base.target_snapshot_bound().get(),
        )?;
        if data.old_members().contains_key(&service.node_id)
            || !data.new_members().contains_key(&service.node_id)
            || base.session_id() != data.promise().session_id
            || base.nonce() != data.promise().nonce
            || base.intended_activation_range() != data.activation_range()
            || base.intended_activation_members() != data.new_members()
            || self.decision.native_database_incarnation() != database.incarnation_id()
            || self.decision.actual_local_visibility_sequence()
                != base.target_snapshot_bound().get()
        {
            return Err(data_error(
                "assigned image policy differs from actual NEW target/cut",
            ));
        }
        policy.facts().require_range(self.range())?;
        gate.validate_source_capture_read_pin(self.range().id, &self.pin)?;
        self.require_service_identity(service, database, policy)
    }
    /// Necessary mutable identity checks for the SAME service borrow at the
    /// final native-policy publication. No nested journal/full lineage read.
    pub(crate) fn require_service_identity(
        &self,
        service: &ClusterDataNodeService,
        database: &BicDb,
        policy: &SealedHostWriterEnrollment,
    ) -> Result<()> {
        self.require_live()?;
        policy.require_live()?;
        self.database.require_database(database)?;
        service.require_native_catalog_database(database)?;
        if service.range_writes.native_writer_store_open_id() != self.slot.id()
            || policy.store_open_id() != self.slot.id()
            || policy.database_incarnation() != database.incarnation_id()
            || policy.metadata_context_id() != self.metadata_context
            || policy.metadata_term() != self.metadata_term
            || policy.metadata_read_index() != self.metadata_index
            || service.node_id != *self.image.native_admission().local_node_id()
        {
            return Err(data_error(
                "assigned image policy changed actual DB/Store/OWN MQ",
            ));
        }
        self.require_live()
    }
}
impl ClusterDataNodeService {
    pub(crate) fn retain_assigned_data_writer<
        T: crate::distribution_range_consensus::RangeWriteTransport,
    >(
        &self,
        database: &BicDb,
        coordinator: &Arc<crate::distribution_range_consensus::RangeWriteCoordinator<T>>,
        guard: &crate::db::InstalledNativeWriterHostGuard,
        range: &RangeDescriptor,
    ) -> Result<(Arc<crate::db::NativeWriterHostState>, PathBuf)> {
        self.require_native_catalog_database(database)?;
        guard.require_for_coordinator(database, range, coordinator)?;
        let writer = self.range_writes.native_writer_host_state()?;
        writer.require_database(database, range)?;
        writer.require_store(
            database.data_path(),
            self.range_writes.native_writer_store_open_id(),
        )?;
        Ok((writer, database.data_path().to_path_buf()))
    }
    /// Fresh authority is consumed only after an actual finalized image and
    /// complete protected decision exist. Registration/topology alone cannot
    /// call this into being. No network occurs while this service is borrowed.
    pub(crate) fn prepare_assigned_image_writer_policy(
        &self,
        image: Arc<NativeImageCatalogueFinalization>,
        frozen: Arc<NativeImageInstallFreeze>,
        metadata: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<(
        NativeAssignedImagePolicyEvidence,
        SealedHostWriterEnrollment,
    )> {
        parent.check()?;
        metadata.require_live_request(business)?;
        let topology = metadata.committed_topology(business)?;
        let id = image.base().capture_range().id;
        if metadata.requester_node_id() != &self.node_id {
            return Err(data_error("assigned policy needs actual OWN host metadata"));
        }
        let record = topology
            .relocations
            .values()
            .find_map(|plan| {
                (plan.range_id == id && plan.target == self.node_id)
                    .then_some(plan.native_joint_handoff_v1.as_ref())
                    .flatten()
                    .filter(|record| {
                        record.stage()
                            >= crate::distribution::NativeJointHandoffStageV1::AwaitingActivation
                    })
            })
            .ok_or_else(|| {
                data_error("assigned policy has no actually committed joint decision")
            })?;
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        let gate = database.range_mutation_gate();
        gate.validate_quiescent_frozen(id, frozen.try_as_frozen()?)?;
        let pin = gate.try_source_capture_read_pin(id, frozen.try_as_frozen()?)?;
        image.require_materialized_fact_current(
            &database,
            &self.range_writes,
            frozen.try_as_frozen()?,
        )?;
        let decision = database.read_native_joint_decision(
            image.native_admission(),
            id,
            record.decision_sha256(),
        )?;
        let anchor = decision.metadata_anchor();
        decision
            .data()
            .require_metadata_domain(topology, metadata.restore_activation(business)?)?;
        if decision.data().checksum_sha256() != record.decision_sha256()
            || decision.data().promise().session_id != record.session_id()
            || decision.data().promise().nonce != record.nonce()
            || decision.data().range_index() != record.range_index()
            || decision.data().logical_genesis_sha256() != record.logical_genesis_sha256()
            || decision.data().logical_history_sha256() != record.logical_history_sha256()
            || metadata.term() < anchor.0
            || metadata.read_index() < anchor.1
        {
            return Err(data_error(
                "assigned policy metadata differs from protected joint decision",
            ));
        }
        let evidence = NativeAssignedImagePolicyEvidence {
            image,
            frozen,
            pin,
            decision,
            database: self.listener_open_guard(),
            slot: self.native_replica_listener_store_open_guard(),
            metadata_context: metadata.context_id(),
            metadata_term: metadata.term(),
            metadata_index: metadata.read_index(),
            parent: parent.clone(),
        };
        let policy = self.prepare_native_host_writer_enrollment(&database, metadata, business)?;
        evidence.require_current(self, &database, &policy)?;
        Ok((evidence, policy))
    }
}
