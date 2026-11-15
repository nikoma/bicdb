//! Genuine original Joined NEW materialization observation. This keeps the
//! actual SAME State/A/native image and remote OLD certificate distinct from
//! Registered/FIRST, local OLD and ordinary installed-writer authority.
use super::*;
use crate::distribution_consensus::HostMetadataReadObservation;
use crate::distribution_native_host::{
    NativeJoinedRemoteJointOwner, NativeJoinedRemoteMaterializedOwner,
};
use crate::distribution_range_handoff::{
    NativeJointMaterializedFacts, NativeRemoteOldCutCertificate,
};
use crate::distribution_range_image_install::NativeImageInstallFreeze;
use crate::distribution_range_recovery::HandoffRoleAdmission;

/// Only observe() constructs this original actual observation. No serde,
/// decoded-record or materialized-facts constructor exists. Facts alone cannot
/// count a NEW vote; consumers must separately check live issuance.
pub(crate) struct ActualServiceNativeJoinedJointNewEvidence {
    certificate: Arc<NativeRemoteOldCutCertificate>,
    role: HandoffRoleAdmission,
    own: HostMetadataReadObservation,
    business: Uuid,
    metadata_point: Arc<crate::distribution_data::ActualNativeJointMetadataPoint>,
    owner: Arc<NativeJoinedRemoteJointOwner>,
    frozen: Arc<NativeImageInstallFreeze>,
    pin: parking_lot::Mutex<Option<SourceCaptureReadPin>>,
    facts: NativeJointMaterializedFacts,
}
impl ActualServiceNativeJoinedJointNewEvidence {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn observe(
        certificate: Arc<NativeRemoteOldCutCertificate>,
        role: HandoffRoleAdmission,
        own: HostMetadataReadObservation,
        business: Uuid,
        owner: Arc<NativeJoinedRemoteJointOwner>,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        certificate.require_current()?;
        let plan = certificate.plan();
        plan.require_role(role.role())?;
        role.role().require_own_host_metadata(&own, business)?;
        let target = owner.target();
        plan.require_actual_native(target.native())?;
        target.readonly_plan().require_manifest(target.manifest())?;
        target
            .readonly_plan()
            .require_fresh_target_own(&own, business)?;
        if target.readonly_plan().checksum_sha256()? != plan.checksum_sha256()? {
            return Err(data_error(
                "Joined NEW changed original complete remote OLD plan",
            ));
        }
        // This may acquire the real donor's guards. Complete it BEFORE any
        // target service/DB guard; never use a close-record DTO as authority.
        let source = require_source_binding(&certificate, target)?;
        let frozen = owner.retained_freeze()?;
        require_joined_lease(&owner, &frozen)?;
        let metadata_point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            owner.service(),
            &own,
            business,
            parent,
        )?
        .ok_or_else(|| data_error("Joined NEW requires actual current-MStore metadata point"))?;
        metadata_point.require_external_current()?;
        let (facts, pin) = {
            let service = owner
                .service()
                .try_lock()
                .ok_or_else(|| data_error("Joined NEW actual service busy"))?;
            let database = checked_database_read(&service.db, &service.storage_policy)?;
            service.require_native_catalog_database(&database)?;
            metadata_point.require_native_store_point(&database, &service.range_writes)?;
            target.require_joint_point(
                &database,
                &service.range_writes,
                frozen.try_as_frozen()?,
            )?;
            let base = target.base();
            let seal = target.source_seal();
            if service.node_id != *target.native().local_node_id()
                || service.node_id != role.committed_relocation().target
                || role.role().local_node_id() != &service.node_id
                || own.requester_node_id() != &service.node_id
                || !plan.new_members().contains_key(&service.node_id)
                || plan.old_members().contains_key(&service.node_id)
                || base.session_id() != plan.promise().session_id
                || base.nonce() != plan.promise().nonce
                || base.purpose()
                    != crate::distribution_range_consensus::ImageInstallPurpose::RelocationLearner
                || !plan.matches_capture_range(base.capture_range())
                || base.intended_activation_range() != plan.activation_range()
                || base.intended_activation_members() != plan.new_members()
                || seal.source_id != *certificate.selected_source_id()
                || seal.source_native_receipt_sha256 != base.source_native_receipt_sha256()
                || seal.range_index != source.range_index
                || seal.logical_genesis_sha256 != source.logical_genesis_sha256
                || seal.logical_history_sha256 != source.logical_history_sha256
                || seal.image_root_sha256 != base.image_root_sha256()
                || seal.old_config_sha256 != plan.promise().old_config_sha256
                || seal.new_config_sha256 != plan.promise().new_config_sha256
                || base.range_index() != source.range_index
                || base.logical_genesis_sha256() != source.logical_genesis_sha256
                || base.logical_history_sha256() != source.logical_history_sha256
                || base.schema_sha256() != role.role().expected_schema_sha256().unwrap_or("")
                || base.restore_activation() != role.role().restore_activation()
            {
                return Err(data_error("Joined NEW actual original image differs from complete remote OLD/native/configuration"));
            }
            let pin = database
                .range_mutation_gate()
                .try_source_capture_read_pin(plan.old_range().id, frozen.try_as_frozen()?)?;
            let facts = NativeJointMaterializedFacts {
                scope_sha256: plan.checksum_sha256()?,
                node_id: service.node_id.clone(),
                birth_id: base.birth_id(),
                birth_sha256: base.birth_sha256().into(),
                native_receipt_sha256: base.native_receipt_sha256().into(),
                range_index: base.range_index(),
                logical_genesis_sha256: base.logical_genesis_sha256().into(),
                logical_history_sha256: base.logical_history_sha256().into(),
                actual_local_visibility_sequence: base.target_snapshot_bound().get(),
                source_snapshot_db_sequence: Some(seal.reserved_db_upper_bound),
                image_root_sha256: Some(seal.image_root_sha256.clone()),
                metadata_term: role.role().metadata_term(),
                metadata_read_index: role.role().metadata_read_index(),
                metadata_context_id: role.role().context_id(),
            };
            plan.require_role(role.role())?;
            role.role().require_own_host_metadata(&own, business)?;
            metadata_point.require_native_store_point(&database, &service.range_writes)?;
            parent.check()?;
            (facts, pin)
        };
        // The original owner already retains A. This read-only observation
        // owns its actual pin before any fallible external postcheck.
        let evidence = Self {
            certificate,
            role,
            own,
            business,
            metadata_point,
            owner,
            frozen,
            pin: parking_lot::Mutex::new(Some(pin)),
            facts,
        };
        evidence.require_issuance_current()?;
        evidence.require_current()?;
        evidence.require_issuance_current()?;
        parent.check()?;
        Ok(evidence)
    }
    pub(crate) fn facts(&self) -> &NativeJointMaterializedFacts {
        &self.facts
    }
    pub(crate) fn service(&self) -> &Arc<parking_lot::Mutex<ClusterDataNodeService>> {
        self.owner.service()
    }
    pub(crate) fn owner(&self) -> &Arc<NativeJoinedRemoteJointOwner> {
        &self.owner
    }
    /// Independently fresh NEW issuance only, outside target native/service
    /// guards. Holding this object never refreshes this original Role/OWN.
    pub(crate) fn require_issuance_current(&self) -> Result<()> {
        self.certificate.require_current()?;
        let plan = self.certificate.plan();
        plan.require_role(self.role.role())?;
        self.role
            .role()
            .require_own_host_metadata(&self.own, self.business)?;
        self.owner
            .target()
            .readonly_plan()
            .require_fresh_target_own(&self.own, self.business)?;
        self.metadata_point
            .require_request(&self.own, self.business)?;
        self.metadata_point.require_external_current()?;
        plan.require_role(self.role.role())?;
        self.role
            .role()
            .require_own_host_metadata(&self.own, self.business)
    }
    /// Historical same-open physical/owner checks only. Fresh issuance is a
    /// separate mandatory boundary; no expired snapshot Role/OWN is revived.
    /// No donor/MStore operation executes while target native guards are held.
    pub(crate) fn require_current(&self) -> Result<()> {
        self.certificate.require_current()?;
        let retained = self.owner.retained_freeze()?;
        if !Arc::ptr_eq(&retained, &self.frozen) {
            return Err(data_error(
                "Joined NEW changed actual original A lease view",
            ));
        }
        require_joined_lease(&self.owner, &self.frozen)?;
        {
            let service = self
                .service()
                .try_lock()
                .ok_or_else(|| data_error("Joined NEW actual service busy"))?;
            let database = checked_database_read(&service.db, &service.storage_policy)?;
            service.require_native_catalog_database(&database)?;
            self.owner.target().require_joint_point(
                &database,
                &service.range_writes,
                self.frozen.try_as_frozen()?,
            )?;
            let base = self.owner.target().base();
            if service.node_id != self.facts.node_id
                || self.facts.scope_sha256
                    != self.owner.target().readonly_plan().checksum_sha256()?
                || self.facts.birth_id != base.birth_id()
                || self.facts.birth_sha256 != base.birth_sha256()
                || self.facts.native_receipt_sha256 != base.native_receipt_sha256()
                || self.facts.range_index != base.range_index()
                || self.facts.logical_genesis_sha256 != base.logical_genesis_sha256()
                || self.facts.logical_history_sha256 != base.logical_history_sha256()
                || self.facts.actual_local_visibility_sequence != base.target_snapshot_bound().get()
                || self.facts.source_snapshot_db_sequence
                    != Some(self.owner.target().source_seal().reserved_db_upper_bound)
                || self.facts.image_root_sha256.as_deref() != Some(base.image_root_sha256())
            {
                return Err(data_error(
                    "Joined NEW actual materialization identity changed",
                ));
            }
            let pin = self
                .pin
                .try_lock()
                .ok_or_else(|| data_error("Joined NEW actual capture pin busy"))?;
            let actual = pin
                .as_ref()
                .ok_or_else(|| data_error("Joined NEW actual capture pin no longer owned"))?;
            database
                .range_mutation_gate()
                .validate_source_capture_read_pin(base.capture_range().id, actual)?;
        }
        self.certificate.require_current()
    }
}
fn require_joined_lease(
    owner: &NativeJoinedRemoteJointOwner,
    frozen: &NativeImageInstallFreeze,
) -> Result<()> {
    let NativeImageInstallFreeze::JoinedRemote(actual) = frozen else {
        return Err(data_error(
            "Joined NEW refuses Registered/legacy substitute lease",
        ));
    };
    owner.target().require_original_lease(actual)
}
/// All certificate/source checks are real producers, performed outside target
/// locks. The OLD close-record digest and exported selected-prefix digest are
/// different domains: exact record -> inner-prefix binding stays mandatory.
fn require_source_binding(
    certificate: &NativeRemoteOldCutCertificate,
    target: &NativeJoinedRemoteMaterializedOwner,
) -> Result<NativeJointMaterializedFacts> {
    certificate.require_current()?;
    let record = certificate.selected_source_close_record()?;
    if record != &target.manifest().source_old_close_record
        || target.manifest().old_quorum_sha256 != certificate.checksum_sha256()?
    {
        return Err(data_error(
            "Joined NEW original authenticated OLD record/MF differs",
        ));
    }
    record.require_source_manifest(target.manifest())?;
    record.require_source_seal(target.source_seal())?;
    let sources = certificate.source_image_materialized_facts()?;
    if target.manifest().actual_old_closures != sources {
        return Err(data_error(
            "Joined NEW omitted or changed actual complete remote OLD members",
        ));
    }
    sources
        .get(certificate.selected_source_id())
        .cloned()
        .ok_or_else(|| data_error("Joined NEW actual selected OLD source absent"))
}
