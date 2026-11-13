//! Actual original Registered target NEW evidence. Source DATA is never a
//! legacy install fence or an ordinary writer registration.
use super::*;
use crate::distribution_native_host::NativeRegisteredRemoteJointOwner;
use crate::distribution_range_handoff::{
    NativeJointMaterializedFacts, NativeRemoteOldCutCertificate,
};

pub(crate) struct ActualServiceNativeRegisteredJointNewEvidence {
    certificate: Arc<NativeRemoteOldCutCertificate>,
    role: crate::distribution_range_recovery::HandoffRoleAdmission,
    owner: Arc<NativeRegisteredRemoteJointOwner>,
    frozen: Arc<crate::distribution_range_image_install::NativeImageInstallFreeze>,
    pin: parking_lot::Mutex<Option<SourceCaptureReadPin>>,
    facts: NativeJointMaterializedFacts,
}
impl ActualServiceNativeRegisteredJointNewEvidence {
    pub(crate) fn observe(
        certificate: Arc<NativeRemoteOldCutCertificate>,
        role: crate::distribution_range_recovery::HandoffRoleAdmission,
        owner: Arc<NativeRegisteredRemoteJointOwner>,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        certificate.require_current()?;
        let plan = certificate.plan();
        plan.require_role(role.role())?;
        let frozen = owner.retained_freeze()?;
        let service = owner
            .service()
            .try_lock()
            .ok_or_else(|| data_error("Registered NEW service busy"))?;
        let database = checked_database_read(&service.db, &service.storage_policy)?;
        service.require_native_catalog_database(&database)?;
        let target = owner.target();
        plan.require_actual_native(target.native())?;
        target.require_current(
            &database,
            &service.range_writes,
            frozen.try_as_frozen()?,
            parent,
        )?;
        let base = target.base();
        let seal = target.source_seal();
        let source_facts = certificate.source_image_materialized_facts()?;
        let source = source_facts
            .get(certificate.selected_source_id())
            .ok_or_else(|| data_error("Registered NEW actual source absent"))?;
        if service.node_id != *target.native().local_node_id()
            || service.node_id != role.committed_relocation().target
            || !plan.new_members().contains_key(&service.node_id)
            || base.session_id() != plan.promise().session_id
            || base.nonce() != plan.promise().nonce
            || base.purpose()
                != crate::distribution_range_consensus::ImageInstallPurpose::RelocationLearner
            || !plan.matches_capture_range(base.capture_range())
            || base.intended_activation_range() != plan.activation_range()
            || base.intended_activation_members() != plan.new_members()
            || seal.source_id != *certificate.selected_source_id()
            || seal.source_native_receipt_sha256 != source.native_receipt_sha256
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
            return Err(data_error("Registered NEW original image differs from complete remote OLD/native/configuration"));
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
        parent.check()?;
        drop(database);
        drop(service);
        let evidence = Self {
            certificate,
            role,
            owner,
            frozen,
            pin: parking_lot::Mutex::new(Some(pin)),
            facts,
        };
        evidence.require_current()?;
        Ok(evidence)
    }
    pub(crate) fn require_remote_source_retirement(
        &self,
        manifest: &crate::distribution_range_image::NativeRemoteImageManifestV1,
    ) -> Result<()> {
        self.require_current()?;
        if manifest.checksum_sha256 != self.owner.target().manifest().checksum_sha256
            || manifest.old_quorum_sha256 != self.certificate.checksum_sha256()?
            || manifest.source_id != *self.certificate.selected_source_id()
        {
            return Err(data_error(
                "actual NEW cannot retire a different original source export",
            ));
        }
        self.certificate.require_current()
    }
    pub(crate) fn service(&self) -> &Arc<parking_lot::Mutex<ClusterDataNodeService>> {
        self.owner.service()
    }
    pub(crate) fn facts(&self) -> &NativeJointMaterializedFacts {
        &self.facts
    }
    pub(crate) fn require_issuance_current(&self) -> Result<()> {
        self.certificate.plan().require_role(self.role.role())
    }
    pub(crate) fn require_current(&self) -> Result<()> {
        let service = self
            .service()
            .try_lock()
            .ok_or_else(|| data_error("Registered NEW actual service busy"))?;
        let database = checked_database_read(&service.db, &service.storage_policy)?;
        service.require_native_catalog_database(&database)?;
        self.owner.target().require_current(
            &database,
            &service.range_writes,
            self.frozen.try_as_frozen()?,
            &CancellationToken::uncancelable(),
        )?;
        let base = self.owner.target().base();
        if service.node_id != self.facts.node_id
            || self.facts.native_receipt_sha256 != base.native_receipt_sha256()
            || self.facts.range_index != base.range_index()
            || self.facts.birth_id != base.birth_id()
            || self.facts.birth_sha256 != base.birth_sha256()
            || self.facts.actual_local_visibility_sequence != base.target_snapshot_bound().get()
        {
            return Err(data_error(
                "Registered NEW actual materialization identity changed",
            ));
        }
        let pin = self
            .pin
            .try_lock()
            .ok_or_else(|| data_error("Registered NEW pin busy"))?;
        if let Some(pin) = pin.as_ref() {
            database
                .range_mutation_gate()
                .validate_source_capture_read_pin(base.capture_range().id, pin)?;
        } else {
            database
                .range_mutation_gate()
                .validate_quiescent_frozen(base.capture_range().id, self.frozen.try_as_frozen()?)?;
        }
        Ok(())
    }
    pub(crate) fn release_capture_pin_for_decision(&self) -> Result<()> {
        self.require_current()?;
        self.pin
            .try_lock()
            .ok_or_else(|| data_error("Registered NEW pin transition busy"))?
            .take();
        self.require_current()
    }
    pub(crate) fn publish_decision(
        &self,
        admission: &crate::distribution_range_handoff::NativeJointDecisionPublicationAdmission,
        parent: &CancellationToken,
    ) -> Result<crate::db::NativeJointDecisionReceipt> {
        parent.check()?;
        admission.require_registered_own()?;
        self.require_current()?;
        if self
            .pin
            .try_lock()
            .ok_or_else(|| data_error("Registered NEW pin busy"))?
            .is_some()
            || self.certificate.plan().checksum_sha256()? != admission.plan().checksum_sha256()?
        {
            return Err(data_error(
                "Registered decision requires drained original pin/same pending plan",
            ));
        }
        let mut service = self
            .service()
            .try_lock()
            .ok_or_else(|| data_error("Registered decision service busy"))?;
        let database_arc = service.db.clone();
        let database = checked_database_read(&database_arc, &service.storage_policy)?;
        database.with_registered_joint_exclusion_scope(
            self.owner.target(),
            self.frozen.try_as_frozen()?,
            parent,
            |scope| {
                admission.require_registered_own()?;
                scope.require_current(&database, &service.range_writes)?;
                let controlled = database
                    .range_mutation_gate()
                    .try_controlled_mutation(self.frozen.try_as_frozen()?)?;
                let receipt = database.persist_native_joint_decision_from_registered(
                    admission,
                    scope,
                    &service.range_writes,
                    &controlled,
                )?;
                service
                    .range_writes
                    .current_mut()
                    .confirm_native_registered_joint_decision(
                        &database, admission, &receipt, scope,
                    )?;
                drop(controlled);
                admission.require_registered_own()?;
                scope.require_current(&database, &service.range_writes)?;
                parent.check()?;
                Ok(receipt)
            },
        )
    }
    pub(crate) fn require_decision_receipt(
        &self,
        receipt: &crate::db::NativeJointDecisionReceipt,
    ) -> Result<()> {
        self.require_current()?;
        self.service()
            .try_lock()
            .ok_or_else(|| data_error("Registered decision receipt service busy"))?
            .require_native_joint_decision_receipt(receipt)
    }
    pub(crate) fn into_serving_lease(
        self,
    ) -> Result<(
        Arc<parking_lot::Mutex<ClusterDataNodeService>>,
        NativeJointServingLease,
    )> {
        self.require_current()?;
        drop(self.pin.into_inner());
        Ok((
            self.owner.service().clone(),
            NativeJointServingLease::Registered {
                owner: self.owner,
                frozen: self.frozen,
            },
        ))
    }
}

impl ClusterDataNodeService {
    pub(crate) fn retain_registered_joint_store_identity(
        &mut self,
        owner: &Arc<NativeRegisteredRemoteJointOwner>,
    ) -> Result<()> {
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        owner.target().original_hook().require_current(&database)?;
        owner
            .target()
            .require_joint_identity(&database, &self.range_writes)?;
        self.range_writes
            .current_mut()
            .retain_registered_joint_identity(owner)
    }
    pub(super) fn observe_registered_committed_joint_decision(
        &self,
        domain: &crate::db::NativeJointWriterDomain,
        owner: &Arc<NativeRegisteredRemoteJointOwner>,
        metadata: crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        range: RangeId,
    ) -> Result<crate::distribution_range_handoff::CommittedNativeJointDecisionObservation> {
        metadata.require_live_request(business)?;
        let target = owner.target();
        let topology = metadata.committed_topology(business)?;
        let descriptor = topology
            .range_by_id(range)
            .ok_or_else(|| data_error("Registered committed joint range absent"))?;
        let record = topology
            .relocations
            .values()
            .filter(|p| {
                p.range_id == range && p.promoted_epoch.is_some_and(|e| e <= descriptor.epoch)
            })
            .filter_map(|p| p.native_joint_handoff_v1.as_ref())
            .filter(|r| {
                r.stage() >= crate::distribution::NativeJointHandoffStageV1::AwaitingActivation
            })
            .max_by_key(|r| r.range_index())
            .ok_or_else(|| data_error("Registered actual committed joint decision absent"))?;
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        domain.require_current(
            &database,
            &self.range_writes,
            target.base().intended_activation_range(),
        )?;
        if self.node_id != *target.native().local_node_id()
            || range != target.base().capture_range().id
            || metadata.requester_node_id() != &self.node_id
        {
            return Err(data_error(
                "Registered committed decision changed original recipient",
            ));
        }
        let actual = database.read_native_joint_decision(
            target.native(),
            range,
            record.decision_sha256(),
        )?;
        let evidence = ActualServiceNativeJointCommittedEvidence {
            database_incarnation: database.incarnation_id().into(),
            native: actual,
            metadata,
            business,
        };
        let observed = crate::distribution_range_handoff::CommittedNativeJointDecisionObservation::from_actual_service(evidence)?;
        observed.require_current(&database)?;
        domain.require_current(
            &database,
            &self.range_writes,
            target.base().intended_activation_range(),
        )?;
        Ok(observed)
    }
    pub(crate) fn prepare_local_registered_joint_serving(
        &self,
        committed: crate::distribution_range_handoff::CommittedNativeJointDecisionObservation,
        lease: NativeJointServingLease,
        parent: &CancellationToken,
    ) -> Result<super::ActualServiceNativeJointPreparedEvidence> {
        parent.check()?;
        let NativeJointServingLease::Registered { owner, .. } = &lease else {
            return Err(data_error(
                "Registered preparation requires actual original target lease",
            ));
        };
        let domain = crate::db::NativeJointWriterDomain::Registered(Arc::downgrade(owner));
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        committed.require_current(&database)?;
        domain.require_current(
            &database,
            &self.range_writes,
            committed.data().activation_range(),
        )?;
        if committed.native_receipt().local_node_id() != &self.node_id {
            return Err(data_error(
                "Registered preparation changed actual recipient",
            ));
        }
        let catalogue = self.range_writes.observe_native_joint_bridge_catalogue(
            &database,
            &committed,
            lease.image(),
            lease.try_frozen()?,
        )?;
        let prepared = database.prepare_native_joint_serving(
            &committed,
            &self.range_writes,
            &catalogue,
            lease.image(),
            lease.try_frozen()?,
            parent,
        )?;
        let pin = database
            .range_mutation_gate()
            .try_source_capture_read_pin(committed.data().old_range().id, lease.try_frozen()?)?;
        let identity = domain.phase_owner();
        drop(database);
        let evidence = super::bridge::ActualServiceNativeJointPreparedEvidence {
            committed,
            prepared,
            catalogue,
            writer: identity,
            writer_domain: domain,
            lease,
            pin: parking_lot::Mutex::new(Some(pin)),
        };
        evidence.require_current(self)?;
        parent.check()?;
        Ok(evidence)
    }
}

impl super::bridge::ActualServiceNativeJointPreparedEvidence {
    pub(crate) fn retain_registered_release_owner(
        &self,
        service: Arc<parking_lot::Mutex<ClusterDataNodeService>>,
        activation: Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>,
    ) -> Result<crate::distribution_native_host::NativeRegisteredRemoteJointReleaseOwner> {
        self.require_original_writer_lifetime()?;
        let NativeJointServingLease::Registered { owner, frozen } = &self.lease else {
            return Err(data_error(
                "Registered release requires original actual receiver lease",
            ));
        };
        if !Arc::ptr_eq(&service, owner.service())
            || self.committed.data() != activation.native().data()
            || self.prepared.local_node_id() != activation.native().local_node_id()
        {
            return Err(data_error(
                "Registered release changed original preparation/service/decision",
            ));
        }
        owner.retain_release(frozen, activation)
    }
}
