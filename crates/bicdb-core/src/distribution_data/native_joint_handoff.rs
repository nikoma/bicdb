//! Same-service joint closure. Fresh metadata/network and owned drain precede
//! this bounded local operation; no network or second store handle is opened.
use super::*;
use crate::db::NativeJointOldCloseReceipt;
use crate::distribution_range_consensus::NativeJointOldPromiseReceipt;
use crate::distribution_range_handoff::NativeJointHandoffPlan;
use crate::distribution_range_mutation::{FrozenRangeMutationPermit, SourceCaptureReadPin};
use crate::distribution_range_recovery::RecoveryRoleAdmission;

mod bridge;
pub(crate) use bridge::{ActualServiceNativeJointPreparedEvidence, NativeJointServingLease};

/// Single-use actual service evidence. No request IDs, peer identities, points
/// or native receipts can be substituted when the observation consumes it.
pub(crate) struct ActualServiceNativeJointOldEvidence {
    plan: Arc<NativeJointHandoffPlan>,
    role: RecoveryRoleAdmission,
    native: NativeJointOldCloseReceipt,
    promise: NativeJointOldPromiseReceipt,
    frozen: Arc<FrozenRangeMutationPermit>,
    pin: Arc<SourceCaptureReadPin>,
}
impl ActualServiceNativeJointOldEvidence {
    pub(crate) fn require_current(&self, service: &ClusterDataNodeService) -> Result<()> {
        let database = try_database_read(&service.db)?;
        service.require_native_catalog_database(&database)?;
        self.native.require_closed_fact_current(
            &database,
            &self.plan,
            &self.promise,
            &service.range_writes,
        )?;
        database
            .range_mutation_gate()
            .validate_source_capture_read_pin(self.plan.old_range().id, &self.pin)
    }
    pub(crate) fn into_parts(
        self,
    ) -> (
        Arc<NativeJointHandoffPlan>,
        RecoveryRoleAdmission,
        NativeJointOldCloseReceipt,
        NativeJointOldPromiseReceipt,
        Arc<FrozenRangeMutationPermit>,
        Arc<SourceCaptureReadPin>,
    ) {
        (
            self.plan,
            self.role,
            self.native,
            self.promise,
            self.frozen,
            self.pin,
        )
    }
}
impl ClusterDataNodeService {
    pub(crate) fn close_native_joint_old(
        &mut self,
        plan: Arc<NativeJointHandoffPlan>,
        role: RecoveryRoleAdmission,
        frozen: Arc<FrozenRangeMutationPermit>,
        parent: &CancellationToken,
    ) -> Result<ActualServiceNativeJointOldEvidence> {
        parent.check()?;
        plan.require_role(&role)?;
        let database = try_database_read(&self.db)?;
        self.storage_policy.require_database(&database)?;
        let admission = database.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        plan.require_actual_native(&admission)?;
        if role.local_node_id() != &self.node_id
            || !plan.old_members().contains_key(&self.node_id)
            || frozen.owner() != plan.promise().session_id
        {
            return Err(data_error(
                "native joint close belongs to another old member/session",
            ));
        }
        database
            .range_mutation_gate()
            .validate_quiescent_frozen(plan.old_range().id, &frozen)?;
        // A writer-aware native birth is independently checked above. The
        // feature flag is only a durable refusal for older native binaries.
        crate::format::enable_native_joint_data_handoff(database.data_path())?;
        self.range_writes
            .refsync_native_joint_session(&database, &plan, &role)?;
        self.require_native_catalog_database(&database)?;
        let lineage = database.current_native_range_source_lineage(plan.old_range().id)?;
        let catalogue = self
            .range_writes
            .prepare_native_joint_old_close(&database, &plan, &role, &lineage)?;
        let physical = database.prepare_native_joint_old_physical(
            &plan,
            &role,
            &self.range_writes,
            catalogue,
            &frozen,
            parent,
        )?;
        let gate = database.range_mutation_gate();
        let controlled = gate.try_controlled_mutation(&frozen)?;
        let promise = self.range_writes.persist_native_joint_old_promise(
            &database,
            &plan,
            &role,
            &physical,
            &controlled,
        )?;
        let native = database.persist_native_joint_old_close(
            &plan,
            &role,
            &self.range_writes,
            &physical,
            &promise,
            &controlled,
        )?;
        self.range_writes
            .confirm_native_joint_old_close(&database, &plan, &role, &promise, &native)?;
        drop(controlled);
        let pin = Arc::new(gate.try_source_capture_read_pin(plan.old_range().id, &frozen)?);
        parent.check()?;
        plan.require_role(&role)?;
        native.require_current(&database, &plan, &role, &promise, &self.range_writes)?;
        Ok(ActualServiceNativeJointOldEvidence {
            plan,
            role,
            native,
            promise,
            frozen,
            pin,
        })
    }
}
impl ClusterDataNodeService {
    pub(crate) fn require_native_joint_old_observation(
        &self,
        plan: &NativeJointHandoffPlan,
        native: &NativeJointOldCloseReceipt,
        promise: &NativeJointOldPromiseReceipt,
        frozen: &FrozenRangeMutationPermit,
        pin: Option<&SourceCaptureReadPin>,
    ) -> Result<()> {
        let database = try_database_read(&self.db)?;
        self.require_native_catalog_database(&database)?;
        if native.native_admission().local_node_id() != &self.node_id
            || frozen.owner() != plan.promise().session_id
        {
            return Err(data_error("old closure service/session identity changed"));
        }
        native.require_closed_fact_current(&database, plan, promise, &self.range_writes)?;
        let gate = database.range_mutation_gate();
        match pin {
            Some(pin) => gate.validate_source_capture_read_pin(plan.old_range().id, pin),
            None => gate.validate_quiescent_frozen(plan.old_range().id, frozen),
        }
    }
}

/// Observed irreversible constraints from a NONVOTING peer. These facts must
/// constrain a candidate even when its unresolved tail prevents closure.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeJointIrrevocableFacts {
    pub(crate) scope_sha256: String,
    pub(crate) node_id: ClusterNodeId,
    pub(crate) birth_id: Uuid,
    pub(crate) birth_sha256: String,
    pub(crate) commands: std::collections::BTreeMap<u64, String>,
    pub(crate) native_point: Option<(u64, String, String)>,
    pub(crate) actual_last: u64,
    pub(crate) actual_resolved: u64,
    pub(crate) actual_compacted: u64,
}
pub(crate) struct ActualServiceNativeJointConstraints {
    plan: Arc<NativeJointHandoffPlan>,
    native: crate::db::NativeRecoveryAdmission,
    catalogue: crate::distribution_range_consensus::TargetImageCatalogHistoryObligations,
    safety: crate::db::NativeRangeInstallSafety,
    frozen: Arc<FrozenRangeMutationPermit>,
}
impl ActualServiceNativeJointConstraints {
    pub(crate) fn into_actual_facts(
        self,
        service: &ClusterDataNodeService,
    ) -> Result<NativeJointIrrevocableFacts> {
        let database = try_database_read(&service.db)?;
        self.plan.require_actual_native(&self.native)?;
        self.native.require_current(&database)?;
        self.catalogue.require_current(&service.range_writes)?;
        self.safety
            .require_current(&database, &self.native, &self.frozen)?;
        if service.node_id != *self.native.local_node_id()
            || self.catalogue.local_node_id() != &service.node_id
        {
            return Err(data_error(
                "joint irreversible evidence belongs to another actual service",
            ));
        }
        let mut commands = self.catalogue.irrevocable_commands().clone();
        if let Some(index) = self.safety.pending_index() {
            let hash = self
                .safety
                .pending_command_checksum_sha256()
                .ok_or_else(|| {
                    data_error("native pending obligation has no immutable command checksum")
                })?;
            if commands.get(&index).is_some_and(|old| old != hash) {
                return Err(data_error(
                    "native pending and raw/sparse joint obligations conflict",
                ));
            }
            commands.insert(index, hash.into());
        }
        let native_point = self.safety.history_point().map(|point| {
            (
                point.range_index(),
                point.logical_genesis_sha256().into(),
                point.logical_history_sha256().into(),
            )
        });
        Ok(NativeJointIrrevocableFacts {
            scope_sha256: self.plan.checksum_sha256()?,
            node_id: self.native.local_node_id().clone(),
            birth_id: self.native.birth_id(),
            birth_sha256: self.native.birth_sha256().into(),
            commands,
            native_point,
            actual_last: self.catalogue.last_index(),
            actual_resolved: self.catalogue.resolved_through(),
            actual_compacted: self.catalogue.compacted_through(),
        })
    }
}
impl ClusterDataNodeService {
    pub(crate) fn observe_native_joint_constraints(
        &self,
        plan: Arc<NativeJointHandoffPlan>,
        role: &RecoveryRoleAdmission,
        frozen: Arc<FrozenRangeMutationPermit>,
        parent: &CancellationToken,
    ) -> Result<ActualServiceNativeJointConstraints> {
        parent.check()?;
        plan.require_role(role)?;
        let database = try_database_read(&self.db)?;
        self.require_native_catalog_database(&database)?;
        let native = database.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        plan.require_actual_native(&native)?;
        if !plan.old_members().contains_key(&self.node_id)
            || native.local_node_id() != &self.node_id
            || frozen.owner() != plan.promise().session_id
        {
            return Err(data_error(
                "joint constraints are not from an actual old voter/session",
            ));
        }
        database
            .range_mutation_gate()
            .validate_quiescent_frozen(plan.old_range().id, &frozen)?;
        let catalogue = self
            .range_writes
            .image_install_catalog_history_obligations(plan.old_range().id)?;
        let safety = database.native_range_install_safety(&native, plan.old_range().id, &frozen)?;
        Ok(ActualServiceNativeJointConstraints {
            plan,
            native,
            catalogue,
            safety,
            frozen,
        })
    }
}

/// Actual source handles remain encapsulated until the waiting capture is
/// started OUTSIDE service/DB locks. No caller DB path/sequence is accepted.
pub(crate) struct ActualServiceNativeJointCaptureInputs {
    database: Arc<parking_lot::RwLock<BicDb>>,
    cut: crate::distribution_range_handoff::CertifiedRangeImageCut,
    frozen: Arc<FrozenRangeMutationPermit>,
    lineage: crate::db::SourceRangeImageLineage,
    pin: Arc<SourceCaptureReadPin>,
}
impl ActualServiceNativeJointCaptureInputs {
    pub(crate) fn begin(
        self,
        budget: std::time::Duration,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_image::RangeImageCapture> {
        crate::distribution_range_image::RangeImageCapture::begin_with_source_pin(
            self.database,
            self.cut,
            self.frozen,
            self.lineage,
            budget,
            parent,
            self.pin,
        )
    }
}
impl ClusterDataNodeService {
    pub(crate) fn require_native_joint_old_ancestor(
        &self,
        plan: &NativeJointHandoffPlan,
        native: &NativeJointOldCloseReceipt,
        promise: &NativeJointOldPromiseReceipt,
        frozen: &FrozenRangeMutationPermit,
        pin: &SourceCaptureReadPin,
        index: u64,
        h0: &str,
        history: &str,
        command: Option<&str>,
    ) -> Result<()> {
        self.require_native_joint_old_observation(plan, native, promise, frozen, Some(pin))?;
        let database = try_database_read(&self.db)?;
        let lineage = database.current_native_range_source_lineage(plan.old_range().id)?;
        if index > lineage.range_index() || h0 != lineage.logical_genesis_sha256() {
            return Err(data_error(
                "joint observed constraint is beyond the complete selected native source",
            ));
        }
        let step = database.export_native_logical_history_step_with_source_pin(
            &lineage,
            frozen,
            pin,
            index,
            1,
            &CancellationToken::uncancelable().child_with_deadline(plan.original_deadline()),
        )?;
        let entry = step
            .entries
            .first()
            .ok_or_else(|| data_error("joint native history constraint is missing"))?;
        if entry.index!=index || entry.commitment_sha256!=history || command.is_some_and(|expected|!matches!(&entry.outcome,crate::db::NativeLogicalHistoryOutcomeV1::Applied{command_checksum_sha256} if command_checksum_sha256==expected)) {
            return Err(data_error("joint observed constraint conflicts with actual complete source history"));
        }
        Ok(())
    }
    pub(crate) fn native_joint_old_history_point(
        &self,
        plan: &NativeJointHandoffPlan,
        native: &NativeJointOldCloseReceipt,
        promise: &NativeJointOldPromiseReceipt,
        frozen: &FrozenRangeMutationPermit,
        pin: &SourceCaptureReadPin,
        index: u64,
    ) -> Result<(String, Option<String>)> {
        self.require_native_joint_old_observation(plan, native, promise, frozen, Some(pin))?;
        let database = try_database_read(&self.db)?;
        let lineage = database.current_native_range_source_lineage(plan.old_range().id)?;
        let step = database.export_native_logical_history_step_with_source_pin(
            &lineage,
            frozen,
            pin,
            index,
            1,
            &CancellationToken::uncancelable().child_with_deadline(plan.original_deadline()),
        )?;
        let entry = step
            .entries
            .first()
            .ok_or_else(|| data_error("joint source history entry missing"))?;
        if entry.index != index {
            return Err(data_error("joint source history index mismatch"));
        }
        let command = match &entry.outcome {
            crate::db::NativeLogicalHistoryOutcomeV1::Applied {
                command_checksum_sha256,
            } => Some(command_checksum_sha256.clone()),
            _ => None,
        };
        Ok((entry.commitment_sha256.clone(), command))
    }
    pub(crate) fn prepare_native_joint_capture_inputs(
        &self,
        plan: &NativeJointHandoffPlan,
        native: &NativeJointOldCloseReceipt,
        promise: &NativeJointOldPromiseReceipt,
        frozen: Arc<FrozenRangeMutationPermit>,
        pin: Arc<SourceCaptureReadPin>,
        cut: crate::distribution_range_handoff::CertifiedRangeImageCut,
    ) -> Result<ActualServiceNativeJointCaptureInputs> {
        self.require_native_joint_old_observation(plan, native, promise, &frozen, Some(&pin))?;
        let lineage = try_database_read(&self.db)?
            .current_native_range_source_lineage(plan.old_range().id)?;
        if cut.source_id() != &self.node_id
            || cut.range_index() != native.range_index()
            || cut.session_id() != plan.promise().session_id
            || cut.nonce() != plan.promise().nonce
        {
            return Err(data_error("joint actual capture source/cut mismatch"));
        }
        Ok(ActualServiceNativeJointCaptureInputs {
            database: self.db.clone(),
            cut,
            frozen,
            lineage,
            pin,
        })
    }
}

/// Single-use actual target image evidence. All fields are filled only after
/// validating the SAME service/store/DB, the completed image and fresh role.
pub(crate) struct ActualServiceNativeJointNewEvidence {
    plan: Arc<NativeJointHandoffPlan>,
    role: crate::distribution_range_recovery::HandoffRoleAdmission,
    image: crate::distribution_range_image_install::NativeImageCatalogueFinalization,
    frozen: crate::distribution_range_image_install::NativeImageInstallFreeze,
    pin: parking_lot::Mutex<Option<SourceCaptureReadPin>>,
    facts: crate::distribution_range_handoff::NativeJointMaterializedFacts,
}
impl ActualServiceNativeJointNewEvidence {
    pub(crate) fn into_joint_serving_lease(
        self,
        service: &ClusterDataNodeService,
    ) -> Result<NativeJointServingLease> {
        self.require_current(service)?;
        let Self {
            image, frozen, pin, ..
        } = self;
        // Only the owned reader is retired. Its actual installer lease is
        // transferred intact, never converted to a generic Frozen authority.
        drop(pin.into_inner());
        Ok(NativeJointServingLease::Image {
            image: Arc::new(image),
            frozen: Arc::new(frozen),
        })
    }
    pub(crate) fn facts(&self) -> &crate::distribution_range_handoff::NativeJointMaterializedFacts {
        &self.facts
    }
    pub(crate) fn require_issuance_current(&self) -> Result<()> {
        self.plan.require_role(self.role.role())
    }
    pub(crate) fn require_current(&self, service: &ClusterDataNodeService) -> Result<()> {
        let database = try_database_read(&service.db)?;
        service.require_native_catalog_database(&database)?;
        self.plan
            .require_actual_native(self.image.native_admission())?;
        self.image.require_materialized_fact_current(
            &database,
            &service.range_writes,
            self.frozen.as_frozen(),
        )?;
        let pin = self
            .pin
            .try_lock()
            .ok_or_else(|| data_error("new image pin transition busy"))?;
        match pin.as_ref() {
            Some(pin) => database
                .range_mutation_gate()
                .validate_source_capture_read_pin(self.plan.old_range().id, pin)?,
            None => database
                .range_mutation_gate()
                .validate_quiescent_frozen(self.plan.old_range().id, self.frozen.as_frozen())?,
        }
        let base = self.image.base();
        if service.node_id != self.facts.node_id
            || self.facts.scope_sha256 != self.plan.checksum_sha256()?
            || self.facts.native_receipt_sha256 != base.native_receipt_sha256()
            || self.facts.range_index != base.range_index()
            || self.facts.logical_genesis_sha256 != base.logical_genesis_sha256()
            || self.facts.logical_history_sha256 != base.logical_history_sha256()
            || self.facts.actual_local_visibility_sequence != base.target_snapshot_bound().get()
            || self.facts.birth_id != base.birth_id()
            || self.facts.birth_sha256 != base.birth_sha256()
            || self.facts.source_snapshot_db_sequence
                != Some(self.image.source_seal().reserved_db_upper_bound)
            || self.facts.image_root_sha256.as_deref() != Some(base.image_root_sha256())
        {
            return Err(data_error(
                "actual NEW image evidence changed service/native materialization",
            ));
        }
        Ok(())
    }
}
impl ClusterDataNodeService {
    pub(crate) fn observe_native_joint_new_target(
        &self,
        old: &crate::distribution_range_handoff::NativeJointOldClosedQuorum,
        role: crate::distribution_range_recovery::HandoffRoleAdmission,
        image: crate::distribution_range_image_install::NativeImageCatalogueFinalization,
        frozen: crate::distribution_range_image_install::NativeImageInstallFreeze,
        parent: &CancellationToken,
    ) -> Result<ActualServiceNativeJointNewEvidence> {
        parent.check()?;
        let plan = old.plan();
        plan.require_role(role.role())?;
        let database = try_database_read(&self.db)?;
        self.require_native_catalog_database(&database)?;
        plan.require_actual_native(image.native_admission())?;
        image.require_current_with_database(
            &database,
            &self.range_writes,
            &crate::distribution_range_consensus::NativeImageInstallRole::CommittedHandoff(&role),
            frozen.as_frozen(),
        )?;
        let base = image.base();
        if self.node_id != *image.native_admission().local_node_id()
            || self.node_id != role.committed_relocation().target
            || !plan.new_members().contains_key(&self.node_id)
            || base.session_id() != plan.promise().session_id
            || base.nonce() != plan.promise().nonce
            || base.purpose()
                != crate::distribution_range_consensus::ImageInstallPurpose::RelocationLearner
            || !plan.matches_capture_range(base.capture_range())
            || base.intended_activation_range() != plan.activation_range()
            || base.intended_activation_members() != plan.new_members()
        {
            return Err(data_error(
                "actual target image is not this exact NEW joint configuration",
            ));
        }
        let seal = image.source_seal();
        if seal.source_id != *old.selected_source_id()
            || seal.source_native_receipt_sha256 != base.source_native_receipt_sha256()
            || seal.range_index != old.range_index()
            || seal.logical_genesis_sha256 != old.logical_genesis_sha256()
            || seal.logical_history_sha256 != old.logical_history_sha256()
            || seal.image_root_sha256 != base.image_root_sha256()
            || seal.old_config_sha256 != plan.promise().old_config_sha256
            || seal.new_config_sha256 != plan.promise().new_config_sha256
            || seal.session_id != plan.promise().session_id
            || seal.nonce != plan.promise().nonce
        {
            return Err(data_error(
                "actual NEW image lost its exact completed OLD source seal",
            ));
        }
        if base.range_index() != old.range_index()
            || base.logical_genesis_sha256() != old.logical_genesis_sha256()
            || base.logical_history_sha256() != old.logical_history_sha256()
            || base.schema_sha256() != role.role().expected_schema_sha256().unwrap_or("")
            || base.restore_activation() != role.role().restore_activation()
        {
            return Err(data_error("actual target image differs from the complete OLD closed native history/schema/restore"));
        }
        let gate = database.range_mutation_gate();
        gate.validate_quiescent_frozen(plan.old_range().id, frozen.as_frozen())?;
        let pin = gate.try_source_capture_read_pin(plan.old_range().id, frozen.as_frozen())?;
        let facts = crate::distribution_range_handoff::NativeJointMaterializedFacts {
            scope_sha256: plan.checksum_sha256()?,
            node_id: self.node_id.clone(),
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
        parent.check()?;
        plan.require_role(role.role())?;
        drop(database);
        let evidence = ActualServiceNativeJointNewEvidence {
            plan,
            role,
            image,
            frozen,
            pin: parking_lot::Mutex::new(Some(pin)),
            facts,
        };
        evidence.require_current(self)?;
        Ok(evidence)
    }
}

impl ActualServiceNativeJointNewEvidence {
    pub(crate) fn release_capture_pin_for_decision(
        &self,
        service: &ClusterDataNodeService,
    ) -> Result<()> {
        self.require_current(service)?;
        self.pin
            .try_lock()
            .ok_or_else(|| data_error("new image capture pin busy"))?
            .take();
        self.require_current(service)
    }
}

impl ClusterDataNodeService {
    pub(crate) fn publish_native_joint_decision_old(
        &mut self,
        admission: &crate::distribution_range_handoff::NativeJointDecisionPublicationAdmission,
        native: &NativeJointOldCloseReceipt,
        promise: &NativeJointOldPromiseReceipt,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<crate::db::NativeJointDecisionReceipt> {
        parent.check()?;
        admission.require_live()?;
        if admission.role().local_node_id() != &self.node_id
            || frozen.owner() != admission.plan().promise().session_id
        {
            return Err(data_error(
                "joint old publisher belongs to another actual service/session",
            ));
        }
        let database = try_database_read(&self.db)?;
        self.require_native_catalog_database(&database)?;
        native.require_current(
            &database,
            admission.plan(),
            admission.role(),
            promise,
            &self.range_writes,
        )?;
        let gate = database.range_mutation_gate();
        gate.validate_quiescent_frozen(admission.data().old_range().id, frozen)?;
        let controlled = gate.try_controlled_mutation(frozen)?;
        let receipt = database.persist_native_joint_decision_from_old(
            admission,
            native,
            promise,
            &self.range_writes,
            &controlled,
        )?;
        self.range_writes
            .confirm_native_joint_decision(&database, admission, &receipt)?;
        drop(controlled);
        parent.check()?;
        Ok(receipt)
    }
}
impl ActualServiceNativeJointNewEvidence {
    pub(crate) fn publish_decision(
        &self,
        service: &mut ClusterDataNodeService,
        admission: &crate::distribution_range_handoff::NativeJointDecisionPublicationAdmission,
        parent: &CancellationToken,
    ) -> Result<crate::db::NativeJointDecisionReceipt> {
        parent.check()?;
        admission.require_live()?;
        if self
            .pin
            .try_lock()
            .ok_or_else(|| data_error("new image pin busy"))?
            .is_some()
            || self.plan.checksum_sha256()? != admission.plan().checksum_sha256()?
            || admission.role().local_node_id() != &service.node_id
        {
            return Err(data_error(
                "joint target publisher lost completed capture/pending plan scope",
            ));
        }
        self.require_current(service)?;
        let database = try_database_read(&service.db)?;
        service.require_native_catalog_database(&database)?;
        let gate = database.range_mutation_gate();
        gate.validate_quiescent_frozen(admission.data().old_range().id, self.frozen.as_frozen())?;
        let controlled = gate.try_controlled_mutation(self.frozen.as_frozen())?;
        let receipt = database.persist_native_joint_decision_from_image(
            admission,
            &self.image,
            &service.range_writes,
            self.frozen.as_frozen(),
            &controlled,
        )?;
        service
            .range_writes
            .confirm_native_joint_decision(&database, admission, &receipt)?;
        drop(controlled);
        parent.check()?;
        Ok(receipt)
    }
}

impl ClusterDataNodeService {
    pub(crate) fn require_native_joint_decision_receipt(
        &self,
        receipt: &crate::db::NativeJointDecisionReceipt,
    ) -> Result<()> {
        let database = try_database_read(&self.db)?;
        self.require_native_catalog_database(&database)?;
        if receipt.local_node_id() != &self.node_id {
            return Err(data_error("actual decision belongs to another service"));
        }
        receipt.require_current(&database)
    }
}

/// Exact same-service bundle. A separately retained local receipt cannot be
/// retagged under caller-supplied MQ/request/open fields by the observation.
pub(crate) struct ActualServiceNativeJointCommittedEvidence {
    native: crate::db::NativeJointDecisionReceipt,
    metadata: crate::distribution_consensus::HostMetadataReadObservation,
    business: Uuid,
    database_incarnation: String,
}
impl ActualServiceNativeJointCommittedEvidence {
    pub(crate) fn into_parts(
        self,
    ) -> (
        crate::db::NativeJointDecisionReceipt,
        crate::distribution_consensus::HostMetadataReadObservation,
        Uuid,
        String,
    ) {
        (
            self.native,
            self.metadata,
            self.business,
            self.database_incarnation,
        )
    }
}
impl ClusterDataNodeService {
    /// MQ/network acquisition happened before the caller's service/DB latches.
    /// This producer reads actual local protected WAL; it never imports a
    /// decision DTO and grants no current physical floor or membership cast.
    pub(crate) fn observe_committed_native_joint_decision(
        &self,
        metadata: crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        range: RangeId,
    ) -> Result<crate::distribution_range_handoff::CommittedNativeJointDecisionObservation> {
        metadata.require_live_request(business)?;
        let topology = metadata.committed_topology(business)?;
        let descriptor = topology
            .range_by_id(range)
            .ok_or_else(|| data_error("joint committed range absent"))?;
        // A cleaned tombstone remains historical publication evidence. It is
        // never converted into a current-open/serving permission here.
        let record = topology
            .relocations
            .values()
            .filter(|plan| {
                plan.range_id == range && plan.promoted_epoch.is_some_and(|e| e <= descriptor.epoch)
            })
            .filter_map(|plan| {
                plan.native_joint_handoff_v1
                    .as_ref()
                    .map(|record| (plan, record))
            })
            .filter(|(_, record)| {
                record.stage() >= crate::distribution::NativeJointHandoffStageV1::AwaitingActivation
            })
            .max_by_key(|(plan, _)| (plan.promoted_epoch, plan.id))
            .map(|(_, record)| record)
            .ok_or_else(|| {
                data_error("actual metadata has no committed native joint decision/tombstone")
            })?;
        let local = topology
            .nodes
            .get(&self.node_id)
            .ok_or_else(|| data_error("joint committed local member absent"))?;
        let database = try_database_read(&self.db)?;
        self.require_native_catalog_database(&database)?;
        self.require_native_writer_host_for_range(&database, descriptor)?;
        let native = database.native_recovery_admission(
            &self.cluster_id,
            topology
                .range_image_bootstrap_id
                .ok_or_else(|| data_error("joint committed metadata has no native bootstrap"))?,
            &self.node_id,
            local.incarnation,
        )?;
        if native.writer_enforcement_version() != 1 || metadata.requester_node_id() != &self.node_id
        {
            return Err(data_error(
                "joint committed observation requires actual local born2 requester",
            ));
        }
        let actual =
            database.read_native_joint_decision(&native, range, record.decision_sha256())?;
        let evidence = ActualServiceNativeJointCommittedEvidence {
            database_incarnation: native.database_incarnation().into(),
            native: actual,
            metadata,
            business,
        };
        let observation=crate::distribution_range_handoff::CommittedNativeJointDecisionObservation::from_actual_service(evidence)?;
        observation.require_current(&database)?;
        Ok(observation)
    }
}
