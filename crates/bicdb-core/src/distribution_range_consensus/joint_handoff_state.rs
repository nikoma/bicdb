//! Native joint catalogue closure. This is actual local observation, never an
//! acknowledgement, data quorum, materialization or serving permission.
use super::*;
use crate::distribution_range_handoff::NativeJointHandoffPlan;
use crate::distribution_range_recovery::RecoveryRoleAdmission;

mod bridge;
pub(crate) use bridge::NativeJointBridgeCatalogue;
mod activation;
use activation::NativeJointCatalogueActivation;
pub(crate) use activation::{
    NativeJointCatalogueActivationFact, NativeJointCatalogueActivationReceipt,
};

pub(super) const NATIVE_JOINT_HANDOFF_CATALOG_FORMAT: u32 = 14;
#[derive(Debug)]
pub(crate) struct NativeJointOldCatalogPreflight {
    decisions: BTreeMap<u64, NativeReadCatalogueDecision>,
    native_r: u64,
    obligations: TargetImageCatalogHistoryObligations,
    plan_sha256: String,
    role_context: Uuid,
    store_open_id: Uuid,
}
impl NativeJointOldCatalogPreflight {
    pub(crate) fn decisions(&self) -> &BTreeMap<u64, NativeReadCatalogueDecision> {
        &self.decisions
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.native_r
    }
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.store_open_id
    }
    pub(crate) fn catalogue_sha256(&self) -> &str {
        self.obligations.range_log_sha256()
    }
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        plan: &NativeJointHandoffPlan,
        role: &RecoveryRoleAdmission,
    ) -> Result<()> {
        plan.require_role(role)?;
        if self.plan_sha256 != plan.checksum_sha256()?
            || self.role_context != role.context_id()
            || self.store_open_id() != store.open_id
        {
            return Err(range_write_error(
                "joint old catalogue preflight changed scope/open/context",
            ));
        }
        self.obligations.require_current(store)
    }
}
impl RangeWriteStore {
    pub(crate) fn prepare_native_joint_old_close(
        &self,
        database: &crate::BicDb,
        plan: &NativeJointHandoffPlan,
        role: &RecoveryRoleAdmission,
        lineage: &crate::db::SourceRangeImageLineage,
    ) -> Result<NativeJointOldCatalogPreflight> {
        plan.require_role(role)?;
        self.require_native_catalog_durability(database.data_path())?;
        self.require_native_writer_host_range_id(plan.old_range().id)?;
        let native = database.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        plan.require_actual_native(&native)?;
        if self.catalog.node_id != *role.local_node_id()
            || !plan.old_members().contains_key(role.local_node_id())
        {
            return Err(range_write_error(
                "joint old close is not this actual old data voter",
            ));
        }
        database.validate_source_range_image_lineage(
            lineage,
            plan.old_range().id,
            lineage.range_index(),
        )?;
        plan.require_old_native_configuration(lineage)?;
        self.require_no_image_install(plan.old_range().id)?;
        self.require_sparse_native_binding(plan.old_range().id, &native)?;
        if self
            .catalog
            .backup_fences
            .contains_key(&plan.old_range().id)
        {
            return Err(range_write_error(
                "joint close cannot bypass a backup fence",
            ));
        }
        if self.progress(plan.old_range().id).current_epoch > plan.old_range().epoch
            && self
                .catalog
                .handoff_promises
                .get(&plan.old_range().id)
                .is_none_or(|promise| {
                    !promise.same_request(plan.promise())
                        || self.progress(plan.old_range().id).current_epoch
                            != promise.activation_epoch
                })
        {
            return Err(range_write_error(
                "joint role is behind an unrelated durable data epoch/fence",
            ));
        }
        let native_r = lineage.range_index();
        self.require_actual_abort_controls_covered(database, plan.old_range().id, native_r)?;
        let decisions = self.collect_native_resolved_decisions(plan.old_range().id, native_r)?;
        let obligations = self.image_install_catalog_history_obligations(plan.old_range().id)?;
        if obligations.last_index() != native_r || obligations.resolved_through() != native_r {
            return Err(range_write_error(
                "joint old close requires the complete resolved native prefix",
            ));
        }
        Ok(NativeJointOldCatalogPreflight {
            decisions,
            native_r,
            store_open_id: self.open_id,
            obligations,
            plan_sha256: plan.checksum_sha256()?,
            role_context: role.context_id(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeJointLocalState {
    scope_sha256: String,
    session_id: Uuid,
    nonce: Uuid,
    old_closed_native_sha256: Option<String>,
    global_decision_sha256: Option<String>,
    global_decision_native_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    serving_activation: Option<NativeJointCatalogueActivation>,
}
impl NativeJointLocalState {
    pub(super) fn validate(&self) -> Result<()> {
        if let Some(activation) = &self.serving_activation {
            activation.validate()?;
            if self.global_decision_sha256.as_deref() != Some(activation.decision_sha256()) {
                return Err(range_write_error(
                    "joint catalogue activation lost complete decision binding",
                ));
            }
        }
        let digest = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if self.session_id.is_nil()
            || self.nonce.is_nil()
            || !digest(&self.scope_sha256)
            || [
                &self.old_closed_native_sha256,
                &self.global_decision_sha256,
                &self.global_decision_native_sha256,
            ]
            .into_iter()
            .any(|v| v.as_ref().is_some_and(|v| !digest(v)))
            || self.global_decision_sha256.is_some() != self.global_decision_native_sha256.is_some()
        {
            return Err(range_write_error("damaged native joint catalogue state"));
        }
        Ok(())
    }
}
/// The actual durable logical promise, not native materialization or a vote.
#[derive(Debug)]
pub(crate) struct NativeJointOldPromiseReceipt {
    store_open_id: Uuid,
    plan_sha256: String,
    promise: RangeHandoffPromise,
    history_sha256: String,
}
impl NativeJointOldPromiseReceipt {
    pub(crate) fn promise(&self) -> &RangeHandoffPromise {
        &self.promise
    }
    pub(crate) fn history_sha256(&self) -> &str {
        &self.history_sha256
    }
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        plan: &NativeJointHandoffPlan,
    ) -> Result<()> {
        store.require_native_writer_host_base()?;
        let current = store
            .catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|m| m.get(&self.promise.range_id))
            .ok_or_else(|| range_write_error("joint old promise catalogue state missing"))?;
        if self.store_open_id != store.open_id
            || self.plan_sha256 != plan.checksum_sha256()?
            || current.scope_sha256 != self.plan_sha256
            || store.catalog.handoff_promises.get(&self.promise.range_id) != Some(&self.promise)
            || store.progress(self.promise.range_id).current_epoch != self.promise.activation_epoch
            || store.native_joint_history_commitment(self.promise.range_id)? != self.history_sha256
        {
            return Err(range_write_error(
                "actual joint promise/store/history changed",
            ));
        }
        Ok(())
    }
}
impl RangeWriteStore {
    /// Full actual history/provenance, normalized ONLY for the epoch raised by
    /// this exact promise. Streaming hash refuses oversized state; no entry or
    /// payload is dropped or summarized as a fabricated applied counter.
    fn native_joint_history_commitment(&self, range: RangeId) -> Result<String> {
        struct Writer {
            hash: Sha256,
            bytes: usize,
        }
        impl std::io::Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                let next = self
                    .bytes
                    .checked_add(bytes.len())
                    .ok_or_else(|| std::io::Error::other("joint history size overflow"))?;
                if next > 64 * 1024 * 1024 {
                    return Err(std::io::Error::other(
                        "complete joint retained history exceeds supported bound",
                    ));
                }
                self.bytes = next;
                self.hash.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let history = self.catalog.ranges.get(&range).map(|log| {
            (
                &log.last_index,
                &log.resolved_through,
                &log.compacted_through,
                &log.entries,
            )
        });
        let mut writer = Writer {
            hash: Sha256::new(),
            bytes: 0,
        };
        serde_json::to_writer(
            &mut writer,
            &(
                "bicdb-native-joint-full-retained-history-v1",
                &self.catalog.cluster_id,
                &self.catalog.node_id,
                range,
                history,
                self.catalog.recovery_fences.get(&range),
                self.catalog.native_image_bases.get(&range),
                self.catalog
                    .sparse_committed_decisions
                    .as_ref()
                    .and_then(|m| m.get(&range)),
                self.catalog
                    .installing_committed_certifications
                    .as_ref()
                    .and_then(|m| m.get(&range)),
                self.catalog
                    .installing_recovery_handovers
                    .as_ref()
                    .and_then(|m| m.get(&range)),
                self.catalog
                    .native_abort_protocol
                    .as_ref()
                    .and_then(|p| p.range(range)),
            ),
        )?;
        Ok(hex::encode(writer.hash.finalize()))
    }
    pub(crate) fn persist_native_joint_old_promise(
        &mut self,
        database: &crate::BicDb,
        plan: &NativeJointHandoffPlan,
        role: &RecoveryRoleAdmission,
        physical: &crate::db::NativeJointOldPhysicalProof,
        controlled: &crate::distribution_range_mutation::ControlledFrozenMutationPermit,
    ) -> Result<NativeJointOldPromiseReceipt> {
        physical.require_before_promise(database, self, plan, role, controlled)?;
        if !crate::format::native_joint_data_handoff_active(&self.root)?
            || physical.native_admission().writer_enforcement_version() != 1
        {
            return Err(range_write_error(
                "joint promise requires its sticky born-v2 reader fence",
            ));
        }
        let range = plan.old_range().id;
        let mut promise = plan.promise().clone();
        promise.resolved_through = physical.range_index();
        if let Some(existing) = self.catalog.handoff_promises.get(&range) {
            if !existing.same_request(&promise)
                || existing.resolved_through != promise.resolved_through
            {
                return Err(range_write_error(
                    "different permanent handoff promise must not be overwritten",
                ));
            }
        }
        let scope_sha = plan.checksum_sha256()?;
        if let Some(existing) = self
            .catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|m| m.get(&range))
        {
            if existing.scope_sha256 != scope_sha {
                return Err(range_write_error(
                    "different native joint session cannot replace a pending decision",
                ));
            }
        }
        let history_sha256 = self.native_joint_history_commitment(range)?;
        let progress = self.progress(range);
        if progress.current_epoch > promise.activation_epoch
            || progress.last_index != progress.resolved_through
            || progress.resolved_through != physical.range_index()
        {
            return Err(range_write_error(
                "joint old raw prefix changed before permanent fence",
            ));
        }
        super::image_install_certification::ensure_installing_certification_maps(&mut self.catalog);
        self.catalog
            .sparse_committed_decisions
            .get_or_insert_with(BTreeMap::new);
        self.catalog
            .native_joint_handoffs
            .get_or_insert_with(BTreeMap::new)
            .entry(range)
            .or_insert(NativeJointLocalState {
                scope_sha256: scope_sha.clone(),
                session_id: promise.session_id,
                nonce: promise.nonce,
                old_closed_native_sha256: None,
                global_decision_sha256: None,
                global_decision_native_sha256: None,
                serving_activation: None,
            });
        self.catalog.handoff_promises.insert(range, promise.clone());
        self.catalog.format_version = self
            .catalog
            .format_version
            .max(NATIVE_JOINT_HANDOFF_CATALOG_FORMAT);
        let log = self.catalog.ranges.entry(range).or_insert(RangeWriteLog {
            current_epoch: promise.old_epoch,
            last_index: 0,
            resolved_through: 0,
            compacted_through: 0,
            entries: BTreeMap::new(),
        });
        log.current_epoch = promise.activation_epoch;
        // Rename/fsync ambiguity retains conservative in-memory promise/state.
        // No native closure or quorum observation is issued on this error.
        self.recovery_persistence_uncertain = true;
        self.persist()?;
        self.recovery_persistence_uncertain = false;
        let receipt = NativeJointOldPromiseReceipt {
            store_open_id: self.open_id,
            plan_sha256: scope_sha,
            promise,
            history_sha256,
        };
        receipt.require_current(self, plan)?;
        Ok(receipt)
    }
}
impl RangeWriteStore {
    /// Re-fsync ONLY the exact already-fenced joint session. This returns no
    /// closure/readiness vote and cannot turn a different session into proof.
    pub(crate) fn refsync_native_joint_session(
        &mut self,
        database: &crate::BicDb,
        plan: &NativeJointHandoffPlan,
        role: &RecoveryRoleAdmission,
    ) -> Result<()> {
        if !self.recovery_persistence_uncertain {
            return Ok(());
        }
        plan.require_role(role)?;
        let native = database.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        plan.require_actual_native(&native)?;
        self.require_native_writer_host_base()?;
        let state = self
            .catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|m| m.get(&plan.old_range().id))
            .ok_or_else(|| range_write_error("uncertain catalogue has no exact joint session"))?;
        if state.scope_sha256 != plan.checksum_sha256()?
            || self
                .catalog
                .handoff_promises
                .get(&plan.old_range().id)
                .is_none_or(|p| !p.same_request(plan.promise()))
        {
            return Err(range_write_error(
                "cannot re-fsync an unrelated uncertain joint context",
            ));
        }
        self.persist()?;
        self.recovery_persistence_uncertain = false;
        Ok(())
    }
    pub(crate) fn confirm_native_joint_old_close(
        &mut self,
        database: &crate::BicDb,
        plan: &NativeJointHandoffPlan,
        role: &RecoveryRoleAdmission,
        promise: &NativeJointOldPromiseReceipt,
        native: &crate::db::NativeJointOldCloseReceipt,
    ) -> Result<()> {
        native.require_current(database, plan, role, promise, self)?;
        let state = self
            .catalog
            .native_joint_handoffs
            .as_mut()
            .and_then(|m| m.get_mut(&plan.old_range().id))
            .ok_or_else(|| range_write_error("joint closure lost its actual logical promise"))?;
        if state
            .old_closed_native_sha256
            .as_ref()
            .is_some_and(|sha| sha != native.checksum_sha256())
        {
            return Err(range_write_error("joint native closure receipt conflicts"));
        }
        state.old_closed_native_sha256 = Some(native.checksum_sha256().into());
        self.recovery_persistence_uncertain = true;
        self.persist()?;
        self.recovery_persistence_uncertain = false;
        native.require_current(database, plan, role, promise, self)
    }
}

impl RangeWriteStore {
    pub(crate) fn confirm_native_joint_decision(
        &mut self,
        database: &crate::BicDb,
        admission: &crate::distribution_range_handoff::NativeJointDecisionPublicationAdmission,
        receipt: &crate::db::NativeJointDecisionReceipt,
    ) -> Result<()> {
        admission.require_live()?;
        receipt.require_current(database)?;
        self.require_native_catalog_durability(database.data_path())?;
        self.require_native_writer_host_base()?;
        if receipt.local_node_id() != &self.catalog.node_id
            || receipt.local_node_id() != admission.role().local_node_id()
            || receipt.decision_sha256() != admission.data().checksum_sha256()
            || database.actual_native_writer_enforcement_version()? != 1
            || !crate::format::native_joint_data_handoff_active(&self.root)?
        {
            return Err(range_write_error(
                "joint decision confirmation differs from actual local native/role",
            ));
        }
        let range = admission.data().old_range().id;
        let scope = admission.plan().checksum_sha256()?;
        if let Some(current) = self
            .catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|m| m.get(&range))
        {
            if current.scope_sha256 != scope
                || current.session_id != admission.data().promise().session_id
                || current.nonce != admission.data().promise().nonce
                || current
                    .global_decision_sha256
                    .as_ref()
                    .is_some_and(|v| v != receipt.decision_sha256())
                || current
                    .global_decision_native_sha256
                    .as_ref()
                    .is_some_and(|v| v != receipt.checksum_sha256())
            {
                return Err(range_write_error(
                    "different native joint decision cannot replace pending local session",
                ));
            }
        }
        super::image_install_certification::ensure_installing_certification_maps(&mut self.catalog);
        self.catalog
            .sparse_committed_decisions
            .get_or_insert_with(BTreeMap::new);
        let current = self
            .catalog
            .native_joint_handoffs
            .get_or_insert_with(BTreeMap::new)
            .entry(range)
            .or_insert(NativeJointLocalState {
                scope_sha256: scope,
                session_id: admission.data().promise().session_id,
                nonce: admission.data().promise().nonce,
                old_closed_native_sha256: None,
                global_decision_sha256: None,
                global_decision_native_sha256: None,
                serving_activation: None,
            });
        current.global_decision_sha256 = Some(receipt.decision_sha256().into());
        current.global_decision_native_sha256 = Some(receipt.checksum_sha256().into());
        current.validate()?;
        self.catalog.format_version = self
            .catalog
            .format_version
            .max(NATIVE_JOINT_HANDOFF_CATALOG_FORMAT);
        self.recovery_persistence_uncertain = true;
        self.persist()?;
        self.recovery_persistence_uncertain = false;
        admission.require_live()?;
        receipt.require_current(database)
    }
}
