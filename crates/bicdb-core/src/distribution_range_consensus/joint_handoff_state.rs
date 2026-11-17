//! Native joint catalogue closure. This is actual local observation, never an
//! acknowledgement, data quorum, materialization or serving permission.
use super::*;
use crate::distribution_range_handoff::NativeJointHandoffPlan;
use crate::distribution_range_recovery::RecoveryRoleAdmission;

#[cfg(feature = "tls-replication")]
mod registered_decision;
#[cfg(feature = "tls-replication")]
pub(crate) use registered_decision::{
    NativeRegisteredDecisionCatalogueAuthority, NativeRegisteredDecisionCataloguePublication,
};

#[cfg(all(test, feature = "tls-replication"))]
pub(crate) use registered_decision::{test_registered_decision_catalogue_cut, CatalogueCut};

mod bridge;
pub(crate) use bridge::NativeJointBridgeCatalogue;
mod activation;
mod committed_confirmation;
mod release;
mod retirement;
use activation::NativeJointCatalogueActivation;
pub(crate) use activation::{
    NativeJointCatalogueActivationFact, NativeJointCatalogueActivationReceipt,
};
pub(crate) use release::NativeJointReleaseObservedObligations;
use retirement::NativeJointSourceRetirement;
pub(crate) use retirement::NativeJointSourceRetirementReceipt;

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
/// True when a durable joint handoff promise of `range` is what moved the
/// local data epoch (`log_epoch`) past the epoch of a completed and activated
/// normal recovery (`recovery_epoch`, anchored at `anchor_index`): the promise
/// starts at or after that epoch, its cut covers the anchor, the log stands
/// exactly at its activation epoch, and the same session's joint state is
/// retained. The older anchor is then history of a superseded epoch, as
/// `native_joint_covers_prior_recovery_context` already reads it.
pub(super) fn joint_promise_supersedes_activated_recovery(
    catalog: &RangeWriteCatalog,
    range: RangeId,
    recovery_epoch: u64,
    anchor_index: u64,
    log_epoch: u64,
) -> bool {
    catalog.handoff_promises.get(&range).is_some_and(|promise| {
        promise.range_id == range
            && recovery_epoch <= promise.old_epoch
            && promise.old_epoch < promise.activation_epoch
            && log_epoch == promise.activation_epoch
            && promise.resolved_through >= anchor_index
            && catalog
                .native_joint_handoffs
                .as_ref()
                .and_then(|states| states.get(&range))
                .is_some_and(|state| {
                    state.session_id == promise.session_id && state.nonce == promise.nonce
                })
    })
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
    /// Written only by a retired source; see `retirement`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_retirement: Option<NativeJointSourceRetirement>,
}
impl NativeJointLocalState {
    /// Read-only binding check; a hash/decoded state never mints recovery.
    pub(super) fn require_current_joint_closure_pointer(
        &self,
        data: &crate::distribution_range_handoff::NativeJointDecisionData,
        old_close: Option<&str>,
    ) -> Result<()> {
        self.validate()?;
        if self.scope_sha256 != data.scope_sha256()?
            || self.global_decision_sha256.as_deref() != Some(data.checksum_sha256())
            || self.old_closed_native_sha256.as_deref() != old_close
        {
            return Err(range_write_error("current joint recovery local promise lost exact protected closure/decision pointer"));
        }
        Ok(())
    }
    pub(super) fn validate(&self) -> Result<()> {
        if let Some(activation) = &self.serving_activation {
            activation.validate()?;
            if self.global_decision_sha256.as_deref() != Some(activation.decision_sha256()) {
                return Err(range_write_error(
                    "joint catalogue activation lost complete decision binding",
                ));
            }
        }
        if let Some(retirement) = &self.source_retirement {
            retirement.validate()?;
            // A retired source never activated this handoff as a new member.
            if self.global_decision_sha256.as_deref() != Some(retirement.decision_sha256())
                || self.old_closed_native_sha256.is_none()
                || self.serving_activation.is_some()
            {
                return Err(range_write_error(
                    "joint source retirement lost its closed decision binding",
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
        let receipt = self.persist_native_joint_old_promise_before_final_check(
            database, plan, role, physical, controlled,
        )?;
        receipt.require_current(self, plan)?;
        Ok(receipt)
    }
    /// The retained slot belongs to the actual original DATA host. A completed
    /// fsync result is recorded BEFORE subsequent fallible checks; an uncertain
    /// persistence error still produces no durable receipt.
    #[cfg(feature = "tls-replication")]
    pub(crate) fn persist_native_joint_old_promise_retained(
        &mut self,
        database: &crate::BicDb,
        plan: &NativeJointHandoffPlan,
        role: &RecoveryRoleAdmission,
        physical: &crate::db::NativeJointOldPhysicalProof,
        controlled: &crate::distribution_range_mutation::ControlledFrozenMutationPermit,
        retained: &mut Option<Arc<NativeJointOldPromiseReceipt>>,
    ) -> Result<Arc<NativeJointOldPromiseReceipt>> {
        if retained.is_none() {
            let actual = self.persist_native_joint_old_promise_before_final_check(
                database, plan, role, physical, controlled,
            )?;
            *retained = Some(Arc::new(actual));
        }
        let receipt = retained.as_ref().unwrap();
        receipt.require_current(self, plan)?;
        Ok(receipt.clone())
    }
    fn persist_native_joint_old_promise_before_final_check(
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
        let previous = self.catalog.clone();
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
                source_retirement: None,
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
        // The permanent promise is never made durable inside a catalogue that
        // this store's own loader refuses: such a file would end every later
        // custody check and every reopen of this member, with the promise
        // already permanent. Refused here, nothing was written and the
        // previous catalogue is unchanged.
        if let Err(error) = RangeWriteEnvelope::new(self.catalog.clone()).and_then(|e| e.verify()) {
            self.catalog = previous;
            return Err(range_write_error(format!(
                "joint old close refused before the permanent promise: the fenced catalogue would not be loadable: {error}"
            )));
        }
        drop(previous);
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
        self.confirm_native_joint_decision_inner(database, admission, receipt)
    }
    fn confirm_native_joint_decision_inner(
        &mut self,
        database: &crate::BicDb,
        admission: &crate::distribution_range_handoff::NativeJointDecisionPublicationAdmission,
        receipt: &crate::db::NativeJointDecisionReceipt,
    ) -> Result<()> {
        admission.require_live()?;
        admission.require_native_metadata_point(database, self, receipt.native_admission())?;
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
                source_retirement: None,
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

#[cfg(test)]
mod superseded_recovery_tests {
    use super::*;

    /// The range catalogue an old voter made durable when its joint source
    /// close advanced the range log from epoch 1 to the activation epoch 4
    /// while the catalogue retained the activated record of a hot recovery
    /// of epoch 1 (anchored at index 7; the promise's cut is index 8). Taken
    /// from a five-process run; before the loader accepted this state the
    /// member refused every later request of the range and could not reopen.
    fn captured() -> RangeWriteCatalog {
        let envelope: RangeWriteEnvelope = serde_json::from_slice(include_bytes!(
            "testdata/joint_promise_over_activated_recovery.json"
        ))
        .unwrap();
        envelope.catalog
    }
    fn verify(catalog: RangeWriteCatalog) -> Result<RangeWriteCatalog> {
        RangeWriteEnvelope::new(catalog)?.verify()
    }
    fn range() -> RangeId {
        RangeId::new(1).unwrap()
    }

    #[test]
    fn joint_promise_over_an_activated_recovery_of_an_older_epoch_is_loadable() {
        let catalog = captured();
        let log = &catalog.ranges[&range()];
        let promise = &catalog.handoff_promises[&range()];
        let fence = &catalog.recovery_fences[&range()];
        assert_eq!(
            (
                log.current_epoch,
                promise.old_epoch,
                promise.activation_epoch
            ),
            (4, 2, 4)
        );
        assert_eq!(fence.scope.range_epoch, 1);
        assert!(matches!(
            fence.phase,
            recovery_state::RecoveryFencePhase::NativeActivated(_)
        ));
        assert!(joint_promise_supersedes_activated_recovery(
            &catalog,
            range(),
            1,
            7,
            4
        ));
        assert_eq!(verify(catalog.clone()).unwrap(), catalog);
    }

    #[test]
    fn a_normal_recovery_is_unfinished_until_it_is_activated_or_completed() {
        use recovery_state::{
            catalog_normal_recovery_unfinished as unfinished, RecoveryFencePhase,
        };
        let mut catalog = captured();
        // Activated: finished. No record: nothing unfinished.
        assert!(!unfinished(&catalog, range()));
        assert!(!unfinished(&catalog, RangeId::new(3).unwrap()));
        let activated = catalog.recovery_fences[&range()].phase.clone();
        let RecoveryFencePhase::NativeActivated(point) = &activated else {
            panic!("captured record is activated")
        };
        let completed = point.completed.clone();
        // Fenced, prepared and completed-frozen recoveries are unfinished.
        for phase in [
            RecoveryFencePhase::Active,
            RecoveryFencePhase::NativePrepared(completed.point.clone()),
            RecoveryFencePhase::NativeCompleted(completed.clone()),
        ] {
            catalog.recovery_fences.get_mut(&range()).unwrap().phase = phase.clone();
            assert!(unfinished(&catalog, range()), "{phase:?}");
            // The record is named for the operator: its own epoch, the
            // log's epoch, the record and who fenced it. The captured
            // record is of epoch 1 under a log at epoch 4; it is reported,
            // never ignored for being older than the log.
            let named =
                recovery_state::catalog_unfinished_normal_recovery(&catalog, range()).unwrap();
            let fence = &catalog.recovery_fences[&range()];
            assert_eq!(
                (
                    named.range_epoch,
                    named.log_epoch,
                    named.recovery_nonce,
                    &named.initiator_node_id
                ),
                (
                    1,
                    4,
                    fence.scope.recovery_nonce,
                    &fence.scope.initiator_node_id
                )
            );
            let text = named.to_string();
            assert!(
                text.contains(&format!("recovery record {}", fence.scope.recovery_nonce))
                    && text.contains(&format!("in phase {}", named.phase))
                    && text.contains("for range epoch 1")
                    && text.contains("is at epoch 4"),
                "{text}"
            );
        }
        assert_eq!(
            recovery_state::catalog_unfinished_normal_recovery(&captured(), range()),
            None
        );
        catalog.recovery_fences.get_mut(&range()).unwrap().phase =
            RecoveryFencePhase::Completed { resolved_index: 7 };
        assert!(!unfinished(&catalog, range()));
        // A prepared or completed-frozen recovery under the joint promise
        // stays a catalogue the loader refuses: only the activated one is
        // superseded by the promise.
        for phase in [
            RecoveryFencePhase::NativePrepared(completed.point.clone()),
            RecoveryFencePhase::NativeCompleted(completed.clone()),
        ] {
            let mut frozen = captured();
            frozen.recovery_fences.get_mut(&range()).unwrap().phase = phase.clone();
            let refused = verify(frozen).unwrap_err().to_string();
            assert!(
                refused.contains("native preparation disagrees with actual resolved catalogue"),
                "{phase:?}: {refused}"
            );
        }
    }

    #[test]
    fn an_activated_recovery_of_another_epoch_stays_refused_without_its_exact_joint_promise() {
        let refused = |change: &dyn Fn(&mut RangeWriteCatalog)| {
            let mut catalog = captured();
            change(&mut catalog);
            verify(catalog).unwrap_err().to_string()
        };
        let disagrees = "native preparation disagrees with actual resolved catalogue";
        // The log stands beyond the promise's activation epoch.
        let beyond = refused(&|catalog| {
            catalog.ranges.get_mut(&range()).unwrap().current_epoch = 5;
        });
        assert!(beyond.contains(disagrees), "{beyond}");
        // The promise's cut does not cover the recovery's anchor.
        let uncovered = refused(&|catalog| {
            catalog
                .handoff_promises
                .get_mut(&range())
                .unwrap()
                .resolved_through = 6;
        });
        assert!(uncovered.contains(disagrees), "{uncovered}");
        // The promise starts before the recovery's epoch.
        let earlier = refused(&|catalog| {
            catalog
                .handoff_promises
                .get_mut(&range())
                .unwrap()
                .old_epoch = 0;
        });
        eprintln!("SUPERSEDED_RECOVERY earlier promise: {earlier}");
        // Epoch 0 is no epoch: this promise is refused as a promise, before
        // the bound on its start is read. The bound itself is pinned by
        // `the_loader_accepts_a_superseded_recovery_only_up_to_the_promise_old_epoch`.
        assert!(
            earlier.contains("invalid permanent range handoff promise"),
            "{earlier}"
        );
        // The joint state of the promise's session is not retained.
        let other_session = refused(&|catalog| {
            catalog
                .native_joint_handoffs
                .as_mut()
                .unwrap()
                .get_mut(&range())
                .unwrap()
                .nonce = Uuid::now_v7();
        });
        eprintln!("SUPERSEDED_RECOVERY other session: {other_session}");
        assert!(other_session.contains(disagrees), "{other_session}");
        // The same for the other half of the session identity, and for a
        // joint state that is not retained at all.
        let other_id = refused(&|catalog| {
            catalog
                .native_joint_handoffs
                .as_mut()
                .unwrap()
                .get_mut(&range())
                .unwrap()
                .session_id = Uuid::now_v7();
        });
        eprintln!("SUPERSEDED_RECOVERY other session id: {other_id}");
        assert!(other_id.contains(disagrees), "{other_id}");
        let no_state = refused(&|catalog| {
            catalog
                .native_joint_handoffs
                .as_mut()
                .unwrap()
                .remove(&range());
        });
        eprintln!("SUPERSEDED_RECOVERY no joint state: {no_state}");
        assert!(no_state.contains(disagrees), "{no_state}");
        // The helper itself on the session binding: the promise supersedes
        // the recovery only together with the joint state of its own
        // session and nonce.
        for change in [
            (&|state: &mut NativeJointLocalState| state.nonce = Uuid::now_v7())
                as &dyn Fn(&mut NativeJointLocalState),
            &|state: &mut NativeJointLocalState| state.session_id = Uuid::now_v7(),
        ] {
            let mut catalog = captured();
            change(
                catalog
                    .native_joint_handoffs
                    .as_mut()
                    .unwrap()
                    .get_mut(&range())
                    .unwrap(),
            );
            assert!(!joint_promise_supersedes_activated_recovery(
                &catalog,
                range(),
                1,
                7,
                4
            ));
        }
        let mut catalog = captured();
        catalog.native_joint_handoffs.as_mut().unwrap().clear();
        assert!(!joint_promise_supersedes_activated_recovery(
            &catalog,
            range(),
            1,
            7,
            4
        ));
        // The same catalogue at the recovery's own epoch without any promise
        // is the ordinary recovered state and loads.
        let mut ordinary = captured();
        ordinary.handoff_promises.clear();
        ordinary.native_joint_handoffs.as_mut().unwrap().clear();
        ordinary.ranges.get_mut(&range()).unwrap().current_epoch = 1;
        verify(ordinary).unwrap();
        // The helper itself, on each binding.
        let catalog = captured();
        for (epoch, anchor, log, expected) in [
            (1, 7, 4, true),
            (2, 8, 4, true),
            (3, 7, 4, false),
            (1, 9, 4, false),
            (1, 7, 5, false),
            (1, 7, 2, false),
        ] {
            assert_eq!(
                joint_promise_supersedes_activated_recovery(&catalog, range(), epoch, anchor, log),
                expected,
                "recovery epoch {epoch} anchor {anchor} log epoch {log}"
            );
        }
        assert!(!joint_promise_supersedes_activated_recovery(
            &catalog,
            RangeId::new(3).unwrap(),
            1,
            3,
            4
        ));
    }

    /// The loader's `old_epoch` bound: an activated recovery is superseded
    /// only by a promise that starts at or after the recovery's own epoch.
    /// The captured promise starts at epoch 2 and activates epoch 4; the
    /// recovery record is moved to each epoch around that start.
    #[test]
    fn the_loader_accepts_a_superseded_recovery_only_up_to_the_promise_old_epoch() {
        let disagrees = "native preparation disagrees with actual resolved catalogue";
        let with_recovery_epoch = |epoch: u64| {
            let mut catalog = captured();
            let fence = catalog.recovery_fences.get_mut(&range()).unwrap();
            fence.scope.range_epoch = epoch;
            for marker in fence.historical_certifications.values_mut() {
                marker.range_epoch = epoch;
            }
            verify(catalog)
        };
        assert_eq!(captured().handoff_promises[&range()].old_epoch, 2);
        // Below and exactly at the promise's start: superseded, loadable.
        with_recovery_epoch(1).unwrap();
        with_recovery_epoch(2).unwrap();
        // Past the promise's start, up to and beyond its activation epoch:
        // a recovery of such an epoch was not ended by this promise.
        for epoch in [3, 5] {
            let refused = with_recovery_epoch(epoch).unwrap_err().to_string();
            eprintln!("SUPERSEDED_RECOVERY recovery epoch {epoch}: {refused}");
            assert!(refused.contains(disagrees), "epoch {epoch}: {refused}");
        }
    }
}
