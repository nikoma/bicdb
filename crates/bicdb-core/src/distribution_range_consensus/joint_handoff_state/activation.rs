//! Joint catalogue activation is conditional on actual protected native NEW
//! publication. It stays CLOSED until the real frozen owner is released and a
//! same-open guard is armed. Old promises and history are never removed here.
use super::*;
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::distribution_range_handoff::NativeJointOpenAdmission;
use crate::distribution_range_image_install::NativeImageCatalogueFinalization;
use crate::distribution_range_mutation::{FrozenRangeMutationPermit, RangeMutationAuthority};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeJointCatalogueActivation {
    version: u32,
    decision_sha256: String,
    native_bridge_sha256: String,
    native_preparation_sha256: String,
    promise: RangeHandoffPromise,
    range: RangeDescriptor,
    range_index: u64,
    logical_genesis_sha256: String,
    logical_history_sha256: String,
    actual_local_visibility_sequence: u64,
}
impl NativeJointCatalogueActivation {
    pub(super) fn decision_sha256(&self) -> &str {
        &self.decision_sha256
    }
    pub(super) fn validate(&self) -> Result<()> {
        self.promise.validate()?;
        let digest = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
        if self.version != 1
            || self.range.id != self.promise.range_id
            || self.range.epoch != self.promise.activation_epoch
            || [
                &self.decision_sha256,
                &self.native_bridge_sha256,
                &self.native_preparation_sha256,
                &self.logical_genesis_sha256,
                &self.logical_history_sha256,
            ]
            .iter()
            .any(|s| !digest(s))
        {
            return Err(range_write_error("damaged joint catalogue activation"));
        }
        Ok(())
    }
    fn from_actual(native: &crate::db::NativeJointServingBridgeReceipt) -> Result<Self> {
        let data = native.data();
        let result = Self {
            version: 1,
            decision_sha256: data.checksum_sha256().into(),
            native_bridge_sha256: native.checksum_sha256().into(),
            native_preparation_sha256: native.native_preparation_sha256().into(),
            promise: data.promise().clone(),
            range: data.activation_range().clone(),
            range_index: data.range_index(),
            logical_genesis_sha256: data.logical_genesis_sha256().into(),
            logical_history_sha256: data.logical_history_sha256().into(),
            actual_local_visibility_sequence: native.actual_local_visibility_sequence(),
        };
        result.validate()?;
        Ok(result)
    }
    fn checksum(&self) -> Result<String> {
        self.validate()?;
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(self)?)))
    }
}

/// Actual durable catalogue/native binding. It is not a fresh range read,
/// floor setter or an already-released open. Creation keeps the original TTL.
pub(crate) struct NativeJointCatalogueActivationReceipt {
    store_open_id: Uuid,
    activated: NativeJointCatalogueActivation,
    native: Arc<crate::db::NativeJointServingBridgeReceipt>,
    cancellation: CancellationToken,
}
/// Actual protected catalogue/native fact, with NO request permission or TTL.
/// Minted only by reading this store against an actual protected native bridge.
pub(crate) struct NativeJointCatalogueActivationFact {
    store_open_id: Uuid,
    activated: NativeJointCatalogueActivation,
    native: Arc<crate::db::NativeJointServingBridgeReceipt>,
}
impl NativeJointCatalogueActivationFact {
    pub(crate) fn native(&self) -> &crate::db::NativeJointServingBridgeReceipt {
        &self.native
    }
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.store_open_id
    }
    pub(crate) fn activation_sha256(&self) -> Result<String> {
        self.activated.checksum()
    }
    pub(crate) fn require_current(
        &self,
        database: &crate::BicDb,
        store: &RangeWriteStore,
    ) -> Result<()> {
        let scope = self.native.current_read_scope(database)?;
        self.require_current_with_scope(&scope, store)
    }
    pub(crate) fn require_current_with_scope(
        &self,
        scope: &crate::db::NativeJointBridgeReadScope<'_>,
        store: &RangeWriteStore,
    ) -> Result<()> {
        scope.require_native(&self.native)?;
        let database = scope.database();
        store.require_native_catalog_durability(database.data_path())?;
        store.require_native_writer_host_base()?;
        if self.store_open_id != store.open_id
            || self.native.local_node_id() != &store.catalog.node_id
            || self.activated != NativeJointCatalogueActivation::from_actual(&self.native)?
            || store
                .catalog
                .image_install_intents
                .contains_key(&self.activated.range.id)
            || store
                .catalog
                .native_joint_handoffs
                .as_ref()
                .and_then(|m| m.get(&self.activated.range.id))
                .and_then(|s| s.serving_activation.as_ref())
                != Some(&self.activated)
        {
            return Err(range_write_error(
                "protected joint activation actual store/open/native binding changed",
            ));
        }
        Ok(())
    }
}
impl NativeJointCatalogueActivationReceipt {
    pub(crate) fn range(&self) -> &RangeDescriptor {
        &self.activated.range
    }
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.store_open_id
    }
    pub(crate) fn decision_sha256(&self) -> &str {
        &self.activated.decision_sha256
    }
    pub(crate) fn native_bridge_sha256(&self) -> &str {
        &self.activated.native_bridge_sha256
    }
    pub(crate) fn activation_sha256(&self) -> Result<String> {
        self.activated.checksum()
    }
    pub(crate) fn native(&self) -> &crate::db::NativeJointServingBridgeReceipt {
        &self.native
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.cancellation.check()
    }
    pub(crate) fn require_current(
        &self,
        database: &crate::BicDb,
        store: &RangeWriteStore,
    ) -> Result<()> {
        self.require_live()?;
        self.native.require_current(database)?;
        store.require_native_catalog_durability(database.data_path())?;
        store.require_native_writer_host_base()?;
        if self.store_open_id != store.open_id
            || self.native.local_node_id() != &store.catalog.node_id
            || self.activated != NativeJointCatalogueActivation::from_actual(&self.native)?
            || store
                .catalog
                .image_install_intents
                .contains_key(&self.activated.range.id)
            || store
                .catalog
                .native_joint_handoffs
                .as_ref()
                .and_then(|states| states.get(&self.activated.range.id))
                .and_then(|state| state.serving_activation.as_ref())
                != Some(&self.activated)
        {
            return Err(range_write_error(
                "joint native/catalogue activation actual open/binding changed",
            ));
        }
        Ok(())
    }
}
impl RangeWriteStore {
    /// Reconcile ONLY an already installed exact activation after fsync
    /// ambiguity. This cannot create a new activation or discard obligations.
    pub(crate) fn refsync_joint_activation_from_actual_host(
        &mut self,
        database: &crate::BicDb,
        host: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        native: &crate::db::NativeJointServingBridgeReceipt,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<()> {
        if !self.recovery_persistence_uncertain {
            return Ok(());
        }
        parent.check()?;
        host.require_live_request(business)?;
        let data = native.data();
        data.require_metadata_domain(
            host.committed_topology(business)?,
            host.restore_activation(business)?,
        )?;
        let (term, index, _) = native.metadata_anchor();
        if host.requester_node_id() != &self.catalog.node_id
            || data
                .new_members()
                .get(&self.catalog.node_id)
                .is_none_or(|m| m.incarnation != host.requester_incarnation())
            || host.term() < term
            || host.read_index() < index
            || !self.fsync
            || self.root.canonicalize()? != database.data_path().canonicalize()?
            || !crate::format::native_joint_data_handoff_active(database.data_path())?
        {
            return Err(range_write_error(
                "joint activation refsync has no actual same-open local HostMQ/domain",
            ));
        }
        native.require_current(database)?;
        self.require_native_writer_host_base()?;
        self.native_writer_host_state()?
            .require_database(database, data.activation_range())?;
        let range = data.old_range().id;
        let expected = NativeJointCatalogueActivation::from_actual(native)?;
        let state = self
            .catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|m| m.get(&range))
            .ok_or_else(|| range_write_error("uncertain joint open lost protected decision"))?;
        if state.serving_activation.as_ref() != Some(&expected)
            || state.global_decision_sha256.as_deref() != Some(data.checksum_sha256())
            || self.catalog.image_install_intents.contains_key(&range)
        {
            return Err(range_write_error(
                "joint activation ambiguity is not the exact existing protected activation",
            ));
        }
        let gate = database.range_mutation_gate();
        gate.validate_quiescent_frozen(range, frozen)?;
        let controlled = gate.try_controlled_mutation(frozen)?;
        self.persist()?;
        self.recovery_persistence_uncertain = false;
        drop(controlled);
        host.require_live_request(business)?;
        parent.check()
    }
    pub(crate) fn observe_native_joint_activation_fact(
        &self,
        database: &crate::BicDb,
        native: Arc<crate::db::NativeJointServingBridgeReceipt>,
    ) -> Result<Option<NativeJointCatalogueActivationFact>> {
        let scope = native.current_read_scope(database)?;
        self.observe_native_joint_activation_fact_with_scope(&scope, native.clone())
    }
    pub(crate) fn observe_native_joint_activation_fact_with_scope(
        &self,
        scope: &crate::db::NativeJointBridgeReadScope<'_>,
        native: Arc<crate::db::NativeJointServingBridgeReceipt>,
    ) -> Result<Option<NativeJointCatalogueActivationFact>> {
        scope.require_native(&native)?;
        let database = scope.database();
        self.require_native_catalog_durability(database.data_path())?;
        let range = native.data().old_range().id;
        let Some(activated) = self
            .catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|m| m.get(&range))
            .and_then(|s| s.serving_activation.as_ref())
            .cloned()
        else {
            return Ok(None);
        };
        let fact = NativeJointCatalogueActivationFact {
            store_open_id: self.open_id,
            activated,
            native,
        };
        fact.require_current_with_scope(scope, self)?;
        Ok(Some(fact))
    }
    pub(crate) fn require_joint_open_phase_not_released(
        &self,
        fact: &NativeJointCatalogueActivationFact,
    ) -> Result<()> {
        if let Some(guard) = self
            .native_joint_activated_opens
            .get(&fact.activated.range.id)
        {
            guard.require_not_armed(self.open_id, &fact.activation_sha256()?)?;
        }
        Ok(())
    }
    pub(crate) fn recover_native_joint_catalogue_activation(
        &self,
        database: &crate::BicDb,
        admission: &NativeJointOpenAdmission,
    ) -> Result<Option<NativeJointCatalogueActivationReceipt>> {
        admission.require_live()?;
        admission.committed().require_current(database)?;
        let Some(fact) =
            self.observe_native_joint_activation_fact(database, admission.native_receipt())?
        else {
            return Ok(None);
        };
        self.require_joint_open_phase_not_released(&fact)?;
        let receipt = NativeJointCatalogueActivationReceipt {
            store_open_id: fact.store_open_id,
            activated: fact.activated,
            native: fact.native,
            cancellation: admission.cancellation().clone(),
        };
        receipt.require_current(database, self)?;
        admission.require_live()?;
        Ok(Some(receipt))
    }
    /// Retry only the exact protected native/catalogue activation. This is
    /// real whole-catalogue fsync, never rollback or a caller durability flag.
    pub(crate) fn refsync_native_joint_catalogue_activation(
        &mut self,
        database: &crate::BicDb,
        admission: &NativeJointOpenAdmission,
    ) -> Result<()> {
        if !self.recovery_persistence_uncertain {
            return Ok(());
        }
        admission.require_live()?;
        admission.committed().require_current(database)?;
        let native = admission.native_receipt();
        native.require_current(database)?;
        self.require_native_writer_host_base()?;
        if !self.fsync
            || self.root.canonicalize()? != database.data_path().canonicalize()?
            || !crate::format::native_joint_data_handoff_active(database.data_path())?
        {
            return Err(range_write_error(
                "joint activation re-fsync has another fsync/root/reader domain",
            ));
        }
        let range = native.data().old_range().id;
        let expected = NativeJointCatalogueActivation::from_actual(&native)?;
        let state = self
            .catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|states| states.get(&range))
            .ok_or_else(|| {
                range_write_error("uncertain joint activation has no exact durable session")
            })?;
        if state.serving_activation.as_ref() != Some(&expected)
            || state.global_decision_sha256.as_deref() != Some(native.data().checksum_sha256())
            || self.catalog.image_install_intents.contains_key(&range)
        {
            return Err(range_write_error(
                "cannot re-fsync an unrelated uncertain joint activation",
            ));
        }
        self.persist()?;
        self.recovery_persistence_uncertain = false;
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn persist_native_joint_catalogue_activation(
        &mut self,
        database: &crate::BicDb,
        admission: &NativeJointOpenAdmission,
        original: &crate::distribution_range_handoff::CommittedNativeJointDecisionObservation,
        catalogue: &NativeJointBridgeCatalogue,
        image: Option<&NativeImageCatalogueFinalization>,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<NativeJointCatalogueActivationReceipt> {
        #[cfg(test)]
        let mut trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "open-kernel-catalogue-total",
            &self.catalog.node_id,
            admission.cancellation(),
        );
        parent.check()?;
        admission.require_external_publications_current()?;
        admission.committed().require_current(database)?;
        let native = admission.native_receipt();
        native.require_current(database)?;
        let data = admission.data();
        let range = data.old_range().id;
        let candidate = NativeJointCatalogueActivation::from_actual(&native)?;
        if native.local_node_id() != &self.catalog.node_id || data != original.data() {
            return Err(range_write_error(
                "joint catalogue activation targets another actual member/decision",
            ));
        }
        database
            .range_mutation_gate()
            .validate_quiescent_frozen(range, frozen)?;
        let existing = self
            .catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|states| states.get(&range))
            .and_then(|state| state.serving_activation.as_ref())
            .cloned();
        if let Some(existing) = &existing {
            // Re-fsync exact native success/catalogue ambiguity. No old intent
            // is reinterpreted, no additional row clear or counter advance.
            if existing != &candidate || self.catalog.image_install_intents.contains_key(&range) {
                return Err(range_write_error(
                    "joint catalogue activation retry changed local native point/intent",
                ));
            }
        } else {
            catalogue.require_current(self, database, original, image, frozen)?;
        }
        // Complete ACTUAL local history, including any sparse/certified tail.
        // Never discard an obligation merely because the old promise is lower.
        let actual = database.current_native_range_source_lineage(range)?;
        let progress = self.progress(range);
        if progress.last_index != actual.range_index()
            || progress.resolved_through != actual.range_index()
            || actual.range_index() < data.range_index()
            || progress.compacted_through > actual.range_index()
            || progress.current_epoch > data.activation_range().epoch
        {
            return Err(range_write_error(
                "joint activation has an unresolved/higher unmatched local obligation",
            ));
        }
        let decisions = self.collect_native_resolved_decisions(range, actual.range_index())?;
        let gate = database.range_mutation_gate();
        let pin = gate.try_source_capture_read_pin(range, frozen)?;
        for (index, expected) in decisions {
            parent.check()?;
            admission.require_live()?;
            let step = database.export_native_logical_history_step_with_source_pin(
                &actual, frozen, &pin, index, 1, parent,
            )?;
            let entry = step
                .entries
                .first()
                .ok_or_else(|| range_write_error("joint activation actual history missing"))?;
            let matches = match (&expected, &entry.outcome) {
                (
                    NativeReadCatalogueDecision::Applied(expected),
                    crate::db::NativeLogicalHistoryOutcomeV1::Applied {
                        command_checksum_sha256,
                    },
                ) => expected == command_checksum_sha256,
                (
                    NativeReadCatalogueDecision::Aborted,
                    crate::db::NativeLogicalHistoryOutcomeV1::Aborted,
                ) => true,
                _ => false,
            };
            if entry.index != index || !matches {
                return Err(range_write_error(
                    "joint activation contradicts actual native command history",
                ));
            }
        }
        drop(pin);
        let controlled = gate.try_controlled_mutation(frozen)?;
        gate.validate(range, &RangeMutationAuthority::Controlled(&controlled))?;
        let state = self
            .catalog
            .native_joint_handoffs
            .as_mut()
            .and_then(|states| states.get_mut(&range))
            .ok_or_else(|| {
                range_write_error("joint activation lost its durable complete decision")
            })?;
        if state.global_decision_sha256.as_deref() != Some(data.checksum_sha256())
            || state.session_id != data.promise().session_id
            || state.nonce != data.promise().nonce
        {
            return Err(range_write_error(
                "joint activation changed its permanent plan/session",
            ));
        }
        // Retiring the exact adopted image refusal and recording the new
        // CLOSED activation happen in the SAME fsynced catalogue publication.
        self.recovery_persistence_uncertain = true;
        state.serving_activation = Some(candidate.clone());
        self.catalog
            .ranges
            .entry(range)
            .or_insert_with(|| RangeWriteLog {
                current_epoch: data.activation_range().epoch,
                last_index: 0,
                resolved_through: 0,
                compacted_through: 0,
                entries: BTreeMap::new(),
            })
            .current_epoch = data.activation_range().epoch;
        if existing.is_none() && image.is_some() {
            self.catalog.image_install_intents.remove(&range);
        }
        let persisted = self.persist();
        #[cfg(test)]
        eprintln!("ACTUAL_JOINT_OPEN_CATALOGUE_PERSIST node={} success={} uncertain={} range={} activation_sha={}",self.catalog.node_id,persisted.is_ok(),self.recovery_persistence_uncertain,range,candidate.checksum().unwrap_or_else(|e|format!("diagnostic-unavailable:{e}")));
        persisted?;
        self.recovery_persistence_uncertain = false;
        drop(controlled);
        let receipt = NativeJointCatalogueActivationReceipt {
            store_open_id: self.open_id,
            activated: candidate,
            native,
            cancellation: admission.cancellation().clone(),
        };
        parent.check()?;
        receipt.require_current(database, self)?;
        #[cfg(test)]
        trace.complete();
        Ok(receipt)
    }
    /// A catalogue activation is a refusal until an actual same-open guard is
    /// installed AND armed after the physical frozen owner has gone away.
    pub(in crate::distribution_range_consensus) fn native_joint_open_allowed(
        &self,
        range: RangeId,
    ) -> Result<bool> {
        let Some(activation) = self
            .catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|states| states.get(&range))
            .and_then(|state| state.serving_activation.as_ref())
        else {
            return Ok(false);
        };
        self.require_native_catalog_durability(&self.root)?;
        activation.validate()?;
        let guard = self
            .native_joint_activated_opens
            .get(&range)
            .ok_or_else(|| {
                range_write_error(
                    "joint catalogue activation requires actual same-open release proof",
                )
            })?;
        guard.require_current(self.open_id, &activation.checksum()?)?;
        Ok(true)
    }
    pub(in crate::distribution_range_consensus) fn native_joint_command_allowed(
        &self,
        command: &RangeWriteCommand,
    ) -> Result<bool> {
        if !self.native_joint_open_allowed(command.range_id)? {
            return Ok(false);
        }
        let activation = self.catalog.native_joint_handoffs.as_ref().unwrap()[&command.range_id]
            .serving_activation
            .as_ref()
            .unwrap();
        self.native_joint_activated_opens[&command.range_id].require_command(
            self.open_id,
            &activation.checksum()?,
            command,
        )?;
        if command.index <= activation.range_index {
            return Err(range_write_error(
                "ordinary joint suffix cannot rewrite the protected closed prefix",
            ));
        }
        Ok(true)
    }
    pub(in crate::distribution_range_consensus) fn native_joint_read_scope_allowed(
        &self,
        scope: &crate::distribution_native_read::NativeRangeReadScope,
    ) -> Result<bool> {
        if !self.native_joint_open_allowed(scope.range.id)? {
            return Ok(false);
        }
        let activation = self.catalog.native_joint_handoffs.as_ref().unwrap()[&scope.range.id]
            .serving_activation
            .as_ref()
            .unwrap();
        self.native_joint_activated_opens[&scope.range.id].require_read_scope(
            self.open_id,
            &activation.checksum()?,
            scope,
        )?;
        Ok(true)
    }
    pub(in crate::distribution_range_consensus) fn native_joint_covers_prior_recovery_context(
        &self,
        range: RangeId,
    ) -> Result<bool> {
        if !self.native_joint_open_allowed(range)? {
            return Ok(false);
        }
        let activation = self.catalog.native_joint_handoffs.as_ref().unwrap()[&range]
            .serving_activation
            .as_ref()
            .unwrap();
        let Some(saved) = self.catalog.recovery_fences.get(&range) else {
            return Ok(true);
        };
        if saved.scope.range_epoch > activation.promise.old_epoch {
            return Ok(false);
        }
        match &saved.scope.purpose {
            MetadataRecoveryRolePurpose::NormalRecovery => Ok(true),
            MetadataRecoveryRolePurpose::CommittedHandoff {
                relocation_id,
                plan_id,
                session_id,
                nonce,
            } => Ok(*relocation_id == activation.promise.relocation_id
                && plan_id == &activation.promise.plan_id
                && *session_id == activation.promise.session_id
                && *nonce == activation.promise.nonce),
            _ => Ok(false),
        }
    }
    pub(crate) fn install_native_joint_activated_open(
        &mut self,
        database: &crate::BicDb,
        receipt: &NativeJointCatalogueActivationReceipt,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        receipt.require_current(database, self)?;
        database
            .range_mutation_gate()
            .validate_quiescent_frozen(receipt.range().id, frozen)?;
        if let Some(guard) = self.native_joint_activated_opens.get(&receipt.range().id) {
            return guard.require_not_armed(self.open_id, &receipt.activation_sha256()?);
        }
        let guard = database.native_joint_activated_open_guard(self, receipt, frozen)?;
        self.native_joint_activated_opens
            .insert(receipt.range().id, Arc::new(guard));
        Ok(())
    }
    pub(crate) fn arm_native_joint_activated_open(
        &self,
        database: &crate::BicDb,
        receipt: &NativeJointCatalogueActivationReceipt,
    ) -> Result<()> {
        receipt.require_current(database, self)?;
        self.native_joint_activated_opens
            .get(&receipt.range().id)
            .ok_or_else(|| range_write_error("joint open has no actual release guard"))?
            .arm_after_actual_release(self.open_id, &receipt.activation_sha256()?)
    }
}
