//! One complete fresh open execution boundary. Every external check precedes
//! the local native journal scope; all local checks then share that REAL lock.
use super::*;

#[cfg(test)]
mod test_cuts;

impl NativeJointCatalogueActivationReceipt {
    pub(crate) fn require_current_with_scope(
        &self,
        scope: &crate::db::NativeJointBridgeReadScope<'_>,
        store: &RangeWriteStore,
    ) -> Result<()> {
        self.require_original_writer_current(scope.database(), store)?;
        let fact = NativeJointCatalogueActivationFact {
            store_open_id: self.store_open_id,
            activated: self.activated.clone(),
            native: self.native.clone(),
        };
        fact.require_current_with_scope(scope, store)?;
        self.require_original_writer_current(scope.database(), store)
    }
}
impl RangeWriteStore {
    pub(super) fn recover_fresh_native_joint_activation(
        &self,
        database: &crate::BicDb,
        admission: &NativeJointOpenAdmission,
    ) -> Result<Option<NativeJointCatalogueActivationReceipt>> {
        admission.require_live()?;
        let (writer, writer_root) =
            admission.original_phase_owner_for_activation(database, self)?;
        let writer_domain = admission.original_writer_domain_for_activation(database, self)?;

        let native = admission.native_receipt();
        let range = native.data().old_range().id;
        // An absence result grants nothing and performs no write. The complete
        // new-publication path below will still perform ALL mandatory checks.
        if self
            .catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|m| m.get(&range))
            .and_then(|s| s.serving_activation.as_ref())
            .is_none()
        {
            return Ok(None);
        }
        admission.require_external_publications_current()?;
        #[cfg(test)]
        let mut scope_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "open-final-native-scope-and-local-proof",
            &self.catalog.node_id,
            admission.cancellation(),
        );
        let scope = native.current_read_scope(database)?;
        admission.require_local_native_scope(&scope, self)?;
        #[cfg(test)]
        {
            scope_trace.complete();
            drop(scope_trace);
        }
        admission
            .committed()
            .require_current_with_native_read_scope(&scope)?;
        let fact = self
            .observe_native_joint_activation_fact_with_scope(&scope, native.clone())?
            .ok_or_else(|| {
                range_write_error("exact native activation disappeared during local recovery")
            })?;
        self.require_joint_open_phase_not_released(&fact)?;
        writer.require_store(&writer_root, self.open_id)?;
        let receipt = NativeJointCatalogueActivationReceipt {
            store_open_id: fact.store_open_id,
            activated: fact.activated,
            native: fact.native,
            writer,
            writer_domain,
            writer_root,
            cancellation: admission.cancellation().clone(),
        };
        receipt.require_current_with_scope(&scope, self)?;
        admission.require_live()?;
        Ok(Some(receipt))
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) fn persist_fresh_native_joint_activation(
        &mut self,
        database: &crate::BicDb,
        admission: &NativeJointOpenAdmission,
        original: &crate::distribution_range_handoff::CommittedNativeJointDecisionObservation,
        catalogue: &NativeJointBridgeCatalogue,
        image: Option<&dyn NativeJointImageMaterialization>,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<NativeJointCatalogueActivationReceipt> {
        parent.check()?;
        let (writer, writer_root) =
            admission.original_phase_owner_for_activation(database, self)?;
        let writer_domain = admission.original_writer_domain_for_activation(database, self)?;

        // Nonwaiting cross-service checks happen BEFORE this local journal.
        #[cfg(test)]
        let mut external_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "open-final-external-before-journal",
            &self.catalog.node_id,
            admission.cancellation(),
        );
        admission.require_external_publications_current()?;
        #[cfg(test)]
        {
            external_trace.complete();
            drop(external_trace);
        }
        #[cfg(test)]
        test_cuts::after_external()?;
        let native = admission.native_receipt();
        let data = admission.data();
        let range = data.old_range().id;
        #[cfg(test)]
        let mut scope_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "open-final-native-scope-and-local-proof",
            &self.catalog.node_id,
            admission.cancellation(),
        );
        let scope = native.current_read_scope(database)?;
        admission.require_local_native_scope(&scope, self)?;
        #[cfg(test)]
        {
            scope_trace.complete();
            drop(scope_trace);
        }
        admission
            .committed()
            .require_current_with_native_read_scope(&scope)?;
        scope.require_prepared_frozen(admission.local_preparation(), frozen)?;
        self.require_native_catalog_durability(database.data_path())?;
        self.require_native_joint_host_base(database, data.old_range().id)?;
        if native.local_node_id() != &self.catalog.node_id || data != original.data() {
            return Err(range_write_error(
                "fresh native activation changed exact local member/decision",
            ));
        }
        // Full opaque catalogue digest was just revalidated by the local
        // preparation check. Verify EVERY captured resolved decision directly
        // against the same locked immutable native spine; never export/rescan
        // the entire source lineage once per retained command.
        scope.require_prepared_catalogue_history(catalogue, admission, parent)?;
        let candidate = NativeJointCatalogueActivation::from_actual(&native)?;
        let existing = self
            .catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|m| m.get(&range))
            .and_then(|s| s.serving_activation.as_ref())
            .cloned();
        if let Some(existing) = &existing {
            if existing != &candidate || self.catalog.image_install_intents.contains_key(&range) {
                return Err(range_write_error(
                    "fresh activation changed existing protected point/intent",
                ));
            }
        }
        let gate = database.range_mutation_gate();
        gate.validate_quiescent_frozen(range, frozen)?;
        let controlled = gate.try_controlled_mutation(frozen)?;
        gate.validate(range, &RangeMutationAuthority::Controlled(&controlled))?;
        writer_domain.require_current(database, self, data.activation_range())?;
        let state = self
            .catalog
            .native_joint_handoffs
            .as_mut()
            .and_then(|m| m.get_mut(&range))
            .ok_or_else(|| range_write_error("fresh activation lost durable joint decision"))?;
        if state.global_decision_sha256.as_deref() != Some(data.checksum_sha256())
            || state.session_id != data.promise().session_id
            || state.nonce != data.promise().nonce
        {
            return Err(range_write_error(
                "fresh activation changed permanent plan/session/nonce",
            ));
        }
        self.recovery_persistence_uncertain = true;
        state.serving_activation = Some(candidate.clone());
        // Missing raw log is possible only at an actually verified genesis
        // cut; the local scope/opaque catalogue checks above reject a nonzero
        // native prefix with such missing raw history.
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
        eprintln!("ACTUAL_JOINT_OPEN_CATALOGUE_PERSIST node={} success={} uncertain={} range={} scoped=true",self.catalog.node_id,persisted.is_ok(),self.recovery_persistence_uncertain,range);
        persisted?;
        self.recovery_persistence_uncertain = false;
        drop(controlled);
        #[cfg(test)]
        test_cuts::after_persist()?;
        writer.require_store(&writer_root, self.open_id)?;
        let receipt = NativeJointCatalogueActivationReceipt {
            store_open_id: self.open_id,
            activated: candidate,
            native: native.clone(),
            writer,
            writer_domain,
            writer_root,
            cancellation: admission.cancellation().clone(),
        };
        receipt.require_current_with_scope(&scope, self)?;
        admission.require_live()?;
        parent.check()?;
        Ok(receipt)
    }
}
