//! Actual complete local catalogue at the decided cut. This is not a native
//! materialization, new-data vote, membership bridge or serving permission.
use super::*;
use crate::distribution_range_handoff::CommittedNativeJointDecisionObservation;
use crate::distribution_range_image_install::NativeImageCatalogueFinalization;
use crate::distribution_range_mutation::FrozenRangeMutationPermit;

pub(crate) struct NativeJointBridgeCatalogue {
    obligations: TargetImageCatalogHistoryObligations,
    decisions: BTreeMap<u64, NativeReadCatalogueDecision>,
    decision_sha256: String,
    local_decision_sha256: String,
    image_sha256: Option<String>,
    store_open_id: Uuid,
}
impl NativeJointBridgeCatalogue {
    pub(crate) fn decisions(&self) -> &BTreeMap<u64, NativeReadCatalogueDecision> {
        &self.decisions
    }
    pub(crate) fn checksum_sha256(&self) -> &str {
        self.obligations.range_log_sha256()
    }
    pub(crate) fn database_root(&self) -> &std::path::Path {
        self.obligations.database_root()
    }
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.store_open_id
    }
    pub(crate) fn require_after_exact_activation(
        &self,
        store: &RangeWriteStore,
        scope: &crate::db::NativeJointBridgeReadScope<'_>,
        committed: &CommittedNativeJointDecisionObservation,
        fact: &NativeJointCatalogueActivationFact,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        committed.require_current_with_native_read_scope(scope)?;
        fact.require_current_with_scope(scope, store)?;
        store.require_joint_open_phase_not_released(fact)?;
        store.require_actual_abort_controls_covered(
            scope.database(),
            committed.data().old_range().id,
            committed.data().range_index(),
        )?;
        if self.store_open_id != store.open_id
            || self.decision_sha256 != committed.data().checksum_sha256()
            || self.local_decision_sha256 != committed.native_receipt().checksum_sha256()
            || fact.native().data() != committed.data()
        {
            return Err(range_write_error(
                "joint open phase changed exact catalogue/decision/open",
            ));
        }
        scope
            .database()
            .range_mutation_gate()
            .validate_quiescent_frozen(committed.data().old_range().id, frozen)?;
        self.obligations
            .require_after_native_joint_activation(store, scope, fact)
    }
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        database: &crate::BicDb,
        committed: &CommittedNativeJointDecisionObservation,
        image: Option<&NativeImageCatalogueFinalization>,
        frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit,
    ) -> Result<()> {
        store.require_native_joint_bridge_base(database, committed, image, frozen)?;
        self.obligations.require_current(store)?;
        if self.store_open_id != store.open_id
            || self.decision_sha256 != committed.data().checksum_sha256()
            || self.local_decision_sha256 != committed.native_receipt().checksum_sha256()
            || self.image_sha256.as_deref()
                != image.map(|value| value.base().native_receipt_sha256())
        {
            return Err(range_write_error(
                "joint bridge actual catalogue/open/image changed",
            ));
        }
        Ok(())
    }
}
impl RangeWriteStore {
    /// Actual refusal DATA only. A finalized image keeps its real image intent;
    /// a survivor must have no image install. Native producer validates the
    /// exact LOCAL promise against CURRENT+archive and global OLD facts.
    pub(crate) fn closed_writer_local_promise(
        &self,
        data: &crate::distribution_range_handoff::NativeJointDecisionData,
        image: Option<&NativeImageCatalogueFinalization>,
    ) -> Result<Option<crate::distribution_range_handoff::RangeHandoffPromise>> {
        self.require_native_writer_host_base()?;
        let range = data.old_range().id;
        if image.is_none() {
            self.require_no_image_install(range)?;
        }
        if self.catalog.backup_fences.contains_key(&range) {
            return Err(range_write_error(
                "CLOSED writer transition cannot bypass backup refusal",
            ));
        }
        let local = self.catalog.handoff_promises.get(&range);
        if local.is_some_and(|p| {
            !p.same_request(data.promise()) || p.resolved_through > data.range_index()
        }) {
            return Err(range_write_error(
                "CLOSED writer transition has foreign/uncovered permanent promise",
            ));
        }
        Ok(local.cloned())
    }

    pub(crate) fn require_closed_writer_promise(
        &self,
        data: &crate::distribution_range_handoff::NativeJointDecisionData,
        expected: &Option<crate::distribution_range_handoff::RangeHandoffPromise>,
        old_close: Option<&str>,
    ) -> Result<()> {
        self.require_native_writer_host_base()?;
        if self
            .catalog
            .backup_fences
            .contains_key(&data.old_range().id)
            || self.catalog.handoff_promises.get(&data.old_range().id) != expected.as_ref()
        {
            return Err(range_write_error(
                "writer DATA domain changed exact permanent LOCAL/backup refusal",
            ));
        }
        self.require_current_joint_local_closure_pointer(data, old_close)
    }
    fn require_native_joint_bridge_base(
        &self,
        database: &crate::BicDb,
        committed: &CommittedNativeJointDecisionObservation,
        image: Option<&NativeImageCatalogueFinalization>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        committed.require_current(database)?;
        self.require_decided_joint_catalogue_base(
            database,
            committed.native_receipt(),
            image,
            frozen,
        )
    }
    fn require_decided_joint_catalogue_base(
        &self,
        database: &crate::BicDb,
        native: &crate::db::NativeJointDecisionReceipt,
        image: Option<&NativeImageCatalogueFinalization>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        native.require_current(database)?;
        let data = native.data();
        let range = data.old_range().id;
        self.require_native_catalog_durability(database.data_path())?;
        self.require_native_writer_host_base()?;
        let local = data
            .new_members()
            .get(&self.catalog.node_id)
            .ok_or_else(|| {
                range_write_error("joint bridge catalogue is not a decided NEW data voter")
            })?;
        let local_native = database.native_recovery_admission(
            data.cluster_id(),
            data.bootstrap_id(),
            &self.catalog.node_id,
            local.incarnation,
        )?;
        data.require_actual_native(&local_native)?;
        if native.local_node_id() != &self.catalog.node_id {
            return Err(range_write_error(
                "joint bridge uses another service's native decision",
            ));
        }
        let state = self
            .catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|states| states.get(&range))
            .ok_or_else(|| range_write_error("joint bridge has no durable catalogue decision"))?;
        if state.session_id != data.promise().session_id
            || state.nonce != data.promise().nonce
            || state.global_decision_sha256.as_deref() != Some(data.checksum_sha256())
            || state.global_decision_native_sha256.as_deref() != Some(native.checksum_sha256())
        {
            return Err(range_write_error(
                "joint bridge catalogue/native decisions disagree",
            ));
        }
        if self.catalog.backup_fences.contains_key(&range) {
            return Err(range_write_error(
                "joint bridge cannot bypass a backup fence",
            ));
        }
        match image {
            Some(image) => {
                image.require_materialized_fact_current(database, self, frozen)?;
                let base = image.base();
                if base.capture_range().id != range
                    || base.session_id() != data.promise().session_id
                    || base.nonce() != data.promise().nonce
                    || base.intended_activation_range() != data.activation_range()
                    || base.intended_activation_members() != data.new_members()
                    || base.range_index() != data.range_index()
                    || base.logical_genesis_sha256() != data.logical_genesis_sha256()
                    || base.logical_history_sha256() != data.logical_history_sha256()
                {
                    return Err(range_write_error(
                        "joint bridge image is another exact decision/point",
                    ));
                }
            }
            None => self.require_no_image_install(range)?,
        }
        self.require_actual_abort_controls_covered(database, range, data.range_index())?;
        let progress = self.progress(range);
        if progress.last_index != data.range_index()
            || progress.resolved_through != data.range_index()
            || progress.compacted_through > data.range_index()
        {
            return Err(range_write_error(
                "joint bridge cannot omit a local unresolved/higher obligation",
            ));
        }
        Ok(())
    }
    pub(crate) fn closed_writer_catalogue_decisions(
        &self,
        database: &crate::BicDb,
        native: &crate::db::NativeJointDecisionReceipt,
        image: Option<&NativeImageCatalogueFinalization>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<BTreeMap<u64, NativeReadCatalogueDecision>> {
        self.require_decided_joint_catalogue_base(database, native, image, frozen)?;
        self.collect_native_resolved_decisions(
            native.data().old_range().id,
            native.data().range_index(),
        )
    }
    pub(crate) fn observe_native_joint_bridge_catalogue(
        &self,
        database: &crate::BicDb,
        committed: &CommittedNativeJointDecisionObservation,
        image: Option<&NativeImageCatalogueFinalization>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<NativeJointBridgeCatalogue> {
        self.require_native_joint_bridge_base(database, committed, image, frozen)?;
        let range = committed.data().old_range().id;
        let decisions =
            self.collect_native_resolved_decisions(range, committed.data().range_index())?;
        Ok(NativeJointBridgeCatalogue {
            obligations: self.image_install_catalog_history_obligations(range)?,
            decisions,
            decision_sha256: committed.data().checksum_sha256().into(),
            local_decision_sha256: committed.native_receipt().checksum_sha256().into(),
            image_sha256: image.map(|value| value.base().native_receipt_sha256().into()),
            store_open_id: self.open_id,
        })
    }
}
