//! Actual native materialization and protected membership-transition receipts.
//! This module never treats intended placement or a decoded quorum as a floor.
use super::*;
use crate::distribution_range_consensus::{
    NativeJointOldCatalogPreflight, NativeReadCatalogueDecision,
};
use crate::distribution_range_handoff::NativeJointHandoffPlan;
use crate::distribution_range_mutation::{
    ControlledFrozenMutationPermit, FrozenRangeMutationPermit, RangeMutationAuthority,
};

mod writer_domain;
pub(in crate::db::range_image_native) use writer_domain::{
    NativeClosedWriterFact, NativeClosedWriterPoint,
};
mod bridge;
pub(in crate::db::range_image_native) use bridge::NativeJointServingDomain;
pub(crate) use bridge::{
    NativeCurrentJointBinding, NativeCurrentJointRecoveryDomain, NativeJointBridgeReadScope,
    NativeJointHistoricalCutScope, NativeJointPreparedPublicationScope,
    NativeJointServingBridgeReceipt, NativeJointServingPreparation,
};

/// Actual physical preflight under an owned/drained handoff freeze. No serde,
/// Clone, raw-counter constructor, mutation permission or vote conversion.
#[derive(Debug)]
pub(crate) struct NativeJointOldPhysicalProof {
    native: NativeRecoveryAdmission,
    lineage: SourceRangeImageLineage,
    catalogue: NativeJointOldCatalogPreflight,
    plan_sha256: String,
    role_context: Uuid,
    freeze_owner: Uuid,
}
impl NativeJointOldPhysicalProof {
    pub(crate) fn range_index(&self) -> u64 {
        self.lineage.range_index()
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        self.lineage.logical_genesis_sha256()
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        self.lineage.logical_history_sha256()
    }
    pub(crate) fn native_receipt_sha256(&self) -> &str {
        self.lineage.native_receipt_sha256()
    }
    pub(crate) fn local_visibility_sequence(&self) -> u64 {
        self.lineage.actual_local_visibility_sequence()
    }
    pub(crate) fn native_admission(&self) -> &NativeRecoveryAdmission {
        &self.native
    }
    pub(crate) fn catalogue(&self) -> &NativeJointOldCatalogPreflight {
        &self.catalogue
    }
    pub(crate) fn require_before_promise(
        &self,
        database: &BicDb,
        store: &RangeWriteStore,
        plan: &NativeJointHandoffPlan,
        role: &RecoveryRoleAdmission,
        controlled: &ControlledFrozenMutationPermit,
    ) -> Result<()> {
        plan.require_role(role)?;
        plan.require_actual_native(&self.native)?;
        self.native.require_current(database)?;
        self.catalogue.require_current(store, plan, role)?;
        if self.plan_sha256 != plan.checksum_sha256()?
            || self.role_context != role.context_id()
            || self.freeze_owner != plan.promise().session_id
        {
            return Err(invalid(
                "joint physical preflight changed actual scope/context/freeze",
            ));
        }
        database.range_mutations.validate(
            plan.old_range().id,
            &RangeMutationAuthority::Controlled(controlled),
        )?;
        database.validate_source_range_image_lineage(
            &self.lineage,
            plan.old_range().id,
            self.range_index(),
        )
    }
}
impl BicDb {
    pub(crate) fn prepare_native_joint_old_physical(
        &self,
        plan: &NativeJointHandoffPlan,
        role: &RecoveryRoleAdmission,
        store: &RangeWriteStore,
        catalogue: NativeJointOldCatalogPreflight,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<NativeJointOldPhysicalProof> {
        parent.check()?;
        plan.require_role(role)?;
        catalogue.require_current(store, plan, role)?;
        if frozen.owner() != plan.promise().session_id {
            return Err(invalid(
                "joint physical freeze is not the committed permanent session",
            ));
        }
        self.range_mutations
            .validate_quiescent_frozen(plan.old_range().id, frozen)?;
        let pin = self
            .range_mutations
            .try_source_capture_read_pin(plan.old_range().id, frozen)?;
        let native = self.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        plan.require_actual_native(&native)?;
        let lineage = self.current_native_range_source_lineage(plan.old_range().id)?;
        plan.require_old_native_configuration(&lineage)?;
        if lineage.range_index() != catalogue.range_index() {
            return Err(invalid(
                "joint raw-resolved prefix is ahead/behind actual native materialization",
            ));
        }
        for (index, decision) in catalogue.decisions() {
            parent.check()?;
            role.require_live_request()?;
            let step = self.export_native_logical_history_step_with_source_pin(
                &lineage, frozen, &pin, *index, 1, parent,
            )?;
            let actual = step
                .entries
                .first()
                .ok_or_else(|| invalid("joint native history obligation missing"))?;
            let matches = match (decision, &actual.outcome) {
                (
                    NativeReadCatalogueDecision::Applied(expected),
                    NativeLogicalHistoryOutcomeV1::Applied {
                        command_checksum_sha256,
                    },
                ) => expected == command_checksum_sha256,
                (NativeReadCatalogueDecision::Aborted, NativeLogicalHistoryOutcomeV1::Aborted) => {
                    true
                }
                _ => false,
            };
            if actual.index != *index || !matches {
                return Err(invalid("joint native history contradicts an actual retained raw/sparse/certified obligation"));
            }
        }
        catalogue.require_current(store, plan, role)?;
        self.validate_source_range_image_lineage(
            &lineage,
            plan.old_range().id,
            lineage.range_index(),
        )?;
        let proof = NativeJointOldPhysicalProof {
            native,
            lineage,
            catalogue,
            plan_sha256: plan.checksum_sha256()?,
            role_context: role.context_id(),
            freeze_owner: frozen.owner(),
        };
        // A counted controlled metadata write follows. Only the actual frozen
        // owner remains; no uncounted Frozen write or pin exception is granted.
        drop(pin);
        Ok(proof)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct NativeJointOldCloseRecord {
    version: u32,
    scope_sha256: String,
    promise: crate::distribution_range_handoff::RangeHandoffPromise,
    birth_id: Uuid,
    birth_sha256: String,
    local_node_id: ClusterNodeId,
    local_incarnation: u64,
    physical_base_sha256: String,
    native_prefix_sha256: String,
    range_index: u64,
    logical_genesis_sha256: String,
    logical_history_sha256: String,
    actual_local_visibility_sequence: u64,
    schema_sha256: String,
    reader_features: Vec<String>,
    local_namespace_ids: Vec<(String, Uuid)>,
    history_generation: Uuid,
    catalogue_history_sha256: String,
    metadata_context_id: Uuid,
    metadata_term: u64,
    metadata_read_index: u64,
    native_page_xid: u64,
    checksum_sha256: String,
}
fn old_close_key(range: RangeId) -> String {
    format!("joint-handoff/old-close/current/{range}")
}
fn old_close_history_key(range: RangeId, checksum: &str) -> String {
    format!("joint-handoff/old-close/history/{range}/{checksum}")
}
impl NativeJointOldCloseRecord {
    fn checksum(&self) -> Result<String> {
        let mut copy = self.clone();
        copy.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &copy,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self) -> Result<()> {
        self.promise.validate()?;
        let digest = |value: &str| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if self.version != 1
            || self.birth_id.is_nil()
            || self.local_incarnation == 0
            || self.history_generation.is_nil()
            || self.metadata_context_id.is_nil()
            || self.metadata_term == 0
            || self.metadata_read_index == 0
            || self.native_page_xid == 0
            || self.promise.resolved_through != self.range_index
            || self.checksum()? != self.checksum_sha256
            || [
                &self.scope_sha256,
                &self.birth_sha256,
                &self.physical_base_sha256,
                &self.native_prefix_sha256,
                &self.logical_genesis_sha256,
                &self.logical_history_sha256,
                &self.schema_sha256,
                &self.catalogue_history_sha256,
            ]
            .into_iter()
            .any(|v| !digest(v))
        {
            return Err(invalid(
                "damaged protected native old-closed handoff record",
            ));
        }
        Ok(())
    }
    fn same_materialization(&self, other: &Self) -> bool {
        let mut left = self.clone();
        let mut right = other.clone();
        for item in [&mut left, &mut right] {
            item.metadata_context_id = Uuid::nil();
            item.metadata_term = 0;
            item.metadata_read_index = 0;
            item.native_page_xid = 0;
            item.checksum_sha256.clear();
        }
        left == right
    }
}
/// Historical PERMANENT closed-promise fact. It is not a current read, writer,
/// new-member readiness, data-quorum certificate, or metadata authority.
#[derive(Debug)]
pub(crate) struct NativeJointOldCloseReceipt {
    native: NativeRecoveryAdmission,
    record: NativeJointOldCloseRecord,
}
impl NativeJointOldCloseReceipt {
    pub(crate) fn scope_sha256(&self) -> &str {
        &self.record.scope_sha256
    }
    pub(crate) fn checksum_sha256(&self) -> &str {
        &self.record.checksum_sha256
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.record.range_index
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.record.logical_genesis_sha256
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.record.logical_history_sha256
    }
    pub(crate) fn native_admission(&self) -> &NativeRecoveryAdmission {
        &self.native
    }
    pub(crate) fn local_visibility_sequence(&self) -> u64 {
        self.record.actual_local_visibility_sequence
    }
    pub(crate) fn require_current(
        &self,
        database: &BicDb,
        plan: &NativeJointHandoffPlan,
        role: &RecoveryRoleAdmission,
        promise: &crate::distribution_range_consensus::NativeJointOldPromiseReceipt,
        store: &RangeWriteStore,
    ) -> Result<()> {
        plan.require_role(role)?;
        if role.metadata_term() < self.record.metadata_term
            || role.metadata_read_index() < self.record.metadata_read_index
        {
            return Err(invalid(
                "joint closure observer regressed its actual metadata anchor",
            ));
        }
        self.require_closed_fact_current(database, plan, promise, store)
    }
    /// A historical permanent fence fact has no TTL lease. Later ACTIONS
    /// still require independently fresh metadata and their own exact proofs.
    pub(crate) fn require_closed_fact_current(
        &self,
        database: &BicDb,
        plan: &NativeJointHandoffPlan,
        promise: &crate::distribution_range_consensus::NativeJointOldPromiseReceipt,
        store: &RangeWriteStore,
    ) -> Result<()> {
        plan.require_actual_native(&self.native)?;
        self.native.require_current(database)?;
        promise.require_current(store, plan)?;
        let actual: NativeJointOldCloseRecord = database
            .read_native_range_image_record(&old_close_key(plan.old_range().id))?
            .ok_or_else(|| invalid("native permanent old closure record missing"))?;
        actual.validate()?;
        if actual != self.record
            || actual.scope_sha256 != plan.checksum_sha256()?
            || &actual.promise != promise.promise()
            || actual.catalogue_history_sha256 != promise.history_sha256()
        {
            return Err(invalid(
                "native old closure record differs from actual current plan/store/history",
            ));
        }
        let lineage = database.current_native_range_source_lineage(plan.old_range().id)?;
        plan.require_old_native_configuration(&lineage)?;
        if lineage.range_index() != actual.range_index
            || lineage.native_receipt_sha256() != actual.native_prefix_sha256
            || lineage.logical_genesis_sha256() != actual.logical_genesis_sha256
            || lineage.logical_history_sha256() != actual.logical_history_sha256
            || lineage.base.checksum_sha256 != actual.physical_base_sha256
            || lineage.base.local_namespace_ids != actual.local_namespace_ids
        {
            return Err(invalid("native old closure physical prefix changed"));
        }
        Ok(())
    }
}
impl BicDb {
    pub(crate) fn persist_native_joint_old_close(
        &self,
        plan: &NativeJointHandoffPlan,
        role: &RecoveryRoleAdmission,
        store: &RangeWriteStore,
        physical: &NativeJointOldPhysicalProof,
        promise: &crate::distribution_range_consensus::NativeJointOldPromiseReceipt,
        controlled: &ControlledFrozenMutationPermit,
    ) -> Result<NativeJointOldCloseReceipt> {
        let receipt = self.persist_native_joint_old_close_before_final_check(
            plan, role, store, physical, promise, controlled,
        )?;
        receipt.require_current(self, plan, role, promise, store)?;
        Ok(receipt)
    }
    /// Retain the actual successful native-WAL observation before original
    /// role/current checks can fail. No receipt is fabricated for WAL errors.
    #[cfg(feature = "tls-replication")]
    pub(crate) fn persist_native_joint_old_close_retained(
        &self,
        plan: &NativeJointHandoffPlan,
        role: &RecoveryRoleAdmission,
        store: &RangeWriteStore,
        physical: &NativeJointOldPhysicalProof,
        promise: &crate::distribution_range_consensus::NativeJointOldPromiseReceipt,
        controlled: &ControlledFrozenMutationPermit,
        retained: &mut Option<Arc<NativeJointOldCloseReceipt>>,
    ) -> Result<Arc<NativeJointOldCloseReceipt>> {
        if retained.is_none() {
            let actual = self.persist_native_joint_old_close_before_final_check(
                plan, role, store, physical, promise, controlled,
            )?;
            *retained = Some(Arc::new(actual));
        }
        let receipt = retained.as_ref().unwrap();
        receipt.require_current(self, plan, role, promise, store)?;
        Ok(receipt.clone())
    }
    fn persist_native_joint_old_close_before_final_check(
        &self,
        plan: &NativeJointHandoffPlan,
        role: &RecoveryRoleAdmission,
        store: &RangeWriteStore,
        physical: &NativeJointOldPhysicalProof,
        promise: &crate::distribution_range_consensus::NativeJointOldPromiseReceipt,
        controlled: &ControlledFrozenMutationPermit,
    ) -> Result<NativeJointOldCloseReceipt> {
        plan.require_role(role)?;
        plan.require_actual_native(physical.native_admission())?;
        promise.require_current(store, plan)?;
        self.range_mutations.validate(
            plan.old_range().id,
            &RangeMutationAuthority::Controlled(controlled),
        )?;
        self.validate_source_range_image_lineage(
            &physical.lineage,
            plan.old_range().id,
            physical.range_index(),
        )?;
        if physical.role_context != role.context_id()
            || physical.plan_sha256 != plan.checksum_sha256()?
            || physical.freeze_owner != plan.promise().session_id
            || !format::native_joint_data_handoff_active(self.data_path())?
        {
            return Err(invalid(
                "native old close lost its actual plan/controlled scope",
            ));
        }
        let point = &physical.lineage;
        let native = self.native_admission_for_actual_birth(&point.birth)?;
        let base = &point.base;
        let mut record = NativeJointOldCloseRecord {
            version: 1,
            scope_sha256: plan.checksum_sha256()?,
            promise: promise.promise().clone(),
            birth_id: point.birth.birth_id,
            birth_sha256: point.birth.checksum_sha256.clone(),
            local_node_id: role.local_node_id().clone(),
            local_incarnation: role.local_member_incarnation(),
            physical_base_sha256: base.checksum_sha256.clone(),
            native_prefix_sha256: point.native_receipt_sha256().into(),
            range_index: point.range_index(),
            logical_genesis_sha256: point.logical_genesis_sha256().into(),
            logical_history_sha256: point.logical_history_sha256().into(),
            actual_local_visibility_sequence: point.actual_local_visibility_sequence(),
            schema_sha256: point.schema_sha256().into(),
            reader_features: base.reader_features.clone(),
            local_namespace_ids: base.local_namespace_ids.clone(),
            history_generation: point.history_generation(),
            catalogue_history_sha256: promise.history_sha256().into(),
            metadata_context_id: role.context_id(),
            metadata_term: role.metadata_term(),
            metadata_read_index: role.metadata_read_index(),
            native_page_xid: 0,
            checksum_sha256: String::new(),
        };
        let _journal = self.try_native_image_journal()?;
        if let Some(existing) = self.read_native_range_image_record::<NativeJointOldCloseRecord>(
            &old_close_key(plan.old_range().id),
        )? {
            existing.validate()?;
            if !existing.same_materialization(&record)
                || role.metadata_term() < existing.metadata_term
                || role.metadata_read_index() < existing.metadata_read_index
            {
                return Err(invalid(
                    "protected native closure retry changes its permanent point or metadata anchor",
                ));
            }
            record = existing;
        } else {
            self.in_paged_transaction(|paged, xid| {
                role.require_live_request()?;
                record.native_page_xid = xid;
                record.checksum_sha256 = record.checksum()?;
                record.validate()?;
                let encoded = bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?;
                for key in [
                    old_close_history_key(plan.old_range().id, &record.checksum_sha256),
                    old_close_key(plan.old_range().id),
                ] {
                    paged
                        .store()
                        .put(xid, &native_key(&key), &encoded)
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                }
                Ok(())
            })?
            .ok_or_else(|| invalid("native old closure has no actual page WAL"))?;
        }
        drop(_journal);
        Ok(NativeJointOldCloseReceipt { native, record })
    }
}

mod old_record_data;
pub(crate) use old_record_data::NativeJointOldCloseRecordData;

mod decision;
#[cfg(all(test, feature = "tls-replication"))]
pub(crate) use decision::test_remote_joint_decision_cut;
pub(crate) use decision::NativeJointDecisionReceipt;
#[cfg(feature = "tls-replication")]
pub(crate) use decision::NativeRegisteredJointDecisionAttempt;
#[cfg(feature = "tls-replication")]
pub(crate) use decision::NativeRemoteJointDecisionAttempt;
