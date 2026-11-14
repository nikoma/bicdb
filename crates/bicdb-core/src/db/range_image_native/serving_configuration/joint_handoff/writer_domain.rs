//! Actual local CLOSED joint facts for a necessary writer-policy transition.
//! No role, Ready, application or Strong permission is constructed here.
use super::*;
use crate::distribution_range_consensus::RangeWriteStore;
use crate::distribution_range_image_install::NativeJointImageMaterialization;

/// Persistable constraints ONLY. Reading/decoding these does not construct
/// the actual same-open fact below or replace independently fresh authority.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(in crate::db::range_image_native) struct NativeClosedWriterPoint {
    pub(in crate::db::range_image_native) decision_sha256: String,
    pub(in crate::db::range_image_native) local_decision_sha256: String,
    pub(in crate::db::range_image_native) local_receipt_sha256: String,
    pub(in crate::db::range_image_native) old_close_sha256: Option<String>,
    pub(in crate::db::range_image_native) local_promise:
        Option<crate::distribution_range_handoff::RangeHandoffPromise>,
    pub(in crate::db::range_image_native) birth_id: Uuid,
    pub(in crate::db::range_image_native) birth_sha256: String,
    pub(in crate::db::range_image_native) physical_base_sha256: String,
    pub(in crate::db::range_image_native) native_prefix_sha256: String,
    pub(in crate::db::range_image_native) history_generation: Uuid,
    pub(in crate::db::range_image_native) namespace_ids: Vec<(String, Uuid)>,
    pub(in crate::db::range_image_native) schema_sha256: String,
    pub(in crate::db::range_image_native) range_index: u64,
    pub(in crate::db::range_image_native) logical_genesis_sha256: String,
    pub(in crate::db::range_image_native) logical_history_sha256: String,
    pub(in crate::db::range_image_native) local_visibility_sequence: u64,
}
/// Actual native reads, never a deserialization/parts constructor. This fact
/// remains a CLOSED materialization constraint; current MQ is a separate input.
pub(in crate::db::range_image_native) struct NativeClosedWriterFact {
    point: NativeClosedWriterPoint,
    native: NativeRecoveryAdmission,
    decision: NativeJointDecisionReceipt,
}
impl NativeClosedWriterFact {
    pub(in crate::db::range_image_native) fn point(&self) -> &NativeClosedWriterPoint {
        &self.point
    }
    pub(in crate::db::range_image_native) fn decision(&self) -> &NativeJointDecisionReceipt {
        &self.decision
    }
    pub(in crate::db::range_image_native) fn require_current(
        &self,
        database: &BicDb,
        store: &RangeWriteStore,
        image: Option<&dyn NativeJointImageMaterialization>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        let current = database.observe_closed_writer_joint_fact(
            &self.native,
            self.decision.data(),
            store,
            image,
            frozen,
        )?;
        if current.point != self.point
            || current.decision.checksum_sha256() != self.decision.checksum_sha256()
        {
            return Err(invalid(
                "CLOSED writer transition changed its actual local joint facts",
            ));
        }
        Ok(())
    }
    #[cfg(feature = "tls-replication")]
    pub(in crate::db::range_image_native) fn require_current_for_transition(
        &self,
        database: &BicDb,
        store: &RangeWriteStore,
        image: Option<&dyn NativeJointImageMaterialization>,
        frozen: &FrozenRangeMutationPermit,
        scope: &crate::db::NativeWriterClosedHistoricalReadScope<'_>,
    ) -> Result<()> {
        let current = database.observe_closed_writer_joint_fact_for_transition(
            &self.native,
            self.decision.data(),
            store,
            image,
            frozen,
            scope,
        )?;
        if current.point != self.point
            || current.decision.checksum_sha256() != self.decision.checksum_sha256()
        {
            return Err(invalid(
                "CLOSED scoped historical fact changed exact native decision/point",
            ));
        }
        Ok(())
    }
}
impl BicDb {
    pub(in crate::db::range_image_native) fn observe_closed_writer_joint_fact(
        &self,
        native: &NativeRecoveryAdmission,
        data: &crate::distribution_range_handoff::NativeJointDecisionData,
        store: &RangeWriteStore,
        image: Option<&dyn NativeJointImageMaterialization>,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<NativeClosedWriterFact> {
        self.observe_closed_writer_joint_fact_with_catalogue(
            native,
            data,
            store,
            image,
            frozen,
            |decision| {
                Ok((
                    store.closed_writer_local_promise(data, image)?,
                    store.closed_writer_catalogue_decisions(self, decision, image, frozen)?,
                ))
            },
        )
    }
    #[cfg(feature = "tls-replication")]
    pub(in crate::db::range_image_native) fn observe_closed_writer_joint_fact_for_transition(
        &self,
        native: &NativeRecoveryAdmission,
        data: &crate::distribution_range_handoff::NativeJointDecisionData,
        store: &RangeWriteStore,
        image: Option<&dyn NativeJointImageMaterialization>,
        frozen: &FrozenRangeMutationPermit,
        scope: &crate::db::NativeWriterClosedHistoricalReadScope<'_>,
    ) -> Result<NativeClosedWriterFact> {
        // Images keep their existing full materialization checks. Only actual
        // survivor no-image historical reads use the retained transition scope.
        match image {
            Some(_) => self.observe_closed_writer_joint_fact(native, data, store, image, frozen),
            None => self.observe_closed_writer_joint_fact_with_catalogue(
                native,
                data,
                store,
                None,
                frozen,
                |decision| {
                    Ok((
                        store.closed_writer_local_promise_for_transition(self, data, scope)?,
                        store.closed_writer_catalogue_decisions_for_transition(
                            self, decision, frozen, scope,
                        )?,
                    ))
                },
            ),
        }
    }
    fn observe_closed_writer_joint_fact_with_catalogue(
        &self,
        native: &NativeRecoveryAdmission,
        data: &crate::distribution_range_handoff::NativeJointDecisionData,
        store: &RangeWriteStore,
        image: Option<&dyn NativeJointImageMaterialization>,
        frozen: &FrozenRangeMutationPermit,
        catalogue: impl FnOnce(
            &NativeJointDecisionReceipt,
        ) -> Result<(
            Option<crate::distribution_range_handoff::RangeHandoffPromise>,
            BTreeMap<u64, NativeReadCatalogueDecision>,
        )>,
    ) -> Result<NativeClosedWriterFact> {
        native.require_current(self)?;
        data.require_actual_native(native)?;
        if !data.new_members().contains_key(native.local_node_id()) {
            return Err(invalid(
                "CLOSED writer transition requires an actual decided NEW member",
            ));
        }
        let range = data.old_range().id;
        let gate = self.range_mutation_gate();
        gate.validate_quiescent_frozen(range, frozen)?;
        let pin = gate.try_source_capture_read_pin(range, frozen)?;
        let decision = self.read_native_joint_decision(native, range, data.checksum_sha256())?;
        if decision.data() != data {
            return Err(invalid(
                "CLOSED writer transition substituted protected joint decision",
            ));
        }
        let actual = self.current_native_range_source_lineage(range)?;
        let (local_promise, obligations) = catalogue(&decision)?;
        for (index, expected) in obligations {
            let actual_entry =
                self.read_native_history_entry(range, actual.history_generation(), index)?;
            let matches = match (expected, &actual_entry.outcome) {
                (
                    NativeReadCatalogueDecision::Applied(expected),
                    NativeLogicalHistoryOutcomeV1::Applied {
                        command_checksum_sha256,
                    },
                ) => expected == *command_checksum_sha256,
                (NativeReadCatalogueDecision::Aborted, NativeLogicalHistoryOutcomeV1::Aborted) => {
                    true
                }
                _ => false,
            };
            if actual_entry.index != index || !matches {
                return Err(invalid("CLOSED writer transition lost or contradicted actual retained catalogue/history obligation"));
            }
        }

        let (local_receipt, old_close) = match image {
            Some(image) => {
                if data.old_members().contains_key(native.local_node_id())
                    || local_promise.is_some()
                {
                    return Err(invalid(
                        "CLOSED image writer cannot manufacture an OLD closure",
                    ));
                }
                image.require_materialized_fact_current(self, store, frozen)?;
                let base = image.base();
                if base.session_id() != data.promise().session_id
                    || base.nonce() != data.promise().nonce
                    || base.intended_activation_range() != data.activation_range()
                    || base.intended_activation_members() != data.new_members()
                {
                    return Err(invalid(
                        "CLOSED writer image differs from protected NEW domain",
                    ));
                }
                (base.native_receipt_sha256().to_owned(), None)
            }
            None => {
                if !data.old_members().contains_key(native.local_node_id())
                    || frozen.owner() != data.promise().session_id
                {
                    return Err(invalid(
                        "CLOSED survivor writer requires actual OLD permanent owner",
                    ));
                }
                let sha = self
                    .require_current_joint_local_promise(
                        native,
                        data,
                        &actual,
                        local_promise.as_ref(),
                    )?
                    .ok_or_else(|| invalid("CLOSED survivor writer lost actual OLD closure"))?;
                store.require_current_joint_local_closure_pointer(data, Some(&sha))?;
                (sha.clone(), Some(sha))
            }
        };
        data.require_actual_local_point(
            native,
            &local_receipt,
            actual.range_index(),
            actual.logical_genesis_sha256(),
            actual.logical_history_sha256(),
            actual.actual_local_visibility_sequence(),
        )?;
        if actual.range_index() != data.range_index()
            || actual.logical_genesis_sha256() != data.logical_genesis_sha256()
            || actual.logical_history_sha256() != data.logical_history_sha256()
            || actual.schema_sha256() != data.schema_sha256()
        {
            return Err(invalid(
                "CLOSED writer local materialization differs from complete decided cut",
            ));
        }
        self.validate_source_range_image_lineage(&actual, range, actual.range_index())?;
        decision.require_current(self)?;
        gate.validate_source_capture_read_pin(range, &pin)?;
        let point = NativeClosedWriterPoint {
            decision_sha256: data.checksum_sha256().into(),
            local_decision_sha256: decision.checksum_sha256().into(),
            local_receipt_sha256: local_receipt,
            old_close_sha256: old_close,
            local_promise,
            birth_id: native.birth_id(),
            birth_sha256: native.birth_sha256().into(),
            physical_base_sha256: actual.base.checksum_sha256.clone(),
            native_prefix_sha256: actual.native_receipt_sha256().into(),
            history_generation: actual.history_generation(),
            namespace_ids: actual.base.local_namespace_ids.clone(),
            schema_sha256: data.schema_sha256().into(),
            range_index: actual.range_index(),
            logical_genesis_sha256: actual.logical_genesis_sha256().into(),
            logical_history_sha256: actual.logical_history_sha256().into(),
            local_visibility_sequence: actual.actual_local_visibility_sequence(),
        };
        drop(pin);
        Ok(NativeClosedWriterFact {
            point,
            native: self.native_recovery_admission(
                data.cluster_id(),
                data.bootstrap_id(),
                native.local_node_id(),
                native.local_member_incarnation(),
            )?,
            decision,
        })
    }
}

impl BicDb {
    pub(in crate::db::range_image_native) fn require_closed_writer_old_archive(
        &self,
        native: &NativeRecoveryAdmission,
        data: &crate::distribution_range_handoff::NativeJointDecisionData,
        actual: &SourceRangeImageLineage,
        promise: Option<&crate::distribution_range_handoff::RangeHandoffPromise>,
        expected: &str,
    ) -> Result<()> {
        if self
            .require_current_joint_local_promise(native, data, actual, promise)?
            .as_deref()
            != Some(expected)
        {
            return Err(invalid(
                "writer DATA original OLD closure CURRENT/archive changed",
            ));
        }
        Ok(())
    }
}
