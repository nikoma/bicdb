//! Historical activated-joint facts for a DISTINCT current recovery operation.
//! No old request/Prepared lifetime or decoded audit becomes fresh authority.
use super::super::super::{old_close_history_key, old_close_key, NativeJointOldCloseRecord};
use super::*;
use crate::distribution_range_recovery::RecoveryRoleAdmission;

/// Persistable constraints only. Deserializing this DATA cannot construct the
/// actual domain fact or authorize any fence, NoData, Ready or serving action.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeCurrentJointBinding {
    pub(crate) joint_decision_sha256: String,
    pub(crate) native_bridge_sha256: String,
    pub(crate) activation_sha256: String,
    pub(crate) local_handoff_promise:
        Option<crate::distribution_range_handoff::RangeHandoffPromise>,
    pub(crate) local_old_close_sha256: Option<String>,
    pub(crate) birth_id: Uuid,
    pub(crate) birth_sha256: String,
    pub(crate) store_id: Uuid,
    pub(crate) physical_base_sha256: String,
    pub(crate) namespace_ids: Vec<(String, Uuid)>,
    pub(crate) history_generation: Uuid,
    pub(crate) logical_genesis_sha256: String,
    pub(crate) schema_sha256: String,
    pub(crate) range: RangeDescriptor,
    pub(crate) members: BTreeMap<ClusterNodeId, RangeImageBirthMember>,
    pub(crate) restore: Option<crate::distribution_consensus::MetadataRestoreActivation>,
    pub(crate) point_index: u64,
    pub(crate) point_history_sha256: String,
    pub(crate) point_native_receipt_sha256: String,
    pub(crate) local_visibility: u64,
    pub(crate) writer_registration: Uuid,
}
/// Minted only by reading the actual protected native current/archive AND
/// matching actual catalogue activation. It supplies historical facts, not MQ.
pub(crate) struct NativeCurrentJointRecoveryDomain {
    native: Arc<NativeJointServingBridgeReceipt>,
    binding: NativeCurrentJointBinding,
    database_incarnation: String,
    store_open_id: Uuid,
    writer: Arc<crate::db::NativeWriterHostState>,
}
fn canonical(range: &RangeDescriptor) -> RangeDescriptor {
    let mut range = range.clone();
    range.approximate_bytes = 0;
    range.approximate_qps = 0;
    range.replicas.sort_by_key(|r| r.id);
    range
}
impl NativeCurrentJointRecoveryDomain {
    /// Necessary same-operation lifetime identity; never a serving grant.
    pub(crate) fn original_writer(&self) -> std::sync::Weak<crate::db::NativeWriterHostState> {
        Arc::downgrade(&self.writer)
    }
    pub(crate) fn require_original_writer(
        &self,
        original: &std::sync::Weak<crate::db::NativeWriterHostState>,
    ) -> Result<()> {
        if original
            .upgrade()
            .is_none_or(|writer| !Arc::ptr_eq(&writer, &self.writer))
        {
            return Err(invalid(
                "current joint recovery original writer object ended or was replaced",
            ));
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn test_local_promise_claim(
        &self,
        db: &BicDb,
        claim: Option<&crate::distribution_range_handoff::RangeHandoffPromise>,
    ) -> Result<()> {
        let actual = self.native.require_current_lineage(db)?;
        db.require_current_joint_local_promise(&self.native.native, self.data(), &actual, claim)?;
        Ok(())
    }
    pub(crate) fn binding(&self) -> &NativeCurrentJointBinding {
        &self.binding
    }
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        self.native.data()
    }
    pub(crate) fn purpose(&self) -> crate::distribution_consensus::MetadataRecoveryRolePurpose {
        crate::distribution_consensus::MetadataRecoveryRolePurpose::CurrentJointRecovery {
            joint_decision_sha256: self.binding.joint_decision_sha256.clone(),
        }
    }
    pub(crate) fn require_role(&self, role: &RecoveryRoleAdmission) -> Result<()> {
        role.require_live_request()?;
        let members = role
            .member_credentials()
            .iter()
            .map(|(id, m)| {
                (
                    id.clone(),
                    RangeImageBirthMember {
                        incarnation: m.incarnation,
                        tls_certificate_sha256: m.active_certificate_sha256.clone(),
                        pending_tls_certificate_sha256: m.pending_certificate_sha256.clone(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        if role.purpose() != &self.purpose()
            || canonical(role.current_range()) != canonical(&self.binding.range)
            || role.initiator_node_id() != &self.binding.range.leader
            || role.cluster_id() != self.native.data().cluster_id()
            || role.bootstrap_id() != self.native.data().bootstrap_id()
            || role.local_node_id() != self.native.local_node_id()
            || role.local_member_incarnation() != self.native.native.local_member_incarnation()
            || members != self.binding.members
            || role.expected_schema_sha256() != Some(self.binding.schema_sha256.as_str())
            || role.restore_activation() != self.binding.restore.as_ref()
        {
            return Err(invalid("current joint recovery needs exact activated epoch/cohort/leader/birth/schema/restore and fresh specific purpose"));
        }
        for node in self.binding.members.keys() {
            if self.data().new_member_birth(node) != Some(role.native_store_birth(node)?) {
                return Err(invalid(
                    "current joint recovery changed an actual enrolled NEW member birth",
                ));
            }
        }
        role.require_live_request()
    }
    pub(crate) fn require_saved_binding(
        &self,
        db: &BicDb,
        saved: &NativeCurrentJointBinding,
    ) -> Result<()> {
        let mut before = saved.clone();
        let mut now = self.binding.clone();
        // The fence captures a floor; subsequent authorized NoData/Applied
        // extends that history without relabeling the saved original point.
        for point in [&mut before, &mut now] {
            point.point_index = 0;
            point.point_history_sha256.clear();
            point.point_native_receipt_sha256.clear();
            point.local_visibility = 0;
        }
        if before != now
            || self.binding.point_index < saved.point_index
            || self.binding.local_visibility < saved.local_visibility
        {
            return Err(invalid("current joint recovery changed saved physical/domain/writer binding or regressed actual point"));
        }
        let entry = db.read_native_history_entry(
            saved.range.id,
            saved.history_generation,
            saved.point_index,
        )?;
        if entry.commitment_sha256 != saved.point_history_sha256
            || (self.binding.point_index == saved.point_index
                && self.binding.point_native_receipt_sha256 != saved.point_native_receipt_sha256)
        {
            return Err(invalid(
                "current joint recovery floor is absent/conflicting in actual immutable history",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_current(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        if self.database_incarnation != db.incarnation_id()
            || self.store_open_id != store.recovery_store_open_id()
        {
            return Err(invalid(
                "current joint recovery belongs to another actual DB/Store open",
            ));
        }
        #[cfg(test)]
        let lineage_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-joint-fact-native-bridge-point-and-archive",
            );
        let actual = self.native.require_current_lineage(db)?;
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(lineage_trace);
        self.require_current_from_actual_lineage(db, store, &actual)
    }
    fn require_current_from_actual_lineage(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        actual: &SourceRangeImageLineage,
    ) -> Result<()> {
        if self.database_incarnation != db.incarnation_id()
            || self.store_open_id != store.recovery_store_open_id()
        {
            return Err(invalid(
                "current joint completion scope changed actual DB/Store open",
            ));
        }
        #[cfg(test)]
        let closure_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-joint-fact-local-original-closure",
            );

        let local_promise = store.current_joint_local_handoff_promise(self.data())?;
        let local_close = db.require_current_joint_local_promise(
            &self.native.native,
            self.data(),
            &actual,
            local_promise.as_ref(),
        )?;
        store.require_current_joint_local_closure_pointer(self.data(), local_close.as_deref())?;
        if local_promise != self.binding.local_handoff_promise
            || local_close != self.binding.local_old_close_sha256
        {
            return Err(invalid(
                "current joint recovery changed exact original LOCAL promise/closure evidence",
            ));
        }
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(closure_trace);
        #[cfg(test)]
        let writer_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
            "refresh-joint-fact-original-writer-database-binding",
        );
        let actual = store.native_writer_host_state()?;
        if !Arc::ptr_eq(&actual, &self.writer) {
            return Err(invalid(
                "current joint recovery writer registration changed",
            ));
        }
        self.writer.require_database(db, &self.binding.range)?;
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(writer_trace);
        Ok(())
    }
}
impl BicDb {
    /// Actual protected LOCAL original-promise proof, independent of a plan's
    /// request cut. The old TTL is historical; no fresh authority comes from it.
    fn require_current_joint_local_promise(
        &self,
        native: &NativeRecoveryAdmission,
        data: &NativeJointDecisionData,
        actual: &SourceRangeImageLineage,
        local: Option<&crate::distribution_range_handoff::RangeHandoffPromise>,
    ) -> Result<Option<String>> {
        if !data.old_members().contains_key(native.local_node_id()) {
            if local.is_some() {
                return Err(invalid(
                    "image target cannot synthesize an original OLD-member promise",
                ));
            }
            return Ok(None);
        }
        let promise = local.ok_or_else(|| {
            invalid("surviving OLD member lost its exact permanent local promise")
        })?;
        let facts = data
            .old_materialized_fact(native.local_node_id())
            .ok_or_else(|| {
                invalid("local original promise has no protected global OLD materialization fact")
            })?;
        let record: NativeJointOldCloseRecord = self
            .read_native_range_image_record(&old_close_key(data.old_range().id))?
            .ok_or_else(|| {
                invalid("local original promise lacks actual protected old-close record")
            })?;
        record.validate()?;
        let archive: NativeJointOldCloseRecord = self
            .read_native_range_image_record(&old_close_history_key(
                data.old_range().id,
                &record.checksum_sha256,
            ))?
            .ok_or_else(|| {
                invalid("local original promise immutable protected old-close archive missing")
            })?;
        archive.validate()?;
        let history = self.read_native_history_entry(
            data.old_range().id,
            record.history_generation,
            record.range_index,
        )?;
        if record != archive
            || &record.promise != promise
            || !promise.same_request(data.promise())
            || promise.resolved_through != record.range_index
            || record.range_index > data.range_index()
            || record.scope_sha256 != data.scope_sha256()?
            || record.birth_id != native.birth_id()
            || record.birth_sha256 != native.birth_sha256()
            || &record.local_node_id != native.local_node_id()
            || record.local_incarnation != native.local_member_incarnation()
            || record.physical_base_sha256 != actual.base.checksum_sha256
            || record.history_generation != actual.history_generation()
            || record.local_namespace_ids != actual.base.local_namespace_ids
            || record.schema_sha256 != data.schema_sha256()
            || record.logical_genesis_sha256 != actual.logical_genesis_sha256()
            || record.logical_history_sha256 != history.commitment_sha256
            || record.actual_local_visibility_sequence > actual.actual_local_visibility_sequence()
            || facts.scope_sha256 != record.scope_sha256
            || facts.node_id != record.local_node_id
            || facts.birth_id != record.birth_id
            || facts.birth_sha256 != record.birth_sha256
            || facts.range_index != record.range_index
            || facts.logical_genesis_sha256 != record.logical_genesis_sha256
            || facts.logical_history_sha256 != record.logical_history_sha256
            || facts.native_receipt_sha256 != record.checksum_sha256
            || facts.actual_local_visibility_sequence != record.actual_local_visibility_sequence
        {
            return Err(invalid("local permanent promise is not the exact protected OLD closure covered by actual joint decision/history"));
        }
        Ok(Some(record.checksum_sha256))
    }
    /// Reader validation for a protected NoData record. No SourceLineage call:
    /// this is used while the prefix reader itself is constructing lineage.
    pub(in crate::db::range_image_native) fn require_actual_current_joint_abort_domain(
        &self,
        birth: &NativeDatabaseBirth,
        base: &NativeRangeBaseBinding,
        scope: &crate::distribution_range_consensus::RecoveryFenceScope,
        domain: &crate::distribution_range_consensus::native_abort::NativeAbortDomain,
        index: u64,
    ) -> Result<()> {
        match &scope.purpose {
            crate::distribution_consensus::MetadataRecoveryRolePurpose::CurrentJointRecovery {
                ..
            } => {
                if !format::native_current_joint_recovery_active(&self.path)? {
                    return Err(invalid("joint NoData lacks reader fence"));
                }
                let joint = self
                    .read_native_joint_serving_domain(birth, base)?
                    .ok_or_else(|| invalid("joint NoData lost actual protected native domain"))?;
                domain.require_protected_joint_data(&joint.record.decision, scope, index)
            }
            crate::distribution_consensus::MetadataRecoveryRolePurpose::NormalRecovery => {
                if self
                    .read_native_joint_serving_domain(birth, base)?
                    .is_some_and(|joint| index > joint.range_index())
                {
                    return Err(invalid(
                        "ordinary NoData cannot reinterpret an activated joint domain",
                    ));
                }
                Ok(())
            }
            _ => Err(invalid(
                "NoData requires a separately qualified recovery purpose",
            )),
        }
    }
    pub(crate) fn observe_current_joint_recovery_domain(
        &self,
        store: &RangeWriteStore,
        range: RangeId,
        parent: &CancellationToken,
    ) -> Result<NativeCurrentJointRecoveryDomain> {
        self.observe_current_joint_recovery_domain_inner(store, range, parent, None)
    }
    pub(in crate::db::range_image_native) fn observe_current_joint_domain_in_completion_scope(
        &self,
        completion: &crate::db::NativeCompletionReadScope<'_>,
    ) -> Result<NativeCurrentJointRecoveryDomain> {
        completion.require_identity(
            completion.prepared(),
            completion.store(),
            completion.role(),
        )?;
        if !std::ptr::eq(self, completion.database()) {
            return Err(invalid(
                "current joint completion scope changed actual database",
            ));
        }
        let parent =
            CancellationToken::uncancelable().child_with_deadline(completion.role().deadline());
        self.observe_current_joint_recovery_domain_inner(
            completion.store(),
            completion.prepared().scope().range_id,
            &parent,
            Some(completion),
        )
    }
    fn observe_current_joint_recovery_domain_inner(
        &self,
        store: &RangeWriteStore,
        range: RangeId,
        parent: &CancellationToken,
        completion: Option<&crate::db::NativeCompletionReadScope<'_>>,
    ) -> Result<NativeCurrentJointRecoveryDomain> {
        parent.check()?;
        store.require_native_catalog_durability(self.data_path())?;
        store.require_no_image_install(range)?;
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("current joint recovery has no actual birth"))?;
        let native = self.native_recovery_admission(
            &birth.spec.cluster_id,
            birth.spec.bootstrap_id,
            &birth.spec.node_id,
            birth.spec.member_incarnation,
        )?;
        if native.writer_enforcement_version() != 1 || native.abort_protocol_version() != 1 {
            return Err(invalid(
                "current joint recovery needs actual from-birth writer/abort capabilities",
            ));
        }
        let record: NativeJointServingBridgeRecord = self
            .read_native_range_image_record(&key(range))?
            .ok_or_else(|| invalid("current joint recovery has no actual protected NEW bridge"))?;
        record.validate()?;
        if !record
            .decision
            .new_members()
            .contains_key(native.local_node_id())
        {
            return Err(invalid(
                "current joint recovery local member is outside protected NEW cohort",
            ));
        }
        let writer = store.native_writer_host_state()?;
        writer.require_database(self, record.decision.activation_range())?;
        let receipt = Arc::new(NativeJointServingBridgeReceipt { native, record });
        #[cfg(test)]
        let scope_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
            "refresh-joint-factory-native-journal-scope-and-catalogue",
        );
        let scope = match completion {
            Some(completion) => receipt.read_scope_for_completion(completion)?,
            None => receipt.current_read_scope(self)?,
        };
        let activated = store
            .observe_native_joint_activation_fact_with_scope(&scope, receipt.clone())?
            .ok_or_else(|| invalid("current joint recovery lacks actual catalogue activation"))?;
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(scope_trace);
        #[cfg(test)]
        let binding_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-joint-factory-local-closure-and-binding",
            );
        let actual = &scope.lineage;
        let local_handoff_promise = store.current_joint_local_handoff_promise(receipt.data())?;
        let local_old_close_sha256 = self.require_current_joint_local_promise(
            &receipt.native,
            receipt.data(),
            actual,
            local_handoff_promise.as_ref(),
        )?;
        store.require_current_joint_local_closure_pointer(
            receipt.data(),
            local_old_close_sha256.as_deref(),
        )?;
        let store_id = self
            .native_abort_catalogue_store_id()?
            .ok_or_else(|| invalid("current joint recovery lacks protected catalogue StoreUUID"))?;
        let binding = NativeCurrentJointBinding {
            joint_decision_sha256: receipt.data().checksum_sha256().into(),
            native_bridge_sha256: receipt.checksum_sha256().into(),
            activation_sha256: activated.activation_sha256()?,
            local_handoff_promise,
            local_old_close_sha256,
            birth_id: receipt.native.birth_id(),
            birth_sha256: receipt.native.birth_sha256().into(),
            store_id,
            physical_base_sha256: actual.base.checksum_sha256.clone(),
            namespace_ids: actual.base.local_namespace_ids.clone(),
            history_generation: actual.history_generation(),
            logical_genesis_sha256: actual.logical_genesis_sha256().into(),
            schema_sha256: actual.schema_sha256().into(),
            range: canonical(receipt.data().activation_range()),
            members: receipt.data().new_members().clone(),
            restore: actual.restore_activation().cloned(),
            point_index: actual.range_index(),
            point_history_sha256: actual.logical_history_sha256().into(),
            point_native_receipt_sha256: actual.native_receipt_sha256().into(),
            local_visibility: actual.actual_local_visibility_sequence(),
            writer_registration: writer.registration_generation(),
        };
        drop(scope);
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(binding_trace);

        let fact = NativeCurrentJointRecoveryDomain {
            native: receipt,
            binding,
            database_incarnation: self.incarnation_id().into(),
            store_open_id: store.recovery_store_open_id(),
            writer,
        };
        #[cfg(test)]
        let recheck_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-joint-factory-final-fact-current",
            );
        if let Some(completion) = completion {
            // The current/archive/anchor were validated through THIS actual
            // completion scope and have not left its retained native journal.
            fact.require_current_from_actual_lineage(self, store, completion.lineage())?;
            completion.require_identity(completion.prepared(), store, completion.role())?;
        } else {
            fact.require_current(self, store)?;
        }
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(recheck_trace);

        parent.check()?;
        Ok(fact)
    }
}
