//! Native-WAL serving configuration after an ACTUAL closed same-voter ready
//! quorum. The physical image anchor is never relabelled to the new epoch.
//! Persist this record while Prepared still refuses writes; only subsequent
//! durable catalogue completion may release the real freeze.
use super::*;
use crate::distribution::RangeDescriptor;
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::distribution_range_consensus::{
    PreparedNativeRecoveryReady, RangeWriteStore, RecoveryFenceScope,
};
use crate::distribution_range_recovery::{ClosedNativeReadyQuorum, RecoveryRoleAdmission};

mod completion_scope;
pub(crate) use completion_scope::NativeCompletionReadScope;
mod activation_decision;
mod joint_handoff;
pub(crate) use activation_decision::NativeActivationDecisionReceipt;
pub(in crate::db::range_image_native) use joint_handoff::NativeJointServingDomain;
pub(crate) use joint_handoff::{
    NativeCurrentJointBinding, NativeCurrentJointRecoveryDomain, NativeJointBridgeReadScope,
    NativeJointDecisionReceipt, NativeJointHistoricalCutScope, NativeJointOldCloseReceipt,
    NativeJointOldPhysicalProof, NativeJointPreparedPublicationScope,
    NativeJointServingBridgeReceipt, NativeJointServingPreparation,
};

#[cfg(test)]
mod floor_tests;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeRecoveredServingConfiguration {
    format_version: u32,
    birth_id: Uuid,
    birth_sha256: String,
    physical_base_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    joint_decision_sha256: Option<String>,
    scope: RecoveryFenceScope,
    range: RangeDescriptor,
    local_node_id: ClusterNodeId,
    local_member_incarnation: u64,
    activation_index: u64,
    activation_history_sha256: String,
    logical_genesis_sha256: String,
    native_receipt_at_activation_sha256: String,
    local_visibility_at_activation: u64,
    checksum_sha256: String,
}
fn replicas(
    range: &RangeDescriptor,
) -> BTreeMap<crate::ReplicaId, (ClusterNodeId, RangeReplicaRole)> {
    range
        .replicas
        .iter()
        .map(|r| (r.id, (r.node_id.clone(), r.role)))
        .collect()
}
impl NativeRecoveredServingConfiguration {
    pub(super) fn range(&self) -> &RangeDescriptor {
        &self.range
    }
    fn checksum(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &unsigned,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    /// A durable historical floor survives a compatible physical image
    /// replacement. Its former base checksum is evidence of that old binding,
    /// never a current serving grant for the new image.
    fn validate_historical_floor(
        &self,
        birth: &NativeDatabaseBirth,
        base: &NativeRangeBaseBinding,
    ) -> Result<()> {
        let digest = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
        self.scope.validate()?;
        if self.format_version != 1
            || self.birth_id != birth.birth_id
            || self.birth_sha256 != birth.checksum_sha256
            || !digest(&self.physical_base_sha256)
            || self.scope.cluster_id != birth.spec.cluster_id
            || self.scope.bootstrap_id != birth.spec.bootstrap_id
            || self.local_node_id != birth.spec.node_id
            || self.local_member_incarnation != base.local_member_incarnation
            || base.restore_activation != birth.spec.restore_activation
            || match (&self.scope.purpose, &self.joint_decision_sha256) {
                (MetadataRecoveryRolePurpose::NormalRecovery, None) => false,
                (
                    MetadataRecoveryRolePurpose::CurrentJointRecovery {
                        joint_decision_sha256,
                    },
                    Some(anchor),
                ) => joint_decision_sha256 != anchor || !digest(anchor),
                _ => true,
            }
            || self.scope.range_id != self.range.id
            || self.scope.range_epoch != self.range.epoch
            || self.scope.initiator_node_id != self.range.leader
            || self.range.id != base.range.id
            || self.range.start_token != base.range.start_token
            || self.range.end_token != base.range.end_token
            || self.range.placement != base.range.placement
            || (self.joint_decision_sha256.is_none()
                && self.range.replicas.len() != base.range.replicas.len())
            || replicas(&self.range).len() != self.range.replicas.len()
            || (self.joint_decision_sha256.is_none()
                && replicas(&self.range) != replicas(&base.range))
            || self.logical_genesis_sha256 != base.logical_genesis_sha256
            || !digest(&self.activation_history_sha256)
            || !digest(&self.native_receipt_at_activation_sha256)
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("recovered serving record does not bind the actual same-voter physical/native domain"));
        }
        Ok(())
    }

    pub(super) fn binds_current_base(&self, base: &NativeRangeBaseBinding) -> bool {
        self.physical_base_sha256 == base.checksum_sha256
    }

    fn validate_current_grant(
        &self,
        birth: &NativeDatabaseBirth,
        base: &NativeRangeBaseBinding,
    ) -> Result<()> {
        self.validate_historical_floor(birth, base)?;
        if !self.binds_current_base(base)
            || self.range.epoch < base.range.epoch
            || self.activation_index < base.range_index
        {
            return Err(invalid(
                "recovered serving grant does not bind the current physical/native image",
            ));
        }
        Ok(())
    }

    fn require_role_floor(&self, role: &RecoveryRoleAdmission) -> Result<()> {
        role.require_live_request()?;
        if role.current_range().epoch < self.range.epoch
            || (role.current_range().epoch == self.range.epoch
                && role.range_configuration_sha256()? != self.scope.range_configuration_sha256)
        {
            return Err(invalid("fresh recovery role is below the durable serving floor or changes its same-epoch semantic domain"));
        }
        Ok(())
    }
}

impl SourceRangeImageLineage {
    pub(crate) fn require_native_serving_role_floor(
        &self,
        role: &RecoveryRoleAdmission,
    ) -> Result<()> {
        if let Some(record) = &self.serving {
            record.require_role_floor(role)?;
        }
        Ok(())
    }
}

/// The record is a locally trusted durable result, not a transferable quorum
/// certificate. This receipt exists only after the actual native WAL write and
/// readback; it never releases a freeze or activates a catalogue on its own.
#[derive(Debug)]
pub(crate) struct NativeRecoveredServingReceipt {
    native: NativeRecoveryAdmission,
    record: NativeRecoveredServingConfiguration,
}
impl NativeRecoveredServingReceipt {
    pub(crate) fn checksum_sha256(&self) -> &str {
        &self.record.checksum_sha256
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.record.activation_index
    }
    pub(crate) fn native_receipt_at_activation_sha256(&self) -> &str {
        &self.record.native_receipt_at_activation_sha256
    }
    pub(crate) fn local_visibility_at_activation(&self) -> u64 {
        self.record.local_visibility_at_activation
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.record.scope
    }
    pub(crate) fn require_current(&self, db: &BicDb) -> Result<()> {
        self.native.require_current(db)?;
        let actual = db.current_native_range_source_lineage(self.record.range.id)?;
        if actual.serving.as_ref() != Some(&self.record)
            || !self.record.binds_current_base(&actual.base)
        {
            return Err(invalid(
                "actual recovered serving receipt/native open changed",
            ));
        }
        Ok(())
    }
}

fn key(range_id: RangeId) -> String {
    format!("serving-configuration/{range_id}")
}
fn history_key(range_id: RangeId, checksum: &str) -> String {
    format!("serving-configuration-history/{range_id}/{checksum}")
}
enum NativeServingDecision<'a> {
    Closed(&'a ClosedNativeReadyQuorum),
    Completed(&'a crate::distribution_range_recovery::NativeCompletionQuorum),
    Activated(&'a crate::distribution_range_recovery::ActivatedNativeCompletionDecision),
}
impl NativeServingDecision<'_> {
    fn require_recipient_role(&self, fresh: &RecoveryRoleAdmission) -> Result<()> {
        match self {
            Self::Closed(q) => q.require_recipient_role(fresh),
            Self::Completed(q) => q.require_recipient_role(fresh),
            Self::Activated(q) => q.require_recipient_role(fresh),
        }
    }
    fn require_local_prepared_in_scope(&self, scope: &NativeCompletionReadScope<'_>) -> Result<()> {
        let prepared = scope.prepared();
        self.require_recipient_role(scope.role())?;
        prepared.require_current_for_completion_scope(scope)?;
        let (saved, r, h0, h) = match self {
            Self::Closed(q) => (
                q.scope(),
                q.range_index(),
                q.logical_genesis_sha256(),
                q.logical_history_sha256(),
            ),
            Self::Completed(q) => (
                q.scope(),
                q.range_index(),
                q.logical_genesis_sha256(),
                q.logical_history_sha256(),
            ),
            Self::Activated(q) => (
                q.scope(),
                q.range_index(),
                q.logical_genesis_sha256(),
                q.logical_history_sha256(),
            ),
        };
        if !prepared.scope().same_session(saved)
            || prepared.range_index() != r
            || prepared.logical_genesis_sha256() != h0
            || prepared.logical_history_sha256() != h
        {
            return Err(invalid(
                "actual completion scope differs from sealed quorum point/session",
            ));
        }
        self.require_recipient_role(scope.role())
    }
}
impl BicDb {
    fn require_recovered_serving_joint_binding(
        &self,
        birth: &NativeDatabaseBirth,
        base: &NativeRangeBaseBinding,
        record: &NativeRecoveredServingConfiguration,
    ) -> Result<()> {
        if let Some(expected) = &record.joint_decision_sha256 {
            if !format::native_current_joint_recovery_active(&self.path)? {
                return Err(invalid("joint recovered serving lacks reader fence"));
            }
            let joint = self
                .read_native_joint_serving_domain(birth, base)?
                .ok_or_else(|| invalid("joint recovered serving lost protected native domain"))?;
            if joint.decision_sha256() != expected
                || crate::distribution_native_read::canonical_range(joint.range())
                    != crate::distribution_native_read::canonical_range(&record.range)
                || record.activation_index <= joint.range_index()
            {
                return Err(invalid(
                    "joint recovered serving differs from actual protected NEW domain",
                ));
            }
        }
        Ok(())
    }
    pub(super) fn read_native_recovered_serving_configuration(
        &self,
        birth: &NativeDatabaseBirth,
        base: &NativeRangeBaseBinding,
        prefix: Option<&NativeCertifiedRangePrefix>,
    ) -> Result<Option<NativeRecoveredServingConfiguration>> {
        let Some(record) = self
            .read_native_range_image_record::<NativeRecoveredServingConfiguration>(&key(base
                .range
                .id))?
        else {
            return Ok(None);
        };
        if !format::native_recovery_serving_active(&self.path)? {
            return Err(invalid(
                "recovered serving record lacks its older-writer reader fence",
            ));
        }
        record.validate_historical_floor(birth, base)?;
        self.require_recovered_serving_joint_binding(birth, base, &record)?;
        let actual_index = prefix.map_or(base.range_index, |p| p.range_index());
        let actual_visibility = prefix.map_or(base.actual_local_visibility_sequence, |p| {
            p.actual_local_visibility_sequence()
        });
        if record.activation_index > actual_index
            || record.local_visibility_at_activation > actual_visibility
        {
            return Err(invalid(
                "recovered serving activation is ahead of actual materialization",
            ));
        }
        let point = self.read_native_history_entry(
            base.range.id,
            base.history_generation,
            record.activation_index,
        )?;
        if point.commitment_sha256 != record.activation_history_sha256 {
            return Err(invalid(
                "recovered serving activation conflicts with actual native history",
            ));
        }
        Ok(Some(record))
    }
    /// Historical completion anchor only. The actual native head may exceed
    /// R; this cannot become NativeReady or a current-prefix/read permit.
    pub(crate) fn observe_native_activated_completion_anchor(
        &self,
        observer: &crate::distribution_range_recovery::RecoveryObservationRoleAdmission,
        scope: &RecoveryFenceScope,
        serving_sha: &str,
        r: u64,
        h0: &str,
        h: &str,
    ) -> Result<NativeRecoveredServingReceipt> {
        observer.require_scope(scope)?;
        self.check_recovery_state()?;
        let _journal = self.try_native_image_journal()?;
        let actual = self.current_native_range_source_lineage(scope.range_id)?;
        let record = actual
            .serving
            .as_ref()
            .ok_or_else(|| invalid("activated anchor has no actual native serving record"))?;
        let canonical = |range: &RangeDescriptor| {
            let mut r = range.clone();
            r.approximate_bytes = 0;
            r.approximate_qps = 0;
            r.replicas.sort_by_key(|r| r.id);
            r
        };
        let native = self.native_recovery_admission(
            observer.cluster_id(),
            observer.bootstrap_id(),
            observer.local_node_id(),
            observer.local_member_incarnation(),
        )?;
        observer.require_native_identity(&native)?;
        if record.checksum_sha256 != serving_sha
            || !record.scope.same_session(scope)
            || !record.binds_current_base(&actual.base)
            || record.activation_index != r
            || record.logical_genesis_sha256 != h0
            || record.activation_history_sha256 != h
            || actual.range_index() < r
            || actual.logical_genesis_sha256() != h0
            || canonical(actual.native_write_range()?) != canonical(observer.current_range())
            || observer.expected_schema_sha256() != Some(actual.schema_sha256())
            || actual.restore_activation() != observer.restore_activation()
            || observer.member_credentials().iter().any(|(id, m)| {
                actual.base_members().get(id).is_none_or(|a| {
                    a.incarnation != m.incarnation
                        || a.tls_certificate_sha256 != m.active_certificate_sha256
                        || a.pending_tls_certificate_sha256 != m.pending_certificate_sha256
                })
            })
        {
            return Err(invalid(
                "activated completion anchor differs from actual current same-voter native domain",
            ));
        }
        let point =
            self.read_native_history_entry(scope.range_id, actual.base.history_generation, r)?;
        if point.commitment_sha256 != h {
            return Err(invalid(
                "activated completion anchor is missing from actual native history",
            ));
        }
        let receipt = NativeRecoveredServingReceipt {
            native,
            record: record.clone(),
        };
        receipt.require_current(self)?;
        Ok(receipt)
    }
    /// Read-only reissue from ACTUAL native WAL state plus an existing owned
    /// materialization pin. This is not a fresh quorum or activation authority.
    pub(crate) fn observe_native_recovered_serving_for_physical(
        &self,
        physical: &LocalNativeRecoveryReady,
        scope: &RecoveryFenceScope,
        expected_checksum: &str,
    ) -> Result<NativeRecoveredServingReceipt> {
        #[cfg(test)]
        let physical_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-serving-owned-materialization",
            );
        physical.require_owned_materialization_current(self)?;
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(physical_trace);
        #[cfg(test)]
        let lineage_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-serving-actual-current-lineage",
            );

        let actual = self.current_native_range_source_lineage(scope.range_id)?;
        let record = actual
            .serving
            .ok_or_else(|| invalid("completed refusal has no actual native serving record"))?;
        if !record.binds_current_base(&actual.base)
            || record.checksum_sha256 != expected_checksum
            || !record.scope.same_session(scope)
            || record.activation_index != physical.range_index()
            || record.activation_history_sha256 != physical.logical_history_sha256()
            || record.logical_genesis_sha256 != physical.logical_genesis_sha256()
            || record.native_receipt_at_activation_sha256 != physical.native_receipt_sha256()
            || record.local_visibility_at_activation != physical.visibility().get()
        {
            return Err(invalid(
                "completed native record differs from actual pinned materialization",
            ));
        }
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(lineage_trace);
        #[cfg(test)]
        let admission_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-serving-admission-mint",
            );
        let native = self.native_recovery_admission(
            &record.scope.cluster_id,
            record.scope.bootstrap_id,
            &record.local_node_id,
            record.local_member_incarnation,
        )?;
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(admission_trace);
        #[cfg(test)]
        let receipt_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-serving-receipt-full-current",
            );
        let receipt = NativeRecoveredServingReceipt { native, record };
        receipt.require_current(self)?;
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(receipt_trace);
        Ok(receipt)
    }
    /// Caller retains the actual owned Prepared pin/freeze throughout both
    /// native persistence and subsequent catalogue completion. Membership,
    /// replica-ID/pin/restore changes and handoff activation are excluded.
    pub(crate) fn persist_native_recovered_serving_configuration(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        store: &RangeWriteStore,
        quorum: &ClosedNativeReadyQuorum,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<NativeRecoveredServingReceipt> {
        self.persist_native_serving_from_decision(
            prepared,
            store,
            NativeServingDecision::Closed(quorum),
            fresh,
        )
    }
    pub(crate) fn persist_native_serving_from_completion_decision(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        store: &RangeWriteStore,
        quorum: &crate::distribution_range_recovery::NativeCompletionQuorum,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<NativeRecoveredServingReceipt> {
        self.persist_native_serving_from_decision(
            prepared,
            store,
            NativeServingDecision::Completed(quorum),
            fresh,
        )
    }
    pub(crate) fn persist_native_serving_from_activated_decision(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        store: &RangeWriteStore,
        decision: &crate::distribution_range_recovery::ActivatedNativeCompletionDecision,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<NativeRecoveredServingReceipt> {
        self.persist_native_serving_from_decision(
            prepared,
            store,
            NativeServingDecision::Activated(decision),
            fresh,
        )
    }
    fn persist_native_serving_from_decision(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        store: &RangeWriteStore,
        quorum: NativeServingDecision<'_>,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<NativeRecoveredServingReceipt> {
        let receipt = self.with_native_completion_read_scope(prepared, store, fresh, |scope| {
            self.persist_native_serving_in_completion_scope(scope, &quorum)
        })?;
        quorum.require_recipient_role(fresh)?;
        Ok(receipt)
    }
    fn persist_native_serving_in_completion_scope(
        &self,
        scope: &NativeCompletionReadScope<'_>,
        quorum: &NativeServingDecision<'_>,
    ) -> Result<NativeRecoveredServingReceipt> {
        let prepared = scope.prepared();
        let store = scope.store();
        let fresh = scope.role();
        scope.require_identity(prepared, store, fresh)?;
        #[cfg(test)]
        let mut preflight_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-complete-native-preflight",
                fresh.local_node_id(),
                fresh.deadline(),
            );
        // require_local_prepared below is a strict superset: it checks THIS
        // Prepared under THIS fresh role/Store/DB and additionally matches the
        // sealed quorum's exact point. Retain pure scope/lifetime preflight;
        // do not repeat the identical deep physical check in this borrow.
        prepared.scope().require_role(fresh)?;
        quorum.require_recipient_role(fresh)?;
        quorum.require_local_prepared_in_scope(scope)?;
        let joint_fact = match fresh.purpose() {
            MetadataRecoveryRolePurpose::NormalRecovery => None,
            MetadataRecoveryRolePurpose::CurrentJointRecovery { .. } => {
                Some(store.require_current_joint_domain_in_completion_scope(scope)?)
            }
            _ => return Err(invalid(
                "recovered serving requires ordinary or actual protected current-joint recovery",
            )),
        };
        #[cfg(test)]
        {
            preflight_trace.complete();
            drop(preflight_trace);
        }
        #[cfg(test)]
        let mut journal_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-complete-native-journal-actual-publication",
                fresh.local_node_id(),
                fresh.deadline(),
            );
        let lineage = scope.lineage();
        lineage.require_native_serving_role_floor(fresh)?;
        let base = &lineage.base;
        let mut canonical_range = fresh.current_range().clone();
        canonical_range.approximate_bytes = 0;
        canonical_range.approximate_qps = 0;
        canonical_range.replicas.sort_by_key(|replica| replica.id);
        let mut record = NativeRecoveredServingConfiguration {
            format_version: 1,
            birth_id: lineage.birth.birth_id,
            birth_sha256: lineage.birth.checksum_sha256.clone(),
            physical_base_sha256: base.checksum_sha256.clone(),
            joint_decision_sha256: joint_fact
                .as_ref()
                .map(|fact| fact.binding().joint_decision_sha256.clone()),
            scope: prepared.scope().clone(),
            range: canonical_range,
            local_node_id: fresh.local_node_id().clone(),
            local_member_incarnation: fresh.local_member_incarnation(),
            activation_index: prepared.range_index(),
            activation_history_sha256: prepared.logical_history_sha256().to_owned(),
            logical_genesis_sha256: prepared.logical_genesis_sha256().to_owned(),
            native_receipt_at_activation_sha256: prepared.native_receipt_sha256().to_owned(),
            local_visibility_at_activation: prepared.local_visibility_sequence(),
            checksum_sha256: String::new(),
        };
        record.checksum_sha256 = record.checksum()?;
        record.validate_current_grant(&lineage.birth, base)?;
        self.require_recovered_serving_joint_binding(&lineage.birth, base, &record)?;
        if lineage.serving.as_ref().is_some_and(|old| {
            old.range.epoch > record.range.epoch
                || old.activation_index > record.activation_index
                || (old.range.epoch == record.range.epoch
                    && old.scope.range_configuration_sha256
                        != record.scope.range_configuration_sha256)
        }) {
            return Err(invalid(
                "recovered serving configuration would regress or conflict",
            ));
        }
        // The actual point is still held by Prepared; a decoded record cannot
        // supply this proof or the independently collected closed ready quorum.
        // Preserve the native-record writer's storage/recovery preconditions
        // when publishing the archive and pointer in one page transaction.
        self.check_recovery_state()?;
        if !self.config.fsync || !format::range_image_install_active(&self.path)? {
            return Err(invalid(
                "native serving publication requires the sticky fsync reader fence",
            ));
        }
        format::enable_native_recovery_serving(&self.path)?;
        if lineage.serving.as_ref() != Some(&record) {
            // Keep every actual old/new record immutable. Append history
            // BEFORE replacing the active pointer in the SAME native page-WAL
            // transaction, so interruption leaves the former fence intact.
            let mut append = Vec::new();
            for item in lineage.serving.iter().chain(std::iter::once(&record)) {
                let history = history_key(item.range.id, &item.checksum_sha256);
                if let Some(existing) = self
                    .read_native_range_image_record::<NativeRecoveredServingConfiguration>(
                        &history,
                    )?
                {
                    if &existing != item {
                        return Err(invalid("immutable native serving history conflicts"));
                    }
                } else {
                    append.push((
                        native_key(&history),
                        bounded_json(item, MAX_NATIVE_RECEIPT_BYTES)?,
                    ));
                }
            }
            let current_key = native_key(&key(record.range.id));
            let encoded = bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?;
            quorum.require_recipient_role(fresh)?;
            // Shared hook/schema/health/cancel invalidation is possible even
            // under the journal. Recheck the original live owner immediately
            // before entering the unchanged native WAL transaction.
            prepared.require_produced_completion_boundary(self, store, fresh)?;
            self.in_paged_transaction(|paged, xid| {
                quorum.require_recipient_role(fresh)?;
                for (history, bytes) in &append {
                    paged
                        .store()
                        .put(xid, history, bytes)
                        .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
                }
                paged
                    .store()
                    .put(xid, &current_key, &encoded)
                    .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
                Ok(())
            })?
            .ok_or_else(|| invalid("native serving publication has no page store"))?;
        }
        #[cfg(test)]
        eprintln!("ACTUAL_RECOVERY_COMPLETE_NATIVE_WAL_RETURNED node={} nonce={} R={} H={} B={} serving_sha={} rewritten={} remaining_ms={}", fresh.local_node_id(), prepared.scope().recovery_nonce, record.activation_index, record.activation_history_sha256, record.local_visibility_at_activation, record.checksum_sha256, lineage.serving.as_ref()!=Some(&record), fresh.deadline().saturating_duration_since(std::time::Instant::now()).as_millis());
        #[cfg(test)]
        {
            journal_trace.complete();
            drop(journal_trace);
        }
        #[cfg(test)]
        let mut readback_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-complete-native-readback-final-current",
                fresh.local_node_id(),
                fresh.deadline(),
            );
        // Read the actual NEW current pointer AND immutable archive under the
        // SAME journal. The original lineage is physical/native evidence only;
        // its pre-WAL serving pointer is never relabelled as poststate.
        let receipt = scope.observe_serving(&record.checksum_sha256)?;
        if receipt.record != record {
            return Err(invalid(
                "actual native completion readback differs from published record",
            ));
        }
        scope.require_unchanged_native_point()?;
        quorum.require_recipient_role(fresh)?;
        prepared.require_current_for_completion_scope(scope)?;
        #[cfg(test)]
        {
            readback_trace.complete();
            drop(readback_trace);
        }
        Ok(receipt)
    }
}
