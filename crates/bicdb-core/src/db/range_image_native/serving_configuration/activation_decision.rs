//! Protected native-WAL evidence that an actual completion quorum decided a
//! point. This is historical decision evidence, never a read/serving permit.
//! The audit DTO cannot publish a record: only the opaque live QQ can do so.
use super::*;
use crate::distribution_range_recovery::{
    NativeCompletionDecisionProvenance, NativeCompletionQuorum, RecoveryMemberIdentity,
    RecoveryObservationRoleAdmission,
};

const ACTIVATION_DECISION_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct NativeActivationDecisionRecord {
    format_version: u32,
    birth_id: Uuid,
    birth_sha256: String,
    scope: RecoveryFenceScope,
    range: RangeDescriptor,
    members: BTreeMap<ClusterNodeId, RecoveryMemberIdentity>,
    schema_sha256: String,
    restore_activation: Option<crate::distribution_consensus::MetadataRestoreActivation>,
    local_node_id: ClusterNodeId,
    local_member_incarnation: u64,
    native_serving_sha256: String,
    native_receipt_sha256: String,
    local_visibility_sequence: u64,
    range_index: u64,
    logical_genesis_sha256: String,
    logical_history_sha256: String,
    completion_quorum_sha256: String,
    provenance: NativeCompletionDecisionProvenance,
    post_quorum_metadata_context_id: Uuid,
    post_quorum_metadata_term: u64,
    post_quorum_metadata_read_index: u64,
    native_page_xid: u64,
    checksum_sha256: String,
}
fn canonical(range: &RangeDescriptor) -> RangeDescriptor {
    let mut range = range.clone();
    range.approximate_bytes = 0;
    range.approximate_qps = 0;
    range.replicas.sort_by_key(|r| r.id);
    range
}
fn decision_key(range: crate::RangeId) -> String {
    format!("activation-decision/{range}")
}
fn decision_history_key(range: crate::RangeId, checksum: &str) -> String {
    format!("activation-decision-history/{range}/{checksum}")
}
impl NativeActivationDecisionRecord {
    fn checksum(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &unsigned,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self) -> Result<()> {
        let digest = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
        self.scope.validate()?;
        self.provenance.validate(&self.scope, &self.members)?;
        if self.format_version != ACTIVATION_DECISION_VERSION
            || self.birth_id.is_nil()
            || self.native_page_xid == 0
            || !matches!(
                self.scope.purpose,
                MetadataRecoveryRolePurpose::NormalRecovery
            )
            || self.scope.range_id != self.range.id
            || self.scope.range_epoch != self.range.epoch
            || self.scope.initiator_node_id != self.range.leader
            || self.range != canonical(&self.range)
            || self.local_member_incarnation == 0
            || self
                .members
                .get(&self.local_node_id)
                .is_none_or(|member| member.incarnation != self.local_member_incarnation)
            || self.provenance.local_recipient != self.local_node_id
            || self.provenance.local_member_incarnation != self.local_member_incarnation
            || self.provenance.range_index != self.range_index
            || self.provenance.logical_genesis_sha256 != self.logical_genesis_sha256
            || self.provenance.logical_history_sha256 != self.logical_history_sha256
            || self.completion_quorum_sha256 != self.provenance.checksum_sha256()?
            || self.post_quorum_metadata_context_id.is_nil()
            || self.post_quorum_metadata_context_id == self.provenance.metadata_context_id
            || self.provenance.voters.values().any(|voter| {
                voter.observed_metadata_context_id == self.post_quorum_metadata_context_id
            })
            || self.post_quorum_metadata_term < self.provenance.metadata_term
            || self.post_quorum_metadata_read_index < self.provenance.metadata_read_index
            || self.post_quorum_metadata_term < self.scope.metadata_term
            || self.post_quorum_metadata_read_index < self.scope.metadata_read_index
            || (self.range_index == 0 && self.logical_genesis_sha256 != self.logical_history_sha256)
            || [
                &self.birth_sha256,
                &self.schema_sha256,
                &self.native_serving_sha256,
                &self.native_receipt_sha256,
                &self.logical_genesis_sha256,
                &self.logical_history_sha256,
                &self.completion_quorum_sha256,
            ]
            .iter()
            .any(|s| !digest(s))
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged protected native activation decision"));
        }
        Ok(())
    }
    fn require_native_domain(
        &self,
        db: &BicDb,
        native: &NativeRecoveryAdmission,
        current_serving: bool,
    ) -> Result<()> {
        self.validate()?;
        native.require_current(db)?;
        let actual = db.current_native_range_source_lineage(self.scope.range_id)?;
        if self.birth_id != native.birth_id()
            || self.birth_sha256 != native.birth_sha256()
            || self.scope.cluster_id != *native.cluster_id()
            || self.scope.bootstrap_id != native.bootstrap_id()
            || self.local_node_id != *native.local_node_id()
            || self.local_member_incarnation != native.local_member_incarnation()
            || self.restore_activation.as_ref() != native.restore_activation()
            || actual.logical_genesis_sha256() != self.logical_genesis_sha256
            || actual.range_index() < self.range_index
            || actual.schema_sha256() != self.schema_sha256
            || actual.restore_activation() != self.restore_activation.as_ref()
            || actual.base_members().len() != self.members.len()
            || self.members.iter().any(|(id, member)| {
                actual.base_members().get(id).is_none_or(|stored| {
                    stored.incarnation != member.incarnation
                        || stored.tls_certificate_sha256 != member.active_certificate_sha256
                        || stored.pending_tls_certificate_sha256
                            != member.pending_certificate_sha256
                })
            })
        {
            return Err(invalid(
                "activation decision has another actual native domain",
            ));
        }
        let history = db.read_native_history_entry(
            self.scope.range_id,
            actual.history_generation(),
            self.range_index,
        )?;
        if history.commitment_sha256 != self.logical_history_sha256 {
            return Err(invalid(
                "activation decision is absent from actual native history",
            ));
        }
        let serving = if current_serving {
            actual.serving.as_ref().cloned().ok_or_else(|| {
                invalid("activation decision has no current actual native serving record")
            })?
        } else {
            db.read_native_range_image_record::<NativeRecoveredServingConfiguration>(&history_key(
                self.scope.range_id,
                &self.native_serving_sha256,
            ))?
            .ok_or_else(|| {
                invalid("historical activation decision lost its native serving record")
            })?
        };
        serving.validate_historical_floor(&actual.birth, &actual.base)?;
        if serving.checksum_sha256 != self.native_serving_sha256
            || !serving.scope.same_session(&self.scope)
            || serving.activation_index != self.range_index
            || serving.logical_genesis_sha256 != self.logical_genesis_sha256
            || serving.activation_history_sha256 != self.logical_history_sha256
            || serving.native_receipt_at_activation_sha256 != self.native_receipt_sha256
            || serving.local_visibility_at_activation != self.local_visibility_sequence
            || (current_serving
                && (!serving.binds_current_base(&actual.base)
                    || canonical(actual.native_write_range()?) != self.range))
        {
            return Err(invalid(
                "activation decision differs from actual native serving/history",
            ));
        }
        Ok(())
    }
}

/// Created only after actual page-WAL publication/readback or a validated read
/// of that protected record. It cannot release a pin or manufacture a QQ.
#[derive(Debug)]
pub(crate) struct NativeActivationDecisionReceipt {
    native: NativeRecoveryAdmission,
    record: NativeActivationDecisionRecord,
}
impl NativeActivationDecisionReceipt {
    pub(crate) fn checksum_sha256(&self) -> &str {
        &self.record.checksum_sha256
    }
    pub(crate) fn completion_quorum_sha256(&self) -> &str {
        &self.record.completion_quorum_sha256
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.record.scope
    }
    pub(crate) fn native_serving_sha256(&self) -> &str {
        &self.record.native_serving_sha256
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
    pub(crate) fn require_current(&self, db: &BicDb) -> Result<()> {
        let _journal = db.try_native_image_journal()?;
        self.require_current_under_journal(db)
    }
    fn require_current_under_journal(&self, db: &BicDb) -> Result<()> {
        self.record.require_native_domain(db, &self.native, true)?;
        let current = db
            .read_native_range_image_record::<NativeActivationDecisionRecord>(&decision_key(
                self.record.scope.range_id,
            ))?
            .ok_or_else(|| invalid("actual protected activation decision is missing"))?;
        let archived = db
            .read_native_range_image_record::<NativeActivationDecisionRecord>(
                &decision_history_key(self.record.scope.range_id, &self.record.checksum_sha256),
            )?
            .ok_or_else(|| invalid("immutable protected activation decision is missing"))?;
        if current != self.record || archived != self.record {
            return Err(invalid("protected activation decision changed/reopened"));
        }
        Ok(())
    }
}
impl BicDb {
    pub(crate) fn persist_native_activation_decision(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        store: &RangeWriteStore,
        quorum: &NativeCompletionQuorum,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<NativeActivationDecisionReceipt> {
        quorum.require_local_prepared(prepared, self, store, fresh)?;
        let serving = prepared.require_completed_for_activation(self, store, fresh)?;
        serving.require_current(self)?;
        let provenance = quorum.decision_provenance()?;
        provenance.validate(prepared.scope(), fresh.member_credentials())?;
        let native = self.native_recovery_admission(
            fresh.cluster_id(),
            fresh.bootstrap_id(),
            fresh.local_node_id(),
            fresh.local_member_incarnation(),
        )?;
        let _journal = self.try_native_image_journal()?;
        let pointer = decision_key(prepared.scope().range_id);
        let old =
            self.read_native_range_image_record::<NativeActivationDecisionRecord>(&pointer)?;
        if let Some(old) = &old {
            old.require_native_domain(self, &native, false)?;
            let archived = self
                .read_native_range_image_record::<NativeActivationDecisionRecord>(
                    &decision_history_key(old.scope.range_id, &old.checksum_sha256),
                )?
                .ok_or_else(|| {
                    invalid("existing activation decision lost its immutable archive")
                })?;
            if archived != *old {
                return Err(invalid("existing activation decision archive conflicts"));
            }
            if old.scope.same_session(prepared.scope()) {
                if old.native_serving_sha256 != serving.checksum_sha256()
                    || old.range_index != prepared.range_index()
                    || old.logical_genesis_sha256 != prepared.logical_genesis_sha256()
                    || old.logical_history_sha256 != prepared.logical_history_sha256()
                {
                    return Err(invalid("same-session native activation decision conflicts"));
                }
                let receipt = NativeActivationDecisionReceipt {
                    native,
                    record: old.clone(),
                };
                receipt.require_current_under_journal(self)?;
                quorum.require_recipient_role(fresh)?;
                prepared.require_current_under_role(self, store, fresh)?;
                return Ok(receipt);
            }
            if old.range.epoch > fresh.current_range().epoch
                || old.range_index > prepared.range_index()
                || (old.range.epoch == fresh.current_range().epoch
                    && old.scope.range_configuration_sha256
                        != fresh.range_configuration_sha256()?)
            {
                return Err(invalid("native activation decision would regress/conflict"));
            }
        }
        let mut record = NativeActivationDecisionRecord {
            format_version: ACTIVATION_DECISION_VERSION,
            birth_id: native.birth_id(),
            birth_sha256: native.birth_sha256().into(),
            scope: prepared.scope().clone(),
            range: canonical(fresh.current_range()),
            members: fresh.member_credentials().clone(),
            schema_sha256: fresh
                .expected_schema_sha256()
                .ok_or_else(|| invalid("activation decision has no verified schema"))?
                .into(),
            restore_activation: fresh.restore_activation().cloned(),
            local_node_id: fresh.local_node_id().clone(),
            local_member_incarnation: fresh.local_member_incarnation(),
            native_serving_sha256: serving.checksum_sha256().into(),
            native_receipt_sha256: serving.native_receipt_at_activation_sha256().into(),
            local_visibility_sequence: serving.local_visibility_at_activation(),
            range_index: prepared.range_index(),
            logical_genesis_sha256: prepared.logical_genesis_sha256().into(),
            logical_history_sha256: prepared.logical_history_sha256().into(),
            completion_quorum_sha256: provenance.checksum_sha256()?,
            provenance,
            post_quorum_metadata_context_id: fresh.context_id(),
            post_quorum_metadata_term: fresh.metadata_term(),
            post_quorum_metadata_read_index: fresh.metadata_read_index(),
            native_page_xid: 0,
            checksum_sha256: String::new(),
        };
        self.check_recovery_state()?;
        if !self.config.fsync
            || !format::range_image_install_active(&self.path)?
            || !format::native_recovery_serving_active(&self.path)?
        {
            return Err(invalid(
                "activation decision requires actual fsync image/serving reader fences",
            ));
        }
        self.in_paged_transaction(|paged, xid| {
            record.native_page_xid = xid;
            record.checksum_sha256 = record.checksum()?;
            record.require_native_domain(self, &native, true)?;
            for item in old.iter().chain(std::iter::once(&record)) {
                let archive = decision_history_key(item.scope.range_id, &item.checksum_sha256);
                if let Some(existing) =
                    self.read_native_range_image_record::<NativeActivationDecisionRecord>(&archive)?
                {
                    if existing != *item {
                        return Err(invalid("immutable activation decision history conflicts"));
                    }
                } else {
                    paged
                        .store()
                        .put(
                            xid,
                            &native_key(&archive),
                            &bounded_json(item, MAX_NATIVE_RECEIPT_BYTES)?,
                        )
                        .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
                }
            }
            paged
                .store()
                .put(
                    xid,
                    &native_key(&pointer),
                    &bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?,
                )
                .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
            Ok(())
        })?
        .ok_or_else(|| invalid("activation decision has no native page store"))?;
        let receipt = NativeActivationDecisionReceipt { native, record };
        receipt.require_current_under_journal(self)?;
        quorum.require_recipient_role(fresh)?;
        prepared.require_current_under_role(self, store, fresh)?;
        Ok(receipt)
    }
}
impl BicDb {
    fn observe_native_activation_decision_bound(
        &self,
        native: NativeRecoveryAdmission,
        scope: &RecoveryFenceScope,
        expected_sha: &str,
        serving_sha: &str,
        r: u64,
        h0: &str,
        h: &str,
    ) -> Result<NativeActivationDecisionReceipt> {
        let _journal = self.try_native_image_journal()?;
        let record = self
            .read_native_range_image_record::<NativeActivationDecisionRecord>(&decision_key(
                scope.range_id,
            ))?
            .ok_or_else(|| invalid("no actual protected native activation decision"))?;
        if record.checksum_sha256 != expected_sha
            || !record.scope.same_session(scope)
            || record.native_serving_sha256 != serving_sha
            || record.range_index != r
            || record.logical_genesis_sha256 != h0
            || record.logical_history_sha256 != h
        {
            return Err(invalid(
                "protected native activation decision binding differs",
            ));
        }
        let receipt = NativeActivationDecisionReceipt { native, record };
        receipt.require_current_under_journal(self)?;
        Ok(receipt)
    }
    pub(crate) fn observe_native_activation_decision_for_role(
        &self,
        fresh: &RecoveryRoleAdmission,
        scope: &RecoveryFenceScope,
        expected_sha: &str,
        serving_sha: &str,
        r: u64,
        h0: &str,
        h: &str,
    ) -> Result<NativeActivationDecisionReceipt> {
        scope.require_role(fresh)?;
        let native = self.native_recovery_admission(
            fresh.cluster_id(),
            fresh.bootstrap_id(),
            fresh.local_node_id(),
            fresh.local_member_incarnation(),
        )?;
        let receipt = self.observe_native_activation_decision_bound(
            native,
            scope,
            expected_sha,
            serving_sha,
            r,
            h0,
            h,
        )?;
        let record = &receipt.record;
        if record.range != canonical(fresh.current_range())
            || record.members != *fresh.member_credentials()
            || fresh.expected_schema_sha256() != Some(record.schema_sha256.as_str())
            || fresh.restore_activation() != record.restore_activation.as_ref()
        {
            return Err(invalid(
                "activation decision differs from fresh role domain",
            ));
        }
        scope.require_role(fresh)?;
        Ok(receipt)
    }
    pub(crate) fn observe_native_activation_decision_for_observer(
        &self,
        observer: &RecoveryObservationRoleAdmission,
        scope: &RecoveryFenceScope,
        expected_sha: &str,
        serving_sha: &str,
        r: u64,
        h0: &str,
        h: &str,
    ) -> Result<NativeActivationDecisionReceipt> {
        observer.require_scope(scope)?;
        let native = self.native_recovery_admission(
            observer.cluster_id(),
            observer.bootstrap_id(),
            observer.local_node_id(),
            observer.local_member_incarnation(),
        )?;
        observer.require_native_identity(&native)?;
        let receipt = self.observe_native_activation_decision_bound(
            native,
            scope,
            expected_sha,
            serving_sha,
            r,
            h0,
            h,
        )?;
        let record = &receipt.record;
        if record.range != canonical(observer.current_range())
            || record.members != *observer.member_credentials()
            || observer.expected_schema_sha256() != Some(record.schema_sha256.as_str())
            || observer.restore_activation() != record.restore_activation.as_ref()
        {
            return Err(invalid(
                "activation decision differs from fresh observer domain",
            ));
        }
        observer.require_scope(scope)?;
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests;
