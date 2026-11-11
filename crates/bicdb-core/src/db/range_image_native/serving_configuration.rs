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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeRecoveredServingConfiguration {
    format_version: u32,
    birth_id: Uuid,
    birth_sha256: String,
    physical_base_sha256: String,
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
    fn validate(&self, birth: &NativeDatabaseBirth, base: &NativeRangeBaseBinding) -> Result<()> {
        let digest = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
        self.scope.validate()?;
        if self.format_version != 1
            || self.birth_id != birth.birth_id
            || self.birth_sha256 != birth.checksum_sha256
            || self.physical_base_sha256 != base.checksum_sha256
            || self.scope.cluster_id != birth.spec.cluster_id
            || self.scope.bootstrap_id != birth.spec.bootstrap_id
            || self.local_node_id != birth.spec.node_id
            || self.local_member_incarnation != base.local_member_incarnation
            || !matches!(
                self.scope.purpose,
                MetadataRecoveryRolePurpose::NormalRecovery
            )
            || self.scope.range_id != self.range.id
            || self.scope.range_epoch != self.range.epoch
            || self.scope.initiator_node_id != self.range.leader
            || self.range.id != base.range.id
            || self.range.start_token != base.range.start_token
            || self.range.end_token != base.range.end_token
            || self.range.placement != base.range.placement
            || self.range.epoch < base.range.epoch
            || self.range.replicas.len() != base.range.replicas.len()
            || replicas(&self.range).len() != self.range.replicas.len()
            || replicas(&self.range) != replicas(&base.range)
            || self.activation_index < base.range_index
            || self.logical_genesis_sha256 != base.logical_genesis_sha256
            || !digest(&self.activation_history_sha256)
            || !digest(&self.native_receipt_at_activation_sha256)
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("recovered serving record does not bind the actual same-voter physical/native domain"));
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
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.record.scope
    }
    pub(crate) fn require_current(&self, db: &BicDb) -> Result<()> {
        self.native.require_current(db)?;
        let actual = db.current_native_range_source_lineage(self.record.range.id)?;
        if actual.serving.as_ref() != Some(&self.record) {
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
impl BicDb {
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
        record.validate(birth, base)?;
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
    /// Read-only reissue from ACTUAL native WAL state plus an existing owned
    /// materialization pin. This is not a fresh quorum or activation authority.
    pub(crate) fn observe_native_recovered_serving_for_physical(
        &self,
        physical: &LocalNativeRecoveryReady,
        scope: &RecoveryFenceScope,
        expected_checksum: &str,
    ) -> Result<NativeRecoveredServingReceipt> {
        physical.require_owned_materialization_current(self)?;
        let actual = self.current_native_range_source_lineage(scope.range_id)?;
        let record = actual
            .serving
            .ok_or_else(|| invalid("completed refusal has no actual native serving record"))?;
        if record.checksum_sha256 != expected_checksum
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
        let native = self.native_recovery_admission(
            &record.scope.cluster_id,
            record.scope.bootstrap_id,
            &record.local_node_id,
            record.local_member_incarnation,
        )?;
        let receipt = NativeRecoveredServingReceipt { native, record };
        receipt.require_current(self)?;
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
        prepared.require_current_under_role(self, store, fresh)?;
        quorum.require_recipient_role(fresh)?;
        quorum.require_local_prepared(prepared, self, store, fresh)?;
        if !matches!(fresh.purpose(), MetadataRecoveryRolePurpose::NormalRecovery) {
            return Err(invalid(
                "same-voter recovery cannot activate a handoff configuration",
            ));
        }
        let journal = self.try_native_image_journal()?;
        let lineage = self.current_native_range_source_lineage(fresh.current_range().id)?;
        let base = &lineage.base;
        let mut record = NativeRecoveredServingConfiguration {
            format_version: 1,
            birth_id: lineage.birth.birth_id,
            birth_sha256: lineage.birth.checksum_sha256.clone(),
            physical_base_sha256: base.checksum_sha256.clone(),
            scope: prepared.scope().clone(),
            range: fresh.current_range().clone(),
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
        record.validate(&lineage.birth, base)?;
        if lineage.serving.as_ref().is_some_and(|old| {
            old.range.epoch > record.range.epoch
                || old.activation_index > record.activation_index
                || (old.range.epoch == record.range.epoch && old.range != record.range)
        }) {
            return Err(invalid(
                "recovered serving configuration would regress or conflict",
            ));
        }
        // The actual point is still held by Prepared; a decoded record cannot
        // supply this proof or the independently collected closed ready quorum.
        format::enable_native_recovery_serving(&self.path)?;
        if lineage.serving.as_ref() != Some(&record) {
            self.write_native_range_image_record(&key(record.range.id), &record)?;
        }
        drop(journal);
        let native = self.native_recovery_admission(
            fresh.cluster_id(),
            fresh.bootstrap_id(),
            fresh.local_node_id(),
            fresh.local_member_incarnation(),
        )?;
        let receipt = NativeRecoveredServingReceipt { native, record };
        receipt.require_current(self)?;
        quorum.require_recipient_role(fresh)?;
        prepared.require_current_under_role(self, store, fresh)?;
        Ok(receipt)
    }
}
