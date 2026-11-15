//! Historical donor provenance and a DISTINCT remote native publication scope.
//! DATA never reconstructs LocalQQ or NativeJointPreparedDecision.
use super::*;
use crate::distribution_data::NativeJointIrrevocableFacts;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteDecisionSourceData {
    pub binding: NativeSourceExportBindingV1,
    pub constraints: Vec<NativeJointIrrevocableFacts>,
    pub original_close_records: BTreeMap<ClusterNodeId, crate::db::NativeJointOldCloseRecordData>,
    pub checksum_sha256: String,
}
impl NativeRemoteDecisionSourceData {
    pub(crate) fn checksum(&self) -> Result<String> {
        let mut value = self.clone();
        value.checksum_sha256.clear();
        digest(&("native-remote-decision-source-v1", value))
    }
    /// Called ONLY on the actual retained donor certificate; the output remains
    /// historical DATA and needs an independently authenticated transport hop.
    pub(crate) fn from_actual_certificate(
        old: &NativeRemoteOldCutCertificate,
        manifest: &NativeRemoteImageManifestV1,
    ) -> Result<Self> {
        old.require_current()?;
        require_manifest_plan(manifest, &old.plan())?;
        if old.checksum_sha256()? != manifest.old_quorum_sha256
            || old.source_image_materialized_facts()? != manifest.actual_old_closures
            || old.selected_source_close_record()? != &manifest.source_old_close_record
        {
            return Err(invalid(
                "remote decision donor differs from original certified MF/cut",
            ));
        }
        let mut result = Self {
            binding: NativeSourceExportBindingV1::from_manifest(manifest)?,
            constraints: old.decision_constraints(),
            original_close_records: old.decision_close_records()?,
            checksum_sha256: String::new(),
        };
        result.checksum_sha256 = result.checksum()?;
        result.require_manifest(manifest)?;
        old.require_current()?;
        Ok(result)
    }
    pub(crate) fn require_manifest(&self, manifest: &NativeRemoteImageManifestV1) -> Result<()> {
        self.binding.require_manifest(manifest)?;
        crate::distribution_range_image::bounded_json(self, 2 * 1024 * 1024)?;
        if self.checksum_sha256 != self.checksum()?
            || self.original_close_records.len() != manifest.actual_old_closures.len()
            || self.constraints.len() != manifest.old_members.len()
            || self.constraints.len() > MAX_NEW_MEMBERS
        {
            return Err(invalid(
                "remote decision source omits original OLD records/obligations",
            ));
        }
        for (id, facts) in &manifest.actual_old_closures {
            self.original_close_records
                .get(id)
                .ok_or_else(|| invalid("remote decision source original close missing"))?
                .require_materialized_facts(facts)?;
        }
        if self.original_close_records.get(&manifest.source_id)
            != Some(&manifest.source_old_close_record)
        {
            return Err(invalid(
                "remote decision source changed selected original close bytes",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for constraint in &self.constraints {
            let fact = manifest
                .actual_old_closures
                .get(&constraint.node_id)
                .ok_or_else(|| {
                    invalid("remote decision source constraint not in actual ALL-OLD cut")
                })?;
            if !seen.insert(&constraint.node_id)
                || constraint.scope_sha256 != self.binding.scope_sha256
                || constraint.birth_id != fact.birth_id
                || constraint.birth_sha256 != fact.birth_sha256
                || constraint.actual_last > manifest.range_index
                || constraint.actual_resolved > manifest.range_index
                || constraint.actual_compacted > manifest.range_index
                || [
                    constraint.native_base_index,
                    constraint.native_applied_index,
                    constraint.native_pending_index,
                    constraint.native_materialized_pending_index,
                ]
                .iter()
                .any(|index| index.is_some_and(|index| index > manifest.range_index))
                || constraint
                    .commands
                    .iter()
                    .chain(&constraint.unresolved_commands)
                    .any(|(index, hash)| *index > manifest.range_index || !is_digest(hash))
                || constraint
                    .native_point
                    .as_ref()
                    .is_some_and(|(index, h0, h)| {
                        *index > manifest.range_index || h0 != &manifest.h0 || !is_digest(h)
                    })
            {
                return Err(invalid(
                    "remote decision source changed certified irreversible obligations",
                ));
            }
        }
        Ok(())
    }
}

pub(crate) use super::participant::decision::NativeRemoteDecisionAdmission;

/// Historical actual native WAL readback, NOT a joint durability quorum/vote.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteDecisionReceiptV1 {
    pub binding: NativeSourceExportBindingV1,
    pub phase_id: Uuid,
    pub fact_set_sha256: String,
    pub node_id: ClusterNodeId,
    pub decision_sha256: String,
    pub native_record_sha256: String,
    pub native_page_xid: u64,
    pub remote_provenance_sha256: String,
}
impl NativeRemoteDecisionReceiptV1 {
    pub(crate) fn require_request(
        &self,
        request: &NativeRemoteNewRequestV1,
        facts: &NativeRemoteNewFactSetV1,
    ) -> Result<()> {
        self.binding.require_manifest(&request.manifest)?;
        if self.phase_id != request.phase_id
            || self.fact_set_sha256 != facts.checksum_sha256
            || (self.node_id != request.manifest.target_id
                && !request.manifest.old_members.contains_key(&self.node_id))
            || self.native_page_xid == 0
            || !is_digest(&self.decision_sha256)
            || !is_digest(&self.native_record_sha256)
            || !is_digest(&self.remote_provenance_sha256)
        {
            return Err(invalid(
                "remote native DECISION receipt changes exact phase/body/native recipient",
            ));
        }
        Ok(())
    }
}

/// Exact immutable remote provenance DATA persisted with the native body.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteDecisionBindingV1 {
    pub binding: NativeSourceExportBindingV1,
    pub phase_id: Uuid,
    pub participant: NativeRemoteNewSnapshotV1,
    pub fact_set_sha256: String,
    pub source_evidence_sha256: String,
    pub source_proof: NativeRemoteDecisionSourceData,
}
impl NativeRemoteDecisionBindingV1 {
    pub(crate) fn validate(&self) -> Result<()> {
        self.binding.require_shape()?;
        if self.source_proof.binding != self.binding
            || self.source_proof.checksum_sha256 != self.source_evidence_sha256
            || self.source_proof.checksum()? != self.source_evidence_sha256
        {
            return Err(invalid("remote DECISION durable source proof changed"));
        }
        if self.phase_id.is_nil()
            || self.participant.binding != self.binding
            || self.participant.phase_id != self.phase_id
            || self.participant.database_open.is_empty()
            || self.participant.store_open.is_nil()
            || self.participant.metadata_store_id.is_nil()
            || self.participant.checksum_sha256 != self.participant.checksum()?
            || !is_digest(&self.fact_set_sha256)
            || !is_digest(&self.source_evidence_sha256)
        {
            return Err(invalid(
                "remote DECISION durable binding changed original actual target/MF/phase",
            ));
        }
        if self.participant.node_id == self.binding.target_id {
            if self.participant.birth != self.binding.target_birth
                || self.participant.new_materialized.is_none()
            {
                return Err(invalid(
                    "remote DECISION target binding lacks genuine image point",
                ));
            }
        } else {
            let old =
                self.participant.original_old.as_ref().ok_or_else(|| {
                    invalid("remote DECISION non-target lacks original OLD point")
                })?;
            let record = self
                .source_proof
                .original_close_records
                .get(&self.participant.node_id)
                .ok_or_else(|| {
                    invalid("remote DECISION OLD participant not in original certificate")
                })?;
            record.require_materialized_facts(old)?;
            if old.node_id != self.participant.node_id
                || old.birth_id != self.participant.birth.birth_id()
                || old.birth_sha256 != self.participant.birth.birth_sha256()
            {
                return Err(invalid(
                    "remote DECISION OLD binding changes actual original native birth",
                ));
            }
        }
        Ok(())
    }
}

/// Readonly participant ingress, DISTINCT from source capture/discovery APIs.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteDecisionSourceRequestV1 {
    pub source: crate::distribution_range_image::NativeSourceDiscoveryRequestV1,
    pub observer_id: ClusterNodeId,
    pub observer_birth: crate::distribution::NativeStoreBirthV1,
}
impl NativeRemoteDecisionSourceRequestV1 {
    pub(crate) fn require_shape(&self) -> Result<()> {
        self.source.require_shape()?;
        if self.source.operation
            != crate::distribution_range_image::NativeSourceDiscoveryOperationV1::DecisionProvenance
            || self.observer_birth.node_incarnation() == 0
        {
            return Err(invalid(
                "participant provenance ingress accepts only readonly decision observation",
            ));
        }
        self.observer_birth.validate_for(
            Some(self.source.bootstrap_id),
            self.observer_birth.node_incarnation(),
        )
    }
}
