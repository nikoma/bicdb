//! Current LOCAL native read observations. The request is untrusted scope;
//! neither a decoded DTO nor a local observation grants MQ/QQ/read authority.
use crate::distribution::{ClusterId, ClusterNodeId, RangeDescriptor, RangeReplicaRole};
use crate::distribution_consensus::MetadataRestoreActivation;
use crate::distribution_range_recovery::RecoveryMemberIdentity;
use crate::{CancellationToken, Result};
pub(crate) const NATIVE_RANGE_READ_VERSION: u32 = 1;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use uuid::Uuid;
mod quorum;
mod rpc;
pub(crate) use quorum::{NativeReadQuorumCandidate, NativeReadQuorumRound};
pub(crate) use rpc::{
    require_peer, NativeRangeReadObservation, NativeRangeReadProbeReply,
    NativeRangeReadProbeRequest,
};

pub(crate) fn invalid(detail: &str) -> crate::BicDbError {
    crate::BicDbError::Cluster(format!("native range read observation: {detail}"))
}
pub(crate) fn canonical_range(range: &RangeDescriptor) -> RangeDescriptor {
    let mut result = range.clone();
    result.approximate_bytes = 0;
    result.approximate_qps = 0;
    // Learner placement grants no vote/read permission. The epoch, leader,
    // span and current voter identities/roles remain exact.
    result
        .replicas
        .retain(|r| r.role == RangeReplicaRole::Voter);
    result.replicas.sort_by_key(|r| r.id);
    result
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRangeReadScope {
    pub cluster_id: ClusterId,
    pub bootstrap_id: Uuid,
    pub range: RangeDescriptor,
    pub members: BTreeMap<ClusterNodeId, RecoveryMemberIdentity>,
    pub schema_sha256: String,
    pub restore_activation: Option<MetadataRestoreActivation>,
}
impl NativeRangeReadScope {
    pub(crate) fn validate(&self) -> Result<()> {
        crate::distribution_range_consensus::validate_native_json_size(self, 128 * 1024)?;
        let voters = self
            .range
            .replicas
            .iter()
            .filter(|r| r.role == RangeReplicaRole::Voter)
            .map(|r| r.node_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let digest = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
        if self.bootstrap_id.is_nil()
            || self.range.epoch == 0
            || voters.is_empty()
            || self.range.replicas.len() > 64
            || voters.len() != self.members.len()
            || !self.members.contains_key(&self.range.leader)
            || self.members.keys().any(|id| !voters.contains(id))
            || !digest(&self.schema_sha256)
            || self.members.values().any(|m| {
                m.incarnation == 0
                    || m.active_certificate_sha256
                        .as_deref()
                        .is_some_and(|h| !digest(h))
                    || m.pending_certificate_sha256
                        .as_deref()
                        .is_some_and(|h| !digest(h))
            })
        {
            return Err(invalid("malformed untrusted observation scope"));
        }
        Ok(())
    }
    pub(crate) fn checksum_sha256(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        self.validate()?;
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
            "bicdb-native-range-read-scope-v1",
            &self.cluster_id,
            self.bootstrap_id,
            canonical_range(&self.range),
            &self.members,
            &self.schema_sha256,
            &self.restore_activation,
        ))?)))
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRangeReadRequest {
    pub protocol_version: u32,
    pub scope: NativeRangeReadScope,
    pub challenge: Uuid,
    pub budget_ms: u64,
}
impl NativeRangeReadRequest {
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        self.scope.validate()?;
        if self.protocol_version != NATIVE_RANGE_READ_VERSION
            || self.challenge.is_nil()
            || self.budget_ms == 0
            || self.budget_ms > 5000
        {
            return Err(invalid("invalid read challenge/budget"));
        }
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(self.budget_ms));
        token.check()?;
        Ok(token)
    }
}
/// Wire/audit facts ONLY. Deserialize never creates the opaque local proof.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRangeReadFacts {
    pub evidence_version: u32,
    pub scope_sha256: String,
    pub challenge: Uuid,
    pub node_id: ClusterNodeId,
    pub member_incarnation: u64,
    pub store_open_id: Uuid,
    pub native_database_incarnation: String,
    pub native_birth_id: Uuid,
    pub native_birth_sha256: String,
    pub range_index: u64,
    pub logical_genesis_sha256: String,
    pub logical_history_sha256: String,
    pub native_receipt_sha256: String,
    pub local_visibility_db_sequence: u64,
    pub native_history_generation: Uuid,
    pub native_global_image_generation: Uuid,
    pub native_range_image_generation: Uuid,
    pub native_serving_record_commitment_sha256: Option<String>,
    pub catalogue_sha256: String,
}
/// Actual current-open LOCAL observation, not a read pin, quorum or permit.
/// Revalidation keeps the ORIGINAL cancellation/deadline. It never renews it.
#[derive(Debug)]
pub(crate) struct LocalNativeRangeReadObservation {
    scope: NativeRangeReadScope,
    physical: crate::db::NativeRangeReadPhysical,
    catalogue: crate::distribution_range_consensus::NativeRangeReadCatalogue,
    facts: NativeRangeReadFacts,
    cancellation: CancellationToken,
}
impl LocalNativeRangeReadObservation {
    pub(crate) fn from_actual_service(
        evidence: crate::distribution_data::ActualServiceNativeReadEvidence,
    ) -> Result<Self> {
        let (request, physical, catalogue, cancellation) = evidence.into_parts();
        let cancellation = request.cancellation(&cancellation)?;
        if !physical.matches_scope(&request.scope)? {
            return Err(invalid("actual local observation scope differs"));
        }
        let facts = physical.facts(request.challenge, &catalogue)?;
        Ok(Self {
            scope: request.scope.clone(),
            physical,
            catalogue,
            facts,
            cancellation,
        })
    }
    pub(crate) fn scope(&self) -> &NativeRangeReadScope {
        &self.scope
    }
    pub(crate) fn physical(&self) -> &crate::db::NativeRangeReadPhysical {
        &self.physical
    }
    pub(crate) fn catalogue(
        &self,
    ) -> &crate::distribution_range_consensus::NativeRangeReadCatalogue {
        &self.catalogue
    }
    pub(crate) fn facts(&self) -> &NativeRangeReadFacts {
        &self.facts
    }
    pub(crate) fn local_visibility(&self) -> &crate::db::NativeReadLocalDbSequence {
        self.physical.local_visibility()
    }
    pub(crate) fn require_lifetime(&self) -> Result<()> {
        self.cancellation.check()?;
        self.physical.require_lifetime()
    }
    pub(crate) fn require_current(&self, service: &crate::ClusterDataNodeService) -> Result<()> {
        self.require_lifetime()?;
        service.revalidate_current_native_range(self)
    }
}

#[cfg(test)]
mod tests;
