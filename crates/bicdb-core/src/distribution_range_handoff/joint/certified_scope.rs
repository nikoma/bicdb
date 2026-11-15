//! The scope of a joint decision, as the certified metadata record carries
//! it. Metadata consensus accepts it only together with the actual dual
//! quorum proof whose scope it equals; from then on it is committed
//! configuration DATA. It names what the later membership stages change and
//! which cluster, member and relocation domain they require. Decoding it
//! constructs no quorum, materialization, writer, activation or cleanup
//! capability.
use super::*;
use std::collections::BTreeMap;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub(crate) struct NativeJointCertifiedScope(NativeJointScopeData);
impl NativeJointCertifiedScope {
    /// Only a decision body hands out its own scope.
    pub(super) fn from_scope(scope: NativeJointScopeData) -> Self {
        Self(scope)
    }
    pub(crate) fn validate(&self) -> Result<()> {
        self.0.validate()
    }
    pub(crate) fn promise(&self) -> &RangeHandoffPromise {
        &self.0.promise
    }
    pub(crate) fn old_range(&self) -> &RangeDescriptor {
        &self.0.old_range
    }
    pub(crate) fn activation_range(&self) -> &RangeDescriptor {
        &self.0.activation_range
    }
    pub(crate) fn old_members(&self) -> &BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        &self.0.old_members
    }
    pub(crate) fn new_members(&self) -> &BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        &self.0.new_members
    }
    pub(crate) fn old_source(&self) -> &ClusterNodeId {
        &self.0.old_source
    }
    pub(crate) fn target(&self) -> &ClusterNodeId {
        &self.0.target
    }
    /// The checksum a decision body reports as its plan checksum.
    pub(crate) fn checksum_sha256(&self) -> Result<String> {
        self.0.checksum()
    }
    /// The same domain check a decision body applies to a fresh committed
    /// configuration: cluster, bootstrap, restore activation, every enrolled
    /// birth, every old and new member credential, and the relocation.
    pub(crate) fn require_metadata_domain(
        &self,
        topology: &crate::distribution::ClusterTopology,
        restore: Option<&crate::distribution_consensus::MetadataRestoreActivation>,
    ) -> Result<()> {
        self.0.validate()?;
        self.0.require_metadata_domain(topology, restore)
    }
    #[cfg(test)]
    pub(crate) fn test_from_json(value: serde_json::Value) -> Result<Self> {
        Ok(serde_json::from_value(value)?)
    }
}

/// The identity of one decision that a bridge request names and a bridge
/// certificate counts members of. It is correlation DATA of the sender; the
/// recipient compares every part with the decision it observes itself.
pub(crate) trait NativeJointBridgeDecision: Send + Sync {
    fn checksum_sha256(&self) -> &str;
    fn plan_sha256(&self) -> Result<String>;
    fn promise(&self) -> &RangeHandoffPromise;
    fn old_range(&self) -> &RangeDescriptor;
    fn old_members(&self) -> &BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember>;
    fn new_members(&self) -> &BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember>;
    fn require_metadata_domain(
        &self,
        topology: &crate::distribution::ClusterTopology,
        restore: Option<&crate::distribution_consensus::MetadataRestoreActivation>,
    ) -> Result<()>;
}
impl NativeJointBridgeDecision for NativeJointDecisionData {
    fn checksum_sha256(&self) -> &str {
        NativeJointDecisionData::checksum_sha256(self)
    }
    fn plan_sha256(&self) -> Result<String> {
        NativeJointDecisionData::plan_sha256(self)
    }
    fn promise(&self) -> &RangeHandoffPromise {
        NativeJointDecisionData::promise(self)
    }
    fn old_range(&self) -> &RangeDescriptor {
        NativeJointDecisionData::old_range(self)
    }
    fn old_members(&self) -> &BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        NativeJointDecisionData::old_members(self)
    }
    fn new_members(&self) -> &BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        NativeJointDecisionData::new_members(self)
    }
    fn require_metadata_domain(
        &self,
        topology: &crate::distribution::ClusterTopology,
        restore: Option<&crate::distribution_consensus::MetadataRestoreActivation>,
    ) -> Result<()> {
        NativeJointDecisionData::require_metadata_domain(self, topology, restore)
    }
}

/// A decision as committed metadata names it: the certified scope and the
/// decision digest of one relocation's committed handoff record. A node that
/// holds no decision body (a coordinator after a restart) addresses the
/// members with it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NativeJointCommittedDecision {
    scope: NativeJointCertifiedScope,
    decision_sha256: String,
}
impl NativeJointCommittedDecision {
    /// From the committed handoff record of `relocation`. `None` when the
    /// record was certified without its scope.
    pub(crate) fn from_committed(
        relocation: &crate::distribution::RangeRelocation,
    ) -> Result<Option<Self>> {
        let Some(record) = relocation.native_joint_handoff_v1.as_ref() else {
            return Err(invalid("relocation has no committed joint decision"));
        };
        record.validate_for(relocation)?;
        Ok(record.certified_scope().map(|scope| Self {
            scope: scope.clone(),
            decision_sha256: record.decision_sha256().to_owned(),
        }))
    }
    pub(crate) fn scope(&self) -> &NativeJointCertifiedScope {
        &self.scope
    }
}
impl NativeJointBridgeDecision for NativeJointCommittedDecision {
    fn checksum_sha256(&self) -> &str {
        &self.decision_sha256
    }
    fn plan_sha256(&self) -> Result<String> {
        self.scope.checksum_sha256()
    }
    fn promise(&self) -> &RangeHandoffPromise {
        self.scope.promise()
    }
    fn old_range(&self) -> &RangeDescriptor {
        self.scope.old_range()
    }
    fn old_members(&self) -> &BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        self.scope.old_members()
    }
    fn new_members(&self) -> &BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        self.scope.new_members()
    }
    fn require_metadata_domain(
        &self,
        topology: &crate::distribution::ClusterTopology,
        restore: Option<&crate::distribution_consensus::MetadataRestoreActivation>,
    ) -> Result<()> {
        self.scope.require_metadata_domain(topology, restore)
    }
}
