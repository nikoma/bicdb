//! Metadata correlation for an independently certified native data handoff.
//! Decoding this record grants no image, write, cleanup or read permission.
use super::*;

pub const NATIVE_JOINT_HANDOFF_TOPOLOGY_FORMAT_VERSION: u32 = 3;
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum NativeJointHandoffStageV1 {
    /// Both old-closed and intended-new-materialized quorums decided one cut.
    Certified,
    /// Intermediate union membership; every ordinary writer remains fenced.
    PromotedFrozen,
    /// Final membership is committed; physical source cleanup is still pending.
    AwaitingActivation,
    /// A distinct actual new-data activation quorum permits retirement only.
    AwaitingCleanup,
    /// Actual protected source tombstone and full empty-range audit completed.
    Cleaned,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeJointHandoffMetadataV1 {
    version: u32,
    session_id: Uuid,
    nonce: Uuid,
    decision_sha256: String,
    old_configuration_sha256: String,
    new_configuration_sha256: String,
    range_index: u64,
    logical_genesis_sha256: String,
    logical_history_sha256: String,
    source_snapshot_db_sequence: u64,
    target_local_snapshot_db_sequence: u64,
    stage: NativeJointHandoffStageV1,
    activated_quorum_sha256: Option<String>,
    source_cleanup_sha256: Option<String>,
}
impl NativeJointHandoffMetadataV1 {
    pub fn stage(&self) -> NativeJointHandoffStageV1 {
        self.stage
    }
    pub fn decision_sha256(&self) -> &str {
        &self.decision_sha256
    }
    pub fn range_index(&self) -> u64 {
        self.range_index
    }
    pub fn logical_genesis_sha256(&self) -> &str {
        &self.logical_genesis_sha256
    }
    pub fn logical_history_sha256(&self) -> &str {
        &self.logical_history_sha256
    }
    pub(crate) fn validate_for(&self, plan: &RangeRelocation) -> Result<()> {
        let digest = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
        };
        if self.version != 1
            || self.session_id.is_nil()
            || self.nonce.is_nil()
            || plan.handoff_session.as_ref().is_none_or(|session| {
                session.session_id != self.session_id || session.nonce != self.nonce
            })
            || [
                &self.decision_sha256,
                &self.old_configuration_sha256,
                &self.new_configuration_sha256,
                &self.logical_genesis_sha256,
                &self.logical_history_sha256,
            ]
            .into_iter()
            .any(|value| !digest(value))
            || self
                .activated_quorum_sha256
                .as_ref()
                .is_some_and(|v| !digest(v))
            || self
                .source_cleanup_sha256
                .as_ref()
                .is_some_and(|v| !digest(v))
            || (self.stage >= NativeJointHandoffStageV1::AwaitingCleanup)
                != self.activated_quorum_sha256.is_some()
            || (self.stage == NativeJointHandoffStageV1::Cleaned)
                != self.source_cleanup_sha256.is_some()
        {
            return Err(cluster_error(
                "invalid native joint handoff metadata correlation",
            ));
        }
        Ok(())
    }
    fn immutable_binding(&self) -> Self {
        let mut record = self.clone();
        record.stage = NativeJointHandoffStageV1::Certified;
        record.activated_quorum_sha256 = None;
        record.source_cleanup_sha256 = None;
        record
    }
}
/// Sequential proposals and authoritative snapshot jumps preserve every known
/// decision and tombstone. Stage data alone never authorizes a runtime action.
pub(crate) fn validate_native_joint_handoff_preservation(
    previous: &ClusterTopology,
    next: &ClusterTopology,
) -> Result<()> {
    if previous.format_version >= NATIVE_JOINT_HANDOFF_TOPOLOGY_FORMAT_VERSION
        && next.format_version < NATIVE_JOINT_HANDOFF_TOPOLOGY_FORMAT_VERSION
    {
        return Err(cluster_error(
            "native joint handoff topology reader fence cannot be downgraded",
        ));
    }
    for (id, plan) in &previous.relocations {
        let Some(old) = &plan.native_joint_handoff_v1 else {
            continue;
        };
        let new = next
            .relocations
            .get(id)
            .and_then(|p| p.native_joint_handoff_v1.as_ref())
            .ok_or_else(|| cluster_error("native joint decision/tombstone cannot be erased"))?;
        if new.immutable_binding() != old.immutable_binding()
            || new.stage < old.stage
            || old
                .activated_quorum_sha256
                .as_ref()
                .is_some_and(|v| new.activated_quorum_sha256.as_ref() != Some(v))
            || old
                .source_cleanup_sha256
                .as_ref()
                .is_some_and(|v| new.source_cleanup_sha256.as_ref() != Some(v))
        {
            return Err(cluster_error("native joint decision changed or regressed"));
        }
    }
    Ok(())
}
