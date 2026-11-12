//! Persisted audit projection only. Deserializing this type NEVER constructs a
//! quorum, role, activation receipt or source/read permission. Only the live
//! opaque quorum's method supplies it to the protected native-WAL producer.
use super::*;
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeCompletionVoterProvenance {
    pub member_incarnation: u64,
    pub peer_certificate_sha256: Option<String>,
    pub peer_plaintext_loopback: bool,
    pub stage: NativeCompletionStage,
    pub challenge: Uuid,
    pub store_open_id: Uuid,
    pub native_database_incarnation: String,
    pub native_serving_sha256: String,
    pub native_receipt_sha256: String,
    pub activation_decision_sha256: Option<String>,
    pub observed_metadata_context_id: Uuid,
    pub observed_metadata_term: u64,
    pub observed_metadata_read_index: u64,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeCompletionDecisionProvenance {
    pub scope: RecoveryFenceScope,
    pub range_index: u64,
    pub logical_genesis_sha256: String,
    pub logical_history_sha256: String,
    pub local_recipient: ClusterNodeId,
    pub local_member_incarnation: u64,
    pub metadata_context_id: Uuid,
    pub metadata_term: u64,
    pub metadata_read_index: u64,
    pub voters: BTreeMap<ClusterNodeId, NativeCompletionVoterProvenance>,
}
impl NativeCompletionDecisionProvenance {
    /// Structural/current-domain validation of a protected record's audit
    /// content, NOT reconstitution of a live authenticated quorum.
    pub(crate) fn validate(
        &self,
        scope: &RecoveryFenceScope,
        members: &BTreeMap<ClusterNodeId, RecoveryMemberIdentity>,
    ) -> Result<()> {
        self.scope.validate()?;
        scope.validate()?;
        let sha = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
        if !self.scope.same_session(scope)
            || !matches!(
                scope.purpose,
                MetadataRecoveryRolePurpose::NormalRecovery
                    | MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
            )
            || members.is_empty()
            || members.len() > 64
            || self.voters.len() > members.len()
            || self.voters.len() < members.len() / 2 + 1
            || self.metadata_context_id.is_nil()
            || self.metadata_term < self.scope.metadata_term
            || self.metadata_read_index < self.scope.metadata_read_index
            || members
                .get(&self.local_recipient)
                .is_none_or(|m| m.incarnation != self.local_member_incarnation)
            || !sha(&self.logical_genesis_sha256)
            || !sha(&self.logical_history_sha256)
            || (self.range_index == 0 && self.logical_genesis_sha256 != self.logical_history_sha256)
        {
            return Err(invalid(
                "damaged native completion provenance domain/majority/point",
            ));
        }
        for (node, voter) in &self.voters {
            let member = members
                .get(node)
                .ok_or_else(|| invalid("completion provenance includes a removed voter"))?;
            let pin = match (
                &member.active_certificate_sha256,
                &member.pending_certificate_sha256,
            ) {
                (None, None) => {
                    voter.peer_plaintext_loopback && voter.peer_certificate_sha256.is_none()
                }
                (a, p) => {
                    !voter.peer_plaintext_loopback
                        && voter
                            .peer_certificate_sha256
                            .as_deref()
                            .is_some_and(|s| a.as_deref() == Some(s) || p.as_deref() == Some(s))
                }
            };
            if !pin
                || member.incarnation != voter.member_incarnation
                || voter.challenge.is_nil()
                || voter.store_open_id.is_nil()
                || voter.native_database_incarnation.is_empty()
                || voter.native_database_incarnation.len() > 128
                || !sha(&voter.native_receipt_sha256)
                || !sha(&voter.native_serving_sha256)
                || voter.observed_metadata_context_id.is_nil()
                || voter.observed_metadata_term < self.metadata_term
                || voter.observed_metadata_read_index < self.metadata_read_index
                || match voter.stage {
                    NativeCompletionStage::CompletedFrozen => {
                        voter.activation_decision_sha256.is_some()
                    }
                    NativeCompletionStage::ActivatedAnchor => voter
                        .activation_decision_sha256
                        .as_deref()
                        .is_none_or(|s| !sha(s)),
                }
            {
                return Err(invalid("damaged native completion voter provenance"));
            }
        }
        Ok(())
    }
    pub(crate) fn checksum_sha256(&self) -> Result<String> {
        crate::distribution_range_consensus::validate_native_json_size(
            self,
            crate::distribution_range_consensus::MAX_NATIVE_RECOVERY_ENTRY_BYTES,
        )?;
        use sha2::Digest;
        Ok(hex::encode(sha2::Sha256::digest(serde_json::to_vec(self)?)))
    }
}
