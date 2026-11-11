//! Ordinary native replica materialization. A sender needs the actual origin
//! certificate; a receiver independently verifies the authenticated current
//! leader. Wire observations never become a locally collected voter quorum.
use std::time::Duration;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::distribution::{ClusterId, ClusterNodeId, RangeId};
use crate::distribution_consensus::{MetadataReadParticipant, MetadataRestoreActivation};
use crate::distribution_range_consensus::{
    validate_native_recovery_command_size, NativeRangeCommandIntent, NativeRangeCommitCertificate,
    RangeWriteCommand, RangeWriteProgress,
};
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use crate::{CancellationToken, Result};

fn invalid(message: &str) -> crate::BicDbError {
    crate::BicDbError::Cluster(format!("native ordinary apply: {message}"))
}
pub(crate) const NATIVE_ORIGIN_SCOPE_VERSION: u32 = 2;
pub(crate) const NATIVE_ORDINARY_BUDGET_MS: u64 = 5_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeOriginApplyRequest {
    pub protocol_version: u32,
    pub command: RangeWriteCommand,
    pub bootstrap_id: Uuid,
    pub restore_activation: Option<MetadataRestoreActivation>,
    pub origin_scope_sha256: String,
    pub challenge: Uuid,
    pub budget_ms: u64,
}
impl NativeOriginApplyRequest {
    /// Sending cannot be authorized by a decoded list or raw CQ label.
    pub(crate) fn from_certificate(
        command: &RangeWriteCommand,
        certificate: &NativeRangeCommitCertificate,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        validate_native_recovery_command_size(command)?;
        let intent = certificate.intent();
        if !certificate.evidence().is_origin()
            || intent.origin_scope_version() != NATIVE_ORIGIN_SCOPE_VERSION
            || intent.command_id() != command.command_id
            || intent.command_checksum_sha256() != command.checksum_sha256
            || intent.range_index() != command.index
            || certificate.certified_voters().len() < certificate.required_quorum()
        {
            return Err(invalid(
                "wire instruction requires the exact actual origin certificate",
            ));
        }
        Ok(Self {
            protocol_version: NATIVE_ORIGIN_SCOPE_VERSION,
            command: command.clone(),
            bootstrap_id: intent.bootstrap_id(),
            restore_activation: intent.restore_activation().cloned(),
            origin_scope_sha256: intent.scope_sha256().into(),
            challenge: Uuid::now_v7(),
            budget_ms: remaining_budget(parent)?,
        })
    }
    pub(crate) fn token(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        validate_native_recovery_command_size(&self.command)?;
        self.command.validate()?;
        if self.protocol_version != NATIVE_ORIGIN_SCOPE_VERSION
            || self.bootstrap_id.is_nil()
            || self.challenge.is_nil()
            || !digest(&self.origin_scope_sha256)
        {
            return Err(invalid("invalid origin instruction domain"));
        }
        request_token(self.budget_ms, parent)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeOriginStatusRequest {
    pub cluster_id: ClusterId,
    pub bootstrap_id: Uuid,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub challenge: Uuid,
    pub budget_ms: u64,
}
impl NativeOriginStatusRequest {
    pub(crate) fn token(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        if self.bootstrap_id.is_nil() || self.range_epoch == 0 || self.challenge.is_nil() {
            return Err(invalid("invalid ordinary native status request"));
        }
        request_token(self.budget_ms, parent)
    }
}
fn request_token(ms: u64, parent: &CancellationToken) -> Result<CancellationToken> {
    if ms == 0 || ms > NATIVE_ORDINARY_BUDGET_MS {
        return Err(invalid("ordinary native request budget must be 1..=5000ms"));
    }
    let token = parent.child_with_deadline(std::time::Instant::now() + Duration::from_millis(ms));
    token.check()?;
    Ok(token)
}
pub(crate) fn remaining_budget(parent: &CancellationToken) -> Result<u64> {
    parent.check()?;
    let ms = parent
        .deadline()
        .map_or(NATIVE_ORDINARY_BUDGET_MS, |deadline| {
            deadline
                .saturating_duration_since(std::time::Instant::now())
                .as_millis()
                .min(u128::from(NATIVE_ORDINARY_BUDGET_MS)) as u64
        });
    if ms == 0 {
        return Err(invalid("ordinary native parent budget expired"));
    }
    Ok(ms)
}
fn digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Current identity/command permission, NOT a quorum or a floor. Only the
/// receiver can combine its independently fresh role and actual incoming peer.
#[derive(Debug)]
pub(crate) struct NativeLeaderInstructionAdmission {
    role: RecoveryRoleAdmission,
    peer_node_id: ClusterNodeId,
    peer_incarnation: u64,
    peer_pin: Option<String>,
    peer_plaintext: bool,
    request: NativeOriginApplyRequest,
    intent: NativeRangeCommandIntent,
}
impl NativeLeaderInstructionAdmission {
    pub(crate) fn from_actual_peer(
        role: RecoveryRoleAdmission,
        peer: &MetadataReadParticipant,
        request: NativeOriginApplyRequest,
    ) -> Result<Self> {
        role.require_live_request()?;
        if peer.node_id() != role.initiator_node_id()
            || peer.member_incarnation() != role.initiator_incarnation()
            || peer.node_id() != &role.current_range().leader
            || request.command.leader_node_id != *peer.node_id()
            || request.bootstrap_id != role.bootstrap_id()
            || request.restore_activation.as_ref() != role.restore_activation()
        {
            return Err(invalid(
                "incoming ordinary instruction is not the fresh current leader/domain",
            ));
        }
        let member = role
            .member_credentials()
            .get(peer.node_id())
            .ok_or_else(|| invalid("current leader is not a current data voter"))?;
        match (
            &member.active_certificate_sha256,
            &member.pending_certificate_sha256,
        ) {
            (None, None) if peer.plaintext_loopback() => {}
            (a, p)
                if peer
                    .certificate_sha256()
                    .is_some_and(|sha| a.as_deref() == Some(sha) || p.as_deref() == Some(sha)) => {}
            _ => {
                return Err(invalid(
                    "actual ordinary caller certificate is no longer authorized",
                ))
            }
        }
        let intent = NativeRangeCommandIntent::from_authenticated_leader(&request.command, &role)?;
        if intent.scope_sha256() != request.origin_scope_sha256 {
            return Err(invalid(
                "origin instruction semantic scope differs from fresh metadata",
            ));
        }
        Ok(Self {
            role,
            peer_node_id: peer.node_id().clone(),
            peer_incarnation: peer.member_incarnation(),
            peer_pin: peer.certificate_sha256().map(str::to_owned),
            peer_plaintext: peer.plaintext_loopback(),
            request,
            intent,
        })
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.role.require_live_request()
    }
    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        &self.role
    }
    pub(crate) fn peer_node_id(&self) -> &ClusterNodeId {
        &self.peer_node_id
    }
    pub(crate) fn command(&self) -> &RangeWriteCommand {
        &self.request.command
    }
    pub(crate) fn intent(&self) -> &NativeRangeCommandIntent {
        &self.intent
    }
    pub(crate) fn challenge(&self) -> Uuid {
        self.request.challenge
    }
    pub(crate) fn materialization_evidence(
        &self,
    ) -> crate::distribution_range_consensus::NativeRangeCertificateEvidence {
        crate::distribution_range_consensus::NativeRangeCertificateEvidence::AuthenticatedLeaderInstruction {
            leader_node_id: self.peer_node_id.clone(), leader_incarnation: self.peer_incarnation,
            peer_certificate_sha256: self.peer_pin.clone(), plaintext_loopback: self.peer_plaintext,
            bootstrap_id: self.role.bootstrap_id(), range_epoch: self.role.current_range().epoch,
            metadata_context_id: self.role.context_id(), metadata_term: self.role.metadata_term(), metadata_read_index: self.role.metadata_read_index(),
            restore_activation: self.role.restore_activation().cloned(), scope_sha256: self.intent.scope_sha256().into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeReplicaProgressReply {
    pub bootstrap_id: Uuid,
    pub node_id: ClusterNodeId,
    pub member_incarnation: u64,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub challenge: Uuid,
    pub progress: RangeWriteProgress,
    // These come only from a verified native lineage; unknown/pending state
    // refuses the observation. Zero therefore means actual verified genesis.
    pub native_index: u64,
    pub native_receipt_sha256: String,
    pub logical_genesis_sha256: String,
    pub logical_history_sha256: String,
    pub command_index: Option<u64>,
    pub command_checksum_sha256: Option<String>,
}
/// Authenticated materialization/status observation; never a QQ/ready proof.
#[derive(Debug)]
pub(crate) struct NativeReplicaApplyObservation {
    reply: NativeReplicaProgressReply,
}
impl NativeReplicaApplyObservation {
    pub(crate) fn from_native_materialization(
        physical: &crate::db::NativeReplicaMaterialization,
        progress: RangeWriteProgress,
        challenge: Uuid,
    ) -> Result<Self> {
        Self::checked(NativeReplicaProgressReply {
            bootstrap_id: physical.bootstrap_id(),
            node_id: physical.local_node_id().clone(),
            member_incarnation: physical.local_member_incarnation(),
            range_id: physical.range().id,
            range_epoch: physical.range().epoch,
            challenge,
            progress,
            native_index: physical.native_index(),
            native_receipt_sha256: physical.native_receipt_sha256().into(),
            logical_genesis_sha256: physical.logical_genesis_sha256().into(),
            logical_history_sha256: physical.logical_history_sha256().into(),
            command_index: physical.command_index(),
            command_checksum_sha256: physical.command_checksum_sha256().map(str::to_owned),
        })
    }
    pub(crate) fn from_transport(
        actual: crate::distribution_transport::AuthenticatedNativeReplicaReply,
    ) -> Result<Self> {
        Self::checked(actual.into_reply())
    }
    fn checked(reply: NativeReplicaProgressReply) -> Result<Self> {
        if reply.bootstrap_id.is_nil()
            || reply.challenge.is_nil()
            || reply.member_incarnation == 0
            || reply.range_epoch == 0
            || reply.progress.node_id != reply.node_id
            || reply.progress.range_id != reply.range_id
            || !digest(&reply.native_receipt_sha256)
            || !digest(&reply.logical_genesis_sha256)
            || !digest(&reply.logical_history_sha256)
            || (reply.native_index == 0
                && reply.logical_genesis_sha256 != reply.logical_history_sha256)
            || reply
                .command_index
                .zip(reply.command_checksum_sha256.as_deref())
                .is_some_and(|(i, h)| i == 0 || i > reply.native_index || !digest(h))
            || reply.command_index.is_some() != reply.command_checksum_sha256.is_some()
        {
            return Err(invalid("malformed actual native replica progress"));
        }
        Ok(Self { reply })
    }
    pub(crate) fn reply(&self) -> &NativeReplicaProgressReply {
        &self.reply
    }
    pub(crate) fn into_reply(self) -> NativeReplicaProgressReply {
        self.reply
    }
}
