//! Distinct completed-decision physical materialization authority. Never an
//! ActiveFenceQQ, new Commit vote, NoData decision or serving grant.
use super::*;
use crate::CancellationToken;

#[derive(Debug)]
pub(crate) struct CompletedNativeCommandAdmission {
    quorum: NativeCompletionQuorum,
    role: RecoveryRoleAdmission,
    command: RangeWriteCommand,
    cancellation: CancellationToken,
}
impl CompletedNativeCommandAdmission {
    pub(super) fn from_actual_quorum(
        quorum: NativeCompletionQuorum,
        role: RecoveryRoleAdmission,
        command: RangeWriteCommand,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        quorum.require_recipient_role(&role)?;
        crate::distribution_range_consensus::validate_native_recovery_command_size(&command)?;
        command.validate()?;
        if !matches!(role.purpose(), MetadataRecoveryRolePurpose::NormalRecovery)
            || command.cluster_id != *role.cluster_id()
            || command.range_id != role.current_range().id
            || command.range_epoch != role.current_range().epoch
            || command.leader_node_id != role.current_range().leader
            || command.index != quorum.range_index()
        {
            return Err(invalid("completed materialization requires exact same-domain command at actual completed cut"));
        }
        let cancellation = parent.child_with_deadline(role.deadline());
        let proof = Self {
            quorum,
            role,
            command,
            cancellation,
        };
        proof.require_live()?;
        Ok(proof)
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        self.role.require_live_request()?;
        self.quorum.require_recipient_role(&self.role)
    }
    pub(crate) fn require_recipient_role(&self, role: &RecoveryRoleAdmission) -> Result<()> {
        self.require_live()?;
        self.quorum.require_recipient_role(role)?;
        if role.context_id() != self.role.context_id() {
            return Err(invalid(
                "completed command belongs to another actual recipient role acquisition",
            ));
        }
        Ok(())
    }
    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        &self.role
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        self.quorum.scope()
    }
    pub(crate) fn command(&self) -> &RangeWriteCommand {
        &self.command
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        self.quorum.logical_genesis_sha256()
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        self.quorum.logical_history_sha256()
    }
    pub(crate) fn distinct_members(&self) -> impl Iterator<Item = &ClusterNodeId> {
        self.quorum.distinct_member_ids()
    }
    pub(crate) fn decision_sha256(&self) -> Result<String> {
        self.quorum.decision_sha256()
    }
}

/// Wire DATA only. The receiver independently collects the real completed QQ.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeCompletedMaterializeRequest {
    pub historical: RecoveryHistoricalCommandRequest,
    pub challenge: Uuid,
}
impl NativeCompletedMaterializeRequest {
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        if self.challenge.is_nil() {
            return Err(invalid(
                "completed materialization has nil request challenge",
            ));
        }
        self.historical.cancellation(parent)
    }
    pub(crate) fn ready_request(&self) -> NativeReadyRequest {
        NativeReadyRequest {
            scope: self.historical.scope.clone(),
            challenge: self.challenge,
            budget_ms: self.historical.budget_ms,
        }
    }
}
