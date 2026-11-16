//! Distinct LOCAL-only metadata role. No generic RecoveryRoleAdmission getter
//! or conversion exists; a self final voter cannot use it for recovery/native
//! mutation fences. Actual released/native custody remains independently due.
use super::*;

pub(crate) struct NativeLocalServingPolicyRoleAdmission {
    actual: RecoveryRoleAdmission,
}
impl NativeLocalServingPolicyRoleAdmission {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_actual_witness(
        witness: MetadataRecoveryRoleObservation,
        actual_self: &MetadataReadParticipant,
        business: Uuid,
        selected: &RangeDescriptor,
        nonce: Uuid,
        challenge: Uuid,
        purpose: &MetadataRecoveryRolePurpose,
    ) -> Result<Self> {
        if !matches!(
            purpose,
            MetadataRecoveryRolePurpose::NativeJointLocalServingPolicy { .. }
        ) {
            return Err(invalid(
                "local policy requires its distinct actual self purpose",
            ));
        }
        let actual = RecoveryRoleAdmission::from_scoped_witness(
            witness,
            actual_self,
            actual_self.node_id(),
            business,
            selected.id,
            selected.epoch,
            nonce,
            challenge,
            purpose,
        )?;
        if actual.initiator_node_id() != actual.local_node_id()
            || actual.initiator_incarnation() != actual.local_member_incarnation()
            || crate::distribution_native_read::canonical_range(actual.current_range())
                != crate::distribution_native_read::canonical_range(selected)
        {
            return Err(invalid(
                "local policy cannot impersonate another recipient/domain",
            ));
        }
        Ok(Self { actual })
    }
    pub(crate) fn require_live_request(&self) -> Result<()> {
        self.actual.require_live_request()
    }
    pub(crate) fn require_own(
        &self,
        own: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        local: &ClusterNodeId,
        incarnation: u64,
    ) -> Result<()> {
        self.actual.require_own_host_metadata(own, business)?;
        if self.actual.local_node_id() != local
            || self.actual.local_member_incarnation() != incarnation
            || self.actual.initiator_node_id() != local
        {
            return Err(invalid("local policy changed actual self/incarnation"));
        }
        self.require_live_request()
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.actual.deadline()
    }
}
