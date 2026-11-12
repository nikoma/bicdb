//! Opaque final HOT leader admission. Actual open state alone is insufficient:
//! retain fresh enrolled dataQQ and independently fresh final range MQ.
use super::*;
use crate::distribution_range_consensus::{
    NativeHotRecoveredOpen, RangeWriteCommand, RecoveryFenceScope,
};
use crate::distribution_range_recovery::RecoveryRoleAdmission;
pub(crate) struct NativeHotRecoveredReadQuorum {
    enrolled: EnrolledNativeReadQuorum,
    open: NativeHotRecoveredOpen,
}
impl NativeReadQuorumCandidate {
    pub(crate) fn into_hot_recovered_open(
        self,
        service: &crate::ClusterDataNodeService,
        fresh: &RecoveryRoleAdmission,
        scope: &RecoveryFenceScope,
        command: &RangeWriteCommand,
        parent: &CancellationToken,
    ) -> Result<NativeHotRecoveredReadQuorum> {
        scope.require_role(fresh)?;
        if self.scope().range != canonical_range(fresh.current_range())
            || self.scope().members != *fresh.member_credentials()
            || self.scope().restore_activation.as_ref() != fresh.restore_activation()
            || Some(self.scope().schema_sha256.as_str()) != fresh.expected_schema_sha256()
        {
            return Err(invalid(
                "hot repaired readQQ differs from independently fresh recovery role",
            ));
        }
        let open = service.observe_hot_recovered_open(fresh, scope, command, parent)?;
        if !open.matches_point(self.point()) {
            return Err(invalid(
                "hot readQQ does not match actual opened native history",
            ));
        }
        let enrolled = self.check_enrolled_voter_births()?;
        for node in enrolled.candidate.scope().members.keys() {
            let topology = enrolled
                .candidate
                .round
                .metadata
                .committed_topology(enrolled.candidate.round.business)?;
            if topology.nodes[node]
                .native_store_birth_v1
                .as_ref()
                .is_none_or(|birth| {
                    birth.abort_protocol_version() != 1 || birth.writer_enforcement_version() != 1
                })
            {
                return Err(invalid(
                    "hot recovery requires from-birth enforced current-voter custody",
                ));
            }
        }
        Ok(NativeHotRecoveredReadQuorum { enrolled, open })
    }
}
impl NativeHotRecoveredReadQuorum {
    pub(crate) fn require_current(
        &self,
        service: &crate::ClusterDataNodeService,
        final_metadata: &MetadataReadObservation,
        business: Uuid,
    ) -> Result<()> {
        let c = &self.enrolled.candidate;
        c.require_lifetime()?;
        let scope = scope_from_metadata(final_metadata, business, &c.round.local_node)?;
        if final_metadata.context_id() == c.round.metadata.context_id()
            || final_metadata.term() < c.round.metadata.term()
            || final_metadata.read_index() < c.round.metadata.read_index()
            || scope != *c.scope()
        {
            return Err(invalid(
                "hot recovered metadata changed or final MQ was reused",
            ));
        }
        self.open.require_metadata(final_metadata, business)?;
        let first_topology = c.round.metadata.committed_topology(c.round.business)?;
        let final_topology = final_metadata.committed_topology(business)?;
        for node in scope.members.keys() {
            if first_topology.nodes[node].native_store_birth_v1
                != final_topology.nodes[node].native_store_birth_v1
            {
                return Err(invalid(
                    "hot recovery final current-voter birth enrollment changed",
                ));
            }
        }
        let local = c.round.observations[&c.round.local_node]
            .local()
            .ok_or_else(|| invalid("hot recovery omitted actual local observation"))?;
        local.require_current(service)?;
        service.revalidate_hot_recovered_open(&self.open, &scope.range)?;
        local.require_current(service)?;
        final_metadata.require_live_request(business)?;
        c.require_lifetime()
    }
}
