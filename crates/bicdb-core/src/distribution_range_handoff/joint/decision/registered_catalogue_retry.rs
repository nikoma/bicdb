//! Exact already-authorized catalogue re-fsync only. This borrows the SAME
//! prepared actual OLD+NEW owners; DATA cannot recreate this admission. It
//! cannot create a native decision, a vote, a NEW quorum or Serving authority.
use super::*;
use crate::distribution_consensus::HostMetadataReadObservation;
use crate::distribution_data::{
    ActualNativeJointMetadataPoint, ActualServiceNativeRegisteredJointNewEvidence,
};

pub(crate) struct NativeRegisteredDecisionCatalogueRetry<'a> {
    prepared: &'a NativeJointPreparedDecision,
    actual: &'a ActualServiceNativeRegisteredJointNewEvidence,
    role: &'a RecoveryRoleAdmission,
    own: &'a HostMetadataReadObservation,
    business: Uuid,
}
impl NativeJointPreparedDecision {
    pub(crate) fn authorize_registered_catalogue_retry<'a>(
        &'a self,
        actual: &'a ActualServiceNativeRegisteredJointNewEvidence,
        role: &'a RecoveryRoleAdmission,
        own: &'a HostMetadataReadObservation,
        business: Uuid,
    ) -> Result<NativeRegisteredDecisionCatalogueRetry<'a>> {
        self.data.validate()?;
        self.quorums.old.require_current()?;
        let selected = self
            .quorums
            .new
            .get(role.local_node_id())
            .ok_or_else(|| invalid("catalogue retry recipient is not original NEW owner"))?;
        match selected {
            NativeJointNewObservation::Registered { actual: original }
                if std::ptr::eq(original, actual) => {}
            _ => {
                return Err(invalid(
                    "catalogue retry changed original prepared Registered NEW owner",
                ))
            }
        }
        for (node, observation) in &self.quorums.new {
            if node != role.local_node_id() {
                observation.require_current()?;
            }
        }
        let plan = self.quorums.old.plan();
        plan.require_role(role)?;
        role.require_own_host_metadata(own, business)?;
        for point in self
            .data
            .old_closed
            .values()
            .chain(self.data.new_materialized.values())
        {
            if role.context_id() == point.metadata_context_id
                || role.metadata_term() < point.metadata_term
                || role.metadata_read_index() < point.metadata_read_index
            {
                return Err(invalid(
                    "catalogue retry metadata is not independently fresh after original quorums",
                ));
            }
        }
        Ok(NativeRegisteredDecisionCatalogueRetry {
            prepared: self,
            actual,
            role,
            own,
            business,
        })
    }
}
impl NativeRegisteredDecisionCatalogueRetry<'_> {
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        &self.prepared.data
    }
    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        self.role
    }
    pub(crate) fn require_native_point(
        &self,
        db: &crate::BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
        target: &crate::distribution_native_host::NativeRegisteredRemoteJointTarget,
        metadata: &ActualNativeJointMetadataPoint,
    ) -> Result<()> {
        self.actual.require_catalogue_retry_target(target)?;
        self.prepared.quorums.old.plan().require_role(self.role)?;
        self.role
            .require_own_host_metadata(self.own, self.business)?;
        self.data().validate()?;
        self.data().require_actual_native(target.native())?;
        if self.role.local_node_id() != target.native().local_node_id() {
            return Err(invalid(
                "catalogue retry role differs from actual original recipient",
            ));
        }
        metadata.require_request(self.own, self.business)?;
        metadata.require_native_store_point(db, store)
    }
}
