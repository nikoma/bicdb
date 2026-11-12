//! Opaque actual old-member closure. It observes a permanent fence; it cannot
//! be deserialized, retagged, or used as a new-member/materialization vote.
use super::*;
use crate::distribution_data::{ActualServiceNativeJointOldEvidence, ClusterDataNodeService};
use crate::distribution_range_mutation::{FrozenRangeMutationPermit, SourceCaptureReadPin};
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use parking_lot::Mutex;
use std::sync::Arc;

pub(crate) struct NativeJointOldObservation {
    plan: Arc<NativeJointHandoffPlan>,
    role: RecoveryRoleAdmission,
    native: crate::db::NativeJointOldCloseReceipt,
    promise: crate::distribution_range_consensus::NativeJointOldPromiseReceipt,
    frozen: Arc<FrozenRangeMutationPermit>,
    pin: Arc<SourceCaptureReadPin>,
    service: Arc<Mutex<ClusterDataNodeService>>,
}
impl NativeJointOldObservation {
    pub(crate) fn from_actual_service(
        evidence: ActualServiceNativeJointOldEvidence,
        service: Arc<Mutex<ClusterDataNodeService>>,
    ) -> Result<Self> {
        {
            let actual = service
                .try_lock()
                .ok_or_else(|| invalid("old closure service busy during binding"))?;
            evidence.require_current(&actual)?;
        }
        let (plan, role, native, promise, frozen, pin) = evidence.into_parts();
        let result = Self {
            plan,
            role,
            native,
            promise,
            frozen,
            pin,
            service,
        };
        result.require_current()?;
        Ok(result)
    }
    pub(crate) fn require_current(&self) -> Result<()> {
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("old closure service busy during revalidation"))?;
        service.require_native_joint_old_observation(
            &self.plan,
            &self.native,
            &self.promise,
            &self.frozen,
            &self.pin,
        )
    }
    pub(crate) fn node_id(&self) -> &ClusterNodeId {
        self.native.native_admission().local_node_id()
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.native.range_index()
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        self.native.logical_genesis_sha256()
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        self.native.logical_history_sha256()
    }
    pub(crate) fn receipt_sha256(&self) -> &str {
        self.native.checksum_sha256()
    }
}
impl NativeJointOldObservation {
    pub(super) fn require_issuance_current(&self) -> Result<()> {
        self.plan.require_live()?;
        self.role.require_live_request()?;
        self.require_current()
    }
    pub(super) fn scope_sha256(&self) -> Result<String> {
        self.plan.checksum_sha256()
    }
    pub(super) fn require_ancestor(&self, index: u64, h0: &str, h: &str) -> Result<()> {
        self.service
            .try_lock()
            .ok_or_else(|| invalid("old selected source busy"))?
            .require_native_joint_old_ancestor(
                &self.plan,
                &self.native,
                &self.promise,
                &self.frozen,
                &self.pin,
                index,
                h0,
                h,
                None,
            )
    }
    pub(super) fn command_at(&self, index: u64) -> Result<Option<String>> {
        let (_, command) = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("old selected source busy"))?
            .native_joint_old_history_point(
                &self.plan,
                &self.native,
                &self.promise,
                &self.frozen,
                &self.pin,
                index,
            )?;
        Ok(command)
    }
    pub(super) fn prepare_capture(
        &self,
        cut: CertifiedRangeImageCut,
    ) -> Result<crate::distribution_data::ActualServiceNativeJointCaptureInputs> {
        self.service
            .try_lock()
            .ok_or_else(|| invalid("old selected source busy before capture"))?
            .prepare_native_joint_capture_inputs(
                &self.plan,
                &self.native,
                &self.promise,
                self.frozen.clone(),
                self.pin.clone(),
                cut,
            )
    }
    pub(super) fn metadata_anchor(&self) -> (u64, u64, Uuid) {
        (
            self.role.metadata_term(),
            self.role.metadata_read_index(),
            self.role.context_id(),
        )
    }
}

impl NativeJointOldObservation {
    pub(super) fn materialized_facts(&self) -> super::materialized::NativeJointMaterializedFacts {
        let (term, read_index, context) = self.metadata_anchor();
        super::materialized::NativeJointMaterializedFacts {
            scope_sha256: self.native.scope_sha256().into(),
            node_id: self.node_id().clone(),
            birth_id: self.native.native_admission().birth_id(),
            birth_sha256: self.native.native_admission().birth_sha256().into(),
            native_receipt_sha256: self.native.checksum_sha256().into(),
            range_index: self.range_index(),
            logical_genesis_sha256: self.logical_genesis_sha256().into(),
            logical_history_sha256: self.logical_history_sha256().into(),
            actual_local_visibility_sequence: self.native.local_visibility_sequence(),
            metadata_term: term,
            metadata_read_index: read_index,
            metadata_context_id: context,
        }
    }
}

impl NativeJointOldObservation {
    pub(super) fn require_new_member_role(&self, role: &RecoveryRoleAdmission) -> Result<()> {
        self.plan.require_role(role)?;
        let (term, index, _) = self.metadata_anchor();
        if role.local_node_id() != self.node_id()
            || role.local_member_incarnation()
                != self.native.native_admission().local_member_incarnation()
            || role.metadata_term() < term
            || role.metadata_read_index() < index
            || !self.plan.new_members().contains_key(self.node_id())
        {
            return Err(invalid(
                "new survivor role differs from actual closed member/current scope",
            ));
        }
        self.require_current()
    }
}
