//! Actual-open joint serving lifetime. Raw activation remains CLOSED until
//! the real owned freeze drains and this guard is explicitly armed.
use super::*;
use crate::distribution::RangeDescriptor;
use crate::distribution_range_consensus::{
    NativeJointCatalogueActivationReceipt, RangeWriteCommand, RangeWriteStore,
};
use crate::distribution_range_mutation::{
    FrozenRangeMutationPermit, RangeMutationGate, RangeServingGeneration,
};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
pub(crate) struct NativeJointActivatedOpenGuard {
    database: std::sync::Weak<()>,
    health: Arc<bicdb_page::DurabilityHealth>,
    gate: RangeMutationGate,
    generation: RangeServingGeneration,
    store_open_id: Uuid,
    range: RangeDescriptor,
    members: BTreeMap<crate::ClusterNodeId, crate::db::RangeImageBirthMember>,
    activation_sha256: String,
    armed: AtomicBool,
}
fn canonical(range: &RangeDescriptor) -> RangeDescriptor {
    let mut range = range.clone();
    range.approximate_bytes = 0;
    range.approximate_qps = 0;
    range.replicas.sort_by_key(|replica| replica.id);
    range
}
impl NativeJointActivatedOpenGuard {
    fn require_binding(&self, store_open: Uuid, activation_sha: &str) -> Result<()> {
        if self.database.upgrade().is_none()
            || self.store_open_id != store_open
            || self.activation_sha256 != activation_sha
        {
            return Err(BicDbError::Cluster(
                "joint activation actual DB/store open ended or changed".into(),
            ));
        }
        self.health
            .check()
            .map_err(crate::error::durability_error)?;
        self.generation.require_current()
    }
    pub(crate) fn require_not_armed(&self, store_open: Uuid, activation_sha: &str) -> Result<()> {
        self.require_binding(store_open, activation_sha)?;
        if self.armed.load(Ordering::Acquire) {
            return Err(BicDbError::Cluster(
                "joint open phase cannot reuse a released/armed member's old Frozen".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn require_current(&self, store_open: Uuid, activation_sha: &str) -> Result<()> {
        self.require_binding(store_open, activation_sha)?;
        if !self.armed.load(Ordering::Acquire) {
            return Err(BicDbError::Cluster(
                "joint activation still waits for actual frozen-owner release".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn require_range(
        &self,
        store_open: Uuid,
        activation_sha: &str,
        range: &RangeDescriptor,
    ) -> Result<()> {
        self.require_current(store_open, activation_sha)?;
        if canonical(range) != self.range {
            return Err(BicDbError::Cluster(
                "joint open does not authorize this exact data configuration".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn require_command(
        &self,
        store_open: Uuid,
        activation_sha: &str,
        command: &RangeWriteCommand,
    ) -> Result<()> {
        self.require_current(store_open, activation_sha)?;
        if command.range_id != self.range.id
            || command.range_epoch != self.range.epoch
            || command.leader_node_id != self.range.leader
        {
            return Err(BicDbError::Cluster(
                "joint open does not authorize an old/other command domain".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn require_read_scope(
        &self,
        store_open: Uuid,
        activation_sha: &str,
        scope: &crate::distribution_native_read::NativeRangeReadScope,
    ) -> Result<()> {
        self.require_range(store_open, activation_sha, &scope.range)?;
        if self.members.len() != scope.members.len()
            || scope.members.iter().any(|(node, member)| {
                self.members.get(node).is_none_or(|actual| {
                    actual.incarnation != member.incarnation
                        || actual.tls_certificate_sha256 != member.active_certificate_sha256
                        || actual.pending_tls_certificate_sha256
                            != member.pending_certificate_sha256
                })
            })
        {
            return Err(BicDbError::Cluster(
                "joint read scope changes actual NEW voter identity".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn arm_after_actual_release(
        &self,
        store_open: Uuid,
        activation_sha: &str,
    ) -> Result<()> {
        self.require_binding(store_open, activation_sha)?;
        // An actual gate observation, not a caller boolean or a matching
        // counter, establishes that no frozen owner/pin/callback remains.
        let drained = self.gate.observe_native_read_stamp(self.range.id)?;
        drained.require_current()?;
        self.require_binding(store_open, activation_sha)?;
        self.armed.store(true, Ordering::Release);
        Ok(())
    }
}
impl BicDb {
    pub(crate) fn native_joint_activated_open_guard(
        &self,
        store: &RangeWriteStore,
        receipt: &NativeJointCatalogueActivationReceipt,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<NativeJointActivatedOpenGuard> {
        receipt.require_current(self, store)?;
        let gate = self.range_mutation_gate();
        let generation = gate.bind_serving_generation(receipt.range().id, frozen)?;
        Ok(NativeJointActivatedOpenGuard {
            database: self.native_metadata_owner_open_lifetime(),
            health: self.native_metadata_owner_health()?,
            gate,
            generation,
            store_open_id: store.recovery_store_open_id(),
            range: canonical(receipt.range()),
            activation_sha256: receipt.activation_sha256()?,
            members: receipt.native().data().new_members().clone(),
            armed: AtomicBool::new(false),
        })
    }
}
