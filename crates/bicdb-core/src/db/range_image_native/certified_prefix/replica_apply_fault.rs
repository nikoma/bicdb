//! One real ordinary receiver instruction can be lost after its actual CQ,
//! before creating a local row transaction. Observations are DATA, not grants.
use super::*;
use crate::distribution_native_apply::NativeLeaderInstructionAdmission;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{OnceLock, Weak};

#[derive(Debug)]
pub(super) struct NativeReplicaApplyFault {
    incarnation: String,
    open: Weak<()>,
    range: RangeId,
    record_id: String,
    until: std::time::Instant,
    armed: AtomicBool,
    actual: OnceLock<serde_json::Value>,
}

/// Only this owner controls the bounded arm; no observation can re-arm it.
#[derive(Debug)]
pub(crate) struct NativeReplicaApplyFaultArm {
    fault: Arc<NativeReplicaApplyFault>,
    registry: Weak<Mutex<NativeRangeScopeRegistry>>,
}
impl NativeReplicaApplyFaultArm {
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "scope":"cfg(test) one actual ordinary receiver CQ/pre-WAL refusal; DATA only",
            "requested_open":self.fault.incarnation,
            "requested_range":self.fault.range,
            "requested_record_id":self.fault.record_id,
            "actual_open_alive":self.fault.open.upgrade().is_some(),
            "arm_live":self.fault.armed.load(Ordering::Acquire)
                && std::time::Instant::now()<self.fault.until,
            "actual_cut":self.fault.actual.get(),
        })
    }
}
impl Drop for NativeReplicaApplyFaultArm {
    fn drop(&mut self) {
        self.fault.armed.store(false, Ordering::Release);
        if let Some(registry) = self.registry.upgrade() {
            if let Some(mut registry) = registry.try_lock() {
                if registry
                    .replica_apply_fault
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, &self.fault))
                {
                    registry.replica_apply_fault = None;
                }
            }
        }
    }
}
impl BicDb {
    pub(crate) fn test_arm_native_replica_cq_before_wal(
        &self,
        range: RangeId,
        namespace: &str,
        key: &str,
    ) -> Result<NativeReplicaApplyFaultArm> {
        let mut registry = self
            .native_range_command_scopes
            .try_lock()
            .ok_or_else(|| invalid("test replica fault registry busy"))?;
        if registry
            .replica_apply_fault
            .as_ref()
            .is_some_and(|f| f.armed.load(Ordering::Acquire) && std::time::Instant::now() < f.until)
        {
            return Err(invalid("test replica fault already armed"));
        }
        let fault = Arc::new(NativeReplicaApplyFault {
            incarnation: self.incarnation.clone(),
            open: self.native_metadata_owner_open_lifetime(),
            range,
            record_id: crate::cluster_event_record_id(namespace, key)?,
            until: std::time::Instant::now() + std::time::Duration::from_secs(30),
            armed: AtomicBool::new(true),
            actual: OnceLock::new(),
        });
        registry.replica_apply_fault = Some(Arc::clone(&fault));
        Ok(NativeReplicaApplyFaultArm {
            fault,
            registry: Arc::downgrade(&self.native_range_command_scopes),
        })
    }
    pub(super) fn test_refuse_native_replica_before_apply(
        &self,
        admission: &NativeLeaderInstructionAdmission,
        actual: &SourceRangeImageLineage,
    ) -> bool {
        let Some(fault) = self
            .native_range_command_scopes
            .try_lock()
            .and_then(|registry| registry.replica_apply_fault.clone())
        else {
            return false;
        };
        let command = admission.command();
        if fault.incarnation != self.incarnation
            || !Weak::ptr_eq(&fault.open, &self.native_metadata_owner_open_lifetime())
            || fault.open.upgrade().is_none()
            || std::time::Instant::now() >= fault.until
            || command.range_id != fault.range
            || command.mutations.len() != 1
            || command.mutations[0].collection != crate::CLUSTER_EVENTS_COLLECTION
            || command.mutations[0].record_id != fault.record_id
            || fault
                .armed
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return false;
        }
        // All normal admission/actual local CQ/config/base checks precede
        // this call. No JSON observation is consulted by the engine.
        let _ = fault.actual.set(serde_json::json!({
            "path":"apply_native_origin_instruction: after actual CQ/config/base, before begin_transaction",
            "actual_open":self.incarnation,
            "actual_node":actual.local_node_id(),
            "range":command.range_id,"epoch":command.range_epoch,
            "command_id":command.command_id,
            "command_sha256":command.checksum_sha256,
            "mutations_sha256":admission.intent().mutations_sha256(),
            "command_index":command.index,
            "native_index_before":actual.range_index(),
            "outcome":"injected actual ordinary CQ before local WAL",
        }));
        true
    }
}
