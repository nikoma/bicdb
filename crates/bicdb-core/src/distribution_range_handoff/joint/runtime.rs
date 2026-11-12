//! Local owner for actual handoff drains. Failed/expired operations retain the
//! freeze and permanent refusals. Only a later actual activation/cleanup proof
//! may release ownership; there is no TTL release or mutable reset method.
use super::*;
use crate::distribution_data::{ActualServiceNativeJointConstraints, ClusterDataNodeService};
use crate::distribution_range_mutation::{FrozenRangeMutationPermit, RangeMutationGate};
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const MAX_LOCAL_JOINT_SESSIONS: usize = 64;
const MAX_LOCAL_JOINT_OPERATIONS: usize = 4;
struct HeldFreeze {
    scope_sha256: String,
    session_id: Uuid,
    nonce: Uuid,
    frozen: Arc<FrozenRangeMutationPermit>,
}
#[derive(Default)]
struct Sessions {
    frozen: BTreeMap<RangeId, HeldFreeze>,
    active: BTreeSet<RangeId>,
}
pub(crate) struct NativeJointHandoffRuntime {
    service: Arc<Mutex<ClusterDataNodeService>>,
    gate: RangeMutationGate,
    node: ClusterNodeId,
    sessions: Arc<Mutex<Sessions>>,
}
struct Operation {
    sessions: Arc<Mutex<Sessions>>,
    range: RangeId,
}
impl Drop for Operation {
    fn drop(&mut self) {
        self.sessions.lock().active.remove(&self.range);
    }
}
impl NativeJointHandoffRuntime {
    pub(crate) fn new(service: Arc<Mutex<ClusterDataNodeService>>) -> Result<Self> {
        let (gate, node) = {
            let actual = service
                .try_lock()
                .ok_or_else(|| invalid("joint service busy at runtime construction"))?;
            (actual.range_mutation_gate(), actual.node_id().clone())
        };
        Ok(Self {
            service,
            gate,
            node,
            sessions: Arc::new(Mutex::new(Sessions::default())),
        })
    }
    fn operation(&self, range: RangeId) -> Result<Operation> {
        let mut sessions = self
            .sessions
            .try_lock()
            .ok_or_else(|| invalid("joint runtime contexts busy"))?;
        if sessions.active.len() >= MAX_LOCAL_JOINT_OPERATIONS || !sessions.active.insert(range) {
            return Err(invalid("joint runtime operation capacity/range busy"));
        }
        Ok(Operation {
            sessions: self.sessions.clone(),
            range,
        })
    }
    fn freeze(
        &self,
        plan: &NativeJointHandoffPlan,
        role: &RecoveryRoleAdmission,
        parent: &crate::CancellationToken,
    ) -> Result<Arc<FrozenRangeMutationPermit>> {
        parent.check()?;
        plan.require_role(role)?;
        if role.local_node_id() != &self.node {
            return Err(invalid("joint role targets another actual runtime"));
        }
        let range = plan.old_range().id;
        let scope = plan.checksum_sha256()?;
        {
            let sessions = self
                .sessions
                .try_lock()
                .ok_or_else(|| invalid("joint sessions busy"))?;
            if let Some(existing) = sessions.frozen.get(&range) {
                if existing.scope_sha256 != scope
                    || existing.session_id != plan.promise().session_id
                    || existing.nonce != plan.promise().nonce
                    || existing.frozen.owner() != existing.session_id
                {
                    return Err(invalid(
                        "joint runtime retains a different permanent session",
                    ));
                }
                self.gate
                    .validate_quiescent_frozen(range, &existing.frozen)?;
                return Ok(existing.frozen.clone());
            }
            if sessions.frozen.len() >= MAX_LOCAL_JOINT_SESSIONS {
                return Err(invalid("joint runtime retained-session capacity"));
            }
        }
        // No database, service or session-map latch is held across this drain.
        // Existing admitted WAL/native callbacks must release their owned lease.
        let deadline = parent
            .deadline()
            .map_or(role.deadline(), |d| d.min(role.deadline()));
        let token = parent.child_with_deadline(deadline);
        let remaining = deadline
            .checked_duration_since(std::time::Instant::now())
            .ok_or_else(|| invalid("joint drain original deadline expired"))?;
        let frozen =
            Arc::new(
                self.gate
                    .freeze(range, plan.promise().session_id, remaining, &token)?,
            );
        // Record ownership BEFORE the next cancellation/role check and BEFORE
        // any durable operation. A later error leaves this real freeze owned.
        let mut sessions = self.sessions.lock();
        if sessions.frozen.contains_key(&range) || sessions.frozen.len() >= MAX_LOCAL_JOINT_SESSIONS
        {
            return Err(invalid(
                "joint runtime concurrent freeze owner/capacity changed",
            ));
        }
        sessions.frozen.insert(
            range,
            HeldFreeze {
                scope_sha256: scope,
                session_id: plan.promise().session_id,
                nonce: plan.promise().nonce,
                frozen: frozen.clone(),
            },
        );
        drop(sessions);
        token.check()?;
        plan.require_role(role)?;
        Ok(frozen)
    }
    pub(crate) fn observe_constraints(
        &self,
        plan: Arc<NativeJointHandoffPlan>,
        role: RecoveryRoleAdmission,
        parent: &crate::CancellationToken,
    ) -> Result<ActualServiceNativeJointConstraints> {
        let _operation = self.operation(plan.old_range().id)?;
        let frozen = self.freeze(&plan, &role, parent)?;
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("joint constraints actual service busy"))?;
        service.observe_native_joint_constraints(plan, &role, frozen, parent)
    }
    pub(crate) fn close_old(
        &self,
        plan: Arc<NativeJointHandoffPlan>,
        role: RecoveryRoleAdmission,
        parent: &crate::CancellationToken,
    ) -> Result<NativeJointOldObservation> {
        let _operation = self.operation(plan.old_range().id)?;
        let frozen = self.freeze(&plan, &role, parent)?;
        let actual = {
            let mut service = self
                .service
                .try_lock()
                .ok_or_else(|| invalid("joint closure actual service busy"))?;
            service.close_native_joint_old(plan, role, frozen, parent)?
        };
        NativeJointOldObservation::from_actual_service(actual, self.service.clone())
    }
}
