//! Local owner for actual handoff drains. Before activation, failed/expired
//! operations retain the freeze and permanent refusals. After actual activation
//! authorizes release, retain its original receipt for bounded same-open retry;
//! there is no TTL release, replacement freeze or mutable reset method.
use super::*;
use crate::distribution_data::{ActualServiceNativeJointConstraints, ClusterDataNodeService};
use crate::distribution_range_mutation::{FrozenRangeMutationPermit, RangeMutationGate};
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const MAX_LOCAL_JOINT_SESSIONS: usize = 64;
const MAX_LOCAL_JOINT_OPERATIONS: usize = 4;
enum RetainedExclusion {
    Frozen(Arc<FrozenRangeMutationPermit>),
    // The real owner has dropped; retain the ORIGINAL actual receipt. The
    // original API still enforces its TTL; a distinct fresh continuation uses
    // only its immutable identity. This is never Frozen or an armed-open proof.
    Released(Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>),
}
struct RetainedSession {
    scope_sha256: String,
    session_id: Uuid,
    nonce: Uuid,
    exclusion: RetainedExclusion,
}
#[derive(Default)]
struct Sessions {
    retained: BTreeMap<RangeId, RetainedSession>,
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
    pub(crate) fn require_same_service(
        &self,
        service: &Arc<Mutex<ClusterDataNodeService>>,
    ) -> Result<()> {
        if !Arc::ptr_eq(&self.service, service) {
            return Err(invalid(
                "joint continuation runtime belongs to another actual service",
            ));
        }
        Ok(())
    }
    pub(crate) fn with_current_release_owner<T>(
        &self,
        native: &crate::db::NativeJointServingBridgeReceipt,
        operation: impl for<'a> FnOnce(
            &Arc<Mutex<ClusterDataNodeService>>,
            crate::distribution_data::NativeJointReleaseOwnerView<'a>,
        ) -> Result<T>,
    ) -> Result<T> {
        let data = native.data();
        let range = data.old_range().id;
        if native.local_node_id() != &self.node || !data.new_members().contains_key(&self.node) {
            return Err(invalid(
                "joint continuation actual survivor identity changed",
            ));
        }
        let scope = data.plan_sha256()?;
        let sessions = self
            .sessions
            .try_lock()
            .ok_or_else(|| invalid("joint continuation sessions busy"))?;
        let held = sessions
            .retained
            .get(&range)
            .ok_or_else(|| invalid("joint continuation has no original retained owner"))?;
        if held.scope_sha256 != scope
            || held.session_id != data.promise().session_id
            || held.nonce != data.promise().nonce
        {
            return Err(invalid(
                "joint continuation changed retained exact session/nonce",
            ));
        }
        match &held.exclusion {
            RetainedExclusion::Frozen(frozen) => {
                self.gate.validate_quiescent_frozen(range, frozen)?;
                operation(
                    &self.service,
                    crate::distribution_data::NativeJointReleaseOwnerView::Frozen(frozen),
                )
            }
            RetainedExclusion::Released(original) => operation(
                &self.service,
                crate::distribution_data::NativeJointReleaseOwnerView::Dropped(original),
            ),
        }
    }
    /// Release only this actual NEW member's exact permanent-session owner,
    /// after native+catalogue activation. Old-source ownership is retained for
    /// proof-authorized cleanup. There is no timeout or caller reset path.
    pub(crate) fn release_activated_new_member(
        &self,
        receipt: Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>,
    ) -> Result<()> {
        let range = receipt.range().id;
        let _operation = self.operation(range)?;
        receipt.require_live()?;
        let data = receipt.native().data();
        if receipt.native().local_node_id() != &self.node
            || !data.new_members().contains_key(&self.node)
        {
            return Err(invalid(
                "joint freeze release is not this actual NEW member",
            ));
        }
        let scope_sha256 = data.plan_sha256()?;
        let activation_sha256 = receipt.activation_sha256()?;
        {
            let service = self
                .service
                .try_lock()
                .ok_or_else(|| invalid("joint release service busy"))?;
            service.require_local_native_joint_activation(&receipt)?;
        }
        let mut sessions = self
            .sessions
            .try_lock()
            .ok_or_else(|| invalid("joint release sessions busy"))?;
        let held = sessions
            .retained
            .get_mut(&range)
            .ok_or_else(|| invalid("joint release has no retained actual session"))?;
        if held.session_id != data.promise().session_id
            || held.nonce != data.promise().nonce
            || held.scope_sha256 != scope_sha256
        {
            return Err(invalid(
                "joint release changed its retained permanent session",
            ));
        }
        let (original, dropped_owner) = match &held.exclusion {
            RetainedExclusion::Frozen(frozen) => {
                if Arc::strong_count(frozen) != 1 {
                    return Err(invalid(
                        "joint release waits for exact session/readers/old decision owners to drain",
                    ));
                }
                self.gate.validate_quiescent_frozen(range, frozen)?;
                // Publish bounded retry ownership BEFORE dropping the only real
                // Frozen. Every later failure retains this original proof/TTL.
                let dropped_owner = std::mem::replace(
                    &mut held.exclusion,
                    RetainedExclusion::Released(receipt.clone()),
                );
                (receipt.clone(), Some(dropped_owner))
            }
            RetainedExclusion::Released(original) => {
                original.require_live()?;
                if original.store_open_id() != receipt.store_open_id()
                    || original.activation_sha256()? != activation_sha256
                    || original.native_bridge_sha256() != receipt.native_bridge_sha256()
                {
                    return Err(invalid(
                        "joint release retry changed its actual activation/open/receipt",
                    ));
                }
                // Use the retained ORIGINAL receipt for the final same-open
                // check and arm, never the retry's potentially later budget.
                (original.clone(), None)
            }
        };
        drop(sessions);
        drop(dropped_owner);
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("joint arm service busy after owner release; retry retained"))?;
        original.require_live()?;
        service.arm_local_native_joint_open(&original)
    }
    /// DISTINCT fresh all-member continuation. Original receipt is retained
    /// as immutable retry custody, never as a renewed deadline permission.
    pub(crate) fn release_with_fresh_phase(
        &self,
        admission: &crate::distribution_range_handoff::NativeJointReleaseAdmission,
        original: Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>,
    ) -> Result<()> {
        admission.require_recipient(&self.service, original.native())?;
        let data = admission.native().data();
        let range = data.old_range().id;
        let _operation = self.operation(range)?;
        let mut sessions = self
            .sessions
            .try_lock()
            .ok_or_else(|| invalid("fresh joint release sessions busy"))?;
        let held = sessions
            .retained
            .get_mut(&range)
            .ok_or_else(|| invalid("fresh joint release lost original owner"))?;
        if held.scope_sha256 != data.plan_sha256()?
            || held.session_id != data.promise().session_id
            || held.nonce != data.promise().nonce
        {
            return Err(invalid(
                "fresh joint release changed original session/nonce",
            ));
        }
        let (retained, dropped) = match &held.exclusion {
            RetainedExclusion::Frozen(frozen) => {
                if Arc::strong_count(frozen) != 1 {
                    return Err(invalid(
                        "fresh joint release waits for actual old preparation/publication owners",
                    ));
                }
                let service = self
                    .service
                    .try_lock()
                    .ok_or_else(|| invalid("fresh joint release local service busy"))?;
                service.require_fresh_native_joint_release(
                    admission,
                    crate::distribution_data::NativeJointReleaseOwnerView::Frozen(frozen),
                )?;
                admission.require_live()?;
                let dropped = std::mem::replace(
                    &mut held.exclusion,
                    RetainedExclusion::Released(original.clone()),
                );
                (original, Some(dropped))
            }
            RetainedExclusion::Released(actual) => {
                if actual.store_open_id() != original.store_open_id()
                    || actual.activation_sha256()? != original.activation_sha256()?
                    || actual.native_bridge_sha256() != original.native_bridge_sha256()
                {
                    return Err(invalid(
                        "fresh joint retry changed original protected activation/open",
                    ));
                }
                (actual.clone(), None)
            }
        };
        drop(sessions);
        // No fallible operation can discard retry custody after the real drop.
        drop(dropped);
        let service = self.service.try_lock().ok_or_else(|| {
            invalid("fresh joint release dropped owner; same-open retry retained")
        })?;
        admission.require_live()?;
        service.arm_fresh_native_joint_release(admission, &retained)
    }
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
            if let Some(existing) = sessions.retained.get(&range) {
                if existing.scope_sha256 != scope
                    || existing.session_id != plan.promise().session_id
                    || existing.nonce != plan.promise().nonce
                {
                    return Err(invalid(
                        "joint runtime retains a different permanent session",
                    ));
                }
                let RetainedExclusion::Frozen(frozen) = &existing.exclusion else {
                    return Err(invalid(
                        "joint session already released its real owner; exact activation retry only",
                    ));
                };
                if frozen.owner() != existing.session_id {
                    return Err(invalid("joint runtime frozen owner changed"));
                }
                self.gate.validate_quiescent_frozen(range, frozen)?;
                return Ok(frozen.clone());
            }
            if sessions.retained.len() >= MAX_LOCAL_JOINT_SESSIONS {
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
        if sessions.retained.contains_key(&range)
            || sessions.retained.len() >= MAX_LOCAL_JOINT_SESSIONS
        {
            return Err(invalid(
                "joint runtime concurrent freeze owner/capacity changed",
            ));
        }
        sessions.retained.insert(
            range,
            RetainedSession {
                scope_sha256: scope,
                session_id: plan.promise().session_id,
                nonce: plan.promise().nonce,
                exclusion: RetainedExclusion::Frozen(frozen.clone()),
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
        self.close_old_with_role_owner(plan, Arc::new(role), parent)
    }
    pub(crate) fn close_old_with_role_owner(
        &self,
        plan: Arc<NativeJointHandoffPlan>,
        role: Arc<RecoveryRoleAdmission>,
        parent: &crate::CancellationToken,
    ) -> Result<NativeJointOldObservation> {
        let _operation = self.operation(plan.old_range().id)?;
        let frozen = self.freeze(&plan, &role, parent)?;
        let actual = {
            let mut service = self
                .service
                .try_lock()
                .ok_or_else(|| invalid("joint closure actual service busy"))?;
            service.close_native_joint_old_with_role_owner(plan, role, frozen, parent)?
        };
        NativeJointOldObservation::from_actual_service(actual, self.service.clone())
    }
}
