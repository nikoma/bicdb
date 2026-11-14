//! DISTINCT all-Frozen catalogue/open phase. An old receipt's expiry is never
//! modified; exact protected activation is checked as an actual durable fact.
use super::*;
use std::time::{Duration, Instant};

struct OpenMember {
    committed: CommittedNativeJointDecisionObservation,
    writer: crate::db::NativeJointPhaseOwner,
}
pub(crate) struct NativeJointOpenPhaseRound {
    original: Arc<NativeJointPublishedServingQuorum>,
    cancellation: crate::CancellationToken,
    members: BTreeMap<ClusterNodeId, OpenMember>,
    poisoned: bool,
}
pub(crate) struct NativeJointOpenPhaseQuorum {
    original: Arc<NativeJointPublishedServingQuorum>,
    cancellation: crate::CancellationToken,
    members: BTreeMap<ClusterNodeId, OpenMember>,
    claimed: std::sync::atomic::AtomicBool,
}
impl NativeJointPublishedServingQuorum {
    /// Aggregation starts empty and confers no permission. EVERY original/final
    /// participant, including leader, must independently reattest under real MQ.
    pub(crate) fn begin_fresh_open_phase(
        self: &Arc<Self>,
        budget: Duration,
    ) -> Result<NativeJointOpenPhaseRound> {
        self.prepared.require_original_caller()?;
        if budget.is_zero()
            || budget > Duration::from_secs(4)
            || self.receipts.keys().ne(self.data().new_members().keys())
            || self
                .prepared
                .observations
                .keys()
                .ne(self.data().new_members().keys())
        {
            return Err(invalid("fresh joint open needs bounded budget and original COMPLETE participant/publication set"));
        }
        Ok(NativeJointOpenPhaseRound {
            original: self.clone(),
            cancellation: self
                .prepared
                .caller
                .child_with_deadline(Instant::now() + budget),
            members: BTreeMap::new(),
            poisoned: false,
        })
    }
}
impl NativeJointOpenPhaseRound {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn acquire_actual_member(
        &mut self,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        leader: &ClusterNodeId,
    ) -> Result<()> {
        if self.poisoned {
            return Err(invalid("fresh open phase is poisoned"));
        }
        self.poisoned = true;
        self.cancellation.check()?;
        self.original.prepared.require_original_caller()?;
        let actor = client.local_metadata_read_participant();
        let node = actor.node_id();
        #[cfg(test)]
        let mut trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "open-collect-member-total",
            node,
            &self.cancellation,
        );
        let old = self
            .original
            .prepared
            .observations
            .get(node)
            .ok_or_else(|| invalid("fresh open participant is not an original final member"))?;
        if self.members.contains_key(node)
            || self.original.data().new_members()[node].incarnation != actor.member_incarnation()
        {
            return Err(invalid("fresh open duplicate/changed recipient"));
        }
        let business = Uuid::now_v7();
        #[cfg(test)]
        let mut mq_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "open-collect-member-hostMQ",
            node,
            &self.cancellation,
        );
        let metadata = client.acquire_host_metadata_observation(
            leader,
            business,
            remaining(&self.cancellation)?,
            &self.cancellation,
        )?;
        #[cfg(test)]
        {
            mq_trace.complete();
            drop(mq_trace);
        }
        let metadata_point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &old.service,
            &metadata,
            business,
            &self.cancellation,
        )?;
        let (committed, writer) = {
            let mut service = old
                .service
                .try_lock()
                .ok_or_else(|| invalid("fresh open service busy"))?;
            service.reconcile_native_joint_open_phase_with_point(
                &metadata,
                business,
                &self.original.receipts[node],
                &old.actual,
                &self.cancellation,
                metadata_point.as_deref(),
            )?;
            let committed = old.actual.observe_fresh_committed_with_point(
                &service,
                metadata,
                business,
                self.original.data().old_range().id,
                metadata_point,
            )?;
            let writer = old.actual.require_current_under_open_phase(
                &service,
                &committed,
                self.original.receipts[node].clone(),
            )?;
            (committed, writer)
        };
        self.cancellation = self.cancellation.child_with_deadline(committed.deadline());
        self.cancellation.check()?;
        self.members
            .insert(node.clone(), OpenMember { committed, writer });
        self.poisoned = false;
        #[cfg(test)]
        trace.complete();
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<NativeJointOpenPhaseQuorum> {
        #[cfg(test)]
        let mut trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "open-close-all-member-round",
            &self.original.data().activation_range().leader,
            &self.cancellation,
        );
        self.cancellation.check()?;
        if self.poisoned || self.members.keys().ne(self.original.receipts.keys()) {
            return Err(invalid(
                "fresh open phase omitted/replaced a final participant",
            ));
        }
        let result = NativeJointOpenPhaseQuorum {
            original: self.original,
            cancellation: self.cancellation,
            members: self.members,
            claimed: std::sync::atomic::AtomicBool::new(false),
        };
        // Collection has already validated every actual member. This closes
        // an aggregation, not an activation grant: retain all original caller,
        // per-member MQ and writer lifetimes here. Activation still performs
        // its COMPLETE final foreign sweep after the distinct post-quorum MQ,
        // then validates the local proof under the actual native journal.
        // A second identical full sweep here consumes the same bounded phase
        // before that required final boundary without supplying authority.
        result.require_live()?;
        #[cfg(test)]
        trace.complete();
        Ok(result)
    }
}
fn remaining(token: &crate::CancellationToken) -> Result<Duration> {
    token.check()?;
    token
        .deadline()
        .and_then(|d| d.checked_duration_since(Instant::now()))
        .filter(|d| !d.is_zero())
        .ok_or(crate::BicDbError::QueryTimedOut)
}
impl NativeJointOpenPhaseQuorum {
    pub(super) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        self.original.prepared.require_original_caller()?;
        for (node, member) in &self.members {
            member.committed.require_metadata_current()?;
            self.original.prepared.observations[node]
                .actual
                .require_phase_writer_lifetime(&member.writer)?;
        }
        Ok(())
    }
    pub(super) fn require_member_with_service(
        &self,
        node: &ClusterNodeId,
        service: &ClusterDataNodeService,
    ) -> Result<()> {
        self.require_live()?;
        let original = self
            .original
            .prepared
            .observations
            .get(node)
            .ok_or_else(|| invalid("fresh open substituted participant"))?;
        let member = self
            .members
            .get(node)
            .ok_or_else(|| invalid("fresh open omitted participant"))?;
        let writer = original.actual.require_current_under_open_phase(
            service,
            &member.committed,
            self.original.receipts[node].clone(),
        )?;
        if !writer.same_actual(&member.writer) {
            return Err(invalid("fresh open writer registration/generation changed"));
        }
        self.require_live()
    }
    pub(super) fn require_local_native_scope(
        &self,
        node: &ClusterNodeId,
        store: &crate::distribution_range_consensus::RangeWriteStore,
        scope: &crate::db::NativeJointBridgeReadScope<'_>,
    ) -> Result<()> {
        self.require_live()?;
        let original = self
            .original
            .prepared
            .observations
            .get(node)
            .ok_or_else(|| invalid("fresh open local scope changed member"))?;
        let member = self
            .members
            .get(node)
            .ok_or_else(|| invalid("fresh open local scope missing member"))?;
        let writer = original.actual.require_local_open_scope(
            scope,
            store,
            &member.committed,
            self.original.receipts[node].clone(),
        )?;
        if !writer.same_actual(&member.writer) {
            return Err(invalid(
                "fresh open local native scope writer generation changed",
            ));
        }
        self.require_live()
    }
    pub(super) fn require_member_current(&self, node: &ClusterNodeId) -> Result<()> {
        let service = self.original.prepared.observations[node]
            .service
            .try_lock()
            .ok_or_else(|| invalid("fresh open current service busy"))?;
        self.require_member_with_service(node, &service)
    }
    pub(crate) fn require_current(&self) -> Result<()> {
        self.require_live()?;
        for node in self.members.keys() {
            self.require_member_current(node)?;
        }
        self.require_live()
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn acquire_local_open(
        self: &Arc<Self>,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        leader: &ClusterNodeId,
    ) -> Result<NativeJointOpenAdmission> {
        self.require_live()?;
        if self.claimed.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return Err(invalid(
                "fresh open phase already selected one local attempt",
            ));
        }
        let actor = client.local_metadata_read_participant();
        let node = actor.node_id();
        let local = self
            .original
            .prepared
            .observations
            .get(node)
            .ok_or_else(|| invalid("fresh open has no actual local member"))?
            .clone();
        if self.original.data().new_members()[node].incarnation != actor.member_incarnation() {
            return Err(invalid("fresh open local incarnation changed"));
        }
        #[cfg(test)]
        let mut trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "open-local-post-QQ-MQ-and-binding",
            node,
            &self.cancellation,
        );
        let business = Uuid::now_v7();
        // Distinct actual post-all-member MQ, no DB/service latch across I/O.
        let metadata = client.acquire_host_metadata_observation(
            leader,
            business,
            remaining(&self.cancellation)?,
            &self.cancellation,
        )?;
        let metadata_point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &local.service,
            &metadata,
            business,
            &self.cancellation,
        )?;
        let fresh = {
            let service = local
                .service
                .try_lock()
                .ok_or_else(|| invalid("fresh open postquorum service busy"))?;
            local.actual.observe_fresh_committed_with_point(
                &service,
                metadata,
                business,
                self.original.data().old_range().id,
                metadata_point,
            )?
        };
        let result = NativeJointOpenAdmission {
            open_phase: Some(self.clone()),
            attempted: std::sync::atomic::AtomicBool::new(false),
            quorum: self.original.clone(),
            local,
            receipt: self.original.receipts[node].clone(),
            cancellation: self.cancellation.child_with_deadline(fresh.deadline()),
            fresh,
        };
        result.require_live()?;
        #[cfg(test)]
        trace.complete();
        Ok(result)
    }
    pub(super) fn require_post_quorum(
        &self,
        fresh: &CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        self.require_live()?;
        let (term, index, context) = fresh.metadata_anchor();
        for member in self.members.values() {
            let old = member.committed.metadata_anchor();
            if term < old.0 || index < old.1 || context == old.2 {
                return Err(invalid(
                    "fresh open local MQ is not independently post-all-member-quorum",
                ));
            }
        }
        Ok(())
    }
}
