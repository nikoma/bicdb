//! A DISTINCT bounded current phase over unchanged protected preparations.
//! Original observations/deadlines are never mutated or made live again.
use super::*;
use std::time::{Duration, Instant};

pub(crate) struct NativeJointPreparationPhaseRound {
    original: Arc<NativeJointPreparedServingQuorum>,
    cancellation: crate::CancellationToken,
    observations: BTreeMap<ClusterNodeId, FreshPreparedMember>,
    poisoned: bool,
}
pub(super) struct FreshPreparedMember {
    pub(super) committed: CommittedNativeJointDecisionObservation,
    writer: crate::db::NativeJointPhaseOwner,
}
pub(crate) struct NativeJointPreparationPhaseQuorum {
    original: Arc<NativeJointPreparedServingQuorum>,
    activation_claimed: std::sync::atomic::AtomicBool,
    pub(super) cancellation: crate::CancellationToken,
    pub(super) observations: BTreeMap<ClusterNodeId, FreshPreparedMember>,
}
impl NativeJointPreparedServingQuorum {
    /// Starting a phase grants nothing. The ORIGINAL caller cancellation and
    /// absolute parent deadline remain; only expired per-request MQ windows
    /// are excluded. EVERY original/final NEW participant must reattest.
    pub(crate) fn begin_fresh_preparation_phase(
        self: &Arc<Self>,
        budget: Duration,
    ) -> Result<NativeJointPreparationPhaseRound> {
        self.require_original_caller()?;
        if budget.is_zero()
            || budget > Duration::from_secs(4)
            || self.observations.len() != self.data.new_members().len()
            || self.observations.keys().ne(self.data.new_members().keys())
            || !self
                .observations
                .contains_key(&self.data.activation_range().leader)
        {
            return Err(invalid("fresh joint phase needs bounded four-second budget and SAME complete NEW participant set/leader"));
        }
        Ok(NativeJointPreparationPhaseRound {
            original: self.clone(),
            cancellation: self.caller.child_with_deadline(Instant::now() + budget),
            observations: BTreeMap::new(),
            poisoned: false,
        })
    }
}
impl NativeJointPreparationPhaseRound {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn acquire_actual_member(
        &mut self,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
    ) -> Result<()> {
        if self.poisoned {
            return Err(invalid("fresh joint phase is poisoned"));
        }
        self.poisoned = true;
        self.acquire_inner(client, metadata_leader)?;
        self.poisoned = false;
        Ok(())
    }
    #[cfg(feature = "tls-replication")]
    fn acquire_inner(
        &mut self,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
    ) -> Result<()> {
        self.cancellation.check()?;
        let actor = client.local_metadata_read_participant();
        let node = actor.node_id();
        let old = self.original.observations.get(node).ok_or_else(|| {
            invalid("fresh joint phase requester is not an original NEW participant")
        })?;
        if self.observations.contains_key(node)
            || self
                .original
                .data
                .new_members()
                .get(node)
                .is_none_or(|member| member.incarnation != actor.member_incarnation())
        {
            return Err(invalid(
                "fresh joint phase changed/duplicated actual recipient incarnation",
            ));
        }
        let budget = remaining(&self.cancellation)?;
        let business = Uuid::now_v7();
        // Actual TLS + committed metadata QQ, with NO service/DB latch held.
        let metadata = client.acquire_host_metadata_observation(
            metadata_leader,
            business,
            budget,
            &self.cancellation,
        )?;
        let metadata_point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &old.service,
            &metadata,
            business,
            &self.cancellation,
        )?;
        let (committed, writer) = {
            let service = old
                .service
                .try_lock()
                .ok_or_else(|| invalid("fresh joint phase actual service busy"))?;
            let committed = old.actual.observe_fresh_committed_with_point(
                &service,
                metadata,
                business,
                self.original.data.old_range().id,
                metadata_point,
            )?;
            let writer = old
                .actual
                .require_current_under_fresh_phase(&service, &committed)?;
            (committed, writer)
        };
        if committed.data() != &self.original.data {
            return Err(invalid(
                "fresh joint phase changed protected global decision",
            ));
        }
        self.cancellation = self.cancellation.child_with_deadline(committed.deadline());
        self.cancellation.check()?;
        self.observations
            .insert(node.clone(), FreshPreparedMember { committed, writer });
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<NativeJointPreparationPhaseQuorum> {
        self.cancellation.check()?;
        if self.poisoned
            || self
                .observations
                .keys()
                .ne(self.original.observations.keys())
        {
            return Err(invalid(
                "fresh joint phase cannot omit/replace any original/final participant",
            ));
        }
        let result = NativeJointPreparationPhaseQuorum {
            original: self.original,
            activation_claimed: std::sync::atomic::AtomicBool::new(false),
            cancellation: self.cancellation,
            observations: self.observations,
        };
        // Collection already read every actual participant. Permission is
        // closed by the complete FINAL foreign sweep + local journal scope,
        // not an extra identical sweep before the post-quorum MQ.
        result.require_live()?;
        Ok(result)
    }
}
fn remaining(token: &crate::CancellationToken) -> Result<Duration> {
    token.check()?;
    token
        .deadline()
        .and_then(|d| d.checked_duration_since(Instant::now()))
        .filter(|d| !d.is_zero())
        .ok_or_else(|| invalid("fresh joint phase original budget expired"))
}
impl NativeJointPreparationPhaseQuorum {
    pub(super) fn original_quorum(&self) -> Arc<NativeJointPreparedServingQuorum> {
        self.original.clone()
    }

    pub(super) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        self.original.require_original_caller()?;
        for (node, member) in &self.observations {
            member.committed.require_metadata_current()?;
            self.original.observations[node]
                .actual
                .require_phase_writer_lifetime(&member.writer)?;
        }
        Ok(())
    }
    pub(super) fn committed(
        &self,
        node: &ClusterNodeId,
    ) -> Result<&CommittedNativeJointDecisionObservation> {
        self.observations
            .get(node)
            .map(|o| &o.committed)
            .ok_or_else(|| invalid("fresh joint participant missing"))
    }
    pub(super) fn require_member_current(&self, node: &ClusterNodeId) -> Result<()> {
        self.require_live()?;
        let observation = self
            .original
            .observations
            .get(node)
            .ok_or_else(|| invalid("fresh joint phase substituted participant"))?;
        let service = observation
            .service
            .try_lock()
            .ok_or_else(|| invalid("fresh joint participant service busy"))?;
        self.require_member_with_service(node, &service)
    }
    pub(super) fn require_member_with_service(
        &self,
        node: &ClusterNodeId,
        service: &ClusterDataNodeService,
    ) -> Result<()> {
        self.require_live()?;
        let observation = self
            .original
            .observations
            .get(node)
            .ok_or_else(|| invalid("fresh joint phase substituted participant"))?;
        let fresh = self
            .observations
            .get(node)
            .ok_or_else(|| invalid("fresh joint phase omitted participant"))?;
        let writer = observation
            .actual
            .require_current_under_fresh_phase(service, &fresh.committed)?;
        if !writer.same_actual(&fresh.writer) {
            return Err(invalid(
                "fresh joint writer registration/generation changed",
            ));
        }
        self.require_live()
    }
    pub(super) fn require_local_publication_writer(
        &self,
        node: &ClusterNodeId,
        actual: &crate::db::NativeJointPhaseOwner,
    ) -> Result<()> {
        self.require_live()?;
        if self
            .observations
            .get(node)
            .is_none_or(|member| !member.writer.same_actual(actual))
        {
            return Err(invalid(
                "publication final scope actual writer registration changed",
            ));
        }
        self.require_live()
    }
    pub(crate) fn require_current(&self) -> Result<()> {
        self.require_live()?;
        for node in self.observations.keys() {
            self.require_member_current(node)?;
        }
        Ok(())
    }
    fn release_read_pins(&self) -> Result<()> {
        self.require_live()?;
        let mut released = true;
        for old in self.original.observations.values() {
            released &= old.actual.read_pin_already_released()?;
        }
        if released {
            // None is a private monotone fact about THESE original objects.
            // It grants no quiescence: final publication checks the actual
            // Frozen/local point and all foreign members again.
            return self.require_live();
        }
        self.require_current()?;
        for (node, fresh) in &self.observations {
            let old = &self.original.observations[node];
            let service = old
                .service
                .try_lock()
                .ok_or_else(|| invalid("fresh joint pin release service busy"))?;
            old.actual
                .release_read_pin_under_fresh_phase(&service, &fresh.committed)?;
        }
        self.require_current()
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn acquire_local_activation(
        self: &Arc<Self>,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
    ) -> Result<NativeJointActivationAdmission> {
        self.require_live()?;
        if self
            .activation_claimed
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(invalid(
                "fresh joint phase already selected its single local publication attempt",
            ));
        }
        let actor = client.local_metadata_read_participant();
        let local = self
            .original
            .observations
            .get(actor.node_id())
            .ok_or_else(|| invalid("fresh phase activation has no actual local preparation"))?
            .clone();
        if self.original.data.new_members()[actor.node_id()].incarnation
            != actor.member_incarnation()
        {
            return Err(invalid("fresh phase activation actual recipient changed"));
        }
        // This operation checks ALL actual members before AND after pin release.
        // Do not add an identical third sweep before this same operation.
        self.release_read_pins()?;
        let business = Uuid::now_v7();
        let metadata = client.acquire_host_metadata_observation(
            metadata_leader,
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
                .ok_or_else(|| invalid("fresh phase post-quorum binding service busy"))?;
            local.actual.observe_fresh_committed_with_point(
                &service,
                metadata,
                business,
                self.original.data.old_range().id,
                metadata_point,
            )?
        };
        let cancellation = self.cancellation.child_with_deadline(fresh.deadline());
        let result = NativeJointActivationAdmission {
            phase: Some(self.clone()),
            publication_claimed: std::sync::atomic::AtomicBool::new(false),
            quorum: self.original.clone(),
            local,
            fresh,
            cancellation,
        };
        result.require_live()?;
        Ok(result)
    }
}
