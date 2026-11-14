//! A DISTINCT bounded current phase over unchanged protected preparations.
//! Original observations/deadlines are never mutated or made live again.
use super::*;
use std::time::{Duration, Instant};

/// Scalar-only, fixed-capacity instrumentation. It owns no native capability,
/// cancellation token, lock, pin or metadata observation. Formatting happens
/// only after the original operation returns an error and drops its guards.
#[cfg(all(test, feature = "tls-replication"))]
struct ActivationFailureTrace {
    started: Instant,
    stages: [Option<(&'static str, Instant)>; 24],
    stage_count: usize,
    phase_deadline: Option<Instant>,
    caller_deadline: Option<Instant>,
    query_deadline: Option<Instant>,
    post_quorum: Option<(Uuid, u64, u64, Instant)>,
}
#[cfg(all(test, feature = "tls-replication"))]
impl ActivationFailureTrace {
    fn new(phase: &NativeJointPreparationPhaseQuorum) -> Self {
        Self {
            started: Instant::now(),
            stages: [None; 24],
            stage_count: 0,
            phase_deadline: phase.cancellation.deadline(),
            caller_deadline: phase.original.caller.deadline(),
            query_deadline: crate::query_budget::test_current_deadline(),
            post_quorum: None,
        }
    }
    fn mark(&mut self, stage: &'static str) {
        if let Some(slot) = self.stages.get_mut(self.stage_count) {
            *slot = Some((stage, Instant::now()));
        }
        self.stage_count += 1;
    }
    fn report(&self, phase: &NativeJointPreparationPhaseQuorum, error: &crate::BicDbError) {
        let ended = Instant::now();
        let clock = |deadline: Option<Instant>| {
            deadline.map(|deadline| serde_json::json!({
                "remaining_at_entry_us":deadline.saturating_duration_since(self.started).as_micros(),
                "expired_at_entry_us":self.started.saturating_duration_since(deadline).as_micros(),
                "remaining_at_return_us":deadline.saturating_duration_since(ended).as_micros(),
                "expired_at_return_us":ended.saturating_duration_since(deadline).as_micros(),
            }))
        };
        let members = phase
            .observations
            .iter()
            .take(64)
            .map(|(node, member)| {
                let (term, index, context) = member.committed.metadata_anchor();
                serde_json::json!({"node":node.as_str().chars().take(128).collect::<String>(),
                "term":term,"read_index":index,"context_id":context,
                "deadline":clock(Some(member.committed.deadline()))})
            })
            .collect::<Vec<_>>();
        let stages = self.stages.iter().flatten().map(|(stage, at)| {
            serde_json::json!({"stage":stage,"since_entry_us":at.saturating_duration_since(self.started).as_micros()})
        }).collect::<Vec<_>>();
        let post_quorum = self.post_quorum.map(|(context, term, index, deadline)| {
            serde_json::json!({"context_id":context,"term":term,"read_index":index,
                "deadline":clock(Some(deadline))})
        });
        // No request check, actual service inspection or RPC occurs here.
        // Missing producer/subcheck facts are explicitly not inferred.
        let value = serde_json::json!({
            "schema":"native-activation-failure-v1",
            "error":error.to_string().chars().take(4096).collect::<String>(),
            "elapsed_us":ended.saturating_duration_since(self.started).as_micros(),
            "phase_deadline":clock(self.phase_deadline),
            "original_caller_deadline":clock(self.caller_deadline),
            "thread_query_deadline":clock(self.query_deadline),
            "phase_cancel_requested_at_report":phase.cancellation.is_cancel_requested(),
            "original_caller_cancel_requested_at_report":phase.original.caller.is_cancel_requested(),
            "original_collection_cancel_requested_at_report":phase.original.cancellation.is_cancel_requested(),
            "members":members,"members_omitted":phase.observations.len().saturating_sub(64),
            "post_quorum_metadata":post_quorum,"stages":stages,
            "stages_omitted":self.stage_count.saturating_sub(self.stages.len()),
            "producer_session_and_inner_subchecks":"not_observed",
            "memory_observation_not_authority":true,
        });
        eprintln!("NATIVE_ACTIVATION_FAILURE {value}");
    }
}

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
            let writer = old.require_fresh_phase_current(&service, &committed)?;
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
        let writer = observation.require_fresh_phase_current(service, &fresh.committed)?;
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
    fn release_read_pins(
        &self,
        #[cfg(all(test, feature = "tls-replication"))] trace: &mut ActivationFailureTrace,
    ) -> Result<()> {
        #[cfg(all(test, feature = "tls-replication"))]
        trace.mark("pin_release_require_live");
        self.require_live()?;
        let mut released = true;
        for old in self.original.observations.values() {
            released &= old.actual.read_pin_already_released()?;
        }
        if released {
            // None is a private monotone fact about THESE original objects.
            // It grants no quiescence: final publication checks the actual
            // Frozen/local point and all foreign members again.
            #[cfg(all(test, feature = "tls-replication"))]
            trace.mark("pins_already_released_require_live");
            return self.require_live();
        }
        #[cfg(all(test, feature = "tls-replication"))]
        trace.mark("pin_release_before_full_sweep");
        self.require_current()?;
        #[cfg(all(test, feature = "tls-replication"))]
        trace.mark("pin_release_actual_members");
        for (node, fresh) in &self.observations {
            let old = &self.original.observations[node];
            let service = old
                .service
                .try_lock()
                .ok_or_else(|| invalid("fresh joint pin release service busy"))?;
            old.release_fresh_pin(&service, &fresh.committed)?;
        }
        #[cfg(all(test, feature = "tls-replication"))]
        trace.mark("pin_release_after_full_sweep");
        self.require_current()
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn acquire_local_activation(
        self: &Arc<Self>,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
    ) -> Result<NativeJointActivationAdmission> {
        #[cfg(test)]
        let mut trace = ActivationFailureTrace::new(self);
        let result = self.acquire_local_activation_inner(
            client,
            metadata_leader,
            #[cfg(test)]
            &mut trace,
        );
        #[cfg(test)]
        if let Err(error) = &result {
            trace.report(self, error);
        }
        result
    }
    #[cfg(feature = "tls-replication")]
    fn acquire_local_activation_inner(
        self: &Arc<Self>,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        #[cfg(test)] trace: &mut ActivationFailureTrace,
    ) -> Result<NativeJointActivationAdmission> {
        #[cfg(test)]
        trace.mark("activation_require_live");
        self.require_live()?;
        #[cfg(test)]
        trace.mark("claim_single_activation");
        if self
            .activation_claimed
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(invalid(
                "fresh joint phase already selected its single local publication attempt",
            ));
        }
        #[cfg(test)]
        trace.mark("bind_actual_local_participant");
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
        self.release_read_pins(
            #[cfg(test)]
            trace,
        )?;
        let business = Uuid::now_v7();
        #[cfg(test)]
        trace.mark("post_quorum_remaining_budget");
        let budget = remaining(&self.cancellation)?;
        #[cfg(test)]
        trace.mark("post_quorum_metadata_rpc");
        let metadata = client.acquire_host_metadata_observation(
            metadata_leader,
            business,
            budget,
            &self.cancellation,
        )?;
        #[cfg(test)]
        {
            trace.post_quorum = Some((
                metadata.context_id(),
                metadata.term(),
                metadata.read_index(),
                metadata.deadline(),
            ));
            trace.mark("capture_actual_metadata_point");
        }
        let metadata_point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &local.service,
            &metadata,
            business,
            &self.cancellation,
        )?;
        let fresh = {
            #[cfg(test)]
            trace.mark("lock_actual_local_service");
            let service = local
                .service
                .try_lock()
                .ok_or_else(|| invalid("fresh phase post-quorum binding service busy"))?;
            #[cfg(test)]
            trace.mark("observe_actual_fresh_committed");
            local.actual.observe_fresh_committed_with_point(
                &service,
                metadata,
                business,
                self.original.data.old_range().id,
                metadata_point,
            )?
        };
        #[cfg(test)]
        trace.mark("construct_activation_admission");
        let cancellation = self.cancellation.child_with_deadline(fresh.deadline());
        let result = NativeJointActivationAdmission {
            phase: Some(self.clone()),
            publication_claimed: std::sync::atomic::AtomicBool::new(false),
            quorum: self.original.clone(),
            local,
            fresh,
            cancellation,
        };
        #[cfg(test)]
        trace.mark("activation_final_require_live");
        result.require_live()?;
        Ok(result)
    }
}
