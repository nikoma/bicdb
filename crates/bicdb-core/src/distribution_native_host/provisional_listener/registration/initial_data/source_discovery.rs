//! Actual recipient-owned donor discovery; no recovery-purpose cast.
use super::*;
mod decision_provenance;
mod export_retirement;

impl NativeInitialDataAdmission {
    pub(super) fn collect_remote_old_for_source(
        &self,
        range: crate::RangeId,
        parent: &CancellationToken,
        after_fetch: impl FnOnce(
            &HostMetadataReadObservation,
            Uuid,
            &[crate::distribution_transport::AuthenticatedNativeRemoteOldFrame],
        ) -> Result<()>,
    ) -> Result<crate::distribution_range_handoff::NativeRemoteOldCutCertificate> {
        let transport = self
            .old_transport
            .upgrade()
            .ok_or_else(|| invalid("actual donor transport ended"))?;
        let provider = self
            .old_provider
            .upgrade()
            .ok_or_else(|| invalid("actual donor provider ended"))?;
        use crate::distribution_range_handoff::{
            NativeRemoteOldOperationV1, NativeRemoteOldRequestV1, NativeRemoteOldRound,
        };
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        self.require_live()?;
        self.image_sources
            .upgrade()
            .ok_or_else(|| invalid("actual donor source process absent"))?
            .require_original_process()?;
        let actual = self.retained_actual_old_source(range)?;
        let plan = actual.plan().clone();
        let leader = provider
            .current_metadata_leader_hint(&token)?
            .ok_or_else(|| invalid("actual donor metadata leader unavailable"))?;
        let business = Uuid::now_v7();
        let own = transport.acquire_host_metadata_observation(
            &leader,
            business,
            Duration::from_secs(5),
            &token,
        )?;
        // This first fresh OWN binds the observation phase, not the final
        // collector lifetime. Retain only necessary monotonic/phase DATA;
        // unrelated range metadata changes need not invalidate this cut.
        plan.require_source_host_metadata(&own, business, actual.node_id())?;
        let started_term = own.term();
        let started_index = own.read_index();
        let started_phase = own
            .committed_topology(business)?
            .relocations
            .get(&plan.promise().relocation_id)
            .ok_or_else(|| invalid("remote OLD observation phase disappeared"))?
            .phase;
        let mut frames = Vec::with_capacity(plan.old_members().len());
        for node in plan.old_members().keys() {
            token.check()?;
            let business = Uuid::now_v7();
            let remaining = token
                .deadline()
                .unwrap()
                .saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(BicDbError::QueryTimedOut);
            }
            let own = transport
                .acquire_host_metadata_observation(&leader, business, remaining, &token)?;
            let request = NativeRemoteOldRequestV1::from_plan(
                &plan,
                NativeRemoteOldOperationV1::Observe,
                business,
                remaining.as_millis().min(4000).max(1) as u64,
            )?;
            let frame = transport.request_remote_native_old(node, &plan, &request, &own, &token)?;
            frame.require_request_live()?;
            frames.push(frame);
        }
        after_fetch(&own, business, &frames)?;
        token.check()?;
        self.require_live()?;
        self.image_sources
            .upgrade()
            .ok_or_else(|| invalid("actual donor source process absent after OLD fetch"))?
            .require_original_process()?;
        actual.require_current()?;
        for frame in &frames {
            frame.require_request_live()?;
        }
        let remaining = token
            .deadline()
            .unwrap()
            .checked_duration_since(Instant::now())
            .ok_or(BicDbError::QueryTimedOut)?;
        // Fresh final authority is acquired AFTER the bounded remote reads.
        // None of their original request tokens or authenticated owners is
        // reissued or relabeled. A canceled/expired frame still refuses below.
        let final_business = Uuid::now_v7();
        let final_own = transport.acquire_host_metadata_observation(
            &leader,
            final_business,
            remaining,
            &token,
        )?;
        plan.require_source_host_metadata(&final_own, final_business, actual.node_id())?;
        let final_phase = final_own
            .committed_topology(final_business)?
            .relocations
            .get(&plan.promise().relocation_id)
            .ok_or_else(|| invalid("remote OLD final Pending phase disappeared"))?
            .phase;
        if final_own.term() < started_term
            || final_own.read_index() < started_index
            || final_phase != started_phase
        {
            return Err(invalid(
                "remote OLD final metadata regressed or selected phase changed",
            ));
        }
        let mut round =
            NativeRemoteOldRound::begin(actual.clone(), final_own, final_business, &token)?;
        for frame in frames {
            round.record(frame)?;
        }
        let result = round.finish()?;
        actual.require_current()?;
        self.require_live()?;
        self.image_sources
            .upgrade()
            .ok_or_else(|| invalid("actual donor source process absent after OLD cut"))?
            .require_original_process()?;
        token.check()?;
        Ok(result)
    }
}

use crate::distribution_consensus::{
    MetadataReadParticipant, NativeOwnedMetadataMemberObservation,
};
use crate::distribution_range_handoff::NativeJointOldObservation;
use crate::distribution_range_image::{
    ActualNativeSourceDiscovery, NativeSourceDiscoveryOperationV1, NativeSourceDiscoveryRequestV1,
};

/// Recipient-local capture/read admission. No RecoveryRole is manufactured:
/// the actual historical OLD owner and independently current own metadata
/// authorize only this source operation, never Close, rows, or Serving.
struct SourceDiscoveryAdmission<'a> {
    original: &'a Arc<NativeInitialDataAdmission>,
    old: Arc<NativeJointOldObservation>,
    metadata: Arc<Mutex<crate::distribution_consensus::MetadataConsensusStore>>,
    own: HostMetadataReadObservation,
    business: Uuid,
    member: NativeOwnedMetadataMemberObservation,
    caller: &'a CancellationToken,
}
impl SourceDiscoveryAdmission<'_> {
    fn require_current(&self) -> Result<()> {
        self.caller.check()?;
        self.original.require_live()?;
        self.old.require_current()?;
        self.old.plan().require_source_host_metadata(
            &self.own,
            self.business,
            self.old.node_id(),
        )?;
        {
            let store = self
                .metadata
                .try_lock()
                .ok_or_else(|| invalid("source discovery metadata busy at boundary"))?;
            store.require_listener_database(&self.original.database)?;
            self.member.require_current(&store)?;
        }
        let service = self
            .original
            .service
            .upgrade()
            .ok_or_else(|| invalid("actual donor service ended"))?;
        let database = service
            .try_lock()
            .ok_or_else(|| invalid("source discovery service busy"))?
            .database();
        {
            let db = database
                .try_read()
                .ok_or_else(|| invalid("source discovery DB busy"))?;
            self.original.database.require_database(&db)?;
            self.member.require_database(&db)?;
        }
        {
            let store = self
                .metadata
                .try_lock()
                .ok_or_else(|| invalid("source discovery metadata busy after local check"))?;
            self.member.require_current(&store)?;
        }
        self.original.require_live()?;
        self.own.require_live_request(self.business)?;
        self.caller.check()
    }
}
enum DiscoveryCaller {
    Target,
    DecisionParticipant,
}
impl NativeInitialDataAdmission {
    fn acquire_source_discovery<'a>(
        self: &'a Arc<Self>,
        peer: &MetadataReadParticipant,
        request: &NativeSourceDiscoveryRequestV1,
        parent: &'a CancellationToken,
    ) -> Result<SourceDiscoveryAdmission<'a>> {
        self.acquire_source_discovery_for(peer, request, parent, DiscoveryCaller::Target)
    }
    fn acquire_source_discovery_for<'a>(
        self: &'a Arc<Self>,
        peer: &MetadataReadParticipant,
        request: &NativeSourceDiscoveryRequestV1,
        parent: &'a CancellationToken,
        caller: DiscoveryCaller,
    ) -> Result<SourceDiscoveryAdmission<'a>> {
        request.require_shape()?;
        self.require_live()?;
        let registry = self
            .image_sources
            .upgrade()
            .ok_or_else(|| invalid("original donor registry ended"))?;
        registry.require_original_process()?;
        let old = self.retained_actual_old_source(request.range_id)?;
        request.require_plan(old.plan())?;
        let observer_birth = match caller {
            DiscoveryCaller::Target => {
                if peer.node_id() != &request.target_id {
                    return Err(invalid("source discovery target identity differs"));
                }
                &request.target_birth
            }
            DiscoveryCaller::DecisionParticipant => {
                if request.operation != NativeSourceDiscoveryOperationV1::DecisionProvenance
                    || (!old.plan().old_members().contains_key(peer.node_id())
                        && !old.plan().new_members().contains_key(peer.node_id()))
                {
                    return Err(invalid("decision provenance readonly observer is not an original OLD/NEW participant"));
                }
                old.plan().enrolled_birth(peer.node_id())?
            }
        };
        if old.node_id() != &request.source_id
            || peer.member_incarnation() != observer_birth.node_incarnation()
            || peer.plaintext_loopback()
        {
            return Err(invalid(
                "source discovery actual donor/target identity differs",
            ));
        }
        let transport = self
            .old_transport
            .upgrade()
            .ok_or_else(|| invalid("actual donor transport ended"))?;
        let provider = self
            .old_provider
            .upgrade()
            .ok_or_else(|| invalid("actual donor provider ended"))?;
        let leader = provider
            .current_metadata_leader_hint(parent)?
            .ok_or_else(|| invalid("source discovery metadata leader absent"))?;
        let business = Uuid::now_v7();
        let remaining = parent
            .deadline()
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .ok_or(BicDbError::QueryTimedOut)?;
        let own = transport.acquire_host_metadata_observation(
            &leader,
            business,
            remaining.min(Duration::from_secs(5)),
            parent,
        )?;
        old.plan()
            .require_source_host_metadata(&own, business, old.node_id())?;
        let current = own.committed_topology(business)?;
        let target = current
            .nodes
            .get(&request.target_id)
            .ok_or_else(|| invalid("discovery target absent"))?;
        let observer = current
            .nodes
            .get(peer.node_id())
            .ok_or_else(|| invalid("decision provenance observer absent from current OWN"))?;
        if current.cluster_id != request.cluster_id
            || current.range_image_bootstrap_id != Some(request.bootstrap_id)
            || target.native_store_birth_v1.as_ref() != Some(&request.target_birth)
            || target.lifecycle != ClusterNodeLifecycle::Active
            || observer.lifecycle != ClusterNodeLifecycle::Active
            || observer.native_store_birth_v1.as_ref() != Some(observer_birth)
            || peer
                .certificate_sha256()
                .is_none_or(|p| !observer.accepts_tls_certificate_sha256(p))
        {
            return Err(invalid(
                "source discovery actual target birth/TLS/current domain differs",
            ));
        }
        let metadata = self
            .old_metadata
            .upgrade()
            .ok_or_else(|| invalid("actual donor MStore ended"))?;
        let member = {
            let store = loop {
                parent.check()?;
                own.require_live_request(business)?;
                if let Some(store) = metadata.try_lock_for(Duration::from_millis(5)) {
                    break store;
                }
            };
            store.require_listener_database(&self.database)?;
            store.observe_owned_native_metadata_member(&own, business, parent)?
        };
        let admission = SourceDiscoveryAdmission {
            original: self,
            old,
            metadata,
            own,
            business,
            member,
            caller: parent,
        };
        admission.require_current()?;
        Ok(admission)
    }
    pub(crate) fn discover_remote_image_source(
        self: &Arc<Self>,
        peer: &MetadataReadParticipant,
        request: &NativeSourceDiscoveryRequestV1,
        parent: &CancellationToken,
    ) -> Result<ActualNativeSourceDiscovery> {
        let token = request.cancellation(parent)?;
        let first = self.acquire_source_discovery(peer, request, &token)?;
        let registry = self
            .image_sources
            .upgrade()
            .ok_or_else(|| invalid("original donor registry ended"))?;
        registry.require_original_process()?;
        let attempt = registry.source_discovery_attempt(request)?;
        let _claim = attempt.claim()?;
        first.require_current()?;
        drop(first);
        let source = if let Some(source) = attempt.source()? {
            source
        } else {
            if request.operation != NativeSourceDiscoveryOperationV1::Start {
                return Err(invalid("source discovery capture has not started"));
            }
            let runtime = self
                .runtime
                .upgrade()
                .ok_or_else(|| invalid("actual donor runtime ended"))?;
            let service = self
                .service
                .upgrade()
                .ok_or_else(|| invalid("actual donor service ended"))?;
            let metadata = self
                .old_metadata
                .upgrade()
                .ok_or_else(|| invalid("actual donor MStore ended"))?;
            let writer = self
                .source_writer
                .read()
                .upgrade()
                .ok_or_else(|| invalid("original donor writer guard ended"))?;
            let coordinator = self
                .source_coordinator
                .read()
                .upgrade()
                .ok_or_else(|| invalid("original donor coordinator ended"))?;
            self.require_listener(&service, &runtime)?;
            let old =
                self.collect_remote_old_for_source(request.range_id, &token, |_, _, _| Ok(()))?;
            // The pure retainer consumes the actual certificate into original
            // process custody BEFORE capture or any subsequent fallible check.
            let retained = registry.retain_initial_source_from_remote_old(
                self.clone(),
                runtime,
                service,
                metadata,
                writer,
                coordinator,
                old,
                request.target_attempt,
            );
            attempt.retain_source(retained)?
        };
        // ALL peer network work completed before this fresh capture authority.
        // This is a new bounded acquisition within the original RPC, not a
        // relabeling of the earlier proof or of any authenticated OLD frame.
        let current = self.acquire_source_discovery(peer, request, &token)?;
        current.require_current()?;
        let actual = ActualNativeSourceDiscovery::from_actual_source(
            request.clone(),
            &source,
            &current.own,
            current.business,
            &token,
        )?;
        // Outside engine/session guards: actual current MStore, original DB
        // point, slot/hook/process and selected plan must still agree. This
        // reply remains historical DATA, never a current mutation grant.
        current.require_current()?;
        registry.require_original_process()?;
        token.check()?;
        Ok(actual)
    }
    #[cfg(test)]
    pub(crate) fn test_take_source_discovery_reply_loss(&self) -> bool {
        self.image_sources
            .upgrade()
            .is_some_and(|x| x.test_take_discovery_reply_loss())
    }
}
#[cfg(test)]
impl NativeMetadataControlHost {
    pub(crate) fn test_source_discovery_state(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        self.data
            .as_ref()
            .ok_or_else(|| invalid("actual donor absent"))?
            .image_sources
            .as_ref()
            .ok_or_else(|| invalid("actual donor registry absent"))?
            .test_discovery_state(range)
    }
    pub(crate) fn test_cancel_source_discovery_original_process(&self) -> Result<()> {
        self.data
            .as_ref()
            .ok_or_else(|| invalid("actual donor absent"))?
            .image_sources
            .as_ref()
            .ok_or_else(|| invalid("actual donor registry absent"))?
            .test_cancel_original_process()
    }
    pub(crate) fn test_drop_next_source_discovery_reply(&self) -> Result<()> {
        self.data
            .as_ref()
            .ok_or_else(|| invalid("actual donor absent"))?
            .image_sources
            .as_ref()
            .ok_or_else(|| invalid("actual donor registry absent"))?
            .test_drop_next_discovery_reply();
        Ok(())
    }
}
