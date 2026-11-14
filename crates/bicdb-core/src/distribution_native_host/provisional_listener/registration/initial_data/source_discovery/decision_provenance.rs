//! Separate READONLY participant ingress. Start/Describe remain target-only.
use super::*;
use crate::distribution_range_handoff::NativeRemoteDecisionSourceRequestV1;
impl NativeInitialDataAdmission {
    pub(crate) fn observe_participant_decision_provenance(
        self: &Arc<Self>,
        peer: &MetadataReadParticipant,
        request: &NativeRemoteDecisionSourceRequestV1,
        parent: &CancellationToken,
    ) -> Result<ActualNativeSourceDiscovery> {
        request.require_shape()?;
        if peer.node_id() != &request.observer_id
            || peer.member_incarnation() != request.observer_birth.node_incarnation()
        {
            return Err(invalid(
                "decision source observer differs from actual authenticated peer",
            ));
        }
        let token = request.source.cancellation(parent)?;
        let current = self.acquire_source_discovery_for(
            peer,
            &request.source,
            &token,
            DiscoveryCaller::DecisionParticipant,
        )?;
        if current.old.plan().enrolled_birth(peer.node_id())? != &request.observer_birth {
            return Err(invalid(
                "decision source observer changed original enrolled birth",
            ));
        }
        let registry = self
            .image_sources
            .upgrade()
            .ok_or_else(|| invalid("decision source original process ended"))?;
        registry.require_original_process()?;
        let attempt = registry.source_discovery_attempt(&request.source)?;
        let _claim = attempt.claim()?;
        let source = attempt
            .source()?
            .ok_or_else(|| invalid("decision provenance cannot start a missing capture"))?;
        current.require_current()?;
        let actual = ActualNativeSourceDiscovery::from_actual_source(
            request.source.clone(),
            &source,
            &current.own,
            current.business,
            &token,
        )?;
        current.require_current()?;
        registry.require_original_process()?;
        token.check()?;
        Ok(actual)
    }
}
