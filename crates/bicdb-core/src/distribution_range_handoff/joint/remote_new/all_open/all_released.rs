//! Distinct actual pinned ALL-released Policy proof. Only the dedicated
//! protected source operation supplies the existing durable audit obligations.
//! Ordinary Open/Released DATA or a subset cannot construct this proof.
use super::*;

pub(crate) struct NativeRemoteAllReleasedRound {
    actual: NativeRemoteAllOpenRound,
}
pub(crate) struct NativeRemoteAllReleasedQuorum {
    actual: NativeRemoteAllOpenQuorum,
}
/// Distinct metadata-only activation proof. Consuming the non-Clone quorum
/// claims this actual collection once; it cannot also become a local Policy
/// admission. No local native decision or local QQ is constructed.
pub(crate) struct NativeRemoteActivatedMetadataProof {
    actual: NativeRemoteAllReleasedQuorum,
    own: HostMetadataReadObservation,
    business: Uuid,
    point: Arc<ActualNativeJointMetadataPoint>,
    caller: crate::CancellationToken,
    sha256: String,
}
impl NativeRemoteAllReleasedRound {
    pub(crate) fn begin(
        manifest: Arc<NativeRemoteImageManifestV1>,
        phase: Uuid,
        decision: String,
        parent: &CancellationToken,
    ) -> Result<Self> {
        Ok(Self {
            actual: NativeRemoteAllOpenRound::begin(manifest, phase, decision, parent)?,
        })
    }
    pub(crate) fn record(&mut self, frame: AuthenticatedNativeRemoteServingFrame) -> Result<()> {
        self.actual.record_actual(frame, true)
    }
    pub(crate) fn try_finish(
        self,
    ) -> std::result::Result<NativeRemoteAllReleasedQuorum, (BicDbError, Self)> {
        if let Err(error) = self.actual.require_complete_memory() {
            return Err((error, self));
        }
        Ok(NativeRemoteAllReleasedQuorum {
            actual: NativeRemoteAllOpenQuorum {
                actual: self.actual,
            },
        })
    }
}
impl NativeRemoteAllReleasedQuorum {
    pub(crate) fn certify_metadata_activation(
        self,
        own: HostMetadataReadObservation,
        business: Uuid,
        point: Arc<ActualNativeJointMetadataPoint>,
        parent: &crate::CancellationToken,
    ) -> Result<NativeRemoteActivatedMetadataProof> {
        self.require_live()?;
        parent.check()?;
        own.require_live_request(business)?;
        point.require_request(&own, business)?;
        point.require_external_current()?;
        if parent.deadline().is_none()
            || own.metadata_leader_id() != own.requester_node_id()
            || self.actual.actual.observer.as_ref() != Some(own.requester_node_id())
        {
            return Err(invalid(
                "remote activation needs the actual metadata leader's own collection",
            ));
        }
        let mut members = Vec::new();
        for (node, frame) in &self.actual.actual.frames {
            let audit = frame
                .point()
                .policy_released_audit
                .as_ref()
                .ok_or_else(|| invalid("remote activation omitted actual released source audit"))?;
            let prior = frame.metadata_anchor();
            if frame.operation() != NativeRemoteServingOperationV1::ObserveReleasedForPolicy
                || !frame.point().actual_dropped_open
                || audit.local_node_id != *node
                || own.term() < prior.0
                || own.read_index() < prior.1
                || own.context_id() == prior.2
            {
                return Err(invalid(
                    "remote activation needs independently post-ALL metadata",
                ));
            }
            // Same v1 durable activation identity as the local producer. Fresh
            // OWN/context/challenge and memory release generations are checked
            // above, never hashed as if they were stable durable facts.
            members.push((
                node.clone(),
                self.actual.actual.manifest.new_members[node].incarnation,
                audit.native_bridge_sha256.clone(),
                audit.catalogue_activation_sha256.clone(),
            ));
        }
        let mf = &self.actual.actual.manifest;
        let sha256 = digest(&(
            "bicdb-native-joint-activated-quorum-v1",
            &self.actual.actual.decision,
            mf.relocation_id,
            mf.activation_range.id,
            mf.activation_range.epoch,
            &members,
        ))?;
        let caller = parent
            .child_linked_to(&own.original_request_cancellation())
            .child_with_deadline(own.deadline());
        let proof = NativeRemoteActivatedMetadataProof {
            actual: self,
            own,
            business,
            point,
            caller,
            sha256,
        };
        proof.require_external_current()?;
        Ok(proof)
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.actual.require_live()
    }
    pub(crate) fn require_post_all(
        &self,
        post: &crate::distribution_range_handoff::CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        self.actual.require_post_all(post)
    }
    pub(crate) fn require_original(
        &self,
        participant: &NativeRemoteNewParticipant,
        successor: &crate::distribution_data::NativeRemoteReleaseOwner,
    ) -> Result<()> {
        participant.require_remote_policy_proof(&self.actual, successor)
    }
    pub(crate) fn require_local_observation(
        &self,
        observed: &crate::distribution_data::NativeRemoteReleaseObservation,
    ) -> Result<()> {
        self.actual.require_release_observation(observed)?;
        let generation = observed.require_released_open()?;
        let frame = self
            .actual
            .actual
            .frames
            .get(observed.owner().native().local_node_id())
            .ok_or_else(|| invalid("Policy ALL-released omits actual local owner"))?;
        let audit = frame
            .point()
            .policy_released_audit
            .as_ref()
            .ok_or_else(|| invalid("Policy ALL-released has no actual source audit"))?;
        if frame.operation() != NativeRemoteServingOperationV1::ObserveReleasedForPolicy
            || !frame.point().actual_dropped_open
            || audit.released_generation != generation
        {
            return Err(invalid(
                "Policy ALL-released changed actual original released generation",
            ));
        }
        self.require_live()
    }
    pub(crate) fn historical_audit(&self) -> Result<serde_json::Value> {
        self.require_live()?;
        let members = self
            .actual
            .actual
            .frames
            .iter()
            .map(|(node, frame)| {
                let audit = frame
                    .point()
                    .policy_released_audit
                    .as_ref()
                    .ok_or_else(|| {
                        invalid("Policy ALL-released cannot fabricate missing source OWN audit")
                    })?;
                if frame.operation() != NativeRemoteServingOperationV1::ObserveReleasedForPolicy
                    || !frame.point().actual_dropped_open
                    || audit.local_node_id != *node
                    || audit.released_generation.is_nil()
                {
                    return Err(invalid("Policy audit changed authenticated source binding"));
                }
                Ok((node, audit))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(
            serde_json::json!({"version":1,"decision_sha256":self.actual.actual.decision,"members":members}),
        )
    }
}

impl NativeRemoteActivatedMetadataProof {
    pub(crate) fn checksum_sha256(&self) -> &str {
        &self.sha256
    }
    pub(crate) fn manifest(&self) -> &NativeRemoteImageManifestV1 {
        &self.actual.actual.actual.manifest
    }
    pub(crate) fn decision_sha256(&self) -> &str {
        &self.actual.actual.actual.decision
    }
    /// Borrow the genuine retained observation only; no DTO constructor or
    /// observation renewal. The owner-specific planning scope keeps it lexical.
    pub(crate) fn metadata(&self) -> (&HostMetadataReadObservation, Uuid) {
        (&self.own, self.business)
    }
    pub(crate) fn require_memory(&self) -> Result<()> {
        self.caller.check()?;
        self.actual.require_live()?;
        self.own.require_live_request(self.business)?;
        self.point.require_current_memory()?;
        self.caller.check()
    }
    pub(crate) fn require_external_current(&self) -> Result<()> {
        self.require_memory()?;
        self.point.require_request(&self.own, self.business)?;
        self.point.require_external_current()?;
        self.require_memory()
    }
    pub(crate) fn require_control_leader(
        &self,
        store: &crate::MetadataConsensusStore,
    ) -> Result<()> {
        self.require_memory()?;
        store.require_native_control_leader(&self.own, self.business)?;
        self.require_memory()
    }
}
