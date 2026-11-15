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
