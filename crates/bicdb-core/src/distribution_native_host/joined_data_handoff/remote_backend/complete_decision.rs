//! The joint decision of the remote backend: durable on EVERY member that
//! later needs it before this coordinator retains a certificate.
//!
//! The certificate's own rule is unchanged: an OLD majority, a NEW majority
//! and the new leader. That rule says when the decision can no longer be
//! lost. It does not say that the handoff can finish: the activation asks
//! every new member to confirm the decision from its own protected native
//! decision record, and the retired source confirms it the same way before
//! it records its retirement. A member that missed the publication has no
//! such record and nothing copies one to it later. A certificate counted on
//! majorities only would therefore let the membership stages commit (the
//! writers frozen, the abort closed) with an activation that can never
//! complete.
//!
//! So this coordinator certifies only what every such member holds. The
//! members that need the record are every new member and the retiring
//! source; the surviving old members are new members, so that is every old
//! voter plus the target. The read-only observation is sent to all of them
//! and a certificate is built only when all of them answered with the same
//! durable decision. While one is missing the publication is repeated for
//! the missing members only, a bounded number of times per call, and the
//! call then ends with an error that names them. Nothing was certified: the
//! committed relocation is still the bare learner allocation and a later
//! call repeats this stage.
//!
//! One fact set per phase. A member that prepared refuses another fact set
//! for the same phase. The fact set is therefore built only when every
//! member materialized its participant, so it always lists every new member
//! and a coordinator without memory computes the same set again from the
//! members' retained participants. A member that cannot materialize ends
//! the round before any member is asked to prepare.
//!
//! What this stage cannot do. It runs after the source cut: every old voter
//! holds its permanent promise, so the handoff cannot be aborted here; it
//! can only be repeated. The repetition is not unbounded on the members'
//! side: a participant retains at most 64 exact requests and 8 preparation
//! attempts for its lifetime and evicts none. A missing member that is
//! unreachable costs the other members nothing (the observation is not
//! retained, and this object asks them to materialize once); a missing
//! member that answers its preparation and then fails its publication uses
//! one of its 8 attempts per repetition.
use super::*;
use crate::distribution_range_handoff::{
    NativeRemoteJointDecisionCertificateRound, NativeRemoteNewOperationV1, NativeRemoteNewRound,
};

/// Publications towards the missing members inside one call.
const DECISION_PUBLICATION_ROUNDS: usize = 2;

/// One decision exchange with one member, as this coordinator, with its
/// time reported to the move trace.
fn decision_exchange(
    transport: &TcpClusterRelocationTransport,
    metadata_leader: &ClusterNodeId,
    recipient: &ClusterNodeId,
    manifest: &NativeRemoteImageManifestV1,
    phase: Uuid,
    operation: NativeRemoteNewOperationV1,
    token: &CancellationToken,
) -> Result<crate::distribution_transport::AuthenticatedNativeRemoteNewFrame> {
    let started = Instant::now();
    let name = match &operation {
        NativeRemoteNewOperationV1::Materialize => "materialize",
        NativeRemoteNewOperationV1::Observe => "observe",
        NativeRemoteNewOperationV1::PrepareDecision { .. } => "prepare",
        NativeRemoteNewOperationV1::PublishDecision { .. } => "publish",
        NativeRemoteNewOperationV1::ObserveDecision => "observe-decision",
    };
    let answer = transport.remote_joint_decision_exchange(
        metadata_leader,
        recipient,
        manifest,
        phase,
        operation,
        token,
    );
    trace(started, || {
        format!(
            "decision {name} of {recipient}: {}",
            match &answer {
                Ok(_) => "ok".to_string(),
                Err(error) => format!("error {error}"),
            }
        )
    });
    answer
}

impl RemoteJoinedHandoffBackend {
    fn decision_exchange(
        &self,
        metadata_leader: &ClusterNodeId,
        recipient: &ClusterNodeId,
        manifest: &NativeRemoteImageManifestV1,
        phase: Uuid,
        operation: NativeRemoteNewOperationV1,
        token: &CancellationToken,
    ) -> Result<crate::distribution_transport::AuthenticatedNativeRemoteNewFrame> {
        decision_exchange(
            &self.transport,
            metadata_leader,
            recipient,
            manifest,
            phase,
            operation,
            token,
        )
    }

    /// Test only: see `decision_publish_fault`.
    #[cfg(test)]
    pub(crate) fn test_decision_publish_fault(
        mut self,
        fault: Arc<Mutex<Option<ClusterNodeId>>>,
    ) -> Self {
        self.decision_publish_fault = Some(fault);
        self
    }

    /// The read-only observation of every publisher's durable decision. The
    /// certificate is built only when every one answered; otherwise the
    /// members that did not, each with its refusal. An authenticated but
    /// inconsistent observation refuses the whole collection.
    fn observe_decision_on_every_member(
        &self,
        plan: Arc<NativeJointHandoffPlan>,
        manifest: &NativeRemoteImageManifestV1,
        phase: Uuid,
        publishers: &[ClusterNodeId],
        members: &RemoteMembers,
        parent: &CancellationToken,
        token: &CancellationToken,
    ) -> Result<
        std::result::Result<NativeRemoteJointDecisionCertificate, Vec<(ClusterNodeId, String)>>,
    > {
        let mut round =
            NativeRemoteJointDecisionCertificateRound::begin(plan, manifest.clone(), phase, token)?;
        let mut missing = Vec::new();
        // One member after the other, although the observation is read-only:
        // each exchange takes a metadata read on the metadata leader for this
        // coordinator and one for the answering member, and the leader
        // admits four at a time. Sent together, an observation was refused
        // as "bounded acquisition capacity is busy" in a measured move; a
        // refused observation counts as a missing decision, which costs the
        // member a repeated preparation (it has eight for its lifetime) and
        // can fail the step. The four observations take about 0.15 s.
        for id in publishers {
            match self.decision_exchange(
                &members.metadata_leader,
                id,
                manifest,
                phase,
                NativeRemoteNewOperationV1::ObserveDecision,
                token,
            ) {
                Ok(frame) => round.record(frame)?,
                Err(error) => {
                    parent.check()?;
                    missing.push((id.clone(), format!("observe decision: {error}")));
                }
            }
        }
        if missing.is_empty() {
            return Ok(Ok(round.finish()?));
        }
        Ok(Err(missing))
    }

    /// The fact set of this phase: the one this object already used, or the
    /// set of every new member's materialized participant. Every publisher
    /// must answer; see the module documentation.
    fn decision_fact_set(
        &mut self,
        plan: &Arc<NativeJointHandoffPlan>,
        manifest: &NativeRemoteImageManifestV1,
        phase: Uuid,
        publishers: &[ClusterNodeId],
        members: &RemoteMembers,
        token: &CancellationToken,
    ) -> Result<NativeRemoteNewFactSetV1> {
        if let Some((retained, facts)) = &self.decision_facts {
            if *retained == phase {
                return Ok(facts.clone());
            }
        }
        let mut round = NativeRemoteNewRound::begin(plan.clone(), manifest.clone(), phase, token)?;
        for id in publishers {
            let frame = self
                .decision_exchange(
                    &members.metadata_leader,
                    id,
                    manifest,
                    phase,
                    NativeRemoteNewOperationV1::Materialize,
                    token,
                )
                .map_err(|error| {
                    remote_error(format!(
                        "joint decision: member {id} did not materialize its participant, so no member was asked to prepare or publish in this round: {error}"
                    ))
                })?;
            if manifest.new_members.contains_key(id) {
                round.record(frame)?;
            }
        }
        let facts = round.finish()?.facts().clone();
        self.decision_facts = Some((phase, facts.clone()));
        Ok(facts)
    }

    /// Prepare and publish the decision on the members in `missing` only.
    /// A member that fails is reported, never fatal: the observation that
    /// follows is what counts.
    #[allow(clippy::too_many_arguments)]
    fn publish_decision_to(
        &mut self,
        plan: &Arc<NativeJointHandoffPlan>,
        manifest: &NativeRemoteImageManifestV1,
        phase: Uuid,
        publishers: &[ClusterNodeId],
        missing: &[ClusterNodeId],
        members: &RemoteMembers,
        token: &CancellationToken,
    ) -> Result<BTreeMap<ClusterNodeId, String>> {
        let facts = self.decision_fact_set(plan, manifest, phase, publishers, members, token)?;
        let mut failures = BTreeMap::new();
        // Every preparation before any publication. A member publishes over
        // the fact set it fetches itself, and that set must show the
        // preparation (the pin handoff) of EVERY new member; a member asked
        // to publish while another new member is not yet prepared refuses
        // with "requires actual participant pin handoff", and its
        // preparation attempt, of which a participant has eight for its
        // lifetime, is spent for nothing. This is the order of the
        // transport's own decision round.
        let mut prepared = Vec::new();
        for id in missing {
            match self.decision_exchange(
                &members.metadata_leader,
                id,
                manifest,
                phase,
                NativeRemoteNewOperationV1::PrepareDecision {
                    facts: facts.clone(),
                },
                token,
            ) {
                Ok(_) => prepared.push(id),
                Err(error) => {
                    failures.insert(id.clone(), format!("prepare: {error}"));
                }
            }
        }
        for id in prepared {
            #[cfg(test)]
            if self
                .decision_publish_fault
                .as_ref()
                .is_some_and(|fault| fault.lock().as_ref() == Some(id))
            {
                failures.insert(
                    id.clone(),
                    "publish: test fault: the request was not sent".into(),
                );
                continue;
            }
            // A lost reply may still be durable: the observation decides.
            if let Err(error) = self.decision_exchange(
                &members.metadata_leader,
                id,
                manifest,
                phase,
                NativeRemoteNewOperationV1::PublishDecision {
                    facts: facts.clone(),
                },
                token,
            ) {
                failures.insert(id.clone(), format!("publish: {error}"));
            }
        }
        Ok(failures)
    }

    /// Retain the decision certificate once the decision is durable on every
    /// old voter (the retiring source included) and on the target. A
    /// decision that is already durable everywhere is found by the read-only
    /// observation alone, which publishes nothing.
    pub(super) fn decide_on_every_member(
        &mut self,
        members: &RemoteMembers,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        let Some(manifest) = self.manifest.clone() else {
            return Err(remote_error(
                "the joint decision needs the retained plan and source manifest of the installed image",
            ));
        };
        let mut publishers = members.old_voters.clone();
        publishers.push(self.target.clone());
        let token = bounded(parent, DECISION_ROUND);
        let mut published: BTreeMap<ClusterNodeId, String> = BTreeMap::new();
        let mut round = 0;
        loop {
            let Some(plan) = self.plan.clone() else {
                return Err(remote_error(
                    "the joint decision needs the retained plan and source manifest of the installed image",
                ));
            };
            let phase = Self::decision_phase(&plan, &manifest)?;
            let missing = match self
                .observe_decision_on_every_member(
                    plan,
                    &manifest,
                    phase,
                    &publishers,
                    members,
                    parent,
                    &token,
                )
                .map_err(|error| stage_error("joint decision observation", error))?
            {
                Ok(certificate) => {
                    self.certificate = Some(certificate);
                    return Ok(());
                }
                Err(missing) => missing,
            };
            parent.check()?;
            if round == DECISION_PUBLICATION_ROUNDS || token.check().is_err() {
                let named = missing
                    .iter()
                    .map(|(id, observed)| match published.get(id) {
                        Some(failure) => format!("{id} ({failure}; {observed})"),
                        None => format!("{id} ({observed})"),
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(remote_error(format!(
                    "the joint decision is not durable on every member that needs it; missing: {named}. Nothing was certified: every new member and the retiring source must hold the decision before the membership stages, because none of them can obtain it afterwards. The source cut is done, so the move cannot be aborted; this stage is repeated on the next step"
                )));
            }
            round += 1;
            // The publication issues new work: it starts under a live plan.
            let fresh = self
                .renewed_plan(members, parent)
                .map_err(|error| stage_error("joint decision metadata", error))?;
            let ids = missing.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>();
            published = self
                .publish_decision_to(&fresh, &manifest, phase, &publishers, &ids, members, &token)
                .map_err(|error| stage_error("joint decision", error))?;
        }
    }
}
