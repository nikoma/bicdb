//! Distinct authenticated durable completion DECISIONS. This is not a closed
//! native-ready/read/source barrier: activated replicas may already be at R+1.
use super::*;
use crate::distribution_range_consensus::{PreparedNativeRecoveryReady, RangeWriteStore};
use crate::CancellationToken;

#[derive(Debug)]
pub(crate) struct NativeCompletionQuorumRound {
    role: RecoveryRoleAdmission,
    scope: RecoveryFenceScope,
    cancellation: CancellationToken,
    observations: BTreeMap<ClusterNodeId, NativeCompletionObservation>,
    poisoned: bool,
}
impl NativeCompletionQuorumRound {
    pub(crate) fn begin(role: RecoveryRoleAdmission, parent: &CancellationToken) -> Result<Self> {
        let cancellation = parent.child_with_deadline(role.deadline());
        cancellation.check()?;
        if !matches!(
            role.purpose(),
            MetadataRecoveryRolePurpose::NormalRecovery
                | MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) || role.member_credentials().is_empty()
            || role.member_credentials().len() > 64
            || role.member_credentials().len()
                != role
                    .current_range()
                    .replicas
                    .iter()
                    .filter(|r| r.role == crate::RangeReplicaRole::Voter)
                    .count()
            || !role.member_credentials().contains_key(role.local_node_id())
        {
            return Err(invalid(
                "completion quorum requires an actual current normal voter",
            ));
        }
        let scope = RecoveryFenceScope::from_role(&role)?;
        Ok(Self {
            role,
            scope,
            cancellation,
            observations: BTreeMap::new(),
            poisoned: false,
        })
    }
    pub(crate) fn record(&mut self, observation: NativeCompletionObservation) -> Result<()> {
        if self.poisoned {
            return Err(invalid("completion quorum round is poisoned"));
        }
        let result = self.record_inner(observation);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn record_inner(&mut self, observation: NativeCompletionObservation) -> Result<()> {
        self.cancellation.check()?;
        self.role.require_live_request()?;
        observation.require_live()?;
        let reply = observation.reply();
        reply.scope.require_role(&self.role)?;
        if !reply.scope.same_session(&self.scope)
            || self.observations.contains_key(&reply.node_id)
            || reply.observed_metadata_term < self.scope.metadata_term
            || reply.observed_metadata_read_index < self.scope.metadata_read_index
        {
            return Err(invalid(
                "duplicate, wrong-session or old-anchor completion voter",
            ));
        }
        let member = self
            .role
            .member_credentials()
            .get(&reply.node_id)
            .ok_or_else(|| invalid("completion peer is not a current voter"))?;
        let peer = observation.authenticated_peer();
        if peer.node_id() != &reply.node_id
            || peer.member_incarnation() != member.incarnation
            || reply.member_incarnation != member.incarnation
        {
            return Err(invalid(
                "completion peer actual identity/incarnation changed",
            ));
        }
        match (
            &member.active_certificate_sha256,
            &member.pending_certificate_sha256,
        ) {
            (None, None) if peer.plaintext_loopback() => {}
            (a, p)
                if peer
                    .certificate_sha256()
                    .is_some_and(|h| a.as_deref() == Some(h) || p.as_deref() == Some(h)) => {}
            _ => {
                return Err(invalid(
                    "completion peer pin differs from independently fresh membership",
                ))
            }
        }
        if self.observations.values().any(|o| {
            let old = o.reply();
            old.range_index != reply.range_index
                || old.logical_genesis_sha256 != reply.logical_genesis_sha256
                || old.logical_history_sha256 != reply.logical_history_sha256
        }) {
            return Err(invalid(
                "durable same-session completion decisions conflict",
            ));
        }
        self.observations.insert(reply.node_id.clone(), observation);
        Ok(())
    }
    pub(crate) fn is_ready(&self) -> bool {
        !self.poisoned && self.observations.len() >= self.role.member_credentials().len() / 2 + 1
    }
    pub(crate) fn has_activated_decision(&self) -> bool {
        !self.poisoned
            && self
                .observations
                .values()
                .any(|o| o.reply().stage == NativeCompletionStage::ActivatedAnchor)
    }
    pub(crate) fn finish_selection(
        self,
        parent: &CancellationToken,
    ) -> Result<NativeCompletionSelection> {
        parent.check()?;
        if self.poisoned {
            return Err(invalid(
                "poisoned completion observations cannot become a fallback",
            ));
        }
        if self.is_ready()
            && self.cancellation.check().is_ok()
            && self.role.require_live_request().is_ok()
        {
            return Ok(NativeCompletionSelection::Quorum(self.finish()?));
        }
        if self.has_activated_decision() {
            let mut observations = self.observations;
            let node = observations
                .iter()
                .find(|(_, o)| o.reply().stage == NativeCompletionStage::ActivatedAnchor)
                .unwrap()
                .0
                .clone();
            let observation = observations.remove(&node).unwrap();
            return Ok(NativeCompletionSelection::Decision(
                ActivatedNativeCompletionDecision::from_recorded(&self.role, observation, parent)?,
            ));
        }
        let point = self.observations.values().next().map(|o| {
            let r = o.reply();
            (
                r.range_index,
                r.logical_genesis_sha256.clone(),
                r.logical_history_sha256.clone(),
            )
        });
        Ok(NativeCompletionSelection::Constraint(
            NativeCompletionConstraint {
                scope: self.scope,
                local_node: self.role.local_node_id().clone(),
                local_incarnation: self.role.local_member_incarnation(),
                point,
                cancellation: parent.child(),
            },
        ))
    }
    pub(crate) fn finish(self) -> Result<NativeCompletionQuorum> {
        self.cancellation.check()?;
        self.role.require_live_request()?;
        if !self.is_ready() {
            return Err(invalid("no distinct current durable completion quorum"));
        }
        Ok(NativeCompletionQuorum {
            role: self.role,
            scope: self.scope,
            cancellation: self.cancellation,
            observations: self.observations,
        })
    }
}
/// A partial observation can constrain another real quorum, never grant one.
/// In particular a known durable higher/different point cannot disappear when
/// another collection path is tried under the original request budget.
#[derive(Debug)]
pub(crate) enum NativeCompletionSelection {
    Quorum(NativeCompletionQuorum),
    Decision(ActivatedNativeCompletionDecision),
    Constraint(NativeCompletionConstraint),
}
#[derive(Debug)]
pub(crate) struct NativeCompletionConstraint {
    scope: RecoveryFenceScope,
    local_node: ClusterNodeId,
    local_incarnation: u64,
    point: Option<(u64, String, String)>,
    cancellation: CancellationToken,
}
impl NativeCompletionConstraint {
    pub(crate) fn require_closed_quorum(&self, quorum: &ClosedNativeReadyQuorum) -> Result<()> {
        self.cancellation.check()?;
        quorum.require_live()?;
        if !self.scope.same_session(quorum.scope())
            || quorum.role().local_node_id() != &self.local_node
            || quorum.role().local_member_incarnation() != self.local_incarnation
            || self.point.as_ref().is_some_and(|(r, h0, h)| {
                *r != quorum.range_index()
                    || h0 != quorum.logical_genesis_sha256()
                    || h != quorum.logical_history_sha256()
            })
        {
            return Err(invalid(
                "closed native quorum contradicts an observed durable completion decision",
            ));
        }
        Ok(())
    }
}
#[derive(Debug)]
pub(crate) struct NativeCompletionQuorum {
    role: RecoveryRoleAdmission,
    scope: RecoveryFenceScope,
    cancellation: CancellationToken,
    observations: BTreeMap<ClusterNodeId, NativeCompletionObservation>,
}
impl NativeCompletionQuorum {
    pub(crate) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        self.role.require_live_request()
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    fn point(&self) -> &NativeCompletionReply {
        self.observations
            .values()
            .next()
            .expect("sealed nonempty quorum")
            .reply()
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.point().range_index
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.point().logical_genesis_sha256
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.point().logical_history_sha256
    }
    pub(crate) fn distinct_member_ids(&self) -> impl Iterator<Item = &ClusterNodeId> {
        self.observations.keys()
    }
    pub(crate) fn require_recipient_role(&self, fresh: &RecoveryRoleAdmission) -> Result<()> {
        self.require_live()?;
        self.scope.require_role(fresh)?;
        if self.role.local_node_id() != fresh.local_node_id()
            || self.role.local_member_incarnation() != fresh.local_member_incarnation()
        {
            return Err(invalid(
                "completion quorum belongs to another actual local recipient",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_local_prepared(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<()> {
        self.require_prepared_identity_point(prepared, fresh)?;
        prepared.require_current_under_role(db, store, fresh)?;
        Ok(())
    }
    pub(crate) fn require_prepared_identity_point(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<()> {
        self.require_recipient_role(fresh)?;
        if !prepared.scope().same_session(&self.scope)
            || prepared.range_index() != self.range_index()
            || prepared.logical_genesis_sha256() != self.logical_genesis_sha256()
            || prepared.logical_history_sha256() != self.logical_history_sha256()
        {
            return Err(invalid(
                "actual local materialization differs from durable completion decision",
            ));
        }
        Ok(())
    }
    pub(crate) fn decision_provenance(&self) -> Result<NativeCompletionDecisionProvenance> {
        self.require_live()?;
        let voters = self
            .observations
            .iter()
            .map(|(n, o)| {
                let r = o.reply();
                (
                    n.clone(),
                    NativeCompletionVoterProvenance {
                        member_incarnation: r.member_incarnation,
                        peer_certificate_sha256: o
                            .authenticated_peer()
                            .certificate_sha256()
                            .map(str::to_owned),
                        peer_plaintext_loopback: o.authenticated_peer().plaintext_loopback(),
                        stage: r.stage,
                        challenge: r.challenge,
                        store_open_id: r.store_open_id,
                        native_database_incarnation: r.native_database_incarnation.clone(),
                        native_serving_sha256: r.native_serving_sha256.clone(),
                        native_receipt_sha256: r.native_receipt_sha256.clone(),
                        activation_decision_sha256: r.activation_decision_sha256.clone(),
                        observed_metadata_context_id: r.observed_metadata_context_id,
                        observed_metadata_term: r.observed_metadata_term,
                        observed_metadata_read_index: r.observed_metadata_read_index,
                    },
                )
            })
            .collect();
        let projection = NativeCompletionDecisionProvenance {
            scope: self.scope.clone(),
            range_index: self.range_index(),
            logical_genesis_sha256: self.logical_genesis_sha256().to_owned(),
            logical_history_sha256: self.logical_history_sha256().to_owned(),
            local_recipient: self.role.local_node_id().clone(),
            local_member_incarnation: self.role.local_member_incarnation(),
            metadata_context_id: self.role.context_id(),
            metadata_term: self.role.metadata_term(),
            metadata_read_index: self.role.metadata_read_index(),
            voters,
        };
        projection.validate(&self.scope, self.role.member_credentials())?;
        Ok(projection)
    }
    pub(crate) fn decision_sha256(&self) -> Result<String> {
        self.decision_provenance()?.checksum_sha256()
    }
}

// Prior activated decision constructor stays PRIVATE to this actual recorded-observation module.
#[derive(Debug)]
pub(crate) struct ActivatedNativeCompletionDecision {
    scope: RecoveryFenceScope,
    local_node: ClusterNodeId,
    local_incarnation: u64,
    observation: NativeCompletionObservation,
    cancellation: CancellationToken,
}
impl ActivatedNativeCompletionDecision {
    fn from_recorded(
        role: &RecoveryRoleAdmission,
        observation: NativeCompletionObservation,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        observation.require_live()?;
        if observation.reply().stage != NativeCompletionStage::ActivatedAnchor
            || observation.reply().activation_decision_sha256.is_none()
        {
            return Err(invalid(
                "a merely completed/prepared vote cannot attest a prior activation decision",
            ));
        }
        // Called ONLY after the actual quorum round authenticated current pins,
        // incarnation/scope/metadata anchor and checked all observed conflicts.
        let proof = Self {
            scope: RecoveryFenceScope::from_role(role)?,
            local_node: role.local_node_id().clone(),
            local_incarnation: role.local_member_incarnation(),
            observation,
            cancellation: parent.child(),
        };
        proof.require_recipient_role(role)?;
        Ok(proof)
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.observation.reply().range_index
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.observation.reply().logical_genesis_sha256
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.observation.reply().logical_history_sha256
    }
    pub(crate) fn activation_decision_sha256(&self) -> &str {
        self.observation
            .reply()
            .activation_decision_sha256
            .as_deref()
            .expect("sealed activated decision")
    }
    pub(crate) fn require_recipient_role(&self, fresh: &RecoveryRoleAdmission) -> Result<()> {
        self.cancellation.check()?;
        self.scope.require_role(fresh)?;
        let source = self.observation.authenticated_peer();
        let reply = self.observation.reply();
        let member = fresh
            .member_credentials()
            .get(source.node_id())
            .ok_or_else(|| invalid("activated decision source is not a current voter"))?;
        let pin = match (
            &member.active_certificate_sha256,
            &member.pending_certificate_sha256,
        ) {
            (None, None) => source.plaintext_loopback(),
            (a, p) => source
                .certificate_sha256()
                .is_some_and(|s| a.as_deref() == Some(s) || p.as_deref() == Some(s)),
        };
        if self.local_node != *fresh.local_node_id()
            || self.local_incarnation != fresh.local_member_incarnation()
            || member.incarnation != source.member_incarnation()
            || member.incarnation != reply.member_incarnation
            || !pin
            || fresh.metadata_term() < reply.observed_metadata_term
            || fresh.metadata_read_index() < reply.observed_metadata_read_index
        {
            return Err(invalid(
                "prior activated decision source/recipient or fresh metadata scope changed",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_local_prepared(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<()> {
        self.require_recipient_role(fresh)?;
        prepared.require_current_under_role(db, store, fresh)?;
        if !prepared.scope().same_session(&self.scope)
            || prepared.range_index() != self.range_index()
            || prepared.logical_genesis_sha256() != self.logical_genesis_sha256()
            || prepared.logical_history_sha256() != self.logical_history_sha256()
        {
            return Err(invalid("local actual prepared prefix does not match the prior immutable activation decision"));
        }
        Ok(())
    }
}
