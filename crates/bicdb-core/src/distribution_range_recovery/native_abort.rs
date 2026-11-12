//! Candidate and permanent-promise observations are sealed at actual local or
//! authenticated transport boundaries. A decoded audit/voter list is no proof.
use super::*;
use crate::db::range_image_native::abort_history::NativeNoEffectIntentRecord;
use crate::distribution_range_consensus::native_abort::{LocalAbortCandidate, LocalAbortPromise};
use crate::CancellationToken;
use std::sync::Arc;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeAbortRequest {
    pub scope: RecoveryFenceScope,
    pub range_index: u64,
    pub origin_node_id: ClusterNodeId,
    pub candidate_sha256: Option<String>,
    pub challenge: Uuid,
    pub budget_ms: u64,
}
impl NativeAbortRequest {
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        self.scope.validate()?;
        if self.range_index == 0
            || self.challenge.is_nil()
            || self.budget_ms == 0
            || self.budget_ms > 5000
            || !matches!(
                self.scope.purpose,
                MetadataRecoveryRolePurpose::NormalRecovery
            )
            || self
                .candidate_sha256
                .as_ref()
                .is_some_and(|h| h.len() != 64)
        {
            return Err(invalid("invalid bounded same-voter native abort request"));
        }
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(self.budget_ms));
        token.check()?;
        Ok(token)
    }
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeAbortCandidateReply {
    pub request: NativeAbortRequest,
    pub record: NativeNoEffectIntentRecord,
    pub node_id: ClusterNodeId,
    pub member_incarnation: u64,
    pub store_open_id: Uuid,
    pub native_database_incarnation: String,
    pub observed_metadata_context_id: Uuid,
    pub observed_metadata_term: u64,
    pub observed_metadata_read_index: u64,
}
#[derive(Debug)]
pub(crate) struct NativeAbortCandidateObservation {
    reply: NativeAbortCandidateReply,
    peer: MetadataReadParticipant,
    cancellation: CancellationToken,
}
fn require_member(
    role: &RecoveryRoleAdmission,
    peer: &MetadataReadParticipant,
    id: &ClusterNodeId,
    inc: u64,
) -> Result<()> {
    let expected = role
        .member_credentials()
        .get(id)
        .ok_or_else(|| invalid("abort observation is not from a current voter"))?;
    if peer.node_id() != id || inc != expected.incarnation || peer.member_incarnation() != inc {
        return Err(invalid(
            "abort peer incarnation/member differs from actual fresh MQ",
        ));
    }
    match (
        &expected.active_certificate_sha256,
        &expected.pending_certificate_sha256,
    ) {
        (None, None) if peer.plaintext_loopback() => Ok(()),
        (a, p)
            if peer
                .certificate_sha256()
                .is_some_and(|h| a.as_deref() == Some(h) || p.as_deref() == Some(h)) =>
        {
            Ok(())
        }
        _ => Err(invalid(
            "abort peer certificate differs from actual fresh MQ",
        )),
    }
}
impl NativeAbortCandidateObservation {
    pub(crate) fn from_local(
        actual: LocalAbortCandidate,
        peer: MetadataReadParticipant,
    ) -> Result<Self> {
        let (reply, cancellation) = actual.into_evidence();
        cancellation.check()?;
        if peer.node_id() != &reply.node_id
            || peer.member_incarnation() != reply.member_incarnation
            || reply.node_id != reply.request.origin_node_id
            || reply.store_open_id.is_nil()
            || reply.native_database_incarnation.is_empty()
            || reply.observed_metadata_context_id.is_nil()
            || reply.observed_metadata_term < reply.request.scope.metadata_term
            || reply.observed_metadata_read_index < reply.request.scope.metadata_read_index
        {
            return Err(invalid(
                "actual local abort candidate identity/anchor is inconsistent",
            ));
        }
        Ok(Self {
            reply,
            peer,
            cancellation,
        })
    }
    pub(crate) fn require_for_role(
        &self,
        role: &RecoveryRoleAdmission,
        request: &NativeAbortRequest,
    ) -> Result<()> {
        self.cancellation.check()?;
        request.scope.require_role(role)?;
        if !self.reply.request.scope.same_session(&request.scope)
            || self.reply.request.range_index != request.range_index
            || self.reply.request.origin_node_id != request.origin_node_id
            || self.reply.record.command.index != request.range_index
            || self.reply.record.command.range_id != request.scope.range_id
            || request
                .candidate_sha256
                .as_deref()
                .is_some_and(|h| h != self.reply.record.checksum_sha256())
        {
            return Err(invalid(
                "abort candidate belongs to another command/session",
            ));
        }
        self.reply.record.domain.require_role(role)?;
        require_member(
            role,
            &self.peer,
            &self.reply.node_id,
            self.reply.member_incarnation,
        )
    }
    pub(crate) fn record(&self) -> &NativeNoEffectIntentRecord {
        &self.reply.record
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.reply.request.scope
    }
    pub(crate) fn sha256(&self) -> &str {
        self.reply.record.checksum_sha256()
    }
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum NativeAbortPromiseDisposition {
    Permanent { promise_sha256: String },
    Irreversible { command_sha256: String },
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeAbortPromiseReply {
    pub request: NativeAbortRequest,
    pub node_id: ClusterNodeId,
    pub member_incarnation: u64,
    pub store_open_id: Uuid,
    pub native_database_incarnation: String,
    pub candidate_sha256: String,
    pub disposition: NativeAbortPromiseDisposition,
    pub observed_metadata_context_id: Uuid,
    pub observed_metadata_term: u64,
    pub observed_metadata_read_index: u64,
}
#[derive(Debug)]
pub(crate) struct NativeAbortPromiseObservation {
    reply: NativeAbortPromiseReply,
    peer: MetadataReadParticipant,
    cancellation: CancellationToken,
}
impl NativeAbortPromiseObservation {
    pub(crate) fn from_local(
        actual: LocalAbortPromise,
        peer: MetadataReadParticipant,
    ) -> Result<Self> {
        let (reply, cancellation) = actual.into_evidence();
        cancellation.check()?;
        if peer.node_id() != &reply.node_id
            || peer.member_incarnation() != reply.member_incarnation
            || reply.store_open_id.is_nil()
            || reply.native_database_incarnation.is_empty()
            || reply.candidate_sha256.len() != 64
            || match &reply.disposition {
                NativeAbortPromiseDisposition::Permanent { promise_sha256 } => {
                    promise_sha256.len() != 64
                }
                NativeAbortPromiseDisposition::Irreversible { command_sha256 } => {
                    command_sha256.len() != 64
                }
            }
            || reply.observed_metadata_context_id.is_nil()
            || reply.observed_metadata_term < reply.request.scope.metadata_term
            || reply.observed_metadata_read_index < reply.request.scope.metadata_read_index
        {
            return Err(invalid(
                "actual abort promise identity/anchor is inconsistent",
            ));
        }
        Ok(Self {
            reply,
            peer,
            cancellation,
        })
    }
}
#[derive(Debug)]
pub(crate) struct NativeAbortPromiseRound {
    role: RecoveryRoleAdmission,
    candidate: Arc<NativeAbortCandidateObservation>,
    request: NativeAbortRequest,
    cancellation: CancellationToken,
    votes: BTreeMap<ClusterNodeId, NativeAbortPromiseReply>,
    failed: bool,
}
#[derive(Debug)]
pub(crate) struct NativeAbortQuorum {
    role: RecoveryRoleAdmission,
    candidate: Arc<NativeAbortCandidateObservation>,
    request: NativeAbortRequest,
    cancellation: CancellationToken,
    votes: BTreeMap<ClusterNodeId, NativeAbortPromiseReply>,
}
impl NativeAbortPromiseRound {
    pub(crate) fn begin(
        role: RecoveryRoleAdmission,
        candidate: Arc<NativeAbortCandidateObservation>,
        request: NativeAbortRequest,
        parent: &CancellationToken,
    ) -> Result<Self> {
        let cancellation = request
            .cancellation(parent)?
            .child_with_deadline(role.deadline());
        candidate.require_for_role(&role, &request)?;
        if request.candidate_sha256.as_deref() != Some(candidate.sha256()) {
            return Err(invalid(
                "abort round requires exact actual candidate digest",
            ));
        }
        Ok(Self {
            role,
            candidate,
            request,
            cancellation,
            votes: BTreeMap::new(),
            failed: false,
        })
    }
    pub(crate) fn record(&mut self, observation: NativeAbortPromiseObservation) -> Result<()> {
        let result = (|| {
            self.cancellation.check()?;
            observation.cancellation.check()?;
            let reply = observation.reply;
            if matches!(
                &reply.disposition,
                NativeAbortPromiseDisposition::Irreversible { .. }
            ) {
                return Err(invalid("actual abort round observed irreversible/ever-Commit evidence; resolve decision instead"));
            }
            require_member(
                &self.role,
                &observation.peer,
                &reply.node_id,
                reply.member_incarnation,
            )?;
            if !reply.request.scope.same_session(&self.request.scope)
                || reply.request.range_index != self.request.range_index
                || reply.request.origin_node_id != self.request.origin_node_id
                || reply.candidate_sha256 != self.candidate.sha256()
                || self.votes.contains_key(&reply.node_id)
            {
                return Err(invalid("foreign/duplicate native abort promise"));
            }
            self.votes.insert(reply.node_id.clone(), reply);
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    pub(crate) fn has_quorum(&self) -> bool {
        !self.failed && self.votes.len() >= self.role.member_credentials().len() / 2 + 1
    }
    pub(crate) fn finish(self) -> Result<NativeAbortQuorum> {
        self.cancellation.check()?;
        self.role.require_live_request()?;
        if !self.has_quorum() {
            return Err(invalid(
                "actual distinct permanent AbortPromise quorum unavailable",
            ));
        }
        Ok(NativeAbortQuorum {
            role: self.role,
            candidate: self.candidate,
            request: self.request,
            cancellation: self.cancellation,
            votes: self.votes,
        })
    }
}
impl NativeAbortQuorum {
    pub(crate) fn require_recipient_role(&self, fresh: &RecoveryRoleAdmission) -> Result<()> {
        self.cancellation.check()?;
        self.request.scope.require_role(fresh)?;
        if fresh.local_node_id() != self.role.local_node_id()
            || fresh.local_member_incarnation() != self.role.local_member_incarnation()
            || fresh.metadata_term()
                < self
                    .votes
                    .values()
                    .map(|v| v.observed_metadata_term)
                    .max()
                    .unwrap_or(0)
            || fresh.metadata_read_index()
                < self
                    .votes
                    .values()
                    .map(|v| v.observed_metadata_read_index)
                    .max()
                    .unwrap_or(0)
        {
            return Err(invalid(
                "abort quorum requires actual independently fresh same-local-recipient role",
            ));
        }
        self.candidate.record().domain.require_role(fresh)
    }
    pub(crate) fn candidate(&self) -> &NativeAbortCandidateObservation {
        &self.candidate
    }
    pub(crate) fn request(&self) -> &NativeAbortRequest {
        &self.request
    }
    pub(crate) fn audit(&self) -> &BTreeMap<ClusterNodeId, NativeAbortPromiseReply> {
        &self.votes
    }
}
