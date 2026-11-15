//! Finite post-Certified phase intent DATA. These values never construct a
//! host, Frozen, local preparation/quorum, Open, or writer permission.
use super::*;
use crate::distribution_consensus::{
    HostMetadataReadObservation, MetadataReadParticipant, MetadataRecoveryRolePurpose,
};
use crate::distribution_data::ActualNativeJointMetadataPoint;
use crate::distribution_range_image::NativeRemoteImageManifestV1;
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use parking_lot::Mutex;
use std::sync::mpsc::{self, SyncSender};
use std::time::Instant;
mod release_request;
pub(crate) use release_request::NativeRemoteReleaseRequestAdmission;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum NativeRemoteServingOperationV1 {
    Prepare,
    ObservePrepared,
    PublishBridge,
    ObserveBridge,
    Open,
    ObserveOpen,
    Release,
    ObserveReleased,
    PublishPolicy,
    ObserveServing,
    ObserveDecision,
    ResolvePrepared,
    ResolveBridge,
    ReconcileOpen,
    ReconcileRelease,
}
impl NativeRemoteServingOperationV1 {
    pub(crate) fn readonly(self) -> bool {
        matches!(
            self,
            Self::ObservePrepared
                | Self::ObserveBridge
                | Self::ObserveOpen
                | Self::ObserveReleased
                | Self::ObserveServing
                | Self::ObserveDecision
                | Self::ResolvePrepared
                | Self::ResolveBridge
        )
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteServingRequestV1 {
    pub version: u32,
    pub manifest: NativeRemoteImageManifestV1,
    pub phase_id: Uuid,
    pub decision_sha256: String,
    pub original_operation: Uuid,
    pub sequence: u64,
    pub operation: NativeRemoteServingOperationV1,
    pub business: Uuid,
    pub challenge: Uuid,
    pub budget_ms: u64,
}
impl NativeRemoteServingRequestV1 {
    pub(crate) fn require_shape(&self) -> Result<()> {
        self.manifest.require_shape()?;
        crate::distribution_range_image::bounded_json(self, 2 * 1024 * 1024)?;
        if self.version != 1
            || self.phase_id.is_nil()
            || self.original_operation.is_nil()
            || self.sequence == 0
            || self.business.is_nil()
            || self.challenge.is_nil()
            || !(1..=5000).contains(&self.budget_ms)
            || self.decision_sha256.len() != 64
            || !self
                .decision_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid("invalid bounded native serving request"));
        }
        Ok(())
    }
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        self.require_shape()?;
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(self.budget_ms));
        token.check()?;
        Ok(token)
    }
    pub(crate) fn purpose(&self) -> MetadataRecoveryRolePurpose {
        MetadataRecoveryRolePurpose::NativeJointServingPhase {
            relocation_id: self.manifest.relocation_id,
            session_id: self.manifest.handoff_session,
            nonce: self.manifest.nonce,
            decision_sha256: self.decision_sha256.clone(),
            readonly: self.operation.readonly(),
        }
    }
    pub(crate) fn hash(&self) -> Result<String> {
        use sha2::Digest;
        Ok(hex::encode(sha2::Sha256::digest(
            crate::distribution_range_image::bounded_json(self, 2 * 1024 * 1024)?,
        )))
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteServingReplyV1 {
    pub request: NativeRemoteServingRequestV1,
    pub node_id: ClusterNodeId,
    /// Protected facts read from the SAME retained actual owner. This DATA is
    /// never converted to the existing local service-owning quorum types.
    pub actual: NativeRemoteServingPointV1,
    pub metadata_term: u64,
    pub metadata_read_index: u64,
    pub metadata_context: Uuid,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteServingPointV1 {
    pub phase_id: Uuid,
    pub original_participant_sha256: String,
    pub decision_sha256: String,
    pub native_decision_record_sha256: String,
    pub remote_provenance_sha256: String,
    /// Original successful owned decision-publication candidate digest.
    pub catalogue_decision_sha256: String,
    /// Actual latest healthy full head independently read at this observation.
    pub current_catalogue_sha256: String,
    pub native_decision_xid: u64,
    pub native_database_open: String,
    pub store_open: Uuid,
    pub preparation: Option<crate::distribution_range_handoff::NativeJointPreparedServingVoteData>,
    pub native_bridge_sha256: Option<String>,
    pub catalogue_activation_sha256: Option<String>,
    pub original_frozen_owner: Uuid,
    pub actual_dropped_open: bool,
    pub actual_writer_installed: bool,
}
impl NativeRemoteServingReplyV1 {
    pub(crate) fn require_request(&self, request: &NativeRemoteServingRequestV1) -> Result<()> {
        request.require_shape()?;
        crate::distribution_range_image::bounded_json(&self.actual, 64 * 1024)?;
        let point = &self.actual;
        let digest = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        let eligible = request.manifest.new_members.contains_key(&self.node_id)
            || (request.operation == NativeRemoteServingOperationV1::ObserveDecision
                && request.manifest.old_members.contains_key(&self.node_id));
        if self.request.hash()? != request.hash()?
            || !eligible
            || point.phase_id != request.phase_id
            || point.decision_sha256 != request.decision_sha256
            || !digest(&point.original_participant_sha256)
            || !digest(&point.native_decision_record_sha256)
            || !digest(&point.remote_provenance_sha256)
            || !digest(&point.catalogue_decision_sha256)
            || !digest(&point.current_catalogue_sha256)
            || point.native_decision_xid == 0
            || point.native_database_open.is_empty()
            || point.native_database_open.len() > 256
            || point.store_open.is_nil()
            || point.original_frozen_owner.is_nil()
            || self.metadata_context.is_nil()
            || self.metadata_term == 0
            || self.metadata_read_index == 0
            || point
                .native_bridge_sha256
                .as_ref()
                .is_some_and(|s| !digest(s))
            || point
                .catalogue_activation_sha256
                .as_ref()
                .is_some_and(|s| !digest(s))
            || (point.catalogue_activation_sha256.is_some() && point.native_bridge_sha256.is_none())
            || (point.actual_dropped_open && point.catalogue_activation_sha256.is_none())
            || (point.actual_writer_installed && !point.actual_dropped_open)
        {
            return Err(invalid(
                "serving actual response substituted request/member/native phase",
            ));
        }
        Ok(())
    }
}

/// Constructed only by an actual authenticated recipient dispatch after a
/// real witness, independent OWN and attached MStore observation. It can
/// outlive the socket only within the SAME original absolute ingress window.
pub(in crate::distribution_native_host) struct NativeRemoteServingIngress {
    pub(in crate::distribution_native_host) peer: MetadataReadParticipant,
    pub(in crate::distribution_native_host) request: NativeRemoteServingRequestV1,
    pub(in crate::distribution_native_host) role: Arc<RecoveryRoleAdmission>,
    pub(in crate::distribution_native_host) own: Arc<HostMetadataReadObservation>,
    pub(in crate::distribution_native_host) business: Uuid,
    pub(in crate::distribution_native_host) point: Arc<ActualNativeJointMetadataPoint>,
    pub(in crate::distribution_native_host) token: CancellationToken,
    effect_entered: bool,
    open_effect: Option<Arc<std::sync::atomic::AtomicBool>>,
    hash: String,
    reply: SyncSender<Result<NativeRemoteServingReplyV1>>,
}
impl NativeRemoteServingIngress {
    pub(in crate::distribution_native_host) fn require_request(&self) -> Result<()> {
        self.token.check()?;
        if self.token.deadline().is_none() {
            return Err(invalid(
                "serving ingress has no original absolute server deadline",
            ));
        }
        self.role.require_live_request()?;
        self.role
            .require_own_host_metadata(&self.own, self.business)?;
        self.point.require_request(&self.own, self.business)?;
        self.point.require_current_memory()?;
        if self.role.purpose() != &self.request.purpose()
            || self.role.initiator_node_id() != self.peer.node_id()
            || self.role.current_range().id != self.request.manifest.activation_range.id
            || self.role.current_range().epoch != self.request.manifest.activation_range.epoch
        {
            return Err(invalid(
                "serving ingress changed actual authenticated phase witness",
            ));
        }
        Ok(())
    }
    pub(in crate::distribution_native_host) fn require_request_memory(&self) -> Result<()> {
        self.token.check()?;
        self.role.require_live_request()?;
        self.own.require_live_request(self.business)?;
        self.point.require_current_memory()
    }
    pub(in crate::distribution_native_host) fn mark_effect_entered(&mut self) -> Result<()> {
        self.require_request()?;
        if self.request.operation.readonly() {
            return Err(invalid(
                "readonly serving intent cannot enter native effects",
            ));
        }
        self.effect_entered = true;
        Ok(())
    }
}
#[derive(Default)]
struct State {
    binding: Option<(ClusterNodeId, u64, String, Uuid, String, Uuid)>,
    admitted: u64,
    pending: Option<NativeRemoteServingIngress>,
    processing: bool,
    unknown: Option<Unknown>,
    returned: u64,
    returned_chain: sha2::Sha256,
    exhausted: bool,
}
struct Unknown {
    request: NativeRemoteServingRequestV1,
    request_sha256: String,
    original_deadline: Instant,
    actual_error_kind: String,
}
#[derive(Default)]
pub(crate) struct NativeRemoteServingQueue {
    state: Mutex<State>,
}
impl NativeRemoteServingQueue {
    /// Sealed values are supplied by the real TLS dispatch, never request
    /// selectors. Admission itself does not enter any native effect.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::distribution_native_host) fn receive(
        &self,
        peer: MetadataReadParticipant,
        request: NativeRemoteServingRequestV1,
        role: RecoveryRoleAdmission,
        own: HostMetadataReadObservation,
        business: Uuid,
        point: Arc<ActualNativeJointMetadataPoint>,
        token: CancellationToken,
    ) -> Result<NativeRemoteServingReplyV1> {
        request.require_shape()?;
        let hash = request.hash()?;
        let (send, recv) = mpsc::sync_channel(1);
        let work = NativeRemoteServingIngress {
            peer,
            request,
            role: Arc::new(role),
            own: Arc::new(own),
            business,
            point,
            token,
            effect_entered: false,
            open_effect: None,
            hash: hash.clone(),
            reply: send,
        };
        work.require_request()?;
        let binding = (
            work.peer.node_id().clone(),
            work.peer.member_incarnation(),
            work.peer
                .certificate_sha256()
                .ok_or_else(|| invalid("serving ingress needs actual TLS peer"))?
                .to_string(),
            work.request.original_operation,
            work.request.manifest.checksum_sha256.clone(),
            work.request.phase_id,
        );
        let wait = work.token.clone();
        {
            let mut state = self
                .state
                .try_lock()
                .ok_or_else(|| invalid("serving ingress admission busy"))?;
            let readonly = work.request.operation.readonly();
            if (!readonly && state.binding.as_ref().is_some_and(|b| b != &binding))
                || state.exhausted
                || state.pending.is_some()
                || state.processing
                || (!readonly && work.request.sequence <= state.admitted)
                || (state.unknown.as_ref().is_some_and(|old| {
                    !readonly
                        && !(((work.request.operation
                            == NativeRemoteServingOperationV1::ReconcileOpen
                            && matches!(
                                old.request.operation,
                                NativeRemoteServingOperationV1::Open
                                    | NativeRemoteServingOperationV1::ReconcileOpen
                            ))
                            || (work.request.operation
                                == NativeRemoteServingOperationV1::ReconcileRelease
                                && matches!(
                                    old.request.operation,
                                    NativeRemoteServingOperationV1::Release
                                        | NativeRemoteServingOperationV1::ReconcileRelease
                                )))
                            && state.binding.as_ref() == Some(&binding))
                }))
            {
                return Err(invalid(
                    "serving ingress refuses replay/rebinding/concurrent or unresolved mutation",
                ));
            }
            work.require_request()?;
            // Readonly requests independently reobserve the SAME actual
            // participant under their own fresh role. They never bind/rebind
            // the mutation coordinator, spend/reset its sequence or resolve
            // an unknown IO merely by returning DATA.
            if !readonly {
                state.binding.get_or_insert(binding);
                state.admitted = work.request.sequence;
            }
            state.pending = Some(work);
        }
        loop {
            wait.check()?;
            let remaining = wait
                .deadline()
                .and_then(|d| d.checked_duration_since(Instant::now()))
                .ok_or(BicDbError::QueryTimedOut)?;
            match recv.recv_timeout(remaining.min(Duration::from_millis(5))) {
                Ok(result) => return result,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(invalid(
                        "serving original owner ended before returning actual outcome",
                    ))
                }
            }
        }
    }
    pub(in crate::distribution_native_host) fn take(
        &self,
    ) -> Result<Option<NativeRemoteServingIngress>> {
        let mut state = self
            .state
            .try_lock()
            .ok_or_else(|| invalid("serving owner queue busy"))?;
        if state.processing {
            return Ok(None);
        }
        let work = state.pending.take();
        state.processing = work.is_some();
        Ok(work)
    }
    pub(in crate::distribution_native_host) fn complete(
        &self,
        work: NativeRemoteServingIngress,
        result: Result<NativeRemoteServingReplyV1>,
    ) -> Result<()> {
        {
            let mut state = self.state.lock();
            state.processing = false;
            state.returned = match state.returned.checked_add(1) {
                Some(n) => n,
                None => {
                    state.exhausted = true;
                    u64::MAX
                }
            };
            use sha2::Digest;
            let error_kind = result
                .as_ref()
                .err()
                .map(|e| format!("{:?}", std::mem::discriminant(e)));
            let return_fact = crate::distribution_range_image::bounded_json(
                &(
                    work.request.sequence,
                    &work.hash,
                    work.request.operation,
                    result.is_ok(),
                    &error_kind,
                ),
                4096,
            );
            match return_fact {
                Ok(bytes) => state.returned_chain.update(bytes),
                Err(_) => state.exhausted = true, // Preserve the original API result.
            }
            let actual_effect = work.effect_entered
                || work
                    .open_effect
                    .as_ref()
                    .is_some_and(|v| v.load(std::sync::atomic::Ordering::Acquire));
            if result.is_err()
                && actual_effect
                && !work.request.operation.readonly()
                && !(matches!(
                    work.request.operation,
                    NativeRemoteServingOperationV1::ReconcileOpen
                        | NativeRemoteServingOperationV1::ReconcileRelease
                ) && state.unknown.is_some())
            {
                state.unknown = Some(Unknown {
                    request: work.request,
                    request_sha256: work.hash,
                    original_deadline: work.token.deadline().unwrap(),
                    actual_error_kind: error_kind.unwrap(),
                });
            }
            // Merely observing DATA cannot clear an unknown native/helper IO.
            // Resolution needs a distinct same-owner protected outcome producer.
        }
        let _ = work.reply.try_send(result);
        Ok(())
    }
}

/// Producer lifetime captured ONLY from the actual serialized native host's
/// retained process. It cannot revive any request, hold a freeze, or provide
/// metadata/native authority. The private host constructor is the sole caller.
pub(crate) struct NativeRemotePreparationProcess {
    process: CancellationToken,
    service: std::sync::Weak<Mutex<crate::distribution_data::ClusterDataNodeService>>,
    node: ClusterNodeId,
    database_open: String,
    participant_sha256: String,
    decision_sha256: String,
    participant: std::sync::Weak<crate::distribution_range_handoff::NativeRemoteNewParticipant>,
}
impl NativeRemotePreparationProcess {
    pub(in crate::distribution_native_host) fn capture_from_original(
        process: &CancellationToken,
        service: &Arc<Mutex<crate::distribution_data::ClusterDataNodeService>>,
        participant: &Arc<crate::distribution_range_handoff::NativeRemoteNewParticipant>,
        decision_sha256: &str,
    ) -> Result<Arc<Self>> {
        process.check()?;
        let snapshot = participant.snapshot();
        let actual = service
            .try_lock()
            .ok_or_else(|| invalid("remote preparation actual producer service busy"))?;
        let database = actual.database();
        let db = database
            .try_read()
            .ok_or_else(|| invalid("remote preparation actual producer DB busy"))?;
        if actual.node_id() != &snapshot.node_id || db.incarnation_id() != snapshot.database_open {
            return Err(invalid(
                "remote preparation producer substituted actual service/DB",
            ));
        }
        let result = Arc::new(Self {
            process: process.child(),
            service: Arc::downgrade(service),
            node: snapshot.node_id.clone(),
            database_open: snapshot.database_open.clone(),
            participant_sha256: snapshot.checksum_sha256.clone(),
            decision_sha256: decision_sha256.into(),
            participant: Arc::downgrade(participant),
        });
        result.require_memory()?;
        Ok(result)
    }
    pub(crate) fn require_memory(&self) -> Result<()> {
        self.process.check()?;
        if self.service.upgrade().is_none()
            || self.participant_sha256.is_empty()
            || self.decision_sha256.len() != 64
            || self.participant.upgrade().is_none()
        {
            return Err(invalid(
                "remote preparation original producer service ended",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_service(
        &self,
        service: &crate::distribution_data::ClusterDataNodeService,
    ) -> Result<()> {
        self.require_memory()?;
        let actual = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("remote preparation original service ended"))?;
        if !std::ptr::eq(service, actual.data_ptr()) || service.node_id() != &self.node {
            return Err(invalid(
                "remote preparation substituted original actual service",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_original_native(
        &self,
        db: &crate::BicDb,
        committed: &crate::distribution_range_handoff::CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        self.require_memory()?;
        let actual = self
            .participant
            .upgrade()
            .ok_or_else(|| invalid("remote preparation original phase owner ended"))?;
        if actual.snapshot().checksum_sha256 != self.participant_sha256
            || committed.data().checksum_sha256() != self.decision_sha256
        {
            return Err(invalid(
                "remote preparation substituted original actual phase/decision",
            ));
        }
        actual.require_original_committed_for_serving(db, committed.native_receipt())
    }
    pub(crate) fn require_participant(&self, sha: &str) -> Result<()> {
        self.require_memory()?;
        if sha != self.participant_sha256 {
            return Err(invalid(
                "remote preparation changed original participant owner",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_prepared(
        &self,
        prepared: &crate::db::NativeJointServingPreparation,
    ) -> Result<()> {
        self.require_memory()?;
        if prepared.local_node_id() != &self.node
            || prepared.native_database_incarnation() != self.database_open
            || prepared.decision_sha256() != self.decision_sha256
        {
            return Err(invalid(
                "remote preparation changed producer native node/open",
            ));
        }
        Ok(())
    }
    /// A NEW request remains bounded by all of its original parents/window.
    /// No raw process token, deadline setter or cancellation handle escapes.
    pub(crate) fn bound_request(&self, parent: &CancellationToken) -> CancellationToken {
        parent.child_linked_to(&self.process)
    }
    pub(crate) fn memory_lifetime(&self) -> crate::cancellation::CancellationLifetime {
        self.process.retain_memory_lifetime()
    }
}

/// A borrowed actual authenticated Prepare step. Unlike process lifetime or
/// intent DATA, only this sealed original ingress can admit its native effect.
pub(crate) struct NativeRemotePreparationAdmission<'a> {
    work: &'a NativeRemoteServingIngress,
    process: Arc<NativeRemotePreparationProcess>,
}
impl NativeRemoteServingIngress {
    pub(in crate::distribution_native_host) fn admit_native_preparation(
        &mut self,
        process: Arc<NativeRemotePreparationProcess>,
    ) -> Result<NativeRemotePreparationAdmission<'_>> {
        self.require_request()?;
        process.require_memory()?;
        if self.request.operation != NativeRemoteServingOperationV1::Prepare {
            return Err(invalid(
                "only actual authenticated Prepare can admit remote native preparation",
            ));
        }
        self.mark_effect_entered()?;
        Ok(NativeRemotePreparationAdmission {
            work: self,
            process,
        })
    }
}
impl NativeRemotePreparationAdmission<'_> {
    pub(crate) fn require_memory_current(&self) -> Result<()> {
        self.work.token.check()?;
        self.work.role.require_live_request()?;
        self.work.own.require_live_request(self.work.business)?;
        self.work.point.require_current_memory()?;
        self.process.require_memory()
    }
    pub(crate) fn require_current(
        &self,
        committed: &crate::distribution_range_handoff::CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        self.work.require_request()?;
        self.require_memory_current()?;
        if committed.data().checksum_sha256() != self.work.request.decision_sha256
            || committed.native_receipt().local_node_id() != self.work.role.local_node_id()
            || committed.data().promise().session_id != self.work.request.manifest.handoff_session
            || committed.data().promise().nonce != self.work.request.manifest.nonce
        {
            return Err(invalid(
                "remote Prepare changed original authenticated native phase/body",
            ));
        }
        committed.require_current_metadata_only()?;
        self.require_memory_current()
    }
    pub(crate) fn bound_effect_request(&self) -> CancellationToken {
        self.process
            .bound_request(&self.work.token)
            .child_linked_to(&self.work.own.original_request_cancellation())
            .child_with_deadline(self.work.role.deadline().min(self.work.own.deadline()))
    }
    pub(crate) fn process(&self) -> &Arc<NativeRemotePreparationProcess> {
        &self.process
    }
    #[cfg(test)]
    pub(crate) fn test_cancel_original_server_request(&self) -> serde_json::Value {
        let now = Instant::now();
        let before = serde_json::json!({"server_remaining_us":self.work.token.deadline().map(|d|d.saturating_duration_since(now).as_micros()),
            "role_remaining_us":self.work.role.deadline().saturating_duration_since(now).as_micros(),
            "own_remaining_us":self.work.own.deadline().saturating_duration_since(now).as_micros(),
            "role_context":self.work.role.context_id(),"own_context":self.work.own.context_id(),
            "server_cancelled":self.work.token.is_cancel_requested(),"producer_process_live":self.process.require_memory().is_ok()});
        self.work.token.cancel();
        serde_json::json!({"cut":"actual native preparation helper returned known commit; before postcheck/pin/owner",
            "before_cancel":before,"original_server_cancelled":self.work.token.is_cancel_requested(),
            "producer_process_live_after_cancel":self.process.require_memory().is_ok(),"client_timeout_is_not_this_cut":true})
    }
}

/// Separate actual authenticated readonly step. There is no conversion to the
/// effect admission accepted by native preparation writes.
pub(crate) struct NativeRemotePreparationReadAdmission<'a> {
    actual: NativeRemotePreparationAdmission<'a>,
}
impl NativeRemoteServingIngress {
    pub(in crate::distribution_native_host) fn admit_known_preparation_read(
        &self,
        process: Arc<NativeRemotePreparationProcess>,
    ) -> Result<NativeRemotePreparationReadAdmission<'_>> {
        self.require_request()?;
        process.require_memory()?;
        if !matches!(
            self.request.operation,
            NativeRemoteServingOperationV1::ObservePrepared
                | NativeRemoteServingOperationV1::ResolvePrepared
        ) {
            return Err(invalid(
                "known preparation read requires actual readonly ObservePrepared",
            ));
        }
        Ok(NativeRemotePreparationReadAdmission {
            actual: NativeRemotePreparationAdmission {
                work: self,
                process,
            },
        })
    }
}
impl NativeRemotePreparationReadAdmission<'_> {
    pub(crate) fn require_current(
        &self,
        committed: &crate::distribution_range_handoff::CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        self.actual.require_current(committed)
    }
    pub(crate) fn require_memory_current(&self) -> Result<()> {
        self.actual.require_memory_current()
    }
    pub(crate) fn bound_read_request(&self) -> CancellationToken {
        self.actual.bound_effect_request()
    }
    pub(crate) fn process(&self) -> &Arc<NativeRemotePreparationProcess> {
        self.actual.process()
    }
}

impl NativeRemoteServingQueue {
    /// Only a genuine fresh protected read of the SAME original private
    /// preparation owner may resolve that original Prepare's unknown return.
    /// No DATA reply, generic Resume, new coordinator or other stage can do so.
    pub(in crate::distribution_native_host) fn resolve_original_preparation(
        &self,
        work: &NativeRemoteServingIngress,
        participant: &crate::distribution_range_handoff::NativeRemoteNewParticipant,
        preparation: &crate::distribution_data::NativeRemoteServicePreparation,
        observation: &crate::distribution_range_handoff::NativeRemotePreparedObservation,
    ) -> Result<()> {
        if work.request.operation != NativeRemoteServingOperationV1::ResolvePrepared {
            return Err(invalid(
                "preparation resolution needs fresh authenticated readonly ObservePrepared",
            ));
        }
        work.require_request()?;
        observation.require_current()?;
        observation.require_original_remote_participant(&participant.snapshot().checksum_sha256)?;
        preparation.require_resolved_observation(observation)?;
        let binding = (
            work.peer.node_id().clone(),
            work.peer.member_incarnation(),
            work.peer
                .certificate_sha256()
                .ok_or_else(|| invalid("preparation resolution actual TLS peer absent"))?
                .to_string(),
            work.request.original_operation,
            work.request.manifest.checksum_sha256.clone(),
            work.request.phase_id,
        );
        let version = work.point.try_publication_guard()?;
        let mut state = self
            .state
            .try_lock()
            .ok_or_else(|| invalid("preparation resolution original queue busy"))?;
        work.require_request_memory()?;
        observation.require_request_memory()?;
        if state.binding.as_ref() != Some(&binding) {
            return Err(invalid(
                "preparation resolution cannot rebind another original coordinator operation",
            ));
        }
        let removed = if let Some(original) = state.unknown.as_ref() {
            if state.binding.as_ref() != Some(&binding)
                || original.request.operation != NativeRemoteServingOperationV1::Prepare
                || original.request.original_operation != work.request.original_operation
                || original.request.phase_id != work.request.phase_id
                || original.request.decision_sha256 != work.request.decision_sha256
                || work.request.sequence <= state.admitted
            {
                return Err(invalid("preparation resolution changed original actor/operation/stage or reused admitted sequence"));
            }
            state.unknown.take()
        } else {
            None
        };
        drop(state);
        drop(version);
        // Original return classification remains in the rolling outcome hash;
        // DATA destruction never occurs under the short publication guard.
        drop(removed);
        Ok(())
    }
}

/// Only the original authenticated PublishBridge request can enter bridge WAL.
/// Readonly phase observations cannot be converted into this admission.
pub(crate) struct NativeRemoteBridgeRequestAdmission<'a> {
    actual: NativeRemotePreparationAdmission<'a>,
}
impl NativeRemoteServingIngress {
    pub(in crate::distribution_native_host) fn admit_bridge(
        &self,
        process: Arc<NativeRemotePreparationProcess>,
    ) -> Result<NativeRemoteBridgeRequestAdmission<'_>> {
        self.require_request()?;
        process.require_memory()?;
        if self.request.operation != NativeRemoteServingOperationV1::PublishBridge {
            return Err(invalid(
                "only actual authenticated PublishBridge may enter bridge WAL",
            ));
        }
        if !self.effect_entered {
            return Err(invalid(
                "actual host must retain bridge effect entry before admission",
            ));
        }
        Ok(NativeRemoteBridgeRequestAdmission {
            actual: NativeRemotePreparationAdmission {
                work: self,
                process,
            },
        })
    }
}
impl NativeRemoteBridgeRequestAdmission<'_> {
    pub(crate) fn require_current(
        &self,
        committed: &crate::distribution_range_handoff::CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        self.actual.require_current(committed)
    }
    pub(crate) fn require_memory_current(&self) -> Result<()> {
        self.actual.require_memory_current()
    }
    pub(crate) fn bound_request(&self) -> CancellationToken {
        self.actual.bound_effect_request()
    }
}

/// A fresh readonly original bridge observation; never accepted by native WAL.
pub(crate) struct NativeRemoteBridgeReadRequestAdmission<'a> {
    actual: NativeRemotePreparationAdmission<'a>,
}
impl NativeRemoteServingIngress {
    pub(in crate::distribution_native_host) fn admit_bridge_read(
        &self,
        process: Arc<NativeRemotePreparationProcess>,
    ) -> Result<NativeRemoteBridgeReadRequestAdmission<'_>> {
        self.require_request()?;
        process.require_memory()?;
        if !matches!(
            self.request.operation,
            NativeRemoteServingOperationV1::ObserveBridge
                | NativeRemoteServingOperationV1::ResolveBridge
        ) {
            return Err(invalid(
                "known bridge read requires fresh authenticated readonly bridge operation",
            ));
        }
        Ok(NativeRemoteBridgeReadRequestAdmission {
            actual: NativeRemotePreparationAdmission {
                work: self,
                process,
            },
        })
    }
}
impl NativeRemoteBridgeReadRequestAdmission<'_> {
    pub(crate) fn require_current(
        &self,
        committed: &crate::distribution_range_handoff::CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        self.actual.require_current(committed)
    }
    pub(crate) fn require_memory_current(&self) -> Result<()> {
        self.actual.require_memory_current()
    }
    pub(crate) fn bound_request(&self) -> CancellationToken {
        self.actual.bound_effect_request()
    }
}
#[cfg(test)]
impl NativeRemoteBridgeRequestAdmission<'_> {
    pub(crate) fn test_cancel_original_server_request(&self) -> serde_json::Value {
        let mut event = self.actual.test_cancel_original_server_request();
        event["cut"] = serde_json::json!(
            "actual bridge native API returned known commit, before receipt/postchecks"
        );
        event
    }
}

impl NativeRemoteServingQueue {
    pub(in crate::distribution_native_host) fn resolve_original_bridge(
        &self,
        work: &NativeRemoteServingIngress,
        participant: &crate::distribution_range_handoff::NativeRemoteNewParticipant,
        bridge: &crate::distribution_data::NativeRemoteServiceBridge,
        admission: &crate::distribution_range_handoff::NativeRemoteBridgeReadAdmission<'_>,
    ) -> Result<()> {
        if work.request.operation != NativeRemoteServingOperationV1::ResolveBridge {
            return Err(invalid(
                "bridge resolution needs fresh authenticated readonly ResolveBridge",
            ));
        }
        work.require_request()?;
        admission.require_live()?;
        admission.require_participant(&participant.snapshot().checksum_sha256)?;
        bridge.require_resolution()?;
        let binding = (
            work.peer.node_id().clone(),
            work.peer.member_incarnation(),
            work.peer
                .certificate_sha256()
                .ok_or_else(|| invalid("bridge resolution actual TLS peer absent"))?
                .to_string(),
            work.request.original_operation,
            work.request.manifest.checksum_sha256.clone(),
            work.request.phase_id,
        );
        let version = work.point.try_publication_guard()?;
        let mut state = self
            .state
            .try_lock()
            .ok_or_else(|| invalid("bridge resolution original queue busy"))?;
        work.require_request_memory()?;
        admission.require_live()?;
        if state.binding.as_ref() != Some(&binding) {
            return Err(invalid(
                "bridge resolution cannot rebind another original coordinator operation",
            ));
        }
        let removed = if let Some(original) = state.unknown.as_ref() {
            if state.binding.as_ref() != Some(&binding)
                || original.request.operation != NativeRemoteServingOperationV1::PublishBridge
                || original.request.original_operation != work.request.original_operation
                || original.request.phase_id != work.request.phase_id
                || original.request.decision_sha256 != work.request.decision_sha256
                || work.request.sequence <= state.admitted
            {
                return Err(invalid("bridge resolution changed original actor/operation/stage or reused admitted sequence"));
            }
            state.unknown.take()
        } else {
            None
        };
        drop(state);
        drop(version);
        // Original return classification remains in the rolling outcome hash;
        // DATA destruction never occurs under the short publication guard.
        drop(removed);
        Ok(())
    }
}

/// Owned witness of ONE actual authenticated Open ingress. Arc ownership keeps
/// the SAME Role/OWN values, never clones/reconstructs their authority. Every
/// check retains their original cancellation and earliest absolute deadline.
pub(crate) struct NativeRemoteOpenRequestAdmission {
    role: Arc<RecoveryRoleAdmission>,
    own: Arc<HostMetadataReadObservation>,
    point: Arc<ActualNativeJointMetadataPoint>,
    business: Uuid,
    phase: Uuid,
    decision: String,
    retry: bool,
    effect: Arc<std::sync::atomic::AtomicBool>,
    session: Uuid,
    nonce: Uuid,
    process: Arc<NativeRemotePreparationProcess>,
    token: CancellationToken,
}
impl NativeRemoteServingIngress {
    pub(in crate::distribution_native_host) fn admit_open(
        &mut self,
        process: Arc<NativeRemotePreparationProcess>,
    ) -> Result<NativeRemoteOpenRequestAdmission> {
        self.require_request()?;
        process.require_memory()?;
        if !matches!(
            self.request.operation,
            NativeRemoteServingOperationV1::Open | NativeRemoteServingOperationV1::ReconcileOpen
        ) {
            return Err(invalid(
                "only actual admitted Open may enter owned catalogue activation",
            ));
        }
        if self.open_effect.is_some() {
            return Err(invalid(
                "Open ingress already issued original effect witness",
            ));
        }
        let effect = Arc::new(std::sync::atomic::AtomicBool::new(false));
        self.open_effect = Some(effect.clone());
        Ok(NativeRemoteOpenRequestAdmission {
            effect,
            role: self.role.clone(),
            own: self.own.clone(),
            point: self.point.clone(),
            business: self.business,
            phase: self.request.phase_id,
            decision: self.request.decision_sha256.clone(),
            retry: self.request.operation == NativeRemoteServingOperationV1::ReconcileOpen,
            session: self.request.manifest.handoff_session,
            nonce: self.request.manifest.nonce,
            token: process
                .bound_request(&self.token)
                .child_linked_to(&self.own.original_request_cancellation())
                .child_with_deadline(self.role.deadline().min(self.own.deadline())),
            process,
        })
    }
}
impl NativeRemoteOpenRequestAdmission {
    pub(crate) fn require_memory_current(&self) -> Result<()> {
        self.token.check()?;
        self.process.require_memory()?;
        self.role.require_live_request()?;
        self.own.require_live_request(self.business)?;
        self.point.require_current_memory()
    }
    pub(crate) fn require_current(
        &self,
        fresh: &crate::distribution_range_handoff::CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        self.require_memory_current()?;
        self.role
            .require_own_host_metadata(&self.own, self.business)?;
        self.point.require_request(&self.own, self.business)?;
        fresh.require_current_metadata_only()?;
        if self.phase.is_nil()
            || self.decision != fresh.data().checksum_sha256()
            || self.session != fresh.data().promise().session_id
            || self.nonce != fresh.data().promise().nonce
            || self.role.local_node_id() != fresh.native_receipt().local_node_id()
        {
            return Err(invalid(
                "remote Open changed original authenticated native phase/body/recipient",
            ));
        }
        self.require_memory_current()
    }
    pub(crate) fn bound_request(&self) -> CancellationToken {
        self.token.child()
    }
}

impl NativeRemoteOpenRequestAdmission {
    pub(crate) fn reconcile_actual_preparation(
        &self,
        service: &mut crate::distribution_data::ClusterDataNodeService,
        actual: &crate::distribution_data::ActualServiceNativeJointPreparedEvidence,
        native: &crate::db::NativeJointServingBridgeReceipt,
    ) -> Result<()> {
        self.require_memory_current()?;
        self.process.require_service(service)?;
        actual.require_remote_preparation_participant(&self.process.participant_sha256)?;
        self.role
            .require_own_host_metadata(&self.own, self.business)?;
        self.point.require_request(&self.own, self.business)?;
        if native.data().checksum_sha256() != self.decision
            || native.local_node_id() != self.role.local_node_id()
        {
            return Err(invalid(
                "Open reconciliation changed actual native recipient/decision",
            ));
        }
        if self.retry {
            actual.require_remote_open_intent(service, native)?;
        }
        self.note_actual_effect_entered()?;
        service.reconcile_native_joint_open_phase_with_point(
            &self.own,
            self.business,
            native,
            actual,
            &self.token,
            Some(&self.point),
        )?;
        self.require_memory_current()
    }
}

impl NativeRemoteServingQueue {
    pub(in crate::distribution_native_host) fn resolve_original_open(
        &self,
        work: &NativeRemoteServingIngress,
        admission: &crate::distribution_range_handoff::NativeJointOpenAdmission,
        observed: &crate::distribution_range_handoff::NativeRemoteOpenObservation,
        activation: &crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt,
    ) -> Result<()> {
        if work.request.operation != NativeRemoteServingOperationV1::ReconcileOpen {
            return Err(invalid(
                "Open resolution requires actual mutating same-owner ReconcileOpen",
            ));
        }
        work.require_request()?;
        admission.require_operation_request_memory()?;
        admission.require_original_remote_open_observation(observed)?;
        let expected = activation.activation_sha256()?;
        let binding = (
            work.peer.node_id().clone(),
            work.peer.member_incarnation(),
            work.peer
                .certificate_sha256()
                .ok_or_else(|| invalid("Open resolution actual TLS peer absent"))?
                .to_string(),
            work.request.original_operation,
            work.request.manifest.checksum_sha256.clone(),
            work.request.phase_id,
        );
        let mut removed = None;
        // Same physical/native/Store proof remains borrowed through the final
        // short metadata gate and this exact queue's one-way resolution.
        observed.inspect(
            |scope, _store, _head, actual, _owner| {
                if actual.as_deref() != Some(expected.as_str())
                    || scope.native().checksum_sha256() != activation.native_bridge_sha256()
                {
                    return Err(invalid(
                        "Open resolution changed actual owned native/catalogue activation",
                    ));
                }
                Ok(())
            },
            || {
                work.require_request_memory()?;
                admission.require_operation_request_memory()?;
                let mut state = self
                    .state
                    .try_lock()
                    .ok_or_else(|| invalid("Open resolution original queue busy"))?;
                if state.binding.as_ref() != Some(&binding)
                    || work.request.sequence != state.admitted
                {
                    return Err(invalid(
                        "Open resolution replaced original actor/admitted mutation",
                    ));
                }
                // A lost wire reply may follow an actual completed queue
                // return. The protected same-owner read above still applies;
                // absence here does not infer that the old RPC returned Ok.
                let Some(old) = state.unknown.as_ref() else {
                    return Ok(());
                };
                if state.binding.as_ref() != Some(&binding)
                    || !matches!(
                        old.request.operation,
                        NativeRemoteServingOperationV1::Open
                            | NativeRemoteServingOperationV1::ReconcileOpen
                    )
                    || old.request.original_operation != work.request.original_operation
                    || old.request.phase_id != work.request.phase_id
                    || old.request.decision_sha256 != work.request.decision_sha256
                    || work.request.sequence != state.admitted
                {
                    return Err(invalid(
                        "Open resolution replaced original actor/phase/admitted mutation",
                    ));
                }
                removed = state.unknown.take();
                Ok(())
            },
        )?;
        drop(removed);
        Ok(())
    }
}

#[cfg(test)]
impl NativeRemoteServingQueue {
    pub(in crate::distribution_native_host) fn test_open_queue_state(&self) -> serde_json::Value {
        let state = self.state.lock();
        serde_json::json!({"admitted":state.admitted,"processing":state.processing,
            "unknown":state.unknown.as_ref().map(|u|serde_json::json!({"operation":u.request.operation,"sequence":u.request.sequence,
                // SAME stored absolute monotonic Instant, not a remaining
                // duration or a new request/window. Debug DATA is meaningful
                // only for these same-process diagnostic comparisons.
                "original_deadline_monotonic_debug":format!("{:?}",u.original_deadline),
                "original_deadline_expired":Instant::now()>=u.original_deadline,"original_error_kind":u.actual_error_kind,
                "request_sha256":u.request_sha256})),"returned":state.returned})
    }
}

impl NativeRemoteOpenRequestAdmission {
    pub(crate) fn note_actual_effect_entered(&self) -> Result<()> {
        self.require_memory_current()?;
        self.effect
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}
