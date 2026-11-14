//! One actual authenticated, freshly admitted request on the ORIGINAL owner
//! thread. Sequence tombstones never acquire a new timer. No queued ACK/job.
use super::*;
use crate::distribution_consensus::{HostMetadataReadObservation, MetadataReadParticipant};
use crate::distribution_data::ActualNativeJointMetadataPoint;
use crate::distribution_native_host::NativeRegisteredReplicaAdmission;
use crate::distribution_range_handoff::NativeJointHandoffPlan;
use crate::distribution_range_recovery::HandoffRoleAdmission;
use std::cell::Cell;
use std::sync::mpsc::{self, SyncSender};
use std::sync::Weak;

pub(crate) struct NativeFirstReceiveIngress {
    pub(in crate::distribution_native_host) peer: MetadataReadParticipant,
    pub(in crate::distribution_native_host) request: NativeFirstReceiveRequestV1,
    pub(in crate::distribution_native_host) token: CancellationToken,
    pub(in crate::distribution_native_host) role: Option<HandoffRoleAdmission>,
    pub(in crate::distribution_native_host) own: Option<HostMetadataReadObservation>,
    pub(in crate::distribution_native_host) business: Uuid,
    pub(in crate::distribution_native_host) plan: Arc<NativeJointHandoffPlan>,
    pub(in crate::distribution_native_host) point: Arc<ActualNativeJointMetadataPoint>,
    pub(in crate::distribution_native_host) original: Arc<NativeRegisteredReplicaAdmission>,
    used: Cell<bool>,
    hash: String,
    queue: Weak<NativeFirstReceiveQueue>,
    reply: SyncSender<Result<NativeFirstReceiveReplyV1>>,
}
impl NativeFirstReceiveIngress {
    pub(in crate::distribution_native_host) fn mark_effect_entered(&self) {
        self.used.set(true);
    }
    pub(in crate::distribution_native_host) fn complete(
        self,
        result: Result<NativeFirstReceiveReplyV1>,
    ) {
        // ALL native/service/DB/journal guards have ended. The gate below owns
        // DATA only; actual image/Tx/Frozen custody stays in the real receiver.
        if let Some(queue) = self.queue.upgrade() {
            queue.record_return(
                &self.request,
                &self.hash,
                self.token.deadline().unwrap(),
                self.used.get(),
                &result,
            );
        }
        let _ = self.reply.try_send(result);
    }
}
#[derive(Clone, PartialEq, Eq)]
struct Binding {
    peer: (ClusterNodeId, u64, String),
    operation: Uuid,
    scope: String,
    source: ClusterNodeId,
}
struct Current {
    sequence: u64,
    hash: String,
    token: CancellationToken,
    processing: bool,
}
struct Returned {
    request: NativeFirstReceiveRequestV1,
    hash: String,
    deadline: Instant,
    classification: &'static str,
    error: Option<serde_json::Value>,
    reply: Option<NativeFirstReceiveReplyV1>,
}
struct QueueState {
    binding: Option<Binding>,
    admitted: u64,
    last_effect_admitted: u64,
    pending: Option<NativeFirstReceiveIngress>,
    current: Option<Current>,
    last: Option<Returned>,
    unknown: Option<Returned>,
    chain: Sha256,
    returned_count: u64,
    uncertain_count: u64,
}
pub(crate) struct NativeFirstReceiveQueue {
    state: Mutex<QueueState>,
}
impl NativeFirstReceiveQueue {
    pub(in crate::distribution_native_host) fn new() -> Self {
        Self {
            state: Mutex::new(QueueState {
                binding: None,
                admitted: 0,
                last_effect_admitted: 0,
                pending: None,
                current: None,
                last: None,
                unknown: None,
                chain: Sha256::new(),
                returned_count: 0,
                uncertain_count: 0,
            }),
        }
    }
    /// Denial only, after real TLS and sealed Fresh-owner checks. This neither
    /// admits nor returns a saved reply. The final current/binding/sequence
    /// checks below still run after the actual fresh Role/OWN admission.
    pub(in crate::distribution_native_host) fn deny_consumed_sequence(
        &self,
        request: &NativeFirstReceiveRequestV1,
    ) -> Result<()> {
        request.require_shape()?;
        let state = self
            .state
            .try_lock()
            .ok_or_else(|| invalid("FIRST ingress busy"))?;
        if request.sequence <= state.admitted {
            return Err(invalid(
                "FIRST request sequence already consumed; original deadline is not renewed",
            ));
        }
        Ok(())
    }
    /// Only the actual receiver's before-latch admission supplies these sealed
    /// Role/OWN/current-MStore objects. No decoder or MF constructs them.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::distribution_native_host) fn request_from_actual_admission(
        self: &Arc<Self>,
        peer: MetadataReadParticipant,
        request: NativeFirstReceiveRequestV1,
        token: CancellationToken,
        role: HandoffRoleAdmission,
        own: HostMetadataReadObservation,
        business: Uuid,
        plan: Arc<NativeJointHandoffPlan>,
        point: Arc<ActualNativeJointMetadataPoint>,
        original: Arc<NativeRegisteredReplicaAdmission>,
    ) -> Result<NativeFirstReceiveReplyV1> {
        token.check()?;
        request.require_shape()?;
        original.require_live()?;
        role.role().require_own_host_metadata(&own, business)?;
        plan.require_role(role.role())?;
        point.require_request(&own, business)?;
        point.require_external_current()?;
        if peer.plaintext_loopback()
            || peer.node_id() != role.role().initiator_node_id()
            || peer.member_incarnation() != role.role().initiator_incarnation()
            || peer.node_id() != &plan.old_range().leader
        {
            return Err(invalid("FIRST queue requires actual range-leader witness"));
        }
        let binding = Binding {
            peer: (
                peer.node_id().clone(),
                peer.member_incarnation(),
                peer.certificate_sha256()
                    .ok_or_else(|| invalid("FIRST TLS pin absent"))?
                    .into(),
            ),
            operation: request.original_operation,
            scope: plan.checksum_sha256()?,
            source: request.source.clone(),
        };
        let hash = hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(&request, 16 * 1024)?,
        ));
        let (send, receive) = mpsc::sync_channel(1);
        let sequence = request.sequence;
        {
            let mut state = self
                .state
                .try_lock()
                .ok_or_else(|| invalid("FIRST ingress busy"))?;
            if state
                .binding
                .as_ref()
                .is_some_and(|actual| actual != &binding)
            {
                return Err(invalid(
                    "FIRST changed original coordinator operation/receiver scope",
                ));
            }
            if sequence <= state.admitted {
                return Err(invalid(
                    "FIRST request sequence already consumed; original deadline is not renewed",
                ));
            }
            if state.current.is_some() {
                // Expiry does NOT prove a processing native call quiescent.
                return Err(invalid("FIRST original request is still pending/in flight"));
            }
            if let Some(unknown) = &state.unknown {
                let allowed = matches!(
                    request.operation,
                    NativeFirstReceiveOperationV1::ObserveLast { .. }
                ) || (matches!(
                    unknown.request.operation,
                    NativeFirstReceiveOperationV1::Start
                ) && matches!(
                    request.operation,
                    NativeFirstReceiveOperationV1::Describe
                ));
                if !allowed {
                    return Err(invalid(
                        "FIRST prior native outcome is unresolved; new mutation refused",
                    ));
                }
            }
            if state.binding.is_none()
                && !matches!(
                    request.operation,
                    NativeFirstReceiveOperationV1::Enroll
                        | NativeFirstReceiveOperationV1::ObserveLast { .. }
                )
            {
                return Err(invalid(
                    "FIRST original operation must enroll before effectful steps",
                ));
            }
            // Admission is irrevocable even when the ORIGINAL request expires
            // in the queue. There is no per-frame challenge set/total-step cap.
            state.binding = Some(binding);
            state.admitted = sequence;
            if !matches!(
                request.operation,
                NativeFirstReceiveOperationV1::ObserveLast { .. }
            ) {
                state.last_effect_admitted = sequence;
            }
            state.current = Some(Current {
                sequence,
                hash: hash.clone(),
                token: token.clone(),
                processing: false,
            });
            state.pending = Some(NativeFirstReceiveIngress {
                peer,
                request,
                token: token.clone(),
                role: Some(role),
                own: Some(own),
                business,
                plan,
                point,
                original,
                used: Cell::new(false),
                hash,
                queue: Arc::downgrade(self),
                reply: send,
            });
        }
        loop {
            token.check()?;
            let remaining = token
                .deadline()
                .unwrap()
                .saturating_duration_since(Instant::now());
            match receive.recv_timeout(remaining.min(Duration::from_millis(5))) {
                Ok(result) => return result,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(invalid("FIRST original receiver owner ended"))
                }
            }
        }
    }
    pub(in crate::distribution_native_host) fn take(
        &self,
    ) -> Result<Option<NativeFirstReceiveIngress>> {
        let mut state = self
            .state
            .try_lock()
            .ok_or_else(|| invalid("FIRST ingress busy"))?;
        let entry = state.pending.take();
        if entry.is_some() {
            state
                .current
                .as_mut()
                .expect("actual admitted request")
                .processing = true;
        }
        Ok(entry)
    }
    fn record_return(
        &self,
        request: &NativeFirstReceiveRequestV1,
        hash: &str,
        deadline: Instant,
        used: bool,
        result: &Result<NativeFirstReceiveReplyV1>,
    ) {
        let classification = if result.is_ok() {
            "returned_success_after_postchecks"
        } else if used {
            "uncertain_effect_original_owner_retained"
        } else {
            "rejected_before_first_image_or_capture_use"
        };
        let record = Returned {
            request: request.clone(),
            hash: hash.into(),
            deadline,
            classification,
            error: result.as_ref().err().map(super::error_data),
            reply: result.as_ref().ok().cloned(),
        };
        let event = serde_json::json!({"sequence":request.sequence,"request_sha256":hash,
            "classification":classification,"error":record.error,
            "reply_sha256":record.reply.as_ref().map(|x|hex::encode(Sha256::digest(serde_json::to_vec(x).expect("bounded reply DATA"))))});
        let bytes = serde_json::to_vec(&event).expect("bounded scalar event");
        let mut state = self.state.lock();
        let Some(current) = state.current.as_ref() else {
            return;
        };
        if current.sequence != request.sequence || current.hash != hash || !current.processing {
            return;
        }
        state.chain.update((bytes.len() as u64).to_be_bytes());
        state.chain.update(bytes);
        state.returned_count = state.returned_count.saturating_add(1);
        if used && result.is_err() {
            state.uncertain_count = state.uncertain_count.saturating_add(1);
        }
        if result.is_ok() {
            let resolves = state.unknown.as_ref().is_some_and(|old| {
                matches!(old.request.operation, NativeFirstReceiveOperationV1::Start)
                    && matches!(request.operation, NativeFirstReceiveOperationV1::Describe)
            });
            if resolves {
                // Specific REAL reobservation, never generic native Resume.
                // The original error classification remains in the hash chain.
                let old = state.unknown.take().unwrap();
                let resolution = serde_json::to_vec(
                    &serde_json::json!({"original_unknown_sequence":old.request.sequence,
                    "original_request_sha256":old.hash,"original_error":old.error,
                    "new_success_sequence":request.sequence,"old_call_relabelled_success":false}),
                )
                .unwrap();
                state.chain.update((resolution.len() as u64).to_be_bytes());
                state.chain.update(resolution);
            }
        }
        if !matches!(
            request.operation,
            NativeFirstReceiveOperationV1::ObserveLast { .. }
        ) {
            if used && result.is_err() && state.unknown.is_none() {
                state.unknown = Some(record);
            } else {
                // A failed specific Describe reobservation must not
                // replace the ORIGINAL unknown Start identity.
                state.last = Some(record);
            }
        }
        state.current = None;
    }
    /// Immutable actual returned-call DATA, not native revalidation, a Role or
    /// permission to mutate. Later native steps still execute all full guards.
    pub(in crate::distribution_native_host) fn observe_return(
        &self,
        sequence: u64,
    ) -> Result<serde_json::Value> {
        let state = self
            .state
            .try_lock()
            .ok_or_else(|| invalid("FIRST result snapshot busy"))?;
        let returned = state
            .unknown
            .as_ref()
            .filter(|r| r.request.sequence == sequence)
            .or_else(|| {
                state
                    .last
                    .as_ref()
                    .filter(|r| r.request.sequence == sequence)
            });
        let Some(record) = returned else {
            // A later actual admitted Observe irreversibly closes any lower
            // never-admitted sequence. This is not inferred from timeout.
            let never_admitted =
                sequence <= state.admitted && sequence > state.last_effect_admitted;
            return Ok(serde_json::json!({"sequence":sequence,
                "classification":if never_admitted { "irrevocably_not_admitted" } else { "not_retained_or_not_returned" },
                "admitted_watermark":state.admitted,"last_effect_admitted":state.last_effect_admitted,
                "authority_granted":false}));
        };
        Ok(
            serde_json::json!({"sequence":sequence,"request_sha256":record.hash,
            "classification":record.classification,"error":record.error,
            "actual_result":record.reply.as_ref().map(|r|&r.result),
            "target_attempt":record.reply.as_ref().and_then(|r|r.target_attempt),
            "original_server_deadline_expired_at_observation":Instant::now()>=record.deadline,
            "admitted_watermark":state.admitted,"returned_count":state.returned_count,
            "uncertain_count":state.uncertain_count,"outcome_chain_sha256":hex::encode(state.chain.clone().finalize()),
            "native_current_or_serving_grant":false}),
        )
    }
}
