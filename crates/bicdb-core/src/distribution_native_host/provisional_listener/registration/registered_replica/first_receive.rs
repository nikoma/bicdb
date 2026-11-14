//! Serialized by the actual host owner's &mut borrow. Network ingress owns
//! only a request and its authenticated peer, never a reconstructed host.
use super::*;
use crate::distribution_consensus::{MetadataReadParticipant, MetadataRecoveryRolePurpose};
use crate::distribution_native_host::remote_first::{
    NativeFirstReceiveIngress, NativeFirstReceiveOperationV1 as Op, NativeFirstReceiveReplyV1,
    NativeFirstReceiveRequestV1,
};
use crate::distribution_range_handoff::NativeJointHandoffPlan;
use crate::distribution_range_image::{
    NativeRemoteImageFact, NativeRemoteImageSourceOperationV1 as SourceOp,
    NativeRemoteImageSourceRequestV1, NativeSourceDiscoveryOperationV1 as DiscoveryOp,
    NativeSourceDiscoveryRequestV1,
};
use crate::distribution_range_recovery::{HandoffRoleAdmission, RecoveryRoleAdmission};
use std::sync::OnceLock;

pub(super) struct NativeFirstReceiverAttempt {
    original: Arc<NativeRegisteredReplicaAdmission>,
    process: CancellationToken,
    coordinator: (ClusterNodeId, u64, String),
    operation: Uuid,
    plan: Arc<NativeJointHandoffPlan>,
    donor: ClusterNodeId,
    attempt: Uuid,
    _enrollment_no_domain: super::no_domain::RegisteredRemoteNativePreflight,
    source: OnceLock<Arc<NativeRemoteImageFact>>,
    /// Bounded returned-call DATA. Native owners retain their own exact
    /// XID/candidate/unknown history before any fallible postcheck.
    last_api_return: Mutex<Option<serde_json::Value>>,
}
impl NativeFirstReceiverAttempt {
    fn require_original(
        &self,
        peer: &MetadataReadParticipant,
        request: &NativeFirstReceiveRequestV1,
    ) -> Result<()> {
        self.original.require_live()?;
        self.process.check()?;
        let p = self.plan.promise();
        if peer.node_id() != &self.coordinator.0
            || peer.member_incarnation() != self.coordinator.1
            || peer.certificate_sha256() != Some(self.coordinator.2.as_str())
            || request.relocation != p.relocation_id
            || request.session != p.session_id
            || request.nonce != p.nonce
            || request.original_operation != self.operation
            || request.source != self.donor
            || request.range != self.plan.old_range().id
            || request.epoch != self.plan.old_range().epoch
            || (!matches!(request.operation, Op::Enroll | Op::ObserveLast { .. })
                && request.target_attempt != Some(self.attempt))
            || request.target_attempt.is_some_and(|a| a != self.attempt)
        {
            return Err(invalid(
                "FIRST receive changed original coordinator/session/attempt",
            ));
        }
        Ok(())
    }
}

impl NativeMetadataControlHost {
    /// Consume at most ONE original authenticated request. Called by the
    /// existing node owner thread; never by a listener with borrowed DATA.
    /// No periodic placement/freezing action is installed by this method.
    pub(in crate::distribution_native_host) fn poll_first_receive(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<bool> {
        parent.check()?;
        let Some(receiver) = self.replica.as_ref() else {
            return Ok(false);
        };
        let queue = receiver.first_receive.clone();
        let Some(mut ingress) = queue.take()? else {
            return Ok(false);
        };
        // Parent controls this poll; ingress retains the ORIGINAL bounded
        // request established BEFORE it waited in the queue. Neither can be
        // renewed by queue delivery or by a later Step.
        let result = match self.data_process.get() {
            Some(process) => {
                let token = ingress
                    .token
                    .child_linked_to(parent)
                    .child_linked_to(process);
                self.consume_first_receive(&mut ingress, &token)
            }
            None => Err(invalid(
                "FIRST consumer requires actual retained supervisor process",
            )),
        };
        ingress.complete(result);
        Ok(true)
    }
    fn consume_first_receive(
        &mut self,
        ingress: &mut NativeFirstReceiveIngress,
        token: &CancellationToken,
    ) -> Result<NativeFirstReceiveReplyV1> {
        token.check()?;
        let request = &ingress.request;
        request.require_shape()?;
        let process = self
            .data_process
            .get()
            .ok_or_else(|| invalid("FIRST consumer requires actual retained supervisor process"))?
            .clone();
        process.check()?;
        if self.data.is_some() || self.assigned.is_some() || self.joined.is_some() {
            return Err(invalid(
                "FIRST cannot replace DATA or revive a Joined receiver",
            ));
        }
        let backend = self.backend.clone();
        let (admission, runtime) = {
            let receiver = self
                .replica
                .as_ref()
                .ok_or_else(|| invalid("original FIRST receiver ended"))?;
            receiver
                .admission
                .require_listener(&backend.service, &receiver.runtime)?;
            (receiver.admission.clone(), receiver.runtime.clone())
        };
        if !Arc::ptr_eq(&admission, &ingress.original) {
            return Err(invalid("FIRST queue substituted original actual receiver"));
        }
        let business = ingress.business;
        let role = ingress
            .role
            .take()
            .ok_or_else(|| invalid("FIRST original request role consumed"))?;
        let own = ingress
            .own
            .take()
            .ok_or_else(|| invalid("FIRST original request OWN consumed"))?;
        role.role().require_own_host_metadata(&own, business)?;
        ingress.point.require_request(&own, business)?;
        ingress.point.require_external_current()?;
        let plan = ingress.plan.clone();
        plan.require_role(role.role())?;
        if let Op::ObserveLast { sequence } = request.operation {
            let receiver = self.replica.as_ref().unwrap();
            let attempt = receiver.first_attempts.get(&request.range);
            if let Some(attempt) = attempt {
                attempt.require_original(&ingress.peer, request)?;
                if attempt.plan.checksum_sha256()? != plan.checksum_sha256()? {
                    return Err(invalid("FIRST status changed original permanent plan"));
                }
            }
            let value = receiver.first_receive.observe_return(sequence)?;
            ingress.point.require_external_current()?;
            token.check()?;
            return Ok(NativeFirstReceiveReplyV1 {
                request: request.clone(),
                target: backend.node_id.clone(),
                birth: plan.enrolled_birth(&backend.node_id)?.clone(),
                target_attempt: attempt.map(|a| a.attempt),
                result: value,
            });
        }
        let attempt = if let Some(attempt) = self
            .replica
            .as_ref()
            .unwrap()
            .first_attempts
            .get(&request.range)
        {
            attempt.clone()
        } else {
            if !matches!(request.operation, Op::Enroll) {
                return Err(invalid(
                    "FIRST original enrollment absent; no reconstruction from request",
                ));
            }
            let receiver = self.replica.as_mut().unwrap();
            if !receiver.first_attempts.is_empty()
                || !receiver.remote_attempts.is_empty()
                || !receiver
                    .remote_joint_targets
                    .try_lock()
                    .ok_or_else(|| invalid("FIRST target table busy"))?
                    .is_empty()
            {
                return Err(invalid(
                    "FIRST receiver already retained an attempt or capacity",
                ));
            }
            // A real target N fence and complete selected no-domain/native
            // preflight are prerequisites BEFORE a coordinator may cut OLD.
            // A cold root with an old A/native attempt cannot enroll anew.
            let scope =
                crate::distribution_range_consensus::RecoveryFenceScope::from_role(role.role())?;
            let enrollment_no_domain = {
                let frozen = runtime.source_capture_freeze(&scope)?;
                admission
                    .observe_selected_range_no_domain(&role, &own, business, &frozen, token)?
                    .into_remote_native_preflight()?
            };
            token.check()?;
            let attempt = Arc::new(NativeFirstReceiverAttempt {
                original: admission.clone(),
                process: process.child(),
                coordinator: (
                    ingress.peer.node_id().clone(),
                    ingress.peer.member_incarnation(),
                    ingress
                        .peer
                        .certificate_sha256()
                        .ok_or_else(|| invalid("FIRST TLS pin absent"))?
                        .into(),
                ),
                operation: request.original_operation,
                plan: plan.clone(),
                donor: request.source.clone(),
                attempt: Uuid::now_v7(),
                _enrollment_no_domain: enrollment_no_domain,
                source: OnceLock::new(),
                last_api_return: Mutex::new(None),
            });
            // Retain the original A before any subsequent source-export or
            // image-WAL effect. The prerequisite N fence/no-domain scan has
            // already executed under this original bounded request.
            ingress.mark_effect_entered();
            receiver
                .first_attempts
                .insert(request.range, attempt.clone());
            attempt
        };
        attempt.require_original(&ingress.peer, request)?;
        if attempt.plan.checksum_sha256()? != plan.checksum_sha256()? {
            return Err(invalid(
                "FIRST fresh admission changed original Pending plan",
            ));
        }
        let step_token = token.child_linked_to(&attempt.process);
        step_token.check()?;
        let result =
            self.apply_first_receive_step(&attempt, ingress, role, own, business, &step_token);
        // Copy actual RETURN before any postcheck or reply. This bounded DATA
        // never overwrites the native sink's original uncertainty or owner.
        *attempt.last_api_return.lock() = Some(serde_json::json!({
            "sequence":request.sequence,"business":request.business,"challenge":request.challenge,"operation":request.operation,
            "returned_ok":result.is_ok(),"actual_api_return":result.as_ref().ok(),
            "error":result.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
            "outer_postchecks_complete":false,
        }));
        let value = result?;
        crate::distribution_range_image::bounded_json(&value, 4 * 1024 * 1024)?;
        attempt.require_original(&ingress.peer, request)?;
        step_token.check()?;
        Ok(NativeFirstReceiveReplyV1 {
            request: request.clone(),
            target: backend.node_id.clone(),
            birth: plan.enrolled_birth(&backend.node_id)?.clone(),
            target_attempt: Some(attempt.attempt),
            result: value,
        })
    }
    #[allow(clippy::too_many_arguments)]
    fn apply_first_receive_step(
        &mut self,
        attempt: &Arc<NativeFirstReceiverAttempt>,
        ingress: &NativeFirstReceiveIngress,
        role: HandoffRoleAdmission,
        own: HostMetadataReadObservation,
        business: Uuid,
        token: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let request = &ingress.request;
        token.check()?;
        attempt.process.check()?;
        if matches!(request.operation, Op::Enroll) {
            // A specific same-A enrollment reobservation repeats the actual
            // no-domain proof. It never reconstructs or replaces the original.
            let runtime = self.replica.as_ref().unwrap().runtime.clone();
            let scope =
                crate::distribution_range_consensus::RecoveryFenceScope::from_role(role.role())?;
            let frozen = runtime.source_capture_freeze(&scope)?;
            attempt
                .original
                .observe_selected_range_no_domain(&role, &own, business, &frozen, token)?
                .into_remote_native_preflight()?;
            token.check()?;
            // Enrollment retains identity only. The existing actual native
            // fence endpoint is called by the real coordinator separately.
            return Ok(serde_json::json!({"enrolled":true,"native_or_serving_grant":false}));
        }
        if matches!(request.operation, Op::Start | Op::Describe) {
            if request.source_operation.is_some() {
                return Err(invalid("discovery is not a frame step"));
            }
            let operation = if matches!(request.operation, Op::Start) {
                DiscoveryOp::Start
            } else {
                DiscoveryOp::Describe
            };
            let discovery = NativeSourceDiscoveryRequestV1::from_plan(
                &attempt.plan,
                attempt.donor.clone(),
                self.backend.node_id.clone(),
                attempt.attempt,
                operation,
                &own,
                business,
                remaining_ms(token)?,
            )?;
            ingress.mark_effect_entered();
            let actual = self
                .backend
                .transport
                .request_remote_image_source_discovery(&attempt.plan, &discovery, &own, token)?;
            let fact = Arc::new(NativeRemoteImageFact::from_actual_discovery(actual)?);
            if let Some(original) = attempt.source.get() {
                if crate::distribution_range_image::bounded_json(
                    original.manifest(),
                    2 * 1024 * 1024,
                )? != crate::distribution_range_image::bounded_json(
                    fact.manifest(),
                    2 * 1024 * 1024,
                )? {
                    return Err(invalid(
                        "FIRST discovery changed original actual MF/session",
                    ));
                }
            } else {
                let _ = attempt.source.set(fact);
            }
            role.role().require_own_host_metadata(&own, business)?;
            token.check()?;
            return Ok(serde_json::to_value(
                attempt.source.get().unwrap().manifest(),
            )?);
        }
        let source = attempt
            .source
            .get()
            .ok_or_else(|| invalid("FIRST actual source discovery absent"))?
            .clone();
        let source_operation = request.source_operation.clone().ok_or_else(|| {
            invalid("FIRST image step requires separately authenticated live frame")
        })?;
        let source_request = NativeRemoteImageSourceRequestV1 {
            source_session: source.manifest().source_session,
            target_attempt: attempt.attempt,
            manifest_sha256: source.manifest().checksum_sha256.clone(),
            business,
            challenge: Uuid::now_v7(),
            budget_ms: remaining_ms(token)?,
            operation: source_operation,
        };
        let actual = self.backend.transport.request_remote_image_source_frame(
            source.manifest(),
            &source_request,
            &own,
            business,
            token,
        )?;
        let frame = source.bind_step(actual)?;
        token.check()?;
        ingress.mark_effect_entered();
        let result = match request.operation {
            Op::Transfer => {
                self.prepare_registered_remote_install_attempt(
                    source.clone(),
                    &frame,
                    role,
                    own,
                    business,
                    token,
                )?;
                serde_json::json!({"transferred_original_attempt":attempt.attempt})
            }
            Op::Proposal => serde_json::to_value(self.publish_registered_remote_proposal(
                &source, &frame, role, own, business, token,
            )?)?,
            Op::Installing => {
                self.publish_registered_remote_installing(
                    &source, &frame, role, own, business, token,
                )?;
                serde_json::json!({"installing":true})
            }
            Op::Materialize => {
                self.take_registered_remote_joint_target(
                    &source, &frame, role, own, business, token,
                )?;
                serde_json::json!({"actual_target_retained":true,"serving":false})
            }
            _ => {
                use crate::db::NativeRegisteredRemoteImageOperation as Image;
                let operation = match request.operation {
                    Op::BeginHistory => Image::BeginHistory,
                    Op::AppendHistory => Image::AppendHistory,
                    Op::AuditHistory { max_entries } => Image::AuditHistory { max_entries },
                    Op::ApplyRows => Image::ApplyRows,
                    Op::BeginVerification => Image::BeginVerification,
                    Op::VerifyTarget {
                        max_scanned_records,
                    } => Image::VerifyTarget {
                        max_scanned_records,
                    },
                    Op::PublishFinalImage => Image::PublishFinalImage,
                    Op::PublishNativeBase => Image::PublishNativeBase,
                    Op::AdoptCatalogueFloor => Image::AdoptCatalogueFloor,
                    _ => return Err(invalid("FIRST unsupported image operation")),
                };
                serde_json::to_value(self.advance_registered_remote_image(
                    &source, &frame, role, own, business, token, operation,
                )?)?
            }
        };
        // Outer caller records this exact API return before its postchecks.
        Ok(result)
    }
}
fn remaining_ms(token: &CancellationToken) -> Result<u64> {
    token.check()?;
    let ms = token
        .deadline()
        .unwrap()
        .saturating_duration_since(Instant::now())
        .as_millis();
    if ms == 0 {
        return Err(BicDbError::QueryTimedOut);
    }
    Ok(ms.min(4000) as u64)
}

impl NativeRegisteredReplicaAdmission {
    pub(super) fn admit_first_from_actual_peer(
        self: &Arc<Self>,
        peer: MetadataReadParticipant,
        request: NativeFirstReceiveRequestV1,
        parent: &CancellationToken,
    ) -> Result<NativeFirstReceiveReplyV1> {
        // This absolute window is fixed BEFORE metadata acquisition and queue
        // waiting. Queue delivery never creates another cancellation deadline.
        let token = request.cancellation(parent)?;
        self.require_live()?;
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| invalid("FIRST original backend ended"))?;
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("FIRST original service ended"))?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("FIRST original runtime ended"))?;
        self.require_listener(&service, &runtime)?;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let purpose = MetadataRecoveryRolePurpose::CommittedHandoff {
            relocation_id: request.relocation,
            plan_id: {
                let store = backend.lock_metadata(&token)?;
                store
                    .committed_topology()
                    .relocations
                    .get(&request.relocation)
                    .ok_or_else(|| invalid("FIRST committed relocation absent"))?
                    .plan_id
                    .clone()
            },
            session_id: request.session,
            nonce: request.nonce,
        };
        let observed = backend.provider.witness_recovery_role(
            &peer,
            business,
            request.range,
            request.epoch,
            request.nonce,
            challenge,
            purpose.clone(),
            &token,
        )?;
        let role = RecoveryRoleAdmission::from_witness(
            observed,
            &peer,
            &backend.node_id,
            business,
            request.range,
            request.epoch,
            request.nonce,
            challenge,
            &purpose,
        )?
        .into_handoff()?;
        let leader = backend
            .provider
            .current_metadata_leader_hint(&token)?
            .ok_or_else(|| invalid("FIRST metadata leader unavailable"))?;
        let remaining = token
            .deadline()
            .unwrap()
            .saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(BicDbError::QueryTimedOut);
        }
        let own = backend.transport.acquire_host_metadata_observation(
            &leader,
            business,
            remaining.min(Duration::from_secs(5)),
            &token,
        )?;
        let plan = Arc::new(NativeJointHandoffPlan::from_actual_role_and_own(
            role.role(),
            &own,
            business,
        )?);
        if peer.node_id() != &plan.old_range().leader
            || !plan.old_members().contains_key(&request.source)
            || plan.promise().relocation_id != request.relocation
            || own.requester_node_id() != &backend.node_id
            || role.committed_relocation().target != backend.node_id
        {
            return Err(invalid(
                "FIRST requires actual range-leader coordinator and selected target",
            ));
        }
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &service, &own, business, &token,
        )?
        .ok_or_else(|| invalid("FIRST ingress requires actual current-MStore owner"))?;
        point.require_request(&own, business)?;
        point.require_external_current()?;
        self.first_receive
            .upgrade()
            .ok_or_else(|| invalid("FIRST original queue ended"))?
            .request_from_actual_admission(
                peer,
                request,
                token,
                role,
                own,
                business,
                plan,
                point,
                self.clone(),
            )
    }
}
