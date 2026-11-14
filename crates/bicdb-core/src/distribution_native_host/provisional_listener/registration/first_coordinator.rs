//! Actual range-leader finite FIRST owner. Not a remote backend or periodic
//! relocation loop. It cannot announce global DECISION/Open/Serving.
use super::*;
use crate::distribution_native_host::remote_first::{
    NativeFirstReceiveOperationV1, NativeFirstReceiveRequestV1,
};
use crate::distribution_range_handoff::{
    NativeJointHandoffPlan, NativeJointOldRound, NativeRemoteOldOperationV1,
    NativeRemoteOldRequestV1,
};
use crate::distribution_range_image::NativeRemoteImageSourceOperationV1;

#[derive(serde::Serialize)]
pub(crate) enum NativeFirstCoordinatorAction {
    EnrollTarget,
    ObservePending,
    ConstrainAll,
    CloseAll,
    FenceTarget,
    Receive {
        operation: NativeFirstReceiveOperationV1,
        frame: Option<NativeRemoteImageSourceOperationV1>,
    },
}
pub(super) struct NativeFirstCoordinator {
    backend: Arc<NativeMetadataControlBackend>,
    plan: Arc<NativeJointHandoffPlan>,
    process: CancellationToken,
    operation: Uuid,
    donor: ClusterNodeId,
    target: ClusterNodeId,
    target_attempt: Option<Uuid>,
    constrained: BTreeSet<ClusterNodeId>,
    closed: BTreeSet<ClusterNodeId>,
    target_fenced: bool,
    /// Constant retained DATA; actual OLD/image owners remain on their hosts.
    next_sequence: u64,
    pending_receive: Option<NativeFirstReceiveRequestV1>,
    pending_control: Option<(NativeFirstControlKind, ClusterNodeId, serde_json::Value)>,
    inflight_intent: Option<serde_json::Value>,
    /// Exact original returned DATA remains until genuine resolution; later
    /// Observe/retries do not overwrite its unknown/history classification.
    pending_original_return: Option<serde_json::Value>,
    last_return: Option<serde_json::Value>,
    returned_count: u64,
    outcome_chain: Sha256,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum NativeFirstControlKind {
    Fence,
    Constrain,
    Close,
}
impl Drop for NativeFirstCoordinator {
    fn drop(&mut self) {
        self.process.cancel();
    }
}
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn begin_finite_remote_first(
        &mut self,
        relocation: crate::distribution::RelocationId,
        donor: ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        if !self.data.as_ref().is_some_and(|data| data.is_running()) {
            return Err(invalid(
                "FIRST coordinator requires actual original INITIAL DATA host",
            ));
        }
        let process = self
            .data_process
            .get()
            .ok_or_else(|| invalid("FIRST coordinator requires actual supervisor process"))?;
        process.check()?;
        let token = parent
            .child_linked_to(process)
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let business = Uuid::now_v7();
        let leader = self
            .backend
            .provider
            .current_metadata_leader_hint(&token)?
            .ok_or_else(|| invalid("FIRST current metadata leader unavailable"))?;
        let own = self.backend.transport.acquire_host_metadata_observation(
            &leader,
            business,
            Duration::from_secs(5),
            &token,
        )?;
        let current = own.committed_topology(business)?;
        let pending = current
            .relocations
            .get(&relocation)
            .ok_or_else(|| invalid("FIRST relocation must already be committed"))?;
        let range = current
            .range_by_id(pending.range_id)
            .ok_or_else(|| invalid("FIRST committed range absent"))?;
        if pending.handoff_session.is_none()
            || range.leader != self.backend.node_id
            || donor == pending.target
            || !range
                .replicas
                .iter()
                .any(|r| r.node_id == donor && r.role == RangeReplicaRole::Voter)
        {
            return Err(invalid(
                "FIRST requires real range leader/existing session/OLD donor",
            ));
        }
        if let Some(original) = self.first_coordinators.get(&range.id) {
            original.process.check()?;
            if original.plan.promise().relocation_id != relocation || original.donor != donor {
                return Err(invalid(
                    "FIRST original coordinator retains another attempt",
                ));
            }
            return Ok(());
        }
        if self.first_coordinators.len() >= 8 {
            return Err(invalid(
                "FIRST coordinator capacity; no uncertain-owner eviction",
            ));
        }
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &self.backend.service,
            &own,
            business,
            &token,
        )?
        .ok_or_else(|| invalid("FIRST coordinator requires actual owned native MStore"))?;
        let range_business = Uuid::now_v7();
        let observed = self.backend.transport.acquire_metadata_read_observation(
            &leader,
            range_business,
            range.id,
            range.epoch,
            Duration::from_secs(5),
            &token,
        )?;
        // This existing factory produces only the actual Pending purpose plan.
        // An empty local round is NEVER completed or called an OLD quorum.
        let round = NativeJointOldRound::begin(observed, range_business, relocation)?;
        let plan = round.plan();
        drop(round);
        point.require_request(&own, business)?;
        point.require_external_current()?;
        own.require_live_request(business)?;
        plan.require_live()?;
        token.check()?;
        self.first_coordinators.insert(
            range.id,
            NativeFirstCoordinator {
                backend: self.backend.clone(),
                plan,
                process: process.child(),
                operation: Uuid::now_v7(),
                donor,
                target: pending.target.clone(),
                target_attempt: None,
                constrained: BTreeSet::new(),
                closed: BTreeSet::new(),
                target_fenced: false,
                next_sequence: 1,
                pending_receive: None,
                pending_control: None,
                inflight_intent: None,
                pending_original_return: None,
                last_return: None,
                returned_count: 0,
                outcome_chain: Sha256::new(),
            },
        );
        Ok(())
    }
    pub(in crate::distribution_native_host) fn drive_finite_remote_first(
        &mut self,
        range: crate::RangeId,
        action: NativeFirstCoordinatorAction,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.first_coordinators
            .get_mut(&range)
            .ok_or_else(|| invalid("actual FIRST coordinator owner absent"))?
            .drive(action, parent)
    }
}
impl NativeFirstCoordinator {
    pub(super) fn stop_original_process(&self) {
        self.process.cancel();
    }
    fn own(&self, parent: &CancellationToken) -> Result<(HostMetadataReadObservation, Uuid)> {
        self.process.check()?;
        parent.check()?;
        let leader = self
            .backend
            .provider
            .current_metadata_leader_hint(parent)?
            .ok_or_else(|| invalid("FIRST metadata leader unavailable"))?;
        let business = Uuid::now_v7();
        let remaining = parent
            .deadline()
            .unwrap()
            .saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(BicDbError::QueryTimedOut);
        }
        let own = self.backend.transport.acquire_host_metadata_observation(
            &leader,
            business,
            remaining.min(Duration::from_secs(5)),
            parent,
        )?;
        let current = own.committed_topology(business)?;
        let p = self.plan.promise();
        if own.requester_node_id() != &self.backend.node_id
            || !current.range_by_id(p.range_id).is_some_and(|r| {
                r.leader == self.backend.node_id && self.plan.matches_capture_range(r)
            })
            || current.relocations.get(&p.relocation_id).is_none_or(|r| {
                r.target != self.target
                    || r.plan_id != p.plan_id
                    || r.handoff_session
                        .as_ref()
                        .is_none_or(|s| s.session_id != p.session_id || s.nonce != p.nonce)
            })
        {
            return Err(invalid(
                "FIRST original actual range-leader/Pending scope changed",
            ));
        }
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &self.backend.service,
            &own,
            business,
            parent,
        )?
        .ok_or_else(|| invalid("FIRST native current-MStore attachment absent"))?;
        point.require_request(&own, business)?;
        point.require_external_current()?;
        Ok((own, business))
    }
    fn reserve_intent(&mut self, intent: serde_json::Value) -> Result<()> {
        crate::distribution_range_image::bounded_json(&intent, 16 * 1024)?;
        if self.inflight_intent.is_some() {
            return Err(invalid(
                "FIRST interrupted original call still retains its intent",
            ));
        }
        self.inflight_intent = Some(intent);
        Ok(())
    }
    fn record_return(&mut self, outcome: serde_json::Value) {
        // Actual transport/API return is retained BEFORE any later postcheck.
        // No actual owner or guard is held in this bounded DATA projection.
        let event =
            serde_json::json!({"intent":self.inflight_intent.take(),"actual_return":outcome});
        let bytes = serde_json::to_vec(&event).expect("bounded FIRST returned DATA");
        self.outcome_chain
            .update((bytes.len() as u64).to_be_bytes());
        self.outcome_chain.update(bytes);
        self.returned_count = self.returned_count.saturating_add(1);
        if self.pending_original_return.is_none()
            && (self.pending_receive.is_some() || self.pending_control.is_some())
        {
            self.pending_original_return = Some(event.clone());
        }
        self.last_return = Some(event);
    }
    fn require_control_retry(
        &self,
        kind: NativeFirstControlKind,
        peer: &ClusterNodeId,
    ) -> Result<()> {
        if self.pending_receive.is_some() {
            return Err(invalid(
                "FIRST target call outcome unresolved before OLD/fence operation",
            ));
        }
        if self
            .pending_control
            .as_ref()
            .is_some_and(|(old, member, _)| *old != kind || member != peer)
        {
            return Err(invalid(
                "FIRST prior control outcome unresolved; another effect refused",
            ));
        }
        Ok(())
    }
    fn drive(
        &mut self,
        action: NativeFirstCoordinatorAction,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.process.check()?;
        let token = parent
            .child_linked_to(&self.process)
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        token.check()?;
        match action {
            NativeFirstCoordinatorAction::ObservePending => {
                let sequence = self
                    .pending_receive
                    .as_ref()
                    .ok_or_else(|| invalid("FIRST has no unresolved target request"))?
                    .sequence;
                self.receive(
                    NativeFirstReceiveOperationV1::ObserveLast { sequence },
                    None,
                    &token,
                )
            }
            NativeFirstCoordinatorAction::EnrollTarget => {
                if !self.target_fenced {
                    return Err(invalid(
                        "FIRST target needs its actual N fence before enrollment",
                    ));
                }
                self.receive(NativeFirstReceiveOperationV1::Enroll, None, &token)
            }
            NativeFirstCoordinatorAction::ConstrainAll | NativeFirstCoordinatorAction::CloseAll => {
                if self.target_attempt.is_none() {
                    return Err(invalid("FIRST target must actually enroll before OLD cut"));
                }
                let operation = if matches!(action, NativeFirstCoordinatorAction::ConstrainAll) {
                    NativeRemoteOldOperationV1::Constrain
                } else {
                    NativeRemoteOldOperationV1::Close
                };
                if operation == NativeRemoteOldOperationV1::Close
                    && self.constrained.len() != self.plan.old_members().len()
                {
                    return Err(invalid(
                        "FIRST Close requires actual ALL-OLD constraint responses",
                    ));
                }
                let peers: Vec<_> = self.plan.old_members().keys().cloned().collect();
                for peer in peers {
                    let done = if operation == NativeRemoteOldOperationV1::Constrain {
                        self.constrained.contains(&peer)
                    } else {
                        self.closed.contains(&peer)
                    };
                    if done {
                        continue;
                    }
                    let kind = if operation == NativeRemoteOldOperationV1::Constrain {
                        NativeFirstControlKind::Constrain
                    } else {
                        NativeFirstControlKind::Close
                    };
                    self.require_control_retry(kind, &peer)?;
                    let (own, business) = self.own(&token)?;
                    let request = NativeRemoteOldRequestV1::from_plan(
                        &self.plan,
                        operation,
                        business,
                        remaining_ms(&token)?,
                    )?;
                    let intent = serde_json::json!({"peer":peer,"request":request});
                    self.reserve_intent(intent.clone())?;
                    // SAME permanent plan/session. A failed Close is retried
                    // only by the existing real OLD retained-outcome producer.
                    if self.pending_control.is_none() {
                        self.pending_control = Some((kind, peer.clone(), intent));
                    }
                    let result = self
                        .backend
                        .transport
                        .request_remote_native_old(&peer, &self.plan, &request, &own, &token);
                    // Preserve actual return/unknown before any postcheck.
                    self.record_return(serde_json::json!({
                        "returned_ok":result.is_ok(),"error":result.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
                        "reply_sha256":result.as_ref().ok().map(|x|x.reply().checksum_sha256.clone()),
                    }));
                    let actual = result?;
                    actual.require_request_live()?;
                    token.check()?;
                    self.pending_control = None;
                    self.pending_original_return = None;
                    if operation == NativeRemoteOldOperationV1::Constrain {
                        self.constrained.insert(peer);
                    } else {
                        self.closed.insert(peer);
                    }
                }
                token.check()?;
                Ok(
                    serde_json::json!({"all_closed":self.closed.len()==self.plan.old_members().len(),
                    "all_constrained":self.constrained.len()==self.plan.old_members().len(),"serving":false}),
                )
            }
            NativeFirstCoordinatorAction::FenceTarget => {
                self.require_control_retry(NativeFirstControlKind::Fence, &self.target)?;
                let (own, business) = self.own(&token)?;
                let local = self.backend.transport.local_metadata_read_participant();
                let p = self.plan.promise();
                let challenge = Uuid::now_v7();
                let purpose =
                    crate::distribution_consensus::MetadataRecoveryRolePurpose::CommittedHandoff {
                        relocation_id: p.relocation_id,
                        plan_id: p.plan_id.clone(),
                        session_id: p.session_id,
                        nonce: p.nonce,
                    };
                let observed = self.backend.provider.witness_recovery_role(
                    &local,
                    business,
                    p.range_id,
                    p.old_epoch,
                    p.nonce,
                    challenge,
                    purpose.clone(),
                    &token,
                )?;
                let role = crate::distribution_range_recovery::RecoveryRoleAdmission::from_witness(
                    observed,
                    &local,
                    &self.backend.node_id,
                    business,
                    p.range_id,
                    p.old_epoch,
                    p.nonce,
                    challenge,
                    &purpose,
                )?;
                role.require_own_host_metadata(&own, business)?;
                let request = crate::distribution_range_recovery::RecoveryEpochFenceRequest {
                    scope: crate::distribution_range_consensus::RecoveryFenceScope::from_role(
                        &role,
                    )?,
                    budget_ms: remaining_ms(&token)?,
                };
                let intent = serde_json::json!({"target":self.target,"request":request});
                self.reserve_intent(intent.clone())?;
                if self.pending_control.is_none() {
                    self.pending_control =
                        Some((NativeFirstControlKind::Fence, self.target.clone(), intent));
                }
                let result = self.backend.transport.recovery_epoch_fence_observation(
                    &self.target,
                    &request,
                    &token,
                );
                self.record_return(serde_json::json!({"returned_ok":result.is_ok(),
                    "error":result.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}));
                result?;
                token.check()?;
                self.pending_control = None;
                self.pending_original_return = None;
                self.target_fenced = true;
                Ok(serde_json::json!({"actual_target_fence":true,"serving":false}))
            }
            NativeFirstCoordinatorAction::Receive { operation, frame } => {
                if self.closed.len() != self.plan.old_members().len() || !self.target_fenced {
                    return Err(invalid(
                        "FIRST receive requires actual ALL-OLD Close and target fence",
                    ));
                }
                self.receive(operation, frame, &token)
            }
        }
    }
    fn receive(
        &mut self,
        operation: NativeFirstReceiveOperationV1,
        frame: Option<NativeRemoteImageSourceOperationV1>,
        token: &CancellationToken,
    ) -> Result<serde_json::Value> {
        if self.pending_control.is_some() {
            return Err(invalid(
                "FIRST prior OLD/fence outcome unresolved before target operation",
            ));
        }
        let observing = matches!(operation, NativeFirstReceiveOperationV1::ObserveLast { .. });
        if let Some(prior) = &self.pending_receive {
            let allowed = matches!(operation, NativeFirstReceiveOperationV1::ObserveLast { sequence } if sequence == prior.sequence)
                || (matches!(prior.operation, NativeFirstReceiveOperationV1::Start)
                    && matches!(operation, NativeFirstReceiveOperationV1::Describe));
            if !allowed {
                return Err(invalid(
                    "FIRST prior target outcome unresolved; new mutation refused",
                ));
            }
        } else if observing {
            return Err(invalid(
                "FIRST observation requires its exact retained request",
            ));
        }
        let (own, business) = self.own(token)?;
        let sequence = self.next_sequence;
        self.next_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| invalid("FIRST sequence exhausted; original operation cannot reset"))?;
        let p = self.plan.promise();
        let request = NativeFirstReceiveRequestV1 {
            version: 1,
            relocation: p.relocation_id,
            range: p.range_id,
            epoch: p.old_epoch,
            session: p.session_id,
            nonce: p.nonce,
            original_operation: self.operation,
            sequence,
            source: self.donor.clone(),
            target_attempt: self.target_attempt,
            operation,
            source_operation: frame,
            business,
            challenge: Uuid::now_v7(),
            budget_ms: remaining_ms(token)?,
        };
        self.reserve_intent(serde_json::to_value(&request)?)?;
        if !observing && self.pending_receive.is_none() {
            // Before the socket can dispatch. Client timeout does not prove
            // that this original server request did not durably act.
            self.pending_receive = Some(request.clone());
        }
        let result = self
            .backend
            .transport
            .request_native_first_receive(&self.plan, &request, &own, business, token);
        self.record_return(serde_json::json!({"returned_ok":result.is_ok(),
            "error":result.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
            "actual_reply":result.as_ref().ok().map(|r|r.reply())}));
        let actual = result?;
        // Retain the returned ORIGINAL A before further fallible postchecks.
        // This authenticated result DATA does not grant a native permission.
        if let Some(a) = actual.reply().target_attempt {
            if self.target_attempt.is_some_and(|old| old != a) {
                return Err(invalid("FIRST changed original target attempt"));
            }
            self.target_attempt = Some(a);
        }
        actual.require_request_live()?;
        token.check()?;
        let value = actual.reply().result.clone();
        if observing {
            let prior = self
                .pending_receive
                .as_ref()
                .expect("retained exact request");
            let expected = hex::encode(Sha256::digest(
                crate::distribution_range_image::bounded_json(prior, 16 * 1024)?,
            ));
            let classification = value.get("classification").and_then(|v| v.as_str());
            let same_request =
                value.get("request_sha256").and_then(|v| v.as_str()) == Some(expected.as_str());
            if (same_request
                && matches!(
                    classification,
                    Some(
                        "returned_success_after_postchecks"
                            | "rejected_before_first_image_or_capture_use"
                    )
                ))
                || classification == Some("irrevocably_not_admitted")
            {
                self.pending_receive = None;
                self.pending_original_return = None;
            }
        } else {
            // A genuinely successful specific Describe may resolve its
            // retained Start uncertainty. Other unknown
            // native stages have no generic Resume/replay operation here.
            self.pending_receive = None;
            self.pending_original_return = None;
        }
        Ok(value)
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
