//! Recipient-owned ALL-Open → actual original Frozen drop/arm. This does not
//! publish Serving policy, reconstruct a cold host or convert remote DATA to
//! the original process-local quorum.
use super::*;
use crate::distribution_native_host::NativeRemoteServingRequestV1;
use crate::distribution_range_handoff::{
    NativeJointReleaseAdmission, NativeRemoteAllOpenQuorum, NativeRemoteAllOpenRetirementAdmission,
    NativeRemoteAllOpenRound,
};

impl NativeMetadataControlHost {
    fn collect_remote_all_open(
        &self,
        manifest: Arc<crate::distribution_range_image::NativeRemoteImageManifestV1>,
        phase: Uuid,
        decision: &str,
        parent: &CancellationToken,
    ) -> Result<Arc<NativeRemoteAllOpenQuorum>> {
        let backend = &self.backend;
        let leader = backend
            .provider
            .current_metadata_leader_hint(parent)?
            .ok_or_else(|| invalid("Release ALL Open metadata leader absent"))?;
        let mut round =
            NativeRemoteAllOpenRound::begin(manifest.clone(), phase, decision.into(), parent)?;
        let operation = Uuid::now_v7();
        for (index, node) in manifest.new_members.keys().enumerate() {
            parent.check()?;
            let business = Uuid::now_v7();
            let own = backend.transport.acquire_host_metadata_observation(
                &leader,
                business,
                remaining(parent)?,
                parent,
            )?;
            let request = NativeRemoteServingRequestV1 {
                version: 1,
                manifest: manifest.as_ref().clone(),
                phase_id: phase,
                decision_sha256: decision.into(),
                original_operation: operation,
                sequence: u64::try_from(index)
                    .map_err(|_| invalid("Release ALL Open sequence overflow"))?
                    + 1,
                operation: NativeRemoteServingOperationV1::ObserveOpen,
                business,
                challenge: Uuid::now_v7(),
                budget_ms: remaining(parent)?.as_millis().clamp(1, 5000) as u64,
            };
            round.record(
                backend
                    .transport
                    .request_remote_native_serving(node, &request, &own, business, parent)?,
            )?;
        }
        let all = round.try_finish().map_err(|(error, _)| error)?;
        parent.check()?;
        Ok(Arc::new(all))
    }

    pub(super) fn ensure_remote_release_successor(
        &mut self,
        participant: &Arc<NativeRemoteNewParticipant>,
        phase: &NativeRemoteAllOpenRetirementAdmission<'_>,
        parent: &CancellationToken,
        before_publish: impl FnOnce() -> Result<()>,
    ) -> Result<Arc<crate::distribution_data::NativeRemoteReleaseOwner>> {
        phase.require_current()?;
        phase.require_original_participant(participant)?;
        let key = (
            participant.snapshot().binding.range_id,
            participant.snapshot().phase_id,
        );
        let actual = self
            .remote_serving
            .get(&key)
            .ok_or_else(|| invalid("release cannot construct missing actual prepared/Open host"))?;
        if !Arc::ptr_eq(participant, &actual.participant) {
            return Err(invalid("release changed original actual host participant"));
        }
        actual.process.require_memory()?;
        actual.bridge.require_resolution()?;
        let original = if let Some(original) = &actual.release {
            original.require_participant(participant)?;
            original.require_process(&actual.process)?;
            original.clone()
        } else {
            let custody = actual
                .preparation
                .completed()
                .cloned()
                .ok_or_else(|| invalid("release original completed preparation absent"))?;
            let native = actual
                .bridge
                .completed()
                .cloned()
                .ok_or_else(|| invalid("release original known native bridge absent"))?;
            let activation = actual.activation.as_ref().cloned().ok_or_else(|| {
                invalid("release requires actual successful original Open return")
            })?;
            let survivor = match actual.domain.as_ref() {
                Some(crate::db::NativeJointWriterDomain::Closed(_)) => Some(
                    self.data
                        .as_ref()
                        .ok_or_else(|| invalid("release original INITIAL host absent"))?
                        .retained_remote_release_runtime(participant)?,
                ),
                Some(crate::db::NativeJointWriterDomain::Registered(_)) => None,
                _ => {
                    return Err(invalid(
                        "remote FIRST release changed actual writer domain kind",
                    ))
                }
            };
            custody.retain_remote_release_successor(
                participant,
                actual.process.clone(),
                native,
                activation,
                survivor,
            )?
        };
        let backend = self.backend.clone();
        let leader = backend
            .provider
            .current_metadata_leader_hint(parent)?
            .ok_or_else(|| invalid("release successor actual metadata leader absent"))?;
        let observed = original.observe_current(&backend.transport, &leader, parent)?;
        phase.require_release_successor_observation(participant, &observed)?;
        before_publish()?;
        phase.require_memory_current()?;
        participant.require_lifetime_memory()?;
        let actual = self.remote_serving.get_mut(&key).unwrap();
        // Retain BEFORE fallible route installation or any old/custody drain.
        // Failure never removes/reconstructs the actual successor.
        if actual.release.is_none() {
            actual.release = Some(original.clone());
        }
        participant.retain_remote_release_route(&original)?;
        actual
            .preparation
            .transfer_to_release_successor(&original)?;
        parent.check()?;
        Ok(original)
    }

    pub(super) fn apply_finite_remote_release(
        &mut self,
        participant: &Arc<NativeRemoteNewParticipant>,
        work: &mut NativeRemoteServingIngress,
    ) -> Result<NativeRemoteServingReplyV1> {
        work.require_request()?;
        let key = (
            work.request.manifest.capture_range.id,
            work.request.phase_id,
        );
        let actual = self
            .remote_serving
            .get(&key)
            .ok_or_else(|| invalid("Release requires SAME actual Open continuation"))?;
        if !Arc::ptr_eq(&actual.participant, participant)
            || actual.manifest_sha256 != work.request.manifest.checksum_sha256
            || actual.decision_sha256 != work.request.decision_sha256
        {
            return Err(invalid(
                "Release changed original recipient/manifest/decision",
            ));
        }
        let process = actual.process.clone();
        let request = process
            .bound_request(&work.token)
            .child_linked_to(&work.own.original_request_cancellation())
            .child_with_deadline(work.role.deadline().min(work.own.deadline()));
        let ingress = work.admit_native_release(process)?;
        let all = self.collect_remote_all_open(
            Arc::new(work.request.manifest.clone()),
            work.request.phase_id,
            &work.request.decision_sha256,
            &request,
        )?;
        let backend = self.backend.clone();
        let leader = backend
            .provider
            .current_metadata_leader_hint(&request)?
            .ok_or_else(|| invalid("Release post-ALL metadata leader absent"))?;
        let business = Uuid::now_v7();
        let own = backend.transport.acquire_host_metadata_observation(
            &leader,
            business,
            remaining(&request)?,
            &request,
        )?;
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &backend.service,
            &own,
            business,
            &request,
        )?
        .ok_or_else(|| invalid("Release post-ALL actual native MStore absent"))?;
        let phase = all.admit_retirement(&own, business, &point, &request)?;
        let original =
            self.ensure_remote_release_successor(participant, &phase, &request, || {
                work.mark_effect_entered()
            })?;
        // Exact original reference producers only. An outside-NEW donor uses
        // the separate finite actor method, never this native mutation Role.
        participant.retire_original_source_after_all_open(&phase)?;
        participant.retire_original_after_all_open(&phase)?;
        let observed = original.observe_current(&backend.transport, &leader, &request)?;
        let admission = NativeJointReleaseAdmission::from_actual_remote_all_open(
            all,
            observed,
            participant,
            ingress,
        )?;
        #[cfg(test)]
        if let Some(mode) = self
            .remote_serving
            .get_mut(&key)
            .unwrap()
            .release_test
            .take()
        {
            if mode != 1 {
                return Err(invalid("unknown actual Release cut"));
            }
            original.test_arm_original_drop_cancel(work.token.clone())?;
        }
        work.mark_effect_entered()?;
        admission.release_actual_local()?;
        let observed = original.observe_current(&backend.transport, &leader, &request)?;
        let reply = participant.observe_actual_release_for_serving(
            &work.request,
            &work.role,
            &work.own,
            work.business,
            &work.point,
            &request,
            &observed,
        )?;
        if work.request.operation == NativeRemoteServingOperationV1::ReconcileRelease {
            participant
                .retained_serving_queue()
                .resolve_original_release(work, &admission, &observed)?;
        }
        work.require_request()?;
        request.check()?;
        Ok(reply)
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn test_remote_release_control(
        &mut self,
        range: crate::RangeId,
        arm: Option<u8>,
    ) -> Result<serde_json::Value> {
        let key = self
            .remote_serving
            .keys()
            .find(|(r, _)| *r == range)
            .copied()
            .ok_or_else(|| invalid("release control needs actual original prepared host"))?;
        let actual = self.remote_serving.get_mut(&key).unwrap();
        actual.process.require_memory()?;
        actual.bridge.require_resolution()?;
        if let Some(mode) = arm {
            if mode != 1
                || actual.release_test.is_some()
                || actual.release.is_some()
                || actual.activation.is_none()
            {
                return Err(invalid(
                    "drop control must arm once after actual Open before original Release",
                ));
            }
            actual.release_test = Some(mode);
        }
        let successor = actual
            .release
            .as_ref()
            .map(|s| s.test_state_data())
            .transpose()?;
        let queue = actual
            .participant
            .retained_serving_queue()
            .test_open_queue_state();
        let native = self.test_remote_preparation_control(range, None)?;
        Ok(
            serde_json::json!({"actual_successor":successor,"queue":queue,
            "native_storage":native["storage"],"original_attempts":native["memory_before_storage"],
            "Serving":false,"selected_APP_Strong_not_proven":true,
            "separate_memory_storage_snapshots_non_atomic":true}),
        )
    }
}
