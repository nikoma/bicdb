//! Explicit finite original-host policy→actual DATA publication. No controller,
//! cold reconstruction, new writer from DATA or local quorum conversion.
use super::*;
use crate::distribution_native_host::{
    NativeRemoteLocalPolicyAuthority, NativeRemoteServingRequestV1,
    NativeRemoteSuccessorReadRegistration,
};
use crate::distribution_range_handoff::{
    NativeJointServingPolicyAdmission, NativeRemoteAllReleasedRound,
};
use std::sync::Weak;

pub(crate) struct NativeRemoteLocalServingPolicy {
    successor: Arc<crate::distribution_data::NativeRemoteReleaseOwner>,
    participant: Weak<NativeRemoteNewParticipant>,
    process: Arc<NativeRemotePreparationProcess>,
    manifest: Arc<crate::distribution_range_image::NativeRemoteImageManifestV1>,
    phase: Uuid,
    queue: Weak<crate::distribution_native_host::NativeRemoteServingQueue>,
    reader: Arc<NativeRemoteSuccessorReadRegistration>,
    // Necessary monotone stage selector only. Each use independently validates
    // actual retained install/coordinator/native/current authority.
    state: parking_lot::Mutex<PolicyState>,
}
#[derive(Default)]
struct PolicyState {
    policy_returned_ok: bool,
    dispatch_returned_ok: bool,
}
impl NativeRemoteLocalServingPolicy {
    pub(in crate::distribution_native_host) fn require_queue(
        &self,
        queue: &crate::distribution_native_host::NativeRemoteServingQueue,
    ) -> Result<()> {
        self.successor.require_memory_lifetime()?;
        self.successor.require_process(&self.process)?;
        let participant = self
            .participant
            .upgrade()
            .ok_or_else(|| invalid("Policy original participant ended"))?;
        self.successor.require_participant(&participant)?;
        if self
            .queue
            .upgrade()
            .is_none_or(|q| !std::ptr::eq(q.as_ref(), queue))
        {
            return Err(invalid("Policy changed SAME original queue"));
        }
        Ok(())
    }
}
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn advance_finite_remote_serving_policy(
        &mut self,
        range: crate::RangeId,
        phase: Uuid,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let original = self
            .remote_serving
            .get(&(range, phase))
            .and_then(|s| s.release.clone());
        let result = self.advance_finite_remote_serving_policy_inner(range, phase, parent);
        if let Some(original) = original {
            original.note_policy_outer_return(result.is_ok(), result.as_ref().err());
        }
        result
    }

    fn collect_actual_all_released_policy(
        &self,
        original: &NativeRemoteLocalServingPolicy,
        parent: &CancellationToken,
    ) -> Result<Arc<crate::distribution_range_handoff::NativeRemoteAllReleasedQuorum>> {
        let backend = &self.backend;
        let leader = backend
            .provider
            .current_metadata_leader_hint(parent)?
            .ok_or_else(|| invalid("Policy ALL-released metadata leader absent"))?;
        let decision = original.successor.native().data().checksum_sha256();
        let mut round = NativeRemoteAllReleasedRound::begin(
            original.manifest.clone(),
            original.phase,
            decision.into(),
            parent,
        )?;
        let operation = Uuid::now_v7();
        for (index, node) in original.manifest.new_members.keys().enumerate() {
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
                manifest: original.manifest.as_ref().clone(),
                phase_id: original.phase,
                decision_sha256: decision.into(),
                original_operation: operation,
                sequence: u64::try_from(index)
                    .map_err(|_| invalid("Policy actual sequence overflow"))?
                    + 1,
                operation: NativeRemoteServingOperationV1::ObserveReleasedForPolicy,
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
        let actual = round.try_finish().map_err(|(error, _)| error)?;
        Ok(Arc::new(actual))
    }
    fn acquire_actual_local_policy_phase(
        &self,
        original: &Arc<NativeRemoteLocalServingPolicy>,
        parent: &CancellationToken,
    ) -> Result<Arc<NativeJointServingPolicyAdmission>> {
        let participant = original
            .participant
            .upgrade()
            .ok_or_else(|| invalid("Policy original participant ended"))?;
        let queue = participant.retained_serving_queue();
        queue.require_local_policy_idle(original)?;
        let successor = original.successor.clone();
        let all = self.collect_actual_all_released_policy(original, parent)?;
        let backend = &self.backend;
        let authority = NativeRemoteLocalPolicyAuthority::acquire(
            &backend.provider,
            &backend.transport,
            &backend.service,
            &original.manifest,
            successor.clone(),
            original.process.clone(),
            parent,
        )?;
        let leader = backend
            .provider
            .current_metadata_leader_hint(parent)?
            .ok_or_else(|| invalid("Policy fresh actual metadata leader absent"))?;
        let post = successor.observe_current(&backend.transport, &leader, parent)?;
        let actual = Arc::new(
            NativeJointServingPolicyAdmission::from_actual_remote_all_released(
                all,
                post,
                &participant,
                authority,
            )?,
        );
        actual.require_external_current()?;
        Ok(actual)
    }
    fn advance_finite_remote_serving_policy_inner(
        &mut self,
        range: crate::RangeId,
        phase: Uuid,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        parent.check()?;
        if parent.deadline().is_none() || remaining(parent)? > Duration::from_secs(5) {
            return Err(invalid(
                "Policy actor step needs original at-most-five-second request",
            ));
        }
        let key = (range, phase);
        let actual = self
            .remote_serving
            .get(&key)
            .ok_or_else(|| invalid("Policy actual original Open/release host absent"))?;
        actual.process.require_memory()?;
        let successor = actual
            .release
            .as_ref()
            .cloned()
            .ok_or_else(|| invalid("Policy cannot manufacture missing released successor"))?;
        successor.require_known_policy_origin()?;
        let participant = actual.participant.clone();
        successor.require_participant(&participant)?;
        let process = actual.process.clone();
        let original = if let Some(original) = &actual.policy {
            if !Arc::ptr_eq(&successor, &original.successor) {
                return Err(invalid("Policy cannot replace actual successor"));
            }
            original.clone()
        } else {
            let (manifest, original_phase, decision) = participant.retained_serving_intent()?;
            if original_phase != phase
                || decision != successor.native().data().checksum_sha256()
                || !manifest.new_members.contains_key(&self.backend.node_id)
            {
                return Err(invalid(
                    "Policy changed actual final recipient/phase/native intent",
                ));
            }
            let reader = match (&self.data, &self.replica) {
                (Some(data), _) => {
                    data.capture_remote_policy_read_route(&self.backend, &participant, &successor)?
                }
                (None, Some(replica)) => replica.capture_remote_policy_read_route(
                    &self.backend,
                    &participant,
                    &successor,
                )?,
                _ => {
                    return Err(invalid(
                        "Policy actual INITIAL/Registered owner absent before initial transfer",
                    ))
                }
            };
            let queue = participant.retained_serving_queue();
            let original = Arc::new(NativeRemoteLocalServingPolicy {
                successor: successor.clone(),
                participant: Arc::downgrade(&participant),
                process: process.clone(),
                manifest,
                phase,
                queue: Arc::downgrade(&queue),
                reader,
                state: parking_lot::Mutex::new(PolicyState::default()),
            });
            queue.require_local_policy_idle(&original)?;
            // Retain BEFORE any route/queue retirement or policy effect.
            self.remote_serving.get_mut(&key).unwrap().policy = Some(original.clone());
            original
        };
        let queue = participant.retained_serving_queue();
        queue.require_local_policy_idle(&original)?;
        let backend = self.backend.clone();
        // A prior native install/bind may have failed after draining the old
        // endpoint. Only the SAME retained Installed/Joined owner may restore
        // its closed socket before collecting NEW self-TLS authority. No native
        // installer or stale admission is used by this repair.
        if matches!(
            successor.domain(),
            crate::db::NativeJointWriterDomain::Registered(_)
        ) {
            original.reader.attach()?;
            self.restore_retained_remote_policy_socket(&successor, range, parent)?;
        }
        let admission = self.acquire_actual_local_policy_phase(&original, parent)?;
        admission.require_external_current()?;
        queue.retain_local_policy_owner(&original)?;
        original.reader.attach()?;
        let was_installed = original
            .state
            .try_lock()
            .ok_or_else(|| invalid("Policy stage busy"))?
            .policy_returned_ok;
        let policy_result = if matches!(
            successor.domain(),
            crate::db::NativeJointWriterDomain::Registered(_)
        ) {
            // A partial real Assigned→Joined transfer remains owned. Never
            // construct another installer merely because its old source moved.
            if self.joined.is_some() {
                self.require_same_remote_registered_joined_policy(&successor, range, parent)
                    .and_then(|()| self.open_registered_joined_data(parent))
            } else {
                self.install_registered_serving_data_policy(range, admission.clone(), parent)
                    .and_then(|()| self.open_registered_joined_data(parent))
            }
        } else {
            self.publish_native_writer_serving_domain(range, &admission, parent)
        };
        successor.note_policy_api_return(policy_result.is_ok(), policy_result.as_ref().err());
        policy_result?;
        // Actual policy helper returned; retain before outer/window checks.
        original.state.lock().policy_returned_ok = true;
        // Actual listener replacement revokes the OLD ingress point. Preserve
        // its known native/install return, then use a NEW current registration
        // and independent authority under this SAME original actor parent.
        drop(admission);
        let post_bind = self.acquire_actual_local_policy_phase(&original, parent)?;
        parent.check()?;
        if !was_installed {
            return Ok(
                serde_json::json!({"transition":"actual_policy_installed_readiness_still_request_bound",
                "original_phase":phase,"actual_local_node":backend.node_id,"incomplete":true,
                "selected_leader":original.manifest.activation_range.leader,
                "actual_self_role_purpose":"NativeJointLocalServingPolicy",
                "Serving":false,"SDK_Strong_NOT_PROVEN":true}),
            );
        }
        let dispatch_request = post_bind.bound_policy_request();
        let dispatch_result = if self.joined.is_some() {
            self.publish_remote_registered_joined_data(&post_bind, &dispatch_request)
        } else {
            self.recover_initial_data_leadership(&dispatch_request)
        };
        successor.note_policy_api_return(dispatch_result.is_ok(), dispatch_result.as_ref().err());
        let recovered = dispatch_result?;
        original.state.lock().dispatch_returned_ok = true;
        post_bind.require_live()?;
        parent.check()?;
        Ok(
            serde_json::json!({"transition":"actual_native_recovery_and_guarded_DATA_publication_returned",
            "original_phase":phase,"actual_local_node":backend.node_id,
            "actual_recovered_range_count":recovered,"selected_leader":original.manifest.activation_range.leader,
            "local_selected_leader":original.manifest.activation_range.leader==backend.node_id,
            "strong_read_readiness":"each original SDK Strong must obtain its real fresh barrier",
            "SDK_Strong_NOT_PROVEN_by_this_return":true,"automatic_controller_cold_cleanup":false}),
        )
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
    /// Actual metadata/TLS self witness, then the UNCHANGED generic factory
    /// must reject this narrow purpose. No Role/OWN/DB getter is returned.
    pub(in crate::distribution_native_host) fn test_local_policy_generic_refusal(
        &self,
        range: crate::RangeId,
        phase: Uuid,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let actual = self
            .remote_serving
            .get(&(range, phase))
            .ok_or_else(|| invalid("generic refusal needs actual original local host"))?;
        let original = actual
            .policy
            .as_ref()
            .ok_or_else(|| invalid("generic refusal needs retained actual Policy continuation"))?;
        let queue = actual.participant.retained_serving_queue();
        queue.require_local_policy_idle(original)?;
        original.successor.require_known_policy_origin()?;
        let peer = self.backend.transport.local_metadata_read_participant();
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let purpose=crate::distribution_consensus::MetadataRecoveryRolePurpose::NativeJointLocalServingPolicy {
            relocation_id:original.manifest.relocation_id,session_id:original.manifest.handoff_session,
            nonce:original.manifest.nonce,decision_sha256:original.successor.native().data().checksum_sha256().into(),
        };
        let witness = self.backend.provider.witness_recovery_role(
            &peer,
            business,
            range,
            original.manifest.activation_range.epoch,
            original.manifest.nonce,
            challenge,
            purpose.clone(),
            parent,
        )?;
        witness.require_live_request(business, challenge)?;
        witness.require_initiator(&peer)?;
        let actual_return = crate::distribution_range_recovery::RecoveryRoleAdmission::from_witness(
            witness,
            &peer,
            peer.node_id(),
            business,
            range,
            original.manifest.activation_range.epoch,
            original.manifest.nonce,
            challenge,
            &purpose,
        );
        Ok(
            serde_json::json!({"actual_generic_factory_returned_ok":actual_return.is_ok(),
            "actual_error":actual_return.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
            "actual_cluster_body":actual_return.as_ref().err().and_then(|e|match e {BicDbError::Cluster(body)=>Some(body),_=>None}),
            "actual_self":peer.node_id(),"actual_incarnation":peer.member_incarnation(),
            "actual_final_leader":original.manifest.activation_range.leader,"actual_local_host":self.backend.node_id,
            "typed_witness_not_reconstructed":true,"no_native_mutation_invoked":true}),
        )
    }
}
