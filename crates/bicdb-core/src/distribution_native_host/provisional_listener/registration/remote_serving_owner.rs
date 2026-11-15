//! Explicit finite serialized native-host consumer. No automatic scheduler,
//! host reconstruction, replacement owner, or authority from reply DATA.
use super::*;
mod open;
mod policy;
mod reference_retirement;
mod release;
use crate::distribution_data::{
    NativeJointServingLease, NativeRemoteServiceBridge, NativeRemoteServicePreparation,
};
use crate::distribution_native_host::remote_serving::NativeRemoteServingIngress;
use crate::distribution_native_host::{
    NativeRemotePreparationProcess, NativeRemoteServingOperationV1, NativeRemoteServingReplyV1,
};
use crate::distribution_range_handoff::NativeRemoteNewParticipant;
pub(crate) use policy::NativeRemoteLocalServingPolicy;
pub(super) use reference_retirement::NativeRemoteReferenceRetirement;

pub(super) struct NativeRemoteLocalServing {
    participant: Arc<NativeRemoteNewParticipant>,
    process: Arc<NativeRemotePreparationProcess>,
    manifest_sha256: String,
    decision_sha256: String,
    lease: Option<NativeJointServingLease>,
    domain: Option<crate::db::NativeJointWriterDomain>,
    closed_started: bool,
    closed_lease_transferred: bool,
    preparation: NativeRemoteServicePreparation,
    bridge: NativeRemoteServiceBridge,
    #[cfg(test)]
    open_test: Option<u8>,
    #[cfg(test)]
    open_cut: Option<serde_json::Value>,
    #[cfg(test)]
    release_test: Option<u8>,
    activation:
        Option<Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>>,
    release: Option<Arc<crate::distribution_data::NativeRemoteReleaseOwner>>,
    policy: Option<Arc<NativeRemoteLocalServingPolicy>>,
}
impl NativeMetadataControlHost {
    /// One explicit actor turn. It operates on this actual &mut host; it does
    /// not periodically freeze placement or create a native host from intent.
    pub(in crate::distribution_native_host) fn poll_finite_remote_serving(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<usize> {
        parent.check()?;
        let mut phases = self
            .remote_serving
            .values()
            .map(|s| s.participant.clone())
            .collect::<Vec<_>>();
        if let Some(data) = &self.data {
            phases.extend(data.retained_remote_serving_participants()?);
        }
        if let Some(target) = &self.replica {
            phases.extend(target.retained_remote_serving_participants()?);
        }
        let mut unique = BTreeMap::new();
        for phase in phases {
            let key = (phase.snapshot().binding.range_id, phase.snapshot().phase_id);
            if let Some(original) = unique.get(&key) {
                if !Arc::ptr_eq(original, &phase) {
                    return Err(invalid(
                        "serving actual host changed original same-range phase owner",
                    ));
                }
            } else {
                unique.insert(key, phase);
            }
        }
        let mut phases = unique.into_iter().collect::<Vec<_>>();
        // Resume after the last attempted phase, even when its queue is
        // busy or its work exhausted the caller. A repeatedly hot first range cannot starve
        // another retained original phase. The cursor grants no authority.
        if let Some(cursor) = self.remote_serving_poll_cursor {
            let start = phases
                .iter()
                .position(|(key, _)| *key > cursor)
                .unwrap_or(0);
            phases.rotate_left(start);
        }
        let mut progressed = 0;
        for (key, participant) in phases {
            // Do not dequeue another original RPC once this poll has ended.
            parent.check()?;
            self.remote_serving_poll_cursor = Some(key);
            let queue = participant.retained_serving_queue();
            let Some(mut work) = queue.take()? else {
                continue;
            };
            let result = work.bind_actor_poll(parent).and_then(|()| {
                #[cfg(test)]
                self.test_after_actual_actor_bind(&work, parent)?;
                self.apply_finite_remote_serving(&participant, &mut work)
            });
            if matches!(
                work.request.operation,
                NativeRemoteServingOperationV1::Release
                    | NativeRemoteServingOperationV1::ReconcileRelease
            ) {
                let key = (
                    work.request.manifest.capture_range.id,
                    work.request.phase_id,
                );
                if let Some(original) = self
                    .remote_serving
                    .get(&key)
                    .and_then(|s| s.release.as_ref())
                {
                    original.note_outer_return(result.is_ok(), result.as_ref().err());
                }
            }
            // The actual primary result is retained/sent unchanged; the
            // original private native state is already held in this host.
            queue.complete(work, result)?;
            progressed += 1;
            parent.check()?;
        }
        Ok(progressed)
    }
    fn apply_finite_remote_serving(
        &mut self,
        participant: &Arc<NativeRemoteNewParticipant>,
        work: &mut NativeRemoteServingIngress,
    ) -> Result<NativeRemoteServingReplyV1> {
        work.require_request()?;
        if matches!(
            work.request.operation,
            NativeRemoteServingOperationV1::Release
                | NativeRemoteServingOperationV1::ReconcileRelease
        ) {
            return self.apply_finite_remote_release(participant, work);
        }
        if matches!(
            work.request.operation,
            NativeRemoteServingOperationV1::Open | NativeRemoteServingOperationV1::ReconcileOpen
        ) {
            return self.apply_finite_remote_open(participant, work);
        }
        if work.request.operation == NativeRemoteServingOperationV1::PublishBridge {
            return self.apply_finite_remote_bridge(participant, work);
        }
        if matches!(
            work.request.operation,
            NativeRemoteServingOperationV1::ObserveBridge
                | NativeRemoteServingOperationV1::ResolveBridge
        ) {
            return self.apply_finite_remote_bridge_read(participant, work);
        }
        if !matches!(
            work.request.operation,
            NativeRemoteServingOperationV1::Prepare
                | NativeRemoteServingOperationV1::ObservePrepared
                | NativeRemoteServingOperationV1::ResolvePrepared
        ) {
            return Err(invalid("finite recipient bridge/Open/release consumer not implemented yet; no effects admitted"));
        }
        let key = (
            work.request.manifest.capture_range.id,
            work.request.phase_id,
        );
        if !self.remote_serving.contains_key(&key) {
            if work.request.operation.readonly() {
                return Err(invalid(
                    "readonly preparation cannot create missing original host continuation",
                ));
            }
            if self.remote_serving.len() >= 8 {
                return Err(invalid(
                    "finite serving preserves eight-original-operation custody bound",
                ));
            }
            let process = self.capture_remote_preparation_process(participant, work)?;
            let (lease, domain) = participant.retain_original_serving_lease(
                &work.request,
                &work.role,
                &work.own,
                work.business,
                &work.point,
            )?;
            work.require_request()?;
            let closed_lease_transferred = domain.is_some();
            self.remote_serving.insert(
                key,
                NativeRemoteLocalServing {
                    participant: participant.clone(),
                    process,
                    manifest_sha256: work.request.manifest.checksum_sha256.clone(),
                    decision_sha256: work.request.decision_sha256.clone(),
                    lease: Some(lease),
                    domain,
                    closed_started: false,
                    closed_lease_transferred,
                    preparation: Default::default(),
                    bridge: Default::default(),
                    activation: None,
                    release: None,
                    policy: None,
                    #[cfg(test)]
                    open_test: None,
                    #[cfg(test)]
                    open_cut: None,
                    #[cfg(test)]
                    release_test: None,
                },
            );
        }
        {
            let actual = self.remote_serving.get(&key).unwrap();
            if !Arc::ptr_eq(&actual.participant, participant)
                || actual.manifest_sha256 != work.request.manifest.checksum_sha256
                || actual.decision_sha256 != work.request.decision_sha256
            {
                return Err(invalid(
                    "serving request substituted retained original native phase/candidate",
                ));
            }
            actual.process.require_memory()?;
        }
        let backend = self.backend.clone();
        let operation = work.request.operation;
        let process = self.remote_serving.get(&key).unwrap().process.clone();
        let request = process
            .bound_request(&work.bound_actor_request())
            .child_linked_to(&work.own.original_request_cancellation())
            .child_with_deadline(work.role.deadline().min(work.own.deadline()));
        let mutating = operation == NativeRemoteServingOperationV1::Prepare;
        if mutating && self.remote_serving[&key].preparation.completed().is_some() {
            return Err(invalid("actual preparation already retained; fresh ObservePrepared required instead of repeated mutation"));
        }
        // This independently acquired OWN is consumed by the actual committed
        // native observer. The ORIGINAL ingress Role/OWN/window also remain
        // live and are checked at the actual native boundary.
        let leader = backend
            .provider
            .current_metadata_leader_hint(&request)?
            .ok_or_else(|| invalid("serving current metadata leader absent"))?;
        let fresh = backend.transport.acquire_host_metadata_observation(
            &leader,
            work.business,
            remaining(&request)?,
            &request,
        )?;
        work.role.require_own_host_metadata(&fresh, work.business)?;
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &backend.service,
            &fresh,
            work.business,
            &request,
        )?
        .ok_or_else(|| invalid("remote preparation requires actual current native MStore"))?;
        let needs_closed = !self.remote_serving[&key].closed_lease_transferred;
        if needs_closed && !mutating {
            return Err(invalid(
                "readonly preparation cannot start or finish missing CLOSED transition",
            ));
        }
        work.require_request()?;
        if needs_closed {
            if self.data.is_none() || self.assigned.is_some() || self.joined.is_some() {
                return Err(invalid("remote OLD survivor requires actual original INITIAL host, no Installed/Joined cast"));
            }
            if !self.remote_serving[&key].closed_started {
                let original_process = self
                    .remote_serving_process
                    .get()
                    .ok_or_else(|| invalid("remote serving original host process absent"))?
                    .clone();
                let lease = self.remote_serving[&key]
                    .lease
                    .as_ref()
                    .ok_or_else(|| invalid("remote original survivor lease absent"))?
                    .clone();
                // Begin is read-only; only the genuine process is retained.
                // Each actual advance below uses the original server window.
                self.begin_native_writer_data_domain(key.0, lease, &original_process)?;
                self.remote_serving.get_mut(&key).unwrap().closed_started = true;
            }
            work.mark_effect_entered()?;
            self.advance_native_writer_closed_domain(key.0, &request)?;
            let closed = self.native_closed_writer_domain(key.0, &request)?;
            let lease = self.take_native_closed_joint_lease(key.0, &request)?;
            let actual = self.remote_serving.get_mut(&key).unwrap();
            actual.domain = Some(crate::db::NativeJointWriterDomain::Closed(closed));
            actual.lease = Some(lease);
            actual.closed_lease_transferred = true;
        }
        let actual = self.remote_serving.get_mut(&key).unwrap();
        #[cfg(test)]
        if let Some(mode) = self.remote_preparation_test.remove(&key.0) {
            actual.preparation.test_arm(mode)?;
        }
        if actual.preparation.completed().is_none() {
            let domain = actual
                .domain
                .as_ref()
                .ok_or_else(|| invalid("remote actual preparation writer domain absent"))?
                .clone();
            let committed = {
                let service = backend
                    .service
                    .try_lock()
                    .ok_or_else(|| invalid("remote committed native observer service busy"))?;
                service.observe_joint_decision_with_writer_domain_and_point(
                    &domain,
                    fresh,
                    work.business,
                    key.0,
                    Some(point),
                )?
            };
            let lease = actual
                .lease
                .as_ref()
                .ok_or_else(|| {
                    invalid("remote original lease disappeared before actual preparation owner")
                })?
                .clone();
            if mutating {
                let admission = work.admit_native_preparation(actual.process.clone())?;
                ClusterDataNodeService::prepare_remote_joint_serving(
                    &backend.service,
                    domain,
                    committed,
                    lease,
                    &admission,
                    &mut actual.preparation,
                )?;
            } else {
                let admission = work.admit_known_preparation_read(actual.process.clone())?;
                ClusterDataNodeService::read_known_remote_joint_preparation(
                    &backend.service,
                    domain,
                    committed,
                    lease,
                    &admission,
                    &mut actual.preparation,
                )?;
            }
        }
        // The actual preparation now owns this lease and ONE real read pin.
        // Drop only the extra pre-entry reference after retention, on success
        // or after a later request finds the already completed original owner.
        if let Some(custody) = actual.preparation.completed() {
            participant.retain_remote_preparation_route(custody)?;
            actual.lease.take();
        }
        work.require_request()?;
        let custody = actual
            .preparation
            .completed()
            .ok_or_else(|| invalid("remote completed preparation owner absent"))?;
        let observation =
            custody.observe_remote_preparation(&backend.transport, &leader, &request)?;
        let reply = participant.observe_actual_preparation_for_serving(
            &work.request,
            &work.role,
            &work.own,
            work.business,
            &work.point,
            &request,
            &observation,
        )?;
        if operation == NativeRemoteServingOperationV1::ResolvePrepared {
            participant
                .retained_serving_queue()
                .resolve_original_preparation(
                    work,
                    participant,
                    &actual.preparation,
                    &observation,
                )?;
        }
        Ok(reply)
    }
}

fn remaining(parent: &CancellationToken) -> Result<Duration> {
    parent.check()?;
    parent
        .deadline()
        .and_then(|d| d.checked_duration_since(Instant::now()))
        .filter(|d| !d.is_zero())
        .ok_or(BicDbError::QueryTimedOut)
}

#[cfg(test)]
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn test_remote_preparation_control(
        &mut self,
        range: crate::RangeId,
        arm: Option<u8>,
    ) -> Result<serde_json::Value> {
        let mut phases = Vec::new();
        if let Some(data) = &self.data {
            phases.extend(data.retained_remote_serving_participants()?);
        }
        if let Some(receiver) = &self.replica {
            phases.extend(receiver.retained_remote_serving_participants()?);
        }
        let phase = phases
            .into_iter()
            .find(|p| p.snapshot().binding.range_id == range)
            .ok_or_else(|| invalid("preparation test needs actual original participant"))?;
        if let Some(mode) = arm {
            if !matches!(mode, 1 | 2 | 3 | 4)
                || self.remote_host_poll_cuts.contains_key(&range)
                || self.remote_preparation_test.contains_key(&range)
                || self
                    .remote_serving
                    .contains_key(&(range, phase.snapshot().phase_id))
            {
                return Err(invalid(
                    "preparation control must arm once before actual first preparation",
                ));
            }
            self.remote_preparation_test.insert(range, mode);
        }
        let state = self.remote_serving.get(&(range, phase.snapshot().phase_id));
        // Capture actual memory-only liveness BEFORE diagnostic storage IO.
        let memory = serde_json::json!({"actual_participant":phase.snapshot(),"deferred_mode":self.remote_preparation_test.get(&range),
            "actual_host_poll_cut":self.remote_host_poll_cuts.get(&range),
            "continuation_present":state.is_some(),"lifetime_error":state.and_then(|s|s.process.require_memory().err()).map(|e|e.to_string()),
            "closed_started":state.map(|s|s.closed_started),"closed_lease_transferred":state.map(|s|s.closed_lease_transferred),
            "original_lease_still_retained":state.map(|s|s.lease.is_some()),"native_attempt":state.map(|s|s.preparation.test_native_outcome()),"bridge":state.map(|s|s.bridge.test_outcome())});
        let native = state.map(|s| s.preparation.test_native_outcome());
        let sha = native.as_ref().and_then(|n| {
            n["actual_native_attempt"]["issued_candidate"]["checksum_sha256"].as_str()
        });
        let xid = native
            .as_ref()
            .and_then(|n| n["actual_native_attempt"]["actual_issued_xid"].as_u64());
        let service = self
            .backend
            .service
            .try_lock()
            .ok_or_else(|| invalid("preparation forensic actual service busy"))?;
        let database = service.database();
        let db = database
            .try_read()
            .ok_or_else(|| invalid("preparation forensic actual DB busy"))?;
        let storage = db.test_remote_preparation_forensics(range, sha, xid)?;
        Ok(serde_json::json!({"memory_before_storage":memory,"storage":storage,"Serving":false}))
    }
}

impl NativeMetadataControlHost {
    fn collect_actual_remote_preparations(
        &self,
        work: &NativeRemoteServingIngress,
        request: &CancellationToken,
    ) -> Result<Arc<crate::distribution_range_handoff::NativeRemotePreparedQuorum>> {
        use crate::distribution_native_host::NativeRemoteServingRequestV1;
        use crate::distribution_range_handoff::NativeRemotePreparedRound;
        let backend = &self.backend;
        let leader = backend
            .provider
            .current_metadata_leader_hint(request)?
            .ok_or_else(|| invalid("remote prepared collection current metadata leader absent"))?;
        let mut round = NativeRemotePreparedRound::begin(
            Arc::new(work.request.manifest.clone()),
            work.request.phase_id,
            work.request.decision_sha256.clone(),
            request,
        )?;
        // Privately allocated readonly collection identity; never the original
        // caller operation's mutation watermark or renewed server request.
        let operation = Uuid::now_v7();
        for (index, node) in work.request.manifest.new_members.keys().enumerate() {
            work.require_request()?;
            request.check()?;
            let business = Uuid::now_v7();
            let own = backend.transport.acquire_host_metadata_observation(
                &leader,
                business,
                remaining(request)?,
                request,
            )?;
            let observe = NativeRemoteServingRequestV1 {
                version: 1,
                manifest: work.request.manifest.clone(),
                phase_id: work.request.phase_id,
                decision_sha256: work.request.decision_sha256.clone(),
                original_operation: operation,
                sequence: u64::try_from(index)
                    .map_err(|_| invalid("remote collection sequence overflow"))?
                    + 1,
                operation: NativeRemoteServingOperationV1::ObservePrepared,
                business,
                challenge: Uuid::now_v7(),
                budget_ms: remaining(request)?.as_millis().clamp(1, 5000) as u64,
            };
            let actual = backend
                .transport
                .request_remote_native_serving(node, &observe, &own, business, request)?;
            round.record(actual)?;
        }
        let actual = round.try_finish().map_err(|(error, _round)| error)?;
        work.require_request()?;
        request.check()?;
        Ok(Arc::new(actual))
    }
    fn apply_finite_remote_bridge(
        &mut self,
        participant: &Arc<NativeRemoteNewParticipant>,
        work: &mut NativeRemoteServingIngress,
    ) -> Result<NativeRemoteServingReplyV1> {
        work.require_request()?;
        let key = (
            work.request.manifest.capture_range.id,
            work.request.phase_id,
        );
        let original = self.remote_serving.get(&key).ok_or_else(|| {
            invalid("remote bridge cannot construct a missing original preparation host")
        })?;
        if !Arc::ptr_eq(&original.participant, participant)
            || original.manifest_sha256 != work.request.manifest.checksum_sha256
            || original.decision_sha256 != work.request.decision_sha256
        {
            return Err(invalid(
                "remote bridge changed original actual host participant/native D",
            ));
        }
        let process = original.process.clone();
        let custody = original.preparation.completed().cloned().ok_or_else(|| {
            invalid("remote bridge needs SAME actual completed preparation owner")
        })?;
        if original.bridge.completed().is_some() {
            return Err(invalid(
                "remote bridge mutation already completed; readonly observation required",
            ));
        }
        let request = process
            .bound_request(&work.bound_actor_request())
            .child_linked_to(&work.own.original_request_cancellation())
            .child_with_deadline(work.role.deadline().min(work.own.deadline()));
        let quorum = self.collect_actual_remote_preparations(work, &request)?;
        let backend = self.backend.clone();
        let leader = backend
            .provider
            .current_metadata_leader_hint(&request)?
            .ok_or_else(|| invalid("remote bridge post-ALL metadata leader absent"))?;
        // Genuine distinct post-ALL OWN/MStore + local protected owner read.
        let local = custody.observe_remote_preparation(&backend.transport, &leader, &request)?;
        work.mark_effect_entered()?;
        let admission = quorum.admit_local_bridge(local, work.admit_bridge(process)?)?;
        let original = self.remote_serving.get_mut(&key).unwrap();
        ClusterDataNodeService::publish_remote_joint_bridge(
            &backend.service,
            &admission,
            &mut original.bridge,
        )?;
        let native = original
            .bridge
            .completed()
            .cloned()
            .ok_or_else(|| invalid("remote bridge actual completed native receipt absent"))?;
        participant.retain_remote_bridge_route(&custody, &native)?;
        // A new actual protected read is not a decoded vote conversion. The
        // underlying bridge receipt was retained before outer request checks.
        let observed = custody.observe_remote_preparation(&backend.transport, &leader, &request)?;
        let mut reply = participant.observe_actual_preparation_for_serving(
            &work.request,
            &work.role,
            &work.own,
            work.business,
            &work.point,
            &request,
            &observed,
        )?;
        ClusterDataNodeService::observe_remote_joint_bridge_point(
            &backend.service,
            &admission,
            &native,
            &mut reply.actual,
        )?;
        work.require_request()?;
        request.check()?;
        reply.require_request(&work.request)?;
        Ok(reply)
    }
}

impl NativeMetadataControlHost {
    fn apply_finite_remote_bridge_read(
        &mut self,
        participant: &Arc<NativeRemoteNewParticipant>,
        work: &mut NativeRemoteServingIngress,
    ) -> Result<NativeRemoteServingReplyV1> {
        work.require_request()?;
        let key = (
            work.request.manifest.capture_range.id,
            work.request.phase_id,
        );
        let original = self.remote_serving.get(&key).ok_or_else(|| {
            invalid("remote bridge cannot construct a missing original preparation host")
        })?;
        if !Arc::ptr_eq(&original.participant, participant)
            || original.manifest_sha256 != work.request.manifest.checksum_sha256
            || original.decision_sha256 != work.request.decision_sha256
        {
            return Err(invalid(
                "remote bridge changed original actual host participant/native D",
            ));
        }
        let process = original.process.clone();
        let custody = original.preparation.completed().cloned().ok_or_else(|| {
            invalid("remote bridge needs SAME actual completed preparation owner")
        })?;
        let request = process
            .bound_request(&work.bound_actor_request())
            .child_linked_to(&work.own.original_request_cancellation())
            .child_with_deadline(work.role.deadline().min(work.own.deadline()));
        let quorum = self.collect_actual_remote_preparations(work, &request)?;
        let backend = self.backend.clone();
        let leader = backend
            .provider
            .current_metadata_leader_hint(&request)?
            .ok_or_else(|| invalid("remote bridge post-ALL metadata leader absent"))?;
        // Genuine distinct post-ALL OWN/MStore + local protected owner read.
        let local = custody.observe_remote_preparation(&backend.transport, &leader, &request)?;
        let admission = quorum.admit_local_bridge_read(local, work.admit_bridge_read(process)?)?;
        let original = self.remote_serving.get_mut(&key).unwrap();
        ClusterDataNodeService::read_known_remote_joint_bridge(
            &backend.service,
            &admission,
            &mut original.bridge,
        )?;
        let native = original
            .bridge
            .completed()
            .cloned()
            .ok_or_else(|| invalid("remote bridge actual completed native receipt absent"))?;
        participant.retain_remote_bridge_route(&custody, &native)?;
        // A new actual protected read is not a decoded vote conversion. The
        // underlying bridge receipt was retained before outer request checks.
        let observed = custody.observe_remote_preparation(&backend.transport, &leader, &request)?;
        let mut reply = participant.observe_actual_preparation_for_serving(
            &work.request,
            &work.role,
            &work.own,
            work.business,
            &work.point,
            &request,
            &observed,
        )?;
        ClusterDataNodeService::observe_remote_joint_bridge_read_point(
            &backend.service,
            &admission,
            &native,
            &mut reply.actual,
        )?;
        work.require_request()?;
        request.check()?;
        reply.require_request(&work.request)?;
        if work.request.operation == NativeRemoteServingOperationV1::ResolveBridge {
            participant
                .retained_serving_queue()
                .resolve_original_bridge(work, participant, &original.bridge, &admission)?;
        }
        Ok(reply)
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn test_remote_bridge_control(
        &mut self,
        range: crate::RangeId,
        arm: Option<u8>,
    ) -> Result<serde_json::Value> {
        if let Some(mode) = arm {
            let actual = self
                .remote_serving
                .iter_mut()
                .find(|((r, _), _)| *r == range)
                .map(|(_, s)| s)
                .ok_or_else(|| {
                    invalid("bridge fault needs SAME actual prepared host continuation")
                })?;
            actual.process.require_memory()?;
            if actual.preparation.completed().is_none() {
                return Err(invalid(
                    "bridge fault requires genuine actual completed preparation",
                ));
            }
            actual.bridge.test_arm(mode)?;
        }
        self.test_remote_preparation_control(range, None)
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
    /// Selected real TLS ingress, after queue take and actual caller binding,
    /// before the ordinary handler. Neither the original RPC nor Role/OWN is
    /// canceled or edited. Mode4 waits for the real short caller's deadline.
    fn test_after_actual_actor_bind(
        &mut self,
        work: &NativeRemoteServingIngress,
        parent: &CancellationToken,
    ) -> Result<()> {
        let range = work.request.manifest.capture_range.id;
        let Some(mode @ (3 | 4)) = self.remote_preparation_test.get(&range).copied() else {
            return Ok(());
        };
        if work.request.operation != NativeRemoteServingOperationV1::Prepare {
            return Err(invalid(
                "host poll cut requires the original actual Prepare",
            ));
        }
        parent.check()?;
        work.require_request()?;
        let now = Instant::now();
        let original_deadline = work.token.deadline();
        let server_left = original_deadline.map(|d| d.saturating_duration_since(now).as_micros());
        let role_left = work
            .role
            .deadline()
            .saturating_duration_since(now)
            .as_micros();
        let own_left = work
            .own
            .deadline()
            .saturating_duration_since(now)
            .as_micros();
        if mode == 4
            && parent
                .deadline()
                .is_none_or(|d| d.saturating_duration_since(now) > Duration::from_millis(250))
        {
            return Err(invalid(
                "expiry cut requires the actual caller's short original window",
            ));
        }
        self.remote_preparation_test.remove(&range);
        if mode == 3 {
            parent.cancel();
        } else {
            while parent.check().is_ok() {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        let original_server_still_live = work.token.check().is_ok();
        let role_still_live = work.role.require_live_request().is_ok();
        let own_still_live = work.own.require_live_request(work.business).is_ok();
        self.remote_host_poll_cuts.insert(range, serde_json::json!({
            "selected_original_tls_ingress":true,"mode":mode,"fired":1,
            "original_operation":work.request.original_operation,"sequence":work.request.sequence,
            "actual_poll_parent_error":parent.check().err().map(|e|e.to_string()),
            "bound_effect_error":work.bound_actor_request().check().err().map(|e|e.to_string()),
            "original_server_remaining_us_before":server_left,
            "original_Role_remaining_us_before":role_left,"original_OWN_remaining_us_before":own_left,
            "original_server_still_live":original_server_still_live,
            "original_Role_still_live":role_still_live,"original_OWN_still_live":own_still_live,
            "original_server_deadline_unchanged":work.token.deadline()==original_deadline,
            "original_server_cancelled":work.token.is_cancel_requested(),
            "real_poll_parent_cancelled":parent.is_cancel_requested(),
        }));
        Ok(())
    }
}
