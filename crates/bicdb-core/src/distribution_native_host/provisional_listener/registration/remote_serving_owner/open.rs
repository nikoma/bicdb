//! One actual recipient Open turn, using the sole existing typed activation
//! sink. No native preparation/bridge replay or decoded local QQ conversion.
use super::*;
use crate::distribution_native_host::NativeRemoteServingRequestV1;
use crate::distribution_range_handoff::{
    NativeJointOpenAdmission, NativeRemoteBridgeQuorum, NativeRemoteBridgeRound,
};
impl NativeMetadataControlHost {
    fn collect_actual_remote_bridges(
        &self,
        work: &NativeRemoteServingIngress,
        request: &CancellationToken,
    ) -> Result<Arc<NativeRemoteBridgeQuorum>> {
        let backend = &self.backend;
        let leader = backend
            .provider
            .current_metadata_leader_hint(request)?
            .ok_or_else(|| invalid("remote bridge collection current metadata leader absent"))?;
        let mut round = NativeRemoteBridgeRound::begin(
            Arc::new(work.request.manifest.clone()),
            work.request.phase_id,
            work.request.decision_sha256.clone(),
            request,
        )?;
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
                    .map_err(|_| invalid("remote bridge collection sequence overflow"))?
                    + 1,
                operation: NativeRemoteServingOperationV1::ObserveBridge,
                business,
                challenge: Uuid::now_v7(),
                budget_ms: remaining(request)?.as_millis().clamp(1, 5000) as u64,
            };
            round.record(
                backend
                    .transport
                    .request_remote_native_serving(node, &observe, &own, business, request)?,
            )?;
        }
        let proof = round.try_finish().map_err(|(error, _round)| error)?;
        work.require_request()?;
        request.check()?;
        Ok(Arc::new(proof))
    }
    pub(super) fn apply_finite_remote_open(
        &mut self,
        participant: &Arc<NativeRemoteNewParticipant>,
        work: &mut NativeRemoteServingIngress,
    ) -> Result<NativeRemoteServingReplyV1> {
        work.require_request()?;
        let key = (
            work.request.manifest.capture_range.id,
            work.request.phase_id,
        );
        let original = self
            .remote_serving
            .get(&key)
            .ok_or_else(|| invalid("Open cannot construct missing actual prepared/native owner"))?;
        if !Arc::ptr_eq(&original.participant, participant)
            || original.manifest_sha256 != work.request.manifest.checksum_sha256
            || original.decision_sha256 != work.request.decision_sha256
        {
            return Err(invalid(
                "Open changed original actual participant/manifest/decision",
            ));
        }
        let process = original.process.clone();
        let custody = original
            .preparation
            .completed()
            .cloned()
            .ok_or_else(|| invalid("Open original actual preparation absent"))?;
        original.bridge.require_resolution()?;
        let native = original
            .bridge
            .completed()
            .cloned()
            .ok_or_else(|| invalid("Open actual known native bridge absent"))?;
        let request = process
            .bound_request(&work.bound_actor_request())
            .child_linked_to(&work.own.original_request_cancellation())
            .child_with_deadline(work.role.deadline().min(work.own.deadline()));
        // Only a genuine new mutating request can enter the old owned retry.
        // Reconciliation preserves exact O/C/generation, actual gate and IO
        // histories; it cannot attribute a failed old fsync as old success.
        let ingress = work.admit_open(process)?;
        if work.request.operation == NativeRemoteServingOperationV1::ReconcileOpen {
            custody.reconcile_remote_open(&ingress, &native)?;
        }
        let bridges = self.collect_actual_remote_bridges(work, &request)?;
        let backend = self.backend.clone();
        let leader = backend
            .provider
            .current_metadata_leader_hint(&request)?
            .ok_or_else(|| invalid("Open post-ALL metadata leader absent"))?;
        let local =
            custody.observe_remote_open(native.clone(), &backend.transport, &leader, &request)?;
        local.require_participant(&participant.snapshot().checksum_sha256)?;
        let admission =
            NativeJointOpenAdmission::from_actual_remote_bridge(bridges, local, ingress)?;
        #[cfg(test)]
        let mode = self.remote_serving.get_mut(&key).unwrap().open_test.take();
        #[cfg(test)]
        let fault = if mode == Some(2) {
            let service = backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("Open actual fault original service busy"))?;
            let database = service.database();
            let database = database
                .try_read()
                .ok_or_else(|| invalid("Open actual fault original DB busy"))?;
            Some(bicdb_page::test_faults::arm_io_failure(
                "native_joint_activation.catalogue.dir_sync",
                database.data_path(),
                1,
                false,
            ))
        } else {
            None
        };
        let returned = admission.persist_local_catalogue_activation(&request);
        #[cfg(test)]
        if let Some(fault) = fault {
            let now = Instant::now();
            self.remote_serving.get_mut(&key).unwrap().open_cut = Some(
                serde_json::json!({"actual_dirsync_faults":fault.fired(),
                "actual_sink_returned_ok":returned.is_ok(),"actual_sink_error":returned.as_ref().err().map(ToString::to_string),
                "original_server_remaining_us":work.token.deadline().map(|d|d.saturating_duration_since(now).as_micros()),
                "original_OWN_remaining_us":work.own.deadline().saturating_duration_since(now).as_micros(),
                "original_Role_remaining_us":work.role.deadline().saturating_duration_since(now).as_micros()}),
            );
            drop(fault);
        }
        let activation = Arc::new(returned?);
        // Retain the actual returned activation before any outer postcheck or
        // response assembly. On earlier Err the Store retains its typed IO owner.
        let original = self.remote_serving.get_mut(&key).unwrap();
        if let Some(previous) = &original.activation {
            if previous.activation_sha256()? != activation.activation_sha256()?
                || previous.native_bridge_sha256() != activation.native_bridge_sha256()
            {
                return Err(invalid("Open replaced original retained activation"));
            }
        } else {
            original.activation = Some(activation.clone());
        }
        #[cfg(test)]
        if mode == Some(1) {
            let now = Instant::now();
            let before = serde_json::json!({"original_server_remaining_us":work.token.deadline().map(|d|d.saturating_duration_since(now).as_micros()),
                "original_OWN_remaining_us":work.own.deadline().saturating_duration_since(now).as_micros(),
                "original_Role_remaining_us":work.role.deadline().saturating_duration_since(now).as_micros(),
                "same_process_live":original.process.require_memory().is_ok(),"actual_activation_retained":original.activation.is_some()});
            work.token.cancel();
            original.open_cut = Some(
                serde_json::json!({"actual_original_server_cancelled":work.token.is_cancel_requested(),
                "before_cancel":before,"same_process_live_after_cancel":original.process.require_memory().is_ok(),
                "client_timeout_is_not_this_cut":true}),
            );
        }
        let observed =
            custody.observe_remote_open(native, &backend.transport, &leader, &request)?;
        let reply = participant.observe_actual_open_for_serving(
            &work.request,
            &work.role,
            &work.own,
            work.business,
            &work.point,
            &request,
            &observed,
        )?;
        work.require_request()?;
        request.check()?;
        if work.request.operation == NativeRemoteServingOperationV1::ReconcileOpen {
            participant.retained_serving_queue().resolve_original_open(
                work,
                &admission,
                &observed,
                &activation,
            )?;
        }
        Ok(reply)
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn test_remote_open_control(
        &mut self,
        range: crate::RangeId,
        mode: Option<u8>,
    ) -> Result<serde_json::Value> {
        let original = self
            .remote_serving
            .iter_mut()
            .find(|((r, _), _)| *r == range)
            .map(|(_, s)| s)
            .ok_or_else(|| invalid("Open control requires actual original prepared host"))?;
        original.process.require_memory()?;
        original.bridge.require_resolution()?;
        let native = original
            .bridge
            .completed()
            .cloned()
            .ok_or_else(|| invalid("Open control actual native bridge absent"))?;
        let custody = original
            .preparation
            .completed()
            .cloned()
            .ok_or_else(|| invalid("Open control actual preparation absent"))?;
        if let Some(mode) = mode {
            if mode == 3 {
                return custody.test_remote_open_io(&native, true);
            }
            if !matches!(mode, 1 | 2)
                || original.open_test.is_some()
                || original.open_cut.is_some()
                || original.activation.is_some()
            {
                return Err(invalid(
                    "Open fault must arm once before actual first activation",
                ));
            }
            original.open_test = Some(mode);
        }
        let memory = serde_json::json!({"original_process_live":original.process.require_memory().is_ok(),
            "actual_activation_retained":original.activation.is_some(),"cut":original.open_cut,
            "queue":original.participant.retained_serving_queue().test_open_queue_state(),
            "native_preparation":original.preparation.test_native_outcome(),"native_bridge":original.bridge.test_outcome()});
        let io = custody.test_remote_open_io(&native, false)?;
        let native = self.test_remote_preparation_control(range, None)?;
        Ok(
            serde_json::json!({"memory_before_storage":memory,"actual_io":io,"native_storage":native["storage"]}),
        )
    }
}
