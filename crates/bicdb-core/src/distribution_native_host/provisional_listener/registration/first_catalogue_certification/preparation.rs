//! Same actual metadata owner's finite preparation caller. Its original
//! operation identity is privately allocated, never supplied by a selector.
use super::*;
use crate::distribution_native_host::{
    NativeRemoteServingOperationV1, NativeRemoteServingRequestV1,
};
use crate::distribution_transport::AuthenticatedNativeRemoteServingFrame;

pub(super) struct NativeFirstServingCoordinator {
    operation: Uuid,
    process: CancellationToken,
    next_sequence: u64,
    pending: Option<Pending>,
    returned: u64,
    return_chain: sha2::Sha256,
    accounting_incomplete: bool,
}
struct Pending {
    node: ClusterNodeId,
    request: NativeRemoteServingRequestV1,
    returned: Option<AuthenticatedNativeRemoteServingFrame>,
    resolution_returned: Option<AuthenticatedNativeRemoteServingFrame>,
    poll_errors: Vec<serde_json::Value>,
    poll_errors_omitted: u64,
    original_error: Option<serde_json::Value>,
    resolution_error: Option<serde_json::Value>,
}
impl NativeFirstServingCoordinator {
    fn new(process: &CancellationToken) -> Self {
        Self {
            operation: Uuid::now_v7(),
            process: process.child(),
            next_sequence: 1,
            pending: None,
            returned: 0,
            return_chain: sha2::Sha256::new(),
            accounting_incomplete: false,
        }
    }
}
impl NativeMetadataControlHost {
    /// One genuine new authenticated bounded step. A failed/lost Prepare is
    /// never sent again; only ResolvePrepared observes its original recipient.
    pub(in crate::distribution_native_host) fn drive_finite_remote_preparation(
        &mut self,
        relocation: crate::distribution::RelocationId,
        phase: Uuid,
        node: &ClusterNodeId,
        resolve: bool,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.drive_finite_remote_serving_step(
            relocation,
            phase,
            node,
            if resolve {
                NativeRemoteServingOperationV1::ResolvePrepared
            } else {
                NativeRemoteServingOperationV1::Prepare
            },
            parent,
        )
    }
    pub(in crate::distribution_native_host) fn drive_finite_remote_bridge(
        &mut self,
        relocation: crate::distribution::RelocationId,
        phase: Uuid,
        node: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.drive_finite_remote_serving_step(
            relocation,
            phase,
            node,
            NativeRemoteServingOperationV1::PublishBridge,
            parent,
        )
    }
    pub(in crate::distribution_native_host) fn resolve_finite_remote_bridge(
        &mut self,
        relocation: crate::distribution::RelocationId,
        phase: Uuid,
        node: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.drive_finite_remote_serving_step(
            relocation,
            phase,
            node,
            NativeRemoteServingOperationV1::ResolveBridge,
            parent,
        )
    }
    pub(in crate::distribution_native_host) fn drive_finite_remote_open(
        &mut self,
        relocation: crate::distribution::RelocationId,
        phase: Uuid,
        node: &ClusterNodeId,
        reconcile: bool,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.drive_finite_remote_serving_step(
            relocation,
            phase,
            node,
            if reconcile {
                NativeRemoteServingOperationV1::ReconcileOpen
            } else {
                NativeRemoteServingOperationV1::Open
            },
            parent,
        )
    }
    pub(in crate::distribution_native_host) fn inspect_finite_remote_open(
        &self,
        relocation: crate::distribution::RelocationId,
        phase: Uuid,
        node: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.inspect_finite_remote_open_or_released(
            relocation,
            phase,
            node,
            NativeRemoteServingOperationV1::ObserveOpen,
            parent,
        )
    }
    pub(in crate::distribution_native_host) fn inspect_finite_remote_released(
        &self,
        relocation: crate::distribution::RelocationId,
        phase: Uuid,
        node: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.inspect_finite_remote_open_or_released(
            relocation,
            phase,
            node,
            NativeRemoteServingOperationV1::ObserveReleased,
            parent,
        )
    }
    pub(in crate::distribution_native_host) fn drive_finite_remote_release(
        &mut self,
        relocation: crate::distribution::RelocationId,
        phase: Uuid,
        node: &ClusterNodeId,
        reconcile: bool,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.drive_finite_remote_serving_step(
            relocation,
            phase,
            node,
            if reconcile {
                NativeRemoteServingOperationV1::ReconcileRelease
            } else {
                NativeRemoteServingOperationV1::Release
            },
            parent,
        )
    }
    fn inspect_finite_remote_open_or_released(
        &self,
        relocation: crate::distribution::RelocationId,
        phase: Uuid,
        node: &ClusterNodeId,
        operation: NativeRemoteServingOperationV1,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        if !matches!(
            operation,
            NativeRemoteServingOperationV1::ObserveOpen
                | NativeRemoteServingOperationV1::ObserveReleased
        ) {
            return Err(invalid("finite Open/released reader operation differs"));
        }
        self.inspect_actual_retained_serving_read(relocation, phase, node, operation, parent)
    }
    fn inspect_actual_retained_serving_read(
        &self,
        relocation: crate::distribution::RelocationId,
        phase: Uuid,
        node: &ClusterNodeId,
        operation: NativeRemoteServingOperationV1,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let process = self
            .data_process
            .get()
            .ok_or_else(|| invalid("readonly Open caller actual host process absent"))?;
        process.check()?;
        let token = parent
            .child_linked_to(process)
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let owner = self
            .remote_catalogue_certifications
            .get(&(relocation, phase))
            .filter(|o| !o.stopped)
            .ok_or_else(|| invalid("readonly Open original coordinator absent"))?;
        let history = owner
            .certified
            .as_ref()
            .ok_or_else(|| invalid("readonly Open original certification absent"))?;
        let original = owner
            .serving
            .as_ref()
            .ok_or_else(|| invalid("readonly Open original serving coordinator absent"))?;
        original.process.check()?;
        let token = token.child_linked_to(&original.process);
        let business = Uuid::now_v7();
        let own = self.backend.transport.acquire_host_metadata_observation(
            &self.backend.node_id,
            business,
            remaining_finite(&token)?,
            &token,
        )?;
        serving::NativeRemoteMembershipScope {
            history,
            fresh: &own,
            business,
            token: &token,
            candidate: None,
        }
        .require_view(own.committed_topology(business)?, &self.backend.node_id)?;
        let request = NativeRemoteServingRequestV1 {
            version: 1,
            manifest: history.manifest.clone(),
            phase_id: phase,
            decision_sha256: history.record.decision_sha256().into(),
            original_operation: Uuid::now_v7(),
            sequence: 1,
            operation,
            business,
            challenge: Uuid::now_v7(),
            budget_ms: remaining_finite(&token)?.as_millis().clamp(1, 5000) as u64,
        };
        let frame = self
            .backend
            .transport
            .request_remote_native_serving(node, &request, &own, business, &token)?;
        frame.require_live_binding(&request.manifest, phase, &request.decision_sha256)?;
        token.check()?;
        process.check()?;
        Ok(
            serde_json::json!({"point":frame.point(),"metadata_anchor":frame.metadata_anchor(),
            "readonly_no_queue_resolution":true,"Serving":false}),
        )
    }
    #[cfg(test)]
    pub(in crate::distribution_native_host) fn test_registered_successor_read_dispatch(
        &self,
        relocation: crate::distribution::RelocationId,
        phase: Uuid,
        node: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        // SAME retained metadata coordinator/manifest and real transport in
        // both calls. DATA inspection cannot construct a Policy admission.
        let positive = self.inspect_actual_retained_serving_read(
            relocation,
            phase,
            node,
            NativeRemoteServingOperationV1::ObserveReleasedForPolicy,
            parent,
        );
        let denied = self.inspect_actual_retained_serving_read(
            relocation,
            phase,
            node,
            NativeRemoteServingOperationV1::ObserveDecision,
            parent,
        );
        Ok(
            serde_json::json!({"actual_policy_read_returned_ok":positive.is_ok(),
            "actual_policy_read":positive.as_ref().ok(),
            "actual_policy_read_error":positive.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
            "actual_nonselected_returned_ok":denied.is_ok(),
            "actual_nonselected_error":denied.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
            "actual_nonselected_cluster_body":denied.as_ref().err().and_then(|e|match e {BicDbError::Cluster(body)=>Some(body),_=>None}),
            "actual_pinned_destination":node,"socket_selector_or_endpoint_override":false}),
        )
    }
    fn drive_finite_remote_serving_step(
        &mut self,
        relocation: crate::distribution::RelocationId,
        phase: Uuid,
        node: &ClusterNodeId,
        operation: NativeRemoteServingOperationV1,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        if !matches!(
            operation,
            NativeRemoteServingOperationV1::Prepare
                | NativeRemoteServingOperationV1::ResolvePrepared
                | NativeRemoteServingOperationV1::PublishBridge
                | NativeRemoteServingOperationV1::ResolveBridge
                | NativeRemoteServingOperationV1::Open
                | NativeRemoteServingOperationV1::ReconcileOpen
                | NativeRemoteServingOperationV1::Release
                | NativeRemoteServingOperationV1::ReconcileRelease
        ) {
            return Err(invalid("finite coordinator operation not implemented"));
        }
        let resolve = matches!(
            operation,
            NativeRemoteServingOperationV1::ResolvePrepared
                | NativeRemoteServingOperationV1::ResolveBridge
                | NativeRemoteServingOperationV1::ReconcileOpen
                | NativeRemoteServingOperationV1::ReconcileRelease
        );
        let actual_process = self
            .data_process
            .get()
            .ok_or_else(|| invalid("remote preparation coordinator actual host process absent"))?;
        actual_process.check()?;
        let host_process = self
            .remote_serving_process
            .get_or_init(|| actual_process.child())
            .clone();
        host_process.check()?;
        let token = parent
            .child_linked_to(&host_process)
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        token.check()?;
        let key = (relocation, phase);
        let backend = self.backend.clone();
        {
            let owner = self
                .remote_catalogue_certifications
                .get(&key)
                .filter(|o| !o.stopped)
                .ok_or_else(|| {
                    invalid("remote preparation caller original metadata owner absent")
                })?;
            owner.certified.as_ref().ok_or_else(|| {
                invalid("remote preparation caller original certification proof absent")
            })?;
            if let Some(state) = &owner.serving {
                state.process.check()?;
                match (&state.pending,resolve) {
                    (Some(pending),true) if &pending.node==node && matches!((operation,pending.request.operation),
                        (NativeRemoteServingOperationV1::ResolvePrepared,NativeRemoteServingOperationV1::Prepare)
                        | (NativeRemoteServingOperationV1::ResolveBridge,NativeRemoteServingOperationV1::PublishBridge)
                        | (NativeRemoteServingOperationV1::ReconcileOpen,NativeRemoteServingOperationV1::Open)
                        | (NativeRemoteServingOperationV1::ReconcileRelease,NativeRemoteServingOperationV1::Release))=>{},
                    (Some(_),_)=>return Err(invalid("remote preparation original pending return requires readonly resolution of SAME recipient; no mutation replay")),
                    (None,true)=>return Err(invalid("remote preparation has no original pending request to resolve")),
                    _=>{},
                }
            } else if resolve {
                return Err(invalid(
                    "remote preparation cannot reconstruct a missing original coordinator",
                ));
            }
        }
        let business = Uuid::now_v7();
        let own = backend.transport.acquire_host_metadata_observation(
            &backend.node_id,
            business,
            remaining_finite(&token)?,
            &token,
        )?;
        let request = {
            let owner = self.remote_catalogue_certifications.get_mut(&key).unwrap();
            let history = owner.certified.as_ref().unwrap();
            let scope = serving::NativeRemoteMembershipScope {
                history,
                fresh: &own,
                business,
                token: &token,
                candidate: None,
            };
            scope.require_view(own.committed_topology(business)?, &backend.node_id)?;
            if !history.manifest.new_members.contains_key(node)
                || own
                    .committed_topology(business)?
                    .relocations
                    .get(&relocation)
                    .and_then(|r| r.native_joint_handoff_v1.as_ref())
                    .is_none_or(|r| {
                        r.stage()
                            != crate::distribution::NativeJointHandoffStageV1::AwaitingActivation
                    })
            {
                return Err(invalid(
                    "remote preparation caller needs actual frozen final member/stage",
                ));
            }
            let state = owner
                .serving
                .get_or_insert_with(|| NativeFirstServingCoordinator::new(&host_process));
            state.process.check()?;
            let sequence = state.next_sequence;
            state.next_sequence = sequence
                .checked_add(1)
                .ok_or_else(|| invalid("remote preparation sequence exhausted"))?;
            let request = NativeRemoteServingRequestV1 {
                version: 1,
                manifest: history.manifest.clone(),
                phase_id: phase,
                decision_sha256: history.record.decision_sha256().into(),
                original_operation: state.operation,
                sequence,
                operation,
                business,
                challenge: Uuid::now_v7(),
                budget_ms: remaining_finite(&token)?.as_millis().clamp(1, 5000) as u64,
            };
            request.require_shape()?;
            if !resolve {
                // Original intent is retained BEFORE the first RPC. The
                // sequence remains consumed even if dispatch never effects.
                state.pending = Some(Pending {
                    node: node.clone(),
                    request: request.clone(),
                    returned: None,
                    resolution_returned: None,
                    poll_errors: Vec::new(),
                    poll_errors_omitted: 0,
                    original_error: None,
                    resolution_error: None,
                });
            }
            request
        };
        // The real local owner can service its own TLS ingress while its
        // transport performs a blocking call on a distinct scoped thread.
        // No service/DB/MStore/version guard spans this wait or network work.
        let mut poll_errors = Vec::new();
        let mut poll_errors_omitted = 0u64;
        let returned = std::thread::scope(|scope| {
            let transport = backend.transport.clone();
            let wire_request = request.clone();
            let node = node.clone();
            let caller = token.child();
            let call = scope.spawn(move || {
                transport.request_remote_native_serving(
                    &node,
                    &wire_request,
                    &own,
                    business,
                    &caller,
                )
            });
            while !call.is_finished() {
                if token.check().is_err() {
                    break;
                }
                // A busy unrelated actor turn does not replace the primary
                // actual RPC outcome. Every queued effect checks its token.
                if let Err(error) = self.poll_finite_remote_serving(&token) {
                    if poll_errors.len() < 8 {
                        poll_errors.push(
                            crate::distribution_native_host::remote_first::error_data(&error),
                        );
                    } else {
                        poll_errors_omitted = poll_errors_omitted.saturating_add(1);
                    }
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            match call.join() {
                Ok(actual) => actual,
                Err(panic) => std::panic::resume_unwind(panic),
            }
        });
        let owner = self.remote_catalogue_certifications.get_mut(&key).unwrap();
        let state = owner.serving.as_mut().unwrap();
        let pending = state.pending.as_mut().unwrap();
        for error in poll_errors {
            if pending.poll_errors.len() < 8 {
                pending.poll_errors.push(error);
            } else {
                pending.poll_errors_omitted = pending.poll_errors_omitted.saturating_add(1);
            }
        }
        pending.poll_errors_omitted = pending
            .poll_errors_omitted
            .saturating_add(poll_errors_omitted);
        match state.returned.checked_add(1) {
            Some(count) => state.returned = count,
            None => state.accounting_incomplete = true,
        }
        let frame = match returned {
            Ok(frame) => {
                // Retain the actual authenticated return BEFORE postchecks.
                if resolve {
                    pending.resolution_returned = Some(frame);
                    pending.resolution_returned.as_ref().unwrap()
                } else {
                    pending.returned = Some(frame);
                    pending.returned.as_ref().unwrap()
                }
            }
            Err(error) => {
                let data = crate::distribution_native_host::remote_first::error_data(&error);
                use sha2::Digest;
                let fact = (|| -> Result<Vec<u8>> {
                    crate::distribution_range_image::bounded_json(
                        &(request.hash()?, false, &data),
                        4 * 1024 * 1024,
                    )
                })();
                match fact {
                    Ok(bytes) => state.return_chain.update(bytes),
                    Err(_) => state.accounting_incomplete = true,
                }
                if resolve {
                    pending.resolution_error = Some(data);
                } else {
                    pending.original_error = Some(data);
                }
                return Err(error);
            }
        };
        use sha2::Digest;
        let fact = (|| -> Result<Vec<u8>> {
            crate::distribution_range_image::bounded_json(
                &(request.hash()?, true, frame.point()),
                128 * 1024,
            )
        })();
        match fact {
            Ok(bytes) => state.return_chain.update(bytes),
            Err(_) => state.accounting_incomplete = true,
        }
        frame.require_live_binding(&request.manifest, phase, &request.decision_sha256)?;
        if frame.point().preparation.is_none()
            || (matches!(
                operation,
                NativeRemoteServingOperationV1::PublishBridge
                    | NativeRemoteServingOperationV1::ResolveBridge
            ) && frame.point().native_bridge_sha256.is_none())
            || (matches!(
                operation,
                NativeRemoteServingOperationV1::Open
                    | NativeRemoteServingOperationV1::ReconcileOpen
                    | NativeRemoteServingOperationV1::Release
                    | NativeRemoteServingOperationV1::ReconcileRelease
            ) && frame.point().catalogue_activation_sha256.is_none())
            || (matches!(
                operation,
                NativeRemoteServingOperationV1::Release
                    | NativeRemoteServingOperationV1::ReconcileRelease
            ) && (frame.point().native_bridge_sha256.is_none()
                || !frame.point().actual_dropped_open))
        {
            return Err(invalid(
                "remote preparation actual protected preparation absent from authenticated return",
            ));
        }
        token.check()?;
        state.process.check()?;
        let actual = serde_json::json!({"node":frame.node_id(),"point":frame.point(),"metadata_anchor":frame.metadata_anchor(),
            "original_request":pending.request,"original_error":pending.original_error,"resolution_error":pending.resolution_error,
            "original_authenticated_return":pending.returned.as_ref().map(|f|f.point()),"owner_poll_errors":pending.poll_errors,"owner_poll_errors_omitted":pending.poll_errors_omitted,
            "actual_authenticated_return_retained":true,"original_call_relabelled_success":false,"readonly_resolution":resolve && operation.readonly(),
            "local_quorum_constructed":false,"Serving":false});
        crate::distribution_range_image::bounded_json(&actual, 4 * 1024 * 1024)?;
        let post_business = Uuid::now_v7();
        let post = backend.transport.acquire_host_metadata_observation(
            &backend.node_id,
            post_business,
            remaining_finite(&token)?,
            &token,
        )?;
        let scope = serving::NativeRemoteMembershipScope {
            history: owner.certified.as_ref().unwrap(),
            fresh: &post,
            business: post_business,
            token: &token,
            candidate: None,
        };
        scope.require_view(post.committed_topology(post_business)?, &backend.node_id)?;
        frame.require_request_live()?;
        token.check()?;
        if state.accounting_incomplete {
            return Err(invalid(
                "remote preparation outcome accounting incomplete; retain original pending owner",
            ));
        }
        // Compact only after genuine return AND all postchecks. No saved
        // reply is reused as fresh Role/OWN/native/quorum permission.
        state.pending.take();
        Ok(actual)
    }
}
