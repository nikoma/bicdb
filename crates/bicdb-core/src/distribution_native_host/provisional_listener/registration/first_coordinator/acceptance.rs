//! Test-only DATA facades on the actual control owner; no raw owner escapes.
use super::*;
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn test_first_topology(
        &self,
        parent: &CancellationToken,
    ) -> Result<ClusterTopology> {
        let backend = &self.backend;
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        let leader = backend
            .provider
            .current_metadata_leader_hint(&token)?
            .ok_or_else(|| invalid("FIRST test setup actual metadata leader unavailable"))?;
        let business = Uuid::now_v7();
        let own = backend.transport.acquire_host_metadata_observation(
            &leader,
            business,
            Duration::from_secs(5),
            &token,
        )?;
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &backend.service,
            &own,
            business,
            &token,
        )?
        .ok_or_else(|| invalid("FIRST test setup actual MStore absent"))?;
        point.require_request(&own, business)?;
        point.require_external_current()?;
        Ok(own.committed_topology(business)?.clone())
    }
    pub(in crate::distribution_native_host) fn test_first_register_learner(
        &self,
        learner: crate::ClusterNode,
        parent: &CancellationToken,
    ) -> Result<ClusterTopology> {
        let backend = &self.backend;
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        // The request traverses the existing real TLS registration handler.
        // Registration DATA does not enroll native birth or grant target OWN.
        let leader = backend
            .provider
            .current_metadata_leader_hint(&token)?
            .ok_or_else(|| invalid("FIRST registration setup actual leader absent"))?;
        if leader != backend.node_id {
            return Err(invalid(
                "FIRST registration setup needs actual metadata leader host",
            ));
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| invalid("FIRST actual registration wall clock before epoch"))?
            .as_millis();
        let now = u64::try_from(now)
            .map_err(|_| invalid("FIRST actual registration wall clock overflow"))?;
        let result = backend
            .transport
            .register_metadata_learner(&backend.node_id, learner, now);
        // Return evidence before any outer test assertion; no result synthesis.
        let actual = result?;
        token.check()?;
        Ok(actual)
    }
    pub(in crate::distribution_native_host) fn test_first_commit_allocation(
        &self,
        range: crate::RangeId,
        target: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<ClusterTopology> {
        let backend = &self.backend;
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        let business = Uuid::now_v7();
        let own = backend.transport.acquire_host_metadata_observation(
            &backend.node_id,
            business,
            Duration::from_secs(5),
            &token,
        )?;
        let candidate =
            crate::distribution_native_host::joined_data_handoff::test_first_allocation_candidate(
                own.committed_topology(business)?,
                range,
                target,
            )?;
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &backend.service,
            &own,
            business,
            &token,
        )?
        .ok_or_else(|| invalid("FIRST test allocation requires actual MStore"))?;
        let open = backend
            .service
            .try_lock()
            .ok_or_else(|| invalid("FIRST test allocation service busy"))?
            .listener_open_guard();
        {
            let mut store = backend.lock_metadata(&token)?;
            store.require_listener_database(&open)?;
            store.require_native_control_leader(&own, business)?;
            point.require_current_memory()?;
            store.propose_topology(candidate.clone())?;
        }
        // Real append/ACK/quorum commit, never direct topology replacement.
        backend.heartbeat(&token)?;
        let post_business = Uuid::now_v7();
        let post = backend.transport.acquire_host_metadata_observation(
            &backend.node_id,
            post_business,
            Duration::from_secs(5),
            &token,
        )?;
        if post.committed_topology(post_business)? != &candidate {
            return Err(invalid("FIRST test actual allocation has not committed"));
        }
        token.check()?;
        Ok(candidate)
    }
    pub(in crate::distribution_native_host) fn test_first_coordinator_snapshot(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        let actual = self
            .first_coordinators
            .get(&range)
            .ok_or_else(|| invalid("actual FIRST coordinator absent"))?;
        Ok(
            serde_json::json!({"original_operation":actual.operation,"target_attempt":actual.target_attempt,
            "next_sequence":actual.next_sequence,"pending_receive":actual.pending_receive,
            "pending_original_return":actual.pending_original_return,"last_return":actual.last_return,
            "pending_control":actual.pending_control.as_ref().map(|(kind,peer,intent)|serde_json::json!({"kind":format!("{kind:?}"),"peer":peer,"intent":intent})),
            "known_closed_peers":actual.closed,"last_close_resolution":actual.last_close_resolution,
            "native_decision":actual.native_decision.snapshot(),
            "original_process_live_at_snapshot":actual.process.check().is_ok(),
            "returned_count":actual.returned_count,
            "outcome_chain_sha256":hex::encode(actual.outcome_chain.clone().finalize()),"data_only":true}),
        )
    }
    pub(in crate::distribution_native_host) fn test_first_replay_request(
        &mut self,
        range: crate::RangeId,
        request: NativeFirstReceiveRequestV1,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let actual = self
            .first_coordinators
            .get_mut(&range)
            .ok_or_else(|| invalid("actual FIRST coordinator absent"))?;
        actual.process.check()?;
        if request.original_operation != actual.operation || request.range != range {
            return Err(invalid(
                "FIRST replay control did not select its actual original factory",
            ));
        }
        let token = parent
            .child_linked_to(&actual.process)
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let (own, business) = actual.own(&token)?;
        // Exact original wire DATA only, sent using the actual source-owned
        // transport and new actual OWN. Target must DENY before any fresh
        // FIRST witness/effect, not treat this as renewed old-step authority.
        let result = actual.backend.transport.request_native_first_receive(
            &actual.plan,
            &request,
            &own,
            business,
            &token,
        );
        result.map(|r| r.reply().result.clone())
    }

    /// Adversarial genuine factory send: production drive intentionally will
    /// not dispatch this while its actual prior native result is unknown.
    /// This test exercises the independent recipient gate using the SAME
    /// owned source transport/process and fresh real OWN, not a DTO grant.
    pub(in crate::distribution_native_host) fn test_first_new_mutation_after_unknown(
        &mut self,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let actual = self
            .first_coordinators
            .get_mut(&range)
            .ok_or_else(|| invalid("actual FIRST coordinator absent"))?;
        let mut request = actual
            .pending_receive
            .clone()
            .ok_or_else(|| invalid("actual FIRST native unknown absent"))?;
        if request.operation != NativeFirstReceiveOperationV1::PublishFinalImage {
            return Err(invalid(
                "FIRST negative must follow its genuine attempted final native call",
            ));
        }
        let token = parent
            .child_linked_to(&actual.process)
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let (own, business) = actual.own(&token)?;
        request.sequence = actual.next_sequence;
        actual.next_sequence = actual
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| invalid("FIRST sequence exhausted"))?;
        request.business = business;
        request.challenge = Uuid::now_v7();
        request.budget_ms = remaining_ms(&token)?;
        request.operation = NativeFirstReceiveOperationV1::PublishNativeBase;
        request.source_operation = Some(NativeRemoteImageSourceOperationV1::Finish);
        actual.reserve_intent(serde_json::to_value(&request)?)?;
        let result = actual.backend.transport.request_native_first_receive(
            &actual.plan,
            &request,
            &own,
            business,
            &token,
        );
        actual.record_return(serde_json::json!({"adversarial_next_mutation_actual_return":true,
            "ok":result.is_ok(),"error":result.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}));
        result.map(|r| r.reply().result.clone())
    }
    pub(in crate::distribution_native_host) fn test_first_arm_close_loss(
        &self,
        range: crate::RangeId,
    ) -> Result<impl Drop + Send + 'static> {
        self.data
            .as_ref()
            .ok_or_else(|| invalid("FIRST actual OLD host absent"))?
            .admission_for_old_step()?
            .test_first_arm_close_reply_loss(range)
    }
    pub(in crate::distribution_native_host) fn test_first_close_loss_snapshot(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        self.data
            .as_ref()
            .ok_or_else(|| invalid("FIRST actual OLD host absent"))?
            .admission_for_old_step()?
            .test_first_close_reply_loss_snapshot(range)
    }

    /// Two genuine source-factory requests race the actual recipient gate.
    /// This is adversarial test sending, not production parallel mutation.
    pub(in crate::distribution_native_host) fn test_first_concurrent_admission(
        &mut self,
        range: crate::RangeId,
        barrier: &crate::distribution_native_host::remote_first::NativeFirstIngressBarrier,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let actual = self
            .first_coordinators
            .get_mut(&range)
            .ok_or_else(|| invalid("actual FIRST factory absent"))?;
        if !actual.target_fenced
            || actual.target_attempt.is_some()
            || actual.pending_control.is_some()
            || actual.pending_receive.is_some()
        {
            return Err(invalid(
                "FIRST concurrent control requires genuine fenced, unenrolled original factory",
            ));
        }
        let token = parent
            .child_linked_to(&actual.process)
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let (own0, business0) = actual.own(&token)?;
        let (own1, business1) = actual.own(&token)?;
        let first_sequence = actual.next_sequence;
        let second_sequence = first_sequence
            .checked_add(1)
            .ok_or_else(|| invalid("FIRST sequence exhausted"))?;
        actual.next_sequence = second_sequence
            .checked_add(1)
            .ok_or_else(|| invalid("FIRST sequence exhausted"))?;
        let make = |sequence, business| {
            let p = actual.plan.promise();
            Ok::<_, BicDbError>(NativeFirstReceiveRequestV1 {
                version: 1,
                relocation: p.relocation_id,
                range: p.range_id,
                epoch: p.old_epoch,
                session: p.session_id,
                nonce: p.nonce,
                original_operation: actual.operation,
                sequence,
                source: actual.donor.clone(),
                target_attempt: None,
                operation: NativeFirstReceiveOperationV1::Enroll,
                source_operation: None,
                business,
                challenge: Uuid::now_v7(),
                budget_ms: remaining_ms(&token)?,
            })
        };
        let first = make(first_sequence, business0)?;
        let second = make(second_sequence, business1)?;
        actual.reserve_intent(serde_json::to_value(&first)?)?;
        actual.pending_receive = Some(first.clone());
        let backend = actual.backend.clone();
        let plan = actual.plan.clone();
        let (first_result, second_result) = std::thread::scope(|scope| {
            let first_token = token.clone();
            let first_request = first.clone();
            let worker = scope.spawn(move || {
                backend.transport.request_native_first_receive(
                    &plan,
                    &first_request,
                    &own0,
                    business0,
                    &first_token,
                )
            });
            // Always release before scoped join, even if wait/second RPC panics.
            let second_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                barrier.wait_entered(&token)?;
                actual.backend.transport.request_native_first_receive(
                    &actual.plan,
                    &second,
                    &own1,
                    business1,
                    &token,
                )
            }));
            barrier.release();
            let first_result = match worker.join() {
                Ok(value) => value,
                Err(panic) => std::panic::resume_unwind(panic),
            };
            (first_result, second_result)
        });
        let second_panicked = second_result.is_err();
        // Exact original API returns before any later checks; native custody
        // remains with the real recipient, and source keeps the first intent.
        let evidence = serde_json::json!({"first_request":first,"second_request":second,
            "first_ok":first_result.is_ok(),"first_reply":first_result.as_ref().ok().map(|r|r.reply()),
            "first_error":first_result.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
            "second_panicked":second_panicked,
            "second_ok":second_result.as_ref().ok().is_some_and(|r|r.is_ok()),
            "second_error":second_result.as_ref().ok().and_then(|r|r.as_ref().err()).map(crate::distribution_native_host::remote_first::error_data),
            "barrier":barrier.snapshot()});
        actual.record_return(evidence.clone());
        let second_result = match second_result {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        drop(second_result);
        let post = match &first_result {
            Ok(frame) => {
                if let Some(a) = frame.reply().target_attempt {
                    actual.target_attempt = Some(a);
                }
                frame.require_request_live().and_then(|()| token.check())
            }
            Err(_) => Err(invalid(
                "FIRST original concurrent request did not return success",
            )),
        };
        let result = serde_json::json!({"actual_returns":evidence,"original_postchecks_ok":post.is_ok(),
            "postcheck_error":post.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)});
        if post.is_ok() {
            actual.pending_receive = None;
            actual.pending_original_return = None;
        }
        Ok(result)
    }
}

impl NativeMetadataControlHost {
    /// Send through the original range-leader factory and genuine pinned
    /// transport. The extra target UUID is request correlation DATA only.
    pub(in crate::distribution_native_host) fn test_first_competing_handoff_step(
        &self,
        range: crate::RangeId,
        target_attempt: Uuid,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        use crate::distribution_range_handoff::{
            NativeJointHandoffStepOperationV1, NativeJointHandoffStepRequestV1,
        };
        let actual = self
            .first_coordinators
            .get(&range)
            .ok_or_else(|| invalid("cross-protocol actual FIRST factory absent"))?;
        actual.process.check()?;
        let token = parent
            .child_linked_to(&actual.process)
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        // This is the existing real OWN+attached current-MStore producer; no
        // old role/token is revived and no request-supplied plan is admitted.
        let (own, business) = actual.own(&token)?;
        let request = NativeJointHandoffStepRequestV1::from_plan(
            &actual.plan,
            NativeJointHandoffStepOperationV1::TargetInstall {
                donor: actual.donor.clone(),
                target_attempt,
            },
            business,
            remaining_ms(&token)?,
        )?;
        let started = Instant::now();
        let result = actual.backend.transport.request_native_joint_handoff_step(
            &actual.target,
            &actual.plan,
            &request,
            &own,
            &token,
        );
        // Capture the EXACT exchange return before diagnostic allocation or
        // any later check. This facade returns DATA even for an RPC error;
        // that must never be interpreted as a successful install/observer.
        let rpc_ok = result.is_ok();
        let error = result
            .as_ref()
            .err()
            .map(crate::distribution_native_host::remote_first::error_data);
        let reply = result.as_ref().ok().map(|frame| frame.reply().clone());
        Ok(
            serde_json::json!({"request":request,"actual_sender":actual.backend.node_id,
            "actual_target":actual.target,"actual_rpc_returned_ok":rpc_ok,
            "actual_rpc_error":error,"actual_rpc_reply":reply,"elapsed_us":started.elapsed().as_micros(),
            "original_factory_operation":actual.operation,"original_factory_target_attempt":actual.target_attempt,
            "facade_return_is_DATA_not_RPC_success":true,"raw_owner_or_grant_exported":false}),
        )
    }
    fn test_first_cross_protocol_native_snapshot(
        &self,
        range: crate::RangeId,
        source_lineage: bool,
    ) -> Result<serde_json::Value> {
        let database = self
            .backend
            .service
            .try_lock()
            .ok_or_else(|| invalid("cross-protocol actual diagnostic service busy"))?
            .database();
        let db = database
            .try_read()
            .ok_or_else(|| invalid("cross-protocol actual diagnostic DB busy"))?;
        let counters = db.test_joint_prepublication_storage(range)?;
        let (records, lineage) = if source_lineage {
            // The target-only8-record inventory is not a supported bound for
            // a populated INITIAL donor. Read its actual protected lineage.
            let actual = db.current_native_range_source_lineage(range)?;
            (
                None,
                Some(serde_json::json!({
                    "range_index":actual.range_index(),"local_B":actual.actual_local_visibility_sequence(),
                    "native_receipt_sha256":actual.native_receipt_sha256(),
                    "history_generation":actual.history_generation(),"H":actual.logical_history_sha256(),
                    "native_write_range":actual.native_write_range()?,
                    "joint_decision_sha256":actual.current_joint_decision_sha256(),
                })),
            )
        } else {
            (Some(db.test_registered_remote_native_inventory()?), None)
        };
        Ok(
            serde_json::json!({"same_actual_database_open":db.incarnation_id(),
            "actual_native_storage":counters,"actual_target_native_record_hashes":records,
            "actual_source_lineage":lineage,
            "health_checked":true,"read_only":true,"authority_granted":false}),
        )
    }
    pub(in crate::distribution_native_host) fn test_first_cross_protocol_target_snapshot(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        // Each existing helper drops its own guards before the next helper.
        // This combined result is explicitly NOT an atomic global snapshot.
        let first = self.test_first_receiver_snapshot()?;
        let step = self.test_joint_handoff_step_state(range)?;
        let native = self.test_first_cross_protocol_native_snapshot(range, false)?;
        Ok(
            serde_json::json!({"first":first,"step":step,"native":native,
            "non_atomic_combined_DATA":true,"authority_granted":false}),
        )
    }
    pub(in crate::distribution_native_host) fn test_first_cross_protocol_source_snapshot(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        let source = self.test_source_discovery_state(range)?;
        let native = self.test_first_cross_protocol_native_snapshot(range, true)?;
        Ok(
            serde_json::json!({"source_discovery":source,"native":native,
            "non_atomic_combined_DATA":true,"authority_granted":false}),
        )
    }
}
