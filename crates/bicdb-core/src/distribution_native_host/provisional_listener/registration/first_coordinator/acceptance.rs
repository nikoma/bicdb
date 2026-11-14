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
}
