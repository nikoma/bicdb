//! DATA-only observations around real service contention on an already-open
//! normal host. Neither this facade nor its scheduling creates authority.
use super::*;

impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn test_bootstrap_request_wait(
        &self,
        range: crate::RangeId,
        mode: u8,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let controller = self
            .controller
            .as_ref()
            .ok_or_else(|| invalid("actual bootstrap controller absent"))?;
        let metadata = || -> Result<serde_json::Value> {
            // Separate finite forensic budget; it neither replaces nor renews
            // the original bootstrap token, including after cancel/expiry.
            let diagnostic = CancellationToken::uncancelable()
                .child_with_deadline(Instant::now() + Duration::from_millis(500));
            let store = self.backend.lock_metadata(&diagnostic)?;
            Ok(
                serde_json::json!({"status":store.status(),"C":store.committed_topology(),
                "store_id":store.test_native_metadata_identity()}),
            )
        };
        let before_native = self.test_request_native_counters(range);
        let before_metadata = metadata();
        let original_deadline = parent.deadline();
        let bootstrap_calls = std::cell::Cell::new(0_u32);
        let parent_live_at_return = std::cell::Cell::new(None::<bool>);
        let actual = if mode == 3 {
            let held = self.backend.service.lock();
            let original = CancellationToken::uncancelable();
            bootstrap_calls.set(bootstrap_calls.get() + 1);
            let result = controller.bootstrap(&original);
            parent_live_at_return.set(Some(original.check().is_ok()));
            let returned = serde_json::json!({"actual_original_instant_busy":true,
                "actual_returned_ok":result.is_ok(),"original_error":result.as_ref().err().map(ToString::to_string),
                "unbounded_parent":original.deadline().is_none()});
            drop(held);
            Ok(returned)
        } else {
            ClusterDataNodeService::test_actual_request_service_wait(
                &self.backend.service,
                parent,
                mode,
                |original| {
                    bootstrap_calls.set(bootstrap_calls.get() + 1);
                    let result = controller.bootstrap(original);
                    // Sample acceptance liveness before holder cleanup or any
                    // forensic reads; those may lawfully outlast this caller.
                    parent_live_at_return.set(Some(original.check().is_ok()));
                    result
                },
            )
        };
        // Preserve the actual result before these separate read diagnostics.
        let after_native = self.test_request_native_counters(range);
        let after_metadata = metadata();
        Ok(serde_json::json!({"actual_control":actual.as_ref().ok(),
            "original_control_error":actual.as_ref().err().map(ToString::to_string),
            "before_native":before_native.as_ref().ok(),"after_native":after_native.as_ref().ok(),
            "before_native_error":before_native.as_ref().err().map(ToString::to_string),
            "after_native_error":after_native.as_ref().err().map(ToString::to_string),
            "before_metadata":before_metadata.as_ref().ok(),"after_metadata":after_metadata.as_ref().ok(),
            "before_metadata_error":before_metadata.as_ref().err().map(ToString::to_string),
            "after_metadata_error":after_metadata.as_ref().err().map(ToString::to_string),
            "original_parent_deadline_unchanged":parent.deadline()==original_deadline,
            "routing_DATA_only":true,"bootstrap_called_once":bootstrap_calls.get()==1,
            "original_parent_live_at_bootstrap_return":parent_live_at_return.get(),
            "diagnostic_metadata_budget_ms":500,
            "controller_store_open_id":controller.store_open_id,
            "actual_open_live_after":controller.actual_open.require_live().is_ok(),
            "same_bound_service_after":controller.require_service(&self.backend.service,controller.store_open_id).is_ok()}))
    }
    pub(in crate::distribution_native_host) fn test_committed_publication_request_wait(
        &self,
        range: crate::RangeId,
        mode: u8,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        // The real publication path is called exactly once, without proposing
        // anything. Reuse custody of the actual same-open service guard.
        self.test_status_request_wait(range, mode, parent, |original| {
            self.backend.publish_committed_metadata(original)
        })
    }
    pub(in crate::distribution_native_host) fn test_hot_request_wait(
        &self,
        range: crate::RangeId,
        mode: u8,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let data = self
            .data
            .as_ref()
            .ok_or_else(|| invalid("actual INITIAL host absent"))?;
        let hot = data
            .hot_recovery
            .as_ref()
            .ok_or_else(|| invalid("actual maintenance owner absent"))?;
        let process = self
            .data_process
            .get()
            .ok_or_else(|| invalid("actual process absent"))?;
        let request = parent
            .child_linked_to(process)
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let before = self.test_request_native_counters(range);
        let actual = hot.test_actual_request_state_wait(mode, &request);
        let after = self.test_request_native_counters(range);
        Ok(serde_json::json!({"actual_control":actual.as_ref().ok(),
            "original_control_error":actual.as_ref().err().map(ToString::to_string),
            "before_native":before.as_ref().ok(),"after_native":after.as_ref().ok(),
            "before_diagnostic_error":before.as_ref().err().map(ToString::to_string),
            "after_diagnostic_error":after.as_ref().err().map(ToString::to_string),
            "original_process_still_live":process.check().is_ok(),
            "scope":"actual retained maintenance State; no synthetic job/progress/readiness"}))
    }
    fn test_request_native_counters(&self, range: crate::RangeId) -> Result<serde_json::Value> {
        let database = self
            .backend
            .service
            .try_lock()
            .ok_or_else(|| invalid("request-wait diagnostic actual service busy"))?
            .database();
        let db = database
            .try_read()
            .ok_or_else(|| invalid("request-wait diagnostic actual DB busy"))?;
        db.test_joint_prepublication_storage(range)
    }
    pub(in crate::distribution_native_host) fn test_status_request_wait<T: serde::Serialize>(
        &self,
        range: crate::RangeId,
        mode: u8,
        parent: &CancellationToken,
        call: impl FnOnce(&CancellationToken) -> Result<T>,
    ) -> Result<serde_json::Value> {
        let process = self
            .data_process
            .get()
            .ok_or_else(|| invalid("request-wait actual DATA process absent"))?;
        let request = parent
            .child_linked_to(process)
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let before = self.test_request_native_counters(range);
        let actual = ClusterDataNodeService::test_actual_request_service_wait(
            &self.backend.service,
            &request,
            mode,
            call,
        );
        let after = self.test_request_native_counters(range);
        // All original API/control outcomes precede fallible diagnostics.
        Ok(serde_json::json!({"actual_control":actual.as_ref().ok(),
            "original_control_error":actual.as_ref().err().map(ToString::to_string),
            "before_native":before.as_ref().ok(),"after_native":after.as_ref().ok(),
            "before_diagnostic_error":before.as_ref().err().map(ToString::to_string),
            "after_diagnostic_error":after.as_ref().err().map(ToString::to_string),
            "original_process_still_live":process.check().is_ok(),
            "actual_OpenInitialData_replayed":false,"scope":"finite status after genuine Open; not first-Open fault schedule"}))
    }
    pub(in crate::distribution_native_host) fn test_old_request_wait(
        &self,
        range: crate::RangeId,
        mode: u8,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let data = self
            .data
            .as_ref()
            .ok_or_else(|| invalid("actual INITIAL OLD host absent"))?;
        let process = self
            .data_process
            .get()
            .ok_or_else(|| invalid("OLD wait actual process absent"))?;
        let request = parent
            .child_linked_to(process)
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let old = data
            .admission
            .as_ref()
            .ok_or_else(|| invalid("actual INITIAL OLD admission absent"))?
            .retained_actual_old_source_for_request(range, &request)?;
        let leader = self
            .backend
            .provider
            .current_metadata_leader_hint(&request)?
            .ok_or_else(|| invalid("OLD wait actual metadata leader absent"))?;
        let business = Uuid::now_v7();
        let own = self.backend.transport.acquire_host_metadata_observation(
            &leader,
            business,
            Duration::from_secs(5),
            &request,
        )?;
        old.plan()
            .require_source_host_metadata(&own, business, old.node_id())?;
        let bounded = request
            .child_linked_to(&own.original_request_cancellation())
            .child_with_deadline(own.deadline());
        let before = self.test_request_native_counters(range);
        let own_live_before = own.require_live_request(business).is_ok();
        let before_owner = old.test_actual_old_request_wait_owner();
        let instant_error = Arc::new(Mutex::new(None::<String>));
        let recorded_instant = instant_error.clone();
        let actual = ClusterDataNodeService::test_actual_request_service_wait(
            &data.initial.service,
            &bounded,
            mode,
            |original| {
                *recorded_instant.lock() = old
                    .require_current()
                    .as_ref()
                    .err()
                    .map(ToString::to_string);
                old.require_current_for_request(original)
            },
        );
        let after = self.test_request_native_counters(range);
        let after_owner = old.test_actual_old_request_wait_owner();
        Ok(serde_json::json!({"actual_control":actual.as_ref().ok(),
            "original_control_error":actual.as_ref().err().map(ToString::to_string),
            "before_native":before.as_ref().ok(),"after_native":after.as_ref().ok(),
            "before_diagnostic_error":before.as_ref().err().map(ToString::to_string),
            "after_diagnostic_error":after.as_ref().err().map(ToString::to_string),
            "original_OWN_live_before":own_live_before,
            "before_actual_OLD_owner":before_owner.as_ref().ok(),
            "after_actual_OLD_owner":after_owner.as_ref().ok(),
            "before_OLD_owner_error":before_owner.as_ref().err().map(ToString::to_string),
            "after_OLD_owner_error":after_owner.as_ref().err().map(ToString::to_string),
            "original_OLD_instant_error":instant_error.lock().clone(),
            "original_OWN_live_after":own.require_live_request(business).is_ok(),
            "actual_OWN_deadline":format!("{:?}",own.deadline()),
            "original_parent_deadline":format!("{:?}",request.deadline()),
            "original_process_still_live":process.check().is_ok(),
            "SourceStart_replayed":false,"capture_permission_manufactured":false,
            "scope":"actual CLOSED observation only; expiry is earliest original caller/OWN, not OWN-only"}))
    }
}
