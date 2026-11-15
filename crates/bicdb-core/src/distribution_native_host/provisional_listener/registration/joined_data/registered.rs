//! Original Registered FIRST custody into genuine DATA ingress. Generic
//! Joined re-enrollment is never used; ordinary requests retain their own
//! fresh metadata/data quorum, fencing and Strong snapshot protocols.
use super::*;

impl NativeJoinedDataHost {
    pub(super) fn registered_range_status(&self) -> Option<serde_json::Value> {
        let original = self.registered.as_ref()?;
        let range = self.selected_range.as_ref()?;
        // No metadata guard spans the independent actual DB status read.
        let metadata = self
            .parts
            .backend
            .metadata
            .try_lock()
            .map(|store| store.status());
        let native_result = match self.parts.factory.database.try_read() {
            Some(db) => db
                .require_joined_publication_storage_health()
                .and_then(|()| db.native_range_prefix_observation(range.id)),
            None => Err(invalid("actual DB busy during readonly Joined status")),
        };
        let (native, error) = match native_result {
            Ok(point) => (Some(point), None),
            Err(error) => (None, Some(error.to_string())),
        };
        let replica_ready = native.as_ref().map(|point| {
            !point.invalidated
                && point.native_applied_index.is_some_and(|index| {
                    index >= original.historical_native_bridge().data().range_index()
                })
                && point.pending_command_index.is_none()
                && point.materialized_pending_index.is_none()
        });
        let leader_prefix = self.enrollment.as_ref().map(|owner| {
            owner
                .coordinator
                .initial_data_leader_admission_observed(range)
        });
        Some(
            serde_json::json!({"range_id":range.id,"range_epoch":range.epoch,
            "local_node":self.parts.backend.node_id,"range_leader":range.leader,
            "replica_ready_observed":replica_ready,"native_watermarks":native,"diagnostic":error,
            "leader_recovered_prefix_observed":leader_prefix,"metadata_status":metadata,
            "leader_current_authority":"must_be_proved_by_each_request",
            "strong_read_readiness":"new_request_bound_barrier_required",
            "last_strong_barrier":null,"status_is_not_a_permission":true}),
        )
    }
    pub(super) fn publish_registered(&mut self, parent: &CancellationToken) -> Result<usize> {
        parent.check()?;
        if self.stopping {
            return Err(invalid("stopping Registered Joined cannot publish"));
        }
        let original = self
            .registered
            .as_ref()
            .ok_or_else(|| invalid("original Registered Joined custody absent"))?;
        original.require_original_lifetime()?;
        let selected = self
            .selected_range
            .as_ref()
            .ok_or_else(|| invalid("Registered Joined selected range absent"))?;
        let enrollment = self
            .enrollment
            .as_ref()
            .ok_or_else(|| invalid("Registered Joined exact installed enrollment absent"))?;
        if !Arc::ptr_eq(&enrollment.coordinator, &original.coordinator)
            || !Arc::ptr_eq(&enrollment.guard, &original.guard)
        {
            return Err(invalid(
                "Registered Joined replaced original installed guard/coordinator",
            ));
        }
        let admission = self
            .admission
            .as_ref()
            .ok_or_else(|| invalid("Registered Joined actual socket admission absent"))?;
        let dispatcher = self
            .dispatcher
            .as_ref()
            .ok_or_else(|| invalid("Registered Joined actual dispatcher absent"))?;
        admission.require_listener(&self.parts.backend.service, &self.parts.runtime)?;
        // An explicit republish starts closed. Existing active workers keep
        // their own original proof; no old binding becomes this new grant.
        self.published.store(false, Ordering::Release);
        dispatcher.clear();
        // Genuine native majority history/epoch recovery, never synthetic
        // Ready, metadata progress or a manual local native floor.
        let recovered = enrollment.coordinator.recover_local_leadership(64)?;
        parent.check()?;
        let route = {
            let metadata = self.parts.backend.lock_metadata(parent)?;
            metadata
                .status()
                .leader_id
                .ok_or_else(|| invalid("Registered Joined metadata leader unknown"))?
        };
        let business = Uuid::now_v7();
        let own = self
            .parts
            .backend
            .transport
            .acquire_host_metadata_observation(&route, business, Duration::from_secs(5), parent)?;
        let current = own.committed_topology(business)?;
        if current
            .range_by_id(selected.id)
            .map(crate::distribution_native_read::canonical_range)
            != Some(crate::distribution_native_read::canonical_range(selected))
        {
            return Err(invalid(
                "Registered Joined current OWN selected domain changed",
            ));
        }
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &self.parts.backend.service,
            &own,
            business,
            parent,
        )?
        .ok_or_else(|| {
            invalid("Registered Joined requires actual native current-MStore publication point")
        })?;
        point.require_external_current()?;
        let native = original.historical_native_bridge();
        let (released, store_open, activation_sha) =
            original.historical_released_publication_point()?;
        let published = &self.published;
        let publication =
            self.parts
                .runtime
                .with_actual_event_dispatcher(dispatcher, || {
                    // Full validation and every mutable slot are obtained before the
                    // short metadata guard. No callback/native I/O runs under it.
                    let mut dispatcher_publication = dispatcher
                        .prepare_publication(&enrollment.coordinator, &enrollment.guard)?;
                    let service = self.parts.backend.service.try_lock().ok_or_else(|| {
                        invalid("Registered Joined actual service busy at publish")
                    })?;
                    let database = service.database();
                    if !Arc::ptr_eq(&database, &self.parts.factory.database) {
                        return Err(invalid("Registered Joined changed original actual DB"));
                    }
                    let db = database
                        .try_read()
                        .ok_or_else(|| invalid("Registered Joined actual DB busy at publish"))?;
                    let physical = enrollment.guard.joined_publication_scope(
                        &db,
                        selected,
                        &enrollment.coordinator,
                        &point,
                    )?;
                    let cut = physical.historical_cut(&native)?;
                    service.with_writer_data_domain_store(&db, |store| {
                        physical.require_actual_store(store)?;
                        let actual = store.observe_native_joint_activation_fact_at_historical_cut(
                            &cut,
                            native.clone(),
                        )?;
                        if actual.store_open_id() != store_open
                            || actual.activation_sha256()? != activation_sha
                        {
                            return Err(invalid(
                                "Registered Joined actual native/catalogue activation changed",
                            ));
                        }
                        let continuity =
                            released.observe_actual_continuity(store_open, &activation_sha)?;
                        store.require_native_joint_open_continuity(&actual, &continuity)?;
                        let range_slots =
                            released.borrow_arm_publication(store_open, &activation_sha)?;
                        parent.check()?;
                        original.require_original_lifetime()?;
                        admission.require_live()?;
                        let retired;
                        {
                            let version = physical.publication_guard()?;
                            version.require_current_memory()?;
                            parent.check()?;
                            retired = dispatcher_publication.publish();
                            published.store(true, Ordering::Release);
                        }
                        // Retired bindings and range-slot guards leave only after the
                        // version guard. Exact original custody survives any error.
                        drop(retired);
                        drop(range_slots);
                        Ok(())
                    })
                });
        if let Err(error) = publication {
            // A fallible outside-gate postcheck can follow the actual memory
            // publish. Retain all native custody, and close future ingress.
            self.published.store(false, Ordering::Release);
            dispatcher.try_clear_for_cleanup();
            return Err(error);
        }
        Ok(recovered)
    }
}
