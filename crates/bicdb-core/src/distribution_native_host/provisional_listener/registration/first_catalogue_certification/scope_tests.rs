//! Real normal-host inputs, real pinned HostOWNs and protected MStore points.
//! These controls expose DATA only; no native plan or local grant is returned.
use super::*;
use crate::distribution_data::{ActualNativeJointMetadataPoint, ClusterServiceOpenGuard};
use std::io::Write as _;
use std::sync::Arc;
type Actual = (
    HostMetadataReadObservation,
    Uuid,
    Arc<ActualNativeJointMetadataPoint>,
    ClusterServiceOpenGuard,
);
fn observed(owner: &NativeMetadataControlHost, parent: &CancellationToken) -> Result<Actual> {
    let backend = &owner.backend;
    let leader = backend
        .provider
        .current_metadata_leader_hint(parent)?
        .ok_or_else(|| invalid("actual scope-control metadata leader absent"))?;
    let business = Uuid::now_v7();
    let own = backend.transport.acquire_host_metadata_observation(
        &leader,
        business,
        remaining_finite(parent)?,
        parent,
    )?;
    let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
        &backend.service,
        &own,
        business,
        parent,
    )?
    .ok_or_else(|| invalid("actual scope-control protected metadata point absent"))?;
    let open = backend
        .service
        .try_lock()
        .ok_or_else(|| invalid("actual scope-control service busy"))?
        .listener_open_guard();
    Ok((own, business, point, open))
}
fn issue(
    owner: &NativeMetadataControlHost,
    actual: Actual,
    mf: &NativeRemoteImageManifestV1,
    parent: &CancellationToken,
) -> Result<Arc<NativeMetadataOwnerCatalogueScope>> {
    let (own, business, point, open) = actual;
    let store = owner.backend.lock_metadata(parent)?;
    NativeMetadataOwnerCatalogueScope::from_actual_metadata_owner(
        own, business, point, open, &store, mf, parent,
    )
}
fn note(events: &mut Vec<serde_json::Value>, value: serde_json::Value) {
    let _ = writeln!(
        std::io::stderr().lock(),
        "NATIVE_CATALOGUE_SCOPE_ACTUAL {}",
        value
    );
    events.push(value);
}
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn test_metadata_owner_catalogue_scope(
        &self,
        range_leader: &Self,
        intent: &NativeFirstCatalogueCertificationIntent,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        let mf = &intent.manifest;
        let mut events = Vec::new();
        if self.backend.node_id == range_leader.backend.node_id
            || self.backend.node_id == mf.capture_range.leader
            || range_leader.backend.node_id != mf.capture_range.leader
        {
            return Err(invalid(
                "scope control requires actual DISTINCT metadata and range leaders",
            ));
        }
        // A metadata owner remains refused by the unchanged generic range MQ.
        let b = Uuid::now_v7();
        let generic = self.backend.transport.acquire_metadata_read_observation(
            &self.backend.node_id,
            b,
            mf.capture_range.id,
            mf.capture_range.epoch,
            remaining_finite(&token)?,
            &token,
        );
        note(
            &mut events,
            serde_json::json!({"cut":"metadata_owner_generic_range_READ","ok":generic.is_ok(),"error":generic.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}),
        );
        let denied = generic
            .err()
            .ok_or_else(|| invalid("metadata owner acquired range-leader-only MQ"))?;
        if !denied
            .to_string()
            .contains("metadata acquisition requester/range authority changed")
        {
            return Err(denied);
        }
        // The actual range leader still issues its original strict plan. It is
        // compared only; no observation/closure/grant is fabricated from it.
        let b = Uuid::now_v7();
        let range = range_leader
            .backend
            .transport
            .acquire_metadata_read_observation(
                &self.backend.node_id,
                b,
                mf.capture_range.id,
                mf.capture_range.epoch,
                remaining_finite(&token)?,
                &token,
            )?;
        let range_plan = crate::distribution_range_handoff::NativeJointOldRound::begin(
            range,
            b,
            mf.relocation_id,
        )?
        .plan();
        let comparison = issue(self, observed(self, &token)?, mf, &token)?;
        let exact = comparison.checksum_sha256()? == range_plan.checksum_sha256()?;
        note(
            &mut events,
            serde_json::json!({"cut":"two_actual_purpose_factories","metadata_owner":self.backend.node_id,"range_owner":range_leader.backend.node_id,"scope_hash_equal":exact}),
        );
        if !exact {
            return Err(invalid(
                "metadata READ comparison differs from actual range-leader plan",
            ));
        }
        drop(comparison);
        drop(range_plan);
        // Actual foreign point/open, not a DTO or a constructed UUID.
        let (_, _, foreign_point, foreign_open) = observed(range_leader, &token)?;
        let (own, business, _, _) = observed(self, &token)?;
        own.require_live_request(business)?;
        token.check()?;
        let wrong = issue(
            self,
            (own, business, foreign_point, foreign_open),
            mf,
            &token,
        );
        note(
            &mut events,
            serde_json::json!({"cut":"actual_foreign_MStore_open_point","ok":wrong.is_ok(),"error":wrong.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}),
        );
        if !wrong.as_ref().err().is_some_and(|e| {
            e.to_string()
                .contains("metadata RPC destination differs from actual native birth/incarnation")
        }) {
            return Err(invalid(
                "actual foreign metadata birth/open refusal cut not reached",
            ));
        }
        // Dedicated original issuance child is canceled; outer action stays live.
        let cancelled_parent = token.child();
        let cancelled = issue(
            self,
            observed(self, &cancelled_parent)?,
            mf,
            &cancelled_parent,
        )?;
        cancelled_parent.cancel();
        let denied = cancelled.require_live();
        token.check()?;
        note(
            &mut events,
            serde_json::json!({"cut":"actual_original_scope_request_cancelled","ok":denied.is_ok(),"error":denied.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),"outer_action_live":true}),
        );
        if !matches!(denied, Err(BicDbError::QueryCanceled)) {
            return Err(invalid("actual original scope cancellation not refused"));
        }
        // Wrong immutable source close DATA cannot stand in for exact ancestry.
        let good = issue(self, observed(self, &token)?, mf, &token)?;
        let mut changed = mf.clone();
        changed.nonce = Uuid::now_v7();
        changed.checksum_sha256 = changed.checksum()?;
        let denied = good.require_manifest(&changed);
        note(
            &mut events,
            serde_json::json!({"cut":"changed_original_manifest_nonce","ok":denied.is_ok(),"error":denied.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}),
        );
        if !denied.as_ref().err().is_some_and(|e| {
            e.to_string()
                .contains("source manifest differs from exact protected OLD record/prefix/base")
        }) {
            return Err(invalid(
                "exact source OLD ancestry refusal cut not reached for changed MF",
            ));
        }
        drop(good);
        // A lawful real proposal changes the actual protected MStore version.
        // It is load metadata on an untouched range, not a second relocation.
        let (fresh, business, point, open) = observed(self, &token)?;
        let current = fresh.committed_topology(business)?;
        let untouched = current
            .ranges
            .values()
            .find(|r| r.id != mf.capture_range.id)
            .map(|r| (r.id, r.approximate_bytes, r.approximate_qps))
            .ok_or_else(|| invalid("scope-control untouched range absent"))?;
        let mut view = crate::DistributionStore::native_registration_plan(
            self.config.clone(),
            current.clone(),
        )?;
        view.update_range_load(
            untouched.0,
            untouched
                .1
                .checked_add(1)
                .ok_or_else(|| invalid("scope-control load overflow"))?,
            untouched.2,
            &self.backend.node_id,
            now_finite_ms(),
        )?;
        let candidate = view.topology().clone();
        let (stale, actual_entry) = {
            let mut store = self.backend.lock_metadata(&token)?;
            store.require_listener_database(&open)?;
            store.require_native_control_leader(&fresh, business)?;
            point.require_request(&fresh, business)?;
            point.require_current_metadata_store(&store)?;
            let original = NativeMetadataOwnerCatalogueScope::from_actual_metadata_owner(
                fresh, business, point, open, &store, mf, &token,
            )?;
            // SAME real MStore latch excludes automatic heartbeat mutations
            // between this successful precheck and the actual load proposal.
            original.require_live()?;
            token.check()?;
            let actual_entry = store.propose_topology(candidate)?;
            (original, actual_entry)
        };
        note(
            &mut events,
            serde_json::json!({"cut":"actual_untouched_range_load_proposal_returned","term":actual_entry.term,"index":actual_entry.index,"range":untouched.0,"native_relocation":false}),
        );
        self.backend.heartbeat(&token)?;
        let (post, business, point, open) = observed(self, &token)?;
        let store = self.backend.lock_metadata(&token)?;
        stale.test_original_request_live()?;
        let denied =
            stale.require_fresh_proposal_owner(&post, business, &point, &open, &store, &token);
        note(
            &mut events,
            serde_json::json!({"cut":"actual_later_MStore_version_before_certification_proposal","ok":denied.is_ok(),"error":denied.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),"actual_current_term":post.term(),"actual_read_index":post.read_index()}),
        );
        if !denied.as_ref().err().is_some_and(|e| {
            e.to_string()
                .contains("actual metadata authority version changed/dirty")
        }) {
            return Err(invalid(
                "actual MStore-version refusal cut not reached before certification proposal",
            ));
        }
        token.check()?;
        // LAST: actual original HostOWN and outer windows may coincide. No
        // shorter independent OWN TTL is assumed, and no later operation uses
        // this original token after natural expiry. Only diagnostic DATA exits.
        let actual = observed(self, &token)?;
        let entered = Instant::now();
        let own_deadline = actual.0.deadline();
        let own_context = actual.0.context_id();
        let outer_deadline = token.deadline();
        let own_remaining_us = own_deadline.saturating_duration_since(entered).as_micros();
        let outer_remaining_us =
            outer_deadline.map(|d| d.saturating_duration_since(entered).as_micros());
        let expired = issue(self, actual, mf, &token)?;
        while expired.test_original_request_live().is_ok() {
            std::thread::sleep(Duration::from_millis(2));
        }
        let denied = expired.require_live();
        let outer_denied = token.check();
        let returned = Instant::now();
        note(
            &mut events,
            serde_json::json!({"cut":"coupled_original_scope_expiry_LAST",
            "original_scope_result":denied.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
            "outer_result":outer_denied.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
            "original_OWN_context":own_context,"original_OWN_remaining_us_at_issuance":own_remaining_us,
            "outer_remaining_us_at_issuance":outer_remaining_us,"own_deadline_equals_outer":outer_deadline==Some(own_deadline),
            "OWN_deadline_reached_at_diagnostic":returned>=own_deadline,
            "outer_deadline_reached_at_diagnostic":outer_deadline.map(|d|returned>=d),
            "elapsed_us":returned.saturating_duration_since(entered).as_micros(),
            "OWN_only_expiry_tested":false,"production_action_success":false,"expired_scope_reused":false}),
        );
        if !matches!(denied, Err(BicDbError::QueryTimedOut)) {
            return Err(invalid(
                "actual coupled original-scope QueryTimedOut refusal not reached",
            ));
        }
        // No post-expiry currentness check, new scope, network request,
        // metadata proposal or native effect. This is test diagnostic DATA.
        Ok(
            serde_json::json!({"actual_control_events":events,"all_actual_refusals_reached":true,"actual_distinct_purpose_hash_match":exact,"metadata_scope_minted_native_grant":false,"opaque_frame_fabricated":false,"source_definition_NOT_TESTED":true}),
        )
    }
}
