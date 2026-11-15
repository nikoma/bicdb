//! Actual metadata-leader owner independently reads recipient dual durability.
//! Immutable coordinator input is intent DATA, never a forwarded quorum.
use super::*;
mod preparation;
mod serving;
#[cfg(test)]
pub(in crate::distribution_native_host) mod test_cuts;
use crate::distribution_range_handoff::{
    NativeMetadataOwnerCatalogueScope, NativeRemoteCatalogueRound, NativeRemoteNewFactSetV1,
    NativeRemoteNewOperationV1, NativeRemoteNewRequestV1,
};
use crate::distribution_range_image::NativeRemoteImageManifestV1;
pub(crate) use serving::NativeRemoteMembershipScope;

#[derive(Clone)]
pub(crate) struct NativeFirstCatalogueCertificationIntent {
    pub(super) manifest: NativeRemoteImageManifestV1,
    pub(super) facts: NativeRemoteNewFactSetV1,
    pub(super) decision_sha256: String,
}
#[cfg(test)]
impl NativeFirstCatalogueCertificationIntent {
    pub(in crate::distribution_native_host) fn relocation_id(
        &self,
    ) -> crate::distribution::RelocationId {
        self.manifest.relocation_id
    }
    pub(in crate::distribution_native_host) fn phase_id(&self) -> Uuid {
        self.facts.phase_id
    }
    pub(in crate::distribution_native_host) fn final_member_ids(&self) -> Vec<ClusterNodeId> {
        self.manifest.new_members.keys().cloned().collect()
    }
    pub(in crate::distribution_native_host) fn participant_count(&self) -> usize {
        self.manifest
            .old_members
            .keys()
            .chain(self.manifest.new_members.keys())
            .collect::<std::collections::BTreeSet<_>>()
            .len()
    }
}
pub(super) struct NativeFirstCatalogueCertificationAttempt {
    manifest_sha256: String,
    facts_sha256: String,
    decision_sha256: String,
    candidate: Option<ClusterTopology>,
    proofs: Vec<crate::distribution_range_handoff::NativeRemoteDualDurableDecision>,
    proof_bytes: usize,
    expected_proposal: Option<(u64, u64)>,
    proposal: Option<crate::distribution_consensus::MetadataLogEntry>,
    actual_proposal_return: Option<serde_json::Value>,
    certified: Option<serving::NativeFirstCertifiedHistory>,
    membership: Option<serving::NativeFirstMembershipAttempt>,
    membership_history: Vec<serving::NativeFirstMembershipAttempt>,
    pub(super) stopped: bool,
    serving: Option<preparation::NativeFirstServingCoordinator>,
}
impl NativeMetadataControlHost {
    /// Finite operator component on the REAL metadata owner. It is not a
    /// periodic controller or a generic proposal endpoint. Every remote fact is
    /// independently fetched here using this actual owner's pinned transport.
    pub(in crate::distribution_native_host) fn certify_finite_remote_catalogue(
        &mut self,
        intent: NativeFirstCatalogueCertificationIntent,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        token.check()?;
        intent.manifest.require_shape()?;
        intent.facts.require_manifest(&intent.manifest)?;
        if intent.decision_sha256.len() != 64
            || !intent
                .decision_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
        {
            return Err(invalid(
                "FIRST certification original complete native D absent",
            ));
        }
        let backend = self.backend.clone();
        let leader = backend
            .provider
            .current_metadata_leader_hint(&token)?
            .ok_or_else(|| invalid("FIRST certification metadata leader absent"))?;
        if leader != backend.node_id {
            return Err(invalid(
                "FIRST certification must run on actual metadata leader owner",
            ));
        }
        let key = (intent.manifest.relocation_id, intent.facts.phase_id);
        if !self.remote_catalogue_certifications.contains_key(&key) {
            if self.remote_catalogue_certifications.len() >= 8 {
                return Err(invalid("FIRST certification eight-attempt custody bound"));
            }
            self.remote_catalogue_certifications.insert(
                key,
                NativeFirstCatalogueCertificationAttempt {
                    manifest_sha256: intent.manifest.checksum_sha256.clone(),
                    facts_sha256: intent.facts.checksum_sha256.clone(),
                    decision_sha256: intent.decision_sha256.clone(),
                    candidate: None,
                    proofs: Vec::new(),
                    proof_bytes: 0,
                    expected_proposal: None,
                    proposal: None,
                    actual_proposal_return: None,
                    certified: None,
                    membership: None,
                    membership_history: Vec::new(),
                    stopped: false,
                    serving: None,
                },
            );
        }
        let original = self.remote_catalogue_certifications.get_mut(&key).unwrap();
        if original.manifest_sha256 != intent.manifest.checksum_sha256
            || original.facts_sha256 != intent.facts.checksum_sha256
            || original.decision_sha256 != intent.decision_sha256
        {
            return Err(invalid(
                "FIRST certification cannot replace original phase/MF/D custody",
            ));
        }
        let own_request =
            |token: &CancellationToken| -> Result<(HostMetadataReadObservation, Uuid)> {
                let business = Uuid::now_v7();
                let own = backend.transport.acquire_host_metadata_observation(
                    &backend.node_id,
                    business,
                    remaining_finite(token)?,
                    token,
                )?;
                Ok((own, business))
            };
        let (initial, business) = own_request(&token)?;
        if let Some(record) = initial
            .committed_topology(business)?
            .relocations
            .get(&intent.manifest.relocation_id)
            .and_then(|r| r.native_joint_handoff_v1.as_ref())
        {
            if record.decision_sha256() != intent.decision_sha256
                || record.session_id() != intent.manifest.handoff_session
                || record.nonce() != intent.manifest.nonce
            {
                return Err(invalid(
                    "FIRST certification current metadata has another native decision",
                ));
            }
            // Lost commit/reply resolution is allowed only for this owner's
            // retained actual proposal and proof, never from the record alone.
            if original.certified.is_none() && original.proposal.is_some() {
                let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
                    &backend.service,
                    &initial,
                    business,
                    &token,
                )?
                .ok_or_else(|| {
                    invalid("FIRST selected certification needs actual current MStore")
                })?;
                let store = backend.lock_metadata(&token)?;
                point.require_current_memory()?;
                original.certified =
                    Some(serving::NativeFirstCertifiedHistory::from_actual_commit(
                        &intent, original, &store, &initial, business,
                    )?);
            }
            // Fresh historical committed DATA only; never recreate owners or
            // use an expired aggregate for a new mutation.
            return Ok(
                serde_json::json!({"actual_committed_record":record,"metadata_term":initial.term(),"read_index":initial.read_index(),
                "historical_metadata_only":true,"Open_release_Serving":false}),
            );
        }
        if original.candidate.is_none() {
            let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
                &backend.service,
                &initial,
                business,
                &token,
            )?
            .ok_or_else(|| invalid("FIRST catalogue scope needs actual protected MStore point"))?;
            let open = backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("FIRST catalogue scope actual service busy"))?
                .listener_open_guard();
            let scope = {
                let store = backend.lock_metadata(&token)?;
                NativeMetadataOwnerCatalogueScope::from_actual_metadata_owner(
                    initial,
                    business,
                    point,
                    open,
                    &store,
                    &intent.manifest,
                    &token,
                )?
            };
            let mut round = NativeRemoteCatalogueRound::begin_for_metadata_owner(
                scope,
                intent.manifest.clone(),
                intent.facts.clone(),
                intent.decision_sha256.clone(),
                &token,
            )?;
            let mut nodes = intent
                .manifest
                .old_members
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            if !nodes.contains(&intent.manifest.target_id) {
                nodes.push(intent.manifest.target_id.clone());
            }
            for node in nodes {
                // Fresh original OWN for EACH real outbound exchange. No old
                // collection token is renewed; all share original outer 5s.
                let (own, request_business) = own_request(&token)?;
                let request = NativeRemoteNewRequestV1 {
                    version: 1,
                    manifest: intent.manifest.clone(),
                    phase_id: intent.facts.phase_id,
                    operation: NativeRemoteNewOperationV1::ObserveCatalogue {
                        facts: intent.facts.clone(),
                    },
                    business: Uuid::now_v7(),
                    challenge: Uuid::now_v7(),
                    budget_ms: remaining_finite(&token)?.as_millis().min(5000) as u64,
                };
                let actual = backend.transport.request_remote_native_catalogue(
                    &node,
                    &request,
                    &own,
                    request_business,
                    &token,
                )?;
                round.record(actual)?;
            }
            let proof = round.try_finish().map_err(|(e, _original)| e)?;
            if proof.actual_observer() != &backend.node_id {
                return Err(invalid(
                    "FIRST certification aggregate observer differs from actual metadata leader",
                ));
            }
            let (fresh, fresh_business) = own_request(&token)?;
            let mut view = crate::DistributionStore::native_registration_plan(
                self.config.clone(),
                fresh.committed_topology(fresh_business)?.clone(),
            )?;
            view.certify_native_remote_joint_handoff(
                &proof,
                &fresh,
                fresh_business,
                &backend.node_id,
                now_finite_ms(),
            )?;
            let candidate = view.topology().clone();
            crate::distribution_range_image::bounded_json(&candidate, 8 * 1024 * 1024)?;
            let bytes = proof.retained_encoded_bytes();
            let total = original
                .proof_bytes
                .checked_add(bytes)
                .ok_or_else(|| invalid("FIRST metadata proof byte overflow"))?;
            if original.proofs.len() >= 8 || total > 8 * 1024 * 1024 {
                return Err(invalid("FIRST metadata proof eight-context/8MiB bound"));
            }
            // Actual opaque observations retained BEFORE currentness/proposal/IO.
            original.proofs.push(proof);
            original.proof_bytes = total;
            let proof = original.proofs.last().unwrap();
            let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
                &backend.service,
                &fresh,
                fresh_business,
                &token,
            )?
            .ok_or_else(|| {
                invalid("FIRST certification needs actual current native MStore point")
            })?;
            let open = backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("FIRST certification actual service busy"))?
                .listener_open_guard();
            let mut store = backend.lock_metadata(&token)?;
            store.require_listener_database(&open)?;
            store.require_native_control_leader(&fresh, fresh_business)?;
            proof.require_metadata_owner_proposal(
                &fresh,
                fresh_business,
                &point,
                &open,
                &store,
                &token,
            )?;
            // Complete allocation and position calculation BEFORE the final
            // fresh request/point check. Actual candidate/position are then
            // retained without a fallible gap before the existing proposal.
            let status = store.status();
            let expected = (
                status.current_term,
                status
                    .last_log_index
                    .checked_add(1)
                    .ok_or_else(|| invalid("FIRST metadata proposal index overflow"))?,
            );
            let retained_candidate = candidate.clone();
            point.require_current_memory()?;
            proof.require_current()?;
            token.check()?;
            original.expected_proposal = Some(expected);
            original.candidate = Some(retained_candidate);
            let result = store.propose_topology(candidate);
            match result {
                Ok(entry) => {
                    // Retain the actual typed successful return BEFORE even
                    // allocating diagnostic JSON or any later postcheck.
                    original.proposal = Some(entry);
                    original.actual_proposal_return = Some(serde_json::json!({
                        "returned_ok":true,"entry":original.proposal,"error":null
                    }));
                }
                Err(error) => {
                    original.actual_proposal_return = Some(serde_json::json!({
                        "returned_ok":false,"entry":null,
                        "error":crate::distribution_native_host::remote_first::error_data(&error)
                    }));
                    return Err(error);
                }
            }
            let entry = original.proposal.as_ref().unwrap();
            if Some((entry.term, entry.index)) != original.expected_proposal {
                return Err(invalid("FIRST actual proposal changed retained term/index"));
            }
        } else {
            let entry=original.proposal.as_ref().ok_or_else(||invalid("FIRST original proposal outcome unknown; readonly committed observation required"))?;
            let store = backend.lock_metadata(&token)?;
            store.require_native_remote_catalogue_pending(entry, &initial, business)?;
        }
        // No service/DB/native/MStore guard spans actual network replication.
        backend.heartbeat(&token)?;
        #[cfg(test)]
        test_cuts::after_commit(
            &backend,
            &self.config,
            intent.manifest.capture_range.id,
            &token,
        )?;
        let (post, post_business) = own_request(&token)?;
        let current = post.committed_topology(post_business)?;
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &backend.service,
            &post,
            post_business,
            &token,
        )?
        .ok_or_else(|| invalid("FIRST committed certification needs actual current MStore"))?;
        let store = backend.lock_metadata(&token)?;
        point.require_current_memory()?;
        store.require_native_remote_selected_commit(
            original.proposal.as_ref().unwrap(),
            intent.manifest.relocation_id,
            &post,
            post_business,
        )?;
        let record = current.relocations[&intent.manifest.relocation_id]
            .native_joint_handoff_v1
            .as_ref()
            .ok_or_else(|| invalid("FIRST committed certification record absent"))?;
        // This constructor is private to the actual metadata owner. A decoded
        // stage or a historical-only reply above cannot create this custody.
        if original.certified.is_none() {
            original.certified = Some(serving::NativeFirstCertifiedHistory::from_actual_commit(
                &intent,
                original,
                &store,
                &post,
                post_business,
            )?);
        }
        drop(store);
        token.check()?;
        Ok(
            serde_json::json!({"actual_committed_record":record,"actual_original_proposal":original.proposal,
            "original_proposal_return":original.actual_proposal_return,"metadata_term":post.term(),"read_index":post.read_index(),
            "actual_metadata_owned_independent_callbacks":true,"Open_release_Serving":false}),
        )
    }
}
fn remaining_finite(token: &CancellationToken) -> Result<Duration> {
    token.check()?;
    token
        .deadline()
        .and_then(|d| d.checked_duration_since(Instant::now()))
        .filter(|d| !d.is_zero())
        .ok_or(BicDbError::QueryTimedOut)
}
fn now_finite_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod scope_tests;
