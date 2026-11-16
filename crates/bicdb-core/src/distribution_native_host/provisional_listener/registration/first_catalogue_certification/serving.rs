//! SAME metadata owner's historical certification into membership-only stages.
//! No public constructor, DTO-to-proof conversion, old TTL renewal or physical
//! preparation/Open/release permission is provided by this scope.
use super::*;
use crate::distribution::{NativeJointHandoffMetadataV1, NativeJointHandoffStageV1};

pub(super) struct NativeFirstCertifiedHistory {
    pub(super) manifest: NativeRemoteImageManifestV1,
    pub(super) record: NativeJointHandoffMetadataV1,
    metadata_node: ClusterNodeId,
    metadata_incarnation: u64,
    issued_certification_entry: crate::distribution_consensus::MetadataLogEntry,
}
pub(super) struct NativeFirstMembershipAttempt {
    candidate: ClusterTopology,
    expected: (u64, u64),
    entry: Option<crate::distribution_consensus::MetadataLogEntry>,
    returned_error_sha256: Option<String>,
    committed: bool,
}
impl NativeFirstCertifiedHistory {
    /// Correlation DATA from the actual selected original certification entry.
    /// Snapshot donor MF.source_id is independent of this retiring OLD voter.
    pub(super) fn retired_source(&self) -> Result<&ClusterNodeId> {
        let original = self
            .issued_certification_entry
            .topology
            .relocations
            .get(&self.manifest.relocation_id)
            .ok_or_else(|| invalid("remote activation original committed relocation absent"))?;
        let mut old_only = self
            .manifest
            .old_members
            .keys()
            .filter(|node| !self.manifest.new_members.contains_key(*node));
        let retired = old_only
            .next()
            .ok_or_else(|| invalid("remote activation has no original retiring OLD member"))?;
        if old_only.next().is_some()
            || original.source.as_ref() != Some(retired)
            || original.range_id != self.manifest.capture_range.id
            || original.target != self.manifest.target_id
        {
            return Err(invalid(
                "remote activation original retirement source is not exact OLD minus NEW",
            ));
        }
        Ok(retired)
    }
    pub(super) fn from_actual_commit(
        intent: &NativeFirstCatalogueCertificationIntent,
        original: &NativeFirstCatalogueCertificationAttempt,
        store: &crate::distribution_consensus::MetadataConsensusStore,
        fresh: &HostMetadataReadObservation,
        business: Uuid,
    ) -> Result<Self> {
        fresh.require_live_request(business)?;
        if original.stopped || original.proofs.is_empty() {
            return Err(invalid(
                "FIRST actual certification producer custody ended or absent",
            ));
        }
        let proof = original.proofs.last().unwrap();
        let candidate = original
            .candidate
            .as_ref()
            .ok_or_else(|| invalid("FIRST actual certification candidate absent"))?;
        let entry = original
            .proposal
            .as_ref()
            .ok_or_else(|| invalid("FIRST actual certification helper return absent"))?;
        let topology = fresh.committed_topology(business)?;
        let record = topology
            .relocations
            .get(&intent.manifest.relocation_id)
            .and_then(|p| p.native_joint_handoff_v1.as_ref())
            .ok_or_else(|| invalid("FIRST actual committed certification absent"))?;
        store.require_native_remote_selected_commit(
            entry,
            intent.manifest.relocation_id,
            fresh,
            business,
        )?;
        if &entry.topology != candidate
            || original.expected_proposal != Some((entry.term, entry.index))
            || fresh.term() < entry.term
            || fresh.read_index() < entry.index
            || record.stage() != NativeJointHandoffStageV1::Certified
            || record.decision_sha256() != proof.data().checksum_sha256()
            || record.decision_sha256() != intent.decision_sha256
            || record.session_id() != intent.manifest.handoff_session
            || record.nonce() != intent.manifest.nonce
            || proof.actual_observer() != fresh.requester_node_id()
            || fresh.metadata_leader_id() != fresh.requester_node_id()
            || original.manifest_sha256 != intent.manifest.checksum_sha256
            || original.facts_sha256 != intent.facts.checksum_sha256
        {
            return Err(invalid(
                "FIRST historical certification lacks SAME actual proposal/proof/current commit",
            ));
        }
        // Expired frame/request objects remain historical. Currentness here
        // comes from this distinct actual committed metadata observation.
        Ok(Self {
            manifest: intent.manifest.clone(),
            record: record.clone(),
            metadata_node: fresh.requester_node_id().clone(),
            metadata_incarnation: fresh.requester_incarnation(),
            issued_certification_entry: entry.clone(),
        })
    }
}

/// Borrowed solely inside the original mutable metadata owner. The stage enum
/// is intent DATA; these private fields bind the genuine historical producer.
pub(crate) struct NativeRemoteMembershipScope<'a> {
    pub(super) history: &'a NativeFirstCertifiedHistory,
    pub(super) fresh: &'a HostMetadataReadObservation,
    pub(super) business: Uuid,
    pub(super) token: &'a CancellationToken,
    pub(super) candidate: Option<&'a ClusterTopology>,
}
impl NativeRemoteMembershipScope<'_> {
    pub(crate) fn require_request(&self) -> Result<()> {
        self.token.check()?;
        self.fresh.require_live_request(self.business)
    }
    pub(crate) fn require_view(
        &self,
        topology: &ClusterTopology,
        actor: &ClusterNodeId,
    ) -> Result<()> {
        self.require_request()?;
        if self.fresh.committed_topology(self.business)? != topology
            || actor != &self.history.metadata_node
            || self.fresh.requester_node_id() != actor
            || self.fresh.metadata_leader_id() != actor
            || self.fresh.requester_incarnation() != self.history.metadata_incarnation
            || self.fresh.term() < self.history.issued_certification_entry.term
            || self.fresh.read_index() < self.history.issued_certification_entry.index
            || self.fresh.restore_activation(self.business)?
                != self.history.manifest.restore.as_ref()
        {
            return Err(invalid(
                "FIRST membership substituted actual metadata owner/current committed domain",
            ));
        }
        let mf = &self.history.manifest;
        if topology.cluster_id != mf.cluster_id
            || topology.range_image_bootstrap_id != Some(mf.bootstrap_id)
        {
            return Err(invalid("FIRST membership changed cluster/bootstrap"));
        }
        for (node, expected) in mf.old_members.iter().chain(&mf.new_members) {
            let actual = topology
                .nodes
                .get(node)
                .ok_or_else(|| invalid("FIRST membership original member absent"))?;
            if actual.incarnation != expected.incarnation
                || actual.lifecycle != ClusterNodeLifecycle::Active
                || actual.tls_certificate_sha256 != expected.tls_certificate_sha256
                || actual.pending_tls_certificate_sha256 != expected.pending_tls_certificate_sha256
                || actual
                    .labels
                    .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
                    != Some(&mf.schema_sha256)
            {
                return Err(invalid(
                    "FIRST membership changed original actual member identity",
                ));
            }
            let birth = actual
                .native_store_birth_v1
                .as_ref()
                .ok_or_else(|| invalid("FIRST membership original native birth absent"))?;
            birth.validate_for(Some(mf.bootstrap_id), actual.incarnation)?;
            if node == &mf.target_id && birth != &mf.target_birth {
                return Err(invalid(
                    "FIRST membership changed original target native birth",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn manifest(&self) -> &NativeRemoteImageManifestV1 {
        &self.history.manifest
    }
    pub(crate) fn certified(&self) -> &NativeJointHandoffMetadataV1 {
        &self.history.record
    }
    pub(crate) fn require_proposal(
        &self,
        previous: &ClusterTopology,
        candidate: &ClusterTopology,
        actor: &ClusterNodeId,
    ) -> Result<()> {
        self.require_view(previous, actor)?;
        if self.candidate != Some(candidate) {
            return Err(invalid(
                "FIRST membership proposal is not the exact actual owner-built candidate",
            ));
        }
        Ok(())
    }
}

impl NativeMetadataControlHost {
    /// Finite membership-only continuation. It never schedules a relocation or
    /// grants native preparation/Open/release. A new request retains its OWN
    /// original five-second window and cannot renew the certification request.
    pub(in crate::distribution_native_host) fn advance_finite_remote_membership(
        &mut self,
        relocation: crate::distribution::RelocationId,
        phase: Uuid,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        token.check()?;
        let backend = self.backend.clone();
        let original = self
            .remote_catalogue_certifications
            .get_mut(&(relocation, phase))
            .ok_or_else(|| invalid("FIRST original metadata certification owner absent"))?;
        if original.stopped {
            return Err(invalid(
                "FIRST original metadata certification owner stopped",
            ));
        }
        let history = original.certified.as_ref().ok_or_else(|| {
            invalid("FIRST actual certification history absent; cold reconstruction refused")
        })?;
        let business = Uuid::now_v7();
        let fresh = backend.transport.acquire_host_metadata_observation(
            &backend.node_id,
            business,
            remaining_finite(&token)?,
            &token,
        )?;
        let current = fresh.committed_topology(business)?;
        let fresh_point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &backend.service,
            &fresh,
            business,
            &token,
        )?
        .ok_or_else(|| invalid("FIRST selected membership needs actual current native MStore"))?;
        // A previous known proposal is observed, never blindly reproposed.
        if let Some(pending) = original.membership.as_mut() {
            if !pending.committed {
                let entry = pending.entry.as_ref().ok_or_else(|| {
                    invalid("FIRST membership proposal return unknown; no reconstructed success")
                })?;
                let selected = {
                    let store = backend.lock_metadata(&token)?;
                    fresh_point.require_current_memory()?;
                    store.require_native_remote_selected_commit(entry, relocation, &fresh, business)
                };
                if selected.is_ok() {
                    if (entry.term, entry.index) != pending.expected
                        || fresh.term() < entry.term
                        || fresh.read_index() < entry.index
                    {
                        return Err(invalid("FIRST membership committed observation regressed"));
                    }
                    pending.committed = true;
                    return Ok(
                        serde_json::json!({"actual_committed_record":current.relocations[&relocation].native_joint_handoff_v1,
                        "actual_proposal":entry,"historical_reply_resolution":true,"Serving":false}),
                    );
                }
                let entry = pending.entry.as_ref().ok_or_else(|| {
                    invalid("FIRST membership actual proposal outcome unknown; mutation refused")
                })?;
                backend
                    .lock_metadata(&token)?
                    .require_native_remote_catalogue_pending(entry, &fresh, business)?;
                backend.heartbeat(&token)?;
                return Err(invalid(
                    "FIRST original membership proposal still pending; observe with a new request",
                ));
            }
        }
        let scope = NativeRemoteMembershipScope {
            history,
            fresh: &fresh,
            business,
            token: &token,
            candidate: None,
        };
        scope.require_view(current, &backend.node_id)?;
        let record = current
            .relocations
            .get(&relocation)
            .and_then(|r| r.native_joint_handoff_v1.as_ref())
            .ok_or_else(|| invalid("FIRST current certified stage absent"))?;
        if record.stage() == NativeJointHandoffStageV1::AwaitingActivation {
            let pending = original
                .membership
                .as_ref()
                .filter(|p| p.committed)
                .ok_or_else(|| {
                    invalid("FIRST final membership lacks original actual transition custody")
                })?;
            let entry = pending
                .entry
                .as_ref()
                .ok_or_else(|| invalid("FIRST final membership original known return absent"))?;
            let store = backend.lock_metadata(&token)?;
            fresh_point.require_current_memory()?;
            store.require_native_remote_selected_commit(entry, relocation, &fresh, business)?;
            return Ok(
                serde_json::json!({"actual_committed_record":record,"membership_complete":true,"Serving":false}),
            );
        }
        let mut view = crate::DistributionStore::native_registration_plan(
            self.config.clone(),
            current.clone(),
        )?;
        view.advance_native_remote_membership_frozen(&scope, &backend.node_id, now_finite_ms())?;
        let candidate = view.topology().clone();
        crate::distribution_range_image::bounded_json(&candidate, 8 * 1024 * 1024)?;
        let retained = candidate.clone();
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &backend.service,
            &fresh,
            business,
            &token,
        )?
        .ok_or_else(|| invalid("FIRST membership needs actual current native MStore"))?;
        let open = backend
            .service
            .try_lock()
            .ok_or_else(|| invalid("FIRST membership actual service busy"))?
            .listener_open_guard();
        let mut store = backend.lock_metadata(&token)?;
        store.require_listener_database(&open)?;
        store.require_native_control_leader(&fresh, business)?;
        let status = store.status();
        let expected = (
            status.current_term,
            status
                .last_log_index
                .checked_add(1)
                .ok_or_else(|| invalid("FIRST membership index overflow"))?,
        );
        scope.require_view(current, &backend.node_id)?;
        point.require_current_memory()?;
        token.check()?;
        // The two legitimate metadata transitions retain their actual earlier
        // typed outcomes; starting the second never erases first-step evidence.
        if original.membership_history.len() >= 2 {
            return Err(invalid("FIRST membership two-transition history exhausted"));
        }
        if let Some(previous) = original.membership.take() {
            if !previous.committed {
                original.membership = Some(previous);
                return Err(invalid(
                    "FIRST unresolved membership outcome cannot be replaced",
                ));
            }
            original.membership_history.push(previous);
        }
        original.membership = Some(NativeFirstMembershipAttempt {
            candidate: retained,
            expected,
            entry: None,
            returned_error_sha256: None,
            committed: false,
        });
        let proposal_scope = NativeRemoteMembershipScope {
            candidate: Some(&original.membership.as_ref().unwrap().candidate),
            ..scope
        };
        let result = store.propose_native_remote_membership(candidate, &proposal_scope);
        let pending = original.membership.as_mut().unwrap();
        match result {
            Ok(entry) => pending.entry = Some(entry),
            Err(error) => {
                use sha2::Digest;
                pending.returned_error_sha256 = Some(hex::encode(sha2::Sha256::digest(
                    error.to_string().as_bytes(),
                )));
                return Err(error);
            }
        }
        drop(store);
        backend.heartbeat(&token)?;
        #[cfg(test)]
        super::test_cuts::after_commit(
            &backend,
            &self.config,
            history.manifest.capture_range.id,
            &token,
        )?;
        let post_business = Uuid::now_v7();
        let post = backend.transport.acquire_host_metadata_observation(
            &backend.node_id,
            post_business,
            remaining_finite(&token)?,
            &token,
        )?;
        let entry = pending.entry.as_ref().unwrap();
        let post_point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &backend.service,
            &post,
            post_business,
            &token,
        )?
        .ok_or_else(|| {
            invalid("FIRST selected committed membership needs actual current MStore")
        })?;
        {
            let store = backend.lock_metadata(&token)?;
            post_point.require_current_memory()?;
            store.require_native_remote_selected_commit(entry, relocation, &post, post_business)?;
        }
        if pending.expected != (entry.term, entry.index)
            || post.term() < entry.term
            || post.read_index() < entry.index
        {
            return Err(invalid(
                "FIRST actual membership proposal/commit changed position",
            ));
        }
        pending.committed = true;
        token.check()?;
        Ok(
            serde_json::json!({"actual_committed_record":post.committed_topology(post_business)?.relocations[&relocation].native_joint_handoff_v1,
            "actual_issued_proposal":entry,"selected_effect_committed":true,"original_entry_commit_not_inferred":true,"Serving":false}),
        )
    }
}

impl NativeMetadataControlHost {
    /// Actual metadata owner's finite independent native/catalogue read. This
    /// returns diagnostic DATA, never a local preparation/durability quorum.
    pub(in crate::distribution_native_host) fn observe_finite_remote_native_decisions(
        &mut self,
        relocation: crate::distribution::RelocationId,
        phase: Uuid,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        use crate::distribution_native_host::{
            NativeRemoteServingOperationV1, NativeRemoteServingRequestV1,
        };
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        token.check()?;
        let owner = self
            .remote_catalogue_certifications
            .get(&(relocation, phase))
            .filter(|o| !o.stopped)
            .ok_or_else(|| invalid("serving read actual metadata owner absent"))?;
        let history = owner
            .certified
            .as_ref()
            .ok_or_else(|| invalid("serving read actual certification custody absent"))?;
        let original_operation = Uuid::now_v7();
        let mut nodes = history
            .manifest
            .old_members
            .keys()
            .chain(history.manifest.new_members.keys())
            .cloned()
            .collect::<Vec<_>>();
        nodes.sort();
        nodes.dedup();
        let mut actual = Vec::new();
        for (ordinal, node) in nodes.iter().enumerate() {
            token.check()?;
            let business = Uuid::now_v7();
            let own = self.backend.transport.acquire_host_metadata_observation(
                &self.backend.node_id,
                business,
                remaining_finite(&token)?,
                &token,
            )?;
            let current = own.committed_topology(business)?;
            let scope = NativeRemoteMembershipScope {
                history,
                fresh: &own,
                business,
                token: &token,
                candidate: None,
            };
            scope.require_view(current, &self.backend.node_id)?;
            if current
                .relocations
                .get(&relocation)
                .and_then(|r| r.native_joint_handoff_v1.as_ref())
                .is_none_or(|r| r.stage() != NativeJointHandoffStageV1::AwaitingActivation)
            {
                return Err(invalid(
                    "serving protected observations require actual frozen final membership",
                ));
            }
            let request = NativeRemoteServingRequestV1 {
                version: 1,
                manifest: history.manifest.clone(),
                phase_id: phase,
                decision_sha256: history.record.decision_sha256().into(),
                original_operation,
                sequence: ordinal as u64 + 1,
                operation: NativeRemoteServingOperationV1::ObserveDecision,
                business,
                challenge: Uuid::now_v7(),
                budget_ms: remaining_finite(&token)?.as_millis().clamp(1, 5000) as u64,
            };
            let frame = self
                .backend
                .transport
                .request_remote_native_serving(node, &request, &own, business, &token)?;
            frame.require_live_binding(
                &history.manifest,
                phase,
                history.record.decision_sha256(),
            )?;
            actual.push(serde_json::json!({"node":frame.node_id(),"actual_native_point":frame.point(),"metadata_anchor":frame.metadata_anchor()}));
        }
        token.check()?;
        Ok(
            serde_json::json!({"actual_protected_recipient_reads":actual,"Serving":false,"local_quorum_constructed":false}),
        )
    }
}
