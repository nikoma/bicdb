//! Metadata owner's own authenticated ALL-released collection. No local D,
//! local QQ, writer or Serving grant is reconstructed from remote DATA.
use super::*;
use crate::distribution::{NativeJointHandoffMetadataV1, NativeJointHandoffStageV1};
use crate::distribution_native_host::{
    NativeRemoteServingOperationV1, NativeRemoteServingRequestV1,
};
use crate::distribution_range_handoff::{
    NativeRemoteActivatedMetadataProof, NativeRemoteAllReleasedRound,
};

pub(super) struct NativeFirstActivationAttempt {
    proof: NativeRemoteActivatedMetadataProof,
    candidate: ClusterTopology,
    expected: (u64, u64),
    entry: Option<crate::distribution_consensus::MetadataLogEntry>,
    first_error: Option<serde_json::Value>,
    committed: bool,
}

/// Constructed only inside the original actual metadata-owner operation.
pub(crate) struct NativeRemoteActivationScope<'a> {
    history: &'a serving::NativeFirstCertifiedHistory,
    proof: &'a NativeRemoteActivatedMetadataProof,
    token: &'a CancellationToken,
    candidate: Option<&'a ClusterTopology>,
}
impl NativeRemoteActivationScope<'_> {
    pub(crate) fn require_request(&self) -> Result<()> {
        self.token.check()?;
        self.proof.require_memory()
    }
    pub(crate) fn require_view(
        &self,
        current: &ClusterTopology,
        actor: &ClusterNodeId,
    ) -> Result<()> {
        self.require_request()?;
        let (fresh, business) = self.proof.metadata();
        let inherited = serving::NativeRemoteMembershipScope {
            history: self.history,
            fresh,
            business,
            token: self.token,
            candidate: None,
        };
        inherited.require_view(current, actor)?;
        if self.proof.manifest().checksum_sha256 != self.history.manifest.checksum_sha256
            || self.proof.manifest().relocation_id != self.history.manifest.relocation_id
            || self.proof.manifest().handoff_session != self.history.manifest.handoff_session
            || self.proof.manifest().nonce != self.history.manifest.nonce
            || self.proof.decision_sha256() != self.history.record.decision_sha256()
        {
            return Err(invalid(
                "remote activation substituted original certified history/MF/D",
            ));
        }
        Ok(())
    }
    pub(crate) fn manifest(&self) -> &NativeRemoteImageManifestV1 {
        self.proof.manifest()
    }
    pub(crate) fn certified(&self) -> &NativeJointHandoffMetadataV1 {
        &self.history.record
    }
    pub(crate) fn activation_sha256(&self) -> &str {
        self.proof.checksum_sha256()
    }
    pub(crate) fn retired_source(&self) -> Result<&ClusterNodeId> {
        self.history.retired_source()
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
                "remote activation substituted actual owner-built candidate",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_control_leader(
        &self,
        actual: &crate::MetadataConsensusStore,
    ) -> Result<()> {
        self.require_request()?;
        self.proof.require_control_leader(actual)
    }
}

impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn record_finite_remote_activation(
        &mut self,
        relocation: crate::distribution::RelocationId,
        phase: Uuid,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let actual = self.record_finite_remote_activation_inner(relocation, phase, parent);
        if let Err(error) = &actual {
            if let Some(original) = self
                .remote_catalogue_certifications
                .get_mut(&(relocation, phase))
            {
                if original.activation_first_error.is_none() {
                    original.activation_first_error = Some(
                        crate::distribution_native_host::remote_first::error_data(error),
                    );
                }
                if let Some(attempt) = original.activation.as_mut() {
                    if attempt.first_error.is_none() {
                        attempt.first_error = Some(
                            crate::distribution_native_host::remote_first::error_data(error),
                        );
                    }
                }
            }
        }
        actual
    }
    fn record_finite_remote_activation_inner(
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
            .filter(|o| !o.stopped)
            .ok_or_else(|| invalid("remote activation original metadata owner absent or ended"))?;
        let history = original
            .certified
            .as_ref()
            .ok_or_else(|| invalid("remote activation original certified history absent"))?;
        // A real issued proposal is observed, never replayed. An unknown
        // proposal helper return (entry=None) stays unresolved and fail-closed.
        if let Some(pending) = original.activation.as_mut() {
            let entry = pending.entry.as_ref().ok_or_else(|| {
                invalid("remote activation original proposal return unknown; no replay")
            })?;
            let business = Uuid::now_v7();
            let own = backend.transport.acquire_host_metadata_observation(
                &backend.node_id,
                business,
                remaining_finite(&token)?,
                &token,
            )?;
            let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
                &backend.service,
                &own,
                business,
                &token,
            )?
            .ok_or_else(|| {
                invalid("remote activation commit observation requires actual MStore")
            })?;
            let committed = {
                let store = backend.lock_metadata(&token)?;
                point.require_current_memory()?;
                store.require_native_remote_selected_commit(entry, relocation, &own, business)
            };
            if committed.is_err() {
                backend
                    .lock_metadata(&token)?
                    .require_native_remote_catalogue_pending(entry, &own, business)?;
                backend.heartbeat(&token)?;
                return Err(invalid(
                    "remote activation original issued proposal still pending; observe separately",
                ));
            }
            let record = own
                .committed_topology(business)?
                .relocations
                .get(&relocation)
                .and_then(|p| p.native_joint_handoff_v1.as_ref())
                .ok_or_else(|| invalid("remote activation committed original record absent"))?;
            if pending.expected != (entry.term, entry.index)
                || record.stage() < NativeJointHandoffStageV1::AwaitingCleanup
                || record.activated_quorum_sha256() != Some(pending.proof.checksum_sha256())
            {
                return Err(invalid(
                    "remote activation actual committed record/entry differs",
                ));
            }
            pending.committed = true;
            token.check()?;
            return Ok(
                serde_json::json!({"actual_committed_record":record,"actual_issued_entry":entry,
                "actual_activation_sha256":pending.proof.checksum_sha256(),"original_error":pending.first_error,
                "first_phase_API_error":original.activation_first_error,
                "known_selected_commit":true,"metadata_only_no_native_or_Serving_grant":true}),
            );
        }
        let manifest = Arc::new(history.manifest.clone());
        let decision = history.record.decision_sha256().to_owned();
        let mut round =
            NativeRemoteAllReleasedRound::begin(manifest.clone(), phase, decision.clone(), &token)?;
        let operation = Uuid::now_v7();
        for (ordinal, node) in manifest.new_members.keys().enumerate() {
            token.check()?;
            let business = Uuid::now_v7();
            let own = backend.transport.acquire_host_metadata_observation(
                &backend.node_id,
                business,
                remaining_finite(&token)?,
                &token,
            )?;
            let comparison = serving::NativeRemoteMembershipScope {
                history,
                fresh: &own,
                business,
                token: &token,
                candidate: None,
            };
            comparison.require_view(own.committed_topology(business)?, &backend.node_id)?;
            let request = NativeRemoteServingRequestV1 {
                version: 1,
                manifest: manifest.as_ref().clone(),
                phase_id: phase,
                decision_sha256: decision.clone(),
                original_operation: operation,
                sequence: u64::try_from(ordinal)
                    .map_err(|_| invalid("remote activation observation sequence overflow"))?
                    + 1,
                operation: NativeRemoteServingOperationV1::ObserveReleasedForPolicy,
                business,
                challenge: Uuid::now_v7(),
                budget_ms: remaining_finite(&token)?.as_millis().clamp(1, 5000) as u64,
            };
            round.record(
                backend
                    .transport
                    .request_remote_native_serving(node, &request, &own, business, &token)?,
            )?;
        }
        let all = round.try_finish().map_err(|(error, _)| error)?;
        let business = Uuid::now_v7();
        let own = backend.transport.acquire_host_metadata_observation(
            &backend.node_id,
            business,
            remaining_finite(&token)?,
            &token,
        )?;
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &backend.service,
            &own,
            business,
            &token,
        )?
        .ok_or_else(|| invalid("remote activation requires actual post-ALL current MStore"))?;
        let proof = all.certify_metadata_activation(own, business, point, &token)?;
        let candidate = {
            let (fresh, business) = proof.metadata();
            let mut view = crate::DistributionStore::native_registration_plan(
                self.config.clone(),
                fresh.committed_topology(business)?.clone(),
            )?;
            let scope = NativeRemoteActivationScope {
                history,
                proof: &proof,
                token: &token,
                candidate: None,
            };
            view.record_native_remote_joint_activation(&scope, &backend.node_id, now_finite_ms())?;
            view.topology().clone()
        };
        crate::distribution_range_image::bounded_json(&candidate, 8 * 1024 * 1024)?;
        let open = backend
            .service
            .try_lock()
            .ok_or_else(|| invalid("remote activation actual service busy"))?
            .listener_open_guard();
        let mut store = backend.lock_metadata(&token)?;
        store.require_listener_database(&open)?;
        proof.require_control_leader(&store)?;
        let expected = (
            store.status().current_term,
            store
                .status()
                .last_log_index
                .checked_add(1)
                .ok_or_else(|| invalid("remote activation proposal index overflow"))?,
        );
        // Retain actual claimed proof/candidate BEFORE any proposal effect.
        original.activation = Some(NativeFirstActivationAttempt {
            proof,
            candidate: candidate.clone(),
            expected,
            entry: None,
            first_error: None,
            committed: false,
        });
        let pending = original.activation.as_mut().unwrap();
        let scope = NativeRemoteActivationScope {
            history,
            proof: &pending.proof,
            token: &token,
            candidate: Some(&pending.candidate),
        };
        let issued = store.propose_native_remote_activation(candidate, &scope);
        match issued {
            Ok(entry) => pending.entry = Some(entry),
            Err(error) => return Err(error),
        }
        drop(store);
        backend.heartbeat(&token)?;
        // A separate observation proves commit; a known append/helper return
        // alone is never reported as a selected committed activation.
        self.record_finite_remote_activation_inner(relocation, phase, &token)
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn test_remote_activation_state(
        &self,
        relocation: crate::distribution::RelocationId,
        phase: Uuid,
    ) -> Result<serde_json::Value> {
        let original = self
            .remote_catalogue_certifications
            .get(&(relocation, phase))
            .ok_or_else(|| invalid("actual activation history absent"))?;
        Ok(
            serde_json::json!({"first_original_API_error":original.activation_first_error,
            "actual_snapshot_donor":original.certified.as_ref().map(|h|&h.manifest.source_id),
            "actual_retiring_OLD_source":original.certified.as_ref().map(|h|h.retired_source()).transpose()?,
            "actual_attempt_present":original.activation.is_some(),
            "actual_issued_entry":original.activation.as_ref().and_then(|a|a.entry.as_ref()),
            "actual_commit_observed":original.activation.as_ref().is_some_and(|a|a.committed),
            "DATA_only_not_authority":true}),
        )
    }
}
