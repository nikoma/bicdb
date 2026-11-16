//! Explicit finite original-owner memory retirement. This is separate from
//! readonly RPC dispatch and from native Frozen release/Serving authority.
use super::*;
use crate::distribution_native_host::NativeRemoteServingRequestV1;
use crate::distribution_range_handoff::NativeRemoteAllOpenRound;

pub(in super::super) struct NativeRemoteReferenceRetirement {
    participant: Arc<NativeRemoteNewParticipant>,
    manifest: Arc<crate::distribution_range_image::NativeRemoteImageManifestV1>,
    decision: String,
    source_retired: bool,
    participant_retired: bool,
    // Bounded outcome attribution only. No error or progress scalar is a grant.
    returned: u64,
    first_error_kind: Option<String>,
}

impl NativeMetadataControlHost {
    fn actual_remote_retirement_participant(
        &self,
        range: crate::RangeId,
        phase: Uuid,
    ) -> Result<Arc<NativeRemoteNewParticipant>> {
        let mut found = self
            .remote_serving
            .get(&(range, phase))
            .map(|s| s.participant.clone());
        let mut originals = Vec::new();
        if let Some(data) = &self.data {
            originals.extend(data.retained_remote_serving_participants()?);
        }
        if let Some(target) = &self.replica {
            originals.extend(target.retained_remote_serving_participants()?);
        }
        for actual in originals {
            let snapshot = actual.snapshot();
            if snapshot.binding.range_id != range || snapshot.phase_id != phase {
                continue;
            }
            if let Some(previous) = &found {
                if !Arc::ptr_eq(previous, &actual) {
                    return Err(invalid(
                        "retirement changed actual original host participant",
                    ));
                }
            } else {
                found = Some(actual);
            }
        }
        let actual = found.ok_or_else(|| invalid("retirement original host participant absent"))?;
        actual.require_lifetime_memory()?;
        if &actual.snapshot().node_id
            != self
                .backend
                .transport
                .local_metadata_read_participant()
                .node_id()
        {
            return Err(invalid("retirement changed actual bound local actor"));
        }
        Ok(actual)
    }

    /// One production actor turn on this actual retained host. It works for
    /// an original OLD donor outside NEW without creating preparation/Open or
    /// a NativeRemoteLocalServing. Caller must supply a fresh bounded request;
    /// no old RPC/Role deadline is extended. Progress survives every error.
    pub(in crate::distribution_native_host) fn advance_remote_all_open_reference_retirement(
        &mut self,
        range: crate::RangeId,
        phase: Uuid,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        let left = remaining(parent)?;
        if parent.deadline().is_none() || left > Duration::from_secs(5) {
            return Err(invalid(
                "reference retirement needs an actual at-most-five-second actor request",
            ));
        }
        let participant = self.actual_remote_retirement_participant(range, phase)?;
        let key = (range, phase);
        if let Some(previous) = self.remote_reference_retirement.get(&key) {
            if !Arc::ptr_eq(&previous.participant, &participant) {
                return Err(invalid(
                    "reference retirement refuses replacement original owner",
                ));
            }
        } else {
            if self.remote_reference_retirement.len() >= 8 {
                return Err(invalid(
                    "reference retirement preserves eight-original-phase custody bound",
                ));
            }
            let (manifest, original_phase, decision) = participant.retained_serving_intent()?;
            if original_phase != phase || manifest.capture_range.id != range {
                return Err(invalid("reference retirement changed retained phase/range"));
            }
            parent.check()?;
            participant.require_lifetime_memory()?;
            // This is a real one-way memory effect, retained BEFORE any drain.
            // A later refusal never removes/rebinds this original state.
            self.remote_reference_retirement.insert(
                key,
                NativeRemoteReferenceRetirement {
                    participant: participant.clone(),
                    manifest,
                    decision,
                    source_retired: false,
                    participant_retired: false,
                    returned: 0,
                    first_error_kind: None,
                },
            );
        }
        let result = self.advance_remote_reference_retirement_retained(key, parent);
        let retained = self.remote_reference_retirement.get_mut(&key).unwrap();
        retained.returned = retained.returned.saturating_add(1);
        if let Err(error) = &result {
            retained
                .first_error_kind
                .get_or_insert_with(|| format!("{:?}", std::mem::discriminant(error)));
        }
        result
    }

    fn advance_remote_reference_retirement_retained(
        &mut self,
        key: (crate::RangeId, Uuid),
        parent: &CancellationToken,
    ) -> Result<()> {
        let retained = &self.remote_reference_retirement[&key];
        retained.participant.require_lifetime_memory()?;
        if retained.source_retired && retained.participant_retired {
            // Historical completion DATA only; release must independently
            // acquire its actual current all-member phase and local authority.
            return parent.check();
        }
        let participant = retained.participant.clone();
        let manifest = retained.manifest.clone();
        let decision = retained.decision.clone();
        let backend = self.backend.clone();
        let leader = backend
            .provider
            .current_metadata_leader_hint(parent)?
            .ok_or_else(|| invalid("reference retirement metadata leader absent"))?;
        let mut round =
            NativeRemoteAllOpenRound::begin(manifest.clone(), key.1, decision.clone(), parent)?;
        let operation = Uuid::now_v7();
        for (index, node) in manifest.new_members.keys().enumerate() {
            parent.check()?;
            participant.require_lifetime_memory()?;
            let business = Uuid::now_v7();
            let own = backend.transport.acquire_host_metadata_observation(
                &leader,
                business,
                remaining(parent)?,
                parent,
            )?;
            let request = NativeRemoteServingRequestV1 {
                version: 1,
                manifest: manifest.as_ref().clone(),
                phase_id: key.1,
                decision_sha256: decision.clone(),
                original_operation: operation,
                sequence: u64::try_from(index)
                    .map_err(|_| invalid("reference retirement sequence overflow"))?
                    + 1,
                operation: NativeRemoteServingOperationV1::ObserveOpen,
                business,
                challenge: Uuid::now_v7(),
                budget_ms: remaining(parent)?.as_millis().clamp(1, 5000) as u64,
            };
            round.record(
                backend
                    .transport
                    .request_remote_native_serving(node, &request, &own, business, parent)?,
            )?;
        }
        let all = round.try_finish().map_err(|(error, _)| error)?;
        let business = Uuid::now_v7();
        let own = backend.transport.acquire_host_metadata_observation(
            &leader,
            business,
            remaining(parent)?,
            parent,
        )?;
        let metadata = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &backend.service,
            &own,
            business,
            parent,
        )?
        .ok_or_else(|| invalid("reference retirement actual native MStore absent"))?;
        let admission = all.admit_retirement(&own, business, &metadata, parent)?;
        participant.require_lifetime_memory()?;
        if manifest
            .new_members
            .contains_key(&participant.snapshot().node_id)
        {
            // Every final member has its actual successor/read route before
            // original slots can drain. Retired OLD never creates NEW custody.
            self.ensure_remote_release_successor(&participant, &admission, parent, || {
                parent.check()
            })?;
        }
        if !self.remote_reference_retirement[&key].source_retired {
            participant.retire_original_source_after_all_open(&admission)?;
            // Actual helper return is retained before any outer check.
            self.remote_reference_retirement
                .get_mut(&key)
                .unwrap()
                .source_retired = true;
        }
        if !self.remote_reference_retirement[&key].participant_retired {
            participant.retire_original_after_all_open(&admission)?;
            self.remote_reference_retirement
                .get_mut(&key)
                .unwrap()
                .participant_retired = true;
        }
        admission.require_memory_current()?;
        participant.require_lifetime_memory()?;
        parent.check()
    }
}
