//! Remote durable catalogue correlation DATA. Only actual pinned exchanges
//! may become observations; decoding this value never recreates a local QQ.
use super::*;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteCatalogueDecisionV1 {
    pub native: NativeRemoteDecisionReceiptV1,
    pub range_index: u64,
    pub h0: String,
    pub h: String,
    pub old_configuration_sha256: String,
    pub new_configuration_sha256: String,
    pub source_snapshot_db_sequence: u64,
    pub target_local_snapshot_db_sequence: u64,
    pub original_participant_sha256: String,
    pub publication_catalogue_sha256: String,
    pub current_catalogue_sha256: String,
}
impl NativeRemoteCatalogueDecisionV1 {
    pub(crate) fn require_manifest_summary(
        &self,
        mf: &NativeRemoteImageManifestV1,
        facts: &NativeRemoteNewFactSetV1,
    ) -> Result<()> {
        self.native.binding.require_manifest(mf)?;
        if self.native.phase_id != facts.phase_id
            || self.native.fact_set_sha256 != facts.checksum_sha256
            || self.native.native_page_xid == 0
            || !is_digest(&self.native.decision_sha256)
            || !is_digest(&self.native.native_record_sha256)
            || !is_digest(&self.native.remote_provenance_sha256)
            || self.range_index != mf.range_index
            || self.h0 != mf.h0
            || self.h != mf.h
            || self.old_configuration_sha256 != mf.old_config_sha256
            || self.new_configuration_sha256 != mf.new_config_sha256
            || self.source_snapshot_db_sequence != mf.source_reserved_b
            || !is_digest(&self.original_participant_sha256)
            || !is_digest(&self.publication_catalogue_sha256)
            || !is_digest(&self.current_catalogue_sha256)
        {
            return Err(invalid(
                "compact remote catalogue changed original manifest/complete D/physical summary",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_request(
        &self,
        request: &NativeRemoteNewRequestV1,
        facts: &NativeRemoteNewFactSetV1,
    ) -> Result<()> {
        self.native.require_request(request, facts)?;
        let mf = &request.manifest;
        if self.range_index != mf.range_index
            || self.h0 != mf.h0
            || self.h != mf.h
            || self.old_configuration_sha256 != mf.old_config_sha256
            || self.new_configuration_sha256 != mf.new_config_sha256
            || self.source_snapshot_db_sequence != mf.source_reserved_b
            || !is_digest(&self.original_participant_sha256)
            || !is_digest(&self.publication_catalogue_sha256)
            || !is_digest(&self.current_catalogue_sha256)
        {
            return Err(invalid(
                "remote catalogue changed original complete native decision/physical scope",
            ));
        }
        Ok(())
    }
}

/// A DIFFERENT opaque remote proof, never NativeJointDurableDecision. Actual
/// recipient native+catalogue returns enter only through pinned exchanges.
pub(crate) struct NativeRemoteCatalogueRound {
    plan: std::sync::Arc<NativeJointHandoffPlan>,
    manifest: std::sync::Arc<NativeRemoteImageManifestV1>,
    facts: NativeRemoteNewFactSetV1,
    expected_decision_sha256: String,
    observer: Option<ClusterNodeId>,
    caller: CancellationToken,
    frames: BTreeMap<
        ClusterNodeId,
        crate::distribution_transport::AuthenticatedNativeRemoteCatalogueFrame,
    >,
    retained_bytes: usize,
    summary: Option<NativeRemoteDecisionSummary>,
}
pub(crate) struct NativeRemoteDualDurableDecision {
    actual: NativeRemoteCatalogueRound,
}
impl NativeRemoteCatalogueRound {
    pub(crate) fn begin(
        plan: std::sync::Arc<NativeJointHandoffPlan>,
        manifest: NativeRemoteImageManifestV1,
        facts: NativeRemoteNewFactSetV1,
        expected_decision_sha256: String,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        plan.require_live()?;
        require_manifest_plan(&manifest, &plan)?;
        facts.require_manifest(&manifest)?;
        if !is_digest(&expected_decision_sha256) {
            return Err(invalid(
                "catalogue round expected original native body absent",
            ));
        }
        Ok(Self {
            plan,
            retained_bytes: crate::distribution_range_image::bounded_json(
                &manifest,
                MAX_NEW_REQUEST_BYTES,
            )?
            .len(),
            manifest: std::sync::Arc::new(manifest),
            facts,
            expected_decision_sha256,
            observer: None,
            caller: parent.child(),
            frames: BTreeMap::new(),
            summary: None,
        })
    }
    pub(crate) fn record(
        &mut self,
        frame: crate::distribution_transport::AuthenticatedNativeRemoteCatalogueFrame,
    ) -> Result<()> {
        self.caller.check()?;
        self.plan.require_live()?;
        frame.require_request_live()?;
        frame.require_manifest(&self.manifest, &self.facts)?;
        let native = &frame.catalogue().native;
        if native.decision_sha256 != self.expected_decision_sha256
            || (!self.manifest.old_members.contains_key(&native.node_id)
                && !self.manifest.new_members.contains_key(&native.node_id))
            || self.frames.contains_key(&native.node_id)
            || self
                .observer
                .as_ref()
                .is_some_and(|n| n != frame.actual_observer())
            || self
                .summary
                .as_ref()
                .is_some_and(|prior| !prior.matches(frame.catalogue()))
        {
            return Err(invalid("dual-durable collection changed original body/phase/member/observer or duplicated recipient"));
        }
        let bytes = frame.retained_encoded_bytes()?;
        let total = self
            .retained_bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid("dual-durable retained bytes overflow"))?;
        if self.frames.len() >= 128 || total > 8 * 1024 * 1024 {
            return Err(invalid(
                "dual-durable collection existing member/8MiB bound",
            ));
        }
        if self.summary.is_none() {
            self.summary = Some(NativeRemoteDecisionSummary::from_actual_frame(
                self.plan.clone(),
                self.manifest.clone(),
                frame.catalogue(),
            ));
        }
        self.observer = Some(frame.actual_observer().clone());
        self.retained_bytes = total;
        self.frames.insert(native.node_id.clone(), frame);
        Ok(())
    }
    pub(crate) fn try_finish(
        self,
    ) -> std::result::Result<NativeRemoteDualDurableDecision, (BicDbError, Self)> {
        if let Err(e) = self.require_current() {
            return Err((e, self));
        }
        Ok(NativeRemoteDualDurableDecision { actual: self })
    }
    fn require_current(&self) -> Result<()> {
        self.caller.check()?;
        self.plan.require_live()?;
        require_manifest_plan(&self.manifest, &self.plan)?;
        self.facts.require_manifest(&self.manifest)?;
        let old = self
            .frames
            .keys()
            .filter(|n| self.manifest.old_members.contains_key(*n))
            .count();
        let new = self
            .frames
            .keys()
            .filter(|n| self.manifest.new_members.contains_key(*n))
            .count();
        if old < self.manifest.old_members.len() / 2 + 1
            || new < self.manifest.new_members.len() / 2 + 1
            || !self
                .frames
                .contains_key(&self.manifest.activation_range.leader)
        {
            return Err(invalid("actual catalogue durability requires OLD and NEW majorities including activation leader"));
        }
        for f in self.frames.values() {
            f.require_request_live()?;
        }
        Ok(())
    }
}
impl NativeRemoteDualDurableDecision {
    pub(crate) fn require_current(&self) -> Result<()> {
        self.actual.require_current()
    }
    pub(crate) fn retained_encoded_bytes(&self) -> usize {
        self.actual.retained_bytes
    }
    pub(crate) fn data(&self) -> &NativeRemoteDecisionSummary {
        self.actual
            .summary
            .as_ref()
            .expect("private actual majority producer")
    }
    pub(crate) fn actual_observer(&self) -> &ClusterNodeId {
        self.actual.observer.as_ref().expect("actual frames")
    }
    #[cfg(test)]
    pub(crate) fn test_data(&self) -> serde_json::Value {
        serde_json::json!({"decision_sha256":self.data().checksum_sha256(),"actual_observer":self.actual_observer(),
            "actual_dual_durable_members":self.actual.frames.keys().collect::<Vec<_>>(),
            "same_original_phase":self.actual.facts.phase_id,"retained_bytes":self.actual.retained_bytes,
            "localQQ_Open_release_Serving":false})
    }
}

/// Scope and physical summary from actual native+catalogue acknowledgements.
/// NOT local NativeJointDecisionData; private constructor needs a real frame.
pub(crate) struct NativeRemoteDecisionSummary {
    plan: std::sync::Arc<NativeJointHandoffPlan>,
    manifest: std::sync::Arc<NativeRemoteImageManifestV1>,
    decision_sha256: String,
    source_b: u64,
    target_b: u64,
}
impl NativeRemoteDecisionSummary {
    fn from_actual_frame(
        plan: std::sync::Arc<NativeJointHandoffPlan>,
        manifest: std::sync::Arc<NativeRemoteImageManifestV1>,
        actual: &NativeRemoteCatalogueDecisionV1,
    ) -> Self {
        Self {
            plan,
            manifest,
            decision_sha256: actual.native.decision_sha256.clone(),
            source_b: actual.source_snapshot_db_sequence,
            target_b: actual.target_local_snapshot_db_sequence,
        }
    }
    fn matches(&self, actual: &NativeRemoteCatalogueDecisionV1) -> bool {
        self.decision_sha256 == actual.native.decision_sha256
            && self.source_b == actual.source_snapshot_db_sequence
            && self.target_b == actual.target_local_snapshot_db_sequence
    }
    pub(crate) fn checksum_sha256(&self) -> &str {
        &self.decision_sha256
    }
    pub(crate) fn promise(&self) -> &crate::distribution_range_handoff::RangeHandoffPromise {
        self.plan.promise()
    }
    pub(crate) fn old_range(&self) -> &crate::RangeDescriptor {
        self.plan.old_range()
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.manifest.range_index
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.manifest.h0
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.manifest.h
    }
    pub(crate) fn source_snapshot_db_sequence(&self) -> Result<u64> {
        Ok(self.source_b)
    }
    pub(crate) fn target_local_snapshot_db_sequence(&self) -> Result<u64> {
        Ok(self.target_b)
    }
    pub(crate) fn require_metadata_domain(
        &self,
        topology: &crate::ClusterTopology,
        restore: Option<&crate::distribution_consensus::MetadataRestoreActivation>,
    ) -> Result<()> {
        let mf = &self.manifest;
        topology.validate()?;
        if topology.cluster_id != mf.cluster_id
            || topology.range_image_bootstrap_id != Some(mf.bootstrap_id)
            || restore != mf.restore.as_ref()
            || topology
                .range_by_id(mf.capture_range.id)
                .is_none_or(|r| canonical_range(r) != canonical_range(&mf.capture_range))
        {
            return Err(invalid(
                "remote durable summary changed current cluster/restore/capture descriptor",
            ));
        }
        let relocation = topology
            .relocations
            .get(&mf.relocation_id)
            .ok_or_else(|| invalid("remote durable relocation absent"))?;
        if relocation.plan_id != mf.plan_id
            || relocation.target != mf.target_id
            || relocation.range_id != mf.capture_range.id
            || relocation.learner_epoch != mf.capture_range.epoch
            || relocation
                .handoff_session
                .as_ref()
                .is_none_or(|s| s.session_id != mf.handoff_session || s.nonce != mf.nonce)
        {
            return Err(invalid(
                "remote durable summary changed exact original selected plan",
            ));
        }
        let retired = mf
            .old_members
            .keys()
            .filter(|id| !mf.new_members.contains_key(*id))
            .collect::<Vec<_>>();
        if retired.len() != 1
            || relocation.source.as_ref() != retired.first().copied()
            || !matches!(
                relocation.phase,
                crate::distribution::RelocationPhase::LearnerAllocated
                    | crate::distribution::RelocationPhase::SnapshotCopying
                    | crate::distribution::RelocationPhase::CatchingUp
                    | crate::distribution::RelocationPhase::ReadyToPromote
            )
        {
            return Err(invalid(
                "remote durable current phase/retiring member changed",
            ));
        }
        for (id, expected) in mf.old_members.iter().chain(&mf.new_members) {
            let actual = topology
                .nodes
                .get(id)
                .ok_or_else(|| invalid("remote durable member absent"))?;
            if actual.lifecycle != crate::distribution::ClusterNodeLifecycle::Active
                || actual.incarnation != expected.incarnation
                || actual.tls_certificate_sha256 != expected.tls_certificate_sha256
                || actual.pending_tls_certificate_sha256 != expected.pending_tls_certificate_sha256
                || actual
                    .labels
                    .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
                    != Some(&mf.schema_sha256)
            {
                return Err(invalid(
                    "remote durable current membership/credential/schema differs",
                ));
            }
            let birth = actual
                .native_store_birth_v1
                .as_ref()
                .ok_or_else(|| invalid("remote durable actual enrolled birth absent"))?;
            birth.validate_for(Some(mf.bootstrap_id), actual.incarnation)?;
            if id == &mf.target_id {
                if birth != &mf.target_birth {
                    return Err(invalid("remote durable target native birth changed"));
                }
            } else if let Some(old) = mf.actual_old_closures.get(id) {
                if birth.birth_id() != old.birth_id || birth.birth_sha256() != old.birth_sha256 {
                    return Err(invalid("remote durable original OLD birth changed"));
                }
            } else {
                return Err(invalid(
                    "remote durable source membership lacks actual OLD birth",
                ));
            }
        }
        Ok(())
    }
}
