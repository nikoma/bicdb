//! Actual pinned ALL-final-NEW preparation observations. This opaque remote
//! collection is distinct from a local quorum and cannot be decoded from DATA.
use super::*;
use crate::distribution_native_host::NativeRemoteServingOperationV1;
use crate::distribution_transport::AuthenticatedNativeRemoteServingFrame;
use std::sync::Arc;

pub(crate) struct NativeRemotePreparedRound {
    manifest: Arc<NativeRemoteImageManifestV1>,
    phase: Uuid,
    decision: String,
    observer: Option<ClusterNodeId>,
    frames: BTreeMap<ClusterNodeId, AuthenticatedNativeRemoteServingFrame>,
    bytes: usize,
    request: CancellationToken,
}
pub(crate) struct NativeRemotePreparedQuorum {
    actual: NativeRemotePreparedRound,
}
impl NativeRemotePreparedRound {
    pub(crate) fn begin(
        manifest: Arc<NativeRemoteImageManifestV1>,
        phase: Uuid,
        decision: String,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        manifest.require_shape()?;
        if phase.is_nil() || !is_digest(&decision) || parent.deadline().is_none() {
            return Err(invalid(
                "remote preparation collection requires original bounded phase/D",
            ));
        }
        let bytes = crate::distribution_range_image::bounded_json(
            manifest.as_ref(),
            MAX_NEW_REQUEST_BYTES,
        )?
        .len();
        Ok(Self {
            manifest,
            phase,
            decision,
            observer: None,
            frames: BTreeMap::new(),
            bytes,
            request: parent.child(),
        })
    }
    pub(crate) fn record(&mut self, frame: AuthenticatedNativeRemoteServingFrame) -> Result<()> {
        self.request.check()?;
        frame.require_live_binding(&self.manifest, self.phase, &self.decision)?;
        let point = frame.point();
        let vote = point.preparation.as_ref().ok_or_else(|| {
            invalid("actual remote preparation frame lacks protected native preparation")
        })?;
        if frame.operation() != NativeRemoteServingOperationV1::ObservePrepared
            || !self.manifest.new_members.contains_key(frame.node_id())
            || self.frames.contains_key(frame.node_id())
            || self
                .observer
                .as_ref()
                .is_some_and(|n| n != frame.actual_observer())
            || point.phase_id != self.phase
            || point.decision_sha256 != self.decision
            || !is_digest(&point.original_participant_sha256)
            || !is_digest(&point.native_decision_record_sha256)
            || !is_digest(&point.remote_provenance_sha256)
            || !is_digest(&point.catalogue_decision_sha256)
            || !is_digest(&point.current_catalogue_sha256)
            || point.native_decision_xid == 0
            || point.store_open.is_nil()
            || point.original_frozen_owner.is_nil()
            || point.native_database_open.is_empty()
            || vote.native_database_incarnation != point.native_database_open
            || !is_digest(&vote.preparation_sha256)
            || vote.metadata_context_id.is_nil()
            || vote.metadata_term == 0
            || vote.metadata_read_index == 0
            || vote.metadata_term > frame.metadata_anchor().0
            || vote.metadata_read_index > frame.metadata_anchor().1
        {
            return Err(invalid(
                "remote preparation changed exact current native member/observer/phase",
            ));
        }
        let bytes = self
            .bytes
            .checked_add(frame.retained_encoded_bytes()?)
            .ok_or_else(|| invalid("remote preparation byte count overflow"))?;
        if self.frames.len() >= MAX_NEW_MEMBERS || bytes > 8 * 1024 * 1024 {
            return Err(invalid(
                "remote preparation preserves existing 128-member/8MiB observation bound",
            ));
        }
        self.observer = Some(frame.actual_observer().clone());
        self.bytes = bytes;
        self.frames.insert(frame.node_id().clone(), frame);
        Ok(())
    }
    pub(crate) fn try_finish(
        self,
    ) -> std::result::Result<NativeRemotePreparedQuorum, (BicDbError, Self)> {
        if let Err(error) = self.require_complete_memory() {
            return Err((error, self));
        }
        Ok(NativeRemotePreparedQuorum { actual: self })
    }
    fn require_complete_memory(&self) -> Result<()> {
        self.request.check()?;
        if self.frames.len() != self.manifest.new_members.len()
            || self
                .manifest
                .new_members
                .keys()
                .any(|n| !self.frames.contains_key(n))
            || !self
                .frames
                .contains_key(&self.manifest.activation_range.leader)
        {
            return Err(invalid(
                "remote publication needs actual ALL final NEW preparations including leader",
            ));
        }
        for frame in self.frames.values() {
            frame.require_request_live()?;
        }
        Ok(())
    }
}
impl NativeRemotePreparedQuorum {
    pub(crate) fn require_live(&self) -> Result<()> {
        self.actual.require_complete_memory()
    }
    pub(crate) fn require_local(
        &self,
        committed: &CommittedNativeJointDecisionObservation,
        local: &crate::distribution_range_handoff::NativeRemotePreparedObservation,
    ) -> Result<()> {
        self.require_live()?;
        local.require_current()?;
        committed.require_current_metadata_only()?;
        let data = committed.data();
        let mf = &self.actual.manifest;
        let node = committed.native_receipt().local_node_id();
        if self.actual.observer.as_ref() != Some(node)
            || data.checksum_sha256() != self.actual.decision
            || data.promise().session_id != mf.handoff_session
            || data.promise().nonce != mf.nonce
            || data.old_range().id != mf.capture_range.id
            || data.activation_range() != &mf.activation_range
        {
            return Err(invalid(
                "remote prepared publication replaced original local native scope",
            ));
        }
        let point = self
            .actual
            .frames
            .get(node)
            .ok_or_else(|| invalid("remote preparation local actual frame absent"))?
            .point();
        local.require_original_remote_participant(&point.original_participant_sha256)?;
        let observed = point.preparation.as_ref().unwrap();
        if local.vote().preparation_sha256 != observed.preparation_sha256
            || local.vote().native_database_incarnation != observed.native_database_incarnation
            || local.vote().actual_local_visibility_sequence
                != observed.actual_local_visibility_sequence
            || local.vote().metadata_term < observed.metadata_term
            || local.vote().metadata_read_index < observed.metadata_read_index
            || committed.native_receipt().checksum_sha256() != point.native_decision_record_sha256
            || committed.native_receipt().native_database_incarnation()
                != point.native_database_open
        {
            return Err(invalid(
                "remote preparation local protected owner/receipt differs from ALL collection",
            ));
        }
        let (term, index, context) = committed.metadata_anchor();
        for frame in self.actual.frames.values() {
            let (t, i, c) = frame.metadata_anchor();
            if context == c || term < t || index < i {
                return Err(invalid(
                    "remote publication requires distinct fresh post-ALL actual MQ anchors",
                ));
            }
        }
        self.require_live()
    }
    pub(crate) fn require_local_memory(
        &self,
        local: &crate::distribution_range_handoff::NativeRemotePreparedObservation,
    ) -> Result<()> {
        self.require_live()?;
        local.require_request_memory()
    }
    pub(crate) fn require_native_local(
        &self,
        db: &crate::BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
        prepared: &crate::db::NativeJointServingPreparation,
        frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.require_live()?;
        let point = self
            .actual
            .frames
            .get(prepared.local_node_id())
            .ok_or_else(|| invalid("remote publication actual local preparation missing"))?
            .point();
        if point.native_database_open != db.incarnation_id()
            || point.store_open != store.native_writer_store_open_id()
            || point.original_frozen_owner != frozen.owner()
            || point.preparation.as_ref().is_none_or(|v| {
                v.preparation_sha256 != prepared.checksum_sha256()
                    || v.native_database_incarnation != prepared.native_database_incarnation()
            })
        {
            return Err(invalid(
                "remote publication substituted original DB/Store/Frozen/preparation",
            ));
        }
        Ok(())
    }
    pub(crate) fn votes(&self) -> BTreeMap<ClusterNodeId, NativeJointPreparedServingVoteData> {
        self.actual
            .frames
            .iter()
            .map(|(n, f)| (n.clone(), f.point().preparation.as_ref().unwrap().clone()))
            .collect()
    }
}
