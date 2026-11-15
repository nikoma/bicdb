//! Actual pinned ALL-new native bridge observations. This is a distinct remote proof;
//! it cannot construct a local release quorum, native Open, or Serving grant.
use super::*;
use crate::distribution_native_host::NativeRemoteServingOperationV1;
use crate::distribution_transport::AuthenticatedNativeRemoteServingFrame;
use std::sync::Arc;

pub(crate) struct NativeRemoteBridgeRound {
    manifest: Arc<NativeRemoteImageManifestV1>,
    phase: Uuid,
    decision: String,
    observer: Option<ClusterNodeId>,
    frames: BTreeMap<ClusterNodeId, AuthenticatedNativeRemoteServingFrame>,
    bytes: usize,
    caller: CancellationToken,
}
pub(crate) struct NativeRemoteBridgeQuorum {
    actual: NativeRemoteBridgeRound,
}
impl NativeRemoteBridgeRound {
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
                "remote ALL native bridge needs bounded original phase/D request",
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
            caller: parent.child(),
        })
    }
    pub(crate) fn record(&mut self, frame: AuthenticatedNativeRemoteServingFrame) -> Result<()> {
        self.caller.check()?;
        frame.require_live_binding(&self.manifest, self.phase, &self.decision)?;
        let point = frame.point();
        let preparation = point.preparation.as_ref().ok_or_else(|| {
            invalid("remote ALL native bridge lacks actual protected preparation")
        })?;
        if frame.operation() != NativeRemoteServingOperationV1::ObserveBridge
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
            || point.native_database_open.is_empty()
            || point.store_open.is_nil()
            || point.original_frozen_owner.is_nil()
            || preparation.native_database_incarnation != point.native_database_open
            || !is_digest(&preparation.preparation_sha256)
            || preparation.metadata_context_id.is_nil()
            || preparation.metadata_term == 0
            || preparation.metadata_read_index == 0
            || preparation.metadata_term > frame.metadata_anchor().0
            || preparation.metadata_read_index > frame.metadata_anchor().1
            || point
                .native_bridge_sha256
                .as_ref()
                .is_none_or(|s| !is_digest(s))
            || point.actual_dropped_open
        {
            return Err(invalid(
                "remote ALL native bridge changed exact member/observer/original native preparation/Open",
            ));
        }
        let bytes = self
            .bytes
            .checked_add(frame.retained_encoded_bytes()?)
            .ok_or_else(|| invalid("remote ALL native bridge byte count overflow"))?;
        if self.frames.len() >= MAX_NEW_MEMBERS || bytes > 8 * 1024 * 1024 {
            return Err(invalid(
                "remote ALL native bridge existing 128-member/8MiB custody bound",
            ));
        }
        self.observer = Some(frame.actual_observer().clone());
        self.bytes = bytes;
        self.frames.insert(frame.node_id().clone(), frame);
        Ok(())
    }
    pub(crate) fn try_finish(
        self,
    ) -> std::result::Result<NativeRemoteBridgeQuorum, (BicDbError, Self)> {
        if let Err(e) = self.require_complete_memory() {
            return Err((e, self));
        }
        Ok(NativeRemoteBridgeQuorum { actual: self })
    }
    fn require_complete_memory(&self) -> Result<()> {
        self.caller.check()?;
        if self.frames.len() != self.manifest.new_members.len()
            || !self
                .frames
                .contains_key(&self.manifest.activation_range.leader)
            || self
                .manifest
                .new_members
                .keys()
                .any(|id| !self.frames.contains_key(id))
        {
            return Err(invalid(
                "remote Open requires actual ALL final NEW native bridges including leader",
            ));
        }
        for frame in self.frames.values() {
            frame.require_request_live()?;
        }
        Ok(())
    }
}

impl NativeRemoteBridgeQuorum {
    pub(crate) fn require_live(&self) -> Result<()> {
        self.actual.require_complete_memory()
    }
    pub(crate) fn require_post_all(
        &self,
        fresh: &CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        self.require_live()?;
        fresh.require_current_metadata_only()?;
        let mf = &self.actual.manifest;
        if fresh.data().checksum_sha256() != self.actual.decision
            || fresh.data().promise().session_id != mf.handoff_session
            || fresh.data().promise().nonce != mf.nonce
            || fresh.data().activation_range() != &mf.activation_range
            || self.actual.observer.as_ref() != Some(fresh.native_receipt().local_node_id())
        {
            return Err(invalid(
                "remote Open changed actual ALL native bridge phase/observer",
            ));
        }
        let (term, index, context) = fresh.metadata_anchor();
        for frame in self.actual.frames.values() {
            let (t, i, c) = frame.metadata_anchor();
            if term < t || index < i || context == c {
                return Err(invalid(
                    "remote Open requires distinct fresh post-ALL MQ/current MStore",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn require_actual_local(
        &self,
        scope: &crate::db::NativeJointBridgeReadScope<'_>,
        store: &crate::distribution_range_consensus::RangeWriteStore,
        prepared: &crate::db::NativeJointServingPreparation,
        frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.require_live()?;
        let native = scope.native();
        let point = self
            .actual
            .frames
            .get(native.local_node_id())
            .ok_or_else(|| invalid("remote Open actual local bridge observation absent"))?
            .point();
        if point.native_bridge_sha256.as_deref() != Some(native.checksum_sha256())
            || point.native_database_open != scope.database().incarnation_id()
            || point.store_open != store.native_writer_store_open_id()
            || point.original_frozen_owner != frozen.owner()
            || point
                .preparation
                .as_ref()
                .is_none_or(|p| p.preparation_sha256 != prepared.checksum_sha256())
        {
            return Err(invalid(
                "remote Open substituted actual local native bridge/preparation/Frozen/Store",
            ));
        }
        Ok(())
    }
}
