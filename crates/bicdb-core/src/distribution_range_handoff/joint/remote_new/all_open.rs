//! Actual pinned ALL-new-Open observations. This is a distinct remote proof;
//! it cannot construct a local release quorum, native Open, or Serving grant.
use super::*;
use crate::distribution_consensus::HostMetadataReadObservation;
use crate::distribution_data::{
    ActualNativeJointMetadataPoint, ActualNativeJointMetadataPublicationGuard,
};
use crate::distribution_native_host::NativeRemoteServingOperationV1;
use crate::distribution_transport::AuthenticatedNativeRemoteServingFrame;
use std::sync::Arc;

pub(crate) struct NativeRemoteAllOpenRound {
    manifest: Arc<NativeRemoteImageManifestV1>,
    binding: NativeSourceExportBindingV1,
    phase: Uuid,
    decision: String,
    observer: Option<ClusterNodeId>,
    frames: BTreeMap<ClusterNodeId, AuthenticatedNativeRemoteServingFrame>,
    bytes: usize,
    caller: CancellationToken,
}
pub(crate) struct NativeRemoteAllOpenQuorum {
    actual: NativeRemoteAllOpenRound,
}
impl NativeRemoteAllOpenRound {
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
                "remote ALL Open needs bounded original phase/D request",
            ));
        }
        let bytes = crate::distribution_range_image::bounded_json(
            manifest.as_ref(),
            MAX_NEW_REQUEST_BYTES,
        )?
        .len();
        let binding = NativeSourceExportBindingV1::from_manifest(&manifest)?;
        Ok(Self {
            manifest,
            binding,
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
        let preparation = point
            .preparation
            .as_ref()
            .ok_or_else(|| invalid("remote ALL Open lacks actual protected preparation"))?;
        if !matches!(
            frame.operation(),
            NativeRemoteServingOperationV1::ObserveOpen
                | NativeRemoteServingOperationV1::ObserveReleased
        ) || !self.manifest.new_members.contains_key(frame.node_id())
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
            || point
                .catalogue_activation_sha256
                .as_ref()
                .is_none_or(|s| !is_digest(s))
            || (frame.operation() == NativeRemoteServingOperationV1::ObserveReleased
                && !point.actual_dropped_open)
        {
            return Err(invalid(
                "remote ALL Open changed exact member/observer/original native preparation/Open",
            ));
        }
        let bytes = self
            .bytes
            .checked_add(frame.retained_encoded_bytes()?)
            .ok_or_else(|| invalid("remote ALL Open byte count overflow"))?;
        if self.frames.len() >= MAX_NEW_MEMBERS || bytes > 8 * 1024 * 1024 {
            return Err(invalid(
                "remote ALL Open existing 128-member/8MiB custody bound",
            ));
        }
        self.observer = Some(frame.actual_observer().clone());
        self.bytes = bytes;
        self.frames.insert(frame.node_id().clone(), frame);
        Ok(())
    }
    pub(crate) fn try_finish(
        self,
    ) -> std::result::Result<NativeRemoteAllOpenQuorum, (BicDbError, Self)> {
        if let Err(e) = self.require_complete_memory() {
            return Err((e, self));
        }
        Ok(NativeRemoteAllOpenQuorum { actual: self })
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
                "remote owner retirement requires actual ALL final NEW Open including leader",
            ));
        }
        for frame in self.frames.values() {
            frame.require_request_live()?;
        }
        Ok(())
    }
}
/// Fresh post-collection HostMQ + actual attached current-MStore point. No
/// data-voter mutation Role is minted for a retired donor: this authorizes
/// ONLY dropping obsolete RAM references, never rows, WAL or Frozen release.
pub(crate) struct NativeRemoteAllOpenRetirementAdmission<'a> {
    quorum: &'a NativeRemoteAllOpenQuorum,
    own: &'a HostMetadataReadObservation,
    business: Uuid,
    metadata: &'a ActualNativeJointMetadataPoint,
    caller: &'a CancellationToken,
}
impl NativeRemoteAllOpenQuorum {
    pub(crate) fn admit_retirement<'a>(
        &'a self,
        own: &'a HostMetadataReadObservation,
        business: Uuid,
        metadata: &'a ActualNativeJointMetadataPoint,
        caller: &'a CancellationToken,
    ) -> Result<NativeRemoteAllOpenRetirementAdmission<'a>> {
        let point = NativeRemoteAllOpenRetirementAdmission {
            quorum: self,
            own,
            business,
            metadata,
            caller,
        };
        point.require_current()?;
        Ok(point)
    }
}
impl NativeRemoteAllOpenRetirementAdmission<'_> {
    pub(crate) fn require_memory_current(&self) -> Result<()> {
        self.caller.check()?;
        self.own.require_live_request(self.business)?;
        self.quorum.actual.require_complete_memory()?;
        self.metadata.require_current_memory()
    }
    /// Full topology/request validation outside final short publication guards.
    pub(crate) fn require_current(&self) -> Result<()> {
        self.require_memory_current()?;
        self.metadata.require_request(self.own, self.business)?;
        self.metadata.require_external_current()?;
        let actual = &self.quorum.actual;
        let mf = &actual.manifest;
        let topology = self.own.committed_topology(self.business)?;
        let record = topology
            .relocations
            .get(&mf.relocation_id)
            .and_then(|p| p.native_joint_handoff_v1.as_ref())
            .ok_or_else(|| invalid("remote ALL Open actual committed phase absent"))?;
        if actual.observer.as_ref() != Some(self.own.requester_node_id())
            || topology.cluster_id != mf.cluster_id
            || topology.range_image_bootstrap_id != Some(mf.bootstrap_id)
            || self.own.restore_activation(self.business)? != mf.restore.as_ref()
            || topology
                .range_by_id(mf.capture_range.id)
                .map(canonical_range)
                != Some(canonical_range(&mf.activation_range))
            || record.stage() != crate::distribution::NativeJointHandoffStageV1::AwaitingActivation
            || record.session_id() != mf.handoff_session
            || record.nonce() != mf.nonce
            || record.decision_sha256() != actual.decision
            || record.old_configuration_sha256() != mf.old_config_sha256
            || record.new_configuration_sha256() != mf.new_config_sha256
            || record.range_index() != mf.range_index
            || record.logical_genesis_sha256() != mf.h0
            || record.logical_history_sha256() != mf.h
        {
            return Err(invalid(
                "remote ALL Open fresh observer/MStore changed original committed scope",
            ));
        }
        for frame in actual.frames.values() {
            let (term, index, context) = frame.metadata_anchor();
            if self.own.context_id() == context
                || self.own.term() < term
                || self.own.read_index() < index
            {
                return Err(invalid(
                    "remote ALL Open requires different post-ALL HostMQ context/anchors",
                ));
            }
        }
        self.require_memory_current()
    }
    pub(crate) fn publication_guard(
        &self,
    ) -> Result<ActualNativeJointMetadataPublicationGuard<'_>> {
        self.require_memory_current()?;
        self.metadata.try_publication_guard()
    }
    pub(crate) fn require_database_store(
        &self,
        db: &crate::BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<()> {
        self.metadata.require_native_store_point(db, store)
    }
    pub(crate) fn observer(&self) -> &ClusterNodeId {
        self.own.requester_node_id()
    }
    pub(crate) fn manifest(&self) -> &NativeRemoteImageManifestV1 {
        &self.quorum.actual.manifest
    }
    pub(crate) fn decision_sha256(&self) -> &str {
        &self.quorum.actual.decision
    }
    pub(crate) fn phase_id(&self) -> Uuid {
        self.quorum.actual.phase
    }
    pub(crate) fn require_original_participant(
        &self,
        participant: &NativeRemoteNewParticipant,
    ) -> Result<()> {
        self.require_memory_current()?;
        let snapshot = participant.snapshot();
        if snapshot.binding != self.quorum.actual.binding {
            return Err(invalid("remote ALL Open changed original exact binding"));
        }
        participant.require_lifetime_memory()?;
        if snapshot.phase_id != self.phase_id() || &snapshot.node_id != self.observer() {
            return Err(invalid(
                "remote ALL Open retirement changed actual original participant/observer",
            ));
        }
        if let Some(frame) = self.quorum.actual.frames.get(&snapshot.node_id) {
            let p = frame.point();
            if p.original_participant_sha256 != snapshot.checksum_sha256
                || p.native_database_open != snapshot.database_open
                || p.store_open != snapshot.store_open
            {
                return Err(invalid(
                    "remote ALL Open local frame substituted original owner/DB/Store",
                ));
            }
        } else if !self.manifest().old_members.contains_key(&snapshot.node_id)
            || self.manifest().new_members.contains_key(&snapshot.node_id)
            || snapshot.new_materialized.is_some()
            || snapshot.original_old.as_ref()
                != self.manifest().actual_old_closures.get(&snapshot.node_id)
        {
            return Err(invalid(
                "remote ALL Open nonfinal retirement changed exact original retired OLD fact",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_original_frozen_owner(
        &self,
        node: &ClusterNodeId,
        owner: Uuid,
    ) -> Result<()> {
        self.require_memory_current()?;
        if node != self.observer() {
            return Err(invalid(
                "ALL Open frozen owner belongs to another actual observer",
            ));
        }
        if let Some(frame) = self.quorum.actual.frames.get(node) {
            if frame.point().original_frozen_owner != owner {
                return Err(invalid("ALL Open changed original actual frozen owner"));
            }
        } else if !self.manifest().old_members.contains_key(node)
            || self.manifest().new_members.contains_key(node)
        {
            return Err(invalid(
                "ALL Open nonfinal frozen is not an exact original retiring OLD member",
            ));
        }
        Ok(())
    }
    pub(super) fn require_native_decision(
        &self,
        participant: &NativeRemoteNewParticipant,
        receipt: &crate::db::NativeJointDecisionReceipt,
        native: &crate::db::NativeRemoteJointDecisionAttempt,
    ) -> Result<()> {
        self.require_original_participant(participant)?;
        let frame = self
            .quorum
            .actual
            .frames
            .get(&participant.snapshot().node_id)
            .ok_or_else(|| {
                invalid("remote surviving participant lacks actual ALL-Open local frame")
            })?;
        let point = frame.point();
        if receipt.decision_sha256() != self.decision_sha256()
            || point.native_decision_record_sha256 != receipt.checksum_sha256()
            || point.native_decision_xid != native.actual_committed_xid()?
            || point.remote_provenance_sha256 != native.actual_provenance_sha256()?
        {
            return Err(invalid(
                "remote ALL Open substituted original known native outcome/provenance",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_source_certificate(
        &self,
        source: &NativeRemoteOldCutCertificate,
    ) -> Result<()> {
        self.require_memory_current()?;
        if self.observer() != &self.manifest().source_id {
            return Err(invalid(
                "remote ALL Open source retirement must be observed by actual original donor",
            ));
        }
        source.require_all_open_retirement_identity(self.manifest())
    }
}
