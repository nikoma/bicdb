//! Actual source-owned session. Wire projections are immutable facts at issue,
//! never a local SourceRangeImageLineage or target mutation permission.
use super::*;
use crate::distribution::NativeStoreBirthV1;
use crate::distribution_consensus::{
    HostMetadataReadObservation, MetadataConsensusStore, MetadataReadParticipant,
};
use crate::distribution_data::{
    ClusterDataNodeService, ClusterServiceOpenGuard, RangeWriteServiceSlotGuard,
};
use crate::distribution_native_host::NativeInitialDataAdmission;
use crate::distribution_range_consensus::RangeWriteCoordinator;
use crate::distribution_range_handoff::{NativeJointMaterializedFacts, NativeJointOldClosedQuorum};
use crate::distribution_range_recovery::RangeRecoveryRuntime;
use crate::distribution_transport::TcpClusterRelocationTransport;
use parking_lot::Mutex;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteImageManifestV1 {
    pub version: u32,
    pub source_session: Uuid,
    pub target_attempt: Uuid,
    pub handoff_session: Uuid,
    pub nonce: Uuid,
    pub cluster_id: ClusterId,
    pub bootstrap_id: Uuid,
    pub relocation_id: crate::distribution::RelocationId,
    pub plan_id: String,
    pub source_id: ClusterNodeId,
    pub target_id: ClusterNodeId,
    pub source_birth: NativeStoreBirthV1,
    pub target_birth: NativeStoreBirthV1,
    pub source_database_open: String,
    pub source_store_open: Uuid,
    pub source_metadata_store_id: Uuid,
    pub source_history_generation: Uuid,
    pub capture_range: crate::RangeDescriptor,
    pub native_predecessor: crate::RangeDescriptor,
    pub activation_range: crate::RangeDescriptor,
    pub old_members: BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember>,
    pub new_members: BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember>,
    pub source_birth_members: BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember>,
    pub restore: Option<crate::distribution_consensus::MetadataRestoreActivation>,
    pub schema_sha256: String,
    pub readers: Vec<String>,
    pub collections: Vec<CollectionMeta>,
    pub source_namespace_ids: Vec<(String, Uuid)>,
    pub source_reserved_b: u64,
    pub source_native_local_b: u64,
    pub range_index: u64,
    pub h0: String,
    pub h: String,
    pub source_native_receipt: String,
    pub old_config_sha256: String,
    pub new_config_sha256: String,
    pub old_quorum_sha256: String,
    pub actual_old_closures: BTreeMap<ClusterNodeId, NativeJointMaterializedFacts>,
    pub portable_catalogue_sha256: String,
    pub checksum_sha256: String,
}
impl NativeRemoteImageManifestV1 {
    pub(crate) fn checksum(&self) -> Result<String> {
        let mut value = self.clone();
        value.checksum_sha256.clear();
        Ok(hex::encode(Sha256::digest(bounded_json(
            &("native-remote-image-manifest-v1", value),
            2 * 1024 * 1024,
        )?)))
    }
    pub(crate) fn require_shape(&self) -> Result<()> {
        if self.version != 1
            || self.source_session.is_nil()
            || self.target_attempt.is_nil()
            || self.handoff_session.is_nil()
            || self.nonce.is_nil()
            || self.bootstrap_id.is_nil()
            || self.source_database_open.is_empty()
            || self.source_store_open.is_nil()
            || self.source_metadata_store_id.is_nil()
            || self.source_history_generation.is_nil()
            || self.capture_range.id != self.native_predecessor.id
            || self.capture_range.id != self.activation_range.id
            || self.source_id == self.target_id
            || !self.old_members.contains_key(&self.source_id)
            || !self.new_members.contains_key(&self.target_id)
            || self.actual_old_closures.len() < self.old_members.len() / 2 + 1
            || self.actual_old_closures.len() > self.old_members.len()
            || self.actual_old_closures.iter().any(|(id, f)| {
                &f.node_id != id
                    || !self.old_members.contains_key(id)
                    || f.range_index > self.range_index
                    || f.logical_genesis_sha256 != self.h0
            })
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid(
                "invalid remote image manifest DATA; not a source/target grant",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) enum NativeRemoteImageSourceOperationV1 {
    Manifest,
    History {
        cursor: RangeImageHistoryCursorV1,
        max_entries: usize,
    },
    Rows {
        cursor: RangeImageCursorV1,
        options: RangeSnapshotOptions,
    },
    Finish,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteImageSourceRequestV1 {
    pub source_session: Uuid,
    pub target_attempt: Uuid,
    pub manifest_sha256: String,
    pub business: Uuid,
    pub challenge: Uuid,
    pub budget_ms: u64,
    pub operation: NativeRemoteImageSourceOperationV1,
}
impl NativeRemoteImageSourceRequestV1 {
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        if self.source_session.is_nil()
            || self.target_attempt.is_nil()
            || self.business.is_nil()
            || self.challenge.is_nil()
            || self.budget_ms == 0
            || self.budget_ms > 5000
            || self.manifest_sha256.len() != 64
            || !self.manifest_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(invalid("invalid bounded remote image request"));
        }
        let child =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(self.budget_ms));
        child.check()?;
        Ok(child)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum NativeRemoteImageSourcePayloadV1 {
    Manifest(NativeRemoteImageManifestV1),
    History(RangeImageHistoryStepV1),
    Rows(RangeImageStepV1),
    Finished(LocalRangeImageSealV1),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRemoteImageSourceReplyV1 {
    pub request: NativeRemoteImageSourceRequestV1,
    pub source_id: ClusterNodeId,
    pub source_birth: NativeStoreBirthV1,
    pub target_id: ClusterNodeId,
    pub manifest_sha256: String,
    pub source_metadata_context: Uuid,
    pub source_metadata_term: u64,
    pub source_metadata_read_index: u64,
    pub payload: NativeRemoteImageSourcePayloadV1,
    pub checksum_sha256: String,
}
impl NativeRemoteImageSourceReplyV1 {
    pub(crate) fn checksum(&self) -> Result<String> {
        let mut value = self.clone();
        value.checksum_sha256.clear();
        Ok(hex::encode(Sha256::digest(bounded_json(
            &("native-remote-image-frame-v1", value),
            MAX_WIRE_STEP_BYTES + 2 * 1024 * 1024,
        )?)))
    }
}
/// Only a real validated source session creates this non-Serde result. The
/// transport serializes its DTO; decoding that DTO never recreates this owner.
pub(crate) struct ActualNativeRemoteImageSourceFrame {
    reply: NativeRemoteImageSourceReplyV1,
}
impl ActualNativeRemoteImageSourceFrame {
    pub(crate) fn into_reply(self) -> NativeRemoteImageSourceReplyV1 {
        self.reply
    }
}
/// Owns the real local source snapshot and quorum. No serialized constructor,
/// replacement setters or deadline-renewal API. Long-session cancellation is
/// separate from every step's original bounded request/fresh OWN metadata.
pub(crate) struct NativeRemoteImageSourceSession {
    admission: Arc<NativeInitialDataAdmission>,
    runtime: Arc<RangeRecoveryRuntime>,
    service: Arc<Mutex<ClusterDataNodeService>>,
    metadata: Arc<Mutex<MetadataConsensusStore>>,
    actual_open: Option<ClusterServiceOpenGuard>,
    slot: Option<RangeWriteServiceSlotGuard>,
    writer: Arc<crate::db::InstalledNativeWriterHostGuard>,
    coordinator: Arc<RangeWriteCoordinator<TcpClusterRelocationTransport>>,
    metadata_store_id: Option<Uuid>,
    old: NativeJointOldClosedQuorum,
    capture: Option<RangeImageCapture>,
    manifest: Option<NativeRemoteImageManifestV1>,
    target_attempt: Uuid,
    capture_attempted: bool,
    published: bool,
    original_session: CancellationToken,
}
impl NativeRemoteImageSourceSession {
    /// Retain EVERY actual owner before the first fallible publication step.
    /// Construction grants no frame or target permission. Once capture starts,
    /// any failure remains quarantined in this owner; begin never recreates it.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn retain_actual_old_quorum(
        admission: Arc<NativeInitialDataAdmission>,
        runtime: Arc<RangeRecoveryRuntime>,
        service: Arc<Mutex<ClusterDataNodeService>>,
        metadata: Arc<Mutex<MetadataConsensusStore>>,
        writer: Arc<crate::db::InstalledNativeWriterHostGuard>,
        coordinator: Arc<RangeWriteCoordinator<TcpClusterRelocationTransport>>,
        old: NativeJointOldClosedQuorum,
        target_attempt: Uuid,
        original_session: &CancellationToken,
    ) -> Self {
        Self {
            admission,
            runtime,
            service,
            metadata,
            writer,
            coordinator,
            old,
            target_attempt,
            original_session: original_session.clone(),
            actual_open: None,
            slot: None,
            metadata_store_id: None,
            capture: None,
            manifest: None,
            capture_attempted: false,
            published: false,
        }
    }
    /// Original publish request bounds capture wait. The retained source
    /// session's original cancellation separately bounds every later frame.
    pub(crate) fn begin(
        &mut self,
        wait_budget: Duration,
        own: &HostMetadataReadObservation,
        business: Uuid,
        original_publish_request: &CancellationToken,
    ) -> Result<()> {
        if self.capture_attempted || self.published {
            return Err(invalid(
                "source capture already attempted; retain quarantined owner",
            ));
        }
        original_publish_request.check()?;
        self.original_session.check()?;
        self.admission
            .require_listener(&self.service, &self.runtime)?;
        if self.target_attempt.is_nil() || wait_budget.is_zero() {
            return Err(invalid(
                "actual image attempt and bounded wait are required",
            ));
        }
        let source = self.old.selected_source_id().clone();
        self.old
            .plan()
            .require_source_host_metadata(own, business, &source)?;
        self.old.require_current()?;
        let (database, actual_open, slot) = {
            let s = self
                .service
                .try_lock()
                .ok_or_else(|| invalid("source service busy at publication"))?;
            (
                s.database(),
                s.listener_open_guard(),
                s.native_replica_listener_store_open_guard(),
            )
        };
        let proof = {
            let m = self
                .metadata
                .try_lock()
                .ok_or_else(|| invalid("source metadata busy at publication"))?;
            m.require_listener_database(&actual_open)?;
            m.observe_owned_native_metadata_member(own, business, original_publish_request)?
        };
        {
            let db = read_database(&database)?;
            proof.require_database(&db)?;
        }
        self.actual_open = Some(actual_open);
        self.slot = Some(slot);
        self.metadata_store_id = Some(proof.store_id());
        let now = Instant::now();
        let wait_budget = wait_budget.min(
            original_publish_request
                .deadline()
                .map_or(wait_budget, |end| end.saturating_duration_since(now)),
        );
        original_publish_request.check()?;
        if wait_budget.is_zero() {
            return Err(BicDbError::QueryTimedOut);
        }
        self.capture_attempted = true;
        self.capture = Some(
            self.old
                .begin_capture(wait_budget, &self.original_session)?,
        );
        // Retain the capture BEFORE any post-capture failure. No failed publish
        // can drop these pins and then silently mint a replacement session.
        let capture = self.capture.as_ref().unwrap();
        if !Arc::ptr_eq(&capture.db, &database) || capture.cut.source_id() != &source {
            return Err(invalid(
                "OLD capture belongs to another actual source owner",
            ));
        }
        let native = {
            let db = read_database(&database)?;
            self.writer.require_for_coordinator(
                &db,
                capture.lineage.native_write_range()?,
                &self.coordinator,
            )?;
            db.native_recovery_admission(
                capture.lineage.cluster_id(),
                capture.lineage.bootstrap_id(),
                &source,
                capture.lineage.local_member_incarnation(),
            )?
        };
        let plan = self.old.plan();
        let mut manifest = NativeRemoteImageManifestV1 {
            version: 1,
            source_session: Uuid::now_v7(),
            target_attempt: self.target_attempt,
            handoff_session: capture.cut.session_id(),
            nonce: capture.cut.nonce(),
            cluster_id: capture.cut.cluster_id().clone(),
            bootstrap_id: capture.lineage.bootstrap_id(),
            relocation_id: plan.promise().relocation_id,
            plan_id: plan.promise().plan_id.clone(),
            source_id: source,
            target_id: capture.cut.target_id().clone(),
            source_birth: NativeStoreBirthV1::from_actual_native(&native)?,
            target_birth: plan.enrolled_birth(capture.cut.target_id())?.clone(),
            source_database_open: capture.bindings.database_incarnation().into(),
            source_store_open: self.slot.as_ref().unwrap().id(),
            source_metadata_store_id: proof.store_id(),
            source_history_generation: capture.lineage.history_generation(),
            capture_range: capture.cut.range().clone(),
            native_predecessor: capture.lineage.native_write_range()?.clone(),
            activation_range: plan.activation_range().clone(),
            old_members: plan.old_members().clone(),
            new_members: plan.new_members().clone(),
            source_birth_members: capture.lineage.base_members().clone(),
            restore: capture.lineage.restore_activation().cloned(),
            schema_sha256: capture.bindings.schema_sha256().into(),
            readers: capture.bindings.reader_features().to_vec(),
            collections: capture.bindings.collections().to_vec(),
            source_namespace_ids: capture.bindings.namespace_ids(),
            source_reserved_b: capture.reserved.0,
            source_native_local_b: capture.lineage.actual_local_visibility_sequence(),
            range_index: capture.lineage.range_index(),
            h0: capture.lineage.logical_genesis_sha256().into(),
            h: capture.lineage.logical_history_sha256().into(),
            source_native_receipt: capture.lineage.native_receipt_sha256().into(),
            old_config_sha256: capture.cut.old_config_sha256().into(),
            new_config_sha256: capture.cut.new_config_sha256().into(),
            old_quorum_sha256: self.old.checksum_sha256()?,
            actual_old_closures: self.old.source_image_materialized_facts()?,
            portable_catalogue_sha256: capture.catalog_sha256.clone(),
            checksum_sha256: String::new(),
        };
        manifest.checksum_sha256 = manifest.checksum()?;
        manifest.require_shape()?;
        self.manifest = Some(manifest);
        self.require_source_current(own, business, original_publish_request)?;
        self.published = true;
        Ok(())
    }
    pub(crate) fn manifest(&self) -> Result<&NativeRemoteImageManifestV1> {
        if !self.published {
            return Err(invalid(
                "source session publication is incomplete/quarantined",
            ));
        }
        self.manifest
            .as_ref()
            .ok_or_else(|| invalid("source manifest missing"))
    }
    fn require_source_current(
        &self,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        self.original_session.check()?;
        self.admission
            .require_listener(&self.service, &self.runtime)?;
        let capture = self
            .capture
            .as_ref()
            .ok_or_else(|| invalid("source capture absent"))?;
        let manifest = self
            .manifest
            .as_ref()
            .ok_or_else(|| invalid("source manifest absent"))?;
        let actual_open = self
            .actual_open
            .as_ref()
            .ok_or_else(|| invalid("source actual open absent"))?;
        let slot = self
            .slot
            .as_ref()
            .ok_or_else(|| invalid("source actual Store slot absent"))?;
        slot.require_live()?;
        // OLD receipt/service checks run OUTSIDE the source DB read guard.
        self.old.require_current()?;
        self.old
            .plan()
            .require_source_host_metadata(own, business, &manifest.source_id)?;
        let proof = {
            let metadata = self
                .metadata
                .try_lock()
                .ok_or_else(|| invalid("source metadata busy during frame"))?;
            metadata.require_listener_database(actual_open)?;
            metadata.observe_owned_native_metadata_member(own, business, parent)?
        };
        if Some(proof.store_id()) != self.metadata_store_id {
            return Err(invalid("source protected MStore changed"));
        }
        {
            let db = read_database(&capture.db)?;
            actual_open.require_database(&db)?;
            proof.require_database(&db)?;
            self.writer.require_for_coordinator(
                &db,
                capture.lineage.native_write_range()?,
                &self.coordinator,
            )?;
            capture.validate(&db)?;
        }
        {
            let metadata = self
                .metadata
                .try_lock()
                .ok_or_else(|| invalid("source metadata changed before frame"))?;
            proof.require_current(&metadata)?;
        }
        self.admission
            .require_listener(&self.service, &self.runtime)?;
        slot.require_live()?;
        actual_open.require_live()?;
        self.original_session.check()?;
        own.require_live_request(business)?;
        parent.check()
    }
    /// Source OWN MQ is acquired by its real provider BEFORE this call and any
    /// session/engine latch. It is READONLY metadata, never a recovery role cast.
    pub(crate) fn observe_frame(
        &mut self,
        actual_target: &MetadataReadParticipant,
        request: &NativeRemoteImageSourceRequestV1,
        own: &HostMetadataReadObservation,
        business: Uuid,
        original_request: &CancellationToken,
    ) -> Result<ActualNativeRemoteImageSourceFrame> {
        self.manifest()?;
        let token = request.cancellation(original_request)?;
        self.require_source_current(own, business, &token)?;
        let manifest = self.manifest()?.clone();
        let current = own.committed_topology(business)?;
        let target = current
            .nodes
            .get(&manifest.target_id)
            .ok_or_else(|| invalid("current image target absent"))?;
        if request.business != business
            || request.source_session != manifest.source_session
            || request.target_attempt != manifest.target_attempt
            || request.manifest_sha256 != manifest.checksum_sha256
            || actual_target.node_id() != &manifest.target_id
            || actual_target.member_incarnation() != target.incarnation
            || target.native_store_birth_v1.as_ref() != Some(&manifest.target_birth)
            || actual_target.plaintext_loopback()
            || actual_target
                .certificate_sha256()
                .is_none_or(|pin| !target.accepts_tls_certificate_sha256(pin))
        {
            return Err(invalid(
                "remote image actual requester/session/cut/birth differs",
            ));
        }
        let payload = match &request.operation {
            NativeRemoteImageSourceOperationV1::Manifest => {
                NativeRemoteImageSourcePayloadV1::Manifest(manifest.clone())
            }
            NativeRemoteImageSourceOperationV1::History {
                cursor,
                max_entries,
            } => {
                if *max_entries == 0 || *max_entries > 256 {
                    return Err(invalid("remote history step exceeds bound"));
                }
                NativeRemoteImageSourcePayloadV1::History(
                    self.capture.as_mut().unwrap().export_history_step(
                        cursor,
                        *max_entries,
                        &token,
                    )?,
                )
            }
            NativeRemoteImageSourceOperationV1::Rows { cursor, options } => {
                NativeRemoteImageSourcePayloadV1::Rows(
                    self.capture
                        .as_mut()
                        .unwrap()
                        .export_step(cursor, options, &token)?,
                )
            }
            NativeRemoteImageSourceOperationV1::Finish => {
                NativeRemoteImageSourcePayloadV1::Finished(
                    self.capture.as_ref().unwrap().completed_seal()?,
                )
            }
        };
        self.require_source_current(own, business, &token)?;
        let mut reply = NativeRemoteImageSourceReplyV1 {
            request: request.clone(),
            source_id: manifest.source_id.clone(),
            source_birth: manifest.source_birth.clone(),
            target_id: manifest.target_id.clone(),
            manifest_sha256: manifest.checksum_sha256.clone(),
            source_metadata_context: own.context_id(),
            source_metadata_term: own.term(),
            source_metadata_read_index: own.read_index(),
            payload,
            checksum_sha256: String::new(),
        };
        reply.checksum_sha256 = reply.checksum()?;
        token.check()?;
        own.require_live_request(business)?;
        self.original_session.check()?;
        Ok(ActualNativeRemoteImageSourceFrame { reply })
    }
}
