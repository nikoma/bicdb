//! Original process-owned NEW participant. Retention here is RAM custody,
//! never a durable DECISION journal or a cold-recovery capability.
use super::*;
use crate::distribution_consensus::HostMetadataReadObservation;
use crate::distribution_data::{ActualNativeJointMetadataPoint, ClusterDataNodeService};
use crate::distribution_native_host::{
    NativeInitialDataAdmission, NativeRegisteredRemoteJointOwner, NativeRegisteredRemoteJointTarget,
};
use crate::distribution_range_image_install::NativeImageInstallFreeze;
use crate::distribution_range_mutation::SourceCaptureReadPin;
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use crate::distribution_transport::{
    AuthenticatedNativeRemoteNewFactSet, AuthenticatedNativeSourceExportTerminal,
};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

enum OriginalParticipant {
    Old {
        actual: Arc<NativeJointOldObservation>,
        admission: Arc<NativeInitialDataAdmission>,
        writer: Arc<crate::db::NativeWriterHostState>,
        process: Arc<crate::distribution_range_image::NativeRemoteImageSourceRegistry>,
    },
    Registered {
        owner: Weak<NativeRegisteredRemoteJointOwner>,
        target: Arc<NativeRegisteredRemoteJointTarget>,
        service: Arc<Mutex<ClusterDataNodeService>>,
        frozen: Arc<NativeImageInstallFreeze>,
        pin: Mutex<Option<Arc<SourceCaptureReadPin>>>,
    },
}

#[derive(Default)]
struct PinProgress {
    old: AtomicBool,
    target_materialization: AtomicBool,
    target_export: AtomicBool,
}
struct RetainedPrepareAttempt {
    frames: Arc<AuthenticatedNativeRemoteNewFactSet>,
    // Fresh authority remains the original authority of THIS bounded attempt.
    // Retaining it never renews it on a subsequent request.
    role: Arc<RecoveryRoleAdmission>,
    own: Arc<HostMetadataReadObservation>,
    business: Uuid,
    metadata: Arc<ActualNativeJointMetadataPoint>,
    request: CancellationToken,
    source_export_terminal: Option<AuthenticatedNativeSourceExportTerminal>,
}
#[derive(Default)]
struct RetainedHandoff {
    intent: Option<NativeRemoteNewFactSetV1>,
    attempts: Vec<Arc<RetainedPrepareAttempt>>,
    encoded_frame_bytes: usize,
}
pub(crate) struct NativeRemoteNewParticipant {
    #[cfg(test)]
    cut: std::sync::atomic::AtomicU8,
    manifest_bytes: Vec<u8>,
    snapshot: NativeRemoteNewSnapshotV1,
    original: OriginalParticipant,
    requests: Mutex<BTreeMap<Uuid, Vec<u8>>>,
    handoff: Mutex<RetainedHandoff>,
    handoff_started: AtomicBool,
    progress: PinProgress,
}
impl NativeRemoteNewParticipant {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_actual_old(
        request: &NativeRemoteNewRequestV1,
        actual: Arc<NativeJointOldObservation>,
        admission: Arc<NativeInitialDataAdmission>,
        writer: Arc<crate::db::NativeWriterHostState>,
        process: Arc<crate::distribution_range_image::NativeRemoteImageSourceRegistry>,
        role: &RecoveryRoleAdmission,
        own: &HostMetadataReadObservation,
        business: Uuid,
        metadata: &ActualNativeJointMetadataPoint,
        metadata_store_id: Uuid,
    ) -> Result<Self> {
        request.require_plan(actual.plan())?;
        actual.plan().require_role(role)?;
        role.require_own_host_metadata(own, business)?;
        metadata.require_request(own, business)?;
        admission.require_remote_new_original_memory(&writer)?;
        process.require_original_process()?;
        let old = actual.actual_materialized_facts()?;
        if request.manifest.actual_old_closures.get(actual.node_id()) != Some(&old)
            || role.local_node_id() != actual.node_id()
        {
            return Err(invalid(
                "remote NEW original OLD owner differs from exact MF/recipient",
            ));
        }
        let new = if actual.plan().new_members().contains_key(actual.node_id()) {
            actual.require_new_member_role(role)?;
            let mut facts = old.clone();
            facts.metadata_context_id = role.context_id();
            facts.metadata_term = role.metadata_term();
            facts.metadata_read_index = role.metadata_read_index();
            Some(facts)
        } else {
            None
        };
        let (birth, database_open, store_open) = {
            let service = actual
                .actual_service()
                .try_lock()
                .ok_or_else(|| invalid("remote NEW original OLD service busy"))?;
            let database = service.database();
            let db = database
                .try_read()
                .ok_or_else(|| invalid("remote NEW original OLD DB busy"))?;
            metadata.require_native_point(&db)?;
            let store_open = service.with_writer_data_domain_store(&db, |store| {
                metadata.require_native_store_point(&db, store)?;
                Ok(store.native_writer_store_open_id())
            })?;
            (
                actual.actual_native_birth()?,
                db.incarnation_id().to_string(),
                store_open,
            )
        };
        let mut snapshot = NativeRemoteNewSnapshotV1 {
            binding: NativeSourceExportBindingV1::from_manifest(&request.manifest)?,
            phase_id: request.phase_id,
            node_id: actual.node_id().clone(),
            birth,
            database_open,
            store_open,
            metadata_store_id,
            original_old: Some(old),
            new_materialized: new,
            checksum_sha256: String::new(),
        };
        snapshot.checksum_sha256 = snapshot.checksum()?;
        snapshot.require_request(request)?;
        Ok(Self {
            #[cfg(test)]
            cut: std::sync::atomic::AtomicU8::new(0),
            manifest_bytes: crate::distribution_range_image::bounded_json(
                &request.manifest,
                MAX_NEW_WIRE_BYTES,
            )?,
            snapshot,
            original: OriginalParticipant::Old {
                actual,
                admission,
                writer,
                process,
            },
            requests: Mutex::new(BTreeMap::new()),
            handoff: Mutex::new(Default::default()),
            handoff_started: AtomicBool::new(false),
            progress: Default::default(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_actual_registered(
        request: &NativeRemoteNewRequestV1,
        owner: &Arc<NativeRegisteredRemoteJointOwner>,
        plan: &NativeJointHandoffPlan,
        role: &RecoveryRoleAdmission,
        own: &HostMetadataReadObservation,
        business: Uuid,
        metadata: &ActualNativeJointMetadataPoint,
        metadata_store_id: Uuid,
        parent: &CancellationToken,
    ) -> Result<Self> {
        request.require_plan(plan)?;
        plan.require_role(role)?;
        role.require_own_host_metadata(own, business)?;
        metadata.require_request(own, business)?;
        owner.require_first_policy_attempt_lifetime()?;
        let target = owner.target().clone();
        let bytes =
            crate::distribution_range_image::bounded_json(&request.manifest, MAX_NEW_WIRE_BYTES)?;
        if bytes
            != crate::distribution_range_image::bounded_json(target.manifest(), MAX_NEW_WIRE_BYTES)?
            || role.local_node_id() != &request.manifest.target_id
            || target.native().local_node_id() != role.local_node_id()
        {
            return Err(invalid(
                "remote NEW original Registered target/MF/A differs",
            ));
        }
        let frozen = owner.retained_freeze()?;
        let (mut snapshot, pin) = {
            let service = owner
                .service()
                .try_lock()
                .ok_or_else(|| invalid("remote NEW original Registered service busy"))?;
            let database = service.database();
            let db = database
                .try_read()
                .ok_or_else(|| invalid("remote NEW original Registered DB busy"))?;
            service.with_writer_data_domain_store(&db, |store| {
                metadata.require_native_store_point(&db, store)?;
                target.require_current(&db, store, frozen.try_as_frozen()?, parent)?;
                let base = target.base();
                let seal = target.source_seal();
                request
                    .manifest
                    .source_old_close_record
                    .require_source_seal(seal)?;
                let issuance = NativeRemoteNewTargetPinIssuance {
                    owner,
                    database: &db,
                    binding: NativeSourceExportBindingV1::from_manifest(&request.manifest)?,
                    frozen: &frozen,
                    role,
                    own,
                    business,
                    metadata,
                    parent,
                    _local: std::marker::PhantomData,
                };
                let pin = owner.retain_remote_new_materialization_pin(&issuance)?;
                let facts = NativeJointMaterializedFacts {
                    scope_sha256: plan.checksum_sha256()?,
                    node_id: role.local_node_id().clone(),
                    birth_id: base.birth_id(),
                    birth_sha256: base.birth_sha256().into(),
                    native_receipt_sha256: base.native_receipt_sha256().into(),
                    range_index: base.range_index(),
                    logical_genesis_sha256: base.logical_genesis_sha256().into(),
                    logical_history_sha256: base.logical_history_sha256().into(),
                    actual_local_visibility_sequence: base.target_snapshot_bound().get(),
                    source_snapshot_db_sequence: Some(seal.reserved_db_upper_bound),
                    image_root_sha256: Some(seal.image_root_sha256.clone()),
                    metadata_term: role.metadata_term(),
                    metadata_read_index: role.metadata_read_index(),
                    metadata_context_id: role.context_id(),
                };
                Ok((
                    NativeRemoteNewSnapshotV1 {
                        binding: NativeSourceExportBindingV1::from_manifest(&request.manifest)?,
                        phase_id: request.phase_id,
                        node_id: role.local_node_id().clone(),
                        birth: NativeStoreBirthV1::from_actual_native(target.native())?,
                        database_open: db.incarnation_id().to_string(),
                        store_open: target.store_open_id(),
                        metadata_store_id,
                        original_old: None,
                        new_materialized: Some(facts),
                        checksum_sha256: String::new(),
                    },
                    pin,
                ))
            })?
        };
        snapshot.checksum_sha256 = snapshot.checksum()?;
        snapshot.require_request(request)?;
        Ok(Self {
            #[cfg(test)]
            cut: std::sync::atomic::AtomicU8::new(0),
            manifest_bytes: bytes,
            snapshot,
            original: OriginalParticipant::Registered {
                owner: Arc::downgrade(owner),
                target,
                service: owner.service().clone(),
                frozen,
                pin: Mutex::new(Some(pin)),
            },
            requests: Mutex::new(BTreeMap::new()),
            handoff: Mutex::new(Default::default()),
            handoff_started: AtomicBool::new(false),
            progress: Default::default(),
        })
    }

    pub(crate) fn snapshot(&self) -> &NativeRemoteNewSnapshotV1 {
        &self.snapshot
    }
    pub(crate) fn handoff_started(&self) -> bool {
        self.handoff_started.load(Ordering::Acquire)
    }
    pub(crate) fn require_request(&self, request: &NativeRemoteNewRequestV1) -> Result<()> {
        request.require_shape()?;
        if request.phase_id != self.snapshot.phase_id
            || self.manifest_bytes
                != crate::distribution_range_image::bounded_json(
                    &request.manifest,
                    MAX_NEW_WIRE_BYTES,
                )?
        {
            return Err(invalid(
                "remote NEW cannot replace original phase/MF/A/session after retention",
            ));
        }
        self.snapshot.require_request(request)
    }
    pub(crate) fn retain_request(&self, request: &NativeRemoteNewRequestV1) -> Result<()> {
        self.require_request(request)?;
        let bytes = request.custody_bytes()?;
        let handoff = self
            .handoff
            .try_lock()
            .ok_or_else(|| invalid("remote NEW request/handoff custody busy"))?;
        let mut requests = self
            .requests
            .try_lock()
            .ok_or_else(|| invalid("remote NEW exact-request custody busy"))?;
        if let Some(old) = requests.get(&request.challenge) {
            if old != &bytes {
                return Err(invalid("remote NEW challenge changed exact request"));
            }
        } else {
            if requests.len() >= 64 {
                return Err(invalid(
                    "remote NEW request capacity; no eviction or replacement",
                ));
            }
            let used = requests
                .values()
                .try_fold(self.manifest_bytes.len(), |sum, b| sum.checked_add(b.len()))
                .and_then(|sum| sum.checked_add(handoff.encoded_frame_bytes))
                .and_then(|sum| sum.checked_add(bytes.len()))
                .ok_or_else(|| invalid("remote NEW exact request custody byte overflow"))?;
            if used > 8 * 1024 * 1024 {
                return Err(invalid(
                    "remote NEW exact request custody exhausted (8MiB); no eviction",
                ));
            }
            requests.insert(request.challenge, bytes);
        }
        Ok(())
    }
    pub(crate) fn require_lifetime_memory(&self) -> Result<()> {
        match &self.original {
            OriginalParticipant::Old {
                admission,
                writer,
                process,
                ..
            } => {
                process.require_original_process()?;
                admission.require_remote_new_original_memory(writer)
            }
            OriginalParticipant::Registered { owner, target, .. } => {
                owner
                    .upgrade()
                    .ok_or_else(|| invalid("remote NEW original Registered owner ended"))?
                    .require_first_policy_attempt_lifetime()?;
                target
                    .original_hook()
                    .require_remote_new_registration_memory()
            }
        }
    }
    /// Called only outside metadata/native publication guards. Read-only
    /// observation after handoff verifies the retained freeze, never repins.
    pub(crate) fn require_current(&self, parent: &CancellationToken) -> Result<()> {
        parent.check()?;
        self.require_lifetime_memory()?;
        match &self.original {
            OriginalParticipant::Old { actual, .. } => actual.require_current()?,
            OriginalParticipant::Registered {
                target,
                service,
                frozen,
                pin,
                ..
            } => {
                let service = service
                    .try_lock()
                    .ok_or_else(|| invalid("remote NEW current target service busy"))?;
                let database = service.database();
                let db = database
                    .try_read()
                    .ok_or_else(|| invalid("remote NEW current target DB busy"))?;
                service.with_writer_data_domain_store(&db, |store| {
                    target.require_current(&db, store, frozen.try_as_frozen()?, parent)?;
                    let pin = pin
                        .try_lock()
                        .ok_or_else(|| invalid("remote NEW current target pin busy"))?;
                    if let Some(pin) = pin.as_ref() {
                        db.range_mutation_gate()
                            .validate_source_capture_read_pin(self.snapshot.binding.range_id, pin)
                    } else {
                        db.range_mutation_gate().validate_quiescent_frozen(
                            self.snapshot.binding.range_id,
                            frozen.try_as_frozen()?,
                        )
                    }
                })?;
            }
        }
        self.require_lifetime_memory()?;
        parent.check()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare_decision_pin_handoff(
        &self,
        request: &NativeRemoteNewRequestV1,
        fetched: AuthenticatedNativeRemoteNewFactSet,
        source_export_terminal: Option<AuthenticatedNativeSourceExportTerminal>,
        role: Arc<RecoveryRoleAdmission>,
        own: Arc<HostMetadataReadObservation>,
        business: Uuid,
        metadata: Arc<ActualNativeJointMetadataPoint>,
        parent: &CancellationToken,
    ) -> Result<NativeRemoteNewHandoffV1> {
        self.require_request(request)?;
        let NativeRemoteNewOperationV1::PrepareDecision { facts } = &request.operation else {
            return Err(invalid("remote pin handoff needs exact prepare operation"));
        };
        if fetched.facts() != facts || fetched.actual_observer() != &self.snapshot.node_id {
            return Err(invalid(
                "remote pin handoff did not independently fetch its exact original fact set",
            ));
        }
        let plan = NativeJointHandoffPlan::from_actual_role_and_own(&role, &own, business)?;
        request.require_plan(&plan)?;
        if role.local_node_id() != &self.snapshot.node_id {
            return Err(invalid(
                "remote pin handoff fresh role belongs to another participant",
            ));
        }
        role.require_own_host_metadata(&own, business)?;
        metadata.require_request(&own, business)?;
        metadata.require_external_current()?;
        fetched.require_request_live()?;
        match (&self.original, &source_export_terminal) {
            (OriginalParticipant::Registered { .. }, Some(actual)) => {
                actual.require_manifest(&request.manifest)?
            }
            (OriginalParticipant::Old { .. }, None) => {}
            _ => {
                return Err(invalid(
                    "remote NEW target pin handoff requires genuine prior source export terminal",
                ))
            }
        }
        self.require_current(parent)?;
        let mut retained = self
            .handoff
            .try_lock()
            .ok_or_else(|| invalid("remote NEW original handoff already active"))?;
        if let Some(original) = &retained.intent {
            if original != facts {
                return Err(invalid(
                    "remote NEW cannot choose a different fact set after uncertain handoff",
                ));
            }
        } else {
            retained.intent = Some(facts.clone());
        }
        // An error is not permission to evict an earlier retained observation.
        // Each genuinely fresh request gets at most one bounded attempt.
        if retained.attempts.len() >= 8 {
            return Err(invalid(
                "remote NEW handoff attempt custody exhausted (8); no reset or eviction",
            ));
        }
        let encoded_bytes = fetched
            .retained_encoded_bytes()?
            .checked_add(
                source_export_terminal
                    .as_ref()
                    .map(|v| v.retained_encoded_bytes())
                    .transpose()?
                    .unwrap_or(0),
            )
            .ok_or_else(|| invalid("remote NEW retained byte count overflow"))?;
        let request_bytes = self
            .requests
            .try_lock()
            .ok_or_else(|| invalid("remote NEW retained request-byte custody busy"))?
            .values()
            .try_fold(0usize, |sum, b| sum.checked_add(b.len()))
            .ok_or_else(|| invalid("remote NEW request byte count overflow"))?;
        let next_bytes = retained
            .encoded_frame_bytes
            .checked_add(encoded_bytes)
            .and_then(|sum| sum.checked_add(self.manifest_bytes.len()))
            .and_then(|sum| sum.checked_add(request_bytes))
            .ok_or_else(|| invalid("remote NEW handoff byte count overflow"))?;
        if next_bytes > 8 * 1024 * 1024 {
            return Err(invalid(
                "remote NEW retained encoded custody exhausted (8MiB); no reset or eviction",
            ));
        }
        let attempt = Arc::new(RetainedPrepareAttempt {
            frames: Arc::new(fetched),
            role,
            own,
            business,
            metadata,
            request: parent.child(),
            source_export_terminal,
        });
        retained.attempts.push(attempt.clone());
        retained.encoded_frame_bytes += encoded_bytes;
        // Complete exact intent, authenticated observations, original native
        // owners and all freeze obligations precede the first possible take.
        self.handoff_started.store(true, Ordering::Release);
        let permit = NativeRemoteNewPinHandoffPermit {
            participant: self,
            attempt: &attempt,
        };
        #[cfg(test)]
        if self
            .cut
            .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            attempt.request.cancel();
        }
        permit.require_memory_current()?;
        match &self.original {
            OriginalParticipant::Old { actual, .. } => {
                // The source's outstanding S2 capture clones prevent this
                // take. Only actual prior export retirement lets it drain.
                let dropped = actual.take_remote_new_decision_pin(&permit)?;
                drop(dropped); // never under the metadata publication guard
            }
            OriginalParticipant::Registered { owner, pin, .. } => {
                let owner = owner
                    .upgrade()
                    .ok_or_else(|| invalid("remote handoff original Registered owner ended"))?;
                let transition = owner.borrow_remote_new_transition(self)?;
                // The readonly export pin is a separate genuine obligation.
                // Its record/freezes remain retained after taking ONLY its pin.
                owner.handoff_remote_new_export_pin(&permit)?;
                let mut pin = pin
                    .try_lock()
                    .ok_or_else(|| invalid("remote handoff target materialization pin busy"))?;
                if pin.is_none() && !self.progress.target_materialization.load(Ordering::Acquire) {
                    return Err(invalid(
                        "remote target materialization pin absent without this original handoff",
                    ));
                }
                if pin.as_ref().is_some_and(|pin| Arc::strong_count(pin) != 1) {
                    return Err(invalid(
                        "remote target actual materialization pin still has other read owners",
                    ));
                }
                let dropped = {
                    let _version = attempt.metadata.try_publication_guard()?;
                    permit.require_memory_current()?;
                    let taken = pin.take();
                    self.progress
                        .target_materialization
                        .store(true, Ordering::Release);
                    taken
                };
                drop(pin);
                drop(transition);
                drop(dropped);
            }
        }
        #[cfg(test)]
        if self
            .cut
            .compare_exchange(2, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            attempt.request.cancel();
        }
        // Every postcheck may fail. Original intent, native owners, actual
        // callback frames and the exact completed pin flags stay retained.
        permit.require_memory_current()?;
        self.require_current(parent)?;
        attempt.metadata.require_external_current()?;
        drop(retained);
        self.handoff_outcome()?
            .ok_or_else(|| invalid("remote handoff progress incomplete"))
    }
    pub(crate) fn handoff_outcome(&self) -> Result<Option<NativeRemoteNewHandoffV1>> {
        let retained = self
            .handoff
            .try_lock()
            .ok_or_else(|| invalid("remote handoff outcome custody busy"))?;
        let Some(intent) = &retained.intent else {
            return Ok(None);
        };
        let old = self.progress.old.load(Ordering::Acquire);
        let target_materialization = self.progress.target_materialization.load(Ordering::Acquire);
        let target_export = self.progress.target_export.load(Ordering::Acquire);
        let complete = match &self.original {
            OriginalParticipant::Old { .. } => old,
            OriginalParticipant::Registered { .. } => target_materialization && target_export,
        };
        if !complete {
            return Ok(None);
        }
        Ok(Some(NativeRemoteNewHandoffV1 {
            snapshot_sha256: self.snapshot.checksum_sha256.clone(),
            fact_set_sha256: intent.checksum_sha256.clone(),
            old_read_pin_detached: old,
            target_materialization_pin_detached: target_materialization,
            target_export_pin_detached: target_export,
            original_freeze_retained: true,
        }))
    }
}

#[cfg(test)]
impl NativeRemoteNewParticipant {
    pub(crate) fn test_arm_original_request_cancel(&self, after_actual_pins: bool) -> Result<()> {
        self.cut
            .compare_exchange(
                0,
                if after_actual_pins { 2 } else { 1 },
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| invalid("remote NEW actual cancellation cut already armed"))?;
        Ok(())
    }
    pub(crate) fn test_evidence(&self) -> Result<serde_json::Value> {
        let handoff = self
            .handoff
            .try_lock()
            .ok_or_else(|| invalid("remote NEW diagnostic handoff custody busy"))?;
        Ok(serde_json::json!({
            "snapshot":self.snapshot,"handoff_started":self.handoff_started(),
            "retained_encoded_frame_bytes":handoff.encoded_frame_bytes,
            "exact_intent":handoff.intent,"attempts":handoff.attempts.iter().map(|a| serde_json::json!({
                "actual_independently_fetched_facts":a.frames.facts(),
                "actual_observer":a.frames.actual_observer(),
                "original_request_cancelled":a.request.is_cancel_requested(),
                "original_request_expired":a.request.deadline().is_some_and(|d|Instant::now()>=d),
                "original_role_context":a.role.context_id(),"original_own_context":a.own.context_id(),
                "source_export_terminal_retained":a.source_export_terminal.is_some(),
            })).collect::<Vec<_>>(),
            "old_pin_reference_detached":self.progress.old.load(Ordering::Acquire),
            "target_materialization_reference_detached":self.progress.target_materialization.load(Ordering::Acquire),
            "target_export_reference_detached":self.progress.target_export.load(Ordering::Acquire),
            "original_freeze_still_retained":true,"RAM_custody_not_native_decision":true,
            "not_a_Ready_or_Serving_grant":true,
        }))
    }
}

/// Sealed borrowed issuance within the genuine target participant constructor,
/// after its complete actual Role/OWN/native/catalogue/A validation. There is
/// no DTO constructor or generic original-pin getter.
pub(crate) struct NativeRemoteNewTargetPinIssuance<'a> {
    owner: &'a Arc<NativeRegisteredRemoteJointOwner>,
    database: &'a crate::BicDb,
    binding: NativeSourceExportBindingV1,
    frozen: &'a Arc<NativeImageInstallFreeze>,
    role: &'a RecoveryRoleAdmission,
    own: &'a HostMetadataReadObservation,
    business: Uuid,
    metadata: &'a ActualNativeJointMetadataPoint,
    parent: &'a CancellationToken,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl NativeRemoteNewTargetPinIssuance<'_> {
    pub(crate) fn require_owner(
        &self,
        owner: &NativeRegisteredRemoteJointOwner,
    ) -> Result<(
        &crate::BicDb,
        &NativeSourceExportBindingV1,
        &Arc<NativeImageInstallFreeze>,
    )> {
        self.parent.check()?;
        self.role
            .require_own_host_metadata(self.own, self.business)?;
        self.metadata.require_request(self.own, self.business)?;
        self.metadata.require_native_point(self.database)?;
        if !std::ptr::eq(self.owner.as_ref(), owner) {
            return Err(invalid(
                "remote NEW target pin issuance changed original actual owner",
            ));
        }
        self.binding.require_manifest(owner.target().manifest())?;
        owner
            .target()
            .require_original_point(self.database, self.frozen.try_as_frozen()?)?;
        owner
            .target()
            .original_hook()
            .require_current(self.database)?;
        self.parent.check()?;
        Ok((self.database, &self.binding, self.frozen))
    }
}

/// Unserializable bounded sink permission. Constructor is private to the
/// retained original participant after authentic independent callbacks and
/// fresh actual Role/OWN/native-MStore checks. It cannot authorize WAL/Serving.
pub(crate) struct NativeRemoteNewPinHandoffPermit<'a> {
    participant: &'a NativeRemoteNewParticipant,
    attempt: &'a RetainedPrepareAttempt,
}
impl NativeRemoteNewPinHandoffPermit<'_> {
    pub(crate) fn require_memory_current(&self) -> Result<()> {
        self.attempt.request.check()?;
        self.attempt.role.require_live_request()?;
        self.attempt
            .own
            .require_live_request(self.attempt.business)?;
        self.attempt.metadata.require_current_memory()?;
        self.attempt.frames.require_request_live()?;
        if let Some(terminal) = &self.attempt.source_export_terminal {
            terminal.require_request_live()?;
        }
        self.participant.require_lifetime_memory()
    }
    pub(crate) fn publication_guard(
        &self,
    ) -> Result<crate::distribution_data::ActualNativeJointMetadataPublicationGuard<'_>> {
        self.require_memory_current()?;
        self.attempt.metadata.try_publication_guard()
    }
    pub(in crate::distribution_range_handoff::joint) fn require_old(
        &self,
        old: &NativeJointOldObservation,
    ) -> Result<()> {
        match &self.participant.original {
            OriginalParticipant::Old { actual, .. } if std::ptr::eq(actual.as_ref(), old) => Ok(()),
            _ => Err(invalid(
                "remote pin handoff changed original actual OLD owner",
            )),
        }
    }
    pub(crate) fn require_registered(
        &self,
        owner: &NativeRegisteredRemoteJointOwner,
    ) -> Result<()> {
        match &self.participant.original {
            OriginalParticipant::Registered {
                owner: original, ..
            } if original
                .upgrade()
                .is_some_and(|a| std::ptr::eq(a.as_ref(), owner)) =>
            {
                Ok(())
            }
            _ => Err(invalid(
                "remote pin handoff changed original actual Registered owner",
            )),
        }
    }
    pub(in crate::distribution_range_handoff::joint) fn old_already_detached(&self) -> bool {
        self.participant.progress.old.load(Ordering::Acquire)
    }
    pub(in crate::distribution_range_handoff::joint) fn record_old_detached(&self) {
        self.participant.progress.old.store(true, Ordering::Release);
    }
    pub(crate) fn record_target_export_detached(&self) {
        self.participant
            .progress
            .target_export
            .store(true, Ordering::Release);
    }
}
