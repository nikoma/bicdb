//! SAME original remote preparation → lease-free release history. The host
//! retains this successor before taking any original reference. No DTO, vote
//! or digest can construct it, and no ownership comparison grants release.
use super::*;
use crate::distribution_range_handoff::{
    NativeJointPreparationCustody, NativeRemoteNewParticipant,
};
use std::sync::Weak;
use std::time::Instant;
mod outcome;
use outcome::RetainedReturns;

struct LeaseFreeHistory {
    origin: CompletedPreparationOrigin,
    prepared: crate::db::NativeJointServingPreparation,
    _committed: CommittedNativeJointDecisionObservation,
    _catalogue: NativeJointBridgeCatalogue,
    _writer: crate::db::NativeJointPhaseOwner,
    _domain: crate::db::NativeJointWriterDomain,
    _pin: parking_lot::Mutex<Option<SourceCaptureReadPin>>,
}
struct ReleaseState {
    custody: Option<Arc<NativeJointPreparationCustody>>,
    evidence: Option<Arc<ActualServiceNativeJointPreparedEvidence>>,
    actual: Option<ActualServiceNativeJointPreparedEvidence>,
    history: Option<LeaseFreeHistory>,
    registered: Option<crate::distribution_native_host::NativeRegisteredRemoteJointReleaseOwner>,
}
pub(crate) struct NativeRemoteReleaseOwner {
    service: Arc<Mutex<ClusterDataNodeService>>,
    participant: Weak<NativeRemoteNewParticipant>,
    process: Arc<crate::distribution_native_host::NativeRemotePreparationProcess>,
    original_custody: Weak<NativeJointPreparationCustody>,
    domain: crate::db::NativeJointWriterDomain,
    writer: crate::db::NativeJointPhaseOwner,
    native: Arc<crate::db::NativeJointServingBridgeReceipt>,
    activation: Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>,
    survivor: Option<Arc<crate::distribution_range_handoff::NativeJointHandoffRuntime>>,
    frozen_owner: Uuid,
    root: std::path::PathBuf,
    state: parking_lot::Mutex<ReleaseState>,
    outer_returns: parking_lot::Mutex<RetainedReturns>,
    release_returns: parking_lot::Mutex<RetainedReturns>,
    policy_returns: parking_lot::Mutex<RetainedReturns>,
    policy_outer_returns: parking_lot::Mutex<RetainedReturns>,
    #[cfg(test)]
    cancel_after_drop: parking_lot::Mutex<Option<CancellationToken>>,
    #[cfg(test)]
    drop_cut: parking_lot::Mutex<Option<serde_json::Value>>,
}
pub(crate) struct NativeRemoteReleaseObservation {
    owner: Arc<NativeRemoteReleaseOwner>,
    actual: ActualServiceNativeJointReleaseEvidence,
    vote: crate::distribution_range_handoff::NativeJointPreparedServingVoteData,
}
impl NativeRemoteReleaseOwner {
    pub(crate) fn observe_current(
        self: &Arc<Self>,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        leader: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<NativeRemoteReleaseObservation> {
        parent.check()?;
        self.require_lifetime()?;
        let actor = client.local_metadata_read_participant();
        if actor.node_id() != self.native.local_node_id() {
            return Err(data_error(
                "remote release observation changed actual local transport",
            ));
        }
        let remaining = parent
            .deadline()
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .filter(|d| !d.is_zero())
            .ok_or(BicDbError::QueryTimedOut)?;
        let business = Uuid::now_v7();
        let own = client.acquire_host_metadata_observation(leader, business, remaining, parent)?;
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &self.service,
            &own,
            business,
            parent,
        )?
        .ok_or_else(|| data_error("remote release observation actual native MStore absent"))?;
        // Real network/MStore acquisition precedes all owner/service guards.
        let actual = self.with_current_owner(|same, owner| {
            if !Arc::ptr_eq(same, &self.service) {
                return Err(data_error(
                    "remote release observation substituted actual service",
                ));
            }
            let service = same
                .try_lock()
                .ok_or_else(|| data_error("remote release observation service busy"))?;
            let actual = service.observe_joint_release_member_with_domain_and_point(
                &self.domain,
                own,
                business,
                self.native.clone(),
                owner,
                Some(point),
            )?;
            actual.require_original_writer_identity(&self.writer)?;
            if actual.activation().activation_sha256()? != self.activation.activation_sha256()? {
                return Err(data_error(
                    "remote release observation changed actual original activation",
                ));
            }
            Ok(actual)
        })?;
        let (term, index, context) = actual.committed().metadata_anchor();
        let state = self
            .state
            .try_lock()
            .ok_or_else(|| data_error("remote release preparation history busy"))?;
        let prepared = if let Some(custody) = &state.custody {
            custody.remote_release_vote(term, index, context)?
        } else {
            let prepared = if let Some(actual) = &state.evidence {
                &actual.prepared
            } else if let Some(actual) = &state.actual {
                &actual.prepared
            } else {
                &state
                    .history
                    .as_ref()
                    .ok_or_else(|| data_error("remote release original prepared history absent"))?
                    .prepared
            };
            crate::distribution_range_handoff::NativeJointPreparedServingVoteData {
                preparation_sha256: prepared.checksum_sha256().into(),
                native_database_incarnation: prepared.native_database_incarnation().into(),
                actual_local_visibility_sequence: prepared.actual_local_visibility_sequence(),
                metadata_term: term,
                metadata_read_index: index,
                metadata_context_id: context,
            }
        };
        drop(state);
        if prepared.preparation_sha256 != self.native.native_preparation_sha256()
            || prepared.native_database_incarnation != self.native.native_database_incarnation()
        {
            return Err(data_error(
                "remote release consumed preparation/native identity changed",
            ));
        }
        parent.check()?;
        self.require_lifetime()?;
        Ok(NativeRemoteReleaseObservation {
            owner: self.clone(),
            actual,
            vote: prepared,
        })
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn retain_original(
        custody: Arc<NativeJointPreparationCustody>,
        actual: &ActualServiceNativeJointPreparedEvidence,
        service: Arc<Mutex<ClusterDataNodeService>>,
        participant: &Arc<NativeRemoteNewParticipant>,
        process: Arc<crate::distribution_native_host::NativeRemotePreparationProcess>,
        native: Arc<crate::db::NativeJointServingBridgeReceipt>,
        activation: Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>,
        survivor: Option<Arc<crate::distribution_range_handoff::NativeJointHandoffRuntime>>,
    ) -> Result<Arc<Self>> {
        custody.require_actual_evidence(actual)?;
        actual.origin.require_actual(actual)?;
        actual.require_remote_preparation_participant(&participant.snapshot().checksum_sha256)?;
        process.require_memory()?;
        process.require_prepared(&actual.prepared)?;
        if native.data() != actual.committed.data()
            || native.native_preparation_sha256() != actual.prepared.checksum_sha256()
            || activation.native_bridge_sha256() != native.checksum_sha256()
            || activation.native().native_database_incarnation()
                != native.native_database_incarnation()
            || native.local_node_id() != &participant.snapshot().node_id
        {
            return Err(data_error(
                "remote release changed actual preparation/native/activation",
            ));
        }
        let frozen = actual.lease.try_frozen()?;
        match (&actual.lease, &survivor) {
            (NativeJointServingLease::Registered { owner, frozen }, None) => {
                if !Arc::ptr_eq(owner.service(), &service) {
                    return Err(data_error(
                        "remote release changed original Registered service",
                    ));
                }
                owner.require_original_release_freeze(frozen)?;
            }
            (NativeJointServingLease::Survivor(_), Some(runtime)) => {
                runtime.require_same_service(&service)?;
                runtime.with_current_release_owner(&native, |same, owner| {
                    if !Arc::ptr_eq(same, &service)
                        || !matches!(owner, NativeJointReleaseOwnerView::Frozen(held) if std::ptr::eq(held, frozen))
                    {
                        return Err(data_error("remote release changed original survivor runtime/Frozen"));
                    }
                    Ok(())
                })?;
            }
            _ => {
                return Err(data_error(
                    "remote FIRST release needs original Registered or survivor owner",
                ))
            }
        }
        let frozen_owner = frozen.owner();
        let original_custody = Arc::downgrade(&custody);
        let result = Arc::new(Self {
            service,
            participant: Arc::downgrade(participant),
            process,
            original_custody,
            domain: actual.writer_domain.clone(),
            writer: actual.writer.clone(),
            native,
            activation,
            survivor,
            frozen_owner,
            root: actual.catalogue.database_root().to_path_buf(),
            state: parking_lot::Mutex::new(ReleaseState {
                custody: Some(custody),
                evidence: None,
                actual: None,
                history: None,
                registered: None,
            }),
            outer_returns: parking_lot::Mutex::new(Default::default()),
            release_returns: parking_lot::Mutex::new(Default::default()),
            policy_returns: parking_lot::Mutex::new(Default::default()),
            policy_outer_returns: parking_lot::Mutex::new(Default::default()),
            #[cfg(test)]
            cancel_after_drop: parking_lot::Mutex::new(None),
            #[cfg(test)]
            drop_cut: parking_lot::Mutex::new(None),
        });
        result.require_lifetime()?;
        Ok(result)
    }
    /// Final-gate lifetime only: immutable binding was checked under the
    /// retained physical scope; never canonicalize paths or hash a range here.
    pub(crate) fn require_memory_lifetime(&self) -> Result<()> {
        self.process.require_memory()?;
        self.participant
            .upgrade()
            .ok_or_else(|| data_error("remote release original participant ended"))?
            .require_released_successor_memory(self)
    }
    /// Necessary sealed identity comparison only. It never checks an empty
    /// hook or constructs a successor from missing original custody.
    pub(crate) fn require_original_participant_identity(
        &self,
        participant: &NativeRemoteNewParticipant,
    ) -> Result<()> {
        self.process.require_memory()?;
        if self
            .participant
            .upgrade()
            .is_none_or(|p| !std::ptr::eq(p.as_ref(), participant))
        {
            return Err(data_error(
                "released successor changed actual original participant",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_original_manifest_identity(
        &self,
        manifest: &crate::distribution_range_image::NativeRemoteImageManifestV1,
    ) -> Result<()> {
        self.require_memory_lifetime()?;
        let participant = self
            .participant
            .upgrade()
            .ok_or_else(|| data_error("Policy original participant ended"))?;
        let original = participant.snapshot();
        if manifest.checksum_sha256 != original.binding.manifest_sha256
            || manifest.capture_range.id != original.binding.range_id
            || manifest.handoff_session != self.native.data().promise().session_id
            || manifest.nonce != self.native.data().promise().nonce
        {
            return Err(data_error(
                "Policy changed original retained manifest/session",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_lifetime(&self) -> Result<()> {
        self.require_memory_lifetime()?;
        self.domain.require_lifetime(
            &self.root,
            self.activation.store_open_id(),
            self.native.data().activation_range(),
        )
    }
    pub(crate) fn require_participant(
        &self,
        participant: &NativeRemoteNewParticipant,
    ) -> Result<()> {
        self.require_lifetime()?;
        if self
            .participant
            .upgrade()
            .is_none_or(|p| !std::ptr::eq(p.as_ref(), participant))
        {
            return Err(data_error(
                "remote release changed actual original participant",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_process(
        &self,
        process: &Arc<crate::distribution_native_host::NativeRemotePreparationProcess>,
    ) -> Result<()> {
        self.require_memory_lifetime()?;
        if !Arc::ptr_eq(process, &self.process) {
            return Err(data_error(
                "remote release replaced original actual host process",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_original_custody(
        &self,
        custody: &Arc<NativeJointPreparationCustody>,
    ) -> Result<()> {
        self.require_lifetime()?;
        if self
            .original_custody
            .upgrade()
            .is_none_or(|p| !Arc::ptr_eq(&p, custody))
        {
            return Err(data_error(
                "remote release changed original actual preparation custody",
            ));
        }
        Ok(())
    }
    pub(crate) fn native(&self) -> &Arc<crate::db::NativeJointServingBridgeReceipt> {
        &self.native
    }
    pub(crate) fn activation(
        &self,
    ) -> &Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt> {
        &self.activation
    }
    pub(crate) fn domain(&self) -> &crate::db::NativeJointWriterDomain {
        &self.domain
    }
    pub(crate) fn service(&self) -> &Arc<Mutex<ClusterDataNodeService>> {
        &self.service
    }
    pub(crate) fn frozen_owner(&self) -> Uuid {
        self.frozen_owner
    }

    /// Borrows the real current owner. It never clones Frozen or reconstructs
    /// a release owner from an empty slot. The full caller scope validates it.
    pub(crate) fn with_current_owner<T>(
        &self,
        operation: impl for<'a> FnOnce(
            &Arc<Mutex<ClusterDataNodeService>>,
            NativeJointReleaseOwnerView<'a>,
        ) -> Result<T>,
    ) -> Result<T> {
        self.require_lifetime()?;
        let state = self
            .state
            .try_lock()
            .ok_or_else(|| data_error("remote release state busy"))?;
        if let Some(custody) = &state.custody {
            return custody.with_original_remote_release_owner(&self.native, operation);
        }
        if let Some(evidence) = &state.evidence {
            return evidence.with_original_remote_release_owner(&self.service, operation);
        }
        if let Some(actual) = &state.actual {
            return actual.with_original_remote_release_owner(&self.service, operation);
        }
        let history = state
            .history
            .as_ref()
            .ok_or_else(|| data_error("remote release original transfer incomplete"))?;
        history.origin.require_session()?;
        if let Some(registered) = &state.registered {
            registered.with_current_release_owner(&self.native, operation)
        } else {
            self.survivor
                .as_ref()
                .ok_or_else(|| data_error("remote release original runtime absent"))?
                .with_current_release_owner(&self.native, operation)
        }
    }

    pub(crate) fn transfer_original_preparation(
        &self,
        admission: &crate::distribution_range_handoff::NativeJointReleaseAdmission,
    ) -> Result<()> {
        admission.require_remote_successor(self)?;
        self.require_lifetime()?;
        let mut state = self
            .state
            .try_lock()
            .ok_or_else(|| data_error("remote release transfer busy"))?;
        if state.history.is_some() {
            return Ok(());
        }
        if let Some(custody) = state.custody.take() {
            match custody.try_into_remote_release_parts(admission, self) {
                Ok((actual, service)) => {
                    // Same original service was checked before successor publication.
                    if !Arc::ptr_eq(&service, &self.service) {
                        state.evidence = Some(actual);
                        return Err(data_error(
                            "remote release transferred service identity changed",
                        ));
                    }
                    state.evidence = Some(actual);
                }
                Err((error, original)) => {
                    state.custody = Some(original);
                    return Err(error);
                }
            }
        }
        if let Some(evidence) = state.evidence.take() {
            match Arc::try_unwrap(evidence) {
                Ok(original) => state.actual = Some(original),
                Err(original) => {
                    state.evidence = Some(original);
                    return Err(data_error(
                        "remote release waits for SAME actual preparation evidence readers",
                    ));
                }
            }
        }
        let actual = state
            .actual
            .as_ref()
            .ok_or_else(|| data_error("remote release original evidence absent"))?;
        if !actual.read_pin_already_released()? {
            return Err(data_error(
                "remote release cannot drain a live preparation pin",
            ));
        }
        actual.require_remote_preparation_participant(
            &self
                .participant
                .upgrade()
                .ok_or_else(|| data_error("remote release participant ended"))?
                .snapshot()
                .checksum_sha256,
        )?;
        match &actual.lease {
            NativeJointServingLease::Registered { owner, frozen } => {
                // The ORIGINAL host owner retains the actual lease before the
                // redundant preparation reference can disappear.
                let released = owner.retain_release(frozen, self.activation.clone())?;
                state.registered = Some(released);
            }
            NativeJointServingLease::Survivor(_) if self.survivor.is_some() => {}
            _ => return Err(data_error("remote release original lease kind changed")),
        }
        let ActualServiceNativeJointPreparedEvidence {
            origin,
            committed,
            prepared,
            catalogue,
            writer,
            writer_domain,
            lease,
            pin,
        } = state.actual.take().unwrap();
        // Infallible one-way actual-object transfer. These are the consumed
        // original producer objects, not decoded vote/digest reconstruction.
        state.history = Some(LeaseFreeHistory {
            origin,
            prepared,
            _committed: committed,
            _catalogue: catalogue,
            _writer: writer,
            _domain: writer_domain,
            _pin: pin,
        });
        drop(lease); // Only the redundant clone; original release state remains.
        Ok(())
    }

    pub(crate) fn release_actual(
        &self,
        admission: &crate::distribution_range_handoff::NativeJointReleaseAdmission,
    ) -> Result<()> {
        self.release_returns.lock().require_not_exhausted()?;
        let result = self.release_actual_inner(admission);
        // Every actual API return, including a partial custody move, is
        // retained after its guards unwind and BEFORE outer postchecks.
        self.release_returns
            .lock()
            .record(result.is_ok(), result.as_ref().err());
        result
    }
    fn release_actual_inner(
        &self,
        admission: &crate::distribution_range_handoff::NativeJointReleaseAdmission,
    ) -> Result<()> {
        admission.require_remote_successor(self)?;
        self.transfer_original_preparation(admission)?;
        let mut state = self
            .state
            .try_lock()
            .ok_or_else(|| data_error("remote release original state busy"))?;
        let history = state
            .history
            .as_ref()
            .ok_or_else(|| data_error("remote release consumed history absent"))?;
        history.origin.require_session()?;
        if let Some(registered) = state.registered.as_mut() {
            #[cfg(test)]
            if let Some(token) = self.cancel_after_drop.lock().take() {
                return registered.test_release_with_fresh_phase_at_actual_drop(admission, || {
                    // SAME sink, after actual original A drop and before arm.
                    let now = Instant::now();
                    let windows = admission.test_remote_release_original_windows()?;
                    let server_left = token
                        .deadline()
                        .map(|d| d.saturating_duration_since(now).as_micros());
                    token.cancel();
                    *self.drop_cut.lock() = Some(serde_json::json!({
                        "actual_registered_after_drop_before_arm_fired":1,
                        "actual_original_frozen_owner":self.frozen_owner,
                        "original_server_remaining_us_before_cancel":server_left,
                        "actual_original_server_cancelled":token.is_cancel_requested(),
                        "windows_before_cancel":windows,"client_timeout_is_not_this_cut":true,
                    }));
                    Ok(())
                });
            }
            registered.release_with_fresh_phase(admission)
        } else {
            self.survivor
                .as_ref()
                .ok_or_else(|| data_error("remote release original runtime absent"))?
                .release_with_fresh_phase(admission, self.activation.clone())
        }
    }
    pub(crate) fn require_known_policy_origin(&self) -> Result<()> {
        self.require_lifetime()?;
        self.release_returns.lock().require_known_ok()
    }
    pub(crate) fn note_policy_outer_return(&self, ok: bool, error: Option<&BicDbError>) {
        self.policy_outer_returns.lock().record(ok, error);
    }
    pub(crate) fn note_policy_api_return(&self, ok: bool, error: Option<&BicDbError>) {
        self.policy_returns.lock().record(ok, error);
    }
    pub(crate) fn note_outer_return(&self, ok: bool, error: Option<&BicDbError>) {
        // Called after the serialized operation unwinds its physical guards.
        // No failure in diagnostic retention replaces the original API result.
        self.outer_returns.lock().record(ok, error);
    }
}

impl NativeRemoteReleaseObservation {
    pub(crate) fn into_admission_post(
        self,
        expected: &Arc<NativeRemoteReleaseOwner>,
    ) -> Result<ActualServiceNativeJointReleaseEvidence> {
        self.require_memory_current()?;
        if !Arc::ptr_eq(expected, &self.owner) {
            return Err(data_error(
                "remote release admission replaced actual observation owner",
            ));
        }
        Ok(self.actual)
    }
    pub(crate) fn owner(&self) -> &Arc<NativeRemoteReleaseOwner> {
        &self.owner
    }
    pub(crate) fn actual(&self) -> &ActualServiceNativeJointReleaseEvidence {
        &self.actual
    }
    pub(crate) fn metadata_anchor(&self) -> (u64, u64, Uuid) {
        self.actual.committed().metadata_anchor()
    }
    pub(crate) fn vote(
        &self,
    ) -> &crate::distribution_range_handoff::NativeJointPreparedServingVoteData {
        &self.vote
    }
    pub(crate) fn require_released_open(&self) -> Result<Uuid> {
        self.actual.require_released_open()
    }
    pub(crate) fn require_memory_current(&self) -> Result<()> {
        self.owner.require_memory_lifetime()?;
        self.actual.committed().require_publication_point_memory()
    }
    pub(crate) fn inspect<T>(
        &self,
        inspect: impl FnOnce(
            &crate::db::NativeJointHistoricalCutScope<'_>,
            &crate::distribution_range_consensus::RangeWriteStore,
            String,
            String,
            Uuid,
            bool,
        ) -> Result<T>,
        final_memory: impl FnOnce() -> Result<()>,
    ) -> Result<T> {
        self.require_memory_current()?;
        self.owner.with_current_owner(|same, owner| {
            let service = same
                .try_lock()
                .ok_or_else(|| data_error("remote release protected service busy"))?;
            let database = checked_database_read(&service.db, &service.storage_policy)?;
            service.require_native_catalog_database(&database)?;
            let scope = self.owner.native.historical_cut_scope(&database)?;
            self.actual
                .require_current_with_scope(&service, &scope, owner)?;
            let (head, activation, generation) = service
                .range_writes
                .observe_remote_release_catalogue(&scope, self.actual.activation())?;
            // Only this actual continuity proof can mark a released/armed
            // observation. A merely dropped or Frozen owner remains Open DATA.
            let released = self.actual.require_released_open().is_ok();
            let value = inspect(
                &scope,
                &service.range_writes,
                head,
                activation,
                self.owner.frozen_owner,
                released,
            )?;
            self.actual
                .require_current_with_scope(&service, &scope, owner)?;
            let version = self.actual.committed().native_publication_guard()?;
            self.require_memory_current()?;
            service
                .range_writes
                .require_current_catalogue_io_generation(generation)?;
            final_memory()?;
            drop(version);
            drop(scope);
            Ok(value)
        })
    }
}

impl ActualServiceNativeJointPreparedEvidence {
    pub(crate) fn with_original_remote_release_owner<T>(
        &self,
        service: &Arc<Mutex<ClusterDataNodeService>>,
        operation: impl for<'a> FnOnce(
            &Arc<Mutex<ClusterDataNodeService>>,
            NativeJointReleaseOwnerView<'a>,
        ) -> Result<T>,
    ) -> Result<T> {
        self.origin.require_actual(self)?;
        operation(
            service,
            NativeJointReleaseOwnerView::Frozen(self.lease.try_frozen()?),
        )
    }
}
impl ClusterDataNodeService {
    pub(crate) fn require_remote_release_request_scope(
        &self,
        request: &crate::distribution_native_host::NativeRemoteReleaseRequestAdmission,
        fresh: &CommittedNativeJointDecisionObservation,
        scope: &crate::db::NativeJointHistoricalCutScope<'_>,
    ) -> Result<()> {
        request.require_service(self)?;
        self.require_native_catalog_database(scope.database())?;
        request.require_scope(fresh, scope, &self.range_writes)
    }
}

#[cfg(test)]
impl NativeRemoteReleaseOwner {
    pub(crate) fn test_arm_original_drop_cancel(&self, token: CancellationToken) -> Result<()> {
        if self.survivor.is_some() || self.drop_cut.lock().is_some() {
            return Err(data_error(
                "drop cancel control needs untouched actual Registered release",
            ));
        }
        self.with_current_owner(|_, owner| {
            if !matches!(owner, NativeJointReleaseOwnerView::Frozen(_)) {
                return Err(data_error(
                    "drop cancel requires actual original Frozen before arm",
                ));
            }
            Ok(())
        })?;
        let mut pending = self.cancel_after_drop.lock();
        if pending.is_some() {
            return Err(data_error("actual drop cancel already armed"));
        }
        *pending = Some(token);
        Ok(())
    }
    pub(crate) fn test_state_data(&self) -> Result<serde_json::Value> {
        self.require_lifetime()?;
        let memory = {
            let state = self
                .state
                .try_lock()
                .ok_or_else(|| data_error("release diagnostic state busy"))?;
            serde_json::json!({"actual_custody_retained":state.custody.is_some(),
                "actual_evidence_retained":state.evidence.is_some(),"actual_owned_evidence_retained":state.actual.is_some(),
                "actual_lease_free_history_retained":state.history.is_some(),
                "actual_registered_release_owner_retained":state.registered.is_some(),
                "actual_survivor_runtime_retained":self.survivor.is_some(),
                "original_frozen_owner":self.frozen_owner})
        };
        let owner = self.with_current_owner(|_, owner| {
            Ok(match owner {
                NativeJointReleaseOwnerView::Frozen(f) => {
                    serde_json::json!({"kind":"actual_Frozen","owner":f.owner()})
                }
                NativeJointReleaseOwnerView::Dropped(a) => {
                    serde_json::json!({"kind":"actual_Dropped_with_original_activation",
                "activation_sha256":a.activation_sha256()?})
                }
            })
        });
        Ok(
            serde_json::json!({"memory":memory,"actual_owner_DATA":owner.as_ref().ok(),
            "actual_owner_error":owner.as_ref().err().map(ToString::to_string),
            "actual_release_API_returns":self.release_returns.lock().data(),
            "outer_API_returns":self.outer_returns.lock().data(),"policy_API_returns":self.policy_returns.lock().data(),"policy_outer_API_returns":self.policy_outer_returns.lock().data(),"actual_drop_cut":*self.drop_cut.lock(),
            "DATA_only_not_released_proof":true,"separate_snapshots_are_not_atomic":true}),
        )
    }
}
