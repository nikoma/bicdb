//! Correlation DATA for the two distinct authenticated hops. No decoded value
//! constructs local NEW, Role, Frozen, quorum, writer or Serving authority.
use super::*;
use crate::db::NativeRegisteredRemoteFinalImagePointer;

#[derive(Default)]
pub(super) struct RetainedExportRetirement {
    requests: BTreeMap<Uuid, Vec<u8>>,
    // Even a failed post-retention check keeps the exact actual observation.
    // No retry can overwrite or silently evict an uncertain attempt.
    observations: Vec<RetainedExportRetirementAttempt>,
    completed: Option<NativeSourceExportRetiredV1>,
}
struct RetainedExportRetirementAttempt {
    observation: crate::distribution_transport::AuthenticatedRegisteredExportNew,
    intended: NativeSourceExportRetiredV1,
    #[cfg(test)]
    export_read_pin: std::sync::Weak<crate::distribution_range_mutation::SourceCaptureReadPin>,
    #[cfg(test)]
    export_read_references_before: usize,
    #[cfg(test)]
    export_read_references_after_drop: Option<usize>,
}
#[cfg(test)]
impl NativeRemoteImageSourceRegistry {
    pub(crate) fn test_arm_export_detach_cut(
        &self,
        callback: Box<dyn FnOnce(Instant, &CancellationToken) + Send>,
    ) -> Result<()> {
        let mut slot = self
            .export_detach_cut
            .try_lock()
            .ok_or_else(|| invalid("export final-cut custody busy"))?;
        if slot.is_some() {
            return Err(invalid("export final cut already armed"));
        }
        *slot = Some(callback);
        Ok(())
    }
    fn test_run_export_detach_cut(&self, deadline: Instant) -> Result<()> {
        let callback = self
            .export_detach_cut
            .try_lock()
            .ok_or_else(|| invalid("export final-cut custody busy"))?
            .take();
        if let Some(callback) = callback {
            callback(
                deadline,
                self.original_process
                    .get()
                    .ok_or_else(|| invalid("original source process missing at test cut"))?,
            );
        }
        Ok(())
    }
    pub(crate) fn test_drop_next_export_reply(&self) {
        self.drop_next_export_reply
            .store(true, std::sync::atomic::Ordering::Release);
    }
    pub(crate) fn test_take_export_reply_loss(&self) -> bool {
        self.drop_next_export_reply
            .swap(false, std::sync::atomic::Ordering::AcqRel)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeSourceExportBindingV1 {
    pub cluster_id: ClusterId,
    pub bootstrap_id: Uuid,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub relocation_id: crate::distribution::RelocationId,
    pub plan_id: String,
    pub handoff_session: Uuid,
    pub nonce: Uuid,
    pub scope_sha256: String,
    pub source_id: ClusterNodeId,
    pub source_birth: NativeStoreBirthV1,
    pub target_id: ClusterNodeId,
    pub target_birth: NativeStoreBirthV1,
    pub source_session: Uuid,
    pub target_attempt: Uuid,
    pub manifest_sha256: String,
    pub old_quorum_sha256: String,
}
impl NativeSourceExportBindingV1 {
    pub(crate) fn from_manifest(m: &NativeRemoteImageManifestV1) -> Result<Self> {
        m.require_shape()?;
        let source = m
            .actual_old_closures
            .get(&m.source_id)
            .ok_or_else(|| invalid("export retirement actual source fact absent"))?;
        Ok(Self {
            cluster_id: m.cluster_id.clone(),
            bootstrap_id: m.bootstrap_id,
            range_id: m.capture_range.id,
            range_epoch: m.capture_range.epoch,
            relocation_id: m.relocation_id,
            plan_id: m.plan_id.clone(),
            handoff_session: m.handoff_session,
            nonce: m.nonce,
            scope_sha256: source.scope_sha256.clone(),
            source_id: m.source_id.clone(),
            source_birth: m.source_birth.clone(),
            target_id: m.target_id.clone(),
            target_birth: m.target_birth.clone(),
            source_session: m.source_session,
            target_attempt: m.target_attempt,
            manifest_sha256: m.checksum_sha256.clone(),
            old_quorum_sha256: m.old_quorum_sha256.clone(),
        })
    }
    pub(crate) fn require_manifest(&self, m: &NativeRemoteImageManifestV1) -> Result<()> {
        if self != &Self::from_manifest(m)? {
            return Err(invalid(
                "export retirement changed exact original MF/A/source session/birth",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_shape(&self) -> Result<()> {
        bounded_json(self, 32 * 1024)?;
        self.source_birth.validate_for(
            Some(self.bootstrap_id),
            self.source_birth.node_incarnation(),
        )?;
        self.target_birth.validate_for(
            Some(self.bootstrap_id),
            self.target_birth.node_incarnation(),
        )?;
        if self.bootstrap_id.is_nil()
            || self.range_id.get() == 0
            || self.range_epoch == 0
            || self.relocation_id.get() == 0
            || self.handoff_session.is_nil()
            || self.nonce.is_nil()
            || self.source_session.is_nil()
            || self.target_attempt.is_nil()
            || self.source_id == self.target_id
            || self.plan_id.is_empty()
            || self.plan_id.len() > 1024
            || [
                &self.scope_sha256,
                &self.manifest_sha256,
                &self.old_quorum_sha256,
            ]
            .iter()
            .any(|h| h.len() != 64 || hex::decode(h).is_err())
        {
            return Err(invalid("invalid exact bounded export-retirement binding"));
        }
        Ok(())
    }
    pub(crate) fn require_plan(
        &self,
        plan: &crate::distribution_range_handoff::NativeJointHandoffPlan,
    ) -> Result<()> {
        self.require_shape()?;
        let p = plan.promise();
        if self.scope_sha256 != plan.checksum_sha256()?
            || self.range_id != plan.old_range().id
            || self.range_epoch != plan.old_range().epoch
            || self.relocation_id != p.relocation_id
            || self.plan_id != p.plan_id
            || self.handoff_session != p.session_id
            || self.nonce != p.nonce
            || !plan.old_members().contains_key(&self.source_id)
            || plan.old_members().contains_key(&self.target_id)
            || !plan.new_members().contains_key(&self.target_id)
            || plan.enrolled_birth(&self.source_id)? != &self.source_birth
            || plan.enrolled_birth(&self.target_id)? != &self.target_birth
        {
            return Err(invalid(
                "export retirement differs from actual retained OLD plan",
            ));
        }
        Ok(())
    }
}
fn request_shape(
    version: u32,
    binding: &NativeSourceExportBindingV1,
    business: Uuid,
    challenge: Uuid,
    budget: u64,
) -> Result<()> {
    binding.require_shape()?;
    if version != 1 || business.is_nil() || challenge.is_nil() || !(1..=5000).contains(&budget) {
        return Err(invalid("invalid bounded export-retirement request"));
    }
    Ok(())
}
fn request_token(parent: &CancellationToken, budget: u64) -> Result<CancellationToken> {
    let token = parent.child_with_deadline(Instant::now() + Duration::from_millis(budget));
    token.check()?;
    Ok(token)
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeObserveRegisteredNewRequestV1 {
    pub version: u32,
    pub binding: NativeSourceExportBindingV1,
    pub business: Uuid,
    pub challenge: Uuid,
    pub budget_ms: u64,
}
impl NativeObserveRegisteredNewRequestV1 {
    pub(crate) fn require_shape(&self) -> Result<()> {
        request_shape(
            self.version,
            &self.binding,
            self.business,
            self.challenge,
            self.budget_ms,
        )
    }
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        self.require_shape()?;
        request_token(parent, self.budget_ms)
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum NativeSourceExportOperationV1 {
    Retire,
    Describe,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeSourceExportRequestV1 {
    pub version: u32,
    pub binding: NativeSourceExportBindingV1,
    pub operation: NativeSourceExportOperationV1,
    pub business: Uuid,
    pub challenge: Uuid,
    pub budget_ms: u64,
}
impl NativeSourceExportRequestV1 {
    pub(crate) fn from_manifest(
        m: &NativeRemoteImageManifestV1,
        operation: NativeSourceExportOperationV1,
        own: &HostMetadataReadObservation,
        business: Uuid,
        budget_ms: u64,
    ) -> Result<Self> {
        own.require_live_request(business)?;
        let r = Self {
            version: 1,
            binding: NativeSourceExportBindingV1::from_manifest(m)?,
            operation,
            business,
            challenge: Uuid::now_v7(),
            budget_ms,
        };
        r.require_shape()?;
        Ok(r)
    }
    pub(crate) fn require_shape(&self) -> Result<()> {
        request_shape(
            self.version,
            &self.binding,
            self.business,
            self.challenge,
            self.budget_ms,
        )
    }
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        self.require_shape()?;
        request_token(parent, self.budget_ms)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRegisteredNewExportReplyV1 {
    pub request: NativeObserveRegisteredNewRequestV1,
    pub materialized: NativeJointMaterializedFacts,
    pub target_database_open: String,
    pub target_store_open: Uuid,
    pub target_metadata_store: Uuid,
    pub final_image: NativeRegisteredRemoteFinalImagePointer,
    pub target_catalogue: Uuid,
    pub target_namespaces: Vec<(String, Uuid)>,
    pub image_generations: (Uuid, Uuid),
    pub record_count: u64,
    pub serialized_record_bytes: u64,
    pub checksum_sha256: String,
}
impl NativeRegisteredNewExportReplyV1 {
    pub(crate) fn checksum(&self) -> Result<String> {
        let mut x = self.clone();
        x.checksum_sha256.clear();
        Ok(hex::encode(Sha256::digest(bounded_json(
            &("registered-new-export-observation-v1", x),
            4 * 1024 * 1024,
        )?)))
    }
    pub(crate) fn require_manifest(&self, m: &NativeRemoteImageManifestV1) -> Result<()> {
        self.request.require_shape()?;
        self.request.binding.require_manifest(m)?;
        self.final_image.validate()?;
        let f = &self.materialized;
        if self.checksum_sha256 != self.checksum()?
            || self.target_database_open.is_empty()
            || self.target_store_open.is_nil()
            || self.target_metadata_store.is_nil()
            || self.target_catalogue.is_nil()
            || self.final_image.attempt_id != m.target_attempt
            || self.image_generations.0.is_nil()
            || self.image_generations.1.is_nil()
            || f.scope_sha256 != self.request.binding.scope_sha256
            || f.node_id != m.target_id
            || f.birth_id != m.target_birth.birth_id()
            || f.birth_sha256 != m.target_birth.birth_sha256()
            || f.native_receipt_sha256.len() != 64
            || f.range_index != m.range_index
            || f.logical_genesis_sha256 != m.h0
            || f.logical_history_sha256 != m.h
            || f.source_snapshot_db_sequence != Some(m.source_reserved_b)
            || f.image_root_sha256.as_deref().is_none_or(|h| h.len() != 64)
            || f.metadata_term == 0
            || f.metadata_context_id.is_nil()
            || self.target_namespaces.windows(2).any(|p| p[0].0 >= p[1].0)
            || self
                .target_namespaces
                .iter()
                .any(|(n, id)| n.is_empty() || id.is_nil())
        {
            return Err(invalid("actual Registered readonly NEW reply differs from original image/birth/native point"));
        }
        Ok(())
    }
}
/// Historical outcome DATA. Only the recipient's actual retained transition
/// constructs it; decoding cannot retire a pin or acknowledge a business write.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeSourceExportRetiredV1 {
    pub binding: NativeSourceExportBindingV1,
    pub target_observation_sha256: String,
    pub source_metadata_store: Uuid,
    pub source_metadata_term: u64,
    pub source_metadata_index: u64,
    pub source_metadata_context: Uuid,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeSourceExportReplyV1 {
    pub request: NativeSourceExportRequestV1,
    pub retired: NativeSourceExportRetiredV1,
    pub source_metadata_term: u64,
    pub source_metadata_index: u64,
    pub source_metadata_context: Uuid,
    pub checksum_sha256: String,
}
impl NativeSourceExportReplyV1 {
    pub(crate) fn checksum(&self) -> Result<String> {
        let mut x = self.clone();
        x.checksum_sha256.clear();
        Ok(hex::encode(Sha256::digest(bounded_json(
            &("source-export-retired-v1", x),
            64 * 1024,
        )?)))
    }
}

impl NativeRetainedRemoteImageSource {
    #[cfg(test)]
    pub(super) fn test_export_retirement_state(&self) -> Result<serde_json::Value> {
        let state = self
            .export_retirement
            .try_lock()
            .ok_or_else(|| invalid("export retirement diagnostic busy"))?;
        Ok(
            serde_json::json!({"completed":state.completed,"request_count":state.requests.len(),
            "observations":state.observations.iter().map(|x| serde_json::json!({"actual_reply":x.observation.reply(),"intended":x.intended,
                "exact_export_pin_references_before":x.export_read_references_before,
                "exact_export_pin_references_after_drop":x.export_read_references_after_drop,
                "exact_export_pin_references_now":x.export_read_pin.strong_count()})).collect::<Vec<_>>(),
            "old_fence_retirement":false,"serving_grant":false}),
        )
    }
    /// Read the ORIGINAL retained manifest without recreating or re-exporting
    /// a capture. After terminal retirement this is historical correlation,
    /// not permission to serve another image frame.
    pub(crate) fn export_retirement_manifest(&self) -> Result<NativeRemoteImageManifestV1> {
        self.registry.require_original_process()?;
        let source = self
            .session
            .try_lock()
            .ok_or_else(|| invalid("export manifest custody busy"))?;
        source
            .admission
            .require_listener(&source.service, &source.runtime)?;
        source
            .actual_open
            .as_ref()
            .ok_or_else(|| invalid("export actual open missing"))?
            .require_live()?;
        source
            .slot
            .as_ref()
            .ok_or_else(|| invalid("export actual slot missing"))?
            .require_live()?;
        source.old.require_current()?;
        let manifest = source.manifest()?.clone();
        self.registry.require_original_process()?;
        Ok(manifest)
    }

    pub(crate) fn retain_export_request(
        &self,
        request: &NativeSourceExportRequestV1,
    ) -> Result<Option<NativeSourceExportRetiredV1>> {
        request.require_shape()?;
        request
            .binding
            .require_manifest(&self.export_retirement_manifest()?)?;
        let bytes = bounded_json(request, 64 * 1024)?;
        let mut state = self
            .export_retirement
            .try_lock()
            .ok_or_else(|| invalid("export retirement request custody busy"))?;
        if let Some(old) = state.requests.get(&request.challenge) {
            if old != &bytes {
                return Err(invalid("export retirement challenge changed exact request"));
            }
        } else {
            if state.requests.len() >= 64 {
                return Err(invalid("export retirement request capacity; no eviction"));
            }
            state.requests.insert(request.challenge, bytes);
        }
        Ok(state.completed.clone())
    }

    /// The only remote side-effect sink. Fresh recipient authority and an
    /// independently fetched actual pinned NEW observation are both required.
    /// The source keeps ALL OLD/native fences and their shared underlying read
    /// pin after removing ONLY this completed export capture/reference.
    pub(crate) fn retire_export_from_remote_new(
        &self,
        request: &NativeSourceExportRequestV1,
        observation: crate::distribution_transport::AuthenticatedRegisteredExportNew,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<NativeSourceExportRetiredV1> {
        parent.check()?;
        self.registry.require_original_process()?;
        let mut source = self
            .session
            .try_lock()
            .ok_or_else(|| invalid("export callback still active"))?;
        source.require_source_current(own, business, parent)?;
        request.binding.require_manifest(source.manifest()?)?;
        let old = match &source.old {
            ActualSourceOld::Remote(old) => old,
            _ => {
                return Err(invalid(
                    "remote export retirement requires original remote OLD certificate",
                ))
            }
        };
        let seal = source
            .capture
            .as_ref()
            .ok_or_else(|| invalid("export capture absent before retirement"))?
            .completed_seal()?;
        observation.require_source(source.manifest()?, old, &seal)?;
        let mut state = self
            .export_retirement
            .try_lock()
            .ok_or_else(|| invalid("export retirement custody busy"))?;
        if state.completed.is_some() {
            return Err(invalid(
                "completed export cannot execute a second transition",
            ));
        }
        if state.observations.len() >= 8 {
            return Err(invalid(
                "export NEW observation custody capacity; no eviction",
            ));
        }
        let outcome = NativeSourceExportRetiredV1 {
            binding: request.binding.clone(),
            target_observation_sha256: observation.reply().checksum_sha256.clone(),
            source_metadata_store: source
                .metadata_store_id
                .ok_or_else(|| invalid("export original MStore absent"))?,
            source_metadata_term: own.term(),
            source_metadata_index: own.read_index(),
            source_metadata_context: own.context_id(),
        };
        // Retain the ACTUAL peer observation and exact decision identity before
        // the final live checks and before either irreversible pin action.
        state.observations.push(RetainedExportRetirementAttempt {
            observation,
            intended: outcome,
            #[cfg(test)]
            export_read_pin: Arc::downgrade(&source.capture.as_ref().unwrap().source_read_pin),
            #[cfg(test)]
            export_read_references_before: Arc::strong_count(
                &source.capture.as_ref().unwrap().source_read_pin,
            ),
            #[cfg(test)]
            export_read_references_after_drop: None,
        });
        let retained = state.observations.last().unwrap();
        retained
            .observation
            .require_source(source.manifest()?, old, &seal)?;
        let outcome = retained.intended.clone();
        let member = source.observe_source_current(own, business, parent)?;
        self.registry.require_original_process()?;
        parent.check()?;
        let terminal = outcome.clone();
        #[cfg(test)]
        self.registry.test_run_export_detach_cut(own.deadline())?;
        // This NONBLOCKING guard co-fences only the actual local metadata
        // version. It is not a distributed lease or foreign-liveness grant.
        // All native/MStore/DB/capture checks and allocations preceded it.
        let publication = member.publication_version().try_publication_guard()?;
        // Memory-only final refusal checks. The version guard already checks
        // the exact OWN request/deadline; original source-process ancestry is
        // a DIFFERENT cancellation owner and must not be omitted.
        parent.check()?;
        source.original_session.check()?;
        if Instant::now() >= own.deadline() {
            return Err(BicDbError::QueryTimedOut);
        }
        source.original_session.cancel();
        let capture = source.capture.take();
        state.completed = Some(terminal);
        drop(publication);
        // Terminal custody is installed before destructors or reply checks.
        drop(capture);
        #[cfg(test)]
        {
            let attempted = state.observations.last_mut().unwrap();
            attempted.export_read_references_after_drop =
                Some(attempted.export_read_pin.strong_count());
        }
        Ok(outcome)
    }
}
