//! Sealed original-A Registered materialization. No legacy image-fence, local
//! OLD quorum or ordinary writer is manufactured from remote Manifest DATA.
use super::*;
mod export_new;
mod first_data_policy;
mod remote_new;
pub(crate) use first_data_policy::NativeRegisteredServingDataPolicyOrigin;
mod release;
use crate::db::{
    NativeInstalledRangeImageBase, NativeRegisteredReceiverHook,
    NativeRegisteredRemoteFinalImageReceipt,
};
use crate::distribution_range_consensus::{
    NativeRegisteredRemoteCatalogueMaterialization, NativeRegisteredRemoteCataloguePoint,
};
pub(crate) use release::NativeRegisteredRemoteJointReleaseOwner;
use release::RegisteredJointReleaseState;
pub(crate) struct NativeRegisteredRemoteJointTarget {
    preflight: Arc<NativeRegisteredRemoteInstallPreflight>,
    database_root: std::path::PathBuf,
    base: NativeInstalledRangeImageBase,
    image: Arc<NativeRegisteredRemoteFinalImageReceipt>,
    catalogue: NativeRegisteredRemoteCatalogueMaterialization,
    readonly_plan: crate::distribution_range_handoff::NativeJointReadOnlyPlanIdentity,
}
/// Process-owned continuation custody, retained in the ORIGINAL receiver
/// before an API can return or later NEW/decision/phase work can fail.
/// The real runtime also retains A; no drop creates N or Serving.
pub(crate) struct NativeRegisteredRemoteJointOwner {
    remote_new: parking_lot::Mutex<
        Option<Arc<crate::distribution_range_handoff::NativeRemoteNewParticipant>>,
    >,
    export_new: parking_lot::Mutex<Option<export_new::RegisteredExportNewPin>>,
    decision_attempt: parking_lot::Mutex<
        Option<Arc<parking_lot::Mutex<crate::db::NativeRegisteredJointDecisionAttempt>>>,
    >,
    terminal_writer: std::sync::OnceLock<Arc<crate::db::NativeWriterHostState>>,
    release: parking_lot::Mutex<Option<RegisteredJointReleaseState>>,
    target: Arc<NativeRegisteredRemoteJointTarget>,
    service: Arc<parking_lot::Mutex<crate::distribution_data::ClusterDataNodeService>>,
    frozen: parking_lot::Mutex<
        Option<Arc<crate::distribution_range_image_install::NativeImageInstallFreeze>>,
    >,
}
impl NativeRegisteredRemoteJointOwner {
    pub(crate) fn retain_decision_attempt(
        &self,
        data: &crate::distribution_range_handoff::NativeJointDecisionData,
    ) -> Result<Arc<parking_lot::Mutex<crate::db::NativeRegisteredJointDecisionAttempt>>> {
        data.require_actual_native(self.target.native())?;
        let base = self.target.base();
        data.require_actual_local_point(
            self.target.native(),
            base.native_receipt_sha256(),
            base.range_index(),
            base.logical_genesis_sha256(),
            base.logical_history_sha256(),
            base.target_snapshot_bound().get(),
        )?;
        let mut retained = self
            .decision_attempt
            .try_lock()
            .ok_or_else(|| invalid("Registered original decision attempt custody busy"))?;
        if retained.is_none() {
            *retained = Some(Arc::new(parking_lot::Mutex::new(
                crate::db::NativeRegisteredJointDecisionAttempt::retain_expected(data)?,
            )));
        }
        Ok(retained.as_ref().unwrap().clone())
    }
    #[cfg(test)]
    pub(crate) fn test_decision_attempt_evidence(&self) -> Result<serde_json::Value> {
        let retained = self
            .decision_attempt
            .try_lock()
            .ok_or_else(|| invalid("Registered decision evidence custody busy"))?;
        match retained.as_ref() {
            Some(attempt) => attempt
                .try_lock()
                .ok_or_else(|| invalid("Registered decision evidence actual attempt busy"))?
                .test_evidence(),
            None => Ok(
                serde_json::json!({"native_attempts":[],"not_started":true,"data_not_authority":true}),
            ),
        }
    }
    /// Same actual host/receiver/DB/slot lifetime, never a finite phase token
    /// and never Role/OWN/serving authority. Each advance independently proves
    /// its own request, current metadata and exact original native candidate.
    pub(crate) fn require_first_policy_attempt_lifetime(&self) -> Result<()> {
        self.target.preflight.require_original_process()?;
        self.target.preflight.original.database.require_live()?;
        self.target.preflight.original.store.require_live()
    }
    pub(crate) fn terminal_writer(&self) -> Option<&Arc<crate::db::NativeWriterHostState>> {
        self.terminal_writer.get()
    }
    fn retain_terminal_writer(&self, state: Arc<crate::db::NativeWriterHostState>) -> Result<()> {
        if let Some(old) = self.terminal_writer.get() {
            if Arc::ptr_eq(old, &state) {
                return Ok(());
            }
            return Err(invalid(
                "Registered terminal policy cannot replace original actual writer State",
            ));
        }
        self.terminal_writer.set(state).map_err(|_| {
            invalid("Registered terminal policy publication raced another actual State")
        })
    }
    pub(crate) fn service(
        &self,
    ) -> &Arc<parking_lot::Mutex<crate::distribution_data::ClusterDataNodeService>> {
        &self.service
    }
    pub(crate) fn target(&self) -> &Arc<NativeRegisteredRemoteJointTarget> {
        &self.target
    }
    pub(crate) fn retained_freeze(
        &self,
    ) -> Result<Arc<crate::distribution_range_image_install::NativeImageInstallFreeze>> {
        let frozen = self
            .frozen
            .try_lock()
            .ok_or_else(|| invalid("Registered joint continuation custody busy"))?;
        let original = frozen.as_ref().ok_or_else(|| {
            invalid("Registered joint freeze moved to actual release continuation")
        })?;
        original.try_as_frozen()?;
        Ok(original.clone())
    }
}
impl NativeRegisteredRemoteJointTarget {
    pub(crate) fn require_original_preflight(
        &self,
        original: &Arc<NativeRegisteredRemoteInstallPreflight>,
    ) -> Result<()> {
        if !Arc::ptr_eq(original, &self.preflight) {
            return Err(invalid(
                "Registered joint release changed actual original preflight",
            ));
        }
        self.preflight.require_original_process()
    }
    pub(crate) fn database_root(&self) -> &std::path::Path {
        &self.database_root
    }
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.catalogue.store_open_id()
    }
    /// Necessary original same-open identity only. Current MQ and actual
    /// Frozen/dropped/Open state must be independently supplied by each phase.
    pub(crate) fn require_joint_identity(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.preflight.require_original_process()?;
        self.preflight.original.database.require_database(db)?;
        self.preflight.original.store.require_live()?;
        self.preflight.native().require_current(db)?;
        db.validate_range_image_bindings(self.preflight.bindings())?;
        self.base.require_current(db, self.preflight.native())?;
        self.catalogue.require_current(store, db, &self.base)
    }
    pub(crate) fn require_joint_lifetime(
        &self,
        root: &std::path::Path,
        store: Uuid,
        range: &crate::RangeDescriptor,
    ) -> Result<()> {
        self.preflight.require_original_process()?;
        if root.canonicalize()? != self.database_root
            || store != self.store_open_id()
            || crate::distribution_native_read::canonical_range(range)
                != *self.base.intended_activation_range()
        {
            return Err(invalid(
                "Registered joint phase changed original root/slot/NEW domain",
            ));
        }
        Ok(())
    }
    pub(crate) fn base(&self) -> &NativeInstalledRangeImageBase {
        &self.base
    }
    pub(crate) fn native(&self) -> &crate::db::NativeRecoveryAdmission {
        self.preflight.native()
    }
    pub(crate) fn source_seal(&self) -> &crate::distribution_range_image::LocalRangeImageSealV1 {
        self.image.source_seal()
    }
    pub(crate) fn manifest(&self) -> &crate::distribution_range_image::NativeRemoteImageManifestV1 {
        self.image.manifest()
    }
    pub(crate) fn original_hook(&self) -> &NativeRegisteredReceiverHook {
        &self.preflight.original.original_hook
    }
    pub(crate) fn require_original_point(
        &self,
        db: &BicDb,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.preflight.require_original_process()?;
        self.preflight.original.database.require_database(db)?;
        self.preflight.original.store.require_live()?;
        self.preflight.native().require_current(db)?;
        db.validate_range_image_bindings(self.preflight.bindings())?;
        if frozen.owner() != self.preflight.attempt_id()
            || self.base.attempt_id() != self.preflight.attempt_id()
            || self.base.capture_range().id != self.preflight.scope().range_id
            || self.image.target_database_open() != db.incarnation_id()
        {
            return Err(invalid(
                "Registered joint materialization changed original actual A/open",
            ));
        }
        db.range_mutation_gate()
            .validate_frozen(self.base.capture_range().id, frozen)?;
        self.base.require_current(db, self.preflight.native())?;
        let actual = db.read_registered_final_image_integrity(
            self.preflight.native(),
            &self.image.pointer(),
        )?;
        if actual.pointer() != self.image.pointer()
            || self.base.registered_final_image_pointer() != Some(&self.image.pointer())
        {
            return Err(invalid(
                "Registered joint materialization lost actual original final archive",
            ));
        }
        self.preflight.require_original_process()
    }
    pub(crate) fn require_materialized_point(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.require_original_point(db, frozen)?;
        self.catalogue.require_current(store, db, &self.base)?;
        self.preflight.original.store.require_live()?;
        Ok(())
    }
    pub(crate) fn require_current(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<()> {
        db.with_registered_joint_exclusion_scope(self, frozen, parent, |scope| {
            scope.require_current(db, store)
        })
    }
}
impl NativeMetadataControlHost {
    /// Consume the real A lease only AFTER actual complete native/profile2 and
    /// catalogue verification. The source/final DATA cannot call this alone.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn take_registered_remote_joint_target(
        &mut self,
        source: &Arc<NativeRemoteImageFact>,
        frame: &NativeRemoteImageStep<'_>,
        role: HandoffRoleAdmission,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<Arc<NativeRegisteredRemoteJointOwner>> {
        parent.check()?;
        if self.assigned.is_some() || self.data.is_some() {
            return Err(invalid(
                "Registered joint cannot replace existing DATA owner",
            ));
        }
        let receiver = self
            .replica
            .as_mut()
            .ok_or_else(|| invalid("actual original Registered receiver absent"))?;
        let range = role.role().current_range().id;
        // Acquire table custody BEFORE any A lease take; insertion after that
        // irreversible transfer stays infallible, as in the original path.
        let mut joint_targets = receiver
            .remote_joint_targets
            .try_lock()
            .ok_or_else(|| invalid("Registered joint owner table busy"))?;
        if joint_targets.contains_key(&range) {
            return Err(invalid(
                "Registered joint continuation already retained; use original actual owner",
            ));
        }
        let attempt = receiver
            .remote_attempts
            .get_mut(&range)
            .ok_or_else(|| invalid("original Registered A absent"))?;
        if !Arc::ptr_eq(source, attempt.preflight.source()) {
            return Err(invalid("Registered joint changed original actual MF Arc"));
        }
        let publication = attempt
            .catalogue_floor
            .as_ref()
            .ok_or_else(|| invalid("Registered joint requires actual catalogue publication"))?;
        let fence = attempt
            .installing
            .as_ref()
            .and_then(|p| p.fence())
            .cloned()
            .ok_or_else(|| invalid("Registered joint Installing owner absent"))?;
        let image = receiver
            .remote_images
            .get(&range)
            .ok_or_else(|| invalid("original Registered image owner absent"))?;
        let physical = image
            .verified_base()
            .ok_or_else(|| invalid("Registered joint requires actual verified native base"))?;
        let boundary = image
            .base_boundary()
            .ok_or_else(|| invalid("Registered joint original base boundary absent"))?;
        let final_image = image
            .final_image_receipt()
            .ok_or_else(|| invalid("Registered joint final image absent"))?;
        let lease = attempt
            .lease
            .as_ref()
            .ok_or_else(|| invalid("Registered original A lease already transferred"))?;
        lease.require_retained()?;
        let request = receiver.admission.observe_remote_catalogue_request(
            frame,
            role,
            own,
            business,
            lease.try_as_frozen()?,
            parent,
            publication,
            &attempt.preflight,
        )?;
        attempt.preflight.require_metadata_current(&request)?;
        let point = NativeRegisteredRemoteProposalPoint {
            preflight: &attempt.preflight,
            lease,
            request: &request,
            frame,
        };
        point.require_live()?;
        let readonly_plan = request.retain_readonly_plan_identity(source, frame)?;
        let target = {
            let service = receiver
                .backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("Registered joint actual service busy"))?;
            let database = service.database();
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Registered joint actual database busy"))?;
            db.with_registered_remote_mutation_exclusion(
                attempt.preflight.native(),
                &receiver.admission.original_hook,
                lease.try_as_frozen()?,
                attempt.preflight.attempt_id(),
                request.cancellation(),
                |scope| {
                    service.with_writer_data_domain_store(&db, |store| {
                        let actual = NativeRegisteredRemoteCataloguePoint::new(
                            &point,
                            scope,
                            &fence,
                            physical,
                            &boundary,
                            final_image,
                        );
                        let catalogue =
                            publication.seal_joint_materialization(store, &db, &actual)?;
                        let base = db
                            .native_installed_range_image_base(attempt.preflight.native(), range)?
                            .ok_or_else(|| {
                                invalid(
                                    "Registered actual native base disappeared at joint transfer",
                                )
                            })?;
                        if base.native_receipt_sha256() != physical.native_receipt_sha256() {
                            return Err(invalid(
                                "Registered joint actual base differs from original publication",
                            ));
                        }
                        Ok(NativeRegisteredRemoteJointTarget {
                            preflight: attempt.preflight.clone(),
                            database_root: db.data_path().canonicalize()?,
                            base,
                            image: final_image.clone(),
                            catalogue,
                            readonly_plan,
                        })
                    })
                },
            )?
        };
        attempt.preflight.require_metadata_current(&request)?;
        point.require_live()?;
        parent.check()?;
        drop(point);
        // Infallible ownership transfer after all postchecks. Dropping this
        // real lease still leaves runtime A custody CLOSED, never implicit N.
        let lease = attempt
            .lease
            .take()
            .expect("same original lease borrowed above");
        let owner = Arc::new(NativeRegisteredRemoteJointOwner {
            remote_new: parking_lot::Mutex::new(None),
            export_new: parking_lot::Mutex::new(None),
            decision_attempt: parking_lot::Mutex::new(None),
            terminal_writer: std::sync::OnceLock::new(),
            release: parking_lot::Mutex::new(None),
            target: Arc::new(target),
            service: receiver.backend.service.clone(),
            frozen: parking_lot::Mutex::new(Some(Arc::new(
                crate::distribution_range_image_install::NativeImageInstallFreeze::RegisteredRemote(
                    lease,
                ),
            ))),
        });
        // Infallible retention follows the infallible original-lease take.
        joint_targets.insert(range, owner.clone());
        drop(joint_targets);
        // Any following busy/error retains the complete original host owner.
        // The caller resumes that owner; no second lease/target is created.
        owner
            .service()
            .try_lock()
            .ok_or_else(|| {
                invalid("Registered joint Store identity busy; original host custody retained")
            })?
            .retain_registered_joint_store_identity(&owner)?;
        Ok(owner)
    }
}

impl NativeMetadataControlHost {
    pub(crate) fn registered_remote_joint_owner(
        &self,
        range: crate::RangeId,
    ) -> Result<Arc<NativeRegisteredRemoteJointOwner>> {
        let receiver = self
            .replica
            .as_ref()
            .ok_or_else(|| invalid("original actual Registered receiver absent"))?;
        let owner = {
            let table = receiver
                .remote_joint_targets
                .try_lock()
                .ok_or_else(|| invalid("Registered joint owner table busy"))?;
            table
                .get(&range)
                .cloned()
                .ok_or_else(|| invalid("original actual Registered joint continuation absent"))?
        };
        owner
            .service()
            .try_lock()
            .ok_or_else(|| invalid("Registered joint identity continuation service busy"))?
            .retain_registered_joint_store_identity(&owner)?;
        Ok(owner)
    }
}
