//! Sealed original-A Registered materialization. No legacy image-fence, local
//! OLD quorum or ordinary writer is manufactured from remote Manifest DATA.
use super::*;
use crate::db::{
    NativeInstalledRangeImageBase, NativeRegisteredReceiverHook,
    NativeRegisteredRemoteFinalImageReceipt,
};
use crate::distribution_range_consensus::{
    NativeRegisteredRemoteCatalogueMaterialization, NativeRegisteredRemoteCataloguePoint,
};
pub(crate) struct NativeRegisteredRemoteJointTarget {
    preflight: Arc<NativeRegisteredRemoteInstallPreflight>,
    base: NativeInstalledRangeImageBase,
    image: Arc<NativeRegisteredRemoteFinalImageReceipt>,
    catalogue: NativeRegisteredRemoteCatalogueMaterialization,
}
impl NativeRegisteredRemoteJointTarget {
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
            .validate_quiescent_frozen(self.base.capture_range().id, frozen)?;
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
        db.with_registered_joint_scope(self, frozen, parent, |scope| {
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
    ) -> Result<(
        NativeRegisteredRemoteJointTarget,
        RegisteredRemoteImageInstallFreeze,
    )> {
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
                            base,
                            image: final_image.clone(),
                            catalogue,
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
        Ok((target, lease))
    }
}
