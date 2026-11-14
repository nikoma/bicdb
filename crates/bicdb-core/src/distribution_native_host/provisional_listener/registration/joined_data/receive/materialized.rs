//! Host-retained actual Joined physical+catalogue result. Original A remains
//! in that SAME host attempt and runtime until the later real lexical joint
//! transfer. This owner cannot mint INITIAL/FIRST/local OLD/Serving authority.
use super::*;
use crate::db::{
    NativeInstalledRangeImageBase, NativeJoinedRemoteFinalImageReceipt,
    NativeJoinedRemoteMutationScope, NativeWriterHostState,
};
use crate::distribution_range_consensus::{
    NativeJoinedRemoteCatalogueMaterialization, NativeJoinedRemoteCataloguePoint,
};
use crate::distribution_range_image::NativeRemoteImageStep;
use crate::distribution_range_mutation::{FrozenRangeMutationIdentity, FrozenRangeMutationPermit};
use crate::distribution_range_recovery::RangeRecoveryRuntime;

/// Only the actual host method below constructs/retains this sealed owner.
/// No Clone/serde/DTO constructor; Arc sharing retains the SAME original proof.
pub(crate) struct NativeJoinedRemoteMaterializedOwner {
    preflight: Arc<NativeJoinedRemoteInstallPreflight>,
    writer: Arc<NativeWriterHostState>,
    database_root: std::path::PathBuf,
    service: std::sync::Weak<Mutex<ClusterDataNodeService>>,
    runtime: std::sync::Weak<RangeRecoveryRuntime>,
    frozen: FrozenRangeMutationIdentity,
    base: NativeInstalledRangeImageBase,
    image: Arc<NativeJoinedRemoteFinalImageReceipt>,
    catalogue: NativeJoinedRemoteCatalogueMaterialization,
}
impl NativeJoinedRemoteMaterializedOwner {
    pub(crate) fn database_root(&self) -> &std::path::Path {
        &self.database_root
    }
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.catalogue.store_open_id()
    }
    /// Full lifetime/physical domain correlation outside final short guards.
    /// This is no current metadata lease or ordinary selected writer grant.
    pub(crate) fn require_joint_lifetime(
        &self,
        root: &std::path::Path,
        store: Uuid,
        range: &crate::RangeDescriptor,
    ) -> Result<()> {
        self.require_lifetime()?;
        if root.canonicalize()? != self.database_root
            || store != self.store_open_id()
            || crate::distribution_native_read::canonical_range(range)
                != *self.base.intended_activation_range()
        {
            return Err(invalid(
                "Joined joint phase changed original root/slot/activation domain",
            ));
        }
        Ok(())
    }
    /// Necessary memory-only host/open refusal; NOT request/metadata/Serving
    /// authority. The actual phase must retain its physical owners lexically.
    pub(crate) fn require_joint_lifetime_memory(&self) -> Result<()> {
        self.preflight.selected().origin().require_live_memory()?;
        if self.service.strong_count() == 0 || self.runtime.strong_count() == 0 {
            return Err(invalid(
                "Joined materialized original service/runtime ended",
            ));
        }
        Ok(())
    }
    pub(crate) fn preflight(&self) -> &Arc<NativeJoinedRemoteInstallPreflight> {
        &self.preflight
    }
    pub(crate) fn original_writer(&self) -> &Arc<NativeWriterHostState> {
        &self.writer
    }
    pub(crate) fn base(&self) -> &NativeInstalledRangeImageBase {
        &self.base
    }
    pub(crate) fn native(&self) -> &crate::db::NativeRecoveryAdmission {
        self.preflight.native()
    }
    pub(crate) fn image(&self) -> &Arc<NativeJoinedRemoteFinalImageReceipt> {
        &self.image
    }
    pub(crate) fn catalogue(&self) -> &NativeJoinedRemoteCatalogueMaterialization {
        &self.catalogue
    }
    pub(crate) fn manifest(&self) -> &crate::distribution_range_image::NativeRemoteImageManifestV1 {
        self.image.manifest()
    }
    pub(crate) fn source_seal(&self) -> &crate::distribution_range_image::LocalRangeImageSealV1 {
        self.image.source_seal()
    }
    pub(crate) fn readonly_plan(
        &self,
    ) -> &crate::distribution_range_handoff::NativeJointReadOnlyPlanIdentity {
        self.preflight.readonly_plan()
    }
    pub(crate) fn service(&self) -> Result<Arc<Mutex<ClusterDataNodeService>>> {
        self.require_lifetime()?;
        self.service
            .upgrade()
            .ok_or_else(|| invalid("Joined materialized original service ended"))
    }
    pub(crate) fn runtime(&self) -> Result<Arc<RangeRecoveryRuntime>> {
        self.require_lifetime()?;
        self.runtime
            .upgrade()
            .ok_or_else(|| invalid("Joined materialized original runtime ended"))
    }
    /// Actual host/open lifetime only; no snapshot Role, finite request renewal
    /// or positive metadata/APP/Strong authority can be obtained here.
    pub(crate) fn require_lifetime(&self) -> Result<()> {
        self.preflight.require_original_process()?;
        if self.service.strong_count() == 0 || self.runtime.strong_count() == 0 {
            return Err(invalid(
                "Joined materialized original service/runtime ended",
            ));
        }
        Ok(())
    }
    /// Exact physical identity only. Fresh joint Role/OWN/current-MStore phase
    /// authority and a real transferred A exclusion scope are SEPARATE inputs
    /// to the later joint consumer; old snapshot requests are not revived.
    pub(crate) fn require_joint_identity(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.require_lifetime()?;
        self.preflight
            .selected()
            .origin()
            .require_database_store(db, store)?;
        self.require_physical_identity(db, store)
    }
    /// Actual same-open hook borrow, not a Boolean skip or detached memo. A
    /// later joint exclusion scope can retain this point lexically alongside
    /// the real A/journal and independently fresh phase authority.
    pub(crate) fn require_joint_identity_with_writer_borrow(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        identity: &crate::db::NativeOriginalWriterIdentityBorrow<'_>,
    ) -> Result<()> {
        self.require_lifetime()?;
        self.preflight
            .selected()
            .origin()
            .require_database_store_borrowed(db, store, identity)?;
        if !Arc::ptr_eq(identity.writer(), &self.writer) {
            return Err(invalid(
                "Joined materialized borrowed identity changed original State",
            ));
        }
        self.require_physical_identity(db, store)
    }
    fn require_physical_identity(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.require_lifetime()?;
        self.preflight.native().require_current(db)?;
        db.validate_range_image_bindings(self.preflight.bindings())?;
        let writer = store.native_writer_host_state()?;
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("Joined materialized original service ended"))?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("Joined materialized original runtime ended"))?;
        if !Arc::ptr_eq(&writer, &self.writer)
            || !Arc::ptr_eq(&self.preflight.service()?, &service)
            || !Arc::ptr_eq(&self.preflight.selected().origin().runtime()?, &runtime)
            || self.base.attempt_id() != self.preflight.attempt_id()
            || self.base.capture_range().id != self.preflight.scope().range_id
            || self.base.joined_final_image_pointer() != Some(&self.image.pointer())
            || self.image.target_database_open() != db.incarnation_id()
            || self.image.target_store_open() != store.native_writer_store_open_id()
        {
            return Err(invalid(
                "Joined materialized changed actual State/service/runtime/open/native image",
            ));
        }
        self.base.require_current(db, self.native())?;
        let actual = db.read_joined_final_image_integrity(self.native(), &self.image.pointer())?;
        if actual.pointer() != self.image.pointer() {
            return Err(invalid(
                "Joined materialized original final publication changed",
            ));
        }
        self.catalogue.require_current(store, db, &self.base)?;
        self.require_lifetime()
    }
    /// Necessary ACTUAL opaque freeze identity, not a constructor for it.
    pub(crate) fn require_joint_point(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        actual_a: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.require_joint_identity(db, store)?;
        self.frozen.require_same_current(actual_a)?;
        db.range_mutation_gate()
            .validate_frozen(self.manifest().capture_range.id, actual_a)
    }
    pub(crate) fn require_joint_point_with_writer_borrow(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        actual_a: &FrozenRangeMutationPermit,
        identity: &crate::db::NativeOriginalWriterIdentityBorrow<'_>,
    ) -> Result<()> {
        self.require_joint_identity_with_writer_borrow(db, store, identity)?;
        self.frozen.require_same_current(actual_a)?;
        db.range_mutation_gate()
            .validate_frozen(self.manifest().capture_range.id, actual_a)
    }
    pub(crate) fn require_current(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        scope: &NativeJoinedRemoteMutationScope<'_>,
    ) -> Result<()> {
        scope.require_store(db, store)?;
        if !Arc::ptr_eq(scope.preflight(), &self.preflight) {
            return Err(invalid(
                "Joined materialized current changed original attempt",
            ));
        }
        self.require_joint_point_with_writer_borrow(
            db,
            store,
            scope.frozen()?,
            scope.original_writer_identity(),
        )?;
        scope.require_store(db, store)
    }
    /// Runtime retention is checked outside service/DB/native guards. The
    /// original lease is borrowed, never copied or extracted from DATA.
    pub(crate) fn require_original_lease(
        &self,
        lease: &crate::distribution_range_recovery::JoinedRemoteImageInstallFreeze,
    ) -> Result<()> {
        self.require_lifetime()?;
        if !Arc::ptr_eq(lease.preflight(), &self.preflight) {
            return Err(invalid("Joined materialized changed original A lease"));
        }
        lease.require_retained()?;
        self.frozen.require_same_current(lease.try_as_frozen()?)
    }
}
impl NativeMetadataControlHost {
    pub(crate) fn materialize_joined_remote_image(
        &mut self,
        request: &NativeJoinedRangeReceiveRequest,
        frame: &NativeRemoteImageStep<'_>,
        parent: &CancellationToken,
    ) -> Result<Arc<NativeJoinedRemoteMaterializedOwner>> {
        parent.check()?;
        let host = self.joined_receive_host_mut()?;
        request.origin().require_host(host)?;
        let backend = host.parts.backend.clone();
        let factory = host.parts.factory.clone();
        let runtime = host.parts.runtime.clone();
        let attempt = host
            .remote_attempts
            .get_mut(&request.origin().range_id())
            .ok_or_else(|| invalid("Joined materialization original attempt absent"))?;
        let preflight = attempt.preflight.clone();
        let lease = attempt
            .lease
            .as_ref()
            .ok_or_else(|| invalid("Joined materialization original A absent"))?;
        lease.require_retained()?;
        preflight.require_metadata_current(request)?;
        let fence = attempt
            .installing
            .as_ref()
            .and_then(|p| p.fence())
            .cloned()
            .ok_or_else(|| invalid("Joined materialization original Installing absent"))?;
        let publication = attempt
            .catalogue
            .as_ref()
            .ok_or_else(|| invalid("Joined materialization actual catalogue publication absent"))?;
        let image = attempt
            .image
            .as_ref()
            .ok_or_else(|| invalid("Joined materialization original image absent"))?;
        let physical = image
            .verified_base()
            .ok_or_else(|| invalid("Joined materialization original native base absent"))?;
        let boundary = image
            .base_boundary()
            .ok_or_else(|| invalid("Joined materialization original native attempt absent"))?;
        let final_image = image
            .final_image_receipt()
            .ok_or_else(|| invalid("Joined materialization actual final image absent"))?;
        // Borrow destination before physical work and final guards; the SAME
        // attempt still independently owns the original A lease throughout.
        let retained = &mut attempt.materialized;
        let point = NativeJoinedRemoteImagePoint {
            preflight: &preflight,
            lease,
            request,
            frame,
        };
        point.require_live()?;
        let result = {
            let _original = preflight.selected().origin().original()?;
            let _admission = preflight.selected().origin().admission()?;
            let service = backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("Joined materialization actual service busy"))?;
            let database = service.database();
            if !Arc::ptr_eq(&database, &factory.database) {
                return Err(invalid("Joined materialization changed actual database"));
            }
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Joined materialization actual DB busy"))?;
            db.with_joined_remote_mutation_scope(&preflight, request, frame, lease, |scope| {
                service.with_writer_data_domain_store(&db, |store| {
                    let proposal = db.read_native_joined_remote_proposal_for_mutation(
                        store,
                        scope,
                        fence.protected_proposal(),
                    )?;
                    let actual = NativeJoinedRemoteCataloguePoint::new(
                        &point,
                        scope,
                        &fence,
                        physical,
                        boundary,
                        final_image,
                        &proposal,
                    );
                    let catalogue = publication.seal_materialization(store, &db, &actual)?;
                    let candidate = if let Some(old) = retained.as_ref() {
                        old.clone()
                    } else {
                        let base = db
                            .native_installed_range_image_base(
                                preflight.native(),
                                preflight.scope().range_id,
                            )?
                            .ok_or_else(|| {
                                invalid("Joined materialization actual base disappeared")
                            })?;
                        if base.native_receipt_sha256() != physical.native_receipt_sha256() {
                            return Err(invalid(
                                "Joined materialization base differs from original attempt",
                            ));
                        }
                        Arc::new(NativeJoinedRemoteMaterializedOwner {
                            preflight: preflight.clone(),
                            writer: scope.require_original_writer(&db, store)?,
                            database_root: db.data_path().canonicalize()?,
                            service: Arc::downgrade(&backend.service),
                            runtime: Arc::downgrade(&runtime),
                            frozen: scope.frozen()?.observe_identity()?,
                            base,
                            image: final_image.clone(),
                            catalogue,
                        })
                    };
                    candidate.require_current(&db, store, scope)?;
                    let identity = scope.frozen()?.observe_identity()?;
                    let gate = scope.frozen()?.try_current_publication()?;
                    let version = request
                        .member()
                        .publication_version()
                        .try_publication_guard()?;
                    request.require_request_memory()?;
                    frame.require_request_live()?;
                    parent.check()?;
                    identity.require_in_current_publication(&gate)?;
                    db.require_joined_publication_storage_health()?;
                    // Infallible host custody before any later fallible
                    // postcheck/API return. No lease take, new A or writer.
                    let old = if retained.is_none() {
                        retained.replace(candidate.clone())
                    } else {
                        None
                    };
                    drop(version);
                    drop(gate);
                    drop(old);
                    Ok(candidate)
                })
            })
        };
        let post = preflight
            .require_metadata_current(request)
            .and_then(|()| lease.require_retained())
            .and_then(|()| point.require_live())
            .and_then(|()| parent.check());
        match result {
            Err(primary) => {
                let _ = post;
                Err(primary)
            }
            Ok(owner) => {
                post?;
                Ok(owner)
            }
        }
    }
}
