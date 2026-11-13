//! Actual Registered native base -> catalogue floor, while CLOSED remains.
//! The original owner retains both exact catalogues before the first write.
use super::*;
use crate::db::{
    NativeInstalledRangeImageBase, NativeRegisteredRemoteFinalImageReceipt,
    NativeRegisteredRemoteMutationScope, RegisteredRemoteBaseBoundary,
};
use crate::distribution_native_host::{
    NativeRegisteredRemoteInstallPreflight, NativeRegisteredRemoteProposalPoint,
};
use crate::distribution_range_consensus::NativeRegisteredRemoteInstallingFence;
use std::sync::Arc;

/// Lexical references to the ORIGINAL retained native-base publication. This
/// has no DTO constructor, lifetime extension, writer or serving authority.
pub(crate) struct NativeRegisteredRemoteCataloguePoint<'a> {
    point: &'a NativeRegisteredRemoteProposalPoint<'a>,
    scope: &'a NativeRegisteredRemoteMutationScope<'a>,
    fence: &'a Arc<NativeRegisteredRemoteInstallingFence>,
    physical: &'a NativeInstalledRangeImageBase,
    boundary: &'a Arc<RegisteredRemoteBaseBoundary>,
    final_image: &'a NativeRegisteredRemoteFinalImageReceipt,
}
impl<'a> NativeRegisteredRemoteCataloguePoint<'a> {
    pub(crate) fn new(
        point: &'a NativeRegisteredRemoteProposalPoint<'a>,
        scope: &'a NativeRegisteredRemoteMutationScope<'a>,
        fence: &'a Arc<NativeRegisteredRemoteInstallingFence>,
        physical: &'a NativeInstalledRangeImageBase,
        boundary: &'a Arc<RegisteredRemoteBaseBoundary>,
        final_image: &'a NativeRegisteredRemoteFinalImageReceipt,
    ) -> Self {
        Self {
            point,
            scope,
            fence,
            physical,
            boundary,
            final_image,
        }
    }
    fn require_owner(&self, db: &crate::BicDb, store: &RangeWriteStore) -> Result<()> {
        self.point
            .require_original_mutation_owner(db, store, self.scope)?;
        self.boundary
            .require_boundary(db, self.scope, self.point.preflight().safety())?;
        self.physical.require_current(db, self.scope.native())?;
        self.final_image.require_installed_native_current(
            db,
            self.scope.native(),
            self.physical,
            self.point,
        )?;
        let proposal = db.read_native_registered_remote_proposal_for_mutation(
            store,
            self.scope,
            self.fence.protected_proposal(),
        )?;
        proposal.require_mutation_current(db, store, self.scope)?;
        self.fence
            .require_database_identity(db, self.point.preflight(), &proposal)?;
        let source = self.point.preflight().source().manifest();
        let crate::distribution_range_image::NativeRemoteImageSourcePayloadV1::Finished(seal) =
            self.point.frame().payload()?
        else {
            return Err(range_write_error(
                "Registered catalogue floor requires actual Finished frame",
            ));
        };
        let json = |value| crate::distribution_range_image::bounded_json(value, 4 * 1024 * 1024);
        if json(source)? != json(self.final_image.manifest())?
            || crate::distribution_range_image::bounded_json(seal, 4 * 1024 * 1024)?
                != crate::distribution_range_image::bounded_json(
                    self.final_image.source_seal(),
                    4 * 1024 * 1024,
                )?
            || self.final_image.proposal() != self.fence.protected_proposal()
            || self.physical.attempt_id() != self.scope.attempt_id()
            || self.physical.intent_sha256() != self.fence.intent_sha256()?
            || self.physical.capture_range() != &source.capture_range
            || self.physical.intended_activation_range() != &source.activation_range
            || self.physical.base_members() != &source.old_members
            || self.physical.intended_activation_members() != &source.new_members
            || self.physical.restore_activation() != source.restore.as_ref()
            || self.physical.cluster_id() != &source.cluster_id
            || self.physical.bootstrap_id() != source.bootstrap_id
            || self.physical.local_node_id() != &source.target_id
            || self.physical.session_id() != source.handoff_session
            || self.physical.nonce() != source.nonce
            || self.physical.range_index() != source.range_index
            || self.physical.history_verified_through() != source.range_index
            || self.physical.logical_genesis_sha256() != source.h0
            || self.physical.logical_history_sha256() != source.h
            || self.physical.source_native_receipt_sha256() != source.source_native_receipt
            || self.physical.image_root_sha256() != self.final_image.image_root_sha256()
            || self.physical.history_generation() != self.final_image.history_generation()
            || self.physical.image_generations() != self.final_image.image_generations()
            || self.physical.local_namespace_ids() != self.final_image.target_namespace_ids()
        {
            return Err(range_write_error(
                "Registered catalogue floor changed original native/source publication",
            ));
        }
        store.require_actual_abort_catalogue(db)?;
        self.point.require_live()
    }
    fn base(&self) -> Result<NativeImageCatalogueBase> {
        let p = self.physical;
        let base = NativeImageCatalogueBase {
            format_version: 1,
            cluster_id: p.cluster_id().clone(),
            node_id: p.local_node_id().clone(),
            bootstrap_id: p.bootstrap_id(),
            range_id: p.capture_range().id,
            capture_epoch: p.capture_range().epoch,
            range_index: p.range_index(),
            logical_genesis_sha256: p.logical_genesis_sha256().into(),
            logical_history_sha256: p.logical_history_sha256().into(),
            native_receipt_sha256: p.native_receipt_sha256().into(),
            native_birth_id: p.birth_id(),
            native_birth_sha256: p.birth_sha256().into(),
            attempt_id: p.attempt_id(),
            session_id: p.session_id(),
            nonce: p.nonce(),
            intent_sha256: p.intent_sha256().into(),
        };
        base.validate()?;
        Ok(base)
    }
}

/// Not serialized or reconstructed on cold open. Unknown file publication
/// outcomes retain this exact candidate, and only this sink can re-fsync it.
pub(crate) struct NativeRegisteredRemoteCataloguePublication {
    original: RangeWriteCatalog,
    candidate: RangeWriteCatalog,
    preflight: Arc<NativeRegisteredRemoteInstallPreflight>,
    fence: Arc<NativeRegisteredRemoteInstallingFence>,
    base: NativeImageCatalogueBase,
    store_open: Uuid,
    database_open: String,
    catalogue_id: Uuid,
    verified: bool,
}
impl NativeRegisteredRemoteCataloguePublication {
    pub(crate) fn require_request_catalogue(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        preflight: &NativeRegisteredRemoteInstallPreflight,
    ) -> Result<()> {
        preflight.require_original_process()?;
        preflight.native().require_current(db)?;
        db.validate_range_image_bindings(preflight.bindings())?;
        if !std::ptr::eq(Arc::as_ref(&self.preflight), preflight)
            || store.open_id != self.store_open
            || db.incarnation_id() != self.database_open
            || db.native_abort_catalogue_store_id()? != Some(self.catalogue_id)
            || db.data_path().canonicalize()? != store.root.canonicalize()?
            || !store.fsync
            || !store.catalogue_loaded_at_open
            || !crate::format::range_image_install_active(db.data_path())?
            || (store.catalog != self.original && store.catalog != self.candidate)
        {
            return Err(range_write_error(
                "Registered catalogue retry lost exact original/candidate owner",
            ));
        }
        store.require_actual_abort_catalogue(db)?;
        preflight.require_original_process()
    }
    pub(crate) fn begin(
        store: &RangeWriteStore,
        db: &crate::BicDb,
        point: &NativeRegisteredRemoteCataloguePoint<'_>,
    ) -> Result<Self> {
        point.require_owner(db, store)?;
        let proposal = db.read_native_registered_remote_proposal_for_mutation(
            store,
            point.scope,
            point.fence.protected_proposal(),
        )?;
        point
            .fence
            .require_catalogue_binding(store, db, point.point.preflight(), &proposal)?;
        // Start only from the complete CURRENT final-image proof. Historical
        // lower-bound integrity or a decoded pointer cannot begin adoption.
        let original_context = crate::db::NativeRegisteredRemoteImageContext::new(
            db,
            store,
            point.point,
            point.scope,
            point.fence,
            Some(point.boundary.clone()),
        )?;
        point.final_image.require_current(db, &original_context)?;
        store.validate_install_local_log(point.fence.range(), point.physical.range_index())?;
        let base = point.base()?;
        if !store.fsync
            || !store.catalogue_loaded_at_open
            || store.recovery_persistence_uncertain
            || !store
                .catalog
                .image_install_intents
                .contains_key(&base.range_id)
            || store
                .catalog
                .native_image_bases
                .contains_key(&base.range_id)
            || store.catalog.native_image_bases.len() >= MAX_NATIVE_BASES
        {
            return Err(range_write_error(
                "Registered catalogue floor requires durable original Installing without adoption",
            ));
        }
        let mut candidate = store.catalog.clone();
        let current_epoch = candidate
            .ranges
            .get(&base.range_id)
            .map_or(base.capture_epoch, |l| {
                l.current_epoch.max(base.capture_epoch)
            });
        candidate.ranges.insert(
            base.range_id,
            RangeWriteLog {
                current_epoch,
                last_index: base.range_index,
                resolved_through: base.range_index,
                compacted_through: base.range_index,
                entries: BTreeMap::new(),
            },
        );
        candidate
            .native_image_bases
            .insert(base.range_id, base.clone());
        candidate.format_version = candidate.format_version.max(NATIVE_IMAGE_CATALOG_FORMAT);
        RangeWriteEnvelope::new(candidate.clone())?.verify()?;
        point.require_owner(db, store)?;
        Ok(Self {
            original: store.catalog.clone(),
            candidate,
            preflight: point.point.preflight_arc().clone(),
            fence: point.fence.clone(),
            base,
            store_open: store.open_id,
            database_open: db.incarnation_id().into(),
            catalogue_id: point.point.preflight().catalogue().catalogue_id(),
            verified: false,
        })
    }
    fn require_point(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        point: &NativeRegisteredRemoteCataloguePoint<'_>,
    ) -> Result<()> {
        self.require_request_catalogue(store, db, point.point.preflight())?;
        point.require_owner(db, store)?;
        if !Arc::ptr_eq(&self.fence, point.fence) || self.base != point.base()? {
            return Err(range_write_error(
                "Registered catalogue floor changed retained native base/fence",
            ));
        }
        Ok(())
    }
    fn require_disk(&self, store: &RangeWriteStore) -> Result<()> {
        let file = open_range_catalog_no_follow(&store.root.join(DEFAULT_RANGE_WRITE_LOG))?;
        let mut bytes = Vec::new();
        file.take(64 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > 64 * 1024 * 1024
            || serde_json::from_slice::<RangeWriteEnvelope>(&bytes)?.verify()? != self.candidate
        {
            return Err(range_write_error(
                "Registered catalogue floor actual file differs from retained candidate",
            ));
        }
        Ok(())
    }
    pub(crate) fn advance(
        &mut self,
        store: &mut RangeWriteStore,
        db: &crate::BicDb,
        point: &NativeRegisteredRemoteCataloguePoint<'_>,
    ) -> Result<crate::db::NativeRegisteredRemoteImageProgress> {
        self.require_point(store, db, point)?;
        self.verified = false;
        // Retained original+candidate precede memory mutation and ALL I/O.
        store.catalog = self.candidate.clone();
        store.recovery_persistence_uncertain = true;
        let bytes = serde_json::to_vec_pretty(&RangeWriteEnvelope::new(self.candidate.clone())?)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(range_write_error("Registered catalogue floor byte bound"));
        }
        crate::storage::replace_durably(
            &store.root.join(DEFAULT_RANGE_WRITE_LOG),
            crate::storage::ReplaceContents::Plain(&bytes),
            true,
            "registered_remote_adoption.catalogue",
            crate::storage::FailClosed::No,
        )?;
        #[cfg(test)]
        cuts::fire();
        self.require_disk(store)?;
        self.require_point(store, db, point)?;
        store.recovery_persistence_uncertain = false;
        if let Err(error) = store
            .require_durable_recovery_catalog()
            .and_then(|()| db.require_native_writer_catalogue_custody(store))
            .and_then(|()| self.require_point(store, db, point))
        {
            store.recovery_persistence_uncertain = true;
            return Err(error);
        }
        self.verified = true;
        Ok(
            crate::db::NativeRegisteredRemoteImageProgress::CatalogueFloor {
                range_index: self.base.range_index,
                native_page_xid: point.physical.native_page_xid(),
                native_receipt_sha256: self.base.native_receipt_sha256.clone(),
            },
        )
    }
    #[cfg(test)]
    pub(crate) fn test_state(&self) -> serde_json::Value {
        serde_json::json!({"original":self.original,"candidate":self.candidate,"candidate_sha256":catalog_checksum(&self.candidate).ok(),"verified":self.verified,"native_base":self.base,"serving":false})
    }
}

#[cfg(test)]
mod cuts {
    use std::cell::RefCell;
    thread_local! {static CUT:RefCell<Option<Box<dyn FnOnce()>>>=RefCell::new(None);}
    pub(super) fn fire() {
        let f = CUT.with(|c| c.borrow_mut().take());
        if let Some(f) = f {
            f();
        }
    }
    pub(crate) struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            CUT.with(|c| {
                c.borrow_mut().take();
            });
        }
    }
    pub(crate) fn arm(f: Box<dyn FnOnce()>) -> Guard {
        CUT.with(|c| assert!(c.borrow_mut().replace(f).is_none()));
        Guard
    }
}
#[cfg(test)]
pub(crate) use cuts::arm as test_registered_remote_adoption_after_fsync;
