//! Original Joined selected catalogue adoption; no FIRST/local-source casts.
//! Exact pending candidates survive uncertainty. A known-success continuation
//! follows only selected obligations and current disk/memory, never rewrites an
//! old whole-catalogue snapshot over unrelated progress.
use super::*;
use crate::db::{
    JoinedRemoteBaseBoundary, NativeInstalledRangeImageBase, NativeJoinedRemoteFinalImageReceipt,
    NativeJoinedRemoteMutationScope,
};
use crate::distribution_native_host::{
    NativeJoinedRemoteImagePoint, NativeJoinedRemoteInstallPreflight,
};
use crate::distribution_range_consensus::NativeJoinedRemoteInstallingFence;
use std::sync::Arc;

/// Actual original host/A/native point. Constructor consumes no DATA authority.
pub(crate) struct NativeJoinedRemoteCataloguePoint<'a, 'scope> {
    point: &'a NativeJoinedRemoteImagePoint<'scope>,
    scope: &'a NativeJoinedRemoteMutationScope<'scope>,
    fence: &'a Arc<NativeJoinedRemoteInstallingFence>,
    physical: &'a NativeInstalledRangeImageBase,
    boundary: &'a Arc<JoinedRemoteBaseBoundary>,
    final_image: &'a NativeJoinedRemoteFinalImageReceipt,
    proposal: &'a crate::db::NativeJoinedRemoteProposalReceipt,
}
impl<'a, 'scope> NativeJoinedRemoteCataloguePoint<'a, 'scope> {
    pub(crate) fn new(
        point: &'a NativeJoinedRemoteImagePoint<'scope>,
        scope: &'a NativeJoinedRemoteMutationScope<'scope>,
        fence: &'a Arc<NativeJoinedRemoteInstallingFence>,
        physical: &'a NativeInstalledRangeImageBase,
        boundary: &'a Arc<JoinedRemoteBaseBoundary>,
        final_image: &'a NativeJoinedRemoteFinalImageReceipt,
        proposal: &'a crate::db::NativeJoinedRemoteProposalReceipt,
    ) -> Self {
        Self {
            point,
            scope,
            fence,
            physical,
            boundary,
            final_image,
            proposal,
        }
    }
    fn require_owner(&self, db: &crate::BicDb, store: &RangeWriteStore) -> Result<()> {
        self.point.require_live()?;
        self.scope.require_store_identity(db, store)?;
        if !std::ptr::eq(self.point.preflight(), self.scope.preflight().as_ref()) {
            return Err(range_write_error(
                "Joined catalogue changed original host preflight",
            ));
        }
        self.boundary.require_boundary(db, self.scope)?;
        self.physical.require_current(db, self.scope.native())?;
        self.final_image
            .require_catalogue_current(db, self.scope, self.point, self.physical)?;
        self.proposal
            .require_catalogue_identity(db, store, self.scope)?;
        self.fence
            .require_catalogue_identity(store, db, self.scope, self.proposal)?;
        let source = self.point.preflight().source().manifest();
        if self.final_image.proposal() != self.fence.protected_proposal()
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
            || self.physical.image_root_sha256() != self.final_image.source_seal().image_root_sha256
            || self.physical.history_generation() != self.final_image.history_generation()
            || self.physical.image_generations() != self.final_image.image_generations()
            || self.physical.local_namespace_ids() != self.final_image.target_namespace_ids()
        {
            return Err(range_write_error(
                "Joined catalogue changed original native/source final publication",
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
#[derive(serde::Serialize)]
struct CatalogueWriteAttempt {
    role_context: Uuid,
    own_context: Uuid,
    metadata_store: Uuid,
    replace_returned: bool,
    readback_exact: bool,
    error: Option<String>,
}
pub(crate) struct NativeJoinedRemoteCataloguePublication {
    original: RangeWriteCatalog,
    candidate: RangeWriteCatalog,
    bytes: Vec<u8>,
    preflight: Arc<NativeJoinedRemoteInstallPreflight>,
    fence: Arc<NativeJoinedRemoteInstallingFence>,
    base: NativeImageCatalogueBase,
    store_open: Uuid,
    database_open: String,
    catalogue_id: Uuid,
    verified: bool,
    writes_started: u64,
    durable_replace_returned: u64,
    last_error: Option<String>,
    attempts: Vec<CatalogueWriteAttempt>,
}
impl NativeJoinedRemoteCataloguePublication {
    fn require_selected(&self, store: &RangeWriteStore) -> Result<()> {
        let range = self.base.range_id;
        if store.catalog.image_install_intents.get(&range)
            != self.candidate.image_install_intents.get(&range)
            || store.catalog.native_image_bases.get(&range) != Some(&self.base)
            || store.catalog.ranges.get(&range) != self.candidate.ranges.get(&range)
        {
            return Err(range_write_error(
                "Joined adopted selected catalogue intent/base/log changed",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_request_catalogue(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        preflight: &NativeJoinedRemoteInstallPreflight,
    ) -> Result<()> {
        preflight.require_original_process()?;
        preflight.native().require_current(db)?;
        db.validate_range_image_bindings(preflight.bindings())?;
        if !std::ptr::eq(self.preflight.as_ref(), preflight)
            || store.open_id != self.store_open
            || db.incarnation_id() != self.database_open
            || db.native_abort_catalogue_store_id()? != Some(self.catalogue_id)
            || db.data_path().canonicalize()? != store.root.canonicalize()?
            || !store.fsync
            || !store.catalogue_loaded_at_open
        {
            return Err(range_write_error(
                "Joined catalogue original actual owner/open/root changed",
            ));
        }
        if self.verified {
            if store.recovery_persistence_uncertain {
                return Err(range_write_error(
                    "Joined known catalogue acquired later persistence uncertainty",
                ));
            }
            self.require_selected(store)?;
            self.require_current_disk(store)?;
        } else if store.catalog != self.original && store.catalog != self.candidate {
            return Err(range_write_error(
                "Joined uncertain catalogue no longer exact original/candidate; refusing overwrite",
            ));
        }
        store.require_actual_abort_catalogue(db)?;
        preflight.require_original_process()
    }
    pub(crate) fn begin(
        store: &RangeWriteStore,
        db: &crate::BicDb,
        point: &NativeJoinedRemoteCataloguePoint<'_, '_>,
    ) -> Result<Self> {
        point.require_owner(db, store)?;
        point.scope.require_store(db, store)?; // genuine durable initial entry only.
        store.validate_install_local_log(point.fence.range(), point.physical.range_index())?;
        let base = point.base()?;
        if store.recovery_persistence_uncertain
            || store
                .catalog
                .native_image_bases
                .contains_key(&base.range_id)
            || !store
                .catalog
                .image_install_intents
                .contains_key(&base.range_id)
            || store.catalog.native_image_bases.len() >= MAX_NATIVE_BASES
        {
            return Err(range_write_error("Joined initial adoption requires durable original Installing without selected floor"));
        }
        let mut candidate = store.catalog.clone();
        let current_epoch = candidate
            .ranges
            .get(&base.range_id)
            .map_or(base.capture_epoch, |log| {
                log.current_epoch.max(base.capture_epoch)
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
        let envelope = RangeWriteEnvelope::new(candidate.clone())?;
        envelope.clone().verify()?;
        let bytes = serde_json::to_vec_pretty(&envelope)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(range_write_error(
                "Joined catalogue exact candidate byte bound exceeded",
            ));
        }
        point.require_owner(db, store)?;
        Ok(Self {
            original: store.catalog.clone(),
            candidate,
            bytes,
            preflight: point.point.preflight_arc().clone(),
            fence: point.fence.clone(),
            base,
            store_open: store.open_id,
            database_open: db.incarnation_id().into(),
            catalogue_id: point
                .point
                .preflight()
                .selected()
                .catalogue()
                .catalogue_id(),
            verified: false,
            writes_started: 0,
            durable_replace_returned: 0,
            last_error: None,
            attempts: Vec::new(),
        })
    }
    fn require_point(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        point: &NativeJoinedRemoteCataloguePoint<'_, '_>,
    ) -> Result<()> {
        self.require_request_catalogue(store, db, point.point.preflight())?;
        point.require_owner(db, store)?;
        if !Arc::ptr_eq(&self.fence, point.fence) || self.base != point.base()? {
            return Err(range_write_error(
                "Joined catalogue changed exact retained base/fence",
            ));
        }
        Ok(())
    }
    fn read_disk_full(store: &RangeWriteStore) -> Result<(RangeWriteCatalog, Vec<u8>)> {
        let file = open_range_catalog_no_follow(&store.root.join(DEFAULT_RANGE_WRITE_LOG))?;
        if file.metadata()?.len() > 64 * 1024 * 1024 {
            return Err(range_write_error(
                "Joined catalogue disk byte bound exceeded",
            ));
        }
        let mut bytes = Vec::new();
        file.take(64 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(range_write_error("Joined catalogue grew beyond disk bound"));
        }
        let actual = serde_json::from_slice::<RangeWriteEnvelope>(&bytes)?.verify()?;
        Ok((actual, bytes))
    }
    fn read_disk(store: &RangeWriteStore) -> Result<RangeWriteCatalog> {
        Ok(Self::read_disk_full(store)?.0)
    }
    fn require_current_disk(&self, store: &RangeWriteStore) -> Result<()> {
        if Self::read_disk(store)? != store.catalog {
            return Err(range_write_error(
                "Joined catalogue current disk/memory differ",
            ));
        }
        Ok(())
    }
    pub(crate) fn advance(
        &mut self,
        store: &mut RangeWriteStore,
        db: &crate::BicDb,
        point: &NativeJoinedRemoteCataloguePoint<'_, '_>,
    ) -> Result<crate::db::NativeJoinedRemoteImageProgress> {
        self.require_point(store, db, point)?;
        let result = (|| -> Result<()> {
            if !self.verified {
                // Exact whole candidate and encoded bytes were host-retained
                // BEFORE memory mutation/IO. Uncertainty never resets them.
                if self.attempts.len() >= 8 {
                    return Err(range_write_error("Joined catalogue write-attempt evidence capacity exhausted; same candidate retained/no eviction"));
                }
                self.attempts.push(CatalogueWriteAttempt {
                    role_context: point.point.request().role().role().context_id(),
                    own_context: point.point.request().own().context_id(),
                    metadata_store: point.point.request().member().store_id(),
                    replace_returned: false,
                    readback_exact: false,
                    error: None,
                });
                self.writes_started = self
                    .writes_started
                    .checked_add(1)
                    .ok_or_else(|| range_write_error("Joined adoption write count exhausted"))?;
                store.catalog = self.candidate.clone();
                store.recovery_persistence_uncertain = true;
                crate::storage::replace_durably(
                    &store.root.join(DEFAULT_RANGE_WRITE_LOG),
                    crate::storage::ReplaceContents::Plain(&self.bytes),
                    true,
                    "joined_remote_adoption.catalogue",
                    crate::storage::FailClosed::No,
                )?;
                self.attempts.last_mut().unwrap().replace_returned = true;
                self.durable_replace_returned = self
                    .durable_replace_returned
                    .checked_add(1)
                    .ok_or_else(|| range_write_error("Joined adoption sync count exhausted"))?;
                #[cfg(test)]
                cuts::fire();
                let (readback, actual_bytes) = Self::read_disk_full(store)?;
                if readback != self.candidate || actual_bytes != self.bytes {
                    return Err(range_write_error(
                        "Joined adoption readback differs from original exact candidate",
                    ));
                }
                self.attempts.last_mut().unwrap().readback_exact = true;
                self.require_point(store, db, point)?;
            } else {
                // Known-success: NEVER write retained whole candidate again.
                self.require_selected(store)?;
                self.require_current_disk(store)?;
            }
            self.require_selected(store)?;
            point.require_owner(db, store)?;
            let identity = point.scope.frozen()?.observe_identity()?;
            // All IO/allocations/readback preceded these short memory guards.
            let verified = &mut self.verified;
            let uncertain = &mut store.recovery_persistence_uncertain;
            let gate = point.scope.frozen()?.try_current_publication()?;
            let version = point
                .point
                .request()
                .member()
                .publication_version()
                .try_publication_guard()?;
            point.point.request().require_request_memory()?;
            point.point.frame().require_request_live()?;
            identity.require_in_current_publication(&gate)?;
            db.require_joined_publication_storage_health()?;
            *uncertain = false;
            *verified = true;
            drop(version);
            drop(gate);
            Ok(())
        })();
        if let Err(error) = &result {
            self.last_error = Some(format!("{error:?}").chars().take(4096).collect());
            if let Some(last) = self.attempts.last_mut() {
                if last.error.is_none() {
                    last.error = self.last_error.clone();
                }
            }
        }
        result?;
        store.require_durable_recovery_catalog()?;
        db.require_native_writer_catalogue_custody(store)?;
        Ok(crate::db::NativeJoinedRemoteImageProgress::CatalogueFloor {
            range_index: self.base.range_index,
            native_page_xid: point.physical.native_page_xid(),
            native_receipt_sha256: self.base.native_receipt_sha256.clone(),
        })
    }
    pub(crate) fn seal_materialization(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        point: &NativeJoinedRemoteCataloguePoint<'_, '_>,
    ) -> Result<NativeJoinedRemoteCatalogueMaterialization> {
        self.require_point(store, db, point)?;
        if !self.verified || store.recovery_persistence_uncertain {
            return Err(range_write_error(
                "Joined materialization requires actual completed durable catalogue",
            ));
        }
        self.require_current_disk(store)?;
        store.require_durable_recovery_catalog()?;
        db.require_native_writer_catalogue_custody(store)?;
        let result = NativeJoinedRemoteCatalogueMaterialization {
            preflight: self.preflight.clone(),
            base: self.base.clone(),
            database_open: self.database_open.clone(),
            store_open: self.store_open,
            catalogue_id: self.catalogue_id,
        };
        result.require_current(store, db, point.physical)?;
        Ok(result)
    }
    #[cfg(test)]
    pub(crate) fn test_state(&self) -> serde_json::Value {
        serde_json::json!({"original":self.original,"candidate":self.candidate,"exact_candidate_bytes_sha256":hex::encode(sha2::Sha256::digest(&self.bytes)),"writes_started":self.writes_started,"durable_replace_returned":self.durable_replace_returned,"verified":self.verified,"last_error":self.last_error,"attempts":self.attempts,"write_attempt_limit":8,"no_Serving":true})
    }
}
/// Sealed actual selected native+catalogue floor; NOT Role/Open/Serving.
pub(crate) struct NativeJoinedRemoteCatalogueMaterialization {
    preflight: Arc<NativeJoinedRemoteInstallPreflight>,
    base: NativeImageCatalogueBase,
    database_open: String,
    store_open: Uuid,
    catalogue_id: Uuid,
}
impl NativeJoinedRemoteCatalogueMaterialization {
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.store_open
    }
    /// No original snapshot Role/OWN or phase token is required here. Fresh
    /// joint phase authority is supplied independently by the actual caller.
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        physical: &NativeInstalledRangeImageBase,
    ) -> Result<()> {
        self.preflight.require_original_process()?;
        self.preflight.native().require_current(db)?;
        physical.require_current(db, self.preflight.native())?;
        db.validate_range_image_bindings(self.preflight.bindings())?;
        store.require_actual_abort_catalogue(db)?;
        store.require_durable_recovery_catalog()?;
        db.require_native_writer_catalogue_custody(store)?;
        let log = store
            .catalog
            .ranges
            .get(&self.base.range_id)
            .ok_or_else(|| range_write_error("Joined selected materialized floor absent"))?;
        if store.open_id != self.store_open
            || db.incarnation_id() != self.database_open
            || db.native_abort_catalogue_store_id()? != Some(self.catalogue_id)
            || store.catalog.native_image_bases.get(&self.base.range_id) != Some(&self.base)
            || log.last_index != self.base.range_index
            || log.resolved_through != self.base.range_index
            || log.compacted_through != self.base.range_index
            || !log.entries.is_empty()
            || physical.attempt_id() != self.base.attempt_id
            || physical.native_receipt_sha256() != self.base.native_receipt_sha256
            || physical.capture_range().id != self.base.range_id
            || physical.range_index() != self.base.range_index
            || physical.logical_genesis_sha256() != self.base.logical_genesis_sha256
            || physical.logical_history_sha256() != self.base.logical_history_sha256
        {
            return Err(range_write_error(
                "Joined materialization changed actual selected floor/open/native evidence",
            ));
        }
        self.preflight.require_original_process()
    }
}

#[cfg(test)]
mod cuts {
    use std::cell::RefCell;
    thread_local! {static CUT:RefCell<Option<Box<dyn FnOnce()>>>=const{RefCell::new(None)};}
    pub(crate) struct Guard(Option<Box<dyn FnOnce()>>);
    impl Drop for Guard {
        fn drop(&mut self) {
            CUT.with(|s| {
                s.replace(self.0.take());
            });
        }
    }
    pub(super) fn fire() {
        let cut = CUT.with(|s| s.borrow_mut().take());
        if let Some(cut) = cut {
            cut();
        }
    }
    pub(crate) fn arm(cut: Box<dyn FnOnce()>) -> Guard {
        Guard(CUT.with(|s| s.replace(Some(cut))))
    }
}
#[cfg(test)]
pub(crate) use cuts::arm as test_joined_remote_adoption_after_fsync;
