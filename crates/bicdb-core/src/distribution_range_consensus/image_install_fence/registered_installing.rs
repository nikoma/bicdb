//! Actual Registered target catalogue refusal, after its protected native proposal.
//! Every retry re-fsyncs the exact retained candidate. This is not a row grant.
use super::*;
use crate::db::{NativeRegisteredRemoteAttemptScope, NativeRegisteredRemoteProposalReceipt};
use crate::distribution_native_host::{
    NativeRegisteredRemoteInstallPreflight, NativeRegisteredRemoteProposalPoint,
};
use std::sync::Arc;

/// Opaque original-owner result. It is never serialized or rebuilt from a
/// pointer. Its checks remain constraints, not native mutation permission.
pub(crate) struct NativeRegisteredRemoteInstallingFence {
    candidate: RangeWriteCatalog,
    intent: ImageInstallIntent,
    store_open: Uuid,
    database_open: String,
    catalogue_id: Uuid,
    preflight: Arc<NativeRegisteredRemoteInstallPreflight>,
}
impl NativeRegisteredRemoteInstallingFence {
    pub(crate) fn range(&self) -> &RangeDescriptor {
        &self.intent.range
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.intent.attempt_id
    }
    pub(crate) fn intent_sha256(&self) -> Result<String> {
        self.intent.sha256()
    }
    pub(crate) fn protected_proposal(
        &self,
    ) -> &super::super::NativeRegisteredRemoteProposalPointer {
        self.intent
            .registered_proposal
            .as_ref()
            .expect("original verified Registered pointer")
    }
    pub(crate) fn source_manifest_sha256(&self) -> &str {
        self.preflight.intent().source_manifest_sha256()
    }
    /// Readonly under-WAL constraints for a separately minted lexical native
    /// mutation scope. The caller still checks the actual CURRENT/archive
    /// native receipt and live source/role/lease; this cannot mint a grant.
    pub(crate) fn require_database_binding(
        &self,
        db: &crate::BicDb,
        preflight: &NativeRegisteredRemoteInstallPreflight,
        receipt: &NativeRegisteredRemoteProposalReceipt,
    ) -> Result<()> {
        preflight.require_original_process()?;
        preflight.native().require_current(db)?;
        db.validate_range_image_bindings(preflight.bindings())?;
        if !std::ptr::eq(Arc::as_ref(&self.preflight), preflight)
            || self.store_open != preflight.catalogue().store_open_id()
            || db.incarnation_id() != self.database_open
            || db.native_abort_catalogue_store_id()? != Some(self.catalogue_id)
            || receipt.pointer() != *self.protected_proposal()
            || receipt.intent_sha256() != self.intent.core_sha256()?
            || receipt.intent_bytes() != preflight.intent().canonical_intent_bytes()?
            || receipt.source_manifest().checksum_sha256 != self.source_manifest_sha256()
        {
            return Err(range_write_error(
                "Registered Installing original database/proposal binding changed",
            ));
        }
        self.require_disk_at(db.data_path())?;
        preflight.require_original_process()
    }
    /// Exact readonly catalogue/identity constraints. A caller must ALSO hold
    /// the actual native exclusion and revalidate its current original point,
    /// protected native receipt and source frame. This alone grants no writes.
    pub(crate) fn require_catalogue_binding(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        preflight: &NativeRegisteredRemoteInstallPreflight,
        receipt: &NativeRegisteredRemoteProposalReceipt,
    ) -> Result<()> {
        preflight.require_original_process()?;
        self.require_database_binding(db, preflight, receipt)?;
        if !std::ptr::eq(Arc::as_ref(&self.preflight), preflight)
            || store.open_id != self.store_open
            || db.incarnation_id() != self.database_open
            || db.data_path().canonicalize()? != store.root.canonicalize()?
            || db.native_abort_catalogue_store_id()? != Some(self.catalogue_id)
            || store.catalog != self.candidate
            || receipt.pointer() != *self.protected_proposal()
            || receipt.intent_sha256() != self.intent.core_sha256()?
            || receipt.intent_bytes() != preflight.intent().canonical_intent_bytes()?
            || receipt.source_manifest().checksum_sha256 != self.source_manifest_sha256()
            || receipt.native_records_sha256() != preflight.safety().records_sha256()
            || receipt.raw_preimage_sha256() != preflight.catalogue().constraint_sha256()?
        {
            return Err(range_write_error(
                "Registered Installing exact owner/catalogue/proposal binding changed",
            ));
        }
        store.require_actual_abort_catalogue(db)?;
        store.require_durable_recovery_catalog()?;
        store.require_active_recovery_scope_record(preflight.scope())?;
        self.require_disk(store)?;
        preflight.require_original_process()
    }
    fn require_disk(&self, store: &RangeWriteStore) -> Result<()> {
        self.require_disk_at(&store.root)
    }
    fn require_disk_at(&self, root: &Path) -> Result<()> {
        let file = open_range_catalog_no_follow(&root.join(DEFAULT_RANGE_WRITE_LOG))?;
        let mut bytes = Vec::new();
        file.take(64 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(range_write_error(
                "Registered Installing readback exceeds bound",
            ));
        }
        let actual = serde_json::from_slice::<RangeWriteEnvelope>(&bytes)?.verify()?;
        if actual != self.candidate {
            return Err(range_write_error(
                "Registered Installing actual durable catalogue differs",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_attempt_current(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        point: &NativeRegisteredRemoteProposalPoint<'_>,
        scope: &NativeRegisteredRemoteAttemptScope<'_>,
        receipt: &NativeRegisteredRemoteProposalReceipt,
    ) -> Result<()> {
        point.require_original_boundary(db, scope)?;
        receipt.require_current(db, store, scope)?;
        self.require_catalogue_binding(store, db, point.preflight(), receipt)?;
        point.require_original_boundary(db, scope)
    }
}

/// Retained in the original host BEFORE any catalogue mutation or file write.
/// A canceled reply or uncertain rename leaves this exact candidate and A owned.
pub(crate) struct NativeRegisteredRemoteInstallingPublication {
    original: RangeWriteCatalog,
    fence: Arc<NativeRegisteredRemoteInstallingFence>,
    verified: bool,
}
impl NativeRegisteredRemoteInstallingPublication {
    /// Necessary request-entry constraints only. Uncertain persistence is
    /// allowed to reach this SAME retained candidate's real re-fsync sink;
    /// this method neither declares durability nor grants native mutations.
    pub(crate) fn require_request_catalogue(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        preflight: &NativeRegisteredRemoteInstallPreflight,
    ) -> Result<()> {
        preflight.require_original_process()?;
        preflight.native().require_current(db)?;
        db.validate_range_image_bindings(preflight.bindings())?;
        if !std::ptr::eq(Arc::as_ref(&self.fence.preflight), preflight)
            || store.open_id != self.fence.store_open
            || db.incarnation_id() != self.fence.database_open
            || db.data_path().canonicalize()? != store.root.canonicalize()?
            || db.native_abort_catalogue_store_id()? != Some(self.fence.catalogue_id)
            || !store.fsync
            || !crate::format::range_image_install_active(db.data_path())?
            || (store.catalog != self.original && store.catalog != self.fence.candidate)
        {
            return Err(range_write_error(
                "Registered retry is outside original retained catalogue candidate",
            ));
        }
        store.require_actual_abort_catalogue(db)?;
        preflight.require_original_process()
    }
    pub(crate) fn begin(
        store: &RangeWriteStore,
        db: &crate::BicDb,
        point: &NativeRegisteredRemoteProposalPoint<'_>,
        scope: &NativeRegisteredRemoteAttemptScope<'_>,
        receipt: &NativeRegisteredRemoteProposalReceipt,
    ) -> Result<Self> {
        point.require_database_store(db, store, scope)?;
        receipt.require_current(db, store, scope)?;
        let actual = point.preflight();
        if !store.fsync
            || !store.catalogue_loaded_at_open
            || store.recovery_persistence_uncertain
            || store.catalog.image_install_intents.len() >= MAX_INSTALL_INTENTS
            || store.catalog.image_install_intents.values().any(|intent| {
                intent.purpose == ImageInstallPurpose::RelocationLearner
                    && intent.registered_proposal.is_none()
            })
            || receipt.pointer().attempt_id != actual.attempt_id()
            || receipt.intent_sha256() != actual.intent().sha256()?
            || receipt.intent_bytes() != actual.intent().canonical_intent_bytes()?
            || crate::distribution_range_image::bounded_json(
                receipt.source_manifest(),
                4 * 1024 * 1024,
            )? != crate::distribution_range_image::bounded_json(
                actual.source().manifest(),
                4 * 1024 * 1024,
            )?
            || receipt.native_records_sha256() != actual.safety().records_sha256()
            || receipt.raw_preimage_sha256() != actual.catalogue().constraint_sha256()?
        {
            return Err(range_write_error(
                "Registered Installing differs from actual protected pre-BEGIN point",
            ));
        }
        let mut intent: ImageInstallIntent = serde_json::from_slice(receipt.intent_bytes())?;
        intent.registered_proposal = Some(receipt.pointer());
        intent.validate()?;
        let mut candidate = store.catalog.clone();
        candidate.format_version = candidate
            .format_version
            .max(super::super::REGISTERED_IMAGE_INSTALL_CATALOG_FORMAT);
        if candidate
            .image_install_intents
            .insert(intent.range.id, intent.clone())
            .is_some()
        {
            return Err(range_write_error(
                "Registered Installing cannot replace an existing image attempt",
            ));
        }
        RangeWriteEnvelope::new(candidate.clone())?.verify()?;
        point.require_database_store(db, store, scope)?;
        Ok(Self {
            original: store.catalog.clone(),
            fence: Arc::new(NativeRegisteredRemoteInstallingFence {
                candidate,
                intent,
                store_open: store.open_id,
                database_open: db.incarnation_id().to_owned(),
                catalogue_id: actual.catalogue().catalogue_id(),
                preflight: point.preflight_arc().clone(),
            }),
            verified: false,
        })
    }
    pub(crate) fn fence(&self) -> Option<&Arc<NativeRegisteredRemoteInstallingFence>> {
        self.verified.then_some(&self.fence)
    }
    pub(crate) fn advance(
        &mut self,
        store: &mut RangeWriteStore,
        db: &crate::BicDb,
        point: &NativeRegisteredRemoteProposalPoint<'_>,
        scope: &NativeRegisteredRemoteAttemptScope<'_>,
        receipt: &NativeRegisteredRemoteProposalReceipt,
    ) -> Result<Arc<NativeRegisteredRemoteInstallingFence>> {
        point.require_original_boundary(db, scope)?;
        receipt.require_current(db, store, scope)?;
        if store.open_id != self.fence.store_open
            || !std::ptr::eq(Arc::as_ref(&self.fence.preflight), point.preflight())
            || (store.catalog != self.original && store.catalog != self.fence.candidate)
            || receipt.pointer() != *self.fence.protected_proposal()
        {
            return Err(range_write_error(
                "Registered Installing continuation lost its exact original candidate",
            ));
        }
        if store.catalog == self.original {
            point.require_database_store(db, store, scope)?;
        }
        // Visible format metadata after an earlier directory-sync error is not
        // durability. Repeat its durable publication on EVERY continuation.
        crate::format::enable_native_registered_remote_proposal(db.data_path())?;
        self.verified = false;
        store.catalog = self.fence.candidate.clone();
        store.recovery_persistence_uncertain = true;
        let envelope = RangeWriteEnvelope::new(store.catalog.clone())?;
        let bytes = serde_json::to_vec_pretty(&envelope)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(range_write_error(
                "Registered Installing catalogue exceeds byte bound",
            ));
        }
        // The real atomic file replacement includes both file and directory
        // fsync. All errors retain the memory refusal and original candidate.
        crate::storage::replace_durably(
            &store.root.join(DEFAULT_RANGE_WRITE_LOG),
            crate::storage::ReplaceContents::Plain(&bytes),
            true,
            "registered_remote_install.catalogue",
            crate::storage::FailClosed::No,
        )?;
        #[cfg(test)]
        test_cut::fire();
        self.fence.require_disk(store)?;
        point.require_original_boundary(db, scope)?;
        receipt.require_current(db, store, scope)?;
        // Clear uncertainty only after successful real fsync, exact readback
        // and postchecks; neither matching memory nor a pointer suffices.
        store.recovery_persistence_uncertain = false;
        if let Err(error) = self
            .fence
            .require_attempt_current(store, db, point, scope, receipt)
        {
            store.recovery_persistence_uncertain = true;
            return Err(error);
        }
        self.verified = true;
        Ok(self.fence.clone())
    }
    #[cfg(test)]
    pub(crate) fn test_outcome(&self) -> serde_json::Value {
        serde_json::json!({"attempt":self.fence.attempt_id(),
            "intent_sha256":self.fence.intent_sha256().ok(),
            "pointer":self.fence.protected_proposal(),"verified":self.verified,
            "candidate_sha256":catalog_checksum(&self.fence.candidate).ok(),
            "candidate":self.fence.candidate})
    }
}

#[cfg(test)]
mod test_cut {
    use std::cell::RefCell;
    thread_local! {
        static CALLBACK: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
    }
    pub(crate) struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            CALLBACK.with(|slot| {
                slot.borrow_mut().take();
            });
        }
    }
    pub(crate) fn arm(callback: Box<dyn FnOnce()>) -> Guard {
        CALLBACK.with(|slot| {
            assert!(slot.borrow_mut().replace(callback).is_none());
        });
        Guard
    }
    pub(super) fn fire() {
        let callback = CALLBACK.with(|slot| slot.borrow_mut().take());
        if let Some(callback) = callback {
            callback();
        }
    }
}
#[cfg(test)]
pub(crate) use test_cut::arm as test_registered_remote_installing_after_fsync;

#[cfg(test)]
impl RangeWriteStore {
    /// Recompute the outer checksum so tests distinguish semantic rejection
    /// of erased fields from merely detecting an unchanged checksum.
    pub(crate) fn test_registered_installing_envelope_semantics(bytes: &[u8]) -> Result<()> {
        let supplied: RangeWriteEnvelope = serde_json::from_slice(bytes)?;
        RangeWriteEnvelope::new(supplied.catalog)?.verify()?;
        Ok(())
    }
}
