//! Additional Joined range Installing: selected constraints, not FIRST, and
//! never an ordinary write grant. Uncertain exact catalogue candidates stay
//! retained; verified fences follow only the selected intent/current disk.
use super::*;
use crate::db::{NativeJoinedRemoteAttemptScope, NativeJoinedRemoteProposalReceipt};
use crate::distribution_native_host::NativeJoinedRemoteInstallPreflight;

pub(crate) struct NativeJoinedRemoteInstallingFence {
    preflight: Arc<NativeJoinedRemoteInstallPreflight>,
    intent: ImageInstallIntent,
    store_open: Uuid,
    catalogue: Uuid,
    database_open: String,
}
impl NativeJoinedRemoteInstallingFence {
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.intent.attempt_id
    }
    pub(crate) fn range(&self) -> &RangeDescriptor {
        &self.intent.range
    }
    pub(crate) fn protected_proposal(&self) -> &super::super::NativeJoinedRemoteProposalPointer {
        self.intent
            .joined_proposal
            .as_ref()
            .expect("actual retained Joined pointer")
    }
    pub(crate) fn intent_sha256(&self) -> Result<String> {
        self.intent.sha256()
    }
    fn require_identity(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        scope: &NativeJoinedRemoteAttemptScope<'_>,
        receipt: &NativeJoinedRemoteProposalReceipt,
    ) -> Result<()> {
        scope.require_store_identity(db, store)?;
        receipt.require_current(db, store, scope)?;
        if !Arc::ptr_eq(&self.preflight, scope.preflight())
            || self.store_open != store.open_id
            || self.database_open != db.incarnation_id()
            || self.catalogue
                != db
                    .native_abort_catalogue_store_id()?
                    .ok_or_else(|| range_write_error("Joined catalogue identity absent"))?
            || self.intent.range.id != scope.range_id()
            || receipt.pointer() != *self.protected_proposal()
            || receipt.intent_bytes() != self.preflight.intent().canonical_intent_bytes()?
            || receipt.intent_sha256() != self.intent.core_sha256()?
            || receipt.source_manifest().checksum_sha256
                != self.preflight.source().manifest().checksum_sha256
        {
            return Err(range_write_error(
                "Joined Installing changed actual original identity",
            ));
        }
        Ok(())
    }
    fn read_disk_current(&self, store: &RangeWriteStore) -> Result<()> {
        let path = store.root.join(DEFAULT_RANGE_WRITE_LOG);
        if std::fs::metadata(&path)?.len() > 64 * 1024 * 1024 {
            return Err(range_write_error(
                "Joined catalogue readback byte bound exceeded",
            ));
        }
        let bytes = std::fs::read(&path)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(range_write_error(
                "Joined catalogue changed beyond readback byte bound",
            ));
        }
        let envelope: RangeWriteEnvelope = serde_json::from_slice(&bytes)?;
        envelope.verify()?;
        // Compare disk against CURRENT memory, not an old full-catalogue
        // snapshot. Other ranges may advance through their actual writers.
        if envelope.catalog != store.catalog
            || store
                .catalog
                .image_install_intents
                .get(&self.intent.range.id)
                != Some(&self.intent)
        {
            return Err(range_write_error(
                "Joined selected Installing/disk-current mismatch",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        scope: &NativeJoinedRemoteAttemptScope<'_>,
        receipt: &NativeJoinedRemoteProposalReceipt,
    ) -> Result<()> {
        self.require_identity(store, db, scope, receipt)?;
        store.require_durable_recovery_catalog()?;
        store.require_actual_abort_catalogue(db)?;
        db.require_native_writer_catalogue_custody(store)?;
        self.read_disk_current(store)?;
        self.require_identity(store, db, scope, receipt)
    }
}
struct PendingCatalogueWrite {
    original: RangeWriteCatalog,
    candidate: RangeWriteCatalog,
    bytes: Vec<u8>,
}
pub(crate) struct NativeJoinedRemoteInstallingPublication {
    fence: Arc<NativeJoinedRemoteInstallingFence>,
    pending: Option<PendingCatalogueWrite>,
    verified: bool,
}
impl NativeJoinedRemoteInstallingPublication {
    pub(crate) fn begin(
        store: &RangeWriteStore,
        db: &crate::BicDb,
        scope: &NativeJoinedRemoteAttemptScope<'_>,
        receipt: &NativeJoinedRemoteProposalReceipt,
    ) -> Result<Self> {
        scope.require_initial_preimage(db, store)?;
        receipt.require_current(db, store, scope)?;
        let actual = scope.preflight();
        if !store.fsync
            || !store.catalogue_loaded_at_open
            || store.recovery_persistence_uncertain
            || store.catalog.image_install_intents.len() >= MAX_INSTALL_INTENTS
            || receipt.pointer().attempt_id != actual.attempt_id()
            || receipt.native_preimage_sha256() != actual.safety().records_sha256()
            || receipt.catalogue_preimage_sha256()
                != actual.selected().catalogue().constraint_sha256()?
            || receipt.intent_bytes() != actual.intent().canonical_intent_bytes()?
            || receipt.source_manifest().checksum_sha256
                != actual.source().manifest().checksum_sha256
        {
            return Err(range_write_error(
                "Joined Installing differs from original protected proposal/preimage",
            ));
        }
        let mut intent: ImageInstallIntent = serde_json::from_slice(receipt.intent_bytes())?;
        intent.joined_proposal = Some(receipt.pointer());
        intent.validate()?;
        Ok(Self {
            fence: Arc::new(NativeJoinedRemoteInstallingFence {
                preflight: actual.clone(),
                intent,
                store_open: store.open_id,
                catalogue: actual.selected().catalogue().catalogue_id(),
                database_open: db.incarnation_id().into(),
            }),
            pending: None,
            verified: false,
        })
    }
    pub(crate) fn fence(&self) -> Option<&Arc<NativeJoinedRemoteInstallingFence>> {
        self.verified.then_some(&self.fence)
    }
    pub(crate) fn advance(
        &mut self,
        store: &mut RangeWriteStore,
        db: &crate::BicDb,
        scope: &NativeJoinedRemoteAttemptScope<'_>,
        receipt: &NativeJoinedRemoteProposalReceipt,
    ) -> Result<Arc<NativeJoinedRemoteInstallingFence>> {
        self.fence.require_identity(store, db, scope, receipt)?;
        if self.verified {
            self.fence.require_current(store, db, scope, receipt)?;
            return Ok(self.fence.clone());
        }
        crate::format::enable_native_joined_remote_receive(db.data_path())?;
        if self.pending.is_none() {
            scope.require_initial_preimage(db, store)?;
            // First actual persistence candidate is derived from the LATEST
            // complete catalogue under its actual Store owner. No earlier
            // unrelated range state can be overwritten by an old snapshot.
            let original = store.catalog.clone();
            let mut candidate = original.clone();
            candidate.format_version = candidate
                .format_version
                .max(super::super::JOINED_IMAGE_INSTALL_CATALOG_FORMAT);
            if candidate
                .image_install_intents
                .insert(self.fence.range().id, self.fence.intent.clone())
                .is_some()
            {
                return Err(range_write_error(
                    "Joined Installing cannot replace another selected attempt",
                ));
            }
            let envelope = RangeWriteEnvelope::new(candidate.clone())?;
            envelope.verify()?;
            let bytes = serde_json::to_vec_pretty(&envelope)?;
            if bytes.len() > 64 * 1024 * 1024 {
                return Err(range_write_error(
                    "Joined Installing catalogue byte bound exceeded",
                ));
            }
            self.pending = Some(PendingCatalogueWrite {
                original,
                candidate,
                bytes,
            });
        }
        let pending = self.pending.as_ref().unwrap();
        if store.catalog != pending.original && store.catalog != pending.candidate {
            return Err(range_write_error(
                "Joined uncertain candidate cannot overwrite later unrelated catalogue changes",
            ));
        }
        // Exact original/candidate bytes already retained before memory/IO.
        store.catalog = pending.candidate.clone();
        store.recovery_persistence_uncertain = true;
        crate::storage::replace_durably(
            &store.root.join(DEFAULT_RANGE_WRITE_LOG),
            crate::storage::ReplaceContents::Plain(&pending.bytes),
            true,
            "joined_remote_install.catalogue",
            crate::storage::FailClosed::No,
        )?;
        self.fence.read_disk_current(store)?;
        self.fence.require_identity(store, db, scope, receipt)?;
        store.recovery_persistence_uncertain = false;
        if let Err(error) = self.fence.require_current(store, db, scope, receipt) {
            store.recovery_persistence_uncertain = true;
            return Err(error);
        }
        self.verified = true;
        Ok(self.fence.clone())
    }
}
