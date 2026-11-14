//! SAME original Registered A, native DECISION and whole catalogue candidate.
//! This private sink never substitutes for the ordinary installed-writer path.
use super::*;
use crate::db::{NativeJointDecisionReceipt, NativeRegisteredJointExclusionScope};
use crate::distribution_data::ActualNativeJointMetadataPoint;
use crate::distribution_native_host::NativeRegisteredRemoteJointTarget;
use crate::distribution_range_handoff::{
    NativeJointDecisionPublicationAdmission, NativeRegisteredDecisionCatalogueRetry,
};
use std::sync::Arc;

pub(crate) enum NativeRegisteredDecisionCatalogueAuthority<'a> {
    Initial(&'a NativeJointDecisionPublicationAdmission),
    Retry(&'a NativeRegisteredDecisionCatalogueRetry<'a>),
}
impl NativeRegisteredDecisionCatalogueAuthority<'_> {
    fn data(&self) -> &crate::distribution_range_handoff::NativeJointDecisionData {
        match self {
            Self::Initial(a) => a.data(),
            Self::Retry(a) => a.data(),
        }
    }
    fn role(&self) -> &RecoveryRoleAdmission {
        match self {
            Self::Initial(a) => a.role(),
            Self::Retry(a) => a.role(),
        }
    }
    fn require_point(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        target: &NativeRegisteredRemoteJointTarget,
        metadata: &ActualNativeJointMetadataPoint,
        receipt: &NativeJointDecisionReceipt,
    ) -> Result<()> {
        match self {
            Self::Initial(a) => {
                a.require_registered_own()?;
                let (own, business) = a.registered_metadata_request()?;
                metadata.require_request(own, business)?;
                a.require_native_metadata_point(db, store, receipt.native_admission())?;
            }
            Self::Retry(a) => {
                a.require_native_point(db, store, target, metadata)?;
                if a.role().context_id() == receipt.metadata_context_id() {
                    return Err(range_write_error("Registered catalogue retry must independently refresh native-decision authority"));
                }
            }
        }
        metadata.require_native_store_point(db, store)?;
        let (native_term, native_index) = receipt.metadata_anchor();
        if self.role().metadata_term() < native_term
            || self.role().metadata_read_index() < native_index
        {
            return Err(range_write_error(
                "Registered catalogue authority precedes retained native decision",
            ));
        }
        if receipt.data() != self.data() || receipt.local_node_id() != self.role().local_node_id() {
            return Err(range_write_error(
                "Registered catalogue changed exact native decision/recipient",
            ));
        }
        receipt.require_current(db)
    }
}
struct CatalogueWriteOutcome {
    succeeded: bool,
    error_sha256: Option<String>,
}
/// Retained on the SAME actual native attempt before any memory mutation/IO.
/// Full original/candidate matching conservatively refuses unrelated changes.
pub(crate) struct NativeRegisteredDecisionCataloguePublication {
    original: RangeWriteCatalog,
    candidate: RangeWriteCatalog,
    bytes: Vec<u8>,
    target: Arc<NativeRegisteredRemoteJointTarget>,
    store_open: Uuid,
    database_open: String,
    catalogue_id: Uuid,
    native_sha256: String,
    decision_sha256: String,
    outcomes: Vec<CatalogueWriteOutcome>,
    verified: bool,
}
impl NativeRegisteredDecisionCataloguePublication {
    pub(crate) fn begin(
        store: &RangeWriteStore,
        db: &crate::BicDb,
        target: Arc<NativeRegisteredRemoteJointTarget>,
        admission: &NativeJointDecisionPublicationAdmission,
        receipt: &NativeJointDecisionReceipt,
        scope: &NativeRegisteredJointExclusionScope<'_>,
        metadata: &ActualNativeJointMetadataPoint,
    ) -> Result<Self> {
        admission.require_registered_own()?;
        scope.require_current(db, store)?;
        receipt.require_current(db)?;
        store.require_native_catalog_durability(db.data_path())?;
        if !std::ptr::eq(target.as_ref(), scope.target())
            || !store.fsync
            || receipt.local_node_id() != &store.catalog.node_id
            || receipt.decision_sha256() != admission.data().checksum_sha256()
            || db.actual_native_writer_enforcement_version()? != 1
            || !crate::format::native_joint_data_handoff_active(db.data_path())?
        {
            return Err(range_write_error(
                "Registered catalogue candidate lacks original native/owner domain",
            ));
        }
        NativeRegisteredDecisionCatalogueAuthority::Initial(admission)
            .require_point(db, store, &target, metadata, receipt)?;
        let mut candidate = store.catalog.clone();
        let range = admission.data().old_range().id;
        let scope_sha = admission.plan().checksum_sha256()?;
        if let Some(current) = candidate
            .native_joint_handoffs
            .as_ref()
            .and_then(|m| m.get(&range))
        {
            if current.scope_sha256 != scope_sha
                || current.session_id != admission.data().promise().session_id
                || current.nonce != admission.data().promise().nonce
                || current
                    .global_decision_sha256
                    .as_ref()
                    .is_some_and(|s| s != receipt.decision_sha256())
                || current
                    .global_decision_native_sha256
                    .as_ref()
                    .is_some_and(|s| s != receipt.checksum_sha256())
            {
                return Err(range_write_error(
                    "Registered decision catalogue cannot replace another joint session",
                ));
            }
        }
        super::super::image_install_certification::ensure_installing_certification_maps(
            &mut candidate,
        );
        candidate
            .sparse_committed_decisions
            .get_or_insert_with(BTreeMap::new);
        let current = candidate
            .native_joint_handoffs
            .get_or_insert_with(BTreeMap::new)
            .entry(range)
            .or_insert(NativeJointLocalState {
                scope_sha256: scope_sha,
                session_id: admission.data().promise().session_id,
                nonce: admission.data().promise().nonce,
                old_closed_native_sha256: None,
                global_decision_sha256: None,
                global_decision_native_sha256: None,
                serving_activation: None,
            });
        current.global_decision_sha256 = Some(receipt.decision_sha256().into());
        current.global_decision_native_sha256 = Some(receipt.checksum_sha256().into());
        current.validate()?;
        candidate.format_version = candidate
            .format_version
            .max(NATIVE_JOINT_HANDOFF_CATALOG_FORMAT);
        let envelope = RangeWriteEnvelope::new(candidate.clone())?;
        let bytes = serde_json::to_vec_pretty(&envelope)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(range_write_error(
                "Registered decision catalogue byte bound",
            ));
        }
        envelope.verify()?;
        Ok(Self {
            original: store.catalog.clone(),
            candidate,
            bytes,
            target,
            store_open: store.open_id,
            database_open: db.incarnation_id().into(),
            catalogue_id: db
                .native_abort_catalogue_store_id()?
                .ok_or_else(|| range_write_error("Registered catalogue UID absent"))?,
            native_sha256: receipt.checksum_sha256().into(),
            decision_sha256: receipt.decision_sha256().into(),
            outcomes: Vec::new(),
            verified: false,
        })
    }
    fn require_binding(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        scope: &NativeRegisteredJointExclusionScope<'_>,
        receipt: &NativeJointDecisionReceipt,
    ) -> Result<()> {
        scope.require_identity_point(db, store)?;
        if !std::ptr::eq(self.target.as_ref(), scope.target())
            || store.open_id != self.store_open
            || db.incarnation_id() != self.database_open
            || db.native_abort_catalogue_store_id()? != Some(self.catalogue_id)
            || !store.fsync
            || db.data_path().canonicalize()? != store.root.canonicalize()?
            || (store.catalog != self.original && store.catalog != self.candidate)
            || receipt.checksum_sha256() != self.native_sha256
            || receipt.decision_sha256() != self.decision_sha256
        {
            return Err(range_write_error(
                "Registered decision retry lost SAME A/open/exact catalogue/native receipt",
            ));
        }
        receipt.require_current(db)
    }
    fn require_disk(&self, store: &RangeWriteStore) -> Result<()> {
        let mut actual = Vec::new();
        open_range_catalog_no_follow(&store.root.join(DEFAULT_RANGE_WRITE_LOG))?
            .take(64 * 1024 * 1024 + 1)
            .read_to_end(&mut actual)?;
        if actual != self.bytes
            || serde_json::from_slice::<RangeWriteEnvelope>(&actual)?.verify()? != self.candidate
        {
            return Err(range_write_error(
                "Registered decision catalogue exact readback differs",
            ));
        }
        Ok(())
    }
    pub(crate) fn advance(
        &mut self,
        db: &crate::BicDb,
        store: &mut RangeWriteStore,
        authority: NativeRegisteredDecisionCatalogueAuthority<'_>,
        receipt: &NativeJointDecisionReceipt,
        scope: &NativeRegisteredJointExclusionScope<'_>,
        metadata: &ActualNativeJointMetadataPoint,
    ) -> Result<()> {
        self.require_binding(db, store, scope, receipt)?;
        authority.require_point(db, store, &self.target, metadata, receipt)?;
        // Only an actual retained successful fsync + exact readback + gate
        // publication may take this path. A merely visible candidate cannot.
        if self.verified && !store.recovery_persistence_uncertain {
            if store.catalog != self.candidate {
                return Err(range_write_error(
                    "verified Registered decision catalogue changed",
                ));
            }
            self.require_disk(store)?;
            authority.require_point(db, store, &self.target, metadata, receipt)?;
            return Ok(());
        }
        if self.outcomes.len() >= 8 {
            return Err(range_write_error(
                "Registered catalogue IO attempt bound; custody retained",
            ));
        }
        #[cfg(test)]
        test_cut::fire(CatalogueCut::BeforeMemory);
        // Last full proof before publishing a refusal and entering actual IO.
        self.require_binding(db, store, scope, receipt)?;
        authority.require_point(db, store, &self.target, metadata, receipt)?;
        self.verified = false;
        store.recovery_persistence_uncertain = true;
        store.catalog = self.candidate.clone();
        self.outcomes.push(CatalogueWriteOutcome {
            succeeded: false,
            error_sha256: None,
        });
        let last = self.outcomes.len() - 1;
        let result = crate::storage::replace_durably(
            &store.root.join(DEFAULT_RANGE_WRITE_LOG),
            crate::storage::ReplaceContents::Plain(&self.bytes),
            true,
            "registered_joint_decision.catalogue",
            crate::storage::FailClosed::No,
        );
        // Record the actual IO result BEFORE readback, cancellation or authority checks.
        match result {
            Ok(()) => self.outcomes[last].succeeded = true,
            Err(error) => {
                use sha2::Digest;
                self.outcomes[last].error_sha256 = Some(hex::encode(sha2::Sha256::digest(
                    error.to_string().as_bytes(),
                )));
                return Err(error);
            }
        }
        #[cfg(test)]
        test_cut::fire(CatalogueCut::AfterFsync);
        self.require_disk(store)?;
        #[cfg(test)]
        test_cut::fire(CatalogueCut::AfterReadback);
        self.require_binding(db, store, scope, receipt)?;
        authority.require_point(db, store, &self.target, metadata, receipt)?;
        let guard = metadata.try_publication_guard()?;
        guard.require_current_memory()?;
        authority.role().require_live_request()?;
        // MEMORY ONLY: exact IO/readback/identity/native proofs remain borrowed.
        store.recovery_persistence_uncertain = false;
        self.verified = true;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn test_evidence(&self) -> serde_json::Value {
        use sha2::Digest;
        serde_json::json!({"candidate_sha256":hex::encode(sha2::Sha256::digest(&self.bytes)),
            "native_receipt_sha256":self.native_sha256,"decision_sha256":self.decision_sha256,
            "verified":self.verified,"attempts":self.outcomes.iter().map(|o|
                serde_json::json!({"actual_replace_returned_ok":o.succeeded,"error_sha256":o.error_sha256})).collect::<Vec<_>>()})
    }
}
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogueCut {
    BeforeMemory,
    AfterFsync,
    AfterReadback,
}
#[cfg(test)]
mod test_cut {
    use super::CatalogueCut;
    use std::cell::RefCell;
    thread_local! { static CALLBACK: RefCell<Option<(CatalogueCut, Box<dyn FnOnce()>)>> = RefCell::new(None); }
    pub(crate) struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            CALLBACK.with(|s| {
                s.borrow_mut().take();
            });
        }
    }
    pub(crate) fn arm(cut: CatalogueCut, callback: Box<dyn FnOnce()>) -> Guard {
        CALLBACK.with(|s| assert!(s.borrow_mut().replace((cut, callback)).is_none()));
        Guard
    }
    pub(super) fn fire(cut: CatalogueCut) {
        let callback = CALLBACK.with(|s| {
            let mut s = s.borrow_mut();
            if s.as_ref().is_some_and(|(c, _)| *c == cut) {
                s.take().map(|(_, cb)| cb)
            } else {
                None
            }
        });
        if let Some(callback) = callback {
            callback();
        }
    }
}
#[cfg(test)]
pub(crate) use test_cut::arm as test_registered_decision_catalogue_cut;
