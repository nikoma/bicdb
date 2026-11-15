//! Exact remote recipient catalogue publication on SAME retained native D.
//! This stores existing format-14 fields only. RAM outcomes are not cold grants.
use super::*;
use crate::db::NativeRemoteCataloguePhysicalPoint;
use crate::distribution_range_handoff::NativeRemoteDecisionAdmission;

struct WriteOutcome {
    generation: u64,
    returned_ok: Option<bool>,
    error_sha256: Option<String>,
}
pub(crate) struct NativeRemoteDecisionCataloguePublication {
    original: RangeWriteCatalog,
    candidate: RangeWriteCatalog,
    bytes: Vec<u8>,
    candidate_sha256: String,
    store_open: Uuid,
    database_open: String,
    catalogue_id: Uuid,
    participant_sha256: String,
    native_sha256: String,
    decision_sha256: String,
    preimage_generation: u64,
    pending_generation: Option<u64>,
    owns_uncertainty: bool,
    verified_generation: Option<u64>,
    outcomes: Vec<WriteOutcome>,
    #[cfg(test)]
    intervening_write_once: bool,
    #[cfg(test)]
    intervening_write_count: u64,
}
impl NativeRemoteDecisionCataloguePublication {
    pub(crate) fn begin(
        store: &RangeWriteStore,
        db: &crate::BicDb,
        admission: &NativeRemoteDecisionAdmission<'_>,
        physical: &NativeRemoteCataloguePhysicalPoint<'_>,
    ) -> Result<Self> {
        physical.require_current(db, store, admission)?;
        store.require_native_catalog_durability(db.data_path())?;
        let receipt = admission.catalogue_receipt()?;
        if receipt.local_node_id() != &store.catalog.node_id
            || receipt.local_node_id() != admission.role().local_node_id()
            || receipt.data() != admission.data()
            || !crate::format::native_remote_joint_decision_active(db.data_path())?
        {
            return Err(range_write_error(
                "remote catalogue original known native/reader domain differs",
            ));
        }
        let range = admission.data().old_range().id;
        let scope = admission.plan().checksum_sha256()?;
        let mut candidate = store.catalog.clone();
        if let Some(current) = candidate
            .native_joint_handoffs
            .as_ref()
            .and_then(|m| m.get(&range))
        {
            if current.scope_sha256 != scope
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
                    "remote catalogue cannot replace original joint session",
                ));
            }
        }
        super::super::image_install_certification::ensure_installing_certification_maps(
            &mut candidate,
        );
        candidate
            .sparse_committed_decisions
            .get_or_insert_with(BTreeMap::new);
        let state = candidate
            .native_joint_handoffs
            .get_or_insert_with(BTreeMap::new)
            .entry(range)
            .or_insert(NativeJointLocalState {
                scope_sha256: scope,
                session_id: admission.data().promise().session_id,
                nonce: admission.data().promise().nonce,
                old_closed_native_sha256: None,
                global_decision_sha256: None,
                global_decision_native_sha256: None,
                serving_activation: None,
            });
        state.global_decision_sha256 = Some(receipt.decision_sha256().into());
        state.global_decision_native_sha256 = Some(receipt.checksum_sha256().into());
        state.validate()?;
        candidate.format_version = candidate
            .format_version
            .max(NATIVE_JOINT_HANDOFF_CATALOG_FORMAT);
        let envelope = RangeWriteEnvelope::new(candidate.clone())?;
        let bytes = serde_json::to_vec_pretty(&envelope)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(range_write_error(
                "remote catalogue candidate exceeds existing 64MiB bound",
            ));
        }
        envelope.verify()?;
        Ok(Self {
            candidate_sha256: {
                use sha2::Digest;
                hex::encode(sha2::Sha256::digest(&bytes))
            },
            original: store.catalog.clone(),
            candidate,
            bytes,
            store_open: store.open_id,
            database_open: db.incarnation_id().into(),
            catalogue_id: db
                .native_abort_catalogue_store_id()?
                .ok_or_else(|| range_write_error("remote catalogue original UID absent"))?,
            participant_sha256: admission.original_participant_sha256().into(),
            native_sha256: receipt.checksum_sha256().into(),
            decision_sha256: receipt.decision_sha256().into(),
            preimage_generation: store.current_catalogue_write_generation(),
            pending_generation: None,
            owns_uncertainty: false,
            verified_generation: None,
            outcomes: Vec::new(),
            #[cfg(test)]
            intervening_write_once: false,
            #[cfg(test)]
            intervening_write_count: 0,
        })
    }
    pub(crate) fn candidate_sha256(&self) -> &str {
        &self.candidate_sha256
    }
    fn require_identity(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        admission: &NativeRemoteDecisionAdmission<'_>,
        physical: &NativeRemoteCataloguePhysicalPoint<'_>,
    ) -> Result<()> {
        physical.require_current(db, store, admission)?;
        let receipt = admission.catalogue_receipt()?;
        if store.open_id != self.store_open
            || db.incarnation_id() != self.database_open
            || db.native_abort_catalogue_store_id()? != Some(self.catalogue_id)
            || admission.original_participant_sha256() != self.participant_sha256
            || receipt.checksum_sha256() != self.native_sha256
            || receipt.decision_sha256() != self.decision_sha256
            || !store.fsync
            || db.data_path().canonicalize()? != store.root.canonicalize()?
            || !range_write_catalog_format_supported(store.catalog.format_version)
            || !crate::format::native_remote_joint_decision_active(db.data_path())?
        {
            return Err(range_write_error(
                "remote catalogue lost SAME actual recipient/open/native/reader identity",
            ));
        }
        Ok(())
    }
    fn require_pending(&self, store: &RangeWriteStore) -> Result<()> {
        let generation = self.pending_generation.unwrap_or(self.preimage_generation);
        if store.current_catalogue_write_generation() != generation
            || (store.catalog != self.original && store.catalog != self.candidate)
            || match self.pending_generation {
                None => self.owns_uncertainty || store.recovery_persistence_uncertain,
                Some(_) => !self.owns_uncertainty || !store.recovery_persistence_uncertain,
            }
        {
            return Err(range_write_error(
                "remote catalogue exact original candidate superseded; no stale rewrite",
            ));
        }
        Ok(())
    }
    fn require_disk(&self, store: &RangeWriteStore, candidate_only: bool) -> Result<String> {
        let mut bytes = Vec::new();
        open_range_catalog_no_follow(&store.root.join(DEFAULT_RANGE_WRITE_LOG))?
            .take(64 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(range_write_error(
                "remote catalogue actual readback exceeds existing 64MiB bound",
            ));
        }
        let parsed = serde_json::from_slice::<RangeWriteEnvelope>(&bytes)?.verify()?;
        if parsed != store.catalog || (candidate_only && bytes != self.bytes) {
            return Err(range_write_error(
                "remote catalogue exact actual disk/memory readback differs",
            ));
        }
        use sha2::Digest;
        Ok(hex::encode(sha2::Sha256::digest(&bytes)))
    }
    pub(crate) fn observe(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        admission: &NativeRemoteDecisionAdmission<'_>,
        physical: &NativeRemoteCataloguePhysicalPoint<'_>,
    ) -> Result<String> {
        self.require_identity(db, store, admission, physical)?;
        if self.owns_uncertainty
            || self.verified_generation.is_none()
            || self
                .outcomes
                .last()
                .is_none_or(|o| o.returned_ok != Some(true))
        {
            return Err(range_write_error(
                "remote catalogue original successful sink absent; visible bytes are not success",
            ));
        }
        // Never clear another operation's uncertainty, even with equal bytes.
        store.require_native_catalog_durability(db.data_path())?;
        let range = admission.data().old_range().id;
        if store
            .catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|m| m.get(&range))
            != self
                .candidate
                .native_joint_handoffs
                .as_ref()
                .and_then(|m| m.get(&range))
        {
            return Err(range_write_error(
                "remote catalogue selected durable decision changed",
            ));
        }
        // Healthy unrelated-range catalogue advancement is allowed. Read the
        // actual latest full file, never compare it to the old whole snapshot.
        let checksum = self.require_disk(store, false)?;
        self.require_identity(db, store, admission, physical)?;
        let _version = admission.publication_guard()?;
        admission.require_memory_current()?;
        Ok(checksum)
    }
    pub(crate) fn advance(
        &mut self,
        db: &crate::BicDb,
        store: &mut RangeWriteStore,
        admission: &NativeRemoteDecisionAdmission<'_>,
        physical: &NativeRemoteCataloguePhysicalPoint<'_>,
    ) -> Result<String> {
        if self.verified_generation.is_some() {
            return self.observe(db, store, admission, physical);
        }
        self.require_identity(db, store, admission, physical)?;
        #[cfg(test)]
        if std::mem::take(&mut self.intervening_write_once) {
            // Genuine byte-identical file IO through the SAME existing
            // generic persistence path; NEVER write the generation scalar.
            store.persist()?;
            self.intervening_write_count += 1;
        }
        self.require_pending(store)?;
        if self.outcomes.len() >= 8 {
            return Err(range_write_error(
                "remote catalogue existing eight-IO attempt bound; original custody retained",
            ));
        }
        // No allocation/framing after IO entry: retain exact candidate and
        // outcome slot first. A failed entered write remains owned and fenced.
        self.outcomes.reserve(1);
        self.owns_uncertainty = true;
        store.recovery_persistence_uncertain = true;
        store.catalog = self.candidate.clone();
        let generation = store.note_catalogue_write_entered()?;
        self.pending_generation = Some(generation);
        self.outcomes.push(WriteOutcome {
            generation,
            returned_ok: None,
            error_sha256: None,
        });
        let at = self.outcomes.len() - 1;
        let result = crate::storage::replace_durably(
            &store.root.join(DEFAULT_RANGE_WRITE_LOG),
            crate::storage::ReplaceContents::Plain(&self.bytes),
            true,
            "remote_joint_decision.catalogue",
            crate::storage::FailClosed::No,
        );
        self.outcomes[at].returned_ok = Some(result.is_ok());
        if let Err(error) = result {
            use sha2::Digest;
            self.outcomes[at].error_sha256 = Some(hex::encode(sha2::Sha256::digest(
                error.to_string().as_bytes(),
            )));
            return Err(error);
        }
        let checksum = self.require_disk(store, true)?;
        self.require_identity(db, store, admission, physical)?;
        self.require_pending(store)?;
        let _version = admission.publication_guard()?;
        admission.require_memory_current()?;
        // MEMORY ONLY. Lexical physical/store/native/readback proofs remain
        // borrowed. No callback, allocation or old-owner destructor here.
        store.recovery_persistence_uncertain = false;
        self.owns_uncertainty = false;
        self.verified_generation = Some(generation);
        Ok(checksum)
    }
    #[cfg(test)]
    pub(crate) fn test_arm_intervening_write(&mut self) -> Result<()> {
        if !self.owns_uncertainty
            || self.pending_generation.is_none()
            || self.verified_generation.is_some()
            || self.intervening_write_once
            || self.intervening_write_count != 0
        {
            return Err(range_write_error(
                "actual intervening-IO control needs SAME pending uncertain candidate",
            ));
        }
        self.intervening_write_once = true;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn test_evidence(&self) -> serde_json::Value {
        use sha2::Digest;
        serde_json::json!({"candidate_sha256":hex::encode(sha2::Sha256::digest(&self.bytes)),
            "preimage_generation":self.preimage_generation,"pending_generation":self.pending_generation,"owns_uncertainty":self.owns_uncertainty,
            "verified_generation":self.verified_generation,"actual_intervening_write_count":self.intervening_write_count,"native_record_sha256":self.native_sha256,
            "decision_sha256":self.decision_sha256,"IO_attempts":self.outcomes.iter().map(|o|
                serde_json::json!({"generation":o.generation,"actual_returned_ok":o.returned_ok,"error_sha256":o.error_sha256})).collect::<Vec<_>>()})
    }
}
