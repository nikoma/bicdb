//! Original selected Joined proposal v2. The FIRST v1/B0 reader is unchanged.
//! Exact native outcome custody is not Installing, a row grant, or Serving.
use super::*;
use crate::distribution_range_consensus::RangeWriteStore;
use crate::distribution_range_image::NativeRemoteImageManifestV1;
use sha2::{Digest, Sha256};

const MAX_BYTES: usize = 4 * 1024 * 1024;
fn current(attempt: Uuid) -> String {
    format!("joined-remote-image-proposal-v2/{attempt}/current")
}
fn archive(attempt: Uuid, xid: u64) -> String {
    format!("joined-remote-image-proposal-v2/{attempt}/archive/{xid:020}")
}
fn is_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}
pub(crate) use crate::distribution_range_consensus::NativeJoinedRemoteProposalPointer;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JoinedProposalV2 {
    version: u32,
    attempt: Uuid,
    range: RangeId,
    source: NativeRemoteImageManifestV1,
    intent_bytes: Vec<u8>,
    intent_sha256: String,
    target_birth: crate::distribution::NativeStoreBirthV1,
    target_database_open: String,
    target_store_open: Uuid,
    target_catalogue: Uuid,
    target_namespaces: Vec<(String, Uuid)>,
    original_writer_policy_sha256: String,
    original_observed_b: u64,
    selected_native_preimage_sha256: String,
    selected_irreversible_sha256: String,
    selected_catalogue_preimage_sha256: String,
    metadata_store: Uuid,
    metadata_context: Uuid,
    metadata_term: u64,
    metadata_index: u64,
    native_page_xid: u64,
    checksum_sha256: String,
}
impl JoinedProposalV2 {
    fn checksum(&self) -> Result<String> {
        let mut body = self.clone();
        body.checksum_sha256.clear();
        Ok(hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &("native-joined-remote-proposal-v2", body),
                MAX_BYTES,
            )?,
        )))
    }
    fn validate(&self) -> Result<()> {
        self.source.require_shape()?;
        let _: serde_json::Value = serde_json::from_slice(&self.intent_bytes)?;
        if self.version != 2
            || self.attempt.is_nil()
            || self.attempt != self.source.target_attempt
            || self.range != self.source.capture_range.id
            || self.target_birth != self.source.target_birth
            || self.target_database_open.is_empty()
            || self.target_store_open.is_nil()
            || self.target_catalogue.is_nil()
            || self.metadata_store.is_nil()
            || self.metadata_context.is_nil()
            || self.metadata_term == 0
            || self.native_page_xid == 0
            || self.intent_bytes.len() > 128 * 1024
            || self.intent_sha256 != hex::encode(Sha256::digest(&self.intent_bytes))
            || !is_digest(&self.original_writer_policy_sha256)
            || !is_digest(&self.selected_native_preimage_sha256)
            || !is_digest(&self.selected_irreversible_sha256)
            || !is_digest(&self.selected_catalogue_preimage_sha256)
            || self.target_namespaces.len() > 65536
            || self.target_namespaces.windows(2).any(|p| p[0].0 >= p[1].0)
            || self
                .target_namespaces
                .iter()
                .any(|(name, id)| name.is_empty() || id.is_nil())
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged Joined selected proposal v2"));
        }
        // B is an observed database watermark, not a range floor. It may be
        // positive and unrelated ranges may subsequently advance it.
        Ok(())
    }
    fn pointer(&self) -> NativeJoinedRemoteProposalPointer {
        NativeJoinedRemoteProposalPointer {
            attempt_id: self.attempt,
            native_page_xid: self.native_page_xid,
            checksum_sha256: self.checksum_sha256.clone(),
        }
    }
}
pub(crate) struct NativeJoinedRemoteProposalReceipt {
    record: JoinedProposalV2,
}
impl NativeJoinedRemoteProposalReceipt {
    pub(crate) fn source_manifest(&self) -> &NativeRemoteImageManifestV1 {
        &self.record.source
    }
    pub(crate) fn intent_bytes(&self) -> &[u8] {
        &self.record.intent_bytes
    }
    pub(crate) fn intent_sha256(&self) -> &str {
        &self.record.intent_sha256
    }
    pub(crate) fn native_preimage_sha256(&self) -> &str {
        &self.record.selected_native_preimage_sha256
    }
    pub(crate) fn catalogue_preimage_sha256(&self) -> &str {
        &self.record.selected_catalogue_preimage_sha256
    }
    pub(crate) fn pointer(&self) -> NativeJoinedRemoteProposalPointer {
        self.record.pointer()
    }
    pub(crate) fn require_mutation_current(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        scope: &NativeJoinedRemoteMutationScope<'_>,
    ) -> Result<()> {
        scope.require_store(db, store)?;
        let actual = db.read_native_joined_remote_proposal_record_point(
            store.native_writer_store_open_id(),
            scope.native(),
            scope.range_id(),
            scope.attempt_id(),
            scope.original_writer_sha256(),
            &self.pointer(),
        )?;
        if crate::distribution_range_image::bounded_json(&actual.record, MAX_BYTES)?
            != crate::distribution_range_image::bounded_json(&self.record, MAX_BYTES)?
        {
            return Err(invalid("Joined row context changed exact proposal"));
        }
        scope.require_store(db, store)
    }
    /// Original proposal integrity for a DISTINCT retained catalogue retry.
    /// Ordinary mutation still requires require_mutation_current/full custody.
    pub(crate) fn require_catalogue_identity(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        scope: &NativeJoinedRemoteMutationScope<'_>,
    ) -> Result<()> {
        scope.require_store_identity(db, store)?;
        let actual = db.read_native_joined_remote_proposal_record_point(
            store.native_writer_store_open_id(),
            scope.native(),
            scope.range_id(),
            scope.attempt_id(),
            scope.original_writer_sha256(),
            &self.pointer(),
        )?;
        if crate::distribution_range_image::bounded_json(&actual.record, MAX_BYTES)?
            != crate::distribution_range_image::bounded_json(&self.record, MAX_BYTES)?
        {
            return Err(invalid(
                "Joined catalogue changed original exact native proposal",
            ));
        }
        scope.require_store_identity(db, store)
    }
    pub(in crate::db::range_image_native) fn require_guarded_transaction_current(
        &self,
        db: &BicDb,
        native: &NativeRecoveryAdmission,
        lexical: &writer_enrollment::JoinedRemoteMutationScopeLiveness,
    ) -> Result<()> {
        lexical.require_current(db, self.record.attempt, self.record.range)?;
        let actual = db.read_native_joined_remote_proposal_record_point(
            self.record.target_store_open,
            native,
            self.record.range,
            self.record.attempt,
            &self.record.original_writer_policy_sha256,
            &self.pointer(),
        )?;
        if crate::distribution_range_image::bounded_json(&actual.record, MAX_BYTES)?
            != crate::distribution_range_image::bounded_json(&self.record, MAX_BYTES)?
        {
            return Err(invalid("Joined guarded row proposal readback changed"));
        }
        lexical.require_current(db, self.record.attempt, self.record.range)
    }
    pub(crate) fn require_current(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        scope: &NativeJoinedRemoteAttemptScope<'_>,
    ) -> Result<()> {
        let actual = db.read_native_joined_remote_proposal(store, scope, &self.pointer())?;
        if crate::distribution_range_image::bounded_json(&actual.record, MAX_BYTES)?
            != crate::distribution_range_image::bounded_json(&self.record, MAX_BYTES)?
        {
            return Err(invalid("Joined retained proposal exact readback changed"));
        }
        Ok(())
    }
}
enum PublicationState {
    Prepared(JoinedProposalV2),
    NativeBegun {
        record: JoinedProposalV2,
    },
    Attempted {
        record: JoinedProposalV2,
        bytes: Vec<u8>,
    },
    NativeCommitted {
        record: JoinedProposalV2,
        bytes: Vec<u8>,
    },
    Verified {
        receipt: NativeJoinedRemoteProposalReceipt,
        bytes: Vec<u8>,
    },
}
/// The actual host must retain this owner before advance. A post-WAL failure
/// never creates another xid or forgets the exact attempted bytes.
pub(crate) struct NativeJoinedRemoteProposalPublication {
    state: PublicationState,
    // Assigned as the FIRST closure operation after actual native begin,
    // before scope/framing/callback work can fail. Never reset for a retry.
    issued_native_xid: Option<u64>,
    commit_returned: Option<u64>,
}
impl NativeJoinedRemoteProposalPublication {
    pub(crate) fn begin(
        db: &BicDb,
        store: &RangeWriteStore,
        scope: &NativeJoinedRemoteAttemptScope<'_>,
    ) -> Result<Self> {
        scope.require_initial_preimage(db, store)?;
        let actual = scope.preflight();
        let native = scope.native();
        let safety = actual.safety();
        let own = scope.request().own();
        let record = JoinedProposalV2 {
            version: 2,
            attempt: scope.attempt_id(),
            range: scope.range_id(),
            source: actual.source().manifest().clone(),
            intent_bytes: actual.intent().canonical_intent_bytes()?,
            intent_sha256: actual.intent().sha256()?,
            target_birth: crate::distribution::NativeStoreBirthV1::from_actual_native(native)?,
            target_database_open: native.database_incarnation().into(),
            target_store_open: actual.selected().catalogue().store_open_id(),
            target_catalogue: actual.selected().catalogue().catalogue_id(),
            target_namespaces: actual.bindings().namespace_ids(),
            original_writer_policy_sha256: scope.original_writer_sha256().into(),
            original_observed_b: db.current_commit_seq(),
            selected_native_preimage_sha256: safety.records_sha256().into(),
            selected_irreversible_sha256: safety.irreversible_records_sha256().into(),
            selected_catalogue_preimage_sha256: actual
                .selected()
                .catalogue()
                .constraint_sha256()?,
            metadata_store: scope.request().member().store_id(),
            metadata_context: own.context_id(),
            metadata_term: own.term(),
            metadata_index: own.read_index(),
            native_page_xid: 0,
            checksum_sha256: String::new(),
        };
        let mut sized = record.clone();
        sized.native_page_xid = u64::MAX;
        sized.checksum_sha256 = sized.checksum()?;
        sized.validate()?;
        Ok(Self {
            state: PublicationState::Prepared(record),
            issued_native_xid: None,
            commit_returned: None,
        })
    }
    #[cfg(test)]
    pub(crate) fn test_outcome(&self) -> serde_json::Value {
        let (phase, record, bytes) = match &self.state {
            PublicationState::Prepared(record) => ("prepared_no_native_begin", record, None),
            PublicationState::NativeBegun { record } => {
                ("actual_native_began_before_candidate", record, None)
            }
            PublicationState::Attempted { record, bytes } => {
                ("attempted_outcome_unproven", record, Some(bytes))
            }
            PublicationState::NativeCommitted { record, bytes } => (
                "native_commit_returned_before_postchecks",
                record,
                Some(bytes),
            ),
            PublicationState::Verified { receipt, bytes } => (
                "exact_current_archive_verified",
                &receipt.record,
                Some(bytes),
            ),
        };
        serde_json::json!({"phase":phase,"issued_native_xid":self.issued_native_xid,
            "actual_commit_returned_xid":self.commit_returned,
            "retained_record_native_xid":record.native_page_xid,
            "attempt":record.attempt,"range":record.range,
            "candidate_bytes_sha256":bytes.map(|b| hex::encode(Sha256::digest(b))),
            "pointer":self.pointer(),"DATA_not_authority":true})
    }
    pub(crate) fn receipt(&self) -> Option<&NativeJoinedRemoteProposalReceipt> {
        match &self.state {
            PublicationState::Verified { receipt, .. } => Some(receipt),
            _ => None,
        }
    }
    pub(crate) fn pointer(&self) -> Option<NativeJoinedRemoteProposalPointer> {
        match &self.state {
            PublicationState::Prepared(_) | PublicationState::NativeBegun { .. } => None,
            PublicationState::Attempted { record, .. }
            | PublicationState::NativeCommitted { record, .. } => Some(record.pointer()),
            PublicationState::Verified { receipt, .. } => Some(receipt.pointer()),
        }
    }
    pub(crate) fn advance(
        &mut self,
        db: &BicDb,
        store: &RangeWriteStore,
        scope: &NativeJoinedRemoteAttemptScope<'_>,
    ) -> Result<NativeJoinedRemoteProposalPointer> {
        scope.require_initial_preimage(db, store)?;
        crate::format::enable_native_joined_remote_receive(db.data_path())?;
        if let PublicationState::Prepared(record) = &self.state {
            if self.issued_native_xid.is_some() {
                return Err(invalid(
                    "Joined native begin already issued; no replacement transaction",
                ));
            }
            if record.attempt != scope.attempt_id() {
                return Err(invalid("Joined proposal changed original attempt"));
            }
            if db
                .read_native_range_image_record::<serde_json::Value>(&current(record.attempt))?
                .is_some()
            {
                return Err(invalid(
                    "Joined proposal CURRENT already exists; no replacement attempt",
                ));
            }
            let mut record = record.clone();
            record.original_observed_b = db.current_commit_seq();
            let result = db.in_paged_transaction(|paged, xid| {
                // Actual begin precedes this closure. Retain its issued xid
                // before EVERY fallible request check, framing, or test cut.
                self.issued_native_xid = Some(xid);
                record.native_page_xid = xid;
                self.state = PublicationState::NativeBegun {
                    record: record.clone(),
                };
                after_begin::fire_if_test();
                scope.require_initial_preimage(db, store)?;
                record.checksum_sha256 = record.checksum()?;
                record.validate()?;
                let bytes = crate::distribution_range_image::bounded_json(&record, MAX_BYTES)?;
                self.state = PublicationState::Attempted {
                    record: record.clone(),
                    bytes: bytes.clone(),
                };
                // Actual candidate/xid/bytes are retained before either put.
                for key in [archive(record.attempt, xid), current(record.attempt)] {
                    paged
                        .store()
                        .put(xid, &native_key(&key), &bytes)
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                }
                scope.require_initial_preimage(db, store)
            });
            match result {
                Ok(Some(xid)) => {
                    self.commit_returned = Some(xid);
                    let PublicationState::Attempted { record, bytes } = &self.state else {
                        return Err(invalid("Joined proposal lost original attempted outcome"));
                    };
                    if record.native_page_xid != xid {
                        return Err(invalid("Joined native returned a different original xid"));
                    }
                    self.state = PublicationState::NativeCommitted {
                        record: record.clone(),
                        bytes: bytes.clone(),
                    };
                }
                Ok(None) => return Err(invalid("Joined proposal requires native page store")),
                Err(error) => return Err(error),
            }
        }
        let (record, bytes) = match &self.state {
            PublicationState::Attempted { record, bytes }
            | PublicationState::NativeCommitted { record, bytes } => {
                (record.clone(), bytes.clone())
            }
            PublicationState::Verified { receipt, bytes } => {
                (receipt.record.clone(), bytes.clone())
            }
            PublicationState::Prepared(_) => {
                return Err(invalid("Joined proposal was never attempted"))
            }
            PublicationState::NativeBegun { record } => {
                if self.issued_native_xid != Some(record.native_page_xid) {
                    return Err(invalid("Joined original native begin custody inconsistent"));
                }
                return Err(invalid("Joined original native begin has no framed candidate; retained xid cannot be replaced"));
            }
        };
        if self.issued_native_xid != Some(record.native_page_xid) {
            return Err(invalid(
                "Joined attempted candidate lost its actual issued native xid",
            ));
        }
        scope.require_initial_preimage(db, store)?;
        if record.intent_bytes != scope.preflight().intent().canonical_intent_bytes()?
            || record.source.checksum_sha256
                != scope.preflight().source().manifest().checksum_sha256
            || record.original_writer_policy_sha256 != scope.original_writer_sha256()
            || scope.request().own().term() < record.metadata_term
            || scope.request().own().read_index() < record.metadata_index
        {
            return Err(invalid(
                "Joined fresh continuation changed original proposal/native attempt",
            ));
        }
        let receipt = db.read_native_joined_remote_proposal(store, scope, &record.pointer())?;
        if crate::distribution_range_image::bounded_json(&receipt.record, MAX_BYTES)? != bytes {
            return Err(invalid(
                "Joined original attempted proposal readback differs",
            ));
        }
        scope.require_initial_preimage(db, store)?;
        let pointer = receipt.pointer();
        self.state = PublicationState::Verified { receipt, bytes };
        Ok(pointer)
    }
}
impl BicDb {
    pub(crate) fn read_native_joined_remote_proposal(
        &self,
        store: &RangeWriteStore,
        scope: &NativeJoinedRemoteAttemptScope<'_>,
        pointer: &NativeJoinedRemoteProposalPointer,
    ) -> Result<NativeJoinedRemoteProposalReceipt> {
        scope.require_store_identity(self, store)?;
        let result = self.read_native_joined_remote_proposal_record_point(
            store.native_writer_store_open_id(),
            scope.native(),
            scope.range_id(),
            scope.attempt_id(),
            scope.original_writer_sha256(),
            pointer,
        )?;
        scope.require_store_identity(self, store)?;
        Ok(result)
    }
    pub(crate) fn read_native_joined_remote_proposal_for_mutation(
        &self,
        store: &RangeWriteStore,
        scope: &NativeJoinedRemoteMutationScope<'_>,
        pointer: &NativeJoinedRemoteProposalPointer,
    ) -> Result<NativeJoinedRemoteProposalReceipt> {
        scope.require_store(self, store)?;
        let result = self.read_native_joined_remote_proposal_record_point(
            store.native_writer_store_open_id(),
            scope.native(),
            scope.range_id(),
            scope.attempt_id(),
            scope.original_writer_sha256(),
            pointer,
        )?;
        scope.require_store(self, store)?;
        Ok(result)
    }
    /// DISTINCT retained catalogue retry integrity read. The lexical original
    /// mutation scope still binds State/open/A/fresh request, not file health.
    pub(crate) fn read_native_joined_remote_proposal_for_catalogue(
        &self,
        store: &RangeWriteStore,
        scope: &NativeJoinedRemoteMutationScope<'_>,
        pointer: &NativeJoinedRemoteProposalPointer,
    ) -> Result<NativeJoinedRemoteProposalReceipt> {
        scope.require_store_identity(self, store)?;
        let actual = self.read_native_joined_remote_proposal_record_point(
            store.native_writer_store_open_id(),
            scope.native(),
            scope.range_id(),
            scope.attempt_id(),
            scope.original_writer_sha256(),
            pointer,
        )?;
        actual.require_catalogue_identity(self, store, scope)?;
        Ok(actual)
    }
    // Exact protected integrity reader only. The real lexical scope and
    // fresh request remain mandatory outside this private DATA reader.
    fn read_native_joined_remote_proposal_record_point(
        &self,
        store_open: Uuid,
        native: &NativeRecoveryAdmission,
        range: RangeId,
        attempt: Uuid,
        writer_sha256: &str,
        pointer: &NativeJoinedRemoteProposalPointer,
    ) -> Result<NativeJoinedRemoteProposalReceipt> {
        native.require_current(self)?;
        if !crate::format::native_joined_remote_receive_active(self.data_path())? {
            return Err(invalid("Joined proposal sticky v2 reader fence absent"));
        }
        let record: JoinedProposalV2 = self
            .read_native_range_image_record(&current(pointer.attempt_id))?
            .ok_or_else(|| invalid("Joined attempted CURRENT absent; outcome unproven"))?;
        let archived: JoinedProposalV2 = self
            .read_native_range_image_record(&archive(pointer.attempt_id, pointer.native_page_xid))?
            .ok_or_else(|| invalid("Joined original archive absent; outcome unproven"))?;
        record.validate()?;
        archived.validate()?;
        if record.pointer() != *pointer
            || archived.pointer() != *pointer
            || record.attempt != attempt
            || record.range != range
            || record.target_store_open != store_open
            || record.target_database_open != native.database_incarnation()
            || record.target_birth
                != crate::distribution::NativeStoreBirthV1::from_actual_native(native)?
            || record.target_namespaces != self.capture_range_image_bindings()?.namespace_ids()
            || self.native_abort_catalogue_store_id()? != Some(record.target_catalogue)
            || record.original_writer_policy_sha256 != writer_sha256
            || crate::distribution_range_image::bounded_json(&record, MAX_BYTES)?
                != crate::distribution_range_image::bounded_json(&archived, MAX_BYTES)?
        {
            return Err(invalid(
                "Joined protected CURRENT/archive/actual origin differs",
            ));
        }
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("Joined proposal native page store absent"))?;
        if paged.store().transactions().status(record.native_page_xid)
            != bicdb_page::TxStatus::Committed
        {
            return Err(invalid(
                "Joined exact original native xid not proven committed; retain uncertainty",
            ));
        }
        Ok(NativeJoinedRemoteProposalReceipt { record })
    }
}

mod after_begin {
    #[cfg(test)]
    pub(super) fn fire_if_test() {
        super::begin_cut::fire();
    }
    #[cfg(not(test))]
    pub(super) fn fire_if_test() {}
}
#[cfg(test)]
mod begin_cut {
    use std::cell::RefCell;
    type Cut = Box<dyn FnOnce()>;
    thread_local! { static CUT: RefCell<Option<Cut>> = const { RefCell::new(None) }; }
    pub(crate) struct Guard {
        previous: Option<Cut>,
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            CUT.with(|c| {
                c.replace(self.previous.take());
            });
        }
    }
    pub(crate) fn install(cut: Cut) -> Guard {
        Guard {
            previous: CUT.with(|c| c.replace(Some(cut))),
        }
    }
    pub(super) fn fire() {
        let cut = CUT.with(|c| c.borrow_mut().take());
        if let Some(cut) = cut {
            cut();
        }
    }
}
#[cfg(test)]
pub(crate) use begin_cut::install as test_joined_remote_proposal_after_native_begin;
