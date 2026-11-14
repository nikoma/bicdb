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
/// Integrity DATA only. It cannot construct an actual scope/lease/receipt.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeJoinedRemoteProposalPointer {
    pub attempt_id: Uuid,
    pub native_page_xid: u64,
    pub checksum_sha256: String,
}
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
    pub(crate) fn pointer(&self) -> NativeJoinedRemoteProposalPointer {
        self.record.pointer()
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
            commit_returned: None,
        })
    }
    pub(crate) fn receipt(&self) -> Option<&NativeJoinedRemoteProposalReceipt> {
        match &self.state {
            PublicationState::Verified { receipt, .. } => Some(receipt),
            _ => None,
        }
    }
    pub(crate) fn pointer(&self) -> Option<NativeJoinedRemoteProposalPointer> {
        match &self.state {
            PublicationState::Prepared(_) => None,
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
                scope.require_initial_preimage(db, store)?;
                record.native_page_xid = xid;
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
        };
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
        scope.require_initial_preimage(self, store)?;
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
            || record.attempt != scope.attempt_id()
            || record.range != scope.range_id()
            || record.target_store_open != store.native_writer_store_open_id()
            || record.target_database_open != scope.native().database_incarnation()
            || record.target_birth
                != crate::distribution::NativeStoreBirthV1::from_actual_native(scope.native())?
            || record.target_namespaces != self.capture_range_image_bindings()?.namespace_ids()
            || self.native_abort_catalogue_store_id()? != Some(record.target_catalogue)
            || record.original_writer_policy_sha256 != scope.original_writer_sha256()
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
        scope.require_initial_preimage(self, store)?;
        Ok(NativeJoinedRemoteProposalReceipt { record })
    }
}
