//! Exact retained Registered Installing proposal. No rows or catalogue grant.
use super::*;
use crate::distribution_native_host::NativeRegisteredRemoteProposalPoint;
use crate::distribution_range_consensus::RangeWriteStore;
use crate::distribution_range_image::NativeRemoteImageManifestV1;
use sha2::{Digest, Sha256};

const MAX_BYTES: usize = 4 * 1024 * 1024;
fn current(id: Uuid) -> String {
    format!("registered-remote-image-proposal/{id}/current")
}
fn archive(id: Uuid, xid: u64) -> String {
    format!("registered-remote-image-proposal/{id}/archive/{xid:020}")
}
fn digest(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRegisteredRemoteProposalPointer {
    pub(crate) attempt_id: Uuid,
    pub(crate) native_page_xid: u64,
    pub(crate) checksum_sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Preimage {
    base: Option<u64>,
    applied: Option<u64>,
    pending: Option<u64>,
    materialized: Option<u64>,
    native_records_sha256: String,
    irreversible_sha256: String,
    raw_sha256: String,
    original_observed_b: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
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
    metadata_store: Uuid,
    metadata_context: Uuid,
    metadata_term: u64,
    metadata_index: u64,
    preimage: Preimage,
    native_page_xid: u64,
    checksum_sha256: String,
}
impl Record {
    fn checksum(&self) -> Result<String> {
        let mut body = self.clone();
        body.checksum_sha256.clear();
        Ok(hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &("native-registered-remote-proposal-v1", body),
                MAX_BYTES,
            )?,
        )))
    }
    fn validate(&self) -> Result<()> {
        self.source.require_shape()?;
        let _: serde_json::Value = serde_json::from_slice(&self.intent_bytes)?;
        if self.version != 1
            || self.attempt.is_nil()
            || self.native_page_xid == 0
            || self.attempt != self.source.target_attempt
            || self.range != self.source.capture_range.id
            || self.target_birth != self.source.target_birth
            || self.target_database_open.is_empty()
            || self.target_store_open.is_nil()
            || self.target_catalogue.is_nil()
            || self.metadata_store.is_nil()
            || self.metadata_context.is_nil()
            || self.metadata_term == 0
            || self.intent_bytes.len() > 128 * 1024
            || !digest(&self.intent_sha256)
            || self.intent_sha256 != hex::encode(Sha256::digest(&self.intent_bytes))
            || self.target_namespaces.len() > 65536
            || self.target_namespaces.windows(2).any(|p| p[0].0 >= p[1].0)
            || self
                .target_namespaces
                .iter()
                .any(|(name, id)| name.is_empty() || id.is_nil())
            || self.preimage.base.is_some()
            || self.preimage.applied.is_some()
            || self.preimage.pending.is_some()
            || self.preimage.materialized.is_some()
            || self.preimage.original_observed_b != 0
            || !digest(&self.preimage.native_records_sha256)
            || !digest(&self.preimage.irreversible_sha256)
            || !digest(&self.preimage.raw_sha256)
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged Registered remote proposal record"));
        }
        Ok(())
    }
    fn pointer(&self) -> NativeRegisteredRemoteProposalPointer {
        NativeRegisteredRemoteProposalPointer {
            attempt_id: self.attempt,
            native_page_xid: self.native_page_xid,
            checksum_sha256: self.checksum_sha256.clone(),
        }
    }
}
pub(crate) struct NativeRegisteredRemoteProposalReceipt {
    record: Record,
}
impl std::fmt::Debug for NativeRegisteredRemoteProposalReceipt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeRegisteredRemoteProposalReceipt")
            .field("pointer", &self.record.pointer())
            .finish_non_exhaustive()
    }
}
impl NativeRegisteredRemoteProposalReceipt {
    pub(crate) fn pointer(&self) -> NativeRegisteredRemoteProposalPointer {
        self.record.pointer()
    }
    pub(crate) fn source_manifest(&self) -> &NativeRemoteImageManifestV1 {
        &self.record.source
    }
    pub(crate) fn intent_bytes(&self) -> &[u8] {
        &self.record.intent_bytes
    }
    pub(crate) fn intent_sha256(&self) -> &str {
        &self.record.intent_sha256
    }
    pub(crate) fn native_records_sha256(&self) -> &str {
        &self.record.preimage.native_records_sha256
    }
    pub(crate) fn raw_preimage_sha256(&self) -> &str {
        &self.record.preimage.raw_sha256
    }
    pub(crate) fn require_current(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        scope: &NativeRegisteredRemoteAttemptScope<'_>,
    ) -> Result<()> {
        let actual = db.read_native_registered_remote_proposal(store, scope, &self.pointer())?;
        if crate::distribution_range_image::bounded_json(&actual.record, MAX_BYTES)?
            != crate::distribution_range_image::bounded_json(&self.record, MAX_BYTES)?
        {
            return Err(invalid("Registered protected proposal readback changed"));
        }
        Ok(())
    }
}
pub(crate) struct NativeRegisteredRemoteProposalPublication {
    state: State,
    /// Actual paged-commit return, retained before all later validation. This
    /// is outcome custody only, never receipt/row/catalogue authority.
    commit_returned: Option<u64>,
}
enum State {
    Prepared(Record),
    Attempted {
        record: Record,
        bytes: Vec<u8>,
    },
    NativeCommitted {
        record: Record,
        bytes: Vec<u8>,
    },
    Verified {
        receipt: NativeRegisteredRemoteProposalReceipt,
        bytes: Vec<u8>,
    },
}
impl NativeRegisteredRemoteProposalPublication {
    pub(crate) fn receipt(&self) -> Option<&NativeRegisteredRemoteProposalReceipt> {
        match &self.state {
            State::Verified { receipt, .. } => Some(receipt),
            _ => None,
        }
    }
    #[cfg(test)]
    pub(crate) fn test_outcome(&self) -> serde_json::Value {
        let (outcome, bytes) = match &self.state {
            State::Prepared(_) => ("prepared_no_native_xid", None),
            State::Attempted { bytes, .. } => ("attempted_outcome_unproven", Some(bytes)),
            State::NativeCommitted { bytes, .. } => {
                ("native_commit_returned_before_postchecks", Some(bytes))
            }
            State::Verified { bytes, .. } => ("exact_current_archive_verified", Some(bytes)),
        };
        serde_json::json!({"outcome":outcome,"attempted_pointer":self.pointer(),
            "actual_commit_returned_xid":self.commit_returned,
            "attempted_bytes_sha256":bytes.map(|b|hex::encode(Sha256::digest(b)))})
    }
    pub(crate) fn begin(point: &NativeRegisteredRemoteProposalPoint<'_>) -> Result<Self> {
        point.require_live()?;
        let actual = point.preflight();
        let native = actual.native();
        let safety = actual.safety();
        let own = point.request().own();
        let record = Record {
            version: 1,
            attempt: actual.attempt_id(),
            range: actual.scope().range_id,
            source: actual.source().manifest().clone(),
            intent_bytes: actual.intent().canonical_intent_bytes()?,
            intent_sha256: actual.intent().sha256()?,
            target_birth: crate::distribution::NativeStoreBirthV1::from_actual_native(native)?,
            target_database_open: native.database_incarnation().into(),
            target_store_open: actual.catalogue().store_open_id(),
            target_catalogue: actual.catalogue().catalogue_id(),
            target_namespaces: actual.bindings().namespace_ids(),
            metadata_store: point.request().member().store_id(),
            metadata_context: own.context_id(),
            metadata_term: own.term(),
            metadata_index: own.read_index(),
            preimage: Preimage {
                base: safety.base_index(),
                applied: safety.applied_index(),
                pending: safety.pending_index(),
                materialized: safety.materialized_pending_index(),
                native_records_sha256: safety.records_sha256().into(),
                irreversible_sha256: safety.irreversible_records_sha256().into(),
                raw_sha256: actual.catalogue().constraint_sha256()?,
                original_observed_b: 0,
            },
            native_page_xid: 0,
            checksum_sha256: String::new(),
        };
        let mut sized = record.clone();
        sized.native_page_xid = u64::MAX;
        sized.checksum_sha256 = sized.checksum()?;
        sized.validate()?;
        Ok(Self {
            state: State::Prepared(record),
            commit_returned: None,
        })
    }
    pub(crate) fn pointer(&self) -> Option<NativeRegisteredRemoteProposalPointer> {
        match &self.state {
            State::Prepared(_) => None,
            State::Attempted { record, .. } | State::NativeCommitted { record, .. } => {
                Some(record.pointer())
            }
            State::Verified { receipt, .. } => Some(receipt.pointer()),
        }
    }
    pub(crate) fn advance(
        &mut self,
        db: &BicDb,
        store: &RangeWriteStore,
        point: &NativeRegisteredRemoteProposalPoint<'_>,
        scope: &NativeRegisteredRemoteAttemptScope<'_>,
    ) -> Result<NativeRegisteredRemoteProposalPointer> {
        point.require_database_store(db, store, scope)?;
        // A prior rename can make the flag visible even when its directory
        // sync failed. Re-persist and sync it on EVERY continuation before
        // the first put or an attempted/verified proposal is relied upon.
        crate::format::enable_native_registered_remote_proposal(db.data_path())?;
        if let State::Prepared(record) = &self.state {
            if record.attempt != point.preflight().attempt_id() {
                return Err(invalid("Registered proposal attempt changed"));
            }
            scope.require_inventory(db, &[])?;
            point.require_database_store(db, store, scope)?;
            let mut record = record.clone();
            // Observe the actual scoped DB watermark. This is not a logical
            // range floor or a None→0 conversion of missing native history.
            record.preimage.original_observed_b = db.current_commit_seq();
            let outcome = db.in_paged_transaction(|paged, xid| {
                point.require_database_store(db, store, scope)?;
                record.native_page_xid = xid;
                record.checksum_sha256 = record.checksum()?;
                record.validate()?;
                let bytes = crate::distribution_range_image::bounded_json(&record, MAX_BYTES)?;
                // Exact original xid and attempted bytes retained BEFORE put.
                self.state = State::Attempted {
                    record: record.clone(),
                    bytes: bytes.clone(),
                };
                for key in [archive(record.attempt, xid), current(record.attempt)] {
                    paged
                        .store()
                        .put(xid, &native_key(&key), &bytes)
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                }
                point.require_database_store(db, store, scope)
            });
            match outcome {
                Ok(Some(xid)) => {
                    self.commit_returned = Some(xid);
                    // Native success is retained BEFORE cancellation, readback
                    // or any other fallible observation after the transaction.
                    let State::Attempted { record, bytes } = &self.state else {
                        return Err(invalid("Registered attempted publication missing"));
                    };
                    if record.native_page_xid != xid {
                        return Err(invalid(
                            "Registered native returned xid differs from retained attempted xid",
                        ));
                    }
                    self.state = State::NativeCommitted {
                        record: record.clone(),
                        bytes: bytes.clone(),
                    };
                }
                Ok(None) => return Err(invalid("Registered proposal has no native page store")),
                Err(error) => return Err(error),
            }
            after_wal::fire_if_test();
        }
        let (record, bytes) = match &self.state {
            State::Attempted { record, bytes } | State::NativeCommitted { record, bytes } => {
                (record.clone(), bytes.clone())
            }
            State::Verified { receipt, bytes } => (receipt.record.clone(), bytes.clone()),
            State::Prepared(_) => {
                return Err(invalid(
                    "Registered proposal never attempted native publication",
                ))
            }
        };
        point.require_database_store(db, store, scope)?;
        if point.preflight().intent().canonical_intent_bytes()? != record.intent_bytes
            || point.preflight().source().manifest().checksum_sha256
                != record.source.checksum_sha256
            || point.request().own().term() < record.metadata_term
            || point.request().own().read_index() < record.metadata_index
        {
            return Err(invalid(
                "fresh Registered reconciliation changed original attempted proposal",
            ));
        }
        scope.require_inventory(
            db,
            &[
                current(record.attempt),
                archive(record.attempt, record.native_page_xid),
            ],
        )?;
        let receipt = db.read_native_registered_remote_proposal(store, scope, &record.pointer())?;
        if crate::distribution_range_image::bounded_json(&receipt.record, MAX_BYTES)? != bytes {
            return Err(invalid(
                "Registered attempted proposal exact readback differs",
            ));
        }
        point.require_database_store(db, store, scope)?;
        let pointer = receipt.pointer();
        self.state = State::Verified { receipt, bytes };
        Ok(pointer)
    }
}
impl BicDb {
    pub(crate) fn read_native_registered_remote_proposal(
        &self,
        store: &RangeWriteStore,
        scope: &NativeRegisteredRemoteAttemptScope<'_>,
        pointer: &NativeRegisteredRemoteProposalPointer,
    ) -> Result<NativeRegisteredRemoteProposalReceipt> {
        scope.require_database(self)?;
        let native = scope.native();
        native.require_current(self)?;
        if !crate::format::native_registered_remote_proposal_active(self.data_path())? {
            return Err(invalid(
                "Registered proposal sticky local reader fence missing",
            ));
        }
        let record: Record = self
            .read_native_range_image_record(&current(pointer.attempt_id))?
            .ok_or_else(|| {
                invalid("Registered attempted proposal CURRENT absent; outcome not proven")
            })?;
        record.validate()?;
        let archived: Record = self
            .read_native_range_image_record(&archive(pointer.attempt_id, pointer.native_page_xid))?
            .ok_or_else(|| {
                invalid("Registered attempted proposal archive absent; outcome not proven")
            })?;
        archived.validate()?;
        if record.pointer() != *pointer
            || archived.pointer() != *pointer
            || record.range != scope.range_id()
            || record.attempt != scope.attempt_id()
            || record.target_store_open != store.native_writer_store_open_id()
            || crate::distribution_range_image::bounded_json(&record, MAX_BYTES)?
                != crate::distribution_range_image::bounded_json(&archived, MAX_BYTES)?
            || record.target_birth
                != crate::distribution::NativeStoreBirthV1::from_actual_native(native)?
            || record.target_database_open != native.database_incarnation()
            || record.target_namespaces != self.capture_range_image_bindings()?.namespace_ids()
            || self.native_abort_catalogue_store_id()? != Some(record.target_catalogue)
        {
            return Err(invalid(
                "Registered protected CURRENT/archive/native identity differs",
            ));
        }
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("Registered proposal page store missing"))?;
        if paged.store().transactions().status(record.native_page_xid)
            != bicdb_page::TxStatus::Committed
        {
            return Err(invalid("Registered original proposal xid is not proven committed; exact attempted outcome retained"));
        }
        self.check_recovery_state()?;
        Ok(NativeRegisteredRemoteProposalReceipt { record })
    }
    #[cfg(test)]
    pub(crate) fn test_registered_remote_proposal_record(
        &self,
        attempt: Uuid,
    ) -> Result<Option<serde_json::Value>> {
        let _journal = self.try_native_image_journal()?;
        let record: Option<Record> = self.read_native_range_image_record(&current(attempt))?;
        if let Some(record) = &record {
            record.validate()?;
        }
        record
            .map(|r| serde_json::to_value(r).map_err(Into::into))
            .transpose()
    }
    #[cfg(test)]
    pub(crate) fn test_registered_remote_native_inventory(
        &self,
    ) -> Result<BTreeMap<String, String>> {
        let _journal = self.try_native_image_journal()?;
        self.check_recovery_state()?;
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("actual proposal inventory page store absent"))?;
        let ns = crate::paged_collection::RANGE_IMAGE_NATIVE_NS_V1;
        let snapshot = paged.latest_snapshot();
        let mut out = BTreeMap::new();
        for entry in paged
            .store()
            .scan_from(&snapshot, &ns)
            .map_err(|e| BicDbError::PagedStorage(e.to_string()))?
        {
            let (key, value) = entry.map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
            if !key.starts_with(&ns) {
                break;
            }
            if out.len() >= 8 || key.len() > 256 || value.len() > MAX_NATIVE_RECEIPT_BYTES {
                return Err(invalid("actual proposal inventory diagnostic bound"));
            }
            let key = std::str::from_utf8(&key[ns.len()..])
                .map_err(|_| invalid("actual proposal inventory malformed native key"))?;
            out.insert(key.to_string(), hex::encode(Sha256::digest(&value)));
        }
        Ok(out)
    }
}
#[cfg(test)]
pub(crate) fn test_fire_before_registered_remote_proposal_scope() {
    cuts::fire(false);
}
mod after_wal {
    #[cfg(test)]
    pub(super) fn fire_if_test() {
        super::cuts::fire(true);
    }
    #[cfg(not(test))]
    pub(super) fn fire_if_test() {}
}
#[cfg(test)]
mod cuts {
    use std::cell::RefCell;
    type Cut = Box<dyn FnOnce()>;
    thread_local! {static BEFORE:RefCell<Option<Cut>>=const{RefCell::new(None)};static AFTER:RefCell<Option<Cut>>=const{RefCell::new(None)};}
    pub(crate) struct Guard {
        after: bool,
        previous: Option<Cut>,
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            if self.after {
                AFTER.with(|c| {
                    c.replace(self.previous.take());
                });
            } else {
                BEFORE.with(|c| {
                    c.replace(self.previous.take());
                });
            }
        }
    }
    pub(crate) fn install(after: bool, cut: Cut) -> Guard {
        let previous = if after {
            AFTER.with(|c| c.replace(Some(cut)))
        } else {
            BEFORE.with(|c| c.replace(Some(cut)))
        };
        Guard { after, previous }
    }
    pub(super) fn fire(after: bool) {
        let cut = if after {
            AFTER.with(|c| c.borrow_mut().take())
        } else {
            BEFORE.with(|c| c.borrow_mut().take())
        };
        if let Some(cut) = cut {
            cut();
        }
    }
}
#[cfg(test)]
pub(crate) use cuts::install as test_registered_remote_proposal_cut;
