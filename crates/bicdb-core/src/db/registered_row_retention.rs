//! Restrictive durable row-WAL debt. None of these DATA records grants recovery,
//! mutation, a fresh transaction, native visibility, or Serving.
use super::*;
use crate::RangeId;
use serde::{Deserialize, Serialize};
use sha2::Digest;

const PREFIX: &str = "registered-row-wal-pending-v1/";
const MAX_ENTRIES: usize = 4096;
const MAX_ENTRY_BYTES: usize = 16 * 1024;
const MAX_TOTAL_BYTES: usize = 16 * 1024 * 1024;
fn refusal(message: &str) -> BicDbError {
    BicDbError::HighAvailability(format!("Registered row WAL preservation: {message}"))
}
fn bounded(value: &impl Serialize, maximum: usize) -> Result<Vec<u8>> {
    // The shared writer stops during serialization, before oversized DATA
    // could allocate an unbounded temporary buffer.
    crate::distribution_range_image::bounded_json(value, maximum)
}
fn native_key(key: &str) -> Vec<u8> {
    let mut bytes = crate::paged_collection::RANGE_IMAGE_NATIVE_NS_V1.to_vec();
    bytes.extend_from_slice(key.as_bytes());
    bytes
}
fn intent_key(intent: &RegisteredRowIntent) -> String {
    format!(
        "registered-remote-image/{}/row-intent/{:020}",
        intent.attempt, intent.step
    )
}
fn pending_key(intent: &RegisteredRowIntent) -> Vec<u8> {
    native_key(&format!("{PREFIX}{}/{:020}", intent.attempt, intent.step))
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegisteredRowIntent {
    pub(super) version: u32,
    pub(super) attempt: Uuid,
    pub(super) range: RangeId,
    pub(super) manifest_sha256: String,
    pub(super) history_generation: Uuid,
    pub(super) history_header_sha256: String,
    pub(super) intent_sha256: String,
    pub(super) step: u64,
    pub(super) step_sha256: String,
    pub(super) previous_progress_sha256: Option<String>,
    pub(super) logical_txid: Option<u64>,
    pub(super) mutations_sha256: Option<String>,
    pub(super) native_xid: u64,
    pub(super) checksum_sha256: String,
}
impl RegisteredRowIntent {
    pub(super) fn checksum(&self) -> Result<String> {
        let mut value = self.clone();
        value.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded(
            &("registered-remote-row-intent-v1", value),
            MAX_ENTRY_BYTES,
        )?)))
    }
    pub(super) fn validate(&self) -> Result<()> {
        let hash_ok = |text: &str| text.len() == 64 && hex::decode(text).is_ok();
        if self.version != 1
            || self.range.get() == 0
            || self.step == 0
            || self.logical_txid == Some(0)
            || ![
                &self.manifest_sha256,
                &self.history_header_sha256,
                &self.intent_sha256,
                &self.step_sha256,
            ]
            .into_iter()
            .all(|hash| hash_ok(hash))
            || self
                .mutations_sha256
                .as_deref()
                .is_some_and(|hash| !hash_ok(hash))
            || self
                .previous_progress_sha256
                .as_deref()
                .is_some_and(|hash| !hash_ok(hash))
            || self.attempt.is_nil()
            || self.history_generation.is_nil()
            || self.native_xid == 0
            || self.logical_txid.is_some() != self.mutations_sha256.is_some()
            || self.checksum()? != self.checksum_sha256
        {
            return Err(refusal("damaged Registered remote row intent"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PendingRegisteredRowWal {
    version: u32,
    original_database_open: String,
    native_birth_sha256: String,
    intent: RegisteredRowIntent,
    checksum_sha256: String,
}
impl PendingRegisteredRowWal {
    pub(crate) fn original_database_open(&self) -> &str {
        &self.original_database_open
    }
    pub(crate) fn native_birth_sha256(&self) -> &str {
        &self.native_birth_sha256
    }
    pub(crate) fn intent(&self) -> &RegisteredRowIntent {
        &self.intent
    }

    fn checksum(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded(
            &("registered-row-wal-pending-v1", unsigned),
            MAX_ENTRY_BYTES,
        )?)))
    }
    fn validate(&self) -> Result<()> {
        self.intent.validate()?;
        if self.version != 1
            || self.native_birth_sha256.len() != 64
            || hex::decode(&self.native_birth_sha256).is_err()
            || Uuid::parse_str(&self.original_database_open).is_err()
            || Uuid::parse_str(&self.original_database_open).is_ok_and(|id| id.is_nil())
            || self.checksum()? != self.checksum_sha256
        {
            return Err(refusal("damaged pending index entry"));
        }
        Ok(())
    }
}
/// Complete bounded scan, including the max+1 refusal. Caller serializes native
/// writes (the actual page write guard), or owns the closed directory at open.
fn scan(
    paged: &crate::paged_collection::PagedRecords,
) -> Result<(Vec<PendingRegisteredRowWal>, usize)> {
    let snapshot = paged.latest_snapshot();
    let prefix = native_key(PREFIX);
    let mut entries = Vec::new();
    let mut total = 0usize;
    let cursor = paged
        .store()
        .scan_from_with_heads(&snapshot, &prefix)
        .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
    for item in cursor {
        let (key, bytes, _head, head_xmin) =
            item.map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
        if !key.starts_with(&prefix) {
            break;
        }
        total = total
            .checked_add(bytes.len())
            .ok_or_else(|| refusal("pending size overflow"))?;
        if entries.len() >= MAX_ENTRIES || bytes.len() > MAX_ENTRY_BYTES || total > MAX_TOTAL_BYTES
        {
            return Err(refusal(
                "pending index count/byte bound exceeded; not empty",
            ));
        }
        let entry: PendingRegisteredRowWal = serde_json::from_slice(&bytes)?;
        entry.validate()?;
        if key != pending_key(&entry.intent)
            || head_xmin != entry.intent.native_xid
            || paged.store().transactions().status(entry.intent.native_xid)
                != bicdb_page::TxStatus::Committed
        {
            return Err(refusal("pending key or committed native outcome differs"));
        }
        let original_key = native_key(&intent_key(&entry.intent));
        let mut original_cursor = paged
            .store()
            .scan_from_with_heads(&snapshot, &original_key)
            .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
        let (actual_key, actual, _head, actual_xmin) = original_cursor
            .next()
            .ok_or_else(|| refusal("pending original native intent absent"))?
            .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
        if actual_key != original_key || actual_xmin != entry.intent.native_xid {
            return Err(refusal(
                "pending/index and original intent are not the same actual native transaction",
            ));
        }
        drop(original_cursor);
        if actual.len() > MAX_ENTRY_BYTES {
            return Err(refusal("original intent byte bound exceeded"));
        }
        let intent: RegisteredRowIntent = serde_json::from_slice(&actual)?;
        intent.validate()?;
        if intent != entry.intent {
            return Err(refusal("pending/original native intent mismatch"));
        }
        let birth = paged
            .store()
            .get_as_of(&snapshot, &native_key("birth"))
            .map_err(|error| BicDbError::PagedStorage(error.to_string()))?
            .ok_or_else(|| refusal("original native database birth missing"))?;
        if birth.len() > MAX_ENTRY_BYTES {
            return Err(refusal("native birth byte bound exceeded"));
        }
        let birth: serde_json::Value = serde_json::from_slice(&birth)?;
        if birth["checksum_sha256"].as_str() != Some(entry.native_birth_sha256.as_str()) {
            return Err(refusal("pending original native birth differs"));
        }
        entries.push(entry);
    }
    Ok((entries, total))
}
fn fenced(root: &Path) -> Result<bool> {
    let image = format::native_registered_remote_image_active(root)?;
    let indexed = format::native_registered_row_wal_index_active(root)?;
    if image != indexed {
        return Err(refusal(
            "unindexed Registered prototype or incomplete reader fence is unsupported",
        ));
    }
    Ok(indexed)
}
/// Called after actual page recovery and before ANY generic logical scanner,
/// materialization, outbox replay, or WAL reset. Also called under the actual
/// native cut guard for live cuts, which excludes acquisition of new row pins.
pub(super) fn require_no_outstanding(
    root: &Path,
    paged: Option<&crate::paged_collection::PagedRecords>,
) -> Result<()> {
    let indexed = fenced(root)?;
    let Some(paged) = paged else {
        return if indexed {
            Err(refusal("indexed native store has no page store"))
        } else {
            Ok(())
        };
    };
    let (entries, _) = scan(paged)?;
    if let Some(first) = entries.first() {
        return Err(refusal(&format!("cold repair required before logical replay/cut: {} outstanding; original attempt {} step {} Tx {:?}",
            entries.len(), first.intent.attempt, first.intent.step, first.intent.logical_txid)));
    }
    Ok(())
}
/// SAME actual native intent TX, before any logical WAL. The page write guard
/// held by in_paged_transaction serializes scan+admission+commit for all rows;
/// the already-owned RAM pin excludes an empty-index/cut/insert race.
pub(super) fn insert(
    paged: &crate::paged_collection::PagedRecords,
    xid: u64,
    open: &str,
    native_birth_sha256: &str,
    intent: &RegisteredRowIntent,
) -> Result<()> {
    if intent.native_xid != xid {
        return Err(refusal("index not in original intent transaction"));
    }
    let (entries, total) = scan(paged)?;
    let mut entry = PendingRegisteredRowWal {
        version: 1,
        original_database_open: open.into(),
        native_birth_sha256: native_birth_sha256.into(),
        intent: intent.clone(),
        checksum_sha256: String::new(),
    };
    entry.checksum_sha256 = entry.checksum()?;
    entry.validate()?;
    let bytes = bounded(&entry, MAX_ENTRY_BYTES)?;
    if entries.len() >= MAX_ENTRIES
        || total
            .checked_add(bytes.len())
            .is_none_or(|n| n > MAX_TOTAL_BYTES)
    {
        return Err(refusal("pending index admission exhausted"));
    }
    if entries
        .iter()
        .any(|old| old.intent.attempt == intent.attempt && old.intent.step == intent.step)
    {
        return Err(refusal(
            "original pending step already indexed; no replacement",
        ));
    }
    paged
        .store()
        .put(xid, &pending_key(intent), &bytes)
        .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
    Ok(())
}
pub(super) fn require_exact(
    paged: &crate::paged_collection::PagedRecords,
    open: &str,
    intent: &RegisteredRowIntent,
) -> Result<()> {
    let (entries, _) = scan(paged)?;
    if !entries
        .iter()
        .any(|entry| entry.original_database_open == open && entry.intent == *intent)
    {
        return Err(refusal("exact original committed pending entry missing"));
    }
    Ok(())
}
/// SAME actual native progress TX. Caller has just revalidated the exact
/// original entry and will publish the matching protected RowsHeader in xid.
pub(super) fn clear(
    paged: &crate::paged_collection::PagedRecords,
    xid: u64,
    intent: &RegisteredRowIntent,
) -> Result<()> {
    if !paged
        .store()
        .delete(xid, &pending_key(intent))
        .map_err(|error| BicDbError::PagedStorage(error.to_string()))?
    {
        return Err(refusal("cannot clear absent original pending entry"));
    }
    Ok(())
}
pub(super) fn require_cleared(
    paged: &crate::paged_collection::PagedRecords,
    intent: &RegisteredRowIntent,
) -> Result<()> {
    if paged
        .store()
        .get_as_of(&paged.latest_snapshot(), &pending_key(intent))
        .map_err(|error| BicDbError::PagedStorage(error.to_string()))?
        .is_some()
    {
        return Err(refusal(
            "committed progress retained unexpected pending entry",
        ));
    }
    Ok(())
}
#[cfg(test)]
pub(crate) fn inspect_for_test(
    root: &Path,
    paged: &crate::paged_collection::PagedRecords,
) -> Result<serde_json::Value> {
    Ok(
        serde_json::json!({"reader_fenced":fenced(root)?,"pending":scan(paged)?.0,"restrictive_data_only":true}),
    )
}

/// Read-only restrictive DATA for a future exact cold outcome resolver. It
/// creates no current-open identity, grant, transaction or Serving authority.
pub(crate) fn read_pending_data(
    root: &Path,
    paged: &crate::paged_collection::PagedRecords,
) -> Result<Vec<PendingRegisteredRowWal>> {
    if !fenced(root)? {
        return Err(refusal("pending DATA reader requires sticky index fence"));
    }
    Ok(scan(paged)?.0)
}

#[cfg(test)]
impl BicDb {
    pub(crate) fn test_registered_row_wal_index_data(&self) -> Result<serde_json::Value> {
        inspect_for_test(
            self.data_path(),
            self.paged_records
                .as_deref()
                .ok_or_else(|| refusal("test requires actual page store"))?,
        )
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn paged(root: &Path) -> crate::paged_collection::PagedRecords {
        crate::paged_collection::PagedRecords::open(
            root,
            crate::paged_collection::PagedRecordsOptions {
                buffer_pool_bytes: 8 * 1024 * 1024,
                ..Default::default()
            },
        )
        .unwrap()
    }
    #[test]
    fn registered_row_pending_malformed_index_is_not_empty() {
        let root = tempfile::tempdir().unwrap();
        let store = paged(root.path());
        let (xid, _) = store.begin();
        store
            .store()
            .put(xid, &native_key(&format!("{PREFIX}damaged")), b"{broken")
            .unwrap();
        store.commit(xid).unwrap();
        assert!(
            scan(&store).is_err(),
            "malformed committed debt cannot mean empty"
        );
    }
    #[test]
    fn registered_row_pending_oversize_index_is_not_empty() {
        let root = tempfile::tempdir().unwrap();
        let store = paged(root.path());
        let (xid, _) = store.begin();
        store
            .store()
            .put(
                xid,
                &native_key(&format!("{PREFIX}oversized")),
                &vec![b'x'; MAX_ENTRY_BYTES + 1],
            )
            .unwrap();
        store.commit(xid).unwrap();
        let error = scan(&store).unwrap_err();
        assert!(error.to_string().contains("byte bound exceeded"), "{error}");
    }
    #[test]
    fn registered_row_pending_aborted_data_is_not_published() {
        let root = tempfile::tempdir().unwrap();
        let store = paged(root.path());
        let (xid, _) = store.begin();
        // Restrictive DATA only; no fabricated business transaction/grant.
        store
            .store()
            .put(xid, &native_key(&format!("{PREFIX}aborted")), b"{broken")
            .unwrap();
        store
            .store()
            .put(
                xid,
                &native_key("registered-remote-image/aborted/row-intent/1"),
                b"intent",
            )
            .unwrap();
        store.abort(xid).unwrap();
        assert!(scan(&store).unwrap().0.is_empty());
        assert!(store
            .store()
            .get_as_of(
                &store.latest_snapshot(),
                &native_key("registered-remote-image/aborted/row-intent/1")
            )
            .unwrap()
            .is_none());
    }
    #[test]
    fn registered_row_unindexed_prototype_is_refused_without_upgrade() {
        let root = tempfile::tempdir().unwrap();
        let mut metadata = format::FormatMetadata::current();
        metadata
            .feature_flags
            .insert(format::NATIVE_REGISTERED_REMOTE_PROPOSAL_FEATURE_FLAG.into());
        metadata
            .feature_flags
            .insert(format::NATIVE_REGISTERED_REMOTE_IMAGE_FEATURE_FLAG.into());
        let path = format::metadata_path(root.path());
        storage::write_atomic(&path, &serde_json::to_vec(&metadata).unwrap(), true).unwrap();
        let original = std::fs::read(&path).unwrap();
        assert!(require_no_outstanding(root.path(), None)
            .unwrap_err()
            .to_string()
            .contains("unindexed"));
        assert!(format::enable_native_registered_remote_image(root.path())
            .unwrap_err()
            .to_string()
            .contains("unindexed"));
        assert_eq!(
            std::fs::read(path).unwrap(),
            original,
            "no inferred safe upgrade"
        );
    }
}
