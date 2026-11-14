//! Bounded read-only WAL facts for one ACTUAL persisted pending import. These
//! facts do not reconstruct a transaction, mutation scope, recovery owner or
//! Serving permission. Absence never means rollback.
use super::*;
use crate::RangeId;
use std::io::{Read, Seek, SeekFrom};

const MAX_WAL_BYTES: u64 = 512 * 1024 * 1024;
const MAX_FRAMES: usize = 1_000_000;
const MAX_SELECTED_BYTES: usize = 8 * 1024 * 1024;
const MAX_SELECTED_WRITES: usize = 4096;

fn refusal(message: &str) -> BicDbError {
    BicDbError::HighAvailability(format!("Registered pending WAL inspection: {message}"))
}

/// Serializable observations, deliberately not a recovery/commit capability.
#[derive(Debug, Serialize)]
pub(crate) struct PendingRowWalFacts {
    attempt: Uuid,
    step: u64,
    range: RangeId,
    original_database_open_from_native_index: String,
    native_birth_sha256: String,
    original_native_intent_xid: u64,
    logical_transaction_id: u64,
    expected_mutations_sha256: String,
    wal_bytes: u64,
    wal_sha256: String,
    frames_inspected: usize,
    selected_frames: usize,
    observed_begin: bool,
    observed_writes: usize,
    observed_mutations_sha256: Option<String>,
    observed_payload_matches_intent: Option<bool>,
    observed_decision: Decision,
    original_open_is_native_index_correlation_not_a_wal_incarnation_field: bool,
    establishes_current_image_or_serving: bool,
    observed_commit_bytes_are_not_a_durable_sync_receipt: bool,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Decision {
    NoCompleteDecision,
    Commit {
        sequence: u64,
        materialized_marker: bool,
    },
    Abort,
}

#[derive(Deserialize)]
struct Header {
    #[serde(rename = "type")]
    _kind: TxFrameKind,
    tx_id: TransactionId,
}

struct Selected {
    begin: bool,
    frames: usize,
    total_write_bytes: usize,
    writes: Vec<TxWrite>,
    decision: Decision,
}
impl Selected {
    fn accept(&mut self, frame: TxFrame, payload_bytes: usize) -> Result<()> {
        self.frames += 1;
        match frame {
            TxFrame::Begin { .. } if !self.begin && self.frames == 1 => self.begin = true,
            // The current actual encoder emits Write* + Commit without Begin.
            // An older explicit Begin remains an optional observed fact.
            TxFrame::Write { write, .. }
                if matches!(self.decision, Decision::NoCompleteDecision) =>
            {
                if !matches!(write.op, TxWriteOp::Upsert) {
                    return Err(refusal("physical import contains an unexpected delete"));
                }
                self.total_write_bytes = self.total_write_bytes.checked_add(payload_bytes)
                    .ok_or_else(|| refusal("selected write byte overflow"))?;
                if self.total_write_bytes > MAX_SELECTED_BYTES || self.writes.len() >= MAX_SELECTED_WRITES {
                    return Err(refusal("selected transaction write bound exceeded"));
                }
                self.writes.push(*write);
            }
            TxFrame::Truncate { len, .. }
                if (self.begin || !self.writes.is_empty()) && matches!(self.decision, Decision::NoCompleteDecision) =>
            {
                if len > self.writes.len() {
                    return Err(refusal("truncate references missing selected writes"));
                }
                self.writes.truncate(len);
            }
            TxFrame::Commit { commit_seq, .. }
                if !self.writes.is_empty() && commit_seq > 0 && matches!(self.decision, Decision::NoCompleteDecision) =>
            {
                self.decision = Decision::Commit { sequence: commit_seq, materialized_marker: false };
            }
            TxFrame::CommitMaterialized { commit_seq, .. } if !self.writes.is_empty() && commit_seq > 0 => {
                match self.decision {
                    Decision::NoCompleteDecision => {},
                    Decision::Commit { sequence, materialized_marker: false } if sequence == commit_seq => {},
                    _ => return Err(refusal("materialized marker conflicts with selected decision")),
                }
                self.decision = Decision::Commit { sequence: commit_seq, materialized_marker: true };
            }
            TxFrame::Abort { .. }
                if (self.begin || !self.writes.is_empty()) && matches!(self.decision, Decision::NoCompleteDecision) =>
            {
                self.decision = Decision::Abort;
            }
            _ => return Err(refusal("selected transaction has missing/repeated prefix, conflicting decision, patch or business outbox")),
        }
        Ok(())
    }
}

fn hash_descriptor(file: &mut File, cancel: &CancellationToken) -> Result<String> {
    file.seek(SeekFrom::Start(0))?;
    let mut digest = sha2::Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    let mut bytes = 0u64;
    loop {
        cancel.check()?;
        let got = file.read(&mut buffer)?;
        if got == 0 {
            break;
        }
        bytes = bytes
            .checked_add(got as u64)
            .ok_or_else(|| refusal("WAL size overflow"))?;
        if bytes > MAX_WAL_BYTES {
            return Err(refusal("WAL scan byte bound exceeded"));
        }
        digest.update(&buffer[..got]);
    }
    Ok(hex::encode(digest.finalize()))
}

/// Reads the bounded actual native index again. A supplied A/step is a selector,
/// never a synthetic expectation or evidence of an absent/rolled-back command.
/// Caller must preserve its actual closed-store ownership through inspection
/// and any later action; this function performs NO action or generic recovery.
pub(crate) fn inspect_pending_row_wal(
    root: &Path,
    paged: &crate::paged_collection::PagedRecords,
    encryption: &EncryptionRuntime,
    attempt: Uuid,
    step: u64,
    cancel: &CancellationToken,
) -> Result<PendingRowWalFacts> {
    cancel.check()?;
    if fs::canonicalize(root.join(DEFAULT_PAGED_DIR))? != paged.opened_directory() {
        return Err(refusal("pending native store and logical WAL roots differ"));
    }
    let pending = super::registered_row_retention::read_pending_data(root, paged)?;
    let entry = pending
        .iter()
        .find(|entry| entry.intent().attempt == attempt && entry.intent().step == step)
        .ok_or_else(|| {
            refusal("actual pending native entry absent; no negative outcome inferred")
        })?;
    let intent = entry.intent();
    let logical_txid = intent
        .logical_txid
        .ok_or_else(|| refusal("selected step has no logical row transaction"))?;
    let expected = intent
        .mutations_sha256
        .as_ref()
        .ok_or_else(|| refusal("selected native mutation hash absent"))?;
    if logical_txid == 0 || expected.len() != 64 || hex::decode(expected).is_err() {
        return Err(refusal("selected logical transaction/hash malformed"));
    }
    let path = root.join(DEFAULT_TRANSACTION_LOG);
    let mut file = File::open(&path)?;
    let before = file.metadata()?;
    if !before.is_file() || before.len() > MAX_WAL_BYTES {
        return Err(refusal("WAL file/byte bound refused"));
    }
    let sha = hash_descriptor(&mut file, cancel)?;
    file.seek(SeekFrom::Start(0))?;
    let mut selected = Selected {
        begin: false,
        frames: 0,
        total_write_bytes: 0,
        writes: Vec::new(),
        decision: Decision::NoCompleteDecision,
    };
    let mut frames = 0usize;
    storage::stream_frames_readonly_complete(
        file.try_clone()?,
        &path,
        FrameKind::Transaction,
        encryption,
        |_, bytes| {
            cancel.check()?;
            frames = frames
                .checked_add(1)
                .ok_or_else(|| refusal("frame count overflow"))?;
            if frames > MAX_FRAMES {
                return Err(refusal("WAL frame count bound exceeded"));
            }
            let header: Header = serde_json::from_slice(&bytes)?;
            if header.tx_id.0 == logical_txid {
                if bytes.len() > MAX_SELECTED_BYTES {
                    return Err(refusal("selected WAL frame bound exceeded"));
                }
                selected.accept(decode_tx_frame(&bytes)?, bytes.len())?;
            }
            Ok(())
        },
    )?;
    let after = file.metadata()?;
    if after.len() != before.len()
        || after.modified()? != before.modified()?
        || hash_descriptor(&mut file, cancel)? != sha
    {
        return Err(refusal("WAL changed during readonly inspection"));
    }
    let observed = (selected.begin || !selected.writes.is_empty())
        .then(|| native_wal_apply::actual_writes_mutations_sha256(&selected.writes))
        .transpose()?;
    if matches!(selected.decision, Decision::Commit { .. }) && observed.as_ref() != Some(expected) {
        return Err(refusal(
            "selected committed payload differs from actual pending intent",
        ));
    }
    cancel.check()?;
    Ok(PendingRowWalFacts {
        attempt,
        step,
        range: intent.range,
        original_database_open_from_native_index: entry.original_database_open().into(),
        native_birth_sha256: entry.native_birth_sha256().into(),
        original_native_intent_xid: intent.native_xid,
        logical_transaction_id: logical_txid,
        expected_mutations_sha256: expected.clone(),
        wal_bytes: before.len(),
        wal_sha256: sha,
        frames_inspected: frames,
        selected_frames: selected.frames,
        observed_begin: selected.begin,
        observed_writes: selected.writes.len(),
        observed_payload_matches_intent: observed.as_ref().map(|hash| hash == expected),
        observed_mutations_sha256: observed,
        observed_decision: selected.decision,
        original_open_is_native_index_correlation_not_a_wal_incarnation_field: true,
        establishes_current_image_or_serving: false,
        observed_commit_bytes_are_not_a_durable_sync_receipt: true,
    })
}
