//! Binary startup snapshot ("true checkpoint").
//!
//! On a full [`crate::db::BicDb::compact`] the engine reaches a quiescent point:
//! segments are rewritten to exactly the live records, the WAL is truncated, and
//! the in-memory heap equals that clean state. At that instant we dump every live
//! record into a compact binary file so the *next* open can rebuild the heap by
//! reading raw fields (length-prefixed bytes, `f32` vectors as raw little-endian)
//! instead of re-parsing millions of JSON frames. On a large transactional seed the JSON
//! `segment_record_load` phase is ~8.4s; the binary form skips the per-record JSON
//! object parse (id/vector/timestamp extraction) entirely — the dominant win for
//! vector/embedding records, whose thousands of JSON floats are otherwise reparsed.
//!
//! ## Safety: the snapshot is a derived cache, never authoritative
//! Segments remain the source of truth. The snapshot is *tried* on open and on ANY
//! mismatch we fall back to the exact JSON segment + index-rebuild path:
//! - missing manifest        -> no snapshot, fall back
//! - manifest parse failure  -> fall back
//! - format-version mismatch -> fall back
//! - torn `records.snap`     -> per-frame CRC fails in `read_frames` (truncated),
//!                              the decoded count then disagrees with the manifest
//!                              -> fall back
//! - WAL gap (a later compaction advanced segments past this snapshot)
//!                           -> the snapshot is *deleted* by every segment-mutating
//!                              path (fuzzy checkpoint / single-collection compact),
//!                              so a stale snapshot can never exist on disk.
//!
//! The manifest is written LAST (after `records.snap` is durable) and is the commit
//! marker: a crash mid-write leaves no manifest, so the next open simply falls back.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::encryption::EncryptionRuntime;
use crate::error::{BicDbError, Result};
use crate::record::StoredRecord;
use crate::storage::{self, FrameKind, SegmentReadMode};

/// Bump whenever the binary record codec or manifest shape changes. An open that
/// finds a different version ignores the snapshot and rebuilds from segments.
pub(crate) const SNAPSHOT_FORMAT_VERSION: u32 = 1;

const SNAPSHOT_DIR: &str = "snapshot";
const RECORDS_FILE: &str = "records.snap";
const MANIFEST_FILE: &str = "manifest.json";

/// Sentinel length marking an absent `Option` field (vector / geometry / payload).
const NONE_LEN: u32 = u32::MAX;

/// Roughly cap each binary frame at this many bytes so `records.snap` is split into
/// many chunks that decode across all cores (mirrors the segment decode fan-out).
const CHUNK_TARGET_BYTES: usize = 8 * 1024 * 1024;

pub(crate) fn snapshot_dir(root: &Path) -> PathBuf {
    root.join(SNAPSHOT_DIR)
}

fn manifest_path(root: &Path) -> PathBuf {
    snapshot_dir(root).join(MANIFEST_FILE)
}

fn records_path(root: &Path) -> PathBuf {
    snapshot_dir(root).join(RECORDS_FILE)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SnapshotManifest {
    pub format_version: u32,
    /// `commit_seq` the snapshot reflects (the compaction's last durable commit).
    /// Informational + a sanity field; correctness rests on segment fallback.
    pub watermark_commit_seq: u64,
    pub created_unix: i64,
    pub collections: Vec<SnapshotCollection>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SnapshotCollection {
    pub name: String,
    pub record_count: u64,
    /// Segment frame count at snapshot time (== live record count post-compact);
    /// restored into `CollectionState` so the fuzzy checkpoint's append-vs-rewrite
    /// math is correct without reading the segment file.
    pub segment_frame_count: u64,
    /// Segment file size (bytes) at snapshot time. The staleness signal: commits
    /// eagerly *append* materialized records to the segment, so if the segment has
    /// grown (or shrunk via rewrite) since the snapshot, the on-open size differs
    /// and this collection falls back to a full segment decode. Append-only growth
    /// is thus caught structurally with no commit-path bookkeeping.
    pub segment_byte_len: u64,
}

/// A fully-decoded, validated snapshot: live records grouped by collection plus the
/// manifest. `None` from [`load`] means "no usable snapshot — use the segment path".
pub(crate) struct LoadedSnapshot {
    pub manifest: SnapshotManifest,
    pub records: FxHashMap<String, Vec<StoredRecord>>,
}

impl LoadedSnapshot {
    pub fn collection(&self, name: &str) -> Option<&SnapshotCollection> {
        self.manifest.collections.iter().find(|c| c.name == name)
    }
}

// ---------------------------------------------------------------------------
// Binary record codec (little-endian, length-prefixed). Hand-rolled (no bincode)
// so the format is explicit and versioned.
// ---------------------------------------------------------------------------

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    put_u32(buf, bytes.len() as u32);
    buf.extend_from_slice(bytes);
}

fn put_opt_bytes(buf: &mut Vec<u8>, bytes: Option<&[u8]>) {
    match bytes {
        None => put_u32(buf, NONE_LEN),
        Some(bytes) => put_bytes(buf, bytes),
    }
}

/// Append one record to a chunk buffer. Field order is fixed and must match
/// [`decode_record`].
pub(crate) fn encode_record(buf: &mut Vec<u8>, record: &StoredRecord) {
    put_bytes(buf, record.id.as_bytes());

    match record.timestamp {
        Some(ts) => {
            buf.push(1);
            buf.extend_from_slice(&ts.to_le_bytes());
        }
        None => buf.push(0),
    }

    match record.vector.as_ref() {
        None => put_u32(buf, NONE_LEN),
        Some(vector) => {
            put_u32(buf, vector.len() as u32);
            for value in vector {
                buf.extend_from_slice(&value.to_le_bytes());
            }
        }
    }

    put_bytes(buf, record.metadata.get().as_bytes());

    // Geometry is rare; carry it as its JSON encoding (None => sentinel length).
    match record.geometry.as_ref() {
        None => put_u32(buf, NONE_LEN),
        Some(geometry) => {
            // Geometry always serializes; on the off chance it does not we drop it
            // (geometry is reconstructable from metadata for spatial use cases).
            let bytes = serde_json::to_vec(geometry).unwrap_or_default();
            put_bytes(buf, &bytes);
        }
    }

    put_opt_bytes(buf, record.payload.as_deref());
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(corrupt("snapshot frame truncated"));
        }
        let slice = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn i64(&mut self) -> Result<i64> {
        let b = self.take(8)?;
        Ok(i64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    /// Returns `None` for the absent sentinel length, else the byte slice.
    fn opt_bytes(&mut self) -> Result<Option<&'a [u8]>> {
        let len = self.u32()?;
        if len == NONE_LEN {
            return Ok(None);
        }
        Ok(Some(self.take(len as usize)?))
    }
}

fn corrupt(message: &str) -> BicDbError {
    BicDbError::Corruption {
        path: PathBuf::from(SNAPSHOT_DIR),
        message: message.to_string(),
    }
}

fn decode_record(reader: &mut Reader<'_>) -> Result<StoredRecord> {
    let id = String::from_utf8(reader.bytes()?.to_vec())
        .map_err(|_| corrupt("snapshot record id is not utf-8"))?;

    let timestamp = if reader.u8()? == 1 {
        Some(reader.i64()?)
    } else {
        None
    };

    let vector = {
        let len = reader.u32()?;
        if len == NONE_LEN {
            None
        } else {
            let raw = reader.take(len as usize * 4)?;
            let mut values = Vec::with_capacity(len as usize);
            for chunk in raw.chunks_exact(4) {
                values.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
            }
            Some(values)
        }
    };

    let metadata_bytes = reader.bytes()?;
    let metadata_str = std::str::from_utf8(metadata_bytes)
        .map_err(|_| corrupt("snapshot metadata is not utf-8"))?;
    let metadata = RawValue::from_string(metadata_str.to_string())
        .map_err(|_| corrupt("snapshot metadata is not valid json"))?;

    let geometry = match reader.opt_bytes()? {
        None => None,
        Some(bytes) => Some(
            serde_json::from_slice(bytes).map_err(|_| corrupt("snapshot geometry is invalid"))?,
        ),
    };

    let payload = reader.opt_bytes()?.map(|bytes| bytes.to_vec());

    Ok(StoredRecord {
        id,
        vector,
        metadata,
        geometry,
        timestamp,
        payload,
        typed: std::sync::OnceLock::new(),
        evicted: None,
    })
}

/// A chunk frame: `[u32 name_len][name][u32 count][record...]`.
fn decode_chunk(payload: &[u8]) -> Result<(String, Vec<StoredRecord>)> {
    let mut reader = Reader::new(payload);
    let name = String::from_utf8(reader.bytes()?.to_vec())
        .map_err(|_| corrupt("snapshot collection name is not utf-8"))?;
    let count = reader.u32()? as usize;
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        records.push(decode_record(&mut reader)?);
    }
    Ok((name, records))
}

/// Split one collection's live records into byte-bounded chunk frame payloads.
pub(crate) fn encode_collection_chunks(name: &str, records: &[&StoredRecord]) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    let mut index = 0usize;
    // Always emit at least one (possibly empty) chunk so an empty collection still
    // round-trips with a record_count of 0.
    loop {
        let mut buf = Vec::new();
        put_bytes(&mut buf, name.as_bytes());
        let count_pos = buf.len();
        put_u32(&mut buf, 0); // placeholder, patched below
        let mut count = 0u32;
        while index < records.len() {
            encode_record(&mut buf, records[index]);
            index += 1;
            count += 1;
            if buf.len() >= CHUNK_TARGET_BYTES {
                break;
            }
        }
        buf[count_pos..count_pos + 4].copy_from_slice(&count.to_le_bytes());
        frames.push(buf);
        if index >= records.len() {
            break;
        }
    }
    frames
}

// ---------------------------------------------------------------------------
// Write / invalidate / read
// ---------------------------------------------------------------------------

/// Atomically publish a snapshot: write `records.snap` (durable) THEN the manifest
/// (the commit marker). The manifest is removed up front so that during the write
/// window no manifest exists and an open falls back to segments.
pub(crate) fn write_snapshot(
    root: &Path,
    manifest: &SnapshotManifest,
    record_frames: &[Vec<u8>],
    fsync: bool,
    compression: &storage::CompressionConfig,
    encryption: &EncryptionRuntime,
) -> Result<()> {
    let dir = snapshot_dir(root);
    fs::create_dir_all(&dir)?;
    // Invalidate first: any reader during the rewrite sees no manifest -> fallback.
    remove_if_present(&manifest_path(root))?;

    crate::db::rewrite_frame_file(
        &records_path(root),
        FrameKind::Record,
        record_frames,
        fsync,
        compression,
        encryption,
    )?;

    let bytes = serde_json::to_vec_pretty(manifest)?;
    storage::write_atomic(&manifest_path(root), &bytes, fsync)?;
    Ok(())
}

/// Drop the snapshot's commit marker. Called by every path that mutates segments
/// without writing a fresh snapshot, so a stale snapshot never survives on disk.
/// Cheap: removes only the small manifest (the large `records.snap` is overwritten
/// by the next [`write_snapshot`]).
pub(crate) fn invalidate(root: &Path) -> Result<()> {
    remove_if_present(&manifest_path(root))
}

fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Try to load a usable snapshot. Returns `Ok(None)` (never an error) for every
/// "not usable" condition so the caller transparently falls back to segments.
pub(crate) fn load(
    root: &Path,
    read_mode: SegmentReadMode,
    encryption: &EncryptionRuntime,
) -> Option<LoadedSnapshot> {
    match load_inner(root, read_mode, encryption) {
        Ok(loaded) => loaded,
        Err(error) => {
            // A corrupt/torn snapshot is not fatal — log and fall back.
            eprintln!("[snapshot] ignoring unusable snapshot: {error}");
            None
        }
    }
}

fn load_inner(
    root: &Path,
    read_mode: SegmentReadMode,
    encryption: &EncryptionRuntime,
) -> Result<Option<LoadedSnapshot>> {
    let manifest_path = manifest_path(root);
    if !manifest_path.exists() {
        return Ok(None);
    }
    let manifest_bytes = fs::read(&manifest_path)?;
    let manifest: SnapshotManifest = match serde_json::from_slice(&manifest_bytes) {
        Ok(manifest) => manifest,
        Err(_) => return Ok(None), // torn/garbled manifest -> fall back
    };
    if manifest.format_version != SNAPSHOT_FORMAT_VERSION {
        return Ok(None); // version skew -> fall back
    }

    let records_path = records_path(root);
    if !records_path.exists() {
        return Ok(None);
    }
    // read_frames verifies each frame's CRC and truncates a torn tail, so a partial
    // write surfaces as a short read that the count check below rejects.
    let recovered = storage::read_frames(&records_path, FrameKind::Record, read_mode, encryption)?;

    let decoded =
        crate::db::parallel_map_ordered(&recovered.frames, |frame| decode_chunk(&frame.payload))?;

    let mut grouped: FxHashMap<String, Vec<StoredRecord>> = FxHashMap::default();
    for (name, records) in decoded {
        grouped.entry(name).or_default().extend(records);
    }

    // Cross-check decoded counts against the manifest. A torn-and-truncated
    // records.snap (or any decode shortfall) trips this and we fall back.
    let mut expected: BTreeMap<&str, u64> = BTreeMap::new();
    for collection in &manifest.collections {
        expected.insert(collection.name.as_str(), collection.record_count);
    }
    for collection in &manifest.collections {
        let got = grouped.get(&collection.name).map(|r| r.len()).unwrap_or(0) as u64;
        if got != collection.record_count {
            return Ok(None);
        }
    }
    // Reject extra collections not described by the manifest (inconsistent file).
    for name in grouped.keys() {
        if !expected.contains_key(name.as_str()) {
            return Ok(None);
        }
    }

    Ok(Some(LoadedSnapshot {
        manifest,
        records: grouped,
    }))
}
