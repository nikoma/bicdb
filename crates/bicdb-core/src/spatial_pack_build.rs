//! Resumable, bounded-memory build pipeline for packed spatial indexes.
//!
//! `pack_spatial_index` originally collected every (pk, rect) into one
//! corpus-sized vector before sorting — ~4–5 GiB at 74M rows, all-or-nothing,
//! and the same OOM shape as the resident build it replaced. This module is
//! the external-sort front half that removes that last unbounded stage:
//!
//! 1. **Scan phase** (resumable per run): stream identity rows in bounded
//!    chunks, key each entry on the fixed-bounds Hilbert curve
//!    ([`crate::spatial_packed::hilbert_key_lonlat`] — fixed lon/lat bounds
//!    make this single-pass), and every `run_entry_budget` entries sort the
//!    buffer and write it as one atomic sorted run file, checkpointing the
//!    last-scanned pk. Peak memory is one run buffer, never the corpus.
//! 2. **Merge + pack phase**: k-way merge the runs into one Hilbert-ordered
//!    stream and feed it straight to
//!    [`crate::spatial_packed::build_packed_tree_from_sorted`], which holds
//!    one leaf plus one level of node summaries. Cheap-v1 resume: this phase
//!    restarts from the runs (pure sequential I/O, no sort).
//! 3. **Publish**: unchanged — one atomic meta-swap transaction.
//!
//! # Resume soundness — delta masking, not write fencing
//!
//! A resumed scan reads a NEWER snapshot than the runs already on disk, so
//! the merged stream can mix row states across snapshots. That is safe
//! because durable delta maintenance is switched ON for the index the moment
//! a workspace exists (`IndexState::spatial_delta_durable`): every commit
//! that touches a pk writes a delta row, and a delta row masks that pk
//! against the packed base at the id level. Whatever stale or duplicate
//! state the runs captured for a touched pk, queries serve the delta version.
//! The publish folds (deletes) only delta rows captured at workspace
//! creation AND byte-identical at publish time — a row rewritten mid-build
//! keeps masking. No write fencing, no horizon checks: interrupted builds
//! resume correctly even after other sessions wrote freely in between.
//!
//! The workspace lives at `<db>/spatial_pack/idx-<name-hash>/` — plain files, atomic
//! checkpoint writes, discarded on publish. Open-time machinery must leave an
//! active workspace's node generation alone (see `active_generation`) and
//! must NOT stream-build a resident tree for an index whose packed build is
//! in flight (that resident build is the OOM this pipeline exists to avoid).

use std::collections::BinaryHeap;
use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{BicDbError, Result};
use crate::spatial_packed::PackedSpatialEntry;
use crate::storage;

const WORKSPACE_DIR: &str = "spatial_pack";
const CHECKPOINT_FILE: &str = "checkpoint.json";
const CHECKPOINT_VERSION: u32 = 1;

/// Entries buffered (and sorted) per run file. ~60 B/entry with short ids,
/// so the default holds peak scan memory around 100 MiB regardless of corpus
/// size. Overridable for tests and constrained hosts.
pub(crate) fn run_entry_budget() -> usize {
    std::env::var("BICDB_SPATIAL_PACK_RUN_ENTRIES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1_000_000)
}

/// Test-only fault injection: error out after N runs have been written, so a
/// mid-scan crash (and its resume) is reproducible from an integration test.
pub(crate) fn crash_after_runs() -> Option<usize> {
    std::env::var("BICDB_SPATIAL_PACK_CRASH_AFTER_RUNS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SpatialPackPhase {
    /// Streaming the corpus into sorted runs; `last_pk` is the scan cursor.
    Scan,
    /// Runs complete; nodes are being written under `generation`.
    Pack,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SpatialPackCheckpoint {
    pub version: u32,
    pub collection: String,
    pub strategy: String,
    pub generation: u64,
    pub run_entry_budget: usize,
    pub phase: SpatialPackPhase,
    pub last_pk: Option<String>,
    pub runs: Vec<String>,
    pub entries: u64,
    /// Delta rows captured when the workspace was created, as
    /// `(pk, hex(value))`. The publish folds a captured row ONLY when its
    /// current value is byte-identical to the capture: a row rewritten after
    /// an interruption may describe state the runs did not scan, and its
    /// masking must survive the publish. Unfolded rows are pure overhead,
    /// never an error — the next uninterrupted re-pack folds them.
    pub delta_rows: Vec<(String, String)>,
    /// The store's snapshot horizon when the scan began. Informational (for
    /// diagnostics); resume soundness comes from delta masking, not from a
    /// no-write guarantee.
    pub expected_xmax: u64,
}

pub(crate) struct SpatialPackWorkspace {
    dir: PathBuf,
    fsync: bool,
    pub checkpoint: SpatialPackCheckpoint,
}

fn workspace_dir(db_path: &Path, index: &str) -> PathBuf {
    // Never place catalog-controlled text in a filesystem path. Catalog
    // definitions are validated on load as a first boundary; hashing here is
    // a second, path-independent boundary before recursive cleanup.
    let digest = Sha256::digest(index.as_bytes());
    db_path
        .join(WORKSPACE_DIR)
        .join(format!("idx-{}", hex::encode(&digest[..16])))
}

impl SpatialPackWorkspace {
    pub fn load(db_path: &Path, index: &str) -> Result<Option<Self>> {
        let dir = workspace_dir(db_path, index);
        let path = dir.join(CHECKPOINT_FILE);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let checkpoint: SpatialPackCheckpoint = match serde_json::from_slice(&bytes) {
            Ok(checkpoint) => checkpoint,
            // A torn or foreign checkpoint is a restart, not an error.
            Err(_) => return Ok(None),
        };
        if checkpoint.version != CHECKPOINT_VERSION {
            return Ok(None);
        }
        Ok(Some(Self {
            dir,
            fsync: true,
            checkpoint,
        }))
    }

    /// The node generation an in-flight build owns, if any — what the
    /// open-time orphan sweep must leave alone.
    pub fn active_generation(db_path: &Path, index: &str) -> Option<u64> {
        Self::load(db_path, index)
            .ok()
            .flatten()
            .map(|workspace| workspace.checkpoint.generation)
    }

    pub fn create(
        db_path: &Path,
        index: &str,
        fsync: bool,
        checkpoint: SpatialPackCheckpoint,
    ) -> Result<Self> {
        let dir = workspace_dir(db_path, index);
        // A fresh build owns the directory outright: stale runs from an
        // abandoned build must not merge into this one.
        if dir.exists() {
            fs::remove_dir_all(&dir)?;
        }
        fs::create_dir_all(&dir)?;
        let workspace = Self {
            dir,
            fsync,
            checkpoint,
        };
        workspace.save()?;
        Ok(workspace)
    }

    pub fn save(&self) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(&self.checkpoint)?;
        storage::write_atomic(&self.dir.join(CHECKPOINT_FILE), &bytes, self.fsync)?;
        Ok(())
    }

    pub fn run_paths(&self) -> Vec<PathBuf> {
        self.checkpoint
            .runs
            .iter()
            .map(|name| self.dir.join(name))
            .collect()
    }

    /// Sort `buffer` by key and persist it as the next run (atomic rename),
    /// then checkpoint the scan cursor. The buffer is drained.
    pub fn write_run(
        &mut self,
        buffer: &mut Vec<(u64, PackedSpatialEntry)>,
        last_pk: Option<String>,
    ) -> Result<()> {
        if buffer.is_empty() {
            if last_pk.is_some() {
                self.checkpoint.last_pk = last_pk;
                self.save()?;
            }
            return Ok(());
        }
        buffer.sort_unstable_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.record_id.cmp(&right.1.record_id))
        });
        let name = format!("run-{:05}.spr", self.checkpoint.runs.len());
        let final_path = self.dir.join(&name);
        let temp_path = self.dir.join(format!("{name}.tmp"));
        {
            let file = fs::File::create(&temp_path)?;
            let mut writer = BufWriter::new(file);
            for (key, entry) in buffer.iter() {
                write_run_entry(&mut writer, *key, entry)?;
            }
            writer.flush()?;
            if self.fsync {
                writer.get_ref().sync_all()?;
            }
        }
        fs::rename(&temp_path, &final_path)?;
        self.checkpoint.entries += buffer.len() as u64;
        self.checkpoint.runs.push(name);
        self.checkpoint.last_pk = last_pk;
        buffer.clear();
        self.save()
    }

    pub fn discard(self) -> Result<()> {
        match fs::remove_dir_all(&self.dir) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

fn write_run_entry(writer: &mut impl Write, key: u64, entry: &PackedSpatialEntry) -> Result<()> {
    writer.write_all(&key.to_le_bytes())?;
    writer.write_all(&entry.min[0].to_le_bytes())?;
    writer.write_all(&entry.min[1].to_le_bytes())?;
    writer.write_all(&entry.max[0].to_le_bytes())?;
    writer.write_all(&entry.max[1].to_le_bytes())?;
    match entry.point {
        Some(point) => {
            writer.write_all(&[1])?;
            writer.write_all(&point[0].to_le_bytes())?;
            writer.write_all(&point[1].to_le_bytes())?;
        }
        None => writer.write_all(&[0])?,
    }
    let id = entry.record_id.as_bytes();
    writer.write_all(&(id.len() as u16).to_le_bytes())?;
    writer.write_all(id)?;
    Ok(())
}

/// One sorted run being merged: a buffered reader plus its next entry.
struct RunCursor {
    reader: BufReader<fs::File>,
    next: Option<(u64, PackedSpatialEntry)>,
}

impl RunCursor {
    fn open(path: &Path) -> Result<Self> {
        let mut cursor = Self {
            reader: BufReader::with_capacity(1 << 20, fs::File::open(path)?),
            next: None,
        };
        cursor.advance()?;
        Ok(cursor)
    }

    fn advance(&mut self) -> Result<()> {
        self.next = read_run_entry(&mut self.reader)?;
        Ok(())
    }
}

fn read_run_entry(reader: &mut impl Read) -> Result<Option<(u64, PackedSpatialEntry)>> {
    let mut key_bytes = [0u8; 8];
    match reader.read_exact(&mut key_bytes) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let mut f64_buf = [0u8; 8];
    let mut read_f64 = |reader: &mut dyn Read| -> Result<f64> {
        reader.read_exact(&mut f64_buf)?;
        Ok(f64::from_le_bytes(f64_buf))
    };
    let min = [read_f64(reader)?, read_f64(reader)?];
    let max = [read_f64(reader)?, read_f64(reader)?];
    let mut flag = [0u8; 1];
    reader.read_exact(&mut flag)?;
    let point = match flag[0] {
        0 => None,
        1 => Some([read_f64(reader)?, read_f64(reader)?]),
        other => {
            return Err(BicDbError::Index(format!(
                "spatial pack run has invalid point flag {other}"
            )));
        }
    };
    let mut len_bytes = [0u8; 2];
    reader.read_exact(&mut len_bytes)?;
    let mut id = vec![0u8; usize::from(u16::from_le_bytes(len_bytes))];
    reader.read_exact(&mut id)?;
    let record_id = String::from_utf8(id)
        .map_err(|error| BicDbError::Index(format!("spatial pack run id not UTF-8: {error}")))?;
    Ok(Some((
        key_bytes_to_u64(key_bytes),
        PackedSpatialEntry {
            record_id,
            min,
            max,
            point,
        },
    )))
}

fn key_bytes_to_u64(bytes: [u8; 8]) -> u64 {
    u64::from_le_bytes(bytes)
}

/// K-way merge over sorted runs, yielding entries in (key, pk) order. 74M
/// entries at the default budget is ~74 runs — one buffered reader each.
pub(crate) struct RunMerge {
    cursors: Vec<RunCursor>,
    heap: BinaryHeap<std::cmp::Reverse<(u64, String, usize)>>,
}

impl RunMerge {
    pub fn open(paths: &[PathBuf]) -> Result<Self> {
        let mut cursors = Vec::with_capacity(paths.len());
        for path in paths {
            cursors.push(RunCursor::open(path)?);
        }
        let mut heap = BinaryHeap::with_capacity(cursors.len());
        for (index, cursor) in cursors.iter().enumerate() {
            if let Some((key, entry)) = &cursor.next {
                heap.push(std::cmp::Reverse((*key, entry.record_id.clone(), index)));
            }
        }
        Ok(Self { cursors, heap })
    }
}

impl Iterator for RunMerge {
    type Item = Result<PackedSpatialEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        let std::cmp::Reverse((_, _, index)) = self.heap.pop()?;
        let cursor = &mut self.cursors[index];
        let (_, entry) = cursor.next.take().expect("heap entry implies cursor entry");
        if let Err(error) = cursor.advance() {
            return Some(Err(error));
        }
        if let Some((key, next_entry)) = &cursor.next {
            self.heap.push(std::cmp::Reverse((
                *key,
                next_entry.record_id.clone(),
                index,
            )));
        }
        Some(Ok(entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, x: f64) -> PackedSpatialEntry {
        PackedSpatialEntry {
            record_id: id.to_string(),
            min: [x, 0.0],
            max: [x, 0.0],
            point: Some([x, 0.0]),
        }
    }

    #[test]
    fn runs_roundtrip_and_merge_in_key_order() {
        let dir = tempfile::tempdir().unwrap();
        let checkpoint = SpatialPackCheckpoint {
            version: CHECKPOINT_VERSION,
            collection: "places".to_string(),
            strategy: "hilbert".to_string(),
            generation: 1,
            run_entry_budget: 4,
            phase: SpatialPackPhase::Scan,
            last_pk: None,
            runs: Vec::new(),
            entries: 0,
            delta_rows: Vec::new(),
            expected_xmax: 7,
        };
        let mut workspace =
            SpatialPackWorkspace::create(dir.path(), "idx", false, checkpoint).unwrap();

        let mut first = vec![
            (5, entry("e", 5.0)),
            (1, entry("a", 1.0)),
            (3, entry("c", 3.0)),
        ];
        let mut second = vec![
            (4, entry("d", 4.0)),
            (2, entry("b", 2.0)),
            (6, entry("f", 6.0)),
        ];
        workspace
            .write_run(&mut first, Some("c".to_string()))
            .unwrap();
        workspace
            .write_run(&mut second, Some("f".to_string()))
            .unwrap();
        assert!(first.is_empty() && second.is_empty());
        assert_eq!(workspace.checkpoint.entries, 6);
        assert_eq!(workspace.checkpoint.last_pk.as_deref(), Some("f"));

        // Reload sees the same checkpoint (atomic write) …
        let reloaded = SpatialPackWorkspace::load(dir.path(), "idx")
            .unwrap()
            .expect("workspace exists");
        assert_eq!(reloaded.checkpoint.runs.len(), 2);
        assert_eq!(reloaded.checkpoint.expected_xmax, 7);

        // … and the merge yields globally key-ordered entries.
        let merged: Vec<String> = RunMerge::open(&reloaded.run_paths())
            .unwrap()
            .map(|entry| entry.unwrap().record_id)
            .collect();
        assert_eq!(merged, vec!["a", "b", "c", "d", "e", "f"]);

        reloaded.discard().unwrap();
        assert!(SpatialPackWorkspace::load(dir.path(), "idx")
            .unwrap()
            .is_none());
    }
}
