//! Bounded, restartable external sorting for bulk full-text index builds.
//!
//! The page store remains the final home of posting blocks. This module owns
//! only derived build artifacts: atomically completed sorted runs and a small
//! phase checkpoint. A crash can leave a `.tmp` file, but never a run that a
//! resume mistakes for complete.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::{BicDbError, Result};
use crate::paged_collection::{decode_doc_terms, NumericBlockPosting};

const CHECKPOINT_FILE: &str = "checkpoint.json";

/// Test-only fault injection: fail as the build ENTERS the named phase, so an
/// interrupted multi-hour build and its resume are reproducible from an
/// integration test.
///
/// A full-text build over a corpus-scale collection runs for hours or days.
/// The module has always been written to be restartable; without a seam like
/// this, "restartable" was a design claim no test had ever exercised.
fn crash_at_phase() -> Option<String> {
    std::env::var("BICDB_FTS_BUILD_CRASH_AT").ok()
}

fn check_crash(phase: FtsBuildPhase) -> Result<()> {
    if let Some(target) = crash_at_phase() {
        let label = format!("{phase:?}").to_ascii_lowercase();
        if target.eq_ignore_ascii_case(&label) {
            return Err(BicDbError::Index(format!(
                "injected full-text build crash entering `{label}`"
            )));
        }
    }
    Ok(())
}
/// Term-grouped run: the term is written once per group, postings follow in
/// document order with their impact bucket/rank precomputed by the parallel
/// tokenize workers. The merge advances per TERM, not per posting.
const GROUPED_RUN_MAGIC: &[u8; 8] = b"BICFTG01";
const BUILD_CHECKPOINT_VERSION: u32 = 6;
/// A term whose postings exceed this within one run splits into several
/// consecutive groups, so a group's encoded payload is always small enough
/// to buffer — which is what lets the writer prefix each group with its
/// byte length, and a range reader skip whole groups without decoding.
const GROUP_POSTING_CAP: usize = 8_192;
/// Trailer appended to every completed run: sentinel, payload length, digest.
///
/// The magic header alone proved only that a file STARTED as a run. A torn
/// write from power loss leaves a valid header above truncated or garbage
/// payload, and the merge would read it as data. The length catches
/// truncation in O(1) at open; the digest catches corruption as the merge
/// streams, at no extra I/O because the merge reads the whole run anyway.
const RUN_TRAILER_MAGIC: &[u8; 8] = b"BICFTT01";
const RUN_TRAILER_BYTES: u64 = 8 + 8 + 32;
const MAX_RUN_FAN_IN: usize = 32;
const MERGE_READER_BUDGET: usize = 16 * 1024;
const MERGE_CHECKPOINT_VERSION: u32 = 1;

/// A running full-text build, as an operator sees it.
///
/// `percent` is only populated during `tokenizing`, where there is a real
/// denominator (the collection's row count). The merge and publish phases have
/// no honest percentage — reporting a fabricated one would be worse than
/// reporting none, because it is exactly the number someone would use to
/// decide whether to kill a build.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FullTextBuildStatus {
    pub index: String,
    pub collection: String,
    pub phase: String,
    pub documents_tokenized: u64,
    pub documents_total: Option<u64>,
    pub runs_written: u64,
    pub scan_cursor: Option<String>,
    pub percent: Option<u8>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FtsBuildPhase {
    Tokenizing,
    MergePk,
    MergeImpact,
    Publishing,
    Complete,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FtsBuildCheckpoint {
    pub version: u32,
    pub logical_index: String,
    pub collection: String,
    pub source_signature: String,
    pub physical_index: String,
    pub phase: FtsBuildPhase,
    pub last_pk: Option<String>,
    pub next_run: u64,
    #[serde(default)]
    pub document_count: u64,
    #[serde(default)]
    pub total_document_length: u64,
    #[serde(default)]
    pub field_total_lengths: [u64; 4],
    /// Progressive build bookkeeping: sub-segments published so far, the
    /// first run not yet covered by one, documents covered, terms written
    /// across subs, and the generation the EARLY visibility flip displaced
    /// (completion reclaims it; its own flip is a self-no-op by then).
    #[serde(default)]
    pub progressive_subs: u32,
    #[serde(default)]
    pub progressive_next_run: u64,
    #[serde(default)]
    pub progressive_docs: u64,
    #[serde(default)]
    pub progressive_terms: u64,
    #[serde(default)]
    pub progressive_old_generation: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MergeLevelCheckpoint {
    version: u32,
    level: usize,
    runs: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct FtsBuildWorkspace {
    dir: PathBuf,
    fsync: bool,
    checkpoint: FtsBuildCheckpoint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RunOrder {
    Pk,
    Impact,
}

impl RunOrder {
    fn label(self) -> &'static str {
        match self {
            Self::Pk => "pk",
            Self::Impact => "impact",
        }
    }
}

#[derive(Debug)]
pub(crate) struct RunPosting {
    pub encoded_term: Vec<u8>,
    pub posting: NumericBlockPosting,
    pub impact_bucket: u16,
    pub impact_rank: f32,
}

impl FtsBuildWorkspace {
    pub(crate) fn existing_indexes(root: &Path) -> Result<Vec<String>> {
        let entries = match fs::read_dir(root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut indexes = Vec::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let checkpoint_path = entry.path().join(CHECKPOINT_FILE);
            let bytes = match fs::read(&checkpoint_path) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            let checkpoint: FtsBuildCheckpoint = serde_json::from_slice(&bytes)?;
            if checkpoint.version != BUILD_CHECKPOINT_VERSION
                || checkpoint.phase == FtsBuildPhase::Complete
            {
                continue;
            }
            let expected = build_dir_name(&checkpoint.logical_index);
            if entry.file_name() != std::ffi::OsStr::new(&expected) {
                return Err(BicDbError::Index(format!(
                    "full-text build checkpoint for `{}` is stored in unexpected workspace `{}`",
                    checkpoint.logical_index,
                    entry.path().display()
                )));
            }
            indexes.push(checkpoint.logical_index);
        }
        indexes.sort();
        indexes.dedup();
        Ok(indexes)
    }

    pub(crate) fn open_existing(
        root: &Path,
        logical_index: &str,
        fsync: bool,
    ) -> Result<Option<Self>> {
        let dir = root.join(build_dir_name(logical_index));
        let checkpoint_path = dir.join(CHECKPOINT_FILE);
        let bytes = match fs::read(checkpoint_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let checkpoint: FtsBuildCheckpoint = serde_json::from_slice(&bytes)?;
        if checkpoint.version != BUILD_CHECKPOINT_VERSION
            || checkpoint.logical_index != logical_index
            || checkpoint.phase == FtsBuildPhase::Complete
        {
            remove_build_dir(&dir)?;
            return Ok(None);
        }
        cleanup_temporary_files(&dir)?;
        let workspace = Self {
            dir,
            fsync,
            checkpoint,
        };
        if workspace.checkpoint.phase == FtsBuildPhase::Tokenizing {
            workspace.remove_uncheckpointed_runs()?;
        }
        Ok(Some(workspace))
    }

    pub(crate) fn open_or_create(
        root: &Path,
        logical_index: &str,
        collection: &str,
        source_signature: &str,
        fsync: bool,
    ) -> Result<Self> {
        fs::create_dir_all(root)?;
        let dir = root.join(build_dir_name(logical_index));
        let checkpoint_path = dir.join(CHECKPOINT_FILE);
        let checkpoint = match fs::read(&checkpoint_path) {
            Ok(bytes) => {
                let checkpoint: FtsBuildCheckpoint = serde_json::from_slice(&bytes)?;
                if checkpoint.version != BUILD_CHECKPOINT_VERSION
                    || checkpoint.logical_index != logical_index
                    || checkpoint.collection != collection
                    || checkpoint.source_signature != source_signature
                    || checkpoint.phase == FtsBuildPhase::Complete
                {
                    remove_build_dir(&dir)?;
                    new_checkpoint(logical_index, collection, source_signature)
                } else {
                    checkpoint
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                new_checkpoint(logical_index, collection, source_signature)
            }
            Err(error) => return Err(error.into()),
        };
        fs::create_dir_all(&dir)?;
        cleanup_temporary_files(&dir)?;
        let workspace = Self {
            dir,
            fsync,
            checkpoint,
        };
        workspace.save()?;
        if workspace.checkpoint.phase == FtsBuildPhase::Tokenizing {
            workspace.remove_uncheckpointed_runs()?;
        }
        Ok(workspace)
    }

    pub(crate) fn checkpoint(&self) -> &FtsBuildCheckpoint {
        &self.checkpoint
    }

    pub(crate) fn phase(&self) -> FtsBuildPhase {
        self.checkpoint.phase
    }

    /// What an operator needs while a multi-day build runs: which phase, how
    /// far the scan has got, and how much intermediate data exists. Read from
    /// the durable checkpoint, so it works from another process while the
    /// build is running.
    pub(crate) fn progress(&self) -> FullTextBuildStatus {
        FullTextBuildStatus {
            index: self.checkpoint.logical_index.clone(),
            collection: self.checkpoint.collection.clone(),
            phase: match self.checkpoint.phase {
                FtsBuildPhase::Tokenizing => "tokenizing",
                FtsBuildPhase::MergePk => "merging_primary_key",
                FtsBuildPhase::MergeImpact => "merging_impact",
                FtsBuildPhase::Publishing => "publishing",
                FtsBuildPhase::Complete => "complete",
            }
            .to_string(),
            documents_tokenized: self.checkpoint.document_count,
            documents_total: None,
            runs_written: self.checkpoint.next_run,
            scan_cursor: self.checkpoint.last_pk.clone(),
            percent: None,
        }
    }

    pub(crate) fn physical_index(&self) -> &str {
        &self.checkpoint.physical_index
    }

    pub(crate) fn source_signature(&self) -> &str {
        &self.checkpoint.source_signature
    }

    pub(crate) fn checkpoint_updated_unix_ms(&self) -> Option<u64> {
        fs::metadata(self.dir.join(CHECKPOINT_FILE))
            .and_then(|metadata| metadata.modified())
            .ok()
            .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
            .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
    }

    pub(crate) fn append_document_batch(
        &mut self,
        blobs: &[(String, Vec<u8>)],
        memory_bytes: usize,
        workers: usize,
    ) -> Result<()> {
        if blobs.is_empty() {
            return Ok(());
        }
        if self.checkpoint.phase != FtsBuildPhase::Tokenizing {
            return Err(BicDbError::Index(format!(
                "full-text build `{}` is no longer tokenizing",
                self.checkpoint.logical_index
            )));
        }
        let memory_bytes = memory_bytes.max(64 * 1024);
        let workers = workers
            .clamp(1, 64)
            .min((memory_bytes / (64 * 1024)).max(1));
        let per_worker = (memory_bytes / workers).max(64 * 1024);
        // The packed blob is materially smaller than the decoded Rust
        // structures. Target one quarter of the worker budget so term/pk
        // allocation and sort scratch remain inside the configured ceiling.
        let input_target = (per_worker / 4).max(16 * 1024);
        let ranges = partition_blobs(blobs, input_target);
        let first_run = self.checkpoint.next_run;
        let first_document_id = self.checkpoint.document_count;
        for (wave_index, wave) in ranges.chunks(workers).enumerate() {
            let wave_start = wave_index * workers;
            let results = std::thread::scope(|scope| {
                let mut handles = Vec::with_capacity(wave.len());
                for (offset, range) in wave.iter().enumerate() {
                    let run = first_run + (wave_start + offset) as u64;
                    let slice = &blobs[range.clone()];
                    let pk_path = self.run_path(RunOrder::Pk, run);
                    let fsync = self.fsync;
                    let document_id = first_document_id + range.start as u64;
                    handles.push(scope.spawn(move || {
                        // Term-grouped runs: sorted by (term, doc) as before
                        // but the term is written once per group, and the
                        // bucket/rank each posting carries were computed
                        // HERE, in the parallel workers — the sequential
                        // merge advances per term and recomputes nothing.
                        let mut postings = decode_postings(slice, document_id)?;
                        postings.sort_by(|left, right| posting_cmp(left, right, RunOrder::Pk));
                        write_grouped_run_atomic(&pk_path, &postings, fsync)
                    }));
                }
                handles
                    .into_iter()
                    .map(|handle| {
                        handle.join().map_err(|_| {
                            BicDbError::Index("full-text posting-run worker panicked".to_string())
                        })?
                    })
                    .collect::<Result<Vec<_>>>()
            });
            results?;
        }
        self.checkpoint.next_run += ranges.len() as u64;
        self.checkpoint.last_pk = blobs.last().map(|(pk, _)| pk.clone());
        for (_, blob) in blobs {
            let statistics = crate::paged_collection::decode_doc_term_statistics(blob)?;
            self.checkpoint.document_count = self.checkpoint.document_count.saturating_add(1);
            self.checkpoint.total_document_length = self
                .checkpoint
                .total_document_length
                .saturating_add(u64::from(statistics.document_length));
            for (total, length) in self
                .checkpoint
                .field_total_lengths
                .iter_mut()
                .zip(statistics.field_lengths)
            {
                *total = total.saturating_add(u64::from(length));
            }
        }
        self.save()
    }

    pub(crate) fn finish_tokenizing(&mut self) -> Result<()> {
        if self.checkpoint.phase == FtsBuildPhase::Tokenizing {
            check_crash(FtsBuildPhase::MergePk)?;
            self.checkpoint.phase = FtsBuildPhase::MergePk;
            self.save()?;
        }
        Ok(())
    }

    pub(crate) fn set_phase(&mut self, phase: FtsBuildPhase) -> Result<()> {
        check_crash(phase)?;
        self.checkpoint.phase = phase;
        self.save()
    }

    /// Plan a parallel range merge: collapse to at most the fan-in, verify
    /// every run's digest up front (range workers never read a file end to
    /// end, so EOF verification cannot apply), and choose split terms that
    /// partition the term space into ranges of roughly equal payload bytes.
    /// Returns `None` when one worker (or a trivial corpus) makes ranges
    /// pointless.
    pub(crate) fn plan_term_ranges(
        &self,
        workers: usize,
        memory_bytes: usize,
    ) -> Result<Option<(Vec<PathBuf>, Vec<Vec<u8>>)>> {
        let parts = workers.clamp(1, 16);
        if parts < 2 {
            return Ok(None);
        }
        let (next_level, inputs) = self.active_run_paths(RunOrder::Pk)?;
        if inputs.is_empty() {
            return Ok(None);
        }
        let merge_workers = workers
            .clamp(1, 8)
            .min((memory_bytes / (2 * MERGE_READER_BUDGET)).max(1));
        let fan_in =
            (memory_bytes / (merge_workers * MERGE_READER_BUDGET)).clamp(2, MAX_RUN_FAN_IN);
        let paths = self.collapse_runs(RunOrder::Pk, inputs, next_level, merge_workers, fan_in)?;
        for wave in paths.chunks(merge_workers.max(1)) {
            std::thread::scope(|scope| {
                let handles = wave
                    .iter()
                    .map(|path| scope.spawn(move || verify_run_digest(path)))
                    .collect::<Vec<_>>();
                for handle in handles {
                    handle.join().map_err(|_| {
                        BicDbError::Index("full-text digest worker panicked".to_string())
                    })??;
                }
                Ok::<(), BicDbError>(())
            })?;
        }
        let splits = sample_term_splits(&paths[0], parts)?;
        if splits.is_empty() {
            return Ok(None);
        }
        Ok(Some((paths, splits)))
    }

    /// Stream every (term, postings) of the build in ascending term order.
    /// Level-collapses when the run count exceeds the fan-in, exactly like
    /// the posting-level merge did — a collapse output is itself a grouped
    /// run, so resume semantics are unchanged.
    pub(crate) fn stream_merged_terms(
        &self,
        workers: usize,
        memory_bytes: usize,
        visit: impl FnMut(TermStreamEvent) -> Result<()>,
    ) -> Result<()> {
        let (next_level, inputs) = self.active_run_paths(RunOrder::Pk)?;
        if inputs.is_empty() {
            return Ok(());
        }
        let merge_workers = workers
            .clamp(1, 8)
            .min((memory_bytes / (2 * MERGE_READER_BUDGET)).max(1));
        let fan_in =
            (memory_bytes / (merge_workers * MERGE_READER_BUDGET)).clamp(2, MAX_RUN_FAN_IN);
        let paths = self.collapse_runs(RunOrder::Pk, inputs, next_level, merge_workers, fan_in)?;
        merge_grouped_paths(&paths, visit)
    }

    pub(crate) fn remove(self) -> Result<()> {
        remove_build_dir(&self.dir)
    }

    fn save(&self) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(&self.checkpoint)?;
        crate::storage::write_atomic(&self.dir.join(CHECKPOINT_FILE), &bytes, self.fsync)
    }

    /// Existing level-0 run files in `[from, to)`, in run order.
    pub(crate) fn run_paths_in(&self, from: u64, to: u64) -> Vec<PathBuf> {
        (from..to)
            .map(|run| self.run_path(RunOrder::Pk, run))
            .filter(|path| path.exists())
            .collect()
    }

    /// Persist progressive bookkeeping after a sub-segment publish.
    pub(crate) fn record_progressive_sub(
        &mut self,
        next_run: u64,
        docs: u64,
        terms: u64,
        old_generation: Option<String>,
    ) -> Result<()> {
        self.checkpoint.progressive_subs += 1;
        self.checkpoint.progressive_next_run = next_run;
        self.checkpoint.progressive_docs = docs;
        self.checkpoint.progressive_terms += terms;
        if self.checkpoint.progressive_old_generation.is_none() {
            self.checkpoint.progressive_old_generation = old_generation;
        }
        self.save()
    }

    /// A scratch file inside the workspace directory, removed with it.
    pub(crate) fn scratch_path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    fn run_path(&self, order: RunOrder, run: u64) -> PathBuf {
        self.dir.join(format!("{}-{run:020}.run", order.label()))
    }

    fn original_run_paths(&self, order: RunOrder) -> Result<Vec<PathBuf>> {
        let prefix = format!("{}-", order.label());
        let mut paths = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name.starts_with(&prefix) && name.ends_with(".run") && !name.contains("-merge-") {
                paths.push(path);
            }
        }
        paths.sort();
        Ok(paths)
    }

    fn active_run_paths(&self, order: RunOrder) -> Result<(usize, Vec<PathBuf>)> {
        let path = self.merge_checkpoint_path(order);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok((0, self.original_run_paths(order)?));
            }
            Err(error) => return Err(error.into()),
        };
        let checkpoint: MergeLevelCheckpoint = serde_json::from_slice(&bytes)?;
        if checkpoint.version != MERGE_CHECKPOINT_VERSION {
            return Err(BicDbError::Index(format!(
                "unsupported full-text {} merge checkpoint version {}",
                order.label(),
                checkpoint.version
            )));
        }
        let mut paths = Vec::with_capacity(checkpoint.runs.len());
        for name in &checkpoint.runs {
            let relative = Path::new(name);
            if relative.components().count() != 1 || relative.file_name().is_none() {
                return Err(BicDbError::Index(format!(
                    "invalid full-text {} merge checkpoint path",
                    order.label()
                )));
            }
            let run = self.dir.join(relative);
            if !run.is_file() {
                return Err(BicDbError::Index(format!(
                    "full-text {} merge checkpoint references missing run `{}`",
                    order.label(),
                    run.display()
                )));
            }
            paths.push(run);
        }
        Ok((checkpoint.level.saturating_add(1), paths))
    }

    fn save_merge_checkpoint(
        &self,
        order: RunOrder,
        level: usize,
        paths: &[PathBuf],
    ) -> Result<()> {
        let runs = paths
            .iter()
            .map(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .map(str::to_string)
                    .ok_or_else(|| {
                        BicDbError::Index(
                            "full-text merge run has a non-UTF-8 filename".to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let bytes = serde_json::to_vec_pretty(&MergeLevelCheckpoint {
            version: MERGE_CHECKPOINT_VERSION,
            level,
            runs,
        })?;
        crate::storage::write_atomic(&self.merge_checkpoint_path(order), &bytes, self.fsync)
    }

    fn merge_checkpoint_path(&self, order: RunOrder) -> PathBuf {
        self.dir
            .join(format!("{}-merge-checkpoint.json", order.label()))
    }

    fn collapse_runs(
        &self,
        order: RunOrder,
        mut inputs: Vec<PathBuf>,
        mut level: usize,
        workers: usize,
        fan_in: usize,
    ) -> Result<Vec<PathBuf>> {
        while inputs.len() > fan_in {
            let groups = inputs
                .chunks(fan_in)
                .map(|chunk| chunk.to_vec())
                .collect::<Vec<_>>();
            let outputs = (0..groups.len())
                .map(|group| {
                    self.dir
                        .join(format!("{}-merge-{level:04}-{group:08}.run", order.label()))
                })
                .collect::<Vec<_>>();
            for wave_start in (0..groups.len()).step_by(workers) {
                let wave_end = (wave_start + workers).min(groups.len());
                std::thread::scope(|scope| {
                    let mut handles = Vec::new();
                    for group in wave_start..wave_end {
                        let inputs = &groups[group];
                        let output = &outputs[group];
                        let fsync = self.fsync;
                        handles.push(scope.spawn(move || {
                            // A completed merge run is itself a level
                            // checkpoint. Atomic rename guarantees an existing
                            // `.run` is complete; `.tmp` is removed on open.
                            if output.exists() {
                                Ok(())
                            } else {
                                let _ = order;
                                merge_grouped_to_run_atomic(inputs, output, fsync)
                            }
                        }));
                    }
                    for handle in handles {
                        handle.join().map_err(|_| {
                            BicDbError::Index("full-text merge worker panicked".to_string())
                        })??;
                    }
                    Ok::<(), BicDbError>(())
                })?;
            }
            // The manifest is the merge-level checkpoint. Only after it is
            // atomically durable can the preceding level be reclaimed. A
            // crash before this write reuses any completed outputs by name;
            // a crash after it resumes from the new level.
            self.save_merge_checkpoint(order, level, &outputs)?;
            for input in &inputs {
                if !outputs.contains(input) {
                    let _ = fs::remove_file(input);
                }
            }
            inputs = outputs;
            level += 1;
        }
        Ok(inputs)
    }

    fn remove_uncheckpointed_runs(&self) -> Result<()> {
        for order in [RunOrder::Pk, RunOrder::Impact] {
            let prefix = format!("{}-", order.label());
            for entry in fs::read_dir(&self.dir)? {
                let path = entry?.path();
                let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                let Some(number) = name
                    .strip_prefix(&prefix)
                    .and_then(|name| name.strip_suffix(".run"))
                    .and_then(|number| number.parse::<u64>().ok())
                else {
                    continue;
                };
                if number >= self.checkpoint.next_run {
                    fs::remove_file(path)?;
                }
            }
        }
        Ok(())
    }
}

fn partition_blobs(blobs: &[(String, Vec<u8>)], target: usize) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::new();
    let mut start = 0usize;
    let mut bytes = 0usize;
    for (index, (pk, blob)) in blobs.iter().enumerate() {
        let next = pk.len().saturating_add(blob.len()).saturating_add(32);
        if index > start && bytes.saturating_add(next) > target {
            ranges.push(start..index);
            start = index;
            bytes = 0;
        }
        bytes = bytes.saturating_add(next);
    }
    if start < blobs.len() {
        ranges.push(start..blobs.len());
    }
    ranges
}

fn decode_postings(blobs: &[(String, Vec<u8>)], first_document_id: u64) -> Result<Vec<RunPosting>> {
    let mut postings = Vec::new();
    for (offset, (_, blob)) in blobs.iter().enumerate() {
        let (doc_length, doc_distinct, terms) = decode_doc_terms(blob)?;
        for (term, packed_positions) in terms {
            // Defense in depth for checkpoints/doc-term blobs produced by an
            // older tokenizer. New SQL tokenization applies the same rule
            // earlier, but a resumed build must never fail on legacy input.
            if !crate::db::full_text_term_is_indexable(&term) {
                continue;
            }
            postings.push(RunPosting {
                encoded_term: crate::db::full_text_term_entry_key(&term),
                impact_bucket: crate::db::fts_impact_bucket(&packed_positions),
                impact_rank: crate::db::fts_rank_single_term(
                    &packed_positions,
                    [0.1, 0.2, 0.4, 1.0],
                ),
                posting: NumericBlockPosting {
                    document_id: first_document_id + offset as u64,
                    doc_length,
                    doc_distinct,
                    packed_positions,
                },
            });
        }
    }
    Ok(postings)
}

fn new_checkpoint(
    logical_index: &str,
    collection: &str,
    source_signature: &str,
) -> FtsBuildCheckpoint {
    FtsBuildCheckpoint {
        version: BUILD_CHECKPOINT_VERSION,
        logical_index: logical_index.to_string(),
        collection: collection.to_string(),
        source_signature: source_signature.to_string(),
        // SHORT ON PURPOSE. This string is embedded in the key of EVERY entry
        // in the index keyspace — postings, impacts, dictionary, doc-terms,
        // doc-ids. On a real Common Crawl index that is 18.4 million keys, and
        // the previous 49-byte `$bicdb_fts_build_<uuid-simple>` cost ~858 MiB
        // of nothing but one repeated constant: 62% of all key bytes, and
        // roughly 1.6 GB on disk once page amplification is applied.
        //
        // 8 hex characters of a v4 UUID is 32 bits. These names only need to be
        // unique among the live generations of ONE database — a handful at a
        // time, created by an operator building an index — not globally, and a
        // collision would be caught by the catalog before any data was written.
        physical_index: format!("$f{}", &Uuid::new_v4().simple().to_string()[..8]),
        phase: FtsBuildPhase::Tokenizing,
        last_pk: None,
        next_run: 0,
        document_count: 0,
        total_document_length: 0,
        field_total_lengths: [0; 4],
        progressive_subs: 0,
        progressive_next_run: 0,
        progressive_docs: 0,
        progressive_terms: 0,
        progressive_old_generation: None,
    }
}

fn build_dir_name(index: &str) -> String {
    let digest = Sha256::digest(index.as_bytes());
    format!("idx-{}", hex::encode(&digest[..16]))
}

fn cleanup_temporary_files(dir: &Path) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("tmp") {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

fn remove_build_dir(dir: &Path) -> Result<()> {
    match fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// One event of the term-grouped merge stream. Terms arrive in ascending
/// order exactly once; each term's postings arrive in ascending document
/// order (runs cover disjoint ascending document ranges, so equal terms
/// concatenate — nothing per-posting is ever compared or heaped).
pub(crate) enum TermStreamEvent<'a> {
    Term(&'a [u8]),
    Posting {
        posting: crate::paged_collection::NumericBlockPosting,
        impact_bucket: u16,
        impact_rank: f32,
    },
}

fn write_grouped_posting(
    writer: &mut impl Write,
    previous_document_id: u64,
    posting: &RunPosting,
) -> Result<()> {
    write_len(
        writer,
        (posting.posting.document_id - previous_document_id) as usize,
    )?;
    write_len(writer, posting.posting.doc_length as usize)?;
    write_len(writer, posting.posting.doc_distinct as usize)?;
    writer.write_all(&posting.impact_bucket.to_be_bytes())?;
    writer.write_all(&posting.impact_rank.to_bits().to_be_bytes())?;
    write_len(writer, posting.posting.packed_positions.len())?;
    for position in &posting.posting.packed_positions {
        writer.write_all(&position.to_be_bytes())?;
    }
    Ok(())
}

/// Write `postings` — sorted by (term, document id) — as a term-grouped run.
fn write_grouped_run_atomic(path: &Path, postings: &[RunPosting], fsync: bool) -> Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let file = File::create(&tmp)?;
        let mut writer = DigestWriter::new(BufWriter::new(file));
        writer.write_all(GROUPED_RUN_MAGIC)?;
        let mut scratch: Vec<u8> = Vec::with_capacity(64 * 1024);
        let mut at = 0usize;
        while at < postings.len() {
            let term = &postings[at].encoded_term;
            let end = at
                + postings[at..]
                    .iter()
                    .take_while(|posting| &posting.encoded_term == term)
                    .count();
            let mut chunk_start = at;
            while chunk_start < end {
                let chunk_end = (chunk_start + GROUP_POSTING_CAP).min(end);
                scratch.clear();
                let mut previous = 0u64;
                for posting in &postings[chunk_start..chunk_end] {
                    write_grouped_posting(&mut scratch, previous, posting)?;
                    previous = posting.posting.document_id;
                }
                write_len(&mut writer, term.len())?;
                writer.write_all(term)?;
                write_len(&mut writer, chunk_end - chunk_start)?;
                write_len(&mut writer, scratch.len())?;
                writer.write_all(&scratch)?;
                chunk_start = chunk_end;
            }
            at = end;
        }
        let mut writer = writer.finish()?;
        writer.flush()?;
        if fsync {
            writer.get_ref().sync_all()?;
        }
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Streaming reader over one term-grouped run: `advance_term` then
/// `next_posting` × count. Verifies the digest at end of payload.
struct GroupedRunReader {
    reader: std::io::Take<DigestReader<BufReader<File>>>,
    term: Vec<u8>,
    remaining_postings: u64,
    previous_document_id: u64,
    expected_digest: [u8; 32],
    verified: bool,
    path: PathBuf,
}

impl GroupedRunReader {
    fn open(path: &Path) -> Result<Self> {
        let (payload_len, expected_digest) = read_run_trailer(path)?;
        let mut reader = DigestReader::new(BufReader::new(File::open(path)?)).take(payload_len);
        let mut magic = [0u8; GROUPED_RUN_MAGIC.len()];
        reader.read_exact(&mut magic)?;
        if &magic != GROUPED_RUN_MAGIC {
            return Err(BicDbError::Index(format!(
                "invalid full-text posting run `{}`",
                path.display()
            )));
        }
        Ok(Self {
            reader,
            term: Vec::new(),
            remaining_postings: 0,
            previous_document_id: 0,
            expected_digest,
            verified: false,
            path: path.to_path_buf(),
        })
    }

    fn verify_digest(&mut self) -> Result<()> {
        if self.verified {
            return Ok(());
        }
        self.verified = true;
        let actual = self.reader.get_mut().digest();
        if actual != self.expected_digest {
            return Err(BicDbError::Index(format!(
                "full-text run `{}` failed its checksum; refusing to build an \
                 index from corrupt intermediate data",
                self.path.display()
            )));
        }
        Ok(())
    }

    fn corrupt(&self, error: impl std::fmt::Display) -> BicDbError {
        BicDbError::Index(format!(
            "full-text run `{}` is corrupt: {error}; refusing to build an \
             index from damaged intermediate data",
            self.path.display()
        ))
    }

    /// Move to the next term group. `false` at a clean end of the run.
    fn advance_term(&mut self) -> Result<bool> {
        self.advance_term_inner()
            .map_err(|error| self.corrupt(error))
    }

    fn advance_term_inner(&mut self) -> Result<bool> {
        if self.remaining_postings != 0 {
            return Err(BicDbError::Index(format!(
                "full-text run `{}` advanced terms mid-group",
                self.path.display()
            )));
        }
        let Some(term_len) = read_len_or_eof(&mut self.reader)? else {
            self.verify_digest()?;
            return Ok(false);
        };
        self.term.resize(term_len, 0);
        self.reader.read_exact(&mut self.term)?;
        self.remaining_postings = read_len(&mut self.reader)? as u64;
        let _group_payload_len = read_len(&mut self.reader)?;
        if self.remaining_postings == 0 {
            return Err(BicDbError::Index(format!(
                "full-text run `{}` holds an empty term group",
                self.path.display()
            )));
        }
        self.previous_document_id = 0;
        Ok(true)
    }

    fn next_posting(&mut self) -> Result<(crate::paged_collection::NumericBlockPosting, u16, f32)> {
        self.next_posting_inner()
            .map_err(|error| self.corrupt(error))
    }

    fn next_posting_inner(
        &mut self,
    ) -> Result<(crate::paged_collection::NumericBlockPosting, u16, f32)> {
        debug_assert!(self.remaining_postings > 0);
        self.remaining_postings -= 1;
        let decoded = read_grouped_posting(&mut self.reader, self.previous_document_id)?;
        self.previous_document_id = decoded.0.document_id;
        Ok(decoded)
    }
}

/// Decode one grouped posting record relative to `previous_document_id`.
fn read_grouped_posting(
    reader: &mut impl Read,
    previous_document_id: u64,
) -> Result<(crate::paged_collection::NumericBlockPosting, u16, f32)> {
    let delta = read_len(reader)? as u64;
    let document_id = previous_document_id + delta;
    let doc_length = read_len(reader)? as u32;
    let doc_distinct = read_len(reader)? as u32;
    let impact_bucket = read_u16(reader)?;
    let impact_rank = f32::from_bits(read_u32(reader)?);
    let positions_len = read_len(reader)?;
    let mut packed_positions = Vec::with_capacity(positions_len);
    for _ in 0..positions_len {
        packed_positions.push(read_u16(reader)?);
    }
    Ok((
        crate::paged_collection::NumericBlockPosting {
            document_id,
            doc_length,
            doc_distinct,
            packed_positions,
        },
        impact_bucket,
        impact_rank,
    ))
}

/// Choose up to `parts - 1` split terms from one run's group headers so the
/// ranges hold roughly equal payload bytes. One run suffices: every run is
/// a slice of the same corpus in document order, so its term-to-bytes
/// distribution mirrors the whole. Skims headers only — no posting decoded.
fn sample_term_splits(path: &Path, parts: usize) -> Result<Vec<Vec<u8>>> {
    let mut reader = RangedGroupedRunReader::open(path, None, None)?;
    let mut terms: Vec<(Vec<u8>, u64)> = Vec::new();
    let mut consumed = 0u64;
    while reader.load_group_header()? {
        consumed += reader.group_payload_bytes;
        match terms.last_mut() {
            Some((last, bytes)) if *last == reader.term => *bytes = consumed,
            _ => terms.push((reader.term.clone(), consumed)),
        }
        reader
            .reader
            .seek_relative(reader.group_payload_bytes as i64)?;
        reader.payload_remaining = reader
            .payload_remaining
            .saturating_sub(reader.group_payload_bytes);
        reader.remaining_postings = 0;
    }
    let total = consumed.max(1);
    let mut splits: Vec<Vec<u8>> = Vec::with_capacity(parts.saturating_sub(1));
    for part in 1..parts {
        let target = total * part as u64 / parts as u64;
        let at = terms.partition_point(|(_, bytes)| *bytes < target);
        if let Some((term, _)) = terms.get(at) {
            if splits.last().map(|last| last < term).unwrap_or(true) {
                splits.push(term.clone());
            }
        }
    }
    Ok(splits)
}

/// Stream one term RANGE `[lower, upper)` of the merged build.
pub(crate) fn stream_term_range(
    paths: &[PathBuf],
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    visit: impl FnMut(TermStreamEvent) -> Result<()>,
) -> Result<()> {
    let readers = paths
        .iter()
        .map(|path| RangedGroupedRunReader::open(path, lower, upper))
        .collect::<Result<Vec<_>>>()?;
    merge_grouped_sources(readers, visit)
}

/// Hash a run's payload against its trailer without decoding a byte of it.
/// The parallel range merge verifies every run up front this way, because
/// no single range worker reads any file end to end.
fn verify_run_digest(path: &Path) -> Result<()> {
    let (payload_len, expected_digest) = read_run_trailer(path)?;
    let mut reader = BufReader::with_capacity(256 * 1024, File::open(path)?).take(payload_len);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let actual: [u8; 32] = hasher.finalize().into();
    if actual != expected_digest {
        return Err(BicDbError::Index(format!(
            "full-text run `{}` failed its checksum; refusing to build an \
             index from corrupt intermediate data",
            path.display()
        )));
    }
    Ok(())
}

/// A source of term groups for the k-way merge — the digest-verifying
/// whole-run reader and the seek-ahead range reader both implement it.
trait GroupSource {
    fn advance_term(&mut self) -> Result<bool>;
    fn term(&self) -> &[u8];
    fn remaining_postings(&self) -> u64;
    fn next_posting(&mut self) -> Result<(crate::paged_collection::NumericBlockPosting, u16, f32)>;
}

impl GroupSource for GroupedRunReader {
    fn advance_term(&mut self) -> Result<bool> {
        GroupedRunReader::advance_term(self)
    }
    fn term(&self) -> &[u8] {
        &self.term
    }
    fn remaining_postings(&self) -> u64 {
        self.remaining_postings
    }
    fn next_posting(&mut self) -> Result<(crate::paged_collection::NumericBlockPosting, u16, f32)> {
        GroupedRunReader::next_posting(self)
    }
}

/// A grouped-run reader restricted to terms in `[lower, upper)`. Skips
/// whole groups via their byte-length prefix without decoding postings.
/// Integrity comes from the up-front digest pass over every run — no range
/// worker reads any file end to end, so EOF verification cannot apply.
struct RangedGroupedRunReader {
    reader: BufReader<File>,
    /// Payload bytes not yet consumed or skipped (excludes the trailer).
    payload_remaining: u64,
    term: Vec<u8>,
    remaining_postings: u64,
    group_payload_bytes: u64,
    /// A header was loaded by the skim but not yet handed to the merge.
    pending: bool,
    previous_document_id: u64,
    upper: Option<Vec<u8>>,
    path: PathBuf,
}

impl RangedGroupedRunReader {
    fn open(path: &Path, lower: Option<&[u8]>, upper: Option<&[u8]>) -> Result<Self> {
        let (payload_len, _digest) = read_run_trailer(path)?;
        let mut reader = BufReader::with_capacity(256 * 1024, File::open(path)?);
        let mut magic = [0u8; GROUPED_RUN_MAGIC.len()];
        reader.read_exact(&mut magic)?;
        if &magic != GROUPED_RUN_MAGIC {
            return Err(BicDbError::Index(format!(
                "invalid full-text posting run `{}`",
                path.display()
            )));
        }
        let mut ranged = Self {
            reader,
            payload_remaining: payload_len - GROUPED_RUN_MAGIC.len() as u64,
            term: Vec::new(),
            remaining_postings: 0,
            group_payload_bytes: 0,
            pending: false,
            previous_document_id: 0,
            upper: upper.map(<[u8]>::to_vec),
            path: path.to_path_buf(),
        };
        if let Some(lower) = lower {
            // Skim: header reads plus one relative seek per skipped group.
            while ranged.load_group_header()? {
                if ranged.term.as_slice() >= lower {
                    ranged.pending = true;
                    break;
                }
                ranged
                    .reader
                    .seek_relative(ranged.group_payload_bytes as i64)?;
                ranged.payload_remaining = ranged
                    .payload_remaining
                    .saturating_sub(ranged.group_payload_bytes);
                ranged.remaining_postings = 0;
            }
        }
        Ok(ranged)
    }

    fn corrupt(&self, error: impl std::fmt::Display) -> BicDbError {
        BicDbError::Index(format!(
            "full-text run `{}` is corrupt: {error}; refusing to build an \
             index from damaged intermediate data",
            self.path.display()
        ))
    }

    /// Read the next group header. `false` at the end of the payload.
    /// Consumes header bytes from `payload_remaining`; the group body is
    /// consumed by streaming or by the skim's seek.
    fn load_group_header(&mut self) -> Result<bool> {
        if self.payload_remaining == 0 {
            return Ok(false);
        }
        let mut header_bytes = 0u64;
        let term_len = read_len(&mut self.reader).map_err(|e| self.corrupt(e))?;
        header_bytes += 4;
        self.term.resize(term_len, 0);
        self.reader
            .read_exact(&mut self.term)
            .map_err(|e| self.corrupt(e))?;
        header_bytes += term_len as u64;
        self.remaining_postings = read_len(&mut self.reader).map_err(|e| self.corrupt(e))? as u64;
        self.group_payload_bytes = read_len(&mut self.reader).map_err(|e| self.corrupt(e))? as u64;
        header_bytes += 8;
        if self.remaining_postings == 0 {
            return Err(self.corrupt("empty term group"));
        }
        self.payload_remaining = self.payload_remaining.saturating_sub(header_bytes);
        self.previous_document_id = 0;
        Ok(true)
    }
}

impl GroupSource for RangedGroupedRunReader {
    fn advance_term(&mut self) -> Result<bool> {
        if !self.pending {
            // The skim leaves a fully-loaded header pending with its posting
            // count already set; the mid-group guard applies only to fresh
            // advances.
            if self.remaining_postings != 0 {
                return Err(self.corrupt("advanced terms mid-group"));
            }
            if !self.load_group_header()? {
                return Ok(false);
            }
        }
        self.pending = false;
        self.payload_remaining = self
            .payload_remaining
            .saturating_sub(self.group_payload_bytes);
        if let Some(upper) = &self.upper {
            if self.term.as_slice() >= upper.as_slice() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn term(&self) -> &[u8] {
        &self.term
    }

    fn remaining_postings(&self) -> u64 {
        self.remaining_postings
    }

    fn next_posting(&mut self) -> Result<(crate::paged_collection::NumericBlockPosting, u16, f32)> {
        debug_assert!(self.remaining_postings > 0);
        self.remaining_postings -= 1;
        let previous = self.previous_document_id;
        let decoded = read_grouped_posting(&mut self.reader, previous)
            .map_err(|error| self.corrupt(error))?;
        self.previous_document_id = decoded.0.document_id;
        Ok(decoded)
    }
}

/// k-way merge of grouped runs by TERM. Equal terms are consumed in run
/// order, which is document-range order — the tokenize waves assign each
/// run a contiguous, ascending slice of document ids — so a term's postings
/// concatenate already sorted. The heap holds one entry per (run, term)
/// switch instead of one per posting.
fn merge_grouped_paths(
    paths: &[PathBuf],
    visit: impl FnMut(TermStreamEvent) -> Result<()>,
) -> Result<()> {
    let readers = paths
        .iter()
        .map(|path| GroupedRunReader::open(path))
        .collect::<Result<Vec<_>>>()?;
    merge_grouped_sources(readers, visit)
}

fn merge_grouped_sources(
    mut readers: Vec<impl GroupSource>,
    mut visit: impl FnMut(TermStreamEvent) -> Result<()>,
) -> Result<()> {
    let mut heap: BinaryHeap<std::cmp::Reverse<(Vec<u8>, usize)>> = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if reader.advance_term()? {
            heap.push(std::cmp::Reverse((reader.term().to_vec(), index)));
        }
    }
    while let Some(std::cmp::Reverse((term, index))) = heap.pop() {
        visit(TermStreamEvent::Term(&term))?;
        let mut current = index;
        loop {
            {
                let reader = &mut readers[current];
                // A monster term splits into several consecutive groups
                // within one run; keep streaming until the run moves on.
                loop {
                    while reader.remaining_postings() > 0 {
                        let (posting, impact_bucket, impact_rank) = reader.next_posting()?;
                        visit(TermStreamEvent::Posting {
                            posting,
                            impact_bucket,
                            impact_rank,
                        })?;
                    }
                    if !reader.advance_term()? {
                        break;
                    }
                    if reader.term() != term.as_slice() {
                        heap.push(std::cmp::Reverse((reader.term().to_vec(), current)));
                        break;
                    }
                }
            }
            // Tuple order (term, run index) makes consecutive pops of the
            // same term arrive in ascending run order.
            match heap.peek() {
                Some(std::cmp::Reverse((next_term, _))) if *next_term == term => {
                    let std::cmp::Reverse((_, next_index)) =
                        heap.pop().expect("peeked entry exists");
                    current = next_index;
                }
                _ => break,
            }
        }
    }
    Ok(())
}

/// Collapse-level variant: merge grouped inputs into one grouped output.
/// Memory is bounded at one CHUNK, not one term — a chunk flushes as soon
/// as it reaches the group cap, re-emitting the term header.
fn merge_grouped_to_run_atomic(inputs: &[PathBuf], output: &Path, fsync: bool) -> Result<()> {
    let tmp = output.with_extension("tmp");
    {
        let file = File::create(&tmp)?;
        let mut writer = DigestWriter::new(BufWriter::new(file));
        writer.write_all(GROUPED_RUN_MAGIC)?;
        let mut term: Vec<u8> = Vec::new();
        let mut chunk: Vec<(crate::paged_collection::NumericBlockPosting, u16, f32)> =
            Vec::with_capacity(GROUP_POSTING_CAP);
        let mut scratch: Vec<u8> = Vec::with_capacity(64 * 1024);
        let mut flush_chunk =
            |writer: &mut DigestWriter<BufWriter<File>>,
             term: &[u8],
             chunk: &mut Vec<(crate::paged_collection::NumericBlockPosting, u16, f32)>|
             -> Result<()> {
                if chunk.is_empty() {
                    return Ok(());
                }
                scratch.clear();
                let mut previous = 0u64;
                for (posting, impact_bucket, impact_rank) in chunk.iter() {
                    write_len(&mut scratch, (posting.document_id - previous) as usize)?;
                    write_len(&mut scratch, posting.doc_length as usize)?;
                    write_len(&mut scratch, posting.doc_distinct as usize)?;
                    scratch.extend_from_slice(&impact_bucket.to_be_bytes());
                    scratch.extend_from_slice(&impact_rank.to_bits().to_be_bytes());
                    write_len(&mut scratch, posting.packed_positions.len())?;
                    for position in &posting.packed_positions {
                        scratch.extend_from_slice(&position.to_be_bytes());
                    }
                    previous = posting.document_id;
                }
                write_len(writer, term.len())?;
                writer.write_all(term)?;
                write_len(writer, chunk.len())?;
                write_len(writer, scratch.len())?;
                writer.write_all(&scratch)?;
                chunk.clear();
                Ok(())
            };
        merge_grouped_paths(inputs, |event| match event {
            TermStreamEvent::Term(next) => {
                flush_chunk(&mut writer, &term, &mut chunk)?;
                term.clear();
                term.extend_from_slice(next);
                Ok(())
            }
            TermStreamEvent::Posting {
                posting,
                impact_bucket,
                impact_rank,
            } => {
                chunk.push((posting, impact_bucket, impact_rank));
                if chunk.len() >= GROUP_POSTING_CAP {
                    flush_chunk(&mut writer, &term, &mut chunk)?;
                }
                Ok(())
            }
        })?;
        flush_chunk(&mut writer, &term, &mut chunk)?;
        let mut writer = writer.finish()?;
        writer.flush()?;
        if fsync {
            writer.get_ref().sync_all()?;
        }
    }
    fs::rename(&tmp, output)?;
    Ok(())
}

fn posting_cmp(left: &RunPosting, right: &RunPosting, order: RunOrder) -> Ordering {
    left.encoded_term
        .cmp(&right.encoded_term)
        .then_with(|| match order {
            RunOrder::Pk => left.posting.document_id.cmp(&right.posting.document_id),
            RunOrder::Impact => right
                .impact_bucket
                .cmp(&left.impact_bucket)
                .then_with(|| left.posting.document_id.cmp(&right.posting.document_id)),
        })
}

/// Wraps a writer and digests everything written through it.
struct DigestWriter<W: Write> {
    inner: W,
    hasher: Sha256,
    written: u64,
}

impl<W: Write> DigestWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            written: 0,
        }
    }

    /// Append the trailer and return the inner writer.
    fn finish(mut self) -> Result<W> {
        let digest = self.hasher.finalize();
        self.inner.write_all(RUN_TRAILER_MAGIC)?;
        self.inner.write_all(&self.written.to_be_bytes())?;
        self.inner.write_all(&digest)?;
        Ok(self.inner)
    }
}

impl<W: Write> Write for DigestWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.hasher.update(&buf[..written]);
        self.written += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Wraps a reader and digests everything read through it, so verification
/// costs no extra I/O — the merge was reading these bytes anyway.
struct DigestReader<R: Read> {
    inner: R,
    hasher: Sha256,
}

impl<R: Read> DigestReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
        }
    }

    fn digest(&mut self) -> [u8; 32] {
        let finished = std::mem::replace(&mut self.hasher, Sha256::new()).finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&finished);
        out
    }
}

impl<R: Read> Read for DigestReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.hasher.update(&buf[..read]);
        Ok(read)
    }
}

/// Read a run's trailer and return the payload length it claims.
///
/// Refuses a file whose actual size disagrees with the recorded payload
/// length — the signature of a write interrupted by power loss.
fn read_run_trailer(path: &Path) -> Result<(u64, [u8; 32])> {
    use std::io::{Seek, SeekFrom};
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    if size < RUN_TRAILER_BYTES {
        return Err(BicDbError::Index(format!(
            "full-text run `{}` is too short to carry a trailer; it was not \
             completely written",
            path.display()
        )));
    }
    file.seek(SeekFrom::End(-(RUN_TRAILER_BYTES as i64)))?;
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)?;
    if &magic != RUN_TRAILER_MAGIC {
        return Err(BicDbError::Index(format!(
            "full-text run `{}` has no completion trailer; it was truncated \
             or never finished",
            path.display()
        )));
    }
    let mut length = [0u8; 8];
    file.read_exact(&mut length)?;
    let payload_len = u64::from_be_bytes(length);
    let mut digest = [0u8; 32];
    file.read_exact(&mut digest)?;
    if payload_len.saturating_add(RUN_TRAILER_BYTES) != size {
        return Err(BicDbError::Index(format!(
            "full-text run `{}` records {payload_len} payload bytes but holds \
             {}; it was torn by an interrupted write",
            path.display(),
            size.saturating_sub(RUN_TRAILER_BYTES)
        )));
    }
    Ok((payload_len, digest))
}

fn write_len(writer: &mut impl Write, len: usize) -> Result<()> {
    let len = u32::try_from(len)
        .map_err(|_| BicDbError::Index("full-text run field exceeds 4 GiB".to_string()))?;
    writer.write_all(&len.to_be_bytes())?;
    Ok(())
}

fn read_len_or_eof(reader: &mut impl Read) -> Result<Option<usize>> {
    let mut bytes = [0u8; 4];
    let mut read = 0usize;
    while read < bytes.len() {
        match reader.read(&mut bytes[read..])? {
            0 if read == 0 => return Ok(None),
            0 => {
                return Err(BicDbError::Index(
                    "truncated full-text posting run".to_string(),
                ))
            }
            count => read += count,
        }
    }
    Ok(Some(u32::from_be_bytes(bytes) as usize))
}

fn read_len(reader: &mut impl Read) -> Result<usize> {
    read_len_or_eof(reader)?
        .ok_or_else(|| BicDbError::Index("truncated full-text posting run".to_string()))
}

fn read_u32(reader: &mut impl Read) -> Result<u32> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u16(reader: &mut impl Read) -> Result<u16> {
    let mut bytes = [0u8; 2];
    reader.read_exact(&mut bytes)?;
    Ok(u16::from_be_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> Result<u64> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grouped_runs_round_trip_and_concatenate_by_term() {
        let dir = tempfile::tempdir().unwrap();
        let posting = |term: &str, doc: u64, positions: Vec<u16>| RunPosting {
            encoded_term: term.as_bytes().to_vec(),
            impact_bucket: crate::db::fts_impact_bucket(&positions),
            impact_rank: crate::db::fts_rank_single_term(&positions, [0.1, 0.2, 0.4, 1.0]),
            posting: crate::paged_collection::NumericBlockPosting {
                document_id: doc,
                doc_length: 40 + doc as u32,
                doc_distinct: 7,
                packed_positions: positions,
            },
        };
        // Two runs over DISJOINT ascending doc ranges sharing term "beta".
        let first = vec![
            posting("alpha", 0, vec![1, 2]),
            posting("beta", 1, vec![(3 << 14) | 5]),
        ];
        let second = vec![
            posting("beta", 10, vec![9]),
            posting("gamma", 11, vec![2, 3, 4]),
        ];
        let first_path = dir.path().join("g-0.run");
        let second_path = dir.path().join("g-1.run");
        write_grouped_run_atomic(&first_path, &first, false).unwrap();
        write_grouped_run_atomic(&second_path, &second, false).unwrap();

        let mut terms: Vec<String> = Vec::new();
        let mut postings: Vec<(u64, u16, u32, Vec<u16>)> = Vec::new();
        merge_grouped_paths(&[first_path, second_path], |event| {
            match event {
                TermStreamEvent::Term(term) => {
                    terms.push(String::from_utf8(term.to_vec()).unwrap());
                }
                TermStreamEvent::Posting {
                    posting,
                    impact_bucket,
                    impact_rank,
                } => {
                    assert!(impact_rank.is_finite());
                    postings.push((
                        posting.document_id,
                        impact_bucket,
                        posting.doc_length,
                        posting.packed_positions,
                    ));
                }
            }
            Ok(())
        })
        .unwrap();

        assert_eq!(terms, ["alpha", "beta", "gamma"]);
        // "beta" concatenated across runs in doc order; every field intact.
        assert_eq!(
            postings
                .iter()
                .map(|(doc, _, _, _)| *doc)
                .collect::<Vec<_>>(),
            [0, 1, 10, 11]
        );
        assert_eq!(
            postings[1].1,
            crate::db::fts_impact_bucket(&[(3 << 14) | 5])
        );
        assert_eq!(postings[3].3, vec![2, 3, 4]);
        assert_eq!(postings[2].2, 50);
    }
}
