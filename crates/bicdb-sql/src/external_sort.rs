//! External merge sort for full-table ORDER BY (Phase 5b).
//!
//! Rows are pushed with a PRE-NORMALIZED, memcmp-comparable sort key; the
//! sorter buffers up to a byte budget, sorts the buffer, spills it as a run
//! file, and merges runs in bounded-fan-in passes. Sort memory, temporary disk,
//! and open file descriptors are therefore bounded independently of input
//! size; only the OUTPUT of the query materializes.
//!
//! Stability: the caller's key is extended with a global sequence number, so
//! equal keys emit in insertion order — the same contract as the stable
//! in-memory sort this replaces.
//!
//! The spill codec is a TAGGED encoding of [`SqlValue`]. The enum's own serde
//! is `#[serde(untagged)]`, which cannot round-trip (a Geometry or TsQuery
//! serializes as a plain string and would deserialize as one); the tag makes
//! the round trip exact. A value the codec does not cover aborts the whole
//! attempt — the caller deletes the spill and falls back to the materializing
//! path, which is always correct.

use std::collections::{BinaryHeap, HashMap};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use bicdb_core::{CancellationToken, DatabaseObjectCipher};

use crate::SqlValue;
use crate::{Result, SqlError};

#[cfg(test)]
fn spill_dir() -> std::path::PathBuf {
    std::env::var_os("BICDB_SPILL_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("bicdb-sql-spill-{}", std::process::id()))
        })
}

fn spill_dir_for_db(db: &bicdb_core::BicDb) -> PathBuf {
    std::env::var_os("BICDB_SPILL_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| db.data_path().join("tmp").join("sql-spill"))
}

static NEXT_SPILL_WORKSPACE: AtomicU64 = AtomicU64::new(1);
static SPILL_MANAGERS: OnceLock<Mutex<HashMap<PathBuf, Weak<SpillManager>>>> = OnceLock::new();

#[derive(Clone, Copy, Debug)]
pub(crate) struct ExternalSortLimits {
    pub(crate) memory_bytes: usize,
    pub(crate) temp_bytes: u64,
    pub(crate) server_temp_bytes: u64,
    pub(crate) merge_fan_in: usize,
}

impl ExternalSortLimits {
    pub(crate) fn for_db(db: &bicdb_core::BicDb) -> Self {
        Self {
            memory_bytes: db.query_work_memory_bytes(),
            temp_bytes: db.query_temp_space_bytes(),
            server_temp_bytes: db.query_server_temp_space_bytes(),
            merge_fan_in: db.query_merge_fan_in(),
        }
    }

    fn normalized(self) -> Self {
        Self {
            memory_bytes: self.memory_bytes.max(64 * 1024),
            temp_bytes: self.temp_bytes.max(64 * 1024),
            server_temp_bytes: self.server_temp_bytes.max(64 * 1024),
            merge_fan_in: self.merge_fan_in.clamp(2, 128),
        }
    }
}

impl Default for ExternalSortLimits {
    fn default() -> Self {
        Self {
            memory_bytes: 64 * 1024 * 1024,
            temp_bytes: 8 * 1024 * 1024 * 1024,
            server_temp_bytes: 64 * 1024 * 1024 * 1024,
            merge_fan_in: 32,
        }
    }
}

#[derive(Debug)]
struct SpillManager {
    root: PathBuf,
    limit_bytes: AtomicU64,
    used_bytes: AtomicU64,
}

impl SpillManager {
    fn acquire(root: PathBuf, limit_bytes: u64) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&root)
            .map_err(|error| SqlError::InvalidSql(format!("sort spill directory: {error}")))?;
        let root = std::fs::canonicalize(&root).map_err(|error| {
            SqlError::InvalidSql(format!("sort spill directory canonicalize: {error}"))
        })?;
        let managers = SPILL_MANAGERS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut managers = managers
            .lock()
            .map_err(|_| SqlError::InvalidSql("sort spill manager lock poisoned".to_string()))?;
        managers.retain(|_, manager| manager.strong_count() > 0);
        if let Some(manager) = managers.get(&root).and_then(Weak::upgrade) {
            // A shared root has one aggregate ceiling. If callers disagree,
            // the stricter live configuration wins; no caller can raise it.
            manager
                .limit_bytes
                .fetch_min(limit_bytes.max(64 * 1024), Ordering::AcqRel);
            return Ok(manager);
        }

        cleanup_abandoned_workspaces(&root)?;
        let manager = Arc::new(Self {
            root: root.clone(),
            limit_bytes: AtomicU64::new(limit_bytes.max(64 * 1024)),
            used_bytes: AtomicU64::new(0),
        });
        managers.insert(root, Arc::downgrade(&manager));
        Ok(manager)
    }

    fn reserve(&self, bytes: u64) -> Result<()> {
        let mut used = self.used_bytes.load(Ordering::Acquire);
        loop {
            let requested = used.saturating_add(bytes);
            let limit = self.limit_bytes.load(Ordering::Acquire);
            if requested > limit {
                return Err(SqlError::resource_limit(
                    "53100",
                    format!(
                        "concurrent sort temporary data would require {requested} bytes, exceeding query_server_temp_space_bytes={limit}"
                    ),
                ));
            }
            match self.used_bytes.compare_exchange_weak(
                used,
                requested,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => used = actual,
            }
        }
    }

    fn release(&self, bytes: u64) {
        let _ = self
            .used_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                Some(used.saturating_sub(bytes))
            });
    }
}

fn cleanup_abandoned_workspaces(root: &Path) -> Result<()> {
    for entry in std::fs::read_dir(root)
        .map_err(|error| SqlError::InvalidSql(format!("sort spill cleanup scan: {error}")))?
    {
        let entry = entry
            .map_err(|error| SqlError::InvalidSql(format!("sort spill cleanup entry: {error}")))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with("bicdb-sort-") {
            continue;
        }
        let file_type = entry.file_type().map_err(|error| {
            SqlError::InvalidSql(format!("sort spill cleanup metadata: {error}"))
        })?;
        let result = if file_type.is_dir() {
            std::fs::remove_dir_all(entry.path())
        } else {
            // Includes legacy `bicdb-sort-*.run` files and symlinks. Removing
            // the directory entry never follows a symlink to another target.
            std::fs::remove_file(entry.path())
        };
        result.map_err(|error| {
            SqlError::InvalidSql(format!(
                "sort spill cleanup {}: {error}",
                entry.path().display()
            ))
        })?;
    }
    Ok(())
}

#[derive(Debug)]
struct SpillWorkspace {
    path: PathBuf,
}

impl SpillWorkspace {
    fn create(root: &Path) -> Result<Self> {
        for _ in 0..1_024 {
            let id = NEXT_SPILL_WORKSPACE.fetch_add(1, Ordering::Relaxed);
            let path = root.join(format!("bicdb-sort-{}-{id}", std::process::id()));
            match std::fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(SqlError::InvalidSql(format!(
                        "sort spill workspace create: {error}"
                    )))
                }
            }
        }
        Err(SqlError::InvalidSql(
            "sort spill workspace namespace exhausted".to_string(),
        ))
    }
}

impl Drop for SpillWorkspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[derive(Debug)]
struct RunFile {
    path: PathBuf,
    bytes: u64,
}

struct RunReader {
    reader: BufReader<std::fs::File>,
    max_frame_bytes: usize,
    path: PathBuf,
    cipher: Option<DatabaseObjectCipher>,
    frame_index: u64,
}

impl RunReader {
    fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<SqlValue>)>> {
        let Some(cipher) = self.cipher.as_ref() else {
            return read_entry(&mut self.reader, self.max_frame_bytes);
        };
        let Some(ciphertext) =
            read_bytes(&mut self.reader, self.max_frame_bytes.saturating_add(128))?
        else {
            return Ok(None);
        };
        let aad = spill_frame_aad(self.frame_index);
        let plaintext = cipher
            .open_for(
                bicdb_core::EncryptionObjectPurpose::Temporary,
                &self.path,
                &ciphertext,
                &aad,
            )
            .map_err(|error| SqlError::InvalidSql(format!("sort spill decrypt: {error}")))?;
        self.frame_index = self.frame_index.saturating_add(1);
        if plaintext.len() > self.max_frame_bytes {
            return Err(SqlError::InvalidSql(format!(
                "decrypted sort spill frame declares {} bytes, exceeding the configured bound {}",
                plaintext.len(),
                self.max_frame_bytes
            )));
        }
        let mut cursor = std::io::Cursor::new(plaintext.as_slice());
        let entry = read_entry(&mut cursor, self.max_frame_bytes)?.ok_or_else(|| {
            SqlError::InvalidSql("encrypted sort spill frame is empty".to_string())
        })?;
        if cursor.position() != plaintext.len() as u64 {
            return Err(SqlError::InvalidSql(
                "encrypted sort spill frame has trailing data".to_string(),
            ));
        }
        Ok(Some(entry))
    }
}

fn spill_frame_aad(frame_index: u64) -> [u8; 32] {
    let mut aad = [0_u8; 32];
    aad[..24].copy_from_slice(b"bicdb-sql-spill-frame-v1");
    aad[24..].copy_from_slice(&frame_index.to_be_bytes());
    aad
}

fn merge_runs(
    runs: &[RunFile],
    max_frame_bytes: usize,
    cancellation: &CancellationToken,
    cipher: Option<&DatabaseObjectCipher>,
    mut visit: impl FnMut(&[u8], Vec<SqlValue>) -> Result<()>,
) -> Result<()> {
    cancellation.check().map_err(SqlError::from)?;
    let mut readers = Vec::with_capacity(runs.len());
    for run in runs {
        cancellation.check().map_err(SqlError::from)?;
        let file = std::fs::File::open(&run.path)
            .map_err(|error| SqlError::InvalidSql(format!("sort spill open: {error}")))?;
        readers.push(RunReader {
            reader: BufReader::new(file),
            max_frame_bytes,
            path: run.path.clone(),
            cipher: cipher.cloned(),
            frame_index: 0,
        });
    }

    // Min-heap over (key, run index); rows ride outside the heap because
    // SqlValue is not Ord. Each reader has exactly one pending row.
    let mut pending: Vec<Option<Vec<SqlValue>>> = Vec::with_capacity(readers.len());
    let mut heap: BinaryHeap<std::cmp::Reverse<(Vec<u8>, usize)>> = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        cancellation.check().map_err(SqlError::from)?;
        match reader.next()? {
            Some((key, row)) => {
                pending.push(Some(row));
                heap.push(std::cmp::Reverse((key, index)));
            }
            None => pending.push(None),
        }
    }
    while let Some(std::cmp::Reverse((key, index))) = heap.pop() {
        cancellation.check().map_err(SqlError::from)?;
        let row = pending[index]
            .take()
            .ok_or_else(|| SqlError::InvalidSql("sort merge lost a pending row".to_string()))?;
        visit(&key, row)?;
        if let Some((key, row)) = readers[index].next()? {
            pending[index] = Some(row);
            heap.push(std::cmp::Reverse((key, index)));
        }
    }
    cancellation.check().map_err(SqlError::from)?;
    Ok(())
}

/// Append one normalized key component: a null marker ordered by NULLS
/// FIRST/LAST, or the escaped (0x00 -> 0x00 0xFF), terminated (0x00 0x00),
/// and — for DESC — byte-inverted encoding of `bytes`. Components stay
/// memcmp-ordered when concatenated; the terminator removes prefix cases so
/// inversion reverses the order exactly.
pub(crate) fn push_normalized_component(
    key: &mut Vec<u8>,
    bytes: Option<&[u8]>,
    descending: bool,
    nulls_first: bool,
) {
    let Some(bytes) = bytes else {
        key.push(if nulls_first { 0x00 } else { 0x02 });
        return;
    };
    key.push(0x01);
    let mut escaped = Vec::with_capacity(bytes.len() + 2);
    for byte in bytes {
        if *byte == 0 {
            escaped.extend_from_slice(&[0x00, 0xFF]);
        } else {
            escaped.push(*byte);
        }
    }
    escaped.extend_from_slice(&[0x00, 0x00]);
    if descending {
        for byte in &mut escaped {
            *byte = !*byte;
        }
    }
    key.extend_from_slice(&escaped);
}

pub(crate) struct ExternalSorter {
    buffer: Vec<(Vec<u8>, Vec<SqlValue>)>,
    buffer_bytes: usize,
    runs: Vec<RunFile>,
    sequence: u64,
    limits: ExternalSortLimits,
    cancellation: CancellationToken,
    spill_root: PathBuf,
    cipher: Option<DatabaseObjectCipher>,
    spill_manager: Option<Arc<SpillManager>>,
    workspace: Option<SpillWorkspace>,
    next_run: u64,
    temp_bytes: u64,
    peak_temp_bytes: u64,
    /// Bytes spilled across all runs, for observability and tests.
    pub(crate) spilled_bytes: u64,
}

impl ExternalSorter {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::new_with_limits(ExternalSortLimits::default())
    }

    pub(crate) fn for_db(db: &bicdb_core::BicDb, cancellation: CancellationToken) -> Self {
        Self::new_with_limits_at_root(
            ExternalSortLimits::for_db(db),
            spill_dir_for_db(db),
            cancellation,
            db.bound_object_cipher(),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_limits(limits: ExternalSortLimits) -> Self {
        Self::new_with_limits_at_root(limits, spill_dir(), CancellationToken::uncancelable(), None)
    }

    fn new_with_limits_at_root(
        limits: ExternalSortLimits,
        spill_root: PathBuf,
        cancellation: CancellationToken,
        cipher: Option<DatabaseObjectCipher>,
    ) -> Self {
        Self {
            buffer: Vec::new(),
            buffer_bytes: 0,
            runs: Vec::new(),
            sequence: 0,
            limits: limits.normalized(),
            cancellation,
            spill_root,
            cipher,
            spill_manager: None,
            workspace: None,
            next_run: 0,
            temp_bytes: 0,
            peak_temp_bytes: 0,
            spilled_bytes: 0,
        }
    }

    /// Number of run files spilled so far (tests assert > 1 under a tiny
    /// budget to prove the merge actually merges).
    #[cfg(test)]
    pub(crate) fn run_count(&self) -> usize {
        self.runs.len()
    }

    pub(crate) fn push(&mut self, mut key: Vec<u8>, row: Vec<SqlValue>) -> Result<()> {
        // Stability tail: insertion order breaks key ties.
        key.extend_from_slice(&self.sequence.to_be_bytes());
        self.sequence += 1;
        let entry_bytes = key
            .capacity()
            .saturating_add(row.capacity() * std::mem::size_of::<SqlValue>())
            .saturating_add(
                row.iter()
                    .map(crate::sql_value_memory_estimate)
                    .sum::<usize>(),
            )
            .saturating_add(std::mem::size_of::<(Vec<u8>, Vec<SqlValue>)>());
        if entry_bytes > self.limits.memory_bytes {
            return Err(SqlError::resource_limit(
                "53200",
                format!(
                    "one sort row requires approximately {entry_bytes} bytes, exceeding query_work_memory_bytes={}",
                    self.limits.memory_bytes
                ),
            ));
        }
        if !self.buffer.is_empty()
            && self.buffer_bytes.saturating_add(entry_bytes) > self.limits.memory_bytes
        {
            self.spill_run()?;
        }
        self.buffer_bytes = self.buffer_bytes.saturating_add(entry_bytes);
        self.buffer.push((key, row));
        if self.buffer_bytes >= self.limits.memory_bytes {
            self.spill_run()?;
        }
        Ok(())
    }

    fn next_run_path(&mut self) -> Result<PathBuf> {
        let manager = self.spill_manager()?;
        if self.workspace.is_none() {
            self.workspace = Some(SpillWorkspace::create(&manager.root)?);
        }
        let path = self
            .workspace
            .as_ref()
            .expect("spill workspace initialized")
            .path
            .join(format!("run-{:020}.bin", self.next_run));
        self.next_run = self.next_run.saturating_add(1);
        Ok(path)
    }

    fn spill_manager(&mut self) -> Result<Arc<SpillManager>> {
        if self.spill_manager.is_none() {
            self.spill_manager = Some(SpillManager::acquire(
                self.spill_root.clone(),
                self.limits.server_temp_bytes,
            )?);
        }
        Ok(Arc::clone(
            self.spill_manager
                .as_ref()
                .expect("spill manager initialized"),
        ))
    }

    fn reserve_temp(&mut self, bytes: u64) -> Result<()> {
        let requested = self.temp_bytes.saturating_add(bytes);
        if requested > self.limits.temp_bytes {
            return Err(SqlError::resource_limit(
                "53100",
                format!(
                    "sort temporary data would require {requested} bytes, exceeding query_temp_space_bytes={}",
                    self.limits.temp_bytes
                ),
            ));
        }
        self.spill_manager()?.reserve(bytes)?;
        self.temp_bytes = requested;
        self.peak_temp_bytes = self.peak_temp_bytes.max(requested);
        self.spilled_bytes = self.spilled_bytes.saturating_add(bytes);
        Ok(())
    }

    fn spill_run(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.cancellation.check().map_err(SqlError::from)?;
        self.buffer.sort_by(|left, right| left.0.cmp(&right.0));
        self.cancellation.check().map_err(SqlError::from)?;
        let path = self.next_run_path()?;
        let file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|error| SqlError::InvalidSql(format!("sort spill create: {error}")))?;
        let mut writer = BufWriter::new(file);
        let mut written = 0u64;
        let mut frame_index = 0_u64;
        let entries = std::mem::take(&mut self.buffer);
        self.buffer_bytes = 0;
        for (key, row) in entries {
            self.cancellation.check().map_err(SqlError::from)?;
            let mut encoded = Vec::new();
            write_entry(&mut encoded, &key, &row)?;
            if encoded.len() > self.limits.memory_bytes {
                return Err(SqlError::resource_limit(
                    "53200",
                    format!(
                        "one encoded sort row requires {} bytes, exceeding query_work_memory_bytes={}",
                        encoded.len(),
                        self.limits.memory_bytes
                    ),
                ));
            }
            let frame = if let Some(cipher) = self.cipher.as_ref() {
                let aad = spill_frame_aad(frame_index);
                let ciphertext = cipher
                    .seal_for(
                        bicdb_core::EncryptionObjectPurpose::Temporary,
                        &path,
                        &encoded,
                        &aad,
                    )
                    .map_err(|error| {
                        SqlError::InvalidSql(format!("sort spill encrypt: {error}"))
                    })?;
                let mut frame = Vec::with_capacity(ciphertext.len().saturating_add(4));
                write_bytes(&mut frame, &ciphertext)?;
                frame
            } else {
                encoded
            };
            self.reserve_temp(frame.len() as u64)?;
            writer
                .write_all(&frame)
                .map_err(|error| SqlError::InvalidSql(format!("sort spill write: {error}")))?;
            written = written.saturating_add(frame.len() as u64);
            frame_index = frame_index.saturating_add(1);
        }
        writer
            .flush()
            .map_err(|error| SqlError::InvalidSql(format!("sort spill flush: {error}")))?;
        self.cancellation.check().map_err(SqlError::from)?;
        self.runs.push(RunFile {
            path,
            bytes: written,
        });
        Ok(())
    }

    fn reduce_runs_to_fan_in(&mut self) -> Result<()> {
        while self.runs.len() > self.limits.merge_fan_in {
            let mut inputs = std::mem::take(&mut self.runs);
            let mut outputs = Vec::new();
            while !inputs.is_empty() {
                let take = inputs.len().min(self.limits.merge_fan_in);
                let group = inputs.drain(..take).collect::<Vec<_>>();
                if group.len() == 1 {
                    outputs.extend(group);
                    continue;
                }

                let path = self.next_run_path()?;
                let file = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&path)
                    .map_err(|error| {
                        SqlError::InvalidSql(format!("sort merge run create: {error}"))
                    })?;
                let mut writer = BufWriter::new(file);
                let mut written = 0u64;
                let mut frame_index = 0_u64;
                let cancellation = self.cancellation.clone();
                let cipher = self.cipher.clone();
                merge_runs(
                    &group,
                    self.limits.memory_bytes,
                    &cancellation,
                    cipher.as_ref(),
                    |key, row| {
                        let mut encoded = Vec::new();
                        write_entry(&mut encoded, key, &row)?;
                        let frame_bytes = if cipher.is_some() {
                            let ciphertext = cipher
                                .as_ref()
                                .expect("cipher presence checked")
                                .seal_for(
                                    bicdb_core::EncryptionObjectPurpose::Temporary,
                                    &path,
                                    &encoded,
                                    &spill_frame_aad(frame_index),
                                )
                                .map_err(|error| {
                                    SqlError::InvalidSql(format!("sort spill encrypt: {error}"))
                                })?;
                            let mut frame = Vec::with_capacity(ciphertext.len().saturating_add(4));
                            write_bytes(&mut frame, &ciphertext)?;
                            frame
                        } else {
                            encoded
                        };
                        self.reserve_temp(frame_bytes.len() as u64)?;
                        writer.write_all(&frame_bytes).map_err(|error| {
                            SqlError::InvalidSql(format!("sort merge run write: {error}"))
                        })?;
                        written = written.saturating_add(frame_bytes.len() as u64);
                        frame_index = frame_index.saturating_add(1);
                        Ok(())
                    },
                )?;
                writer.flush().map_err(|error| {
                    SqlError::InvalidSql(format!("sort merge run flush: {error}"))
                })?;

                for run in group {
                    std::fs::remove_file(&run.path).map_err(|error| {
                        SqlError::InvalidSql(format!(
                            "sort merge input cleanup {}: {error}",
                            run.path.display()
                        ))
                    })?;
                    self.temp_bytes = self.temp_bytes.saturating_sub(run.bytes);
                    if let Some(manager) = &self.spill_manager {
                        manager.release(run.bytes);
                    }
                }
                outputs.push(RunFile {
                    path,
                    bytes: written,
                });
            }
            self.runs = outputs;
        }
        Ok(())
    }

    /// Sort everything pushed and return the rows in key order. Consumes the
    /// sorter; run files are removed before returning.
    pub(crate) fn finish(self) -> Result<Vec<Vec<SqlValue>>> {
        let mut rows = Vec::new();
        self.finish_each(|_, row| {
            rows.push(row);
            Ok(())
        })?;
        Ok(rows)
    }

    /// Stream every entry in key order through `visit` without materializing
    /// the sorted set: `visit(key, row)` receives the CALLER's key (the
    /// stability tail is stripped), so grouping can detect key boundaries.
    /// This is the bounded form grouping builds on — its output is one row
    /// per GROUP, never one per input row.
    pub(crate) fn finish_each(
        mut self,
        mut visit: impl FnMut(&[u8], Vec<SqlValue>) -> Result<()>,
    ) -> Result<()> {
        fn caller_key(key: &[u8]) -> &[u8] {
            &key[..key.len().saturating_sub(8)]
        }
        if self.runs.is_empty() {
            // Everything fit in memory: one stable in-memory sort.
            self.buffer.sort_by(|left, right| left.0.cmp(&right.0));
            for (key, row) in self.buffer.drain(..) {
                visit(caller_key(&key), row)?;
            }
            return Ok(());
        }
        self.spill_run()?;
        self.reduce_runs_to_fan_in()?;
        let cancellation = self.cancellation.clone();
        merge_runs(
            &self.runs,
            self.limits.memory_bytes,
            &cancellation,
            self.cipher.as_ref(),
            |key, row| visit(caller_key(key), row),
        )?;
        Ok(())
    }
}

impl Drop for ExternalSorter {
    fn drop(&mut self) {
        for run in &self.runs {
            let _ = std::fs::remove_file(&run.path);
        }
        // Remove the owned directory before returning its aggregate quota;
        // another query may reserve those bytes immediately afterwards.
        drop(self.workspace.take());
        if let Some(manager) = &self.spill_manager {
            manager.release(self.temp_bytes);
            self.temp_bytes = 0;
        }
    }
}

// ---- spill codec -----------------------------------------------------------

fn write_bytes(writer: &mut impl Write, bytes: &[u8]) -> Result<u64> {
    writer
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .and_then(|()| writer.write_all(bytes))
        .map_err(|error| SqlError::InvalidSql(format!("sort spill write: {error}")))?;
    Ok(4 + bytes.len() as u64)
}

fn read_bytes(reader: &mut impl Read, max_bytes: usize) -> Result<Option<Vec<u8>>> {
    let mut len = [0u8; 4];
    match reader.read(&mut len[..1]) {
        Ok(0) => return Ok(None),
        Ok(1) => {}
        Ok(_) => unreachable!("a one-byte read returned more than one byte"),
        Err(error) => {
            return Err(SqlError::InvalidSql(format!("sort spill read: {error}")));
        }
    }
    reader
        .read_exact(&mut len[1..])
        .map_err(|error| SqlError::InvalidSql(format!("sort spill read: {error}")))?;
    let len = u32::from_be_bytes(len) as usize;
    if len > max_bytes {
        return Err(SqlError::InvalidSql(format!(
            "sort spill frame declares {len} bytes, exceeding the configured decode bound {max_bytes}"
        )));
    }
    let mut bytes = vec![0u8; len];
    reader
        .read_exact(&mut bytes)
        .map_err(|error| SqlError::InvalidSql(format!("sort spill read: {error}")))?;
    Ok(Some(bytes))
}

/// Encode one value, tagged. Errors on variants the codec does not cover —
/// the caller treats that as "this query cannot spill" and falls back.
fn encode_value(value: &SqlValue, out: &mut Vec<u8>) -> Result<()> {
    match value {
        SqlValue::Null => out.push(0),
        SqlValue::Bool(value) => {
            out.push(1);
            out.push(*value as u8);
        }
        SqlValue::Int(value) => {
            out.push(2);
            out.extend_from_slice(&value.to_be_bytes());
        }
        SqlValue::Float(value) => {
            out.push(3);
            out.extend_from_slice(&value.to_be_bytes());
        }
        SqlValue::String(value) => {
            out.push(4);
            out.extend_from_slice(&(value.len() as u32).to_be_bytes());
            out.extend_from_slice(value.as_bytes());
        }
        // Structured variants round-trip through their own (tagged-enough)
        // serde within a KNOWN variant, so the ambiguity of SqlValue's
        // untagged serde never applies.
        SqlValue::JsonText(value) => {
            out.push(5);
            let bytes = serde_json::to_vec(value)
                .map_err(|error| SqlError::InvalidSql(format!("sort spill encode: {error}")))?;
            out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(&bytes);
        }
        SqlValue::Json(value) => {
            out.push(6);
            let bytes = serde_json::to_vec(value)
                .map_err(|error| SqlError::InvalidSql(format!("sort spill encode: {error}")))?;
            out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(&bytes);
        }
        SqlValue::Geometry(value) => {
            out.push(7);
            let bytes = serde_json::to_vec(value)
                .map_err(|error| SqlError::InvalidSql(format!("sort spill encode: {error}")))?;
            out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(&bytes);
        }
        // TsQuery and Composite are rare in sorted projections and their
        // round-trip fidelity is the least certain — decline, fall back.
        SqlValue::TsQuery(_) | SqlValue::Composite(_) => {
            return Err(SqlError::Unsupported(
                "spill codec does not cover this value".to_string(),
            ));
        }
    }
    Ok(())
}

fn decode_value(bytes: &[u8], cursor: &mut usize) -> Result<SqlValue> {
    let corrupt = || SqlError::InvalidSql("sort spill decode: truncated entry".to_string());
    let tag = *bytes.get(*cursor).ok_or_else(corrupt)?;
    *cursor += 1;
    let mut take = |count: usize| -> Result<&[u8]> {
        let slice = bytes.get(*cursor..*cursor + count).ok_or_else(corrupt)?;
        *cursor += count;
        Ok(slice)
    };
    Ok(match tag {
        0 => SqlValue::Null,
        1 => SqlValue::Bool(take(1)?[0] != 0),
        2 => SqlValue::Int(i64::from_be_bytes(take(8)?.try_into().unwrap())),
        3 => SqlValue::Float(f64::from_be_bytes(take(8)?.try_into().unwrap())),
        4 => {
            let len = u32::from_be_bytes(take(4)?.try_into().unwrap()) as usize;
            SqlValue::String(
                String::from_utf8(take(len)?.to_vec())
                    .map_err(|error| SqlError::InvalidSql(format!("sort spill decode: {error}")))?,
            )
        }
        5 => {
            let len = u32::from_be_bytes(take(4)?.try_into().unwrap()) as usize;
            SqlValue::JsonText(
                serde_json::from_slice(take(len)?)
                    .map_err(|error| SqlError::InvalidSql(format!("sort spill decode: {error}")))?,
            )
        }
        6 => {
            let len = u32::from_be_bytes(take(4)?.try_into().unwrap()) as usize;
            SqlValue::Json(
                serde_json::from_slice(take(len)?)
                    .map_err(|error| SqlError::InvalidSql(format!("sort spill decode: {error}")))?,
            )
        }
        7 => {
            let len = u32::from_be_bytes(take(4)?.try_into().unwrap()) as usize;
            SqlValue::Geometry(
                serde_json::from_slice(take(len)?)
                    .map_err(|error| SqlError::InvalidSql(format!("sort spill decode: {error}")))?,
            )
        }
        other => {
            return Err(SqlError::InvalidSql(format!(
                "sort spill decode: unknown tag {other}"
            )));
        }
    })
}

fn write_entry(writer: &mut impl Write, key: &[u8], row: &[SqlValue]) -> Result<u64> {
    let mut payload = Vec::with_capacity(key.len() + 16);
    payload.extend_from_slice(&(row.len() as u32).to_be_bytes());
    for value in row {
        encode_value(value, &mut payload)?;
    }
    let mut checksum = crc32fast::Hasher::new();
    checksum.update(key);
    checksum.update(&payload);
    let checksum = checksum.finalize().to_be_bytes();
    let written = write_bytes(writer, key)? + write_bytes(writer, &payload)?;
    writer
        .write_all(&checksum)
        .map_err(|error| SqlError::InvalidSql(format!("sort spill write: {error}")))?;
    Ok(written + checksum.len() as u64)
}

fn read_entry(
    reader: &mut impl Read,
    max_frame_bytes: usize,
) -> Result<Option<(Vec<u8>, Vec<SqlValue>)>> {
    let Some(key) = read_bytes(reader, max_frame_bytes)? else {
        return Ok(None);
    };
    let payload = read_bytes(reader, max_frame_bytes)?
        .ok_or_else(|| SqlError::InvalidSql("sort spill decode: missing row".to_string()))?;
    let mut expected_checksum = [0u8; 4];
    reader.read_exact(&mut expected_checksum).map_err(|error| {
        SqlError::InvalidSql(format!("sort spill decode: missing checksum: {error}"))
    })?;
    let mut checksum = crc32fast::Hasher::new();
    checksum.update(&key);
    checksum.update(&payload);
    if checksum.finalize().to_be_bytes() != expected_checksum {
        return Err(SqlError::InvalidSql(
            "sort spill decode: checksum mismatch".to_string(),
        ));
    }
    let mut cursor = 0usize;
    let count = u32::from_be_bytes(
        payload
            .get(0..4)
            .ok_or_else(|| SqlError::InvalidSql("sort spill decode: truncated row".to_string()))?
            .try_into()
            .unwrap(),
    ) as usize;
    cursor += 4;
    let mut row = Vec::with_capacity(count);
    for _ in 0..count {
        row.push(decode_value(&payload, &mut cursor)?);
    }
    Ok(Some((key, row)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(value: i64) -> Vec<u8> {
        // Order-preserving i64: flip the sign bit.
        ((value as u64) ^ (1 << 63)).to_be_bytes().to_vec()
    }

    #[test]
    fn in_memory_sort_orders_and_keeps_ties_stable() {
        let mut sorter = ExternalSorter::new();
        for (index, value) in [3i64, 1, 2, 1, 3].iter().enumerate() {
            sorter
                .push(key(*value), vec![SqlValue::Int(index as i64)])
                .unwrap();
        }
        let rows: Vec<i64> = sorter
            .finish()
            .unwrap()
            .into_iter()
            .map(|row| match row[0] {
                SqlValue::Int(value) => value,
                _ => unreachable!(),
            })
            .collect();
        // Values 1,1,2,3,3 -> original indices 1,3,2,0,4 (ties in order).
        assert_eq!(rows, vec![1, 3, 2, 0, 4]);
    }

    #[test]
    fn spilled_runs_merge_to_the_same_order() {
        let limits = ExternalSortLimits {
            memory_bytes: 64 * 1024,
            temp_bytes: 64 * 1024 * 1024,
            server_temp_bytes: 64 * 1024 * 1024,
            merge_fan_in: 2,
        };
        let mut sorter = ExternalSorter::new_with_limits(limits);
        let mut expected: Vec<i64> = Vec::new();
        let mut state = 0x1234_5678_u64;
        for index in 0..1_000i64 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let value = (state >> 40) as i64;
            expected.push(value);
            sorter
                .push(
                    key(value),
                    vec![
                        SqlValue::Int(value),
                        SqlValue::Int(index),
                        SqlValue::String("x".repeat(4_096)),
                    ],
                )
                .unwrap();
        }
        assert!(
            sorter.run_count() > 2,
            "byte budget did not force many runs"
        );
        sorter.reduce_runs_to_fan_in().unwrap();
        assert!(sorter.run_count() <= 2, "merge fan-in was not enforced");
        assert!(sorter.peak_temp_bytes <= limits.temp_bytes);
        let paths: Vec<_> = sorter.runs.iter().map(|run| run.path.clone()).collect();
        let rows = sorter.finish().unwrap();
        expected.sort();
        let sorted: Vec<i64> = rows
            .iter()
            .map(|row| match row[0] {
                SqlValue::Int(value) => value,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(sorted, expected);
        for path in paths {
            assert!(!path.exists(), "run file {path:?} not cleaned up");
        }
    }

    #[test]
    fn temp_quota_fails_closed_and_workspace_is_removed() {
        let limits = ExternalSortLimits {
            memory_bytes: 64 * 1024,
            temp_bytes: 64 * 1024,
            server_temp_bytes: 64 * 1024 * 1024,
            merge_fan_in: 2,
        };
        let mut sorter = ExternalSorter::new_with_limits(limits);
        let mut error = None;
        for index in 0..1_000i64 {
            if let Err(found) =
                sorter.push(key(index), vec![SqlValue::String("quota".repeat(1_024))])
            {
                error = Some(found);
                break;
            }
        }
        let error = error.expect("temporary quota should reject the sort");
        assert!(error.is_resource_limit());
        assert_eq!(error.sqlstate(), "53100");
        let workspace = sorter
            .workspace
            .as_ref()
            .expect("quota path should have created a workspace")
            .path
            .clone();
        drop(sorter);
        assert!(
            !workspace.exists(),
            "failed sort left temporary data behind"
        );
    }

    #[test]
    fn cancellation_removes_spills_and_returns_shared_quota() {
        let root = tempfile::tempdir().unwrap();
        let cancellation = CancellationToken::uncancelable();
        let limits = ExternalSortLimits {
            memory_bytes: 64 * 1024,
            temp_bytes: 64 * 1024 * 1024,
            server_temp_bytes: 64 * 1024 * 1024,
            merge_fan_in: 2,
        };
        let mut sorter = ExternalSorter::new_with_limits_at_root(
            limits,
            root.path().to_path_buf(),
            cancellation.clone(),
            None,
        );
        for index in 0..100i64 {
            sorter
                .push(key(index), vec![SqlValue::String("cancel".repeat(1_024))])
                .unwrap();
        }
        let workspace = sorter
            .workspace
            .as_ref()
            .expect("test input should spill")
            .path
            .clone();
        let manager = Arc::clone(
            sorter
                .spill_manager
                .as_ref()
                .expect("spilled sort should hold the manager"),
        );
        assert!(manager.used_bytes.load(Ordering::Acquire) > 0);

        cancellation.cancel();
        let error = sorter.finish().unwrap_err();
        assert_eq!(error.sqlstate(), "57014");
        assert!(!workspace.exists(), "canceled sort left spill files behind");
        assert_eq!(manager.used_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn cell_bound_database_encrypts_external_sort_spills() {
        let root = tempfile::tempdir().unwrap();
        let database_path = root.path().join("db");
        let database = bicdb_core::BicDb::open_with_encryption(
            &database_path,
            bicdb_core::DbConfig::default()
                .with_fsync(false)
                .with_query_work_memory_bytes(64 * 1024),
            bicdb_core::EncryptionConfig::with_raw_key([0x41; 32]).with_binding(
                bicdb_core::EncryptionBinding::new(
                    "018f7b30-4f4d-7b5c-a1f6-a183663e1240",
                    "cell-bound-test",
                    7,
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let canary = "confidential-sort-canary-89f0d2";
        let mut sorter = ExternalSorter::for_db(&database, CancellationToken::uncancelable());
        for index in (0..160_i64).rev() {
            sorter
                .push(
                    key(index),
                    vec![SqlValue::String(format!("{canary}-{}", "x".repeat(1_024)))],
                )
                .unwrap();
        }
        assert!(sorter.run_count() > 1);
        let workspace = sorter.workspace.as_ref().unwrap().path.clone();
        let mut persisted = Vec::new();
        for entry in std::fs::read_dir(&workspace).unwrap() {
            persisted.extend_from_slice(&std::fs::read(entry.unwrap().path()).unwrap());
        }
        assert!(
            !persisted
                .windows(canary.len())
                .any(|window| window == canary.as_bytes()),
            "cell-bound SQL spill exposed plaintext"
        );
        let rows = sorter.finish().unwrap();
        assert_eq!(rows.len(), 160);
        assert!(matches!(
            &rows[0][0],
            SqlValue::String(value) if value.starts_with(canary)
        ));
    }

    #[test]
    fn spill_manager_enforces_one_quota_across_concurrent_owners() {
        let root = tempfile::tempdir().unwrap();
        let first = SpillManager::acquire(root.path().to_path_buf(), 64 * 1024).unwrap();
        first.reserve(48 * 1024).unwrap();

        // The same canonical root resolves to the same live manager. A later
        // caller cannot raise the original server ceiling.
        let second = SpillManager::acquire(root.path().to_path_buf(), 1024 * 1024).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        let error = second.reserve(20 * 1024).unwrap_err();
        assert!(error.is_resource_limit());
        assert_eq!(error.sqlstate(), "53100");
        assert_eq!(first.used_bytes.load(Ordering::Acquire), 48 * 1024);

        first.release(48 * 1024);
        second.reserve(64 * 1024).unwrap();
        second.release(64 * 1024);
        assert_eq!(first.used_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn spill_manager_scavenges_only_owned_abandoned_paths() {
        let root = tempfile::tempdir().unwrap();
        let abandoned = root.path().join("bicdb-sort-crashed-1");
        std::fs::create_dir(&abandoned).unwrap();
        std::fs::write(abandoned.join("run.bin"), b"partial").unwrap();
        let legacy = root.path().join("bicdb-sort-crashed.run");
        std::fs::write(&legacy, b"partial").unwrap();
        let unrelated = root.path().join("operator-notes");
        std::fs::create_dir(&unrelated).unwrap();
        std::fs::write(unrelated.join("keep"), b"important").unwrap();

        #[cfg(unix)]
        let outside = {
            let outside = tempfile::tempdir().unwrap();
            std::fs::write(outside.path().join("keep"), b"outside").unwrap();
            std::os::unix::fs::symlink(outside.path(), root.path().join("bicdb-sort-hostile-link"))
                .unwrap();
            outside
        };

        let manager = SpillManager::acquire(root.path().to_path_buf(), 64 * 1024).unwrap();
        assert!(!abandoned.exists());
        assert!(!legacy.exists());
        assert!(unrelated.join("keep").exists());
        #[cfg(unix)]
        assert!(outside.path().join("keep").exists());
        drop(manager);
    }

    #[test]
    fn one_row_cannot_bypass_the_memory_limit() {
        let mut sorter = ExternalSorter::new_with_limits(ExternalSortLimits {
            memory_bytes: 64 * 1024,
            temp_bytes: 1024 * 1024,
            server_temp_bytes: 64 * 1024 * 1024,
            merge_fan_in: 4,
        });
        let error = sorter
            .push(key(1), vec![SqlValue::String("x".repeat(128 * 1024))])
            .unwrap_err();
        assert!(error.is_resource_limit());
        assert_eq!(error.sqlstate(), "53200");
        assert!(sorter.runs.is_empty());
    }

    #[test]
    fn codec_round_trips_every_covered_variant() {
        let row = vec![
            SqlValue::Null,
            SqlValue::Bool(true),
            SqlValue::Int(-42),
            SqlValue::Float(2.5),
            SqlValue::String("héllo\0world".to_string()),
            SqlValue::Json(serde_json::json!({"a": [1, null, "x"]})),
        ];
        let mut buffer = Vec::new();
        write_entry(&mut buffer, b"k", &row).unwrap();
        let mut cursor = buffer.as_slice();
        let (read_key, read_row) = read_entry(&mut cursor, usize::MAX).unwrap().unwrap();
        assert_eq!(read_key, b"k");
        assert_eq!(read_row, row);
        assert!(read_entry(&mut cursor, usize::MAX).unwrap().is_none());
    }

    #[test]
    fn spill_checksum_detects_corruption() {
        let mut buffer = Vec::new();
        write_entry(
            &mut buffer,
            b"key",
            &[SqlValue::String("value".to_string())],
        )
        .unwrap();
        // The first four bytes are the key length. Corrupting the key itself
        // preserves framing and deterministically exercises the checksum.
        buffer[4] ^= 0x40;
        let error = read_entry(&mut buffer.as_slice(), usize::MAX).unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn spill_rejects_a_truncated_length_prefix() {
        let error = read_entry(&mut [0u8, 0u8].as_slice(), usize::MAX).unwrap_err();
        assert!(error.to_string().contains("sort spill read"));
    }

    #[test]
    fn spill_length_cannot_force_an_unbounded_allocation() {
        let declared = (1024u32 * 1024).to_be_bytes();
        let error = read_entry(&mut declared.as_slice(), 64 * 1024).unwrap_err();
        assert!(error.to_string().contains("configured decode bound"));
    }
}
