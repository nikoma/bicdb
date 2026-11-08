//! Commit log archive: every commit of a database, in order, on disk, for
//! point-in-time recovery of the whole database.
//!
//! A server with an archive appends each commit frame (the same frames the
//! commit stream sends to standbys) to segment files in the archive
//! directory, and holds its transaction log until a commit is archived. A
//! restore starts from a base backup and replays the archive up to a chosen
//! commit or time.
//!
//! Layout: `archive.json` (identity and first commit) and segments
//! `commits-<first commit, 20 digits>.log`, each a sequence of records
//! `[length: u32 BE][crc32: u32 BE][JSON CommitFrame]`. Appends are fsynced
//! before the commit counts as archived; a torn record at the tail of the
//! last segment (a crash mid-append) is cut off when the archive is opened.
use crate::error::{BicDbError, Result};
use crate::replication::CommitFrame;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const META_FILE: &str = "archive.json";
const SEGMENT_PREFIX: &str = "commits-";
const SEGMENT_SUFFIX: &str = ".log";
/// Segments rotate past this size.
const SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
const RECORD_HEADER: usize = 8;
const MAX_RECORD_BYTES: usize = 512 * 1024 * 1024;

fn archive_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Backup(format!("log archive: {}", message.into()))
}

/// Identity of an archive.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogArchiveMeta {
    /// Replication cluster id of the archived database.
    pub cluster_id: String,
    /// First archived commit sequence number.
    pub first_commit_seq: u64,
}

/// An archive open for appending.
///
/// Fail-closed: a commit counts as archived (and the caller may advance the
/// transaction-log hold past it) only once its record AND every directory
/// entry it depends on are durable, confirmed by this handle's own fsyncs.
/// The first failed write, fsync, rename or directory fsync stops this
/// handle for good ([`Self::failure`]): a failed `fsync` may drop the dirty
/// pages it could not write and then report success on a retry, so nothing
/// this handle wrote after its last confirmed point is trusted again. A new
/// handle opened through `BicDb::open_log_archive` discards the unconfirmed
/// suffix and rewrites it from the retained transaction log.
#[derive(Debug)]
pub struct LogArchive {
    dir: PathBuf,
    meta: Option<LogArchiveMeta>,
    /// Last commit whose record is confirmed durable (or, for a handle that
    /// was just opened, present on disk).
    last_commit_seq: u64,
    segment: Option<(File, u64)>,
    health: std::sync::Arc<bicdb_page::ArchiveHealth>,
    fsync: bool,
}

fn segment_name(first_commit_seq: u64) -> String {
    format!("{SEGMENT_PREFIX}{first_commit_seq:020}{SEGMENT_SUFFIX}")
}

/// Segment files of an archive with their first commit, ascending.
fn segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(first) = name
            .to_str()
            .and_then(|name| name.strip_prefix(SEGMENT_PREFIX))
            .and_then(|name| name.strip_suffix(SEGMENT_SUFFIX))
            .and_then(|digits| digits.parse::<u64>().ok())
        else {
            continue;
        };
        segments.push((first, entry.path()));
    }
    segments.sort();
    Ok(segments)
}

/// Reads one record; `Ok(None)` at a clean end, `Err` for a torn or corrupt
/// record (with the offset where it starts).
fn read_record(reader: &mut impl Read) -> std::result::Result<Option<CommitFrame>, String> {
    let mut header = [0u8; RECORD_HEADER];
    let mut filled = 0;
    while filled < RECORD_HEADER {
        match reader.read(&mut header[filled..]) {
            Ok(0) if filled == 0 => return Ok(None),
            Ok(0) => return Err("torn record header".to_string()),
            Ok(read) => filled += read,
            Err(error) => return Err(error.to_string()),
        }
    }
    let len = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
    let crc = u32::from_be_bytes(header[4..].try_into().unwrap());
    if len > MAX_RECORD_BYTES {
        return Err(format!("record length {len} is implausible"));
    }
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body).map_err(|error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            "torn record body".to_string()
        } else {
            error.to_string()
        }
    })?;
    if crc32fast::hash(&body) != crc {
        return Err("record checksum mismatch".to_string());
    }
    let frame: CommitFrame =
        serde_json::from_slice(&body).map_err(|error| format!("undecodable record: {error}"))?;
    frame.verify_checksum().map_err(|error| error.to_string())?;
    Ok(Some(frame))
}

/// One fault-injectable archive I/O step.
fn step<T>(site: &str, path: &Path, op: impl FnOnce() -> std::io::Result<T>) -> std::io::Result<T> {
    bicdb_page::test_faults::io_fault(site, path)?;
    op()
}

fn sync_dir(site: &str, dir: &Path) -> std::io::Result<()> {
    step(site, dir, || File::open(dir)?.sync_all())
}

impl LogArchive {
    /// Opens (or creates) the archive in `dir`, repairing a torn tail.
    ///
    /// The handle trusts what is on disk. A server reopening an archive it
    /// was appending to should use `BicDb::open_log_archive`, which also
    /// discards and rewrites whatever the transaction-log hold never
    /// confirmed.
    pub fn open(dir: impl AsRef<Path>, fsync: bool) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        if !dir.is_dir() {
            fs::create_dir_all(&dir)?;
            if fsync {
                if let Some(parent) = dir.parent().filter(|p| !p.as_os_str().is_empty()) {
                    sync_dir("log_archive.dir_sync", parent)?;
                }
            }
        }
        let meta = match fs::read(dir.join(META_FILE)) {
            Ok(bytes) => Some(serde_json::from_slice::<LogArchiveMeta>(&bytes)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let mut archive = Self {
            dir,
            meta,
            last_commit_seq: 0,
            segment: None,
            health: bicdb_page::ArchiveHealth::new(),
            fsync,
        };
        if let Some((first, path)) = segments(&archive.dir)?.pop() {
            // Find the last whole record of the newest segment.
            let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
            let (good, last) = archive.scan_segment(&mut file, first, &path, None)?;
            if good < file.metadata()?.len() {
                step("log_archive.repair", &path, || {
                    file.set_len(good)?;
                    file.sync_all()
                })?;
            }
            file.seek(SeekFrom::Start(good))?;
            archive.last_commit_seq = last;
            archive.segment = Some((file, good));
        } else if let Some(meta) = &archive.meta {
            archive.last_commit_seq = meta.first_commit_seq.saturating_sub(1);
        }
        Ok(archive)
    }

    /// Reads the records of one segment, checking sequence and identity.
    /// Returns the offset after the last whole record (or after the record
    /// of commit `stop_after`, when given) and that record's commit.
    fn scan_segment(
        &self,
        file: &mut File,
        first: u64,
        path: &Path,
        stop_after: Option<u64>,
    ) -> Result<(u64, u64)> {
        file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(file);
        let mut good = 0u64;
        let mut last = first.saturating_sub(1);
        if stop_after.is_some_and(|stop| stop <= last) {
            return Ok((good, last));
        }
        loop {
            match read_record(&mut reader) {
                Ok(Some(frame)) => {
                    if frame.commit_seq != last + 1 || frame.previous_commit_seq != last {
                        return Err(archive_error("non-consecutive frames in archive tail"));
                    }
                    if self
                        .meta
                        .as_ref()
                        .is_none_or(|meta| meta.cluster_id != frame.cluster_id)
                    {
                        return Err(archive_error("archive frame identity mismatch"));
                    }
                    last = frame.commit_seq;
                    good = reader.stream_position()?;
                    if stop_after == Some(last) {
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) if error.starts_with("torn record") => break,
                Err(error) => return Err(archive_error(format!("{}: {error}", path.display()))),
            }
        }
        Ok((good, last))
    }

    pub fn meta(&self) -> Option<&LogArchiveMeta> {
        self.meta.as_ref()
    }

    /// Last commit the archive holds durably: confirmed by this handle, or
    /// present on disk when it was opened. Including a fresh archive's
    /// initial base backup.
    pub fn last_commit_seq(&self) -> u64 {
        self.last_commit_seq
    }

    /// The first failure of this handle, if any. Once set, nothing more is
    /// appended or confirmed through this handle.
    pub fn failure(&self) -> Option<crate::WriteFailure> {
        self.health.failure()
    }

    /// `Err` once this handle failed.
    pub fn check_healthy(&self) -> Result<()> {
        match self.health.failure() {
            None => Ok(()),
            Some(failure) => Err(failure_error(failure)),
        }
    }

    fn fail(&self, site: &str, path: &Path, error: impl std::fmt::Display) -> BicDbError {
        failure_error(self.health.record_failure(
            "commit_log",
            site,
            &format!("{}: {error}", path.display()),
        ))
    }

    /// Writes `archive.json` atomically and durably.
    fn write_meta(&self, meta: &LogArchiveMeta) -> Result<()> {
        let path = self.dir.join(META_FILE);
        let tmp = self.dir.join(format!("{META_FILE}.tmp"));
        let bytes = serde_json::to_vec_pretty(meta)?;
        let result = (|| -> std::result::Result<(), (&'static str, std::io::Error)> {
            let file = step("log_archive.meta_write", &tmp, || {
                let mut file = File::create(&tmp)?;
                file.write_all(&bytes)?;
                Ok(file)
            })
            .map_err(|error| ("log_archive.meta_write", error))?;
            if self.fsync {
                step("log_archive.meta_sync", &tmp, || file.sync_all())
                    .map_err(|error| ("log_archive.meta_sync", error))?;
            }
            drop(file);
            step("log_archive.rename", &path, || fs::rename(&tmp, &path))
                .map_err(|error| ("log_archive.rename", error))?;
            if self.fsync {
                sync_dir("log_archive.dir_sync", &self.dir)
                    .map_err(|error| ("log_archive.dir_sync", error))?;
            }
            Ok(())
        })();
        result.map_err(|(site, error)| {
            let _ = fs::remove_file(&tmp);
            self.fail(site, &path, error)
        })
    }

    /// Anchor a fresh archive at an already-published consistent base backup.
    /// Existing archive histories must never be rebased over a missing suffix.
    pub fn initialize_from_base(&mut self, cluster_id: &str, base_commit_seq: u64) -> Result<()> {
        self.check_healthy()?;
        if self.meta.is_some() || self.segment.is_some() {
            return Err(archive_error(
                "only an uninitialized archive can start from a base backup",
            ));
        }
        let first_commit_seq = base_commit_seq
            .checked_add(1)
            .ok_or_else(|| archive_error("base commit sequence exhausted"))?;
        let meta = LogArchiveMeta {
            cluster_id: cluster_id.into(),
            first_commit_seq,
        };
        self.write_meta(&meta)?;
        self.meta = Some(meta);
        self.last_commit_seq = base_commit_seq;
        Ok(())
    }

    /// Appends consecutive commit frames durably. On `Ok`, every frame is
    /// confirmed and [`Self::last_commit_seq`] covers it; on `Err`, nothing
    /// past the previous confirmed commit counts, and an I/O failure stops
    /// this handle.
    pub fn append(&mut self, cluster_id: &str, frames: &[CommitFrame]) -> Result<()> {
        self.check_healthy()?;
        let Some(first) = frames.first() else {
            if let Some(meta) = &self.meta {
                if meta.cluster_id != cluster_id {
                    return Err(archive_error("archive cluster identity mismatch"));
                }
            } else {
                let meta = LogArchiveMeta {
                    cluster_id: cluster_id.into(),
                    first_commit_seq: self.last_commit_seq + 1,
                };
                self.write_meta(&meta)?;
                self.meta = Some(meta);
            }
            return Ok(());
        };
        // Reject the entire input before changing metadata or writing a prefix.
        let mut previous = first
            .commit_seq
            .checked_sub(1)
            .ok_or_else(|| archive_error("commit sequence must be positive"))?;
        for frame in frames {
            frame.verify_checksum()?;
            if frame.cluster_id != cluster_id
                || frame.commit_seq != previous + 1
                || frame.previous_commit_seq != previous
            {
                return Err(archive_error("invalid frame identity or sequence"));
            }
            if serde_json::to_vec(frame)?.len() > MAX_RECORD_BYTES {
                return Err(archive_error("record exceeds archive size limit"));
            }
            previous = frame.commit_seq;
        }
        match &self.meta {
            Some(meta) => {
                if meta.cluster_id != cluster_id {
                    return Err(archive_error(format!(
                        "belongs to cluster {}, not {cluster_id}",
                        meta.cluster_id
                    )));
                }
                if first.commit_seq != self.last_commit_seq + 1 {
                    return Err(archive_error(format!(
                        "commit {} does not follow the last archived commit {}; the commits in between are no longer in the log. Start a new archive and take a new base backup",
                        first.commit_seq, self.last_commit_seq
                    )));
                }
            }
            None => {
                let meta = LogArchiveMeta {
                    cluster_id: cluster_id.to_string(),
                    first_commit_seq: first.commit_seq,
                };
                self.write_meta(&meta)?;
                self.meta = Some(meta);
                self.last_commit_seq = first.commit_seq - 1;
            }
        }
        // Written but not yet confirmed.
        let mut written = self.last_commit_seq;
        for frame in frames {
            let rotate = self
                .segment
                .as_ref()
                .is_none_or(|(_, bytes)| *bytes >= SEGMENT_BYTES);
            if rotate {
                // The finished segment's records were confirmed by earlier
                // syncs, but this one must pass before any record of the new
                // segment is written: the new segment is only reachable once
                // the old one is complete.
                self.finish_segment()?;
                let path = self.dir.join(segment_name(frame.commit_seq));
                let file = step("log_archive.create", &path, || {
                    OpenOptions::new().create(true).append(true).open(&path)
                })
                .map_err(|error| self.fail("log_archive.create", &path, error))?;
                if self.fsync {
                    // Without the entry, every record in the segment is lost
                    // on a crash, however well its data was synced.
                    sync_dir("log_archive.dir_sync", &self.dir)
                        .map_err(|error| self.fail("log_archive.dir_sync", &self.dir, error))?;
                }
                self.segment = Some((file, 0));
            }
            let body = serde_json::to_vec(frame)?;
            let mut record = Vec::with_capacity(RECORD_HEADER + body.len());
            record.extend_from_slice(&(body.len() as u32).to_be_bytes());
            record.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
            record.extend_from_slice(&body);
            let path = self.dir.join(segment_name(frame.commit_seq));
            let (file, bytes) = self.segment.as_mut().unwrap();
            let wrote = step("log_archive.write", &path, || file.write_all(&record));
            if let Err(error) = wrote {
                // A torn prefix may now sit at the tail. This handle stops;
                // a reopen cuts the tail back to the last whole record and
                // the unconfirmed suffix is rewritten from the log.
                return Err(self.fail("log_archive.write", &path, error));
            }
            *bytes += record.len() as u64;
            written = frame.commit_seq;
        }
        self.sync()?;
        // Crash matrix: records durable, nothing confirmed to the caller yet.
        bicdb_page::test_faults::crash_point("log_archive.after_sync");
        self.last_commit_seq = written;
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        if self.fsync {
            if let Some((file, _)) = &self.segment {
                step("log_archive.sync", &self.dir, || file.sync_data())
                    .map_err(|error| self.fail("log_archive.sync", &self.dir, error))?;
            }
        }
        Ok(())
    }

    fn finish_segment(&mut self) -> Result<()> {
        if self.fsync {
            if let Some((file, _)) = &self.segment {
                step("log_archive.sync", &self.dir, || file.sync_all())
                    .map_err(|error| self.fail("log_archive.sync", &self.dir, error))?;
            }
        }
        self.segment = None;
        Ok(())
    }

    /// Discards every record after commit `keep_through` (whole later
    /// segments are removed, the segment holding it is cut after it), so the
    /// caller can rewrite that suffix from a source that is known durable.
    /// Returns how many commits were discarded.
    pub fn discard_after(&mut self, keep_through: u64) -> Result<u64> {
        self.check_healthy()?;
        let Some(meta) = self.meta.clone() else {
            return Err(archive_error(
                "an uninitialized archive has nothing to discard",
            ));
        };
        if keep_through + 1 < meta.first_commit_seq {
            return Err(archive_error(format!(
                "cannot discard below the archive start (commit {})",
                meta.first_commit_seq
            )));
        }
        let discarded = self.last_commit_seq.saturating_sub(keep_through);
        self.segment = None;
        let all = segments(&self.dir)?;
        let mut removed = false;
        for (first, path) in all.iter().filter(|(first, _)| *first > keep_through) {
            let _ = first;
            step("log_archive.truncate", path, || fs::remove_file(path))
                .map_err(|error| self.fail("log_archive.truncate", path, error))?;
            removed = true;
        }
        if removed && self.fsync {
            sync_dir("log_archive.dir_sync", &self.dir)
                .map_err(|error| self.fail("log_archive.dir_sync", &self.dir, error))?;
        }
        if let Some((first, path)) = all.iter().rev().find(|(first, _)| *first <= keep_through) {
            let mut file = OpenOptions::new().read(true).write(true).open(path)?;
            let (good, last) = self.scan_segment(&mut file, *first, path, Some(keep_through))?;
            if last != keep_through {
                return Err(archive_error(format!(
                    "{} ends at commit {last}, before commit {keep_through}",
                    path.display()
                )));
            }
            if good < file.metadata()?.len() {
                step("log_archive.truncate", path, || {
                    file.set_len(good)?;
                    if self.fsync {
                        file.sync_all()?;
                    }
                    Ok(())
                })
                .map_err(|error| self.fail("log_archive.truncate", path, error))?;
            }
            file.seek(SeekFrom::Start(good))?;
            self.segment = Some((file, good));
        }
        self.last_commit_seq = keep_through;
        Ok(discarded)
    }
}

fn failure_error(failure: crate::WriteFailure) -> BicDbError {
    BicDbError::ArchiveFailed {
        archive: "commit_log".into(),
        site: failure.site,
        detail: failure.error,
    }
}

/// Where a replay stops.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplayTarget {
    /// Apply commits up to and including this commit sequence number.
    pub commit_seq: Option<u64>,
    /// Apply commits made at or before this Unix time (seconds).
    pub timestamp: Option<i64>,
}

impl ReplayTarget {
    fn includes(&self, frame: &CommitFrame) -> bool {
        if self
            .commit_seq
            .is_some_and(|target| frame.commit_seq > target)
        {
            return false;
        }
        // Commits that wrote nothing carry no meaningful time.
        if !frame.writes.is_empty()
            && self
                .timestamp
                .is_some_and(|target| frame.timestamp > target)
        {
            return false;
        }
        true
    }
}

/// Reads the archive in `dir` in commit order, starting after `after`, and
/// hands each frame to `visit` until the archive ends, `target` excludes a
/// frame, or `visit` returns false. Fails when the archive does not reach
/// back to `after + 1` or has a gap.
pub fn read_log_archive(
    dir: impl AsRef<Path>,
    after: u64,
    target: ReplayTarget,
    mut visit: impl FnMut(CommitFrame) -> Result<bool>,
) -> Result<()> {
    let dir = dir.as_ref();
    let meta = serde_json::from_slice::<LogArchiveMeta>(
        &fs::read(dir.join(META_FILE))
            .map_err(|error| archive_error(format!("{}: {error}", dir.display())))?,
    )?;
    if meta.first_commit_seq > after + 1 {
        return Err(archive_error(format!(
            "starts at commit {}, but the base backup ends at commit {after}: commits {}..{} are missing",
            meta.first_commit_seq,
            after + 1,
            meta.first_commit_seq - 1
        )));
    }
    let segments = segments(dir)?;
    let mut expected = after + 1;
    for (index, (first, path)) in segments.iter().enumerate() {
        // Skip segments that end before the replay starts.
        if let Some((next_first, _)) = segments.get(index + 1) {
            if *next_first <= expected {
                continue;
            }
        }
        let _ = first;
        let mut reader = BufReader::new(File::open(path)?);
        loop {
            let frame = match read_record(&mut reader) {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                // A torn tail is only acceptable at the very end.
                Err(error) if index + 1 == segments.len() && error.starts_with("torn record") => {
                    break;
                }
                Err(error) => {
                    return Err(archive_error(format!("{}: {error}", path.display())));
                }
            };
            if frame.cluster_id != meta.cluster_id {
                return Err(archive_error("archive frame identity mismatch"));
            }
            if frame.commit_seq < expected {
                continue;
            }
            if frame.commit_seq > expected || frame.previous_commit_seq != expected - 1 {
                return Err(archive_error(format!(
                    "commit {expected} is missing (found {})",
                    frame.commit_seq
                )));
            }
            if !target.includes(&frame) {
                return Ok(());
            }
            expected += 1;
            if !visit(frame)? {
                return Ok(());
            }
        }
    }
    if target.timestamp.is_none() && target.commit_seq.is_some_and(|seq| expected <= seq) {
        return Err(archive_error(format!(
            "archive ends at commit {}, before requested target {}",
            expected - 1,
            target.commit_seq.unwrap()
        )));
    }
    // A time target counts as reached only when the archive holds a commit
    // past it (that commit stopped the replay above) or the commit target
    // was reached. An archive that simply ends before the time may be
    // missing commits made up to it (a failed or lagging archiver), so
    // restoring "to that time" from it would silently restore to a gap.
    if let Some(time) = target.timestamp {
        let commit_reached = target.commit_seq.is_some_and(|seq| expected > seq);
        if !commit_reached {
            return Err(archive_error(format!(
                "archive ends at commit {} without reaching the requested time {time}; commits \
                 made up to that time may be missing from the archive. Restore without a target \
                 to restore to the end of the archive",
                expected - 1
            )));
        }
    }
    Ok(())
}

const BASE_PREFIX: &str = "base-";
const BASE_SUFFIX: &str = ".bicbackup";

/// A base backup kept in the archive directory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchivedBaseBackup {
    /// Last commit the backup contains.
    pub commit_seq: u64,
    /// When it was taken (Unix seconds).
    pub created_at: i64,
    #[serde(skip)]
    pub path: PathBuf,
}

/// Path of the base backup that ends at `commit_seq`.
pub fn base_backup_path(dir: &Path, commit_seq: u64) -> PathBuf {
    dir.join(format!("{BASE_PREFIX}{commit_seq:020}{BASE_SUFFIX}"))
}

fn base_backup_info_path(backup: &Path) -> PathBuf {
    backup.with_extension("json")
}

/// Records a finished base backup (its sidecar makes it visible).
pub fn record_base_backup(
    backup: &Path,
    commit_seq: u64,
    created_at: i64,
    fsync: bool,
) -> Result<()> {
    let info = ArchivedBaseBackup {
        commit_seq,
        created_at,
        path: PathBuf::new(),
    };
    crate::storage::write_atomic(
        &base_backup_info_path(backup),
        &serde_json::to_vec_pretty(&info)?,
        fsync,
    )
}

/// Base backups in the archive directory, oldest first.
pub fn list_base_backups(dir: &Path) -> Result<Vec<ArchivedBaseBackup>> {
    let mut bases = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let is_base = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(BASE_PREFIX) && name.ends_with(BASE_SUFFIX));
        if !is_base {
            continue;
        }
        // A base without its sidecar is unfinished.
        let Ok(bytes) = fs::read(base_backup_info_path(&path)) else {
            continue;
        };
        let mut info = serde_json::from_slice::<ArchivedBaseBackup>(&bytes)?;
        info.path = path;
        bases.push(info);
    }
    bases.sort_by(|a, b| {
        a.commit_seq
            .cmp(&b.commit_seq)
            .then(a.created_at.cmp(&b.created_at))
            .then(a.path.cmp(&b.path))
    });
    Ok(bases)
}

/// The newest base backup a restore to `target` can start from.
pub fn base_backup_for(dir: &Path, target: ReplayTarget) -> Result<ArchivedBaseBackup> {
    list_base_backups(dir)?
        .into_iter()
        .filter(|base| target.commit_seq.is_none_or(|seq| base.commit_seq <= seq))
        .filter(|base| target.timestamp.is_none_or(|time| base.created_at <= time))
        .next_back()
        .ok_or_else(|| archive_error("no base backup in the archive is old enough for that target"))
}

/// What pruning removed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PruneReport {
    pub base_backups: usize,
    pub segments: usize,
}

/// Keeps the newest `keep_bases` base backups and the log segments a
/// restore from the oldest of them needs; removes the rest.
pub fn prune_log_archive(dir: &Path, keep_bases: usize, fsync: bool) -> Result<PruneReport> {
    let mut report = PruneReport::default();
    let bases = list_base_backups(dir)?;
    if bases.len() <= keep_bases.max(1) {
        return Ok(report);
    }
    let (old, kept) = bases.split_at(bases.len() - keep_bases.max(1));
    for base in old {
        fs::remove_file(&base.path)?;
        fs::remove_file(base_backup_info_path(&base.path))?;
        report.base_backups += 1;
    }
    let oldest_needed = kept[0].commit_seq + 1;
    let all_segments = segments(dir)?;
    for window in all_segments.windows(2) {
        // The segment ends right before the next one starts.
        let ((_, path), (next_first, _)) = (&window[0], &window[1]);
        if *next_first <= oldest_needed {
            fs::remove_file(path)?;
            report.segments += 1;
        }
    }
    if report.segments > 0 {
        let first = segments(dir)?.first().map(|(first, _)| *first);
        if let (Some(first), Ok(bytes)) = (first, fs::read(dir.join(META_FILE))) {
            let mut meta = serde_json::from_slice::<LogArchiveMeta>(&bytes)?;
            meta.first_commit_seq = first;
            crate::storage::write_atomic(
                &dir.join(META_FILE),
                &serde_json::to_vec_pretty(&meta)?,
                fsync,
            )?;
        }
    }
    Ok(report)
}
