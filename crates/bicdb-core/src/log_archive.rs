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
#[derive(Debug)]
pub struct LogArchive {
    dir: PathBuf,
    meta: Option<LogArchiveMeta>,
    last_commit_seq: u64,
    segment: Option<(File, u64)>,
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
    reader
        .read_exact(&mut body)
        .map_err(|_| "torn record body".to_string())?;
    if crc32fast::hash(&body) != crc {
        return Err("record checksum mismatch".to_string());
    }
    serde_json::from_slice(&body)
        .map(Some)
        .map_err(|error| format!("undecodable record: {error}"))
}

impl LogArchive {
    /// Opens (or creates) the archive in `dir`, repairing a torn tail.
    pub fn open(dir: impl AsRef<Path>, fsync: bool) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
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
            fsync,
        };
        if let Some((first, path)) = segments(&archive.dir)?.pop() {
            // Find the last whole record of the newest segment.
            let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
            let mut reader = BufReader::new(&mut file);
            let mut good = 0u64;
            let mut last = first.saturating_sub(1);
            loop {
                match read_record(&mut reader) {
                    Ok(Some(frame)) => {
                        last = frame.commit_seq;
                        good = reader.stream_position()?;
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            drop(reader);
            if good < file.metadata()?.len() {
                file.set_len(good)?;
                file.sync_all()?;
            }
            file.seek(SeekFrom::Start(good))?;
            archive.last_commit_seq = last;
            archive.segment = Some((file, good));
        } else if let Some(meta) = &archive.meta {
            archive.last_commit_seq = meta.first_commit_seq.saturating_sub(1);
        }
        Ok(archive)
    }

    pub fn meta(&self) -> Option<&LogArchiveMeta> {
        self.meta.as_ref()
    }

    /// The last archived commit (0 for an empty archive).
    pub fn last_commit_seq(&self) -> u64 {
        self.last_commit_seq
    }

    /// Appends consecutive commit frames durably.
    pub fn append(&mut self, cluster_id: &str, frames: &[CommitFrame]) -> Result<()> {
        let Some(first) = frames.first() else {
            return Ok(());
        };
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
                crate::storage::write_atomic(
                    &self.dir.join(META_FILE),
                    &serde_json::to_vec_pretty(&meta)?,
                    self.fsync,
                )?;
                self.meta = Some(meta);
                self.last_commit_seq = first.commit_seq - 1;
            }
        }
        for frame in frames {
            if frame.commit_seq != self.last_commit_seq + 1 {
                return Err(archive_error(format!(
                    "frames are not consecutive at commit {}",
                    frame.commit_seq
                )));
            }
            let rotate = self
                .segment
                .as_ref()
                .is_none_or(|(_, bytes)| *bytes >= SEGMENT_BYTES);
            if rotate {
                self.finish_segment()?;
                let path = self.dir.join(segment_name(frame.commit_seq));
                let file = OpenOptions::new().create(true).append(true).open(&path)?;
                if self.fsync {
                    if let Ok(dir) = File::open(&self.dir) {
                        let _ = dir.sync_all();
                    }
                }
                self.segment = Some((file, 0));
            }
            let body = serde_json::to_vec(frame)?;
            let mut record = Vec::with_capacity(RECORD_HEADER + body.len());
            record.extend_from_slice(&(body.len() as u32).to_be_bytes());
            record.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
            record.extend_from_slice(&body);
            let (file, bytes) = self.segment.as_mut().unwrap();
            file.write_all(&record)?;
            *bytes += record.len() as u64;
            self.last_commit_seq = frame.commit_seq;
        }
        if self.fsync {
            if let Some((file, _)) = &self.segment {
                file.sync_data()?;
            }
        }
        Ok(())
    }

    fn finish_segment(&mut self) -> Result<()> {
        if let Some((file, _)) = self.segment.take() {
            if self.fsync {
                file.sync_all()?;
            }
        }
        Ok(())
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
                Err(error) if index + 1 == segments.len() => {
                    let _ = error;
                    break;
                }
                Err(error) => {
                    return Err(archive_error(format!("{}: {error}", path.display())));
                }
            };
            if frame.commit_seq < expected {
                continue;
            }
            if frame.commit_seq > expected {
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
    bases.sort_by_key(|base| base.commit_seq);
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
