//! The short database cut is separate from repository I/O. Captured descriptors
//! retain atomic-replacement files and append prefixes after the host releases
//! its outer database read guard.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OnlineBackupLimits {
    pub page_images: bicdb_page::PageBackupLimits,
    pub max_files: usize,
    pub max_manifest_bytes: usize,
    pub max_coordination_millis: u64,
    pub max_duration_millis: u64,
    pub max_retained_wal_bytes: u64,
    /// Zero means no explicit rate limit. The worker still checks cancellation
    /// and its retention limits between bounded frames.
    pub bytes_per_second: u64,
}

impl Default for OnlineBackupLimits {
    fn default() -> Self {
        Self {
            page_images: bicdb_page::PageBackupLimits::default(),
            max_files: 65_536,
            max_manifest_bytes: MAX_BACKUP_HEADER_BYTES,
            max_coordination_millis: 1_000,
            max_duration_millis: 6 * 60 * 60 * 1000,
            max_retained_wal_bytes: 8 * 1024 * 1024 * 1024,
            bytes_per_second: 0,
        }
    }
}

impl OnlineBackupLimits {
    pub fn validate(&self) -> Result<()> {
        if !(1..=MAX_BACKUP_FILES).contains(&self.max_files)
            || !(1024..=MAX_BACKUP_HEADER_BYTES).contains(&self.max_manifest_bytes)
            || !(10..=60_000).contains(&self.max_coordination_millis)
            || !(100..=7 * 24 * 60 * 60 * 1000).contains(&self.max_duration_millis)
            || self.max_retained_wal_bytes == 0
        {
            return Err(error("online backup limits are outside supported bounds"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnlineBackupPhase {
    Capturing,
    Copying,
    Verifying,
    Publishing,
    Complete,
    Failed,
    Cancelled,
}

impl OnlineBackupPhase {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Capturing,
            1 => Self::Copying,
            2 => Self::Verifying,
            3 => Self::Publishing,
            4 => Self::Complete,
            5 => Self::Failed,
            _ => Self::Cancelled,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OnlineBackupProgress {
    pub phase: OnlineBackupPhase,
    pub files_total: u64,
    pub files_copied: u64,
    pub source_bytes_read: u64,
    pub uploaded_bytes: u64,
    pub reused_bytes: u64,
    pub wal_retention_released: bool,
}

#[derive(Debug, Default)]
struct ControlState {
    cancel: AtomicBool,
    phase: AtomicU8,
    files_total: AtomicU64,
    files_copied: AtomicU64,
    source_bytes_read: AtomicU64,
    uploaded_bytes: AtomicU64,
    reused_bytes: AtomicU64,
    wal_retention_released: AtomicBool,
}

#[derive(Clone, Debug, Default)]
pub struct OnlineBackupControl(Arc<ControlState>);

impl OnlineBackupControl {
    pub fn cancel(&self) {
        self.0.cancel.store(true, Ordering::Release);
    }
    pub fn progress(&self) -> OnlineBackupProgress {
        OnlineBackupProgress {
            phase: OnlineBackupPhase::from_u8(self.0.phase.load(Ordering::Acquire)),
            files_total: self.0.files_total.load(Ordering::Relaxed),
            files_copied: self.0.files_copied.load(Ordering::Relaxed),
            source_bytes_read: self.0.source_bytes_read.load(Ordering::Relaxed),
            uploaded_bytes: self.0.uploaded_bytes.load(Ordering::Relaxed),
            reused_bytes: self.0.reused_bytes.load(Ordering::Relaxed),
            wal_retention_released: self.0.wal_retention_released.load(Ordering::Acquire),
        }
    }
    pub(super) fn phase(&self, phase: OnlineBackupPhase) {
        self.0.phase.store(phase as u8, Ordering::Release);
    }
    pub(super) fn read(&self, bytes: u64) {
        self.0.source_bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }
    pub(super) fn reused(&self, bytes: u64) {
        self.0.reused_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    pub(super) fn uploaded(&self, bytes: u64) {
        self.0.uploaded_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    pub(super) fn copied_file(&self) {
        self.0.files_copied.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn total_files(&self, files: u64) {
        self.0.files_total.store(files, Ordering::Relaxed);
    }
    pub(super) fn release_wal(&self) {
        self.0.wal_retention_released.store(true, Ordering::Release);
    }
    pub(super) fn check(&self) -> Result<()> {
        if self.0.cancel.load(Ordering::Acquire) {
            Err(error("online backup cancelled"))
        } else {
            Ok(())
        }
    }
}

pub(super) fn error(reason: impl Into<String>) -> BicDbError {
    BicDbError::Backup(reason.into())
}

pub(super) enum CapturedSource {
    File(File),
    Pages(usize),
}

pub(super) struct CapturedFile {
    pub path: String,
    pub bytes: u64,
    pub source: CapturedSource,
}

pub struct OnlineBackupSnapshot {
    pub(super) source_id: Uuid,
    pub(super) commit_seq: u64,
    pub(super) format: FormatMetadata,
    pub(super) files: Vec<CapturedFile>,
    pub(super) paged: Option<bicdb_page::PagedBackupCapture>,
    pub(super) limits: OnlineBackupLimits,
    pub(super) started: Instant,
    pub(super) coordination_millis: u64,
    pub(super) captured_at: i64,
    pub(super) _owner: OnlineBackupCheckpoint,
    _database_owner: Option<Arc<bicdb_page::DirectoryLock>>,
    pub(super) journal: PathBuf,
}

impl Drop for OnlineBackupSnapshot {
    fn drop(&mut self) {
        // The page capture closes/removes its journal too; this also covers a
        // failed construction or platforms where deleting an open file fails.
        self.paged.take();
        let _ = fs::remove_file(&self.journal);
    }
}

impl OnlineBackupSnapshot {
    pub fn commit_seq(&self) -> u64 {
        self.commit_seq
    }
    pub fn coordination_millis(&self) -> u64 {
        self.coordination_millis
    }
    pub(super) fn check(&self, control: &OnlineBackupControl) -> Result<()> {
        control.check()?;
        if self.started.elapsed().as_millis() > u128::from(self.limits.max_duration_millis) {
            return Err(error("online backup duration budget exceeded"));
        }
        if let Some(capture) = &self.paged {
            capture.pages.stats().map_err(|e| error(e.to_string()))?;
            if capture.retained_wal_bytes() > self.limits.max_retained_wal_bytes {
                return Err(error("online backup retained WAL budget exceeded"));
            }
        }
        Ok(())
    }

    pub(super) fn throttle(&self, control: &OnlineBackupControl) -> Result<()> {
        if self.limits.bytes_per_second == 0 {
            return self.check(control);
        }
        let bytes = control.progress().source_bytes_read;
        let due =
            std::time::Duration::from_secs_f64(bytes as f64 / self.limits.bytes_per_second as f64);
        while self.started.elapsed() < due {
            self.check(control)?;
            std::thread::sleep(
                (due - self.started.elapsed().min(due)).min(std::time::Duration::from_millis(20)),
            );
        }
        self.check(control)
    }
}

/// Caller owns the core commit boundary and excludes catalog DDL. This does no
/// payload hashing, encryption, checkpoint flushing, or repository I/O.
pub(crate) fn capture(
    db: &BicDb,
    paged: Option<Arc<bicdb_page::PagedStore>>,
    commit_seq: u64,
    limits: OnlineBackupLimits,
    started: Instant,
    mut owner: OnlineBackupCheckpoint,
    database_owner: Option<Arc<bicdb_page::DirectoryLock>>,
) -> Result<OnlineBackupSnapshot> {
    limits.validate()?;
    let root = db.data_path();
    let source_path = root.join("backup-source.json");
    let source_id = if source_path.exists() {
        let metadata = fs::symlink_metadata(&source_path)?;
        if !metadata.is_file() || metadata.len() > 128 {
            return Err(error("unsafe backup source identity"));
        }
        serde_json::from_slice(&fs::read(&source_path)?)?
    } else {
        let id = Uuid::new_v4();
        storage::write_atomic(&source_path, &serde_json::to_vec(&id)?, true)?;
        id
    };
    if paged.is_some() {
        format::enable_online_backup_extent_tracking(root)?;
    }
    owner.acquire_snapshot_gate(root)?;
    // Holding the stable owner lock proves that no prior process still owns
    // these transient before-images. A crash never makes them recovery data.
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_prefix(".backup-pages-"))
            .is_some_and(|id| Uuid::parse_str(id).is_ok())
        {
            fs::remove_file(entry.path())?;
        }
        if started.elapsed().as_millis() > u128::from(limits.max_coordination_millis) {
            return Err(error("online backup coordination budget exceeded"));
        }
    }
    let journal = root.join(format!(".backup-pages-{}", Uuid::new_v4()));
    let mut page_limits = limits.page_images;
    page_limits.max_coordination_millis = page_limits.max_coordination_millis.min(
        limits
            .max_coordination_millis
            .saturating_sub(started.elapsed().as_millis() as u64)
            .max(1),
    );
    page_limits.max_segments = page_limits.max_segments.min(limits.max_files);
    let mut paged_capture = paged
        .as_ref()
        .map(|store| store.capture_backup(&journal, page_limits))
        .transpose()
        .map_err(|e| error(e.to_string()))?;
    let mut files = Vec::new();
    if let Some(capture) = &mut paged_capture {
        for (path, file, bytes) in capture.wal.drain(..) {
            files.push(CapturedFile {
                path: relative(root, &path)?,
                bytes,
                source: CapturedSource::File(file),
            });
        }
    }
    // This is a preserved physical cut, not the legacy fuzzy copy. Copy its
    // WAL first so that its retention pin can be released before a long page
    // or blob upload. Descriptors and page before-images retain the cut.
    let mut metadata_bytes = files.iter().map(|file| file.path.len() * 6 + 512).sum();
    collect(
        root,
        root,
        &limits,
        started,
        &mut files,
        &mut metadata_bytes,
    )?;
    if let Some(capture) = &mut paged_capture {
        for (index, bytes) in capture.pages.segment_bytes().iter().copied().enumerate() {
            let path = if index == 0 {
                "paged/store.pages".into()
            } else {
                format!("paged/store.pages.{index}")
            };
            files.push(CapturedFile {
                path,
                bytes,
                source: CapturedSource::Pages(index),
            });
        }
    }
    if files.len() > limits.max_files {
        return Err(error("online backup file limit exceeded"));
    }
    let format = format::read_or_legacy_metadata(root)?;
    let coordination_millis = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    if coordination_millis > limits.max_coordination_millis {
        return Err(error("online backup coordination budget exceeded"));
    }
    // The owner lock prevents another backup, but copying needs no physical
    // write exclusion. Release this before returning to the host.
    drop(owner.snapshot_gate.take());
    Ok(OnlineBackupSnapshot {
        source_id,
        commit_seq,
        format,
        files,
        paged: paged_capture,
        limits,
        started,
        coordination_millis,
        _owner: owner,
        _database_owner: database_owner,
        journal,
        captured_at: unix_timestamp(),
    })
}

fn relative(root: &Path, path: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(root)
        .map_err(|e| error(e.to_string()))?
        .to_str()
        .ok_or_else(|| error("backup paths must be UTF-8"))?
        .replace('\\', "/");
    validate_backup_relative_path(&relative)?;
    if relative.len() > MAX_BACKUP_PATH_BYTES {
        return Err(error("backup path exceeds its bound"));
    }
    Ok(relative)
}

fn collect(
    root: &Path,
    dir: &Path,
    limits: &OnlineBackupLimits,
    started: Instant,
    out: &mut Vec<CapturedFile>,
    metadata_bytes: &mut usize,
) -> Result<()> {
    if started.elapsed().as_millis() > u128::from(limits.max_coordination_millis) {
        return Err(error("online backup coordination budget exceeded"));
    }
    for entry in fs::read_dir(dir)? {
        if started.elapsed().as_millis() > u128::from(limits.max_coordination_millis) {
            return Err(error("online backup coordination budget exceeded"));
        }
        if out.len() >= limits.max_files {
            return Err(error("online backup file limit exceeded"));
        }
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| error("backup paths must be UTF-8"))?;
        if name.starts_with(".backup-")
            || name == "backup-source.json"
            || matches!(
                name,
                storage::BACKUP_ONLINE_LOCK
                    | storage::BACKUP_WRITE_GATE
                    | "store.lock"
                    | "server.shutdown"
            )
            || name.ends_with(".tmp")
            || name.ends_with(".compact")
        {
            continue;
        }
        let kind = entry.file_type()?;
        if kind.is_symlink() {
            return Err(error("online backup source contains a symbolic link"));
        }
        if kind.is_dir() {
            collect(root, &path, limits, started, out, metadata_bytes)?;
            continue;
        }
        if !kind.is_file() {
            return Err(error("online backup source contains a non-regular file"));
        }
        let relative = relative(root, &path)?;
        if physical_file(&relative, "paged/store.pages")
            || physical_file(&relative, "paged/store.wal")
        {
            continue;
        }
        *metadata_bytes = metadata_bytes
            .checked_add(relative.len() * 6 + 512)
            .ok_or_else(|| error("backup metadata accounting overflow"))?;
        if *metadata_bytes > limits.max_manifest_bytes {
            return Err(error("backup source metadata exceeds its memory bound"));
        }
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW);
        let file = options.open(&path)?;
        let meta = file.metadata()?;
        if !meta.is_file() {
            return Err(error("online backup source changed file type"));
        }
        out.push(CapturedFile {
            path: relative,
            bytes: meta.len(),
            source: CapturedSource::File(file),
        });
    }
    Ok(())
}

fn physical_file(path: &str, base: &str) -> bool {
    path == base
        || path
            .strip_prefix(base)
            .and_then(|p| p.strip_prefix('.'))
            .is_some_and(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
}
