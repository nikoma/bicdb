use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::aead::{Aead, KeyInit, OsRng, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;
use zeroize::Zeroize;

use crate::db::{BicDb, DbConfig};
use crate::error::{BicDbError, Result};
use crate::format::{self, FormatMetadata};
use crate::storage;

const LEGACY_BACKUP_VERSION: u32 = 1;
const BACKUP_VERSION: u32 = 2;
const BACKUP_MAGIC: &[u8; 8] = b"BICBAK02";
const STREAMING_BACKUP_VERSION: u32 = 3;
const STREAMING_BACKUP_MAGIC: &[u8; 8] = b"BICBAK03";
const SALT_LEN: usize = 16;
const NONCE_LEN: usize = 24;
const NONCE_PREFIX_LEN: usize = NONCE_LEN - 8;
const KEY_LEN: usize = 32;
const MAX_BACKUP_HEADER_BYTES: usize = 64 * 1024 * 1024;
const MAX_BACKUP_MANIFEST_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const BACKUP_SOURCE_ENTRY_MEMORY_CHARGE: usize = 256;
const MAX_BACKUP_FILES: usize = 1_000_000;
const MAX_BACKUP_PATH_BYTES: usize = 16 * 1024;
const STREAMING_BACKUP_CHUNK_BYTES: usize = 1024 * 1024;
const MAX_STREAMING_BACKUP_CHUNK_BYTES: usize = 4 * 1024 * 1024;
const STREAMING_FRAME_HEADER_BYTES: usize = 9;
const AEAD_TAG_BYTES: usize = 16;
const STREAMING_HEADER_SEAL_BYTES: usize = AEAD_TAG_BYTES;
const STREAMING_CODEC_RAW: u8 = 0;
const STREAMING_CODEC_ZSTD: u8 = 1;
const STREAMING_HEADER_SEAL_CODEC: u8 = u8::MAX;
const FILE_HASH_BUFFER_BYTES: usize = 256 * 1024;
const ARGON2_MEMORY_KIB: u32 = 19 * 1024;
const ARGON2_TIME_COST: u32 = 2;
const ARGON2_PARALLELISM: u32 = 1;

#[derive(Clone)]
pub struct BackupCreateOptions {
    pub passphrase: String,
    pub base_backup: Option<PathBuf>,
}

impl std::fmt::Debug for BackupCreateOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BackupCreateOptions")
            .field("passphrase", &"<redacted>")
            .field("base_backup", &self.base_backup)
            .finish()
    }
}

impl Drop for BackupCreateOptions {
    fn drop(&mut self) {
        self.passphrase.zeroize();
    }
}

#[derive(Clone)]
pub struct BackupRestoreOptions {
    pub passphrase: String,
    pub force: bool,
}

impl std::fmt::Debug for BackupRestoreOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BackupRestoreOptions")
            .field("passphrase", &"<redacted>")
            .field("force", &self.force)
            .finish()
    }
}

impl Drop for BackupRestoreOptions {
    fn drop(&mut self) {
        self.passphrase.zeroize();
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupCreateReport {
    pub backup_id: Uuid,
    pub full: bool,
    pub files_total: usize,
    pub files_included: usize,
    pub plaintext_bytes: u64,
    pub encrypted_bytes: u64,
    pub manifest_hash: String,
    pub path: PathBuf,
    pub source_format_version: u32,
    pub source_feature_flags: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupVerifyReport {
    pub backup_id: Uuid,
    pub full: bool,
    pub files_total: usize,
    pub files_included: usize,
    pub manifest_hash: String,
    pub base_backup_id: Option<Uuid>,
    pub base_manifest_hash: Option<String>,
    pub chain_start_timestamp: Option<i64>,
    pub chain_end_timestamp: Option<i64>,
    pub archived_events: usize,
    pub source_format_version: u32,
    pub source_feature_flags: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupRestoreReport {
    pub backup_id: Uuid,
    pub files_restored: usize,
    pub target_path: PathBuf,
    pub manifest_hash: String,
    pub target_timestamp: Option<i64>,
    pub restored_records: Option<usize>,
    pub source_format_version: u32,
}

#[derive(Clone)]
pub struct BackupPointInTimeRestoreOptions {
    pub passphrase: String,
    pub force: bool,
    pub target_timestamp: Option<i64>,
}

impl std::fmt::Debug for BackupPointInTimeRestoreOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BackupPointInTimeRestoreOptions")
            .field("passphrase", &"<redacted>")
            .field("force", &self.force)
            .field("target_timestamp", &self.target_timestamp)
            .finish()
    }
}

impl Drop for BackupPointInTimeRestoreOptions {
    fn drop(&mut self) {
        self.passphrase.zeroize();
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupPitrReplayLimits {
    pub max_records_per_batch: usize,
    pub max_events_per_batch: usize,
    pub max_event_bytes_per_batch: usize,
}

impl Default for BackupPitrReplayLimits {
    fn default() -> Self {
        Self {
            max_records_per_batch: 1_024,
            max_events_per_batch: 1_024,
            max_event_bytes_per_batch: 8 * 1024 * 1024,
        }
    }
}

impl BackupPitrReplayLimits {
    pub fn validate(&self) -> Result<()> {
        if self.max_records_per_batch == 0
            || self.max_records_per_batch > 1_000_000
            || self.max_events_per_batch == 0
            || self.max_events_per_batch > 1_000_000
            || self.max_event_bytes_per_batch == 0
            || self.max_event_bytes_per_batch > 256 * 1024 * 1024
        {
            return Err(BicDbError::Backup(
                "PITR replay limits require 1..=1,000,000 records/events and 1..=256 MiB of event data per batch"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupChainVerifyReport {
    pub backups_verified: usize,
    pub base_backup_id: Uuid,
    pub final_backup_id: Uuid,
    pub final_manifest_hash: String,
    pub files_total: usize,
    pub files_included: usize,
    pub chain_start_timestamp: Option<i64>,
    pub chain_end_timestamp: Option<i64>,
    pub archived_events: usize,
    pub final_source_format_version: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupDrillReport {
    pub backup_id: Uuid,
    pub target_path: PathBuf,
    pub target_timestamp: Option<i64>,
    pub files_restored: usize,
    pub manifest_hash: String,
    pub integrity_checked: bool,
    pub smoke_collections: usize,
    pub smoke_records: usize,
    pub archived_events: usize,
    pub rto_ms: u64,
    pub rpo_seconds: Option<i64>,
    pub source_format_version: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EncryptedBackup {
    version: u32,
    backup_id: Uuid,
    created_at: i64,
    kdf: BackupKdf,
    nonce: [u8; NONCE_LEN],
    #[serde(default)]
    compression: Option<String>,
    ciphertext_sha256: String,
    ciphertext: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BackupKdf {
    algorithm: String,
    memory_kib: u32,
    time_cost: u32,
    parallelism: u32,
    salt: [u8; SALT_LEN],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BackupArchive {
    version: u32,
    backup_id: Uuid,
    created_at: i64,
    full: bool,
    base_backup_id: Option<Uuid>,
    base_manifest_hash: Option<String>,
    manifest: BackupManifest,
    #[serde(default = "legacy_source_format")]
    source_format: FormatMetadata,
    files: Vec<BackupFile>,
    #[serde(default)]
    recovery: BackupRecoveryMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct BackupRecoveryMetadata {
    protocol: String,
    checkpoint_timestamp: i64,
    chain_start_timestamp: Option<i64>,
    chain_end_timestamp: Option<i64>,
    archived_events: usize,
}

impl Default for BackupRecoveryMetadata {
    fn default() -> Self {
        Self {
            protocol: "legacy-file-archive".to_string(),
            checkpoint_timestamp: 0,
            chain_start_timestamp: None,
            chain_end_timestamp: None,
            archived_events: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct BackupManifest {
    manifest_hash: String,
    files: Vec<BackupManifestEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct BackupManifestEntry {
    path: String,
    size: u64,
    sha256: String,
    /// A file copied FUZZILY from a live paged store (page segments, active
    /// WAL): its size was pinned at collect time, exactly that many bytes
    /// were streamed while writes continued, and `sha256` is a zero
    /// sentinel. Integrity comes from the archive's authenticated frames;
    /// CONSISTENCY comes from WAL replay on open — restore verification
    /// checks size only. Absent (false) in every non-online archive, so
    /// standard archives are byte-identical to previous releases.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    fuzzy: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BackupFile {
    path: String,
    size: u64,
    sha256: String,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EncryptedBackupHeader {
    version: u32,
    backup_id: Uuid,
    created_at: i64,
    kdf: BackupKdf,
    nonce: [u8; NONCE_LEN],
    compression: Option<String>,
    ciphertext_sha256: String,
    ciphertext_len: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct BackupArchiveHeader {
    version: u32,
    backup_id: Uuid,
    created_at: i64,
    full: bool,
    base_backup_id: Option<Uuid>,
    base_manifest_hash: Option<String>,
    manifest: BackupManifest,
    source_format: FormatMetadata,
    files: Vec<BackupManifestEntry>,
    #[serde(default)]
    removed_files: Vec<String>,
    recovery: BackupRecoveryMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StreamingBackupHeader {
    version: u32,
    backup_id: Uuid,
    created_at: i64,
    kdf: BackupKdf,
    nonce_prefix: [u8; NONCE_PREFIX_LEN],
    chunk_plaintext_bytes: u32,
}

#[derive(Clone, Debug)]
struct BackupSourceFile {
    entry: BackupManifestEntry,
    source_path: PathBuf,
}

pub fn create_backup(
    db_path: impl AsRef<Path>,
    backup_path: impl AsRef<Path>,
    options: BackupCreateOptions,
) -> Result<BackupCreateReport> {
    create_backup_with_mode(db_path, backup_path, options, BackupSourceMode::Standard)
}

/// Create a consistent archive from an already-open database, including an
/// encrypted database which cannot be reopened without its live key
/// capability. The caller's handle supplies recovery bounds; the physical
/// snapshot gate still prevents writers from crossing the file-copy cut.
pub fn create_backup_from_open_database(
    db: &BicDb,
    backup_path: impl AsRef<Path>,
    options: BackupCreateOptions,
) -> Result<BackupCreateReport> {
    db.flush()?;
    let (chain_start_timestamp, chain_end_timestamp, archived_events) = db.backup_event_bounds();
    let recovery = BackupRecoveryMetadata {
        protocol: "open-handle-checkpoint-plus-audit-event-archive".to_string(),
        checkpoint_timestamp: unix_timestamp(),
        chain_start_timestamp,
        chain_end_timestamp,
        archived_events,
    };
    create_backup_with_mode(
        db.data_path(),
        backup_path,
        options,
        BackupSourceMode::OpenDatabase { recovery },
    )
}

/// The base half of an ONLINE paged backup: page segments and the active WAL
/// are copied fuzzily (size-pinned, streamed while writes continue), in
/// pages-before-WAL order. The caller must hold the store's backup pin for
/// the whole call; `BicDb::create_online_backup` is the supported entry.
pub fn create_online_paged_base_backup(
    db_path: impl AsRef<Path>,
    backup_path: impl AsRef<Path>,
    options: BackupCreateOptions,
) -> Result<BackupCreateReport> {
    create_backup_with_mode(
        db_path,
        backup_path,
        options,
        BackupSourceMode::OnlinePagedBase,
    )
}

/// The consistency-cut half: an incremental over `options.base_backup`
/// shipping exactly the sealed WAL segments named in `sealed_relative`
/// (immutable files, hashed normally). Applied after the base, restore
/// recovery replays the sealed chain past every page write the base could
/// have observed and truncates the base's stale active-WAL copy.
pub fn create_wal_tail_backup(
    db_path: impl AsRef<Path>,
    backup_path: impl AsRef<Path>,
    options: BackupCreateOptions,
    sealed_relative: Vec<String>,
) -> Result<BackupCreateReport> {
    create_backup_with_mode(
        db_path,
        backup_path,
        options,
        BackupSourceMode::WalTail { sealed_relative },
    )
}

fn create_backup_with_mode(
    db_path: impl AsRef<Path>,
    backup_path: impl AsRef<Path>,
    options: BackupCreateOptions,
    mode: BackupSourceMode,
) -> Result<BackupCreateReport> {
    let db_path = db_path.as_ref();
    let backup_path = backup_path.as_ref();
    if let Some(parent) = backup_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary_path = randomized_sibling(backup_path, "backup");
    let mut temporary = TemporaryBackupFile::create(&temporary_path)?;
    let mut writer = BufWriter::new(temporary.take_file());
    let mut report = write_streaming_backup(
        db_path,
        backup_path,
        &mut writer,
        &options,
        backup_path.to_path_buf(),
        &mode,
    )?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    fs::rename(&temporary_path, backup_path)?;
    sync_parent_directory(backup_path)?;
    temporary.disarm();
    // A publish that was interrupted before its rename leaves a partial temp
    // beside the archive. Nothing reads it, but nothing removed it either, so
    // every interrupted backup left a half-archive on disk forever. Reclaim
    // them now that a complete archive is published.
    reclaim_stale_backup_temporaries(backup_path);
    report.encrypted_bytes = fs::metadata(backup_path)?.len();
    Ok(report)
}

/// Remove partial temporaries left beside `backup_path` by interrupted
/// publishes: both the historical `<archive>.tmp` form and the current
/// `.<archive>.<purpose>.<uuid>.tmp` form.
///
/// Only siblings whose name is derived from THIS archive are considered, and
/// failures are ignored — the archive is already published, and refusing to
/// report success because cleanup failed would turn a full disk into a lost
/// backup.
fn reclaim_stale_backup_temporaries(backup_path: &Path) {
    let _ = fs::remove_file(backup_path.with_extension("tmp"));
    let Some(name) = backup_path.file_name().and_then(|name| name.to_str()) else {
        return;
    };
    let Some(parent) = backup_path.parent() else {
        return;
    };
    let prefix = format!(".{name}.");
    let Ok(entries) = fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let entry_name = entry.file_name();
        let Some(entry_name) = entry_name.to_str() else {
            continue;
        };
        if entry_name.starts_with(&prefix) && entry_name.ends_with(".tmp") {
            let _ = fs::remove_file(entry.path());
        }
    }
}

/// How `write_streaming_backup` sources and orders its file set.
enum BackupSourceMode {
    Standard,
    OpenDatabase { recovery: BackupRecoveryMetadata },
    OnlinePagedBase,
    WalTail { sealed_relative: Vec<String> },
}

/// Roll a restored paged database forward from a WAL archive: copy every
/// archived sealed segment the restored directory does not already hold
/// into its `paged/` directory. The next open replays the full chain —
/// recovery is what applies the roll-forward, exactly as after a crash.
/// Returns the number of segments applied.
///
/// Apply BEFORE the first open of the restored directory. Segments are
/// named by their first LSN, so "newer than the restore" is simply "not
/// already present".
pub fn apply_archived_wal(
    restored_db_path: impl AsRef<Path>,
    archive_path: impl AsRef<Path>,
) -> Result<usize> {
    let paged_dir = restored_db_path.as_ref().join("paged");
    if !paged_dir.is_dir() {
        return Err(BicDbError::Backup(format!(
            "{} has no paged/ directory; restore a backup chain first",
            restored_db_path.as_ref().display()
        )));
    }
    let mut applied = 0usize;
    let mut entries: Vec<_> =
        fs::read_dir(archive_path.as_ref())?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(BicDbError::Backup(format!(
                "archived WAL entry {} is a symbolic link",
                entry.path().display()
            )));
        }
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(suffix) = name.strip_prefix("store.wal.") else {
            continue;
        };
        if suffix.parse::<u64>().is_err() {
            continue; // .partial staging leftovers and strangers
        }
        let target = paged_dir.join(name);
        if target.exists() {
            continue;
        }
        let staging = paged_dir.join(format!(".{name}.{}.applying", Uuid::new_v4().simple()));
        let mut source_options = OpenOptions::new();
        source_options.read(true);
        #[cfg(unix)]
        source_options.custom_flags(libc::O_NOFOLLOW);
        let mut source = source_options.open(entry.path())?;
        let mut copied = create_new_private(&staging)?;
        std::io::copy(&mut source, &mut copied)?;
        copied.sync_all()?;
        drop(copied);
        fs::rename(&staging, &target)?;
        applied += 1;
    }
    Ok(applied)
}

/// What one archived-WAL pruning pass did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalPruneReport {
    pub examined: usize,
    pub pruned: usize,
    pub bytes_freed: u64,
    pub kept: usize,
    pub keep_from_sequence: u64,
}

/// The WAL floor a restored/online base backup establishes: the highest
/// sealed segment sequence its `paged/` directory already holds. Archived
/// segments strictly below it are contained in (or superseded by) the base
/// and are safe to prune once that base is verified.
pub fn wal_floor_of_base(base_path: impl AsRef<Path>) -> Result<u64> {
    let paged_dir = base_path.as_ref().join("paged");
    let mut floor: Option<u64> = None;
    for entry in fs::read_dir(&paged_dir).map_err(|error| {
        BicDbError::Backup(format!("cannot read {}: {error}", paged_dir.display()))
    })? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() || !file_type.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(suffix) = name.strip_prefix("store.wal.") else {
            continue;
        };
        if let Ok(sequence) = suffix.parse::<u64>() {
            floor = Some(floor.map_or(sequence, |current| current.max(sequence)));
        }
    }
    floor.ok_or_else(|| {
        BicDbError::Backup(format!(
            "{} holds no sealed WAL segments; cannot establish a prune floor",
            paged_dir.display()
        ))
    })
}

fn verified_wal_floor_of_base(base_path: &Path) -> Result<u64> {
    let paged_dir = base_path.join("paged");
    let pages_path = paged_dir.join("store.pages");
    let wal_path = paged_dir.join("store.wal");
    let store = bicdb_page::PageStore::open_existing_auto(&pages_path, false).map_err(|error| {
        BicDbError::Backup(format!(
            "base {} failed page-store verification: {error}",
            base_path.display()
        ))
    })?;
    let wal_metadata = fs::symlink_metadata(&wal_path).map_err(|error| {
        BicDbError::Backup(format!(
            "base {} has no verifiable active WAL: {error}",
            base_path.display()
        ))
    })?;
    if wal_metadata.file_type().is_symlink() || !wal_metadata.is_file() {
        return Err(BicDbError::Backup(format!(
            "base {} active WAL is not a regular file",
            base_path.display()
        )));
    }
    let wal =
        bicdb_page::Wal::open_with_segments_and_floor(&wal_path, false, 0, store.checkpoint_lsn())
            .map_err(|error| {
                BicDbError::Backup(format!(
                    "base {} failed WAL-chain verification: {error}",
                    base_path.display()
                ))
            })?;
    wal.verify_integrity().map_err(|error| {
        BicDbError::Backup(format!(
            "base {} failed WAL-chain verification: {error}",
            base_path.display()
        ))
    })?;
    wal_floor_of_base(base_path)
}

/// Deletes archived WAL segments strictly below the oldest verified retained
/// base's floor. Every retained base must be supplied; choosing the minimum is
/// what preserves roll-forward chains for older bases.
/// The floor segment itself and everything above it are kept — they are
/// the roll-forward chain. Only well-formed `store.wal.<seq>` names are
/// ever touched; staging leftovers and strangers are ignored.
pub fn prune_archived_wal(
    archive_path: impl AsRef<Path>,
    retained_bases: &[PathBuf],
) -> Result<WalPruneReport> {
    if retained_bases.is_empty() {
        return Err(BicDbError::Backup(
            "refusing to prune without every retained base directory".to_string(),
        ));
    }
    let mut keep_from_sequence = u64::MAX;
    for base in retained_bases {
        keep_from_sequence = keep_from_sequence.min(verified_wal_floor_of_base(base)?);
    }
    let mut report = WalPruneReport {
        keep_from_sequence,
        ..WalPruneReport::default()
    };
    for entry in fs::read_dir(archive_path.as_ref())? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(suffix) = name.strip_prefix("store.wal.") else {
            continue;
        };
        let Ok(sequence) = suffix.parse::<u64>() else {
            continue;
        };
        report.examined += 1;
        if sequence >= keep_from_sequence {
            report.kept += 1;
            continue;
        }
        let bytes = fs::metadata(entry.path())
            .map(|meta| meta.len())
            .unwrap_or(0);
        fs::remove_file(entry.path())?;
        report.pruned += 1;
        report.bytes_freed = report.bytes_freed.saturating_add(bytes);
    }
    Ok(report)
}

pub fn verify_backup(
    backup_path: impl AsRef<Path>,
    passphrase: impl AsRef<str>,
) -> Result<BackupVerifyReport> {
    let mut reader = BufReader::new(File::open(backup_path)?);
    let header = verify_backup_reader_header(&mut reader, passphrase.as_ref())?;
    Ok(verify_report_from_header(&header))
}

pub fn verify_backup_chain<I, P>(
    backup_paths: I,
    passphrase: impl AsRef<str>,
) -> Result<BackupChainVerifyReport>
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
{
    let passphrase = passphrase.as_ref();
    let mut archives = Vec::new();
    for path in backup_paths {
        let mut reader = BufReader::new(File::open(path)?);
        archives.push(verify_backup_reader_header(&mut reader, passphrase)?);
    }

    if archives.is_empty() {
        return Err(BicDbError::Backup(
            "backup chain must contain at least one backup".to_string(),
        ));
    }
    if !archives[0].full {
        return Err(BicDbError::Backup(
            "backup chain must start with a full backup".to_string(),
        ));
    }

    for pair in archives.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];
        if current.full {
            return Err(BicDbError::Backup(
                "backup chain contains a full backup after the first entry".to_string(),
            ));
        }
        if current.base_backup_id != Some(previous.backup_id) {
            return Err(BicDbError::Backup(format!(
                "backup {} does not reference previous backup {}",
                current.backup_id, previous.backup_id
            )));
        }
        if current.base_manifest_hash.as_deref() != Some(previous.manifest.manifest_hash.as_str()) {
            return Err(BicDbError::Backup(format!(
                "backup {} base manifest does not match previous backup",
                current.backup_id
            )));
        }
    }

    let first = archives.first().unwrap();
    let final_archive = archives.last().unwrap();
    Ok(BackupChainVerifyReport {
        backups_verified: archives.len(),
        base_backup_id: first.backup_id,
        final_backup_id: final_archive.backup_id,
        final_manifest_hash: final_archive.manifest.manifest_hash.clone(),
        files_total: final_archive.manifest.files.len(),
        files_included: archives.iter().map(|archive| archive.files.len()).sum(),
        chain_start_timestamp: first.recovery.chain_start_timestamp,
        chain_end_timestamp: final_archive.recovery.chain_end_timestamp,
        archived_events: archives
            .iter()
            .map(|archive| archive.recovery.archived_events)
            .sum(),
        final_source_format_version: final_archive.source_format.format_version,
    })
}

pub fn restore_backup(
    backup_path: impl AsRef<Path>,
    target_path: impl AsRef<Path>,
    options: BackupRestoreOptions,
) -> Result<BackupRestoreReport> {
    let mut reader = BufReader::new(File::open(backup_path)?);
    let verified = verify_backup_reader_header(&mut reader, &options.passphrase)?;
    format::check_backup_restore_compatible(&verified.source_format)?;
    let target_path = target_path.as_ref();

    if fs::symlink_metadata(target_path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        return Err(BicDbError::Backup(format!(
            "restore target {} is a symbolic link",
            target_path.display()
        )));
    }

    if options.force && verified.full && target_path.exists() {
        fs::remove_dir_all(target_path)?;
    }
    fs::create_dir_all(target_path)?;
    if verified.full && !options.force && target_has_entries(target_path)? {
        return Err(BicDbError::Backup(format!(
            "restore target {} is not empty; pass --force for a full restore",
            target_path.display()
        )));
    }

    reader.seek(SeekFrom::Start(0))?;
    let mut magic = [0_u8; 8];
    reader.read_exact(&mut magic)?;
    reader.seek(SeekFrom::Start(0))?;
    let files_restored = if &magic == STREAMING_BACKUP_MAGIC {
        restore_streaming_archive(&mut reader, &options.passphrase, target_path, &verified)?
    } else {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        let encrypted = decode_encrypted_backup(&bytes)?;
        let archive = decrypt_archive(&encrypted, &options.passphrase)?;
        for file in &archive.files {
            let path = safe_join(target_path, &file.path)?;
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            write_atomic(&path, &file.bytes)?;
        }
        archive.files.len()
    };

    verify_restored_manifest(target_path, &verified.manifest)?;
    Ok(BackupRestoreReport {
        backup_id: verified.backup_id,
        files_restored,
        target_path: target_path.to_path_buf(),
        manifest_hash: verified.manifest.manifest_hash,
        target_timestamp: None,
        restored_records: None,
        source_format_version: verified.source_format.format_version,
    })
}

pub fn restore_backup_to_point(
    backup_path: impl AsRef<Path>,
    target_path: impl AsRef<Path>,
    options: BackupPointInTimeRestoreOptions,
) -> Result<BackupRestoreReport> {
    restore_backup_to_point_with_limits(
        backup_path,
        target_path,
        options,
        BackupPitrReplayLimits::default(),
    )
}

pub fn restore_backup_to_point_with_limits(
    backup_path: impl AsRef<Path>,
    target_path: impl AsRef<Path>,
    options: BackupPointInTimeRestoreOptions,
    limits: BackupPitrReplayLimits,
) -> Result<BackupRestoreReport> {
    limits.validate()?;
    let restore = restore_backup(
        backup_path,
        target_path.as_ref(),
        BackupRestoreOptions {
            passphrase: options.passphrase.clone(),
            force: options.force,
        },
    )?;
    let Some(target_timestamp) = options.target_timestamp else {
        return Ok(restore);
    };

    let db = BicDb::open_with_config(target_path.as_ref(), config_for(target_path.as_ref())?)?;
    let restored_records = db.restore_audit_snapshot_bounded(
        target_timestamp,
        limits.max_records_per_batch,
        limits.max_events_per_batch,
        limits.max_event_bytes_per_batch,
    )?;
    db.close()?;

    Ok(BackupRestoreReport {
        target_timestamp: Some(target_timestamp),
        restored_records: Some(restored_records),
        ..restore
    })
}

/// Config for opening the database at `path` in the storage mode recorded in it.
///
/// Backup and restore act on *whatever database they were pointed at*. Opening
/// with the ambient default instead refuses a `server_paged` database with a
/// mode-mismatch error blaming the caller for a mode they never chose — and it
/// made backup, point-in-time restore, and the recovery drill entirely
/// unavailable in paged mode, which is exactly where an operator most wants to
/// rehearse recovery.
fn config_for(path: &Path) -> Result<DbConfig> {
    Ok(DbConfig::default().with_storage_mode(crate::format::storage_mode(path)?))
}

/// Whether opening `path` a second time is refused because the page engine holds
/// an exclusive lock on it.
///
/// An *online* backup runs against a database another handle already has open.
/// The paged engine refuses a second opener (it would corrupt the store), so any
/// backup step that works by opening the database has to tolerate not being able
/// to. That is sound for the paged engine specifically: a commit is durable in
/// its WAL before it returns, so a file-level copy plus WAL replay is restorable
/// without anyone flushing anything first. The step is skipped, not faked.
fn already_open(error: &BicDbError) -> bool {
    matches!(error, BicDbError::PagedStorage(message) if message.contains("already open"))
}

pub fn drill_backup_restore(
    backup_path: impl AsRef<Path>,
    target_path: impl AsRef<Path>,
    options: BackupPointInTimeRestoreOptions,
) -> Result<BackupDrillReport> {
    drill_backup_restore_with_limits(
        backup_path,
        target_path,
        options,
        BackupPitrReplayLimits::default(),
    )
}

pub fn drill_backup_restore_with_limits(
    backup_path: impl AsRef<Path>,
    target_path: impl AsRef<Path>,
    options: BackupPointInTimeRestoreOptions,
    limits: BackupPitrReplayLimits,
) -> Result<BackupDrillReport> {
    let started = Instant::now();
    let verify = verify_backup(&backup_path, &options.passphrase)?;
    let restore =
        restore_backup_to_point_with_limits(&backup_path, target_path.as_ref(), options, limits)?;
    let config = config_for(target_path.as_ref())?;
    BicDb::verify_path(target_path.as_ref(), config.clone(), None)?;
    let db = BicDb::open_with_config(target_path.as_ref(), config)?;
    let stats = db.stats()?;
    let rto_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    let rpo_seconds = match (verify.chain_end_timestamp, restore.target_timestamp) {
        (Some(end), Some(target)) => Some((end - target).abs()),
        _ => None,
    };
    Ok(BackupDrillReport {
        backup_id: restore.backup_id,
        target_path: restore.target_path,
        target_timestamp: restore.target_timestamp,
        files_restored: restore.files_restored,
        manifest_hash: restore.manifest_hash,
        integrity_checked: true,
        smoke_collections: stats.collection_count,
        smoke_records: stats.record_count,
        archived_events: verify.archived_events,
        rto_ms,
        rpo_seconds,
        source_format_version: verify.source_format_version,
    })
}

pub fn create_backup_to_writer<W: Write>(
    db_path: impl AsRef<Path>,
    writer: &mut W,
    options: BackupCreateOptions,
) -> Result<BackupCreateReport> {
    write_streaming_backup(
        db_path.as_ref(),
        Path::new("stream.bicbackup"),
        writer,
        &options,
        PathBuf::from("<stream>"),
        &BackupSourceMode::Standard,
    )
}

pub fn verify_backup_from_reader<R: Read>(
    reader: &mut R,
    passphrase: impl AsRef<str>,
) -> Result<BackupVerifyReport> {
    let header = verify_backup_reader_header(reader, passphrase.as_ref())?;
    Ok(verify_report_from_header(&header))
}

fn write_streaming_backup<W: Write>(
    db_path: &Path,
    backup_path: &Path,
    writer: &mut W,
    options: &BackupCreateOptions,
    report_path: PathBuf,
    mode: &BackupSourceMode,
) -> Result<BackupCreateReport> {
    if !db_path.exists() {
        return Err(BicDbError::Backup(format!(
            "database path {} does not exist",
            db_path.display()
        )));
    }
    // The WAL-tail archive runs immediately after its base, inside the same
    // backup pin; the online-checkpoint marker (and the segment-writer pause
    // it implies) belongs to the base pass alone.
    let mut checkpoint = match mode {
        BackupSourceMode::WalTail { .. } => None,
        BackupSourceMode::OpenDatabase { .. } => {
            Some(OnlineBackupCheckpoint::begin_open_database(db_path)?)
        }
        _ => Some(OnlineBackupCheckpoint::begin(db_path)?),
    };
    let recovery = match mode {
        BackupSourceMode::OpenDatabase { recovery } => recovery.clone(),
        _ => recovery_metadata(db_path)?,
    };
    let source_format = format::read_or_legacy_metadata(db_path)?;
    if let Some(checkpoint) = checkpoint.as_mut() {
        // All preparation that may open/recover the database is complete.
        // From this point until the archive finishes, physical writers hold
        // the same inode shared and therefore cannot cross this exclusive cut.
        checkpoint.acquire_snapshot_gate(db_path)?;
    }

    let base = options
        .base_backup
        .as_ref()
        .map(|path| {
            let mut reader = BufReader::new(File::open(path)?);
            verify_backup_reader_header(&mut reader, &options.passphrase)
        })
        .transpose()?;

    let (manifest, files, removed_files) = match mode {
        BackupSourceMode::WalTail { sealed_relative } => {
            let base_header = base.as_ref().ok_or_else(|| {
                BicDbError::Backup("a WAL-tail archive requires its base backup".to_string())
            })?;
            // No source rescan: the restored tree IS the base's files plus
            // the sealed segments shipped here, so the manifest is the base
            // manifest (fuzzy entries and all) plus the sealed additions.
            let mut entries = base_header
                .manifest
                .files
                .iter()
                .filter(|entry| !sealed_relative.contains(&entry.path))
                .cloned()
                .collect::<Vec<_>>();
            let mut ship = Vec::new();
            for relative in sealed_relative {
                validate_backup_relative_path(relative)?;
                let path = safe_join(db_path, relative)?;
                let (size, sha256) = hash_file(&path)?;
                let entry = BackupManifestEntry {
                    path: relative.clone(),
                    size,
                    sha256,
                    fuzzy: false,
                };
                entries.push(entry.clone());
                ship.push(BackupSourceFile {
                    entry,
                    source_path: path,
                });
            }
            entries.sort_by(|left, right| {
                online_copy_order(&left.path)
                    .cmp(&online_copy_order(&right.path))
                    .then_with(|| left.path.cmp(&right.path))
            });
            let manifest = BackupManifest {
                manifest_hash: manifest_hash(&entries),
                files: entries,
            };
            (manifest, ship, Vec::new())
        }
        _ => {
            // `recovery_metadata` may open and checkpoint a closed paged
            // database. Hash the source set only after every such mutating
            // preparation step so the second streaming pass is guaranteed to
            // describe the same bytes.
            let online = matches!(mode, BackupSourceMode::OnlinePagedBase);
            let current_files = collect_source_files_with_mode(db_path, Some(backup_path), online)?;
            let manifest_files = current_files
                .iter()
                .map(|file| file.entry.clone())
                .collect::<Vec<_>>();
            let manifest = BackupManifest {
                manifest_hash: manifest_hash(&manifest_files),
                files: manifest_files,
            };
            let base_files = base
                .as_ref()
                .map(|header| {
                    header
                        .manifest
                        .files
                        .iter()
                        .map(|entry| (entry.path.clone(), entry.sha256.clone()))
                        .collect::<BTreeMap<_, _>>()
                })
                .unwrap_or_default();
            let files = current_files
                .into_iter()
                .filter(|file| {
                    file.entry.fuzzy || base_files.get(&file.entry.path) != Some(&file.entry.sha256)
                })
                .collect::<Vec<_>>();
            let current_paths = manifest
                .files
                .iter()
                .map(|entry| entry.path.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            let removed_files = base
                .as_ref()
                .map(|header| {
                    header
                        .manifest
                        .files
                        .iter()
                        .filter(|entry| !current_paths.contains(entry.path.as_str()))
                        .map(|entry| entry.path.clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            (manifest, files, removed_files)
        }
    };
    let backup_id = Uuid::new_v4();
    let created_at = unix_timestamp();
    let archive_header = BackupArchiveHeader {
        version: STREAMING_BACKUP_VERSION,
        backup_id,
        created_at,
        full: base.is_none(),
        base_backup_id: base.as_ref().map(|header| header.backup_id),
        base_manifest_hash: base
            .as_ref()
            .map(|header| header.manifest.manifest_hash.clone()),
        manifest,
        source_format,
        files: files.iter().map(|file| file.entry.clone()).collect(),
        removed_files,
        recovery,
    };
    verify_archive_header(&archive_header)?;

    let mut salt = [0_u8; SALT_LEN];
    let mut nonce_prefix = [0_u8; NONCE_PREFIX_LEN];
    fill_random(&mut salt);
    fill_random(&mut nonce_prefix);
    let outer = StreamingBackupHeader {
        version: STREAMING_BACKUP_VERSION,
        backup_id,
        created_at,
        kdf: BackupKdf {
            algorithm: "argon2id".to_string(),
            memory_kib: ARGON2_MEMORY_KIB,
            time_cost: ARGON2_TIME_COST,
            parallelism: ARGON2_PARALLELISM,
            salt,
        },
        nonce_prefix,
        chunk_plaintext_bytes: STREAMING_BACKUP_CHUNK_BYTES as u32,
    };
    let outer_bytes = serde_json::to_vec(&outer)?;
    if outer_bytes.len() > MAX_BACKUP_HEADER_BYTES {
        return Err(BicDbError::Backup(
            "streaming backup header exceeds the safety limit".to_string(),
        ));
    }
    let key = derive_key_with_kdf(&options.passphrase, &outer.kdf)?;
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&key));
    let header_hash = streaming_header_hash(&outer_bytes);
    let mut counting = CountingWriter::new(writer);
    counting.write_all(STREAMING_BACKUP_MAGIC)?;
    counting.write_all(&(outer_bytes.len() as u64).to_le_bytes())?;
    counting.write_all(&outer_bytes)?;

    let seal_aad = streaming_frame_aad(&header_hash, 0, STREAMING_HEADER_SEAL_CODEC, 0);
    let seal = cipher
        .encrypt(
            XNonce::from_slice(&streaming_nonce(&outer.nonce_prefix, 0)),
            Payload {
                msg: &[],
                aad: &seal_aad,
            },
        )
        .map_err(|_| BicDbError::Backup("backup header authentication failed".to_string()))?;
    debug_assert_eq!(seal.len(), STREAMING_HEADER_SEAL_BYTES);
    counting.write_all(&(seal.len() as u32).to_le_bytes())?;
    counting.write_all(&seal)?;

    let mut frame_index = 1_u64;
    let archive_bytes = serde_json::to_vec(&archive_header)?;
    if archive_bytes.len() > MAX_BACKUP_HEADER_BYTES {
        return Err(BicDbError::Backup(
            "backup archive manifest exceeds the safety limit".to_string(),
        ));
    }
    write_streaming_frame(
        &mut counting,
        &cipher,
        &outer.nonce_prefix,
        &header_hash,
        &mut frame_index,
        &archive_bytes,
        false,
    )?;

    let mut plaintext_bytes = 0_u64;
    let mut buffer = vec![0_u8; STREAMING_BACKUP_CHUNK_BYTES];
    for source in &files {
        // Fuzzy entries stream EXACTLY the pinned size from a file that may
        // be mutating underneath (the paged engine's WAL-replay-on-open is
        // what makes the copy usable); everything else keeps the strict
        // pre-hash/stream double check.
        let file = File::open(&source.source_path)?;
        let mut input: Box<dyn Read> = if source.entry.fuzzy {
            Box::new(BufReader::with_capacity(
                FILE_HASH_BUFFER_BYTES,
                file.take(source.entry.size),
            ))
        } else {
            Box::new(BufReader::with_capacity(FILE_HASH_BUFFER_BYTES, file))
        };
        let mut hasher = Sha256::new();
        let mut observed_size = 0_u64;
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            let chunk = &buffer[..read];
            hasher.update(chunk);
            observed_size = observed_size
                .checked_add(read as u64)
                .ok_or_else(|| BicDbError::Backup("source file size overflow".to_string()))?;
            plaintext_bytes = plaintext_bytes
                .checked_add(read as u64)
                .ok_or_else(|| BicDbError::Backup("backup byte count overflow".to_string()))?;
            write_streaming_frame(
                &mut counting,
                &cipher,
                &outer.nonce_prefix,
                &header_hash,
                &mut frame_index,
                chunk,
                true,
            )?;
        }
        if source.entry.fuzzy {
            if observed_size != source.entry.size {
                return Err(BicDbError::Backup(format!(
                    "online backup source {} shrank below its pinned size while streaming \
                     ({observed_size} of {} bytes); is the backup pin engaged?",
                    source.entry.path, source.entry.size
                )));
            }
            continue;
        }
        let observed_sha256 = hex::encode(hasher.finalize());
        if observed_size != source.entry.size || observed_sha256 != source.entry.sha256 {
            return Err(BicDbError::Backup(format!(
                "source file {} changed while the backup was streaming",
                source.entry.path
            )));
        }
    }
    counting.flush()?;
    let encrypted_bytes = counting.bytes_written();
    Ok(BackupCreateReport {
        backup_id,
        full: archive_header.full,
        files_total: archive_header.manifest.files.len(),
        files_included: archive_header.files.len(),
        plaintext_bytes,
        encrypted_bytes,
        manifest_hash: archive_header.manifest.manifest_hash,
        path: report_path,
        source_format_version: archive_header.source_format.format_version,
        source_feature_flags: feature_flags(&archive_header.source_format),
    })
}

struct CountingWriter<'a, W> {
    inner: &'a mut W,
    written: u64,
}

impl<'a, W> CountingWriter<'a, W> {
    fn new(inner: &'a mut W) -> Self {
        Self { inner, written: 0 }
    }

    fn bytes_written(&self) -> u64 {
        self.written
    }
}

impl<W: Write> Write for CountingWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.written = self
            .written
            .checked_add(written as u64)
            .ok_or_else(|| std::io::Error::other("backup byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn write_streaming_frame<W: Write>(
    writer: &mut W,
    cipher: &XChaCha20Poly1305,
    nonce_prefix: &[u8; NONCE_PREFIX_LEN],
    header_hash: &[u8; 32],
    frame_index: &mut u64,
    plaintext: &[u8],
    allow_compression: bool,
) -> Result<()> {
    let plaintext_len = u32::try_from(plaintext.len())
        .map_err(|_| BicDbError::Backup("backup frame is too large".to_string()))?;
    let (codec, encoded) = encode_streaming_chunk(plaintext, allow_compression)?;
    let aad = streaming_frame_aad(header_hash, *frame_index, codec, plaintext_len);
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&streaming_nonce(nonce_prefix, *frame_index)),
            Payload {
                msg: &encoded,
                aad: &aad,
            },
        )
        .map_err(|_| BicDbError::Backup("backup chunk encryption failed".to_string()))?;
    let ciphertext_len = u32::try_from(ciphertext.len())
        .map_err(|_| BicDbError::Backup("encrypted backup frame is too large".to_string()))?;
    writer.write_all(&[codec])?;
    writer.write_all(&plaintext_len.to_le_bytes())?;
    writer.write_all(&ciphertext_len.to_le_bytes())?;
    writer.write_all(&ciphertext)?;
    *frame_index = frame_index
        .checked_add(1)
        .ok_or_else(|| BicDbError::Backup("backup frame counter overflow".to_string()))?;
    Ok(())
}

struct StreamingArchiveReader<'a, R> {
    reader: &'a mut R,
    cipher: XChaCha20Poly1305,
    nonce_prefix: [u8; NONCE_PREFIX_LEN],
    header_hash: [u8; 32],
    frame_index: u64,
    chunk_plaintext_bytes: usize,
}

impl<'a, R: Read> StreamingArchiveReader<'a, R> {
    fn open(reader: &'a mut R, passphrase: &str) -> Result<(Self, BackupArchiveHeader)> {
        let mut magic = [0_u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != STREAMING_BACKUP_MAGIC {
            return Err(BicDbError::Backup(
                "streaming backup magic mismatch".to_string(),
            ));
        }
        let header_len = read_u64_from_reader(reader)?;
        let header_len = usize::try_from(header_len)
            .map_err(|_| BicDbError::Backup("backup header length is too large".to_string()))?;
        if header_len == 0 || header_len > MAX_BACKUP_HEADER_BYTES {
            return Err(BicDbError::Backup(
                "streaming backup header exceeds the safety limit".to_string(),
            ));
        }
        let mut outer_bytes = vec![0_u8; header_len];
        reader.read_exact(&mut outer_bytes)?;
        let outer: StreamingBackupHeader = serde_json::from_slice(&outer_bytes)?;
        if outer.version != STREAMING_BACKUP_VERSION {
            return Err(BicDbError::Backup(format!(
                "unsupported streaming backup version {}",
                outer.version
            )));
        }
        let chunk_plaintext_bytes = outer.chunk_plaintext_bytes as usize;
        if chunk_plaintext_bytes == 0 || chunk_plaintext_bytes > MAX_STREAMING_BACKUP_CHUNK_BYTES {
            return Err(BicDbError::Backup(format!(
                "invalid streaming backup chunk size {}",
                outer.chunk_plaintext_bytes
            )));
        }
        let key = derive_key_with_kdf(passphrase, &outer.kdf)?;
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&key));
        let header_hash = streaming_header_hash(&outer_bytes);
        let seal_len = read_u32_from_reader(reader)? as usize;
        if seal_len != STREAMING_HEADER_SEAL_BYTES {
            return Err(BicDbError::Backup(
                "streaming backup header seal length is invalid".to_string(),
            ));
        }
        let mut seal = vec![0_u8; seal_len];
        reader.read_exact(&mut seal)?;
        let seal_aad = streaming_frame_aad(&header_hash, 0, STREAMING_HEADER_SEAL_CODEC, 0);
        let opened = cipher
            .decrypt(
                XNonce::from_slice(&streaming_nonce(&outer.nonce_prefix, 0)),
                Payload {
                    msg: &seal,
                    aad: &seal_aad,
                },
            )
            .map_err(|_| BicDbError::Backup("backup header authentication failed".to_string()))?;
        if !opened.is_empty() {
            return Err(BicDbError::Backup(
                "backup header seal contained unexpected data".to_string(),
            ));
        }
        let mut streaming = Self {
            reader,
            cipher,
            nonce_prefix: outer.nonce_prefix,
            header_hash,
            frame_index: 1,
            chunk_plaintext_bytes,
        };
        let archive_bytes = streaming.read_frame(MAX_BACKUP_HEADER_BYTES)?;
        let archive: BackupArchiveHeader = serde_json::from_slice(&archive_bytes)?;
        if archive.backup_id != outer.backup_id || archive.created_at != outer.created_at {
            return Err(BicDbError::Backup(
                "backup outer and encrypted archive identities differ".to_string(),
            ));
        }
        verify_archive_header(&archive)?;
        Ok((streaming, archive))
    }

    fn read_frame(&mut self, plaintext_limit: usize) -> Result<Vec<u8>> {
        let mut frame_header = [0_u8; STREAMING_FRAME_HEADER_BYTES];
        self.reader.read_exact(&mut frame_header)?;
        let codec = frame_header[0];
        if !matches!(codec, STREAMING_CODEC_RAW | STREAMING_CODEC_ZSTD) {
            return Err(BicDbError::Backup(format!(
                "unsupported streaming backup codec {codec}"
            )));
        }
        let plaintext_len = u32::from_le_bytes(frame_header[1..5].try_into().unwrap());
        let ciphertext_len = u32::from_le_bytes(frame_header[5..9].try_into().unwrap());
        let plaintext_len_usize = plaintext_len as usize;
        let ciphertext_len_usize = ciphertext_len as usize;
        if plaintext_len_usize > plaintext_limit
            || ciphertext_len_usize < AEAD_TAG_BYTES
            || ciphertext_len_usize > plaintext_len_usize.saturating_add(AEAD_TAG_BYTES)
        {
            return Err(BicDbError::Backup(
                "streaming backup frame exceeds its authenticated bounds".to_string(),
            ));
        }
        let mut ciphertext = vec![0_u8; ciphertext_len_usize];
        self.reader.read_exact(&mut ciphertext)?;
        let aad = streaming_frame_aad(&self.header_hash, self.frame_index, codec, plaintext_len);
        let encoded = self
            .cipher
            .decrypt(
                XNonce::from_slice(&streaming_nonce(&self.nonce_prefix, self.frame_index)),
                Payload {
                    msg: &ciphertext,
                    aad: &aad,
                },
            )
            .map_err(|_| BicDbError::Backup("backup chunk authentication failed".to_string()))?;
        self.frame_index = self
            .frame_index
            .checked_add(1)
            .ok_or_else(|| BicDbError::Backup("backup frame counter overflow".to_string()))?;
        decode_streaming_chunk(codec, encoded, plaintext_len_usize)
    }

    fn ensure_eof(&mut self) -> Result<()> {
        let mut trailing = [0_u8; 1];
        match self.reader.read(&mut trailing) {
            Ok(0) => Ok(()),
            Ok(_) => Err(BicDbError::Backup(
                "streaming backup has unexpected trailing bytes".to_string(),
            )),
            Err(error) => Err(error.into()),
        }
    }
}

fn verify_backup_reader_header<R: Read>(
    reader: &mut R,
    passphrase: &str,
) -> Result<BackupArchiveHeader> {
    let mut prefix = [0_u8; 8];
    let mut prefix_len = 0usize;
    while prefix_len < prefix.len() {
        let read = reader.read(&mut prefix[prefix_len..])?;
        if read == 0 {
            break;
        }
        prefix_len += read;
    }
    let prefix_bytes = prefix[..prefix_len].to_vec();
    let mut chained = Cursor::new(prefix_bytes).chain(reader);
    if prefix_len == STREAMING_BACKUP_MAGIC.len() && &prefix == STREAMING_BACKUP_MAGIC {
        let (mut stream, archive) = StreamingArchiveReader::open(&mut chained, passphrase)?;
        consume_streaming_payloads(&mut stream, &archive, |_, _, _| Ok(()))?;
        stream.ensure_eof()?;
        return Ok(archive);
    }
    let mut bytes = Vec::new();
    chained.read_to_end(&mut bytes)?;
    let encrypted = decode_encrypted_backup(&bytes)?;
    let archive = decrypt_archive(&encrypted, passphrase)?;
    verify_archive(&archive)?;
    Ok(archive_header_from_legacy(&archive))
}

fn consume_streaming_payloads<R: Read>(
    stream: &mut StreamingArchiveReader<'_, R>,
    archive: &BackupArchiveHeader,
    mut consume: impl FnMut(&BackupManifestEntry, u64, &[u8]) -> Result<()>,
) -> Result<()> {
    for entry in &archive.files {
        let mut remaining = entry.size;
        let mut offset = 0_u64;
        let mut hasher = Sha256::new();
        while remaining > 0 {
            let expected = remaining.min(stream.chunk_plaintext_bytes as u64) as usize;
            let chunk = stream.read_frame(stream.chunk_plaintext_bytes)?;
            if chunk.len() != expected {
                return Err(BicDbError::Backup(format!(
                    "backup file {} has a non-canonical chunk length",
                    entry.path
                )));
            }
            consume(entry, offset, &chunk)?;
            hasher.update(&chunk);
            offset = offset.saturating_add(chunk.len() as u64);
            remaining -= chunk.len() as u64;
        }
        // Fuzzy (online paged) entries carry a sentinel hash: the frames'
        // AEAD authentication is their integrity check.
        if !entry.fuzzy && hex::encode(hasher.finalize()) != entry.sha256 {
            return Err(BicDbError::Backup(format!(
                "backup file {} checksum mismatch",
                entry.path
            )));
        }
    }
    Ok(())
}

fn restore_streaming_archive<R: Read>(
    reader: &mut R,
    passphrase: &str,
    target_path: &Path,
    expected: &BackupArchiveHeader,
) -> Result<usize> {
    let (mut stream, archive) = StreamingArchiveReader::open(reader, passphrase)?;
    if &archive != expected {
        return Err(BicDbError::Backup(
            "backup changed between verification and restore".to_string(),
        ));
    }
    for removed in &archive.removed_files {
        let path = safe_join(target_path, removed)?;
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() || metadata.is_file() => {
                fs::remove_file(&path)?;
            }
            Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(&path)?,
            Ok(_) => {
                return Err(BicDbError::Backup(format!(
                    "cannot remove unsupported restore target {}",
                    path.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    for entry in &archive.files {
        let path = safe_join(target_path, &entry.path)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut temporary = TemporaryRestoreFile::create(&path)?;
        let mut remaining = entry.size;
        let mut hasher = Sha256::new();
        while remaining > 0 {
            let expected_len = remaining.min(stream.chunk_plaintext_bytes as u64) as usize;
            let chunk = stream.read_frame(stream.chunk_plaintext_bytes)?;
            if chunk.len() != expected_len {
                return Err(BicDbError::Backup(format!(
                    "backup file {} has a non-canonical chunk length",
                    entry.path
                )));
            }
            temporary.file.write_all(&chunk)?;
            hasher.update(&chunk);
            remaining -= chunk.len() as u64;
        }
        if !entry.fuzzy && hex::encode(hasher.finalize()) != entry.sha256 {
            return Err(BicDbError::Backup(format!(
                "backup file {} checksum mismatch",
                entry.path
            )));
        }
        temporary.publish()?;
    }
    stream.ensure_eof()?;
    Ok(archive.files.len())
}

fn verify_report_from_header(archive: &BackupArchiveHeader) -> BackupVerifyReport {
    BackupVerifyReport {
        backup_id: archive.backup_id,
        full: archive.full,
        files_total: archive.manifest.files.len(),
        files_included: archive.files.len(),
        manifest_hash: archive.manifest.manifest_hash.clone(),
        base_backup_id: archive.base_backup_id,
        base_manifest_hash: archive.base_manifest_hash.clone(),
        chain_start_timestamp: archive.recovery.chain_start_timestamp,
        chain_end_timestamp: archive.recovery.chain_end_timestamp,
        archived_events: archive.recovery.archived_events,
        source_format_version: archive.source_format.format_version,
        source_feature_flags: feature_flags(&archive.source_format),
    }
}

fn archive_header_from_legacy(archive: &BackupArchive) -> BackupArchiveHeader {
    BackupArchiveHeader {
        version: archive.version,
        backup_id: archive.backup_id,
        created_at: archive.created_at,
        full: archive.full,
        base_backup_id: archive.base_backup_id,
        base_manifest_hash: archive.base_manifest_hash.clone(),
        manifest: archive.manifest.clone(),
        source_format: archive.source_format.clone(),
        files: archive
            .files
            .iter()
            .map(|file| BackupManifestEntry {
                path: file.path.clone(),
                size: file.size,
                sha256: file.sha256.clone(),
                fuzzy: false,
            })
            .collect(),
        removed_files: Vec::new(),
        recovery: archive.recovery.clone(),
    }
}

fn verify_archive_header(archive: &BackupArchiveHeader) -> Result<()> {
    if !matches!(
        archive.version,
        LEGACY_BACKUP_VERSION | BACKUP_VERSION | STREAMING_BACKUP_VERSION
    ) {
        return Err(BicDbError::Backup(format!(
            "unsupported backup archive version {}",
            archive.version
        )));
    }
    if archive.full != archive.base_backup_id.is_none()
        || archive.full != archive.base_manifest_hash.is_none()
    {
        return Err(BicDbError::Backup(
            "backup base metadata is inconsistent".to_string(),
        ));
    }
    if archive.manifest.files.len() > MAX_BACKUP_FILES
        || archive.files.len() > MAX_BACKUP_FILES
        || archive.removed_files.len() > MAX_BACKUP_FILES
    {
        return Err(BicDbError::Backup(format!(
            "backup metadata exceeds the {MAX_BACKUP_FILES}-file safety limit"
        )));
    }
    if manifest_hash(&archive.manifest.files) != archive.manifest.manifest_hash {
        return Err(BicDbError::Backup(
            "backup manifest checksum mismatch".to_string(),
        ));
    }
    if archive.recovery.protocol.is_empty() {
        return Err(BicDbError::Backup(
            "backup recovery protocol metadata is missing".to_string(),
        ));
    }
    format::check_backup_restore_compatible(&archive.source_format)?;
    let mut manifest = BTreeMap::new();
    for entry in &archive.manifest.files {
        validate_backup_manifest_entry(entry)?;
        if manifest.insert(entry.path.as_str(), entry).is_some() {
            return Err(BicDbError::Backup(format!(
                "backup manifest repeats file {}",
                entry.path
            )));
        }
    }
    let mut included = std::collections::BTreeSet::new();
    for entry in &archive.files {
        validate_backup_manifest_entry(entry)?;
        if !included.insert(entry.path.as_str())
            || manifest.get(entry.path.as_str()) != Some(&entry)
        {
            return Err(BicDbError::Backup(format!(
                "backup file {} is duplicate or does not match its manifest",
                entry.path
            )));
        }
    }
    let mut removed = std::collections::BTreeSet::new();
    for path in &archive.removed_files {
        validate_backup_relative_path(path)?;
        if archive.full
            || manifest.contains_key(path.as_str())
            || included.contains(path.as_str())
            || !removed.insert(path.as_str())
        {
            return Err(BicDbError::Backup(format!(
                "backup removal entry {} is invalid or duplicated",
                path
            )));
        }
    }
    Ok(())
}

fn validate_backup_manifest_entry(entry: &BackupManifestEntry) -> Result<()> {
    validate_backup_relative_path(&entry.path)?;
    if entry.sha256.len() != 64
        || !entry
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(BicDbError::Backup(format!(
            "backup file {} has an invalid SHA-256",
            entry.path
        )));
    }
    Ok(())
}

fn validate_backup_relative_path(relative: &str) -> Result<()> {
    let path = Path::new(relative);
    if relative.is_empty()
        || relative.len() > MAX_BACKUP_PATH_BYTES
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::CurDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        return Err(BicDbError::Backup(format!("unsafe backup path {relative}")));
    }
    Ok(())
}

fn streaming_header_hash(header: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(STREAMING_BACKUP_MAGIC);
    hasher.update((header.len() as u64).to_le_bytes());
    hasher.update(header);
    hasher.finalize().into()
}

fn streaming_nonce(prefix: &[u8; NONCE_PREFIX_LEN], frame_index: u64) -> [u8; NONCE_LEN] {
    let mut nonce = [0_u8; NONCE_LEN];
    nonce[..NONCE_PREFIX_LEN].copy_from_slice(prefix);
    nonce[NONCE_PREFIX_LEN..].copy_from_slice(&frame_index.to_le_bytes());
    nonce
}

fn streaming_frame_aad(
    header_hash: &[u8; 32],
    frame_index: u64,
    codec: u8,
    plaintext_len: u32,
) -> Vec<u8> {
    let mut aad = Vec::with_capacity(32 + 8 + 1 + 4);
    aad.extend_from_slice(header_hash);
    aad.extend_from_slice(&frame_index.to_le_bytes());
    aad.push(codec);
    aad.extend_from_slice(&plaintext_len.to_le_bytes());
    aad
}

#[cfg(feature = "compression")]
fn encode_streaming_chunk(plaintext: &[u8], allow_compression: bool) -> Result<(u8, Vec<u8>)> {
    if allow_compression && !plaintext.is_empty() {
        let compressed = zstd::stream::encode_all(plaintext, 3)?;
        if compressed.len() < plaintext.len() {
            return Ok((STREAMING_CODEC_ZSTD, compressed));
        }
    }
    Ok((STREAMING_CODEC_RAW, plaintext.to_vec()))
}

#[cfg(not(feature = "compression"))]
fn encode_streaming_chunk(plaintext: &[u8], _allow_compression: bool) -> Result<(u8, Vec<u8>)> {
    Ok((STREAMING_CODEC_RAW, plaintext.to_vec()))
}

fn decode_streaming_chunk(codec: u8, encoded: Vec<u8>, expected: usize) -> Result<Vec<u8>> {
    let decoded = match codec {
        STREAMING_CODEC_RAW => encoded,
        #[cfg(feature = "compression")]
        STREAMING_CODEC_ZSTD => {
            let decoder = zstd::stream::Decoder::new(encoded.as_slice())?;
            let mut decoded = Vec::with_capacity(expected);
            decoder
                .take(expected.saturating_add(1) as u64)
                .read_to_end(&mut decoded)?;
            decoded
        }
        #[cfg(not(feature = "compression"))]
        STREAMING_CODEC_ZSTD => {
            return Err(BicDbError::Backup(
                "backup uses zstd compression, but this build has no compression support"
                    .to_string(),
            ));
        }
        value => {
            return Err(BicDbError::Backup(format!(
                "unsupported streaming backup codec {value}"
            )));
        }
    };
    if decoded.len() != expected {
        return Err(BicDbError::Backup(
            "streaming backup frame plaintext length mismatch".to_string(),
        ));
    }
    Ok(decoded)
}

fn read_u64_from_reader(reader: &mut impl Read) -> Result<u64> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_u32_from_reader(reader: &mut impl Read) -> Result<u32> {
    let mut bytes = [0_u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn collect_source_files(root: &Path, exclude: Option<&Path>) -> Result<Vec<BackupSourceFile>> {
    collect_source_files_with_mode(root, exclude, false)
}

/// `online_paged` marks the live paged store's mutable files (page segments,
/// active WAL) as fuzzy — size pinned now, hashed as streamed, never
/// pre-hashed — and orders the copy so every page byte is streamed BEFORE
/// any WAL byte. That ordering plus the caller's backup pin is what makes
/// the chain restore consistent: any page state the copy observes has its
/// WAL record at or before the WAL cut taken after the copy.
fn collect_source_files_with_mode(
    root: &Path,
    exclude: Option<&Path>,
    online_paged: bool,
) -> Result<Vec<BackupSourceFile>> {
    let mut files = Vec::new();
    let mut metadata_bytes = 0_usize;
    collect_source_files_inner(
        root,
        root,
        exclude,
        online_paged,
        &mut files,
        &mut metadata_bytes,
    )?;
    files.sort_by(|left, right| {
        online_copy_order(&left.entry.path)
            .cmp(&online_copy_order(&right.entry.path))
            .then_with(|| left.entry.path.cmp(&right.entry.path))
    });
    Ok(files)
}

/// Copy-order class + numeric index: ordinary files first, page segments
/// ascending, sealed WAL segments ascending, the active WAL dead last.
/// (For non-online archives this degenerates to plain path order within
/// class 0 plus a stable paged-file suffix — harmless.)
fn online_copy_order(relative: &str) -> (u8, u64) {
    if let Some(rest) = relative.strip_prefix("paged/store.pages") {
        if rest.is_empty() {
            return (1, 0);
        }
        if let Some(index) = rest.strip_prefix('.').and_then(|s| s.parse::<u64>().ok()) {
            return (1, index);
        }
    }
    if let Some(rest) = relative.strip_prefix("paged/store.wal") {
        if rest.is_empty() {
            return (3, 0);
        }
        if let Some(sequence) = rest.strip_prefix('.').and_then(|s| s.parse::<u64>().ok()) {
            return (2, sequence);
        }
    }
    (0, 0)
}

/// Files an online paged backup must copy fuzzily: they mutate while the
/// backup streams. Sealed WAL segments (`store.wal.<n>`) are immutable and
/// deliberately NOT here.
fn is_paged_mutable(relative: &str) -> bool {
    matches!(online_copy_order(relative), (1, _) | (3, _))
}

fn collect_source_files_inner(
    root: &Path,
    path: &Path,
    exclude: Option<&Path>,
    online_paged: bool,
    files: &mut Vec<BackupSourceFile>,
    metadata_bytes: &mut usize,
) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if matches!(
            path.file_name().and_then(|name| name.to_str()),
            Some(storage::BACKUP_ONLINE_LOCK | storage::BACKUP_WRITE_GATE)
        ) || exclude.is_some_and(|exclude| {
            paths_identical(&path, exclude)
                || paths_identical(&path, &exclude.with_extension("tmp"))
        }) {
            continue;
        }
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(BicDbError::Backup(format!(
                "backup source contains unsupported symbolic link {}",
                path.display()
            )));
        }
        if file_type.is_dir() {
            collect_source_files_inner(root, &path, exclude, online_paged, files, metadata_bytes)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .map_err(|error| BicDbError::Backup(error.to_string()))?
            .to_string_lossy()
            .replace('\\', "/");
        validate_backup_relative_path(&relative)?;
        // The active WAL (`paged/store.wal`, copy-order group 3) rotates
        // under append at `wal_segment_bytes` regardless of the backup pin:
        // the pin blocks checkpoint truncation and sealed-segment deletion,
        // NOT append-time rotation (wal.rs `append` -> `seal_active_locked`).
        // Copying it fuzzily by pinned-size-and-pathname therefore aborts the
        // whole backup the moment a rotation renames the active file
        // mid-stream and the new active is shorter than the pinned size (the
        // observed 48 MiB -> 30 MiB production abort -- safe, but no snapshot).
        // Its content is captured durably and completely by the post-cut
        // WAL-tail seal (`seal_wal_for_backup` returns EVERY sealed segment),
        // and a restored base with no active WAL opens with a fresh empty one
        // (`Wal::open` uses `create(true)`) that replays that sealed chain --
        // so the base's active-WAL copy is pure, rotation-fragile redundancy.
        if online_paged && matches!(online_copy_order(&relative), (3, _)) {
            continue;
        }
        if relative.len() > MAX_BACKUP_PATH_BYTES {
            return Err(BicDbError::Backup(format!(
                "backup path exceeds {MAX_BACKUP_PATH_BYTES} bytes"
            )));
        }
        if files.len() >= MAX_BACKUP_FILES {
            return Err(BicDbError::Backup(format!(
                "backup contains more than {MAX_BACKUP_FILES} files"
            )));
        }
        let entry_charge = relative
            .len()
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(BACKUP_SOURCE_ENTRY_MEMORY_CHARGE))
            .ok_or_else(|| BicDbError::Backup("backup manifest size overflow".to_string()))?;
        *metadata_bytes = metadata_bytes
            .checked_add(entry_charge)
            .ok_or_else(|| BicDbError::Backup("backup manifest size overflow".to_string()))?;
        if *metadata_bytes > MAX_BACKUP_MANIFEST_MEMORY_BYTES {
            return Err(BicDbError::Backup(format!(
                "backup manifest exceeds the {MAX_BACKUP_MANIFEST_MEMORY_BYTES}-byte in-memory safety limit"
            )));
        }
        let fuzzy = online_paged && is_paged_mutable(&relative);
        let (size, sha256) = if fuzzy {
            // Size pinned now; the bytes are hashed as they stream. Skipping
            // the pre-hash also skips a full read pass over the store.
            let size = fs::metadata(&path)?.len();
            (size, "0".repeat(64))
        } else {
            hash_file(&path)?
        };
        files.push(BackupSourceFile {
            entry: BackupManifestEntry {
                path: relative,
                size,
                sha256,
                fuzzy,
            },
            source_path: path,
        });
    }
    Ok(())
}

fn hash_file(path: &Path) -> Result<(u64, String)> {
    let mut reader = BufReader::with_capacity(FILE_HASH_BUFFER_BYTES, File::open(path)?);
    let mut buffer = vec![0_u8; FILE_HASH_BUFFER_BYTES];
    let mut hasher = Sha256::new();
    let mut size = 0_u64;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size = size
            .checked_add(read as u64)
            .ok_or_else(|| BicDbError::Backup("source file size overflow".to_string()))?;
    }
    Ok((size, hex::encode(hasher.finalize())))
}

fn paths_identical(left: &Path, right: &Path) -> bool {
    left == right
        || left
            .canonicalize()
            .ok()
            .zip(right.canonicalize().ok())
            .is_some_and(|(left, right)| left == right)
}

struct TemporaryBackupFile {
    path: PathBuf,
    file: Option<File>,
    armed: bool,
}

impl TemporaryBackupFile {
    fn create(path: &Path) -> Result<Self> {
        Ok(Self {
            path: path.to_path_buf(),
            file: Some(create_new_private(path)?),
            armed: true,
        })
    }

    fn take_file(&mut self) -> File {
        self.file.take().expect("temporary backup file is present")
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for TemporaryBackupFile {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

struct TemporaryRestoreFile {
    target: PathBuf,
    temporary: PathBuf,
    file: File,
    published: bool,
}

impl TemporaryRestoreFile {
    fn create(target: &Path) -> Result<Self> {
        let temporary = randomized_sibling(target, "restore");
        Ok(Self {
            target: target.to_path_buf(),
            file: create_new_private(&temporary)?,
            temporary,
            published: false,
        })
    }

    fn publish(mut self) -> Result<()> {
        self.file.flush()?;
        self.file.sync_all()?;
        fs::rename(&self.temporary, &self.target)?;
        sync_parent_directory(&self.target)?;
        self.published = true;
        Ok(())
    }
}

impl Drop for TemporaryRestoreFile {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.temporary);
        }
    }
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<()> {
    // Windows does not permit opening a directory as an ordinary File. The
    // file itself is flushed before rename; directory-entry durability follows
    // the platform rename contract until a native directory handle is added.
    Ok(())
}

#[derive(Debug)]
struct OnlineBackupCheckpoint {
    path: PathBuf,
    lock_file: Option<File>,
    snapshot_gate: Option<storage::BackupGateGuard>,
}

impl OnlineBackupCheckpoint {
    fn begin(db_path: &Path) -> Result<Self> {
        let path = db_path.join(storage::BACKUP_ONLINE_LOCK);
        let lock_file = open_backup_lock_file(&path)?;
        let mut checkpoint = Self {
            path,
            lock_file: Some(lock_file),
            snapshot_gate: None,
        };
        let lock_file = checkpoint.lock_file.as_mut().ok_or_else(|| {
            BicDbError::Backup("online backup checkpoint lost its lock handle".to_string())
        })?;
        lock_file.set_len(0)?;
        lock_file.write_all(unix_timestamp().to_string().as_bytes())?;
        lock_file.sync_data()?;
        // Flush so the copy captures the in-memory tail of the segment engine.
        // If another handle already holds the database, there is no flush to be
        // had — see `already_open`. Proceeding is correct rather than merely
        // convenient: the segment log and the page WAL are both append-only and
        // recover their own tails, which is what makes an online copy restorable.
        match BicDb::open_with_config(db_path, config_for(db_path)?) {
            Ok(db) => db.flush()?,
            Err(error) if already_open(&error) => {}
            Err(error) => return Err(error),
        }
        Ok(checkpoint)
    }

    fn begin_open_database(db_path: &Path) -> Result<Self> {
        let path = db_path.join(storage::BACKUP_ONLINE_LOCK);
        let mut lock_file = open_backup_lock_file(&path)?;
        lock_file.set_len(0)?;
        lock_file.write_all(unix_timestamp().to_string().as_bytes())?;
        lock_file.sync_data()?;
        Ok(Self {
            path,
            lock_file: Some(lock_file),
            snapshot_gate: None,
        })
    }

    fn acquire_snapshot_gate(&mut self, db_path: &Path) -> Result<()> {
        if self.snapshot_gate.is_some() {
            return Err(BicDbError::Backup(
                "online backup snapshot gate was acquired twice".to_string(),
            ));
        }
        self.snapshot_gate = Some(storage::acquire_database_backup_exclusive_guard(db_path)?);
        Ok(())
    }
}

impl Drop for OnlineBackupCheckpoint {
    fn drop(&mut self) {
        drop(self.snapshot_gate.take());
        drop(self.lock_file.take());
        let _ = fs::remove_file(&self.path);
    }
}

#[cfg(unix)]
fn open_backup_lock_file(path: &Path) -> Result<File> {
    use std::os::fd::AsRawFd;

    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        return Err(BicDbError::Backup(format!(
            "another online backup owns {}: {error}",
            path.display()
        )));
    }
    Ok(file)
}

#[cfg(windows)]
fn open_backup_lock_file(path: &Path) -> Result<File> {
    use std::os::windows::fs::OpenOptionsExt;

    OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .share_mode(0)
        .open(path)
        .map_err(|error| {
            BicDbError::Backup(format!(
                "another online backup owns {}: {error}",
                path.display()
            ))
        })
}

#[cfg(not(any(unix, windows)))]
fn open_backup_lock_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)
        .map_err(Into::into)
}

fn recovery_metadata(db_path: &Path) -> Result<BackupRecoveryMetadata> {
    // Event timestamps bound the recovery chain. They are read by opening the
    // database, which an online backup of a paged database cannot do; the chain
    // bounds are then genuinely unknown, and `None` says so. Reporting a
    // fabricated or partial range would be worse than reporting none, because
    // the RPO figure in a drill report is derived from it.
    let (chain_start_timestamp, chain_end_timestamp, archived_events) =
        match BicDb::open_with_config(db_path, config_for(db_path)?) {
            Ok(db) => db.backup_event_bounds(),
            Err(error) if already_open(&error) => (None, None, 0),
            Err(error) => return Err(error),
        };
    Ok(BackupRecoveryMetadata {
        protocol: "online-checkpoint-plus-audit-event-archive".to_string(),
        checkpoint_timestamp: unix_timestamp(),
        chain_start_timestamp,
        chain_end_timestamp,
        archived_events,
    })
}

fn decrypt_archive(encrypted: &EncryptedBackup, passphrase: &str) -> Result<BackupArchive> {
    if !matches!(encrypted.version, LEGACY_BACKUP_VERSION | BACKUP_VERSION) {
        return Err(BicDbError::Backup(format!(
            "unsupported backup version {}",
            encrypted.version
        )));
    }
    let checksum = hex::encode(Sha256::digest(&encrypted.ciphertext));
    if checksum != encrypted.ciphertext_sha256 {
        return Err(BicDbError::Backup(
            "encrypted backup checksum mismatch".to_string(),
        ));
    }
    let key = derive_key_with_kdf(passphrase, &encrypted.kdf)?;
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&key));
    let plaintext = cipher
        .decrypt(
            XNonce::from_slice(&encrypted.nonce),
            encrypted.ciphertext.as_ref(),
        )
        .map_err(|_| BicDbError::Backup("backup decryption failed".to_string()))?;
    let plaintext = decompress_archive(&encrypted.compression, plaintext)?;
    let archive = if encrypted.version == LEGACY_BACKUP_VERSION {
        serde_json::from_slice(&plaintext)?
    } else {
        decode_archive(&plaintext)?
    };
    Ok(archive)
}

fn decode_encrypted_backup(bytes: &[u8]) -> Result<EncryptedBackup> {
    if !bytes.starts_with(BACKUP_MAGIC) {
        let legacy: EncryptedBackup = serde_json::from_slice(bytes)?;
        if legacy.version != LEGACY_BACKUP_VERSION {
            return Err(BicDbError::Backup(format!(
                "unsupported JSON backup version {}",
                legacy.version
            )));
        }
        return Ok(legacy);
    }
    let header_len = read_u64(bytes, BACKUP_MAGIC.len())?;
    let header_len = usize::try_from(header_len)
        .map_err(|_| BicDbError::Backup("backup header length is too large".to_string()))?;
    if header_len > MAX_BACKUP_HEADER_BYTES {
        return Err(BicDbError::Backup(
            "backup header exceeds the safety limit".to_string(),
        ));
    }
    let header_start = BACKUP_MAGIC.len() + 8;
    let header_end = header_start
        .checked_add(header_len)
        .ok_or_else(|| BicDbError::Backup("backup header length overflow".to_string()))?;
    let header_bytes = bytes
        .get(header_start..header_end)
        .ok_or_else(|| BicDbError::Backup("backup header is truncated".to_string()))?;
    let header: EncryptedBackupHeader = serde_json::from_slice(header_bytes)?;
    if header.version != BACKUP_VERSION {
        return Err(BicDbError::Backup(format!(
            "unsupported binary backup version {}",
            header.version
        )));
    }
    let ciphertext = bytes
        .get(header_end..)
        .ok_or_else(|| BicDbError::Backup("backup ciphertext is missing".to_string()))?;
    if ciphertext.len() as u64 != header.ciphertext_len {
        return Err(BicDbError::Backup(
            "backup ciphertext length mismatch".to_string(),
        ));
    }
    Ok(EncryptedBackup {
        version: header.version,
        backup_id: header.backup_id,
        created_at: header.created_at,
        kdf: header.kdf,
        nonce: header.nonce,
        compression: header.compression,
        ciphertext_sha256: header.ciphertext_sha256,
        ciphertext: ciphertext.to_vec(),
    })
}

fn decode_archive(bytes: &[u8]) -> Result<BackupArchive> {
    let header_len = usize::try_from(read_u64(bytes, 0)?)
        .map_err(|_| BicDbError::Backup("archive header length is too large".to_string()))?;
    if header_len > MAX_BACKUP_HEADER_BYTES {
        return Err(BicDbError::Backup(
            "archive header exceeds the safety limit".to_string(),
        ));
    }
    let header_end = 8usize
        .checked_add(header_len)
        .ok_or_else(|| BicDbError::Backup("archive header length overflow".to_string()))?;
    let header_bytes = bytes
        .get(8..header_end)
        .ok_or_else(|| BicDbError::Backup("archive header is truncated".to_string()))?;
    let header: BackupArchiveHeader = serde_json::from_slice(header_bytes)?;
    if header.version != BACKUP_VERSION {
        return Err(BicDbError::Backup(format!(
            "unsupported archive version {}",
            header.version
        )));
    }
    let mut offset = header_end;
    let mut files = Vec::with_capacity(header.files.len());
    for entry in header.files {
        let size = usize::try_from(entry.size)
            .map_err(|_| BicDbError::Backup("archive file is too large".to_string()))?;
        let end = offset
            .checked_add(size)
            .ok_or_else(|| BicDbError::Backup("archive file length overflow".to_string()))?;
        let file_bytes = bytes
            .get(offset..end)
            .ok_or_else(|| BicDbError::Backup("archive file is truncated".to_string()))?;
        files.push(BackupFile {
            path: entry.path,
            size: entry.size,
            sha256: entry.sha256,
            bytes: file_bytes.to_vec(),
        });
        offset = end;
    }
    if offset != bytes.len() {
        return Err(BicDbError::Backup(
            "archive has unexpected trailing bytes".to_string(),
        ));
    }
    Ok(BackupArchive {
        version: header.version,
        backup_id: header.backup_id,
        created_at: header.created_at,
        full: header.full,
        base_backup_id: header.base_backup_id,
        base_manifest_hash: header.base_manifest_hash,
        manifest: header.manifest,
        source_format: header.source_format,
        files,
        recovery: header.recovery,
    })
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| BicDbError::Backup("backup length prefix is truncated".to_string()))?;
    Ok(u64::from_le_bytes(value.try_into().map_err(|_| {
        BicDbError::Backup("invalid backup length prefix".to_string())
    })?))
}

fn decompress_archive(compression: &Option<String>, plaintext: Vec<u8>) -> Result<Vec<u8>> {
    match compression.as_deref() {
        None => Ok(plaintext),
        #[cfg(feature = "compression")]
        Some("zstd") => Ok(zstd::stream::decode_all(plaintext.as_slice())?),
        #[cfg(not(feature = "compression"))]
        Some("zstd") => Err(BicDbError::Backup(
            "backup uses zstd compression, but this build has no compression support".to_string(),
        )),
        Some(value) => Err(BicDbError::Backup(format!(
            "unsupported backup compression {value}"
        ))),
    }
}

#[cfg(test)]
fn derive_key(passphrase: &str, salt: &[u8; SALT_LEN]) -> Result<[u8; KEY_LEN]> {
    derive_key_with_params(
        passphrase,
        salt,
        ARGON2_MEMORY_KIB,
        ARGON2_TIME_COST,
        ARGON2_PARALLELISM,
    )
}

fn derive_key_with_kdf(passphrase: &str, kdf: &BackupKdf) -> Result<[u8; KEY_LEN]> {
    if kdf.algorithm != "argon2id" {
        return Err(BicDbError::Backup(format!(
            "unsupported backup KDF {}",
            kdf.algorithm
        )));
    }
    if kdf.memory_kib != ARGON2_MEMORY_KIB
        || kdf.time_cost != ARGON2_TIME_COST
        || kdf.parallelism != ARGON2_PARALLELISM
    {
        return Err(BicDbError::Backup(
            "unsupported or unsafe backup KDF parameters".to_string(),
        ));
    }
    derive_key_with_params(
        passphrase,
        &kdf.salt,
        kdf.memory_kib,
        kdf.time_cost,
        kdf.parallelism,
    )
}

fn derive_key_with_params(
    passphrase: &str,
    salt: &[u8; SALT_LEN],
    memory_kib: u32,
    time_cost: u32,
    parallelism: u32,
) -> Result<[u8; KEY_LEN]> {
    let params = Params::new(memory_kib, time_cost, parallelism, Some(KEY_LEN))
        .map_err(|error| BicDbError::Backup(error.to_string()))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key = [0_u8; KEY_LEN];
    argon2
        .hash_password_into(passphrase.as_bytes(), salt, &mut key)
        .map_err(|error| BicDbError::Backup(error.to_string()))?;
    Ok(key)
}

fn fill_random(bytes: &mut [u8]) {
    use chacha20poly1305::aead::rand_core::RngCore;
    OsRng.fill_bytes(bytes);
}

fn verify_archive(archive: &BackupArchive) -> Result<()> {
    if !matches!(archive.version, LEGACY_BACKUP_VERSION | BACKUP_VERSION) {
        return Err(BicDbError::Backup(format!(
            "unsupported backup archive version {}",
            archive.version
        )));
    }
    let manifest_hash = manifest_hash(&archive.manifest.files);
    if manifest_hash != archive.manifest.manifest_hash {
        return Err(BicDbError::Backup(
            "backup manifest checksum mismatch".to_string(),
        ));
    }
    if archive.recovery.protocol.is_empty() {
        return Err(BicDbError::Backup(
            "backup recovery protocol metadata is missing".to_string(),
        ));
    }
    format::check_backup_restore_compatible(&archive.source_format)?;
    let manifest = archive
        .manifest
        .files
        .iter()
        .map(|entry| (&entry.path, entry))
        .collect::<BTreeMap<_, _>>();
    for file in &archive.files {
        let entry = manifest.get(&file.path).ok_or_else(|| {
            BicDbError::Backup(format!("backup file {} missing from manifest", file.path))
        })?;
        if entry.size != file.size || entry.sha256 != file.sha256 {
            return Err(BicDbError::Backup(format!(
                "backup file {} does not match manifest",
                file.path
            )));
        }
        let sha256 = hex::encode(Sha256::digest(&file.bytes));
        if sha256 != file.sha256 {
            return Err(BicDbError::Backup(format!(
                "backup file {} checksum mismatch",
                file.path
            )));
        }
    }
    Ok(())
}

fn legacy_source_format() -> FormatMetadata {
    format::FormatMetadata {
        format_version: 1,
        min_reader_version: 1,
        min_writer_version: 1,
        feature_flags: Default::default(),
        // A backup with no recorded format metadata predates storage modes.
        storage_mode: Default::default(),
    }
}

fn feature_flags(metadata: &FormatMetadata) -> Vec<String> {
    metadata.feature_flags.iter().cloned().collect()
}

fn verify_restored_manifest(root: &Path, manifest: &BackupManifest) -> Result<()> {
    for entry in &manifest.files {
        let path = safe_join(root, &entry.path)?;
        if !path.exists() {
            return Err(BicDbError::Backup(format!(
                "restored target is missing {}; apply the base backup first for incremental restores",
                entry.path
            )));
        }
        if entry.fuzzy {
            // Size is the fuzzy contract; the bytes were authenticated
            // frame-by-frame and consistency comes from WAL replay on open.
            let size = fs::metadata(&path)?.len();
            if size != entry.size {
                return Err(BicDbError::Backup(format!(
                    "restored file {} failed size verification",
                    entry.path
                )));
            }
            continue;
        }
        let (size, sha256) = hash_file(&path)?;
        if size != entry.size || sha256 != entry.sha256 {
            return Err(BicDbError::Backup(format!(
                "restored file {} failed verification",
                entry.path
            )));
        }
    }
    Ok(())
}

fn manifest_hash(entries: &[BackupManifestEntry]) -> String {
    let mut hasher = Sha256::new();
    for entry in entries {
        hash_str(&mut hasher, &entry.path);
        hasher.update(entry.size.to_be_bytes());
        hash_str(&mut hasher, &entry.sha256);
    }
    hex::encode(hasher.finalize())
}

fn hash_str(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn safe_join(root: &Path, relative: &str) -> Result<PathBuf> {
    validate_backup_relative_path(relative)?;
    if fs::symlink_metadata(root)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        return Err(BicDbError::Backup(format!(
            "restore root {} is a symbolic link",
            root.display()
        )));
    }
    let mut joined = root.to_path_buf();
    for component in Path::new(relative).components() {
        let std::path::Component::Normal(component) = component else {
            return Err(BicDbError::Backup(format!("unsafe backup path {relative}")));
        };
        joined.push(component);
        if fs::symlink_metadata(&joined)
            .map(|metadata| metadata.file_type().is_symlink())
            .unwrap_or(false)
        {
            return Err(BicDbError::Backup(format!(
                "restore path {} traverses a symbolic link",
                joined.display()
            )));
        }
    }
    Ok(joined)
}

fn target_has_entries(path: &Path) -> Result<bool> {
    Ok(path.exists() && fs::read_dir(path)?.next().transpose()?.is_some())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = randomized_sibling(path, "atomic");
    let result = (|| -> Result<()> {
        let mut file = create_new_private(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, path)?;
        sync_parent_directory(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn randomized_sibling(path: &Path, purpose: &str) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("bicdb");
    path.with_file_name(format!(".{name}.{purpose}.{}.tmp", Uuid::new_v4().simple()))
}

fn create_new_private(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW).mode(0o600);
    Ok(options.open(path)?)
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_KEY: &str = "streaming-backup-test-key";

    fn streaming_fixture(payload_bytes: usize) -> Vec<u8> {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
        db.create_collection("items").unwrap();
        db.insert("items", crate::Record::new("item-1")).unwrap();
        db.close().unwrap();
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let payload = (0..payload_bytes)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        fs::write(root.path().join("opaque.bin"), payload).unwrap();
        let mut backup = Vec::new();
        create_backup_to_writer(
            root.path(),
            &mut backup,
            BackupCreateOptions {
                passphrase: TEST_KEY.to_string(),
                base_backup: None,
            },
        )
        .unwrap();
        backup
    }

    fn frame_end(bytes: &[u8], start: usize) -> usize {
        let ciphertext_len =
            u32::from_le_bytes(bytes[start + 5..start + 9].try_into().unwrap()) as usize;
        start + STREAMING_FRAME_HEADER_BYTES + ciphertext_len
    }

    fn first_data_frame(bytes: &[u8]) -> usize {
        let outer_len = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        let seal_start = 16 + outer_len;
        let seal_len =
            u32::from_le_bytes(bytes[seal_start..seal_start + 4].try_into().unwrap()) as usize;
        let archive_frame = seal_start + 4 + seal_len;
        frame_end(bytes, archive_frame)
    }

    struct BoundedRead<R> {
        inner: R,
        max_request: usize,
        largest_request: usize,
    }

    impl<R: Read> Read for BoundedRead<R> {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            self.largest_request = self.largest_request.max(buffer.len());
            if buffer.len() > self.max_request {
                return Err(std::io::Error::other(format!(
                    "reader requested {} bytes; limit is {}",
                    buffer.len(),
                    self.max_request
                )));
            }
            self.inner.read(buffer)
        }
    }

    #[test]
    fn streaming_verification_never_requests_the_whole_archive() {
        let backup = streaming_fixture(6 * STREAMING_BACKUP_CHUNK_BYTES + 137);
        let max_request = STREAMING_BACKUP_CHUNK_BYTES + AEAD_TAG_BYTES;
        let mut reader = BoundedRead {
            inner: Cursor::new(backup),
            max_request,
            largest_request: 0,
        };
        verify_backup_from_reader(&mut reader, TEST_KEY).unwrap();
        assert!(reader.largest_request <= max_request);
    }

    #[test]
    fn pitr_limits_reject_unbounded_or_zero_batches() {
        assert!(BackupPitrReplayLimits::default().validate().is_ok());
        assert!(BackupPitrReplayLimits {
            max_records_per_batch: 0,
            ..BackupPitrReplayLimits::default()
        }
        .validate()
        .is_err());
        assert!(BackupPitrReplayLimits {
            max_event_bytes_per_batch: 256 * 1024 * 1024 + 1,
            ..BackupPitrReplayLimits::default()
        }
        .validate()
        .is_err());
    }

    #[test]
    fn streaming_frames_are_order_authenticated() {
        let backup = streaming_fixture(3 * STREAMING_BACKUP_CHUNK_BYTES);
        let first = first_data_frame(&backup);
        let second = frame_end(&backup, first);
        let third = frame_end(&backup, second);
        assert!(third <= backup.len());
        let mut reordered = Vec::with_capacity(backup.len());
        reordered.extend_from_slice(&backup[..first]);
        reordered.extend_from_slice(&backup[second..third]);
        reordered.extend_from_slice(&backup[first..second]);
        reordered.extend_from_slice(&backup[third..]);
        let error = verify_backup_from_reader(&mut Cursor::new(reordered), TEST_KEY).unwrap_err();
        assert!(error.to_string().contains("authentication"), "{error}");
    }

    #[test]
    fn hostile_kdf_parameters_fail_before_key_derivation() {
        let backup = streaming_fixture(1);
        let outer_len = u64::from_le_bytes(backup[8..16].try_into().unwrap()) as usize;
        let mut outer: serde_json::Value =
            serde_json::from_slice(&backup[16..16 + outer_len]).unwrap();
        outer["kdf"]["memory_kib"] = serde_json::json!(u32::MAX);
        let tampered_outer = serde_json::to_vec(&outer).unwrap();
        let mut tampered = Vec::new();
        tampered.extend_from_slice(STREAMING_BACKUP_MAGIC);
        tampered.extend_from_slice(&(tampered_outer.len() as u64).to_le_bytes());
        tampered.extend_from_slice(&tampered_outer);
        tampered.extend_from_slice(&backup[16 + outer_len..]);
        let error = verify_backup_from_reader(&mut Cursor::new(tampered), TEST_KEY).unwrap_err();
        assert!(error.to_string().contains("unsafe backup KDF"), "{error}");
    }

    #[test]
    fn online_backup_lock_has_one_owner_and_is_removed_on_release() {
        let root = tempfile::tempdir().unwrap();
        drop(BicDb::open(root.path()).unwrap());
        let first = OnlineBackupCheckpoint::begin(root.path()).unwrap();
        let lock = root.path().join(storage::BACKUP_ONLINE_LOCK);
        assert!(lock.exists());

        let error = OnlineBackupCheckpoint::begin(root.path()).unwrap_err();
        assert!(
            error.to_string().contains("another online backup"),
            "{error}"
        );
        assert!(lock.exists());

        drop(first);
        assert!(!lock.exists());
        drop(OnlineBackupCheckpoint::begin(root.path()).unwrap());
        assert!(!lock.exists());
    }

    #[cfg(unix)]
    #[test]
    fn backup_rejects_source_and_restore_target_symbolic_links() {
        use std::os::unix::fs::symlink;

        let source = tempfile::tempdir().unwrap();
        let external = tempfile::tempdir().unwrap();
        fs::write(external.path().join("secret"), b"not part of the database").unwrap();
        symlink(external.path().join("secret"), source.path().join("escape")).unwrap();

        let mut bytes = Vec::new();
        let error = create_backup_to_writer(
            source.path(),
            &mut bytes,
            BackupCreateOptions {
                passphrase: TEST_KEY.to_string(),
                base_backup: None,
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("symbolic link"), "{error}");

        fs::remove_file(source.path().join("escape")).unwrap();
        bytes.clear();
        create_backup_to_writer(
            source.path(),
            &mut bytes,
            BackupCreateOptions {
                passphrase: TEST_KEY.to_string(),
                base_backup: None,
            },
        )
        .unwrap();
        let archive_dir = tempfile::tempdir().unwrap();
        let archive = archive_dir.path().join("backup.bicbackup");
        fs::write(&archive, &bytes).unwrap();
        let restore_parent = tempfile::tempdir().unwrap();
        let restore_link = restore_parent.path().join("restore");
        symlink(external.path(), &restore_link).unwrap();
        let error = restore_backup(
            &archive,
            &restore_link,
            BackupRestoreOptions {
                passphrase: TEST_KEY.to_string(),
                force: true,
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("symbolic link"), "{error}");
        assert!(external.path().join("secret").exists());
    }

    #[test]
    fn legacy_v1_json_backup_remains_readable() {
        let backup_id = Uuid::new_v4();
        let archive = BackupArchive {
            version: LEGACY_BACKUP_VERSION,
            backup_id,
            created_at: 1,
            full: true,
            base_backup_id: None,
            base_manifest_hash: None,
            manifest: BackupManifest {
                manifest_hash: manifest_hash(&[]),
                files: Vec::new(),
            },
            source_format: legacy_source_format(),
            files: Vec::new(),
            recovery: BackupRecoveryMetadata::default(),
        };
        let salt = [3_u8; SALT_LEN];
        let nonce = [7_u8; NONCE_LEN];
        let key = derive_key("legacy-key", &salt).unwrap();
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&key));
        let ciphertext = cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                serde_json::to_vec(&archive).unwrap().as_ref(),
            )
            .unwrap();
        let legacy = EncryptedBackup {
            version: LEGACY_BACKUP_VERSION,
            backup_id,
            created_at: 1,
            kdf: BackupKdf {
                algorithm: "argon2id".to_string(),
                memory_kib: ARGON2_MEMORY_KIB,
                time_cost: ARGON2_TIME_COST,
                parallelism: ARGON2_PARALLELISM,
                salt,
            },
            nonce,
            compression: None,
            ciphertext_sha256: hex::encode(Sha256::digest(&ciphertext)),
            ciphertext,
        };

        let bytes = serde_json::to_vec_pretty(&legacy).unwrap();
        let encrypted = decode_encrypted_backup(&bytes).unwrap();
        let decoded = decrypt_archive(&encrypted, "legacy-key").unwrap();
        verify_archive(&decoded).unwrap();
        assert_eq!(decoded.backup_id, backup_id);
    }

    #[test]
    fn binary_v2_backup_remains_readable() {
        #[derive(Serialize)]
        struct HistoricalArchiveHeader<'a> {
            version: u32,
            backup_id: Uuid,
            created_at: i64,
            full: bool,
            base_backup_id: Option<Uuid>,
            base_manifest_hash: Option<&'a str>,
            manifest: &'a BackupManifest,
            source_format: &'a FormatMetadata,
            files: &'a [BackupManifestEntry],
            recovery: &'a BackupRecoveryMetadata,
        }

        let backup_id = Uuid::new_v4();
        let manifest = BackupManifest {
            manifest_hash: manifest_hash(&[]),
            files: Vec::new(),
        };
        let source_format = legacy_source_format();
        let recovery = BackupRecoveryMetadata::default();
        let archive_header = HistoricalArchiveHeader {
            version: BACKUP_VERSION,
            backup_id,
            created_at: 2,
            full: true,
            base_backup_id: None,
            base_manifest_hash: None,
            manifest: &manifest,
            source_format: &source_format,
            files: &[],
            recovery: &recovery,
        };
        let archive_header = serde_json::to_vec(&archive_header).unwrap();
        let mut plaintext = Vec::new();
        plaintext.extend_from_slice(&(archive_header.len() as u64).to_le_bytes());
        plaintext.extend_from_slice(&archive_header);

        let salt = [11_u8; SALT_LEN];
        let nonce = [13_u8; NONCE_LEN];
        let kdf = BackupKdf {
            algorithm: "argon2id".to_string(),
            memory_kib: ARGON2_MEMORY_KIB,
            time_cost: ARGON2_TIME_COST,
            parallelism: ARGON2_PARALLELISM,
            salt,
        };
        let key = derive_key(TEST_KEY, &salt).unwrap();
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&key));
        let ciphertext = cipher
            .encrypt(XNonce::from_slice(&nonce), plaintext.as_slice())
            .unwrap();
        let encrypted_header = EncryptedBackupHeader {
            version: BACKUP_VERSION,
            backup_id,
            created_at: 2,
            kdf,
            nonce,
            compression: None,
            ciphertext_sha256: hex::encode(Sha256::digest(&ciphertext)),
            ciphertext_len: ciphertext.len() as u64,
        };
        let encrypted_header = serde_json::to_vec(&encrypted_header).unwrap();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(BACKUP_MAGIC);
        bytes.extend_from_slice(&(encrypted_header.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&encrypted_header);
        bytes.extend_from_slice(&ciphertext);

        let report = verify_backup_from_reader(&mut Cursor::new(bytes), TEST_KEY).unwrap();
        assert_eq!(report.backup_id, backup_id);
        assert!(report.full);
    }
}
