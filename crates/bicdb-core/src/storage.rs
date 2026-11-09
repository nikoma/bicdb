use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crc32fast::Hasher;
#[cfg(feature = "mmap")]
use memmap2::MmapOptions;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::encryption::{self, EncryptionObjectPurpose, EncryptionRuntime};
use crate::error::{BicDbError, Result};

const MAGIC: &[u8; 4] = b"BICF";
const VERSION: u8 = 1;
pub(crate) const HEADER_LEN: usize = 16;
const MAX_PAYLOAD_LEN: usize = 128 * 1024 * 1024;
const MAX_DECOMPRESSION_RATIO: usize = 1_024;
const FLAG_ZSTD: u16 = 1;
const FLAG_ENCRYPTED: u16 = 1 << 1;
const KNOWN_FLAGS: u16 = FLAG_ZSTD | FLAG_ENCRYPTED;
pub(crate) const BACKUP_ONLINE_LOCK: &str = ".backup-online.lock";
/// Persistent coordination inode shared by physical writers and backups.
///
/// `BACKUP_ONLINE_LOCK` remains the one-owner marker for backup callers. This
/// separate file is never removed: replacing or unlinking a locked inode would
/// let a new opener lock a different inode and recreate the TOCTOU this gate
/// exists to close.
pub(crate) const BACKUP_WRITE_GATE: &str = ".backup-write.lock";
const BACKUP_LOCK_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub(crate) struct BackupGateGuard {
    _file: File,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AppendSyncMode {
    None,
    ExplicitDataSync,
    OpenDataSync,
}

#[cfg(test)]
static APPEND_EXPLICIT_DATA_SYNC_CALLS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static APPEND_OPEN_DATA_SYNC_CALLS: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompressionConfig {
    pub enabled: bool,
    pub level: i32,
    pub min_bytes: usize,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            level: 3,
            min_bytes: 8 * 1024,
        }
    }
}

impl CompressionConfig {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn zstd(level: i32, min_bytes: usize) -> Self {
        Self {
            enabled: true,
            level,
            min_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SegmentReadMode {
    #[default]
    Buffered,
    Mmap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FrameKind {
    Record = 1,
    Sync = 2,
    Event = 3,
    Transaction = 4,
    Index = 5,
    Search = 6,
}

impl FrameKind {
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Record),
            2 => Some(Self::Sync),
            3 => Some(Self::Event),
            4 => Some(Self::Transaction),
            5 => Some(Self::Index),
            6 => Some(Self::Search),
            _ => None,
        }
    }

    fn encryption_purpose(self) -> EncryptionObjectPurpose {
        match self {
            Self::Record => EncryptionObjectPurpose::Data,
            Self::Sync => EncryptionObjectPurpose::Replication,
            Self::Event => EncryptionObjectPurpose::Audit,
            Self::Transaction => EncryptionObjectPurpose::Wal,
            Self::Index => EncryptionObjectPurpose::Index,
            Self::Search => EncryptionObjectPurpose::Search,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RecoveredFrames {
    pub frames: Vec<RecoveredFrame>,
    pub truncated_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct RecoveredFrame {
    pub offset: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FrameVerification {
    pub frame_count: usize,
    pub encrypted_frames: usize,
    pub unencrypted_frames: usize,
    pub bytes_checked: u64,
}

pub(crate) fn append_frame(
    path: &Path,
    kind: FrameKind,
    payload: &[u8],
    fsync: bool,
    encryption: &EncryptionRuntime,
) -> Result<u64> {
    let offsets = append_frames(
        path,
        kind,
        &[payload.to_vec()],
        fsync,
        &CompressionConfig::disabled(),
        encryption,
    )?;
    Ok(offsets[0])
}

pub(crate) fn append_frames(
    path: &Path,
    kind: FrameKind,
    payloads: &[Vec<u8>],
    fsync: bool,
    compression: &CompressionConfig,
    encryption: &EncryptionRuntime,
) -> Result<Vec<u64>> {
    append_frames_for_object(path, path, kind, payloads, fsync, compression, encryption)
}

/// Write frames through `write_path` while cryptographically binding them to
/// `object_path`. Atomic rewrite paths must use the final published object as
/// the binding; otherwise a temporary filename would produce ciphertext that
/// cannot authenticate after rename.
pub(crate) fn append_frames_for_object(
    write_path: &Path,
    object_path: &Path,
    kind: FrameKind,
    payloads: &[Vec<u8>],
    fsync: bool,
    compression: &CompressionConfig,
    encryption: &EncryptionRuntime,
) -> Result<Vec<u64>> {
    let sync_mode = if fsync {
        AppendSyncMode::ExplicitDataSync
    } else {
        AppendSyncMode::None
    };
    append_frames_with_sync_mode(
        write_path,
        object_path,
        kind,
        payloads,
        sync_mode,
        compression,
        encryption,
    )
}

#[allow(dead_code)] // retained as a storage primitive; tx log now uses a cached handle
pub(crate) fn append_frames_open_datasync(
    path: &Path,
    kind: FrameKind,
    payloads: &[Vec<u8>],
    fsync: bool,
    compression: &CompressionConfig,
    encryption: &EncryptionRuntime,
) -> Result<Vec<u64>> {
    let sync_mode = if fsync {
        AppendSyncMode::OpenDataSync
    } else {
        AppendSyncMode::None
    };
    append_frames_with_sync_mode(
        path,
        path,
        kind,
        payloads,
        sync_mode,
        compression,
        encryption,
    )
}

fn append_frames_with_sync_mode(
    write_path: &Path,
    object_path: &Path,
    kind: FrameKind,
    payloads: &[Vec<u8>],
    sync_mode: AppendSyncMode,
    compression: &CompressionConfig,
    encryption: &EncryptionRuntime,
) -> Result<Vec<u64>> {
    if payloads.is_empty() {
        return Ok(Vec::new());
    }

    check_not_poisoned(write_path)?;
    let _backup_gate = acquire_backup_write_guard(write_path)?;
    ensure_parent(write_path)?;
    let (file, sync_after_write) = open_append_file(write_path, sync_mode)?;

    let mut offsets = Vec::with_capacity(payloads.len());
    let mut next_offset = file.metadata()?.len();
    let mut write_buffer = Vec::new();
    for payload in payloads {
        if payload.len() > MAX_PAYLOAD_LEN {
            return Err(BicDbError::Corruption {
                path: write_path.to_path_buf(),
                message: format!("frame payload is too large: {} bytes", payload.len()),
            });
        }

        let (stored_payload, flags) =
            encode_payload(object_path, payload, kind, compression, encryption)?;
        let offset = next_offset;
        let header = encode_header(kind, flags, &stored_payload);
        write_buffer.extend_from_slice(&header);
        write_buffer.extend_from_slice(&stored_payload);
        offsets.push(offset);
        next_offset = next_offset.saturating_add((header.len() + stored_payload.len()) as u64);
    }

    // The same append-or-repair discipline as the transaction log
    // (`write_raw`): a failed or short write, or a failed sync, may have
    // left some or all of the batch in the file. Nothing in the batch was
    // acknowledged, so the tail is cut back to the frame boundary it started
    // at (and that cut is synced) before the error is returned. Without
    // this, the next append lands after the torn prefix, and recovery,
    // which stops at the first bad frame, would later drop every frame
    // written after it, including acknowledged ones.
    let handle_len_before = file.metadata()?.len();
    let mut handle = AppendHandle {
        file,
        sync_after_write,
        durable: sync_mode != AppendSyncMode::None,
        failed_repair: None,
    };
    let result = write_raw_with_repair(
        &mut handle,
        &write_buffer,
        |file, bytes, sync_after_write| {
            // Fault injection (tests only; one atomic load otherwise), scoped
            // by path. `frame_log.write` fails before any byte is written.
            // `frame_log.short_write` writes about half of the batch, always
            // ending inside a frame, and then fails, as a write cut short by a
            // full disk or a signal does: the file gets complete frames of the
            // failed batch (when it has several) and a torn one.
            // `frame_log.sync` writes the whole batch and then fails its
            // sync: the bytes may or may not be durable.
            bicdb_page::test_faults::io_fault("frame_log.write", write_path)?;
            if bicdb_page::test_faults::io_fault("frame_log.short_write", write_path).is_err() {
                let mut cut = bytes.len() / 2;
                if offsets
                    .iter()
                    .any(|offset| offset - handle_len_before == cut as u64)
                {
                    cut += 1;
                }
                file.write_all(&bytes[..cut])?;
                return Err(std::io::Error::from_raw_os_error(libc::EIO));
            }
            file.write_all(bytes)?;
            if sync_mode != AppendSyncMode::None {
                bicdb_page::test_faults::io_fault("frame_log.sync", write_path)?;
            }
            if sync_after_write {
                file.sync_data()?;
            }
            Ok(())
        },
        |file, len, durable| {
            // `frame_log.repair` fails the tail repair too, leaving the torn
            // bytes in place.
            bicdb_page::test_faults::io_fault("frame_log.repair", write_path)?;
            file.set_len(len)?;
            if durable {
                file.sync_all()?;
            }
            Ok(())
        },
    );
    if let Err(error) = result {
        if let Some(reason) = handle.failed_repair {
            // The file now holds bytes of unknown status after its last
            // acknowledged frame. Appending after them would put later,
            // acknowledged frames behind damage, so the file accepts no
            // more appends until recovery (the next open) re-establishes
            // its tail. The caller reports a durability failure.
            //
            // The bytes may include complete frames of the failed batch,
            // which nobody acknowledged. A marker records where the log's
            // acknowledged content ends, so recovery cuts them off rather
            // than accepting them (a failed sync.log append, for example,
            // belongs to a commit that aborted). Writing it is best effort:
            // the disk is already failing.
            poison(write_path, &reason);
            let marker = match write_append_failure_marker(write_path, handle_len_before) {
                Ok(()) => String::new(),
                Err(error) => format!("; the append-failure marker was not written: {error}"),
            };
            return Err(BicDbError::WriteFailed {
                site: FRAME_LOG_REPAIR_SITE.into(),
                detail: format!("{}: {reason}{marker}", write_path.display()),
            });
        }
        return Err(error);
    }

    Ok(offsets)
}

/// Record a frame log's durability failure (a failed tail repair, or an
/// append refused because of one) in the database's fail-closed state, and
/// hand the error back. Other errors pass through: a failed append whose
/// tail was repaired is an ordinary error, because nothing it wrote remains
/// and nothing acknowledged depended on it.
pub(crate) fn fence_on_frame_log_failure(
    health: Option<&std::sync::Arc<bicdb_page::DurabilityHealth>>,
    error: BicDbError,
) -> BicDbError {
    if let (Some(health), BicDbError::WriteFailed { site, detail }) = (health, &error) {
        health.record_failure(site, detail);
    }
    error
}

/// The site recorded when a generic frame log could not repair its tail
/// after a failed append. See `docs/durability-failure-handling.md`.
pub(crate) const FRAME_LOG_REPAIR_SITE: &str = "frame_log.repair";

/// Frame files whose tail repair failed in this process, with the reason.
/// The set is process-wide because the generic appender holds no handle:
/// each call opens the file. An entry blocks every later append to that path
/// until a recovery reader ([`read_frames`], [`stream_frames`],
/// [`read_frames_from`]) or [`salvage_frame_log`] re-establishes a tail that
/// ends on a frame boundary.
fn poisoned_frame_logs() -> &'static parking_lot::Mutex<std::collections::HashMap<PathBuf, String>>
{
    static POISONED: std::sync::OnceLock<
        parking_lot::Mutex<std::collections::HashMap<PathBuf, String>>,
    > = std::sync::OnceLock::new();
    POISONED.get_or_init(Default::default)
}

static ANY_FRAME_LOG_POISONED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

fn poison(path: &Path, reason: &str) {
    let mut poisoned = poisoned_frame_logs().lock();
    poisoned.insert(path.to_path_buf(), reason.to_string());
    ANY_FRAME_LOG_POISONED.store(true, std::sync::atomic::Ordering::Release);
}

fn clear_poison(path: &Path) {
    if !ANY_FRAME_LOG_POISONED.load(std::sync::atomic::Ordering::Acquire) {
        return;
    }
    let mut poisoned = poisoned_frame_logs().lock();
    poisoned.remove(path);
    ANY_FRAME_LOG_POISONED.store(!poisoned.is_empty(), std::sync::atomic::Ordering::Release);
}

/// `<log>.append-failed`: written when a failed append could not be cut
/// back. Holds the log's length before that append, which is where its
/// acknowledged content ends.
pub(crate) fn append_failure_marker_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("frames");
    path.with_file_name(format!("{name}.append-failed"))
}

#[derive(Serialize, Deserialize)]
struct AppendFailureMarker {
    acknowledged_len: u64,
}

fn write_append_failure_marker(path: &Path, acknowledged_len: u64) -> Result<()> {
    let bytes = serde_json::to_vec(&AppendFailureMarker { acknowledged_len })?;
    write_atomic(&append_failure_marker_path(path), &bytes, true)
}

/// Recovery's first step for a log whose earlier append failed and could not
/// be cut back: cut it now, to the length recorded before that append. In
/// the process that failed, the log was poisoned, so nothing was appended
/// after the failed bytes; and every open runs recovery before it appends.
/// So everything past the recorded length belongs to failed appends.
fn apply_append_failure_marker(file: &File, path: &Path) -> Result<()> {
    let marker_path = append_failure_marker_path(path);
    let bytes = match fs::read(&marker_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let marker: AppendFailureMarker =
        serde_json::from_slice(&bytes).map_err(|error| BicDbError::Corruption {
            path: marker_path.clone(),
            message: format!("unreadable append-failure marker: {error}"),
        })?;
    if marker.acknowledged_len < file.metadata()?.len() {
        file.set_len(marker.acknowledged_len)?;
        file.sync_all()?;
    }
    fs::remove_file(&marker_path)?;
    sync_parent(path)?;
    Ok(())
}

fn check_not_poisoned(path: &Path) -> Result<()> {
    // A marker left by an earlier process means the file still ends in
    // failed bytes that no recovery read has cut yet (a log the open does not
    // read, such as a disabled `sync.log`). Appending after them would put
    // acknowledged frames behind damage.
    let marker = append_failure_marker_path(path);
    if marker.exists() {
        return Err(BicDbError::WriteFailed {
            site: FRAME_LOG_REPAIR_SITE.into(),
            detail: format!(
                "{}: refusing to append while {} records an unrepaired failed append; \
                 open the database with this log enabled so recovery cuts the failed bytes",
                path.display(),
                marker.display()
            ),
        });
    }
    if !ANY_FRAME_LOG_POISONED.load(std::sync::atomic::Ordering::Acquire) {
        return Ok(());
    }
    match poisoned_frame_logs().lock().get(path) {
        Some(reason) => Err(BicDbError::WriteFailed {
            site: FRAME_LOG_REPAIR_SITE.into(),
            detail: format!(
                "{}: refusing to append after an unrepaired failed append ({reason}); \
                 reopen the database so recovery re-establishes the log's tail",
                path.display()
            ),
        }),
        None => Ok(()),
    }
}

/// Cached append descriptor and its platform-specific durability obligation.
#[derive(Debug)]
pub(crate) struct AppendHandle {
    file: File,
    sync_after_write: bool,
    durable: bool,
    failed_repair: Option<String>,
}

impl AppendHandle {
    pub(crate) fn check_writable(&self) -> Result<()> {
        if let Some(reason) = &self.failed_repair {
            return Err(std::io::Error::other(format!(
                "append log requires recovery after failed tail repair: {reason}"
            ))
            .into());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn read_only_for_test(path: &Path) -> Self {
        Self {
            file: File::open(path).unwrap(),
            sync_after_write: false,
            durable: true,
            failed_repair: None,
        }
    }
}

/// Opens (creating if needed) an append-only log handle using the requested sync
/// mode. Durable handles use O_DSYNC on Unix and explicit sync elsewhere.
pub(crate) fn open_append_handle(path: &Path, fsync: bool) -> Result<AppendHandle> {
    ensure_parent(path)?;
    let sync_mode = if fsync {
        AppendSyncMode::OpenDataSync
    } else {
        AppendSyncMode::None
    };
    let (file, sync_after_write) = open_append_file(path, sync_mode)?;
    Ok(AppendHandle {
        file,
        sync_after_write,
        durable: fsync,
        failed_repair: None,
    })
}

/// Encodes frames into a single on-disk byte buffer (header + stored payload per
/// frame) WITHOUT writing anything. Lets a caller build the WAL bytes under one
/// lock (e.g. the commit lock) and perform the durable write under a different
/// lock so the two pipeline.
pub(crate) fn encode_frames_to_bytes(
    path: &Path,
    kind: FrameKind,
    payloads: &[Vec<u8>],
    compression: &CompressionConfig,
    encryption: &EncryptionRuntime,
) -> Result<Vec<u8>> {
    // Headers are fixed-size and payloads rarely grow under encoding, so the
    // sum of the inputs (plus a header per frame) sizes the buffer once.
    let mut write_buffer =
        Vec::with_capacity(payloads.iter().map(|payload| payload.len() + 64).sum());
    for payload in payloads {
        if payload.len() > MAX_PAYLOAD_LEN {
            return Err(BicDbError::Corruption {
                path: path.to_path_buf(),
                message: format!("frame payload is too large: {} bytes", payload.len()),
            });
        }
        // Plain frames already have their final payload. Do not allocate and
        // copy it through encode_compressed_payload only to copy it again into
        // the batch buffer. Compression/encryption retain the existing path.
        if !encryption.is_enabled()
            && (!compression.enabled || payload.len() < compression.min_bytes)
        {
            let header = encode_header(kind, 0, payload);
            write_buffer.extend_from_slice(&header);
            write_buffer.extend_from_slice(payload);
            continue;
        }
        let (stored_payload, flags) = encode_payload(path, payload, kind, compression, encryption)?;
        let header = encode_header(kind, flags, &stored_payload);
        write_buffer.extend_from_slice(&header);
        write_buffer.extend_from_slice(&stored_payload);
    }
    Ok(write_buffer)
}

/// Append an ordinary uncompressed, unencrypted JSON frame directly to a batch.
pub(crate) fn append_plain_json_frame<T: Serialize>(
    out: &mut Vec<u8>,
    kind: FrameKind,
    value: &T,
) -> Result<()> {
    // Caller explicitly selects uncompressed, unencrypted framing. Serialize
    // directly into the final batch, then fill its ordinary header/CRC. No
    // per-frame payload allocation or copy; errors remove the incomplete frame.
    let start = out.len();
    out.resize(start + HEADER_LEN, 0);
    if let Err(error) = serde_json::to_writer(&mut *out, value) {
        out.truncate(start);
        return Err(error.into());
    }
    let payload = &out[start + HEADER_LEN..];
    if payload.len() > MAX_PAYLOAD_LEN {
        let len = payload.len();
        out.truncate(start);
        return Err(BicDbError::Corruption {
            path: PathBuf::from("prepared WAL"),
            message: format!("frame payload is too large: {len} bytes"),
        });
    }
    let header = encode_header(kind, 0, payload);
    out[start..start + HEADER_LEN].copy_from_slice(&header);
    Ok(())
}

#[cfg(test)]
pub(crate) fn first_plain_frame_payload(bytes: &[u8]) -> &[u8] {
    let (kind, flags, len, crc) = decode_header(bytes[..HEADER_LEN].try_into().unwrap()).unwrap();
    assert_eq!(flags, 0);
    let payload = &bytes[HEADER_LEN..HEADER_LEN + len];
    assert_eq!(checksum(kind, flags, len as u32, payload), crc);
    payload
}

/// Writes pre-encoded bytes to an already-open append handle. Durability is
/// determined when the handle is opened. Platforms without O_DSYNC must
/// complete the explicit sync before the caller can acknowledge the write.
pub(crate) fn write_raw(handle: &mut AppendHandle, path: &Path, bytes: &[u8]) -> Result<()> {
    handle.check_writable()?;
    if bytes.is_empty() {
        return Ok(());
    }
    let _backup_gate = acquire_backup_write_guard(path)?;
    write_raw_holding_gate(handle, path, bytes)
}

/// [`write_raw`] for a caller that already holds the backup write gate.
/// Separating the two lets the transaction log tell a gate timeout (nothing
/// was written; an ordinary, retryable error) from a failed write or sync
/// (a durability failure that fences the database).
pub(crate) fn write_raw_holding_gate(
    handle: &mut AppendHandle,
    path: &Path,
    bytes: &[u8],
) -> Result<()> {
    handle.check_writable()?;
    if bytes.is_empty() {
        return Ok(());
    }
    write_raw_with_repair(
        handle,
        bytes,
        |file, bytes, sync_after_write| {
            // Fault injection (tests only; one atomic load otherwise).
            // `transaction_log.write` fails before any byte is written.
            // `transaction_log.sync` writes the bytes and then fails, as an
            // O_DSYNC write whose data sync failed does: the bytes may or may
            // not be durable. `transaction_log.repair` then fails the tail
            // repair too, so the bytes stay in the file.
            bicdb_page::test_faults::io_fault("transaction_log.write", path)?;
            append_and_sync(file, bytes, sync_after_write, File::sync_data)?;
            bicdb_page::test_faults::io_fault("transaction_log.sync", path)
        },
        |file, len, durable| {
            bicdb_page::test_faults::io_fault("transaction_log.repair", path)?;
            file.set_len(len)?;
            // O_DSYNC covers writes, not a subsequent ftruncate. Persist the
            // repaired boundary before permitting another acknowledged batch.
            if durable {
                file.sync_all()?;
            }
            Ok(())
        },
    )
}

/// The caller serializes ALL appends and truncations of this file. In
/// particular the transaction log holds its writer mutex across this method;
/// the backup gate alone is not an exclusive writer lock.
fn write_raw_with_repair(
    handle: &mut AppendHandle,
    bytes: &[u8],
    append: impl FnOnce(&mut File, &[u8], bool) -> std::io::Result<()>,
    repair: impl FnOnce(&File, u64, bool) -> std::io::Result<()>,
) -> Result<()> {
    handle.check_writable()?;
    let previous_len = handle.file.metadata()?.len();
    match append(&mut handle.file, bytes, handle.sync_after_write) {
        Ok(()) => Ok(()),
        Err(write_error) => {
            // write_all may already have appended a prefix, and an explicit
            // sync can fail after writing the entire batch. Neither case can
            // safely retry by appending another copy after those bytes.
            if let Err(repair_error) = repair(&handle.file, previous_len, handle.durable) {
                let reason =
                    format!("append failed: {write_error}; tail repair failed: {repair_error}");
                handle.failed_repair = Some(reason.clone());
                return Err(std::io::Error::other(reason).into());
            }
            Err(write_error.into())
        }
    }
}

fn append_and_sync<W: Write>(
    file: &mut W,
    bytes: &[u8],
    sync_after_write: bool,
    sync: impl FnOnce(&W) -> std::io::Result<()>,
) -> std::io::Result<()> {
    file.write_all(bytes)?;
    if sync_after_write {
        sync(file)?;
    }
    Ok(())
}

fn open_append_file(path: &Path, sync_mode: AppendSyncMode) -> Result<(File, bool)> {
    #[cfg(test)]
    match sync_mode {
        AppendSyncMode::ExplicitDataSync => {
            APPEND_EXPLICIT_DATA_SYNC_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        AppendSyncMode::OpenDataSync => {
            APPEND_OPEN_DATA_SYNC_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        AppendSyncMode::None => {}
    }

    let mut options = OpenOptions::new();
    options.create(true).append(true).read(true);
    let sync_after_write = match sync_mode {
        AppendSyncMode::None => false,
        AppendSyncMode::ExplicitDataSync => true,
        AppendSyncMode::OpenDataSync => {
            #[cfg(unix)]
            {
                options.custom_flags(libc::O_DSYNC);
                false
            }
            #[cfg(not(unix))]
            {
                true
            }
        }
    };
    Ok((options.open(path)?, sync_after_write))
}

#[cfg(test)]
pub(crate) fn reset_append_sync_mode_counts() {
    APPEND_EXPLICIT_DATA_SYNC_CALLS.store(0, Ordering::Relaxed);
    APPEND_OPEN_DATA_SYNC_CALLS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn append_sync_mode_counts() -> (usize, usize) {
    (
        APPEND_EXPLICIT_DATA_SYNC_CALLS.load(Ordering::Relaxed),
        APPEND_OPEN_DATA_SYNC_CALLS.load(Ordering::Relaxed),
    )
}

/// Create the stable gate inode before a database can perform recovery or
/// foreground writes. Backups also call this for closed databases.
pub(crate) fn initialize_backup_write_gate(db_path: &Path) -> Result<()> {
    fs::create_dir_all(db_path)?;
    let path = db_path.join(BACKUP_WRITE_GATE);
    drop(open_backup_gate(&path)?);
    Ok(())
}

/// Hold shared authority for the complete physical write. The guard, not an
/// earlier path-existence check, closes the writer/backup acquisition race.
pub(crate) fn acquire_backup_write_guard(path: &Path) -> Result<Option<BackupGateGuard>> {
    let Some(gate) = backup_write_gate_path(path) else {
        // Low-level storage unit tests and standalone codecs may not belong to
        // an opened database. Real database opens always initialize the gate.
        return Ok(None);
    };
    // `backup_gate.timeout` (tests only), keyed by the file about to be
    // written: report the timeout at once, as if a backup had held the
    // gate for the whole wait.
    if bicdb_page::test_faults::io_fault("backup_gate.timeout", path).is_err() {
        return Err(BicDbError::Backup(format!(
            "timed out waiting for shared backup I/O gate {}",
            gate.display()
        )));
    }
    acquire_backup_gate(&gate, false).map(Some)
}

pub(crate) fn acquire_database_backup_write_guard(db_path: &Path) -> Result<BackupGateGuard> {
    initialize_backup_write_gate(db_path)?;
    acquire_backup_gate(&db_path.join(BACKUP_WRITE_GATE), false)
}

pub(crate) fn acquire_database_backup_exclusive_guard(db_path: &Path) -> Result<BackupGateGuard> {
    initialize_backup_write_gate(db_path)?;
    acquire_backup_gate(&db_path.join(BACKUP_WRITE_GATE), true)
}

fn acquire_backup_gate(path: &Path, exclusive: bool) -> Result<BackupGateGuard> {
    let started = Instant::now();
    loop {
        match try_acquire_backup_gate(path, exclusive) {
            Ok(Some(file)) => return Ok(BackupGateGuard { _file: file }),
            Ok(None) => {}
            Err(error) => return Err(error),
        }
        if started.elapsed() >= BACKUP_LOCK_WAIT_TIMEOUT {
            return Err(BicDbError::Backup(format!(
                "timed out waiting for {} backup I/O gate {}",
                if exclusive { "exclusive" } else { "shared" },
                path.display()
            )));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(unix)]
fn try_acquire_backup_gate(path: &Path, exclusive: bool) -> Result<Option<File>> {
    use std::os::fd::AsRawFd;

    let file = open_backup_gate(path)?;
    let operation = if exclusive {
        libc::LOCK_EX
    } else {
        libc::LOCK_SH
    };
    let result = unsafe { libc::flock(file.as_raw_fd(), operation | libc::LOCK_NB) };
    if result == 0 {
        return Ok(Some(file));
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::EWOULDBLOCK) || error.raw_os_error() == Some(libc::EAGAIN)
    {
        return Ok(None);
    }
    Err(error.into())
}

#[cfg(windows)]
fn try_acquire_backup_gate(path: &Path, exclusive: bool) -> Result<Option<File>> {
    use std::os::windows::fs::OpenOptionsExt;

    let mut options = std::fs::OpenOptions::new();
    options.create(true).read(true).write(true);
    if exclusive {
        options.share_mode(0);
    }
    match options.open(path) {
        Ok(file) => Ok(Some(file)),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::WouldBlock
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(not(any(unix, windows)))]
fn try_acquire_backup_gate(path: &Path, _exclusive: bool) -> Result<Option<File>> {
    open_backup_gate(path).map(Some)
}

fn open_backup_gate(path: &Path) -> Result<File> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(BicDbError::Backup(format!(
                "backup I/O gate {} is not a regular file",
                path.display()
            )));
        }
    }
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW).mode(0o600);
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(BicDbError::Backup(format!(
            "backup I/O gate {} is not a regular file",
            path.display()
        )));
    }
    Ok(file)
}

fn backup_write_gate_path(path: &Path) -> Option<PathBuf> {
    // The nearest initialized database wins. This also covers deeply nested
    // content-addressed blob paths without guessing storage directory names.
    path.ancestors()
        .skip(1)
        .take(16)
        .map(|ancestor| ancestor.join(BACKUP_WRITE_GATE))
        .find(|candidate| candidate.is_file())
}

pub(crate) fn read_frames(
    path: &Path,
    expected_kind: FrameKind,
    read_mode: SegmentReadMode,
    encryption: &EncryptionRuntime,
) -> Result<RecoveredFrames> {
    ensure_parent(path)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    apply_append_failure_marker(&file, path)?;

    let file_len = file.metadata()?.len();
    if file_len == 0 {
        clear_poison(path);
        return Ok(RecoveredFrames {
            frames: Vec::new(),
            truncated_bytes: 0,
        });
    }

    match read_mode {
        SegmentReadMode::Buffered => {
            read_frames_buffered(file, file_len, path, expected_kind, encryption, false)
                .map(|(recovered, _)| recovered)
        }
        #[cfg(feature = "mmap")]
        SegmentReadMode::Mmap => {
            read_frames_mmap(file, file_len, path, expected_kind, encryption, false)
                .map(|(recovered, _)| recovered)
        }
        // Builds without the `mmap` feature read identically, just buffered.
        #[cfg(not(feature = "mmap"))]
        SegmentReadMode::Mmap => {
            read_frames_buffered(file, file_len, path, expected_kind, encryption, false)
                .map(|(recovered, _)| recovered)
        }
    }
}

/// Visit every valid frame in order WITHOUT materializing the file or the
/// frame set: the segment streams through a bounded buffer and each decoded
/// payload is handed to `visit`, then dropped. Recovery semantics match the
/// non-strict [`read_frames`]: the first torn or invalid frame ends the walk
/// and the file is truncated to the last valid boundary. This exists because
/// recovering a multi-gigabyte event segment through `read_frames` holds the
/// raw file AND every decoded payload simultaneously — the resident cost
/// that OOM-killed a broker whose log outgrew its container.
pub(crate) fn stream_frames(
    path: &Path,
    expected_kind: FrameKind,
    encryption: &EncryptionRuntime,
    visit: impl FnMut(u64, Vec<u8>) -> Result<()>,
) -> Result<u64> {
    ensure_parent(path)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    apply_append_failure_marker(&file, path)?;
    stream_frames_from_file(file, path, expected_kind, encryption, true, visit)
}

/// [`stream_frames`] for a log another thread may be appending to: the walk
/// ends at the first frame that is not complete and valid, and the file is
/// never truncated or created. A frame still being written is the normal
/// state of a live log's tail; a recovery reader would mistake it for a torn
/// tail and cut it off under the writer, which then reports the write as
/// successful.
pub(crate) fn stream_frames_live(
    path: &Path,
    expected_kind: FrameKind,
    encryption: &EncryptionRuntime,
    visit: impl FnMut(u64, Vec<u8>) -> Result<()>,
) -> Result<()> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    stream_frames_from_file(file, path, expected_kind, encryption, false, visit).map(|_| ())
}

fn stream_frames_from_file(
    mut file: File,
    path: &Path,
    expected_kind: FrameKind,
    encryption: &EncryptionRuntime,
    recover: bool,
    mut visit: impl FnMut(u64, Vec<u8>) -> Result<()>,
) -> Result<u64> {
    const READ_CHUNK: usize = 8 * 1024 * 1024;
    let file_len = file.metadata()?.len();
    if file_len == 0 {
        if recover {
            clear_poison(path);
        }
        return Ok(0);
    }

    let mut buffer: Vec<u8> = Vec::with_capacity(READ_CHUNK + HEADER_LEN);
    let mut buffer_file_offset = 0u64; // file offset of buffer[0]
    let mut consumed = 0usize; // parsed prefix of `buffer`
    let mut valid_up_to = 0u64;
    let mut exhausted = false;

    'walk: loop {
        // Ensure at least one whole frame (or EOF) is buffered.
        loop {
            let available = buffer.len() - consumed;
            let need = if available < HEADER_LEN {
                HEADER_LEN
            } else {
                let mut header = [0_u8; HEADER_LEN];
                header.copy_from_slice(&buffer[consumed..consumed + HEADER_LEN]);
                match decode_header(&header) {
                    Some((_, _, len, _)) if len <= MAX_PAYLOAD_LEN => HEADER_LEN + len,
                    // Invalid or oversized headers end the walk below with
                    // exactly the bytes already buffered.
                    _ => break,
                }
            };
            if available >= need || exhausted {
                break;
            }
            // Drop the parsed prefix before growing the buffer.
            if consumed > 0 {
                buffer.drain(..consumed);
                buffer_file_offset += consumed as u64;
                consumed = 0;
            }
            let read_started = buffer.len();
            buffer.resize(read_started + READ_CHUNK, 0);
            let got = read_fully(&mut file, &mut buffer[read_started..])?;
            buffer.truncate(read_started + got);
            if got == 0 {
                exhausted = true;
            }
        }

        let offset = buffer_file_offset + consumed as u64;
        let available = buffer.len() - consumed;
        if available < HEADER_LEN {
            break 'walk;
        }
        let mut header = [0_u8; HEADER_LEN];
        header.copy_from_slice(&buffer[consumed..consumed + HEADER_LEN]);
        let Some((kind, flags, len, expected_checksum)) = decode_header(&header) else {
            break 'walk;
        };
        if kind != expected_kind || len > MAX_PAYLOAD_LEN {
            break 'walk;
        }
        if available < HEADER_LEN + len {
            break 'walk;
        }
        let stored_payload = &buffer[consumed + HEADER_LEN..consumed + HEADER_LEN + len];
        if expected_checksum != checksum(kind, flags, len as u32, stored_payload) {
            break 'walk;
        }
        let payload = decode_payload(path, kind, flags, stored_payload, encryption)?;
        consumed += HEADER_LEN + len;
        valid_up_to = buffer_file_offset + consumed as u64;
        visit(offset, payload)?;
    }

    if recover {
        settle_recovered_tail(&file, path, valid_up_to, file_len)?;
    }
    Ok(file_len.saturating_sub(valid_up_to))
}

/// The end of a recovery walk that stopped at `valid_up_to`, the end of the
/// last complete, valid frame.
///
/// A crash or a failed append can only leave a torn *suffix*: appends are
/// sequential, and a failed append cuts its own bytes back (or poisons the
/// file so nothing is appended after them). So when no valid frame starts
/// anywhere after the stop point, the bytes after it are a torn tail: they
/// were never acknowledged, and they are truncated (and the truncation is
/// synced) so the next append starts on a frame boundary.
///
/// When a valid frame does start after the stop point, the damage is in the
/// middle of the log: truncating would silently drop that frame and every
/// one after it, any of which may have been acknowledged. The file is left
/// unchanged and the open fails with `Corruption`. [`salvage_frame_log`] is
/// the explicit operator action that accepts the loss.
fn settle_recovered_tail(file: &File, path: &Path, valid_up_to: u64, file_len: u64) -> Result<()> {
    if valid_up_to < file_len {
        if let Some(next) = first_valid_frame_after(file, valid_up_to, file_len)? {
            return Err(mid_file_damage(path, valid_up_to, next, file_len));
        }
        file.set_len(valid_up_to)?;
        file.sync_data()?;
    }
    // The tail now ends on a frame boundary: appends may resume.
    clear_poison(path);
    Ok(())
}

fn mid_file_damage(path: &Path, damaged_at: u64, next_valid: u64, file_len: u64) -> BicDbError {
    BicDbError::Corruption {
        path: path.to_path_buf(),
        message: format!(
            "frame log is damaged at offset {damaged_at}, and a valid frame follows at offset \
             {next_valid} ({} bytes after the damage). This is not a torn tail: truncating \
             would silently drop frames that may have been acknowledged, so the file is left \
             unchanged. Restore it from a backup, or accept the loss of everything from offset \
             {damaged_at} with salvage_frame_log, which keeps those bytes in a side file",
            file_len.saturating_sub(damaged_at)
        ),
    }
}

/// The offset of the first complete, checksum-valid frame of any kind that
/// starts after `damaged_at`, or `None`. Only runs on the rare path where a
/// recovery walk stopped before the end of the file. A frame header begins
/// with a 4-byte magic and a version byte, and serialized JSON payloads
/// never contain that byte sequence (control bytes are escaped), so a match
/// that also passes the CRC is a real frame.
fn first_valid_frame_after(file: &File, damaged_at: u64, file_len: u64) -> Result<Option<u64>> {
    const CHUNK: usize = 1024 * 1024;
    let mut reader = file.try_clone()?;
    let mut window = vec![0_u8; CHUNK];
    let mut position = damaged_at.saturating_add(1);
    while position.saturating_add(HEADER_LEN as u64) <= file_len {
        let want = ((file_len - position) as usize).min(CHUNK);
        reader.seek(SeekFrom::Start(position))?;
        reader.read_exact(&mut window[..want])?;
        let bytes = &window[..want];
        let mut search = 0;
        while let Some(found) = bytes[search..]
            .windows(MAGIC.len())
            .position(|candidate| candidate == MAGIC)
        {
            let candidate = position + (search + found) as u64;
            if frame_is_valid_at(&mut reader, candidate, file_len)? {
                return Ok(Some(candidate));
            }
            search += found + 1;
        }
        if want < CHUNK {
            break;
        }
        // Overlap by the magic's length minus one so a magic that straddles
        // two windows is still found.
        position += (want - (MAGIC.len() - 1)) as u64;
    }
    Ok(None)
}

fn frame_is_valid_at(reader: &mut File, offset: u64, file_len: u64) -> Result<bool> {
    if offset.saturating_add(HEADER_LEN as u64) > file_len {
        return Ok(false);
    }
    let mut header = [0_u8; HEADER_LEN];
    reader.seek(SeekFrom::Start(offset))?;
    reader.read_exact(&mut header)?;
    let Some((kind, flags, len, expected_checksum)) = decode_header(&header) else {
        return Ok(false);
    };
    if len > MAX_PAYLOAD_LEN || offset + (HEADER_LEN + len) as u64 > file_len {
        return Ok(false);
    }
    let mut payload = vec![0_u8; len];
    reader.read_exact(&mut payload)?;
    Ok(checksum(kind, flags, len as u32, &payload) == expected_checksum)
}

/// What [`salvage_frame_log`] did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameLogSalvage {
    /// Bytes kept: the log now ends after its last valid frame before the
    /// first damage.
    pub kept_bytes: u64,
    /// Bytes moved out of the log (the damage and everything after it).
    pub moved_bytes: u64,
    /// Where the moved bytes were preserved, when any were moved.
    pub moved_to: Option<PathBuf>,
}

/// Operator recovery for a frame log that recovery refused because of
/// damage in the middle (`Corruption`, "a valid frame follows"). Keeps every
/// frame before the first damaged one, copies the damage and everything after
/// it to `<file>.damaged-<offset>` next to the log (synced before the log is
/// cut), and truncates the log there. This deliberately gives up the frames
/// after the damage; use it only when no backup can restore them. Run it
/// with the database closed. A log without damage is left unchanged.
pub fn salvage_frame_log(path: &Path) -> Result<FrameLogSalvage> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    apply_append_failure_marker(&file, path)?;
    let file_len = file.metadata()?.len();
    // Walk the same way recovery does: frames of the log's own kind (the
    // first frame's), complete, with a valid checksum.
    let mut offset = 0_u64;
    let mut log_kind = None;
    while offset + HEADER_LEN as u64 <= file_len {
        let mut header = [0_u8; HEADER_LEN];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut header)?;
        let Some((kind, _, len, _)) = decode_header(&header) else {
            break;
        };
        if *log_kind.get_or_insert(kind) != kind || !frame_is_valid_at(&mut file, offset, file_len)?
        {
            break;
        }
        offset += (HEADER_LEN + len) as u64;
    }
    if offset == file_len {
        clear_poison(path);
        return Ok(FrameLogSalvage {
            kept_bytes: file_len,
            moved_bytes: 0,
            moved_to: None,
        });
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("frames");
    let mut side = path.with_file_name(format!("{file_name}.damaged-{offset}"));
    if side.exists() {
        side = path.with_file_name(format!(
            "{file_name}.damaged-{offset}-{}",
            Uuid::new_v4().simple()
        ));
    }
    {
        let mut out = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&side)?;
        file.seek(SeekFrom::Start(offset))?;
        std::io::copy(&mut (&mut file).take(file_len - offset), &mut out)?;
        out.sync_all()?;
    }
    sync_parent(path)?;
    file.set_len(offset)?;
    file.sync_all()?;
    clear_poison(path);
    Ok(FrameLogSalvage {
        kept_bytes: offset,
        moved_bytes: file_len - offset,
        moved_to: Some(side),
    })
}

fn read_fully(file: &mut File, buffer: &mut [u8]) -> Result<usize> {
    use std::io::Read;
    let mut filled = 0usize;
    while filled < buffer.len() {
        let got = file.read(&mut buffer[filled..])?;
        if got == 0 {
            break;
        }
        filled += got;
    }
    Ok(filled)
}

pub(crate) fn verify_frames(
    path: &Path,
    expected_kind: FrameKind,
    read_mode: SegmentReadMode,
    encryption: &EncryptionRuntime,
) -> Result<FrameVerification> {
    if !path.exists() {
        return Ok(FrameVerification::default());
    }

    let (file, file_len) = open_regular_nofollow(path)?;
    if file_len == 0 {
        return Ok(FrameVerification::default());
    }

    let (_, verification) = match read_mode {
        SegmentReadMode::Buffered => {
            read_frames_buffered(file, file_len, path, expected_kind, encryption, true)?
        }
        #[cfg(feature = "mmap")]
        SegmentReadMode::Mmap => {
            read_frames_mmap(file, file_len, path, expected_kind, encryption, true)?
        }
        #[cfg(not(feature = "mmap"))]
        SegmentReadMode::Mmap => {
            read_frames_buffered(file, file_len, path, expected_kind, encryption, true)?
        }
    };
    Ok(verification)
}

/// Strictly authenticate and decode a frame file without recovery truncation.
/// Immutable index/search sidecars use this path: corruption must fail closed
/// rather than silently publishing an empty or shortened structure.
pub(crate) fn read_frames_strict(
    path: &Path,
    expected_kind: FrameKind,
    read_mode: SegmentReadMode,
    encryption: &EncryptionRuntime,
) -> Result<RecoveredFrames> {
    let (file, file_len) = open_regular_nofollow(path)?;
    if file_len == 0 {
        return Err(BicDbError::Corruption {
            path: path.to_path_buf(),
            message: "immutable frame file is empty".to_string(),
        });
    }
    let (recovered, _) = match read_mode {
        SegmentReadMode::Buffered => {
            read_frames_buffered(file, file_len, path, expected_kind, encryption, true)?
        }
        #[cfg(feature = "mmap")]
        SegmentReadMode::Mmap => {
            read_frames_mmap(file, file_len, path, expected_kind, encryption, true)?
        }
        #[cfg(not(feature = "mmap"))]
        SegmentReadMode::Mmap => {
            read_frames_buffered(file, file_len, path, expected_kind, encryption, true)?
        }
    };
    Ok(recovered)
}

/// Open an immutable authenticated object without following a host-controlled
/// link and prove that the directory entry still names the object we opened.
/// Cell-bound ciphertext already prevents useful substitution; rejecting links
/// also prevents path races from turning verification into an oracle over an
/// unexpected host file.
fn open_regular_nofollow(path: &Path) -> Result<(File, u64)> {
    let before = fs::symlink_metadata(path)?;
    if before.file_type().is_symlink() || !before.is_file() {
        return Err(BicDbError::Corruption {
            path: path.to_path_buf(),
            message: "immutable frame object must be a regular non-symlink file".to_string(),
        });
    }
    #[cfg(unix)]
    if before.nlink() != 1 {
        return Err(BicDbError::Corruption {
            path: path.to_path_buf(),
            message: "immutable frame object must not have hard links".to_string(),
        });
    }

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options.open(path)?;
    let opened = file.metadata()?;
    if !opened.is_file() || opened.len() != before.len() {
        return Err(BicDbError::Corruption {
            path: path.to_path_buf(),
            message: "immutable frame object changed while opening".to_string(),
        });
    }
    #[cfg(unix)]
    if opened.dev() != before.dev() || opened.ino() != before.ino() || opened.nlink() != 1 {
        return Err(BicDbError::Corruption {
            path: path.to_path_buf(),
            message: "immutable frame object identity changed while opening".to_string(),
        });
    }
    Ok((file, opened.len()))
}

fn read_frames_buffered(
    mut file: File,
    file_len: u64,
    path: &Path,
    expected_kind: FrameKind,
    encryption: &EncryptionRuntime,
    strict: bool,
) -> Result<(RecoveredFrames, FrameVerification)> {
    // Read the whole segment in one pass and split frames from the in-memory
    // buffer. The previous frame-at-a-time reader issued `stream_position` +
    // two `read_exact` per frame (three syscalls each), which on a multi-GB,
    // millions-of-frames segment turned recovery into a syscall storm. One bulk
    // read + a cursor walk keeps identical truncation/verification semantics.
    let mut data = Vec::with_capacity(file_len as usize);
    file.read_to_end(&mut data)?;
    let bytes = &data[..];

    let mut frames = Vec::new();
    let mut valid_up_to = 0_usize;
    let mut cursor = 0_usize;
    let mut verification = FrameVerification::default();

    while cursor < bytes.len() {
        let offset = cursor;
        if bytes.len().saturating_sub(cursor) < HEADER_LEN {
            if strict {
                return Err(encryption::tamper(
                    path,
                    format!("partial frame header at offset {offset}"),
                ));
            }
            break;
        }

        let mut header = [0_u8; HEADER_LEN];
        header.copy_from_slice(&bytes[cursor..cursor + HEADER_LEN]);
        cursor += HEADER_LEN;

        let Some((kind, flags, len, expected_checksum)) = decode_header(&header) else {
            if strict {
                return Err(encryption::tamper(
                    path,
                    format!("invalid frame header at offset {offset}"),
                ));
            }
            break;
        };

        if kind != expected_kind {
            if strict {
                return Err(encryption::tamper(
                    path,
                    format!("unexpected frame kind {:?} at offset {offset}", kind),
                ));
            }
            break;
        }

        if len > MAX_PAYLOAD_LEN {
            if strict {
                return Err(encryption::tamper(
                    path,
                    format!("frame payload is too large at offset {offset}: {len} bytes"),
                ));
            }
            break;
        }

        if bytes.len().saturating_sub(cursor) < len {
            if strict {
                return Err(encryption::tamper(
                    path,
                    format!("partial frame payload at offset {offset}"),
                ));
            }
            break;
        }

        let stored_payload = &bytes[cursor..cursor + len];
        cursor += len;

        if expected_checksum != checksum(kind, flags, len as u32, stored_payload) {
            if strict {
                return Err(encryption::tamper(
                    path,
                    format!("frame checksum mismatch at offset {offset}"),
                ));
            }
            break;
        }

        let payload = decode_payload(path, kind, flags, stored_payload, encryption)?;
        valid_up_to = cursor;
        update_verification(&mut verification, flags, valid_up_to as u64);
        frames.push(RecoveredFrame {
            offset: offset as u64,
            payload,
        });
    }

    drop(data);
    if !strict {
        settle_recovered_tail(&file, path, valid_up_to as u64, file_len)?;
    }

    Ok((
        RecoveredFrames {
            frames,
            truncated_bytes: file_len.saturating_sub(valid_up_to as u64),
        },
        verification,
    ))
}

/// Read only the frames at or beyond `start_offset` (the segment "tail"). Used by
/// the binary-snapshot tail-merge: the snapshot covers the segment prefix `[0,
/// start_offset)` and only appended frames past it need decoding. `start_offset`
/// MUST fall on a frame boundary (it is the segment's byte length at snapshot time,
/// and segments only grow by whole appended frames while a snapshot manifest exists;
/// rewrites invalidate the manifest). Returned frame offsets are absolute (from the
/// start of the file). Truncation of a torn trailing frame is applied exactly as in
/// the full reader, so semantics match a whole-file recovery of the same tail.
pub(crate) fn read_frames_from(
    path: &Path,
    start_offset: u64,
    expected_kind: FrameKind,
    encryption: &EncryptionRuntime,
) -> Result<RecoveredFrames> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    apply_append_failure_marker(&file, path)?;
    let file_len = file.metadata()?.len();
    if start_offset >= file_len {
        if start_offset == file_len {
            clear_poison(path);
        }
        return Ok(RecoveredFrames {
            frames: Vec::new(),
            truncated_bytes: file_len.saturating_sub(start_offset),
        });
    }

    file.seek(SeekFrom::Start(start_offset))?;
    let mut data = Vec::with_capacity((file_len - start_offset) as usize);
    file.read_to_end(&mut data)?;
    let bytes = &data[..];

    let mut frames = Vec::new();
    let mut valid_up_to = 0_usize; // relative to start_offset
    let mut cursor = 0_usize;

    while cursor < bytes.len() {
        let offset = cursor;
        if bytes.len().saturating_sub(cursor) < HEADER_LEN {
            break;
        }
        let mut header = [0_u8; HEADER_LEN];
        header.copy_from_slice(&bytes[cursor..cursor + HEADER_LEN]);
        cursor += HEADER_LEN;

        let Some((kind, flags, len, expected_checksum)) = decode_header(&header) else {
            break;
        };
        if kind != expected_kind || len > MAX_PAYLOAD_LEN {
            break;
        }
        if bytes.len().saturating_sub(cursor) < len {
            break;
        }
        let stored_payload = &bytes[cursor..cursor + len];
        cursor += len;
        if expected_checksum != checksum(kind, flags, len as u32, stored_payload) {
            break;
        }
        let payload = decode_payload(path, kind, flags, stored_payload, encryption)?;
        valid_up_to = cursor;
        frames.push(RecoveredFrame {
            offset: start_offset + offset as u64,
            payload,
        });
    }

    drop(data);
    // A torn trailing frame in the tail is truncated just like the full reader, so a
    // later append never lands after dead bytes (which a future full recovery would
    // otherwise cut, taking the good appends with it).
    let absolute_valid = start_offset + valid_up_to as u64;
    settle_recovered_tail(&file, path, absolute_valid, file_len)?;

    Ok(RecoveredFrames {
        frames,
        truncated_bytes: file_len.saturating_sub(absolute_valid),
    })
}

/// `len` bytes at `offset`, or `None` when the file is shorter.
pub(crate) fn read_bytes_at(path: &Path, offset: u64, len: usize) -> Result<Option<Vec<u8>>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    read_bytes_at_file(&mut file, offset, len)
}

/// [`read_bytes_at`] on an already open file (the same inode as a
/// following [`read_frames_readonly_from_file`]).
pub(crate) fn read_bytes_at_file(
    file: &mut File,
    offset: u64,
    len: usize,
) -> Result<Option<Vec<u8>>> {
    if file.metadata()?.len() < offset + len as u64 {
        return Ok(None);
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0_u8; len];
    file.read_exact(&mut bytes)?;
    Ok(Some(bytes))
}

/// Read-only tail reader for a log another thread may be appending to.
///
/// Unlike [`read_frames_from`] it never truncates: a frame still being written
/// simply ends the scan. Returns each complete frame's start and end offsets
/// with its decoded payload.
pub(crate) fn read_frames_readonly_from(
    path: &Path,
    start_offset: u64,
    expected_kind: FrameKind,
    encryption: &EncryptionRuntime,
) -> Result<Vec<(u64, u64, Vec<u8>)>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    read_frames_readonly_from_file(&mut file, path, start_offset, expected_kind, encryption)
}

/// [`read_frames_readonly_from`] on an already open file. A reader that
/// validates a resume position and then reads must use one descriptor for
/// both: a log cut renames a new file over `path` in between, and offsets
/// of the old file mean nothing in the new one.
pub(crate) fn read_frames_readonly_from_file(
    file: &mut File,
    path: &Path,
    start_offset: u64,
    expected_kind: FrameKind,
    encryption: &EncryptionRuntime,
) -> Result<Vec<(u64, u64, Vec<u8>)>> {
    let file_len = file.metadata()?.len();
    if start_offset >= file_len {
        return Ok(Vec::new());
    }
    file.seek(SeekFrom::Start(start_offset))?;
    let mut data = Vec::with_capacity((file_len - start_offset) as usize);
    file.read_to_end(&mut data)?;
    let mut frames = Vec::new();
    let mut cursor = 0_usize;
    while data.len().saturating_sub(cursor) >= HEADER_LEN {
        let start = cursor;
        let mut header = [0_u8; HEADER_LEN];
        header.copy_from_slice(&data[cursor..cursor + HEADER_LEN]);
        let Some((kind, flags, len, expected_checksum)) = decode_header(&header) else {
            break;
        };
        if kind != expected_kind || len > MAX_PAYLOAD_LEN {
            break;
        }
        cursor += HEADER_LEN;
        if data.len().saturating_sub(cursor) < len {
            break;
        }
        let stored = &data[cursor..cursor + len];
        cursor += len;
        if expected_checksum != checksum(kind, flags, len as u32, stored) {
            break;
        }
        let payload = decode_payload(path, kind, flags, stored, encryption)?;
        frames.push((
            start_offset + start as u64,
            start_offset + cursor as u64,
            payload,
        ));
    }
    Ok(frames)
}

#[cfg(feature = "mmap")]
fn read_frames_mmap(
    file: File,
    file_len: u64,
    path: &Path,
    expected_kind: FrameKind,
    encryption: &EncryptionRuntime,
    strict: bool,
) -> Result<(RecoveredFrames, FrameVerification)> {
    // Safety: this is a read-only mapping of an opened segment file. BicDB only
    // uses this during recovery/open before appending new frames to the segment.
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let bytes = &mmap[..];
    let mut frames = Vec::new();
    let mut valid_up_to = 0_usize;
    let mut cursor = 0_usize;
    let mut verification = FrameVerification::default();

    while cursor < bytes.len() {
        let offset = cursor;
        if bytes.len().saturating_sub(cursor) < HEADER_LEN {
            if strict {
                return Err(encryption::tamper(
                    path,
                    format!("partial frame header at offset {offset}"),
                ));
            }
            break;
        }

        let mut header = [0_u8; HEADER_LEN];
        header.copy_from_slice(&bytes[cursor..cursor + HEADER_LEN]);
        cursor += HEADER_LEN;

        let Some((kind, flags, len, expected_checksum)) = decode_header(&header) else {
            if strict {
                return Err(encryption::tamper(
                    path,
                    format!("invalid frame header at offset {offset}"),
                ));
            }
            break;
        };

        if kind != expected_kind {
            if strict {
                return Err(encryption::tamper(
                    path,
                    format!("unexpected frame kind {:?} at offset {offset}", kind),
                ));
            }
            break;
        }

        if len > MAX_PAYLOAD_LEN {
            if strict {
                return Err(encryption::tamper(
                    path,
                    format!("frame payload is too large at offset {offset}: {len} bytes"),
                ));
            }
            break;
        }

        if bytes.len().saturating_sub(cursor) < len {
            if strict {
                return Err(encryption::tamper(
                    path,
                    format!("partial frame payload at offset {offset}"),
                ));
            }
            break;
        }

        let stored_payload = &bytes[cursor..cursor + len];
        cursor += len;

        if expected_checksum != checksum(kind, flags, len as u32, stored_payload) {
            if strict {
                return Err(encryption::tamper(
                    path,
                    format!("frame checksum mismatch at offset {offset}"),
                ));
            }
            break;
        }

        let payload = decode_payload(path, kind, flags, stored_payload, encryption)?;
        valid_up_to = cursor;
        update_verification(&mut verification, flags, valid_up_to as u64);
        frames.push(RecoveredFrame {
            offset: offset as u64,
            payload,
        });
    }

    drop(mmap);
    if !strict {
        settle_recovered_tail(&file, path, valid_up_to as u64, file_len)?;
    }

    Ok((
        RecoveredFrames {
            frames,
            truncated_bytes: file_len.saturating_sub(valid_up_to as u64),
        },
        verification,
    ))
}

pub(crate) fn sync_file(path: &Path) -> Result<()> {
    ensure_parent(path)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    file.sync_all()?;
    Ok(())
}

pub(crate) fn write_atomic(path: &Path, bytes: &[u8], fsync: bool) -> Result<()> {
    replace_durably(
        path,
        ReplaceContents::Plain(bytes),
        fsync,
        "",
        FailClosed::No,
    )
}

/// Atomically replace one immutable framed object while binding ciphertext to
/// its final path. This is the durable primitive for cell-bound index/search
/// sidecars and avoids the classic temporary-name AEAD binding error.
pub(crate) fn write_frame_atomic(
    path: &Path,
    kind: FrameKind,
    payload: &[u8],
    fsync: bool,
    encryption: &EncryptionRuntime,
) -> Result<()> {
    replace_durably(
        path,
        ReplaceContents::Framed {
            kind,
            payload,
            encryption,
        },
        fsync,
        "",
        FailClosed::No,
    )
}

/// What a failure of a durable file replacement means for the database
/// (see `docs/durability-failure-handling.md`, section 7).
#[derive(Clone, Copy)]
pub(crate) enum FailClosed<'a> {
    /// Never fatal: the error is returned and the handle stays healthy.
    No,
    /// Fatal once the rename happened, that is when the directory `fsync`
    /// after it fails: the new file is live now and may vanish at the next
    /// power loss, while the caller reports a failure. A failure up to and
    /// including the rename itself (which is atomic: on error nothing was
    /// renamed) leaves the old file in place and is not fatal.
    AfterRename(&'a bicdb_page::DurabilityHealth),
    /// Fatal at every step: the caller has already changed its memory to
    /// the new contents, so any failure leaves memory ahead of the disk.
    Always(&'a bicdb_page::DurabilityHealth),
}

impl FailClosed<'_> {
    fn health(&self) -> Option<&bicdb_page::DurabilityHealth> {
        match self {
            Self::No => None,
            Self::AfterRename(health) | Self::Always(health) => Some(health),
        }
    }
}

pub(crate) enum ReplaceContents<'a> {
    Plain(&'a [u8]),
    Framed {
        kind: FrameKind,
        payload: &'a [u8],
        encryption: &'a EncryptionRuntime,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplaceStep {
    Write,
    Sync,
    Rename,
    DirSync,
}

impl ReplaceStep {
    fn name(self) -> &'static str {
        match self {
            Self::Write => "write",
            Self::Sync => "sync",
            Self::Rename => "rename",
            Self::DirSync => "dir_sync",
        }
    }
}

/// The durable replacement protocol: write a new temporary file, `fsync`
/// it, rename it over `path`, `fsync` the directory. With `fsync` off only
/// the write and the rename happen (nothing is crash durable then).
///
/// `site` names the file for fault injection and for the recorded failure:
/// injection sites `<site>.write`, `<site>.sync`, `<site>.rename` and
/// `<site>.dir_sync` fail the corresponding step, and crash points
/// `<site>.after_write`, `<site>.after_sync`, `<site>.after_rename` and
/// `<site>.after_dir_sync` follow each step. An empty site has neither.
///
/// Under `policy` a fatal failure fences `health` and returns
/// [`BicDbError::WriteFailed`] with site `<site>.<step>`; a non-fatal one
/// returns the I/O error. A health that is already failed refuses the
/// replacement before anything is written.
pub(crate) fn replace_durably(
    path: &Path,
    contents: ReplaceContents<'_>,
    fsync: bool,
    site: &str,
    policy: FailClosed<'_>,
) -> Result<()> {
    if let Some(health) = policy.health() {
        health.check().map_err(crate::error::durability_error)?;
    }
    let result = replace_durably_steps(path, contents, fsync, site);
    let Err((step, error)) = result else {
        return Ok(());
    };
    let fatal = match policy {
        FailClosed::No => None,
        FailClosed::AfterRename(health) => (step == ReplaceStep::DirSync).then_some(health),
        FailClosed::Always(health) => Some(health),
    };
    let Some(health) = fatal else {
        return Err(error);
    };
    let recorded = if site.is_empty() {
        format!("atomic_file.{}", step.name())
    } else {
        format!("{site}.{}", step.name())
    };
    let detail = format!("{}: {error}", path.display());
    health.record_failure(&recorded, &detail);
    Err(BicDbError::WriteFailed {
        site: recorded,
        detail,
    })
}

fn replace_durably_steps(
    path: &Path,
    contents: ReplaceContents<'_>,
    fsync: bool,
    site: &str,
) -> std::result::Result<(), (ReplaceStep, BicDbError)> {
    use bicdb_page::test_faults;
    let inject = |step: ReplaceStep| -> std::result::Result<(), (ReplaceStep, BicDbError)> {
        if site.is_empty() {
            return Ok(());
        }
        test_faults::io_fault(&format!("{site}.{}", step.name()), path)
            .map_err(|error| (step, BicDbError::from(error)))
    };
    let crash = |point: &str| {
        if !site.is_empty() {
            test_faults::crash_point(&format!("{site}.{point}"));
        }
    };
    let at = |step: ReplaceStep| move |error: BicDbError| (step, error);
    ensure_parent(path).map_err(at(ReplaceStep::Write))?;
    let tmp_path = tmp_path(path);
    let result = (|| -> std::result::Result<(), (ReplaceStep, BicDbError)> {
        inject(ReplaceStep::Write)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW).mode(0o600);
        let file = options
            .open(&tmp_path)
            .map_err(|error| (ReplaceStep::Write, error.into()))?;
        let data_synced = match contents {
            ReplaceContents::Plain(bytes) => {
                let mut file = file;
                file.write_all(bytes)
                    .map_err(|error| (ReplaceStep::Write, error.into()))?;
                crash("after_write");
                if fsync {
                    inject(ReplaceStep::Sync)?;
                    file.sync_all()
                        .map_err(|error| (ReplaceStep::Sync, error.into()))?;
                    crash("after_sync");
                }
                fsync
            }
            ReplaceContents::Framed {
                kind,
                payload,
                encryption,
            } => {
                drop(file);
                // The framed appender writes and (with `fsync`) syncs in one
                // call; a failure is attributed to the write.
                if fsync {
                    inject(ReplaceStep::Sync)?;
                }
                append_frames_for_object(
                    &tmp_path,
                    path,
                    kind,
                    &[payload.to_vec()],
                    fsync,
                    &CompressionConfig::disabled(),
                    encryption,
                )
                .map_err(|error| {
                    // The temporary file is discarded whole, so a failed
                    // tail repair inside it leaves nothing of unknown
                    // status: drop the frame log's poison and marker for it
                    // and report an ordinary write failure; the policy
                    // decides whether that is fatal.
                    clear_poison(&tmp_path);
                    let _ = fs::remove_file(append_failure_marker_path(&tmp_path));
                    let error = match error {
                        BicDbError::WriteFailed { site, detail } => {
                            BicDbError::Io(std::io::Error::other(format!("{site}: {detail}")))
                        }
                        other => other,
                    };
                    (ReplaceStep::Write, error)
                })?;
                crash("after_write");
                if fsync {
                    crash("after_sync");
                }
                fsync
            }
        };
        inject(ReplaceStep::Rename)?;
        let previous = if test_faults::volatile_dir_enabled() {
            volatile_previous_link(path)
        } else {
            None
        };
        fs::rename(&tmp_path, path).map_err(|error| (ReplaceStep::Rename, error.into()))?;
        if test_faults::volatile_dir_enabled() {
            test_faults::register_volatile_rename(path, previous, data_synced);
        }
        crash("after_rename");
        if fsync {
            inject(ReplaceStep::DirSync)?;
            sync_parent(path).map_err(at(ReplaceStep::DirSync))?;
            crash("after_dir_sync");
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    result
}

/// Power-loss emulation: keep the file a rename is about to replace, so a
/// crash before the directory `fsync` can put it back.
fn volatile_previous_link(path: &Path) -> Option<PathBuf> {
    let link = path.with_file_name(format!(
        ".{}.{}.volatile-previous",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("bicdb"),
        Uuid::new_v4().simple()
    ));
    fs::hard_link(path, &link).ok().map(|()| link)
}

fn encode_header(kind: FrameKind, flags: u16, payload: &[u8]) -> [u8; HEADER_LEN] {
    let len = payload.len() as u32;
    let checksum = checksum(kind, flags, len, payload);
    let mut header = [0_u8; HEADER_LEN];
    header[0..4].copy_from_slice(MAGIC);
    header[4] = VERSION;
    header[5] = kind as u8;
    header[6..8].copy_from_slice(&flags.to_le_bytes());
    header[8..12].copy_from_slice(&len.to_le_bytes());
    header[12..16].copy_from_slice(&checksum.to_le_bytes());
    header
}

fn decode_header(header: &[u8; HEADER_LEN]) -> Option<(FrameKind, u16, usize, u32)> {
    if &header[0..4] != MAGIC || header[4] != VERSION {
        return None;
    }

    let kind = FrameKind::from_u8(header[5])?;
    let flags = u16::from_le_bytes(header[6..8].try_into().ok()?);
    if flags & !KNOWN_FLAGS != 0 {
        return None;
    }
    let len = u32::from_le_bytes(header[8..12].try_into().ok()?) as usize;
    let checksum = u32::from_le_bytes(header[12..16].try_into().ok()?);
    Some((kind, flags, len, checksum))
}

fn checksum(kind: FrameKind, flags: u16, len: u32, payload: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&[VERSION, kind as u8]);
    hasher.update(&flags.to_le_bytes());
    hasher.update(&len.to_le_bytes());
    hasher.update(payload);
    hasher.finalize()
}

fn encode_payload(
    path: &Path,
    payload: &[u8],
    kind: FrameKind,
    compression: &CompressionConfig,
    encryption: &EncryptionRuntime,
) -> Result<(Vec<u8>, u16)> {
    let (mut stored, mut flags) = encode_compressed_payload(payload, compression)?;
    if encryption.is_enabled() {
        flags |= FLAG_ENCRYPTED;
        stored = encryption.encrypt_for(
            kind.encryption_purpose(),
            path,
            &stored,
            &frame_aad(kind, flags),
        )?;
    }
    if stored.len() > MAX_PAYLOAD_LEN {
        return Err(BicDbError::Corruption {
            path: path.to_path_buf(),
            message: format!("stored frame payload is too large: {} bytes", stored.len()),
        });
    }
    Ok((stored, flags))
}

fn encode_compressed_payload(
    payload: &[u8],
    compression: &CompressionConfig,
) -> Result<(Vec<u8>, u16)> {
    if !compression.enabled || payload.len() < compression.min_bytes {
        return Ok((payload.to_vec(), 0));
    }

    // Without the `compression` feature an enabled config degrades to plain
    // frames: the on-disk format stays valid and readable by every build.
    #[cfg(not(feature = "compression"))]
    {
        return Ok((payload.to_vec(), 0));
    }

    #[cfg(feature = "compression")]
    {
        let compressed = zstd::stream::encode_all(payload, compression.level)?;
        if compressed.len() < payload.len() {
            Ok((compressed, FLAG_ZSTD))
        } else {
            Ok((payload.to_vec(), 0))
        }
    }
}

fn decode_payload(
    path: &Path,
    kind: FrameKind,
    flags: u16,
    payload: &[u8],
    encryption: &EncryptionRuntime,
) -> Result<Vec<u8>> {
    if flags & FLAG_ENCRYPTED != 0 {
        let decrypted = encryption.decrypt_for(
            kind.encryption_purpose(),
            path,
            payload,
            &frame_aad(kind, flags),
        )?;
        return decode_compressed_payload(path, flags & !FLAG_ENCRYPTED, &decrypted);
    }

    decode_compressed_payload(path, flags, payload)
}

fn decode_compressed_payload(path: &Path, flags: u16, payload: &[u8]) -> Result<Vec<u8>> {
    if flags & FLAG_ZSTD == 0 {
        return Ok(payload.to_vec());
    }

    #[cfg(not(feature = "compression"))]
    {
        let _ = payload;
        Err(BicDbError::Corruption {
            path: path.to_path_buf(),
            message: "segment contains zstd-compressed frames, but this build has no `compression` support"
                .to_string(),
        })
    }

    #[cfg(feature = "compression")]
    {
        let ratio_limit = payload.len().saturating_mul(MAX_DECOMPRESSION_RATIO);
        let limit = MAX_PAYLOAD_LEN.min(ratio_limit).saturating_add(1);
        let decoder = zstd::stream::read::Decoder::new(payload)?;
        let mut decoded = Vec::with_capacity(limit.min(1024 * 1024));
        decoder.take(limit as u64).read_to_end(&mut decoded)?;
        if decoded.len() >= limit {
            return Err(BicDbError::Corruption {
                path: path.to_path_buf(),
                message: format!(
                    "decompressed frame payload exceeds the {}-byte/{}x bound",
                    MAX_PAYLOAD_LEN, MAX_DECOMPRESSION_RATIO
                ),
            });
        }
        Ok(decoded)
    }
}

fn frame_aad(kind: FrameKind, flags: u16) -> [u8; 8] {
    let mut aad = [0_u8; 8];
    aad[0..4].copy_from_slice(MAGIC);
    aad[4] = VERSION;
    aad[5] = kind as u8;
    aad[6..8].copy_from_slice(&flags.to_le_bytes());
    aad
}

/// Rewrap one complete encrypted frame file without decompressing or
/// deserializing its payloads. Both object paths participate in their
/// respective cell-bound AEAD contexts, so this is also a strict proof that
/// every source frame authenticates under the retiring key and identity.
pub(crate) fn reencrypt_bound_frame_file(
    source_path: &Path,
    target_path: &Path,
    source_encryption: &EncryptionRuntime,
    target_encryption: &EncryptionRuntime,
    fsync: bool,
) -> Result<u64> {
    ensure_parent(target_path)?;
    let (mut source, _) = open_regular_nofollow(source_path)?;

    let mut target_options = OpenOptions::new();
    target_options.create_new(true).write(true);
    #[cfg(unix)]
    target_options.custom_flags(libc::O_NOFOLLOW).mode(0o600);
    let mut target = target_options.open(target_path)?;
    let mut frames = 0_u64;

    loop {
        let mut header = [0_u8; HEADER_LEN];
        let mut read = 0_usize;
        while read < HEADER_LEN {
            let count = source.read(&mut header[read..])?;
            if count == 0 {
                if read == 0 {
                    if fsync {
                        target.sync_all()?;
                    }
                    return Ok(frames);
                }
                return Err(BicDbError::Corruption {
                    path: source_path.to_path_buf(),
                    message: "key rotation found a truncated frame header".to_string(),
                });
            }
            read += count;
        }
        let (kind, flags, payload_len, expected_checksum) =
            decode_header(&header).ok_or_else(|| BicDbError::Corruption {
                path: source_path.to_path_buf(),
                message: "key rotation found an invalid frame header".to_string(),
            })?;
        if payload_len > MAX_PAYLOAD_LEN {
            return Err(BicDbError::Corruption {
                path: source_path.to_path_buf(),
                message: "key rotation found an oversized frame".to_string(),
            });
        }
        if flags & FLAG_ENCRYPTED == 0 {
            return Err(BicDbError::EncryptionKeyInvalid(format!(
                "cell-bound key rotation refuses plaintext frame in {}",
                source_path.display()
            )));
        }
        let mut payload = vec![0_u8; payload_len];
        source
            .read_exact(&mut payload)
            .map_err(|error| BicDbError::Corruption {
                path: source_path.to_path_buf(),
                message: format!("key rotation found a truncated frame payload: {error}"),
            })?;
        if checksum(kind, flags, payload_len as u32, &payload) != expected_checksum {
            return Err(BicDbError::Corruption {
                path: source_path.to_path_buf(),
                message: "key rotation found a frame checksum mismatch".to_string(),
            });
        }
        let compressed = source_encryption.decrypt_for(
            kind.encryption_purpose(),
            source_path,
            &payload,
            &frame_aad(kind, flags),
        )?;
        let rotated = target_encryption.encrypt_for(
            kind.encryption_purpose(),
            target_path,
            &compressed,
            &frame_aad(kind, flags),
        )?;
        if rotated.len() > MAX_PAYLOAD_LEN {
            return Err(BicDbError::Corruption {
                path: target_path.to_path_buf(),
                message: "rotated frame exceeds the storage bound".to_string(),
            });
        }
        target.write_all(&encode_header(kind, flags, &rotated))?;
        target.write_all(&rotated)?;
        frames = frames.saturating_add(1);
    }
}

fn update_verification(verification: &mut FrameVerification, flags: u16, bytes_checked: u64) {
    verification.frame_count += 1;
    if flags & FLAG_ENCRYPTED != 0 {
        verification.encrypted_frames += 1;
    } else {
        verification.unencrypted_frames += 1;
    }
    verification.bytes_checked = bytes_checked;
}

fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

// wasm/WASI (and OPFS underneath) has no directory fd to fsync; rename
// durability there is the storage layer's job, so this is a no-op.
// Windows cannot open a directory as a File either (access denied), and NTFS
// metadata durability rides on the file-handle sync_all, so it is also a no-op.
#[cfg(any(target_arch = "wasm32", windows))]
fn sync_parent(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(not(any(target_arch = "wasm32", windows)))]
fn sync_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        let dir = File::open(parent)?;
        dir.sync_all()?;
        bicdb_page::test_faults::directory_synced(parent);
    }
    Ok(())
}

/// `fsync` the directory that holds `path` (a no-op where directories
/// cannot be synced).
pub(crate) fn sync_parent_directory(path: &Path) -> Result<()> {
    sync_parent(path)
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.to_path_buf();
    let tmp_name = format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("bicdb"),
        Uuid::new_v4().simple()
    );
    tmp.set_file_name(tmp_name);
    tmp
}

pub(crate) fn total_dir_size(path: &Path) -> Result<u64> {
    let mut total = 0_u64;
    if !path.exists() {
        return Ok(0);
    }

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total += total_dir_size(&entry.path())?;
        } else if metadata.is_file() {
            total += metadata.len();
        }
    }
    Ok(total)
}

pub(crate) fn seek_to_end(path: &Path) -> Result<u64> {
    ensure_parent(path)?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    Ok(file.seek(SeekFrom::End(0))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_append_sync_failure_repairs_before_retry() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("cached.wal");
        let mut handle = open_append_handle(&path, true).unwrap();
        write_raw(&mut handle, &path, b"previous").unwrap();
        handle.sync_after_write = true;
        let error = write_raw_with_repair(
            &mut handle,
            b"next",
            |file, bytes, explicit_sync| {
                assert!(explicit_sync);
                append_and_sync(file, bytes, explicit_sync, |_| {
                    Err(std::io::Error::other("injected sync failure"))
                })
            },
            |file, len, durable| {
                assert!(durable);
                assert_eq!(file.metadata()?.len(), 12);
                assert_eq!(len, 8);
                file.set_len(len)?;
                file.sync_all()
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("injected sync failure"));
        assert_eq!(fs::read(&path).unwrap(), b"previous");
        handle.check_writable().unwrap();
        write_raw(&mut handle, &path, b"next").unwrap();
        assert_eq!(fs::read(path).unwrap(), b"previousnext");
    }

    #[test]
    fn cached_append_failed_repair_refuses_all_later_writes() {
        // Both an unsuccessful truncate and an unsuccessful sync of the
        // truncation must latch the failure, including for empty commits.
        for truncate_succeeds in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("cached.wal");
            let mut handle = open_append_handle(&path, true).unwrap();
            write_raw(&mut handle, &path, b"previous").unwrap();
            let error = write_raw_with_repair(
                &mut handle,
                b"next",
                |file, bytes, _| {
                    file.write_all(&bytes[..2])?;
                    Err(std::io::Error::other("injected short append"))
                },
                |file, len, durable| {
                    assert!(durable);
                    if truncate_succeeds {
                        file.set_len(len)?;
                    }
                    Err(std::io::Error::other("injected repair failure"))
                },
            )
            .unwrap_err();
            assert!(error.to_string().contains("injected repair failure"));
            let after_failure = fs::read(&path).unwrap();
            assert_eq!(
                after_failure,
                if truncate_succeeds {
                    b"previous".as_slice()
                } else {
                    b"previousne".as_slice()
                }
            );
            assert!(write_raw(&mut handle, &path, b"later").is_err());
            assert!(write_raw(&mut handle, &path, b"").is_err());
            assert_eq!(fs::read(&path).unwrap(), after_failure);
        }
    }

    #[test]
    fn cached_append_explicit_sync_waits_and_propagates_failure() {
        let mut bytes = Vec::new();
        let calls = std::cell::Cell::new(0);
        append_and_sync(&mut bytes, b"first", true, |written| {
            assert_eq!(written, b"first");
            calls.set(calls.get() + 1);
            Ok(())
        })
        .unwrap();
        let error = append_and_sync(&mut bytes, b"second", true, |written| {
            assert_eq!(written, b"firstsecond");
            calls.set(calls.get() + 1);
            Err(std::io::Error::other("injected disk sync failure"))
        })
        .unwrap_err();
        assert_eq!(calls.get(), 2);
        assert_eq!(error.to_string(), "injected disk sync failure");
        append_and_sync(&mut bytes, b"third", false, |_| {
            panic!("O_DSYNC and non-durable handles must not issue an extra sync")
        })
        .unwrap();
    }

    #[test]
    fn cached_append_write_failure_does_not_report_success_or_sync() {
        struct FailingWriter;
        impl Write for FailingWriter {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("injected write failure"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let error = append_and_sync(&mut FailingWriter, b"record", true, |_| {
            panic!("sync must not mask a write failure")
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "injected write failure");
    }

    #[test]
    fn cached_append_retains_platform_sync_requirement() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("cached.wal");
        let mut durable = open_append_handle(&path, true).unwrap();
        assert_eq!(durable.sync_after_write, !cfg!(unix));
        write_raw(&mut durable, &path, b"one").unwrap();
        // Exercise the non-O_DSYNC fallback on every test platform.
        let (file, sync_after_write) =
            open_append_file(&path, AppendSyncMode::ExplicitDataSync).unwrap();
        let mut fallback = AppendHandle {
            file,
            sync_after_write,
            durable: true,
            failed_repair: None,
        };
        assert!(fallback.sync_after_write);
        write_raw(&mut fallback, &path, b"two").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"onetwo");
        assert!(!open_append_handle(&path, false).unwrap().sync_after_write);
    }

    #[test]
    fn direct_json_frames_match_prepared_payloads_and_rollback_failed_serialization() {
        let values = vec![
            serde_json::json!({"type":"tx_write","metadata":{"name":"héllo\\\"","value":42}}),
            serde_json::json!({"type":"tx_commit","commit_seq":12345,"timestamp":42}),
            serde_json::json!([null, true, i64::MIN, i64::MAX, 1.5]),
        ];
        let payloads: Vec<Vec<u8>> = values
            .iter()
            .map(|value| serde_json::to_vec(value).unwrap())
            .collect();
        let expected = encode_frames_to_bytes(
            Path::new("test-wal"),
            FrameKind::Transaction,
            &payloads,
            &CompressionConfig::disabled(),
            &EncryptionRuntime::disabled(),
        )
        .unwrap();
        let mut direct = Vec::new();
        for value in &values {
            append_plain_json_frame(&mut direct, FrameKind::Transaction, value).unwrap();
        }
        assert_eq!(direct, expected);
        assert_eq!(first_plain_frame_payload(&direct), payloads[0]);

        struct Fails;
        impl Serialize for Fails {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                use serde::ser::SerializeSeq;
                let mut sequence = serializer.serialize_seq(Some(2))?;
                sequence.serialize_element(&1)?;
                Err(serde::ser::Error::custom(
                    "intentional serialization failure",
                ))
            }
        }
        assert!(append_plain_json_frame(&mut direct, FrameKind::Transaction, &Fails).is_err());
        assert_eq!(
            direct, expected,
            "a failed frame must not damage the completed prefix"
        );
    }

    #[test]
    fn plain_frame_batch_is_byte_identical_without_intermediate_payload_copies() {
        let path = Path::new("differential-wal");
        let encryption = EncryptionRuntime::disabled();
        let payloads = vec![
            Vec::new(),
            r#"{"text":"héllo","value":42}"#.as_bytes().to_vec(),
            (0..=255).collect::<Vec<u8>>(),
            vec![b'x'; 4096],
        ];
        for compression in [
            CompressionConfig::disabled(),
            CompressionConfig::zstd(3, 1024),
        ] {
            for kind in [FrameKind::Transaction, FrameKind::Record] {
                let mut expected = Vec::new();
                for payload in &payloads {
                    let (stored, flags) =
                        encode_payload(path, payload, kind, &compression, &encryption).unwrap();
                    expected.extend_from_slice(&encode_header(kind, flags, &stored));
                    expected.extend_from_slice(&stored);
                }
                let actual =
                    encode_frames_to_bytes(path, kind, &payloads, &compression, &encryption)
                        .unwrap();
                assert_eq!(actual, expected);
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn strict_immutable_open_rejects_symbolic_and_hard_links() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.bicf");
        fs::write(&target, b"not examined after identity rejection").unwrap();

        let symbolic = dir.path().join("symbolic.bicf");
        symlink(&target, &symbolic).unwrap();
        assert!(matches!(
            open_regular_nofollow(&symbolic),
            Err(BicDbError::Corruption { .. }) | Err(BicDbError::Io(_))
        ));

        let hard = dir.path().join("hard.bicf");
        fs::hard_link(&target, &hard).unwrap();
        assert!(matches!(
            open_regular_nofollow(&hard),
            Err(BicDbError::Corruption { .. })
        ));
        assert!(matches!(
            open_regular_nofollow(&target),
            Err(BicDbError::Corruption { .. })
        ));
    }

    #[test]
    fn stream_frames_matches_read_frames_across_recovery_shapes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.seg");
        let encryption = EncryptionRuntime::disabled();
        let compression = CompressionConfig::default();
        let payloads: Vec<Vec<u8>> = (0..500u32)
            .map(|index| {
                format!(
                    "{{\"n\":{index},\"body\":\"{}\"}}",
                    "x".repeat(index as usize % 97)
                )
                .into_bytes()
            })
            .collect();
        append_frames(
            &path,
            FrameKind::Event,
            &payloads,
            false,
            &compression,
            &encryption,
        )
        .unwrap();

        // Torn tail: append garbage that recovery must truncate away.
        {
            use std::io::Write;
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&[0xAB; 21]).unwrap();
        }

        let mut streamed = Vec::new();
        let truncated = stream_frames(&path, FrameKind::Event, &encryption, |offset, payload| {
            streamed.push((offset, payload));
            Ok(())
        })
        .unwrap();
        assert_eq!(truncated, 21);
        assert_eq!(streamed.len(), payloads.len());

        // The reference reader over the (now truncated) file agrees exactly.
        let recovered = read_frames(
            &path,
            FrameKind::Event,
            SegmentReadMode::Buffered,
            &encryption,
        )
        .unwrap();
        assert_eq!(recovered.frames.len(), streamed.len());
        for (frame, (offset, payload)) in recovered.frames.iter().zip(&streamed) {
            assert_eq!(frame.offset, *offset);
            assert_eq!(&frame.payload, payload);
        }

        // A corrupted mid-file checksum is damage, not a torn tail: both
        // readers refuse it the same way and leave the file unchanged.
        let mid = recovered.frames[300].offset;
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = OpenOptions::new().write(true).open(&path).unwrap();
            file.seek(SeekFrom::Start(mid + HEADER_LEN as u64 + 2))
                .unwrap();
            file.write_all(&[0xFF]).unwrap();
        }
        let damaged = fs::read(&path).unwrap();
        let mut streamed = Vec::new();
        let error = stream_frames(&path, FrameKind::Event, &encryption, |offset, payload| {
            streamed.push((offset, payload));
            Ok(())
        })
        .unwrap_err();
        assert!(matches!(error, BicDbError::Corruption { .. }), "{error}");
        assert_eq!(streamed.len(), 300);
        let error = read_frames(
            &path,
            FrameKind::Event,
            SegmentReadMode::Buffered,
            &encryption,
        )
        .unwrap_err();
        assert!(matches!(error, BicDbError::Corruption { .. }), "{error}");
        assert_eq!(fs::read(&path).unwrap(), damaged);

        // Empty file: no frames, no truncation.
        let empty = dir.path().join("empty.seg");
        let truncated =
            stream_frames(&empty, FrameKind::Event, &encryption, |_, _| Ok(())).unwrap();
        assert_eq!(truncated, 0);
    }

    #[test]
    fn append_frames_returns_offsets_without_per_frame_metadata_refresh() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("records.seg");
        let encryption = EncryptionRuntime::disabled();
        let payloads = vec![b"alpha".to_vec(), b"beta-value".to_vec(), b"gamma".to_vec()];

        let offsets = append_frames(
            &path,
            FrameKind::Record,
            &payloads,
            false,
            &CompressionConfig::disabled(),
            &encryption,
        )
        .unwrap();

        assert_eq!(
            offsets,
            vec![
                0,
                (HEADER_LEN + payloads[0].len()) as u64,
                (HEADER_LEN + payloads[0].len() + HEADER_LEN + payloads[1].len()) as u64,
            ]
        );
        let recovered = read_frames(
            &path,
            FrameKind::Record,
            SegmentReadMode::Buffered,
            &encryption,
        )
        .unwrap();
        assert_eq!(
            recovered
                .frames
                .iter()
                .map(|frame| frame.offset)
                .collect::<Vec<_>>(),
            offsets
        );
        assert_eq!(
            recovered
                .frames
                .iter()
                .map(|frame| frame.payload.clone())
                .collect::<Vec<_>>(),
            payloads
        );

        let more_payloads = vec![b"delta".to_vec()];
        let more_offsets = append_frames(
            &path,
            FrameKind::Record,
            &more_payloads,
            false,
            &CompressionConfig::disabled(),
            &encryption,
        )
        .unwrap();
        assert_eq!(
            more_offsets,
            vec![offsets[2] + (HEADER_LEN + payloads[2].len()) as u64]
        );
    }

    #[test]
    fn append_frames_open_datasync_round_trips_offsets_and_payloads() {
        // The sync-mode counters are process-wide: other tests appending in
        // parallel bump them too, so this test measures its own delta
        // rather than resetting and asserting an absolute count.
        let (_, open_datasyncs_before) = append_sync_mode_counts();
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("transactions.log");
        let encryption = EncryptionRuntime::disabled();
        let payloads = vec![b"first".to_vec(), b"second".to_vec()];

        let offsets = append_frames_open_datasync(
            &path,
            FrameKind::Transaction,
            &payloads,
            true,
            &CompressionConfig::disabled(),
            &encryption,
        )
        .unwrap();

        assert_eq!(offsets, vec![0, (HEADER_LEN + payloads[0].len()) as u64]);
        let (_explicit_syncs, open_datasyncs) = append_sync_mode_counts();
        assert!(
            open_datasyncs > open_datasyncs_before,
            "the append went through the open-datasync path ({open_datasyncs_before} -> {open_datasyncs})"
        );

        let recovered = read_frames(
            &path,
            FrameKind::Transaction,
            SegmentReadMode::Buffered,
            &encryption,
        )
        .unwrap();
        assert_eq!(
            recovered
                .frames
                .iter()
                .map(|frame| frame.payload.clone())
                .collect::<Vec<_>>(),
            payloads
        );
    }

    #[test]
    fn stale_online_backup_owner_does_not_block_storage_write() {
        let temp = tempfile::tempdir().unwrap();
        let segments = temp.path().join("segments");
        fs::create_dir_all(&segments).unwrap();
        initialize_backup_write_gate(temp.path()).unwrap();
        let path = segments.join("records.seg");
        let lock = temp.path().join(BACKUP_ONLINE_LOCK);
        fs::write(&lock, b"crashed-owner").unwrap();

        append_frame(
            &path,
            FrameKind::Record,
            b"survives-stale-lock",
            false,
            &EncryptionRuntime::disabled(),
        )
        .unwrap();

        assert!(lock.exists());
        let recovered = read_frames(
            &path,
            FrameKind::Record,
            SegmentReadMode::Buffered,
            &EncryptionRuntime::disabled(),
        )
        .unwrap();
        assert_eq!(recovered.frames[0].payload, b"survives-stale-lock");
    }

    #[test]
    fn backup_gate_excludes_writes_without_an_acquisition_gap() {
        use std::sync::mpsc;

        let temp = tempfile::tempdir().unwrap();
        initialize_backup_write_gate(temp.path()).unwrap();
        let storage_path = temp.path().join("segments/records.seg");

        let writer = acquire_backup_write_guard(&storage_path)
            .unwrap()
            .expect("opened databases have a write gate");
        let root = temp.path().to_path_buf();
        let (backup_tx, backup_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let guard = acquire_database_backup_exclusive_guard(&root).unwrap();
            backup_tx.send(guard).unwrap();
        });
        assert!(backup_rx.recv_timeout(Duration::from_millis(50)).is_err());
        drop(writer);
        let backup = backup_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let write_path = storage_path.clone();
        let (writer_tx, writer_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let guard = acquire_backup_write_guard(&write_path)
                .unwrap()
                .expect("opened databases have a write gate");
            writer_tx.send(guard).unwrap();
        });
        assert!(writer_rx.recv_timeout(Duration::from_millis(50)).is_err());
        drop(backup);
        drop(writer_rx.recv_timeout(Duration::from_secs(2)).unwrap());
    }

    // ---- Torn-tail recovery and failed-append repair of the generic frame
    // appender. See docs/durability-failure-handling.md, section 7.4.

    fn frame_payloads(count: usize, salt: &str) -> Vec<Vec<u8>> {
        (0..count)
            .map(|index| {
                format!(
                    "{{\"{salt}\":{index},\"pad\":\"{}\"}}",
                    "p".repeat(index * 7 % 41)
                )
                .into_bytes()
            })
            .collect()
    }

    /// Every recovery reader, applied to `path`. Returns the payloads it
    /// accepted, or its error.
    fn recover_with(reader: &str, path: &Path) -> Result<Vec<Vec<u8>>> {
        let encryption = EncryptionRuntime::disabled();
        match reader {
            "buffered" => read_frames(
                path,
                FrameKind::Sync,
                SegmentReadMode::Buffered,
                &encryption,
            )
            .map(|r| r.frames.into_iter().map(|f| f.payload).collect()),
            "mmap" => read_frames(path, FrameKind::Sync, SegmentReadMode::Mmap, &encryption)
                .map(|r| r.frames.into_iter().map(|f| f.payload).collect()),
            "stream" => {
                let mut out = Vec::new();
                stream_frames(path, FrameKind::Sync, &encryption, |_, payload| {
                    out.push(payload);
                    Ok(())
                })
                .map(|_| out)
            }
            // The snapshot tail reader, started at the first frame boundary.
            "from" => {
                let first = HEADER_LEN as u64 + frame_payloads(1, "a")[0].len() as u64;
                let mut out = vec![frame_payloads(1, "a")[0].clone()];
                read_frames_from(path, first, FrameKind::Sync, &encryption).map(|r| {
                    out.extend(r.frames.into_iter().map(|f| f.payload));
                    out
                })
            }
            _ => unreachable!(),
        }
    }

    const READERS: [&str; 4] = ["buffered", "mmap", "stream", "from"];

    fn append_plain(path: &Path, payloads: &[Vec<u8>], fsync: bool) -> Result<Vec<u64>> {
        append_frames(
            path,
            FrameKind::Sync,
            payloads,
            fsync,
            &CompressionConfig::disabled(),
            &EncryptionRuntime::disabled(),
        )
    }

    /// A torn last frame, cut at every byte offset inside it, and a complete
    /// last frame with any one byte damaged: every reader truncates exactly
    /// the last frame, keeps every frame before it, and the log then
    /// accepts appends on the repaired boundary.
    #[test]
    fn a_torn_or_damaged_last_frame_at_every_byte_offset_is_cut_to_the_last_good_frame() {
        let dir = tempfile::tempdir().unwrap();
        let reference = dir.path().join("reference.log");
        let payloads = frame_payloads(5, "a");
        let offsets = append_plain(&reference, &payloads, false).unwrap();
        let full = fs::read(&reference).unwrap();
        let last = offsets[4] as usize;
        let kept = payloads[..4].to_vec();
        let mut cases = 0;
        for reader in READERS {
            // Torn: the file ends `cut` bytes into the last frame.
            for cut in 1..(full.len() - last) {
                let path = dir.path().join(format!("torn-{reader}-{cut}.log"));
                fs::write(&path, &full[..last + cut]).unwrap();
                assert_eq!(
                    recover_with(reader, &path).unwrap(),
                    kept,
                    "{reader} torn at +{cut}"
                );
                assert_eq!(
                    fs::metadata(&path).unwrap().len(),
                    last as u64,
                    "{reader} torn at +{cut}"
                );
                cases += 1;
            }
            // Damaged: the last frame is complete but one byte is wrong.
            for at in last..full.len() {
                let path = dir.path().join(format!("flip-{reader}-{at}.log"));
                let mut bytes = full.clone();
                bytes[at] ^= 0x5A;
                fs::write(&path, &bytes).unwrap();
                assert_eq!(
                    recover_with(reader, &path).unwrap(),
                    kept,
                    "{reader} flip at {at}"
                );
                assert_eq!(
                    fs::metadata(&path).unwrap().len(),
                    last as u64,
                    "{reader} flip at {at}"
                );
                // The next append starts on the repaired boundary.
                append_plain(&path, &[b"next".to_vec()], false).unwrap();
                let mut expected = kept.clone();
                expected.push(b"next".to_vec());
                assert_eq!(
                    recover_with(reader, &path).unwrap(),
                    expected,
                    "{reader} flip at {at}"
                );
                cases += 1;
            }
        }
        assert!(cases > 4 * 2 * 20, "{cases}");
    }

    /// Damage in the middle of the log, at every byte of a middle frame, and
    /// a zeroed run, is reported as corruption by every recovery reader and
    /// the file is left byte-for-byte unchanged: the valid frames after the
    /// damage are never silently dropped.
    #[test]
    fn damage_followed_by_valid_frames_is_corruption_and_the_file_is_left_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let reference = dir.path().join("reference.log");
        let payloads = frame_payloads(5, "a");
        let offsets = append_plain(&reference, &payloads, false).unwrap();
        let full = fs::read(&reference).unwrap();
        let (start, end) = (offsets[2] as usize, offsets[3] as usize);
        let mut damaged_files = Vec::new();
        for at in start..end {
            let mut bytes = full.clone();
            bytes[at] ^= 0x5A;
            damaged_files.push((format!("flip-{at}"), bytes));
        }
        let mut zeroed = full.clone();
        zeroed[start..start + 24].fill(0);
        damaged_files.push(("zeroed".to_string(), zeroed));
        // A torn frame followed by complete frames: what an append that
        // landed after an unrepaired torn prefix produced before this change.
        let mut torn_then_valid = full[..end - 3].to_vec();
        torn_then_valid.extend_from_slice(&full[end..]);
        damaged_files.push(("torn-then-valid".to_string(), torn_then_valid));
        for reader in READERS {
            for (label, bytes) in &damaged_files {
                let path = dir.path().join(format!("{reader}-{label}.log"));
                fs::write(&path, bytes).unwrap();
                let error = recover_with(reader, &path).unwrap_err();
                assert!(
                    matches!(&error, BicDbError::Corruption { message, .. } if message.contains("valid frame follows")),
                    "{reader} {label}: {error}"
                );
                assert_eq!(
                    &fs::read(&path).unwrap(),
                    bytes,
                    "{reader} {label}: file changed"
                );
            }
        }

        // Salvage is the explicit way out: it keeps the frames before the
        // damage, preserves the rest in a side file, and recovery then opens.
        let path = dir.path().join("salvage.log");
        let mut bytes = full.clone();
        bytes[start + 1] ^= 0x5A;
        fs::write(&path, &bytes).unwrap();
        let salvage = salvage_frame_log(&path).unwrap();
        assert_eq!(salvage.kept_bytes, start as u64);
        assert_eq!(salvage.moved_bytes, (full.len() - start) as u64);
        let side = salvage.moved_to.clone().unwrap();
        assert_eq!(fs::read(&side).unwrap(), &bytes[start..]);
        assert_eq!(
            recover_with("buffered", &path).unwrap(),
            payloads[..2].to_vec()
        );
        // A clean log is left alone.
        let clean = salvage_frame_log(&reference).unwrap();
        assert_eq!(clean.moved_bytes, 0);
        assert_eq!(fs::read(&reference).unwrap(), full);
    }

    /// The live-log readers never truncate a frame that is still being
    /// written, and stop before it.
    #[test]
    fn live_readers_never_truncate_an_in_flight_frame() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("live.log");
        let payloads = frame_payloads(3, "a");
        append_plain(&path, &payloads, false).unwrap();
        let in_flight = encode_frames_to_bytes(
            &path,
            FrameKind::Sync,
            &[b"in-flight".to_vec()],
            &CompressionConfig::disabled(),
            &EncryptionRuntime::disabled(),
        )
        .unwrap();
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&in_flight[..in_flight.len() - 4]).unwrap();
        }
        let before = fs::read(&path).unwrap();
        let mut seen = Vec::new();
        stream_frames_live(
            &path,
            FrameKind::Sync,
            &EncryptionRuntime::disabled(),
            |_, p| {
                seen.push(p);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(seen, payloads);
        let frames =
            read_frames_readonly_from(&path, 0, FrameKind::Sync, &EncryptionRuntime::disabled())
                .unwrap();
        assert_eq!(frames.len(), 3);
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    /// A failed write (nothing written), a short write (half the batch
    /// written) and a failed sync (the whole batch written) each cut the log
    /// back to the frame boundary before the batch and return an ordinary
    /// error. The log keeps accepting appends, and recovery returns exactly
    /// the acknowledged batches.
    #[test]
    fn a_failed_short_or_unsynced_append_is_cut_back_and_later_appends_survive() {
        for fsync in [true, false] {
            for site in ["frame_log.write", "frame_log.short_write", "frame_log.sync"] {
                if site == "frame_log.sync" && !fsync {
                    continue; // no sync to fail
                }
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("outbox.log");
                let mut acknowledged = Vec::new();
                for batch in 0..3 {
                    let payloads = frame_payloads(4, &format!("ok{batch}"));
                    append_plain(&path, &payloads, fsync).unwrap();
                    acknowledged.extend(payloads);
                }
                let before = fs::metadata(&path).unwrap().len();
                let fault = bicdb_page::test_faults::arm_io_failure(site, dir.path(), 1, false);
                let error = append_plain(&path, &frame_payloads(4, "failed"), fsync).unwrap_err();
                assert_eq!(fault.fired(), 1, "{site} fsync={fsync}");
                assert!(
                    !error.is_durability_failure(),
                    "{site} fsync={fsync}: {error}"
                );
                assert_eq!(
                    fs::metadata(&path).unwrap().len(),
                    before,
                    "{site} fsync={fsync}: the failed batch must be cut back"
                );
                drop(fault);
                for batch in 0..2 {
                    let payloads = frame_payloads(3, &format!("later{batch}"));
                    append_plain(&path, &payloads, fsync).unwrap();
                    acknowledged.extend(payloads);
                }
                for reader in ["buffered", "stream"] {
                    assert_eq!(
                        recover_with(reader, &path).unwrap(),
                        acknowledged,
                        "{site} fsync={fsync} {reader}"
                    );
                }
            }
        }
    }

    /// When the cut-back fails too, the log is poisoned: every later append
    /// is refused with a durability failure and writes nothing. Recovery
    /// then cuts the log to its acknowledged length (from the marker), so
    /// even complete frames of the failed batch are not accepted, and the
    /// log takes appends again.
    #[test]
    fn a_failed_repair_poisons_the_log_until_recovery_cuts_the_failed_bytes() {
        for site in ["frame_log.short_write", "frame_log.sync"] {
            for reader in ["buffered", "stream", "from"] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("outbox.log");
                let mut acknowledged = frame_payloads(1, "a");
                append_plain(&path, &acknowledged, true).unwrap();
                let more = frame_payloads(3, "b");
                append_plain(&path, &more, true).unwrap();
                acknowledged.extend(more);
                let before = fs::metadata(&path).unwrap().len();
                let failing = bicdb_page::test_faults::arm_io_failure(site, dir.path(), 1, false);
                let repair = bicdb_page::test_faults::arm_io_failure(
                    "frame_log.repair",
                    dir.path(),
                    1,
                    false,
                );
                // Eight frames: a short write leaves complete frames of the
                // failed batch in the file, not only a torn one.
                let error = append_plain(&path, &frame_payloads(8, "failed"), true).unwrap_err();
                assert_eq!((failing.fired(), repair.fired()), (1, 1));
                assert!(
                    matches!(&error, BicDbError::WriteFailed { site, .. } if site == FRAME_LOG_REPAIR_SITE),
                    "{error}"
                );
                let poisoned_len = fs::metadata(&path).unwrap().len();
                assert!(poisoned_len > before, "{site}: the failed bytes stayed");
                assert!(append_failure_marker_path(&path).exists());
                drop((failing, repair));

                let refused = append_plain(&path, &frame_payloads(1, "refused"), true).unwrap_err();
                assert!(refused.is_durability_failure(), "{refused}");
                assert_eq!(
                    fs::metadata(&path).unwrap().len(),
                    poisoned_len,
                    "nothing appended"
                );
                // A later process has no in-memory poison; the marker alone
                // still refuses appends until a recovery read cuts the tail.
                clear_poison(&path);
                let refused = append_plain(&path, &frame_payloads(1, "refused"), true).unwrap_err();
                assert!(refused.is_durability_failure(), "{refused}");
                assert_eq!(
                    fs::metadata(&path).unwrap().len(),
                    poisoned_len,
                    "nothing appended"
                );

                assert_eq!(
                    recover_with(reader, &path).unwrap(),
                    acknowledged,
                    "{site} {reader}"
                );
                assert_eq!(fs::metadata(&path).unwrap().len(), before);
                assert!(!append_failure_marker_path(&path).exists());
                let after = frame_payloads(2, "c");
                append_plain(&path, &after, true).unwrap();
                acknowledged.extend(after);
                assert_eq!(recover_with("buffered", &path).unwrap(), acknowledged);
            }
        }
    }

    /// Every step of the durable replacement, under every policy: what is
    /// on disk afterwards, whether the handle is fenced, and that no
    /// temporary file is left behind.
    #[test]
    fn durable_replacement_fails_closed_per_step_and_policy() {
        use bicdb_page::test_faults::arm_io_failure;
        for step in ["write", "sync", "rename", "dir_sync"] {
            for policy_name in ["no", "after_rename", "always"] {
                let temp = tempfile::tempdir().unwrap();
                let path = temp.path().join("state.json");
                write_atomic(&path, b"old", true).unwrap();
                let health = bicdb_page::DurabilityHealth::new();
                let policy = match policy_name {
                    "no" => FailClosed::No,
                    "after_rename" => FailClosed::AfterRename(&health),
                    _ => FailClosed::Always(&health),
                };
                let fault = arm_io_failure(&format!("unit.{step}"), temp.path(), 1, false);
                let error =
                    replace_durably(&path, ReplaceContents::Plain(b"new"), true, "unit", policy)
                        .unwrap_err();
                assert_eq!(fault.fired(), 1, "{step}/{policy_name}");
                let label = format!("{step}/{policy_name}: {error}");
                let renamed = step == "dir_sync";
                let fatal = match policy_name {
                    "no" => false,
                    "after_rename" => step == "dir_sync",
                    _ => true,
                };
                assert_eq!(health.is_failed(), fatal, "{label}");
                assert_eq!(
                    matches!(&error, BicDbError::WriteFailed { site, .. } if site == &format!("unit.{step}")),
                    fatal,
                    "{label}"
                );
                // The injected rename fails before renaming: the old file is
                // intact. A failed directory sync follows a real rename.
                let expected: &[u8] = if renamed { b"new" } else { b"old" };
                assert_eq!(fs::read(&path).unwrap(), expected, "{label}");
                let leftovers = fs::read_dir(temp.path())
                    .unwrap()
                    .filter_map(|entry| entry.ok())
                    .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
                    .count();
                assert_eq!(leftovers, 0, "{label}: temporary file left behind");
                // A fenced health refuses the next replacement outright.
                drop(fault);
                let again =
                    replace_durably(&path, ReplaceContents::Plain(b"next"), true, "unit", policy);
                assert_eq!(
                    again.is_err(),
                    fatal,
                    "{label}: replacement after the failure"
                );
            }
        }
    }

    /// With `fsync` off nothing is synced, so only the write and rename
    /// steps exist.
    #[test]
    fn durable_replacement_without_fsync_skips_the_sync_steps() {
        use bicdb_page::test_faults::arm_io_failure;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.json");
        let sync = arm_io_failure("unit.sync", temp.path(), 1, true);
        let dir_sync = arm_io_failure("unit.dir_sync", temp.path(), 1, true);
        let health = bicdb_page::DurabilityHealth::new();
        replace_durably(
            &path,
            ReplaceContents::Plain(b"new"),
            false,
            "unit",
            FailClosed::Always(&health),
        )
        .unwrap();
        assert_eq!(sync.seen() + dir_sync.seen(), 0);
        assert_eq!(fs::read(&path).unwrap(), b"new");
    }

    /// A framed (encrypted-sidecar) replacement whose frame append fails and
    /// whose tail repair fails too: the temporary file is discarded, so the
    /// frame log's poison and marker do not outlive it, the failure before
    /// the rename is not fatal, and the next replacement succeeds.
    #[test]
    fn a_failed_framed_replacement_leaves_no_poison_or_marker() {
        use bicdb_page::test_faults::arm_io_failure;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("sidecar.json");
        write_atomic(&path, b"old", true).unwrap();
        let health = bicdb_page::DurabilityHealth::new();
        let encryption = EncryptionRuntime::disabled();
        let write = arm_io_failure("frame_log.short_write", temp.path(), 1, false);
        let repair = arm_io_failure("frame_log.repair", temp.path(), 1, false);
        let framed = || ReplaceContents::Framed {
            kind: FrameKind::Index,
            payload: b"new",
            encryption: &encryption,
        };
        let error = replace_durably(
            &path,
            framed(),
            true,
            "unit",
            FailClosed::AfterRename(&health),
        )
        .unwrap_err();
        assert_eq!((write.fired(), repair.fired()), (1, 1), "{error}");
        assert!(!error.is_durability_failure(), "{error}");
        assert!(!health.is_failed());
        assert_eq!(fs::read(&path).unwrap(), b"old");
        let leftovers: Vec<_> = fs::read_dir(temp.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name != "sidecar.json")
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
        drop((write, repair));
        replace_durably(
            &path,
            framed(),
            true,
            "unit",
            FailClosed::AfterRename(&health),
        )
        .unwrap();
        assert!(!health.is_failed());
    }
}
