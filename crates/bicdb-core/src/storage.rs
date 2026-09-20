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
const HEADER_LEN: usize = 16;
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

    let _backup_gate = acquire_backup_write_guard(write_path)?;
    ensure_parent(write_path)?;
    let (mut file, sync_after_write) = open_append_file(write_path, sync_mode)?;

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
    file.write_all(&write_buffer)?;

    if sync_after_write {
        file.sync_data()?;
    }

    Ok(offsets)
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
    write_raw_with_repair(
        handle,
        bytes,
        |file, bytes, sync_after_write| {
            append_and_sync(file, bytes, sync_after_write, File::sync_data)
        },
        |file, len, durable| {
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

    let file_len = file.metadata()?.len();
    if file_len == 0 {
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
    mut visit: impl FnMut(u64, Vec<u8>) -> Result<()>,
) -> Result<u64> {
    const READ_CHUNK: usize = 8 * 1024 * 1024;
    ensure_parent(path)?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    let file_len = file.metadata()?.len();
    if file_len == 0 {
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

    if valid_up_to < file_len {
        file.set_len(valid_up_to)?;
        file.sync_data()?;
    }
    Ok(file_len.saturating_sub(valid_up_to))
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
    if !strict && (valid_up_to as u64) < file_len {
        file.set_len(valid_up_to as u64)?;
        file.sync_data()?;
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
    let file_len = file.metadata()?.len();
    if start_offset >= file_len {
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
    if absolute_valid < file_len {
        file.set_len(absolute_valid)?;
        file.sync_data()?;
    }

    Ok(RecoveredFrames {
        frames,
        truncated_bytes: file_len.saturating_sub(absolute_valid),
    })
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
    if !strict && (valid_up_to as u64) < file_len {
        file.set_len(valid_up_to as u64)?;
        file.sync_data()?;
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
    ensure_parent(path)?;
    let tmp_path = tmp_path(path);
    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW).mode(0o600);
        let mut file = options.open(&tmp_path)?;
        file.write_all(bytes)?;
        if fsync {
            file.sync_all()?;
        }
        drop(file);
        fs::rename(&tmp_path, path)?;
        if fsync {
            sync_parent(path)?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    result
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
    ensure_parent(path)?;
    let tmp_path = tmp_path(path);
    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW).mode(0o600);
        drop(options.open(&tmp_path)?);
        append_frames_for_object(
            &tmp_path,
            path,
            kind,
            &[payload.to_vec()],
            fsync,
            &CompressionConfig::disabled(),
            encryption,
        )?;
        fs::rename(&tmp_path, path)?;
        if fsync {
            sync_parent(path)?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    result
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
    }
    Ok(())
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

        // A corrupted mid-file checksum ends the walk at the last valid
        // frame, exactly like the reference reader.
        let mid = recovered.frames[300].offset;
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = OpenOptions::new().write(true).open(&path).unwrap();
            file.seek(SeekFrom::Start(mid + HEADER_LEN as u64 + 2))
                .unwrap();
            file.write_all(&[0xFF]).unwrap();
        }
        let mut streamed = Vec::new();
        stream_frames(&path, FrameKind::Event, &encryption, |offset, payload| {
            streamed.push((offset, payload));
            Ok(())
        })
        .unwrap();
        assert_eq!(streamed.len(), 300);
        let recovered = read_frames(
            &path,
            FrameKind::Event,
            SegmentReadMode::Buffered,
            &encryption,
        )
        .unwrap();
        assert_eq!(recovered.frames.len(), 300);

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
}
