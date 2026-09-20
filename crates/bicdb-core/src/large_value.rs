use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, ErrorKind, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::encryption::{EncryptionObjectPurpose, EncryptionRuntime};
use crate::error::{BicDbError, Result};
use crate::storage;

pub const DEFAULT_LARGE_VALUE_THRESHOLD_BYTES: usize = 1024 * 1024;
pub const DEFAULT_LARGE_VALUE_CHUNK_BYTES: usize = 1024 * 1024;
pub const DEFAULT_LARGE_VALUES_DIR: &str = "large_values";
pub const LARGE_VALUE_REF_MARKER: &str = "__bicdb_large_value_ref";

const MAGIC: &[u8; 4] = b"BICB";
const VERSION: u8 = 1;
const FLAG_ENCRYPTED: u8 = 1;
const HEADER_LEN: usize = 4 + 1 + 1 + 2 + 8 + 4 + 4 + 32;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LargeValueRef {
    pub marker: String,
    pub storage: String,
    pub hash_sha256: String,
    pub size_bytes: u64,
    pub chunk_bytes: usize,
    pub chunks: u64,
    pub encrypted: bool,
    pub checksum_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    pub created_at: i64,
}

impl LargeValueRef {
    pub fn new(
        hash_sha256: String,
        size_bytes: u64,
        chunks: u64,
        encrypted: bool,
        media_type: Option<String>,
    ) -> Self {
        Self {
            marker: LARGE_VALUE_REF_MARKER.to_string(),
            storage: "content_addressed_blob_store".to_string(),
            checksum_sha256: hash_sha256.clone(),
            hash_sha256,
            size_bytes,
            chunk_bytes: DEFAULT_LARGE_VALUE_CHUNK_BYTES,
            chunks,
            encrypted,
            media_type,
            created_at: unix_timestamp(),
        }
    }

    pub fn from_value(value: &serde_json::Value) -> Result<Self> {
        let descriptor: Self = serde_json::from_value(value.clone())?;
        if descriptor.marker != LARGE_VALUE_REF_MARKER {
            return Err(BicDbError::LargeValue(
                "metadata value is not a BicDB large value reference".to_string(),
            ));
        }
        descriptor.validate()?;
        Ok(descriptor)
    }

    fn validate(&self) -> Result<()> {
        if !is_sha256_hex(&self.hash_sha256)
            || !is_sha256_hex(&self.checksum_sha256)
            || self.hash_sha256 != self.checksum_sha256
        {
            return Err(BicDbError::LargeValue(
                "large value reference requires matching 64-character hexadecimal SHA-256 hashes"
                    .to_string(),
            ));
        }
        if self.storage != "content_addressed_blob_store"
            || self.chunk_bytes == 0
            || self.chunk_bytes > DEFAULT_LARGE_VALUE_CHUNK_BYTES
        {
            return Err(BicDbError::LargeValue(
                "large value reference has invalid storage or chunk bounds".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct LargeValueIntegrityReport {
    pub blobs_checked: usize,
    pub bytes_checked: u64,
    pub checksum_failures: Vec<String>,
    pub orphan_tmp_files: usize,
}

impl LargeValueIntegrityReport {
    pub fn passed(&self) -> bool {
        self.checksum_failures.is_empty()
    }
}

pub struct LargeValueReader {
    file: File,
    path: PathBuf,
    encryption: EncryptionRuntime,
    current: Cursor<Vec<u8>>,
    next_chunk_index: u64,
    finished: bool,
    total_read: u64,
    expected: LargeValueRef,
    hash: Sha256,
}

pub fn write_from_reader(
    db_path: &Path,
    mut reader: impl Read,
    encryption: &EncryptionRuntime,
    fsync: bool,
    media_type: Option<String>,
) -> Result<LargeValueRef> {
    let _backup_gate = storage::acquire_database_backup_write_guard(db_path)?;
    let root = db_path.join(DEFAULT_LARGE_VALUES_DIR);
    fs::create_dir_all(&root)?;
    let tmp_path = root.join(format!("{}.tmp", Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW).mode(0o600);
    let mut tmp = options.open(&tmp_path)?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; DEFAULT_LARGE_VALUE_CHUNK_BYTES];
    let mut size_bytes = 0_u64;
    let mut chunks = 0_u64;

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        let chunk = &buffer[..bytes_read];
        hash.update(chunk);
        write_chunk(
            &mut tmp,
            &tmp_path,
            &tmp_path,
            chunks,
            chunk,
            encryption,
            encryption.is_enabled(),
        )?;
        size_bytes = size_bytes.saturating_add(bytes_read as u64);
        chunks += 1;
    }

    if fsync {
        tmp.sync_data()?;
    }
    drop(tmp);

    let digest = hex::encode(hash.finalize());
    let final_path = blob_path(&root, &digest);
    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let publication_path = if encryption.bound_object_cipher().is_some() {
        let rebound_path = root.join(format!("{}.rebound.tmp", Uuid::new_v4()));
        let mut source = open_regular_nofollow(&tmp_path)?;
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW).mode(0o600);
        let mut rebound = options.open(&rebound_path)?;
        let mut index = 0_u64;
        while let Some(plaintext) =
            read_next_chunk(&mut source, &tmp_path, index, encryption, false)?
        {
            write_chunk(
                &mut rebound,
                &rebound_path,
                &final_path,
                index,
                &plaintext,
                encryption,
                true,
            )?;
            index = index.saturating_add(1);
        }
        if index != chunks {
            return Err(corruption(
                &tmp_path,
                "large value rebound chunk count mismatch",
            ));
        }
        if fsync {
            rebound.sync_data()?;
        }
        drop(rebound);
        fs::remove_file(&tmp_path)?;
        rebound_path
    } else {
        tmp_path.clone()
    };
    match fs::rename(&publication_path, &final_path) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            fs::remove_file(&publication_path)?;
        }
        Err(error) if final_path.exists() => {
            fs::remove_file(&publication_path)?;
            if error.kind() != ErrorKind::AlreadyExists {
                return Err(error.into());
            }
        }
        Err(error) => return Err(error.into()),
    }

    if fsync {
        storage::sync_file(&final_path)?;
    }

    Ok(LargeValueRef::new(
        digest,
        size_bytes,
        chunks,
        encryption.is_enabled(),
        media_type,
    ))
}

pub fn open_reader(
    db_path: &Path,
    reference: &LargeValueRef,
    encryption: &EncryptionRuntime,
) -> Result<LargeValueReader> {
    reference.validate()?;
    let path = blob_path(
        &db_path.join(DEFAULT_LARGE_VALUES_DIR),
        &reference.hash_sha256,
    );
    let file = open_regular_nofollow(&path)?;
    Ok(LargeValueReader {
        file,
        path,
        encryption: encryption.clone(),
        current: Cursor::new(Vec::new()),
        next_chunk_index: 0,
        finished: false,
        total_read: 0,
        expected: reference.clone(),
        hash: Sha256::new(),
    })
}

pub fn verify_store(
    db_path: &Path,
    encryption: &EncryptionRuntime,
) -> Result<LargeValueIntegrityReport> {
    let root = db_path.join(DEFAULT_LARGE_VALUES_DIR);
    let mut report = LargeValueIntegrityReport::default();
    if !root.exists() {
        return Ok(report);
    }
    verify_store_inner(&root, db_path, encryption, &mut report)?;
    Ok(report)
}

impl Read for LargeValueReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let mut written = 0;
        while written < out.len() {
            let count = self.current.read(&mut out[written..])?;
            if count > 0 {
                written += count;
                continue;
            }
            if self.finished {
                if self.expected.chunks == 0 || self.total_read == self.expected.size_bytes {
                    return Ok(written);
                }
                return Err(std::io::Error::new(
                    ErrorKind::InvalidData,
                    "large value ended before expected byte count",
                ));
            }
            let next = read_next_chunk(
                &mut self.file,
                &self.path,
                self.next_chunk_index,
                &self.encryption,
                false,
            )
            .map_err(std::io::Error::other)?;
            match next {
                Some(chunk) => {
                    self.hash.update(&chunk);
                    self.total_read = self.total_read.saturating_add(chunk.len() as u64);
                    self.current = Cursor::new(chunk);
                    self.next_chunk_index += 1;
                }
                None => {
                    self.finished = true;
                    let digest = hex::encode(self.hash.clone().finalize());
                    if digest != self.expected.checksum_sha256 {
                        return Err(std::io::Error::new(
                            ErrorKind::InvalidData,
                            "large value checksum mismatch",
                        ));
                    }
                }
            }
        }
        Ok(written)
    }
}

fn write_chunk(
    file: &mut File,
    diagnostic_path: &Path,
    object_path: &Path,
    index: u64,
    plaintext: &[u8],
    encryption: &EncryptionRuntime,
    encrypted: bool,
) -> Result<()> {
    let stored = if encrypted {
        encryption.encrypt_for(
            EncryptionObjectPurpose::Blob,
            object_path,
            plaintext,
            &chunk_aad(index),
        )?
    } else {
        plaintext.to_vec()
    };
    let mut header = Vec::with_capacity(HEADER_LEN);
    header.extend_from_slice(MAGIC);
    header.push(VERSION);
    header.push(if encrypted { FLAG_ENCRYPTED } else { 0 });
    header.extend_from_slice(&0_u16.to_le_bytes());
    header.extend_from_slice(&index.to_le_bytes());
    header.extend_from_slice(&(plaintext.len() as u32).to_le_bytes());
    header.extend_from_slice(&(stored.len() as u32).to_le_bytes());
    header.extend_from_slice(&Sha256::digest(plaintext));
    if header.len() != HEADER_LEN {
        return Err(BicDbError::Corruption {
            path: diagnostic_path.to_path_buf(),
            message: "large value chunk header length mismatch".to_string(),
        });
    }
    file.write_all(&header)?;
    file.write_all(&stored)?;
    Ok(())
}

fn read_next_chunk(
    file: &mut File,
    path: &Path,
    expected_index: u64,
    encryption: &EncryptionRuntime,
    require_encrypted: bool,
) -> Result<Option<Vec<u8>>> {
    let mut header = [0_u8; HEADER_LEN];
    let mut read = 0;
    while read < HEADER_LEN {
        let count = file.read(&mut header[read..])?;
        if count == 0 {
            if read == 0 {
                return Ok(None);
            }
            return Err(corruption(path, "truncated large value chunk header"));
        }
        read += count;
    }
    if &header[0..4] != MAGIC {
        return Err(corruption(path, "invalid large value magic"));
    }
    if header[4] != VERSION {
        return Err(corruption(path, "unsupported large value version"));
    }
    let flags = header[5];
    if flags & !FLAG_ENCRYPTED != 0 {
        return Err(corruption(path, "large value chunk has unknown flags"));
    }
    if require_encrypted && flags & FLAG_ENCRYPTED == 0 {
        return Err(BicDbError::EncryptionKeyInvalid(format!(
            "cell-bound key rotation refuses plaintext attachment chunk in {}",
            path.display()
        )));
    }
    let index = u64::from_le_bytes(header[8..16].try_into().unwrap());
    if index != expected_index {
        return Err(corruption(path, "out-of-order large value chunk"));
    }
    let plaintext_len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
    let stored_len = u32::from_le_bytes(header[20..24].try_into().unwrap()) as usize;
    let expected_hash = &header[24..56];
    let mut stored = vec![0_u8; stored_len];
    file.read_exact(&mut stored)?;
    let plaintext = if flags & FLAG_ENCRYPTED != 0 {
        encryption.decrypt_for(
            EncryptionObjectPurpose::Blob,
            path,
            &stored,
            &chunk_aad(index),
        )?
    } else {
        stored
    };
    if plaintext.len() != plaintext_len {
        return Err(corruption(path, "large value chunk length mismatch"));
    }
    if Sha256::digest(&plaintext).as_slice() != expected_hash {
        return Err(corruption(path, "large value chunk checksum mismatch"));
    }
    Ok(Some(plaintext))
}

pub(crate) fn reencrypt_bound_file(
    source_path: &Path,
    target_path: &Path,
    source_encryption: &EncryptionRuntime,
    target_encryption: &EncryptionRuntime,
    fsync: bool,
) -> Result<u64> {
    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut source = open_regular_nofollow(source_path)?;
    let mut target_options = OpenOptions::new();
    target_options.create_new(true).write(true);
    #[cfg(unix)]
    target_options.custom_flags(libc::O_NOFOLLOW).mode(0o600);
    let mut target = target_options.open(target_path)?;
    let mut index = 0_u64;
    while let Some(plaintext) =
        read_next_chunk(&mut source, source_path, index, source_encryption, true)?
    {
        write_chunk(
            &mut target,
            target_path,
            target_path,
            index,
            &plaintext,
            target_encryption,
            true,
        )?;
        index = index.saturating_add(1);
    }
    if fsync {
        target.sync_all()?;
    }
    Ok(index)
}

fn verify_store_inner(
    root: &Path,
    db_path: &Path,
    encryption: &EncryptionRuntime,
    report: &mut LargeValueIntegrityReport,
) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        let metadata = fs::metadata(&path)?;
        if metadata.is_dir() {
            verify_store_inner(&path, db_path, encryption, report)?;
            continue;
        }
        if path.extension().and_then(|extension| extension.to_str()) == Some("tmp") {
            report.orphan_tmp_files += 1;
            continue;
        }
        if !metadata.is_file() {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let reference = LargeValueRef::new(
            stem.to_string(),
            metadata.len(),
            0,
            encryption.is_enabled(),
            None,
        );
        let mut reader = open_reader(db_path, &reference, encryption)?;
        let mut hash = Sha256::new();
        let mut buffer = vec![0_u8; 64 * 1024];
        let mut bytes = 0_u64;
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => {
                    hash.update(&buffer[..count]);
                    bytes = bytes.saturating_add(count as u64);
                }
                Err(error) => {
                    report
                        .checksum_failures
                        .push(format!("{}: {error}", path.display()));
                    break;
                }
            }
        }
        let digest = hex::encode(hash.finalize());
        if digest != stem {
            report.checksum_failures.push(format!(
                "{}: content hash {digest} does not match address {stem}",
                path.display()
            ));
        }
        report.blobs_checked += 1;
        report.bytes_checked = report.bytes_checked.saturating_add(bytes);
    }
    Ok(())
}

fn blob_path(root: &Path, hash: &str) -> PathBuf {
    let prefix = hash.get(..2).unwrap_or("00");
    root.join(prefix).join(format!("{hash}.blob"))
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn chunk_aad(index: u64) -> Vec<u8> {
    let mut aad = b"bicdb.large_value.v1".to_vec();
    aad.extend_from_slice(&index.to_le_bytes());
    aad
}

fn corruption(path: &Path, message: &str) -> BicDbError {
    BicDbError::Corruption {
        path: path.to_path_buf(),
        message: message.to_string(),
    }
}

fn open_regular_nofollow(path: &Path) -> Result<File> {
    let before = fs::symlink_metadata(path)?;
    if before.file_type().is_symlink() || !before.is_file() {
        return Err(corruption(
            path,
            "large value object must be a regular non-symlink file",
        ));
    }
    #[cfg(unix)]
    if before.nlink() != 1 {
        return Err(corruption(
            path,
            "large value object must not have hard links",
        ));
    }

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options.open(path)?;
    let opened = file.metadata()?;
    if !opened.is_file() || opened.len() != before.len() {
        return Err(corruption(path, "large value object changed while opening"));
    }
    #[cfg(unix)]
    if opened.dev() != before.dev() || opened.ino() != before.ino() || opened.nlink() != 1 {
        return Err(corruption(
            path,
            "large value object identity changed while opening",
        ));
    }
    Ok(file)
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}
