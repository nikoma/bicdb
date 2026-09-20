use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use crate::encryption::{
    self, EncryptionBinding, EncryptionConfig, EncryptionMode, EncryptionObjectPurpose,
    EncryptionRuntime, ENCRYPTION_METADATA_FILE,
};
use crate::error::{BicDbError, Result};
use crate::{large_value, storage};

const ROTATION_FORMAT: &str = "bicdb.bound-key-rotation/v1";
const MESH_SIGNING_KEY_FILE: &str = "mesh_signing_key.json";
const MESH_SIGNING_KEY_AAD: &[u8] = b"bicdb.mesh.signing-key.v2";
const COPY_BUFFER_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BoundEncryptionRotationCheckpoint {
    Prepared,
    SourceRetired,
    Activated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BoundEncryptionRotationOptions {
    pub fsync: bool,
    /// A controlled recovery-test hook. Production callers leave this `None`;
    /// tests can halt at either durable boundary and invoke the same operation
    /// again to prove deterministic recovery.
    pub stop_after: Option<BoundEncryptionRotationCheckpoint>,
}

impl Default for BoundEncryptionRotationOptions {
    fn default() -> Self {
        Self {
            fsync: true,
            stop_after: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BoundEncryptionRotationReport {
    pub checkpoint: BoundEncryptionRotationCheckpoint,
    pub activated: bool,
    pub source_path: PathBuf,
    pub retired_path: PathBuf,
    pub old_key_epoch: u64,
    pub new_key_epoch: u64,
    pub files_processed: u64,
    pub frames_reencrypted: u64,
    pub attachment_chunks_reencrypted: u64,
    pub staged_tree_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RotationJournal {
    format: String,
    checkpoint: BoundEncryptionRotationCheckpoint,
    database_name: String,
    old_binding: EncryptionBinding,
    new_binding: EncryptionBinding,
    files_processed: u64,
    frames_reencrypted: u64,
    attachment_chunks_reencrypted: u64,
    staged_tree_sha256: String,
}

#[derive(Default)]
struct StageCounters {
    files: u64,
    frames: u64,
    attachment_chunks: u64,
}

/// Crash-safe, offline rotation for one cell-bound encrypted database.
///
/// The source database is held under its kernel directory lock. A complete
/// replacement tree is authenticated under the retiring key and written under
/// the next key before any namespace switch occurs. Activation is two atomic
/// same-filesystem renames with a durable journal between them. Reinvoking the
/// function after either rename deterministically resumes; mixed-key trees are
/// never opened or published. The retired ciphertext tree is intentionally
/// retained until an external KMS/recovery authority confirms old-key
/// retirement.
pub fn rotate_bound_database_encryption(
    database_path: impl AsRef<Path>,
    old_config: EncryptionConfig,
    new_config: EncryptionConfig,
    options: BoundEncryptionRotationOptions,
) -> Result<BoundEncryptionRotationReport> {
    let old_binding = require_bound_config("retiring", &old_config)?.clone();
    let new_binding = require_bound_config("next", &new_config)?.clone();
    if old_binding.security_domain != new_binding.security_domain
        || old_binding.profile != new_binding.profile
        || new_binding.key_epoch <= old_binding.key_epoch
    {
        return Err(BicDbError::EncryptionKeyInvalid(
            "bound key rotation requires the same security domain/profile and a strictly newer key epoch"
                .to_string(),
        ));
    }

    let requested = database_path.as_ref();
    let database_name = normal_file_name(requested)?;
    let parent = requested
        .parent()
        .ok_or_else(|| {
            BicDbError::EncryptionKeyInvalid(
                "database rotation path has no parent directory".to_string(),
            )
        })?
        .canonicalize()?;
    let source_path = parent.join(&database_name);
    reject_symlink_if_exists(&source_path, "database rotation source")?;
    let staging_path = parent.join(format!(
        ".{database_name}.key-epoch-{}.staging",
        new_binding.key_epoch
    ));
    let retired_path = parent.join(format!(
        ".{database_name}.key-epoch-{}.retired",
        old_binding.key_epoch
    ));
    let journal_path = parent.join(format!(".{database_name}.key-rotation.json"));
    reject_symlink_if_exists(&staging_path, "database rotation staging tree")?;
    reject_symlink_if_exists(&retired_path, "database rotation retired tree")?;
    reject_symlink_if_exists(&journal_path, "database rotation journal")?;

    if !journal_path.exists() {
        if let Some(metadata) = encryption::load_metadata(&source_path)? {
            if metadata.binding.as_ref() == Some(&new_binding) {
                return Ok(BoundEncryptionRotationReport {
                    checkpoint: BoundEncryptionRotationCheckpoint::Activated,
                    activated: true,
                    source_path,
                    retired_path,
                    old_key_epoch: old_binding.key_epoch,
                    new_key_epoch: new_binding.key_epoch,
                    files_processed: 0,
                    frames_reencrypted: 0,
                    attachment_chunks_reencrypted: 0,
                    staged_tree_sha256: tree_digest_hex(requested)?,
                });
            }
        }
        if !source_path.is_dir() {
            return Err(BicDbError::EncryptionKeyInvalid(format!(
                "database rotation source {} is not a directory",
                source_path.display()
            )));
        }
        if staging_path.exists() || retired_path.exists() {
            return Err(BicDbError::EncryptionKeyInvalid(
                "rotation staging/retired path exists without its journal; operator inspection is required"
                    .to_string(),
            ));
        }

        let _source_lock = bicdb_page::DirectoryLock::acquire(&source_path).map_err(|error| {
            BicDbError::EncryptionKeyInvalid(format!(
                "database must be closed before key rotation: {error}"
            ))
        })?;
        let source_metadata = encryption::load_metadata(&source_path)?.ok_or_else(|| {
            BicDbError::EncryptionKeyInvalid(
                "bound key rotation source has no encryption metadata".to_string(),
            )
        })?;
        if source_metadata.version != 2 || source_metadata.binding.as_ref() != Some(&old_binding) {
            return Err(BicDbError::EncryptionKeyInvalid(
                "retiring encryption binding does not match the source database".to_string(),
            ));
        }
        let old_runtime = encryption::runtime_for_existing(&source_path, Some(old_config))?;
        let next_key_version = source_metadata.key_version.checked_add(1).ok_or_else(|| {
            BicDbError::EncryptionKeyInvalid("encryption key version exhausted".to_string())
        })?;
        create_restricted_directory(&staging_path)?;
        let (new_runtime, new_metadata) =
            encryption::runtime_for_rotation_target(&staging_path, &new_config, next_key_version)?;
        let stage_result = stage_tree(
            &source_path,
            &staging_path,
            &old_runtime,
            &new_runtime,
            options.fsync,
        );
        let counters = match stage_result {
            Ok(counters) => counters,
            Err(error) => {
                let _ = fs::remove_dir_all(&staging_path);
                return Err(error);
            }
        };
        encryption::persist_rotation_metadata(&staging_path, &new_metadata, options.fsync)?;
        if options.fsync {
            sync_tree_directories(&staging_path)?;
        }
        let staged_tree_sha256 = tree_digest_hex(&staging_path)?;
        let journal = RotationJournal {
            format: ROTATION_FORMAT.to_string(),
            checkpoint: BoundEncryptionRotationCheckpoint::Prepared,
            database_name: database_name.clone(),
            old_binding: old_binding.clone(),
            new_binding: new_binding.clone(),
            files_processed: counters.files,
            frames_reencrypted: counters.frames,
            attachment_chunks_reencrypted: counters.attachment_chunks,
            staged_tree_sha256,
        };
        persist_journal(&journal_path, &journal, options.fsync)?;
    }

    let mut journal = load_journal(&journal_path)?;
    validate_journal(&journal, &database_name, &old_binding, &new_binding)?;
    if journal.checkpoint == BoundEncryptionRotationCheckpoint::Prepared
        && options.stop_after == Some(BoundEncryptionRotationCheckpoint::Prepared)
    {
        return Ok(report(&journal, false, source_path, retired_path));
    }
    if journal.checkpoint == BoundEncryptionRotationCheckpoint::Prepared {
        let _source_lock = bicdb_page::DirectoryLock::acquire(&source_path).map_err(|error| {
            BicDbError::EncryptionKeyInvalid(format!(
                "database must remain closed during key rotation: {error}"
            ))
        })?;
        if tree_digest_hex(&staging_path)? != journal.staged_tree_sha256 {
            return Err(BicDbError::TamperEvidence {
                path: staging_path,
                message: "prepared rotation tree digest changed".to_string(),
            });
        }
        fs::rename(&source_path, &retired_path)?;
        sync_directory(&parent, options.fsync)?;
        journal.checkpoint = BoundEncryptionRotationCheckpoint::SourceRetired;
        persist_journal(&journal_path, &journal, options.fsync)?;
        if options.stop_after == Some(BoundEncryptionRotationCheckpoint::SourceRetired) {
            return Ok(report(&journal, false, source_path, retired_path));
        }
    }

    if journal.checkpoint == BoundEncryptionRotationCheckpoint::SourceRetired {
        reject_symlink_if_exists(&retired_path, "retired database")?;
        if source_path.exists() || !retired_path.is_dir() || !staging_path.is_dir() {
            return Err(BicDbError::EncryptionKeyInvalid(
                "rotation journal/source/staging namespace is inconsistent at source_retired"
                    .to_string(),
            ));
        }
        let _retired_lock = bicdb_page::DirectoryLock::acquire(&retired_path).map_err(|error| {
            BicDbError::EncryptionKeyInvalid(format!(
                "retired database lock could not be reacquired during rotation recovery: {error}"
            ))
        })?;
        fs::rename(&staging_path, &source_path)?;
        sync_directory(&parent, options.fsync)?;
        journal.checkpoint = BoundEncryptionRotationCheckpoint::Activated;
        persist_journal(&journal_path, &journal, options.fsync)?;
    }

    let active_metadata = encryption::load_metadata(&source_path)?.ok_or_else(|| {
        BicDbError::EncryptionKeyInvalid(
            "activated rotation tree has no encryption metadata".to_string(),
        )
    })?;
    if active_metadata.binding.as_ref() != Some(&new_binding) {
        return Err(BicDbError::EncryptionKeyInvalid(
            "activated rotation tree has the wrong key binding".to_string(),
        ));
    }
    if tree_digest_hex(&source_path)? != journal.staged_tree_sha256 {
        return Err(BicDbError::TamperEvidence {
            path: source_path,
            message: "activated rotation tree does not match the prepared digest".to_string(),
        });
    }
    if options.stop_after == Some(BoundEncryptionRotationCheckpoint::Activated) {
        return Ok(report(&journal, true, source_path, retired_path));
    }
    fs::remove_file(&journal_path)?;
    sync_directory(&parent, options.fsync)?;
    Ok(report(&journal, true, source_path, retired_path))
}

fn require_bound_config<'a>(
    label: &str,
    config: &'a EncryptionConfig,
) -> Result<&'a EncryptionBinding> {
    if config.mode != EncryptionMode::Enabled {
        return Err(BicDbError::EncryptionKeyInvalid(format!(
            "{label} key rotation config is not enabled"
        )));
    }
    config.binding.as_ref().ok_or_else(|| {
        BicDbError::EncryptionKeyInvalid(format!("{label} key rotation config is not cell-bound"))
    })
}

fn stage_tree(
    source_root: &Path,
    target_root: &Path,
    source_encryption: &EncryptionRuntime,
    target_encryption: &EncryptionRuntime,
    fsync: bool,
) -> Result<StageCounters> {
    let mut counters = StageCounters::default();
    stage_directory(
        source_root,
        target_root,
        Path::new(""),
        source_encryption,
        target_encryption,
        fsync,
        &mut counters,
    )?;
    Ok(counters)
}

fn stage_directory(
    source_root: &Path,
    target_root: &Path,
    relative: &Path,
    source_encryption: &EncryptionRuntime,
    target_encryption: &EncryptionRuntime,
    fsync: bool,
    counters: &mut StageCounters,
) -> Result<()> {
    let source_directory = source_root.join(relative);
    let mut entries = fs::read_dir(&source_directory)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name = entry.file_name();
        let child_relative = relative.join(&name);
        let source_path = entry.path();
        let target_path = target_root.join(&child_relative);
        let metadata = fs::symlink_metadata(&source_path)?;
        if metadata.file_type().is_symlink() {
            return Err(BicDbError::EncryptionKeyInvalid(format!(
                "key rotation refuses symbolic link {}",
                source_path.display()
            )));
        }
        if metadata.is_dir() {
            if child_relative.components().next()
                == Some(Component::Normal(std::ffi::OsStr::new("tmp")))
            {
                continue;
            }
            create_restricted_directory(&target_path)?;
            stage_directory(
                source_root,
                target_root,
                &child_relative,
                source_encryption,
                target_encryption,
                fsync,
                counters,
            )?;
            continue;
        }
        if !metadata.is_file() {
            return Err(BicDbError::EncryptionKeyInvalid(format!(
                "key rotation refuses non-regular object {}",
                source_path.display()
            )));
        }
        #[cfg(unix)]
        if metadata.nlink() > 1 {
            return Err(BicDbError::EncryptionKeyInvalid(format!(
                "key rotation refuses multiply-linked object {}",
                source_path.display()
            )));
        }
        if child_relative == Path::new(ENCRYPTION_METADATA_FILE)
            || child_relative == Path::new("store.lock")
            || child_relative == Path::new(storage::BACKUP_WRITE_GATE)
        {
            continue;
        }
        if child_relative == Path::new(storage::BACKUP_ONLINE_LOCK)
            || name.to_string_lossy().ends_with(".tmp")
        {
            return Err(BicDbError::EncryptionKeyInvalid(format!(
                "key rotation refuses incomplete transient object {}",
                source_path.display()
            )));
        }
        counters.files = counters.files.saturating_add(1);
        if child_relative == Path::new(MESH_SIGNING_KEY_FILE) {
            reencrypt_mesh_signing_key(
                &source_path,
                &target_path,
                source_encryption,
                target_encryption,
                fsync,
            )?;
            continue;
        }
        let magic = read_magic(&source_path)?;
        if magic == Some(*b"BICF") {
            counters.frames = counters
                .frames
                .saturating_add(storage::reencrypt_bound_frame_file(
                    &source_path,
                    &target_path,
                    source_encryption,
                    target_encryption,
                    fsync,
                )?);
            continue;
        }
        let in_large_values = child_relative.components().next()
            == Some(Component::Normal(std::ffi::OsStr::new(
                large_value::DEFAULT_LARGE_VALUES_DIR,
            )));
        if in_large_values {
            if magic != Some(*b"BICB") {
                return Err(BicDbError::EncryptionKeyInvalid(format!(
                    "key rotation found an unknown attachment object {}",
                    source_path.display()
                )));
            }
            counters.attachment_chunks =
                counters
                    .attachment_chunks
                    .saturating_add(large_value::reencrypt_bound_file(
                        &source_path,
                        &target_path,
                        source_encryption,
                        target_encryption,
                        fsync,
                    )?);
            continue;
        }
        copy_regular_file(&source_path, &target_path, fsync)?;
    }
    Ok(())
}

fn reencrypt_mesh_signing_key(
    source_path: &Path,
    target_path: &Path,
    source_encryption: &EncryptionRuntime,
    target_encryption: &EncryptionRuntime,
    fsync: bool,
) -> Result<()> {
    let bytes = read_regular_nofollow(source_path, 64 * 1024)?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    if value.get("format").and_then(serde_json::Value::as_u64) != Some(2)
        || value.get("protection").and_then(serde_json::Value::as_str)
            != Some("database_encryption")
    {
        return Err(BicDbError::EncryptionKeyInvalid(
            "cell-bound rotation refuses an unencrypted mesh identity secret".to_string(),
        ));
    }
    let ciphertext_hex = value
        .get("ciphertext_hex")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            BicDbError::EncryptionKeyInvalid(
                "encrypted mesh identity secret has no ciphertext".to_string(),
            )
        })?;
    let ciphertext = hex::decode(ciphertext_hex).map_err(|_| {
        BicDbError::EncryptionKeyInvalid(
            "encrypted mesh identity secret is not hexadecimal".to_string(),
        )
    })?;
    let secret = Zeroizing::new(source_encryption.decrypt_for(
        EncryptionObjectPurpose::IdentitySecret,
        source_path,
        &ciphertext,
        MESH_SIGNING_KEY_AAD,
    )?);
    if secret.len() != 32 {
        return Err(BicDbError::EncryptionKeyInvalid(
            "mesh identity secret has the wrong length".to_string(),
        ));
    }
    let rotated = target_encryption.encrypt_for(
        EncryptionObjectPurpose::IdentitySecret,
        target_path,
        &secret,
        MESH_SIGNING_KEY_AAD,
    )?;
    let document = serde_json::json!({
        "format": 2,
        "algorithm": "ed25519",
        "protection": "database_encryption",
        "ciphertext_hex": hex::encode(rotated),
    });
    storage::write_atomic(target_path, &serde_json::to_vec_pretty(&document)?, fsync)
}

fn copy_regular_file(source: &Path, target: &Path, fsync: bool) -> Result<()> {
    let mut input = open_regular_nofollow(source)?;
    let mut target_options = OpenOptions::new();
    target_options.create_new(true).write(true);
    #[cfg(unix)]
    target_options.custom_flags(libc::O_NOFOLLOW).mode(0o600);
    let mut output = target_options.open(target)?;
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        output.write_all(&buffer[..count])?;
    }
    if fsync {
        output.sync_all()?;
    }
    Ok(())
}

fn read_magic(path: &Path) -> Result<Option<[u8; 4]>> {
    let mut file = open_regular_nofollow(path)?;
    let mut magic = [0_u8; 4];
    let mut read = 0_usize;
    while read < magic.len() {
        let count = file.read(&mut magic[read..])?;
        if count == 0 {
            return Ok(None);
        }
        read += count;
    }
    Ok(Some(magic))
}

fn tree_digest_hex(root: &Path) -> Result<String> {
    let mut files = Vec::new();
    collect_tree_files(root, Path::new(""), &mut files)?;
    files.sort();
    let mut digest = Sha256::new();
    digest.update(b"bicdb.rotation-tree.v1\0");
    for relative in files {
        let relative_text = relative.to_str().ok_or_else(|| {
            BicDbError::EncryptionKeyInvalid("rotation tree contains a non-UTF-8 path".to_string())
        })?;
        let bytes = relative_text.as_bytes();
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(bytes);
        let mut file = open_regular_nofollow(&root.join(&relative))?;
        let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            digest.update(&buffer[..count]);
        }
    }
    Ok(hex::encode(digest.finalize()))
}

fn collect_tree_files(root: &Path, relative: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    let mut entries = fs::read_dir(root.join(relative))?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let child = relative.join(entry.file_name());
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() || (!metadata.is_dir() && !metadata.is_file()) {
            return Err(BicDbError::EncryptionKeyInvalid(format!(
                "rotation digest refuses unsafe object {}",
                entry.path().display()
            )));
        }
        #[cfg(unix)]
        if metadata.is_file() && metadata.nlink() > 1 {
            return Err(BicDbError::EncryptionKeyInvalid(format!(
                "rotation digest refuses multiply-linked object {}",
                entry.path().display()
            )));
        }
        if metadata.is_dir() {
            collect_tree_files(root, &child, files)?;
        } else if child != Path::new("store.lock") && child != Path::new(storage::BACKUP_WRITE_GATE)
        {
            files.push(child);
        }
    }
    Ok(())
}

fn persist_journal(path: &Path, journal: &RotationJournal, fsync: bool) -> Result<()> {
    storage::write_atomic(path, &serde_json::to_vec_pretty(journal)?, fsync)
}

fn load_journal(path: &Path) -> Result<RotationJournal> {
    let bytes = read_regular_nofollow(path, 64 * 1024)?;
    let journal: RotationJournal = serde_json::from_slice(&bytes)?;
    if journal.format != ROTATION_FORMAT {
        return Err(BicDbError::EncryptionKeyInvalid(
            "unsupported key-rotation journal format".to_string(),
        ));
    }
    Ok(journal)
}

fn open_regular_nofollow(path: &Path) -> Result<File> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(BicDbError::EncryptionKeyInvalid(format!(
            "rotation refuses non-regular object {}",
            path.display()
        )));
    }
    #[cfg(unix)]
    if metadata.nlink() > 1 {
        return Err(BicDbError::EncryptionKeyInvalid(format!(
            "rotation refuses multiply-linked object {}",
            path.display()
        )));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    let file = options.open(path)?;
    let opened = file.metadata()?;
    if !opened.is_file() || opened.len() != metadata.len() {
        return Err(BicDbError::EncryptionKeyInvalid(format!(
            "rotation object changed while opening {}",
            path.display()
        )));
    }
    #[cfg(unix)]
    if opened.dev() != metadata.dev() || opened.ino() != metadata.ino() || opened.nlink() > 1 {
        return Err(BicDbError::EncryptionKeyInvalid(format!(
            "rotation object identity changed while opening {}",
            path.display()
        )));
    }
    Ok(file)
}

fn read_regular_nofollow(path: &Path, max_bytes: u64) -> Result<Vec<u8>> {
    let file = open_regular_nofollow(path)?;
    let length = file.metadata()?.len();
    if length > max_bytes {
        return Err(BicDbError::EncryptionKeyInvalid(format!(
            "rotation object {} exceeds its {}-byte bound",
            path.display(),
            max_bytes
        )));
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length {
        return Err(BicDbError::EncryptionKeyInvalid(format!(
            "rotation object changed while reading {}",
            path.display()
        )));
    }
    Ok(bytes)
}

fn validate_journal(
    journal: &RotationJournal,
    database_name: &str,
    old_binding: &EncryptionBinding,
    new_binding: &EncryptionBinding,
) -> Result<()> {
    if journal.database_name != database_name
        || &journal.old_binding != old_binding
        || &journal.new_binding != new_binding
        || journal.staged_tree_sha256.len() != 64
        || !journal
            .staged_tree_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(BicDbError::EncryptionKeyInvalid(
            "key-rotation journal does not match this exact transition".to_string(),
        ));
    }
    Ok(())
}

fn report(
    journal: &RotationJournal,
    activated: bool,
    source_path: PathBuf,
    retired_path: PathBuf,
) -> BoundEncryptionRotationReport {
    BoundEncryptionRotationReport {
        checkpoint: journal.checkpoint,
        activated,
        source_path,
        retired_path,
        old_key_epoch: journal.old_binding.key_epoch,
        new_key_epoch: journal.new_binding.key_epoch,
        files_processed: journal.files_processed,
        frames_reencrypted: journal.frames_reencrypted,
        attachment_chunks_reencrypted: journal.attachment_chunks_reencrypted,
        staged_tree_sha256: journal.staged_tree_sha256.clone(),
    }
}

fn create_restricted_directory(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    builder.mode(0o700);
    builder.create(path)?;
    Ok(())
}

fn sync_tree_directories(root: &Path) -> Result<()> {
    let mut directories = Vec::new();
    collect_directories(root, &mut directories)?;
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        File::open(directory)?.sync_all()?;
    }
    Ok(())
}

fn collect_directories(path: &Path, directories: &mut Vec<PathBuf>) -> Result<()> {
    directories.push(path.to_path_buf());
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            collect_directories(&entry.path(), directories)?;
        }
    }
    Ok(())
}

fn sync_directory(path: &Path, fsync: bool) -> Result<()> {
    if fsync {
        File::open(path)?.sync_all()?;
    }
    Ok(())
}

fn reject_symlink_if_exists(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(BicDbError::EncryptionKeyInvalid(
            format!("{label} is a symbolic link"),
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn normal_file_name(path: &Path) -> Result<String> {
    if path.components().next_back().is_none()
        || !matches!(path.components().next_back(), Some(Component::Normal(_)))
    {
        return Err(BicDbError::EncryptionKeyInvalid(
            "database rotation path must end in a normal component".to_string(),
        ));
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            BicDbError::EncryptionKeyInvalid(
                "database rotation name must be valid UTF-8".to_string(),
            )
        })?;
    if name.is_empty() || name.len() > 200 {
        return Err(BicDbError::EncryptionKeyInvalid(
            "database rotation name is empty or too long".to_string(),
        ));
    }
    Ok(name.to_string())
}
