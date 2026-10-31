//! Encrypted immutable objects and complete, authenticated snapshot manifests.
//! Payloads stream once from the captured source; the manifest is published last.
use super::capture::{error, CapturedSource};
use super::*;
use chacha20poly1305::aead::AeadCore;
use std::collections::BTreeSet;
use std::time::Duration;
use zeroize::Zeroizing;

pub(super) const MAGIC: &[u8; 8] = b"BICBAK04";
const OBJECT_MAGIC: &[u8; 8] = b"BICOBJ04";
const CONFIG_FILE: &str = "repository.json";
const MAX_CONTROL_BYTES: usize = 4096;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RepositoryConfig {
    version: u32,
    repository_id: Uuid,
    kdf: BackupKdf,
    nonce: [u8; NONCE_LEN],
    key_check: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestEnvelope {
    config: RepositoryConfig,
    nonce: [u8; NONCE_LEN],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ObjectRef {
    pub sha256: String,
    pub bytes: u64,
    pub encrypted_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RepositoryFile {
    pub path: String,
    pub object: ObjectRef,
    pub page_extent: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RepositoryManifest {
    pub version: u32,
    pub repository_id: Uuid,
    pub backup_id: Uuid,
    pub source_id: Uuid,
    pub created_at: i64,
    pub commit_seq: u64,
    pub wal_lsn: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extent_generations: Option<bicdb_page::ExtentGenerations>,
    pub source_format: FormatMetadata,
    pub parent_id: Option<Uuid>,
    pub parent_hash: Option<String>,
    pub files: Vec<RepositoryFile>,
    pub manifest_hash: String,
}

impl RepositoryManifest {
    fn hash(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.manifest_hash.clear();
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&unsigned)?)))
    }
    fn validate(&self) -> Result<()> {
        if self.version != 4
            || self.backup_id.is_nil()
            || self.source_id.is_nil()
            || self.repository_id.is_nil()
            || self.created_at <= 0
            || self.files.len() > MAX_BACKUP_FILES
            || self.parent_id.is_some() != self.parent_hash.is_some()
            || self.manifest_hash != self.hash()?
        {
            return Err(error("invalid repository snapshot manifest"));
        }
        if let Some(generations) = &self.extent_generations {
            if generations.identity.len() != 32
                || hex::decode(&generations.identity).is_err()
                || generations.epoch == 0
                || generations.extent_pages == 0
                || generations.versions.len() > MAX_BACKUP_FILES
                || generations
                    .versions
                    .iter()
                    .any(|version| *version > generations.epoch)
            {
                return Err(error("invalid extent generation metadata"));
            }
        }
        if let Some(hash) = &self.parent_hash {
            validate_digest(hash)?;
        }
        let mut names = BTreeSet::new();
        for entry in &self.files {
            validate_backup_relative_path(&entry.path)?;
            if matches!(
                entry.path.as_str(),
                "store.lock" | "paged/store.lock" | "backup-restore.pending"
            ) || entry.path.starts_with(".backup-")
                || entry.path == "backup-source.json"
            {
                return Err(error("repository manifest contains a runtime control path"));
            }
            if entry.path.len() > MAX_BACKUP_PATH_BYTES || !names.insert(&entry.path) {
                return Err(error("duplicate or oversized repository path"));
            }
            validate_digest(&entry.object.sha256)?;
            if entry.object.encrypted_bytes < 32 {
                return Err(error("invalid encrypted object length"));
            }
            if let Some(index) = entry.page_extent {
                let expected = if index == 0 {
                    "paged/store.pages".into()
                } else {
                    format!("paged/store.pages.{index}")
                };
                if entry.path != expected {
                    return Err(error("page extent identity differs from its path"));
                }
            }
        }
        // Files cannot simultaneously name another file's parent directory.
        for path in &names {
            let mut parent = Path::new(path).parent();
            while let Some(directory) = parent {
                if names.contains(&directory.to_str().unwrap_or_default().to_string()) {
                    return Err(error("repository manifest has overlapping file paths"));
                }
                parent = directory.parent();
            }
        }
        format::check_backup_restore_compatible(&self.source_format)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepositoryBackupReport {
    pub artifact: BackupCreateReport,
    pub source_id: Uuid,
    pub commit_seq: u64,
    pub captured_at: i64,
    pub wal_lsn: Option<u64>,
    pub coordination_millis: u64,
    pub source_bytes_read: u64,
    pub uploaded_bytes: u64,
    pub reused_bytes: u64,
    pub page_before_image_bytes: u64,
    pub page_extents_copied: u64,
    pub page_extents_reused: u64,
    pub page_extent_bytes_reused: u64,
}

struct PendingFile(PathBuf);
impl Drop for PendingFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

struct Repository {
    root: PathBuf,
    config: RepositoryConfig,
    key: Zeroizing<[u8; KEY_LEN]>,
    _lock: bicdb_page::DirectoryLock,
}

fn validate_digest(digest: &str) -> Result<()> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(error("invalid repository object digest"));
    }
    Ok(())
}

fn reject_symlinks(path: &Path) -> Result<()> {
    let mut checked = PathBuf::new();
    for part in path.components() {
        if matches!(part, std::path::Component::ParentDir) {
            return Err(error("parent traversal is not a repository path"));
        }
        checked.push(part);
        match fs::symlink_metadata(&checked) {
            Ok(m) if m.file_type().is_symlink() => {
                return Err(error(format!(
                    "symbolic link in backup path {}",
                    checked.display()
                )))
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

fn open_read(path: &Path) -> Result<File> {
    reject_symlinks(path)?;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(error("backup object is not a regular file"));
    }
    Ok(file)
}

fn create_file(path: &Path) -> Result<File> {
    reject_symlinks(path)?;
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    Ok(options.open(path)?)
}

fn config_aad(config: &RepositoryConfig) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&(
        4_u32,
        config.repository_id,
        &config.kdf,
    ))?)
}

fn new_nonce() -> [u8; NONCE_LEN] {
    XChaCha20Poly1305::generate_nonce(&mut OsRng).into()
}

impl RepositoryConfig {
    fn unlock(&self, passphrase: &str) -> Result<Zeroizing<[u8; KEY_LEN]>> {
        if self.version != 4 || self.repository_id.is_nil() || self.key_check.len() > 128 {
            return Err(error("invalid repository configuration"));
        }
        let key = Zeroizing::new(derive_key_with_kdf(passphrase, &self.kdf)?);
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&key[..]));
        let opened = cipher
            .decrypt(
                XNonce::from_slice(&self.nonce),
                Payload {
                    msg: &self.key_check,
                    aad: &config_aad(self)?,
                },
            )
            .map_err(|_| error("repository key authentication failed"))?;
        if opened != b"BicDB backup repository v4" {
            return Err(error("invalid repository key check"));
        }
        Ok(key)
    }
}

impl Repository {
    fn open(root: &Path, passphrase: &str) -> Result<Self> {
        reject_symlinks(root)?;
        durable_create_dir_all(root)?;
        let lock_dir = root.join(".backup-repository");
        reject_symlinks(&lock_dir)?;
        durable_create_dir_all(&lock_dir)?;
        let lock =
            bicdb_page::DirectoryLock::acquire(&lock_dir).map_err(|e| error(e.to_string()))?;
        let config_path = root.join(CONFIG_FILE);
        let (config, key) = match open_read(&config_path) {
            Ok(mut file) => {
                if file.metadata()?.len() > MAX_CONTROL_BYTES as u64 {
                    return Err(error("repository configuration is too large"));
                }
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)?;
                let config: RepositoryConfig = serde_json::from_slice(&bytes)?;
                let key = config.unlock(passphrase)?;
                (config, key)
            }
            Err(BicDbError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut existing_repository = root.join("objects").exists();
                for entry in fs::read_dir(root)? {
                    let entry = entry?;
                    if entry
                        .path()
                        .extension()
                        .is_some_and(|ext| ext == "bicbackup")
                        && is_manifest(&entry.path())?
                    {
                        existing_repository = true;
                    }
                }
                if existing_repository {
                    return Err(error("repository configuration is missing; refusing to initialize over existing backup objects"));
                }
                let nonce = new_nonce();
                let mut salt = [0; SALT_LEN];
                salt.copy_from_slice(&new_nonce()[..SALT_LEN]);
                let mut config = RepositoryConfig {
                    version: 4,
                    repository_id: Uuid::new_v4(),
                    kdf: BackupKdf {
                        algorithm: "argon2id".into(),
                        memory_kib: ARGON2_MEMORY_KIB,
                        time_cost: ARGON2_TIME_COST,
                        parallelism: ARGON2_PARALLELISM,
                        salt,
                    },
                    nonce,
                    key_check: Vec::new(),
                };
                let key = Zeroizing::new(derive_key_with_kdf(passphrase, &config.kdf)?);
                let cipher = XChaCha20Poly1305::new(Key::from_slice(&key[..]));
                config.key_check = cipher
                    .encrypt(
                        XNonce::from_slice(&nonce),
                        Payload {
                            msg: b"BicDB backup repository v4",
                            aad: &config_aad(&config)?,
                        },
                    )
                    .map_err(|_| error("repository initialization failed"))?;
                storage::write_atomic(&config_path, &serde_json::to_vec(&config)?, true)?;
                (config, key)
            }
            Err(e) => return Err(e),
        };
        let objects = root.join("objects");
        reject_symlinks(&objects)?;
        durable_create_dir_all(objects)?;
        Ok(Self {
            root: root.to_path_buf(),
            config,
            key,
            _lock: lock,
        })
    }

    fn object_path(&self, hash: &str) -> Result<PathBuf> {
        validate_digest(hash)?;
        let path = self.root.join("objects").join(&hash[..2]).join(hash);
        reject_symlinks(&path)?;
        Ok(path)
    }

    fn put_object(
        &self,
        reader: &mut dyn Read,
        bytes: u64,
        snapshot: &OnlineBackupSnapshot,
        control: &OnlineBackupControl,
    ) -> Result<ObjectRef> {
        let pending = PendingFile(
            self.root
                .join("objects")
                .join(format!(".tmp-{}", Uuid::new_v4())),
        );
        let file = create_file(&pending.0)?;
        let mut buffered = BufWriter::new(file);
        let mut out = CountingWriter::new(&mut buffered);
        let nonce = new_nonce();
        let mut prefix = [0; NONCE_PREFIX_LEN];
        prefix.copy_from_slice(&nonce[..NONCE_PREFIX_LEN]);
        let header = ObjectHeader {
            version: 4,
            repository_id: self.config.repository_id,
            bytes,
            nonce_prefix: prefix,
        };
        let header_bytes = serde_json::to_vec(&header)?;
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&self.key[..]));
        let hash = object_header_hash(&header_bytes);
        out.write_all(OBJECT_MAGIC)?;
        out.write_all(&(header_bytes.len() as u32).to_le_bytes())?;
        out.write_all(&header_bytes)?;
        let seal = cipher
            .encrypt(
                XNonce::from_slice(&streaming_nonce(&prefix, 0)),
                Payload {
                    msg: &[],
                    aad: &streaming_frame_aad(&hash, 0, STREAMING_HEADER_SEAL_CODEC, 0),
                },
            )
            .map_err(|_| error("object header authentication failed"))?;
        out.write_all(&seal)?;
        let mut frame = 1;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0; STREAMING_BACKUP_CHUNK_BYTES];
        let mut remaining = bytes;
        while remaining > 0 {
            snapshot.check(control)?;
            let count = remaining.min(buffer.len() as u64) as usize;
            reader.read_exact(&mut buffer[..count])?;
            hasher.update(&buffer[..count]);
            write_streaming_frame(
                &mut out,
                &cipher,
                &prefix,
                &hash,
                &mut frame,
                &buffer[..count],
                true,
            )?;
            control.read(count as u64);
            snapshot.throttle(control)?;
            remaining -= count as u64;
        }
        out.flush()?;
        let encrypted_bytes = out.bytes_written();
        // CountingWriter borrows its writer; drop it before syncing/publication.
        drop(out);
        buffered.get_ref().sync_all()?;
        drop(buffered);
        let object = ObjectRef {
            sha256: hex::encode(hasher.finalize()),
            bytes,
            encrypted_bytes,
        };
        let destination = self.object_path(&object.sha256)?;
        let parent = destination.parent().unwrap();
        durable_create_dir_all(parent)?;
        match fs::hard_link(&pending.0, &destination) {
            Ok(()) => {
                sync_parent_directory(&destination)?;
                // Verify the first upload from its actual encrypted bytes.
                self.read_object_checked(&object, &mut std::io::sink(), &|| {
                    snapshot.check(control)
                })?;
                control.uploaded(encrypted_bytes);
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Encryption uses fresh nonces: the pre-existing ciphertext
                // can have a different length. Verify its plaintext identity.
                let mut existing = object.clone();
                existing.encrypted_bytes = open_read(&destination)?.metadata()?.len();
                self.read_object_checked(&existing, &mut std::io::sink(), &|| {
                    snapshot.check(control)
                })?;
                control.reused(existing.bytes);
                return Ok(existing);
            }
            Err(e) => return Err(e.into()),
        }
        Ok(object)
    }

    fn check_object(&self, object: &ObjectRef) -> Result<()> {
        let file = open_read(&self.object_path(&object.sha256)?)?;
        if file.metadata()?.len() != object.encrypted_bytes {
            return Err(error("repository object size differs from its manifest"));
        }
        Ok(())
    }

    fn read_object(&self, object: &ObjectRef, destination: &mut dyn Write) -> Result<()> {
        self.read_object_checked(object, destination, &|| Ok(()))
    }

    fn read_object_checked(
        &self,
        object: &ObjectRef,
        destination: &mut dyn Write,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<()> {
        check()?;
        self.check_object(object)?;
        let mut reader = BufReader::new(open_read(&self.object_path(&object.sha256)?)?);
        let mut magic = [0; 8];
        reader.read_exact(&mut magic)?;
        if &magic != OBJECT_MAGIC {
            return Err(error("invalid repository object magic"));
        }
        let len = read_u32_from_reader(&mut reader)? as usize;
        if len == 0 || len > MAX_CONTROL_BYTES {
            return Err(error("repository object header exceeds its bound"));
        }
        let mut bytes = vec![0; len];
        reader.read_exact(&mut bytes)?;
        let header: ObjectHeader = serde_json::from_slice(&bytes)?;
        if header.version != 4
            || header.repository_id != self.config.repository_id
            || header.bytes != object.bytes
        {
            return Err(error("repository object header differs from its manifest"));
        }
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&self.key[..]));
        let header_hash = object_header_hash(&bytes);
        let mut seal = [0; AEAD_TAG_BYTES];
        reader.read_exact(&mut seal)?;
        let clear = cipher
            .decrypt(
                XNonce::from_slice(&streaming_nonce(&header.nonce_prefix, 0)),
                Payload {
                    msg: &seal,
                    aad: &streaming_frame_aad(&header_hash, 0, STREAMING_HEADER_SEAL_CODEC, 0),
                },
            )
            .map_err(|_| error("repository object header authentication failed"))?;
        if !clear.is_empty() {
            return Err(error("invalid repository object header seal"));
        }
        let mut frames = StreamingArchiveReader {
            reader: &mut reader,
            cipher,
            nonce_prefix: header.nonce_prefix,
            header_hash,
            frame_index: 1,
            chunk_plaintext_bytes: STREAMING_BACKUP_CHUNK_BYTES,
        };
        let mut remaining = object.bytes;
        let mut hasher = Sha256::new();
        while remaining > 0 {
            check()?;
            let expected = remaining.min(STREAMING_BACKUP_CHUNK_BYTES as u64) as usize;
            let chunk = frames.read_frame(expected)?;
            if chunk.len() != expected {
                return Err(error("noncanonical repository object frame length"));
            }
            remaining -= chunk.len() as u64;
            hasher.update(&chunk);
            destination.write_all(&chunk)?;
        }
        frames.ensure_eof()?;
        if hex::encode(hasher.finalize()) != object.sha256 {
            return Err(error("repository object digest mismatch"));
        }
        Ok(())
    }

    fn publish_manifest(
        &self,
        path: &Path,
        manifest: &RepositoryManifest,
        limit: usize,
    ) -> Result<u64> {
        manifest.validate()?;
        reject_symlinks(path)?;
        if path.exists() {
            return Err(error("repository snapshot destination already exists"));
        }
        let plaintext = serde_json::to_vec(manifest)?;
        if plaintext.len() > limit {
            return Err(error("repository manifest exceeds its memory bound"));
        }
        let outer = ManifestEnvelope {
            config: self.config.clone(),
            nonce: new_nonce(),
        };
        let encoded = serde_json::to_vec(&outer)?;
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&self.key[..]));
        let sealed = cipher
            .encrypt(
                XNonce::from_slice(&outer.nonce),
                Payload {
                    msg: &plaintext,
                    aad: &manifest_aad(&encoded),
                },
            )
            .map_err(|_| error("snapshot manifest authentication failed"))?;
        let pending = PendingFile(self.root.join(format!(".tmp-manifest-{}", Uuid::new_v4())));
        let mut file = create_file(&pending.0)?;
        file.write_all(MAGIC)?;
        file.write_all(&(encoded.len() as u32).to_le_bytes())?;
        file.write_all(&encoded)?;
        file.write_all(&(sealed.len() as u64).to_le_bytes())?;
        file.write_all(&sealed)?;
        file.sync_all()?;
        let len = file.metadata()?.len();
        fs::hard_link(&pending.0, path)?;
        sync_parent_directory(path)?;
        Ok(len)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectHeader {
    version: u32,
    repository_id: Uuid,
    bytes: u64,
    nonce_prefix: [u8; NONCE_PREFIX_LEN],
}

fn object_header_hash(bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(OBJECT_MAGIC);
    hash.update(bytes);
    hash.finalize().into()
}
fn manifest_aad(bytes: &[u8]) -> Vec<u8> {
    let mut aad = MAGIC.to_vec();
    aad.extend_from_slice(bytes);
    aad
}

pub(super) fn is_manifest(path: &Path) -> Result<bool> {
    let mut file = open_read(path)?;
    let mut magic = [0; 8];
    let n = file.read(&mut magic)?;
    Ok(n == 8 && &magic == MAGIC)
}

fn read_snapshot(
    path: &Path,
    passphrase: &str,
) -> Result<(
    RepositoryConfig,
    Zeroizing<[u8; KEY_LEN]>,
    RepositoryManifest,
)> {
    let mut file = BufReader::new(open_read(path)?);
    let mut magic = [0; 8];
    file.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(error("invalid repository snapshot magic"));
    }
    let len = read_u32_from_reader(&mut file)? as usize;
    if len == 0 || len > MAX_CONTROL_BYTES {
        return Err(error("repository snapshot envelope exceeds its bound"));
    }
    let mut outer_bytes = vec![0; len];
    file.read_exact(&mut outer_bytes)?;
    let outer: ManifestEnvelope = serde_json::from_slice(&outer_bytes)?;
    let key = outer.config.unlock(passphrase)?;
    let cipher_len = read_u64_from_reader(&mut file)?;
    if cipher_len < AEAD_TAG_BYTES as u64
        || cipher_len > (MAX_BACKUP_HEADER_BYTES + AEAD_TAG_BYTES) as u64
    {
        return Err(error("repository manifest exceeds its memory bound"));
    }
    let mut ciphertext = vec![0; cipher_len as usize];
    file.read_exact(&mut ciphertext)?;
    let mut trailing = [0];
    if file.read(&mut trailing)? != 0 {
        return Err(error("repository snapshot has trailing bytes"));
    }
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&key[..]));
    let plaintext = cipher
        .decrypt(
            XNonce::from_slice(&outer.nonce),
            Payload {
                msg: &ciphertext,
                aad: &manifest_aad(&outer_bytes),
            },
        )
        .map_err(|_| error("repository manifest authentication failed"))?;
    let manifest: RepositoryManifest = serde_json::from_slice(&plaintext)?;
    manifest.validate()?;
    if manifest.repository_id != outer.config.repository_id {
        return Err(error(
            "repository manifest identity differs from its envelope",
        ));
    }
    Ok((outer.config, key, manifest))
}

fn open_snapshot(path: &Path, passphrase: &str) -> Result<(Repository, RepositoryManifest)> {
    let root = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    reject_symlinks(root)?;
    let lock_dir = root.join(".backup-repository");
    reject_symlinks(&lock_dir)?;
    durable_create_dir_all(&lock_dir)?;
    let lock = bicdb_page::DirectoryLock::acquire(&lock_dir).map_err(|e| error(e.to_string()))?;
    let (config, key, manifest) = read_snapshot(path, passphrase)?;
    let repo = Repository {
        root: root.to_path_buf(),
        config,
        key,
        _lock: lock,
    };
    Ok((repo, manifest))
}

fn header_from_manifest(manifest: &RepositoryManifest) -> BackupArchiveHeader {
    let files = manifest
        .files
        .iter()
        .map(|f| BackupManifestEntry {
            path: f.path.clone(),
            size: f.object.bytes,
            sha256: f.object.sha256.clone(),
            fuzzy: false,
        })
        .collect::<Vec<_>>();
    BackupArchiveHeader {
        version: 4,
        backup_id: manifest.backup_id,
        created_at: manifest.created_at,
        full: manifest.parent_id.is_none(),
        base_backup_id: manifest.parent_id,
        base_manifest_hash: manifest.parent_hash.clone(),
        manifest: BackupManifest {
            manifest_hash: manifest.manifest_hash.clone(),
            files: files.clone(),
        },
        source_format: manifest.source_format.clone(),
        files,
        removed_files: Vec::new(),
        recovery: BackupRecoveryMetadata {
            protocol: "pinned-physical-snapshot-plus-captured-WAL".into(),
            checkpoint_timestamp: manifest.created_at,
            ..Default::default()
        },
    }
}

pub(super) fn verify_header(
    path: &Path,
    passphrase: &str,
    payloads: bool,
) -> Result<BackupArchiveHeader> {
    let (repo, manifest) = open_snapshot(path, passphrase)?;
    for file in &manifest.files {
        if payloads {
            repo.read_object(&file.object, &mut std::io::sink())?;
        } else {
            repo.check_object(&file.object)?;
        }
    }
    Ok(header_from_manifest(&manifest))
}

struct PendingDirectory(PathBuf);
impl Drop for PendingDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Restore into inactive staging, authenticate every payload, then install
/// under the destination's stable directory lock. A durable pending marker
/// makes interruption fail closed before any server can open a partial tree.
pub(super) fn restore(
    path: &Path,
    target: &Path,
    options: &BackupRestoreOptions,
) -> Result<BackupRestoreReport> {
    reject_symlinks(target)?;
    let (repo, manifest) = open_snapshot(path, &options.passphrase)?;
    let parent = target
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    reject_symlinks(parent)?;
    durable_create_dir_all(parent)?;
    let target_name = target
        .file_name()
        .ok_or_else(|| error("restore target must name a database directory"))?;
    let canonical_target = parent.canonicalize()?.join(target_name);
    let canonical_repo = repo.root.canonicalize()?;
    if canonical_target.starts_with(&canonical_repo)
        || canonical_repo.starts_with(&canonical_target)
    {
        return Err(error(
            "restore target must be outside the backup repository tree",
        ));
    }
    let staging = PendingDirectory(parent.join(format!(".backup-restore-{}", Uuid::new_v4())));
    durable_create_dir_all(&staging.0)?;
    let mut directories = BTreeSet::new();
    for entry in &manifest.files {
        let output = safe_join(&staging.0, &entry.path)?;
        if let Some(dir) = output.parent() {
            durable_create_dir_all(dir)?;
            directories.insert(dir.to_path_buf());
        }
        let mut writer = BufWriter::new(create_file(&output)?);
        repo.read_object(&entry.object, &mut writer)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
    }
    for directory in directories.iter().rev() {
        sync_parent_directory(&directory.join("entry"))?;
    }
    sync_parent_directory(&staging.0.join("entry"))?;
    durable_create_dir_all(target)?;
    let mut ownership = RestoreOwnership::acquire(target)?;
    let existing = fs::read_dir(target)?.collect::<std::io::Result<Vec<_>>>()?;
    if !options.force && existing.iter().any(|e| e.file_name() != "store.lock") {
        return Err(error(
            "restore target is not empty; pass --force to replace an inactive database",
        ));
    }
    let marker = target.join("backup-restore.pending");
    storage::write_atomic(
        &marker,
        &serde_json::to_vec(&(manifest.backup_id, &manifest.manifest_hash))?,
        true,
    )?;
    clear_restore_payloads(target)?;
    for entry in fs::read_dir(&staging.0)? {
        let entry = entry?;
        if entry.file_name() == "paged" && entry.file_type()?.is_dir() {
            ownership.ensure_paged(target)?;
            for child in fs::read_dir(entry.path())? {
                let child = child?;
                fs::rename(child.path(), target.join("paged").join(child.file_name()))?;
            }
            sync_parent_directory(&target.join("paged/entry"))?;
        } else {
            fs::rename(entry.path(), target.join(entry.file_name()))?;
        }
    }
    sync_parent_directory(&target.join("entry"))?;
    fs::remove_file(&marker)?;
    sync_parent_directory(&marker)?;
    Ok(BackupRestoreReport {
        backup_id: manifest.backup_id,
        files_restored: manifest.files.len(),
        target_path: target.to_path_buf(),
        manifest_hash: manifest.manifest_hash,
        target_timestamp: None,
        restored_records: None,
        source_format_version: manifest.source_format.format_version,
    })
}

struct PageReader<'a> {
    pages: &'a bicdb_page::PageBackupSnapshot,
    first: u64,
    bytes: u64,
    offset: u64,
    page: Vec<u8>,
    position: usize,
}
impl Read for PageReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.offset >= self.bytes || out.is_empty() {
            return Ok(0);
        }
        if self.position == self.page.len() {
            let page = self.first + self.offset / self.pages.page_size() as u64;
            self.pages
                .read_page(page, &mut self.page)
                .map_err(std::io::Error::other)?;
            self.position = 0;
        }
        let count = out
            .len()
            .min(self.page.len() - self.position)
            .min((self.bytes - self.offset) as usize);
        out[..count].copy_from_slice(&self.page[self.position..self.position + count]);
        self.position += count;
        self.offset += count as u64;
        Ok(count)
    }
}

impl OnlineBackupSnapshot {
    /// Write after releasing the host's database guard. Repository publication
    /// is serialized independently and never holds database commit admission.
    pub fn write_backup(
        mut self,
        path: impl AsRef<Path>,
        options: BackupCreateOptions,
        control: OnlineBackupControl,
    ) -> Result<RepositoryBackupReport> {
        let result = self.write_inner(path.as_ref(), &options, &control);
        control.phase(if result.is_ok() {
            OnlineBackupPhase::Complete
        } else if control.check().is_err() {
            OnlineBackupPhase::Cancelled
        } else {
            OnlineBackupPhase::Failed
        });
        result
    }

    fn write_inner(
        &mut self,
        path: &Path,
        options: &BackupCreateOptions,
        control: &OnlineBackupControl,
    ) -> Result<RepositoryBackupReport> {
        if !path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.ends_with(".bicbackup") && !n.starts_with('.'))
        {
            return Err(error(
                "repository manifests must have a visible .bicbackup filename",
            ));
        }
        let root = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        reject_symlinks(root)?;
        let source = self._owner.path.parent().unwrap().canonicalize()?;
        let target = prospective_canonical(root)?;
        if target.starts_with(&source) || source.starts_with(&target) {
            return Err(error("repository must be outside the source database tree"));
        }
        self.check(control)?;
        let repo = Repository::open(root, &options.passphrase)?;
        let generations = self
            .paged
            .as_ref()
            .and_then(|capture| capture.pages.generations.clone());
        let parent = options
            .base_backup
            .as_ref()
            .map(|parent| -> Result<RepositoryManifest> {
                reject_symlinks(parent)?;
                if parent
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new("."))
                    .canonicalize()?
                    != root.canonicalize()?
                {
                    return Err(error("incremental parent must be in the same repository"));
                }
                let (_, _, manifest) = read_snapshot(parent, &options.passphrase)?;
                if manifest.repository_id != repo.config.repository_id
                    || manifest.source_id != self.source_id
                {
                    return Err(error(
                        "incremental parent belongs to another repository or database",
                    ));
                }
                if manifest.commit_seq > self.commit_seq
                    || manifest
                        .wal_lsn
                        .zip(self.paged.as_ref().map(|p| p.lsn))
                        .is_some_and(|(old, new)| old > new)
                {
                    return Err(error(
                        "incremental parent is newer than the captured database",
                    ));
                }
                if manifest
                    .extent_generations
                    .as_ref()
                    .zip(generations.as_ref())
                    .is_some_and(|(old, new)| {
                        old.identity == new.identity && old.epoch >= new.epoch
                    })
                {
                    return Err(error(
                        "incremental parent is newer than the captured extent epoch",
                    ));
                }
                // Check every reference before copying or publishing. Full payload
                // verification and restore drills remain explicit operations.
                for file in &manifest.files {
                    repo.check_object(&file.object)?;
                }
                Ok(manifest)
            })
            .transpose()?;
        let parent_files = parent
            .as_ref()
            .map(|p| {
                p.files
                    .iter()
                    .map(|f| (f.path.as_str(), f))
                    .collect::<std::collections::BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        control.total_files(self.files.len() as u64);
        control.phase(OnlineBackupPhase::Copying);
        let mut files = Vec::with_capacity(self.files.len());
        let mut charge = 0usize;
        let mut page_extents_copied = 0;
        let mut page_extents_reused = 0;
        let mut page_extent_bytes_reused = 0;
        let mut wal_remaining = self
            .files
            .iter()
            .filter(|file| file.path.starts_with("paged/store.wal"))
            .count();
        for file in std::mem::take(&mut self.files) {
            self.check(control)?;
            charge = charge
                .checked_add(file.path.len() * 6 + 512)
                .ok_or_else(|| error("manifest accounting overflow"))?;
            if charge > self.limits.max_manifest_bytes {
                return Err(error("repository manifest exceeds its memory bound"));
            }
            let extent = match &file.source {
                CapturedSource::Pages(index) => Some(*index as u64),
                _ => None,
            };
            let reusable = extent.and_then(|extent| {
                let prior = parent_files.get(file.path.as_str())?;
                let old = parent.as_ref()?.extent_generations.as_ref()?;
                let new = generations.as_ref()?;
                let index = usize::try_from(extent).ok()?;
                (old.identity == new.identity
                    && old.extent_pages == new.extent_pages
                    && old.page_size == new.page_size
                    && prior.page_extent == Some(extent)
                    && prior.object.bytes == file.bytes
                    && old
                        .versions
                        .get(index)
                        .zip(new.versions.get(index))
                        .is_some_and(|(a, b)| a == b))
                .then(|| prior.object.clone())
            });
            if let Some(object) = reusable {
                page_extents_reused += 1;
                page_extent_bytes_reused += object.bytes;
                control.reused(object.bytes);
                files.push(RepositoryFile {
                    path: file.path,
                    object,
                    page_extent: extent,
                });
                control.copied_file();
                continue;
            }
            let mut reader: Box<dyn Read + '_> = match file.source {
                CapturedSource::File(source) => Box::new(BufReader::new(source.take(file.bytes))),
                CapturedSource::Pages(index) => {
                    let pages = &self
                        .paged
                        .as_ref()
                        .ok_or_else(|| error("page capture is missing"))?
                        .pages;
                    Box::new(PageReader {
                        pages,
                        first: index as u64 * pages.extent_pages(),
                        bytes: file.bytes,
                        offset: 0,
                        page: vec![0; pages.page_size() as usize],
                        position: pages.page_size() as usize,
                    })
                }
            };
            if extent.is_some() {
                page_extents_copied += 1;
            }
            let object = repo.put_object(&mut reader, file.bytes, self, control)?;
            drop(reader);
            if file.path.starts_with("paged/store.wal") {
                wal_remaining -= 1;
                if wal_remaining == 0 {
                    if let Some(capture) = &self.paged {
                        capture.release_wal_pin();
                    }
                    control.release_wal();
                }
            }
            files.push(RepositoryFile {
                path: file.path,
                object,
                page_extent: extent,
            });
            control.copied_file();
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));
        self.check(control)?;
        control.phase(OnlineBackupPhase::Verifying);
        for file in &files {
            self.check(control)?;
            repo.check_object(&file.object)?;
        }
        let mut manifest = RepositoryManifest {
            version: 4,
            repository_id: repo.config.repository_id,
            backup_id: Uuid::new_v4(),
            source_id: self.source_id,
            created_at: self.captured_at,
            commit_seq: self.commit_seq,
            wal_lsn: self.paged.as_ref().map(|p| p.lsn),
            extent_generations: generations,
            source_format: self.format.clone(),
            parent_id: parent.as_ref().map(|p| p.backup_id),
            parent_hash: parent.as_ref().map(|p| p.manifest_hash.clone()),
            files,
            manifest_hash: String::new(),
        };
        manifest.manifest_hash = manifest.hash()?;
        self.check(control)?;
        control.phase(OnlineBackupPhase::Publishing);
        let before = self
            .paged
            .as_ref()
            .map(|p| p.pages.stats())
            .transpose()
            .map_err(|e| error(e.to_string()))?
            .map_or(0, |s| s.before_image_bytes);
        let manifest_bytes =
            repo.publish_manifest(path, &manifest, self.limits.max_manifest_bytes)?;
        let progress = control.progress();
        Ok(RepositoryBackupReport {
            artifact: BackupCreateReport {
                backup_id: manifest.backup_id,
                full: manifest.parent_id.is_none(),
                files_total: manifest.files.len(),
                files_included: manifest.files.len(),
                plaintext_bytes: progress.source_bytes_read,
                encrypted_bytes: progress.uploaded_bytes + manifest_bytes,
                manifest_hash: manifest.manifest_hash.clone(),
                path: path.to_path_buf(),
                source_format_version: manifest.source_format.format_version,
                source_feature_flags: feature_flags(&manifest.source_format),
            },
            source_id: self.source_id,
            commit_seq: self.commit_seq,
            wal_lsn: manifest.wal_lsn,
            captured_at: self.captured_at,
            coordination_millis: self.coordination_millis,
            source_bytes_read: progress.source_bytes_read,
            uploaded_bytes: progress.uploaded_bytes + manifest_bytes,
            reused_bytes: progress.reused_bytes,
            page_before_image_bytes: before,
            page_extents_copied,
            page_extents_reused,
            page_extent_bytes_reused,
        })
    }
}

/// Persist every newly created directory entry, including intermediate parents.
pub(super) fn durable_create_dir_all(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    reject_symlinks(path)?;
    if path.as_os_str().is_empty() || path.is_dir() {
        return Ok(());
    }
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        durable_create_dir_all(parent)?;
    }
    match fs::create_dir(path) {
        Ok(()) => sync_parent_directory(path)?,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => {}
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

fn prospective_canonical(path: &Path) -> Result<PathBuf> {
    reject_symlinks(path)?;
    if path.exists() {
        return Ok(path.canonicalize()?);
    }
    let name = path
        .file_name()
        .ok_or_else(|| error("invalid repository directory"))?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    Ok(prospective_canonical(parent)?.join(name))
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RepositoryPruneReport {
    pub manifests_retained: usize,
    pub objects_unreferenced: usize,
    pub bytes_unreferenced: u64,
    pub objects_removed: usize,
    pub bytes_removed: u64,
}

/// Delete only objects unreachable from every authenticated retained manifest.
/// Publication, reads and collection share the same stable repository lock.
pub fn prune_backup_repository(
    root: impl AsRef<Path>,
    passphrase: &str,
    min_age: Duration,
    dry_run: bool,
) -> Result<RepositoryPruneReport> {
    let root = root.as_ref();
    if !root.join(CONFIG_FILE).is_file() {
        return Err(error("backup repository configuration is missing"));
    }
    let repo = Repository::open(root, passphrase)?;
    let mut live = BTreeSet::new();
    let mut report = RepositoryPruneReport::default();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.path().extension().is_some_and(|e| e == "bicbackup") {
            continue;
        }
        if !is_manifest(&entry.path())? {
            // Authenticate the legacy format before excluding it. A damaged
            // v4 magic must not make a retained manifest disappear from GC.
            let mut reader = BufReader::new(open_read(&entry.path())?);
            let mut magic = [0; 8];
            reader.read_exact(&mut magic)?;
            reader.seek(SeekFrom::Start(0))?;
            if &magic == STREAMING_BACKUP_MAGIC {
                let (_stream, _header) = StreamingArchiveReader::open(&mut reader, passphrase)?;
            } else {
                verify_backup_reader_header(&mut reader, passphrase)?;
            }
            continue; // v1-v3 archives own their payloads.
        }
        let (_, _, manifest) = read_snapshot(&entry.path(), passphrase)?;
        if manifest.repository_id != repo.config.repository_id {
            return Err(error("retained manifest belongs to another repository"));
        }
        for file in manifest.files {
            repo.check_object(&file.object)?;
            if live.len() >= MAX_BACKUP_FILES && !live.contains(&file.object.sha256) {
                return Err(error(
                    "repository reachability metadata exceeds its memory bound",
                ));
            }
            live.insert(file.object.sha256);
        }
        report.manifests_retained += 1;
    }
    // Persist observed manifest retirements before deleting payloads. Otherwise
    // a power loss could resurrect a retired manifest after its objects vanish.
    sync_parent_directory(&root.join("entry"))?;
    let now = std::time::SystemTime::now();
    // A killed uploader can leave only unpublished temporary files. The lock
    // excludes live uploaders; the same grace period covers recent debris.
    for directory in [root.to_path_buf(), root.join("objects")] {
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let id = name
                .strip_prefix(".tmp-manifest-")
                .or_else(|| name.strip_prefix(".tmp-"));
            if !id.is_some_and(|id| Uuid::parse_str(id).is_ok()) {
                continue;
            }
            let metadata = fs::symlink_metadata(entry.path())?;
            if !metadata.is_file() {
                return Err(error("invalid repository temporary object"));
            }
            if now.duration_since(metadata.modified()?).unwrap_or_default() < min_age {
                continue;
            }
            report.objects_unreferenced += 1;
            report.bytes_unreferenced += metadata.len();
            if !dry_run {
                fs::remove_file(entry.path())?;
                sync_parent_directory(&entry.path())?;
                report.objects_removed += 1;
                report.bytes_removed += metadata.len();
            }
        }
    }
    for prefix in fs::read_dir(root.join("objects"))? {
        let prefix = prefix?;
        let name = prefix.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.len() != 2
            || !name
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            continue;
        }
        reject_symlinks(&prefix.path())?;
        if !prefix.file_type()?.is_dir() {
            return Err(error("invalid repository object directory"));
        }
        for object in fs::read_dir(prefix.path())? {
            let object = object?;
            let hash = object.file_name().to_string_lossy().into_owned();
            validate_digest(&hash)?;
            if !hash.starts_with(name) {
                return Err(error("repository object is in the wrong directory"));
            }
            let metadata = fs::symlink_metadata(object.path())?;
            if !metadata.is_file() {
                return Err(error("repository object is not a regular file"));
            }
            if live.contains(&hash)
                || now.duration_since(metadata.modified()?).unwrap_or_default() < min_age
            {
                continue;
            }
            report.objects_unreferenced += 1;
            report.bytes_unreferenced += metadata.len();
            if !dry_run {
                fs::remove_file(object.path())?;
                sync_parent_directory(&object.path())?;
                report.objects_removed += 1;
                report.bytes_removed += metadata.len();
            }
        }
    }
    Ok(report)
}
