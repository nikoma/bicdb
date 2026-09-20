use std::fmt;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::aead::{Aead, KeyInit, OsRng};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use crate::error::{BicDbError, Result};
use crate::storage;

pub const ENCRYPTION_METADATA_FILE: &str = "encryption.json";

const METADATA_VERSION: u32 = 1;
const BOUND_METADATA_VERSION: u32 = 2;
const ALGORITHM_XCHACHA20_POLY1305: &str = "xchacha20poly1305";
const BOUND_KDF_HKDF_SHA256: &str = "hkdf-sha256-purpose-object";
const BOUND_PURPOSE_KEY_DOMAIN: &[u8] = b"bicdb.bound-purpose-key.v2\0";
const BOUND_OBJECT_KEY_DOMAIN: &[u8] = b"bicdb.bound-object-key.v2\0";
const BOUND_AAD_DOMAIN: &[u8] = b"bicdb.bound-object-aad.v2\0";
const KDF_ARGON2ID: &str = "argon2id";
const SALT_LEN: usize = 16;
pub(crate) const NONCE_LEN: usize = 24;
const KEY_LEN: usize = 32;
const ARGON2_MEMORY_KIB: u32 = 19 * 1024;
const ARGON2_TIME_COST: u32 = 2;
const ARGON2_PARALLELISM: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EncryptionMode {
    Disabled,
    Enabled,
}

#[derive(Clone, PartialEq, Eq)]
pub struct EncryptionConfig {
    pub mode: EncryptionMode,
    pub key_source: KeySource,
    pub binding: Option<EncryptionBinding>,
}

impl EncryptionConfig {
    pub fn disabled() -> Self {
        Self {
            mode: EncryptionMode::Disabled,
            key_source: KeySource::RawKey(Vec::new()),
            binding: None,
        }
    }

    pub fn with_raw_key(key: impl Into<Vec<u8>>) -> Self {
        Self {
            mode: EncryptionMode::Enabled,
            key_source: KeySource::RawKey(key.into()),
            binding: None,
        }
    }

    pub fn with_passphrase(passphrase: impl Into<String>) -> Self {
        Self {
            mode: EncryptionMode::Enabled,
            key_source: KeySource::Passphrase(passphrase.into()),
            binding: None,
        }
    }

    /// Bind every encrypted object to one immutable security domain, profile,
    /// key epoch, and database-relative object path. Bound encryption is a
    /// distinct on-disk format; it never silently opens or upgrades a legacy
    /// unbound database.
    pub fn with_binding(mut self, binding: EncryptionBinding) -> Self {
        self.binding = Some(binding);
        self
    }
}

impl fmt::Debug for EncryptionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncryptionConfig")
            .field("mode", &self.mode)
            .field("key_source", &self.key_source)
            .field("binding", &self.binding)
            .finish()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EncryptionBinding {
    pub security_domain: String,
    pub profile: String,
    pub key_epoch: u64,
}

impl EncryptionBinding {
    pub fn new(
        security_domain: impl Into<String>,
        profile: impl Into<String>,
        key_epoch: u64,
    ) -> Result<Self> {
        let binding = Self {
            security_domain: security_domain.into(),
            profile: profile.into(),
            key_epoch,
        };
        binding.validate()?;
        Ok(binding)
    }

    fn validate(&self) -> Result<()> {
        validate_binding_label("security_domain", &self.security_domain)?;
        validate_binding_label("profile", &self.profile)?;
        if self.key_epoch == 0 {
            return Err(BicDbError::EncryptionKeyInvalid(
                "bound encryption key_epoch must be positive".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum KeySource {
    RawKey(Vec<u8>),
    Passphrase(String),
}

impl Drop for KeySource {
    fn drop(&mut self) {
        match self {
            Self::RawKey(key) => key.zeroize(),
            Self::Passphrase(passphrase) => passphrase.zeroize(),
        }
    }
}

impl fmt::Debug for KeySource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RawKey(_) => formatter.write_str("RawKey(<redacted>)"),
            Self::Passphrase(_) => formatter.write_str("Passphrase(<redacted>)"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncryptionMetadata {
    pub version: u32,
    pub mode: EncryptionMode,
    pub algorithm: String,
    pub kdf: Option<EncryptionKdfMetadata>,
    pub key_version: u32,
    pub created_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<EncryptionBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_key_kdf: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncryptionKdfMetadata {
    pub algorithm: String,
    pub memory_kib: u32,
    pub time_cost: u32,
    pub parallelism: u32,
    pub salt: [u8; SALT_LEN],
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct EncryptedBlob {
    pub version: u32,
    pub algorithm: String,
    pub kdf: Option<EncryptionKdfMetadata>,
    pub nonce: [u8; NONCE_LEN],
    pub ciphertext_sha256: String,
    pub ciphertext: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct KeyRotationPlan {
    pub current_key_version: Option<u32>,
    pub next_key_version: u32,
    pub requires_segment_rewrite: bool,
    pub note: String,
}

/// A cryptographic purpose domain below a cell root key. Bound encryption
/// derives an independent purpose key before deriving an object-specific key,
/// preventing a derived key from crossing data, log, replica, temporary,
/// attachment, index, search, audit, identity, or backup authority boundaries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncryptionObjectPurpose {
    Data,
    Wal,
    Replication,
    Temporary,
    Blob,
    Index,
    Search,
    Audit,
    IdentitySecret,
    Backup,
    General,
}

impl EncryptionObjectPurpose {
    fn label(self) -> &'static [u8] {
        match self {
            Self::Data => b"data",
            Self::Wal => b"wal",
            Self::Replication => b"replication",
            Self::Temporary => b"temporary",
            Self::Blob => b"blob",
            Self::Index => b"index",
            Self::Search => b"search",
            Self::Audit => b"audit",
            Self::IdentitySecret => b"identity-secret",
            Self::Backup => b"backup",
            Self::General => b"general",
        }
    }
}

/// Opaque, cloneable access to a database's cell-bound object cipher. It can
/// seal data only for paths below the database root and never exposes key
/// material. SQL spill and other host-side transient stores use this handle so
/// they inherit the same security-domain, key-epoch, and object-path binding
/// as durable frames.
#[derive(Clone)]
pub struct DatabaseObjectCipher {
    runtime: EncryptionRuntime,
}

impl fmt::Debug for DatabaseObjectCipher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatabaseObjectCipher")
            .field("cell_bound", &true)
            .finish()
    }
}

impl DatabaseObjectCipher {
    pub fn seal(&self, object_path: &Path, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        self.seal_for(
            EncryptionObjectPurpose::General,
            object_path,
            plaintext,
            aad,
        )
    }

    pub fn open(&self, object_path: &Path, ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        self.open_for(
            EncryptionObjectPurpose::General,
            object_path,
            ciphertext,
            aad,
        )
    }

    pub fn seal_for(
        &self,
        purpose: EncryptionObjectPurpose,
        object_path: &Path,
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>> {
        self.runtime
            .encrypt_for(purpose, object_path, plaintext, aad)
    }

    pub fn open_for(
        &self,
        purpose: EncryptionObjectPurpose,
        object_path: &Path,
        ciphertext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>> {
        self.runtime
            .decrypt_for(purpose, object_path, ciphertext, aad)
    }
}

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub(crate) struct EncryptionRuntime {
    #[zeroize(skip)]
    mode: EncryptionMode,
    key: Option<[u8; KEY_LEN]>,
    #[zeroize(skip)]
    key_version: Option<u32>,
    #[zeroize(skip)]
    binding: Option<EncryptionBinding>,
    #[zeroize(skip)]
    database_root: Option<PathBuf>,
}

impl fmt::Debug for EncryptionRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncryptionRuntime")
            .field("mode", &self.mode)
            .field("key_version", &self.key_version)
            .field("key", &self.key.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl EncryptionRuntime {
    pub(crate) fn disabled() -> Self {
        Self {
            mode: EncryptionMode::Disabled,
            key: None,
            key_version: None,
            binding: None,
            database_root: None,
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.key.is_some() && self.mode == EncryptionMode::Enabled
    }

    pub(crate) fn mode(&self) -> EncryptionMode {
        self.mode.clone()
    }

    pub(crate) fn key_version(&self) -> Option<u32> {
        self.key_version
    }

    pub(crate) fn bound_object_cipher(&self) -> Option<DatabaseObjectCipher> {
        self.binding.as_ref()?;
        Some(DatabaseObjectCipher {
            runtime: self.clone(),
        })
    }

    pub(crate) fn encrypt_for(
        &self,
        purpose: EncryptionObjectPurpose,
        object_path: &Path,
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>> {
        let Some(key) = self.key.as_ref() else {
            return Err(BicDbError::EncryptionKeyRequired);
        };
        let (object_key, bound_aad) = self.object_context(purpose, object_path, key, aad)?;
        let mut nonce = [0_u8; NONCE_LEN];
        fill_random(&mut nonce);
        let cipher = XChaCha20Poly1305::new(Key::from_slice(object_key.as_ref()));
        let ciphertext = cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                chacha20poly1305::aead::Payload {
                    msg: plaintext,
                    aad: &bound_aad,
                },
            )
            .map_err(|_| BicDbError::EncryptionKeyInvalid("frame encryption failed".to_string()))?;
        let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    pub(crate) fn decrypt_for(
        &self,
        purpose: EncryptionObjectPurpose,
        path: &Path,
        payload: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>> {
        let Some(key) = self.key.as_ref() else {
            return Err(BicDbError::EncryptionKeyRequired);
        };
        if payload.len() < NONCE_LEN {
            return Err(BicDbError::DecryptionFailed {
                path: path.to_path_buf(),
                message: "encrypted frame is shorter than its nonce".to_string(),
            });
        }
        let (object_key, bound_aad) = self.object_context(purpose, path, key, aad)?;
        let (nonce, ciphertext) = payload.split_at(NONCE_LEN);
        let cipher = XChaCha20Poly1305::new(Key::from_slice(object_key.as_ref()));
        cipher
            .decrypt(
                XNonce::from_slice(nonce),
                chacha20poly1305::aead::Payload {
                    msg: ciphertext,
                    aad: &bound_aad,
                },
            )
            .map_err(|_| BicDbError::DecryptionFailed {
                path: path.to_path_buf(),
                message: "wrong key or tampered ciphertext".to_string(),
            })
    }

    fn object_context(
        &self,
        purpose: EncryptionObjectPurpose,
        object_path: &Path,
        root_key: &[u8; KEY_LEN],
        caller_aad: &[u8],
    ) -> Result<(Zeroizing<[u8; KEY_LEN]>, Vec<u8>)> {
        let Some(binding) = self.binding.as_ref() else {
            return Ok((Zeroizing::new(*root_key), caller_aad.to_vec()));
        };
        let root = self.database_root.as_ref().ok_or_else(|| {
            BicDbError::EncryptionKeyInvalid(
                "bound encryption runtime has no database root".to_string(),
            )
        })?;
        let relative = normalized_relative_object_path(root, object_path)?;

        let mut salt_hasher = Sha256::new();
        salt_hasher.update(BOUND_PURPOSE_KEY_DOMAIN);
        update_len_prefixed(&mut salt_hasher, binding.security_domain.as_bytes());
        update_len_prefixed(&mut salt_hasher, binding.profile.as_bytes());
        salt_hasher.update(binding.key_epoch.to_le_bytes());
        let salt = salt_hasher.finalize();

        let mut purpose_info = Vec::with_capacity(BOUND_PURPOSE_KEY_DOMAIN.len() + 24);
        purpose_info.extend_from_slice(BOUND_PURPOSE_KEY_DOMAIN);
        append_len_prefixed(&mut purpose_info, purpose.label());
        let purpose_hkdf = Hkdf::<Sha256>::new(Some(&salt), root_key);
        let mut purpose_key = Zeroizing::new([0_u8; KEY_LEN]);
        purpose_hkdf
            .expand(&purpose_info, purpose_key.as_mut())
            .map_err(|_| {
                BicDbError::EncryptionKeyInvalid("bound purpose-key derivation failed".to_string())
            })?;

        let mut object_info =
            Vec::with_capacity(BOUND_OBJECT_KEY_DOMAIN.len() + relative.len() + 8);
        object_info.extend_from_slice(BOUND_OBJECT_KEY_DOMAIN);
        append_len_prefixed(&mut object_info, relative.as_bytes());
        let object_hkdf = Hkdf::<Sha256>::new(Some(&salt), purpose_key.as_ref());
        let mut object_key = Zeroizing::new([0_u8; KEY_LEN]);
        object_hkdf
            .expand(&object_info, object_key.as_mut())
            .map_err(|_| {
                BicDbError::EncryptionKeyInvalid("bound object-key derivation failed".to_string())
            })?;

        let mut bound_aad = Vec::with_capacity(
            BOUND_AAD_DOMAIN.len()
                + binding.security_domain.len()
                + binding.profile.len()
                + purpose.label().len()
                + relative.len()
                + caller_aad.len()
                + 40,
        );
        bound_aad.extend_from_slice(BOUND_AAD_DOMAIN);
        append_len_prefixed(&mut bound_aad, binding.security_domain.as_bytes());
        append_len_prefixed(&mut bound_aad, binding.profile.as_bytes());
        bound_aad.extend_from_slice(&binding.key_epoch.to_le_bytes());
        append_len_prefixed(&mut bound_aad, purpose.label());
        append_len_prefixed(&mut bound_aad, relative.as_bytes());
        append_len_prefixed(&mut bound_aad, caller_aad);
        Ok((object_key, bound_aad))
    }
}

pub(crate) fn open_runtime(
    db_path: &Path,
    config: Option<EncryptionConfig>,
    fsync: bool,
) -> Result<EncryptionRuntime> {
    let metadata_path = db_path.join(ENCRYPTION_METADATA_FILE);
    let existing = read_metadata(&metadata_path)?;

    match (existing, config) {
        (Some(metadata), None) if metadata.mode == EncryptionMode::Enabled => {
            Err(BicDbError::EncryptionKeyRequired)
        }
        (Some(metadata), Some(config)) if metadata.mode == EncryptionMode::Enabled => {
            if config.mode != EncryptionMode::Enabled {
                return Err(BicDbError::EncryptionKeyRequired);
            }
            validate_metadata(&metadata)?;
            validate_requested_binding(&metadata, config.binding.as_ref())?;
            let key = key_from_source(&config.key_source, metadata.kdf.as_ref())?;
            Ok(EncryptionRuntime {
                mode: EncryptionMode::Enabled,
                key: Some(key),
                key_version: Some(metadata.key_version),
                binding: metadata.binding,
                database_root: Some(db_path.to_path_buf()),
            })
        }
        (Some(_), _) => Ok(EncryptionRuntime::disabled()),
        (None, Some(config)) if config.mode == EncryptionMode::Enabled => {
            let (metadata, key) =
                create_metadata_and_key(&config.key_source, config.binding.as_ref())?;
            let bytes = serde_json::to_vec_pretty(&metadata)?;
            storage::write_atomic(&metadata_path, &bytes, fsync)?;
            Ok(EncryptionRuntime {
                mode: EncryptionMode::Enabled,
                key: Some(key),
                key_version: Some(metadata.key_version),
                binding: metadata.binding,
                database_root: Some(db_path.to_path_buf()),
            })
        }
        (None, _) => Ok(EncryptionRuntime::disabled()),
    }
}

pub(crate) fn load_metadata(db_path: &Path) -> Result<Option<EncryptionMetadata>> {
    read_metadata(&db_path.join(ENCRYPTION_METADATA_FILE))
}

/// Refuse the combination of encryption-at-rest and `storage_mode = server_paged`.
///
/// The paged engine writes record bytes straight into its page file through
/// `bicdb-page`, which has no dependency on this module and therefore no
/// knowledge of the database key. Segment frames go through [`EncryptionRuntime`];
/// pages do not. So an encrypted database opened in paged mode would faithfully
/// encrypt its segments while writing the same rows as *plaintext* into
/// `paged/` — the exact confidentiality guarantee the operator asked for,
/// silently not delivered.
///
/// That is worth refusing rather than documenting. An operator who enables
/// encryption has stated a requirement; a mode that cannot meet it must say so
/// at open, not leave readable patient records on disk. `bicdb-core`'s own
/// `encrypted_database_round_trips_and_hides_record_plaintext` test scans every
/// file under the database directory for plaintext, which is how this was
/// caught: running the established suite under `BICDB_STORAGE_MODE=server_paged`
/// failed that assertion on the page file.
///
/// This runs *before* [`open_runtime`], because `open_runtime` creates
/// `encryption.json` for a new encrypted database. Fencing afterwards would
/// leave that file behind on a refused open, breaking ADR-004's requirement that
/// a refusal mutate nothing.
///
/// Lifting this fence means encrypting pages themselves (page-level AEAD with
/// the page id as associated data), which is tracked as its own phase in
/// `docs/server-paged-storage-todo.md`.
pub(crate) fn ensure_compatible_with_storage_mode(
    db_path: &Path,
    mode: &crate::format::StorageMode,
    requested: Option<&EncryptionConfig>,
) -> Result<()> {
    if *mode != crate::format::StorageMode::ServerPaged {
        return Ok(());
    }
    let requested_enabled = requested.is_some_and(|config| config.mode == EncryptionMode::Enabled);
    let existing_enabled =
        load_metadata(db_path)?.is_some_and(|metadata| metadata.mode == EncryptionMode::Enabled);
    if !requested_enabled && !existing_enabled {
        return Ok(());
    }
    Err(BicDbError::FormatCompatibility(
        "encryption at rest is not yet supported with storage_mode `server_paged`: \
         the paged engine writes record bytes into its page file outside the \
         encryption layer, so rows would be stored as plaintext; open refused \
         rather than silently weakening encryption"
            .to_string(),
    ))
}

pub(crate) fn runtime_for_existing(
    db_path: &Path,
    config: Option<EncryptionConfig>,
) -> Result<EncryptionRuntime> {
    let metadata_path = db_path.join(ENCRYPTION_METADATA_FILE);
    let existing = read_metadata(&metadata_path)?;

    match (existing, config) {
        (Some(metadata), None) if metadata.mode == EncryptionMode::Enabled => {
            Err(BicDbError::EncryptionKeyRequired)
        }
        (Some(metadata), Some(config)) if metadata.mode == EncryptionMode::Enabled => {
            if config.mode != EncryptionMode::Enabled {
                return Err(BicDbError::EncryptionKeyRequired);
            }
            validate_metadata(&metadata)?;
            validate_requested_binding(&metadata, config.binding.as_ref())?;
            let key = key_from_source(&config.key_source, metadata.kdf.as_ref())?;
            Ok(EncryptionRuntime {
                mode: EncryptionMode::Enabled,
                key: Some(key),
                key_version: Some(metadata.key_version),
                binding: metadata.binding,
                database_root: Some(db_path.to_path_buf()),
            })
        }
        _ => Ok(EncryptionRuntime::disabled()),
    }
}

pub(crate) fn rotation_plan(metadata: Option<&EncryptionMetadata>) -> KeyRotationPlan {
    let current_key_version = metadata
        .filter(|metadata| metadata.mode == EncryptionMode::Enabled)
        .map(|metadata| metadata.key_version);
    let next_key_version = current_key_version.unwrap_or(0).saturating_add(1);
    KeyRotationPlan {
        current_key_version,
        next_key_version,
        requires_segment_rewrite: current_key_version.is_some(),
        note: "BicDB records key versions in metadata; full key rotation must rewrite existing encrypted segment frames under the next key version.".to_string(),
    }
}

pub(crate) fn encrypt_blob(
    plaintext: &[u8],
    config: &EncryptionConfig,
    aad: &[u8],
) -> Result<EncryptedBlob> {
    if config.mode != EncryptionMode::Enabled {
        return Err(BicDbError::EncryptionKeyRequired);
    }

    let (kdf, key) = match &config.key_source {
        KeySource::RawKey(raw) => (None, raw_key(raw)?),
        KeySource::Passphrase(passphrase) => {
            if passphrase.is_empty() {
                return Err(BicDbError::EncryptionKeyInvalid(
                    "passphrase must not be empty".to_string(),
                ));
            }
            let mut salt = [0_u8; SALT_LEN];
            fill_random(&mut salt);
            let kdf = EncryptionKdfMetadata {
                algorithm: KDF_ARGON2ID.to_string(),
                memory_kib: ARGON2_MEMORY_KIB,
                time_cost: ARGON2_TIME_COST,
                parallelism: ARGON2_PARALLELISM,
                salt,
            };
            let key = derive_passphrase_key(passphrase, &kdf)?;
            (Some(kdf), key)
        }
    };

    let mut nonce = [0_u8; NONCE_LEN];
    fill_random(&mut nonce);
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&key));
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            chacha20poly1305::aead::Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| BicDbError::EncryptionKeyInvalid("encryption failed".to_string()))?;

    Ok(EncryptedBlob {
        version: METADATA_VERSION,
        algorithm: ALGORITHM_XCHACHA20_POLY1305.to_string(),
        kdf,
        nonce,
        ciphertext_sha256: hex::encode(Sha256::digest(&ciphertext)),
        ciphertext,
    })
}

pub(crate) fn decrypt_blob(
    path: &Path,
    blob: &EncryptedBlob,
    config: &EncryptionConfig,
    aad: &[u8],
) -> Result<Vec<u8>> {
    if config.mode != EncryptionMode::Enabled {
        return Err(BicDbError::EncryptionKeyRequired);
    }
    if blob.version != METADATA_VERSION {
        return Err(BicDbError::EncryptionKeyInvalid(format!(
            "unsupported encrypted payload version {}",
            blob.version
        )));
    }
    if blob.algorithm != ALGORITHM_XCHACHA20_POLY1305 {
        return Err(BicDbError::EncryptionKeyInvalid(format!(
            "unsupported encryption algorithm {}",
            blob.algorithm
        )));
    }
    let checksum = hex::encode(Sha256::digest(&blob.ciphertext));
    if checksum != blob.ciphertext_sha256 {
        return Err(BicDbError::TamperEvidence {
            path: path.to_path_buf(),
            message: "encrypted payload checksum mismatch".to_string(),
        });
    }

    let key = match &config.key_source {
        KeySource::RawKey(raw) => raw_key(raw)?,
        KeySource::Passphrase(passphrase) => {
            let Some(kdf) = blob.kdf.as_ref() else {
                return Err(BicDbError::EncryptionKeyInvalid(
                    "encrypted payload has no passphrase KDF".to_string(),
                ));
            };
            derive_passphrase_key(passphrase, kdf)?
        }
    };
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&key));
    cipher
        .decrypt(
            XNonce::from_slice(&blob.nonce),
            chacha20poly1305::aead::Payload {
                msg: blob.ciphertext.as_ref(),
                aad,
            },
        )
        .map_err(|_| BicDbError::DecryptionFailed {
            path: path.to_path_buf(),
            message: "wrong key or tampered ciphertext".to_string(),
        })
}

pub(crate) fn create_metadata_and_key(
    source: &KeySource,
    binding: Option<&EncryptionBinding>,
) -> Result<(EncryptionMetadata, [u8; KEY_LEN])> {
    if let Some(binding) = binding {
        binding.validate()?;
    }
    let (kdf, key) = match source {
        KeySource::RawKey(raw) => (None, raw_key(raw)?),
        KeySource::Passphrase(passphrase) => {
            if passphrase.is_empty() {
                return Err(BicDbError::EncryptionKeyInvalid(
                    "passphrase must not be empty".to_string(),
                ));
            }
            let mut salt = [0_u8; SALT_LEN];
            fill_random(&mut salt);
            let kdf = EncryptionKdfMetadata {
                algorithm: KDF_ARGON2ID.to_string(),
                memory_kib: ARGON2_MEMORY_KIB,
                time_cost: ARGON2_TIME_COST,
                parallelism: ARGON2_PARALLELISM,
                salt,
            };
            let key = derive_passphrase_key(passphrase, &kdf)?;
            (Some(kdf), key)
        }
    };

    Ok((
        EncryptionMetadata {
            version: if binding.is_some() {
                BOUND_METADATA_VERSION
            } else {
                METADATA_VERSION
            },
            mode: EncryptionMode::Enabled,
            algorithm: ALGORITHM_XCHACHA20_POLY1305.to_string(),
            kdf,
            key_version: 1,
            created_at: unix_timestamp(),
            binding: binding.cloned(),
            object_key_kdf: binding.map(|_| BOUND_KDF_HKDF_SHA256.to_string()),
        },
        key,
    ))
}

pub(crate) fn runtime_for_rotation_target(
    db_path: &Path,
    config: &EncryptionConfig,
    key_version: u32,
) -> Result<(EncryptionRuntime, EncryptionMetadata)> {
    if config.mode != EncryptionMode::Enabled || config.binding.is_none() {
        return Err(BicDbError::EncryptionKeyInvalid(
            "rotation target requires enabled cell-bound encryption".to_string(),
        ));
    }
    let (mut metadata, key) = create_metadata_and_key(&config.key_source, config.binding.as_ref())?;
    metadata.key_version = key_version;
    Ok((
        EncryptionRuntime {
            mode: EncryptionMode::Enabled,
            key: Some(key),
            key_version: Some(key_version),
            binding: metadata.binding.clone(),
            database_root: Some(db_path.to_path_buf()),
        },
        metadata,
    ))
}

pub(crate) fn persist_rotation_metadata(
    db_path: &Path,
    metadata: &EncryptionMetadata,
    fsync: bool,
) -> Result<()> {
    validate_metadata(metadata)?;
    let bytes = serde_json::to_vec_pretty(metadata)?;
    storage::write_atomic(&db_path.join(ENCRYPTION_METADATA_FILE), &bytes, fsync)
}

fn key_from_source(
    source: &KeySource,
    kdf: Option<&EncryptionKdfMetadata>,
) -> Result<[u8; KEY_LEN]> {
    match source {
        KeySource::RawKey(raw) => raw_key(raw),
        KeySource::Passphrase(passphrase) => {
            if passphrase.is_empty() {
                return Err(BicDbError::EncryptionKeyInvalid(
                    "passphrase must not be empty".to_string(),
                ));
            }
            let Some(kdf) = kdf else {
                return Err(BicDbError::EncryptionKeyInvalid(
                    "database was not initialized with a passphrase KDF".to_string(),
                ));
            };
            derive_passphrase_key(passphrase, kdf)
        }
    }
}

fn raw_key(raw: &[u8]) -> Result<[u8; KEY_LEN]> {
    if raw.len() != KEY_LEN {
        return Err(BicDbError::EncryptionKeyInvalid(format!(
            "raw key must be {KEY_LEN} bytes"
        )));
    }
    let mut key = [0_u8; KEY_LEN];
    key.copy_from_slice(raw);
    Ok(key)
}

fn derive_passphrase_key(passphrase: &str, kdf: &EncryptionKdfMetadata) -> Result<[u8; KEY_LEN]> {
    if kdf.algorithm != KDF_ARGON2ID {
        return Err(BicDbError::EncryptionKeyInvalid(format!(
            "unsupported KDF {}",
            kdf.algorithm
        )));
    }
    let params = Params::new(
        kdf.memory_kib,
        kdf.time_cost,
        kdf.parallelism,
        Some(KEY_LEN),
    )
    .map_err(|error| BicDbError::EncryptionKeyInvalid(error.to_string()))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key = [0_u8; KEY_LEN];
    argon2
        .hash_password_into(passphrase.as_bytes(), &kdf.salt, &mut key)
        .map_err(|error| BicDbError::EncryptionKeyInvalid(error.to_string()))?;
    Ok(key)
}

fn validate_metadata(metadata: &EncryptionMetadata) -> Result<()> {
    if metadata.version != METADATA_VERSION && metadata.version != BOUND_METADATA_VERSION {
        return Err(BicDbError::EncryptionKeyInvalid(format!(
            "unsupported encryption metadata version {}",
            metadata.version
        )));
    }
    if metadata.algorithm != ALGORITHM_XCHACHA20_POLY1305 {
        return Err(BicDbError::EncryptionKeyInvalid(format!(
            "unsupported encryption algorithm {}",
            metadata.algorithm
        )));
    }
    if let Some(kdf) = &metadata.kdf {
        if kdf.algorithm != KDF_ARGON2ID {
            return Err(BicDbError::EncryptionKeyInvalid(format!(
                "unsupported KDF {}",
                kdf.algorithm
            )));
        }
    }
    match metadata.version {
        METADATA_VERSION => {
            if metadata.binding.is_some() || metadata.object_key_kdf.is_some() {
                return Err(BicDbError::EncryptionKeyInvalid(
                    "legacy encryption metadata cannot carry a security-domain binding".to_string(),
                ));
            }
        }
        BOUND_METADATA_VERSION => {
            let binding = metadata.binding.as_ref().ok_or_else(|| {
                BicDbError::EncryptionKeyInvalid(
                    "bound encryption metadata is missing its binding".to_string(),
                )
            })?;
            binding.validate()?;
            if metadata.object_key_kdf.as_deref() != Some(BOUND_KDF_HKDF_SHA256) {
                return Err(BicDbError::EncryptionKeyInvalid(
                    "bound encryption metadata has an unsupported object-key KDF".to_string(),
                ));
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn validate_requested_binding(
    metadata: &EncryptionMetadata,
    requested: Option<&EncryptionBinding>,
) -> Result<()> {
    if metadata.binding.as_ref() != requested {
        return Err(BicDbError::EncryptionKeyInvalid(
            "encryption security-domain/profile/key-epoch binding does not match the database"
                .to_string(),
        ));
    }
    Ok(())
}

fn read_metadata(path: &Path) -> Result<Option<EncryptionMetadata>> {
    const MAX_ENCRYPTION_METADATA_BYTES: u64 = 64 * 1024;
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > MAX_ENCRYPTION_METADATA_BYTES
    {
        return Err(BicDbError::EncryptionKeyInvalid(format!(
            "encryption metadata {} is not a bounded regular file",
            path.display()
        )));
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    let opened = file.metadata()?;
    if !opened.is_file() || opened.len() != metadata.len() {
        return Err(BicDbError::EncryptionKeyInvalid(
            "encryption metadata changed while opening".to_string(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if opened.dev() != metadata.dev() || opened.ino() != metadata.ino() || opened.nlink() > 1 {
            return Err(BicDbError::EncryptionKeyInvalid(
                "encryption metadata identity changed while opening".to_string(),
            ));
        }
    }
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    file.take(MAX_ENCRYPTION_METADATA_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != opened.len() {
        return Err(BicDbError::EncryptionKeyInvalid(
            "encryption metadata changed while reading".to_string(),
        ));
    }
    let metadata = serde_json::from_slice(&bytes)?;
    Ok(Some(metadata))
}

fn validate_binding_label(name: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(BicDbError::EncryptionKeyInvalid(format!(
            "bound encryption {name} must be 1..=256 portable identifier characters"
        )));
    }
    Ok(())
}

fn normalized_relative_object_path(root: &Path, object_path: &Path) -> Result<String> {
    use std::path::Component;

    let relative = object_path.strip_prefix(root).map_err(|_| {
        BicDbError::EncryptionKeyInvalid(format!(
            "encrypted object {} is outside database root {}",
            object_path.display(),
            root.display()
        ))
    })?;
    let mut normalized = String::new();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(BicDbError::EncryptionKeyInvalid(
                "encrypted object path contains a non-normal component".to_string(),
            ));
        };
        let component = component.to_str().ok_or_else(|| {
            BicDbError::EncryptionKeyInvalid("encrypted object path is not valid UTF-8".to_string())
        })?;
        if !normalized.is_empty() {
            normalized.push('/');
        }
        normalized.push_str(component);
    }
    if normalized.is_empty() || normalized.len() > 4096 {
        return Err(BicDbError::EncryptionKeyInvalid(
            "encrypted object path is empty or exceeds 4096 bytes".to_string(),
        ));
    }
    Ok(normalized)
}

fn append_len_prefixed(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u64).to_le_bytes());
    out.extend_from_slice(value);
}

fn update_len_prefixed(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value);
}

fn fill_random(bytes: &mut [u8]) {
    use chacha20poly1305::aead::rand_core::RngCore;
    OsRng.fill_bytes(bytes);
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

pub(crate) fn tamper(path: &Path, message: impl Into<String>) -> BicDbError {
    BicDbError::TamperEvidence {
        path: PathBuf::from(path),
        message: message.into(),
    }
}
