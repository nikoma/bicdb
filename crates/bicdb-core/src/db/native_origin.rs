//! Native local factory root. The persisted record is private control data;
//! its checksum supplies integrity, not peer or imported recovery authority.
use super::*;

const ROOT_FILE: &str = "native-origin-root-v1.json";
const MAX_ROOT_BYTES: u64 = 16 * 1024;
/// Written only by this crate's own archive restore and open-database copy,
/// for the directory they just materialized. The first open consumes it.
const RESTORE_FILE: &str = "native-origin-restore-v1.json";

fn invalid(detail: &str) -> BicDbError {
    BicDbError::FormatCompatibility(format!("Native origin root: {detail}"))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootRecord {
    version: u32,
    root_id: Uuid,
    directory: bicdb_page::DirectoryPhysicalBirth,
    /// Version 2 only: number of authorized physical rebinds since the factory.
    #[serde(default, skip_serializing_if = "is_zero")]
    custody_generation: u64,
    /// Version 2 only: checksum of the genuine record this one was rebound from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    predecessor_sha256: Option<String>,
    /// Version 2 only: checksum of the original factory record. It names the
    /// root independently of whichever directory currently holds it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    genesis_sha256: Option<String>,
    checksum_sha256: String,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}
fn sha256_shape(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

impl RootRecord {
    fn checksum(&self) -> Result<String> {
        if self.version == 1 {
            return Ok(hex::encode(sha2::Sha256::digest(serde_json::to_vec(&(
                "BicDbNativeOriginRoot-v1",
                self.version,
                self.root_id,
                &self.directory,
            ))?)));
        }
        Ok(hex::encode(sha2::Sha256::digest(serde_json::to_vec(&(
            "BicDbNativeOriginRoot-v2",
            self.version,
            self.root_id,
            &self.directory,
            self.custody_generation,
            &self.predecessor_sha256,
            &self.genesis_sha256,
        ))?)))
    }
    fn validate(&self) -> Result<()> {
        let lineage = match self.version {
            1 => {
                self.custody_generation == 0
                    && self.predecessor_sha256.is_none()
                    && self.genesis_sha256.is_none()
            }
            2 => {
                self.custody_generation > 0
                    && self.predecessor_sha256.as_deref().is_some_and(sha256_shape)
                    && self.genesis_sha256.as_deref().is_some_and(sha256_shape)
            }
            _ => false,
        };
        if !lineage || self.root_id.is_nil() || self.checksum()? != self.checksum_sha256 {
            return Err(invalid("damaged private Native root record"));
        }
        Ok(())
    }
    /// Directory-independent identity: the factory record's checksum. An
    /// authorized restore carries it forward, so bindings that retained it
    /// from an earlier observation still name this root afterwards.
    fn identity_sha256(&self) -> &str {
        self.genesis_sha256
            .as_deref()
            .unwrap_or(&self.checksum_sha256)
    }
}

impl RootRecord {
    /// The same root bound to another physical directory by an authorized
    /// restore: one custody generation later, naming its predecessor.
    fn rebound(&self, directory: bicdb_page::DirectoryPhysicalBirth) -> Result<Self> {
        let mut record = RootRecord {
            version: 2,
            root_id: self.root_id,
            directory,
            custody_generation: self
                .custody_generation
                .checked_add(1)
                .ok_or_else(|| invalid("custody generation overflow"))?,
            predecessor_sha256: Some(self.checksum_sha256.clone()),
            genesis_sha256: Some(self.identity_sha256().to_owned()),
            checksum_sha256: String::new(),
        };
        record.checksum_sha256 = record.checksum()?;
        record.validate()?;
        Ok(record)
    }
}

/// Authorizes exactly one materialized directory to take custody of exactly
/// one archived root record. Like the root record, its checksum is integrity,
/// not a secret: it stops a plain copy, not a forger with write access.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreAuthorization {
    version: u32,
    source_record_sha256: String,
    directory: bicdb_page::DirectoryPhysicalBirth,
    checksum_sha256: String,
}

impl RestoreAuthorization {
    fn checksum(&self) -> Result<String> {
        Ok(hex::encode(sha2::Sha256::digest(serde_json::to_vec(&(
            "BicDbNativeOriginRestore-v1",
            self.version,
            &self.source_record_sha256,
            &self.directory,
        ))?)))
    }
}

fn read_restore_authorization(path: &Path) -> Result<Option<RestoreAuthorization>> {
    let file = path.join(RESTORE_FILE);
    match fs::metadata(&file) {
        Ok(metadata) if metadata.len() > MAX_ROOT_BYTES => {
            return Err(invalid("restore authorization size bound exceeded"))
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let authorization: RestoreAuthorization = serde_json::from_slice(&fs::read(file)?)?;
    if authorization.version != 1 || authorization.checksum()? != authorization.checksum_sha256 {
        return Err(invalid("damaged Native restore authorization"));
    }
    Ok(Some(authorization))
}

fn restore_authorized(
    path: &Path,
    record: &RootRecord,
    actual: &bicdb_page::DirectoryPhysicalBirth,
) -> Result<bool> {
    Ok(
        read_restore_authorization(path)?.is_some_and(|authorization| {
            authorization.source_record_sha256 == record.checksum_sha256
                && authorization.directory == *actual
        }),
    )
}

/// Called by this crate's archive restore and open-database copy once they
/// have materialized `target` themselves. The archive or copy came from a
/// root that passed its own custody check, so the new directory may take
/// custody at its first open. The authorization names this directory's
/// physical identity: a later plain copy of `target` does not inherit it.
pub(crate) fn authorize_materialized_copy(target: &Path) -> Result<()> {
    if !root_active(target)? {
        return Ok(());
    }
    let record = read_record(target)?.ok_or_else(|| invalid("restored root record is missing"))?;
    let actual = bicdb_page::physical_directory_birth(target)
        .map_err(|e| invalid(&e.to_string()))?
        .ok_or_else(|| invalid("physical directory birth is unsupported"))?;
    if record.directory == actual {
        return Ok(());
    }
    let mut authorization = RestoreAuthorization {
        version: 1,
        source_record_sha256: record.checksum_sha256,
        directory: actual,
        checksum_sha256: String::new(),
    };
    authorization.checksum_sha256 = authorization.checksum()?;
    crate::storage::write_atomic(
        &target.join(RESTORE_FILE),
        &serde_json::to_vec_pretty(&authorization)?,
        true,
    )?;
    File::open(target)?.sync_all()?;
    Ok(())
}

/// Under the open's directory claim: take custody of a root this directory
/// was authorized to restore, then consume the authorization.
pub(super) fn complete_authorized_restore(
    path: &Path,
    claim: &bicdb_page::DirectoryLock,
) -> Result<()> {
    if !root_active(path)? {
        return Ok(());
    }
    let record = read_record(path)?.ok_or_else(|| invalid("active root record is missing"))?;
    let actual = claim
        .physical_birth()
        .map_err(|e| invalid(&e.to_string()))?
        .ok_or_else(|| invalid("physical directory birth is unsupported"))?;
    if record.directory != actual {
        if !restore_authorized(path, &record, &actual)? {
            return Err(invalid("claimed directory does not own this Native root"));
        }
        crate::storage::write_atomic(
            &path.join(ROOT_FILE),
            &serde_json::to_vec_pretty(&record.rebound(actual)?)?,
            true,
        )?;
        File::open(path)?.sync_all()?;
    }
    match fs::remove_file(path.join(RESTORE_FILE)) {
        Ok(()) => File::open(path)?.sync_all()?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

/// Constructed only from this actual claimed root's native record and OS
/// custody, never by decoding a caller-supplied capsule into an authority.
#[derive(Debug)]
pub(super) struct NativeOriginRoot {
    record: RootRecord,
}

/// In-process observation from the actual live root. Not deserializable,
/// and still not authenticated peer or target import/recovery authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NativeOriginObservation {
    root_id: Uuid,
    native_record_sha256: String,
    source_open_incarnation: String,
}
impl NativeOriginObservation {
    pub(crate) fn root_id(&self) -> Uuid {
        self.root_id
    }
    pub(super) fn record_sha256(&self) -> &str {
        &self.native_record_sha256
    }
}

pub(super) fn has_prepared_factory(path: &Path) -> Result<bool> {
    Ok(read_record(path)?.is_some())
}

pub(super) fn create_directory(path: &Path) -> Result<bool> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    match fs::create_dir(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => {
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

fn read_record(path: &Path) -> Result<Option<RootRecord>> {
    let file = path.join(ROOT_FILE);
    match fs::metadata(&file) {
        Ok(metadata) if metadata.len() > MAX_ROOT_BYTES => {
            return Err(invalid("root record size bound exceeded"))
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let record: RootRecord = serde_json::from_slice(&fs::read(file)?)?;
    record.validate()?;
    Ok(Some(record))
}

fn root_active(path: &Path) -> Result<bool> {
    Ok(format::read_or_legacy_metadata(path)?
        .feature_flags
        .contains(format::NATIVE_ORIGIN_ROOT_FEATURE_FLAG))
}

pub(crate) fn refuse_foreign_root_preflight(path: &Path) -> Result<()> {
    if !root_active(path)? {
        return Ok(());
    }
    let record = read_record(path)?.ok_or_else(|| invalid("active root record is missing"))?;
    let actual = bicdb_page::physical_directory_birth(path)
        .map_err(|e| invalid(&e.to_string()))?
        .ok_or_else(|| invalid("physical directory birth is unsupported"))?;
    if actual != record.directory && !restore_authorized(path, &record, &actual)? {
        // Operators rebind a legitimate file-level copy offline with
        // `authorize_native_cold_restore` before its first open.
        return Err(invalid("copied or replaced physical directory; an authorized Native import/restore producer is required"));
    }
    Ok(())
}

pub(super) fn load_active(
    path: &Path,
    claim: &bicdb_page::DirectoryLock,
) -> Result<Option<NativeOriginRoot>> {
    if !root_active(path)? {
        return Ok(None);
    }
    let record = read_record(path)?.ok_or_else(|| invalid("active root record is missing"))?;
    let actual = claim
        .physical_birth()
        .map_err(|e| invalid(&e.to_string()))?
        .ok_or_else(|| invalid("physical directory birth is unsupported"))?;
    if actual != record.directory {
        return Err(invalid("claimed directory does not own this Native root"));
    }
    Ok(Some(NativeOriginRoot { record }))
}

pub(super) fn validate_catalog(
    metas: &[CollectionMeta],
    root: Option<&NativeOriginRoot>,
) -> Result<()> {
    for meta in metas {
        if let Some(origin) = meta.native_backing_origin_root {
            if origin.is_nil()
                || meta.native_backing_birth_id.is_none()
                || root.is_none_or(|r| r.record.root_id != origin)
            {
                return Err(invalid(
                    "backing has an unfenced or foreign allocation root",
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn validate_recorded_roots(
    roots: &BTreeSet<Uuid>,
    root: Option<&NativeOriginRoot>,
) -> Result<()> {
    if roots
        .iter()
        .any(|id| root.is_none_or(|r| r.record.root_id != *id))
    {
        return Err(invalid(
            "recorded WAL allocation roots do not belong to this Native root",
        ));
    }
    Ok(())
}

/// What an authorized cold restore did to the copied directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeColdRestoreOutcome {
    /// The source has no Native origin root; a plain copy already opens.
    NotRequired,
    /// The copy now owns the root at this custody generation.
    Rebound { custody_generation: u64 },
    /// An earlier call already rebound this copy from this source.
    AlreadyRebound { custody_generation: u64 },
}

/// Offline, operator-authorized rebind of a file-level copy of a stopped
/// Native store. Authority is exclusive custody of the genuine source: the
/// source must be lockable (stopped) and its record must match its own
/// physical directory, so a copy of a copy that was never rebound is refused.
/// The target must still hold the byte-identical source record. Nothing but
/// the target's root record is written; the root identity is preserved and
/// only its physical custody advances by one generation.
///
/// This does not retire the source. Running both afterwards duplicates one
/// node identity exactly as any cold copy does; higher layers must fence it.
pub fn authorize_native_cold_restore(
    source: impl AsRef<Path>,
    target: impl AsRef<Path>,
) -> Result<NativeColdRestoreOutcome> {
    let (source, target) = (source.as_ref(), target.as_ref());
    if !source.is_dir() || !target.is_dir() {
        return Err(invalid(
            "restore source and target must be existing directories",
        ));
    }
    let source_lock = bicdb_page::DirectoryLock::acquire(source)
        .map_err(|e| invalid(&format!("restore source is not stopped: {e}")))?;
    let target_lock = bicdb_page::DirectoryLock::acquire(target)
        .map_err(|e| invalid(&format!("restore target is in use or is the source: {e}")))?;
    if !root_active(source)? {
        if root_active(target)? {
            return Err(invalid("restore target is not a copy of this source"));
        }
        return Ok(NativeColdRestoreOutcome::NotRequired);
    }
    if !root_active(target)? {
        return Err(invalid("restore target is not a copy of this source"));
    }
    let birth = |lock: &bicdb_page::DirectoryLock| -> Result<bicdb_page::DirectoryPhysicalBirth> {
        lock.physical_birth()
            .map_err(|e| invalid(&e.to_string()))?
            .ok_or_else(|| invalid("physical directory birth is unsupported"))
    };
    let source_record =
        read_record(source)?.ok_or_else(|| invalid("restore source root record is missing"))?;
    if birth(&source_lock)? != source_record.directory {
        return Err(invalid(
            "restore source is itself an unauthorized copy; restore from the genuine root",
        ));
    }
    let actual = birth(&target_lock)?;
    if actual == source_record.directory {
        return Err(invalid("restore target is the source directory"));
    }
    let rebound = source_record.rebound(actual.clone())?;
    let custody_generation = rebound.custody_generation;
    let target_record =
        read_record(target)?.ok_or_else(|| invalid("restore target root record is missing"))?;
    if target_record.directory == actual {
        if target_record.root_id == source_record.root_id
            && target_record.custody_generation == custody_generation
            && target_record.predecessor_sha256.as_deref()
                == Some(source_record.checksum_sha256.as_str())
        {
            return Ok(NativeColdRestoreOutcome::AlreadyRebound { custody_generation });
        }
        return Err(invalid(
            "restore target already owns a different Native root",
        ));
    }
    if fs::read(source.join(ROOT_FILE))? != fs::read(target.join(ROOT_FILE))? {
        return Err(invalid("restore target is not a copy of this source"));
    }
    crate::storage::write_atomic(
        &target.join(ROOT_FILE),
        &serde_json::to_vec_pretty(&rebound)?,
        true,
    )?;
    File::open(target)?.sync_all()?;
    drop(target_lock);
    drop(source_lock);
    Ok(NativeColdRestoreOutcome::Rebound { custody_generation })
}

impl BicDb {
    /// Trusted factory activation for a directory this actual open created
    /// atomically. Merely empty/existing directories are not eligible. A
    /// private durable prepared record may finish its interrupted flag fence.
    /// Existing backing birth metadata is never upgraded or rebound.
    pub fn activate_native_origin_root_v1(&mut self) -> Result<()> {
        self.ensure_writable("activate Native origin root")?;
        self.install_actual_native_origin_root_v1()
    }

    // Also used by the private accepted-import factory. This never accepts a
    // caller's root UUID and never changes this node's role to enable writes.
    pub(super) fn install_actual_native_origin_root_v1(&mut self) -> Result<()> {
        self.check_recovery_state()?;
        self.check_catalog_recovery_quiescent()?;
        if !self.config.fsync {
            return Err(invalid("origin factory requires fsync-enabled storage"));
        }
        let claim = self
            ._directory_lock
            .as_ref()
            .ok_or_else(|| invalid("root has no actual directory claim"))?;
        let actual = claim
            .physical_birth()
            .map_err(|e| invalid(&e.to_string()))?
            .ok_or_else(|| invalid("physical directory birth is unsupported"))?;
        if let Some(root) = &self.native_origin {
            if root.record.directory != actual {
                return Err(invalid("live root directory changed"));
            }
            return Ok(());
        }
        let record = match read_record(&self.path)? {
            Some(record) if record.directory == actual => record,
            Some(_) => return Err(invalid("prepared root belongs to another physical directory")),
            None if self.fresh_directory_created => {
                let mut record = RootRecord { version: 1, root_id: Uuid::new_v4(), directory: actual,
                    custody_generation: 0, predecessor_sha256: None, genesis_sha256: None,
                    checksum_sha256: String::new() };
                record.checksum_sha256 = record.checksum()?;
                // Persist every containing directory entry too, including
                // ancestors just created for a nested database path.
                for ancestor in self.path.canonicalize()?.ancestors() { File::open(ancestor)?.sync_all()?; }
                write_sidecar(&self.path.join(ROOT_FILE), FrameKind::Index,
                    &serde_json::to_vec_pretty(&record)?, true, &EncryptionRuntime::disabled(),
                    "native_origin.root", storage::FailClosed::Always(&self.durability))?;
                record
            }
            None => return Err(invalid("actual atomic directory creation proof is required; an empty or copied directory is insufficient")),
        };
        if let Err(error) = format::enable_native_origin_root(&self.path) {
            self.fence_durability("native_origin.format_fence", &error);
            return Err(error);
        }
        self.native_origin = Some(NativeOriginRoot { record });
        Ok(())
    }

    /// Data-only identity; this grants no imported recovery or peer authority.
    pub fn native_origin_root_id(&self) -> Result<Option<Uuid>> {
        Ok(self.observe_native_origin_root()?.map(|r| r.root_id))
    }

    pub(super) fn fresh_native_backing_origin_root(&self) -> Option<Uuid> {
        self.native_collection_birth_active
            .then(|| self.native_origin.as_ref().map(|r| r.record.root_id))
            .flatten()
    }

    pub(super) fn observe_native_origin_root(&self) -> Result<Option<NativeOriginObservation>> {
        self.check_recovery_state()?;
        let Some(root) = &self.native_origin else {
            return Ok(None);
        };
        let claim = self
            ._directory_lock
            .as_ref()
            .ok_or_else(|| invalid("root has no actual directory claim"))?;
        if claim
            .physical_birth()
            .map_err(|e| invalid(&e.to_string()))?
            .as_ref()
            != Some(&root.record.directory)
        {
            return Err(invalid("live root custody changed"));
        }
        Ok(Some(NativeOriginObservation {
            root_id: root.record.root_id,
            native_record_sha256: root.record.identity_sha256().to_owned(),
            source_open_incarnation: self.incarnation.clone(),
        }))
    }
}

#[cfg(all(test, unix))]
mod restore_tests;
#[cfg(all(test, unix))]
mod tests;
