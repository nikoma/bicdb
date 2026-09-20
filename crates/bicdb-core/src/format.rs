use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{BicDbError, Result};
use crate::storage;

pub const FORMAT_METADATA_FILE: &str = "format.json";
pub const FORMAT_MIGRATION_STATE_FILE: &str = "format-migration.json";
pub const CURRENT_FORMAT_VERSION: u32 = 2;
pub const MIN_READ_FORMAT_VERSION: u32 = 1;
pub const MIN_WRITE_FORMAT_VERSION: u32 = 2;

/// Feature flag written alongside `storage_mode = server_paged`. It exists so
/// that a binary predating [`StorageMode`] entirely — which ignores the
/// `storage_mode` field but already rejects unknown feature flags in
/// [`validate_supported`] — still refuses a server-paged database instead of
/// silently reading it as `embedded_memory`. The compatibility fence therefore
/// holds for binaries already in the field, not just future ones.
pub const SERVER_PAGED_FEATURE_FLAG: &str = "server_paged_storage";

const KNOWN_FEATURE_FLAGS: &[&str] = &["format_metadata_v2", SERVER_PAGED_FEATURE_FLAG];

/// Which storage engine owns a database's durable state. See
/// `docs/decisions/ADR-004-storage-mode-compatibility.md` and
/// `docs/server-paged-storage-todo.md`.
///
/// Serialized as a plain lowercase string. An unrecognized value deserializes to
/// [`StorageMode::Unknown`] rather than failing the parse, so that a database
/// written by a future binary produces an actionable "this binary cannot
/// understand the selected mode" error instead of an opaque serde failure.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum StorageMode {
    /// The current engine: durable segments/WAL on disk, live rows and derived
    /// access structures resident in memory. The only mode this binary implements.
    #[default]
    EmbeddedMemory,
    /// The native-server demand-paged engine: rows live durably in `bicdb-page`
    /// with its own WAL, checkpoints, crash recovery, MVCC and bounded buffer
    /// pool. See `docs/decisions/ADR-004-storage-mode-compatibility.md` for what
    /// this mode does and does not yet guarantee.
    ServerPaged,
    /// A mode name this binary does not recognize (written by a newer binary).
    Unknown(String),
}

impl StorageMode {
    pub fn as_str(&self) -> &str {
        match self {
            Self::EmbeddedMemory => "embedded_memory",
            Self::ServerPaged => "server_paged",
            Self::Unknown(name) => name.as_str(),
        }
    }

    /// Whether this build can actually run the mode. `server_paged` is a known
    /// name with no implementation behind it yet, so it is parsed but not
    /// supported — that distinction is what makes the rejection message useful.
    pub fn is_supported(&self) -> bool {
        matches!(self, Self::EmbeddedMemory | Self::ServerPaged)
    }

    /// Parse a mode name. Unrecognized names become [`Self::Unknown`] rather
    /// than an error, so the refusal message can name what it actually saw.
    pub fn from_name(name: &str) -> Self {
        match name {
            "embedded_memory" => Self::EmbeddedMemory,
            "server_paged" => Self::ServerPaged,
            other => Self::Unknown(other.to_string()),
        }
    }

    /// Feature flags implied by the mode, written into [`FormatMetadata`].
    fn implied_feature_flags(&self) -> BTreeSet<String> {
        match self {
            Self::EmbeddedMemory => BTreeSet::new(),
            _ => BTreeSet::from([SERVER_PAGED_FEATURE_FLAG.to_string()]),
        }
    }
}

impl std::fmt::Display for StorageMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for StorageMode {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for StorageMode {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Ok(Self::from_name(&name))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FormatMetadata {
    pub format_version: u32,
    pub min_reader_version: u32,
    pub min_writer_version: u32,
    #[serde(default)]
    pub feature_flags: BTreeSet<String>,
    /// Durable storage-engine selection. Absent in databases written before this
    /// field existed, which are unambiguously `embedded_memory`.
    #[serde(default)]
    pub storage_mode: StorageMode,
}

impl FormatMetadata {
    pub fn current() -> Self {
        Self::current_with_mode(StorageMode::EmbeddedMemory)
    }

    pub fn current_with_mode(storage_mode: StorageMode) -> Self {
        let mut feature_flags = BTreeSet::from(["format_metadata_v2".to_string()]);
        feature_flags.extend(storage_mode.implied_feature_flags());
        Self {
            format_version: CURRENT_FORMAT_VERSION,
            min_reader_version: MIN_READ_FORMAT_VERSION,
            min_writer_version: MIN_WRITE_FORMAT_VERSION,
            feature_flags,
            storage_mode,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FormatMigrationPlan {
    pub from_version: u32,
    pub to_version: u32,
    pub dry_run: bool,
    pub backup_recommended: bool,
    pub steps: Vec<FormatMigrationStep>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FormatMigrationStep {
    pub name: String,
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FormatMigrationReport {
    pub from_version: u32,
    pub to_version: u32,
    pub backup_recommended: bool,
    pub steps_total: usize,
    pub steps_completed: usize,
    pub recovered_interrupted_migration: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FormatMigrationState {
    from_version: u32,
    to_version: u32,
    steps_total: usize,
    steps_completed: usize,
}

pub fn metadata_path(root: &Path) -> PathBuf {
    root.join(FORMAT_METADATA_FILE)
}

pub fn migration_state_path(root: &Path) -> PathBuf {
    root.join(FORMAT_MIGRATION_STATE_FILE)
}

pub fn load_metadata(root: &Path) -> Result<Option<FormatMetadata>> {
    let path = metadata_path(root);
    match fs::read(&path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub fn read_or_legacy_metadata(root: &Path) -> Result<FormatMetadata> {
    Ok(load_metadata(root)?.unwrap_or_else(|| FormatMetadata {
        format_version: 1,
        min_reader_version: 1,
        min_writer_version: 1,
        feature_flags: BTreeSet::new(),
        // A database with no format metadata at all predates every storage-mode
        // change and is unambiguously the in-memory engine.
        storage_mode: StorageMode::EmbeddedMemory,
    }))
}

/// The durable storage mode of the database at `root`, without opening it.
pub fn storage_mode(root: &Path) -> Result<StorageMode> {
    Ok(read_or_legacy_metadata(root)?.storage_mode)
}

pub fn ensure_open_compatible(root: &Path, fsync: bool) -> Result<FormatMigrationReport> {
    ensure_open_compatible_in_mode(root, fsync, StorageMode::EmbeddedMemory)
}

/// As [`ensure_open_compatible`], but records `mode` for a database being
/// created. Existing databases keep the mode already written down.
pub fn ensure_open_compatible_in_mode(
    root: &Path,
    fsync: bool,
    mode: StorageMode,
) -> Result<FormatMigrationReport> {
    let interrupted = load_migration_state(root)?;
    let mut metadata = read_or_legacy_metadata(root)?;
    if load_metadata(root)?.is_none() && !has_legacy_data(root) {
        // Brand-new database: adopt the requested mode so the migration below
        // persists it, instead of writing the legacy default over it.
        metadata.storage_mode = mode;
    }
    validate_supported(&metadata)?;

    if metadata.format_version == CURRENT_FORMAT_VERSION {
        cleanup_migration_state(root)?;
        return Ok(FormatMigrationReport {
            from_version: metadata.format_version,
            to_version: CURRENT_FORMAT_VERSION,
            backup_recommended: false,
            steps_total: 0,
            steps_completed: 0,
            recovered_interrupted_migration: interrupted.is_some(),
        });
    }

    if metadata.format_version < CURRENT_FORMAT_VERSION {
        let mut report = migrate_to_current(root, &metadata, fsync)?;
        report.recovered_interrupted_migration = interrupted.is_some();
        return Ok(report);
    }

    Err(BicDbError::FormatCompatibility(format!(
        "database format version {} is newer than this binary supports (current {}); open refused without mutating data",
        metadata.format_version, CURRENT_FORMAT_VERSION
    )))
}

pub fn verify_open_compatible(root: &Path) -> Result<()> {
    let metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    if metadata.format_version < MIN_READ_FORMAT_VERSION {
        return Err(BicDbError::FormatCompatibility(format!(
            "database format version {} is older than the minimum readable version {}",
            metadata.format_version, MIN_READ_FORMAT_VERSION
        )));
    }
    if metadata.format_version > CURRENT_FORMAT_VERSION {
        return Err(BicDbError::FormatCompatibility(format!(
            "database format version {} is newer than this binary supports (current {})",
            metadata.format_version, CURRENT_FORMAT_VERSION
        )));
    }
    Ok(())
}

pub fn plan_migration(root: &Path, dry_run: bool) -> Result<FormatMigrationPlan> {
    let metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    if metadata.format_version > CURRENT_FORMAT_VERSION {
        return Err(BicDbError::FormatCompatibility(format!(
            "database format version {} is newer than this binary supports (current {})",
            metadata.format_version, CURRENT_FORMAT_VERSION
        )));
    }
    let steps = if metadata.format_version < CURRENT_FORMAT_VERSION {
        vec![FormatMigrationStep {
            name: "write-format-metadata-v2".to_string(),
            description:
                "write explicit database format metadata and format_metadata_v2 feature flag"
                    .to_string(),
        }]
    } else {
        Vec::new()
    };
    Ok(FormatMigrationPlan {
        from_version: metadata.format_version,
        to_version: CURRENT_FORMAT_VERSION,
        dry_run,
        backup_recommended: !steps.is_empty(),
        steps,
    })
}

pub fn check_backup_restore_compatible(metadata: &FormatMetadata) -> Result<()> {
    validate_supported(metadata)?;
    if metadata.format_version > CURRENT_FORMAT_VERSION {
        return Err(BicDbError::Backup(format!(
            "backup source format version {} is newer than this binary supports (current {})",
            metadata.format_version, CURRENT_FORMAT_VERSION
        )));
    }
    if metadata.format_version < MIN_READ_FORMAT_VERSION {
        return Err(BicDbError::Backup(format!(
            "backup source format version {} is older than the minimum restorable version {}",
            metadata.format_version, MIN_READ_FORMAT_VERSION
        )));
    }
    Ok(())
}

/// Rewrite the format metadata for `root`, preserving its durable storage mode.
///
/// The mode is deliberately read back from disk rather than defaulted: this runs
/// on ordinary flush paths, and defaulting would silently rewrite a server-paged
/// database's metadata as `embedded_memory` — a downgrade of the compatibility
/// fence performed by a routine operation.
pub fn persist_current(root: &Path, fsync: bool) -> Result<()> {
    let mode = read_or_legacy_metadata(root)?.storage_mode;
    ensure_mode_supported(&mode)?;
    persist_metadata(root, &FormatMetadata::current_with_mode(mode), fsync)
}

/// Initialize format metadata for a database being created in `mode`.
pub fn persist_current_with_mode(root: &Path, mode: StorageMode, fsync: bool) -> Result<()> {
    ensure_mode_supported(&mode)?;
    persist_metadata(root, &FormatMetadata::current_with_mode(mode), fsync)
}

/// Refuse a storage mode this build cannot run. Public so that `open` can reject
/// a *requested* mode before any compatibility migration touches the directory.
pub fn ensure_mode_supported(mode: &StorageMode) -> Result<()> {
    if mode.is_supported() {
        return Ok(());
    }
    let detail = match mode {
        StorageMode::ServerPaged => {
            "the server_paged engine is not implemented in this build; see docs/server-paged-storage-todo.md"
        }
        _ => "this binary does not recognize that storage mode; a newer BicDB binary wrote it",
    };
    Err(BicDbError::FormatCompatibility(format!(
        "database storage_mode `{mode}` cannot be opened by this binary: {detail}; open refused without mutating data"
    )))
}

/// Reject an attempt to open an existing database under a different storage mode
/// than the one durably recorded for it. Conversion is an explicit, verified
/// migration (Phase 8) — never a side effect of opening with different config.
pub fn ensure_requested_mode_matches(root: &Path, requested: StorageMode) -> Result<()> {
    let persisted = read_or_legacy_metadata(root)?.storage_mode;
    ensure_modes_match(root, &persisted, &requested)
}

fn ensure_modes_match(root: &Path, persisted: &StorageMode, requested: &StorageMode) -> Result<()> {
    if persisted == requested {
        return Ok(());
    }
    Err(BicDbError::FormatCompatibility(format!(
        "database at {} has storage_mode `{persisted}` but was opened with `{requested}`; \
         BicDB never converts storage mode implicitly — run an explicit migration",
        root.display()
    )))
}

/// The complete pre-open storage-mode fence. Returns the effective mode.
///
/// The order is the contract, not an implementation detail, so it lives here
/// rather than at the call site:
///
/// 1. the *requested* mode must be one this build implements — checked first so
///    that asking for an unavailable engine never creates the directory;
/// 2. the *persisted* mode must be one this build implements — checked before the
///    mismatch test so that meeting a server-paged database reports "this binary
///    cannot run that engine" rather than blaming the caller's default config;
/// 3. only once both are individually runnable do we require that they agree.
///
/// Every step is a pure read. No caller may write to `root` until this returns
/// `Ok`, which is what makes "refused without mutating data" true.
pub fn ensure_storage_mode_compatible(root: &Path, requested: &StorageMode) -> Result<StorageMode> {
    ensure_mode_supported(requested)?;

    if let Some(metadata) = load_metadata(root)? {
        ensure_mode_supported(&metadata.storage_mode)?;
        ensure_modes_match(root, &metadata.storage_mode, requested)?;
        return Ok(metadata.storage_mode);
    }

    // No format metadata. Either a brand-new database, which may be created in
    // whichever mode was asked for, or a pre-v2 database, which predates storage
    // modes entirely and is unambiguously `embedded_memory`.
    //
    // Getting this apart matters in both directions: defaulting a NEW database
    // to embedded_memory makes `server_paged` impossible to create, while
    // adopting the requested mode for an EXISTING legacy database would
    // reinterpret its segments as a page file.
    if has_legacy_data(root) {
        ensure_modes_match(root, &StorageMode::EmbeddedMemory, requested)?;
        return Ok(StorageMode::EmbeddedMemory);
    }

    Ok(requested.clone())
}

/// Whether `root` holds data written before format metadata existed.
fn has_legacy_data(root: &Path) -> bool {
    root.join("segments").exists()
        || root.join("collections.json").exists()
        || root.join("transactions.log").exists()
}

fn migrate_to_current(
    root: &Path,
    metadata: &FormatMetadata,
    fsync: bool,
) -> Result<FormatMigrationReport> {
    let plan = plan_migration(root, false)?;
    let state = FormatMigrationState {
        from_version: metadata.format_version,
        to_version: CURRENT_FORMAT_VERSION,
        steps_total: plan.steps.len(),
        steps_completed: 0,
    };
    persist_migration_state(root, &state, fsync)?;
    // Carry the existing mode forward. A version migration must never be the
    // thing that changes which engine owns the data.
    persist_metadata(
        root,
        &FormatMetadata::current_with_mode(metadata.storage_mode.clone()),
        fsync,
    )?;
    let state = FormatMigrationState {
        steps_completed: plan.steps.len(),
        ..state
    };
    persist_migration_state(root, &state, fsync)?;
    cleanup_migration_state(root)?;
    Ok(FormatMigrationReport {
        from_version: metadata.format_version,
        to_version: CURRENT_FORMAT_VERSION,
        backup_recommended: true,
        steps_total: plan.steps.len(),
        steps_completed: plan.steps.len(),
        recovered_interrupted_migration: false,
    })
}

fn validate_supported(metadata: &FormatMetadata) -> Result<()> {
    // Checked first, and before any migration writes: the whole point of the
    // storage-mode fence is that a binary which cannot run the selected engine
    // refuses the database without mutating a byte of it.
    ensure_mode_supported(&metadata.storage_mode)?;
    if metadata.min_reader_version > CURRENT_FORMAT_VERSION {
        return Err(BicDbError::FormatCompatibility(format!(
            "database requires reader format version {}, but this binary supports {}",
            metadata.min_reader_version, CURRENT_FORMAT_VERSION
        )));
    }
    if metadata.min_writer_version > CURRENT_FORMAT_VERSION {
        return Err(BicDbError::FormatCompatibility(format!(
            "database requires writer format version {}, but this binary supports {}",
            metadata.min_writer_version, CURRENT_FORMAT_VERSION
        )));
    }
    for flag in &metadata.feature_flags {
        if !KNOWN_FEATURE_FLAGS.contains(&flag.as_str()) {
            return Err(BicDbError::FormatCompatibility(format!(
                "database requires unknown format feature flag `{flag}`"
            )));
        }
    }
    Ok(())
}

fn persist_metadata(root: &Path, metadata: &FormatMetadata, fsync: bool) -> Result<()> {
    storage::write_atomic(
        &metadata_path(root),
        &serde_json::to_vec_pretty(metadata)?,
        fsync,
    )
}

fn load_migration_state(root: &Path) -> Result<Option<FormatMigrationState>> {
    let path = migration_state_path(root);
    match fs::read(&path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn persist_migration_state(root: &Path, state: &FormatMigrationState, fsync: bool) -> Result<()> {
    storage::write_atomic(
        &migration_state_path(root),
        &serde_json::to_vec_pretty(state)?,
        fsync,
    )
}

fn cleanup_migration_state(root: &Path) -> Result<()> {
    match fs::remove_file(migration_state_path(root)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}
