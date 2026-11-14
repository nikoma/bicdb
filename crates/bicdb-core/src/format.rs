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

pub const ONLINE_BACKUP_EXTENT_FEATURE_FLAG: &str = "online_backup_extent_tracking_v1";
pub const UNIQUE_NULLS_NOT_DISTINCT_FEATURE_FLAG: &str = "unique_nulls_not_distinct_v1";
pub const ORACLE_INVALID_ROUTINES_FEATURE_FLAG: &str = "oracle_invalid_routines_v1";
pub const SQL_CATALOG_INTENTS_FEATURE_FLAG: &str = "sql_catalog_intents_v1";
pub const SQL_PARTITION_EXECUTION_V1_FEATURE_FLAG: &str = "sql_partition_execution_v1";
/// Generic rename before-images require recovery beyond the recycle journal.
pub const SQL_RENAME_INTENTS_FEATURE_FLAG: &str = "sql_rename_intents_v1";
/// Node-local namespace lineage. Never replicate this as a portable schema
/// reader capability: each destination activates and owns its own identities.
pub const LOCAL_NAMESPACE_IDENTITY_FEATURE_FLAG: &str = "local_namespace_identity_v1";
/// Future-only local allocation identities, never portable reader authority.
pub const NATIVE_COLLECTION_BIRTH_FEATURE_FLAG: &str = "native_collection_birth_v1";
/// Local historical WAL allocation contracts; birth-aware older writers must
/// refuse rather than silently discard these additional commit facts.
pub const NATIVE_WAL_BACKING_CAPTURE_FEATURE_FLAG: &str = "native_wal_backing_capture_v1";
/// Physical Native factory root and allocation scope; local, not peer auth.
pub const NATIVE_ORIGIN_ROOT_FEATURE_FLAG: &str = "native_origin_root_v1";
pub const NATIVE_CATALOG_ALLOCATION_FACTS_FEATURE_FLAG: &str = "native_catalog_allocation_facts_v1";
pub const NATIVE_IMPORTED_BACKING_ORIGINS_FEATURE_FLAG: &str = "native_imported_backing_origins_v1";
/// Local native image receipts require writers that honor image invalidation.
/// This capability is separate from namespace IDs and is never portable schema.
pub const RANGE_IMAGE_INSTALL_FEATURE_FLAG: &str = "range_image_install_v1";
/// Protected pre-BEGIN current-repair proposals; node-local, never portable.
pub const CURRENT_IMAGE_PROPOSAL_FEATURE_FLAG: &str = "current_range_image_proposal_v1";
/// Local recovered serving epochs; older native writers must refuse this DB.
pub const NATIVE_RECOVERY_SERVING_FEATURE_FLAG: &str = "native_recovery_serving_v1";
/// From-birth writer enrollment; local and deliberately not a portable schema capability.
/// Local decision-catalogue custody contract; old writers must refuse.
pub const NATIVE_CATALOGUE_CUSTODY_FEATURE_FLAG: &str = "native_catalogue_custody_v1";
pub const NATIVE_WRITER_ENROLLMENT_FEATURE_FLAG: &str = "native_writer_enrollment_v1";
pub const NATIVE_JOINT_DATA_HANDOFF_FEATURE_FLAG: &str = "native_joint_data_handoff_v1";
/// Local qualified recovery in an already activated joint domain.
pub const NATIVE_CURRENT_JOINT_RECOVERY_FEATURE_FLAG: &str = "native_current_joint_recovery_v1";
/// Local metadata-store initialization/ownership; excluded from portable schema.
pub const NATIVE_METADATA_STORE_FEATURE_FLAG: &str = "native_metadata_store_v1";
pub const NATIVE_INITIAL_PREPARATION_FEATURE_FLAG: &str = "native_initial_preparation_v1";
pub const ORACLE_SIMPLE_CASE_V1_FEATURE_FLAG: &str = "oracle_simple_case_v1";
pub const NATIVE_ABORT_HISTORY_FEATURE_FLAG: &str = "native_abort_history_v1";
pub const APPLICATION_UNIQUE_SCOPE_FEATURE_FLAG: &str = "application_unique_scope_transition_v1";
pub const DECIMAL_PREDICATE_COMPARISON_V1_FEATURE_FLAG: &str = "decimal_predicate_comparison_v1";
pub const NATIVE_IMMUTABLE_FACTS_FEATURE_FLAG: &str = "native_immutable_fact_publications_v1";
pub const EXACT_PROPORTIONAL_INTERVALS_FEATURE_FLAG: &str = "exact_proportional_intervals_v1";
pub const EXACT_DECIMAL_LIST_SUM_FEATURE_FLAG: &str = "exact_decimal_list_sum_v1";
pub const EXACT_DECIMAL_UNITS_FEATURE_FLAG: &str = "exact_decimal_units_v1";
pub const NATIVE_FACT_PROJECTION_CONTRACTS_FEATURE_FLAG: &str =
    "native_fact_projection_contracts_v1";
/// Sticky node-local receipt semantics. Never a portable schema capability.
pub const NATIVE_COMPLETED_DECISION_MATERIALIZATION_FEATURE_FLAG: &str =
    "native_completed_decision_materialization_v1";
/// Node-local election lineage semantics; never a portable SQL/schema feature.
pub const HA_QUALIFIED_DATA_EPOCH_FEATURE_FLAG: &str = "ha_qualified_data_epoch_v1";
const KNOWN_FEATURE_FLAGS: &[&str] = &[
    HA_QUALIFIED_DATA_EPOCH_FEATURE_FLAG,
    SQL_PARTITION_EXECUTION_V1_FEATURE_FLAG,
    EXACT_DECIMAL_UNITS_FEATURE_FLAG,
    NATIVE_COMPLETED_DECISION_MATERIALIZATION_FEATURE_FLAG,
    EXACT_DECIMAL_LIST_SUM_FEATURE_FLAG,
    EXACT_PROPORTIONAL_INTERVALS_FEATURE_FLAG,
    NATIVE_FACT_PROJECTION_CONTRACTS_FEATURE_FLAG,
    NATIVE_IMMUTABLE_FACTS_FEATURE_FLAG,
    DECIMAL_PREDICATE_COMPARISON_V1_FEATURE_FLAG,
    APPLICATION_UNIQUE_SCOPE_FEATURE_FLAG,
    ORACLE_INVALID_ROUTINES_FEATURE_FLAG,
    ORACLE_SIMPLE_CASE_V1_FEATURE_FLAG,
    UNIQUE_NULLS_NOT_DISTINCT_FEATURE_FLAG,
    SQL_CATALOG_INTENTS_FEATURE_FLAG,
    SQL_RENAME_INTENTS_FEATURE_FLAG,
    LOCAL_NAMESPACE_IDENTITY_FEATURE_FLAG,
    NATIVE_COLLECTION_BIRTH_FEATURE_FLAG,
    NATIVE_WAL_BACKING_CAPTURE_FEATURE_FLAG,
    NATIVE_ORIGIN_ROOT_FEATURE_FLAG,
    NATIVE_CATALOG_ALLOCATION_FACTS_FEATURE_FLAG,
    NATIVE_IMPORTED_BACKING_ORIGINS_FEATURE_FLAG,
    RANGE_IMAGE_INSTALL_FEATURE_FLAG,
    CURRENT_IMAGE_PROPOSAL_FEATURE_FLAG,
    NATIVE_RECOVERY_SERVING_FEATURE_FLAG,
    NATIVE_WRITER_ENROLLMENT_FEATURE_FLAG,
    NATIVE_JOINT_DATA_HANDOFF_FEATURE_FLAG,
    NATIVE_CURRENT_JOINT_RECOVERY_FEATURE_FLAG,
    NATIVE_ABORT_HISTORY_FEATURE_FLAG,
    NATIVE_CATALOGUE_CUSTODY_FEATURE_FLAG,
    NATIVE_METADATA_STORE_FEATURE_FLAG,
    NATIVE_INITIAL_PREPARATION_FEATURE_FLAG,
    "format_metadata_v2",
    SERVER_PAGED_FEATURE_FLAG,
    ONLINE_BACKUP_EXTENT_FEATURE_FLAG,
];

// Required semantic readers travel with a captured image; node-local storage,
// identity and authority flags do not. This list only includes implemented
// readers. The contract itself grants no recovery or portable origin authority.
const REPLICATED_READER_FEATURES: &[&str] = &[
    EXACT_DECIMAL_UNITS_FEATURE_FLAG,
    EXACT_DECIMAL_LIST_SUM_FEATURE_FLAG,
    EXACT_PROPORTIONAL_INTERVALS_FEATURE_FLAG,
    NATIVE_FACT_PROJECTION_CONTRACTS_FEATURE_FLAG,
    NATIVE_IMMUTABLE_FACTS_FEATURE_FLAG,
    SQL_PARTITION_EXECUTION_V1_FEATURE_FLAG,
    DECIMAL_PREDICATE_COMPARISON_V1_FEATURE_FLAG,
    APPLICATION_UNIQUE_SCOPE_FEATURE_FLAG,
    ORACLE_INVALID_ROUTINES_FEATURE_FLAG,
    ORACLE_SIMPLE_CASE_V1_FEATURE_FLAG,
    UNIQUE_NULLS_NOT_DISTINCT_FEATURE_FLAG,
    SQL_CATALOG_INTENTS_FEATURE_FLAG,
    SQL_RENAME_INTENTS_FEATURE_FLAG,
];

pub(crate) fn required_reader_features(root: &Path) -> Result<Vec<String>> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    Ok(metadata
        .feature_flags
        .into_iter()
        .filter(|flag| REPLICATED_READER_FEATURES.contains(&flag.as_str()))
        .collect())
}

fn feature_flags_lock(root: &Path) -> std::sync::Arc<parking_lot::Mutex<()>> {
    use std::sync::{Arc, OnceLock, Weak};
    type Locks = std::collections::HashMap<PathBuf, Weak<parking_lot::Mutex<()>>>;
    static LOCKS: OnceLock<parking_lot::Mutex<Locks>> = OnceLock::new();
    let mut locks = LOCKS.get_or_init(Default::default).lock();
    locks.retain(|_, lock| lock.strong_count() != 0);
    let path = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    if let Some(lock) = locks.get(&path).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(parking_lot::Mutex::new(()));
    locks.insert(path, Arc::downgrade(&lock));
    lock
}

pub(crate) fn validate_replicated_reader_features(features: &[String]) -> Result<()> {
    if features.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(BicDbError::FormatCompatibility(
            "replicated reader capabilities must be sorted and unique".into(),
        ));
    }
    for feature in features {
        if !REPLICATED_READER_FEATURES.contains(&feature.as_str()) {
            return Err(BicDbError::FormatCompatibility(format!(
                "unsupported replicated reader capability `{feature}`"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_collection_reader_features(name: &str, features: &[String]) -> Result<()> {
    let required = match name {
        "__bicdb_rename_intents" | "__bicdb_rename_images" | "__bicdb_rename_gc" => {
            Some(SQL_RENAME_INTENTS_FEATURE_FLAG)
        }
        "__bicdb_recycle_intents" | "__bicdb_recycle_purges" | "__bicdb_recycle_creations" => {
            Some(SQL_CATALOG_INTENTS_FEATURE_FLAG)
        }
        _ => None,
    };
    if let Some(required) = required {
        if !features.iter().any(|feature| feature == required) {
            return Err(BicDbError::FormatCompatibility(format!(
                "replicated collection `{name}` requires reader capability `{required}`"
            )));
        }
    }
    Ok(())
}

pub(crate) fn install_replicated_reader_features(root: &Path, features: &[String]) -> Result<bool> {
    validate_replicated_reader_features(features)?;
    if features.is_empty() {
        return Ok(false);
    }
    enable_feature_flags_changed(
        root,
        &features.iter().map(String::as_str).collect::<Vec<_>>(),
    )
}

fn enable_feature_flags(root: &Path, features: &[&str]) -> Result<()> {
    enable_feature_flags_changed(root, features).map(|_| ())
}

fn enable_feature_flags_changed(root: &Path, features: &[&str]) -> Result<bool> {
    // Shared range-apply workers can install different capabilities at once.
    // Serialize the read/merge/fsync with local feature enables so neither
    // writer can replace a flag that the other has just made durable.
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    let changed = features.iter().fold(false, |changed, feature| {
        metadata.feature_flags.insert((*feature).into()) || changed
    });
    if changed {
        persist_metadata(root, &metadata, true)?;
    }
    Ok(changed)
}

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
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
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
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mode = read_or_legacy_metadata(root)?.storage_mode;
    ensure_mode_supported(&mode)?;
    let mut metadata = FormatMetadata::current_with_mode(mode);
    if let Some(previous) = load_metadata(root)? {
        validate_supported(&previous)?;
        metadata.feature_flags.extend(previous.feature_flags);
    }
    persist_metadata(root, &metadata, fsync)
}

/// Initialize format metadata for a database being created in `mode`.
pub fn persist_current_with_mode(root: &Path, mode: StorageMode, fsync: bool) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    ensure_mode_supported(&mode)?;
    let mut metadata = FormatMetadata::current_with_mode(mode);
    if let Some(previous) = load_metadata(root)? {
        validate_supported(&previous)?;
        metadata.feature_flags.extend(previous.feature_flags);
    }
    persist_metadata(root, &metadata, fsync)
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
    let mut migrated = FormatMetadata::current_with_mode(metadata.storage_mode.clone());
    migrated
        .feature_flags
        .extend(metadata.feature_flags.iter().cloned());
    persist_metadata(root, &migrated, fsync)?;
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
    validate_supported_with_flags(metadata, KNOWN_FEATURE_FLAGS)
}
fn validate_supported_with_flags(metadata: &FormatMetadata, known: &[&str]) -> Result<()> {
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
        if !known.contains(&flag.as_str()) {
            return Err(BicDbError::FormatCompatibility(format!(
                "database requires unknown format feature flag `{flag}`"
            )));
        }
    }
    Ok(())
}

fn persist_metadata(root: &Path, metadata: &FormatMetadata, fsync: bool) -> Result<()> {
    persist_metadata_with(root, metadata, fsync, storage::write_atomic)
}

fn persist_metadata_with(
    root: &Path,
    metadata: &FormatMetadata,
    fsync: bool,
    write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
) -> Result<()> {
    // Once published, the local authority fence must survive every replacement,
    // including routine flushes for databases whose row fsync is disabled.
    let fsync = fsync
        || metadata
            .feature_flags
            .contains(HA_QUALIFIED_DATA_EPOCH_FEATURE_FLAG)
        || metadata
            .feature_flags
            .contains(LOCAL_NAMESPACE_IDENTITY_FEATURE_FLAG)
        || metadata
            .feature_flags
            .contains(NATIVE_WAL_BACKING_CAPTURE_FEATURE_FLAG)
        || metadata
            .feature_flags
            .contains(NATIVE_ORIGIN_ROOT_FEATURE_FLAG)
        || metadata
            .feature_flags
            .contains(SQL_PARTITION_EXECUTION_V1_FEATURE_FLAG)
        || metadata
            .feature_flags
            .contains(RANGE_IMAGE_INSTALL_FEATURE_FLAG)
        || metadata
            .feature_flags
            .contains(CURRENT_IMAGE_PROPOSAL_FEATURE_FLAG)
        || metadata
            .feature_flags
            .contains(NATIVE_METADATA_STORE_FEATURE_FLAG)
        || metadata
            .feature_flags
            .contains(NATIVE_CATALOGUE_CUSTODY_FEATURE_FLAG);
    write(
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

/// Fence writers predating durable extent tracking before a reusable identity
/// can be published. Routine metadata rewrites must preserve this capability.
pub(crate) fn enable_online_backup_extent_tracking(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    if metadata.storage_mode == StorageMode::ServerPaged
        && metadata
            .feature_flags
            .insert(ONLINE_BACKUP_EXTENT_FEATURE_FLAG.into())
    {
        persist_metadata(root, &metadata, true)?;
    }
    Ok(())
}

/// Older binaries must refuse a database whose unique keys compare NULLs as equal.
pub(crate) fn enable_unique_nulls_not_distinct(root: &Path) -> Result<()> {
    enable_feature_flags(root, &[UNIQUE_NULLS_NOT_DISTINCT_FEATURE_FLAG])
}

/// Fence older SQL runtimes before publishing a transactional catalog intent.
/// Even an empty journal remains sticky: unknown readers must not skip recovery.
pub(crate) fn enable_sql_catalog_intents(root: &Path) -> Result<()> {
    enable_feature_flags(root, &[SQL_CATALOG_INTENTS_FEATURE_FLAG])
}

/// Install before publishing any generic rename undo images. Readers that
/// only understand recycle moves must refuse this store, even after the last
/// rename intent has been resolved. Never remove this sticky capability.
pub(crate) fn enable_sql_rename_intents(root: &Path) -> Result<()> {
    enable_feature_flags(root, &[SQL_RENAME_INTENTS_FEATURE_FLAG])
}

/// Always sync an activation retry too: a flag visible after a failed
/// directory sync does not yet establish the required-reader fence.
pub(crate) fn enable_sql_partition_execution_v1(
    root: &Path,
    health: &bicdb_page::DurabilityHealth,
) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    metadata
        .feature_flags
        .insert(SQL_PARTITION_EXECUTION_V1_FEATURE_FLAG.into());
    persist_metadata_with(root, &metadata, true, |path, bytes, fsync| {
        storage::replace_durably(
            path,
            storage::ReplaceContents::Plain(bytes),
            fsync,
            "sql_partition.format_fence",
            storage::FailClosed::Always(health),
        )
    })
}

/// Always fsync the sticky fence, including an interrupted activation retry,
/// before any local namespace UUID can become durable in the catalog.
pub(crate) fn enable_local_namespace_identity(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    metadata
        .feature_flags
        .insert(LOCAL_NAMESPACE_IDENTITY_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

/// Fence older readers/writers before a new backing can acquire a birth ID.
/// Retrying this fence must not manufacture identities for older backings.
pub(crate) fn enable_native_collection_birth(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    if !metadata
        .feature_flags
        .contains(LOCAL_NAMESPACE_IDENTITY_FEATURE_FLAG)
    {
        return Err(BicDbError::InvalidConfig(
            "Native collection birth requires local namespace identities".into(),
        ));
    }
    metadata
        .feature_flags
        .insert(NATIVE_COLLECTION_BIRTH_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

pub(crate) fn enable_native_wal_backing_capture(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    if !metadata
        .feature_flags
        .contains(NATIVE_COLLECTION_BIRTH_FEATURE_FLAG)
        || !metadata
            .feature_flags
            .contains(LOCAL_NAMESPACE_IDENTITY_FEATURE_FLAG)
    {
        return Err(BicDbError::InvalidConfig(
            "Native WAL backing capture requires collection births and local namespace identities"
                .into(),
        ));
    }
    metadata
        .feature_flags
        .insert(NATIVE_WAL_BACKING_CAPTURE_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

/// Fence image-unaware readers/writers before trusting native image receipts.
pub(crate) fn enable_native_origin_root(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    metadata
        .feature_flags
        .insert(NATIVE_ORIGIN_ROOT_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

pub(crate) fn enable_native_imported_backing_origins(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    for required in [
        NATIVE_ORIGIN_ROOT_FEATURE_FLAG,
        NATIVE_COLLECTION_BIRTH_FEATURE_FLAG,
        NATIVE_WAL_BACKING_CAPTURE_FEATURE_FLAG,
        LOCAL_NAMESPACE_IDENTITY_FEATURE_FLAG,
    ] {
        if !metadata.feature_flags.contains(required) {
            return Err(BicDbError::InvalidConfig(
                "Native imported origins require actual root, birth, namespace and WAL capture"
                    .into(),
            ));
        }
    }
    metadata
        .feature_flags
        .insert(NATIVE_IMPORTED_BACKING_ORIGINS_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

pub(crate) fn enable_native_catalog_allocation_facts(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    for required in [
        NATIVE_ORIGIN_ROOT_FEATURE_FLAG,
        NATIVE_WAL_BACKING_CAPTURE_FEATURE_FLAG,
    ] {
        if !metadata.feature_flags.contains(required) {
            return Err(BicDbError::InvalidConfig(
                "Native catalog allocation facts require actual origin and WAL capture".into(),
            ));
        }
    }
    metadata
        .feature_flags
        .insert(NATIVE_CATALOG_ALLOCATION_FACTS_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

/// Fence image-unaware readers/writers before trusting native image receipts.
pub(crate) fn enable_range_image_install(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    metadata
        .feature_flags
        .insert(RANGE_IMAGE_INSTALL_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

/// Trusted fresh-factory use only. A flag on a birth-v1 root cannot upgrade its birth.
pub(crate) fn enable_native_writer_enrollment(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    if !metadata
        .feature_flags
        .contains(RANGE_IMAGE_INSTALL_FEATURE_FLAG)
    {
        return Err(BicDbError::InvalidConfig(
            "writer enrollment requires image-aware storage".into(),
        ));
    }
    metadata
        .feature_flags
        .insert(NATIVE_WRITER_ENROLLMENT_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

/// Fresh factory only; existing roots cannot obtain bootstrap authority from it.
pub(crate) fn enable_native_catalogue_custody(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    if !metadata
        .feature_flags
        .contains(NATIVE_WRITER_ENROLLMENT_FEATURE_FLAG)
    {
        return Err(BicDbError::InvalidConfig(
            "catalogue custody requires enforced native writer".into(),
        ));
    }
    metadata
        .feature_flags
        .insert(NATIVE_CATALOGUE_CUSTODY_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}
#[cfg(test)]
pub(crate) fn test_old_reader_without_catalogue_custody(root: &Path) -> Result<()> {
    let metadata = read_or_legacy_metadata(root)?;
    let old: Vec<_> = KNOWN_FEATURE_FLAGS
        .iter()
        .copied()
        .filter(|flag| *flag != NATIVE_CATALOGUE_CUSTODY_FEATURE_FLAG)
        .collect();
    validate_supported_with_flags(&metadata, &old)
}

pub(crate) fn native_catalogue_custody_active(root: &Path) -> Result<bool> {
    let metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    Ok(metadata
        .feature_flags
        .contains(NATIVE_CATALOGUE_CUSTODY_FEATURE_FLAG))
}

pub(crate) fn native_writer_enrollment_active(root: &Path) -> Result<bool> {
    let metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    Ok(metadata
        .feature_flags
        .contains(NATIVE_WRITER_ENROLLMENT_FEATURE_FLAG))
}

/// Fence old HA readers before any qualified-lineage state publication.
/// Unconditional fsync repairs an earlier unknown reader-flag replacement.
pub(crate) fn enable_ha_qualified_data_epoch(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    metadata
        .feature_flags
        .insert(HA_QUALIFIED_DATA_EPOCH_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

/// Fence older native hosts BEFORE publishing protected metadata identity.
pub(crate) fn enable_native_metadata_store(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    if !metadata
        .feature_flags
        .contains(NATIVE_WRITER_ENROLLMENT_FEATURE_FLAG)
    {
        return Err(BicDbError::InvalidConfig(
            "native metadata requires born-v2 writer enrollment".into(),
        ));
    }
    metadata
        .feature_flags
        .insert(NATIVE_METADATA_STORE_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}
pub(crate) fn enable_native_initial_preparation(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    if !metadata
        .feature_flags
        .contains(NATIVE_METADATA_STORE_FEATURE_FLAG)
    {
        return Err(BicDbError::InvalidConfig(
            "initial preparation requires native metadata ownership".into(),
        ));
    }
    metadata
        .feature_flags
        .insert(NATIVE_INITIAL_PREPARATION_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}
pub(crate) fn native_initial_preparation_active(root: &Path) -> Result<bool> {
    let metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    Ok(metadata
        .feature_flags
        .contains(NATIVE_INITIAL_PREPARATION_FEATURE_FLAG))
}
pub(crate) fn native_metadata_store_active(root: &Path) -> Result<bool> {
    let metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    Ok(metadata
        .feature_flags
        .contains(NATIVE_METADATA_STORE_FEATURE_FLAG))
}

/// Reader refusal only. Actual joint producers additionally require a real
/// immutable born-v2 record and both independent data-quorum decisions.
pub(crate) fn enable_native_joint_data_handoff(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    if !metadata
        .feature_flags
        .contains(NATIVE_WRITER_ENROLLMENT_FEATURE_FLAG)
    {
        return Err(BicDbError::InvalidConfig(
            "native joint data handoff requires from-birth writer enrollment".into(),
        ));
    }
    metadata
        .feature_flags
        .insert(NATIVE_JOINT_DATA_HANDOFF_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}
pub(crate) fn native_joint_data_handoff_active(root: &Path) -> Result<bool> {
    let metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    Ok(metadata
        .feature_flags
        .contains(NATIVE_JOINT_DATA_HANDOFF_FEATURE_FLAG))
}

/// Reader refusal only; neither a metadata role nor a native/data quorum.
/// Deliberately excluded from REPLICATED_READER_FEATURES.
pub(crate) fn enable_native_completed_decision_materialization(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    if !metadata
        .feature_flags
        .contains(NATIVE_WRITER_ENROLLMENT_FEATURE_FLAG)
        || !metadata
            .feature_flags
            .contains(NATIVE_ABORT_HISTORY_FEATURE_FLAG)
        || !metadata
            .feature_flags
            .contains(NATIVE_CATALOGUE_CUSTODY_FEATURE_FLAG)
    {
        return Err(BicDbError::InvalidConfig(
            "completed materialization needs actual native writer/abort/custody fences".into(),
        ));
    }
    metadata
        .feature_flags
        .insert(NATIVE_COMPLETED_DECISION_MATERIALIZATION_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}
#[cfg(test)]
pub(crate) fn test_simulated_reader_without_completed_materialization(root: &Path) -> Result<()> {
    let metadata = read_or_legacy_metadata(root)?;
    let old = KNOWN_FEATURE_FLAGS
        .iter()
        .copied()
        .filter(|name| *name != NATIVE_COMPLETED_DECISION_MATERIALIZATION_FEATURE_FLAG)
        .collect::<Vec<_>>();
    validate_supported_with_flags(&metadata, &old)
}
pub(crate) fn native_completed_decision_materialization_active(root: &Path) -> Result<bool> {
    let metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    Ok(metadata
        .feature_flags
        .contains(NATIVE_COMPLETED_DECISION_MATERIALIZATION_FEATURE_FLAG))
}

pub(crate) fn enable_native_current_joint_recovery(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    if !metadata
        .feature_flags
        .contains(NATIVE_JOINT_DATA_HANDOFF_FEATURE_FLAG)
        || !metadata
            .feature_flags
            .contains(NATIVE_ABORT_HISTORY_FEATURE_FLAG)
    {
        return Err(BicDbError::InvalidConfig(
            "current joint recovery needs actual joint and abort reader fences".into(),
        ));
    }
    metadata
        .feature_flags
        .insert(NATIVE_CURRENT_JOINT_RECOVERY_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}
pub(crate) fn native_current_joint_recovery_active(root: &Path) -> Result<bool> {
    let metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    Ok(metadata
        .feature_flags
        .contains(NATIVE_CURRENT_JOINT_RECOVERY_FEATURE_FLAG))
}

pub(crate) fn enable_current_image_proposal(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    if !metadata
        .feature_flags
        .contains(RANGE_IMAGE_INSTALL_FEATURE_FLAG)
    {
        return Err(BicDbError::InvalidConfig(
            "current image proposal requires image-aware storage".into(),
        ));
    }
    metadata
        .feature_flags
        .insert(CURRENT_IMAGE_PROPOSAL_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}
pub(crate) fn current_image_proposal_active(root: &Path) -> Result<bool> {
    let metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    Ok(metadata
        .feature_flags
        .contains(CURRENT_IMAGE_PROPOSAL_FEATURE_FLAG))
}

/// Fence older image-aware writers BEFORE persisting a recovered serving
/// configuration. This flag remains local, outside portable SQL schema flags.
pub(crate) fn enable_native_recovery_serving(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    if !metadata
        .feature_flags
        .contains(RANGE_IMAGE_INSTALL_FEATURE_FLAG)
    {
        return Err(BicDbError::InvalidConfig(
            "native recovery serving requires image-aware storage".into(),
        ));
    }
    metadata
        .feature_flags
        .insert(NATIVE_RECOVERY_SERVING_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

pub(crate) fn native_recovery_serving_active(root: &Path) -> Result<bool> {
    let metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    Ok(metadata
        .feature_flags
        .contains(NATIVE_RECOVERY_SERVING_FEATURE_FLAG))
}

/// Presence of a receipt alone is never activation. Validate format and mode
/// before a native adapter may consider this local receipt capability active.
pub(crate) fn range_image_install_active(root: &Path) -> Result<bool> {
    let metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    Ok(metadata
        .feature_flags
        .contains(RANGE_IMAGE_INSTALL_FEATURE_FLAG))
}

/// Persist the reader/writer fence before any Oracle-v1 key definition or build
/// checkpoint becomes durable. The capability is sticky even after index DROP.
pub(crate) fn enable_oracle_simple_case_v1(root: &Path) -> Result<()> {
    enable_feature_flags(root, &[ORACLE_SIMPLE_CASE_V1_FEATURE_FLAG])
}

/// Persist a sticky reader fence before publishing an invalid Oracle routine.
pub(crate) fn enable_oracle_invalid_routines(root: &Path) -> Result<()> {
    enable_feature_flags(root, &[ORACLE_INVALID_ROUTINES_FEATURE_FLAG])
}

#[cfg(test)]
mod replicated_reader_tests {
    use super::*;

    #[test]
    fn portable_capabilities_preserve_local_layout_and_backup_fences() {
        let root = tempfile::tempdir().unwrap();
        persist_current_with_mode(root.path(), StorageMode::ServerPaged, true).unwrap();
        enable_online_backup_extent_tracking(root.path()).unwrap();
        enable_ha_qualified_data_epoch(root.path()).unwrap();
        install_replicated_reader_features(root.path(), &[SQL_RENAME_INTENTS_FEATURE_FLAG.into()])
            .unwrap();
        install_replicated_reader_features(
            root.path(),
            &[ORACLE_SIMPLE_CASE_V1_FEATURE_FLAG.into()],
        )
        .unwrap();
        persist_current(root.path(), true).unwrap();
        let metadata = read_or_legacy_metadata(root.path()).unwrap();
        assert_eq!(metadata.storage_mode, StorageMode::ServerPaged);
        assert!(metadata
            .feature_flags
            .contains(ONLINE_BACKUP_EXTENT_FEATURE_FLAG));
        assert!(metadata
            .feature_flags
            .contains(HA_QUALIFIED_DATA_EPOCH_FEATURE_FLAG));
        assert_eq!(
            required_reader_features(root.path()).unwrap(),
            vec![
                ORACLE_SIMPLE_CASE_V1_FEATURE_FLAG.to_string(),
                SQL_RENAME_INTENTS_FEATURE_FLAG.to_string()
            ]
        );
    }

    #[test]
    fn unknown_nonportable_and_noncanonical_requirements_leave_metadata_untouched() {
        let root = tempfile::tempdir().unwrap();
        persist_current(root.path(), true).unwrap();
        let before = std::fs::read(metadata_path(root.path())).unwrap();
        for features in [
            vec!["unknown_reader_v999".into()],
            vec![SERVER_PAGED_FEATURE_FLAG.into()],
            vec![
                SQL_RENAME_INTENTS_FEATURE_FLAG.into(),
                SQL_CATALOG_INTENTS_FEATURE_FLAG.into(),
            ],
            vec![
                SQL_RENAME_INTENTS_FEATURE_FLAG.into(),
                SQL_RENAME_INTENTS_FEATURE_FLAG.into(),
            ],
        ] {
            assert!(install_replicated_reader_features(root.path(), &features).is_err());
            assert_eq!(std::fs::read(metadata_path(root.path())).unwrap(), before);
        }
    }

    #[test]
    fn simultaneous_feature_installs_and_metadata_refresh_keep_the_union() {
        let root = tempfile::tempdir().unwrap();
        persist_current(root.path(), true).unwrap();
        let barrier = std::sync::Barrier::new(3);
        std::thread::scope(|scope| {
            for feature in [
                SQL_CATALOG_INTENTS_FEATURE_FLAG,
                SQL_RENAME_INTENTS_FEATURE_FLAG,
            ] {
                let root = root.path();
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..16 {
                        install_replicated_reader_features(root, &[feature.into()]).unwrap();
                    }
                });
            }
            let root = root.path();
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..16 {
                    persist_current(root, true).unwrap();
                }
            });
        });
        assert_eq!(
            required_reader_features(root.path()).unwrap(),
            vec![
                SQL_CATALOG_INTENTS_FEATURE_FLAG.to_string(),
                SQL_RENAME_INTENTS_FEATURE_FLAG.to_string()
            ]
        );
    }
}

#[cfg(test)]
mod local_identity_format_tests {
    use super::*;

    // The native image reader is supplied by this composition. Keep the
    // current-main unknown-reader refusal invariant for a genuinely unknown
    // future capability; historical source-476 evidence remains unchanged.
    #[test]
    fn namespace_identity_does_not_advertise_unknown_native_readers() {
        let root = tempfile::tempdir().unwrap();
        let mut metadata = FormatMetadata::current();
        metadata
            .feature_flags
            .insert("reserved_future_native_reader_v999".into());
        persist_metadata(root.path(), &metadata, true).unwrap();
        let error = ensure_open_compatible(root.path(), false).unwrap_err();
        assert!(error
            .to_string()
            .contains("unknown format feature flag `reserved_future_native_reader_v999`"));
    }

    #[test]
    fn namespace_identity_metadata_replacements_keep_sticky_fences_durable() {
        let root = tempfile::tempdir().unwrap();
        let legacy = FormatMetadata::current();
        persist_metadata_with(root.path(), &legacy, false, |_, _, sync| {
            assert!(!sync, "legacy/default row-fsync choice remains unchanged");
            Ok(())
        })
        .unwrap();
        for flag in [
            LOCAL_NAMESPACE_IDENTITY_FEATURE_FLAG,
            RANGE_IMAGE_INSTALL_FEATURE_FLAG,
            HA_QUALIFIED_DATA_EPOCH_FEATURE_FLAG,
        ] {
            let mut metadata = legacy.clone();
            metadata.feature_flags.insert(flag.into());
            persist_metadata_with(root.path(), &metadata, false, |path, bytes, sync| {
                assert!(sync, "sticky local fences cannot be replaced without sync");
                assert_eq!(path, metadata_path(root.path()));
                assert!(serde_json::from_slice::<FormatMetadata>(bytes)
                    .unwrap()
                    .feature_flags
                    .contains(flag));
                Ok(())
            })
            .unwrap();
        }
    }

    #[test]
    fn namespace_identity_concurrent_capability_and_flush_writers_preserve_all_flags() {
        use std::sync::{Arc, Barrier};
        let root = tempfile::tempdir().unwrap();
        persist_current(root.path(), false).unwrap();
        let barrier = Arc::new(Barrier::new(6));
        std::thread::scope(|scope| {
            for action in 0..6 {
                let barrier = barrier.clone();
                let root = root.path();
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..24 {
                        match action {
                            0 => enable_local_namespace_identity(root).unwrap(),
                            1 => enable_range_image_install(root).unwrap(),
                            2 => enable_unique_nulls_not_distinct(root).unwrap(),
                            3 => enable_sql_catalog_intents(root).unwrap(),
                            4 => persist_current(root, false).unwrap(),
                            _ => {
                                persist_current_with_mode(root, StorageMode::EmbeddedMemory, false)
                                    .unwrap()
                            }
                        }
                    }
                });
            }
        });
        let metadata = read_or_legacy_metadata(root.path()).unwrap();
        for flag in [
            LOCAL_NAMESPACE_IDENTITY_FEATURE_FLAG,
            RANGE_IMAGE_INSTALL_FEATURE_FLAG,
            SQL_CATALOG_INTENTS_FEATURE_FLAG,
            UNIQUE_NULLS_NOT_DISTINCT_FEATURE_FLAG,
        ] {
            assert!(metadata.feature_flags.contains(flag));
        }
        validate_supported(&metadata).unwrap();
    }

    #[test]
    fn namespace_identity_all_format_writers_share_canonical_root_lock() {
        use std::sync::{mpsc, Arc};
        let root = tempfile::tempdir().unwrap();
        persist_current(root.path(), false).unwrap();
        let canonical_lock = feature_flags_lock(root.path());
        let alias = root.path().join(".");
        assert!(Arc::ptr_eq(&canonical_lock, &feature_flags_lock(&alias)));
        for action in 0..10 {
            let guard = canonical_lock.lock();
            let (entered_tx, entered_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    entered_tx.send(()).unwrap();
                    match action {
                        0 => enable_local_namespace_identity(&alias),
                        1 => enable_range_image_install(&alias),
                        2 => enable_unique_nulls_not_distinct(&alias),
                        3 => enable_sql_catalog_intents(&alias),
                        4 => enable_oracle_simple_case_v1(&alias),
                        5 => enable_oracle_invalid_routines(&alias),
                        6 => enable_online_backup_extent_tracking(&alias),
                        7 => persist_current(&alias, false),
                        8 => persist_current_with_mode(&alias, StorageMode::EmbeddedMemory, false),
                        _ => ensure_open_compatible_in_mode(
                            &alias,
                            false,
                            StorageMode::EmbeddedMemory,
                        )
                        .map(|_| ()),
                    }
                    .unwrap();
                    done_tx.send(()).unwrap();
                });
                entered_rx.recv().unwrap();
                assert!(matches!(
                    done_rx.recv_timeout(std::time::Duration::from_millis(20)),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ));
                drop(guard);
                done_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
            });
        }
    }
}

/// Fence old writers before the first monotone abort-aware range start.
pub(crate) fn enable_native_abort_history(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    if !metadata
        .feature_flags
        .contains(RANGE_IMAGE_INSTALL_FEATURE_FLAG)
        || !metadata
            .feature_flags
            .contains(NATIVE_WRITER_ENROLLMENT_FEATURE_FLAG)
        || !metadata
            .feature_flags
            .contains(NATIVE_METADATA_STORE_FEATURE_FLAG)
    {
        return Err(BicDbError::InvalidConfig(
            "native abort writer requires from-birth enforced writer and metadata initialization fences".into(),
        ));
    }
    metadata
        .feature_flags
        .insert(NATIVE_ABORT_HISTORY_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}
pub(crate) fn native_abort_history_active(root: &Path) -> Result<bool> {
    let metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    Ok(metadata
        .feature_flags
        .contains(NATIVE_ABORT_HISTORY_FEATURE_FLAG))
}

/// Sticky fence: older native binaries must refuse an offline scope transition.
pub(crate) fn enable_application_unique_scope(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    metadata
        .feature_flags
        .insert(APPLICATION_UNIQUE_SCOPE_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

/// Sticky before any publication authority is installed; old readers refuse.
pub(crate) fn enable_native_immutable_facts(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    metadata
        .feature_flags
        .insert(NATIVE_IMMUTABLE_FACTS_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

/// Sticky exact owner-version/resource policy fence, including normalized v1 publications.
pub(crate) fn enable_native_fact_projection_contracts(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    metadata
        .feature_flags
        .insert(NATIVE_FACT_PROJECTION_CONTRACTS_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

/// Sticky before activating programs/guards that require exact interval arithmetic.
pub(crate) fn enable_exact_proportional_intervals(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    metadata
        .feature_flags
        .insert(EXACT_PROPORTIONAL_INTERVALS_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

pub(crate) fn enable_exact_decimal_list_sum(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    metadata
        .feature_flags
        .insert(EXACT_DECIMAL_LIST_SUM_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

pub(crate) fn enable_exact_decimal_units(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    metadata
        .feature_flags
        .insert(EXACT_DECIMAL_UNITS_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

#[cfg(test)]
mod semantic_reader_capture_tests {
    use super::*;
    use crate::{BicDb, DbConfig, Record};

    fn preserves_semantic_requirement(flag: &str, activate: impl Fn(&mut BicDb)) {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let config = DbConfig::default()
                .with_storage_mode(mode)
                .with_fsync(true)
                .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            db.create_collection("reader_capture_ACK").unwrap();
            db.insert(
                "reader_capture_ACK",
                Record::new("one")
                    .with_metadata(serde_json::json!({"payload":"independent-native-ACK"})),
            )
            .unwrap();
            let hash = db
                .get("reader_capture_ACK", "one")
                .unwrap()
                .unwrap()
                .content_hash()
                .unwrap();
            activate(&mut db);
            assert!(read_or_legacy_metadata(root.path())
                .unwrap()
                .feature_flags
                .contains(flag));
            let sequence = db.last_commit_seq();
            let captured = required_reader_features(root.path()).unwrap();
            assert!(captured.iter().any(|f| f == flag), "Native reader contract silently dropped active semantic requirement {flag}: {captured:?}");
            assert_eq!(db.last_commit_seq(), sequence, "capture must be read-only");
            assert_eq!(
                db.get("reader_capture_ACK", "one")
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap(),
                hash
            );
            db.close().unwrap();
            for _ in 0..2 {
                let db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
                assert!(required_reader_features(root.path())
                    .unwrap()
                    .iter()
                    .any(|f| f == flag));
                assert_eq!(
                    db.get("reader_capture_ACK", "one")
                        .unwrap()
                        .unwrap()
                        .content_hash()
                        .unwrap(),
                    hash
                );
                db.close().unwrap();
            }
        }
    }

    #[test]
    fn partition_execution_requirement_survives_native_reader_capture() {
        preserves_semantic_requirement(SQL_PARTITION_EXECUTION_V1_FEATURE_FLAG, |db| {
            db.enable_sql_partition_execution_v1().unwrap()
        });
    }
    #[test]
    fn decimal_comparison_requirement_survives_native_reader_capture() {
        preserves_semantic_requirement(DECIMAL_PREDICATE_COMPARISON_V1_FEATURE_FLAG, |db| {
            enable_decimal_predicate_comparison_v1(db.data_path()).unwrap()
        });
    }
    #[test]
    fn application_unique_scope_requirement_survives_native_reader_capture() {
        preserves_semantic_requirement(APPLICATION_UNIQUE_SCOPE_FEATURE_FLAG, |db| {
            db.enable_application_unique_scope().unwrap()
        });
    }

    #[test]
    fn finance_requirements_survive_native_reader_capture_and_reopens() {
        for (flag, activate) in [
            (
                NATIVE_IMMUTABLE_FACTS_FEATURE_FLAG,
                enable_native_immutable_facts as fn(&Path) -> Result<()>,
            ),
            (
                NATIVE_FACT_PROJECTION_CONTRACTS_FEATURE_FLAG,
                enable_native_fact_projection_contracts,
            ),
            (
                EXACT_PROPORTIONAL_INTERVALS_FEATURE_FLAG,
                enable_exact_proportional_intervals,
            ),
            (
                EXACT_DECIMAL_LIST_SUM_FEATURE_FLAG,
                enable_exact_decimal_list_sum,
            ),
        ] {
            preserves_semantic_requirement(flag, |db| activate(db.data_path()).unwrap());
        }
    }
}

pub(crate) fn enable_decimal_predicate_comparison_v1(root: &Path) -> Result<()> {
    let lock = feature_flags_lock(root);
    let _guard = lock.lock();
    let mut metadata = read_or_legacy_metadata(root)?;
    validate_supported(&metadata)?;
    metadata
        .feature_flags
        .insert(DECIMAL_PREDICATE_COMPARISON_V1_FEATURE_FLAG.into());
    persist_metadata(root, &metadata, true)
}

#[cfg(test)]
pub(crate) fn semantic_reader_test_activations() -> [(&'static str, fn(&Path) -> Result<()>); 7] {
    [
        (
            NATIVE_IMMUTABLE_FACTS_FEATURE_FLAG,
            enable_native_immutable_facts,
        ),
        (
            NATIVE_FACT_PROJECTION_CONTRACTS_FEATURE_FLAG,
            enable_native_fact_projection_contracts,
        ),
        (
            EXACT_PROPORTIONAL_INTERVALS_FEATURE_FLAG,
            enable_exact_proportional_intervals,
        ),
        (
            EXACT_DECIMAL_LIST_SUM_FEATURE_FLAG,
            enable_exact_decimal_list_sum,
        ),
        (EXACT_DECIMAL_UNITS_FEATURE_FLAG, enable_exact_decimal_units),
        (
            DECIMAL_PREDICATE_COMPARISON_V1_FEATURE_FLAG,
            enable_decimal_predicate_comparison_v1,
        ),
        (
            APPLICATION_UNIQUE_SCOPE_FEATURE_FLAG,
            enable_application_unique_scope,
        ),
    ]
}

#[cfg(test)]
mod mutation_reader_contract_tests {
    use super::*;
    use crate::{BicDb, DbConfig, Record};

    #[test]
    fn implemented_mutation_reader_requirements_survive_capture_and_two_reopens() {
        for (flag, activate) in semantic_reader_test_activations() {
            for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
                let root = tempfile::tempdir().unwrap();
                let config = DbConfig::default()
                    .with_fsync(true)
                    .with_storage_mode(mode)
                    .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
                let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
                db.create_collection("ack").unwrap();
                db.insert(
                    "ack",
                    Record::new("one")
                        .with_metadata(serde_json::json!({"payload":"independent-ack"})),
                )
                .unwrap();
                let hash = db
                    .get("ack", "one")
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap();
                activate(root.path()).unwrap();
                assert!(read_or_legacy_metadata(root.path())
                    .unwrap()
                    .feature_flags
                    .contains(flag));
                let sequence = db.last_commit_seq();
                assert!(
                    required_reader_features(root.path())
                        .unwrap()
                        .iter()
                        .any(|f| f == flag),
                    "active semantic reader {flag} was omitted from the Native contract"
                );
                assert_eq!(db.last_commit_seq(), sequence);
                assert_eq!(
                    db.get("ack", "one")
                        .unwrap()
                        .unwrap()
                        .content_hash()
                        .unwrap(),
                    hash
                );
                db.close().unwrap();
                for _ in 0..2 {
                    let db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
                    assert!(required_reader_features(root.path())
                        .unwrap()
                        .iter()
                        .any(|f| f == flag));
                    assert_eq!(
                        db.get("ack", "one")
                            .unwrap()
                            .unwrap()
                            .content_hash()
                            .unwrap(),
                        hash
                    );
                    db.close().unwrap();
                }
            }
        }
    }
}
