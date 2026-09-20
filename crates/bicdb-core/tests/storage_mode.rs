//! Storage-mode compatibility fence (Phase 0 of `docs/server-paged-storage-todo.md`).
//!
//! The contract under test is narrow but load-bearing: `storage_mode` is durable
//! per-database metadata, and a binary that cannot run the recorded mode must
//! refuse the database *before mutating any of it*. These tests are the reason
//! it is safe to start writing server-paged databases later — an
//! `embedded_memory`-only binary meeting one will bounce off rather than
//! misinterpret it.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use bicdb_core::{BicDb, DbConfig, FormatMetadata, StorageMode, SERVER_PAGED_FEATURE_FLAG};
use serde_json::json;
use tempfile::TempDir;

/// An explicitly `embedded_memory` config.
///
/// This file asserts what the *fence* does, so it must never depend on which
/// mode happens to be ambient. `BICDB_STORAGE_MODE` exists so the whole suite
/// can be swept against the paged engine; a fence test that changed meaning
/// under that sweep would be testing the sweep rather than the fence.
fn embedded() -> DbConfig {
    DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory)
}

fn format_path(root: &Path) -> std::path::PathBuf {
    root.join("format.json")
}

/// Write a `format.json` by hand, standing in for "a newer binary created this".
fn write_raw_format(root: &Path, value: serde_json::Value) {
    fs::create_dir_all(root).unwrap();
    fs::write(
        format_path(root),
        serde_json::to_vec_pretty(&value).unwrap(),
    )
    .unwrap();
}

fn read_raw_format(root: &Path) -> serde_json::Value {
    serde_json::from_slice(&fs::read(format_path(root)).unwrap()).unwrap()
}

#[test]
fn new_database_records_embedded_memory_durably() {
    let dir = TempDir::new().unwrap();
    let db = BicDb::open_with_config(dir.path(), embedded()).unwrap();
    db.close().unwrap();

    let metadata: FormatMetadata =
        serde_json::from_slice(&fs::read(format_path(dir.path())).unwrap()).unwrap();
    assert_eq!(metadata.storage_mode, StorageMode::EmbeddedMemory);
    assert_eq!(
        bicdb_core::storage_mode(dir.path()).unwrap(),
        StorageMode::EmbeddedMemory
    );
    // embedded_memory is the default and implies no extra feature flag, so
    // databases written today stay readable by binaries that predate this field.
    assert!(!metadata.feature_flags.contains(SERVER_PAGED_FEATURE_FLAG));
}

#[test]
fn database_without_format_metadata_is_embedded_memory() {
    let dir = TempDir::new().unwrap();
    // No format.json at all: the pre-v2 layout.
    assert_eq!(
        bicdb_core::storage_mode(dir.path()).unwrap(),
        StorageMode::EmbeddedMemory
    );
    let db = BicDb::open_with_config(dir.path(), embedded()).unwrap();
    db.close().unwrap();
}

#[test]
fn format_metadata_missing_storage_mode_field_defaults_to_embedded_memory() {
    let dir = TempDir::new().unwrap();
    // Exactly what a binary from before this change writes.
    write_raw_format(
        dir.path(),
        json!({
            "format_version": 2,
            "min_reader_version": 1,
            "min_writer_version": 2,
            "feature_flags": ["format_metadata_v2"],
        }),
    );
    assert_eq!(
        bicdb_core::storage_mode(dir.path()).unwrap(),
        StorageMode::EmbeddedMemory
    );
    BicDb::open_with_config(dir.path(), embedded())
        .unwrap()
        .close()
        .unwrap();
}

#[test]
fn a_mode_mismatch_is_refused_without_mutating_the_database() {
    // `server_paged` is implemented now, so meeting one is no longer a refusal.
    // What must still hold is that opening it under the WRONG mode refuses
    // without touching a byte — the property that makes the fence trustworthy.
    let dir = TempDir::new().unwrap();
    let raw = json!({
        "format_version": 2,
        "min_reader_version": 1,
        "min_writer_version": 2,
        "feature_flags": ["format_metadata_v2", SERVER_PAGED_FEATURE_FLAG],
        "storage_mode": "server_paged",
    });
    write_raw_format(dir.path(), raw.clone());
    let before = fs::read(format_path(dir.path())).unwrap();

    // Default config requests embedded_memory.
    let error = BicDb::open_with_config(dir.path(), embedded()).unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("never converts storage mode implicitly"),
        "a mismatch must be an explicit refusal: {message}"
    );

    assert_eq!(
        fs::read(format_path(dir.path())).unwrap(),
        before,
        "refused open must not rewrite format metadata"
    );
    assert_eq!(read_raw_format(dir.path()), raw);
    assert!(
        !dir.path().join("format-migration.json").exists(),
        "refused open must not start a format migration"
    );
}

#[test]
fn a_server_paged_database_opens_and_keeps_its_mode() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig::default().with_storage_mode(StorageMode::ServerPaged);
    let db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
    db.close().unwrap();

    assert_eq!(
        bicdb_core::storage_mode(dir.path()).unwrap(),
        StorageMode::ServerPaged,
        "creating a database in server_paged did not persist the mode"
    );
    let metadata: FormatMetadata =
        serde_json::from_slice(&fs::read(format_path(dir.path())).unwrap()).unwrap();
    assert!(
        metadata.feature_flags.contains(SERVER_PAGED_FEATURE_FLAG),
        "the flag that makes older binaries refuse this database is missing"
    );

    // Reopening in the same mode works and does not change it.
    BicDb::open_with_config(dir.path(), config)
        .unwrap()
        .close()
        .unwrap();
    assert_eq!(
        bicdb_core::storage_mode(dir.path()).unwrap(),
        StorageMode::ServerPaged
    );
}

#[test]
fn unknown_storage_mode_from_a_newer_binary_is_refused() {
    let dir = TempDir::new().unwrap();
    write_raw_format(
        dir.path(),
        json!({
            "format_version": 2,
            "min_reader_version": 1,
            "min_writer_version": 2,
            "feature_flags": ["format_metadata_v2"],
            "storage_mode": "columnar_lakehouse",
        }),
    );
    let before = fs::read(format_path(dir.path())).unwrap();

    let error = BicDb::open_with_config(dir.path(), embedded()).unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("columnar_lakehouse"),
        "unknown mode must be named verbatim so an operator can act on it: {message}"
    );
    assert!(
        message.contains("newer BicDB binary"),
        "unknown mode should point at the likely cause: {message}"
    );
    assert_eq!(fs::read(format_path(dir.path())).unwrap(), before);
}

#[test]
fn unknown_storage_mode_survives_a_serde_round_trip_verbatim() {
    // An unrecognized mode must not be silently normalized: if this binary ever
    // rewrote such a file it would have to preserve the name it could not read.
    let metadata = FormatMetadata {
        format_version: 2,
        min_reader_version: 1,
        min_writer_version: 2,
        feature_flags: BTreeSet::from(["format_metadata_v2".to_string()]),
        storage_mode: StorageMode::Unknown("columnar_lakehouse".to_string()),
    };
    let encoded = serde_json::to_vec(&metadata).unwrap();
    let decoded: FormatMetadata = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, metadata);
    assert_eq!(decoded.storage_mode.as_str(), "columnar_lakehouse");

    let raw: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(raw["storage_mode"], json!("columnar_lakehouse"));
}

#[test]
fn requesting_an_unimplemented_mode_is_refused_before_the_directory_is_touched() {
    let dir = TempDir::new().unwrap();
    let empty = dir.path().join("fresh");

    // server_paged is implemented, so an UNRECOGNIZED mode is what stands in
    // for "this build cannot run what you asked for".
    let config = DbConfig::default()
        .with_storage_mode(StorageMode::Unknown("columnar_lakehouse".to_string()));
    let error = BicDb::open_with_config(&empty, config).unwrap_err();
    assert!(
        error.to_string().contains("columnar_lakehouse"),
        "unexpected error: {error}"
    );
    assert!(
        !empty.exists(),
        "a refused mode request must not create the database directory"
    );
}

#[test]
fn opening_an_existing_database_under_a_different_mode_is_refused_not_converted() {
    let dir = TempDir::new().unwrap();
    BicDb::open_with_config(dir.path(), embedded())
        .unwrap()
        .close()
        .unwrap();

    // Directly exercise the mismatch check: `open` also refuses server_paged as
    // unimplemented, so this asserts the never-convert rule on its own terms and
    // will keep asserting it once server_paged becomes runnable.
    let error = bicdb_core::ensure_requested_mode_matches(dir.path(), StorageMode::ServerPaged)
        .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("never converts storage mode implicitly"),
        "mismatch must be an explicit refusal, not a conversion: {message}"
    );
}

#[test]
fn server_paged_metadata_carries_the_feature_flag_that_old_binaries_reject() {
    // Defense in depth. A binary predating `storage_mode` ignores the field
    // entirely, but every shipped binary already rejects unrecognized feature
    // flags — so writing this flag is what makes the fence hold retroactively.
    let metadata = FormatMetadata::current_with_mode(StorageMode::ServerPaged);
    assert!(metadata.feature_flags.contains(SERVER_PAGED_FEATURE_FLAG));
    assert!(metadata.feature_flags.contains("format_metadata_v2"));

    let embedded = FormatMetadata::current_with_mode(StorageMode::EmbeddedMemory);
    assert!(!embedded.feature_flags.contains(SERVER_PAGED_FEATURE_FLAG));
}

#[test]
fn routine_metadata_rewrite_cannot_downgrade_a_server_paged_database() {
    // `persist_current` runs on ordinary flush paths. Now that server_paged is
    // supported it must PRESERVE the mode rather than default it — silently
    // rewriting a paged database as embedded_memory would dismantle the fence
    // through a routine operation.
    let dir = TempDir::new().unwrap();
    write_raw_format(
        dir.path(),
        json!({
            "format_version": 2,
            "min_reader_version": 1,
            "min_writer_version": 2,
            "feature_flags": ["format_metadata_v2", SERVER_PAGED_FEATURE_FLAG],
            "storage_mode": "server_paged",
        }),
    );

    bicdb_core::persist_current(dir.path(), false).unwrap();
    assert_eq!(
        read_raw_format(dir.path())["storage_mode"],
        json!("server_paged"),
        "a routine metadata rewrite downgraded the storage mode"
    );
}

#[test]
fn version_migration_preserves_the_recorded_storage_mode() {
    // A v1 -> v2 format migration must not be the thing that changes which
    // engine owns the data.
    let dir = TempDir::new().unwrap();
    write_raw_format(
        dir.path(),
        json!({
            "format_version": 1,
            "min_reader_version": 1,
            "min_writer_version": 1,
            "feature_flags": [],
        }),
    );
    BicDb::open_with_config(dir.path(), embedded())
        .unwrap()
        .close()
        .unwrap();

    let after = read_raw_format(dir.path());
    assert_eq!(after["format_version"], json!(2));
    assert_eq!(after["storage_mode"], json!("embedded_memory"));
}
