//! Offline migration of a database from one storage engine to another.
//!
//! [`ADR-004`](../../../docs/decisions/ADR-004-storage-mode-compatibility.md)
//! makes the owning engine durable metadata and forbids reinterpreting a
//! directory under a different engine — deliberately, because a segment log
//! read as a page file does not fail cleanly. That fence has no escape hatch,
//! so a database created in `embedded_memory` stays there for life even when
//! its working set outgrows RAM.
//!
//! `embedded_memory` keeps every live row and every derived access structure
//! resident (see [`StorageMode`]); a collection of large JSON payloads
//! therefore costs multiples of its on-disk size in RSS at rest, and the only
//! real fix is the demand-paged engine with its bounded buffer pool. This
//! module is that escape hatch: a **copy** into a freshly created database in
//! the target mode, which respects the fence instead of defeating it. The
//! source is opened under its recorded mode with the ordinary recovery path.
//! No logical source rows are changed, but opening/closing may checkpoint or
//! upgrade physical metadata. Use an offline copy if the original must remain
//! byte-for-byte unchanged.
//!
//! Memory: the source engine's own mode dictates its residency, so migrating
//! *out of* `embedded_memory` needs enough RAM to open the source. Records are
//! copied in bounded batches in primary-key order, so the target side adds
//! only one batch at a time — and sorted keys are also what keeps the target's
//! WAL small.

use std::path::Path;

use crate::db::{BicDb, DbConfig};
use crate::error::{BicDbError, Result};
use crate::format::{self, StorageMode};
use crate::record::Record;

/// Default records per copy batch. Large enough that per-batch overhead is
/// irrelevant, small enough that a batch of multi-KiB payloads stays bounded.
pub const DEFAULT_MIGRATION_BATCH: usize = 1_000;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageModeMigrationReport {
    pub collections: usize,
    pub records: u64,
    pub indexes: usize,
    pub source_mode: String,
    pub target_mode: String,
    /// Full logical record readback after closing and reopening the target.
    pub verified_records: u64,
}

/// Copy `source` into a new database at `target` running `target_mode`.
///
/// Offline on both sides: the source must not be open (its lock is taken for
/// the read), and `target` must not already exist as a database. Indexes are
/// recreated *after* the rows land, which is both faster and the documented
/// bulk-load discipline.
pub fn migrate_storage_mode(
    source: impl AsRef<Path>,
    target: impl AsRef<Path>,
    target_mode: StorageMode,
    batch: usize,
) -> Result<StorageModeMigrationReport> {
    let source = source.as_ref();
    let target = target.as_ref();
    if !target_mode.is_supported() {
        return Err(BicDbError::FormatCompatibility(format!(
            "target storage mode `{}` is not runnable by this binary",
            target_mode.as_str()
        )));
    }
    if source == target {
        return Err(BicDbError::FormatCompatibility(
            "storage-mode migration copies into a NEW directory; source and target must differ"
                .to_string(),
        ));
    }
    if target.join("format.json").exists() || target.join("segments").exists() {
        return Err(BicDbError::FormatCompatibility(format!(
            "{} already holds a database; migrate into a new directory",
            target.display()
        )));
    }
    let batch = batch.max(1);

    // The recorded mode is readable without opening the database at all.
    let recorded = format::storage_mode(source)?;
    if recorded == target_mode {
        return Err(BicDbError::FormatCompatibility(format!(
            "{} is already `{}`; nothing to migrate",
            source.display(),
            target_mode.as_str()
        )));
    }
    let source_mode = recorded.as_str().to_string();

    // Recovery must use the owning engine in either migration direction.
    let source_db =
        BicDb::open_with_config(source, DbConfig::default().with_storage_mode(recorded))?;
    if source_db
        .collections()
        .iter()
        .any(|meta| meta.flashback.is_some())
    {
        return Err(BicDbError::FormatCompatibility(
            "storage-mode migration of flashback history requires a history/SCN migration contract"
                .into(),
        ));
    }

    let mut target_db = BicDb::open_with_config(
        target,
        DbConfig::default().with_storage_mode(target_mode.clone()),
    )?;

    let mut report = StorageModeMigrationReport {
        source_mode,
        target_mode: target_mode.as_str().to_string(),
        ..StorageModeMigrationReport::default()
    };

    for meta in source_db.collections() {
        target_db.create_collection_with_mode(&meta.name, meta.mode.clone())?;
        report.collections += 1;

        // Ids come back sorted, so the copy is in primary-key order: bounded
        // memory on the read side and a compact WAL on the write side.
        let ids = source_db.scan_collection_record_ids_with_prefix(&meta.name, "")?;
        let mut pending: Vec<Record> = Vec::with_capacity(batch.min(ids.len()));
        for id in &ids {
            let record = source_db.get(&meta.name, id)?.ok_or_else(|| {
                BicDbError::FormatCompatibility(format!(
                    "source record disappeared from exclusively locked collection `{}`",
                    meta.name
                ))
            })?;
            pending.push(record.as_ref().clone());
            if pending.len() >= batch {
                report.records += pending.len() as u64;
                target_db.bulk_load_insert(&meta.name, pending.drain(..))?;
            }
        }
        if !pending.is_empty() {
            report.records += pending.len() as u64;
            target_db.bulk_load_insert(&meta.name, pending.drain(..))?;
        }
        // Install the complete logical collection contract after trusted copy.
        // Physical names belong to the new target; a prior rename is not an
        // instruction to reopen an old segment path in that target.
        target_db.restore_migrated_collection_contract(&meta)?;
    }

    // Indexes last: building them over the landed rows beats maintaining them
    // per insert, and a definition that fails here names itself.
    for definition in source_db.index_definitions() {
        target_db
            .create_index(definition.clone())
            .map_err(|error| {
                BicDbError::FormatCompatibility(format!(
                    "migrated rows landed, but index `{}` could not be recreated: {error}",
                    definition.name
                ))
            })?;
        report.indexes += 1;
    }

    target_db.close()?;
    // Read the durable result, rather than trusting only in-memory copy counters.
    let reopened =
        BicDb::open_with_config(target, DbConfig::default().with_storage_mode(target_mode))?;
    let expected = source_db.verification_manifest()?;
    let actual = reopened.verification_manifest()?;
    if expected.tables != actual.tables {
        return Err(BicDbError::FormatCompatibility(
            "migrated database record readback differs from source; preserve both directories"
                .into(),
        ));
    }
    let actual_collections = reopened.collections();
    for meta in source_db.collections() {
        let mut expected = meta;
        let mut actual = actual_collections
            .iter()
            .find(|meta| meta.name == expected.name)
            .ok_or_else(|| {
                BicDbError::FormatCompatibility("migrated collection is missing".into())
            })?
            .clone();
        expected.storage_name = None;
        actual.storage_name = None;
        expected.renamed_from = None;
        actual.renamed_from = None;
        if expected != actual {
            return Err(BicDbError::FormatCompatibility(format!(
                "migrated collection contract differs for `{}`",
                expected.name
            )));
        }
    }
    let mut expected_indexes = source_db.index_definitions();
    let mut actual_indexes = reopened.index_definitions();
    expected_indexes.sort_by(|a, b| a.name.cmp(&b.name));
    actual_indexes.sort_by(|a, b| a.name.cmp(&b.name));
    if expected_indexes != actual_indexes
        || reopened
            .verify_indexes_consistently()?
            .iter()
            .any(|index| !index.valid)
    {
        return Err(BicDbError::FormatCompatibility(
            "migrated indexes differ from source definitions or target rows".into(),
        ));
    }
    report.verified_records = actual.tables.values().map(|table| table.rows).sum();
    reopened.close()?;
    source_db.close()?;
    Ok(report)
}
