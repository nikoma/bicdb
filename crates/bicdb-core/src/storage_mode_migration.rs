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
//! source is opened read-only and never mutated.
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

    // Open the source under whatever engine owns it (the fence decides), and
    // never write to it.
    let source_db = BicDb::open(source)?;

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
        target_db.create_collection_with_mode(&meta.name, meta.mode)?;
        if let Some(policy) = meta.policy.clone() {
            target_db.set_collection_policy(&meta.name, policy)?;
        }
        report.collections += 1;

        // Ids come back sorted, so the copy is in primary-key order: bounded
        // memory on the read side and a compact WAL on the write side.
        let ids = source_db.scan_collection_record_ids_with_prefix(&meta.name, "")?;
        let mut pending: Vec<Record> = Vec::with_capacity(batch.min(ids.len()));
        for id in &ids {
            let Some(record) = source_db.get(&meta.name, id)? else {
                // Concurrently removed under a non-exclusive source: the copy
                // is a point-in-time snapshot, so skipping is correct.
                continue;
            };
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
    Ok(report)
}
