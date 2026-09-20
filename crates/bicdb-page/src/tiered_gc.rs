//! Resumable, reference-safe garbage collection for tiered extent providers.
//!
//! A GC run is pinned to one exact active manifest hash and a configured
//! rollback window. Each step holds the same catalog lock as generation
//! publication, rebuilds the protected reference set, and scans one finite
//! content-hash prefix. A concurrent activation therefore fences the old run
//! before it can delete anything newly referenced.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{PageError, Result};
use crate::lock::DirectoryLock;
use crate::tiered::{
    validate_sha256, ExtentObjectKey, ImmutableExtentStore, LocalImmutableExtentStore,
    TieredStorageLimits,
};
use crate::tiered_manifest::{TieredManifestCatalog, TieredManifestLimits};

pub const TIERED_EXTENT_GC_FORMAT_VERSION: u32 = 1;

fn gc_error(message: impl Into<String>) -> PageError {
    PageError::TieredStorage {
        reason: format!("extent GC: {}", message.into()),
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TieredExtentGcLimits {
    pub retain_generations: usize,
    pub max_references: usize,
    pub max_inventory_entries_per_prefix: usize,
    pub max_deletes_per_step: usize,
    pub max_protected_objects: usize,
    pub max_metadata_prunes: usize,
}

impl Default for TieredExtentGcLimits {
    fn default() -> Self {
        Self {
            retain_generations: 2,
            max_references: 10_000_000,
            max_inventory_entries_per_prefix: 1_000_000,
            max_deletes_per_step: 10_000,
            max_protected_objects: 1_000_000,
            max_metadata_prunes: 1_000_000,
        }
    }
}

impl TieredExtentGcLimits {
    pub fn validate(&self) -> Result<()> {
        if !(1..=1_000).contains(&self.retain_generations)
            || !(1..=10_000_000).contains(&self.max_references)
            || !(1..=10_000_000).contains(&self.max_inventory_entries_per_prefix)
            || !(1..=1_000_000).contains(&self.max_deletes_per_step)
            || self.max_protected_objects > 1_000_000
            || !(1..=2_000_000).contains(&self.max_metadata_prunes)
        {
            return Err(gc_error(
                "retention, reference, inventory, deletion, protection, or prune limits are invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TieredExtentGcState {
    pub format_version: u32,
    pub active_generation: u64,
    pub active_manifest_sha256: String,
    pub retain_generations: usize,
    pub max_references: usize,
    pub max_inventory_entries_per_prefix: usize,
    pub max_deletes_per_step: usize,
    pub max_metadata_prunes: usize,
    pub protected_objects_sha256: String,
    /// Next two-hex-digit namespace to scan. 256 means complete.
    pub next_prefix: u16,
    pub scanned_objects: u64,
    pub deleted_objects: u64,
    pub deleted_bytes: u64,
    pub checksum_sha256: String,
}

impl TieredExtentGcState {
    pub fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            active_generation: u64,
            active_manifest_sha256: &'a str,
            retain_generations: usize,
            max_references: usize,
            max_inventory_entries_per_prefix: usize,
            max_deletes_per_step: usize,
            max_metadata_prunes: usize,
            protected_objects_sha256: &'a str,
            next_prefix: u16,
            scanned_objects: u64,
            deleted_objects: u64,
            deleted_bytes: u64,
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            active_generation: self.active_generation,
            active_manifest_sha256: &self.active_manifest_sha256,
            retain_generations: self.retain_generations,
            max_references: self.max_references,
            max_inventory_entries_per_prefix: self.max_inventory_entries_per_prefix,
            max_deletes_per_step: self.max_deletes_per_step,
            max_metadata_prunes: self.max_metadata_prunes,
            protected_objects_sha256: &self.protected_objects_sha256,
            next_prefix: self.next_prefix,
            scanned_objects: self.scanned_objects,
            deleted_objects: self.deleted_objects,
            deleted_bytes: self.deleted_bytes,
        })
    }

    pub fn validate(&self, limits: &TieredExtentGcLimits) -> Result<()> {
        limits.validate()?;
        validate_sha256(&self.active_manifest_sha256)?;
        validate_sha256(&self.protected_objects_sha256)?;
        validate_sha256(&self.checksum_sha256)?;
        if self.format_version != TIERED_EXTENT_GC_FORMAT_VERSION
            || self.active_generation == 0
            || self.next_prefix > 256
            || self.retain_generations != limits.retain_generations
            || self.max_references != limits.max_references
            || self.max_inventory_entries_per_prefix != limits.max_inventory_entries_per_prefix
            || self.max_deletes_per_step != limits.max_deletes_per_step
            || self.max_metadata_prunes != limits.max_metadata_prunes
            || self.calculate_checksum()? != self.checksum_sha256
        {
            return Err(gc_error(
                "checkpoint identity, limits, progress, or checksum is invalid",
            ));
        }
        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        self.next_prefix == 256
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TieredExtentGcProgress {
    pub prefix: u16,
    pub scanned_objects: usize,
    pub deleted_objects: usize,
    pub deleted_bytes: u64,
    pub advanced: bool,
    pub complete: bool,
}

pub fn start_tiered_extent_gc(
    catalog: &TieredManifestCatalog,
    protected_objects: &[ExtentObjectKey],
    limits: &TieredExtentGcLimits,
    manifest_limits: &TieredManifestLimits,
    storage_limits: &TieredStorageLimits,
) -> Result<TieredExtentGcState> {
    limits.validate()?;
    validate_protected(protected_objects, limits)?;
    let _lock = DirectoryLock::acquire(catalog.root())?;
    let retained =
        catalog.retained_generations(limits.retain_generations, manifest_limits, storage_limits)?;
    let active = retained
        .first()
        .ok_or_else(|| gc_error("cannot start without an active generation"))?;
    let mut state = TieredExtentGcState {
        format_version: TIERED_EXTENT_GC_FORMAT_VERSION,
        active_generation: active.manifest.generation,
        active_manifest_sha256: active.manifest_sha256.clone(),
        retain_generations: limits.retain_generations,
        max_references: limits.max_references,
        max_inventory_entries_per_prefix: limits.max_inventory_entries_per_prefix,
        max_deletes_per_step: limits.max_deletes_per_step,
        max_metadata_prunes: limits.max_metadata_prunes,
        protected_objects_sha256: protected_identity(protected_objects)?,
        next_prefix: 0,
        scanned_objects: 0,
        deleted_objects: 0,
        deleted_bytes: 0,
        checksum_sha256: String::new(),
    };
    state.checksum_sha256 = state.calculate_checksum()?;
    state.validate(limits)?;
    Ok(state)
}

#[allow(clippy::too_many_arguments)]
pub fn run_tiered_extent_gc_step(
    catalog: &TieredManifestCatalog,
    provider: &LocalImmutableExtentStore,
    protected_objects: &[ExtentObjectKey],
    state: &mut TieredExtentGcState,
    limits: &TieredExtentGcLimits,
    manifest_limits: &TieredManifestLimits,
    storage_limits: &TieredStorageLimits,
) -> Result<TieredExtentGcProgress> {
    state.validate(limits)?;
    validate_protected(protected_objects, limits)?;
    if protected_identity(protected_objects)? != state.protected_objects_sha256 {
        return Err(gc_error("protected object set changed since GC started"));
    }
    if state.is_complete() {
        return Ok(TieredExtentGcProgress {
            prefix: 256,
            scanned_objects: 0,
            deleted_objects: 0,
            deleted_bytes: 0,
            advanced: false,
            complete: true,
        });
    }

    let _lock = DirectoryLock::acquire(catalog.root())?;
    let retained =
        catalog.retained_generations(state.retain_generations, manifest_limits, storage_limits)?;
    let active = retained
        .first()
        .ok_or_else(|| gc_error("active generation disappeared during GC"))?;
    if active.manifest.generation != state.active_generation
        || active.manifest_sha256 != state.active_manifest_sha256
    {
        return Err(gc_error(
            "active generation changed; start a new GC run before deleting more objects",
        ));
    }

    let prefix = u8::try_from(state.next_prefix)
        .map_err(|_| gc_error("GC prefix does not fit its namespace"))?;
    let mut references = BTreeSet::new();
    let mut observed_references = 0_usize;
    for generation in &retained {
        for extent in &generation.manifest.extents {
            observed_references = observed_references
                .checked_add(1)
                .ok_or_else(|| gc_error("reference counter overflow"))?;
            if observed_references > state.max_references {
                return Err(gc_error("retained manifest references exceed the GC bound"));
            }
            if key_prefix(&extent.object.key)? == prefix {
                references.insert(extent.object.key.clone());
            }
        }
    }
    for key in protected_objects {
        if key_prefix(key)? == prefix {
            references.insert(key.clone());
        }
    }

    let inventory = provider.inventory_prefix(prefix, state.max_inventory_entries_per_prefix)?;
    let mut deleted_objects = 0_usize;
    let mut deleted_bytes = 0_u64;
    let mut candidates_remain = false;
    for entry in &inventory {
        if references.contains(&entry.key) {
            continue;
        }
        if deleted_objects == state.max_deletes_per_step {
            candidates_remain = true;
            continue;
        }
        if provider.delete(&entry.key)? {
            deleted_objects += 1;
            deleted_bytes = deleted_bytes
                .checked_add(entry.bytes)
                .ok_or_else(|| gc_error("deleted byte counter overflow"))?;
        }
    }

    let mut next = state.clone();
    next.scanned_objects = next
        .scanned_objects
        .checked_add(inventory.len() as u64)
        .ok_or_else(|| gc_error("scanned object counter overflow"))?;
    next.deleted_objects = next
        .deleted_objects
        .checked_add(deleted_objects as u64)
        .ok_or_else(|| gc_error("deleted object counter overflow"))?;
    next.deleted_bytes = next
        .deleted_bytes
        .checked_add(deleted_bytes)
        .ok_or_else(|| gc_error("deleted byte counter overflow"))?;
    let advanced = !candidates_remain;
    if advanced {
        next.next_prefix += 1;
    }
    if next.next_prefix == 256 {
        let oldest_retained = retained
            .last()
            .ok_or_else(|| gc_error("retained generation window is empty"))?;
        catalog.prune_generations_before(
            oldest_retained.manifest.generation,
            state.max_metadata_prunes,
        )?;
    }
    next.checksum_sha256 = next.calculate_checksum()?;
    next.validate(limits)?;
    *state = next;

    Ok(TieredExtentGcProgress {
        prefix: u16::from(prefix),
        scanned_objects: inventory.len(),
        deleted_objects,
        deleted_bytes,
        advanced,
        complete: state.is_complete(),
    })
}

fn validate_protected(keys: &[ExtentObjectKey], limits: &TieredExtentGcLimits) -> Result<()> {
    if keys.len() > limits.max_protected_objects {
        return Err(gc_error("protected object set exceeds its bound"));
    }
    for key in keys {
        key.sha256()?;
    }
    Ok(())
}

fn protected_identity(keys: &[ExtentObjectKey]) -> Result<String> {
    let mut canonical: Vec<&str> = keys.iter().map(ExtentObjectKey::as_str).collect();
    canonical.sort_unstable();
    canonical.dedup();
    sha256_json(&canonical)
}

fn key_prefix(key: &ExtentObjectKey) -> Result<u8> {
    let sha256 = key.sha256()?;
    u8::from_str_radix(&sha256[..2], 16)
        .map_err(|_| gc_error("extent key prefix is not hexadecimal"))
}

fn sha256_json(value: &impl Serialize) -> Result<String> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| gc_error(format!("cannot encode checksum payload: {error}")))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        seal_page_extent, ImmutableExtentStore, PageExtentTier, PageGenerationManifest, PageHeader,
        PageStore, PageStoreOptions, PageType, PAGE_HEADER_BYTES,
    };

    fn extent(
        directory: &tempfile::TempDir,
        provider: &LocalImmutableExtentStore,
        marker: u8,
        checkpoint: u64,
        storage_limits: &TieredStorageLimits,
    ) -> crate::PageExtentDescriptor {
        let path = directory.path().join(format!("pages-{marker}"));
        let store = PageStore::open(&path, PageStoreOptions::default().with_fsync(false)).unwrap();
        let page_id = store.allocate(PageType::Heap).unwrap();
        let mut page = vec![0_u8; store.page_size() as usize];
        let mut header = PageHeader::new(page_id, PageType::Heap, store.page_size());
        header.lsn = checkpoint;
        header.encode(&mut page);
        page[PAGE_HEADER_BYTES] = marker;
        store.write_page(page_id, &mut page).unwrap();
        store.flush().unwrap();
        seal_page_extent(
            provider,
            path,
            1,
            1,
            crate::DEFAULT_PAGE_SIZE,
            checkpoint,
            PageExtentTier::Cold,
            storage_limits,
        )
        .unwrap()
    }

    fn generation(
        number: u64,
        checkpoint: u64,
        previous: Option<String>,
        extent: crate::PageExtentDescriptor,
        manifest_limits: &TieredManifestLimits,
        storage_limits: &TieredStorageLimits,
    ) -> PageGenerationManifest {
        PageGenerationManifest::create(
            "database-a",
            number,
            crate::DEFAULT_PAGE_SIZE,
            checkpoint,
            number,
            previous,
            vec![extent],
            manifest_limits,
            storage_limits,
        )
        .unwrap()
    }

    fn run_to_completion(
        catalog: &TieredManifestCatalog,
        provider: &LocalImmutableExtentStore,
        protected: &[ExtentObjectKey],
        state: &mut TieredExtentGcState,
        limits: &TieredExtentGcLimits,
        manifest_limits: &TieredManifestLimits,
        storage_limits: &TieredStorageLimits,
    ) {
        while !state.is_complete() {
            run_tiered_extent_gc_step(
                catalog,
                provider,
                protected,
                state,
                limits,
                manifest_limits,
                storage_limits,
            )
            .unwrap();
        }
    }

    #[test]
    fn gc_keeps_active_and_protected_objects_but_reclaims_orphans_and_old_history() {
        let directory = tempfile::tempdir().unwrap();
        let provider =
            LocalImmutableExtentStore::open(directory.path().join("provider"), false).unwrap();
        let catalog = TieredManifestCatalog::open(directory.path().join("catalog"), false).unwrap();
        let storage_limits = TieredStorageLimits {
            io_buffer_bytes: 4096,
            max_extent_bytes: 1024 * 1024,
        };
        let manifest_limits = TieredManifestLimits::default();
        let old = extent(&directory, &provider, 1, 10, &storage_limits);
        let active_one = catalog
            .publish(
                &provider,
                &generation(1, 10, None, old.clone(), &manifest_limits, &storage_limits),
                None,
                &manifest_limits,
                &storage_limits,
            )
            .unwrap();
        let current = extent(&directory, &provider, 2, 11, &storage_limits);
        catalog
            .publish(
                &provider,
                &generation(
                    2,
                    11,
                    Some(active_one.manifest_sha256.clone()),
                    current.clone(),
                    &manifest_limits,
                    &storage_limits,
                ),
                Some(&active_one.manifest_sha256),
                &manifest_limits,
                &storage_limits,
            )
            .unwrap();
        let orphan = extent(&directory, &provider, 3, 12, &storage_limits);
        let protected = extent(&directory, &provider, 4, 12, &storage_limits);
        let limits = TieredExtentGcLimits {
            retain_generations: 1,
            ..TieredExtentGcLimits::default()
        };
        let mut state = start_tiered_extent_gc(
            &catalog,
            &[protected.object.key.clone()],
            &limits,
            &manifest_limits,
            &storage_limits,
        )
        .unwrap();
        run_to_completion(
            &catalog,
            &provider,
            &[protected.object.key.clone()],
            &mut state,
            &limits,
            &manifest_limits,
            &storage_limits,
        );
        assert!(provider
            .read_verified(&current.object, &mut Vec::new(), &storage_limits)
            .is_ok());
        assert!(provider
            .read_verified(&protected.object, &mut Vec::new(), &storage_limits)
            .is_ok());
        assert!(provider
            .read_verified(&old.object, &mut Vec::new(), &storage_limits)
            .is_err());
        assert!(provider
            .read_verified(&orphan.object, &mut Vec::new(), &storage_limits)
            .is_err());
        assert_eq!(
            catalog
                .retained_generations(10, &manifest_limits, &storage_limits)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(state.deleted_objects, 2);
    }

    #[test]
    fn active_generation_change_fences_an_old_gc_checkpoint_before_deletion() {
        let directory = tempfile::tempdir().unwrap();
        let provider =
            LocalImmutableExtentStore::open(directory.path().join("provider"), false).unwrap();
        let catalog = TieredManifestCatalog::open(directory.path().join("catalog"), false).unwrap();
        let storage_limits = TieredStorageLimits {
            io_buffer_bytes: 4096,
            max_extent_bytes: 1024 * 1024,
        };
        let manifest_limits = TieredManifestLimits::default();
        let first = extent(&directory, &provider, 1, 10, &storage_limits);
        let active = catalog
            .publish(
                &provider,
                &generation(1, 10, None, first, &manifest_limits, &storage_limits),
                None,
                &manifest_limits,
                &storage_limits,
            )
            .unwrap();
        let orphan = extent(&directory, &provider, 9, 10, &storage_limits);
        let limits = TieredExtentGcLimits {
            retain_generations: 1,
            ..TieredExtentGcLimits::default()
        };
        let mut state =
            start_tiered_extent_gc(&catalog, &[], &limits, &manifest_limits, &storage_limits)
                .unwrap();
        let second = extent(&directory, &provider, 2, 11, &storage_limits);
        catalog
            .publish(
                &provider,
                &generation(
                    2,
                    11,
                    Some(active.manifest_sha256.clone()),
                    second,
                    &manifest_limits,
                    &storage_limits,
                ),
                Some(&active.manifest_sha256),
                &manifest_limits,
                &storage_limits,
            )
            .unwrap();
        assert!(run_tiered_extent_gc_step(
            &catalog,
            &provider,
            &[],
            &mut state,
            &limits,
            &manifest_limits,
            &storage_limits,
        )
        .is_err());
        assert!(provider
            .read_verified(&orphan.object, &mut Vec::new(), &storage_limits)
            .is_ok());
        assert_eq!(state.deleted_objects, 0);
    }

    #[test]
    fn checkpoint_tampering_or_a_changed_protection_set_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let provider =
            LocalImmutableExtentStore::open(directory.path().join("provider"), false).unwrap();
        let catalog = TieredManifestCatalog::open(directory.path().join("catalog"), false).unwrap();
        let storage_limits = TieredStorageLimits {
            io_buffer_bytes: 4096,
            max_extent_bytes: 1024 * 1024,
        };
        let manifest_limits = TieredManifestLimits::default();
        let first = extent(&directory, &provider, 1, 10, &storage_limits);
        catalog
            .publish(
                &provider,
                &generation(
                    1,
                    10,
                    None,
                    first.clone(),
                    &manifest_limits,
                    &storage_limits,
                ),
                None,
                &manifest_limits,
                &storage_limits,
            )
            .unwrap();
        let limits = TieredExtentGcLimits::default();
        let mut state =
            start_tiered_extent_gc(&catalog, &[], &limits, &manifest_limits, &storage_limits)
                .unwrap();
        state.deleted_bytes = 1;
        assert!(run_tiered_extent_gc_step(
            &catalog,
            &provider,
            &[],
            &mut state,
            &limits,
            &manifest_limits,
            &storage_limits,
        )
        .is_err());
        state.deleted_bytes = 0;
        state.checksum_sha256 = state.calculate_checksum().unwrap();
        assert!(run_tiered_extent_gc_step(
            &catalog,
            &provider,
            &[first.object.key],
            &mut state,
            &limits,
            &manifest_limits,
            &storage_limits,
        )
        .is_err());
    }
}
