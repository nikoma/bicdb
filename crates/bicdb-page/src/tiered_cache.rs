//! Bounded local hydration cache for immutable tiered page extents.
//!
//! The cache is explicitly non-authoritative. It admits immutable objects under
//! hard byte/entry/concurrency limits, verifies downloads before publication,
//! evicts only unpinned objects, and can be deleted without affecting the active
//! generation manifest or provider copy.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use serde::{Deserialize, Serialize};

use crate::error::{PageError, Result};
use crate::lock::DirectoryLock;
use crate::page::{PageHeader, PageId};
use crate::tiered::{
    verify_page_extent, verify_page_extent_reader, ExtentObjectKey, ImmutableExtentStore,
    LocalImmutableExtentStore, PageExtentDescriptor, TieredStorageLimits,
};

static HYDRATION_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn cache_error(message: impl Into<String>) -> PageError {
    PageError::TieredStorage {
        reason: format!("extent cache: {}", message.into()),
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TieredExtentCacheLimits {
    pub max_bytes: u64,
    pub max_entries: usize,
    pub max_concurrent_hydrations: usize,
    pub hydration_wait_ms: u64,
    pub max_incomplete_cleanup: usize,
    pub provider_max_attempts: u32,
    pub retry_initial_backoff_ms: u64,
    pub retry_max_backoff_ms: u64,
}

impl Default for TieredExtentCacheLimits {
    fn default() -> Self {
        Self {
            max_bytes: 256 * 1024 * 1024 * 1024,
            max_entries: 100_000,
            max_concurrent_hydrations: 8,
            hydration_wait_ms: 30_000,
            max_incomplete_cleanup: 1_024,
            provider_max_attempts: 4,
            retry_initial_backoff_ms: 25,
            retry_max_backoff_ms: 1_000,
        }
    }
}

impl TieredExtentCacheLimits {
    pub fn validate(&self) -> Result<()> {
        if !(1024 * 1024..=1024 * 1024 * 1024 * 1024 * 1024).contains(&self.max_bytes)
            || !(1..=1_000_000).contains(&self.max_entries)
            || !(1..=1024).contains(&self.max_concurrent_hydrations)
            || !(1..=3_600_000).contains(&self.hydration_wait_ms)
            || !(1..=100_000).contains(&self.max_incomplete_cleanup)
            || !(1..=16).contains(&self.provider_max_attempts)
            || !(1..=60_000).contains(&self.retry_initial_backoff_ms)
            || self.retry_max_backoff_ms < self.retry_initial_backoff_ms
            || self.retry_max_backoff_ms > 300_000
        {
            return Err(cache_error(
                "byte, entry, hydration, wait, cleanup, attempt, or backoff limits are outside supported bounds",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TieredExtentCacheSnapshot {
    pub max_bytes: u64,
    pub max_entries: usize,
    pub resident_bytes: u64,
    pub reserved_bytes: u64,
    pub entries: usize,
    pub active_hydrations: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub corruptions: u64,
    pub provider_retries: u64,
}

#[derive(Debug)]
struct CacheEntry {
    bytes: u64,
    access: u64,
    pins: u32,
    invalid: bool,
    /// Hydrations are verified before publication. Restart discovery knows
    /// only the filename and byte count, so its first use must re-establish the
    /// full content-address and page-layout binding before point reads begin.
    identity_verified: bool,
}

#[derive(Debug, Default)]
struct CacheState {
    entries: BTreeMap<ExtentObjectKey, CacheEntry>,
    hydrating: BTreeSet<ExtentObjectKey>,
    resident_bytes: u64,
    reserved_bytes: u64,
    access_sequence: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
    corruptions: u64,
    provider_retries: u64,
}

#[derive(Debug)]
pub struct TieredExtentCache {
    root: PathBuf,
    store: LocalImmutableExtentStore,
    limits: TieredExtentCacheLimits,
    state: Mutex<CacheState>,
    changed: Condvar,
    fsync: bool,
    _lock: DirectoryLock,
}

impl TieredExtentCache {
    pub fn open(
        root: impl AsRef<Path>,
        limits: TieredExtentCacheLimits,
        fsync: bool,
    ) -> Result<Self> {
        limits.validate()?;
        let root = root.as_ref().to_path_buf();
        reject_unsafe_directory_if_present(&root, "cache root")?;
        fs::create_dir_all(&root).map_err(|error| PageError::io(&root, error))?;
        reject_unsafe_directory(&root, "cache root")?;
        let lock = DirectoryLock::acquire(&root)?;
        cleanup_incomplete(&root, limits.max_incomplete_cleanup)?;
        let store = LocalImmutableExtentStore::open(&root, fsync)?;
        let mut cache = Self {
            root,
            store,
            limits,
            state: Mutex::new(CacheState::default()),
            changed: Condvar::new(),
            fsync,
            _lock: lock,
        };
        cache.discover_existing()?;
        Ok(cache)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn snapshot(&self) -> TieredExtentCacheSnapshot {
        let state = self.state.lock();
        TieredExtentCacheSnapshot {
            max_bytes: self.limits.max_bytes,
            max_entries: self.limits.max_entries,
            resident_bytes: state.resident_bytes,
            reserved_bytes: state.reserved_bytes,
            entries: state.entries.len(),
            active_hydrations: state.hydrating.len(),
            hits: state.hits,
            misses: state.misses,
            evictions: state.evictions,
            corruptions: state.corruptions,
            provider_retries: state.provider_retries,
        }
    }

    /// Read an extent through the cache. A cache miss downloads to an
    /// operation-owned staging file, verifies the complete object and every
    /// page, and atomically publishes the immutable local copy before serving
    /// it. The destination may contain a prefix when an error is returned.
    pub fn read_extent(
        &self,
        provider: &dyn ImmutableExtentStore,
        descriptor: &PageExtentDescriptor,
        destination: &mut dyn Write,
        storage_limits: &TieredStorageLimits,
    ) -> Result<()> {
        descriptor.validate(storage_limits)?;
        let pin = self.ensure_cached(provider, descriptor, storage_limits)?;
        let result = self
            .store
            .read_verified(&descriptor.object, destination, storage_limits);
        let release = pin.release(result.is_err());
        match (result, release) {
            (Err(error), _) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    /// Read and verify exactly one page through the local hydration cache.
    /// The immutable object remains pinned until the positioned read finishes;
    /// any local read or page-integrity failure invalidates and removes that
    /// cache entry so a later call must rehydrate it from the provider.
    pub fn read_page(
        &self,
        provider: &dyn ImmutableExtentStore,
        descriptor: &PageExtentDescriptor,
        page_id: PageId,
        buffer: &mut [u8],
        storage_limits: &TieredStorageLimits,
    ) -> Result<PageHeader> {
        descriptor.validate(storage_limits)?;
        if !descriptor.contains(page_id) {
            return Err(cache_error(format!(
                "page {page_id} is outside extent {}..{}",
                descriptor.first_page,
                descriptor.end_page()?
            )));
        }
        if buffer.len() != descriptor.page_size as usize {
            return Err(cache_error(format!(
                "point-read buffer is {} bytes, expected {}",
                buffer.len(),
                descriptor.page_size
            )));
        }
        let page_index = page_id
            .checked_sub(descriptor.first_page)
            .ok_or_else(|| cache_error("extent page offset underflow"))?;
        let object_offset = page_index
            .checked_mul(u64::from(descriptor.page_size))
            .ok_or_else(|| cache_error("extent page byte offset overflow"))?;
        let pin = self.ensure_cached(provider, descriptor, storage_limits)?;
        let result = self.store.read_page_verified(
            &descriptor.object,
            object_offset,
            page_id,
            buffer,
            storage_limits,
        );
        let release = pin.release(result.is_err());
        match (result, release) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(header), Ok(())) => Ok(header),
        }
    }

    /// Evict one exact object. Pinned or currently hydrating objects are never
    /// unlinked and cause a fail-closed busy error instead.
    pub fn evict(&self, key: &ExtentObjectKey) -> Result<bool> {
        key.sha256()?;
        let mut state = self.state.lock();
        if state.hydrating.contains(key) {
            return Err(cache_error("cannot evict an object while it hydrates"));
        }
        let Some(entry) = state.entries.get(key) else {
            return Ok(false);
        };
        if entry.pins != 0 {
            return Err(cache_error("cannot evict a pinned cache object"));
        }
        let bytes = entry.bytes;
        self.store.delete(key)?;
        state.entries.remove(key);
        state.resident_bytes = state.resident_bytes.saturating_sub(bytes);
        state.evictions = state.evictions.saturating_add(1);
        self.changed.notify_all();
        Ok(true)
    }

    fn ensure_cached<'a>(
        &'a self,
        provider: &dyn ImmutableExtentStore,
        descriptor: &PageExtentDescriptor,
        storage_limits: &TieredStorageLimits,
    ) -> Result<CachePin<'a>> {
        let key = descriptor.object.key.clone();
        let bytes = descriptor.object.bytes;
        if bytes > self.limits.max_bytes {
            return Err(cache_error(format!(
                "object needs {bytes} bytes but cache budget is {}",
                self.limits.max_bytes
            )));
        }
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(self.limits.hydration_wait_ms))
            .ok_or_else(|| cache_error("hydration deadline overflow"))?;

        loop {
            let mut state = self.state.lock();
            let needs_identity_verification = state.entries.get(&key).is_some_and(|entry| {
                !entry.invalid
                    && !entry.identity_verified
                    && entry.pins == 0
                    && !state.hydrating.contains(&key)
            });
            if needs_identity_verification {
                let entry = state.entries.get_mut(&key).ok_or_else(|| {
                    cache_error("cache entry vanished before identity verification")
                })?;
                entry.pins = 1;
                state.hydrating.insert(key.clone());
                drop(state);

                let verification = verify_page_extent(&self.store, descriptor, storage_limits);
                let mut state = self.state.lock();
                state.hydrating.remove(&key);
                let entry = state.entries.get_mut(&key).ok_or_else(|| {
                    cache_error("cache entry vanished during identity verification")
                })?;
                entry.pins = entry
                    .pins
                    .checked_sub(1)
                    .ok_or_else(|| cache_error("cache verification pin counter underflow"))?;
                match verification {
                    Ok(()) => entry.identity_verified = true,
                    Err(error) => {
                        entry.invalid = true;
                        self.remove_entry_locked(&mut state, &key, true)?;
                        self.changed.notify_all();
                        return Err(error);
                    }
                }
                self.changed.notify_all();
                continue;
            }
            if let Some(entry) = state.entries.get(&key) {
                if !entry.invalid && entry.identity_verified {
                    let access = next_access(&mut state);
                    let entry = state
                        .entries
                        .get_mut(&key)
                        .ok_or_else(|| cache_error("cache entry vanished while acquiring a pin"))?;
                    entry.access = access;
                    entry.pins = entry
                        .pins
                        .checked_add(1)
                        .ok_or_else(|| cache_error("cache pin counter overflow"))?;
                    state.hits = state.hits.saturating_add(1);
                    return Ok(CachePin {
                        cache: self,
                        key,
                        released: false,
                    });
                }
            }

            let blocked = state.hydrating.contains(&key)
                || state.hydrating.len() >= self.limits.max_concurrent_hydrations
                || state.entries.get(&key).is_some_and(|entry| entry.pins != 0);
            if blocked {
                let now = Instant::now();
                if now >= deadline {
                    return Err(cache_error("timed out waiting for cache admission"));
                }
                let wait = deadline.saturating_duration_since(now);
                if self.changed.wait_for(&mut state, wait).timed_out() {
                    return Err(cache_error("timed out waiting for cache admission"));
                }
                continue;
            }

            if state.entries.get(&key).is_some_and(|entry| entry.invalid) {
                self.remove_entry_locked(&mut state, &key, true)?;
            }
            self.evict_until_admitted(&mut state, bytes)?;
            state.reserved_bytes = state
                .reserved_bytes
                .checked_add(bytes)
                .ok_or_else(|| cache_error("cache reservation overflow"))?;
            state.hydrating.insert(key.clone());
            state.misses = state.misses.saturating_add(1);
            drop(state);

            let hydration = self.hydrate(provider, descriptor, storage_limits);
            let mut state = self.state.lock();
            state.hydrating.remove(&key);
            state.reserved_bytes = state.reserved_bytes.saturating_sub(bytes);
            match hydration {
                Ok(()) => {
                    let access = next_access(&mut state);
                    if state
                        .entries
                        .insert(
                            key.clone(),
                            CacheEntry {
                                bytes,
                                access,
                                pins: 1,
                                invalid: false,
                                identity_verified: true,
                            },
                        )
                        .is_some()
                    {
                        return Err(cache_error(
                            "hydration would replace an existing cache entry",
                        ));
                    }
                    state.resident_bytes = state
                        .resident_bytes
                        .checked_add(bytes)
                        .ok_or_else(|| cache_error("cache residency overflow"))?;
                    self.changed.notify_all();
                    return Ok(CachePin {
                        cache: self,
                        key,
                        released: false,
                    });
                }
                Err(error) => {
                    self.changed.notify_all();
                    return Err(error);
                }
            }
        }
    }

    fn hydrate(
        &self,
        provider: &dyn ImmutableExtentStore,
        descriptor: &PageExtentDescriptor,
        storage_limits: &TieredStorageLimits,
    ) -> Result<()> {
        let mut backoff_ms = self.limits.retry_initial_backoff_ms;
        for attempt in 1..=self.limits.provider_max_attempts {
            match self.hydrate_once(provider, descriptor, storage_limits) {
                Ok(()) => return Ok(()),
                Err(error)
                    if attempt < self.limits.provider_max_attempts
                        && is_transient_provider_error(&error) =>
                {
                    {
                        let mut state = self.state.lock();
                        state.provider_retries = state.provider_retries.saturating_add(1);
                    }
                    std::thread::sleep(Duration::from_millis(backoff_ms));
                    backoff_ms = backoff_ms
                        .saturating_mul(2)
                        .min(self.limits.retry_max_backoff_ms);
                }
                Err(error) => return Err(error),
            }
        }
        Err(cache_error("provider retry loop exhausted unexpectedly"))
    }

    fn hydrate_once(
        &self,
        provider: &dyn ImmutableExtentStore,
        descriptor: &PageExtentDescriptor,
        storage_limits: &TieredStorageLimits,
    ) -> Result<()> {
        let (staging_path, mut staging) = create_hydration_file(&self.root)?;
        let guard = StagingGuard(staging_path.clone());
        provider.read_verified(&descriptor.object, &mut staging, storage_limits)?;
        if self.fsync {
            staging
                .sync_data()
                .map_err(|error| PageError::io(&staging_path, error))?;
        }
        staging
            .seek(SeekFrom::Start(0))
            .map_err(|error| PageError::io(&staging_path, error))?;
        verify_page_extent_reader(&mut staging, descriptor, storage_limits)?;
        drop(staging);
        self.store.publish_verified_staging(
            &descriptor.object.key,
            descriptor.object.bytes,
            &staging_path,
            storage_limits,
        )?;
        drop(guard);
        Ok(())
    }

    fn evict_until_admitted(&self, state: &mut CacheState, bytes: u64) -> Result<()> {
        loop {
            let projected_bytes = state
                .resident_bytes
                .checked_add(state.reserved_bytes)
                .and_then(|value| value.checked_add(bytes))
                .ok_or_else(|| cache_error("cache admission byte overflow"))?;
            let projected_entries = state
                .entries
                .len()
                .checked_add(state.hydrating.len())
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| cache_error("cache admission entry overflow"))?;
            if projected_bytes <= self.limits.max_bytes
                && projected_entries <= self.limits.max_entries
            {
                return Ok(());
            }
            let candidate = state
                .entries
                .iter()
                .filter(|(_, entry)| entry.pins == 0)
                .min_by_key(|(_, entry)| entry.access)
                .map(|(key, _)| key.clone())
                .ok_or_else(|| {
                    cache_error("cache is full and every eviction candidate is pinned")
                })?;
            self.remove_entry_locked(state, &candidate, false)?;
        }
    }

    fn remove_entry_locked(
        &self,
        state: &mut CacheState,
        key: &ExtentObjectKey,
        corruption: bool,
    ) -> Result<()> {
        let entry = state
            .entries
            .get(key)
            .ok_or_else(|| cache_error("cache eviction entry is missing"))?;
        if entry.pins != 0 {
            return Err(cache_error("refusing to unlink a pinned cache object"));
        }
        let bytes = entry.bytes;
        self.store.delete(key)?;
        state.entries.remove(key);
        state.resident_bytes = state.resident_bytes.saturating_sub(bytes);
        state.evictions = state.evictions.saturating_add(1);
        if corruption {
            state.corruptions = state.corruptions.saturating_add(1);
        }
        Ok(())
    }

    fn release_pin(&self, key: &ExtentObjectKey, invalidate: bool) -> Result<()> {
        let mut state = self.state.lock();
        let entry = state
            .entries
            .get_mut(key)
            .ok_or_else(|| cache_error("cache pin refers to a missing entry"))?;
        if entry.pins == 0 {
            return Err(cache_error("cache pin counter underflow"));
        }
        entry.pins -= 1;
        entry.invalid |= invalidate;
        let remove = entry.invalid && entry.pins == 0;
        if remove {
            let result = self.remove_entry_locked(&mut state, key, true);
            self.changed.notify_all();
            return result;
        }
        self.changed.notify_all();
        Ok(())
    }

    fn discover_existing(&mut self) -> Result<()> {
        let namespace = self.root.join("sha256");
        let mut state = self.state.lock();
        for prefix_entry in
            fs::read_dir(&namespace).map_err(|error| PageError::io(&namespace, error))?
        {
            let prefix_entry = prefix_entry.map_err(|error| PageError::io(&namespace, error))?;
            let prefix = prefix_entry
                .file_name()
                .into_string()
                .map_err(|_| cache_error("cache prefix is not UTF-8"))?;
            if prefix.len() != 2
                || !prefix.bytes().all(|byte| byte.is_ascii_hexdigit())
                || prefix.bytes().any(|byte| byte.is_ascii_uppercase())
                || !prefix_entry
                    .file_type()
                    .map_err(|error| PageError::io(prefix_entry.path(), error))?
                    .is_dir()
            {
                return Err(cache_error(format!(
                    "invalid cache prefix {}",
                    prefix_entry.path().display()
                )));
            }
            for object_entry in fs::read_dir(prefix_entry.path())
                .map_err(|error| PageError::io(prefix_entry.path(), error))?
            {
                let object_entry =
                    object_entry.map_err(|error| PageError::io(prefix_entry.path(), error))?;
                let hash = object_entry
                    .file_name()
                    .into_string()
                    .map_err(|_| cache_error("cache object name is not UTF-8"))?;
                let key = ExtentObjectKey::from_sha256(hash)?;
                if !key.as_str().starts_with(&format!("sha256/{prefix}/")) {
                    return Err(cache_error("cache object is in the wrong hash prefix"));
                }
                let file_type = object_entry
                    .file_type()
                    .map_err(|error| PageError::io(object_entry.path(), error))?;
                if !file_type.is_file() || file_type.is_symlink() {
                    return Err(cache_error(format!(
                        "refusing non-regular cache object {}",
                        object_entry.path().display()
                    )));
                }
                let bytes = object_entry
                    .metadata()
                    .map_err(|error| PageError::io(object_entry.path(), error))?
                    .len();
                let admitted = bytes > 0
                    && state.entries.len() < self.limits.max_entries
                    && state
                        .resident_bytes
                        .checked_add(bytes)
                        .is_some_and(|total| total <= self.limits.max_bytes);
                if !admitted {
                    self.store.delete(&key)?;
                    state.evictions = state.evictions.saturating_add(1);
                    continue;
                }
                let access = next_access(&mut state);
                state.entries.insert(
                    key,
                    CacheEntry {
                        bytes,
                        access,
                        pins: 0,
                        invalid: false,
                        identity_verified: false,
                    },
                );
                state.resident_bytes += bytes;
            }
        }
        Ok(())
    }
}

fn is_transient_provider_error(error: &PageError) -> bool {
    let PageError::Io { source, .. } = error else {
        return false;
    };
    matches!(
        source.kind(),
        std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::NotConnected
    )
}

struct CachePin<'a> {
    cache: &'a TieredExtentCache,
    key: ExtentObjectKey,
    released: bool,
}

impl CachePin<'_> {
    fn release(mut self, invalidate: bool) -> Result<()> {
        let result = self.cache.release_pin(&self.key, invalidate);
        self.released = true;
        result
    }
}

impl Drop for CachePin<'_> {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.cache.release_pin(&self.key, false);
        }
    }
}

fn next_access(state: &mut CacheState) -> u64 {
    state.access_sequence = state.access_sequence.saturating_add(1);
    state.access_sequence
}

fn create_hydration_file(root: &Path) -> Result<(PathBuf, File)> {
    for _ in 0..128 {
        let sequence = HYDRATION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = root.join(format!(
            ".hydrate.{}.{}.incomplete",
            std::process::id(),
            sequence
        ));
        let mut options = OpenOptions::new();
        options.create_new(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        match options.open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(PageError::io(&path, error)),
        }
    }
    Err(cache_error("could not allocate a hydration staging file"))
}

struct StagingGuard(PathBuf);

impl Drop for StagingGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn cleanup_incomplete(root: &Path, max_files: usize) -> Result<usize> {
    let mut removed = 0_usize;
    for entry in fs::read_dir(root).map_err(|error| PageError::io(root, error))? {
        let entry = entry.map_err(|error| PageError::io(root, error))?;
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with(".hydrate.")
            || !name.to_string_lossy().ends_with(".incomplete")
        {
            continue;
        }
        if removed == max_files {
            return Err(cache_error(
                "incomplete hydration cleanup budget exhausted; retry open",
            ));
        }
        let file_type = entry
            .file_type()
            .map_err(|error| PageError::io(entry.path(), error))?;
        if !file_type.is_file() || file_type.is_symlink() {
            return Err(cache_error(format!(
                "refusing non-regular hydration staging path {}",
                entry.path().display()
            )));
        }
        fs::remove_file(entry.path()).map_err(|error| PageError::io(entry.path(), error))?;
        removed += 1;
    }
    Ok(removed)
}

fn reject_unsafe_directory_if_present(path: &Path, kind: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => Err(
            cache_error(format!("refusing non-directory {kind} {}", path.display())),
        ),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(PageError::io(path, error)),
    }
}

fn reject_unsafe_directory(path: &Path, kind: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| PageError::io(path, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(cache_error(format!(
            "refusing non-directory {kind} {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        seal_page_extent, PageExtentTier, PageHeader, PageStore, PageStoreOptions, PageType,
        PAGE_HEADER_BYTES,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::Arc;
    use std::thread;

    fn descriptor(
        directory: &tempfile::TempDir,
        provider: &LocalImmutableExtentStore,
        marker: u8,
        limits: &TieredStorageLimits,
    ) -> PageExtentDescriptor {
        let path = directory.path().join(format!("pages-{marker}"));
        let store = PageStore::open(&path, PageStoreOptions::default().with_fsync(false)).unwrap();
        let page_id = store.allocate(PageType::Heap).unwrap();
        let mut page = vec![0_u8; store.page_size() as usize];
        let header = PageHeader::new(page_id, PageType::Heap, store.page_size());
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
            1,
            PageExtentTier::Cold,
            limits,
        )
        .unwrap()
    }

    #[derive(Debug)]
    struct CountingProvider<'a> {
        inner: &'a LocalImmutableExtentStore,
        reads: AtomicUsize,
        fail: AtomicBool,
        transient_failures: AtomicUsize,
    }

    impl ImmutableExtentStore for CountingProvider<'_> {
        fn put_verified(
            &self,
            key: &ExtentObjectKey,
            expected_bytes: u64,
            source: &mut dyn std::io::Read,
            limits: &TieredStorageLimits,
        ) -> Result<crate::ExtentObjectMetadata> {
            self.inner.put_verified(key, expected_bytes, source, limits)
        }

        fn read_verified(
            &self,
            metadata: &crate::ExtentObjectMetadata,
            destination: &mut dyn Write,
            limits: &TieredStorageLimits,
        ) -> Result<()> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self
                .transient_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(PageError::io(
                    PathBuf::from("<injected-provider>"),
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "injected timeout"),
                ));
            }
            if self.fail.load(Ordering::SeqCst) {
                return Err(cache_error("injected provider failure"));
            }
            self.inner.read_verified(metadata, destination, limits)
        }

        fn delete(&self, key: &ExtentObjectKey) -> Result<bool> {
            self.inner.delete(key)
        }
    }

    #[test]
    fn misses_hydrate_once_hits_stay_local_and_restart_discovers_cache() {
        let directory = tempfile::tempdir().unwrap();
        let provider_store =
            LocalImmutableExtentStore::open(directory.path().join("provider"), false).unwrap();
        let storage_limits = TieredStorageLimits {
            io_buffer_bytes: 4096,
            max_extent_bytes: 1024 * 1024,
        };
        let descriptor = descriptor(&directory, &provider_store, 7, &storage_limits);
        let provider = CountingProvider {
            inner: &provider_store,
            reads: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            transient_failures: AtomicUsize::new(0),
        };
        let cache_limits = TieredExtentCacheLimits {
            max_bytes: 1024 * 1024,
            max_entries: 4,
            ..TieredExtentCacheLimits::default()
        };
        let cache_root = directory.path().join("cache");
        {
            let cache = TieredExtentCache::open(&cache_root, cache_limits, false).unwrap();
            let mut first = Vec::new();
            cache
                .read_extent(&provider, &descriptor, &mut first, &storage_limits)
                .unwrap();
            let mut second = Vec::new();
            cache
                .read_extent(&provider, &descriptor, &mut second, &storage_limits)
                .unwrap();
            assert_eq!(first, second);
            assert_eq!(provider.reads.load(Ordering::SeqCst), 1);
            assert_eq!(cache.snapshot().entries, 1);
        }
        fs::write(cache_root.join(".hydrate.stale.incomplete"), b"partial").unwrap();
        provider.fail.store(true, Ordering::SeqCst);
        let cache = TieredExtentCache::open(&cache_root, cache_limits, false).unwrap();
        cache
            .read_extent(&provider, &descriptor, &mut Vec::new(), &storage_limits)
            .unwrap();
        assert_eq!(provider.reads.load(Ordering::SeqCst), 1);
        assert!(!cache_root.join(".hydrate.stale.incomplete").exists());
    }

    #[test]
    fn restart_rechecks_the_content_address_before_serving_point_reads() {
        let directory = tempfile::tempdir().unwrap();
        let provider =
            LocalImmutableExtentStore::open(directory.path().join("provider"), false).unwrap();
        let storage_limits = TieredStorageLimits {
            io_buffer_bytes: 4096,
            max_extent_bytes: 1024 * 1024,
        };
        let descriptor = descriptor(&directory, &provider, 17, &storage_limits);
        let cache_root = directory.path().join("cache");
        let cache_limits = TieredExtentCacheLimits {
            max_bytes: 1024 * 1024,
            max_entries: 4,
            ..TieredExtentCacheLimits::default()
        };
        {
            let cache = TieredExtentCache::open(&cache_root, cache_limits, false).unwrap();
            cache
                .read_page(
                    &provider,
                    &descriptor,
                    descriptor.first_page,
                    &mut vec![0_u8; descriptor.page_size as usize],
                    &storage_limits,
                )
                .unwrap();
        }

        // Make the cached page structurally valid but different from its
        // content address. Per-page verification alone cannot detect this.
        let object_path = cache_root.join(descriptor.object.key.as_str());
        let mut changed = fs::read(&object_path).unwrap();
        let header = PageHeader::decode(&changed, &object_path).unwrap();
        changed[PAGE_HEADER_BYTES] ^= 0xff;
        crate::page::finalize(&mut changed, header.generation);
        fs::write(&object_path, changed).unwrap();

        let cache = TieredExtentCache::open(&cache_root, cache_limits, false).unwrap();
        let mut page = vec![0_u8; descriptor.page_size as usize];
        assert!(cache
            .read_page(
                &provider,
                &descriptor,
                descriptor.first_page,
                &mut page,
                &storage_limits,
            )
            .is_err());
        assert_eq!(cache.snapshot().entries, 0);
        assert_eq!(cache.snapshot().corruptions, 1);
        cache
            .read_page(
                &provider,
                &descriptor,
                descriptor.first_page,
                &mut page,
                &storage_limits,
            )
            .unwrap();
        assert_eq!(page[PAGE_HEADER_BYTES], 17);
    }

    #[test]
    fn hard_budget_evicts_lru_and_corruption_is_removed_for_rehydration() {
        let directory = tempfile::tempdir().unwrap();
        let provider =
            LocalImmutableExtentStore::open(directory.path().join("provider"), false).unwrap();
        let storage_limits = TieredStorageLimits {
            io_buffer_bytes: 4096,
            max_extent_bytes: 1024 * 1024,
        };
        let first = descriptor(&directory, &provider, 1, &storage_limits);
        let second = descriptor(&directory, &provider, 2, &storage_limits);
        let cache = TieredExtentCache::open(
            directory.path().join("cache"),
            TieredExtentCacheLimits {
                max_bytes: 1024 * 1024,
                max_entries: 1,
                ..TieredExtentCacheLimits::default()
            },
            false,
        )
        .unwrap();
        let pin = cache
            .ensure_cached(&provider, &first, &storage_limits)
            .unwrap();
        assert!(cache
            .ensure_cached(&provider, &second, &storage_limits)
            .is_err());
        drop(pin);
        cache
            .read_extent(&provider, &first, &mut Vec::new(), &storage_limits)
            .unwrap();
        cache
            .read_extent(&provider, &second, &mut Vec::new(), &storage_limits)
            .unwrap();
        let snapshot = cache.snapshot();
        assert_eq!(snapshot.entries, 1);
        assert_eq!(snapshot.evictions, 1);
        assert!(snapshot.resident_bytes <= snapshot.max_bytes);

        let cached_path = cache.root.join(second.object.key.as_str());
        let cached = OpenOptions::new().write(true).open(cached_path).unwrap();
        crate::pio::write_at(&cached, &[0xff], 100).unwrap();
        assert!(cache
            .read_extent(&provider, &second, &mut Vec::new(), &storage_limits)
            .is_err());
        assert_eq!(cache.snapshot().entries, 0);
        cache
            .read_extent(&provider, &second, &mut Vec::new(), &storage_limits)
            .unwrap();
        assert_eq!(cache.snapshot().corruptions, 1);
    }

    #[test]
    fn concurrent_readers_share_one_hydration_and_failures_leave_no_staging() {
        let directory = tempfile::tempdir().unwrap();
        let provider_store = Box::leak(Box::new(
            LocalImmutableExtentStore::open(directory.path().join("provider"), false).unwrap(),
        ));
        let storage_limits = TieredStorageLimits {
            io_buffer_bytes: 4096,
            max_extent_bytes: 1024 * 1024,
        };
        let descriptor = Arc::new(descriptor(&directory, provider_store, 9, &storage_limits));
        let provider = Arc::new(CountingProvider {
            inner: provider_store,
            reads: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            transient_failures: AtomicUsize::new(0),
        });
        let cache = Arc::new(
            TieredExtentCache::open(
                directory.path().join("cache"),
                TieredExtentCacheLimits {
                    max_bytes: 1024 * 1024,
                    max_entries: 4,
                    hydration_wait_ms: 5_000,
                    ..TieredExtentCacheLimits::default()
                },
                false,
            )
            .unwrap(),
        );
        let mut workers = Vec::new();
        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let provider = Arc::clone(&provider);
            let descriptor = Arc::clone(&descriptor);
            workers.push(thread::spawn(move || {
                cache
                    .read_extent(
                        provider.as_ref(),
                        &descriptor,
                        &mut Vec::new(),
                        &storage_limits,
                    )
                    .unwrap();
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(provider.reads.load(Ordering::SeqCst), 1);

        cache.evict(&descriptor.object.key).unwrap();
        provider.fail.store(true, Ordering::SeqCst);
        assert!(cache
            .read_extent(
                provider.as_ref(),
                &descriptor,
                &mut Vec::new(),
                &storage_limits,
            )
            .is_err());
        assert!(fs::read_dir(cache.root()).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".incomplete")));
    }

    #[test]
    fn transient_provider_failures_retry_within_the_attempt_budget() {
        let directory = tempfile::tempdir().unwrap();
        let provider_store =
            LocalImmutableExtentStore::open(directory.path().join("provider"), false).unwrap();
        let storage_limits = TieredStorageLimits {
            io_buffer_bytes: 4096,
            max_extent_bytes: 1024 * 1024,
        };
        let descriptor = descriptor(&directory, &provider_store, 11, &storage_limits);
        let provider = CountingProvider {
            inner: &provider_store,
            reads: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            transient_failures: AtomicUsize::new(2),
        };
        let cache = TieredExtentCache::open(
            directory.path().join("cache"),
            TieredExtentCacheLimits {
                max_bytes: 1024 * 1024,
                max_entries: 4,
                provider_max_attempts: 3,
                retry_initial_backoff_ms: 1,
                retry_max_backoff_ms: 2,
                ..TieredExtentCacheLimits::default()
            },
            false,
        )
        .unwrap();
        cache
            .read_extent(&provider, &descriptor, &mut Vec::new(), &storage_limits)
            .unwrap();
        assert_eq!(provider.reads.load(Ordering::SeqCst), 3);
        assert_eq!(cache.snapshot().provider_retries, 2);
    }
}
