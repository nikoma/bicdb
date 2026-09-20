//! Point reads from one immutable, authenticated page-generation snapshot.
//!
//! A reader owns the [`ActivePageGeneration`] it was created with. Activation
//! of a newer generation therefore cannot make one operation observe a mixture
//! of checkpoint extents. Extent lookup is logarithmic, cache residency is
//! bounded independently, and a hit copies only one page into the caller's
//! existing buffer.

use std::sync::Arc;

use crate::error::{PageError, Result};
use crate::page::{PageHeader, PageId};
use crate::pool::PageReadSource;
use crate::tiered::{ImmutableExtentStore, PageExtentDescriptor, TieredStorageLimits};
use crate::tiered_cache::TieredExtentCache;
use crate::tiered_manifest::{ActivePageGeneration, TieredManifestLimits};

/// Read-only view of one atomically activated tiered checkpoint generation.
pub struct TieredPageReader {
    active: ActivePageGeneration,
    cache: Arc<TieredExtentCache>,
    provider: Arc<dyn ImmutableExtentStore>,
    storage_limits: TieredStorageLimits,
}

impl std::fmt::Debug for TieredPageReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TieredPageReader")
            .field("database_id", &self.active.manifest.database_id)
            .field("generation", &self.active.manifest.generation)
            .field("manifest_sha256", &self.active.manifest_sha256)
            .field("page_size", &self.active.manifest.page_size)
            .field("sealed_page_count", &self.active.manifest.sealed_page_count)
            .finish_non_exhaustive()
    }
}

impl TieredPageReader {
    pub fn new(
        active: ActivePageGeneration,
        cache: Arc<TieredExtentCache>,
        provider: Arc<dyn ImmutableExtentStore>,
        manifest_limits: &TieredManifestLimits,
        storage_limits: TieredStorageLimits,
    ) -> Result<Self> {
        active.validate(manifest_limits, &storage_limits)?;
        Ok(Self {
            active,
            cache,
            provider,
            storage_limits,
        })
    }

    pub fn active_generation(&self) -> &ActivePageGeneration {
        &self.active
    }

    pub fn page_size(&self) -> u32 {
        self.active.manifest.page_size
    }

    pub fn checkpoint_lsn(&self) -> u64 {
        self.active.manifest.checkpoint_lsn
    }

    pub fn page_count(&self) -> u64 {
        self.active.manifest.sealed_page_count.saturating_add(1)
    }

    /// Read a non-superblock page from the pinned active generation.
    pub fn read_page(&self, page_id: PageId, buffer: &mut [u8]) -> Result<PageHeader> {
        if page_id == 0 || page_id >= self.page_count() {
            return Err(PageError::OutOfBounds {
                page_id,
                page_count: self.page_count(),
            });
        }
        if buffer.len() != self.page_size() as usize {
            return Err(PageError::InvalidPageSize(
                u32::try_from(buffer.len()).unwrap_or(u32::MAX),
            ));
        }
        let descriptor = self.extent_for(page_id)?;
        self.cache.read_page(
            self.provider.as_ref(),
            descriptor,
            page_id,
            buffer,
            &self.storage_limits,
        )
    }

    fn extent_for(&self, page_id: PageId) -> Result<&PageExtentDescriptor> {
        let extents = &self.active.manifest.extents;
        let position = extents.partition_point(|extent| extent.first_page <= page_id);
        let descriptor = position
            .checked_sub(1)
            .and_then(|index| extents.get(index))
            .filter(|extent| extent.contains(page_id))
            .ok_or(PageError::OutOfBounds {
                page_id,
                page_count: self.page_count(),
            })?;
        Ok(descriptor)
    }
}

impl PageReadSource for TieredPageReader {
    fn page_size(&self) -> u32 {
        TieredPageReader::page_size(self)
    }

    fn page_count(&self) -> u64 {
        TieredPageReader::page_count(self)
    }

    fn read_page(&self, page_id: PageId, buffer: &mut [u8]) -> Result<PageHeader> {
        TieredPageReader::read_page(self, page_id, buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        seal_page_extent, BufferPool, BufferPoolOptions, LocalImmutableExtentStore, PageExtentTier,
        PageGenerationManifest, PageStore, PageStoreOptions, PageType, TieredExtentCacheLimits,
        TieredManifestCatalog, DEFAULT_PAGE_SIZE, PAGE_HEADER_BYTES,
    };
    use std::fs::OpenOptions;

    fn active_generation(
        directory: &tempfile::TempDir,
        provider: &LocalImmutableExtentStore,
        storage_limits: &TieredStorageLimits,
        manifest_limits: &TieredManifestLimits,
    ) -> ActivePageGeneration {
        let page_path = directory.path().join("pages");
        let store =
            PageStore::open(&page_path, PageStoreOptions::default().with_fsync(false)).unwrap();
        for marker in [11_u8, 22, 33] {
            let page_id = store.allocate(PageType::Heap).unwrap();
            let mut bytes = vec![0_u8; store.page_size() as usize];
            let header = PageHeader::new(page_id, PageType::Heap, store.page_size());
            header.encode(&mut bytes);
            bytes[PAGE_HEADER_BYTES] = marker;
            store.write_page(page_id, &mut bytes).unwrap();
        }
        store.flush().unwrap();
        let extents = vec![
            seal_page_extent(
                provider,
                &page_path,
                1,
                2,
                DEFAULT_PAGE_SIZE,
                91,
                PageExtentTier::Warm,
                storage_limits,
            )
            .unwrap(),
            seal_page_extent(
                provider,
                &page_path,
                3,
                1,
                DEFAULT_PAGE_SIZE,
                91,
                PageExtentTier::Cold,
                storage_limits,
            )
            .unwrap(),
        ];
        let manifest = PageGenerationManifest::create(
            "reader-database",
            1,
            DEFAULT_PAGE_SIZE,
            91,
            1,
            None,
            extents,
            manifest_limits,
            storage_limits,
        )
        .unwrap();
        TieredManifestCatalog::open(directory.path().join("catalog"), false)
            .unwrap()
            .publish(provider, &manifest, None, manifest_limits, storage_limits)
            .unwrap()
    }

    fn fixture() -> (
        tempfile::TempDir,
        Arc<LocalImmutableExtentStore>,
        Arc<TieredExtentCache>,
        ActivePageGeneration,
        TieredStorageLimits,
        TieredManifestLimits,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let provider = Arc::new(
            LocalImmutableExtentStore::open(directory.path().join("provider"), false).unwrap(),
        );
        let storage_limits = TieredStorageLimits {
            io_buffer_bytes: 4096,
            max_extent_bytes: 1024 * 1024,
        };
        let manifest_limits = TieredManifestLimits::default();
        let active = active_generation(
            &directory,
            provider.as_ref(),
            &storage_limits,
            &manifest_limits,
        );
        let cache = Arc::new(
            TieredExtentCache::open(
                directory.path().join("cache"),
                TieredExtentCacheLimits {
                    max_bytes: 1024 * 1024,
                    max_entries: 2,
                    ..TieredExtentCacheLimits::default()
                },
                false,
            )
            .unwrap(),
        );
        (
            directory,
            provider,
            cache,
            active,
            storage_limits,
            manifest_limits,
        )
    }

    #[test]
    fn point_reads_route_across_extents_and_reuse_one_hydration() {
        let (_directory, provider, cache, active, storage_limits, manifest_limits) = fixture();
        let reader = TieredPageReader::new(
            active,
            Arc::clone(&cache),
            provider,
            &manifest_limits,
            storage_limits,
        )
        .unwrap();
        let mut bytes = vec![0_u8; reader.page_size() as usize];
        for (page_id, marker) in [(2, 22_u8), (1, 11), (3, 33)] {
            let header = reader.read_page(page_id, &mut bytes).unwrap();
            assert_eq!(header.page_id, page_id);
            assert_eq!(bytes[PAGE_HEADER_BYTES], marker);
        }
        let snapshot = cache.snapshot();
        assert_eq!(snapshot.misses, 2);
        assert_eq!(snapshot.hits, 1);
        assert_eq!(snapshot.entries, 2);
        assert!(snapshot.resident_bytes <= snapshot.max_bytes);
    }

    #[test]
    fn local_page_corruption_evicts_then_rehydrates_from_the_provider() {
        let (_directory, provider, cache, active, storage_limits, manifest_limits) = fixture();
        let first_key = active.manifest.extents[0].object.key.clone();
        let reader = TieredPageReader::new(
            active,
            Arc::clone(&cache),
            provider,
            &manifest_limits,
            storage_limits,
        )
        .unwrap();
        let mut bytes = vec![0_u8; reader.page_size() as usize];
        reader.read_page(1, &mut bytes).unwrap();
        let cached_path = cache.root().join(first_key.as_str());
        let cached = OpenOptions::new().write(true).open(cached_path).unwrap();
        crate::pio::write_at(&cached, &[0xff], PAGE_HEADER_BYTES as u64).unwrap();

        assert!(reader.read_page(1, &mut bytes).is_err());
        assert_eq!(cache.snapshot().entries, 0);
        reader.read_page(1, &mut bytes).unwrap();
        assert_eq!(bytes[PAGE_HEADER_BYTES], 11);
        assert_eq!(cache.snapshot().corruptions, 1);
    }

    #[test]
    fn reader_rejects_superblock_out_of_range_and_wrong_buffers() {
        let (_directory, provider, cache, active, storage_limits, manifest_limits) = fixture();
        let reader =
            TieredPageReader::new(active, cache, provider, &manifest_limits, storage_limits)
                .unwrap();
        assert!(matches!(
            reader.read_page(0, &mut vec![0_u8; DEFAULT_PAGE_SIZE as usize]),
            Err(PageError::OutOfBounds { .. })
        ));
        assert!(matches!(
            reader.read_page(4, &mut vec![0_u8; DEFAULT_PAGE_SIZE as usize]),
            Err(PageError::OutOfBounds { .. })
        ));
        assert!(matches!(
            reader.read_page(1, &mut [0_u8; 16]),
            Err(PageError::InvalidPageSize(16))
        ));
    }

    #[test]
    fn detached_active_generation_must_match_its_content_hash() {
        let (_directory, provider, cache, mut active, storage_limits, manifest_limits) = fixture();
        active.manifest.checkpoint_lsn += 1;
        let error =
            TieredPageReader::new(active, cache, provider, &manifest_limits, storage_limits)
                .unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn tiered_reader_feeds_a_bounded_read_only_buffer_pool() {
        let (directory, provider, cache, active, storage_limits, manifest_limits) = fixture();
        let reader = Arc::new(
            TieredPageReader::new(active, cache, provider, &manifest_limits, storage_limits)
                .unwrap(),
        );
        let local_metadata = Arc::new(
            PageStore::open(
                directory.path().join("local-metadata-pages"),
                PageStoreOptions::default().with_fsync(false),
            )
            .unwrap(),
        );
        let pool = BufferPool::new_read_only(
            local_metadata,
            reader,
            BufferPoolOptions::default()
                .with_budget_bytes(u64::from(DEFAULT_PAGE_SIZE) * 2)
                .with_shards(1),
        )
        .unwrap();

        for (page_id, marker) in [(1, 11_u8), (2, 22), (3, 33)] {
            let page = pool.get(page_id).unwrap();
            assert_eq!(page[PAGE_HEADER_BYTES], marker);
        }
        let snapshot = pool.snapshot();
        assert!(snapshot.read_only);
        assert_eq!(snapshot.total_frames, 2);
        assert!(snapshot.resident_pages <= snapshot.total_frames);
        assert!(matches!(
            pool.get_mut(1),
            Err(PageError::ReadOnlyPageSource { page_id: 1 })
        ));
    }
}
