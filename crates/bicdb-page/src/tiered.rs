//! Immutable, content-addressed page extents for warm and cold storage.
//!
//! Mutable page files are never tiered directly. A caller first establishes a
//! durable checkpoint, then seals a page-aligned range. Every source page is
//! verified before upload, the raw extent receives a SHA-256 identity, and the
//! provider publishes it without overwrite. This is the safe primitive on
//! which generation manifests and remote-read caches are built.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{PageError, Result};
use crate::page::{self, PageHeader, PageId};

pub const PAGE_EXTENT_FORMAT_VERSION: u32 = 1;
const OBJECT_NAMESPACE: &str = "sha256";
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn tier_error(message: impl Into<String>) -> PageError {
    PageError::TieredStorage {
        reason: message.into(),
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TieredStorageLimits {
    /// Scratch buffer for upload/download hashing. This is independent of the
    /// extent size, so memory remains constant as the database grows.
    pub io_buffer_bytes: usize,
    /// One immutable object is finite even if the database is petabytes.
    pub max_extent_bytes: u64,
}

impl Default for TieredStorageLimits {
    fn default() -> Self {
        Self {
            io_buffer_bytes: 1024 * 1024,
            max_extent_bytes: 64 * 1024 * 1024 * 1024,
        }
    }
}

impl TieredStorageLimits {
    pub fn validate(&self) -> Result<()> {
        if self.io_buffer_bytes < 4 * 1024
            || self.io_buffer_bytes > 4 * 1024 * 1024
            || self.max_extent_bytes < 1024 * 1024
            || self.max_extent_bytes > 1024 * 1024 * 1024 * 1024
        {
            return Err(tier_error(
                "tiered I/O buffers must be 4KiB..=4MiB and extents 1MiB..=1TiB",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PageExtentTier {
    Warm,
    Cold,
}

/// Provider key derived solely from the lowercase SHA-256. Callers cannot use
/// this type to escape the provider namespace or choose an alias for bytes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct ExtentObjectKey(String);

impl ExtentObjectKey {
    pub fn from_sha256(sha256: impl Into<String>) -> Result<Self> {
        let sha256 = sha256.into();
        validate_sha256(&sha256)?;
        Ok(Self(format!(
            "{OBJECT_NAMESPACE}/{}/{}",
            &sha256[..2],
            sha256
        )))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn sha256(&self) -> Result<&str> {
        let rest = self
            .0
            .strip_prefix(&format!("{OBJECT_NAMESPACE}/"))
            .ok_or_else(|| tier_error("extent object key has an invalid namespace"))?;
        let (prefix, sha256) = rest
            .split_once('/')
            .ok_or_else(|| tier_error("extent object key has an invalid layout"))?;
        validate_sha256(sha256)?;
        if prefix.len() != 2 || prefix != &sha256[..2] {
            return Err(tier_error("extent object key is not canonical"));
        }
        Ok(sha256)
    }

    fn validate(&self) -> Result<()> {
        self.sha256().map(|_| ())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtentObjectMetadata {
    pub key: ExtentObjectKey,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtentObjectInventoryEntry {
    pub key: ExtentObjectKey,
    pub bytes: u64,
}

impl ExtentObjectMetadata {
    fn validate(&self, limits: &TieredStorageLimits) -> Result<()> {
        self.key.validate()?;
        validate_sha256(&self.sha256)?;
        if self.bytes == 0
            || self.bytes > limits.max_extent_bytes
            || self.key.sha256()? != self.sha256
        {
            return Err(tier_error(
                "extent object metadata has an invalid size or content identity",
            ));
        }
        Ok(())
    }
}

/// Minimal capability for immutable extent bytes. Implementations may use a
/// local directory, object storage, or another provider, but must retain
/// no-overwrite and exact-byte semantics.
pub trait ImmutableExtentStore: Send + Sync {
    fn put_verified(
        &self,
        key: &ExtentObjectKey,
        expected_bytes: u64,
        source: &mut dyn Read,
        limits: &TieredStorageLimits,
    ) -> Result<ExtentObjectMetadata>;

    fn read_verified(
        &self,
        metadata: &ExtentObjectMetadata,
        destination: &mut dyn Write,
        limits: &TieredStorageLimits,
    ) -> Result<()>;

    fn delete(&self, key: &ExtentObjectKey) -> Result<bool>;
}

/// First-party filesystem provider. It is also the reference implementation
/// for remote providers: writes go to a unique incomplete file, are hashed and
/// synced, then a no-replace hard-link publication makes the content address
/// visible atomically. Existing objects are verified, never overwritten.
#[derive(Debug)]
pub struct LocalImmutableExtentStore {
    root: PathBuf,
    fsync: bool,
}

impl LocalImmutableExtentStore {
    pub fn open(root: impl AsRef<Path>, fsync: bool) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        reject_symlink_if_exists(&root, "extent provider root")?;
        fs::create_dir_all(&root).map_err(|error| PageError::io(&root, error))?;
        reject_symlink(&root, "extent provider root")?;
        let namespace = root.join(OBJECT_NAMESPACE);
        reject_symlink_if_exists(&namespace, "extent namespace")?;
        fs::create_dir_all(&namespace).map_err(|error| PageError::io(&namespace, error))?;
        reject_symlink(&namespace, "extent namespace")?;
        Ok(Self { root, fsync })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Enumerate one of the 256 content-address prefixes under an explicit
    /// entry bound. Provider GC checkpoints between these finite namespaces
    /// instead of accumulating a PB-scale inventory in memory.
    pub fn inventory_prefix(
        &self,
        prefix: u8,
        max_entries: usize,
    ) -> Result<Vec<ExtentObjectInventoryEntry>> {
        if max_entries == 0 || max_entries > 10_000_000 {
            return Err(tier_error(
                "extent inventory limit must be within 1..=10,000,000",
            ));
        }
        let prefix_name = format!("{prefix:02x}");
        let directory = self.root.join(OBJECT_NAMESPACE).join(&prefix_name);
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(PageError::io(&directory, error)),
        };
        reject_symlink(&directory, "extent hash prefix")?;
        let mut inventory = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| PageError::io(&directory, error))?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| tier_error("extent inventory filename is not UTF-8"))?;
            if name.ends_with(".incomplete") {
                continue;
            }
            let key = ExtentObjectKey::from_sha256(name)?;
            if !key
                .as_str()
                .starts_with(&format!("{OBJECT_NAMESPACE}/{prefix_name}/"))
            {
                return Err(tier_error("extent object is in the wrong hash prefix"));
            }
            let file_type = entry
                .file_type()
                .map_err(|error| PageError::io(entry.path(), error))?;
            if !file_type.is_file() || file_type.is_symlink() {
                return Err(tier_error(format!(
                    "refusing non-regular extent inventory object {}",
                    entry.path().display()
                )));
            }
            let bytes = entry
                .metadata()
                .map_err(|error| PageError::io(entry.path(), error))?
                .len();
            if bytes == 0 {
                return Err(tier_error("extent inventory contains an empty object"));
            }
            if inventory.len() == max_entries {
                return Err(tier_error(format!(
                    "extent prefix {prefix_name} exceeds its inventory bound"
                )));
            }
            inventory.push(ExtentObjectInventoryEntry { key, bytes });
        }
        inventory.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(inventory)
    }

    fn object_path(&self, key: &ExtentObjectKey) -> Result<PathBuf> {
        key.validate()?;
        Ok(self.root.join(key.as_str()))
    }

    fn ensure_object_parent(&self, key: &ExtentObjectKey) -> Result<PathBuf> {
        let path = self.object_path(key)?;
        let parent = path
            .parent()
            .ok_or_else(|| tier_error("extent object path has no parent"))?;
        reject_symlink_if_exists(parent, "extent hash prefix")?;
        fs::create_dir_all(parent).map_err(|error| PageError::io(parent, error))?;
        reject_symlink(parent, "extent hash prefix")?;
        Ok(path)
    }

    fn verify_existing(
        &self,
        path: &Path,
        key: &ExtentObjectKey,
        expected_bytes: u64,
        limits: &TieredStorageLimits,
    ) -> Result<ExtentObjectMetadata> {
        let file = open_regular_no_follow(path, "extent object")?;
        let (bytes, sha256) = hash_exact(file, expected_bytes, limits)?;
        if sha256 != key.sha256()? {
            return Err(tier_error(format!(
                "existing extent object {} failed its content address",
                key.as_str()
            )));
        }
        let metadata = ExtentObjectMetadata {
            key: key.clone(),
            bytes,
            sha256,
        };
        metadata.validate(limits)?;
        Ok(metadata)
    }

    /// Read one page from a previously verified immutable object without
    /// materializing the complete extent. The containing cache keeps the
    /// object pinned while this method runs; this method rechecks the local
    /// object's exact length and the selected page's checksum/torn-write guard.
    pub(crate) fn read_page_verified(
        &self,
        metadata: &ExtentObjectMetadata,
        object_offset: u64,
        page_id: PageId,
        buffer: &mut [u8],
        limits: &TieredStorageLimits,
    ) -> Result<PageHeader> {
        limits.validate()?;
        metadata.validate(limits)?;
        if buffer.is_empty() {
            return Err(tier_error(
                "tiered point read requires a non-empty page buffer",
            ));
        }
        let end = object_offset
            .checked_add(buffer.len() as u64)
            .ok_or_else(|| tier_error("tiered point-read range overflow"))?;
        if end > metadata.bytes {
            return Err(tier_error("tiered point read exceeds its extent object"));
        }

        let path = self.object_path(&metadata.key)?;
        let file = open_regular_no_follow(&path, "extent object")?;
        let file_bytes = file
            .metadata()
            .map_err(|error| PageError::io(&path, error))?
            .len();
        if file_bytes != metadata.bytes {
            return Err(tier_error(format!(
                "extent object {} size changed from {} to {file_bytes}",
                metadata.key.as_str(),
                metadata.bytes
            )));
        }
        let got = crate::pio::read_at(&file, buffer, object_offset)
            .map_err(|error| PageError::io(&path, error))?;
        if got != buffer.len() {
            return Err(PageError::ShortRead {
                page_id,
                got,
                want: buffer.len(),
            });
        }
        page::verify(buffer, page_id)?;
        PageHeader::decode(buffer, &path)
    }

    /// Publish an already downloaded staging file without making a second
    /// full-size copy. The cache is the only caller: it creates the staging
    /// file directly under this provider root, verifies every contained page,
    /// and then asks the immutable provider to independently recheck the byte
    /// count and content address before a no-overwrite hard-link publication.
    pub(crate) fn publish_verified_staging(
        &self,
        key: &ExtentObjectKey,
        expected_bytes: u64,
        staging_path: &Path,
        limits: &TieredStorageLimits,
    ) -> Result<ExtentObjectMetadata> {
        limits.validate()?;
        key.validate()?;
        if staging_path.parent() != Some(self.root.as_path()) {
            return Err(tier_error(
                "extent staging file is outside the provider root",
            ));
        }
        let staging = open_regular_no_follow(staging_path, "extent staging file")?;
        let (bytes, sha256) = hash_exact(staging, expected_bytes, limits)?;
        if sha256 != key.sha256()? {
            return Err(tier_error(
                "extent staging file differs from its content address",
            ));
        }
        let path = self.ensure_object_parent(key)?;
        match fs::symlink_metadata(&path) {
            Ok(_) => return self.verify_existing(&path, key, expected_bytes, limits),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(PageError::io(&path, error)),
        }
        match fs::hard_link(staging_path, &path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return self.verify_existing(&path, key, expected_bytes, limits);
            }
            Err(error) => return Err(PageError::io(&path, error)),
        }
        if self.fsync {
            let parent = path
                .parent()
                .ok_or_else(|| tier_error("extent object path has no parent"))?;
            File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| PageError::io(parent, error))?;
        }
        let metadata = ExtentObjectMetadata {
            key: key.clone(),
            bytes,
            sha256,
        };
        metadata.validate(limits)?;
        Ok(metadata)
    }
}

impl ImmutableExtentStore for LocalImmutableExtentStore {
    fn put_verified(
        &self,
        key: &ExtentObjectKey,
        expected_bytes: u64,
        source: &mut dyn Read,
        limits: &TieredStorageLimits,
    ) -> Result<ExtentObjectMetadata> {
        limits.validate()?;
        key.validate()?;
        if expected_bytes == 0 || expected_bytes > limits.max_extent_bytes {
            return Err(tier_error("extent upload size is outside its bound"));
        }
        let path = self.ensure_object_parent(key)?;
        match fs::symlink_metadata(&path) {
            Ok(_) => return self.verify_existing(&path, key, expected_bytes, limits),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(PageError::io(&path, error)),
        }

        let parent = path
            .parent()
            .ok_or_else(|| tier_error("extent object path has no parent"))?
            .to_path_buf();
        let (temporary_path, mut temporary) = create_incomplete_file(&parent, key.sha256()?)?;
        let mut guard = IncompleteObjectGuard {
            path: temporary_path.clone(),
            published: false,
        };
        let mut buffer = vec![0_u8; limits.io_buffer_bytes];
        let mut remaining = expected_bytes;
        let mut hasher = Sha256::new();
        while remaining > 0 {
            let request = usize::try_from(remaining.min(buffer.len() as u64))
                .map_err(|_| tier_error("extent upload request does not fit address space"))?;
            let read = source
                .read(&mut buffer[..request])
                .map_err(|error| PageError::io(&temporary_path, error))?;
            if read == 0 {
                return Err(tier_error(format!(
                    "extent upload ended with {remaining} bytes missing"
                )));
            }
            temporary
                .write_all(&buffer[..read])
                .map_err(|error| PageError::io(&temporary_path, error))?;
            hasher.update(&buffer[..read]);
            remaining -= read as u64;
        }
        let mut extra = [0_u8; 1];
        if source
            .read(&mut extra)
            .map_err(|error| PageError::io(&temporary_path, error))?
            != 0
        {
            return Err(tier_error(
                "extent upload source contains bytes beyond its declared length",
            ));
        }
        let sha256 = hex::encode(hasher.finalize());
        if sha256 != key.sha256()? {
            return Err(tier_error(
                "extent source changed or differs from its expected SHA-256",
            ));
        }
        if self.fsync {
            temporary
                .sync_data()
                .map_err(|error| PageError::io(&temporary_path, error))?;
        }
        drop(temporary);

        match fs::hard_link(&temporary_path, &path) {
            Ok(()) => {
                fs::remove_file(&temporary_path)
                    .map_err(|error| PageError::io(&temporary_path, error))?;
                guard.published = true;
                if self.fsync {
                    File::open(&parent)
                        .and_then(|directory| directory.sync_all())
                        .map_err(|error| PageError::io(&parent, error))?;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = self.verify_existing(&path, key, expected_bytes, limits)?;
                return Ok(existing);
            }
            Err(error) => return Err(PageError::io(&path, error)),
        }
        let metadata = ExtentObjectMetadata {
            key: key.clone(),
            bytes: expected_bytes,
            sha256,
        };
        metadata.validate(limits)?;
        Ok(metadata)
    }

    fn read_verified(
        &self,
        metadata: &ExtentObjectMetadata,
        destination: &mut dyn Write,
        limits: &TieredStorageLimits,
    ) -> Result<()> {
        limits.validate()?;
        metadata.validate(limits)?;
        let path = self.object_path(&metadata.key)?;
        let mut file = open_regular_no_follow(&path, "extent object")?;
        let file_bytes = file
            .metadata()
            .map_err(|error| PageError::io(&path, error))?
            .len();
        if file_bytes != metadata.bytes {
            return Err(tier_error(format!(
                "extent object {} size changed from {} to {file_bytes}",
                metadata.key.as_str(),
                metadata.bytes
            )));
        }
        let mut buffer = vec![0_u8; limits.io_buffer_bytes];
        let mut remaining = metadata.bytes;
        let mut hasher = Sha256::new();
        while remaining > 0 {
            let request = usize::try_from(remaining.min(buffer.len() as u64))
                .map_err(|_| tier_error("extent read request does not fit address space"))?;
            let read = file
                .read(&mut buffer[..request])
                .map_err(|error| PageError::io(&path, error))?;
            if read == 0 {
                return Err(tier_error("extent object ended before its declared length"));
            }
            hasher.update(&buffer[..read]);
            destination
                .write_all(&buffer[..read])
                .map_err(|error| PageError::io(PathBuf::from("<extent-destination>"), error))?;
            remaining -= read as u64;
        }
        let mut extra = [0_u8; 1];
        if file
            .read(&mut extra)
            .map_err(|error| PageError::io(&path, error))?
            != 0
        {
            return Err(tier_error(
                "extent object contains bytes beyond its declared length",
            ));
        }
        let actual = hex::encode(hasher.finalize());
        if actual != metadata.sha256 {
            return Err(tier_error(format!(
                "extent object {} checksum mismatch",
                metadata.key.as_str()
            )));
        }
        Ok(())
    }

    fn delete(&self, key: &ExtentObjectKey) -> Result<bool> {
        let path = self.object_path(key)?;
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                Err(tier_error(format!(
                    "refusing to delete non-regular extent object {}",
                    path.display()
                )))
            }
            Ok(_) => {
                fs::remove_file(&path).map_err(|error| PageError::io(&path, error))?;
                if self.fsync {
                    let parent = path
                        .parent()
                        .ok_or_else(|| tier_error("extent object path has no parent"))?;
                    File::open(parent)
                        .and_then(|directory| directory.sync_all())
                        .map_err(|error| PageError::io(parent, error))?;
                }
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(PageError::io(&path, error)),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PageExtentDescriptor {
    pub format_version: u32,
    pub first_page: PageId,
    pub page_count: u64,
    pub page_size: u32,
    pub checkpoint_lsn: u64,
    pub tier: PageExtentTier,
    pub object: ExtentObjectMetadata,
}

impl PageExtentDescriptor {
    pub fn end_page(&self) -> Result<PageId> {
        self.first_page
            .checked_add(self.page_count)
            .ok_or_else(|| tier_error("page extent end overflows page id"))
    }

    pub fn contains(&self, page_id: PageId) -> bool {
        page_id >= self.first_page
            && self
                .first_page
                .checked_add(self.page_count)
                .is_some_and(|end| page_id < end)
    }

    pub fn validate(&self, limits: &TieredStorageLimits) -> Result<()> {
        limits.validate()?;
        page::validate_page_size(self.page_size)?;
        self.object.validate(limits)?;
        let expected_bytes = self
            .page_count
            .checked_mul(u64::from(self.page_size))
            .ok_or_else(|| tier_error("page extent byte length overflow"))?;
        self.end_page()?;
        if self.format_version != PAGE_EXTENT_FORMAT_VERSION
            || self.first_page == 0
            || self.page_count == 0
            || expected_bytes != self.object.bytes
        {
            return Err(tier_error(
                "page extent identity, range, or byte length is invalid",
            ));
        }
        Ok(())
    }
}

/// Verify and seal a page-aligned range from a durable checkpoint file. The
/// source is read twice: once to verify pages and derive its content address,
/// then again while the provider independently checks the upload. Any mutation
/// between the passes fails the provider hash check.
#[allow(clippy::too_many_arguments)]
pub fn seal_page_extent(
    store: &dyn ImmutableExtentStore,
    page_file: impl AsRef<Path>,
    first_page: PageId,
    page_count: u64,
    page_size: u32,
    checkpoint_lsn: u64,
    tier: PageExtentTier,
    limits: &TieredStorageLimits,
) -> Result<PageExtentDescriptor> {
    limits.validate()?;
    page::validate_page_size(page_size)?;
    if first_page == 0 || page_count == 0 {
        return Err(tier_error(
            "tiered extents cannot include the mutable superblock or be empty",
        ));
    }
    let extent_bytes = page_count
        .checked_mul(u64::from(page_size))
        .ok_or_else(|| tier_error("page extent byte length overflow"))?;
    if extent_bytes > limits.max_extent_bytes {
        return Err(tier_error(format!(
            "page extent is {extent_bytes} bytes, above its configured bound"
        )));
    }
    let offset = first_page
        .checked_mul(u64::from(page_size))
        .ok_or_else(|| tier_error("page extent file offset overflow"))?;
    let path = page_file.as_ref();
    let mut file = open_regular_no_follow(path, "checkpoint page file")?;
    let required_end = offset
        .checked_add(extent_bytes)
        .ok_or_else(|| tier_error("page extent source end overflow"))?;
    let source_bytes = file
        .metadata()
        .map_err(|error| PageError::io(path, error))?
        .len();
    if source_bytes < required_end {
        return Err(tier_error(format!(
            "checkpoint page file ends at {source_bytes}, before extent end {required_end}"
        )));
    }
    file.seek(SeekFrom::Start(offset))
        .map_err(|error| PageError::io(path, error))?;
    let mut hasher = Sha256::new();
    let mut page_buffer = vec![0_u8; page_size as usize];
    for ordinal in 0..page_count {
        file.read_exact(&mut page_buffer)
            .map_err(|error| PageError::io(path, error))?;
        let page_id = first_page
            .checked_add(ordinal)
            .ok_or_else(|| tier_error("page extent page id overflow"))?;
        page::verify(&page_buffer, page_id)?;
        hasher.update(&page_buffer);
    }
    let sha256 = hex::encode(hasher.finalize());
    let key = ExtentObjectKey::from_sha256(sha256)?;

    file.seek(SeekFrom::Start(offset))
        .map_err(|error| PageError::io(path, error))?;
    let mut source = file.take(extent_bytes);
    let object = store.put_verified(&key, extent_bytes, &mut source, limits)?;
    let descriptor = PageExtentDescriptor {
        format_version: PAGE_EXTENT_FORMAT_VERSION,
        first_page,
        page_count,
        page_size,
        checkpoint_lsn,
        tier,
        object,
    };
    descriptor.validate(limits)?;
    Ok(descriptor)
}

pub fn verify_page_extent(
    store: &dyn ImmutableExtentStore,
    descriptor: &PageExtentDescriptor,
    limits: &TieredStorageLimits,
) -> Result<()> {
    descriptor.validate(limits)?;
    let mut sink = PageVerificationSink::new(descriptor)?;
    let read_result = store.read_verified(&descriptor.object, &mut sink, limits);
    if let Some(error) = sink.error.take() {
        return Err(error);
    }
    read_result?;
    sink.finish(descriptor)
}

/// Verify raw extent bytes from a local staging/cache reader using bounded
/// memory. This repeats both the content hash and every page's structural
/// verification before downloaded bytes are published into a cache.
pub fn verify_page_extent_reader(
    reader: &mut dyn Read,
    descriptor: &PageExtentDescriptor,
    limits: &TieredStorageLimits,
) -> Result<()> {
    descriptor.validate(limits)?;
    let mut sink = PageVerificationSink::new(descriptor)?;
    let mut buffer = vec![0_u8; limits.io_buffer_bytes];
    let mut remaining = descriptor.object.bytes;
    while remaining > 0 {
        let request = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| tier_error("extent verification request does not fit address space"))?;
        let read = reader
            .read(&mut buffer[..request])
            .map_err(|error| PageError::io(PathBuf::from("<extent-reader>"), error))?;
        if read == 0 {
            return Err(tier_error(
                "extent reader ended before its declared byte length",
            ));
        }
        if let Err(write_error) = sink.write_all(&buffer[..read]) {
            if let Some(error) = sink.error.take() {
                return Err(error);
            }
            return Err(PageError::io(
                PathBuf::from("<extent-verifier>"),
                write_error,
            ));
        }
        remaining -= read as u64;
    }
    let mut extra = [0_u8; 1];
    if reader
        .read(&mut extra)
        .map_err(|error| PageError::io(PathBuf::from("<extent-reader>"), error))?
        != 0
    {
        return Err(tier_error(
            "extent reader contains bytes beyond its declared length",
        ));
    }
    sink.finish(descriptor)
}

struct PageVerificationSink {
    page: Vec<u8>,
    filled: usize,
    next_page_id: PageId,
    remaining_pages: u64,
    hasher: Sha256,
    error: Option<PageError>,
}

impl PageVerificationSink {
    fn new(descriptor: &PageExtentDescriptor) -> Result<Self> {
        let page_size = usize::try_from(descriptor.page_size)
            .map_err(|_| tier_error("page size does not fit address space"))?;
        Ok(Self {
            page: vec![0_u8; page_size],
            filled: 0,
            next_page_id: descriptor.first_page,
            remaining_pages: descriptor.page_count,
            hasher: Sha256::new(),
            error: None,
        })
    }

    fn fail(&mut self, error: PageError) -> std::io::Error {
        let message = error.to_string();
        self.error = Some(error);
        std::io::Error::new(std::io::ErrorKind::InvalidData, message)
    }

    fn verify_full_page(&mut self) -> std::io::Result<()> {
        if self.remaining_pages == 0 {
            return Err(self.fail(tier_error(
                "extent provider returned pages beyond its descriptor",
            )));
        }
        if let Err(error) = page::verify(&self.page, self.next_page_id) {
            return Err(self.fail(error));
        }
        self.hasher.update(&self.page);
        self.remaining_pages -= 1;
        self.next_page_id = self
            .next_page_id
            .checked_add(1)
            .ok_or_else(|| self.fail(tier_error("extent page verification overflowed page id")))?;
        self.filled = 0;
        Ok(())
    }

    fn finish(self, descriptor: &PageExtentDescriptor) -> Result<()> {
        if self.filled != 0 || self.remaining_pages != 0 {
            return Err(tier_error(
                "extent provider ended with an incomplete page range",
            ));
        }
        let actual = hex::encode(self.hasher.finalize());
        if actual != descriptor.object.sha256 {
            return Err(tier_error("verified extent sink hash mismatch"));
        }
        Ok(())
    }
}

impl Write for PageVerificationSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let mut offset = 0_usize;
        while offset < bytes.len() {
            let available = self.page.len() - self.filled;
            let count = available.min(bytes.len() - offset);
            self.page[self.filled..self.filled + count]
                .copy_from_slice(&bytes[offset..offset + count]);
            self.filled += count;
            offset += count;
            if self.filled == self.page.len() {
                self.verify_full_page()?;
            }
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn hash_exact(
    mut file: File,
    expected_bytes: u64,
    limits: &TieredStorageLimits,
) -> Result<(u64, String)> {
    let actual_bytes = file
        .metadata()
        .map_err(|error| PageError::io(PathBuf::from("<extent-object>"), error))?
        .len();
    if actual_bytes != expected_bytes || actual_bytes > limits.max_extent_bytes {
        return Err(tier_error(format!(
            "extent object has {actual_bytes} bytes, expected {expected_bytes}"
        )));
    }
    let mut buffer = vec![0_u8; limits.io_buffer_bytes];
    let mut remaining = expected_bytes;
    let mut hasher = Sha256::new();
    while remaining > 0 {
        let request = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| tier_error("extent hash request does not fit address space"))?;
        file.read_exact(&mut buffer[..request])
            .map_err(|error| PageError::io(PathBuf::from("<extent-object>"), error))?;
        hasher.update(&buffer[..request]);
        remaining -= request as u64;
    }
    Ok((actual_bytes, hex::encode(hasher.finalize())))
}

pub(crate) fn validate_sha256(value: &str) -> Result<()> {
    if value.len() != 64
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(tier_error("extent SHA-256 must be 64 lowercase hex bytes"));
    }
    Ok(())
}

fn create_incomplete_file(parent: &Path, sha256: &str) -> Result<(PathBuf, File)> {
    for _ in 0..128 {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(
            ".{sha256}.{}.{}.incomplete",
            std::process::id(),
            sequence
        ));
        let result = {
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .custom_flags(libc::O_NOFOLLOW)
                    .open(&path)
            }
            #[cfg(not(unix))]
            {
                OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .open(&path)
            }
        };
        match result {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(PageError::io(&path, error)),
        }
    }
    Err(tier_error(
        "could not allocate a unique incomplete extent object",
    ))
}

struct IncompleteObjectGuard {
    path: PathBuf,
    published: bool,
}

impl Drop for IncompleteObjectGuard {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn open_regular_no_follow(path: &Path, kind: &str) -> Result<File> {
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .map_err(|error| PageError::io(path, error))?
    };
    #[cfg(not(unix))]
    let file = {
        reject_symlink(path, kind)?;
        OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|error| PageError::io(path, error))?
    };
    if !file
        .metadata()
        .map_err(|error| PageError::io(path, error))?
        .is_file()
    {
        return Err(tier_error(format!(
            "refusing non-regular {kind} {}",
            path.display()
        )));
    }
    Ok(file)
}

fn reject_symlink(path: &Path, kind: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| PageError::io(path, error))?;
    if metadata.file_type().is_symlink() {
        return Err(tier_error(format!(
            "refusing symlink {kind} {}",
            path.display()
        )));
    }
    Ok(())
}

fn reject_symlink_if_exists(path: &Path, kind: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(tier_error(format!(
            "refusing symlink {kind} {}",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(PageError::io(path, error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PageHeader, PageStore, PageStoreOptions, PageType, PAGE_HEADER_BYTES};

    fn page_file(directory: &tempfile::TempDir, pages: usize) -> (PathBuf, Vec<PageId>) {
        let path = directory.path().join("data.pages");
        let store = PageStore::open(&path, PageStoreOptions::default().with_fsync(false)).unwrap();
        let mut ids = Vec::new();
        for index in 0..pages {
            let page_id = store.allocate(PageType::Heap).unwrap();
            let mut bytes = vec![0_u8; store.page_size() as usize];
            let mut header = PageHeader::new(page_id, PageType::Heap, store.page_size());
            header.lsn = 100 + index as u64;
            header.encode(&mut bytes);
            bytes[PAGE_HEADER_BYTES] = index as u8;
            store.write_page(page_id, &mut bytes).unwrap();
            ids.push(page_id);
        }
        store.flush().unwrap();
        (path, ids)
    }

    #[test]
    fn sealed_pages_publish_once_and_verify_without_unbounded_buffers() {
        let directory = tempfile::tempdir().unwrap();
        let object_root = directory.path().join("objects");
        let provider = LocalImmutableExtentStore::open(&object_root, false).unwrap();
        let (page_file, pages) = page_file(&directory, 4);
        let limits = TieredStorageLimits {
            io_buffer_bytes: 4096,
            max_extent_bytes: 1024 * 1024,
        };
        let descriptor = seal_page_extent(
            &provider,
            &page_file,
            pages[0],
            pages.len() as u64,
            crate::DEFAULT_PAGE_SIZE,
            99,
            PageExtentTier::Cold,
            &limits,
        )
        .unwrap();
        descriptor.validate(&limits).unwrap();
        verify_page_extent(&provider, &descriptor, &limits).unwrap();
        assert_eq!(descriptor.first_page, 1);
        assert_eq!(descriptor.end_page().unwrap(), 5);
        assert!(descriptor.contains(4));
        assert!(!descriptor.contains(5));

        let duplicate = seal_page_extent(
            &provider,
            &page_file,
            pages[0],
            pages.len() as u64,
            crate::DEFAULT_PAGE_SIZE,
            99,
            PageExtentTier::Cold,
            &limits,
        )
        .unwrap();
        assert_eq!(duplicate.object, descriptor.object);
    }

    #[test]
    fn corruption_and_failed_uploads_never_publish_or_leave_incomplete_files() {
        struct FailingReader {
            emitted: usize,
        }
        impl Read for FailingReader {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                if self.emitted > 0 {
                    return Err(std::io::Error::other("injected read failure"));
                }
                let count = buffer.len().min(1024);
                buffer[..count].fill(7);
                self.emitted += count;
                Ok(count)
            }
        }

        let directory = tempfile::tempdir().unwrap();
        let provider =
            LocalImmutableExtentStore::open(directory.path().join("objects"), false).unwrap();
        let limits = TieredStorageLimits {
            io_buffer_bytes: 4096,
            max_extent_bytes: 1024 * 1024,
        };
        let key = ExtentObjectKey::from_sha256("a".repeat(64)).unwrap();
        assert!(provider
            .put_verified(
                &key,
                1024 * 1024,
                &mut FailingReader { emitted: 0 },
                &limits
            )
            .is_err());
        let prefix = provider.root().join(OBJECT_NAMESPACE).join("aa");
        assert!(fs::read_dir(&prefix).unwrap().next().is_none());

        let (page_file, pages) = page_file(&directory, 1);
        let descriptor = seal_page_extent(
            &provider,
            &page_file,
            pages[0],
            1,
            crate::DEFAULT_PAGE_SIZE,
            1,
            PageExtentTier::Warm,
            &limits,
        )
        .unwrap();
        let object_path = provider.root().join(descriptor.object.key.as_str());
        let file = OpenOptions::new().write(true).open(&object_path).unwrap();
        crate::pio::write_at(&file, &[0xff], 100).unwrap();
        assert!(verify_page_extent(&provider, &descriptor, &limits).is_err());
        assert!(provider
            .put_verified(
                &descriptor.object.key,
                descriptor.object.bytes,
                &mut File::open(&page_file)
                    .unwrap()
                    .take(descriptor.object.bytes),
                &limits,
            )
            .is_err());
    }

    #[test]
    fn descriptor_and_provider_bounds_fail_closed() {
        let limits = TieredStorageLimits::default();
        assert!(ExtentObjectKey::from_sha256("../escape").is_err());
        let malformed: ExtentObjectKey = serde_json::from_str("\"x\"").unwrap();
        assert!(malformed.sha256().is_err());
        let mut descriptor = PageExtentDescriptor {
            format_version: PAGE_EXTENT_FORMAT_VERSION,
            first_page: 1,
            page_count: 1,
            page_size: crate::DEFAULT_PAGE_SIZE,
            checkpoint_lsn: 1,
            tier: PageExtentTier::Warm,
            object: ExtentObjectMetadata {
                key: ExtentObjectKey::from_sha256("b".repeat(64)).unwrap(),
                bytes: crate::DEFAULT_PAGE_SIZE as u64,
                sha256: "b".repeat(64),
            },
        };
        descriptor.validate(&limits).unwrap();
        descriptor.first_page = 0;
        assert!(descriptor.validate(&limits).is_err());
        descriptor.first_page = 1;
        descriptor.object.bytes += 1;
        assert!(descriptor.validate(&limits).is_err());

        let directory = tempfile::tempdir().unwrap();
        let provider =
            LocalImmutableExtentStore::open(directory.path().join("objects"), false).unwrap();
        let invalid_page = vec![0_u8; crate::DEFAULT_PAGE_SIZE as usize];
        let sha256 = hex::encode(Sha256::digest(&invalid_page));
        let key = ExtentObjectKey::from_sha256(sha256.clone()).unwrap();
        let metadata = provider
            .put_verified(
                &key,
                invalid_page.len() as u64,
                &mut invalid_page.as_slice(),
                &limits,
            )
            .unwrap();
        let invalid_descriptor = PageExtentDescriptor {
            format_version: PAGE_EXTENT_FORMAT_VERSION,
            first_page: 1,
            page_count: 1,
            page_size: crate::DEFAULT_PAGE_SIZE,
            checkpoint_lsn: 1,
            tier: PageExtentTier::Cold,
            object: ExtentObjectMetadata {
                key,
                bytes: metadata.bytes,
                sha256,
            },
        };
        assert!(verify_page_extent(&provider, &invalid_descriptor, &limits).is_err());
    }
}
