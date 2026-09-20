//! Atomic publication catalog for immutable tiered page generations.
//!
//! Generation manifests and activation markers are append-only. Publication
//! never replaces the active record: the highest valid activation marker wins.
//! Therefore a crash before the marker leaves the previous generation active,
//! while a crash after its atomic no-overwrite publication exposes the complete
//! new generation. Old generations remain available for rollback and GC.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{PageError, Result};
use crate::lock::DirectoryLock;
use crate::tiered::{
    validate_sha256, verify_page_extent, ImmutableExtentStore, PageExtentDescriptor,
    TieredStorageLimits,
};

pub const PAGE_GENERATION_FORMAT_VERSION: u32 = 1;
const GENERATIONS_DIRECTORY: &str = "generations";
const ACTIVATIONS_DIRECTORY: &str = "activations";
const MANIFEST_SUFFIX: &str = ".json";
const ACTIVATION_SUFFIX: &str = ".active";
const CONTROL_FILE_MAX_BYTES: u64 = 64 * 1024;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn manifest_error(message: impl Into<String>) -> PageError {
    PageError::TieredStorage {
        reason: format!("page generation: {}", message.into()),
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TieredManifestLimits {
    pub max_manifest_bytes: u64,
    pub max_extents: usize,
    pub max_activation_markers: usize,
    pub max_incomplete_cleanup: usize,
}

impl Default for TieredManifestLimits {
    fn default() -> Self {
        Self {
            max_manifest_bytes: 256 * 1024 * 1024,
            max_extents: 1_000_000,
            max_activation_markers: 10_000,
            max_incomplete_cleanup: 1_024,
        }
    }
}

impl TieredManifestLimits {
    pub fn validate(&self) -> Result<()> {
        if !(1024..=256 * 1024 * 1024).contains(&self.max_manifest_bytes)
            || !(1..=1_000_000).contains(&self.max_extents)
            || !(1..=1_000_000).contains(&self.max_activation_markers)
            || !(1..=100_000).contains(&self.max_incomplete_cleanup)
        {
            return Err(manifest_error(
                "manifest, extent, activation, or cleanup limits are outside supported bounds",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PageGenerationManifest {
    pub format_version: u32,
    pub database_id: String,
    pub generation: u64,
    pub page_size: u32,
    pub checkpoint_lsn: u64,
    pub sealed_page_count: u64,
    pub created_at_ms: u64,
    pub previous_manifest_sha256: Option<String>,
    pub extents: Vec<PageExtentDescriptor>,
    pub checksum_sha256: String,
}

impl PageGenerationManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        database_id: impl Into<String>,
        generation: u64,
        page_size: u32,
        checkpoint_lsn: u64,
        created_at_ms: u64,
        previous_manifest_sha256: Option<String>,
        extents: Vec<PageExtentDescriptor>,
        manifest_limits: &TieredManifestLimits,
        storage_limits: &TieredStorageLimits,
    ) -> Result<Self> {
        let sealed_page_count = extents.iter().try_fold(0_u64, |count, extent| {
            count
                .checked_add(extent.page_count)
                .ok_or_else(|| manifest_error("sealed page count overflow"))
        })?;
        let mut manifest = Self {
            format_version: PAGE_GENERATION_FORMAT_VERSION,
            database_id: database_id.into(),
            generation,
            page_size,
            checkpoint_lsn,
            sealed_page_count,
            created_at_ms,
            previous_manifest_sha256,
            extents,
            checksum_sha256: String::new(),
        };
        manifest.checksum_sha256 = manifest.calculate_checksum()?;
        manifest.validate(manifest_limits, storage_limits)?;
        Ok(manifest)
    }

    pub fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            database_id: &'a str,
            generation: u64,
            page_size: u32,
            checkpoint_lsn: u64,
            sealed_page_count: u64,
            created_at_ms: u64,
            previous_manifest_sha256: &'a Option<String>,
            extents: &'a [PageExtentDescriptor],
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            database_id: &self.database_id,
            generation: self.generation,
            page_size: self.page_size,
            checkpoint_lsn: self.checkpoint_lsn,
            sealed_page_count: self.sealed_page_count,
            created_at_ms: self.created_at_ms,
            previous_manifest_sha256: &self.previous_manifest_sha256,
            extents: &self.extents,
        })
    }

    pub fn validate(
        &self,
        manifest_limits: &TieredManifestLimits,
        storage_limits: &TieredStorageLimits,
    ) -> Result<()> {
        manifest_limits.validate()?;
        storage_limits.validate()?;
        crate::page::validate_page_size(self.page_size)?;
        if self.format_version != PAGE_GENERATION_FORMAT_VERSION
            || self.generation == 0
            || self.database_id.is_empty()
            || self.database_id.len() > 256
            || self.database_id.chars().any(char::is_control)
            || self.created_at_ms == 0
            || self.extents.len() > manifest_limits.max_extents
        {
            return Err(manifest_error(
                "manifest identity, timestamp, or extent count is invalid",
            ));
        }
        if let Some(previous) = &self.previous_manifest_sha256 {
            validate_sha256(previous)?;
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(manifest_error("manifest checksum mismatch"));
        }

        let mut expected_first_page = 1_u64;
        let mut observed_page_count = 0_u64;
        for extent in &self.extents {
            extent.validate(storage_limits)?;
            if extent.first_page != expected_first_page
                || extent.page_size != self.page_size
                || extent.checkpoint_lsn != self.checkpoint_lsn
            {
                return Err(manifest_error(
                    "extents must be ordered, contiguous, and from one checkpoint",
                ));
            }
            observed_page_count = observed_page_count
                .checked_add(extent.page_count)
                .ok_or_else(|| manifest_error("sealed page count overflow"))?;
            expected_first_page = extent.end_page()?;
        }
        if observed_page_count != self.sealed_page_count
            || (self.sealed_page_count == 0) != self.extents.is_empty()
        {
            return Err(manifest_error("sealed page count does not match extents"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivePageGeneration {
    pub manifest: PageGenerationManifest,
    /// Hash of the complete serialized manifest, distinct from the manifest's
    /// internal payload checksum and used to bind the generation chain.
    pub manifest_sha256: String,
}

impl ActivePageGeneration {
    /// Revalidate a detached active-generation snapshot before it is used for
    /// reads. Public fields make construction convenient for transport, so a
    /// consumer must not assume the manifest/hash binding was catalog-loaded.
    pub fn validate(
        &self,
        manifest_limits: &TieredManifestLimits,
        storage_limits: &TieredStorageLimits,
    ) -> Result<()> {
        self.manifest.validate(manifest_limits, storage_limits)?;
        validate_sha256(&self.manifest_sha256)?;
        let bytes = serde_json::to_vec(&self.manifest)
            .map_err(|error| manifest_error(format!("cannot encode active manifest: {error}")))?;
        if bytes.len() as u64 > manifest_limits.max_manifest_bytes
            || sha256_bytes(&bytes) != self.manifest_sha256
        {
            return Err(manifest_error(
                "active generation does not match its manifest content hash",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ActivationMarker {
    format_version: u32,
    generation: u64,
    manifest_sha256: String,
    checksum_sha256: String,
}

impl ActivationMarker {
    fn create(generation: u64, manifest_sha256: String) -> Result<Self> {
        let mut marker = Self {
            format_version: PAGE_GENERATION_FORMAT_VERSION,
            generation,
            manifest_sha256,
            checksum_sha256: String::new(),
        };
        marker.checksum_sha256 = marker.calculate_checksum()?;
        marker.validate()?;
        Ok(marker)
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            generation: u64,
            manifest_sha256: &'a str,
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            generation: self.generation,
            manifest_sha256: &self.manifest_sha256,
        })
    }

    fn validate(&self) -> Result<()> {
        validate_sha256(&self.manifest_sha256)?;
        validate_sha256(&self.checksum_sha256)?;
        if self.format_version != PAGE_GENERATION_FORMAT_VERSION
            || self.generation == 0
            || self.calculate_checksum()? != self.checksum_sha256
        {
            return Err(manifest_error("activation marker is invalid"));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct TieredManifestCatalog {
    root: PathBuf,
    generations: PathBuf,
    activations: PathBuf,
    fsync: bool,
}

impl TieredManifestCatalog {
    pub fn open(root: impl AsRef<Path>, fsync: bool) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        create_safe_directory(&root, "generation catalog")?;
        let generations = root.join(GENERATIONS_DIRECTORY);
        let activations = root.join(ACTIVATIONS_DIRECTORY);
        create_safe_directory(&generations, "generation manifests")?;
        create_safe_directory(&activations, "generation activations")?;
        Ok(Self {
            root,
            generations,
            activations,
            fsync,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn load_active(
        &self,
        manifest_limits: &TieredManifestLimits,
        storage_limits: &TieredStorageLimits,
    ) -> Result<Option<ActivePageGeneration>> {
        manifest_limits.validate()?;
        storage_limits.validate()?;
        let markers = self.activation_markers(manifest_limits)?;
        let Some((generation, (marker_path, filename_hash))) = markers.into_iter().next_back()
        else {
            return Ok(None);
        };
        let marker_bytes = read_bounded(&marker_path, CONTROL_FILE_MAX_BYTES)?;
        let marker: ActivationMarker = serde_json::from_slice(&marker_bytes)
            .map_err(|error| manifest_error(format!("invalid activation JSON: {error}")))?;
        marker.validate()?;
        if marker.generation != generation || marker.manifest_sha256 != filename_hash {
            return Err(manifest_error(
                "activation filename disagrees with its authenticated contents",
            ));
        }
        self.load_generation(&marker, manifest_limits, storage_limits)
            .map(Some)
    }

    /// Load the active rollback window from newest to oldest and verify every
    /// adjacent generation/hash link. Callers performing destructive provider
    /// maintenance hold the catalog lock while using this view.
    pub fn retained_generations(
        &self,
        retain: usize,
        manifest_limits: &TieredManifestLimits,
        storage_limits: &TieredStorageLimits,
    ) -> Result<Vec<ActivePageGeneration>> {
        if retain == 0 || retain > manifest_limits.max_activation_markers {
            return Err(manifest_error(
                "retained generation count is outside the activation bound",
            ));
        }
        let markers = self.activation_markers(manifest_limits)?;
        let mut retained = Vec::new();
        for (generation, (path, filename_hash)) in markers.into_iter().rev().take(retain) {
            let marker_bytes = read_bounded(&path, CONTROL_FILE_MAX_BYTES)?;
            let marker: ActivationMarker = serde_json::from_slice(&marker_bytes)
                .map_err(|error| manifest_error(format!("invalid activation JSON: {error}")))?;
            marker.validate()?;
            if marker.generation != generation || marker.manifest_sha256 != filename_hash {
                return Err(manifest_error(
                    "activation filename disagrees with its authenticated contents",
                ));
            }
            retained.push(self.load_generation(&marker, manifest_limits, storage_limits)?);
        }
        for adjacent in retained.windows(2) {
            let newer = &adjacent[0];
            let older = &adjacent[1];
            if newer.manifest.generation != older.manifest.generation.saturating_add(1)
                || newer.manifest.previous_manifest_sha256.as_deref()
                    != Some(older.manifest_sha256.as_str())
            {
                return Err(manifest_error(
                    "retained activation history is not a contiguous authenticated chain",
                ));
            }
        }
        Ok(retained)
    }

    pub fn publish(
        &self,
        store: &dyn ImmutableExtentStore,
        manifest: &PageGenerationManifest,
        expected_active_sha256: Option<&str>,
        manifest_limits: &TieredManifestLimits,
        storage_limits: &TieredStorageLimits,
    ) -> Result<ActivePageGeneration> {
        self.publish_inner(
            store,
            manifest,
            expected_active_sha256,
            manifest_limits,
            storage_limits,
            || Ok(()),
        )
    }

    fn publish_inner<F>(
        &self,
        store: &dyn ImmutableExtentStore,
        manifest: &PageGenerationManifest,
        expected_active_sha256: Option<&str>,
        manifest_limits: &TieredManifestLimits,
        storage_limits: &TieredStorageLimits,
        before_activation: F,
    ) -> Result<ActivePageGeneration>
    where
        F: FnOnce() -> Result<()>,
    {
        manifest.validate(manifest_limits, storage_limits)?;
        let manifest_bytes = serde_json::to_vec(manifest)
            .map_err(|error| manifest_error(format!("cannot encode manifest: {error}")))?;
        if manifest_bytes.len() as u64 > manifest_limits.max_manifest_bytes {
            return Err(manifest_error("encoded manifest exceeds its byte limit"));
        }
        let manifest_sha256 = sha256_bytes(&manifest_bytes);
        let _lock = DirectoryLock::acquire(&self.root)?;
        self.cleanup_incomplete(manifest_limits.max_incomplete_cleanup)?;
        let current = self.load_active(manifest_limits, storage_limits)?;

        if let Some(active) = &current {
            if active.manifest_sha256 == manifest_sha256 && active.manifest == *manifest {
                return Ok(active.clone());
            }
        }
        let current_sha = current
            .as_ref()
            .map(|active| active.manifest_sha256.as_str());
        if current_sha != expected_active_sha256 {
            return Err(manifest_error(
                "active generation changed before publication",
            ));
        }
        let expected_generation = match &current {
            Some(active) => active
                .manifest
                .generation
                .checked_add(1)
                .ok_or_else(|| manifest_error("generation counter is exhausted"))?,
            None => 1,
        };
        if manifest.generation != expected_generation
            || manifest.previous_manifest_sha256.as_deref() != current_sha
        {
            return Err(manifest_error(
                "generation number or previous-manifest binding is not the next active generation",
            ));
        }
        if let Some(active) = &current {
            if manifest.database_id != active.manifest.database_id
                || manifest.page_size != active.manifest.page_size
                || manifest.checkpoint_lsn < active.manifest.checkpoint_lsn
                || manifest.created_at_ms < active.manifest.created_at_ms
            {
                return Err(manifest_error(
                    "database identity, page size, checkpoint, or creation time regressed",
                ));
            }
        }

        self.verify_changed_extents(store, current.as_ref(), manifest, storage_limits)?;
        let manifest_path = self
            .generations
            .join(manifest_filename(manifest.generation, &manifest_sha256));
        write_immutable(
            &manifest_path,
            &manifest_bytes,
            self.fsync,
            manifest_limits.max_manifest_bytes,
        )?;
        before_activation()?;

        let marker = ActivationMarker::create(manifest.generation, manifest_sha256.clone())?;
        let marker_bytes = serde_json::to_vec(&marker)
            .map_err(|error| manifest_error(format!("cannot encode activation: {error}")))?;
        let marker_path = self
            .activations
            .join(activation_filename(marker.generation, &manifest_sha256));
        write_immutable(
            &marker_path,
            &marker_bytes,
            self.fsync,
            CONTROL_FILE_MAX_BYTES,
        )?;
        if self.fsync {
            sync_directory(&self.activations)?;
        }
        Ok(ActivePageGeneration {
            manifest: manifest.clone(),
            manifest_sha256,
        })
    }

    fn verify_changed_extents(
        &self,
        store: &dyn ImmutableExtentStore,
        current: Option<&ActivePageGeneration>,
        manifest: &PageGenerationManifest,
        storage_limits: &TieredStorageLimits,
    ) -> Result<()> {
        let previous: BTreeMap<(u64, u64), _> = current
            .into_iter()
            .flat_map(|active| active.manifest.extents.iter())
            .map(|extent| ((extent.first_page, extent.page_count), &extent.object))
            .collect();
        for extent in &manifest.extents {
            let unchanged = previous
                .get(&(extent.first_page, extent.page_count))
                .is_some_and(|object| **object == extent.object);
            if !unchanged {
                verify_page_extent(store, extent, storage_limits)?;
            }
        }
        Ok(())
    }

    fn load_generation(
        &self,
        marker: &ActivationMarker,
        manifest_limits: &TieredManifestLimits,
        storage_limits: &TieredStorageLimits,
    ) -> Result<ActivePageGeneration> {
        let path = self.generations.join(manifest_filename(
            marker.generation,
            &marker.manifest_sha256,
        ));
        let bytes = read_bounded(&path, manifest_limits.max_manifest_bytes)?;
        let actual_sha256 = sha256_bytes(&bytes);
        if actual_sha256 != marker.manifest_sha256 {
            return Err(manifest_error("active manifest content hash mismatch"));
        }
        let manifest: PageGenerationManifest = serde_json::from_slice(&bytes)
            .map_err(|error| manifest_error(format!("invalid manifest JSON: {error}")))?;
        manifest.validate(manifest_limits, storage_limits)?;
        if manifest.generation != marker.generation {
            return Err(manifest_error(
                "activation generation disagrees with its manifest",
            ));
        }
        Ok(ActivePageGeneration {
            manifest,
            manifest_sha256: actual_sha256,
        })
    }

    fn activation_markers(
        &self,
        limits: &TieredManifestLimits,
    ) -> Result<BTreeMap<u64, (PathBuf, String)>> {
        let mut markers = BTreeMap::new();
        let entries = fs::read_dir(&self.activations)
            .map_err(|error| PageError::io(&self.activations, error))?;
        for entry in entries {
            let entry = entry.map_err(|error| PageError::io(&self.activations, error))?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| manifest_error("activation filename is not UTF-8"))?;
            if name.ends_with(".incomplete") {
                continue;
            }
            let (generation, sha256) = parse_generation_filename(&name, ACTIVATION_SUFFIX)?;
            let file_type = entry
                .file_type()
                .map_err(|error| PageError::io(entry.path(), error))?;
            if !file_type.is_file() || file_type.is_symlink() {
                return Err(manifest_error(format!(
                    "refusing non-regular activation marker {}",
                    entry.path().display()
                )));
            }
            if markers.insert(generation, (entry.path(), sha256)).is_some() {
                return Err(manifest_error(
                    "more than one activation exists for a generation",
                ));
            }
            if markers.len() > limits.max_activation_markers {
                return Err(manifest_error("activation history exceeds its bound"));
            }
        }
        Ok(markers)
    }

    fn cleanup_incomplete(&self, max_files: usize) -> Result<usize> {
        let mut removed = 0_usize;
        for directory in [&self.generations, &self.activations] {
            for entry in fs::read_dir(directory).map_err(|error| PageError::io(directory, error))? {
                let entry = entry.map_err(|error| PageError::io(directory, error))?;
                let name = entry.file_name();
                if !name.to_string_lossy().ends_with(".incomplete") {
                    continue;
                }
                if removed == max_files {
                    return Err(manifest_error(
                        "incomplete-file cleanup budget exhausted; retry publication",
                    ));
                }
                let metadata = entry
                    .file_type()
                    .map_err(|error| PageError::io(entry.path(), error))?;
                if !metadata.is_file() || metadata.is_symlink() {
                    return Err(manifest_error(format!(
                        "refusing non-regular incomplete path {}",
                        entry.path().display()
                    )));
                }
                fs::remove_file(entry.path())
                    .map_err(|error| PageError::io(entry.path(), error))?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    pub(crate) fn prune_generations_before(
        &self,
        minimum_generation: u64,
        max_files: usize,
    ) -> Result<usize> {
        if minimum_generation == 0 || max_files == 0 {
            return Err(manifest_error("generation prune bounds are invalid"));
        }
        let mut removed = 0_usize;
        for (directory, suffix) in [
            (&self.activations, ACTIVATION_SUFFIX),
            (&self.generations, MANIFEST_SUFFIX),
        ] {
            let entries =
                fs::read_dir(directory).map_err(|error| PageError::io(directory, error))?;
            for entry in entries {
                let entry = entry.map_err(|error| PageError::io(directory, error))?;
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| manifest_error("generation prune filename is not UTF-8"))?;
                if name.ends_with(".incomplete") {
                    continue;
                }
                let (generation, _) = parse_generation_filename(&name, suffix)?;
                if generation >= minimum_generation {
                    continue;
                }
                if removed == max_files {
                    return Err(manifest_error(
                        "generation metadata prune budget exhausted; retry GC completion",
                    ));
                }
                let file_type = entry
                    .file_type()
                    .map_err(|error| PageError::io(entry.path(), error))?;
                if !file_type.is_file() || file_type.is_symlink() {
                    return Err(manifest_error(format!(
                        "refusing non-regular generation metadata {}",
                        entry.path().display()
                    )));
                }
                fs::remove_file(entry.path())
                    .map_err(|error| PageError::io(entry.path(), error))?;
                removed += 1;
            }
            if self.fsync && removed != 0 {
                sync_directory(directory)?;
            }
        }
        Ok(removed)
    }
}

fn manifest_filename(generation: u64, sha256: &str) -> String {
    format!("generation-{generation:020}-{sha256}{MANIFEST_SUFFIX}")
}

fn activation_filename(generation: u64, sha256: &str) -> String {
    format!("generation-{generation:020}-{sha256}{ACTIVATION_SUFFIX}")
}

fn parse_generation_filename(name: &str, suffix: &str) -> Result<(u64, String)> {
    let body = name
        .strip_prefix("generation-")
        .and_then(|value| value.strip_suffix(suffix))
        .ok_or_else(|| manifest_error(format!("unexpected catalog entry {name}")))?;
    let (generation, sha256) = body
        .split_once('-')
        .ok_or_else(|| manifest_error("generation filename has an invalid layout"))?;
    if generation.len() != 20 || !generation.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(manifest_error("generation filename is not canonical"));
    }
    let generation = generation
        .parse::<u64>()
        .map_err(|_| manifest_error("generation filename overflows u64"))?;
    validate_sha256(sha256)?;
    Ok((generation, sha256.to_string()))
}

fn create_safe_directory(path: &Path, kind: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(manifest_error(format!(
                "refusing non-directory {kind} {}",
                path.display()
            )));
        }
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(PageError::io(path, error)),
    }
    fs::create_dir_all(path).map_err(|error| PageError::io(path, error))?;
    let metadata = fs::symlink_metadata(path).map_err(|error| PageError::io(path, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(manifest_error(format!(
            "refusing non-directory {kind} {}",
            path.display()
        )));
    }
    Ok(())
}

fn read_bounded(path: &Path, max_bytes: u64) -> Result<Vec<u8>> {
    let file = open_regular_no_follow(path)?;
    let bytes = file
        .metadata()
        .map_err(|error| PageError::io(path, error))?
        .len();
    if bytes > max_bytes || bytes > usize::MAX as u64 {
        return Err(manifest_error(format!(
            "control file {} exceeds its byte limit",
            path.display()
        )));
    }
    let mut output = Vec::with_capacity(bytes as usize);
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut output)
        .map_err(|error| PageError::io(path, error))?;
    if output.len() as u64 != bytes {
        return Err(manifest_error("control file changed while being read"));
    }
    Ok(output)
}

fn write_immutable(path: &Path, bytes: &[u8], fsync: bool, max_bytes: u64) -> Result<()> {
    if bytes.is_empty() || bytes.len() as u64 > max_bytes {
        return Err(manifest_error("immutable control object exceeds its bound"));
    }
    if let Ok(existing) = read_bounded(path, max_bytes) {
        if existing == bytes {
            return Ok(());
        }
        return Err(manifest_error(format!(
            "immutable control object {} already differs",
            path.display()
        )));
    } else if path.exists() {
        return Err(manifest_error(format!(
            "existing immutable control object {} is unreadable",
            path.display()
        )));
    }
    let parent = path
        .parent()
        .ok_or_else(|| manifest_error("immutable control object has no parent"))?;
    let (temporary_path, mut temporary) = create_incomplete_file(parent)?;
    let mut guard = IncompleteGuard(temporary_path.clone());
    temporary
        .write_all(bytes)
        .map_err(|error| PageError::io(&temporary_path, error))?;
    if fsync {
        temporary
            .sync_data()
            .map_err(|error| PageError::io(&temporary_path, error))?;
    }
    drop(temporary);
    match fs::hard_link(&temporary_path, path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = read_bounded(path, max_bytes)?;
            if existing != bytes {
                return Err(manifest_error(format!(
                    "concurrent immutable publication conflicts at {}",
                    path.display()
                )));
            }
        }
        Err(error) => return Err(PageError::io(path, error)),
    }
    fs::remove_file(&temporary_path).map_err(|error| PageError::io(&temporary_path, error))?;
    guard.0.clear();
    if fsync {
        sync_directory(parent)?;
    }
    Ok(())
}

fn create_incomplete_file(parent: &Path) -> Result<(PathBuf, File)> {
    for _ in 0..128 {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(
            ".generation.{}.{}.incomplete",
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
    Err(manifest_error(
        "could not allocate an incomplete control object",
    ))
}

struct IncompleteGuard(PathBuf);

impl Drop for IncompleteGuard {
    fn drop(&mut self) {
        if !self.0.as_os_str().is_empty() {
            let _ = fs::remove_file(&self.0);
        }
    }
}

fn open_regular_no_follow(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|error| PageError::io(path, error))?;
    if !file
        .metadata()
        .map_err(|error| PageError::io(path, error))?
        .is_file()
    {
        return Err(manifest_error(format!(
            "refusing non-regular control object {}",
            path.display()
        )));
    }
    Ok(file)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| PageError::io(path, error))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn sha256_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn sha256_json(value: &impl Serialize) -> Result<String> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| manifest_error(format!("cannot encode checksum payload: {error}")))?;
    Ok(sha256_bytes(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        seal_page_extent, LocalImmutableExtentStore, PageExtentTier, PageHeader, PageStore,
        PageStoreOptions, PageType, PAGE_HEADER_BYTES,
    };

    fn sealed_extent(
        directory: &tempfile::TempDir,
        provider: &LocalImmutableExtentStore,
        first: u64,
        count: u64,
        checkpoint_lsn: u64,
        limits: &TieredStorageLimits,
    ) -> PageExtentDescriptor {
        let page_path = directory.path().join(format!("pages-{first}-{count}"));
        let store =
            PageStore::open(&page_path, PageStoreOptions::default().with_fsync(false)).unwrap();
        let needed = first + count - 1;
        for index in 0..needed {
            let page_id = store.allocate(PageType::Heap).unwrap();
            let mut bytes = vec![0_u8; store.page_size() as usize];
            let mut header = PageHeader::new(page_id, PageType::Heap, store.page_size());
            header.lsn = checkpoint_lsn;
            header.encode(&mut bytes);
            bytes[PAGE_HEADER_BYTES] = index as u8;
            store.write_page(page_id, &mut bytes).unwrap();
        }
        store.flush().unwrap();
        seal_page_extent(
            provider,
            page_path,
            first,
            count,
            crate::DEFAULT_PAGE_SIZE,
            checkpoint_lsn,
            PageExtentTier::Cold,
            limits,
        )
        .unwrap()
    }

    fn manifest(
        generation: u64,
        previous: Option<String>,
        extents: Vec<PageExtentDescriptor>,
        manifest_limits: &TieredManifestLimits,
        storage_limits: &TieredStorageLimits,
    ) -> PageGenerationManifest {
        PageGenerationManifest::create(
            "database-a",
            generation,
            crate::DEFAULT_PAGE_SIZE,
            77,
            generation,
            previous,
            extents,
            manifest_limits,
            storage_limits,
        )
        .unwrap()
    }

    #[test]
    fn publication_is_atomic_resumable_and_keeps_the_previous_generation() {
        let directory = tempfile::tempdir().unwrap();
        let provider =
            LocalImmutableExtentStore::open(directory.path().join("objects"), false).unwrap();
        let catalog = TieredManifestCatalog::open(directory.path().join("catalog"), false).unwrap();
        let storage_limits = TieredStorageLimits {
            io_buffer_bytes: 4096,
            max_extent_bytes: 1024 * 1024,
        };
        let manifest_limits = TieredManifestLimits::default();
        let first = sealed_extent(&directory, &provider, 1, 1, 77, &storage_limits);
        let generation_one = manifest(
            1,
            None,
            vec![first.clone()],
            &manifest_limits,
            &storage_limits,
        );
        let active_one = catalog
            .publish(
                &provider,
                &generation_one,
                None,
                &manifest_limits,
                &storage_limits,
            )
            .unwrap();
        assert_eq!(
            catalog
                .load_active(&manifest_limits, &storage_limits)
                .unwrap()
                .unwrap(),
            active_one
        );

        let second = sealed_extent(&directory, &provider, 2, 1, 77, &storage_limits);
        let generation_two = manifest(
            2,
            Some(active_one.manifest_sha256.clone()),
            vec![first, second],
            &manifest_limits,
            &storage_limits,
        );
        let interrupted = catalog.publish_inner(
            &provider,
            &generation_two,
            Some(&active_one.manifest_sha256),
            &manifest_limits,
            &storage_limits,
            || Err(manifest_error("injected crash before activation")),
        );
        assert!(interrupted.is_err());
        assert_eq!(
            catalog
                .load_active(&manifest_limits, &storage_limits)
                .unwrap()
                .unwrap(),
            active_one
        );
        fs::write(catalog.generations.join(".stale.incomplete"), b"partial").unwrap();
        fs::write(catalog.activations.join(".stale.incomplete"), b"partial").unwrap();
        let active_two = catalog
            .publish(
                &provider,
                &generation_two,
                Some(&active_one.manifest_sha256),
                &manifest_limits,
                &storage_limits,
            )
            .unwrap();
        assert_eq!(active_two.manifest.generation, 2);
        assert_eq!(
            catalog
                .publish(
                    &provider,
                    &generation_two,
                    Some(&active_one.manifest_sha256),
                    &manifest_limits,
                    &storage_limits,
                )
                .unwrap(),
            active_two
        );
        assert!(walk(&catalog.root).iter().all(|path| !path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with(".incomplete")));
    }

    #[test]
    fn stale_publishers_gaps_and_invalid_extent_layouts_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let provider =
            LocalImmutableExtentStore::open(directory.path().join("objects"), false).unwrap();
        let catalog = TieredManifestCatalog::open(directory.path().join("catalog"), false).unwrap();
        let storage_limits = TieredStorageLimits {
            io_buffer_bytes: 4096,
            max_extent_bytes: 1024 * 1024,
        };
        let manifest_limits = TieredManifestLimits::default();
        let extent = sealed_extent(&directory, &provider, 1, 1, 77, &storage_limits);
        let first = manifest(
            1,
            None,
            vec![extent.clone()],
            &manifest_limits,
            &storage_limits,
        );
        let active = catalog
            .publish(&provider, &first, None, &manifest_limits, &storage_limits)
            .unwrap();
        let gap = manifest(
            3,
            Some(active.manifest_sha256.clone()),
            vec![extent.clone()],
            &manifest_limits,
            &storage_limits,
        );
        assert!(catalog
            .publish(
                &provider,
                &gap,
                Some(&active.manifest_sha256),
                &manifest_limits,
                &storage_limits,
            )
            .is_err());
        let next = manifest(
            2,
            Some(active.manifest_sha256.clone()),
            vec![extent.clone()],
            &manifest_limits,
            &storage_limits,
        );
        assert!(catalog
            .publish(
                &provider,
                &next,
                Some(&"0".repeat(64)),
                &manifest_limits,
                &storage_limits,
            )
            .is_err());

        let different_database = PageGenerationManifest::create(
            "database-b",
            2,
            crate::DEFAULT_PAGE_SIZE,
            77,
            2,
            Some(active.manifest_sha256.clone()),
            vec![extent.clone()],
            &manifest_limits,
            &storage_limits,
        )
        .unwrap();
        assert!(catalog
            .publish(
                &provider,
                &different_database,
                Some(&active.manifest_sha256),
                &manifest_limits,
                &storage_limits,
            )
            .is_err());

        let mut invalid = extent;
        invalid.first_page = 2;
        assert!(PageGenerationManifest::create(
            "database-a",
            2,
            crate::DEFAULT_PAGE_SIZE,
            77,
            2,
            Some(active.manifest_sha256),
            vec![invalid],
            &manifest_limits,
            &storage_limits,
        )
        .is_err());
    }

    #[test]
    fn marker_tampering_and_history_overflow_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let catalog = TieredManifestCatalog::open(directory.path().join("catalog"), false).unwrap();
        let limits = TieredManifestLimits {
            max_activation_markers: 1,
            ..TieredManifestLimits::default()
        };
        let hash = "a".repeat(64);
        let first = catalog.activations.join(activation_filename(1, &hash));
        fs::write(&first, b"{}").unwrap();
        assert!(catalog
            .load_active(&limits, &TieredStorageLimits::default())
            .is_err());
        let second = catalog.activations.join(activation_filename(2, &hash));
        fs::write(second, b"{}").unwrap();
        assert!(catalog
            .load_active(&limits, &TieredStorageLimits::default())
            .is_err());
    }

    fn walk(root: &Path) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        for entry in fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                paths.extend(walk(&path));
            } else {
                paths.push(path);
            }
        }
        paths
    }
}
