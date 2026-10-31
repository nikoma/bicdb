//! Write-ahead extent generations. Uncertain tracking starts a new identity;
//! it can force a full copy but can never prove that an extent is unchanged.
use crate::error::{PageError, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

const MAGIC: &[u8; 8] = b"BICEXT01";
const MAX_BYTES: u64 = 64 * 1024 * 1024;
const MAX_EXTENTS: usize = 65_536;
const SEAL: u64 = u64::MAX;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtentGenerations {
    pub identity: String,
    pub epoch: u64,
    pub extent_pages: u64,
    pub page_size: u32,
    pub versions: Vec<u64>,
}

#[derive(Debug)]
pub(crate) struct ExtentTracker {
    path: PathBuf,
    file: File,
    identity: String,
    epoch: u64,
    versions: BTreeMap<u64, u64>,
    bytes: u64,
    failed: bool,
}

fn err(reason: impl Into<String>) -> PageError {
    PageError::BackupSnapshot {
        reason: reason.into(),
    }
}
fn tracker_path(base: &Path) -> PathBuf {
    base.parent().unwrap_or(Path::new(".")).join(format!(
        ".backup-extents-{}",
        base.file_name().unwrap().to_string_lossy()
    ))
}
fn options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW).mode(0o600);
    }
    options
}
impl ExtentTracker {
    /// A missing, truncated or corrupt journal has no reusable identity. It is
    /// replaced durably before the store can perform recovery or new writes.
    pub(crate) fn load(base: &Path) -> Result<Option<Self>> {
        let path = tracker_path(base);
        let file = match options().open(&path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Self::new(base).map(Some),
            Err(e) => return Err(PageError::io(&path, e)),
        };
        let mut bytes = Vec::new();
        (&file)
            .take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| PageError::io(&path, e))?;
        if bytes.len() < 24
            || bytes.len() as u64 > MAX_BYTES
            || &bytes[..8] != MAGIC
            || (bytes.len() - 24) % 20 != 0
        {
            return Self::reset_uncertain(base, file);
        }
        let mut versions = BTreeMap::new();
        let mut epoch: u64 = 1;
        for record in bytes[24..].chunks_exact(20) {
            if crc32fast::hash(&record[..16])
                != u32::from_le_bytes(record[16..].try_into().unwrap())
            {
                return Self::reset_uncertain(base, file);
            }
            let generation = u64::from_le_bytes(record[..8].try_into().unwrap());
            let index = u64::from_le_bytes(record[8..16].try_into().unwrap());
            if index == SEAL {
                if Some(generation) != epoch.checked_add(1) {
                    return Self::reset_uncertain(base, file);
                }
                epoch = generation;
            } else {
                if generation != epoch
                    || (versions.len() >= MAX_EXTENTS && !versions.contains_key(&index))
                {
                    return Self::reset_uncertain(base, file);
                }
                versions.insert(index, generation);
            }
        }
        Ok(Some(Self {
            path,
            file,
            identity: hex::encode(&bytes[8..24]),
            epoch,
            versions,
            bytes: bytes.len() as u64,
            failed: false,
        }))
    }

    fn reset_uncertain(base: &Path, file: File) -> Result<Option<Self>> {
        // Before permitting recovery/foreground writes, make the old identity
        // durably unreachable. Merely disabling tracking in memory lets a
        // second crash resurrect an old valid prefix after untracked writes.
        drop(file);
        Self::new(base).map(Some)
    }

    pub(crate) fn new(base: &Path) -> Result<Self> {
        let path = tracker_path(base);
        let identity = uuid::Uuid::new_v4();
        let pending = path.with_extension(format!("backup-extents-{}.tmp", identity));
        let result = (|| {
            let mut file = options()
                .create_new(true)
                .open(&pending)
                .map_err(|e| PageError::io(&pending, e))?;
            file.write_all(MAGIC)
                .and_then(|()| file.write_all(identity.as_bytes()))
                .and_then(|()| file.sync_all())
                .map_err(|e| PageError::io(&pending, e))?;
            std::fs::rename(&pending, &path).map_err(|e| PageError::io(&path, e))?;
            #[cfg(unix)]
            File::open(path.parent().unwrap())
                .and_then(|f| f.sync_all())
                .map_err(|e| PageError::io(&path, e))?;
            Ok(Self {
                path: path.clone(),
                file,
                identity: hex::encode(identity.as_bytes()),
                epoch: 1,
                versions: BTreeMap::new(),
                bytes: 24,
                failed: false,
            })
        })();
        let _ = std::fs::remove_file(pending);
        result
    }

    fn append(&mut self, epoch: u64, index: u64) -> Result<()> {
        if self.failed {
            return Err(err(
                "extent tracking is unavailable after an I/O failure; reopen the database",
            ));
        }
        let mut bytes = Vec::with_capacity(20);
        bytes.extend_from_slice(&epoch.to_le_bytes());
        bytes.extend_from_slice(&index.to_le_bytes());
        bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
        if let Err(e) = self
            .file
            .write_all(&bytes)
            .and_then(|()| self.file.sync_data())
        {
            self.failed = true;
            return Err(PageError::io(&self.path, e));
        }
        self.bytes += 20;
        Ok(())
    }

    pub(crate) fn mark(&mut self, base: &Path, extent: u64) -> Result<()> {
        if self.failed {
            return Err(err("extent tracking failed; reopen the database"));
        }
        if self.versions.get(&extent) == Some(&self.epoch) {
            return Ok(());
        }
        if self.bytes + 40 > MAX_BYTES
            || self.epoch == u64::MAX
            || self.versions.len() >= MAX_EXTENTS
        {
            *self = Self::new(base)?;
        }
        self.append(self.epoch, extent)?;
        self.versions.insert(extent, self.epoch);
        Ok(())
    }

    pub(crate) fn seal(
        &mut self,
        base: &Path,
        count: usize,
        extent_pages: u64,
        page_size: u32,
    ) -> Result<ExtentGenerations> {
        if self.bytes + 40 > MAX_BYTES
            || self.epoch == u64::MAX
            || self.versions.len() >= MAX_EXTENTS
        {
            *self = Self::new(base)?;
        }
        let cut = ExtentGenerations {
            identity: self.identity.clone(),
            epoch: self.epoch,
            extent_pages,
            page_size,
            versions: (0..count as u64)
                .map(|i| self.versions.get(&i).copied().unwrap_or(0))
                .collect(),
        };
        self.append(self.epoch + 1, SEAL)?;
        self.epoch += 1;
        Ok(cut)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn corrupt_or_missing_tracking_is_durably_replaced_before_writes() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().join("store.pages");
        let mut tracker = ExtentTracker::new(&base).unwrap();
        tracker.mark(&base, 0).unwrap();
        let old = tracker.identity.clone();
        let path = tracker.path.clone();
        drop(tracker);
        let mut bytes = std::fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1; // Valid length, invalid record checksum.
        std::fs::write(&path, bytes).unwrap();
        let reset = ExtentTracker::load(&base).unwrap().unwrap();
        assert_ne!(reset.identity, old);
        let new = reset.identity.clone();
        drop(reset);
        assert_eq!(ExtentTracker::load(&base).unwrap().unwrap().identity, new);
        std::fs::remove_file(path).unwrap();
        let missing = ExtentTracker::load(&base).unwrap().unwrap();
        assert_ne!(missing.identity, new);
        let fresh = missing.identity.clone();
        drop(missing);
        assert_eq!(ExtentTracker::load(&base).unwrap().unwrap().identity, fresh);
    }

    #[test]
    fn tracking_io_failure_never_acknowledges_a_dirty_extent_and_remains_sticky() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().join("store.pages");
        let mut tracker = ExtentTracker::new(&base).unwrap();
        let cut = tracker.seal(&base, 2, 256, 4096).unwrap();
        tracker.file = File::open(&tracker.path).unwrap(); // deterministic write failure
        assert!(tracker.mark(&base, 1).is_err());
        assert!(!tracker.versions.contains_key(&1));
        tracker.file = options().open(&tracker.path).unwrap();
        assert!(tracker.mark(&base, 1).is_err());
        drop(tracker);
        let recovered = ExtentTracker::load(&base).unwrap().unwrap();
        assert_eq!(recovered.identity, cut.identity);
        assert!(!recovered.versions.contains_key(&1));
    }
    #[test]
    fn rollover_changes_identity_before_forgetting_any_extent() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().join("store.pages");
        let mut tracker = ExtentTracker::new(&base).unwrap();
        tracker.mark(&base, 0).unwrap();
        let old = tracker.seal(&base, 2, 256, 4096).unwrap();
        tracker.bytes = MAX_BYTES; // exercise the bounded journal rollover
        tracker.mark(&base, 1).unwrap();
        let new = tracker.seal(&base, 2, 256, 4096).unwrap();
        assert_ne!(old.identity, new.identity);
        assert!(new.versions[1] > 0);
        drop(tracker);
        assert_eq!(
            ExtentTracker::load(&base).unwrap().unwrap().identity,
            new.identity
        );
    }
}
