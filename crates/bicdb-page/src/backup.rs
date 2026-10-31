//! A physical page-file snapshot whose copy does not hold the writer gate.
//!
//! The manager preserves an uncopied page's first before-image before changing
//! its on-disk bytes. Readers and physical writers cross the same small mutex;
//! neither holds it across an archive upload. WAL and transaction coordination
//! belong to the caller. This snapshot alone is not a database backup.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::error::{PageError, Result};
use crate::page::PageId;

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PageBackupLimits {
    pub max_before_image_bytes: u64,
    pub max_before_image_pages: usize,
    pub max_coordination_millis: u64,
    pub max_segments: usize,
}

impl Default for PageBackupLimits {
    fn default() -> Self {
        Self {
            max_before_image_bytes: 1024 * 1024 * 1024,
            max_before_image_pages: 262_144,
            max_coordination_millis: 1_000,
            max_segments: 65_536,
        }
    }
}

impl PageBackupLimits {
    pub fn validate(self, page_size: u32) -> Result<()> {
        if self.max_before_image_bytes < u64::from(page_size)
            || self.max_before_image_bytes > 1024 * 1024 * 1024 * 1024
            || self.max_before_image_pages == 0
            || self.max_before_image_pages > 1_000_000
            || !(1..=60_000).contains(&self.max_coordination_millis)
            || !(1..=1_000_000).contains(&self.max_segments)
        {
            return Err(snapshot_error(
                "before-image limits are outside supported bounds",
            ));
        }
        Ok(())
    }
}

fn snapshot_error(reason: impl Into<String>) -> PageError {
    PageError::BackupSnapshot {
        reason: reason.into(),
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PageBackupStats {
    pub pages_read: u64,
    pub before_image_pages: u64,
    pub before_image_bytes: u64,
    pub peak_retained_pages: usize,
}

/// File lengths and layout at the cut remain immutable. Reads must proceed in
/// increasing logical-page order; skipping an extent declares it consumed.
#[derive(Debug)]
pub struct PageBackupSnapshot {
    pub generations: Option<crate::ExtentGenerations>,
    pub(crate) state: Arc<Mutex<PageBackupState>>,
    segment_bytes: Vec<u64>,
    extent_pages: u64,
    page_size: u32,
}

#[derive(Debug)]
pub(crate) struct PageBackupState {
    source: Vec<File>,
    segment_bytes: Vec<u64>,
    extent_pages: u64,
    page_size: u32,
    journal: File,
    journal_path: PathBuf,
    saved: BTreeMap<PageId, u64>,
    free_slots: Vec<u64>,
    slots: u64,
    next_page: PageId,
    error: Option<String>,
    limits: PageBackupLimits,
    stats: PageBackupStats,
}

impl Drop for PageBackupState {
    fn drop(&mut self) {
        // An incomplete snapshot is never a published backup. Its journal is
        // only a live-process aid and is not needed for source recovery.
        let _ = std::fs::remove_file(&self.journal_path);
    }
}

impl PageBackupSnapshot {
    pub(crate) fn create(
        source: Vec<File>,
        segment_bytes: Vec<u64>,
        extent_pages: u64,
        page_size: u32,
        journal_path: &Path,
        limits: PageBackupLimits,
    ) -> Result<Self> {
        limits.validate(page_size)?;
        if extent_pages == 0 || source.len() != segment_bytes.len() {
            return Err(snapshot_error(
                "snapshot requires a stable segmented layout",
            ));
        }
        let mut options = OpenOptions::new();
        options.create_new(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let journal = options
            .open(journal_path)
            .map_err(|e| PageError::io(journal_path, e))?;
        let state = PageBackupState {
            source,
            segment_bytes: segment_bytes.clone(),
            extent_pages,
            page_size,
            journal,
            journal_path: journal_path.to_path_buf(),
            saved: BTreeMap::new(),
            free_slots: Vec::new(),
            slots: 0,
            next_page: 0,
            error: None,
            limits,
            stats: PageBackupStats::default(),
        };
        Ok(Self {
            generations: None,
            state: Arc::new(Mutex::new(state)),
            segment_bytes,
            extent_pages,
            page_size,
        })
    }

    pub fn segment_bytes(&self) -> &[u64] {
        &self.segment_bytes
    }

    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    pub fn extent_pages(&self) -> u64 {
        self.extent_pages
    }

    pub fn stats(&self) -> Result<PageBackupStats> {
        let state = self.state.lock();
        state.check()?;
        Ok(state.stats)
    }

    /// Read one original physical page. A failed/cancelled snapshot never
    /// returns later live bytes as a substitute for a missing before-image.
    pub fn read_page(&self, page_id: PageId, buffer: &mut [u8]) -> Result<()> {
        let mut state = self.state.lock();
        state.check()?;
        if buffer.len() != self.page_size as usize || page_id < state.next_page {
            return Err(snapshot_error(
                "snapshot pages must be read once in increasing order",
            ));
        }
        let (segment, offset) = state.location(page_id)?;
        let read = if let Some(slot) = state.saved.get(&page_id).copied() {
            let read = read_exact_at(&state.journal, buffer, slot * u64::from(self.page_size));
            if read.is_ok() {
                state.saved.remove(&page_id);
                state.free_slots.push(slot);
            }
            read
        } else {
            read_exact_at(&state.source[segment], buffer, offset)
        };
        if let Err(error) = read {
            state.fail(format!("snapshot read failed: {error}"));
            return state.check();
        }
        state.next_page = page_id
            .checked_add(1)
            .ok_or_else(|| snapshot_error("page cursor overflow"))?;
        // Skipping an extent must also release before-images it accumulated.
        let next = state.next_page;
        let future = state.saved.split_off(&next);
        for (_, slot) in std::mem::replace(&mut state.saved, future) {
            state.free_slots.push(slot);
        }
        state.stats.pages_read += 1;
        Ok(())
    }

    pub fn cancel(&self) {
        self.state.lock().fail("snapshot cancelled".to_string());
    }
}

impl PageBackupState {
    fn check(&self) -> Result<()> {
        match &self.error {
            Some(reason) => Err(snapshot_error(reason.clone())),
            None => Ok(()),
        }
    }

    fn location(&self, page_id: PageId) -> Result<(usize, u64)> {
        let segment = usize::try_from(page_id / self.extent_pages)
            .map_err(|_| snapshot_error("segment index overflow"))?;
        let offset = (page_id % self.extent_pages) * u64::from(self.page_size);
        if self
            .segment_bytes
            .get(segment)
            .is_none_or(|len| offset.saturating_add(u64::from(self.page_size)) > *len)
        {
            return Err(snapshot_error("page is outside the captured files"));
        }
        Ok((segment, offset))
    }

    fn fail(&mut self, reason: String) {
        if self.error.is_none() {
            self.error = Some(reason);
        }
        self.saved.clear();
        self.free_slots.clear();
    }

    /// Failure aborts the snapshot, not the foreground page write. Its reader
    /// observes the sticky error and cannot publish a successful manifest.
    pub(crate) fn preserve(&mut self, page_id: PageId) {
        if self.error.is_some() || page_id < self.next_page || self.saved.contains_key(&page_id) {
            return;
        }
        let Ok((segment, offset)) = self.location(page_id) else {
            return;
        };
        if self.saved.len() >= self.limits.max_before_image_pages {
            self.fail("snapshot before-image page budget exceeded".to_string());
            return;
        }
        let slot = match self.free_slots.pop() {
            Some(slot) => slot,
            None => {
                let Some(bytes) = (self.slots + 1).checked_mul(u64::from(self.page_size)) else {
                    self.fail("snapshot journal offset overflow".to_string());
                    return;
                };
                if bytes > self.limits.max_before_image_bytes {
                    self.fail("snapshot before-image byte budget exceeded".to_string());
                    return;
                }
                let slot = self.slots;
                self.slots += 1;
                slot
            }
        };
        let mut image = vec![0; self.page_size as usize];
        let result = read_exact_at(&self.source[segment], &mut image, offset)
            .and_then(|()| write_all_at(&self.journal, &image, slot * u64::from(self.page_size)));
        if let Err(error) = result {
            self.fail(format!("snapshot before-image I/O failed: {error}"));
            return;
        }
        self.saved.insert(page_id, slot);
        self.stats.before_image_pages += 1;
        self.stats.before_image_bytes += u64::from(self.page_size);
        self.stats.peak_retained_pages = self.stats.peak_retained_pages.max(self.saved.len());
    }
}

fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buffer.is_empty() {
        match crate::pio::read_at(file, buffer, offset) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(n) => {
                offset += n as u64;
                buffer = &mut buffer[n..];
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn write_all_at(file: &File, mut buffer: &[u8], mut offset: u64) -> io::Result<()> {
    while !buffer.is_empty() {
        match crate::pio::write_at(file, buffer, offset) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
            Ok(n) => {
                offset += n as u64;
                buffer = &buffer[n..];
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
