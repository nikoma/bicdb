//! Measurement-only trace of page-WAL write amplification.
//!
//! Off by default. Enabled by `BICDB_PAGE_WAL_AMPLIFICATION=1` in the
//! environment (read once), or by [`enable`] from a probe. When off, every hook
//! is one relaxed atomic load and a branch: no allocation, no lock, no change
//! to what is written. When on, it changes nothing that is written either; it
//! only observes.
//!
//! What it records, per page image appended to the page WAL:
//! - the page's class (heap, key-tree leaf/interior, catalog leaf/interior,
//!   overflow, free, meta) taken from the image's own header plus the scope
//!   that dirtied it;
//! - the logical operations that dirtied the page since its previous image
//!   (`put`/`delete` of a row, an index entry, an intern-dictionary entry...,
//!   the version-header `xmax` stamp, a B-tree split, a free-space-map update,
//!   a meta-page write);
//! - the record's bytes, and how many bytes of the image differ from the same
//!   page's previous image in the log (the diff), plus an estimate of a
//!   byte-range delta record's size;
//! - whether the page already had an image at or after the current redo point
//!   (so the classic full-page-write rule would have logged a delta instead),
//!   and whether it was also imaged by the immediately preceding commit.
//!
//! See `docs/page-wal-write-amplification.md` for the measurements.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::OnceLock;

use parking_lot::Mutex;
use rustc_hash::FxHashMap;

use crate::page::PageId;

const UNKNOWN: u8 = 0;
const OFF: u8 = 1;
const ON: u8 = 2;

static STATE: AtomicU8 = AtomicU8::new(UNKNOWN);

/// Environment switch. Only the exact value `1` enables the trace.
pub const ENV: &str = "BICDB_PAGE_WAL_AMPLIFICATION";

/// Previous images kept for diffing. Pages beyond this are counted without a
/// baseline rather than growing the probe without bound.
const MAX_BASELINE_PAGES: usize = 65_536;

/// Gap (in bytes) below which two changed runs are merged into one delta run:
/// a run header costs 4 bytes, so splitting across a smaller gap never pays.
const DELTA_MERGE_GAP: usize = 4;
/// Per-run header of the estimated delta encoding: u16 offset + u16 length.
const DELTA_RUN_HEADER: u64 = 4;
/// Fixed bytes of a page WAL record: header (24) + page id and xid (16).
pub const RECORD_OVERHEAD: u64 = 40;

/// Whether the trace is recording. One relaxed load after the first call.
#[inline]
pub fn enabled() -> bool {
    match STATE.load(Ordering::Relaxed) {
        ON => true,
        OFF => false,
        _ => init_from_env(),
    }
}

#[cold]
fn init_from_env() -> bool {
    let on = std::env::var(ENV).is_ok_and(|value| env_value_enables(&value));
    let _ = STATE.compare_exchange(
        UNKNOWN,
        if on { ON } else { OFF },
        Ordering::Relaxed,
        Ordering::Relaxed,
    );
    STATE.load(Ordering::Relaxed) == ON
}

/// Whether a value of [`ENV`] turns the trace on: only the exact value `1`.
pub fn env_value_enables(value: &str) -> bool {
    value == "1"
}

/// Turn the trace on for this process (probes only).
pub fn enable() {
    STATE.store(ON, Ordering::Relaxed);
}

/// Turn the trace off for this process.
pub fn disable() {
    STATE.store(OFF, Ordering::Relaxed);
}

/// Where in the write path an image was appended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Site {
    /// `PagedStore::commit` logging the pages a transaction dirtied.
    Commit,
    /// The writeback barrier logging a page on its way to disk.
    Writeback,
    /// Any other caller (status spill, structural frees, maintenance).
    Other,
}

impl Site {
    pub fn as_str(self) -> &'static str {
        match self {
            Site::Commit => "commit",
            Site::Writeback => "writeback",
            Site::Other => "other",
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Context {
    op: Option<&'static str>,
    key_class: Option<&'static str>,
    component: Option<&'static str>,
    detail: Option<&'static str>,
    site: Option<Site>,
}

thread_local! {
    static CONTEXT: RefCell<Context> = RefCell::new(Context::default());
}

/// Which context field a scope sets.
#[derive(Clone, Copy, Debug)]
pub enum Field {
    /// The logical operation (`put`, `delete`, `meta`...).
    Op,
    /// What the key belongs to (row, index entry, intern dictionary...).
    KeyClass,
    /// Which structure is mutating (`catalog`, `status_spill`).
    Component,
    /// A sub-step (`xmax_stamp`, `split`, `free_space_map`).
    Detail,
}

/// Restores the previous value of one context field on drop.
#[must_use]
pub struct Scope {
    field: Field,
    previous: Option<&'static str>,
}

impl Drop for Scope {
    fn drop(&mut self) {
        let previous = self.previous;
        let field = self.field;
        CONTEXT.with(|context| {
            let mut context = context.borrow_mut();
            match field {
                Field::Op => context.op = previous,
                Field::KeyClass => context.key_class = previous,
                Field::Component => context.component = previous,
                Field::Detail => context.detail = previous,
            }
        });
    }
}

/// Set one context field for the lifetime of the returned guard. `None` (and
/// no thread-local access at all) when the trace is off.
#[inline]
pub fn scope(field: Field, value: &'static str) -> Option<Scope> {
    if !enabled() {
        return None;
    }
    Some(set_scope(field, value))
}

#[cold]
fn set_scope(field: Field, value: &'static str) -> Scope {
    CONTEXT.with(|context| {
        let mut context = context.borrow_mut();
        let slot = match field {
            Field::Op => &mut context.op,
            Field::KeyClass => &mut context.key_class,
            Field::Component => &mut context.component,
            Field::Detail => &mut context.detail,
        };
        let previous = std::mem::replace(slot, Some(value));
        Scope { field, previous }
    })
}

/// Restores the previous log site on drop.
#[must_use]
pub struct SiteScope {
    previous: Option<Site>,
}

impl Drop for SiteScope {
    fn drop(&mut self) {
        let previous = self.previous;
        CONTEXT.with(|context| context.borrow_mut().site = previous);
    }
}

/// Label images appended on this thread with `site` until the guard drops.
#[inline]
pub fn site(site: Site) -> Option<SiteScope> {
    if !enabled() {
        return None;
    }
    Some(CONTEXT.with(|context| {
        let previous = std::mem::replace(&mut context.borrow_mut().site, Some(site));
        SiteScope { previous }
    }))
}

/// Classify a paged-store key by the namespace conventions of `bicdb-core`.
/// A record key starts with its collection name's non-zero u16 length, so a
/// leading `[0, 0]` marks an engine namespace.
pub fn key_class(key: &[u8]) -> &'static str {
    if key.len() < 3 || key[0] != 0 || key[1] != 0 {
        return "row";
    }
    match key[2] {
        1 => "index_entry",
        2..=12 => "fulltext",
        13 => "spatial",
        14 => "intern_dict",
        15 => "entry_format",
        _ => "engine_other",
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Tag {
    op: &'static str,
    key_class: &'static str,
    component: &'static str,
    detail: &'static str,
}

#[derive(Default)]
struct PageState {
    previous: Option<Box<[u8]>>,
    last_image_lsn: Option<u64>,
    last_image_commit: Option<u64>,
}

#[derive(Default)]
struct State {
    dirty: FxHashMap<PageId, Vec<Tag>>,
    pages: FxHashMap<PageId, PageState>,
    baselines: usize,
    rows: BTreeMap<(Site, &'static str, String), Row>,
    commits: u64,
    commit_record_bytes: u64,
    abort_records: u64,
    lock_hold: LockHold,
}

/// Aggregated counters for one (site, page class, operations) group.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Row {
    pub images: u64,
    /// Full record bytes, header included.
    pub record_bytes: u64,
    /// Images with a previous image of the same page to diff against.
    pub images_with_baseline: u64,
    /// Bytes that differ from the previous image (only images with a baseline).
    pub changed_bytes: u64,
    /// Estimated size of a byte-range delta record instead of the image
    /// (record overhead + 4 bytes per run + run bytes); full image size for
    /// images without a baseline.
    pub delta_record_bytes: u64,
    /// Images identical to the previous image of the page.
    pub unchanged_images: u64,
    /// Images of a page that already had an image at or after the redo point
    /// in force when this one was appended (the full-page-write rule would
    /// have logged a delta).
    pub repeat_since_redo: u64,
    pub repeat_since_redo_bytes: u64,
    /// Delta-record estimate for those repeats.
    pub repeat_since_redo_delta_bytes: u64,
    /// Images of a page that was also imaged by the previous commit.
    pub consecutive_commit_images: u64,
}

impl Row {
    fn add(&mut self, other: &Row) {
        self.images += other.images;
        self.record_bytes += other.record_bytes;
        self.images_with_baseline += other.images_with_baseline;
        self.changed_bytes += other.changed_bytes;
        self.delta_record_bytes += other.delta_record_bytes;
        self.unchanged_images += other.unchanged_images;
        self.repeat_since_redo += other.repeat_since_redo;
        self.repeat_since_redo_bytes += other.repeat_since_redo_bytes;
        self.repeat_since_redo_delta_bytes += other.repeat_since_redo_delta_bytes;
        self.consecutive_commit_images += other.consecutive_commit_images;
    }
}

/// Time spent under the store-wide writer gate by core row commits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct LockHold {
    pub commits: u64,
    /// begin + row/index puts and deletes (page mutation).
    pub apply_nanos: u64,
    /// Logging dirty page images (copy + append).
    pub log_images_nanos: u64,
    /// Commit record append + WAL sync.
    pub commit_record_and_sync_nanos: u64,
    /// The whole `PagedStore::commit` critical section.
    pub commit_nanos: u64,
}

/// One row of [`Report::rows`].
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ReportRow {
    pub site: &'static str,
    pub page_class: &'static str,
    pub operations: String,
    #[serde(flatten)]
    pub counters: Row,
}

/// A snapshot of everything recorded since the last [`reset`].
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Report {
    pub commits: u64,
    pub commit_record_bytes: u64,
    pub abort_records: u64,
    pub totals: Row,
    pub rows: Vec<ReportRow>,
    pub lock_hold: LockHold,
}

fn state() -> &'static Mutex<State> {
    static STATE_CELL: OnceLock<Mutex<State>> = OnceLock::new();
    STATE_CELL.get_or_init(|| Mutex::new(State::default()))
}

/// A write guard on `page_id` was released: attribute the page to the
/// current context. Called by the buffer pool only when [`enabled`].
#[cold]
pub fn note_dirty(page_id: PageId) {
    let context = CONTEXT.with(|context| *context.borrow());
    let tag = Tag {
        op: context.op.unwrap_or("untracked"),
        key_class: context.key_class.unwrap_or("-"),
        component: context.component.unwrap_or("-"),
        detail: context.detail.unwrap_or("-"),
    };
    let mut state = state().lock();
    let tags = state.dirty.entry(page_id).or_default();
    if !tags.contains(&tag) {
        tags.push(tag);
    }
}

fn page_class(page_type: u8, tags: &[Tag]) -> &'static str {
    let any = |pred: &dyn Fn(&Tag) -> bool| tags.iter().any(pred);
    if any(&|tag| tag.op == "meta") {
        return "meta";
    }
    let catalog = any(&|tag| tag.component == "catalog");
    let spill = any(&|tag| tag.component == "status_spill");
    match page_type {
        2 if spill => "status_spill",
        2 => "heap",
        3 => "overflow",
        4 if catalog => "catalog_interior",
        4 => "key_tree_interior",
        5 if catalog => "catalog_leaf",
        5 => "key_tree_leaf",
        6 => "free",
        1 => "superblock",
        _ => "unknown",
    }
}

fn operations_label(tags: &mut Vec<Tag>) -> String {
    if tags.is_empty() {
        return "(no dirtying write seen)".to_string();
    }
    tags.sort();
    let mut parts: Vec<String> = tags
        .iter()
        .map(|tag| {
            let mut label = tag.op.to_string();
            if tag.key_class != "-" {
                label.push(':');
                label.push_str(tag.key_class);
            }
            if tag.component != "-" {
                label.push('/');
                label.push_str(tag.component);
            }
            if tag.detail != "-" {
                label.push('/');
                label.push_str(tag.detail);
            }
            label
        })
        .collect();
    parts.dedup();
    parts.join(" + ")
}

/// Changed bytes and the estimated byte-range delta size between two images.
pub fn diff(previous: &[u8], current: &[u8]) -> (u64, u64) {
    if previous.len() != current.len() {
        return (current.len() as u64, RECORD_OVERHEAD + current.len() as u64);
    }
    let mut changed = 0u64;
    let mut delta = RECORD_OVERHEAD;
    let mut run: Option<(usize, usize)> = None; // [start, end)
    for (index, (a, b)) in previous.iter().zip(current.iter()).enumerate() {
        if a == b {
            continue;
        }
        changed += 1;
        run = match run {
            Some((start, end)) if index - end <= DELTA_MERGE_GAP => Some((start, index + 1)),
            Some((start, end)) => {
                delta += DELTA_RUN_HEADER + (end - start) as u64;
                Some((index, index + 1))
            }
            None => Some((index, index + 1)),
        };
    }
    if let Some((start, end)) = run {
        delta += DELTA_RUN_HEADER + (end - start) as u64;
    }
    (changed, delta)
}

/// A page image was appended at `lsn` while `redo_lsn` was the redo point.
#[cold]
pub fn record_image(page_id: PageId, image: &[u8], lsn: u64, redo_lsn: u64) {
    let site = CONTEXT
        .with(|context| context.borrow().site)
        .unwrap_or(Site::Other);
    let record_bytes = RECORD_OVERHEAD + image.len() as u64;
    let page_type = image.get(6).copied().unwrap_or(0);
    let mut guard = state().lock();
    let state = &mut *guard;
    let mut tags = state.dirty.remove(&page_id).unwrap_or_default();
    let class = page_class(page_type, &tags);
    let label = operations_label(&mut tags);
    let commit_now = state.commits;
    let mut row = Row {
        images: 1,
        record_bytes,
        ..Row::default()
    };
    let can_add_baseline = state.baselines < MAX_BASELINE_PAGES;
    let page = state.pages.entry(page_id).or_default();
    let delta = match page.previous.as_deref() {
        Some(previous) => {
            let (changed, delta) = diff(previous, image);
            row.images_with_baseline = 1;
            row.changed_bytes = changed;
            row.unchanged_images = u64::from(changed == 0);
            delta
        }
        None => record_bytes,
    };
    row.delta_record_bytes = delta;
    if page.last_image_lsn.is_some_and(|last| last >= redo_lsn) {
        row.repeat_since_redo = 1;
        row.repeat_since_redo_bytes = record_bytes;
        row.repeat_since_redo_delta_bytes = delta;
    }
    // `commits` counts commit records appended so far; an image logged by the
    // commit about to be numbered `commit_now` is "consecutive" when the same
    // page was imaged by commit `commit_now - 1`.
    if site == Site::Commit
        && commit_now > 0
        && page.last_image_commit == Some(commit_now - 1)
    {
        row.consecutive_commit_images = 1;
    }
    page.last_image_lsn = Some(lsn);
    if site == Site::Commit {
        page.last_image_commit = Some(commit_now);
    }
    match &mut page.previous {
        Some(previous) if previous.len() == image.len() => previous.copy_from_slice(image),
        Some(previous) => *previous = image.into(),
        None if can_add_baseline => {
            page.previous = Some(image.into());
            state.baselines += 1;
        }
        None => {}
    }
    state
        .rows
        .entry((site, class, label))
        .or_default()
        .add(&row);
}

/// A commit record was appended.
#[cold]
pub fn record_commit() {
    let mut state = state().lock();
    state.commits += 1;
    state.commit_record_bytes += RECORD_OVERHEAD;
}

/// An abort record was appended.
#[cold]
pub fn record_abort() {
    state().lock().abort_records += 1;
}

/// Writer-gate timings of one `PagedStore::commit`.
#[cold]
pub fn record_commit_timing(log_images_nanos: u64, commit_record_and_sync_nanos: u64, total: u64) {
    let mut state = state().lock();
    state.lock_hold.log_images_nanos += log_images_nanos;
    state.lock_hold.commit_record_and_sync_nanos += commit_record_and_sync_nanos;
    state.lock_hold.commit_nanos += total;
}

/// Writer-gate apply time of one core row commit (begin through the last put).
#[cold]
pub fn record_apply_timing(apply_nanos: u64) {
    let mut state = state().lock();
    state.lock_hold.commits += 1;
    state.lock_hold.apply_nanos += apply_nanos;
}

/// Everything recorded since the last [`reset`].
pub fn report() -> Report {
    let state = state().lock();
    let mut totals = Row::default();
    let rows = state
        .rows
        .iter()
        .map(|((site, class, label), counters)| {
            totals.add(counters);
            ReportRow {
                site: site.as_str(),
                page_class: class,
                operations: label.clone(),
                counters: counters.clone(),
            }
        })
        .collect();
    Report {
        commits: state.commits,
        commit_record_bytes: state.commit_record_bytes,
        abort_records: state.abort_records,
        totals,
        rows,
        lock_hold: state.lock_hold,
    }
}

/// Forget counters (but keep per-page baselines, so the first image after a
/// reset still diffs against what the log already holds).
pub fn reset() {
    let mut state = state().lock();
    state.rows.clear();
    state.commits = 0;
    state.commit_record_bytes = 0;
    state.abort_records = 0;
    state.lock_hold = LockHold::default();
    for page in state.pages.values_mut() {
        page.last_image_commit = None;
    }
}

/// Forget everything, baselines included (a new store is being measured).
pub fn clear() {
    *state().lock() = State::default();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_counts_changed_bytes_and_merges_close_runs() {
        let a = vec![0u8; 64];
        let mut b = a.clone();
        assert_eq!(diff(&a, &b), (0, RECORD_OVERHEAD));
        b[10] = 1;
        b[12] = 1; // gap 1: merged into one run of 3 bytes
        b[40] = 1; // far: second run of 1 byte
        let (changed, delta) = diff(&a, &b);
        assert_eq!(changed, 3);
        assert_eq!(delta, RECORD_OVERHEAD + (4 + 3) + (4 + 1));
    }

    #[test]
    fn key_classes_follow_core_namespaces() {
        assert_eq!(key_class(&[0, 4, b'r', b'o', b'w', b's', b'x']), "row");
        assert_eq!(key_class(&[0, 0, 1, 0, 3]), "index_entry");
        assert_eq!(key_class(&[0, 0, 14, 0, 3]), "intern_dict");
        assert_eq!(key_class(&[0, 0, 7]), "fulltext");
    }
}
