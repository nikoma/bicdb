//! The page-log reset is crash-ordered and fails closed (GAP-2 in
//! `docs/recovery-semantics.md`).
//!
//! `Wal::reset` deletes the sealed segments oldest first, syncs the
//! directory, then truncates and syncs the active file, which holds the
//! checkpoint record. Each test injects `EIO` at one step of a checkpoint's
//! reset, which leaves the files exactly as a crash at that step would, then
//! crashes (drops the store) and recovers. Recovery must end with every
//! committed value at its newest version, and the store must have fenced
//! itself at the failure.

use std::path::{Path, PathBuf};

use bicdb_page::test_faults::arm_io_failure;
use bicdb_page::{DurabilityState, PagedStore, PagedStoreOptions};
use tempfile::TempDir;

fn options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(4096)
        .with_buffer_pool_bytes(4 * 1024 * 1024)
        .with_fsync(true)
        .with_wal_max_bytes(1 << 30)
        .with_extent_bytes(1024 * 1024)
        .with_wal_segment_bytes(64 * 1024)
}

fn sealed_segments(dir: &Path) -> Vec<PathBuf> {
    let mut sealed: Vec<(u64, PathBuf)> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter_map(|path| {
            let sequence = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_prefix("store.wal."))
                .and_then(|suffix| suffix.parse::<u64>().ok())?;
            Some((sequence, path))
        })
        .collect();
    sealed.sort();
    sealed.into_iter().map(|(_, path)| path).collect()
}

fn active_bytes(dir: &Path) -> u64 {
    std::fs::metadata(dir.join("store.wal"))
        .map(|meta| meta.len())
        .unwrap_or(0)
}

/// Update one hot key until at least `segments` sealed segments exist, with
/// the last update in the active file only. Returns the committed value.
fn hot_updates_across_segments(store: &PagedStore, dir: &Path, segments: usize) -> Vec<u8> {
    let filler = "f".repeat(600);
    let mut generation = 0u64;
    loop {
        generation += 1;
        let before = sealed_segments(dir).len();
        let xid = store.begin();
        store
            .put(xid, b"hot", format!("v{generation}").as_bytes())
            .unwrap();
        store
            .put(
                xid,
                format!("fill-{generation:05}").as_bytes(),
                filler.as_bytes(),
            )
            .unwrap();
        store.commit(xid).unwrap();
        if before >= segments && sealed_segments(dir).len() == before {
            return format!("v{generation}").into_bytes();
        }
    }
}

fn assert_recovers_newest(dir: &Path, committed: &[u8], label: &str) {
    let (store, report) = PagedStore::open(dir, options())
        .unwrap_or_else(|error| panic!("{label}: reopen failed: {error}"));
    assert_eq!(
        store.get(b"hot").unwrap().as_deref(),
        Some(committed),
        "{label}: the hot key regressed (report {report:?})"
    );
    assert!(
        store.get(b"fill-00001").unwrap().is_some(),
        "{label}: an early committed row is missing"
    );
    // A commit after recovery must survive the next crash: the chain the
    // reopen left must continue to the record it appends.
    let xid = store.begin();
    store.put(xid, b"after", b"recovery").unwrap();
    store.commit(xid).unwrap();
    drop(store);
    let (store, _) = PagedStore::open(dir, options()).unwrap();
    assert_eq!(
        store.get(b"after").unwrap().as_deref(),
        Some(&b"recovery"[..]),
        "{label}: a commit made after recovery was lost"
    );
    assert_eq!(
        store.get(b"hot").unwrap().as_deref(),
        Some(committed),
        "{label}: the hot key regressed on the second reopen"
    );
}

/// Inject a failure at the `nth` call of `site` during the checkpoint's
/// reset, then crash and recover.
fn fail_reset_step(site: &str, nth: u64) {
    let label = format!("{site}:{nth}");
    let dir = TempDir::new().unwrap();
    let committed;
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        committed = hot_updates_across_segments(&store, dir.path(), 3);
        let segments_before = sealed_segments(dir.path()).len();
        let fault = arm_io_failure(site, dir.path(), nth, false);
        let result = store.checkpoint();
        assert_eq!(
            fault.fired(),
            1,
            "{label}: the reset step was never reached"
        );
        assert!(
            result.is_err(),
            "{label}: the checkpoint must report the failure"
        );
        assert!(
            matches!(
                store.durability_health().state(),
                DurabilityState::WriteFailed(_)
            ),
            "{label}: the store did not fence itself"
        );
        // Fenced: no later commit is acknowledged.
        let xid = store.begin();
        let refused = store
            .put(xid, b"hot", b"after-failure")
            .and_then(|()| store.commit(xid));
        assert!(
            refused.is_err(),
            "{label}: a commit was acknowledged after the failure"
        );
        // The crash-order property: whatever the failed step left, the
        // active file (and its checkpoint record) is the last thing to go.
        let remaining = sealed_segments(dir.path()).len();
        if site == "page_log.reset_unlink" {
            assert_eq!(remaining, segments_before - (nth as usize - 1));
            assert!(
                active_bytes(dir.path()) > 0,
                "{label}: active file emptied early"
            );
        } else {
            assert_eq!(remaining, 0, "{label}: segments left behind");
        }
        // Crash.
    }
    assert_recovers_newest(dir.path(), &committed, &label);
}

#[test]
fn a_failed_first_segment_unlink_fences_and_recovers_from_the_redo_point() {
    fail_reset_step("page_log.reset_unlink", 1);
}

#[test]
fn a_failed_later_segment_unlink_fences_and_recovers_from_the_redo_point() {
    fail_reset_step("page_log.reset_unlink", 3);
}

#[test]
fn a_failed_directory_sync_fences_and_recovers() {
    fail_reset_step("page_log.reset_dir_sync", 1);
}

#[test]
fn a_failed_active_truncation_fences_and_recovers() {
    fail_reset_step("page_log.reset", 1);
}

#[test]
fn a_failed_active_sync_fences_and_recovers() {
    fail_reset_step("page_log.reset_sync", 1);
}

#[test]
fn a_completed_reset_leaves_an_empty_log_that_continues() {
    let dir = TempDir::new().unwrap();
    let committed;
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        committed = hot_updates_across_segments(&store, dir.path(), 2);
        store.checkpoint().unwrap();
        assert!(sealed_segments(dir.path()).is_empty());
        assert_eq!(active_bytes(dir.path()), 0);
    }
    assert_recovers_newest(dir.path(), &committed, "completed reset");
}
