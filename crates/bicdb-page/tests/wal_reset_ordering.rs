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
use bicdb_page::{DurabilityState, PageError, PagedStore, PagedStoreOptions, Wal};
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
        if site == "page_log.reset_dir_sync" {
            // The directory sync precedes the truncation: at its failure the
            // active file, with the checkpoint record, must still be whole.
            assert!(
                active_bytes(dir.path()) > 0,
                "{label}: active file emptied before the directory sync"
            );
        }
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

/// Copy the current sealed segments aside, so a test can put them back after
/// a completed reset: the files an interrupted reset of the old order (active
/// truncated first) leaves behind.
fn keep_sealed(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    sealed_segments(dir)
        .into_iter()
        .map(|path| {
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect()
}

fn restore_sealed(kept: &[(PathBuf, Vec<u8>)]) {
    for (path, bytes) in kept {
        std::fs::write(path, bytes).unwrap();
    }
}

fn wal_files(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files: Vec<(String, Vec<u8>)> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("store.wal"))
        })
        .map(|path| {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            (name, std::fs::read(&path).unwrap())
        })
        .collect();
    files.sort();
    files
}

/// An older engine's interrupted reset (stale sealed segments, active file
/// truncated), after which it kept committing into the active file. Its
/// first record is the horizon `H`. Those commits are acknowledged: recovery
/// drops the stale segments and replays the active file, instead of
/// discarding it as a torn tail behind a chain it does not continue.
#[test]
fn commits_after_an_old_engines_interrupted_reset_are_recovered() {
    let dir = TempDir::new().unwrap();
    let kept;
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        hot_updates_across_segments(&store, dir.path(), 2);
        kept = keep_sealed(dir.path());
        store.checkpoint().unwrap();
        assert!(sealed_segments(dir.path()).is_empty());
        let xid = store.begin();
        store.put(xid, b"hot", b"after-reset").unwrap();
        store.put(xid, b"later", b"commit").unwrap();
        store.commit(xid).unwrap();
        assert!(active_bytes(dir.path()) > 0);
        assert!(sealed_segments(dir.path()).is_empty());
        // Crash.
    }
    restore_sealed(&kept);
    let (store, report) = PagedStore::open(dir.path(), options()).unwrap();
    assert!(report.stale_segments_discarded > 0, "{report:?}");
    assert!(sealed_segments(dir.path()).is_empty());
    assert_eq!(
        store.get(b"hot").unwrap().as_deref(),
        Some(&b"after-reset"[..])
    );
    assert_eq!(
        store.get(b"later").unwrap().as_deref(),
        Some(&b"commit"[..])
    );
    drop(store);
    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    assert_eq!(
        store.get(b"later").unwrap().as_deref(),
        Some(&b"commit"[..])
    );
}

/// A chain that ends before the horizon in any other shape is not an
/// interrupted reset: here the stale segments sit next to an active file that
/// does not start at the horizon. Recovery must refuse and leave every file
/// as it was, not delete the evidence.
#[test]
fn a_stale_chain_next_to_an_unexpected_active_file_is_refused_untouched() {
    let dir = TempDir::new().unwrap();
    let kept;
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        hot_updates_across_segments(&store, dir.path(), 2);
        kept = keep_sealed(dir.path());
        store.checkpoint().unwrap();
        // Crash.
    }
    restore_sealed(&kept);
    std::fs::write(dir.path().join("store.wal"), b"not a page-log record").unwrap();
    let before = wal_files(dir.path());
    let error = PagedStore::open(dir.path(), options())
        .map(|_| ())
        .expect_err("an unexplained stale chain must be refused");
    assert!(
        matches!(error, PageError::WalCorruption { .. }),
        "expected WalCorruption, got {error}"
    );
    assert_eq!(
        wal_files(dir.path()),
        before,
        "a refused log must stay untouched"
    );
}

/// A rolled-forward restore: the base copy of the page file was taken at an
/// older checkpoint (horizon `H1`) while the log kept growing past later
/// checkpoints. Recovery must redo from the base's own checkpoint record (at
/// most `H1`), not from LSN 0 (the old rule distrusted the later records'
/// redo points and replayed everything), and not past `H1`.
#[test]
fn a_roll_forward_over_an_older_base_redoes_from_the_bases_checkpoint() {
    let dir = TempDir::new().unwrap();
    let monolithic = || {
        PagedStoreOptions::default()
            .with_page_size(4096)
            .with_buffer_pool_bytes(4 * 1024 * 1024)
            .with_fsync(false)
            .with_wal_max_bytes(1 << 30)
    };
    let base = dir.path().join("base.pages");
    let rows = |prefix: &'static str| (0..60).map(move |i| format!("{prefix}-{i:03}").into_bytes());
    {
        let (store, _) = PagedStore::open(dir.path(), monolithic()).unwrap();
        // A backup pin: checkpoints keep the whole log.
        store.begin_backup_pin();
        for key in rows("a") {
            let xid = store.begin();
            store.put(xid, &key, b"a-value").unwrap();
            store.commit(xid).unwrap();
        }
        store.checkpoint().unwrap();
        std::fs::copy(dir.path().join("store.pages"), &base).unwrap();
        for key in rows("b") {
            let xid = store.begin();
            store.put(xid, &key, b"b-value").unwrap();
            store.commit(xid).unwrap();
        }
        store.checkpoint().unwrap();
        for key in rows("c") {
            let xid = store.begin();
            store.put(xid, &key, b"c-value").unwrap();
            store.commit(xid).unwrap();
        }
        // Crash.
    }
    let base_horizon = bicdb_page::PageStore::open_existing_auto(&base, false)
        .unwrap()
        .checkpoint_lsn();
    assert!(base_horizon > 0);
    let total_images = {
        let wal = Wal::open(dir.path().join("store.wal"), false).unwrap();
        let (records, _) = wal.read_all().unwrap();
        records
            .iter()
            .filter(|record| record.kind == bicdb_page::wal::RecordKind::PageImage)
            .count() as u64
    };
    std::fs::rename(&base, dir.path().join("store.pages")).unwrap();
    let (store, report) = PagedStore::open(dir.path(), monolithic()).unwrap();
    assert!(
        report.redo_lsn > 0 && report.redo_lsn <= base_horizon,
        "redo {} must lie in (0, {base_horizon}]",
        report.redo_lsn
    );
    assert!(
        report.pages_replayed < total_images,
        "replayed {} of {total_images} images: the prefix below the base's horizon must be skipped",
        report.pages_replayed
    );
    for (prefix, value) in [("a", "a-value"), ("b", "b-value"), ("c", "c-value")] {
        for key in rows(prefix) {
            assert_eq!(
                store.get(&key).unwrap().as_deref(),
                Some(value.as_bytes()),
                "{}",
                String::from_utf8_lossy(&key)
            );
        }
    }
}
