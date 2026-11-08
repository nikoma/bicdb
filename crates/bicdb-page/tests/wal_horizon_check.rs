//! The page log must not start above the page file's checkpoint horizon.
//!
//! `docs/recovery-semantics.md`, section 8 (Q2) and GAP-4: a chain whose
//! first record lies above the horizon has lost records the page file does
//! not reflect, so recovery refuses it and leaves the files untouched. Every
//! legitimate state still opens: an empty log, a log that a checkpoint did
//! not reset, sealed segments kept by archive retention, a fresh store.

use std::path::{Path, PathBuf};

use bicdb_page::{PageError, PagedStore, PagedStoreOptions};
use tempfile::TempDir;

fn options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(4096)
        .with_buffer_pool_bytes(4 * 1024 * 1024)
        .with_fsync(false)
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

fn commit_until_sealed(store: &PagedStore, dir: &Path, segments: usize, prefix: &str) -> usize {
    let filler = "f".repeat(600);
    let mut written = 0usize;
    while sealed_segments(dir).len() < segments {
        let xid = store.begin();
        store
            .put(
                xid,
                format!("{prefix}-{written:05}").as_bytes(),
                filler.as_bytes(),
            )
            .unwrap();
        store.commit(xid).unwrap();
        written += 1;
    }
    written
}

fn wal_files(dir: &Path) -> Vec<(PathBuf, u64)> {
    let mut files: Vec<(PathBuf, u64)> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("store.wal"))
        })
        .map(|path| {
            let len = std::fs::metadata(&path).unwrap().len();
            (path, len)
        })
        .collect();
    files.sort();
    files
}

#[test]
fn a_log_that_lost_its_first_segment_after_the_horizon_is_refused_untouched() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        commit_until_sealed(&store, dir.path(), 1, "before");
        // Truncating checkpoint: the horizon H is published and the log is
        // empty, so the next record has LSN H.
        store.checkpoint().unwrap();
        assert!(sealed_segments(dir.path()).is_empty());
        commit_until_sealed(&store, dir.path(), 3, "after");
        // Crash.
    }
    // Lose the segment that starts at H.
    let first = sealed_segments(dir.path()).remove(0);
    std::fs::remove_file(&first).unwrap();
    // A torn tail on the active file too: the refusal must happen before any
    // truncation, so the evidence stays as it was.
    {
        use std::io::Write;
        let mut wal = std::fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join("store.wal"))
            .unwrap();
        wal.write_all(b"BWAL-torn").unwrap();
    }
    let before = wal_files(dir.path());

    let error = PagedStore::open(dir.path(), options())
        .map(|_| ())
        .expect_err("a log that starts above the horizon must be refused");
    match &error {
        PageError::WalCorruption { reason, .. } => assert!(
            reason.contains("checkpoint horizon"),
            "unexpected corruption reason: {reason}"
        ),
        other => panic!("expected WalCorruption, got {other}"),
    }
    assert_eq!(
        wal_files(dir.path()),
        before,
        "a refused log must stay untouched"
    );
}

#[test]
fn an_empty_log_after_a_reset_opens() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        commit_until_sealed(&store, dir.path(), 2, "k");
        store.checkpoint().unwrap();
    }
    let (store, report) = PagedStore::open(dir.path(), options()).unwrap();
    assert_eq!(report.records_scanned, 0);
    assert!(store.get(b"k-00000").unwrap().is_some());
}

#[test]
fn a_log_after_a_completed_reset_starts_exactly_at_the_horizon_and_opens() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        commit_until_sealed(&store, dir.path(), 1, "a");
        store.checkpoint().unwrap();
        commit_until_sealed(&store, dir.path(), 2, "b");
        // Crash.
    }
    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    assert!(store.get(b"a-00000").unwrap().is_some());
    assert!(store.get(b"b-00000").unwrap().is_some());
}

#[test]
fn a_checkpoint_that_did_not_reset_leaves_a_log_below_the_horizon_that_opens() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        commit_until_sealed(&store, dir.path(), 2, "k");
        // A backup pin defers the reset: the chain keeps everything, starts
        // far below the horizon and ends at the checkpoint record H - 1. This
        // is also the state after a crash between publication and reset.
        store.begin_backup_pin();
        store.checkpoint().unwrap();
        assert!(!sealed_segments(dir.path()).is_empty());
        commit_until_sealed(&store, dir.path(), 4, "m");
        // Crash.
    }
    let (store, report) = PagedStore::open(dir.path(), options()).unwrap();
    assert!(report.redo_lsn > 0);
    assert!(store.get(b"k-00000").unwrap().is_some());
    assert!(store.get(b"m-00000").unwrap().is_some());
}

#[test]
fn sealed_segments_kept_by_archive_retention_open() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        store.enable_wal_archive_retention();
        commit_until_sealed(&store, dir.path(), 2, "k");
        // Retention seals the active file and keeps every unarchived segment
        // through the reset: the chain is entirely below the new horizon.
        store.checkpoint().unwrap();
        assert!(!sealed_segments(dir.path()).is_empty());
        let xid = store.begin();
        store.put(xid, b"after", b"reset").unwrap();
        store.commit(xid).unwrap();
        // Crash.
    }
    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    assert_eq!(store.get(b"after").unwrap().as_deref(), Some(&b"reset"[..]));
    assert!(store.get(b"k-00000").unwrap().is_some());
}

#[test]
fn a_fresh_store_and_its_first_crash_open() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        let xid = store.begin();
        store.put(xid, b"first", b"row").unwrap();
        store.commit(xid).unwrap();
        // Crash before any further checkpoint.
    }
    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    assert_eq!(store.get(b"first").unwrap().as_deref(), Some(&b"row"[..]));
}

#[test]
fn a_monolithic_log_opens_across_reopen_cycles() {
    let dir = TempDir::new().unwrap();
    let monolithic = PagedStoreOptions::default()
        .with_page_size(4096)
        .with_buffer_pool_bytes(4 * 1024 * 1024)
        .with_fsync(false)
        .with_wal_max_bytes(1 << 30);
    for round in 0..4 {
        let (store, _) = PagedStore::open(dir.path(), monolithic.clone()).unwrap();
        for index in 0..20 {
            let xid = store.begin();
            store
                .put(xid, format!("r{round}-{index}").as_bytes(), b"v")
                .unwrap();
            store.commit(xid).unwrap();
        }
        if round % 2 == 0 {
            store.checkpoint().unwrap();
        }
        // Crash.
    }
    let (store, _) = PagedStore::open(dir.path(), monolithic).unwrap();
    assert!(store.get(b"r3-19").unwrap().is_some());
    assert!(store.get(b"r0-0").unwrap().is_some());
}
