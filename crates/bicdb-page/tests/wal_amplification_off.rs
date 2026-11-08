//! The page-WAL amplification trace is measurement-only and OFF by default.
//!
//! Off, its hooks must record nothing (and therefore cost one relaxed load).
//! On, it must observe without changing what is written: the WAL bytes a
//! commit appends are identical with the trace on and off.

use bicdb_page::wal_amplification::{self, Report};
use bicdb_page::{PagedStore, PagedStoreOptions};
use tempfile::TempDir;

fn options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(4096)
        .with_buffer_pool_bytes(1024 * 1024)
        .with_fsync(false)
}

/// Bytes the WAL appended for one fixed commit on a fresh store.
fn one_commit_wal_bytes() -> u64 {
    let dir = TempDir::new().unwrap();
    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    // Measure only the commit, not what opening a fresh store appends.
    wal_amplification::clear();
    let before = store.wal().snapshot().bytes_appended;
    let xid = store.begin();
    store.put(xid, b"\x00\x04rowsr-1", b"value").unwrap();
    store
        .put(xid, b"\x00\x00\x01\x00\x08rows_idxk\x00\x00r-1", b"")
        .unwrap();
    store.commit(xid).unwrap();
    store.wal().snapshot().bytes_appended - before
}

#[test]
fn the_trace_is_off_by_default_records_nothing_and_never_changes_the_log() {
    // Only the exact value `1` enables it from the environment.
    assert!(wal_amplification::env_value_enables("1"));
    for value in ["", "0", "true", "on", "yes", " 1", "1 "] {
        assert!(!wal_amplification::env_value_enables(value), "{value:?}");
    }
    if std::env::var(wal_amplification::ENV).is_ok_and(|v| v == "1") {
        eprintln!(
            "{} is set in this environment; skipping",
            wal_amplification::ENV
        );
        return;
    }

    // Default: off, and a real commit leaves no trace.
    assert!(!wal_amplification::enabled());
    let off_bytes = one_commit_wal_bytes();
    assert!(off_bytes > 0);
    assert_eq!(wal_amplification::report(), Report::default());

    // On: the same commit is observed, and the log is byte-for-byte the same size.
    wal_amplification::enable();
    let on_bytes = one_commit_wal_bytes();
    let report = wal_amplification::report();
    wal_amplification::disable();
    wal_amplification::clear();
    assert_eq!(
        on_bytes, off_bytes,
        "the trace must not change what is logged"
    );
    assert_eq!(report.commits, 1);
    assert!(report.totals.images > 0);
    assert_eq!(
        report.totals.record_bytes + report.commit_record_bytes,
        // Everything this commit appended is an image or its commit record.
        on_bytes,
    );
    assert!(report
        .rows
        .iter()
        .any(|row| row.operations.contains("put:index_entry")));
    assert!(report
        .rows
        .iter()
        .any(|row| row.operations.contains("put:row")));

    // Off again: nothing further is recorded.
    assert!(!wal_amplification::enabled());
    one_commit_wal_bytes();
    assert_eq!(wal_amplification::report(), Report::default());
}
