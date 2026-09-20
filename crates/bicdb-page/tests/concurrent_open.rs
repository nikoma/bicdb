//! Two page stores must never own the same directory at once.
//!
//! A `PagedStore` keeps a buffer pool of dirty pages, a WAL it truncates at
//! checkpoints, and a meta page recording the index root and transaction
//! watermarks. All three are *authoritative in memory* between checkpoints. Two
//! stores over one directory therefore each believe they own that state, and the
//! last one to flush wins — silently discarding the other's committed
//! transactions.
//!
//! This is not a hypothetical. It is how a plain `open`-twice pattern (easy to
//! write by accident, and legal against the in-memory engine) destroys committed
//! data, and it is what two *processes* would do to each other with no lock.

use bicdb_page::{PagedStore, PagedStoreOptions};
use tempfile::TempDir;

fn options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(4096)
        .with_buffer_pool_bytes(1024 * 1024)
        .with_fsync(false)
}

#[test]
fn a_second_open_of_the_same_directory_is_refused() {
    let dir = TempDir::new().unwrap();
    let (first, _) = PagedStore::open(dir.path(), options()).expect("first open should succeed");

    let error = PagedStore::open(dir.path(), options())
        .err()
        .expect("a second concurrent open must be refused, not silently allowed");
    let message = error.to_string();
    assert!(
        message.contains("already open") || message.contains("locked"),
        "the refusal should say the database is already open, said: {message}"
    );

    drop(first);
}

#[test]
fn the_lock_is_released_when_the_store_is_dropped() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        let (xid, _) = store.begin_transaction();
        store.put(xid, b"k", b"v").unwrap();
        store.commit(xid).unwrap();
    }

    // Reopening after a clean drop must work, or the lock would be a one-shot
    // that bricks the database.
    let (store, _) = PagedStore::open(dir.path(), options())
        .expect("reopen after drop should succeed once the lock is released");
    assert_eq!(store.get(b"k").unwrap().as_deref(), Some(b"v".as_slice()));
}

// A lock left behind by a process killed with `SIGKILL` must not block
// reopening, or every crash would need manual repair — turning a crash-safe
// engine into an operational hazard. That is a property of holding the lock via
// the OS (released when the process dies) rather than via a "file exists" check.
//
// It is deliberately not asserted here: within one process the lock is held by
// the open file description, so `mem::forget`ing a store leaks the descriptor
// and keeps the lock held — the test would fail against a *correct*
// implementation. `tests/kill_recovery.rs` covers it for real, reopening in the
// parent after `SIGKILL`ing an actual child; a stale-lock bug fails those tests.

#[test]
fn a_reader_is_not_locked_out_by_being_in_the_same_process() {
    // The lock is per-directory, so distinct databases stay independent. Without
    // this, one lock taken too broadly would serialize unrelated databases.
    let first_dir = TempDir::new().unwrap();
    let second_dir = TempDir::new().unwrap();
    let (_first, _) = PagedStore::open(first_dir.path(), options()).unwrap();
    let (_second, _) = PagedStore::open(second_dir.path(), options())
        .expect("a different directory must not be blocked by another's lock");
}
