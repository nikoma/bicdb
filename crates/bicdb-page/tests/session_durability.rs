//! Data written by the second and later sessions must survive.
//!
//! A store is normally created by one process and written by the next. That
//! ordinary sequence lost everything: `open` decided a store was "fresh" from
//! the superblock alone, but the superblock is published at creation while the
//! meta page's *contents* only become durable when something logs or
//! checkpoints them. A session that created a store and exited without writing
//! left the superblock published over an unwritten meta page — so the next
//! session read "no magic, empty trees", built new roots, and then skipped
//! `write_meta` because it did not believe it was fresh. Those roots died with
//! the process, and the third session opened an empty database sitting on top
//! of every committed row.
//!
//! Nothing about it looks like a crash: every commit returns `Ok`, every read
//! within the writing session succeeds, and the loss only appears one reopen
//! later.

use bicdb_page::{PagedStore, PagedStoreOptions};
use tempfile::TempDir;

fn options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(4096)
        .with_fsync(false)
}

#[test]
fn a_store_created_by_one_session_keeps_what_the_next_session_writes() {
    let dir = TempDir::new().unwrap();

    // Session 1 creates the store and writes nothing at all.
    drop(PagedStore::open(dir.path(), options()).unwrap());

    // Session 2 writes.
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        for index in 0..200u32 {
            let (xid, _) = store.begin_transaction();
            store
                .put(xid, format!("k{index:05}").as_bytes(), b"value")
                .unwrap();
            store.commit(xid).unwrap();
        }
    }

    // Session 3 must still see all of it.
    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    let mut missing = Vec::new();
    for index in 0..200u32 {
        if store
            .get(format!("k{index:05}").as_bytes())
            .unwrap()
            .is_none()
        {
            missing.push(index);
        }
    }
    assert!(
        missing.is_empty(),
        "{} of 200 rows written by the second session were lost; first few: {:?}",
        missing.len(),
        &missing[..missing.len().min(10)]
    );
}

#[test]
fn writes_survive_across_many_sessions() {
    // Each session both reads what every earlier one wrote and adds its own, so
    // a regression shows up as the session at which the chain breaks.
    let dir = TempDir::new().unwrap();
    for session in 0..5u32 {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        for earlier in 0..session {
            assert!(
                store
                    .get(format!("s{earlier}").as_bytes())
                    .unwrap()
                    .is_some(),
                "session {session} cannot see the row written by session {earlier}"
            );
        }
        let (xid, _) = store.begin_transaction();
        store
            .put(xid, format!("s{session}").as_bytes(), b"v")
            .unwrap();
        store.commit(xid).unwrap();
    }
}

#[test]
fn an_empty_store_reopens_without_reinitializing() {
    // The complement: opening repeatedly without writing must not keep treating
    // the store as new, nor allocate a fresh meta page each time.
    let dir = TempDir::new().unwrap();
    for _ in 0..4 {
        drop(PagedStore::open(dir.path(), options()).unwrap());
    }
    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    let (xid, _) = store.begin_transaction();
    store.put(xid, b"after", b"reopens").unwrap();
    store.commit(xid).unwrap();
    drop(store);

    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    assert_eq!(
        store.get(b"after").unwrap().as_deref(),
        Some(b"reopens".as_slice())
    );
}
