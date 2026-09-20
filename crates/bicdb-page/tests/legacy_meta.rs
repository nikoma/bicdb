//! A store whose meta page predates durable abort exceptions must be
//! readable — deliberately, not silently.
//!
//! Engines before durable abort exceptions wrote only the 40-byte meta
//! prefix; the extension region held whatever the reused page frame held.
//! The strict reader rejects that as corruption (correct for modern stores,
//! wrong for old ones), which is exactly what stranded a production store
//! behind a fake "1009 abort exceptions exceed the 1006-entry region" error.
//! These tests pin the contract: default opens fail closed with actionable
//! guidance, `accept_legacy_meta` reads and transforms the store once, and
//! afterwards the store opens strictly everywhere.

use bicdb_page::page::PAGE_HEADER_BYTES;
use bicdb_page::{PageStore, PageStoreOptions, PagedStore, PagedStoreOptions};
use tempfile::TempDir;

const PAGE_SIZE: u32 = 4096;

fn options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(PAGE_SIZE)
        .with_buffer_pool_bytes(2 * 1024 * 1024)
        .with_fsync(false)
}

fn key(index: usize) -> Vec<u8> {
    format!("row-{index:05}").into_bytes()
}

/// Build a store with committed rows and a truncated WAL, then overwrite the
/// meta extension region with bytes no current engine would write — the
/// signature of a pre-durable-abort-exceptions meta page.
fn build_legacy_store(dir: &TempDir, rows: usize) {
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        for index in 0..rows {
            let (xid, _) = store.begin_transaction();
            store.put(xid, &key(index), b"value").unwrap();
            store.commit(xid).unwrap();
        }
        // Truncate the WAL so recovery on the next open has nothing to replay
        // over the corruption below — mirroring an old store whose last writer
        // checkpointed and exited.
        store.checkpoint().unwrap();
    }
    let store = PageStore::open(
        dir.path().join("store.pages"),
        PageStoreOptions {
            page_size: PAGE_SIZE,
            fsync: false,
            create: false,
            extent_bytes: 0,
        },
    )
    .unwrap();
    let meta_page = store.root_page();
    assert_ne!(meta_page, 0, "checkpointed store must publish a meta page");
    let mut buffer = vec![0u8; PAGE_SIZE as usize];
    store.read_page(meta_page, &mut buffer).unwrap();
    let body = &mut buffer[PAGE_HEADER_BYTES..];
    // An exception count far past capacity: garbage that the strict reader
    // must reject, as observed on the stranded production store.
    body[40..48].copy_from_slice(&100_000u64.to_le_bytes());
    store.write_page(meta_page, &mut buffer).unwrap();
    store.flush().unwrap();
}

#[test]
fn default_open_fails_closed_with_guidance() {
    let dir = TempDir::new().unwrap();
    build_legacy_store(&dir, 20);

    let error = match PagedStore::open(dir.path(), options()) {
        Err(error) => error.to_string(),
        Ok(_) => panic!("default open must reject an invalid extension region"),
    };
    assert!(
        error.contains("abort exceptions exceed"),
        "error must name the validation failure: {error}"
    );
    assert!(
        error.contains("migrate-meta"),
        "error must point the operator at the migration: {error}"
    );
}

#[test]
fn accept_legacy_meta_reads_and_transforms_once() {
    let dir = TempDir::new().unwrap();
    build_legacy_store(&dir, 20);

    // Pass 1: opt-in open reads the legacy layout, keeps every committed row,
    // and durably rewrites the meta page in the current layout.
    {
        let (store, recovery) =
            PagedStore::open(dir.path(), options().with_accept_legacy_meta(true)).unwrap();
        assert!(recovery.legacy_meta_migrated, "transform must be reported");
        for index in 0..20 {
            assert!(
                store.get(&key(index)).unwrap().is_some(),
                "row {index} must survive the legacy read"
            );
        }
    }

    // Pass 2: the store now opens STRICTLY — no flag — and reads the same
    // rows. This is the whole point of transforming at open.
    let (store, recovery) = PagedStore::open(dir.path(), options()).unwrap();
    assert!(
        !recovery.legacy_meta_migrated,
        "a transformed store must not report another migration"
    );
    for index in 0..20 {
        assert!(store.get(&key(index)).unwrap().is_some());
    }

    // And the store still accepts new writes on top of the transformed page.
    let (xid, _) = store.begin_transaction();
    store.put(xid, b"post-migration", b"value").unwrap();
    store.commit(xid).unwrap();
    assert!(store.get(b"post-migration").unwrap().is_some());
}

#[test]
fn accept_legacy_meta_is_inert_on_a_current_store() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
        let (xid, _) = store.begin_transaction();
        store.put(xid, b"row", b"value").unwrap();
        store.commit(xid).unwrap();
        store.checkpoint().unwrap();
    }
    let (store, recovery) =
        PagedStore::open(dir.path(), options().with_accept_legacy_meta(true)).unwrap();
    assert!(
        !recovery.legacy_meta_migrated,
        "a valid extension region must never be treated as legacy"
    );
    assert!(store.get(b"row").unwrap().is_some());
}
