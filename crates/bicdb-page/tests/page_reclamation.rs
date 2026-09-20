//! GC 3/3 — page-level reclamation: vacuum returns FULLY-DEAD heap pages to
//! the free list, reuse can never alias a stale locator (the catalog keeps a
//! per-page generation FLOOR across tenancies), and a checkpoint with an
//! empty WAL truncates the trailing run of free pages off the file.

use bicdb_page::{PagedStore, PagedStoreOptions, VacuumCursor, VacuumLimits};
use tempfile::TempDir;

fn options() -> PagedStoreOptions {
    PagedStoreOptions::default().with_fsync(false)
}

fn open(dir: &TempDir) -> PagedStore {
    PagedStore::open(dir.path(), options()).unwrap().0
}

fn put_all(store: &PagedStore, keys: std::ops::Range<usize>, tag: &str) {
    let transaction = store.begin();
    for index in keys {
        store
            .put(
                transaction,
                format!("k{index:05}").as_bytes(),
                format!("{tag}-{index}-{}", "x".repeat(200)).as_bytes(),
            )
            .unwrap();
    }
    store.commit(transaction).unwrap();
}

fn delete_all(store: &PagedStore, keys: std::ops::Range<usize>) {
    let transaction = store.begin();
    for index in keys {
        store
            .delete(transaction, format!("k{index:05}").as_bytes())
            .unwrap();
    }
    store.commit(transaction).unwrap();
}

#[test]
fn vacuum_frees_fully_dead_pages_and_new_writes_reuse_them() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir);
    put_all(&store, 0..500, "first");
    delete_all(&store, 0..500);

    let report = store.vacuum(usize::MAX).unwrap();
    assert!(
        report.pages_freed > 0,
        "500 deleted 200-byte rows must leave fully-dead pages: {report:?}"
    );
    let store_pages = store.buffer_pool().store().page_count();
    let free_before = store.buffer_pool().store().free_page_count();
    assert!(free_before >= report.pages_freed);

    // New rows must be absorbed by the freed pages, not grow the file.
    put_all(&store, 1000..1400, "second");
    let grown = store.buffer_pool().store().page_count() - store_pages;
    assert!(
        grown <= 2,
        "reuse should absorb the second wave (grew {grown} pages)"
    );
    for index in 1000..1400 {
        let value = store.get(format!("k{index:05}").as_bytes()).unwrap();
        assert!(value.is_some(), "k{index:05} lost after reuse");
    }
}

/// The alias regression the generation floor exists to kill: a key whose
/// whole chain died on a page that was freed and REUSED must read as gone —
/// never as some new tenant's bytes.
#[test]
fn reused_page_never_aliases_a_stale_locator() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir);
    put_all(&store, 0..300, "victim");
    delete_all(&store, 0..300);
    let report = store.vacuum(usize::MAX).unwrap();
    assert!(report.pages_freed > 0);

    // Refill the freed pages with new tuples, enough to re-mint every slot.
    put_all(&store, 5000..5300, "tenant");

    // Every victim key still has its index entry (sweep has not run) whose
    // head locator points into a reused page. It must resolve to NOTHING.
    for index in 0..300 {
        let read = store.get(format!("k{index:05}").as_bytes()).unwrap();
        assert_eq!(
            read, None,
            "k{index:05} resurrected with a new tenant's bytes"
        );
    }
    // And the sweep can now retire those keys entirely.
    let swept = store.sweep_dead_index_entries(usize::MAX).unwrap();
    assert!(swept >= 300, "sweep retired only {swept} keys");
}

/// Deterministic truncation check at the PageStore level: free a run of
/// trailing pages plus one interior page, truncate, and verify the file
/// shrinks to the interior hole while the list survives with exactly that
/// hole on it.
#[test]
fn trailing_free_pages_truncate_and_interior_holes_survive() {
    use bicdb_page::{PageStore, PageStoreOptions, PageType};
    let dir = TempDir::new().unwrap();
    let store = PageStore::open(
        dir.path().join("t.pages"),
        PageStoreOptions {
            fsync: false,
            ..Default::default()
        },
    )
    .unwrap();
    let pages: Vec<_> = (0..10)
        .map(|_| store.allocate(PageType::Heap).unwrap())
        .collect();
    // Free an interior page and the trailing three.
    store.free(pages[2]).unwrap();
    store.free(pages[9]).unwrap();
    store.free(pages[8]).unwrap();
    store.free(pages[7]).unwrap();
    let before = store.page_count();
    let dropped = store.truncate_trailing_free_pages().unwrap();
    assert_eq!(dropped, 3, "exactly the trailing run must go");
    assert_eq!(store.page_count(), before - 3);
    assert_eq!(store.free_page_count(), 1, "the interior hole stays listed");
    let file_len = std::fs::metadata(dir.path().join("t.pages")).unwrap().len();
    assert_eq!(file_len, store.page_count() * u64::from(store.page_size()));
    // The surviving free page must still be allocatable, then fresh growth
    // resumes past the truncated boundary.
    let reused = store.allocate(PageType::Heap).unwrap();
    assert_eq!(reused, pages[2], "interior hole must be handed out first");
    let fresh = store.allocate(PageType::Heap).unwrap();
    assert_eq!(
        fresh,
        before - 3,
        "growth resumes at the truncated boundary"
    );
}

/// Checkpoint runs the truncation opportunistically; whatever it decides,
/// the store must stay fully readable and writable.
#[test]
fn checkpoint_with_free_pages_stays_consistent() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir);
    put_all(&store, 0..100, "keep");
    put_all(&store, 100..800, "drop");
    delete_all(&store, 100..800);
    store.vacuum(usize::MAX).unwrap();
    let before = store.buffer_pool().store().page_count();
    store.checkpoint().unwrap();
    let after = store.buffer_pool().store().page_count();
    assert!(after <= before);
    let file_len = std::fs::metadata(dir.path().join("store.pages"))
        .unwrap()
        .len();
    assert_eq!(
        file_len,
        after * u64::from(store.buffer_pool().store().page_size())
    );
    for index in 0..100 {
        assert!(store
            .get(format!("k{index:05}").as_bytes())
            .unwrap()
            .is_some());
    }
    put_all(&store, 2000..2100, "post");
    for index in 2000..2100 {
        assert!(store
            .get(format!("k{index:05}").as_bytes())
            .unwrap()
            .is_some());
    }
}

/// Crash between the vacuum's page-free and the next checkpoint: recovery
/// replays the WAL over the file and must come back consistent — live rows
/// readable, freed pages either on the list or leaked, never corrupt.
#[test]
fn reopen_after_vacuum_free_without_checkpoint_is_consistent() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(&dir);
        put_all(&store, 0..100, "keep");
        put_all(&store, 100..600, "drop");
        delete_all(&store, 100..600);
        let report = store.vacuum(usize::MAX).unwrap();
        assert!(report.pages_freed > 0);
        // No checkpoint, no clean close: drop simulates the crash.
    }
    let store = open(&dir);
    for index in 0..100 {
        assert!(
            store
                .get(format!("k{index:05}").as_bytes())
                .unwrap()
                .is_some(),
            "live row k{index:05} lost across crash-reopen"
        );
    }
    for index in 100..600 {
        assert_eq!(store.get(format!("k{index:05}").as_bytes()).unwrap(), None);
    }
    // The store must still be able to allocate and write.
    put_all(&store, 1000..1100, "after");
    for index in 1000..1100 {
        assert!(store
            .get(format!("k{index:05}").as_bytes())
            .unwrap()
            .is_some());
    }
}

#[test]
fn bounded_vacuum_cursor_round_trips_and_resumes_after_reopen() {
    let dir = TempDir::new().unwrap();
    let checkpoint = {
        let store = open(&dir);
        put_all(&store, 0..600, "resume");
        delete_all(&store, 0..600);
        let report = store
            .vacuum_step(
                VacuumCursor::default(),
                VacuumLimits {
                    max_pages: 2,
                    max_bytes: 1024 * 1024,
                    max_duration_millis: 10_000,
                },
            )
            .unwrap();
        assert!(!report.complete);
        assert!(report.next_cursor.next_page_id.is_some());
        serde_json::to_vec(&report.next_cursor).unwrap()
    };

    let store = open(&dir);
    let mut cursor: VacuumCursor = serde_json::from_slice(&checkpoint).unwrap();
    let mut complete = false;
    for _ in 0..1_000 {
        let report = store
            .vacuum_step(
                cursor,
                VacuumLimits {
                    max_pages: 2,
                    max_bytes: 1024 * 1024,
                    max_duration_millis: 10_000,
                },
            )
            .unwrap();
        if report.complete {
            complete = true;
            break;
        }
        cursor = report.next_cursor;
    }
    assert!(complete, "reopened vacuum cursor did not finish its sweep");
    for index in (0..600).step_by(37) {
        assert_eq!(store.get(format!("k{index:05}").as_bytes()).unwrap(), None);
    }
}

/// The floor must survive a full free -> reuse -> free -> reuse cycle across
/// a REOPEN (it lives in the durable catalog, not in page bytes).
#[test]
fn generation_floor_survives_reopen_and_repeated_reuse() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(&dir);
        put_all(&store, 0..300, "one");
        delete_all(&store, 0..300);
        assert!(store.vacuum(usize::MAX).unwrap().pages_freed > 0);
        store.checkpoint().unwrap();
    }
    {
        let store = open(&dir);
        put_all(&store, 300..600, "two");
        delete_all(&store, 300..600);
        assert!(store.vacuum(usize::MAX).unwrap().pages_freed > 0);
        // Both waves' keys must be gone, not aliased, after two tenancies.
        put_all(&store, 600..900, "three");
        for index in 0..600 {
            assert_eq!(
                store.get(format!("k{index:05}").as_bytes()).unwrap(),
                None,
                "k{index:05} aliased after repeated reuse"
            );
        }
        for index in 600..900 {
            assert!(store
                .get(format!("k{index:05}").as_bytes())
                .unwrap()
                .is_some());
        }
    }
}
