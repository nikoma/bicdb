//! TID-style hinted reads: `get_as_of_hinted` must return exactly what the
//! key descent would — or `Fallback`, never a fabricated result — across every
//! way a hint can go stale: updates, deletes, old snapshots, vacuumed slots,
//! and slot reuse.

use bicdb_page::{HintedRead, PagedStore, PagedStoreOptions, TupleLocator};

fn store(dir: &tempfile::TempDir) -> PagedStore {
    PagedStore::open(dir.path(), PagedStoreOptions::default().with_fsync(false))
        .unwrap()
        .0
}

fn put_committed(store: &PagedStore, key: &[u8], value: &[u8]) -> (TupleLocator, u64) {
    let xid = store.begin();
    let locator = store.put_returning_locator(xid, key, value).unwrap();
    store.commit(xid).unwrap();
    (locator, xid)
}

#[test]
fn hinted_read_hits_the_live_version() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    let (locator, xmin) = put_committed(&store, b"k", b"v1");

    let snapshot = store.latest_snapshot();
    assert_eq!(
        store.get_as_of_hinted(&snapshot, locator, xmin).unwrap(),
        HintedRead::Hit(b"v1".to_vec())
    );
}

#[test]
fn superseded_hint_falls_back_and_old_snapshot_still_hits() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    let (old_locator, old_xmin) = put_committed(&store, b"k", b"v1");
    let before_update = store.latest_snapshot();

    let (new_locator, new_xmin) = put_committed(&store, b"k", b"v2");

    // Under the LATEST snapshot the old hint is superseded: fall back, never
    // serve v1 as current.
    let latest = store.latest_snapshot();
    assert_eq!(
        store
            .get_as_of_hinted(&latest, old_locator, old_xmin)
            .unwrap(),
        HintedRead::Fallback
    );
    // The fresh hint hits.
    assert_eq!(
        store
            .get_as_of_hinted(&latest, new_locator, new_xmin)
            .unwrap(),
        HintedRead::Hit(b"v2".to_vec())
    );
    // A reader still holding the pre-update snapshot walks the fresh hint's
    // prev link down to v1 — same answer as the key descent.
    assert_eq!(
        store
            .get_as_of_hinted(&before_update, new_locator, new_xmin)
            .unwrap(),
        HintedRead::Hit(b"v1".to_vec())
    );
    assert_eq!(
        store.get_as_of(&before_update, b"k").unwrap(),
        Some(b"v1".to_vec())
    );
}

#[test]
fn deleted_row_hint_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    let (locator, xmin) = put_committed(&store, b"k", b"v1");

    let xid = store.begin();
    store.delete(xid, b"k").unwrap();
    store.commit(xid).unwrap();

    // The delete stamped xmax on the hinted version: visible xmax => the hint
    // cannot answer; the descent correctly reports the row gone.
    let latest = store.latest_snapshot();
    assert_eq!(
        store.get_as_of_hinted(&latest, locator, xmin).unwrap(),
        HintedRead::Fallback
    );
    assert_eq!(store.get_as_of(&latest, b"k").unwrap(), None);
}

#[test]
fn wrong_xmin_is_a_stale_hint_not_a_hit() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    let (locator, xmin) = put_committed(&store, b"k", b"v1");

    let snapshot = store.latest_snapshot();
    // A mismatched identity stamp models a vacuumed-and-reused slot: whatever
    // tuple sits there now is not the one the hint was written for.
    assert_eq!(
        store
            .get_as_of_hinted(&snapshot, locator, xmin + 1)
            .unwrap(),
        HintedRead::Fallback
    );
}

#[test]
fn vacuumed_slot_hint_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    let (v1_locator, v1_xmin) = put_committed(&store, b"k", b"v1");
    put_committed(&store, b"k", b"v2");
    put_committed(&store, b"k", b"v3");

    let report = store.vacuum(usize::MAX).unwrap();
    assert!(report.versions_reclaimed > 0, "vacuum reclaimed nothing");

    // The v1 slot is reclaimed (possibly reused): the stale hint must fall
    // back, and the descent still serves the live version.
    let latest = store.latest_snapshot();
    assert_eq!(
        store
            .get_as_of_hinted(&latest, v1_locator, v1_xmin)
            .unwrap(),
        HintedRead::Fallback
    );
    assert_eq!(
        store.get_as_of(&latest, b"k").unwrap(),
        Some(b"v3".to_vec())
    );
}

#[test]
fn heads_scan_yields_hints_that_hit() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    for row in 0..100u32 {
        put_committed(&store, format!("k{row:03}").as_bytes(), &row.to_be_bytes());
    }
    // One key gets history so a head differs from its oldest version.
    put_committed(&store, b"k050", b"updated");

    let snapshot = store.latest_snapshot();
    let mut rows = 0;
    for entry in store.scan_from_with_heads(&snapshot, b"k").unwrap() {
        let (key, value, head, head_xmin) = entry.unwrap();
        rows += 1;
        // Every yielded hint must reproduce the scan's own answer.
        match store.get_as_of_hinted(&snapshot, head, head_xmin).unwrap() {
            HintedRead::Hit(hinted) => assert_eq!(hinted, value, "key {key:?}"),
            HintedRead::Fallback => panic!("fresh head hint fell back for {key:?}"),
        }
    }
    assert_eq!(rows, 100);
}
