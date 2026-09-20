//! B11: after vacuum reclaims versions that are dead to everyone, every
//! subsequent operation on the affected keys must behave as if the history
//! simply isn't there — never surface "slot is dead" errors.

use bicdb_page::{PagedStore, PagedStoreOptions};

fn store(dir: &tempfile::TempDir) -> PagedStore {
    PagedStore::open(dir.path(), PagedStoreOptions::default().with_fsync(false))
        .unwrap()
        .0
}

#[test]
fn reads_and_writes_survive_vacuumed_history() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);

    // History shape 1: insert -> delete -> vacuum (whole chain dead).
    let x1 = store.begin();
    store.put(x1, b"k-deleted", b"v1").unwrap();
    store.commit(x1).unwrap();
    let x2 = store.begin();
    store.delete(x2, b"k-deleted").unwrap();
    store.commit(x2).unwrap();

    // History shape 2: insert -> update -> update (old versions dead).
    let x3 = store.begin();
    store.put(x3, b"k-updated", b"v1").unwrap();
    store.commit(x3).unwrap();
    let x4 = store.begin();
    store.put(x4, b"k-updated", b"v2").unwrap();
    store.commit(x4).unwrap();
    let x5 = store.begin();
    store.put(x5, b"k-updated", b"v3").unwrap();
    store.commit(x5).unwrap();

    let report = store.vacuum(usize::MAX).unwrap();
    assert!(report.versions_reclaimed > 0, "vacuum reclaimed nothing");

    // Point reads.
    assert_eq!(store.get(b"k-deleted").unwrap(), None, "deleted key get");
    assert_eq!(
        store.get(b"k-updated").unwrap().as_deref(),
        Some(b"v3".as_ref()),
        "updated key get"
    );
    // Scans.
    let snapshot = store.latest_snapshot();
    let visible: Vec<Vec<u8>> = store
        .scan_from(&snapshot, b"")
        .unwrap()
        .map(|entry| entry.map(|(key, _)| key))
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(visible.contains(&b"k-updated".to_vec()));
    assert!(!visible.contains(&b"k-deleted".to_vec()));

    // Writes over vacuumed history: re-insert the deleted key, update and
    // delete the updated key.
    let x6 = store.begin();
    store.put(x6, b"k-deleted", b"v2").unwrap();
    store.commit(x6).unwrap();
    assert_eq!(
        store.get(b"k-deleted").unwrap().as_deref(),
        Some(b"v2".as_ref())
    );
    let x7 = store.begin();
    store.delete(x7, b"k-updated").unwrap();
    store.commit(x7).unwrap();
    assert_eq!(store.get(b"k-updated").unwrap(), None);

    // Vacuum again over the new state, then read once more.
    store.vacuum(usize::MAX).unwrap();
    assert_eq!(
        store.get(b"k-deleted").unwrap().as_deref(),
        Some(b"v2".as_ref())
    );
    assert_eq!(store.get(b"k-updated").unwrap(), None);
}

/// Index-entry GC: after vacuum retires a key's whole chain, the sweep
/// removes the KEY — and only such keys.
#[test]
fn sweep_removes_only_fully_dead_keys() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    // k-live stays; k-gone is deleted then vacuumed; k-history has an old
    // dead version but a LIVE head.
    let x = store.begin();
    store.put(x, b"k-live", b"v").unwrap();
    store.put(x, b"k-gone", b"v").unwrap();
    store.put(x, b"k-history", b"v1").unwrap();
    store.commit(x).unwrap();
    let x = store.begin();
    store.delete(x, b"k-gone").unwrap();
    store.put(x, b"k-history", b"v2").unwrap();
    store.commit(x).unwrap();

    store.vacuum(usize::MAX).unwrap();
    let removed = store.sweep_dead_index_entries(usize::MAX).unwrap();
    assert_eq!(removed, 1, "exactly the fully-dead key");

    assert_eq!(
        store.get(b"k-live").unwrap().as_deref(),
        Some(b"v".as_ref())
    );
    assert_eq!(
        store.get(b"k-history").unwrap().as_deref(),
        Some(b"v2".as_ref())
    );
    assert_eq!(store.get(b"k-gone").unwrap(), None);
    // The key is REALLY gone from the tree (scan sees no trace), and can be
    // re-inserted cleanly.
    let snapshot = store.latest_snapshot();
    let keys: Vec<Vec<u8>> = store
        .scan_from(&snapshot, b"")
        .unwrap()
        .map(|entry| entry.map(|(key, _)| key))
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(!keys.contains(&b"k-gone".to_vec()));
    let x = store.begin();
    store.put(x, b"k-gone", b"v-again").unwrap();
    store.commit(x).unwrap();
    assert_eq!(
        store.get(b"k-gone").unwrap().as_deref(),
        Some(b"v-again".as_ref())
    );
}
