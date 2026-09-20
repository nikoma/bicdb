//! Per-collection durable intern dictionary (the identity behind v3 ordered
//! index entries): allocation is monotonic, deduplicated, transactional, and
//! durable; retirement fences ids across a delete boundary.

use bicdb_core::{PagedRecords, PagedRecordsOptions};
use tempfile::TempDir;

fn paged(dir: &TempDir) -> PagedRecords {
    PagedRecords::open(
        dir.path().join("paged"),
        PagedRecordsOptions {
            fsync: false,
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn allocation_is_monotonic_deduplicated_and_durable() {
    let dir = TempDir::new().unwrap();
    {
        let paged = paged(&dir);
        let (xid, _) = paged.begin();
        let a = paged.intern_or_alloc(xid, "places", "row-a").unwrap();
        let b = paged.intern_or_alloc(xid, "places", "row-b").unwrap();
        // Same pk twice in one transaction: one id.
        let a_again = paged.intern_or_alloc(xid, "places", "row-a").unwrap();
        assert_eq!((a, b, a_again), (0, 1, 0));
        // Another collection has its own id space.
        let other = paged.intern_or_alloc(xid, "orders", "row-a").unwrap();
        assert_eq!(other, 0);
        paged.commit(xid).unwrap();

        // A later transaction resolves committed ids and continues the counter.
        let (xid, _) = paged.begin();
        assert_eq!(paged.intern_or_alloc(xid, "places", "row-a").unwrap(), 0);
        assert_eq!(paged.intern_or_alloc(xid, "places", "row-c").unwrap(), 2);
        paged.commit(xid).unwrap();

        let snapshot = paged.latest_snapshot();
        assert_eq!(
            paged.pk_for_intern_id(&snapshot, "places", 1).unwrap(),
            Some("row-b".to_string())
        );
        assert_eq!(
            paged
                .intern_id_for_pk(&snapshot, "places", "row-c")
                .unwrap(),
            Some(2)
        );
        assert_eq!(
            paged.pk_for_intern_id(&snapshot, "places", 9).unwrap(),
            None
        );
    }
    // Reopen: everything persisted, counter continues where it left off.
    let paged = paged(&dir);
    let snapshot = paged.latest_snapshot();
    assert_eq!(
        paged.pk_for_intern_id(&snapshot, "places", 2).unwrap(),
        Some("row-c".to_string())
    );
    let (xid, _) = paged.begin();
    assert_eq!(paged.intern_or_alloc(xid, "places", "row-d").unwrap(), 3);
    paged.commit(xid).unwrap();
}

#[test]
fn aborted_allocation_rolls_back_atomically() {
    let dir = TempDir::new().unwrap();
    let paged = paged(&dir);
    let (xid, _) = paged.begin();
    assert_eq!(paged.intern_or_alloc(xid, "places", "row-a").unwrap(), 0);
    paged.commit(xid).unwrap();

    let (xid, _) = paged.begin();
    assert_eq!(paged.intern_or_alloc(xid, "places", "row-b").unwrap(), 1);
    paged.abort(xid).unwrap();

    // The aborted allocation left nothing behind: mapping gone, counter
    // rolled back, the next allocation reuses the id.
    let snapshot = paged.latest_snapshot();
    assert_eq!(
        paged
            .intern_id_for_pk(&snapshot, "places", "row-b")
            .unwrap(),
        None
    );
    let (xid, _) = paged.begin();
    assert_eq!(paged.intern_or_alloc(xid, "places", "row-c").unwrap(), 1);
    paged.commit(xid).unwrap();
}

#[test]
fn retirement_fences_ids_across_a_delete_boundary() {
    let dir = TempDir::new().unwrap();
    let paged = paged(&dir);
    let (xid, _) = paged.begin();
    assert_eq!(paged.intern_or_alloc(xid, "places", "row-a").unwrap(), 0);
    paged.commit(xid).unwrap();
    let before_delete = paged.latest_snapshot();

    let (xid, _) = paged.begin();
    paged.retire_intern(xid, "places", "row-a").unwrap();
    paged.commit(xid).unwrap();

    // Current readers see the mapping gone; the pre-delete snapshot still
    // resolves it (MVCC delete, not an erasure).
    let snapshot = paged.latest_snapshot();
    assert_eq!(
        paged
            .intern_id_for_pk(&snapshot, "places", "row-a")
            .unwrap(),
        None
    );
    assert_eq!(
        paged.pk_for_intern_id(&snapshot, "places", 0).unwrap(),
        None
    );
    assert_eq!(
        paged
            .pk_for_intern_id(&before_delete, "places", 0)
            .unwrap()
            .as_deref(),
        Some("row-a")
    );

    // Re-inserting the same pk allocates a FRESH id, never id 0 again.
    let (xid, _) = paged.begin();
    assert_eq!(paged.intern_or_alloc(xid, "places", "row-a").unwrap(), 1);
    paged.commit(xid).unwrap();
}
