use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use std::ops::Bound;

#[test]
fn key_batches_bound_resident_paged_and_pending_overlays_without_skipping_rows() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default().with_storage_mode(mode.clone());
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        db.create_collection("rows").unwrap();
        db.batch_insert("rows", (0..200).map(|id| Record::new(format!("{id:03}"))))
            .unwrap();
        for reopen in [false, true] {
            if reopen {
                db.close().unwrap();
                db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            }
            for descending in [false, true] {
                let mut tx = db.begin_transaction().unwrap();
                for id in 200..220 {
                    tx.insert("rows", Record::new(format!("{id:03}"))).unwrap();
                }
                for id in (0..200).step_by(7) {
                    tx.delete("rows", &format!("{id:03}")).unwrap();
                }
                let mut cursor: Option<String> = None;
                let mut found = Vec::new();
                let mut previous = None;
                loop {
                    let lower = if descending {
                        Bound::Included("020")
                    } else {
                        cursor
                            .as_deref()
                            .map(Bound::Excluded)
                            .unwrap_or(Bound::Included("020"))
                    };
                    let upper = if descending {
                        cursor
                            .as_deref()
                            .map(Bound::Excluded)
                            .unwrap_or(Bound::Excluded("210"))
                    } else {
                        Bound::Excluded("210")
                    };
                    let (ids, complete) = tx
                        .scan_collection_record_key_batch_in_range_unchecked(
                            "rows", lower, upper, descending, 7,
                        )
                        .unwrap();
                    assert!(
                        ids.len() <= 7,
                        "{mode} reopen={reopen} descending={descending}"
                    );
                    assert!(!ids.is_empty() || complete);
                    for id in &ids {
                        if let Some(previous) = &previous {
                            assert!(
                                if descending {
                                    id < previous
                                } else {
                                    id > previous
                                },
                                "cursor must progress strictly"
                            );
                        }
                        previous = Some(id.clone());
                        if tx.get("rows", id).unwrap().is_some() {
                            found.push(id.clone());
                        }
                    }
                    if complete {
                        break;
                    }
                    cursor = ids.last().cloned();
                }
                let mut expected = (20..210)
                    .filter(|id| *id >= 200 || id % 7 != 0)
                    .map(|id| format!("{id:03}"))
                    .collect::<Vec<_>>();
                if descending {
                    expected.reverse();
                }
                assert_eq!(
                    found, expected,
                    "{mode} reopen={reopen} descending={descending}"
                );
                assert!(tx
                    .scan_collection_record_key_batch_in_range_unchecked(
                        "rows",
                        Bound::Unbounded,
                        Bound::Unbounded,
                        false,
                        0
                    )
                    .is_err());
                tx.rollback().unwrap();
            }
        }
        db.close().unwrap();
    }
}
