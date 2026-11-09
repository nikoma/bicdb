//! GAP-1 page-only work in a small buffer pool. Complete structural cuts
//! preserve atomic oversized backfills and whole bulk operations without
//! requiring external logical redo or refusing valid work.

use bicdb_core::{BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode};
use serde_json::json;

/// 4 KiB pages, 512 KiB pool: 128 frames, 16 a shard.
fn small_pool() -> DbConfig {
    let mut config = DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
        .with_paged_buffer_pool_bytes(512 * 1024);
    config.paged_page_size = 4096;
    config
}

fn roomy() -> DbConfig {
    let mut config = small_pool().with_paged_buffer_pool_bytes(256 * 1024 * 1024);
    config.paged_page_size = 4096;
    config
}

fn full_text(collection: &str) -> IndexDefinition {
    IndexDefinition {
        nulls_not_distinct: false,
        name: format!("{collection}_fts"),
        collection: collection.to_string(),
        fields: vec![IndexField::MetadataPath(vec!["terms".to_string()])],
        unique: false,
        kind: IndexKind::FullText,
        predicate: None,
        exclusion: None,
    }
}

fn btree(collection: &str) -> IndexDefinition {
    IndexDefinition {
        nulls_not_distinct: false,
        name: format!("{collection}_value"),
        collection: collection.to_string(),
        fields: vec![IndexField::MetadataPath(vec!["value".to_string()])],
        unique: false,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    }
}

/// Load `rows` rows in pool-sized batches, then reopen so the collection is
/// paged-primary, the state an index is created on.
fn loaded(dir: &std::path::Path, config: DbConfig, collection: &str, rows: usize) -> BicDb {
    {
        let mut db = BicDb::open_with_config(dir, config.clone()).unwrap();
        db.create_collection(collection).unwrap();
        db.close().unwrap();
    }
    let mut db = BicDb::open_with_config(dir, config.clone()).unwrap();
    for chunk in (0..rows).collect::<Vec<_>>().chunks(200) {
        db.bulk_load_insert(
            collection,
            chunk.iter().map(|index| {
                Record::new(format!("doc-{index:06}")).with_metadata(json!({
                    "terms": ["common", format!("rare{index}")],
                    "value": format!("v-{:08}", (index * 7919) % 1_000_003),
                }))
            }),
        )
        .unwrap();
    }
    db.close().unwrap();
    BicDb::open_with_config(dir, config).unwrap()
}

/// A row commit whose page apply fails halfway through a B-tree split (the
/// test hook between rebuilding the split leaf and recording the split in
/// its parent). The page store poisons the transaction and fails closed, so
/// the half-done split is never logged: not by the abort, not by a later
/// commit. The commit is reported as failed; its decision is already in the
/// transaction log, so after the restart it is replayed whole (or, had the
/// decision not been durable, absent). Every acknowledged row survives.
#[test]
fn a_page_apply_that_fails_mid_split_recovers_every_acknowledged_row() {
    let dir = tempfile::tempdir().unwrap();
    let config = || {
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_sync_outbox(false)
    };
    let mut acknowledged = Vec::new();
    {
        let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
        db.create_collection("q1").unwrap();
        for i in 0..150u64 {
            let mut tx = db.begin_transaction().unwrap();
            tx.insert(
                "q1",
                Record::new(format!("r-{i:05}")).with_metadata(json!({"i": i})),
            )
            .unwrap();
            tx.commit().unwrap();
            acknowledged.push(format!("r-{i:05}"));
        }
        db.close().unwrap();
    }
    {
        let db = BicDb::open_with_config(dir.path(), config()).unwrap();
        let fault =
            bicdb_page::test_faults::arm_io_failure("btree.insert_separator", dir.path(), 1, false);
        let mut tx = db.begin_transaction().unwrap();
        for i in 0..2_000u64 {
            tx.insert(
                "q1",
                Record::new(format!("a-{i:05}")).with_metadata(json!({"i": i})),
            )
            .unwrap();
        }
        let committed = tx.commit();
        assert!(fault.fired() >= 1, "the page apply never split");
        assert!(
            committed.is_err(),
            "a page apply that failed mid-split reported success"
        );
        drop(fault);
        // No clean close: the store is fenced, so dropping the handle writes
        // nothing more to it.
        drop(db);
    }
    let db = BicDb::open_with_config(dir.path(), config())
        .unwrap_or_else(|error| panic!("reopen after the failed page apply: {error}"));
    let mut missing = Vec::new();
    for id in &acknowledged {
        match db.get("q1", id) {
            Ok(Some(_)) => {}
            other => missing.push(format!("{id}: {other:?}")),
        }
    }
    let replayed = db
        .scan_collection("q1")
        .unwrap()
        .iter()
        .filter(|row| row.id.starts_with("a-"))
        .count();
    assert!(
        missing.is_empty(),
        "{} of 150 acknowledged rows lost, e.g. {:?}",
        missing.len(),
        &missing[..missing.len().min(5)]
    );
    assert!(
        replayed == 0 || replayed == 2_000,
        "the failed transaction is partial after recovery: {replayed} of 2000"
    );
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("q1", Record::new("after")).unwrap();
    tx.commit().expect("the recovered store must accept writes");
}

/// Structural cuts replace the interim refusal with complete atomic backfill.
/// Check every ID through the index immediately and after repeated recovery.
#[test]
fn structural_cuts_complete_index_backfills_larger_than_the_pool() {
    const ROWS: usize = 30_000;
    let expected = (0..ROWS)
        .map(|i| format!("doc-{i:06}"))
        .collect::<std::collections::BTreeSet<_>>();
    for (label, definition) in [("b-tree", btree("rows")), ("full-text", full_text("rows"))] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = loaded(dir.path(), small_pool(), "rows", ROWS);
        db.create_index(definition.clone()).unwrap();
        for recovery in 0..3 {
            assert_eq!(db.scan_collection("rows").unwrap().len(), ROWS);
            let ids = if definition.kind == IndexKind::FullText {
                db.lookup_full_text_term(&definition.name, "common", false)
                    .unwrap()
            } else {
                (0..ROWS)
                    .map(|i| {
                        let ids = db
                            .lookup_index(
                                &definition.name,
                                &[bicdb_core::IndexValue::from(format!(
                                    "v-{:08}",
                                    (i * 7919) % 1_000_003
                                ))],
                            )
                            .unwrap();
                        assert_eq!(
                            ids,
                            vec![format!("doc-{i:06}")],
                            "{label}: recovery {recovery}"
                        );
                        ids.into_iter().next().unwrap()
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(ids.len(), ROWS, "{label}: recovery {recovery}");
            assert_eq!(
                ids.into_iter().collect::<std::collections::BTreeSet<_>>(),
                expected,
                "{label}: recovery {recovery}"
            );
            for i in (0..ROWS).step_by(97) {
                let ids = if definition.kind == IndexKind::FullText {
                    db.lookup_full_text_term(&definition.name, &format!("rare{i}"), false)
                        .unwrap()
                } else {
                    db.lookup_index(
                        &definition.name,
                        &[bicdb_core::IndexValue::from(format!(
                            "v-{:08}",
                            (i * 7919) % 1_000_003
                        ))],
                    )
                    .unwrap()
                };
                assert_eq!(
                    ids,
                    vec![format!("doc-{i:06}")],
                    "{label}: recovery {recovery}"
                );
            }
            db.close().unwrap();
            db = BicDb::open_with_config(dir.path(), small_pool()).unwrap();
        }
    }
}
