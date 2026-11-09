use bicdb_core::{BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode};
use serde_json::json;

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
}

/// Open, create the collection, and reopen so it is lazy/paged-primary —
/// the state a bulk importer loads into.
fn reopened_with_collection(dir: &std::path::Path, collection: &str) -> BicDb {
    let mut db = BicDb::open_with_config(dir, config()).unwrap();
    db.create_collection(collection).unwrap();
    db.close().unwrap();
    BicDb::open_with_config(dir, config()).unwrap()
}

fn record(id: &str, value: u64) -> Record {
    Record::new(id.to_string()).with_metadata(json!({ "value": value }))
}

#[test]
fn bulk_loaded_rows_are_readable_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = reopened_with_collection(dir.path(), "places");

    // Deliberately unsorted input: the API sorts internally.
    let records = (0..500u64)
        .rev()
        .map(|index| record(&format!("place-{index:05}"), index))
        .collect::<Vec<_>>();
    db.bulk_load_insert("places", records).unwrap();

    // Readable in the loading session through the paged fallback.
    for index in [0u64, 250, 499] {
        let row = db
            .get("places", &format!("place-{index:05}"))
            .unwrap()
            .unwrap_or_else(|| panic!("row {index} missing before reopen"));
        assert_eq!(row.metadata["value"], json!(index));
    }

    // Durable across reopen.
    db.close().unwrap();
    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    for index in [0u64, 123, 499] {
        let row = db
            .get("places", &format!("place-{index:05}"))
            .unwrap()
            .unwrap_or_else(|| panic!("row {index} missing after reopen"));
        assert_eq!(row.metadata["value"], json!(index));
    }
    assert_eq!(db.scan_collection("places").unwrap().len(), 500);
}

#[test]
fn bulk_load_repeats_are_idempotent_upserts() {
    // A crashed import replays its unfinished shard from the top; the same
    // ids must land once, with the replayed values winning.
    let dir = tempfile::tempdir().unwrap();
    let mut db = reopened_with_collection(dir.path(), "places");

    let first = (0..100u64)
        .map(|index| record(&format!("p-{index:04}"), index))
        .collect::<Vec<_>>();
    db.bulk_load_insert("places", first).unwrap();
    let replay = (0..100u64)
        .map(|index| record(&format!("p-{index:04}"), index + 1_000))
        .collect::<Vec<_>>();
    db.bulk_load_insert("places", replay).unwrap();

    assert_eq!(db.scan_collection("places").unwrap().len(), 100);
    let row = db.get("places", "p-0042").unwrap().unwrap();
    assert_eq!(row.metadata["value"], json!(1_042));
}

#[test]
fn indexed_collections_fall_back_to_the_checked_path() {
    // With a B-tree index present the bypass would skip entry maintenance;
    // the API must route through batch_insert instead, keeping the index
    // consistent.
    let dir = tempfile::tempdir().unwrap();
    let mut db = reopened_with_collection(dir.path(), "places");
    db.create_index(IndexDefinition {
        nulls_not_distinct: false,
        name: "places-by-value".to_string(),
        collection: "places".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["value".to_string()])],
        kind: IndexKind::BTree,
        unique: false,
        predicate: None,
        exclusion: None,
    })
    .unwrap();

    let records = (0..50u64)
        .map(|index| record(&format!("i-{index:03}"), index))
        .collect::<Vec<_>>();
    db.bulk_load_insert("places", records).unwrap();

    assert_eq!(db.scan_collection("places").unwrap().len(), 50);
    db.close().unwrap();
    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    assert_eq!(db.scan_collection("places").unwrap().len(), 50);
    let row = db.get("places", "i-007").unwrap().unwrap();
    assert_eq!(row.metadata["value"], json!(7));
}

/// The structural-cut option removes the interim whole-load refusal. Every
/// acknowledged row, including its payload, must survive repeated recovery.
#[test]
fn structural_cuts_accept_a_whole_bulk_load_larger_than_the_buffer_pool() {
    let dir = tempfile::tempdir().unwrap();
    let small = || config().with_paged_buffer_pool_bytes(256 * 1024);
    let mut db = BicDb::open_with_config(dir.path(), small()).unwrap();
    db.create_collection("places").unwrap();
    db.close().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), small()).unwrap();
    db.bulk_load_insert(
        "places",
        (0..50u64).map(|index| record(&format!("before-{index:05}"), index)),
    )
    .unwrap();
    db.bulk_load_insert(
        "places",
        (0..20_000u64).map(|index| record(&format!("big-{index:05}"), index)),
    )
    .unwrap();
    for recovery in 0..3 {
        let rows = db.scan_collection("places").unwrap();
        assert_eq!(rows.len(), 20_050, "recovery {recovery}");
        let by_id = rows
            .iter()
            .map(|row| (row.id.as_str(), &row.metadata))
            .collect::<std::collections::BTreeMap<_, _>>();
        for index in 0..20_000u64 {
            assert_eq!(
                by_id[format!("big-{index:05}").as_str()]["value"],
                json!(index),
                "recovery {recovery} / big row {index}"
            );
        }
        for index in 0..50u64 {
            assert_eq!(
                by_id[format!("before-{index:05}").as_str()]["value"],
                json!(index),
                "recovery {recovery} / preexisting row {index}"
            );
        }
        drop(by_id);
        drop(rows);
        db.close().unwrap();
        db = BicDb::open_with_config(dir.path(), small()).unwrap();
    }
}
