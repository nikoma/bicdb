//! v3 entries through the full BicDb surface: a newly created paged B-tree
//! index writes intern-keyed entries (verified at the durable layer), serves
//! every lookup shape, engages TID hints on the fetch, enforces uniqueness,
//! and handles update/re-key/delete row lifecycles.

use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexEntryRef, IndexField, IndexKind, IndexValue,
    PagedRecords, PagedRecordsOptions, Record, StorageMode,
};
use serde_json::json;
use tempfile::TempDir;

/// Opt this process into v3 creation BEFORE the gate's OnceLock initializes.
/// Every test calls it first; all writers agree on the value, so the race
/// between parallel tests is benign.
fn opt_into_v3() {
    std::env::set_var("BICDB_PAGED_V3_ENTRIES", "1");
}

fn paged_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

fn year_index(unique: bool) -> IndexDefinition {
    IndexDefinition {
        name: "articles_year".to_string(),
        collection: "articles".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["year".to_string()])],
        unique,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    }
}

fn article(id: &str, year: i64) -> Record {
    Record::new(id).with_metadata(json!({ "year": year, "title": format!("t-{id}") }))
}

/// Entry refs of the CLOSED store — proof of what format is actually on disk.
fn durable_refs(dir: &TempDir, index: &str) -> Vec<IndexEntryRef> {
    let paged = PagedRecords::open(
        dir.path().join("paged"),
        PagedRecordsOptions {
            fsync: false,
            ..Default::default()
        },
    )
    .unwrap();
    let snapshot = paged.latest_snapshot();
    let mut keys: Vec<Vec<u8>> = paged
        .scan_index(&snapshot, index)
        .unwrap()
        .map(|entry| entry.map(|(encoded, _)| encoded))
        .collect::<Result<_, _>>()
        .unwrap();
    keys.dedup();
    let mut out = Vec::new();
    for encoded in keys {
        for entry in paged
            .scan_index_exact_refs(&snapshot, index, &encoded)
            .unwrap()
        {
            out.push(entry.unwrap().0);
        }
    }
    out
}

#[test]
fn new_index_writes_intern_entries_and_serves_all_lookups() {
    opt_into_v3();
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        db.create_index(year_index(false)).unwrap();
        for (id, year) in [("a", 2001), ("b", 2002), ("c", 2003), ("d", 2002)] {
            db.insert("articles", article(id, year)).unwrap();
        }
        db.close().unwrap();
    }
    // Every durable entry is intern-keyed — the pk string is out of the key.
    let refs = durable_refs(&dir, "articles_year");
    assert_eq!(refs.len(), 4);
    assert!(
        refs.iter().all(|r| matches!(r, IndexEntryRef::Intern(_))),
        "expected v3 entries, got {refs:?}"
    );

    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    // Point lookup + hinted fetch.
    assert_eq!(
        db.lookup_index("articles_year", &[IndexValue::from(2002)])
            .unwrap(),
        vec!["b".to_string(), "d".to_string()]
    );
    let (hits_before, _) = bicdb_core::paged_tid_hint_stats();
    let pks = db
        .lookup_index("articles_year", &[IndexValue::from(2003)])
        .unwrap();
    let records = db.get_records_by_pks("articles", &pks).unwrap();
    assert_eq!(records[0].as_ref().unwrap().metadata["title"], "t-c");
    let (hits_after, _) = bicdb_core::paged_tid_hint_stats();
    assert!(
        hits_after > hits_before,
        "v3 lookup did not engage TID hints"
    );

    // Range, ordered, and extreme shapes.
    assert_eq!(
        db.range_index(
            "articles_year",
            Some(&IndexValue::from(2002)),
            Some(&IndexValue::from(2003)),
        )
        .unwrap(),
        vec!["b".to_string(), "c".to_string(), "d".to_string()]
    );

    // Update keeping the key (stale hint), then re-keying, then delete.
    db.insert(
        "articles",
        Record::new("b").with_metadata(json!({ "year": 2002, "title": "t-b-v2" })),
    )
    .unwrap();
    assert_eq!(
        db.get("articles", "b").unwrap().unwrap().metadata["title"],
        "t-b-v2"
    );
    db.insert(
        "articles",
        Record::new("c").with_metadata(json!({ "year": 2005, "title": "t-c-v2" })),
    )
    .unwrap();
    assert_eq!(
        db.lookup_index("articles_year", &[IndexValue::from(2005)])
            .unwrap(),
        vec!["c".to_string()]
    );
    assert_eq!(
        db.lookup_index("articles_year", &[IndexValue::from(2003)])
            .unwrap(),
        Vec::<String>::new()
    );
    db.delete("articles", "d").unwrap();
    assert_eq!(
        db.lookup_index("articles_year", &[IndexValue::from(2002)])
            .unwrap(),
        vec!["b".to_string()]
    );
    db.close().unwrap();
}

#[test]
fn paged_batch_fetch_preserves_rank_order_across_parallel_reads() {
    opt_into_v3();
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        for ordinal in 0..32 {
            let id = format!("doc-{ordinal:02}");
            db.insert("articles", article(&id, ordinal)).unwrap();
        }
        db.close().unwrap();
    }

    // Reopen so every row is a lazy paged read. More than eight misses engages
    // the bounded parallel path used to materialize ranked FTS winners.
    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    let mut primary_keys = (0..32)
        .rev()
        .map(|ordinal| format!("doc-{ordinal:02}"))
        .collect::<Vec<_>>();
    primary_keys.insert(7, "missing".to_string());
    primary_keys.push("doc-31".to_string());
    let records = db.get_records_by_pks("articles", &primary_keys).unwrap();

    assert_eq!(records.len(), primary_keys.len());
    for (primary_key, record) in primary_keys.iter().zip(records) {
        match primary_key.as_str() {
            "missing" => assert!(record.is_none()),
            primary_key => assert_eq!(record.unwrap().id, primary_key),
        }
    }
}

#[test]
fn unique_v3_index_rejects_duplicates() {
    opt_into_v3();
    let dir = TempDir::new().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    db.create_collection("articles").unwrap();
    db.create_index(year_index(true)).unwrap();
    db.insert("articles", article("a", 2001)).unwrap();
    let error = db
        .insert("articles", article("b", 2001))
        .unwrap_err()
        .to_string();
    assert!(error.contains("duplicate"), "unexpected error: {error}");
    // The same row updating itself is not a duplicate.
    db.insert(
        "articles",
        Record::new("a").with_metadata(json!({ "year": 2001, "title": "again" })),
    )
    .unwrap();
    db.close().unwrap();
}

#[test]
fn backfilled_index_after_rows_is_v3_and_correct() {
    opt_into_v3();
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        for (id, year) in [("a", 2001), ("b", 2002), ("c", 2003)] {
            db.insert("articles", article(id, year)).unwrap();
        }
        db.create_index(year_index(false)).unwrap();
        db.close().unwrap();
    }
    let refs = durable_refs(&dir, "articles_year");
    assert_eq!(refs.len(), 3);
    assert!(refs.iter().all(|r| matches!(r, IndexEntryRef::Intern(_))));

    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    assert_eq!(
        db.lookup_index("articles_year", &[IndexValue::from(2002)])
            .unwrap(),
        vec!["b".to_string()]
    );
    db.close().unwrap();
}

#[test]
fn drop_then_recreate_registers_the_index_again() {
    opt_into_v3();
    // Pre-existing bug caught by the v3 net: the completed build's state file
    // survived DROP INDEX, and a recreate adopted it — "completing" instantly
    // without re-registering the logical index.
    let dir = TempDir::new().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    db.create_collection("articles").unwrap();
    db.create_index(year_index(false)).unwrap();
    db.insert("articles", article("a", 2001)).unwrap();
    assert!(db.drop_index("articles_year").unwrap());
    assert!(!db
        .index_definitions()
        .iter()
        .any(|d| d.name == "articles_year"));

    db.create_index(year_index(false)).unwrap();
    assert!(
        db.index_definitions()
            .iter()
            .any(|d| d.name == "articles_year"),
        "recreate after drop must register the index"
    );
    assert_eq!(
        db.lookup_index("articles_year", &[IndexValue::from(2001)])
            .unwrap(),
        vec!["a".to_string()]
    );
    db.close().unwrap();
}
