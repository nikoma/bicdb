//! TID hints end to end: ordered durable index entries carry a heap-locator
//! hint, the read-through lookup+fetch path uses it, and every way a hint can
//! go stale (update, delete, re-key, backfill, reopen) still reads exactly
//! what the hint-less descent would.

use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, IndexValue, PagedRecords,
    PagedRecordsOptions, Record, StorageMode,
};
use serde_json::json;
use tempfile::TempDir;

fn paged_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

fn year_index() -> IndexDefinition {
    IndexDefinition {
        name: "articles_year".to_string(),
        collection: "articles".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["year".to_string()])],
        unique: false,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    }
}

fn article(id: &str, year: i64) -> Record {
    Record::new(id).with_metadata(json!({ "year": year, "title": format!("t-{id}") }))
}

/// `(pk, entry_value)` for every durable entry of `index`, read straight from
/// the CLOSED database's page store.
fn durable_entry_values(dir: &TempDir, index: &str) -> Vec<(String, Vec<u8>)> {
    let paged = PagedRecords::open(
        dir.path().join("paged"),
        PagedRecordsOptions {
            fsync: false,
            ..Default::default()
        },
    )
    .unwrap();
    let snapshot = paged.latest_snapshot();
    let keys: Vec<Vec<u8>> = paged
        .scan_index(&snapshot, index)
        .unwrap()
        .map(|entry| entry.map(|(encoded, _)| encoded))
        .collect::<Result<_, _>>()
        .unwrap();
    let mut out = Vec::new();
    for encoded in keys {
        for entry in paged.scan_index_exact(&snapshot, index, &encoded).unwrap() {
            out.push(entry.unwrap());
        }
    }
    out
}

#[test]
fn write_path_entries_carry_hints_and_reads_survive_staleness() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        db.create_index(year_index()).unwrap();
        for (id, year) in [("a", 2001), ("b", 2002), ("c", 2003), ("d", 2002)] {
            db.insert("articles", article(id, year)).unwrap();
        }
        db.close().unwrap();
    }
    // Entries written on the normal write path carry the 22-byte TID hint.
    for (pk, value) in durable_entry_values(&dir, "articles_year") {
        assert_eq!(value.len(), 22, "entry for `{pk}` should carry a TID hint");
    }

    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();

    // Fresh-hint reads: lookup then fetch resolves through the hint.
    assert_eq!(
        db.lookup_index("articles_year", &[IndexValue::from(2002)])
            .unwrap(),
        vec!["b".to_string(), "d".to_string()]
    );
    let record = db.get("articles", "b").unwrap().expect("b exists");
    assert_eq!(record.metadata["title"], "t-b");

    // Stale hint via update that KEEPS the index key: the entry (and its
    // hint) is not rewritten; the fetch must fall back and serve the update.
    db.insert(
        "articles",
        Record::new("b").with_metadata(json!({ "year": 2002, "title": "t-b-v2" })),
    )
    .unwrap();
    assert_eq!(
        db.lookup_index("articles_year", &[IndexValue::from(2002)])
            .unwrap(),
        vec!["b".to_string(), "d".to_string()]
    );
    assert_eq!(
        db.get("articles", "b").unwrap().unwrap().metadata["title"],
        "t-b-v2"
    );

    // Re-key: the new entry carries a fresh hint, the old entry is gone.
    db.insert(
        "articles",
        Record::new("c").with_metadata(json!({ "year": 2004, "title": "t-c-v2" })),
    )
    .unwrap();
    assert_eq!(
        db.lookup_index("articles_year", &[IndexValue::from(2003)])
            .unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(
        db.lookup_index("articles_year", &[IndexValue::from(2004)])
            .unwrap(),
        vec!["c".to_string()]
    );
    assert_eq!(
        db.get("articles", "c").unwrap().unwrap().metadata["title"],
        "t-c-v2"
    );

    // Delete: the lookup no longer lists the row and the fetch agrees.
    db.delete("articles", "d").unwrap();
    assert_eq!(
        db.lookup_index("articles_year", &[IndexValue::from(2002)])
            .unwrap(),
        vec!["b".to_string()]
    );
    assert!(db.get("articles", "d").unwrap().is_none());

    // The hint path must actually ENGAGE, not just stay silently correct:
    // a lookup followed by the pk-batch fetch resolves through the stash.
    let (hits_before, _) = bicdb_core::paged_tid_hint_stats();
    let pks = db
        .lookup_index("articles_year", &[IndexValue::from(2001)])
        .unwrap();
    let records = db.get_records_by_pks("articles", &pks).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].as_ref().unwrap().metadata["title"], "t-a");
    let (hits_after, _) = bicdb_core::paged_tid_hint_stats();
    assert!(
        hits_after > hits_before,
        "hinted fetch did not engage (hits {hits_before} -> {hits_after})"
    );
    db.close().unwrap();
}

#[test]
fn backfilled_entries_carry_hints_from_the_heads_scan() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        // Rows FIRST, index AFTER: entries come from the backfill scan, whose
        // hints must stamp each head's ORIGINAL xmin, not the backfill xid.
        for (id, year) in [("a", 2001), ("b", 2002), ("c", 2003)] {
            db.insert("articles", article(id, year)).unwrap();
        }
        db.create_index(year_index()).unwrap();
        db.close().unwrap();
    }
    for (pk, value) in durable_entry_values(&dir, "articles_year") {
        assert_eq!(
            value.len(),
            22,
            "backfilled entry for `{pk}` should carry a TID hint"
        );
    }

    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    assert_eq!(
        db.lookup_index("articles_year", &[IndexValue::from(2002)])
            .unwrap(),
        vec!["b".to_string()]
    );
    assert_eq!(
        db.get("articles", "b").unwrap().unwrap().metadata["title"],
        "t-b"
    );
    db.close().unwrap();
}
