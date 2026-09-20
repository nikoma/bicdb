//! v2→v3 rekey migration: a v2 index (forced via the env gate — this file is
//! its own process, and the single test sets the variable before the gate's
//! OnceLock initializes) migrates to intern-keyed entries in bounded batches
//! and serves identical lookups after.

use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexEntryRef, IndexField, IndexKind, IndexValue,
    PagedRecords, PagedRecordsOptions, Record, StorageMode,
};
use serde_json::json;
use tempfile::TempDir;

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
fn rekey_migrates_a_v2_index_to_intern_entries() {
    // MUST run before any BicDb open in this process: the gate is a OnceLock.
    std::env::set_var("BICDB_PAGED_V3_ENTRIES", "0");

    let dir = TempDir::new().unwrap();
    let config = DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged);
    {
        let mut db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
        db.create_collection("articles").unwrap();
        db.create_index(IndexDefinition {
            name: "articles_year".to_string(),
            collection: "articles".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["year".to_string()])],
            unique: false,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        })
        .unwrap();
        for row in 0..100 {
            db.insert(
                "articles",
                Record::new(format!("r{row:03}"))
                    .with_metadata(json!({ "year": 2000 + (row % 7) })),
            )
            .unwrap();
        }
        db.close().unwrap();
    }
    // The gate forced v2 entries.
    let before = durable_refs(&dir, "articles_year");
    assert_eq!(before.len(), 100);
    assert!(before.iter().all(|r| matches!(r, IndexEntryRef::Pk(_))));

    // Rekey, then prove the on-disk identity flipped and reads are identical.
    let mut db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
    let expected: Vec<String> = db
        .lookup_index("articles_year", &[IndexValue::from(2003)])
        .unwrap();
    let rewritten = db.rekey_index("articles_year").unwrap();
    assert_eq!(rewritten, 100);
    assert_eq!(
        db.lookup_index("articles_year", &[IndexValue::from(2003)])
            .unwrap(),
        expected
    );
    // Idempotent: nothing left to rewrite.
    assert_eq!(db.rekey_index("articles_year").unwrap(), 0);
    // Writes after the flip produce v3 entries and reads stay correct.
    db.insert(
        "articles",
        Record::new("fresh").with_metadata(json!({ "year": 2003 })),
    )
    .unwrap();
    let mut with_fresh = expected.clone();
    with_fresh.push("fresh".to_string());
    with_fresh.sort();
    assert_eq!(
        db.lookup_index("articles_year", &[IndexValue::from(2003)])
            .unwrap(),
        with_fresh
    );
    db.close().unwrap();

    let after = durable_refs(&dir, "articles_year");
    assert_eq!(after.len(), 101);
    assert!(
        after.iter().all(|r| matches!(r, IndexEntryRef::Intern(_))),
        "rekey left non-intern entries"
    );
}
