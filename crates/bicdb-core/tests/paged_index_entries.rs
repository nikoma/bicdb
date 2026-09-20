//! Durable secondary-index entries in the page store (Phase 4, first slice).
//!
//! The contract: a B-tree index's entries are written in the SAME paged
//! transaction as the rows they describe, live under a reserved key namespace
//! in the same tree/WAL, and therefore survive restart — and crash — with no
//! rebuild step, because recovery keeps or discards row and entries together.
//!
//! These tests inspect the durable keyspace directly through `PagedRecords`
//! after the database is closed, which is the point: what they see is what a
//! future open could trust WITHOUT scanning the corpus.

use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, PagedRecords, PagedRecordsOptions,
    Record, StorageMode,
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

/// The durable entries of `index`, read straight from the closed database's
/// page store: `(encoded_key, pk)` in index order.
fn durable_entries(dir: &TempDir, index: &str) -> Vec<(Vec<u8>, String)> {
    let paged = PagedRecords::open(
        dir.path().join("paged"),
        PagedRecordsOptions {
            fsync: false,
            ..Default::default()
        },
    )
    .unwrap();
    let snapshot = paged.latest_snapshot();
    paged
        .scan_index(&snapshot, index)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

#[test]
fn index_entries_survive_restart_without_rebuild() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        db.create_index(year_index()).unwrap();
        // Deliberately out of key order, so the ordering below is the index's
        // doing rather than insertion order.
        db.insert("articles", article("c", 2003)).unwrap();
        db.insert("articles", article("a", 2001)).unwrap();
        db.insert("articles", article("b", 2002)).unwrap();
        db.close().unwrap();
    }

    let entries = durable_entries(&dir, "articles_year");
    assert_eq!(
        entries
            .iter()
            .map(|(_, pk)| pk.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b", "c"],
        "durable entries must exist after close and order by indexed value"
    );
    // Distinct years must produce distinct encoded keys, ascending.
    assert!(entries[0].0 < entries[1].0 && entries[1].0 < entries[2].0);
}

#[test]
fn updates_and_deletes_move_the_durable_entries() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        db.create_index(year_index()).unwrap();
        db.insert("articles", article("a", 2001)).unwrap();
        db.insert("articles", article("b", 2002)).unwrap();
        // Move `a` to the top of the order, then remove `b` entirely.
        db.insert("articles", article("a", 2030)).unwrap();
        assert!(db.delete("articles", "b").unwrap());
        db.close().unwrap();
    }

    let entries = durable_entries(&dir, "articles_year");
    assert_eq!(
        entries
            .iter()
            .map(|(_, pk)| pk.as_str())
            .collect::<Vec<_>>(),
        vec!["a"],
        "update must supersede the old entry and delete must remove b's"
    );
}

#[test]
fn an_index_created_after_the_rows_is_backfilled_durably() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        for (id, year) in [("x", 2010), ("y", 2005), ("z", 2020)] {
            db.insert("articles", article(id, year)).unwrap();
        }
        db.close().unwrap();
    }
    {
        // Second session: the rows predate the index.
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_index(year_index()).unwrap();
        db.close().unwrap();
    }

    let entries = durable_entries(&dir, "articles_year");
    assert_eq!(
        entries
            .iter()
            .map(|(_, pk)| pk.as_str())
            .collect::<Vec<_>>(),
        vec!["y", "x", "z"],
        "backfill must cover pre-existing rows, ordered by year (2005,2010,2020)"
    );
}

#[test]
fn dropping_an_index_removes_its_durable_entries() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        db.create_index(year_index()).unwrap();
        db.insert("articles", article("a", 2001)).unwrap();
        assert!(db.drop_index("articles_year").unwrap());
        db.close().unwrap();
    }
    assert!(
        durable_entries(&dir, "articles_year").is_empty(),
        "a dropped index must not leave entries in the reserved keyspace"
    );
}

#[test]
fn entries_survive_a_drop_without_close() {
    // The crash-shaped exit: no close, no checkpoint — recovery must bring the
    // entries back alongside their rows, because they committed together.
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        db.create_index(year_index()).unwrap();
        for index in 0..50 {
            db.insert("articles", article(&format!("r{index:03}"), 2000 + index))
                .unwrap();
        }
        drop(db);
    }
    // Reopen through BicDb (recovery runs), close cleanly, then inspect.
    {
        let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.close().unwrap();
    }
    let entries = durable_entries(&dir, "articles_year");
    assert_eq!(
        entries.len(),
        50,
        "all 50 committed rows' entries must survive an unclean exit"
    );
    // And they are exactly the committed pks, in year order == id order here.
    let pks: Vec<&str> = entries.iter().map(|(_, pk)| pk.as_str()).collect();
    let expected: Vec<String> = (0..50).map(|index| format!("r{index:03}")).collect();
    assert_eq!(pks, expected.iter().map(String::as_str).collect::<Vec<_>>());
}

#[test]
fn the_resident_index_still_verifies_in_both_modes() {
    // The dual-write must not disturb the resident index that serves queries
    // today. `verify_index` recomputes it from the collection and compares —
    // in both modes, across a reopen. (Query-level equivalence is covered by
    // the SQL suite, which runs its WHERE-over-index tests in both modes.)
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = TempDir::new().unwrap();
        {
            let mut db = BicDb::open_with_config(
                dir.path(),
                DbConfig::default()
                    .with_fsync(false)
                    .with_storage_mode(mode.clone()),
            )
            .unwrap();
            db.create_collection("articles").unwrap();
            db.create_index(year_index()).unwrap();
            for (id, year) in [("a", 2001), ("b", 2002), ("c", 2003), ("d", 2002)] {
                db.insert("articles", article(id, year)).unwrap();
            }
            db.close().unwrap();
        }
        let db = BicDb::open_with_config(
            dir.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode.clone()),
        )
        .unwrap();
        let report = db.verify_index("articles_year").unwrap();
        assert!(report.valid, "[{mode}] index invalid after reopen");
    }
}

#[test]
fn a_reopened_index_is_loaded_from_durable_entries_and_verifies() {
    // After reopen, the resident index is LOADED from the durable keyspace
    // rather than rebuilt from rows. `verify_index` then recomputes it from
    // the collection and compares — so this asserts load == rebuild, which is
    // exactly the property that makes the durable entries trustworthy.
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        db.create_index(year_index()).unwrap();
        for index in 0..300 {
            db.insert(
                "articles",
                article(&format!("r{index:04}"), 1990 + (index % 40)),
            )
            .unwrap();
        }
        // Churn: move some, remove some, so the durable entries have real
        // update/delete history rather than a pristine insert-only shape.
        for index in (0..300).step_by(7) {
            db.insert("articles", article(&format!("r{index:04}"), 2100 + index))
                .unwrap();
        }
        for index in (0..300).step_by(13) {
            db.delete("articles", &format!("r{index:04}")).unwrap();
        }
        db.close().unwrap();
    }

    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    let report = db.verify_index("articles_year").unwrap();
    assert!(
        report.valid,
        "index loaded from durable entries diverged from a rebuild: {report:?}"
    );
}

#[test]
fn a_reopened_index_serves_read_through_with_no_resident_bytes() {
    // Phase 4c-2: reopening a database whose B-tree index has durable entries
    // must not materialize a resident store — the index's own bytes stay on
    // pages, and every lookup shape answers from the durable keyspace through
    // the registry.
    use bicdb_core::IndexValue;

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

    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    let residency = db.residency_report().unwrap();
    let index = residency
        .indexes
        .iter()
        .find(|index| index.name == "articles_year")
        .expect("index reported");
    assert_eq!(
        index.store_bytes, 0,
        "a read-through index must keep no resident store bytes"
    );
    assert_eq!(
        index.entry_count, 0,
        "the resident store stays empty; entries live on pages"
    );

    // Point, prefix-as-full-key, range, ordered, and extreme lookups all
    // answer from the durable keyspace.
    assert_eq!(
        db.lookup_index("articles_year", &[IndexValue::from(2002)])
            .unwrap(),
        vec!["b".to_string(), "d".to_string()]
    );
    assert_eq!(
        db.range_index(
            "articles_year",
            Some(&IndexValue::from(2002)),
            Some(&IndexValue::from(2003)),
        )
        .unwrap(),
        vec!["b".to_string(), "c".to_string(), "d".to_string()]
    );
    assert_eq!(
        db.range_index_with_prefix_filters(
            "articles_year",
            &[],
            Some(&IndexValue::from(2002)),
            None,
            &[],
        )
        .unwrap(),
        vec!["b".to_string(), "c".to_string(), "d".to_string()]
    );
    let rowids = db
        .range_index_with_prefix_filters_rowids(
            "articles_year",
            &[],
            Some(&IndexValue::from(2002)),
            None,
            &[],
        )
        .unwrap();
    assert_eq!(rowids.len(), 3, "rowid range path must serve read-through");
    assert_eq!(
        db.ordered_index_records("articles_year", false, Some(2))
            .unwrap(),
        vec!["a".to_string(), "b".to_string()]
    );
    // Descending runs the reverse durable cursor: greatest keys first,
    // stopping after `limit` entries instead of scanning the whole namespace.
    assert_eq!(
        db.ordered_index_records("articles_year", true, Some(2))
            .unwrap(),
        vec!["c".to_string(), "d".to_string()]
    );
    assert_eq!(
        db.ordered_index_records("articles_year", true, None)
            .unwrap(),
        vec![
            "c".to_string(),
            "d".to_string(),
            "b".to_string(),
            "a".to_string()
        ]
    );
    assert_eq!(
        db.lookup_index_extreme("articles_year", &[], true)
            .unwrap()
            .unwrap()
            .0,
        vec![IndexValue::from(2003)]
    );
    assert_eq!(
        db.lookup_index_extreme("articles_year", &[], false)
            .unwrap()
            .unwrap(),
        (vec![IndexValue::from(2001)], vec!["a".to_string()])
    );

    // The durable entries verify against a recompute from the page store.
    let report = db.verify_index("articles_year").unwrap();
    assert!(report.valid, "read-through index failed verify: {report:?}");

    // Writes keep maintaining the durable entries, and a further reopen sees
    // them — still read-through.
    db.insert("articles", article("e", 2000)).unwrap();
    assert_eq!(
        db.lookup_index("articles_year", &[IndexValue::from(2000)])
            .unwrap(),
        vec!["e".to_string()]
    );
    db.close().unwrap();

    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    let residency = db.residency_report().unwrap();
    let index = residency
        .indexes
        .iter()
        .find(|index| index.name == "articles_year")
        .expect("index reported");
    assert_eq!(index.store_bytes, 0);
    assert_eq!(
        db.ordered_index_records("articles_year", false, Some(1))
            .unwrap(),
        vec!["e".to_string()]
    );
    assert!(db.verify_index("articles_year").unwrap().valid);
}
