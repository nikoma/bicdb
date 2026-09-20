//! The planner row estimate for lazy paged collections (B3).
//!
//! `estimated_record_count` used to fall back to a full visibility-checked
//! scan — per call, and the planner calls it per query, so every simple
//! statement against a lazy paged table paid O(n) before executing. It now
//! seeds once from a key-only walk and is nudged by commits.
//!
//! It is an ESTIMATE: dead keys at seed time and drift from paths that do not
//! nudge (replication imports) are accepted. What these tests pin is the
//! contract planning actually relies on — right order of magnitude, moving in
//! the right direction with inserts and deletes, and never negative.

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::json;
use tempfile::TempDir;

const ROWS: usize = 500;

fn paged_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

fn seeded() -> (TempDir, BicDb) {
    let dir = TempDir::new().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    db.create_collection("t").unwrap();
    let records: Vec<Record> = (0..ROWS)
        .map(|index| Record::new(format!("k{index:05}")).with_metadata(json!({ "n": index })))
        .collect();
    db.batch_insert("t", records).unwrap();
    db.close().unwrap();
    // Reopen so the collection is lazy with an empty registry — the exact
    // state whose estimate used to cost a full scan.
    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    (dir, db)
}

#[test]
fn the_seed_matches_the_corpus() {
    let (_dir, db) = seeded();
    assert_eq!(db.estimated_record_count("t").unwrap(), ROWS);
    // And repeated calls answer from the counter, identically.
    assert_eq!(db.estimated_record_count("t").unwrap(), ROWS);
}

#[test]
fn inserts_of_new_rows_raise_the_estimate_and_updates_do_not() {
    let (_dir, mut db) = seeded();
    assert_eq!(db.estimated_record_count("t").unwrap(), ROWS);

    // Ten genuinely new rows.
    for index in 0..10 {
        db.insert(
            "t",
            Record::new(format!("new-{index}")).with_metadata(json!({})),
        )
        .unwrap();
    }
    assert_eq!(db.estimated_record_count("t").unwrap(), ROWS + 10);

    // Updates of pre-existing rows (first touch AND re-touch) move nothing.
    for index in 0..20 {
        db.insert(
            "t",
            Record::new(format!("k{index:05}")).with_metadata(json!({ "n": -1 })),
        )
        .unwrap();
    }
    db.insert("t", Record::new("k00000").with_metadata(json!({ "n": -2 })))
        .unwrap();
    assert_eq!(
        db.estimated_record_count("t").unwrap(),
        ROWS + 10,
        "updates must not inflate the estimate"
    );
}

#[test]
fn deletes_lower_the_estimate_for_touched_and_untouched_rows() {
    let (_dir, mut db) = seeded();
    assert_eq!(db.estimated_record_count("t").unwrap(), ROWS);

    // Untouched rows: the delete's baseline seed reports the pre-image.
    db.batch_delete("t", (0..15).map(|index| format!("k{index:05}")))
        .unwrap();
    assert_eq!(db.estimated_record_count("t").unwrap(), ROWS - 15);

    // A row created this session and then deleted: +1 then -1.
    db.insert("t", Record::new("ephemeral").with_metadata(json!({})))
        .unwrap();
    assert_eq!(db.estimated_record_count("t").unwrap(), ROWS - 15 + 1);
    assert!(db.delete("t", "ephemeral").unwrap());
    assert_eq!(db.estimated_record_count("t").unwrap(), ROWS - 15);

    // Deleting an absent row moves nothing.
    assert!(!db.delete("t", "never-existed").unwrap());
    assert_eq!(db.estimated_record_count("t").unwrap(), ROWS - 15);
}

#[test]
fn the_estimate_never_goes_negative() {
    let dir = TempDir::new().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    db.create_collection("t").unwrap();
    db.insert("t", Record::new("only").with_metadata(json!({})))
        .unwrap();
    db.close().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    // Seed (1), then delete twice — the second is a no-op.
    assert_eq!(db.estimated_record_count("t").unwrap(), 1);
    assert!(db.delete("t", "only").unwrap());
    assert!(!db.delete("t", "only").unwrap());
    assert_eq!(db.estimated_record_count("t").unwrap(), 0);
}

#[test]
fn the_exact_count_is_untouched_by_the_estimate() {
    // `collection_record_count` is the user-visible EXACT count (RESP DBSIZE);
    // it must keep visibility-checking rather than trusting the estimate.
    let (_dir, mut db) = seeded();
    let _ = db.estimated_record_count("t").unwrap(); // seed it
    db.batch_delete("t", (0..30).map(|index| format!("k{index:05}")))
        .unwrap();
    assert_eq!(db.collection_record_count("t").unwrap(), ROWS - 30);
}
