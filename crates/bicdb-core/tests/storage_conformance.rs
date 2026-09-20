//! Shared behavioural conformance suite, run against every supported storage mode.
//!
//! Phase 0 of `docs/server-paged-storage-todo.md` requires "behavioral
//! conformance tests that run unchanged against both storage modes", and the
//! Definition of Done requires that point, range, transactional, constraint, and
//! vector semantics all "pass the shared conformance suite". This is that suite.
//!
//! # The property that matters
//!
//! Every test here is written against [`for_each_mode`] and touches no
//! mode-specific API. When `server_paged` becomes runnable, the *only* change
//! required to run this entire file against it is that
//! `StorageMode::is_supported` starts returning true — no test edits, no
//! parameterization, no copies. `modes_under_test` asserts that property so it
//! cannot quietly stop holding.
//!
//! Today only `embedded_memory` is implemented, so these run once. That is not
//! wasted: it fixes the *observable contract* of the current engine now, while
//! it is the only engine, so the second one is measured against behaviour that
//! was written down before it existed rather than behaviour reverse-engineered
//! from it afterwards.

use std::sync::Arc;

use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, IndexValue, Record, StorageMode,
};
use serde_json::json;
use tempfile::TempDir;

/// Every storage mode this build can actually run.
fn supported_modes() -> Vec<StorageMode> {
    [StorageMode::EmbeddedMemory, StorageMode::ServerPaged]
        .into_iter()
        .filter(StorageMode::is_supported)
        .collect()
}

/// Run `body` against a fresh database in each supported storage mode.
///
/// The directory and the database are recreated per mode so no test can leak
/// state between them, and the mode name is included in panic messages so a
/// failure says which engine failed.
fn for_each_mode(body: impl Fn(&mut BicDb, &StorageMode)) {
    let modes = supported_modes();
    assert!(
        !modes.is_empty(),
        "no storage mode is runnable; the conformance suite would vacuously pass"
    );
    for mode in modes {
        let dir = TempDir::new().unwrap();
        let config = DbConfig::default().with_storage_mode(mode.clone());
        let mut db = BicDb::open_with_config(dir.path(), config)
            .unwrap_or_else(|error| panic!("open in mode `{mode}` failed: {error}"));
        body(&mut db, &mode);
    }
}

/// Like [`for_each_mode`], but the body gets the directory too so it can close
/// and reopen the database to exercise durability.
fn for_each_mode_with_dir(body: impl Fn(&TempDir, &StorageMode)) {
    for mode in supported_modes() {
        let dir = TempDir::new().unwrap();
        body(&dir, &mode);
    }
}

fn open_in(dir: &TempDir, mode: &StorageMode) -> BicDb {
    BicDb::open_with_config(
        dir.path(),
        DbConfig::default().with_storage_mode(mode.clone()),
    )
    .unwrap_or_else(|error| panic!("open in mode `{mode}` failed: {error}"))
}

fn note(id: &str, kind: &str, seq: i64) -> Record {
    Record::new(id)
        .with_metadata(json!({ "kind": kind, "seq": seq }))
        .with_timestamp(seq)
}

// ---------------------------------------------------------------------------
// The harness itself
// ---------------------------------------------------------------------------

#[test]
fn modes_under_test() {
    let modes = supported_modes();
    assert!(
        modes.contains(&StorageMode::EmbeddedMemory),
        "embedded_memory must always be runnable"
    );
    // When server_paged lands this assertion flips, and every test below starts
    // running against it with no other change. That is the whole design of this
    // file; if it ever needs editing to add a mode, the suite is no longer
    // "unchanged across modes" and Phase 0's requirement has been lost.
    assert!(
        modes.contains(&StorageMode::ServerPaged),
        "server_paged became runnable; every test below now covers it"
    );
    assert_eq!(
        modes.len(),
        2,
        "a new storage mode became runnable; the suite now covers it automatically — \
         update this count and confirm nothing else in this file needed changing"
    );
}

// ---------------------------------------------------------------------------
// Point reads and writes
// ---------------------------------------------------------------------------

#[test]
fn point_write_then_read_returns_the_same_record() {
    for_each_mode(|db, mode| {
        db.create_collection("notes").unwrap();
        db.insert("notes", note("n1", "memo", 1)).unwrap();

        let found = db.get("notes", "n1").unwrap();
        let found: Arc<Record> = found.unwrap_or_else(|| panic!("{mode}: record not found"));
        assert_eq!(found.id, "n1");
        assert_eq!(found.metadata["kind"], json!("memo"));
        assert_eq!(found.metadata["seq"], json!(1));
    });
}

#[test]
fn reading_an_absent_key_is_none_not_an_error() {
    for_each_mode(|db, mode| {
        db.create_collection("notes").unwrap();
        assert!(
            db.get("notes", "missing").unwrap().is_none(),
            "{mode}: absent key should be None"
        );
    });
}

#[test]
fn overwriting_a_key_replaces_the_visible_record() {
    for_each_mode(|db, mode| {
        db.create_collection("notes").unwrap();
        db.insert("notes", note("n1", "first", 1)).unwrap();
        db.insert("notes", note("n1", "second", 2)).unwrap();

        let found = db.get("notes", "n1").unwrap().unwrap();
        assert_eq!(found.metadata["kind"], json!("second"), "{mode}");
        assert_eq!(
            db.scan_collection("notes").unwrap().len(),
            1,
            "{mode}: overwrite must not create a second live row"
        );
    });
}

#[test]
fn delete_removes_the_record_and_reports_whether_it_existed() {
    for_each_mode(|db, mode| {
        db.create_collection("notes").unwrap();
        db.insert("notes", note("n1", "memo", 1)).unwrap();

        assert!(db.delete("notes", "n1").unwrap(), "{mode}: first delete");
        assert!(
            db.get("notes", "n1").unwrap().is_none(),
            "{mode}: deleted record still visible"
        );
        assert!(
            !db.delete("notes", "n1").unwrap(),
            "{mode}: deleting an absent record should report false"
        );
    });
}

// ---------------------------------------------------------------------------
// Range / scan
// ---------------------------------------------------------------------------

#[test]
fn scan_returns_every_live_record_and_no_deleted_one() {
    for_each_mode(|db, mode| {
        db.create_collection("notes").unwrap();
        for i in 0..50 {
            db.insert("notes", note(&format!("n{i:03}"), "memo", i))
                .unwrap();
        }
        db.delete("notes", "n007").unwrap();
        db.delete("notes", "n042").unwrap();

        let scanned = db.scan_collection("notes").unwrap();
        assert_eq!(scanned.len(), 48, "{mode}");

        let mut ids: Vec<&str> = scanned.iter().map(|r| r.id.as_str()).collect();
        ids.sort_unstable();
        assert!(!ids.contains(&"n007"), "{mode}: deleted record in scan");
        assert!(!ids.contains(&"n042"), "{mode}: deleted record in scan");
        assert_eq!(ids.first(), Some(&"n000"), "{mode}");
    });
}

#[test]
fn scanning_an_empty_collection_yields_nothing() {
    for_each_mode(|db, mode| {
        db.create_collection("empty").unwrap();
        assert!(db.scan_collection("empty").unwrap().is_empty(), "{mode}");
    });
}

// ---------------------------------------------------------------------------
// Transactions and snapshot visibility
// ---------------------------------------------------------------------------

#[test]
fn committed_transaction_writes_become_visible() {
    for_each_mode(|db, mode| {
        db.create_collection("notes").unwrap();

        let mut tx = db.begin_transaction().unwrap();
        tx.insert("notes", note("t1", "memo", 1)).unwrap();
        tx.insert("notes", note("t2", "memo", 2)).unwrap();
        tx.commit().unwrap();

        assert!(db.get("notes", "t1").unwrap().is_some(), "{mode}");
        assert!(db.get("notes", "t2").unwrap().is_some(), "{mode}");
    });
}

#[test]
fn rolled_back_transaction_writes_are_never_visible() {
    for_each_mode(|db, mode| {
        db.create_collection("notes").unwrap();
        db.insert("notes", note("kept", "memo", 0)).unwrap();

        let mut tx = db.begin_transaction().unwrap();
        tx.insert("notes", note("discarded", "memo", 1)).unwrap();
        tx.rollback().unwrap();

        assert!(
            db.get("notes", "discarded").unwrap().is_none(),
            "{mode}: rolled-back write is visible"
        );
        assert!(db.get("notes", "kept").unwrap().is_some(), "{mode}");
        assert_eq!(db.scan_collection("notes").unwrap().len(), 1, "{mode}");
    });
}

#[test]
fn a_transaction_reads_its_own_uncommitted_writes() {
    for_each_mode(|db, mode| {
        db.create_collection("notes").unwrap();

        let mut tx = db.begin_transaction().unwrap();
        tx.insert("notes", note("own", "memo", 1)).unwrap();
        let seen = tx.get("notes", "own").unwrap();
        assert!(
            seen.is_some(),
            "{mode}: a transaction must see its own pending write"
        );
        tx.rollback().unwrap();
    });
}

#[test]
fn a_transaction_does_not_see_writes_committed_after_it_began() {
    for_each_mode(|db, mode| {
        db.create_collection("notes").unwrap();
        db.insert("notes", note("before", "memo", 0)).unwrap();

        let tx = db.begin_transaction().unwrap();
        // Established before the concurrent write, so the snapshot is pinned.
        assert!(tx.get("notes", "before").unwrap().is_some(), "{mode}");

        db.insert("notes", note("after", "memo", 1)).unwrap();

        assert!(
            tx.get("notes", "after").unwrap().is_none(),
            "{mode}: snapshot isolation violated — a later commit is visible"
        );
        tx.rollback().unwrap();
    });
}

// ---------------------------------------------------------------------------
// Constraints and indexes
// ---------------------------------------------------------------------------

#[test]
fn index_lookup_finds_matching_records() {
    for_each_mode(|db, mode| {
        db.create_collection("notes").unwrap();
        for i in 0..30 {
            let kind = if i % 3 == 0 { "alpha" } else { "beta" };
            db.insert("notes", note(&format!("n{i:03}"), kind, i))
                .unwrap();
        }
        db.create_index(IndexDefinition {
            name: "notes_kind".to_string(),
            collection: "notes".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["kind".to_string()])],
            kind: IndexKind::BTree,
            unique: false,
            predicate: None,
            exclusion: None,
        })
        .unwrap();

        let alpha = db
            .lookup_index_exact("notes_kind", &[IndexValue::from("alpha")])
            .unwrap();
        assert_eq!(alpha.len(), 10, "{mode}");

        let beta = db
            .lookup_index_exact("notes_kind", &[IndexValue::from("beta")])
            .unwrap();
        assert_eq!(beta.len(), 20, "{mode}");
    });
}

#[test]
fn index_reflects_writes_made_after_it_was_created() {
    for_each_mode(|db, mode| {
        db.create_collection("notes").unwrap();
        db.create_index(IndexDefinition {
            name: "notes_kind".to_string(),
            collection: "notes".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["kind".to_string()])],
            kind: IndexKind::BTree,
            unique: false,
            predicate: None,
            exclusion: None,
        })
        .unwrap();

        db.insert("notes", note("n1", "gamma", 1)).unwrap();
        db.insert("notes", note("n2", "gamma", 2)).unwrap();
        assert_eq!(
            db.lookup_index_exact("notes_kind", &[IndexValue::from("gamma")])
                .unwrap()
                .len(),
            2,
            "{mode}: index missed a post-creation insert"
        );

        db.delete("notes", "n1").unwrap();
        assert_eq!(
            db.lookup_index_exact("notes_kind", &[IndexValue::from("gamma")])
                .unwrap()
                .len(),
            1,
            "{mode}: index retained a deleted record"
        );
    });
}

#[test]
fn unique_index_rejects_a_duplicate_key() {
    for_each_mode(|db, mode| {
        db.create_collection("notes").unwrap();
        db.create_index(IndexDefinition {
            name: "notes_kind_unique".to_string(),
            collection: "notes".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["kind".to_string()])],
            kind: IndexKind::BTree,
            unique: true,
            predicate: None,
            exclusion: None,
        })
        .unwrap();

        db.insert("notes", note("n1", "only", 1)).unwrap();
        let duplicate = db.insert("notes", note("n2", "only", 2));
        assert!(
            duplicate.is_err(),
            "{mode}: unique index accepted a duplicate key"
        );
        assert!(
            db.get("notes", "n2").unwrap().is_none(),
            "{mode}: rejected insert must not leave the record behind"
        );
    });
}

// ---------------------------------------------------------------------------
// Vectors
// ---------------------------------------------------------------------------

#[test]
fn vector_search_ranks_the_nearest_record_first() {
    for_each_mode(|db, mode| {
        db.create_collection("embeds").unwrap();
        db.insert(
            "embeds",
            Record::new("east")
                .with_vector(vec![1.0, 0.0])
                .with_metadata(json!({})),
        )
        .unwrap();
        db.insert(
            "embeds",
            Record::new("north")
                .with_vector(vec![0.0, 1.0])
                .with_metadata(json!({})),
        )
        .unwrap();

        let hits = db.search_vector("embeds", &[0.9, 0.1], 2, None).unwrap();
        assert_eq!(hits.len(), 2, "{mode}");
        assert_eq!(hits[0].record.id, "east", "{mode}: wrong nearest neighbour");
    });
}

// ---------------------------------------------------------------------------
// Durability across reopen
// ---------------------------------------------------------------------------

#[test]
fn committed_data_survives_close_and_reopen() {
    for_each_mode_with_dir(|dir, mode| {
        {
            let mut db = open_in(dir, mode);
            db.create_collection("notes").unwrap();
            for i in 0..25 {
                db.insert("notes", note(&format!("n{i:03}"), "memo", i))
                    .unwrap();
            }
            db.delete("notes", "n003").unwrap();
            db.close().unwrap();
        }

        let db = open_in(dir, mode);
        assert_eq!(db.scan_collection("notes").unwrap().len(), 24, "{mode}");
        assert!(db.get("notes", "n000").unwrap().is_some(), "{mode}");
        assert!(
            db.get("notes", "n003").unwrap().is_none(),
            "{mode}: deleted record came back after reopen"
        );
    });
}

#[test]
fn indexes_are_queryable_after_reopen() {
    for_each_mode_with_dir(|dir, mode| {
        {
            let mut db = open_in(dir, mode);
            db.create_collection("notes").unwrap();
            db.create_index(IndexDefinition {
                name: "notes_kind".to_string(),
                collection: "notes".to_string(),
                fields: vec![IndexField::MetadataPath(vec!["kind".to_string()])],
                kind: IndexKind::BTree,
                unique: false,
                predicate: None,
                exclusion: None,
            })
            .unwrap();
            for i in 0..10 {
                db.insert("notes", note(&format!("n{i}"), "delta", i))
                    .unwrap();
            }
            db.close().unwrap();
        }

        let db = open_in(dir, mode);
        assert_eq!(
            db.lookup_index_exact("notes_kind", &[IndexValue::from("delta")])
                .unwrap()
                .len(),
            10,
            "{mode}: index did not survive reopen"
        );
    });
}

#[test]
fn storage_mode_is_stable_across_reopen() {
    for_each_mode_with_dir(|dir, mode| {
        {
            let mut db = open_in(dir, mode);
            db.create_collection("notes").unwrap();
            db.close().unwrap();
        }
        assert_eq!(
            &bicdb_core::storage_mode(dir.path()).unwrap(),
            mode,
            "reopening must not change the recorded storage mode"
        );
        let db = open_in(dir, mode);
        db.close().unwrap();
        assert_eq!(&bicdb_core::storage_mode(dir.path()).unwrap(), mode);
    });
}

// ---------------------------------------------------------------------------
// Memory envelope
// ---------------------------------------------------------------------------

/// `server_paged` must keep row *bytes* out of resident memory.
///
/// This test began life as its own inverse — asserting the two modes used
/// EQUAL memory, so the gap between the mode's name and its behavior could not
/// be forgotten. Eviction stubs closed that gap: in paged mode the shards keep
/// identity fields only, and metadata/payload bytes stay in the page store,
/// fetched on demand pinned to the version's own snapshot.
///
/// Two properties, asserted separately because they fail differently:
///
/// - **paged < memory**: the stubs are genuinely smaller than the rows. If
///   this fails, eviction silently stopped happening on some write path.
/// - **paged row bytes do not scale with payload size**: the per-row resident
///   cost is identity + bookkeeping, so quadrupling the payload must not move
///   `rows_bytes` materially. This is the one that catches a partial
///   regression — a path that retains payloads for only some rows would still
///   pass the first assertion.
#[test]
fn paged_mode_keeps_row_bytes_out_of_resident_memory() {
    let measure = |mode: &StorageMode, pad: usize| {
        let dir = TempDir::new().unwrap();
        let mut db = BicDb::open_with_config(
            dir.path(),
            DbConfig::default().with_storage_mode(mode.clone()),
        )
        .unwrap();
        db.create_collection("rows").unwrap();
        for index in 0..2_000 {
            db.insert(
                "rows",
                Record::new(format!("k{index:06}"))
                    .with_metadata(json!({ "pad": "x".repeat(pad), "n": index })),
            )
            .unwrap();
        }
        let report = db.residency_report().unwrap();
        println!(
            "[{mode} pad={pad}] rows={} versions={} pk_maps={} accounted={}",
            report.rows_bytes,
            report.version_chains_bytes,
            report.primary_key_maps_bytes,
            report.accounted_bytes,
        );
        report
    };

    let memory = measure(&StorageMode::EmbeddedMemory, 400);
    let paged = measure(&StorageMode::ServerPaged, 400);
    assert!(
        paged.accounted_bytes < memory.accounted_bytes,
        "server_paged no longer uses less resident memory than embedded_memory \
         ({} vs {}): eviction stubs stopped happening on some write path",
        paged.accounted_bytes,
        memory.accounted_bytes,
    );

    // The sharper property: resident row bytes must not scale with payload.
    let paged_wide = measure(&StorageMode::ServerPaged, 1_600);
    let growth = paged_wide.rows_bytes as f64 / paged.rows_bytes as f64;
    assert!(
        growth < 1.15,
        "quadrupling the payload grew paged resident rows_bytes by {growth:.2}x \
         ({} -> {}); some path is retaining row bytes in memory",
        paged.rows_bytes,
        paged_wide.rows_bytes,
    );
    // While the same payload growth must be fully visible in embedded mode —
    // if it is not, the *measurement* is broken, and the assertion above would
    // pass vacuously.
    let memory_wide = measure(&StorageMode::EmbeddedMemory, 1_600);
    assert!(
        memory_wide.rows_bytes as f64 > memory.rows_bytes as f64 * 2.0,
        "embedded rows_bytes did not track payload growth ({} -> {}); the \
         residency measurement itself is broken",
        memory.rows_bytes,
        memory_wide.rows_bytes,
    );
}

#[test]
fn residency_accounting_is_available_in_every_mode() {
    // Each mode must be able to state where its memory goes; a mode whose
    // accounting silently returned zero could pass every envelope gate in the
    // roadmap while using unbounded memory.
    for_each_mode(|db, mode| {
        db.create_collection("notes").unwrap();
        for i in 0..100 {
            db.insert("notes", note(&format!("n{i}"), "memo", i))
                .unwrap();
        }
        let report = db.residency_report().unwrap();
        assert_eq!(report.record_count, 100, "{mode}");
        assert!(
            report.accounted_bytes > 0,
            "{mode}: accounting reported zero bytes for 100 records"
        );
    });
}
